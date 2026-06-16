use std::collections::HashSet;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::info;

use crate::config::AppConfig;
use crate::orderbook_worker::OrderBookWorkerPool;
use crate::publisher::NatsDispatcher;
use crate::replay::{
    ReplayControl, ReplayController, ReplayEvent, ReplayHandler, ReplayRunReport,
    ReplayStatusReporter,
};
use crate::replay_manager::ReplayTaskConfig;
use crate::trading::{TradingStore, trading_db_path_from_config};
use tokio::sync::mpsc;

struct OrderBookSnapshotHandler {
    workers: OrderBookWorkerPool,
    _dispatcher: NatsDispatcher,
}

impl OrderBookSnapshotHandler {
    fn new(
        worker_count: usize,
        tracked_codes: Option<HashSet<String>>,
        snapshot_depth: usize,
        write_snapshot_parquet: bool,
        snapshot_parquet_dir: String,
        trading_store: TradingStore,
        dispatcher: NatsDispatcher,
    ) -> Result<Self> {
        Ok(Self {
            workers: OrderBookWorkerPool::new(
                worker_count,
                tracked_codes,
                snapshot_depth,
                write_snapshot_parquet,
                snapshot_parquet_dir,
                Some(trading_store),
            )?,
            _dispatcher: dispatcher,
        })
    }
}

#[async_trait]
impl ReplayHandler for OrderBookSnapshotHandler {
    async fn on_day_start(&mut self, day: &str) -> anyhow::Result<()> {
        self.workers.start_day(day).await?;
        info!(day = %day, "reset order books for replay day");
        Ok(())
    }

    async fn on_events(&mut self, events: Vec<ReplayEvent>) -> anyhow::Result<()> {
        self.workers.process_events(events).await
    }

    async fn on_day_end(&mut self, day: &str) -> anyhow::Result<()> {
        self.workers.end_day().await?;
        info!(day = %day, "flushed order books for replay day");
        Ok(())
    }
}

pub async fn run_with_control(
    config: AppConfig,
    task_config: ReplayTaskConfig,
    command_rx: mpsc::UnboundedReceiver<crate::replay::ReplayCommand>,
    status_reporter: ReplayStatusReporter,
) -> Result<ReplayRunReport> {
    run_internal(
        config,
        task_config,
        Some(ReplayControl {
            command_rx,
            status_reporter,
        }),
    )
    .await
}

async fn run_internal(
    config: AppConfig,
    task_config: ReplayTaskConfig,
    control: Option<ReplayControl>,
) -> Result<ReplayRunReport> {
    let parquet_output_dir = config.replay.snapshot_parquet_dir.clone();
    let write_snapshot_parquet = config.replay.write_snapshot_parquet;
    let trading_db_path = trading_db_path_from_config(&config.db.schema.trading_db_path)?;
    let trading_store = TradingStore::new(trading_db_path);

    info!(
        write_snapshot_parquet = write_snapshot_parquet,
        snapshot_parquet_dir = %parquet_output_dir,
        db_url = %config.db.url,
        db_name = %config.db.database,
        sh_order_table = %config.db.tables.sh_order,
        sz_order_table = %config.db.tables.sz_order,
        transaction_table = %config.db.tables.transaction,
        nats_subject = %config.nats.subject,
        replay_start_date = %task_config.replay_start_date,
        replay_end_date = %task_config.replay_end_date,
        replay_start_time = %task_config.replay_start_time.format("%H:%M:%S%.3f"),
        replay_end_time = %task_config.replay_end_time.format("%H:%M:%S%.3f"),
        replay_speed = task_config.replay_speed,
        tick_interval_ms = config.replay.tick_interval_ms,
        batch_size = config.replay.batch_size,
        orderbook_workers = config.replay.orderbook_workers,
        snapshot_depth = config.replay.snapshot_depth,
        skip_intraday_breaks = task_config.skip_intraday_breaks,
        replay_codes = ?task_config.replay_codes,
        "starting replay"
    );

    let dispatcher = NatsDispatcher::new(&config.nats)
        .await
        .context("failed to initialize nats dispatcher")?;
    let tracked_codes = if task_config.replay_codes.is_empty() {
        None
    } else {
        Some(
            task_config
                .replay_codes
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
        )
    };
    let snapshot_depth = config.replay.snapshot_depth;
    let orderbook_workers = config.replay.orderbook_workers;
    let controller = ReplayController::new(config.db, config.replay, task_config);
    let mut handler = OrderBookSnapshotHandler::new(
        orderbook_workers,
        tracked_codes,
        snapshot_depth,
        write_snapshot_parquet,
        parquet_output_dir,
        trading_store,
        dispatcher,
    )?;

    let report = controller
        .replay_with_control(&mut handler, control)
        .await?;
    info!(
        ticks = report.ticks,
        total_events = report.total_events,
        total_order_rows = report.total_order_rows,
        total_transaction_rows = report.total_transaction_rows,
        max_lag_ms = report.max_lag_ms,
        avg_lag_ms = report.avg_lag_ms,
        final_lag_ms = report.final_lag_ms,
        skipped_days = ?report.skipped_days,
        total_elapsed_ms = report.total_elapsed_ms,
        "replay finished"
    );
    Ok(report)
}
