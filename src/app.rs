use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use crate::config::AppConfig;
use crate::orderbook_worker::{MarketSnapshotUpdate, OrderBookWorkerPool};
use crate::publisher::NatsDispatcher;
use crate::replay::{
    ReplayControl, ReplayController, ReplayEvent, ReplayHandler, ReplayHandlerPerfSnapshot,
    ReplayRunReport, ReplayStatusReporter, SequencedReplayEvent,
};
use crate::replay_manager::ReplayTaskConfig;
use crate::trading::{TradingStore, trading_db_path_from_config};
use crate::webdata::EventBus;
use crate::webdata::MarketState;
use tokio::sync::mpsc;

struct OrderBookSnapshotHandler {
    workers: OrderBookWorkerPool,
    dispatcher: NatsDispatcher,
    market_state: MarketState,
    next_event_sequence: u64,
    last_watermark_ms: Option<i64>,
    last_event_timestamp_ms: Option<i64>,
    last_perf: Option<ReplayHandlerPerfSnapshot>,
}

impl OrderBookSnapshotHandler {
    fn new(
        worker_count: usize,
        tracked_codes: Option<HashSet<String>>,
        snapshot_depth: usize,
        write_snapshot_parquet: bool,
        snapshot_parquet_dir: String,
        trading_store: TradingStore,
        market_state: MarketState,
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
            dispatcher,
            market_state,
            next_event_sequence: 1,
            last_watermark_ms: None,
            last_event_timestamp_ms: None,
            last_perf: None,
        })
    }
}

fn next_second_watermark(
    safe_emit_time_ms: i64,
    last_event_timestamp_ms: Option<i64>,
    last_watermark_ms: Option<i64>,
) -> Option<i64> {
    let effective_safe_time = if safe_emit_time_ms == i64::MAX {
        last_event_timestamp_ms?.saturating_add(1_000)
    } else {
        safe_emit_time_ms
    };
    let boundary = effective_safe_time.div_euclid(1_000) * 1_000;
    if last_watermark_ms.is_some_and(|last| boundary <= last) {
        None
    } else {
        Some(boundary)
    }
}

fn validate_event_timestamp(timestamp_ms: i64, watermark_ms: Option<i64>) -> Result<()> {
    if watermark_ms.is_some_and(|watermark| timestamp_ms < watermark) {
        anyhow::bail!("event timestamp {timestamp_ms} is older than watermark {watermark_ms:?}");
    }
    Ok(())
}

#[async_trait]
impl ReplayHandler for OrderBookSnapshotHandler {
    async fn on_day_start(&mut self, day: &str) -> anyhow::Result<()> {
        self.workers.start_day(day).await?;
        info!(day = %day, "reset order books for replay day");
        Ok(())
    }

    async fn on_events(&mut self, events: Vec<ReplayEvent>) -> anyhow::Result<()> {
        let sequenced_events = events
            .into_iter()
            .filter(|event| self.workers.should_track_event(event))
            .map(|event| {
                let sequence = self.next_event_sequence;
                self.next_event_sequence += 1;
                self.last_event_timestamp_ms = Some(event.timestamp_ms());
                SequencedReplayEvent::new(sequence, event)
            })
            .collect::<Vec<_>>();
        let worker_result = self
            .workers
            .process_events(sequenced_events.clone())
            .await?;
        let snapshots = worker_result
            .snapshots
            .into_iter()
            .map(|snapshot| (snapshot.sequence, snapshot))
            .collect::<HashMap<_, _>>();

        for event in sequenced_events {
            validate_event_timestamp(event.timestamp_ms(), self.last_watermark_ms)?;
            self.dispatcher
                .publish_raw_event(event.sequence, &event.event)
                .await?;
            if let Some(snapshot) = snapshots.get(&event.sequence) {
                self.publish_snapshot(snapshot).await?;
            }
        }

        self.last_perf = Some(worker_result.perf);
        Ok(())
    }

    async fn on_watermark(&mut self, safe_emit_time_ms: i64) -> anyhow::Result<()> {
        let Some(boundary) = next_second_watermark(
            safe_emit_time_ms,
            self.last_event_timestamp_ms,
            self.last_watermark_ms,
        ) else {
            return Ok(());
        };

        self.dispatcher.publish_watermark(boundary).await?;
        self.last_watermark_ms = Some(boundary);
        Ok(())
    }

    fn last_perf_snapshot(&self) -> Option<ReplayHandlerPerfSnapshot> {
        self.last_perf.clone()
    }

    async fn on_day_end(&mut self, day: &str) -> anyhow::Result<()> {
        let mut snapshots = self.workers.end_day().await?;
        for snapshot in &mut snapshots {
            snapshot.sequence = self.next_event_sequence;
            self.next_event_sequence += 1;
            self.last_event_timestamp_ms = Some(
                self.last_event_timestamp_ms
                    .map_or(snapshot.timestamp_ms, |last| {
                        last.max(snapshot.timestamp_ms)
                    }),
            );
            self.publish_snapshot(snapshot).await?;
        }
        info!(day = %day, "flushed order books for replay day");
        Ok(())
    }
}

impl OrderBookSnapshotHandler {
    async fn publish_snapshot(&mut self, snapshot: &MarketSnapshotUpdate) -> anyhow::Result<()> {
        validate_event_timestamp(snapshot.timestamp_ms, self.last_watermark_ms)?;
        self.dispatcher
            .publish_snapshot(
                snapshot.sequence,
                snapshot.timestamp_ms,
                &snapshot.code,
                snapshot.snapshot.clone(),
            )
            .await?;
        self.market_state.update(
            &snapshot.code,
            snapshot.timestamp_ms,
            snapshot.last_price,
            snapshot.is_call_auction,
            &snapshot.snapshot,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{next_second_watermark, validate_event_timestamp};

    #[test]
    fn watermark_is_a_half_open_second_boundary() {
        assert_eq!(next_second_watermark(1_999, Some(1_234), None), Some(1_000));
        assert_eq!(
            next_second_watermark(2_000, Some(1_999), Some(1_000)),
            Some(2_000)
        );
        assert_eq!(next_second_watermark(2_999, Some(2_500), Some(2_000)), None);
    }

    #[test]
    fn rejects_event_older_than_published_watermark() {
        assert!(validate_event_timestamp(999, Some(1_000)).is_err());
        assert!(validate_event_timestamp(1_000, Some(1_000)).is_ok());
    }
}

pub async fn run_with_control(
    config: AppConfig,
    task_config: ReplayTaskConfig,
    command_rx: mpsc::UnboundedReceiver<crate::replay::ReplayCommand>,
    status_reporter: ReplayStatusReporter,
    market_state: MarketState,
    event_bus: Option<EventBus>,
) -> Result<ReplayRunReport> {
    run_internal(
        config,
        task_config,
        Some(ReplayControl {
            command_rx,
            status_reporter,
        }),
        market_state,
        event_bus,
    )
    .await
}

async fn run_internal(
    config: AppConfig,
    task_config: ReplayTaskConfig,
    control: Option<ReplayControl>,
    market_state: MarketState,
    event_bus: Option<EventBus>,
) -> Result<ReplayRunReport> {
    let parquet_output_dir = config.replay.snapshot_parquet_dir.clone();
    let write_snapshot_parquet = config.replay.write_snapshot_parquet;
    let trading_db_path = trading_db_path_from_config(&config.db.schema.trading_db_path)?;
    let trading_store = match event_bus {
        Some(event_bus) => TradingStore::with_event_bus(trading_db_path, event_bus),
        None => TradingStore::new(trading_db_path),
    };

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

    let replay_run_id = format!(
        "{}-{}-{}",
        task_config.replay_start_date,
        task_config.replay_end_date,
        Utc::now().timestamp_millis()
    );
    let dispatcher = NatsDispatcher::new(&config.nats, replay_run_id)
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
        market_state,
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
