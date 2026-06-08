use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::info;

use crate::common::{L2Order, Market, OrderType};
use crate::config::AppConfig;
use crate::matcher::order_book::{OrderBook, OrderBookSnapshot};
use crate::publisher::NatsDispatcher;
use crate::replay::{
    ReplayControl, ReplayController, ReplayEvent, ReplayHandler, ReplayRunReport,
    ReplayStatusReporter,
};
use crate::replay_manager::ReplayTaskConfig;
use crate::snapshot_exporter::SnapshotParquetExporter;
use tokio::sync::mpsc;

struct OrderBookSnapshotHandler {
    tracked_codes: Option<HashSet<String>>,
    books: HashMap<String, OrderBook>,
    last_event_timestamps: HashMap<String, i64>,
    snapshot_depth: usize,
    exporter: Option<SnapshotParquetExporter>,
    dispatcher: NatsDispatcher,
}

impl OrderBookSnapshotHandler {
    fn new(
        tracked_codes: Option<HashSet<String>>,
        snapshot_depth: usize,
        exporter: Option<SnapshotParquetExporter>,
        dispatcher: NatsDispatcher,
    ) -> Self {
        Self {
            tracked_codes,
            books: HashMap::new(),
            last_event_timestamps: HashMap::new(),
            snapshot_depth,
            exporter,
            dispatcher,
        }
    }

    fn should_track_order(order: &L2Order) -> bool {
        matches!(order.order_type, OrderType::Limit)
            || matches!(order.order_type, OrderType::Market)
            || matches!(order.order_type, OrderType::BestOwn)
            || (matches!(order.order_type, OrderType::Cancel)
                && matches!(order.market, crate::common::Market::XSHG))
    }

    fn canonical_code(code: &str, market: Market) -> String {
        if code.ends_with(".XSHG") || code.ends_with(".XSHE") {
            return code.to_string();
        }

        match market {
            Market::XSHG => format!("{code}.XSHG"),
            Market::XSHE => format!("{code}.XSHE"),
            Market::Unknown => code.to_string(),
        }
    }

    fn canonical_event_code(&self, event: &ReplayEvent) -> String {
        match event {
            ReplayEvent::Order(order) => Self::canonical_code(&order.code, order.market),
            ReplayEvent::Transaction(transaction) => {
                Self::canonical_code(&transaction.code, transaction.market)
            }
        }
    }

    fn should_track_code(&self, code: &str) -> bool {
        match &self.tracked_codes {
            Some(tracked_codes) => tracked_codes.contains(code),
            None => true,
        }
    }

    fn book_for_code(&mut self, code: &str) -> &mut OrderBook {
        self.books
            .entry(code.to_string())
            .or_insert_with(OrderBook::new)
    }

    fn current_snapshot(&mut self, code: &str) -> OrderBookSnapshot {
        let snapshot_depth = self.snapshot_depth;
        self.book_for_code(code).snapshot(snapshot_depth)
    }

    fn can_emit_snapshot(&mut self, code: &str) -> bool {
        !self.book_for_code(code).has_unsettled_holdings()
    }

    async fn record_snapshot(&mut self, timestamp_ms: i64, code: &str) -> anyhow::Result<()> {
        let snapshot = self.current_snapshot(code);
        if let Some(exporter) = self.exporter.as_mut() {
            exporter
                .write_snapshot(timestamp_ms, code, &snapshot)
                .context("failed to write order book snapshot parquet row")?;
        }
        // self.dispatcher
        //     .publish_snapshot(timestamp_ms, code, snapshot)
        //     .await
        //     .context("failed to publish order book snapshot to nats")?;

        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        let codes = self.books.keys().cloned().collect::<Vec<_>>();
        for code in codes {
            let Some(timestamp_ms) = self.last_event_timestamps.get(&code).copied() else {
                continue;
            };
            let changed = self
                .book_for_code(&code)
                .finalize_all_holdings()
                .with_context(|| format!("failed to finalize holdings for code={code}"))?;
            if changed && self.can_emit_snapshot(&code) {
                self.record_snapshot(timestamp_ms, &code).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ReplayHandler for OrderBookSnapshotHandler {
    async fn on_day_start(&mut self, day: &str) -> anyhow::Result<()> {
        self.books.clear();
        self.last_event_timestamps.clear();
        if let Some(exporter) = self.exporter.as_mut() {
            exporter.start_day(day)?;
        }
        info!(day = %day, "reset order books for replay day");
        Ok(())
    }

    async fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()> {
        for event in events {
            let event_code = self.canonical_event_code(event);
            if !self.should_track_code(&event_code) {
                continue;
            }
            self.last_event_timestamps
                .insert(event_code.clone(), event.timestamp_ms());

            match event {
                ReplayEvent::Order(order) => {
                    if !Self::should_track_order(order) {
                        continue;
                    }

                    self.book_for_code(&event_code)
                        .apply_order(order.clone())
                        .with_context(|| {
                            format!(
                                "failed to apply order for code={} channel={} message_number={}",
                                order.code, order.channel, order.message_number
                            )
                        })?;
                }
                ReplayEvent::Transaction(transaction) => {
                    self.book_for_code(&event_code)
                        .apply_transaction(transaction.clone())
                        .with_context(|| {
                            format!(
                                "failed to apply transaction for code={} channel={} message_number={}",
                                transaction.code, transaction.channel, transaction.message_number
                            )
                        })?;
                }
            }

            if self.can_emit_snapshot(&event_code) {
                self.record_snapshot(event.timestamp_ms(), &event_code)
                    .await?;
            }
        }

        Ok(())
    }

    async fn on_day_end(&mut self, day: &str) -> anyhow::Result<()> {
        self.flush().await?;
        if let Some(exporter) = self.exporter.as_mut() {
            exporter.close_day()?;
        }
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
    let exporter = if write_snapshot_parquet {
        Some(SnapshotParquetExporter::new(&parquet_output_dir))
    } else {
        None
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
    let controller = ReplayController::new(config.db, config.replay, task_config);
    let mut handler =
        OrderBookSnapshotHandler::new(tracked_codes, snapshot_depth, exporter, dispatcher);

    let report = controller
        .replay_with_control(&mut handler, control)
        .await?;
    handler.flush().await?;
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
