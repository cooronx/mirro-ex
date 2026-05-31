use std::collections::{HashMap, HashSet};
use std::fs::{self, File};

use anyhow::{Context, Result};
use async_trait::async_trait;
use csv::Writer;
use tracing::info;

use crate::common::{L2Order, Market, OrderType};
use crate::config::AppConfig;
use crate::matcher::order_book::{LevelSnapshot, OrderBook, OrderBookSnapshot};
use crate::publisher::NatsDispatcher;
use crate::replay::{ReplayController, ReplayEvent, ReplayHandler};

struct OrderBookSnapshotHandler {
    tracked_codes: Option<HashSet<String>>,
    books: HashMap<String, OrderBook>,
    snapshot_depth: usize,
    writer: Writer<File>,
    dispatcher: NatsDispatcher,
}

impl OrderBookSnapshotHandler {
    fn new(
        tracked_codes: Option<HashSet<String>>,
        snapshot_depth: usize,
        writer: Writer<File>,
        dispatcher: NatsDispatcher,
    ) -> Self {
        Self {
            tracked_codes,
            books: HashMap::new(),
            snapshot_depth,
            writer,
            dispatcher,
        }
    }

    fn should_track_order(order: &L2Order) -> bool {
        matches!(order.order_type, OrderType::Limit)
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

    fn level_cell(levels: &[LevelSnapshot], index: usize) -> String {
        levels
            .get(index)
            .map(|level| format!("{:.4}:{}", level.price as f64 / 10000.0, level.total_qty))
            .unwrap_or_default()
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

    async fn record_snapshot(&mut self, timestamp_ms: i64, code: &str) -> anyhow::Result<()> {
        let snapshot = self.current_snapshot(code);
        let mut row = vec![timestamp_ms.to_string(), code.to_string()];
        for index in 0..5 {
            row.push(Self::level_cell(&snapshot.bids, index));
        }
        for index in 0..5 {
            row.push(Self::level_cell(&snapshot.asks, index));
        }

        self.writer
            .write_record(&row)
            .context("failed to write order book snapshot csv row")?;
        self.dispatcher
            .publish_snapshot(timestamp_ms, code, snapshot)
            .await
            .context("failed to publish order book snapshot to nats")?;

        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.writer
            .flush()
            .context("failed to flush order book snapshot csv writer")
    }
}

#[async_trait]
impl ReplayHandler for OrderBookSnapshotHandler {
    async fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()> {
        for event in events {
            let event_code = self.canonical_event_code(event);
            if !self.should_track_code(&event_code) {
                continue;
            }

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

            self.record_snapshot(event.timestamp_ms(), &event_code)
                .await?;
        }

        Ok(())
    }
}

pub async fn run(config: AppConfig) -> Result<()> {
    fs::create_dir_all("data").context("failed to create data directory")?;
    let output_file =
        File::create("data/order_book_snapshot.csv").context("failed to create output csv")?;
    let mut writer = Writer::from_writer(output_file);
    writer
        .write_record([
            "ts", "code", "bid1", "bid2", "bid3", "bid4", "bid5", "ask1", "ask2", "ask3", "ask4",
            "ask5",
        ])
        .context("failed to write csv header")?;

    info!(
        output_path = "data/order_book_snapshot.csv",
        db_url = %config.db.url,
        db_name = %config.db.database,
        sh_order_table = %config.db.tables.sh_order,
        sz_order_table = %config.db.tables.sz_order,
        transaction_table = %config.db.tables.transaction,
        nats_subject = %config.nats.subject,
        replay_start_date = %config.replay.replay_start_date,
        replay_end_date = %config.replay.replay_end_date,
        replay_start_time = %config.replay.replay_start_time.format("%H:%M:%S%.3f"),
        replay_end_time = %config.replay.replay_end_time.format("%H:%M:%S%.3f"),
        replay_speed = config.replay.replay_speed,
        batch_size = config.replay.batch_size,
        snapshot_depth = config.replay.snapshot_depth,
        skip_intraday_breaks = config.replay.skip_intraday_breaks,
        replay_codes = ?config.replay.replay_codes,
        "starting replay"
    );

    let dispatcher = NatsDispatcher::new(&config.nats)
        .await
        .context("failed to initialize nats dispatcher")?;
    let tracked_codes = config
        .replay
        .replay_codes
        .clone()
        .map(|codes| codes.into_iter().collect::<HashSet<_>>());
    let snapshot_depth = config.replay.snapshot_depth;
    let controller = ReplayController::new(config.db, config.replay);
    let mut handler =
        OrderBookSnapshotHandler::new(tracked_codes, snapshot_depth, writer, dispatcher);

    let report = controller.replay(&mut handler).await?;
    handler.flush()?;
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
    Ok(())
}
