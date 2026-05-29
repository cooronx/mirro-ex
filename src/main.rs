mod common;
mod config;
mod db;
mod marketdata;
mod matcher;
mod publisher;
mod replay;
mod sim_clock;

use std::fs::{self, File};

use anyhow::{Context, Result};
use async_trait::async_trait;
use csv::Writer;

use crate::common::{L2Order, Market, OrderType};
use crate::config::AppConfig;
use crate::matcher::order_book::{LevelSnapshot, OrderBook, OrderBookSnapshot};
use crate::publisher::NatsDispatcher;
use crate::replay::{ReplayController, ReplayEvent, ReplayHandler};

const DEBUG_TARGET_CODE: Option<&str> = Some("SH600410");
const DEBUG_SNAPSHOT_DEPTH: usize = 10;

struct OrderBookSnapshotHandler {
    target_code: Option<String>,
    book: OrderBook,
    snapshot_depth: usize,
    writer: Writer<File>,
    dispatcher: NatsDispatcher,
}

impl OrderBookSnapshotHandler {
    fn new(
        target_code: Option<String>,
        snapshot_depth: usize,
        writer: Writer<File>,
        dispatcher: NatsDispatcher,
    ) -> Self {
        Self {
            target_code,
            book: OrderBook::new(),
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
        if code.starts_with("SH") || code.starts_with("SZ") {
            return code.to_string();
        }

        match market {
            Market::XSHG => format!("SH{code}"),
            Market::XSHE => format!("SZ{code}"),
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

    fn target_code_for_event(&mut self, event: &ReplayEvent) -> String {
        if self.target_code.is_none() {
            self.target_code = Some(self.canonical_event_code(event));
        }

        self.target_code
            .as_ref()
            .cloned()
            .expect("target code should be initialized")
    }

    fn level_cell(levels: &[LevelSnapshot], index: usize) -> String {
        levels
            .get(index)
            .map(|level| format!("{:.4}:{}", level.price as f64 / 10000.0, level.total_qty))
            .unwrap_or_default()
    }

    fn current_snapshot(&mut self) -> OrderBookSnapshot {
        self.book.snapshot(self.snapshot_depth)
    }

    async fn record_snapshot(&mut self, timestamp_ms: i64, code: &str) -> anyhow::Result<()> {
        let snapshot = self.current_snapshot();
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
            let target_code = self.target_code_for_event(event);
            if self.canonical_event_code(event) != target_code {
                continue;
            }

            match event {
                ReplayEvent::Order(order) => {
                    if !Self::should_track_order(order) {
                        continue;
                    }

                    self.book.apply_order(order.clone()).with_context(|| {
                        format!(
                            "failed to apply order for code={} channel={} channel_number={}",
                            order.code, order.channel, order.channel_number
                        )
                    })?;
                }
                ReplayEvent::Transaction(transaction) => {
                    self.book
                        .apply_transaction(transaction.clone())
                        .with_context(|| {
                            format!(
                                "failed to apply transaction for code={} channel={} channel_number={}",
                                transaction.code, transaction.channel, transaction.channel_number
                            )
                        })?;
                }
            }

            self.record_snapshot(event.timestamp_ms(), &target_code)
                .await?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
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

    let config = AppConfig::load()?;
    let dispatcher = NatsDispatcher::new(&config.nats)
        .await
        .context("failed to initialize nats dispatcher")?;
    let controller = ReplayController::new(config.db, config.replay);
    let mut handler = OrderBookSnapshotHandler::new(
        DEBUG_TARGET_CODE.map(str::to_string),
        DEBUG_SNAPSHOT_DEPTH,
        writer,
        dispatcher,
    );

    controller.replay(&mut handler).await?;
    handler.flush()?;
    Ok(())
}
