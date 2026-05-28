mod common;
mod config;
mod db;
mod matcher;
mod replay;
mod sim_clock;

use anyhow::{Context, Result};

use crate::common::{L2Order, Market, OrderType};
use crate::config::AppConfig;
use crate::matcher::order_book::{LevelSnapshot, OrderBook, OrderBookSnapshot};
use crate::replay::{ReplayController, ReplayEvent, ReplayHandler};

const DEBUG_TARGET_CODE: Option<&str> = Some("SH600410");
const DEBUG_SNAPSHOT_DEPTH: usize = 10;

struct OrderBookSnapshotHandler {
    target_code: Option<String>,
    book: OrderBook,
    snapshot_depth: usize,
}

impl OrderBookSnapshotHandler {
    fn new(target_code: Option<String>, snapshot_depth: usize) -> Self {
        Self {
            target_code,
            book: OrderBook::new(),
            snapshot_depth,
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

    fn format_side_levels(levels: &[LevelSnapshot], prefix: &str) -> String {
        (0..5)
            .map(|index| {
                if let Some(level) = levels.get(index) {
                    format!(
                        "{}{}={}:{}",
                        prefix,
                        index + 1,
                        level.price as f64 / 10000.0,
                        level.total_qty
                    )
                } else {
                    format!("{}{}=-", prefix, index + 1)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn current_snapshot(&mut self) -> OrderBookSnapshot {
        self.book.snapshot(self.snapshot_depth)
    }

    fn record_snapshot(&mut self, timestamp_ms: i64, code: &str) {
        let snapshot = self.current_snapshot();
        let bid_levels = Self::format_side_levels(&snapshot.bids, "bid");
        let ask_levels = Self::format_side_levels(&snapshot.asks, "ask");
        println!(
            "order_book_snapshot ts={} code={} {} {}",
            timestamp_ms, code, bid_levels, ask_levels
        );
    }
}

impl ReplayHandler for OrderBookSnapshotHandler {
    fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()> {
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

            self.record_snapshot(event.timestamp_ms(), &target_code);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let controller = ReplayController::new(config.db, config.replay);
    let mut handler =
        OrderBookSnapshotHandler::new(DEBUG_TARGET_CODE.map(str::to_string), DEBUG_SNAPSHOT_DEPTH);

    controller.replay(&mut handler).await?;
    Ok(())
}
