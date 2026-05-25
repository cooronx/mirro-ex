mod common;
mod config;
mod db;
mod matcher;
mod replay;
mod sim_clock;

use std::env;

use anyhow::{Context, Result};

use crate::common::{L2Order, Market, OrderType};
use crate::config::AppConfig;
use crate::matcher::order_book::{AuctionAwareOrderBook, OrderBookSnapshot};
use crate::replay::{ReplayController, ReplayEvent, ReplayHandler, ReplayRequest};

const DEBUG_TARGET_CODE: Option<&str> = Some("SH600410");
const DEBUG_SNAPSHOT_DEPTH: usize = 10;
const VERIFY_ORDER_BOOK_INVARIANTS: bool = false;

struct OrderBookDebugHandler {
    target_code: Option<String>,
    book: AuctionAwareOrderBook,
    snapshot_depth: usize,
    print_stats: bool,
    pending_snapshot_second_ms: Option<i64>,
    pending_snapshot: Option<OrderBookSnapshot>,
    matched_event_count: usize,
}

impl OrderBookDebugHandler {
    fn new(target_code: Option<String>, snapshot_depth: usize, print_stats: bool) -> Self {
        Self {
            target_code,
            book: AuctionAwareOrderBook::new(),
            snapshot_depth,
            print_stats,
            pending_snapshot_second_ms: None,
            pending_snapshot: None,
            matched_event_count: 0,
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

    fn select_target_code(&mut self, event: &ReplayEvent) -> String {
        if self.target_code.is_none() {
            let code = self.canonical_event_code(event);
            println!("order_book_debug_target code={code}");
            self.target_code = Some(code);
        }

        self.target_code
            .as_ref()
            .cloned()
            .expect("target code should be initialized")
    }

    fn format_side_levels(
        levels: &[crate::matcher::order_book::LevelSnapshot],
        prefix: &str,
    ) -> String {
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

    fn snapshot_second_ms(timestamp_ms: i64) -> i64 {
        (timestamp_ms / 1000) * 1000
    }

    fn current_snapshot(&mut self) -> OrderBookSnapshot {
        self.book.snapshot(self.snapshot_depth)
    }

    fn record_snapshot(&mut self, timestamp_ms: i64, code: &str) {
        let snapshot = self.current_snapshot();
        let second_ms = Self::snapshot_second_ms(timestamp_ms);
        match self.pending_snapshot_second_ms {
            Some(pending_second_ms) if pending_second_ms == second_ms => {
                self.pending_snapshot = Some(snapshot);
            }
            Some(_) => {
                self.emit_pending_snapshot(code);
                self.pending_snapshot_second_ms = Some(second_ms);
                self.pending_snapshot = Some(snapshot);
            }
            None => {
                self.pending_snapshot_second_ms = Some(second_ms);
                self.pending_snapshot = Some(snapshot);
            }
        }
    }

    fn emit_pending_snapshot(&mut self, code: &str) {
        let (Some(second_ms), Some(snapshot)) = (
            self.pending_snapshot_second_ms.take(),
            self.pending_snapshot.take(),
        ) else {
            return;
        };

        let bid_levels = Self::format_side_levels(&snapshot.bids, "bid");
        let ask_levels = Self::format_side_levels(&snapshot.asks, "ask");
        println!(
            "order_book_snapshot ts={} code={} {} {}",
            second_ms, code, bid_levels, ask_levels
        );
        if self.print_stats {
            let stats = self.book.stats();
            println!(
                "order_book_stats ts={} code={} live_orders={} stale_slots={} bid_levels={} ask_levels={} bid_slots={} ask_slots={} pending_cancels={} pending_reductions={}",
                second_ms,
                code,
                stats.live_orders,
                stats.stale_slots,
                stats.bid_levels,
                stats.ask_levels,
                stats.bid_slots,
                stats.ask_slots,
                stats.pending_cancels,
                stats.pending_reductions,
            );
        }
    }

    fn flush_pending_snapshot_before(&mut self, next_timestamp_ms: i64, code: &str) {
        let next_second_ms = Self::snapshot_second_ms(next_timestamp_ms);
        if self
            .pending_snapshot_second_ms
            .is_some_and(|pending_second_ms| pending_second_ms < next_second_ms)
        {
            self.emit_pending_snapshot(code);
        }
    }

    fn flush_before_timestamp(&mut self, next_timestamp_ms: i64) -> anyhow::Result<()> {
        let Some(code) = self.target_code.clone() else {
            return Ok(());
        };

        self.flush_pending_snapshot_before(next_timestamp_ms, &code);

        if let Some(snapshot_timestamp_ms) = self
            .book
            .flush_before_timestamp(next_timestamp_ms)
            .context("failed to flush opening auction before next timestamp")?
        {
            self.record_snapshot(snapshot_timestamp_ms, &code);
            self.flush_pending_snapshot_before(next_timestamp_ms, &code);
        }

        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        let Some(code) = self.target_code.clone() else {
            return Ok(());
        };

        if let Some(snapshot_timestamp_ms) = self
            .book
            .finish()
            .context("failed to finalize opening auction at end of replay")?
        {
            self.record_snapshot(snapshot_timestamp_ms, &code);
        }
        self.emit_pending_snapshot(&code);

        Ok(())
    }
}

impl ReplayHandler for OrderBookDebugHandler {
    fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()> {
        for event in events {
            self.flush_before_timestamp(event.timestamp_ms())?;
            let target_code = self.select_target_code(event);
            let event_code = self.canonical_event_code(event);
            if event_code != target_code {
                continue;
            }

            let snapshot_timestamp_ms = match event {
                ReplayEvent::Order(order) => {
                    if !Self::should_track_order(order) {
                        continue;
                    }

                    self.book.apply_order(order.clone()).with_context(|| {
                        format!(
                            "failed to apply order for code={} channel={} channel_number={}",
                            order.code, order.channel, order.channel_number
                        )
                    })?
                }
                ReplayEvent::Transaction(transaction) => self
                    .book
                    .apply_transaction(transaction.clone())
                    .with_context(|| {
                        format!(
                            "failed to apply transaction for code={} channel={} channel_number={}",
                            transaction.code, transaction.channel, transaction.channel_number
                        )
                    })?,
            };
            if VERIFY_ORDER_BOOK_INVARIANTS {
                self.book.verify_invariants().map_err(|message| {
                    anyhow::anyhow!(
                        "order book invariant violation after code={} event_ts={} matched_event_count={}: {}",
                        target_code,
                        event.timestamp_ms(),
                        self.matched_event_count + 1,
                        message
                    )
                })?;
            }
            self.matched_event_count += 1;

            if let Some(snapshot_timestamp_ms) = snapshot_timestamp_ms {
                self.record_snapshot(snapshot_timestamp_ms, &target_code);
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let request = ReplayRequest {
        start_date: config.replay.replay_start_date,
        end_date: config.replay.replay_end_date,
        start_time: config.replay.replay_start_time,
        end_time: config.replay.replay_end_time,
        replay_speed: config.replay.replay_speed,
    };
    let controller = ReplayController::new(config.db, config.replay);
    let mut handler = OrderBookDebugHandler::new(
        DEBUG_TARGET_CODE.map(str::to_string),
        DEBUG_SNAPSHOT_DEPTH,
        env_flag("MIRRO_ORDER_BOOK_DIAGNOSTICS"),
    );

    let report = controller.replay(request, &mut handler).await?;
    handler.finish()?;
    println!("{report:#?}");
    Ok(())
}

fn env_flag(key: &str) -> bool {
    env::var(key)
        .ok()
        .map(|value| {
            let value = value.trim();
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}
