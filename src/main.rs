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
const DEFAULT_AUDIT_START_MS: i64 = 1_778_721_900_000;
const DEFAULT_AUDIT_END_MS: i64 = 1_778_722_200_000;

#[derive(Clone, Copy)]
struct OrderBookAuditConfig {
    start_ms: i64,
    end_ms: i64,
}

struct OrderBookDebugHandler {
    target_code: Option<String>,
    book: AuctionAwareOrderBook,
    snapshot_depth: usize,
    print_stats: bool,
    audit_config: Option<OrderBookAuditConfig>,
    matched_event_count: usize,
}

impl OrderBookDebugHandler {
    fn new(
        target_code: Option<String>,
        snapshot_depth: usize,
        print_stats: bool,
        audit_config: Option<OrderBookAuditConfig>,
    ) -> Self {
        Self {
            target_code,
            book: AuctionAwareOrderBook::new(),
            snapshot_depth,
            print_stats,
            audit_config,
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

    fn should_audit_event(&self, event: &ReplayEvent, target_code: &str) -> bool {
        let Some(audit_config) = self.audit_config else {
            return false;
        };

        let event_code = self.canonical_event_code(event);
        let timestamp_ms = event.timestamp_ms();
        event_code == target_code
            && timestamp_ms >= audit_config.start_ms
            && timestamp_ms < audit_config.end_ms
    }

    fn current_snapshot(&mut self) -> OrderBookSnapshot {
        self.book.snapshot(self.snapshot_depth)
    }

    fn format_top_of_book(snapshot: &OrderBookSnapshot) -> String {
        let bid = snapshot
            .bids
            .first()
            .map(|level| format!("{:.4}:{}", level.price as f64 / 10000.0, level.total_qty))
            .unwrap_or_else(|| "-".to_string());
        let ask = snapshot
            .asks
            .first()
            .map(|level| format!("{:.4}:{}", level.price as f64 / 10000.0, level.total_qty))
            .unwrap_or_else(|| "-".to_string());
        format!("bid1={bid} ask1={ask}")
    }

    fn format_event_brief(event: &ReplayEvent) -> String {
        match event {
            ReplayEvent::Order(order) => format!(
                "type=order ts={} channel={} channel_number={} order_number={} order_type={:?} direction={:?} price={:.4} volume={}",
                order.timestamp_ms,
                order.channel,
                order.channel_number,
                order.extra_message_number,
                order.order_type,
                order.direction,
                order.price as f64 / 10000.0,
                order.volume
            ),
            ReplayEvent::Transaction(transaction) => format!(
                "type=transaction ts={} channel={} channel_number={} buy_order_number={} sell_order_number={} deal_type={} price={:.4} volume={}",
                transaction.timestamp_ms,
                transaction.channel,
                transaction.channel_number,
                transaction.buy_order_number,
                transaction.sell_order_number,
                transaction.deal_type.trim(),
                transaction.price as f64 / 10000.0,
                transaction.volume
            ),
        }
    }

    fn log_audit_event(&mut self, target_code: &str, event: &ReplayEvent, stage: &str) {
        let snapshot = self.current_snapshot();
        let stats = self.book.stats();
        println!(
            "order_book_audit stage={} code={} {} {} live_orders={} stale_slots={} pending_cancels={} pending_reductions={}",
            stage,
            target_code,
            Self::format_event_brief(event),
            Self::format_top_of_book(&snapshot),
            stats.live_orders,
            stats.stale_slots,
            stats.pending_cancels,
            stats.pending_reductions,
        );
    }

    fn record_snapshot(&mut self, timestamp_ms: i64, code: &str) {
        let snapshot = self.current_snapshot();
        let bid_levels = Self::format_side_levels(&snapshot.bids, "bid");
        let ask_levels = Self::format_side_levels(&snapshot.asks, "ask");
        println!(
            "order_book_snapshot ts={} code={} {} {}",
            timestamp_ms, code, bid_levels, ask_levels
        );
        if self.print_stats {
            let stats = self.book.stats();
            println!(
                "order_book_stats ts={} code={} live_orders={} stale_slots={} bid_levels={} ask_levels={} bid_slots={} ask_slots={} pending_cancels={} pending_reductions={}",
                timestamp_ms,
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

    fn flush_before_timestamp(&mut self, next_timestamp_ms: i64) -> anyhow::Result<()> {
        let Some(code) = self.target_code.clone() else {
            return Ok(());
        };

        if let Some(snapshot_timestamp_ms) = self
            .book
            .flush_before_timestamp(next_timestamp_ms)
            .context("failed to flush opening auction before next timestamp")?
        {
            self.record_snapshot(snapshot_timestamp_ms, &code);
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

        Ok(())
    }
}

impl ReplayHandler for OrderBookDebugHandler {
    fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()> {
        for event in events {
            let target_code = self.select_target_code(event);
            self.flush_before_timestamp(event.timestamp_ms())?;
            let event_code = self.canonical_event_code(event);
            if event_code != target_code {
                continue;
            }
            let should_audit = self.should_audit_event(event, &target_code);
            if should_audit {
                self.log_audit_event(&target_code, event, "before");
            }

            match event {
                ReplayEvent::Order(order) => {
                    if !Self::should_track_order(order) {
                        continue;
                    }

                    let _ = self.book.apply_order(order.clone()).with_context(|| {
                        format!(
                            "failed to apply order for code={} channel={} channel_number={}",
                            order.code, order.channel, order.channel_number
                        )
                    })?;
                }
                ReplayEvent::Transaction(transaction) => {
                    let _ = self
                        .book
                        .apply_transaction(transaction.clone())
                        .with_context(|| {
                            format!(
                                "failed to apply transaction for code={} channel={} channel_number={}",
                                transaction.code, transaction.channel, transaction.channel_number
                            )
                        })?;
                }
            }
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
            if should_audit {
                self.log_audit_event(&target_code, event, "after");
            }

            self.record_snapshot(event.timestamp_ms(), &target_code);
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
    let audit_config = order_book_audit_config_from_env()?;
    let mut handler = OrderBookDebugHandler::new(
        DEBUG_TARGET_CODE.map(str::to_string),
        DEBUG_SNAPSHOT_DEPTH,
        env_flag("MIRRO_ORDER_BOOK_DIAGNOSTICS"),
        audit_config,
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

fn order_book_audit_config_from_env() -> Result<Option<OrderBookAuditConfig>> {
    if !env_flag("MIRRO_ORDER_BOOK_AUDIT") {
        return Ok(None);
    }

    let start_ms = env::var("MIRRO_ORDER_BOOK_AUDIT_START_MS")
        .ok()
        .map(|value| {
            value.parse::<i64>().with_context(|| {
                format!("failed to parse MIRRO_ORDER_BOOK_AUDIT_START_MS as i64: {value}")
            })
        })
        .transpose()?
        .unwrap_or(DEFAULT_AUDIT_START_MS);
    let end_ms = env::var("MIRRO_ORDER_BOOK_AUDIT_END_MS")
        .ok()
        .map(|value| {
            value.parse::<i64>().with_context(|| {
                format!("failed to parse MIRRO_ORDER_BOOK_AUDIT_END_MS as i64: {value}")
            })
        })
        .transpose()?
        .unwrap_or(DEFAULT_AUDIT_END_MS);

    Ok(Some(OrderBookAuditConfig { start_ms, end_ms }))
}
