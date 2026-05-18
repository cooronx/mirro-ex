use std::time::Instant;

use anyhow::Error as AnyhowError;
use thiserror::Error;
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::config::{DbConfig, ReplayConfig};
use crate::db::dbpool::{DbPool, DbPoolError};
use crate::db::queries::sh_order_query::SHOrderRangeQuery;
use crate::db::queries::sz_order_query::SZOrderRangeQuery;
use crate::db::queries::transaction_query::TransactionRangeQuery;
use crate::sim_clock::SimClock;

use super::coordinator::{ReplayCoordinator, ReplayCoordinatorError};
use super::db_reader::{ReplayDbReader, ReplayDbReaderError};
use super::event::ReplayEvent;

const DEFAULT_BATCH_SIZE: i64 = 1_000_000;
const DEFAULT_TICK_INTERVAL_MS: u64 = 100;
const DEFAULT_STALL_TICK_LIMIT: usize = 20;
const ASIA_SHANGHAI_UTC_OFFSET_SECONDS: i64 = 8 * 60 * 60;

pub type Result<T> = std::result::Result<T, ReplayControllerError>;

#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRequest {
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub replay_speed: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayStopReason {
    Finished,
    Stalled { frontier_lanes: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    pub day: String,
    pub ticks: usize,
    pub total_events: usize,
    pub total_order_rows: usize,
    pub total_transaction_rows: usize,
    pub reader_build_elapsed_ms: u128,
    pub bootstrap_elapsed_ms: u128,
    pub wall_elapsed_ms: u128,
    pub total_elapsed_ms: u128,
    pub stop_reason: ReplayStopReason,
}

pub trait ReplayHandler {
    fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()>;
}

#[derive(Debug, Default)]
pub struct PrintReplayHandler;

impl ReplayHandler for PrintReplayHandler {
    fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()> {
        for event in events {
            match event {
                ReplayEvent::Order(order) => println!("{order:?}"),
                ReplayEvent::Transaction(transaction) => println!("{transaction:?}"),
            }
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ReplayControllerError {
    #[error("invalid replay time range: start_time_ms={start_time_ms}, end_time_ms={end_time_ms}")]
    InvalidTimeRange {
        start_time_ms: i64,
        end_time_ms: i64,
    },
    #[error("cross-day replay is not supported yet: start_day={start_day}, end_day={end_day}")]
    CrossDayRequest {
        start_day: String,
        end_day: String,
    },
    #[error("failed to build db pool")]
    DbPool(#[from] DbPoolError),
    #[error("failed to build replay db reader")]
    Reader(#[from] ReplayDbReaderError),
    #[error("failed to drive replay coordinator")]
    Coordinator(#[from] ReplayCoordinatorError),
    #[error("failed to initialize sim clock")]
    Clock(#[from] crate::sim_clock::SimClockError),
    #[error("replay handler failed")]
    Handler(#[source] AnyhowError),
}

pub struct ReplayController {
    db_config: DbConfig,
    replay_config: ReplayConfig,
}

impl ReplayController {
    pub fn new(db_config: DbConfig, replay_config: ReplayConfig) -> Self {
        Self {
            db_config,
            replay_config,
        }
    }

    pub async fn replay<H>(&self, request: ReplayRequest, handler: &mut H) -> Result<ReplayReport>
    where
        H: ReplayHandler,
    {
        let total_start = Instant::now();
        let day = validate_request_and_resolve_day(&request)?;

        let db_pool = DbPool::new(&self.db_config)?;
        let sh_query = SHOrderRangeQuery::new(
            &day,
            request.start_time_ms,
            request.end_time_ms,
            &self.db_config.tables.sh_order,
        );
        let sz_query = SZOrderRangeQuery::new(
            &day,
            request.start_time_ms,
            request.end_time_ms,
            &self.db_config.tables.sz_order,
        );
        let transaction_query = TransactionRangeQuery::new(
            &day,
            request.start_time_ms,
            request.end_time_ms,
            &self.db_config.tables.transaction,
        );

        let reader_build_start = Instant::now();
        let reader = ReplayDbReader::from_range_queries(
            db_pool,
            DEFAULT_BATCH_SIZE,
            Some(&sh_query),
            Some(&sz_query),
            Some(&transaction_query),
        )
        .await?;
        let reader_build_elapsed = reader_build_start.elapsed();

        let clock = SimClock::new(
            request.start_time_ms as u64,
            request.end_time_ms as u64,
            request.replay_speed,
        )?;
        let mut coordinator = ReplayCoordinator::from_reader(
            reader,
            clock,
            DEFAULT_TICK_INTERVAL_MS,
            self.replay_config.lane_queue_capacity,
        )
        .await?;

        let bootstrap_start = Instant::now();
        coordinator.bootstrap().await?;
        let bootstrap_elapsed = bootstrap_start.elapsed();

        let run_start = Instant::now();
        let mut tick = interval(Duration::from_millis(coordinator.tick_interval_ms()));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut tick_count = 0usize;
        let mut total_events = 0usize;
        let mut total_order_rows = 0usize;
        let mut total_transaction_rows = 0usize;
        let mut consecutive_empty_ticks = 0usize;
        let stop_reason = loop {
            tick.tick().await;
            let result = coordinator.poll_ready_events().await?;
            tick_count += 1;

            let (order_rows, transaction_rows) = count_event_rows(&result.events);
            total_events += result.events.len();
            total_order_rows += order_rows;
            total_transaction_rows += transaction_rows;

            if result.events.is_empty() {
                consecutive_empty_ticks += 1;
            } else {
                consecutive_empty_ticks = 0;
                handler
                    .on_events(&result.events)
                    .map_err(ReplayControllerError::Handler)?;
            }

            if result.finished {
                break ReplayStopReason::Finished;
            }

            if consecutive_empty_ticks >= DEFAULT_STALL_TICK_LIMIT {
                break ReplayStopReason::Stalled {
                    frontier_lanes: coordinator.debug_frontier_lanes(8),
                };
            }
        };

        Ok(ReplayReport {
            day,
            ticks: tick_count,
            total_events,
            total_order_rows,
            total_transaction_rows,
            reader_build_elapsed_ms: reader_build_elapsed.as_millis(),
            bootstrap_elapsed_ms: bootstrap_elapsed.as_millis(),
            wall_elapsed_ms: run_start.elapsed().as_millis(),
            total_elapsed_ms: total_start.elapsed().as_millis(),
            stop_reason,
        })
    }
}

fn count_event_rows(events: &[ReplayEvent]) -> (usize, usize) {
    let mut order_rows = 0usize;
    let mut transaction_rows = 0usize;

    for event in events {
        match event {
            ReplayEvent::Order(_) => order_rows += 1,
            ReplayEvent::Transaction(_) => transaction_rows += 1,
        }
    }

    (order_rows, transaction_rows)
}

fn validate_request_and_resolve_day(request: &ReplayRequest) -> Result<String> {
    if request.start_time_ms >= request.end_time_ms {
        return Err(ReplayControllerError::InvalidTimeRange {
            start_time_ms: request.start_time_ms,
            end_time_ms: request.end_time_ms,
        });
    }

    let start_day = trading_day_for_timestamp_ms(request.start_time_ms);
    let end_day = trading_day_for_timestamp_ms(request.end_time_ms - 1);
    if start_day != end_day {
        return Err(ReplayControllerError::CrossDayRequest { start_day, end_day });
    }

    Ok(start_day)
}

fn trading_day_for_timestamp_ms(timestamp_ms: i64) -> String {
    let local_seconds = timestamp_ms.div_euclid(1_000) + ASIA_SHANGHAI_UTC_OFFSET_SECONDS;
    let local_days = local_seconds.div_euclid(86_400);
    let (year, month, day) = civil_from_days(local_days);

    format!("{year:04}-{month:02}-{day:02}")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }

    (year as i32, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::{
        PrintReplayHandler, ReplayControllerError, ReplayHandler, ReplayRequest,
        trading_day_for_timestamp_ms, validate_request_and_resolve_day,
    };
    use crate::common::{L2Order, Market, OrderDirection, OrderType};
    use crate::replay::ReplayEvent;

    #[test]
    fn resolves_trading_day_from_unix_ms() {
        assert_eq!(
            trading_day_for_timestamp_ms(1_778_549_400_000),
            "2026-05-12"
        );
    }

    #[test]
    fn validates_same_day_request() {
        let request = ReplayRequest {
            start_time_ms: 1_778_549_400_000,
            end_time_ms: 1_778_549_460_000,
            replay_speed: 1.0,
        };

        assert_eq!(
            validate_request_and_resolve_day(&request).unwrap(),
            "2026-05-12"
        );
    }

    #[test]
    fn rejects_cross_day_request() {
        let request = ReplayRequest {
            start_time_ms: 1_778_687_999_000,
            end_time_ms: 1_778_688_001_000,
            replay_speed: 1.0,
        };

        let err = validate_request_and_resolve_day(&request).unwrap_err();
        assert!(matches!(err, ReplayControllerError::CrossDayRequest { .. }));
    }

    #[test]
    fn print_handler_accepts_event_batch() {
        let mut handler = PrintReplayHandler;
        let events = vec![ReplayEvent::Order(L2Order {
            market: Market::XSHG,
            channel: 1,
            channel_number: 1,
            code: "SH600000".to_string(),
            price: 100,
            volume: 10,
            direction: OrderDirection::Buy,
            order_type: OrderType::Limit,
            timestamp_ms: 1_000,
            extra_message_number: 0,
        })];

        handler.on_events(&events).unwrap();
    }
}
