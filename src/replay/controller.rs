use std::time::Instant;

use anyhow::Error as AnyhowError;
use async_trait::async_trait;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
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

pub type Result<T> = std::result::Result<T, ReplayControllerError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayStopReason {
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    pub day: String,
    pub ticks: usize,
    pub total_events: usize,
    pub total_order_rows: usize,
    pub total_transaction_rows: usize,
    pub max_lag_ms: u64,
    pub avg_lag_ms: u64,
    pub final_lag_ms: u64,
    pub reader_build_elapsed_ms: u128,
    pub bootstrap_elapsed_ms: u128,
    pub wall_elapsed_ms: u128,
    pub total_elapsed_ms: u128,
    pub stop_reason: ReplayStopReason,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplayRunReport {
    pub start_date: String,
    pub end_date: String,
    pub start_time: String,
    pub end_time: String,
    pub replay_speed: f64,
    pub daily_reports: Vec<ReplayReport>,
    pub skipped_days: Vec<String>,
    pub ticks: usize,
    pub total_events: usize,
    pub total_order_rows: usize,
    pub total_transaction_rows: usize,
    pub max_lag_ms: u64,
    pub avg_lag_ms: u64,
    pub final_lag_ms: u64,
    pub total_reader_build_elapsed_ms: u128,
    pub total_bootstrap_elapsed_ms: u128,
    pub total_wall_elapsed_ms: u128,
    pub total_elapsed_ms: u128,
    pub stop_reason: ReplayStopReason,
}

#[async_trait]
pub trait ReplayHandler {
    async fn on_events(&mut self, events: &[ReplayEvent]) -> anyhow::Result<()>;
}

#[derive(Debug, Default)]
pub struct PrintReplayHandler;

#[async_trait]
impl ReplayHandler for PrintReplayHandler {
    async fn on_events(&mut self, _events: &[ReplayEvent]) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ReplayControllerError {
    #[error("invalid replay date range: start_date={start_date}, end_date={end_date}")]
    InvalidDateRange {
        start_date: String,
        end_date: String,
    },
    #[error("invalid replay time range: start_time={start_time}, end_time={end_time}")]
    InvalidTimeRange {
        start_time: String,
        end_time: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DailyReplayWindow {
    day: String,
    start_time_ms: i64,
    end_time_ms: i64,
}

enum SingleDayReplayOutcome {
    Skipped {
        day: String,
        reader_build_elapsed_ms: u128,
    },
    Report {
        report: ReplayReport,
        total_lag_ms: u128,
    },
}

impl ReplayController {
    pub fn new(db_config: DbConfig, replay_config: ReplayConfig) -> Self {
        Self {
            db_config,
            replay_config,
        }
    }

    pub async fn replay<H>(&self, handler: &mut H) -> Result<ReplayRunReport>
    where
        H: ReplayHandler,
    {
        let total_start = Instant::now();
        validate_replay_config(&self.replay_config)?;

        let db_pool = DbPool::new(&self.db_config)?;
        let daily_windows = split_request_into_daily_windows(&self.replay_config);

        let mut daily_reports = Vec::new();
        let mut skipped_days = Vec::new();
        let mut total_ticks = 0usize;
        let mut total_events = 0usize;
        let mut total_order_rows = 0usize;
        let mut total_transaction_rows = 0usize;
        let mut max_lag_ms = 0u64;
        let mut total_lag_ms = 0u128;
        let mut final_lag_ms = None;
        let mut total_reader_build_elapsed_ms = 0u128;
        let mut total_bootstrap_elapsed_ms = 0u128;
        let mut total_wall_elapsed_ms = 0u128;

        for daily_window in daily_windows {
            match self
                .replay_single_day(
                    &db_pool,
                    &daily_window,
                    self.replay_config.replay_speed,
                    handler,
                )
                .await?
            {
                SingleDayReplayOutcome::Skipped {
                    day,
                    reader_build_elapsed_ms,
                } => {
                    skipped_days.push(day);
                    total_reader_build_elapsed_ms += reader_build_elapsed_ms;
                }
                SingleDayReplayOutcome::Report {
                    report,
                    total_lag_ms: day_total_lag_ms,
                } => {
                    total_ticks += report.ticks;
                    total_events += report.total_events;
                    total_order_rows += report.total_order_rows;
                    total_transaction_rows += report.total_transaction_rows;
                    max_lag_ms = max_lag_ms.max(report.max_lag_ms);
                    total_lag_ms += day_total_lag_ms;
                    final_lag_ms = Some(report.final_lag_ms);
                    total_reader_build_elapsed_ms += report.reader_build_elapsed_ms;
                    total_bootstrap_elapsed_ms += report.bootstrap_elapsed_ms;
                    total_wall_elapsed_ms += report.wall_elapsed_ms;

                    daily_reports.push(report);
                }
            }
        }

        Ok(ReplayRunReport {
            start_date: self
                .replay_config
                .replay_start_date
                .format("%Y-%m-%d")
                .to_string(),
            end_date: self
                .replay_config
                .replay_end_date
                .format("%Y-%m-%d")
                .to_string(),
            start_time: self
                .replay_config
                .replay_start_time
                .format("%H:%M:%S%.3f")
                .to_string(),
            end_time: self
                .replay_config
                .replay_end_time
                .format("%H:%M:%S%.3f")
                .to_string(),
            replay_speed: self.replay_config.replay_speed,
            daily_reports,
            skipped_days,
            ticks: total_ticks,
            total_events,
            total_order_rows,
            total_transaction_rows,
            max_lag_ms,
            avg_lag_ms: average_lag_ms(total_lag_ms, total_ticks),
            final_lag_ms: final_lag_ms.unwrap_or(0),
            total_reader_build_elapsed_ms,
            total_bootstrap_elapsed_ms,
            total_wall_elapsed_ms,
            total_elapsed_ms: total_start.elapsed().as_millis(),
            stop_reason: ReplayStopReason::Finished,
        })
    }

    async fn replay_single_day<H>(
        &self,
        db_pool: &DbPool,
        daily_window: &DailyReplayWindow,
        replay_speed: f64,
        handler: &mut H,
    ) -> Result<SingleDayReplayOutcome>
    where
        H: ReplayHandler,
    {
        let total_start = Instant::now();
        let sh_query = SHOrderRangeQuery::new(
            &daily_window.day,
            daily_window.start_time_ms,
            daily_window.end_time_ms,
            &self.db_config.tables.sh_order,
        )
        .with_codes(self.replay_config.replay_codes.clone().unwrap_or_default());
        let sz_query = SZOrderRangeQuery::new(
            &daily_window.day,
            daily_window.start_time_ms,
            daily_window.end_time_ms,
            &self.db_config.tables.sz_order,
        )
        .with_codes(self.replay_config.replay_codes.clone().unwrap_or_default());
        let transaction_query = TransactionRangeQuery::new(
            &daily_window.day,
            daily_window.start_time_ms,
            daily_window.end_time_ms,
            &self.db_config.tables.transaction,
        )
        .with_codes(self.replay_config.replay_codes.clone().unwrap_or_default());

        let reader_build_start = Instant::now();
        let reader = ReplayDbReader::from_range_queries(
            db_pool.clone(),
            self.replay_config.batch_size,
            Some(&sh_query),
            Some(&sz_query),
            Some(&transaction_query),
        )
        .await?;
        let reader_build_elapsed = reader_build_start.elapsed();

        if reader.cursors().is_empty() {
            return Ok(SingleDayReplayOutcome::Skipped {
                day: daily_window.day.clone(),
                reader_build_elapsed_ms: reader_build_elapsed.as_millis(),
            });
        }

        let clock = SimClock::new(
            daily_window.start_time_ms as u64,
            daily_window.end_time_ms as u64,
            replay_speed,
            self.replay_config.skip_intraday_breaks,
        )?;
        let mut coordinator = ReplayCoordinator::from_reader(
            reader,
            clock,
            self.replay_config.tick_interval_ms,
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
        let mut max_lag_ms = 0u64;
        let mut total_lag_ms = 0u128;
        let mut final_lag_ms = None;
        loop {
            tick.tick().await;
            let result = coordinator.poll_ready_events().await?;
            tick_count += 1;
            max_lag_ms = max_lag_ms.max(result.lag_ms);
            total_lag_ms += u128::from(result.lag_ms);
            final_lag_ms = Some(result.lag_ms);

            let (order_rows, transaction_rows) = count_event_rows(&result.events);
            total_events += result.events.len();
            total_order_rows += order_rows;
            total_transaction_rows += transaction_rows;

            if !result.events.is_empty() {
                handler
                    .on_events(&result.events)
                    .await
                    .map_err(ReplayControllerError::Handler)?;
            }

            if result.finished {
                break;
            }
        }

        Ok(SingleDayReplayOutcome::Report {
            total_lag_ms,
            report: ReplayReport {
                day: daily_window.day.clone(),
                ticks: tick_count,
                total_events,
                total_order_rows,
                total_transaction_rows,
                max_lag_ms,
                avg_lag_ms: average_lag_ms(total_lag_ms, tick_count),
                final_lag_ms: final_lag_ms.unwrap_or(0),
                reader_build_elapsed_ms: reader_build_elapsed.as_millis(),
                bootstrap_elapsed_ms: bootstrap_elapsed.as_millis(),
                wall_elapsed_ms: run_start.elapsed().as_millis(),
                total_elapsed_ms: total_start.elapsed().as_millis(),
                stop_reason: ReplayStopReason::Finished,
            },
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

fn average_lag_ms(total_lag_ms: u128, tick_count: usize) -> u64 {
    if tick_count == 0 {
        return 0;
    }

    (total_lag_ms / tick_count as u128) as u64
}

fn asia_shanghai_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("Asia/Shanghai offset should be valid")
}

fn validate_replay_config(config: &ReplayConfig) -> Result<()> {
    if config.replay_start_date > config.replay_end_date {
        return Err(ReplayControllerError::InvalidDateRange {
            start_date: config.replay_start_date.format("%Y-%m-%d").to_string(),
            end_date: config.replay_end_date.format("%Y-%m-%d").to_string(),
        });
    }

    if config.replay_start_time >= config.replay_end_time {
        return Err(ReplayControllerError::InvalidTimeRange {
            start_time: config.replay_start_time.format("%H:%M:%S%.3f").to_string(),
            end_time: config.replay_end_time.format("%H:%M:%S%.3f").to_string(),
        });
    }

    Ok(())
}

fn split_request_into_daily_windows(request: &ReplayConfig) -> Vec<DailyReplayWindow> {
    let mut day = request.replay_start_date;
    let mut windows = Vec::new();

    loop {
        windows.push(DailyReplayWindow {
            day: day.format("%Y-%m-%d").to_string(),
            start_time_ms: timestamp_ms_for_local_datetime(day, request.replay_start_time),
            end_time_ms: timestamp_ms_for_local_datetime(day, request.replay_end_time),
        });

        if day == request.replay_end_date {
            break;
        }

        day = day
            .succ_opt()
            .expect("replay day should be able to advance to next day");
    }

    windows
}

fn timestamp_ms_for_local_datetime(day: NaiveDate, time: NaiveTime) -> i64 {
    asia_shanghai_offset()
        .from_local_datetime(&NaiveDateTime::new(day, time))
        .single()
        .expect("local replay datetime in Asia/Shanghai should be unambiguous")
        .timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::{
        DailyReplayWindow, split_request_into_daily_windows, timestamp_ms_for_local_datetime,
    };
    use crate::config::ReplayConfig;
    use chrono::{NaiveDate, NaiveTime};

    #[test]
    fn resolves_east8_local_datetime_to_unix_ms() {
        assert_eq!(
            timestamp_ms_for_local_datetime(
                NaiveDate::from_ymd_opt(2026, 5, 12).unwrap(),
                NaiveTime::from_hms_milli_opt(9, 30, 0, 0).unwrap()
            ),
            1_778_549_400_000
        );
    }

    #[test]
    fn splits_cross_day_request_into_daily_windows() {
        let request = ReplayConfig {
            lane_queue_capacity: 1,
            tick_interval_ms: 5,
            batch_size: 100_000,
            snapshot_depth: 5,
            write_snapshot_csv: true,
            snapshot_csv_path: "data/order_book_snapshot.csv".to_string(),
            replay_start_date: NaiveDate::from_ymd_opt(2026, 5, 12).unwrap(),
            replay_end_date: NaiveDate::from_ymd_opt(2026, 5, 13).unwrap(),
            replay_start_time: NaiveTime::from_hms_milli_opt(9, 30, 0, 0).unwrap(),
            replay_end_time: NaiveTime::from_hms_milli_opt(15, 0, 0, 0).unwrap(),
            replay_codes: None,
            skip_intraday_breaks: false,
            replay_speed: 1.0,
        };

        assert_eq!(
            split_request_into_daily_windows(&request),
            vec![
                DailyReplayWindow {
                    day: "2026-05-12".to_string(),
                    start_time_ms: 1_778_549_400_000,
                    end_time_ms: 1_778_569_200_000,
                },
                DailyReplayWindow {
                    day: "2026-05-13".to_string(),
                    start_time_ms: 1_778_635_800_000,
                    end_time_ms: 1_778_655_600_000,
                },
            ]
        );
    }
}
