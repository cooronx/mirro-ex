//!
//! replay总入口模块。
//! 1. 输入：
//!    - 数据库配置 `DbConfig`
//!    - 回放引擎配置 `ReplayConfig`
//!    - 单次回放任务配置 `ReplayTaskConfig`
//!    - 上层提供的 `ReplayHandler`
//!
//! 2. 输出：
//!    - 按模拟时间节奏切好的 `ReplayEvent` 批次会被持续交给 `ReplayHandler::on_events()`
//!    - 回放结束后返回一份 `ReplayRunReport`
//!
//! 3. 逻辑：
//!    - 根据日期和时间窗构造查询条件
//!    - 创建 `ReplayDbReader`、`SimClock` 和 `ReplayCoordinator`
//!    - 驱动主循环定时tick，不断从 coordinator 取出当前可安全发出的事件
//!    - 汇总整次回放的统计信息并形成最终报告
//!
use std::sync::Arc;
use std::time::Instant;

use anyhow::Error as AnyhowError;
use async_trait::async_trait;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::config::{DbConfig, ReplayConfig};
use crate::db::dbpool::{DbPool, DbPoolError};
use crate::db::queries::sh_order_query::SHOrderRangeQuery;
use crate::db::queries::sz_order_query::SZOrderRangeQuery;
use crate::db::queries::transaction_query::TransactionRangeQuery;
use crate::replay_manager::ReplayTaskConfig;
use crate::sim_clock::SimClock;
use crate::webdata::{AppEvent, EventBus};

use super::coordinator::{ReplayCoordinator, ReplayCoordinatorError};
use super::db_reader::{ReplayDbReader, ReplayDbReaderError};
use super::event::ReplayEvent;

pub type Result<T> = std::result::Result<T, ReplayControllerError>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplayCommand {
    Pause,
    Resume,
    Stop,
    SetSpeed(f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayRuntimeState {
    Idle,
    Running,
    Paused,
    Stopping,
    Finished,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReplayStatusSnapshot {
    pub state: ReplayRuntimeState,
    pub sim_now_ms: Option<u64>,
    pub progress: Option<f64>,
    pub replay_speed: Option<f64>,
    pub current_day: Option<String>,
    pub ticks: usize,
    pub total_events: usize,
    pub max_lag_ms: u64,
    pub final_lag_ms: Option<u64>,
    pub perf: ReplayPerfSnapshot,
    pub debug: ReplayDebugSnapshot,
    pub error_message: Option<String>,
    pub report: Option<ReplayRunReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ReplayPerfSnapshot {
    pub last_tick_events: usize,
    pub last_poll_elapsed_ms: u128,
    pub last_handler_elapsed_ms: u128,
    pub last_tick_elapsed_ms: u128,
    pub max_poll_elapsed_ms: u128,
    pub max_handler_elapsed_ms: u128,
    pub max_tick_elapsed_ms: u128,
    pub last_safe_emit_time_ms: Option<i64>,
    pub last_emitted_min_ts_ms: Option<i64>,
    pub last_emitted_max_ts_ms: Option<i64>,
    pub handler_detail: Option<ReplayHandlerPerfSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ReplayHandlerPerfSnapshot {
    pub worker_count: usize,
    pub active_workers: usize,
    pub worker_max_events: usize,
    pub worker_max_elapsed_ms: u128,
    pub worker_total_elapsed_ms: u128,
    pub apply_elapsed_ms: u128,
    pub snapshot_elapsed_ms: u128,
    pub record_snapshot_elapsed_ms: u128,
    pub market_queue_elapsed_ms: u128,
    pub trading_init_elapsed_ms: u128,
    pub trading_match_elapsed_ms: u128,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ReplayDebugSnapshot {
    pub unfinished_lanes: Vec<ReplayLaneDebugSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReplayLaneDebugSnapshot {
    pub market: String,
    pub channel: i64,
    pub ready_events: usize,
    pub watermark_ms: Option<i64>,
    pub warmed_up: bool,
    pub finished: bool,
}

impl Default for ReplayStatusSnapshot {
    fn default() -> Self {
        Self {
            state: ReplayRuntimeState::Idle,
            sim_now_ms: None,
            progress: None,
            replay_speed: None,
            current_day: None,
            ticks: 0,
            total_events: 0,
            max_lag_ms: 0,
            final_lag_ms: None,
            perf: ReplayPerfSnapshot::default(),
            debug: ReplayDebugSnapshot::default(),
            error_message: None,
            report: None,
        }
    }
}

#[derive(Clone)]
pub struct ReplayStatusReporter {
    status: Arc<RwLock<ReplayStatusSnapshot>>,
    event_bus: Option<EventBus>,
}

impl ReplayStatusReporter {
    pub fn new(status: Arc<RwLock<ReplayStatusSnapshot>>) -> Self {
        Self {
            status,
            event_bus: None,
        }
    }

    pub fn with_event_bus(status: Arc<RwLock<ReplayStatusSnapshot>>, event_bus: EventBus) -> Self {
        Self {
            status,
            event_bus: Some(event_bus),
        }
    }

    pub async fn snapshot(&self) -> ReplayStatusSnapshot {
        self.status.read().await.clone()
    }

    pub async fn set_status(&self, snapshot: ReplayStatusSnapshot) {
        *self.status.write().await = snapshot;
        self.publish_replay_changed();
    }

    pub async fn update_running(
        &self,
        current_day: String,
        sim_now_ms: u64,
        progress: f64,
        ticks: usize,
        total_events: usize,
        max_lag_ms: u64,
        final_lag_ms: u64,
        replay_speed: f64,
        perf: ReplayPerfSnapshot,
        debug: ReplayDebugSnapshot,
    ) {
        let mut guard = self.status.write().await;
        guard.state = ReplayRuntimeState::Running;
        guard.sim_now_ms = Some(sim_now_ms);
        guard.progress = Some(progress);
        guard.replay_speed = Some(replay_speed);
        guard.current_day = Some(current_day);
        guard.ticks = ticks;
        guard.total_events = total_events;
        guard.max_lag_ms = max_lag_ms;
        guard.final_lag_ms = Some(final_lag_ms);
        guard.perf = perf;
        guard.debug = debug;
        guard.error_message = None;
    }

    pub async fn mark_paused(
        &self,
        current_day: String,
        sim_now_ms: u64,
        progress: f64,
        ticks: usize,
        total_events: usize,
        max_lag_ms: u64,
        final_lag_ms: Option<u64>,
        replay_speed: f64,
    ) {
        let mut guard = self.status.write().await;
        guard.state = ReplayRuntimeState::Paused;
        guard.sim_now_ms = Some(sim_now_ms);
        guard.progress = Some(progress);
        guard.replay_speed = Some(replay_speed);
        guard.current_day = Some(current_day);
        guard.ticks = ticks;
        guard.total_events = total_events;
        guard.max_lag_ms = max_lag_ms;
        guard.final_lag_ms = final_lag_ms;
    }

    pub async fn mark_stopping(
        &self,
        current_day: String,
        sim_now_ms: u64,
        progress: f64,
        ticks: usize,
        total_events: usize,
        max_lag_ms: u64,
        final_lag_ms: Option<u64>,
        replay_speed: f64,
    ) {
        let mut guard = self.status.write().await;
        guard.state = ReplayRuntimeState::Stopping;
        guard.sim_now_ms = Some(sim_now_ms);
        guard.progress = Some(progress);
        guard.replay_speed = Some(replay_speed);
        guard.current_day = Some(current_day);
        guard.ticks = ticks;
        guard.total_events = total_events;
        guard.max_lag_ms = max_lag_ms;
        guard.final_lag_ms = final_lag_ms;
    }

    pub async fn mark_finished(&self, report: ReplayRunReport) {
        let mut guard = self.status.write().await;
        guard.state = ReplayRuntimeState::Finished;
        guard.sim_now_ms = None;
        guard.progress = Some(1.0);
        guard.current_day = report.daily_reports.last().map(|r| r.day.clone());
        guard.ticks = report.ticks;
        guard.total_events = report.total_events;
        guard.max_lag_ms = report.max_lag_ms;
        guard.final_lag_ms = Some(report.final_lag_ms);
        guard.error_message = None;
        guard.report = Some(report);
        drop(guard);
        self.publish_replay_changed();
    }

    pub async fn mark_failed(&self, error_message: String) {
        let mut guard = self.status.write().await;
        guard.state = ReplayRuntimeState::Failed;
        guard.error_message = Some(error_message);
        drop(guard);
        self.publish_replay_changed();
    }

    fn publish_replay_changed(&self) {
        if let Some(event_bus) = &self.event_bus {
            event_bus.publish(AppEvent::ReplayChanged);
        }
    }
}

pub struct ReplayControl {
    pub command_rx: mpsc::UnboundedReceiver<ReplayCommand>,
    pub status_reporter: ReplayStatusReporter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ReplayStopReason {
    Finished,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize)]
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
pub trait ReplayHandler: Send {
    async fn on_day_start(&mut self, _day: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_events(&mut self, events: Vec<ReplayEvent>) -> anyhow::Result<()>;

    fn last_perf_snapshot(&self) -> Option<ReplayHandlerPerfSnapshot> {
        None
    }

    async fn on_day_end(&mut self, _day: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct PrintReplayHandler;

#[async_trait]
impl ReplayHandler for PrintReplayHandler {
    async fn on_events(&mut self, _events: Vec<ReplayEvent>) -> anyhow::Result<()> {
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
    replay_engine_config: ReplayConfig,
    replay_task_config: ReplayTaskConfig,
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
        stopped: bool,
    },
}

impl ReplayController {
    pub fn new(
        db_config: DbConfig,
        replay_engine_config: ReplayConfig,
        replay_task_config: ReplayTaskConfig,
    ) -> Self {
        Self {
            db_config,
            replay_engine_config,
            replay_task_config,
        }
    }

    pub async fn replay<H>(&self, handler: &mut H) -> Result<ReplayRunReport>
    where
        H: ReplayHandler,
    {
        self.replay_with_control(handler, None).await
    }

    pub async fn replay_with_control<H>(
        &self,
        handler: &mut H,
        mut control: Option<ReplayControl>,
    ) -> Result<ReplayRunReport>
    where
        H: ReplayHandler,
    {
        let total_start = Instant::now();
        validate_replay_task_config(&self.replay_task_config)?;

        let db_pool = DbPool::new(&self.db_config)?;
        let daily_windows = split_request_into_daily_windows(&self.replay_task_config);

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
        let mut stop_reason = ReplayStopReason::Finished;

        for daily_window in daily_windows {
            match self
                .replay_single_day(
                    &db_pool,
                    &daily_window,
                    self.replay_task_config.replay_speed,
                    handler,
                    control.as_mut(),
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
                    stopped,
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

                    if stopped {
                        stop_reason = ReplayStopReason::Stopped;
                        break;
                    }
                }
            }
        }

        let report = ReplayRunReport {
            start_date: self
                .replay_task_config
                .replay_start_date
                .format("%Y-%m-%d")
                .to_string(),
            end_date: self
                .replay_task_config
                .replay_end_date
                .format("%Y-%m-%d")
                .to_string(),
            start_time: self
                .replay_task_config
                .replay_start_time
                .format("%H:%M:%S%.3f")
                .to_string(),
            end_time: self
                .replay_task_config
                .replay_end_time
                .format("%H:%M:%S%.3f")
                .to_string(),
            replay_speed: self.replay_task_config.replay_speed,
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
            stop_reason,
        };

        if let Some(control) = control.as_ref() {
            control.status_reporter.mark_finished(report.clone()).await;
        }

        Ok(report)
    }

    async fn replay_single_day<H>(
        &self,
        db_pool: &DbPool,
        daily_window: &DailyReplayWindow,
        replay_speed: f64,
        handler: &mut H,
        mut control: Option<&mut ReplayControl>,
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
        .with_codes(self.replay_task_config.replay_codes.clone());
        let sz_query = SZOrderRangeQuery::new(
            &daily_window.day,
            daily_window.start_time_ms,
            daily_window.end_time_ms,
            &self.db_config.tables.sz_order,
        )
        .with_codes(self.replay_task_config.replay_codes.clone());
        let transaction_query = TransactionRangeQuery::new(
            &daily_window.day,
            daily_window.start_time_ms,
            daily_window.end_time_ms,
            &self.db_config.tables.transaction,
        )
        .with_codes(self.replay_task_config.replay_codes.clone());

        let reader_build_start = Instant::now();
        let reader = ReplayDbReader::from_range_queries(
            db_pool.clone(),
            self.replay_engine_config.batch_size,
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

        handler
            .on_day_start(&daily_window.day)
            .await
            .map_err(ReplayControllerError::Handler)?;

        let clock = SimClock::new(
            daily_window.start_time_ms as u64,
            daily_window.end_time_ms as u64,
            replay_speed,
            self.replay_task_config.skip_intraday_breaks,
        )?;
        let mut coordinator = ReplayCoordinator::from_reader(
            reader,
            clock,
            self.replay_engine_config.tick_interval_ms,
            self.replay_engine_config.lane_queue_capacity,
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
        let mut max_poll_elapsed_ms = 0u128;
        let mut max_handler_elapsed_ms = 0u128;
        let mut max_tick_elapsed_ms = 0u128;
        let mut stopped = false;
        let mut replay_speed = self.replay_task_config.replay_speed;
        loop {
            if let Some(control) = control.as_mut() {
                if self
                    .handle_control_before_tick(
                        &mut coordinator,
                        &daily_window.day,
                        control,
                        tick_count,
                        total_events,
                        max_lag_ms,
                        final_lag_ms,
                        &mut replay_speed,
                    )
                    .await?
                {
                    stopped = true;
                    break;
                }
            }

            tick.tick().await;
            let tick_process_start = Instant::now();
            let poll_start = Instant::now();
            let result = coordinator.poll_ready_events().await?;
            let poll_elapsed_ms = poll_start.elapsed().as_millis();
            tick_count += 1;
            max_lag_ms = max_lag_ms.max(result.lag_ms);
            total_lag_ms += u128::from(result.lag_ms);
            final_lag_ms = Some(result.lag_ms);
            max_poll_elapsed_ms = max_poll_elapsed_ms.max(poll_elapsed_ms);

            let tick_events = result.events.len();
            let emitted_min_ts_ms = result.events.iter().map(ReplayEvent::timestamp_ms).min();
            let emitted_max_ts_ms = result.events.iter().map(ReplayEvent::timestamp_ms).max();

            let progress = if control.is_some() {
                Some(coordinator.progress()?)
            } else {
                None
            };
            let mut handler_elapsed_ms = 0;
            let (order_rows, transaction_rows) = count_event_rows(&result.events);
            total_events += result.events.len();
            total_order_rows += order_rows;
            total_transaction_rows += transaction_rows;

            if !result.events.is_empty() {
                let handler_start = Instant::now();
                handler
                    .on_events(result.events)
                    .await
                    .map_err(ReplayControllerError::Handler)?;
                handler_elapsed_ms = handler_start.elapsed().as_millis();
            }
            let handler_detail = handler.last_perf_snapshot();
            max_handler_elapsed_ms = max_handler_elapsed_ms.max(handler_elapsed_ms);
            let tick_elapsed_ms = tick_process_start.elapsed().as_millis();
            max_tick_elapsed_ms = max_tick_elapsed_ms.max(tick_elapsed_ms);

            if let (Some(control), Some(progress)) = (control.as_mut(), progress) {
                control
                    .status_reporter
                    .update_running(
                        daily_window.day.clone(),
                        result.sim_now_ms,
                        progress,
                        tick_count,
                        total_events,
                        max_lag_ms,
                        result.lag_ms,
                        replay_speed,
                        ReplayPerfSnapshot {
                            last_tick_events: tick_events,
                            last_poll_elapsed_ms: poll_elapsed_ms,
                            last_handler_elapsed_ms: handler_elapsed_ms,
                            last_tick_elapsed_ms: tick_elapsed_ms,
                            max_poll_elapsed_ms,
                            max_handler_elapsed_ms,
                            max_tick_elapsed_ms,
                            last_safe_emit_time_ms: result.safe_emit_time_ms,
                            last_emitted_min_ts_ms: emitted_min_ts_ms,
                            last_emitted_max_ts_ms: emitted_max_ts_ms,
                            handler_detail,
                        },
                        coordinator.debug_snapshot(),
                    )
                    .await;
            }

            if result.finished {
                break;
            }
        }

        handler
            .on_day_end(&daily_window.day)
            .await
            .map_err(ReplayControllerError::Handler)?;

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
                stop_reason: if stopped {
                    ReplayStopReason::Stopped
                } else {
                    ReplayStopReason::Finished
                },
            },
            stopped,
        })
    }

    async fn handle_control_before_tick(
        &self,
        coordinator: &mut ReplayCoordinator,
        current_day: &str,
        control: &mut ReplayControl,
        tick_count: usize,
        total_events: usize,
        max_lag_ms: u64,
        final_lag_ms: Option<u64>,
        replay_speed: &mut f64,
    ) -> Result<bool> {
        while let Ok(command) = control.command_rx.try_recv() {
            match command {
                ReplayCommand::Pause => {
                    coordinator.pause_clock()?;
                    let sim_now_ms = coordinator.current_sim_now()?;
                    let progress = coordinator.progress()?;
                    control
                        .status_reporter
                        .mark_paused(
                            current_day.to_string(),
                            sim_now_ms,
                            progress,
                            tick_count,
                            total_events,
                            max_lag_ms,
                            final_lag_ms,
                            *replay_speed,
                        )
                        .await;

                    loop {
                        match control.command_rx.recv().await {
                            Some(ReplayCommand::Resume) => {
                                coordinator.resume_clock()?;
                                let sim_now_ms = coordinator.current_sim_now()?;
                                let progress = coordinator.progress()?;
                                control
                                    .status_reporter
                                    .update_running(
                                        current_day.to_string(),
                                        sim_now_ms,
                                        progress,
                                        tick_count,
                                        total_events,
                                        max_lag_ms,
                                        final_lag_ms.unwrap_or(0),
                                        *replay_speed,
                                        ReplayPerfSnapshot::default(),
                                        ReplayDebugSnapshot::default(),
                                    )
                                    .await;
                                break;
                            }
                            Some(ReplayCommand::SetSpeed(speed)) => {
                                coordinator.set_clock_speed(speed)?;
                                *replay_speed = speed;
                                let sim_now_ms = coordinator.current_sim_now()?;
                                let progress = coordinator.progress()?;
                                control
                                    .status_reporter
                                    .mark_paused(
                                        current_day.to_string(),
                                        sim_now_ms,
                                        progress,
                                        tick_count,
                                        total_events,
                                        max_lag_ms,
                                        final_lag_ms,
                                        *replay_speed,
                                    )
                                    .await;
                            }
                            Some(ReplayCommand::Stop) | None => {
                                let sim_now_ms = coordinator.current_sim_now()?;
                                let progress = coordinator.progress()?;
                                control
                                    .status_reporter
                                    .mark_stopping(
                                        current_day.to_string(),
                                        sim_now_ms,
                                        progress,
                                        tick_count,
                                        total_events,
                                        max_lag_ms,
                                        final_lag_ms,
                                        *replay_speed,
                                    )
                                    .await;
                                return Ok(true);
                            }
                            Some(ReplayCommand::Pause) => {}
                        }
                    }
                }
                ReplayCommand::Resume => {}
                ReplayCommand::SetSpeed(speed) => {
                    coordinator.set_clock_speed(speed)?;
                    *replay_speed = speed;
                    let sim_now_ms = coordinator.current_sim_now()?;
                    let progress = coordinator.progress()?;
                    control
                        .status_reporter
                        .update_running(
                            current_day.to_string(),
                            sim_now_ms,
                            progress,
                            tick_count,
                            total_events,
                            max_lag_ms,
                            final_lag_ms.unwrap_or(0),
                            *replay_speed,
                            ReplayPerfSnapshot::default(),
                            ReplayDebugSnapshot::default(),
                        )
                        .await;
                }
                ReplayCommand::Stop => {
                    let sim_now_ms = coordinator.current_sim_now()?;
                    let progress = coordinator.progress()?;
                    control
                        .status_reporter
                        .mark_stopping(
                            current_day.to_string(),
                            sim_now_ms,
                            progress,
                            tick_count,
                            total_events,
                            max_lag_ms,
                            final_lag_ms,
                            *replay_speed,
                        )
                        .await;
                    return Ok(true);
                }
            }
        }

        Ok(false)
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

fn validate_replay_task_config(config: &ReplayTaskConfig) -> Result<()> {
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

fn split_request_into_daily_windows(request: &ReplayTaskConfig) -> Vec<DailyReplayWindow> {
    let mut day = request.replay_start_date;
    let mut windows = Vec::new();

    loop {
        windows.push(DailyReplayWindow {
            day: day.format("%Y-%m-%d").to_string(),
            start_time_ms: timestamp_ms_for_local_datetime(day, request.replay_start_time),
            end_time_ms: exclusive_end_time_ms(day, request.replay_end_time),
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

fn exclusive_end_time_ms(day: NaiveDate, time: NaiveTime) -> i64 {
    // 这里多加1毫秒，这样的话用户传入的区间就可以被包含完全了
    timestamp_ms_for_local_datetime(day, time)
        .checked_add(1)
        .expect("exclusive replay end timestamp should not overflow")
}

#[cfg(test)]
mod tests {
    use super::{
        DailyReplayWindow, split_request_into_daily_windows, timestamp_ms_for_local_datetime,
    };
    use crate::replay_manager::ReplayTaskConfig;
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
        let request = ReplayTaskConfig {
            replay_start_date: NaiveDate::from_ymd_opt(2026, 5, 12).unwrap(),
            replay_end_date: NaiveDate::from_ymd_opt(2026, 5, 13).unwrap(),
            replay_start_time: NaiveTime::from_hms_milli_opt(9, 30, 0, 0).unwrap(),
            replay_end_time: NaiveTime::from_hms_milli_opt(15, 0, 0, 0).unwrap(),
            replay_codes: Vec::new(),
            skip_intraday_breaks: false,
            replay_speed: 1.0,
        };

        assert_eq!(
            split_request_into_daily_windows(&request),
            vec![
                DailyReplayWindow {
                    day: "2026-05-12".to_string(),
                    start_time_ms: 1_778_549_400_000,
                    end_time_ms: 1_778_569_200_001,
                },
                DailyReplayWindow {
                    day: "2026-05-13".to_string(),
                    start_time_ms: 1_778_635_800_000,
                    end_time_ms: 1_778_655_600_001,
                },
            ]
        );
    }
}
