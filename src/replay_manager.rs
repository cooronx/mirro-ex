use std::sync::Arc;

use anyhow::Error as AnyhowError;
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::app;
use crate::config::{AppConfig, DEFAULT_CONFIG_PATH, ReplayConfig};
use crate::replay::{
    ReplayCommand, ReplayRuntimeState, ReplayStatusReporter, ReplayStatusSnapshot,
};

const REPLAY_DATE_FORMAT: &str = "%Y-%m-%d";
const REPLAY_TIME_FORMAT_WITH_MILLISECONDS: &str = "%H:%M:%S%.3f";
const REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS: &str = "%H:%M:%S";

#[derive(Debug, Default, Deserialize, Clone)]
pub struct ReplayStartRequest {
    pub replay_start_date: Option<String>,
    pub replay_end_date: Option<String>,
    pub replay_start_time: Option<String>,
    pub replay_end_time: Option<String>,
    pub replay_speed: Option<f64>,
    pub skip_intraday_breaks: Option<bool>,
}

#[derive(Debug, Error)]
pub enum ReplayManagerError {
    #[error("a replay task is already active")]
    ActiveReplayExists,
    #[error("cannot pause replay while state is {0:?}")]
    InvalidPauseState(ReplayRuntimeState),
    #[error("cannot resume replay while state is {0:?}")]
    InvalidResumeState(ReplayRuntimeState),
    #[error("cannot stop replay while state is {0:?}")]
    InvalidStopState(ReplayRuntimeState),
    #[error("missing replay command channel")]
    MissingCommandChannel,
    #[error("failed to send replay command")]
    SendCommand,
    #[error("invalid replay start date format: {0}, expected YYYY-MM-DD")]
    InvalidReplayStartDate(String),
    #[error("invalid replay end date format: {0}, expected YYYY-MM-DD")]
    InvalidReplayEndDate(String),
    #[error("invalid replay start time format: {0}, expected HH:MM:SS or HH:MM:SS.sss")]
    InvalidReplayStartTime(String),
    #[error("invalid replay end time format: {0}, expected HH:MM:SS or HH:MM:SS.sss")]
    InvalidReplayEndTime(String),
}

pub type Result<T> = std::result::Result<T, ReplayManagerError>;

#[derive(Debug, Clone, Serialize)]
pub struct ReplayConfigView {
    pub lane_queue_capacity: usize,
    pub tick_interval_ms: u64,
    pub batch_size: i64,
    pub snapshot_depth: usize,
    pub write_snapshot_csv: bool,
    pub snapshot_csv_path: String,
    pub replay_start_date: String,
    pub replay_end_date: String,
    pub replay_start_time: String,
    pub replay_end_time: String,
    pub replay_codes: Option<Vec<String>>,
    pub skip_intraday_breaks: bool,
    pub replay_speed: f64,
}

impl From<&ReplayConfig> for ReplayConfigView {
    fn from(config: &ReplayConfig) -> Self {
        Self {
            lane_queue_capacity: config.lane_queue_capacity,
            tick_interval_ms: config.tick_interval_ms,
            batch_size: config.batch_size,
            snapshot_depth: config.snapshot_depth,
            write_snapshot_csv: config.write_snapshot_csv,
            snapshot_csv_path: config.snapshot_csv_path.clone(),
            replay_start_date: config
                .replay_start_date
                .format(REPLAY_DATE_FORMAT)
                .to_string(),
            replay_end_date: config
                .replay_end_date
                .format(REPLAY_DATE_FORMAT)
                .to_string(),
            replay_start_time: config
                .replay_start_time
                .format(REPLAY_TIME_FORMAT_WITH_MILLISECONDS)
                .to_string(),
            replay_end_time: config
                .replay_end_time
                .format(REPLAY_TIME_FORMAT_WITH_MILLISECONDS)
                .to_string(),
            replay_codes: config.replay_codes.clone(),
            skip_intraday_breaks: config.skip_intraday_breaks,
            replay_speed: config.replay_speed,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayConfigResponse {
    pub base_config_path: String,
    pub base_replay_config: ReplayConfigView,
    pub active_replay_config: Option<ReplayConfigView>,
}

pub struct ReplayManager {
    base_config: AppConfig,
    status: Arc<RwLock<ReplayStatusSnapshot>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<ReplayCommand>>>>,
    task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    active_replay_config: Arc<RwLock<Option<ReplayConfigView>>>,
}

impl ReplayManager {
    pub fn new(base_config: AppConfig) -> Self {
        Self {
            base_config,
            status: Arc::new(RwLock::new(ReplayStatusSnapshot::default())),
            command_tx: Arc::new(Mutex::new(None)),
            task: Arc::new(Mutex::new(None)),
            active_replay_config: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn start(&self, request: ReplayStartRequest) -> Result<ReplayStatusSnapshot> {
        self.cleanup_finished_task().await;

        {
            let task_guard = self.task.lock().await;
            if task_guard.is_some() {
                return Err(ReplayManagerError::ActiveReplayExists);
            }
        }

        let config = self.config_with_overrides(request)?;
        let active_replay_config = ReplayConfigView::from(&config.replay);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let reporter = ReplayStatusReporter::new(self.status.clone());
        reporter
            .set_status(ReplayStatusSnapshot {
                state: ReplayRuntimeState::Running,
                ..ReplayStatusSnapshot::default()
            })
            .await;

        {
            let mut command_guard = self.command_tx.lock().await;
            *command_guard = Some(command_tx);
        }
        {
            let mut active_guard = self.active_replay_config.write().await;
            *active_guard = Some(active_replay_config);
        }

        let reporter_for_task = reporter.clone();
        let task = tokio::spawn(async move {
            if let Err(err) =
                app::run_with_control(config, command_rx, reporter_for_task.clone()).await
            {
                reporter_for_task.mark_failed(error_chain(err)).await;
            }
        });

        {
            let mut task_guard = self.task.lock().await;
            *task_guard = Some(task);
        }

        Ok(self.status().await)
    }

    pub async fn pause(&self) -> Result<ReplayStatusSnapshot> {
        self.cleanup_finished_task().await;
        let state = self.status.read().await.state;
        if state != ReplayRuntimeState::Running {
            return Err(ReplayManagerError::InvalidPauseState(state));
        }
        self.send_command(ReplayCommand::Pause).await?;

        {
            let mut status = self.status.write().await;
            status.state = ReplayRuntimeState::Paused;
        }

        Ok(self.status().await)
    }

    pub async fn resume(&self) -> Result<ReplayStatusSnapshot> {
        self.cleanup_finished_task().await;
        let state = self.status.read().await.state;
        if state != ReplayRuntimeState::Paused {
            return Err(ReplayManagerError::InvalidResumeState(state));
        }
        self.send_command(ReplayCommand::Resume).await?;

        {
            let mut status = self.status.write().await;
            status.state = ReplayRuntimeState::Running;
        }

        Ok(self.status().await)
    }

    pub async fn stop(&self) -> Result<ReplayStatusSnapshot> {
        self.cleanup_finished_task().await;
        let state = self.status.read().await.state;
        if !matches!(
            state,
            ReplayRuntimeState::Running | ReplayRuntimeState::Paused
        ) {
            return Err(ReplayManagerError::InvalidStopState(state));
        }
        self.send_command(ReplayCommand::Stop).await?;

        {
            let mut status = self.status.write().await;
            status.state = ReplayRuntimeState::Stopping;
        }

        Ok(self.status().await)
    }

    pub async fn status(&self) -> ReplayStatusSnapshot {
        self.cleanup_finished_task().await;
        self.status.read().await.clone()
    }

    pub async fn config(&self) -> ReplayConfigResponse {
        self.cleanup_finished_task().await;
        ReplayConfigResponse {
            base_config_path: DEFAULT_CONFIG_PATH.to_string(),
            base_replay_config: ReplayConfigView::from(&self.base_config.replay),
            active_replay_config: self.active_replay_config.read().await.clone(),
        }
    }

    async fn send_command(&self, command: ReplayCommand) -> Result<()> {
        let command_guard = self.command_tx.lock().await;
        let Some(command_tx) = command_guard.as_ref() else {
            return Err(ReplayManagerError::MissingCommandChannel);
        };
        command_tx
            .send(command)
            .map_err(|_| ReplayManagerError::SendCommand)
    }

    async fn cleanup_finished_task(&self) {
        let maybe_handle = {
            let mut task_guard = self.task.lock().await;
            match task_guard.as_ref() {
                Some(handle) if handle.is_finished() => task_guard.take(),
                _ => None,
            }
        };

        if let Some(handle) = maybe_handle {
            let _ = handle.await;
            let mut command_guard = self.command_tx.lock().await;
            *command_guard = None;
            let mut active_guard = self.active_replay_config.write().await;
            *active_guard = None;
        }
    }

    fn config_with_overrides(&self, request: ReplayStartRequest) -> Result<AppConfig> {
        let mut config = self.base_config.clone();

        if let Some(raw) = request.replay_start_date {
            config.replay.replay_start_date =
                NaiveDate::parse_from_str(raw.trim(), REPLAY_DATE_FORMAT)
                    .map_err(|_| ReplayManagerError::InvalidReplayStartDate(raw))?;
        }
        if let Some(raw) = request.replay_end_date {
            config.replay.replay_end_date =
                NaiveDate::parse_from_str(raw.trim(), REPLAY_DATE_FORMAT)
                    .map_err(|_| ReplayManagerError::InvalidReplayEndDate(raw))?;
        }
        if let Some(raw) = request.replay_start_time {
            config.replay.replay_start_time = parse_replay_time(&raw)
                .map_err(|_| ReplayManagerError::InvalidReplayStartTime(raw))?;
        }
        if let Some(raw) = request.replay_end_time {
            config.replay.replay_end_time = parse_replay_time(&raw)
                .map_err(|_| ReplayManagerError::InvalidReplayEndTime(raw))?;
        }
        if let Some(speed) = request.replay_speed {
            config.replay.replay_speed = speed;
        }
        if let Some(skip_intraday_breaks) = request.skip_intraday_breaks {
            config.replay.skip_intraday_breaks = skip_intraday_breaks;
        }

        Ok(config)
    }
}

fn parse_replay_time(raw: &str) -> std::result::Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(raw.trim(), REPLAY_TIME_FORMAT_WITH_MILLISECONDS)
        .or_else(|_| NaiveTime::parse_from_str(raw.trim(), REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS))
}

fn error_chain(err: AnyhowError) -> String {
    let mut chain = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    chain
}
