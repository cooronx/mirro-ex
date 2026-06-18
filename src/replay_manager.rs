use std::sync::Arc;

use anyhow::Error as AnyhowError;
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::error;

use crate::app;
use crate::config::{AppConfig, DEFAULT_CONFIG_PATH, ReplayConfig};
use crate::event_bus::{AppEvent, EventBus};
use crate::market::MarketState;
use crate::replay::{
    ReplayCommand, ReplayRuntimeState, ReplayStatusReporter, ReplayStatusSnapshot,
};

const REPLAY_DATE_FORMAT: &str = "%Y-%m-%d";
const REPLAY_TIME_FORMAT_WITH_MILLISECONDS: &str = "%H:%M:%S%.3f";
const REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS: &str = "%H:%M:%S";

#[derive(Debug, Deserialize, Clone)]
pub struct ReplayStartRequest {
    pub replay_start_date: String,
    pub replay_end_date: String,
    pub replay_start_time: String,
    pub replay_end_time: String,
    pub replay_codes: Vec<String>,
    pub replay_speed: f64,
    pub skip_intraday_breaks: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ReplaySpeedRequest {
    pub replay_speed: f64,
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
    #[error("cannot set replay speed while state is {0:?}")]
    InvalidSetSpeedState(ReplayRuntimeState),
    #[error("replay_speed must be finite")]
    NonFiniteReplaySpeed,
    #[error("replay_speed must be at least 1.0")]
    ReplaySpeedTooSlow,
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
pub struct ReplayEngineConfigView {
    pub lane_queue_capacity: usize,
    pub orderbook_workers: usize,
    pub tick_interval_ms: u64,
    pub batch_size: i64,
    pub snapshot_depth: usize,
    pub write_snapshot_parquet: bool,
    pub snapshot_parquet_dir: String,
}

impl From<&ReplayConfig> for ReplayEngineConfigView {
    fn from(config: &ReplayConfig) -> Self {
        Self {
            lane_queue_capacity: config.lane_queue_capacity,
            orderbook_workers: config.orderbook_workers,
            tick_interval_ms: config.tick_interval_ms,
            batch_size: config.batch_size,
            snapshot_depth: config.snapshot_depth,
            write_snapshot_parquet: config.write_snapshot_parquet,
            snapshot_parquet_dir: config.snapshot_parquet_dir.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplayTaskConfig {
    pub replay_start_date: NaiveDate,
    pub replay_end_date: NaiveDate,
    pub replay_start_time: NaiveTime,
    pub replay_end_time: NaiveTime,
    pub replay_codes: Vec<String>,
    pub skip_intraday_breaks: bool,
    pub replay_speed: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayTaskConfigView {
    pub replay_start_date: String,
    pub replay_end_date: String,
    pub replay_start_time: String,
    pub replay_end_time: String,
    pub replay_codes: Vec<String>,
    pub skip_intraday_breaks: bool,
    pub replay_speed: f64,
}

impl From<&ReplayTaskConfig> for ReplayTaskConfigView {
    fn from(config: &ReplayTaskConfig) -> Self {
        Self {
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
    pub engine_replay_config: ReplayEngineConfigView,
    pub active_replay_task: Option<ReplayTaskConfigView>,
}

pub struct ReplayManager {
    base_config: AppConfig,
    market_state: MarketState,
    status: Arc<RwLock<ReplayStatusSnapshot>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<ReplayCommand>>>>,
    task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    active_replay_task: Arc<RwLock<Option<ReplayTaskConfigView>>>,
    event_bus: Option<EventBus>,
}

impl ReplayManager {
    pub fn new(base_config: AppConfig, market_state: MarketState) -> Self {
        Self {
            base_config,
            market_state,
            status: Arc::new(RwLock::new(ReplayStatusSnapshot::default())),
            command_tx: Arc::new(Mutex::new(None)),
            task: Arc::new(Mutex::new(None)),
            active_replay_task: Arc::new(RwLock::new(None)),
            event_bus: None,
        }
    }

    pub fn with_event_bus(
        base_config: AppConfig,
        market_state: MarketState,
        event_bus: EventBus,
    ) -> Self {
        Self {
            base_config,
            market_state,
            status: Arc::new(RwLock::new(ReplayStatusSnapshot::default())),
            command_tx: Arc::new(Mutex::new(None)),
            task: Arc::new(Mutex::new(None)),
            active_replay_task: Arc::new(RwLock::new(None)),
            event_bus: Some(event_bus),
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

        let task_config = self.task_config_from_request(request)?;
        self.market_state.clear();
        let active_replay_task = ReplayTaskConfigView::from(&task_config);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let reporter = match &self.event_bus {
            Some(event_bus) => {
                ReplayStatusReporter::with_event_bus(self.status.clone(), event_bus.clone())
            }
            None => ReplayStatusReporter::new(self.status.clone()),
        };
        reporter
            .set_status(ReplayStatusSnapshot {
                state: ReplayRuntimeState::Running,
                replay_speed: Some(task_config.replay_speed),
                ..ReplayStatusSnapshot::default()
            })
            .await;

        {
            let mut command_guard = self.command_tx.lock().await;
            *command_guard = Some(command_tx);
        }
        {
            let mut active_guard = self.active_replay_task.write().await;
            *active_guard = Some(active_replay_task);
        }
        self.publish_replay_changed();

        let config = self.base_config.clone();
        let market_state = self.market_state.clone();
        let event_bus = self.event_bus.clone();
        let reporter_for_task = reporter.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = app::run_with_control(
                config,
                task_config,
                command_rx,
                reporter_for_task.clone(),
                market_state,
                event_bus,
            )
            .await
            {
                let error_chain = error_chain(err);
                error!(error_chain = %error_chain, "replay task failed");
                reporter_for_task.mark_failed(error_chain).await;
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
        self.publish_replay_changed();

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
        self.publish_replay_changed();

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
        self.publish_replay_changed();

        Ok(self.status().await)
    }

    pub async fn set_speed(&self, request: ReplaySpeedRequest) -> Result<ReplayStatusSnapshot> {
        self.cleanup_finished_task().await;
        validate_replay_speed(request.replay_speed)?;
        let state = self.status.read().await.state;
        if !matches!(
            state,
            ReplayRuntimeState::Running | ReplayRuntimeState::Paused
        ) {
            return Err(ReplayManagerError::InvalidSetSpeedState(state));
        }
        self.send_command(ReplayCommand::SetSpeed(request.replay_speed))
            .await?;

        {
            let mut status = self.status.write().await;
            status.replay_speed = Some(request.replay_speed);
        }
        {
            let mut active_guard = self.active_replay_task.write().await;
            if let Some(task) = active_guard.as_mut() {
                task.replay_speed = request.replay_speed;
            }
        }
        self.publish_replay_changed();

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
            engine_replay_config: ReplayEngineConfigView::from(&self.base_config.replay),
            active_replay_task: self.active_replay_task.read().await.clone(),
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
            let mut active_guard = self.active_replay_task.write().await;
            *active_guard = None;
            self.publish_replay_changed();
        }
    }

    fn task_config_from_request(&self, request: ReplayStartRequest) -> Result<ReplayTaskConfig> {
        validate_replay_speed(request.replay_speed)?;
        Ok(ReplayTaskConfig {
            replay_start_date: NaiveDate::parse_from_str(
                request.replay_start_date.trim(),
                REPLAY_DATE_FORMAT,
            )
            .map_err(|_| ReplayManagerError::InvalidReplayStartDate(request.replay_start_date))?,
            replay_end_date: NaiveDate::parse_from_str(
                request.replay_end_date.trim(),
                REPLAY_DATE_FORMAT,
            )
            .map_err(|_| ReplayManagerError::InvalidReplayEndDate(request.replay_end_date))?,
            replay_start_time: parse_replay_time(&request.replay_start_time).map_err(|_| {
                ReplayManagerError::InvalidReplayStartTime(request.replay_start_time)
            })?,
            replay_end_time: parse_replay_time(&request.replay_end_time)
                .map_err(|_| ReplayManagerError::InvalidReplayEndTime(request.replay_end_time))?,
            replay_codes: request.replay_codes,
            skip_intraday_breaks: request.skip_intraday_breaks,
            replay_speed: request.replay_speed,
        })
    }

    fn publish_replay_changed(&self) {
        if let Some(event_bus) = &self.event_bus {
            event_bus.publish(AppEvent::ReplayChanged);
        }
    }
}

fn validate_replay_speed(speed: f64) -> Result<()> {
    if !speed.is_finite() {
        return Err(ReplayManagerError::NonFiniteReplaySpeed);
    }
    if speed < 1.0 {
        return Err(ReplayManagerError::ReplaySpeedTooSlow);
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{ReplayManagerError, validate_replay_speed};

    #[test]
    fn rejects_invalid_replay_speed() {
        assert!(matches!(
            validate_replay_speed(f64::NAN),
            Err(ReplayManagerError::NonFiniteReplaySpeed)
        ));
        assert!(matches!(
            validate_replay_speed(0.5),
            Err(ReplayManagerError::ReplaySpeedTooSlow)
        ));
    }

    #[test]
    fn accepts_valid_replay_speed() {
        validate_replay_speed(1.0).unwrap();
        validate_replay_speed(10.0).unwrap();
    }
}
