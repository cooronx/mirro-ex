/**
 * 一个简单的模拟时钟
 * 1. start()的时候，将当前真实时间写入real_anchor_time，将模拟时段的起点写入sim_anchor_ms
 * 2. now()的时候，先判断状态是不是running，如果是就按照如下公式计算
 * real_elapsed = Instant::now() - real_anchor_time
 * sim_elapsed = real_elapsed * speed
 * sim_now = sim_anchor_ms + sim_elapsed
 * 3.如果 pause()，就先算一次当前 now，把它写回 sim_anchor_ms，然后清掉 real_anchor_time，状态改成 Paused
 * 4.如果 resume()，就重新记一个新的 real_anchor_time = Instant::now()，但 sim_anchor_ms 不变，所以会从暂停点继续往前走。
 */
use std::time::Instant;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, SimClockError>;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum SimClockError {
    #[error("sim_end_ms must be later than sim_start_ms")]
    InvalidTimeRange,
    #[error("speed must be finite")]
    NonFiniteSpeed,
    #[error("speed must be at least 1.0")]
    SpeedTooSlow,
    #[error("clock is already running")]
    AlreadyRunning,
    #[error("clock is paused; call resume instead of start")]
    PausedUseResume,
    #[error("clock is already finished")]
    AlreadyFinished,
    #[error("clock is not running")]
    NotRunning,
    #[error("clock has not started yet; call start first")]
    NotStartedYet,
    #[error("running clock is missing real anchor time")]
    MissingRealAnchorTime,
    #[error("real time must not be earlier than real anchor time")]
    RealTimeWentBackwards,
    #[error("simulated elapsed milliseconds overflowed")]
    SimElapsedOverflow,
    #[error("simulated time overflowed while advancing")]
    SimTimeOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimClockState {
    Ready,
    Running,
    Paused,
    Finished,
}

#[derive(Debug, Clone)]
pub struct SimClock {
    sim_start_ms: u64,
    sim_end_ms: u64,
    sim_anchor_ms: u64,
    real_anchor_time: Option<Instant>,
    speed: f64,
    state: SimClockState,
}

impl SimClock {
    pub fn new(sim_start_ms: u64, sim_end_ms: u64, speed: f64) -> Result<Self> {
        validate_time_range(sim_start_ms, sim_end_ms)?;
        validate_speed(speed)?;

        Ok(Self {
            sim_start_ms,
            sim_end_ms,
            sim_anchor_ms: sim_start_ms,
            real_anchor_time: None,
            speed,
            state: SimClockState::Ready,
        })
    }

    pub fn start(&mut self) -> Result<()> {
        self.start_impl(Instant::now())
    }

    pub fn pause(&mut self) -> Result<()> {
        self.pause_impl(Instant::now())
    }

    pub fn resume(&mut self) -> Result<()> {
        self.resume_impl(Instant::now())
    }

    /**
     * 返回当前模拟的unix毫秒时间戳
     */
    pub fn now(&mut self) -> Result<u64> {
        self.now_at(Instant::now())
    }

    pub fn progress(&mut self) -> Result<f64> {
        let sim_ms = self.now()?;
        Ok(self.progress_for(sim_ms))
    }

    pub fn speed(&self) -> f64 {
        self.speed
    }

    pub fn state(&self) -> SimClockState {
        self.state
    }

    pub fn sim_start_ms(&self) -> u64 {
        self.sim_start_ms
    }

    pub fn sim_end_ms(&self) -> u64 {
        self.sim_end_ms
    }

    fn start_impl(&mut self, now_real: Instant) -> Result<()> {
        match self.state {
            SimClockState::Ready => {
                self.real_anchor_time = Some(now_real);
                self.sim_anchor_ms = self.sim_start_ms;
                self.state = SimClockState::Running;
                Ok(())
            }
            SimClockState::Running => Err(SimClockError::AlreadyRunning),
            SimClockState::Paused => Err(SimClockError::PausedUseResume),
            SimClockState::Finished => Err(SimClockError::AlreadyFinished),
        }
    }

    fn pause_impl(&mut self, now_real: Instant) -> Result<()> {
        if self.state != SimClockState::Running {
            return Err(SimClockError::NotRunning);
        }

        let sim_ms = self.now_at(now_real)?;
        self.real_anchor_time = None;
        self.sim_anchor_ms = sim_ms;
        self.state = if sim_ms >= self.sim_end_ms {
            SimClockState::Finished
        } else {
            SimClockState::Paused
        };

        Ok(())
    }

    fn resume_impl(&mut self, now_real: Instant) -> Result<()> {
        match self.state {
            SimClockState::Paused => {
                self.real_anchor_time = Some(now_real);
                self.state = SimClockState::Running;
                Ok(())
            }
            SimClockState::Ready => Err(SimClockError::NotStartedYet),
            SimClockState::Running => Err(SimClockError::AlreadyRunning),
            SimClockState::Finished => Err(SimClockError::AlreadyFinished),
        }
    }

    fn now_at(&mut self, now_real: Instant) -> Result<u64> {
        let sim_ms = match self.state {
            SimClockState::Ready => self.sim_start_ms,
            SimClockState::Paused => self.sim_anchor_ms,
            SimClockState::Finished => self.sim_end_ms,
            SimClockState::Running => self.running_sim_ms_at(now_real)?,
        };

        if self.state == SimClockState::Running && sim_ms >= self.sim_end_ms {
            self.real_anchor_time = None;
            self.sim_anchor_ms = self.sim_end_ms;
            self.state = SimClockState::Finished;
            return Ok(self.sim_end_ms);
        }

        if matches!(self.state, SimClockState::Paused | SimClockState::Finished) {
            self.sim_anchor_ms = sim_ms;
        }

        Ok(sim_ms)
    }

    fn running_sim_ms_at(&self, now_real: Instant) -> Result<u64> {
        let real_anchor_time = self
            .real_anchor_time
            .ok_or(SimClockError::MissingRealAnchorTime)?;
        let real_elapsed = now_real
            .checked_duration_since(real_anchor_time)
            .ok_or(SimClockError::RealTimeWentBackwards)?;
        let sim_elapsed_ms = real_elapsed.mul_f64(self.speed).as_millis();
        let sim_elapsed_ms =
            u64::try_from(sim_elapsed_ms).map_err(|_| SimClockError::SimElapsedOverflow)?;
        let sim_ms = self
            .sim_anchor_ms
            .checked_add(sim_elapsed_ms)
            .ok_or(SimClockError::SimTimeOverflow)?;

        Ok(sim_ms.min(self.sim_end_ms))
    }

    fn progress_for(&self, sim_ms: u64) -> f64 {
        let elapsed_ms = sim_ms.saturating_sub(self.sim_start_ms);
        let duration_ms = self.sim_end_ms - self.sim_start_ms;

        (elapsed_ms as f64 / duration_ms as f64).clamp(0.0, 1.0)
    }
}

fn validate_time_range(sim_start_ms: u64, sim_end_ms: u64) -> Result<()> {
    if sim_end_ms <= sim_start_ms {
        return Err(SimClockError::InvalidTimeRange);
    }

    Ok(())
}

fn validate_speed(speed: f64) -> Result<()> {
    if !speed.is_finite() {
        return Err(SimClockError::NonFiniteSpeed);
    }

    if speed < 1.0 {
        return Err(SimClockError::SpeedTooSlow);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SimClock, SimClockError, SimClockState};
    use std::time::{Duration, Instant};

    #[test]
    fn rejects_slow_speed() {
        let err = SimClock::new(1_000, 11_000, 0.5).unwrap_err();

        assert_eq!(err, SimClockError::SpeedTooSlow);
    }

    #[test]
    fn rejects_invalid_time_range() {
        let err = SimClock::new(1_000, 1_000, 1.0).unwrap_err();

        assert_eq!(err, SimClockError::InvalidTimeRange);
    }

    #[test]
    fn returns_start_time_before_clock_is_started() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 61_000, 2.0).unwrap();

        assert_eq!(clock.state(), SimClockState::Ready);
        assert_eq!(clock.now_at(real_start).unwrap(), 1_000);
    }

    #[test]
    fn advances_when_running_and_caps_at_end_time() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 11_000, 3.0).unwrap();

        clock.start_impl(real_start).unwrap();
        let sim_ms = clock.now_at(real_start + Duration::from_secs(5)).unwrap();

        assert_eq!(sim_ms, 11_000);
        assert_eq!(clock.state(), SimClockState::Finished);
    }

    #[test]
    fn pause_and_resume_keep_sim_time_continuous() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 61_000, 2.0).unwrap();

        clock.start_impl(real_start).unwrap();
        let before_pause = clock.now_at(real_start + Duration::from_secs(3)).unwrap();
        clock
            .pause_impl(real_start + Duration::from_secs(3))
            .unwrap();

        let while_paused = clock.now_at(real_start + Duration::from_secs(10)).unwrap();
        assert_eq!(before_pause, while_paused);
        assert_eq!(clock.state(), SimClockState::Paused);

        clock
            .resume_impl(real_start + Duration::from_secs(10))
            .unwrap();
        let after_resume = clock.now_at(real_start + Duration::from_secs(12)).unwrap();

        assert_eq!(after_resume - clock.sim_start_ms(), 10_000);
        assert_eq!(clock.state(), SimClockState::Running);
    }

    #[test]
    fn reports_progress_between_zero_and_one() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 11_000, 2.0).unwrap();

        assert_eq!(clock.progress().unwrap(), 0.0);

        clock.start_impl(real_start).unwrap();
        let sim_ms = clock.now_at(real_start + Duration::from_secs(2)).unwrap();

        assert_eq!(sim_ms, 5_000);
        assert!((clock.progress_for(sim_ms) - 0.4).abs() < f64::EPSILON);

        let sim_ms = clock.now_at(real_start + Duration::from_secs(10)).unwrap();
        assert_eq!(sim_ms, 11_000);
        assert_eq!(clock.progress_for(sim_ms), 1.0);
    }
}
