//!
//! 一个简单的模拟时钟
//! 1. start()的时候，将当前真实时间写入real_anchor_time，将模拟时段的起点写入sim_anchor_ms
//! 2. now()的时候，先判断状态是不是running，如果是就按照如下公式计算
//!
//! real_elapsed = Instant::now() - real_anchor_time
//!
//! sim_elapsed = real_elapsed * speed
//!
//! sim_now = sim_anchor_ms + sim_elapsed
//!
//! 3.如果 pause()，就先算一次当前 now，把它写回 sim_anchor_ms，然后清掉 real_anchor_time，状态改成 Paused
//!
//! 4.如果 resume()，就重新记一个新的 real_anchor_time = Instant::now()，但 sim_anchor_ms 不变，所以会从暂停点继续往前走。
//!
use std::time::Instant;

use chrono::{DateTime, FixedOffset, NaiveTime, TimeZone, Utc};
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

/// # 模拟时钟
/// 1. `start()`的时候，将当前真实时间写入real_anchor_time，将模拟时段的起点写入sim_anchor_ms
///
/// 2. `now()`的时候，先判断状态是不是running，如果是就按照如下公式计算
/// ```
/// real_elapsed = Instant::now() - real_anchor_time
/// sim_elapsed = real_elapsed * speed
/// sim_now = sim_anchor_ms + sim_elapsed
/// ```
///
/// 3.如果 `pause()`，就先算一次当前 now，把它写回 sim_anchor_ms，然后清掉 real_anchor_time，状态改成 Paused
///
/// 4.如果 `resume()`，就重新记一个新的 real_anchor_time = Instant::now()，但 sim_anchor_ms 不变，所以会从暂停点继续往前走。
#[derive(Debug, Clone)]
pub struct SimClock {
    sim_start_ms: u64,
    sim_end_ms: u64,
    sim_anchor_ms: u64,
    real_anchor_time: Option<Instant>,
    skip_intraday_breaks: bool,
    speed: f64,
    state: SimClockState,
}

impl SimClock {
    /// 创建一个新的模拟时钟。
    ///
    /// `sim_start_ms` 和 `sim_end_ms` 使用 Unix 毫秒时间戳表示模拟区间；
    /// `speed` 表示模拟时间相对真实时间的推进倍率；
    /// `skip_intraday_breaks` 控制是否跳过盘中非连续交易时段。
    pub fn new(
        sim_start_ms: u64,
        sim_end_ms: u64,
        speed: f64,
        skip_intraday_breaks: bool,
    ) -> Result<Self> {
        validate_time_range(sim_start_ms, sim_end_ms)?;
        validate_speed(speed)?;

        Ok(Self {
            sim_start_ms,
            sim_end_ms,
            sim_anchor_ms: sim_start_ms,
            real_anchor_time: None,
            skip_intraday_breaks,
            speed,
            state: SimClockState::Ready,
        })
    }

    /// 启动模拟时钟。
    ///
    /// 启动后时钟进入 [`SimClockState::Running`]，
    /// 后续 [`Self::now`] 会开始按倍率推进模拟时间。
    pub fn start(&mut self) -> Result<()> {
        self.start_impl(Instant::now())
    }

    /// 暂停模拟时钟。
    ///
    /// 暂停时会先结算一次当前模拟时间，并把该时间写回锚点，
    /// 后续再次恢复时会从这个暂停点继续推进。
    pub fn pause(&mut self) -> Result<()> {
        self.pause_impl(Instant::now())
    }

    /// 恢复一个已暂停的模拟时钟。
    ///
    /// 恢复后时钟重新进入 [`SimClockState::Running`]，
    /// 并从最近一次暂停时的模拟时间继续推进。
    pub fn resume(&mut self) -> Result<()> {
        self.resume_impl(Instant::now())
    }

    /// 调整模拟时间推进倍率。
    ///
    /// 如果时钟正在运行，会先按旧倍率结算到当前模拟时间，再从该点用新倍率继续推进。
    /// 如果时钟处于暂停或就绪状态，只更新倍率，后续启动/恢复时生效。
    pub fn set_speed(&mut self, speed: f64) -> Result<()> {
        self.set_speed_impl(speed, Instant::now())
    }

    /// 返回当前模拟时间对应的 Unix 毫秒时间戳。
    ///
    /// 当时钟处于：
    /// - [`SimClockState::Ready`]：返回规范化后的起始时间；
    /// - [`SimClockState::Running`]：按当前倍率计算出的实时模拟时间；
    /// - [`SimClockState::Paused`]：返回暂停时刻；
    /// - [`SimClockState::Finished`]：返回模拟终点。
    pub fn now(&mut self) -> Result<u64> {
        self.now_at(Instant::now())
    }

    /// 返回当前模拟进度，范围固定为 `[0.0, 1.0]`。
    ///
    /// 当启用了 `skip_intraday_breaks` 时，
    /// 进度只按连续交易时段的有效时长计算。
    pub fn progress(&mut self) -> Result<f64> {
        let sim_ms = self.now()?;
        Ok(self.progress_for(sim_ms))
    }

    /// 返回当前配置的模拟时间推进倍率。
    pub fn speed(&self) -> f64 {
        self.speed
    }

    /// 返回当前模拟时钟状态。
    pub fn state(&self) -> SimClockState {
        self.state
    }

    /// 返回模拟区间起点的 Unix 毫秒时间戳。
    pub fn sim_start_ms(&self) -> u64 {
        self.sim_start_ms
    }

    /// 返回模拟区间终点的 Unix 毫秒时间戳。
    pub fn sim_end_ms(&self) -> u64 {
        self.sim_end_ms
    }

    fn start_impl(&mut self, now_real: Instant) -> Result<()> {
        match self.state {
            SimClockState::Ready => {
                self.real_anchor_time = Some(now_real);
                self.sim_anchor_ms = self.normalize_to_trading_time(self.sim_start_ms);
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

    fn set_speed_impl(&mut self, speed: f64, now_real: Instant) -> Result<()> {
        validate_speed(speed)?;
        if self.state == SimClockState::Running {
            let sim_ms = self.now_at(now_real)?;
            self.sim_anchor_ms = sim_ms;
            self.real_anchor_time = Some(now_real);
        }
        self.speed = speed;
        Ok(())
    }

    fn now_at(&mut self, now_real: Instant) -> Result<u64> {
        let sim_ms = match self.state {
            SimClockState::Ready => self.normalize_to_trading_time(self.sim_start_ms),
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
        if !self.skip_intraday_breaks {
            return self
                .sim_anchor_ms
                .checked_add(sim_elapsed_ms)
                .map(|value| value.min(self.sim_end_ms))
                .ok_or(SimClockError::SimTimeOverflow);
        }

        Ok(advance_trading_time(
            self.sim_anchor_ms,
            sim_elapsed_ms,
            self.sim_end_ms,
        )?)
    }

    fn progress_for(&self, sim_ms: u64) -> f64 {
        let elapsed_ms = if self.skip_intraday_breaks {
            let normalized_start = self.normalize_to_trading_time(self.sim_start_ms);
            let normalized_now = self.normalize_to_trading_time(sim_ms.min(self.sim_end_ms));
            trading_duration_between(normalized_start, normalized_now)
        } else {
            sim_ms
                .min(self.sim_end_ms)
                .saturating_sub(self.sim_start_ms)
        };
        let duration_ms = if self.skip_intraday_breaks {
            trading_duration_between(
                self.normalize_to_trading_time(self.sim_start_ms),
                self.sim_end_ms,
            )
        } else {
            self.sim_end_ms.saturating_sub(self.sim_start_ms)
        };

        if duration_ms == 0 {
            return 1.0;
        }

        (elapsed_ms as f64 / duration_ms as f64).clamp(0.0, 1.0)
    }

    fn normalize_to_trading_time(&self, timestamp_ms: u64) -> u64 {
        if !self.skip_intraday_breaks {
            return timestamp_ms.min(self.sim_end_ms);
        }

        normalize_to_trading_time(timestamp_ms, self.sim_end_ms)
    }
}

fn shanghai_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("valid UTC+8 offset")
}

fn local_time(timestamp_ms: u64) -> NaiveTime {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms as i64)
        .expect("valid replay timestamp")
        .with_timezone(&shanghai_offset())
        .time()
}

fn break_end_for(timestamp_ms: u64) -> Option<u64> {
    let time = local_time(timestamp_ms);

    if time > NaiveTime::from_hms_opt(9, 25, 0).expect("valid time")
        && time < NaiveTime::from_hms_opt(9, 30, 0).expect("valid time")
    {
        return Some(replace_local_time(timestamp_ms, 9, 30, 0));
    }

    if time > NaiveTime::from_hms_opt(11, 30, 0).expect("valid time")
        && time < NaiveTime::from_hms_opt(13, 0, 0).expect("valid time")
    {
        return Some(replace_local_time(timestamp_ms, 13, 0, 0));
    }

    None
}

fn next_break_start_after(timestamp_ms: u64) -> Option<u64> {
    let pre_open_break_start = replace_local_time(timestamp_ms, 9, 25, 0);
    let lunch_break_start = replace_local_time(timestamp_ms, 11, 30, 0);

    if timestamp_ms < pre_open_break_start {
        Some(pre_open_break_start)
    } else if timestamp_ms < lunch_break_start {
        Some(lunch_break_start)
    } else {
        None
    }
}

fn break_end_if_at_boundary(timestamp_ms: u64) -> Option<u64> {
    let pre_open_break_start = replace_local_time(timestamp_ms, 9, 25, 0);
    if timestamp_ms == pre_open_break_start {
        return Some(replace_local_time(timestamp_ms, 9, 30, 0));
    }

    let lunch_break_start = replace_local_time(timestamp_ms, 11, 30, 0);
    if timestamp_ms == lunch_break_start {
        return Some(replace_local_time(timestamp_ms, 13, 0, 0));
    }

    None
}

fn normalize_to_trading_time(timestamp_ms: u64, sim_end_ms: u64) -> u64 {
    break_end_for(timestamp_ms)
        .map(|break_end| break_end.min(sim_end_ms))
        .unwrap_or(timestamp_ms.min(sim_end_ms))
}

fn advance_trading_time(start_ms: u64, active_elapsed_ms: u64, sim_end_ms: u64) -> Result<u64> {
    let mut current = normalize_to_trading_time(start_ms, sim_end_ms);
    let mut remaining = active_elapsed_ms;

    while remaining > 0 && current < sim_end_ms {
        let next_break_start = next_break_start_after(current).unwrap_or(sim_end_ms);
        let segment_end = next_break_start.min(sim_end_ms);
        let segment_len = segment_end.saturating_sub(current);

        if remaining < segment_len {
            return current
                .checked_add(remaining)
                .ok_or(SimClockError::SimTimeOverflow);
        }

        remaining -= segment_len;
        current = if remaining > 0 {
            break_end_if_at_boundary(segment_end)
                .map(|break_end| break_end.min(sim_end_ms))
                .unwrap_or_else(|| normalize_to_trading_time(segment_end, sim_end_ms))
        } else {
            segment_end.min(sim_end_ms)
        };
    }

    Ok(current.min(sim_end_ms))
}

fn trading_duration_between(start_ms: u64, end_ms: u64) -> u64 {
    if end_ms <= start_ms {
        return 0;
    }

    let mut current = normalize_to_trading_time(start_ms, end_ms);
    let target = normalize_to_trading_time(end_ms, end_ms);
    let mut duration = 0_u64;

    while current < target {
        let next_break_start = next_break_start_after(current).unwrap_or(target);
        let segment_end = next_break_start.min(target);
        duration = duration.saturating_add(segment_end.saturating_sub(current));
        current = normalize_to_trading_time(segment_end, target);
    }

    duration
}

fn replace_local_time(timestamp_ms: u64, hour: u32, minute: u32, second: u32) -> u64 {
    let offset = shanghai_offset();
    let local = DateTime::<Utc>::from_timestamp_millis(timestamp_ms as i64)
        .expect("valid replay timestamp")
        .with_timezone(&offset);
    let date = local.date_naive();
    let time = date
        .and_hms_opt(hour, minute, second)
        .expect("valid local time");
    offset
        .from_local_datetime(&time)
        .single()
        .expect("unambiguous local datetime")
        .timestamp_millis() as u64
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
    use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
    use std::time::{Duration, Instant};

    fn timestamp_ms(time: &str) -> u64 {
        let date = NaiveDate::from_ymd_opt(2026, 5, 14).expect("valid test date");
        let time = NaiveTime::parse_from_str(time, "%H:%M:%S%.3f").expect("valid test time");
        let local = NaiveDateTime::new(date, time);
        FixedOffset::east_opt(8 * 3600)
            .expect("valid UTC+8 offset")
            .from_local_datetime(&local)
            .single()
            .expect("unambiguous timestamp")
            .timestamp_millis() as u64
    }

    #[test]
    fn rejects_slow_speed() {
        let err = SimClock::new(1_000, 11_000, 0.5, false).unwrap_err();

        assert_eq!(err, SimClockError::SpeedTooSlow);
    }

    #[test]
    fn rejects_invalid_time_range() {
        let err = SimClock::new(1_000, 1_000, 1.0, false).unwrap_err();

        assert_eq!(err, SimClockError::InvalidTimeRange);
    }

    #[test]
    fn returns_start_time_before_clock_is_started() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 61_000, 2.0, false).unwrap();

        assert_eq!(clock.state(), SimClockState::Ready);
        assert_eq!(clock.now_at(real_start).unwrap(), 1_000);
    }

    #[test]
    fn advances_when_running_and_caps_at_end_time() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 11_000, 3.0, false).unwrap();

        clock.start_impl(real_start).unwrap();
        let sim_ms = clock.now_at(real_start + Duration::from_secs(5)).unwrap();

        assert_eq!(sim_ms, 11_000);
        assert_eq!(clock.state(), SimClockState::Finished);
    }

    #[test]
    fn pause_and_resume_keep_sim_time_continuous() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 61_000, 2.0, false).unwrap();

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
    fn set_speed_while_running_keeps_sim_time_continuous() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 61_000, 2.0, false).unwrap();

        clock.start_impl(real_start).unwrap();
        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(3)).unwrap(),
            7_000
        );
        clock
            .set_speed_impl(4.0, real_start + Duration::from_secs(3))
            .unwrap();

        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(5)).unwrap(),
            15_000
        );
        assert_eq!(clock.speed(), 4.0);
    }

    #[test]
    fn reports_progress_between_zero_and_one() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(1_000, 11_000, 2.0, false).unwrap();

        assert_eq!(clock.progress().unwrap(), 0.0);

        clock.start_impl(real_start).unwrap();
        let sim_ms = clock.now_at(real_start + Duration::from_secs(2)).unwrap();

        assert_eq!(sim_ms, 5_000);
        assert!((clock.progress_for(sim_ms) - 0.4).abs() < f64::EPSILON);

        let sim_ms = clock.now_at(real_start + Duration::from_secs(10)).unwrap();
        assert_eq!(sim_ms, 11_000);
        assert_eq!(clock.progress_for(sim_ms), 1.0);
    }

    #[test]
    fn skips_pre_open_break() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(
            timestamp_ms("09:24:59.000"),
            timestamp_ms("09:30:02.000"),
            1.0,
            true,
        )
        .unwrap();

        clock.start_impl(real_start).unwrap();

        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(1)).unwrap(),
            timestamp_ms("09:25:00.000")
        );
        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(2)).unwrap(),
            timestamp_ms("09:30:01.000")
        );
        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(3)).unwrap(),
            timestamp_ms("09:30:02.000")
        );
    }

    #[test]
    fn skips_midday_break() {
        let real_start = Instant::now();
        let mut clock = SimClock::new(
            timestamp_ms("11:29:59.000"),
            timestamp_ms("13:00:02.000"),
            1.0,
            true,
        )
        .unwrap();

        clock.start_impl(real_start).unwrap();

        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(1)).unwrap(),
            timestamp_ms("11:30:00.000")
        );
        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(2)).unwrap(),
            timestamp_ms("13:00:01.000")
        );
        assert_eq!(
            clock.now_at(real_start + Duration::from_secs(3)).unwrap(),
            timestamp_ms("13:00:02.000")
        );
    }
}
