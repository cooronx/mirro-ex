//!
//! replay全局调度与归并模块。
//! 1. 输入：
//!    - `SimClock` 当前的模拟时间
//!    - 各 lane 通过 channel 送来的 `LaneOutput`
//!    - `ReplayDbReader` 与 lane producer 组成的后台供给链路
//!
//! 2. 输出：
//!    - `ReplayTickResult`
//!    - 包含当前 tick 能安全发出的 `ReplayEvent` 列表，以及 lag / finished 等状态
//!
//! 3. 逻辑：
//!    - 启动并管理所有 lane producer
//!    - 维护每个 lane 当前 ready 的事件队列和 watermark
//!    - 根据 `SimClock` 当前时间计算本轮允许发出的安全时间上界
//!    - 从所有 lane 的队首事件中做全局归并，确保跨 channel 的事件按正确顺序输出
//!
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};

use thiserror::Error;
use tokio::sync::mpsc::error::TryRecvError;

use crate::replay::controller::{ReplayDebugSnapshot, ReplayLaneDebugSnapshot};
use crate::sim_clock::{SimClock, SimClockError};

use super::db_reader::ReplayDbReader;
use super::event::ReplayEvent;
use super::producer::{LaneKey, LaneOutput, LaneProducerError, LaneReceiver, spawn_lane_producers};

pub type Result<T> = std::result::Result<T, ReplayCoordinatorError>;

#[derive(Debug, Error)]
pub enum ReplayCoordinatorError {
    #[error("tick_interval_ms must be greater than 0")]
    InvalidTickInterval,
    #[error("lane queue capacity must be greater than 0")]
    InvalidQueueCapacity,
    #[error("sim clock failed")]
    Clock(#[from] SimClockError),
    #[error("lane producer failed")]
    Producer(#[from] LaneProducerError),
    #[error("lane output receiver closed before finished for market={market:?} channel={channel}")]
    LaneReceiverClosed {
        market: crate::common::Market,
        channel: i64,
    },
    #[error(
        "lane output key mismatch: expected market={expected_market:?} channel={expected_channel}, actual market={actual_market:?} channel={actual_channel}"
    )]
    LaneOutputKeyMismatch {
        expected_market: crate::common::Market,
        expected_channel: i64,
        actual_market: crate::common::Market,
        actual_channel: i64,
    },
}

#[derive(Debug, Clone)]
pub struct ReplayTickResult {
    pub sim_now_ms: u64,
    pub safe_emit_time_ms: Option<i64>,
    pub lag_ms: u64,
    pub events: Vec<ReplayEvent>,
    pub finished: bool,
}

struct LaneRuntime {
    receiver: tokio::sync::mpsc::Receiver<LaneOutput>,
    ready_events: VecDeque<ReplayEvent>,
    watermark_ms: Option<i64>,
    warmed_up: bool,
    finished: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeapHead {
    timestamp_ms: i64,
    lane_key: LaneKey,
    message_number: i64,
}

impl Ord for HeapHead {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.timestamp_ms, other.lane_key, other.message_number).cmp(&(
            self.timestamp_ms,
            self.lane_key,
            self.message_number,
        ))
    }
}

impl PartialOrd for HeapHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct ReplayCoordinator {
    clock: SimClock,
    tick_interval_ms: u64,
    lanes: BTreeMap<LaneKey, LaneRuntime>,
    clock_started: bool,
    last_emitted_timestamp_ms: Option<i64>,
}

impl ReplayCoordinator {
    pub fn new(
        lane_receivers: Vec<LaneReceiver>,
        clock: SimClock,
        tick_interval_ms: u64,
    ) -> Result<Self> {
        if tick_interval_ms == 0 {
            return Err(ReplayCoordinatorError::InvalidTickInterval);
        }

        let lanes = lane_receivers
            .into_iter()
            .map(|lane_receiver| {
                (
                    lane_receiver.lane_key,
                    LaneRuntime {
                        receiver: lane_receiver.receiver,
                        ready_events: VecDeque::new(),
                        watermark_ms: None,
                        warmed_up: false,
                        finished: false,
                    },
                )
            })
            .collect();

        Ok(Self {
            clock,
            tick_interval_ms,
            lanes,
            clock_started: false,
            last_emitted_timestamp_ms: None,
        })
    }

    pub async fn from_reader(
        reader: ReplayDbReader,
        clock: SimClock,
        tick_interval_ms: u64,
        queue_capacity: usize,
    ) -> Result<Self> {
        if queue_capacity == 0 {
            return Err(ReplayCoordinatorError::InvalidQueueCapacity);
        }

        let lane_receivers = spawn_lane_producers(reader, queue_capacity).await?;
        Self::new(lane_receivers, clock, tick_interval_ms)
    }

    pub fn tick_interval_ms(&self) -> u64 {
        self.tick_interval_ms
    }

    pub fn pause_clock(&mut self) -> Result<()> {
        self.clock.pause()?;
        Ok(())
    }

    pub fn resume_clock(&mut self) -> Result<()> {
        self.clock.resume()?;
        Ok(())
    }

    pub fn set_clock_speed(&mut self, speed: f64) -> Result<()> {
        self.clock.set_speed(speed)?;
        Ok(())
    }

    pub fn current_sim_now(&mut self) -> Result<u64> {
        Ok(self.clock.now()?)
    }

    pub fn progress(&mut self) -> Result<f64> {
        Ok(self.clock.progress()?)
    }

    pub fn is_finished(&self) -> bool {
        self.lanes
            .values()
            .all(|lane_runtime| lane_runtime.finished && lane_runtime.ready_events.is_empty())
    }

    pub fn debug_snapshot(&self) -> ReplayDebugSnapshot {
        ReplayDebugSnapshot {
            unfinished_lanes: self
                .lanes
                .iter()
                .filter(|(_, lane_runtime)| {
                    !lane_runtime.finished || !lane_runtime.ready_events.is_empty()
                })
                .map(|(lane_key, lane_runtime)| ReplayLaneDebugSnapshot {
                    market: format!("{:?}", lane_key.market),
                    channel: lane_key.channel,
                    ready_events: lane_runtime.ready_events.len(),
                    watermark_ms: lane_runtime.watermark_ms,
                    warmed_up: lane_runtime.warmed_up,
                    finished: lane_runtime.finished,
                })
                .collect(),
        }
    }

    pub async fn bootstrap(&mut self) -> Result<()> {
        self.bootstrap_impl().await
    }

    pub async fn poll_ready_events(&mut self) -> Result<ReplayTickResult> {
        if !self.clock_started {
            self.bootstrap_impl().await?;
        }

        self.drain_available_outputs()?;
        let sim_now_ms = self.clock.now()?;
        let safe_emit_time_ms = self.compute_safe_emit_time();
        let lag_ms = safe_emit_time_ms
            .map(|safe| sim_now_ms.saturating_sub(safe.max(0) as u64))
            .unwrap_or(0);
        let emit_until = safe_emit_time_ms.map(|safe| safe.min(sim_now_ms as i64));
        let events = self.emit_events_until(emit_until);
        let finished = self.is_finished();

        Ok(ReplayTickResult {
            sim_now_ms,
            safe_emit_time_ms,
            lag_ms,
            events,
            finished,
        })
    }

    async fn bootstrap_impl(&mut self) -> Result<()> {
        if self.clock_started {
            return Ok(());
        }

        let lane_keys: Vec<LaneKey> = self.lanes.keys().copied().collect();
        for lane_key in lane_keys {
            loop {
                let lane_runtime = self
                    .lanes
                    .get_mut(&lane_key)
                    .expect("lane must exist during bootstrap");
                if lane_runtime.warmed_up {
                    break;
                }

                match lane_runtime.receiver.recv().await {
                    Some(output) => {
                        Self::apply_lane_output_to_runtime(lane_runtime, lane_key, output)?
                    }
                    None => {
                        return Err(ReplayCoordinatorError::LaneReceiverClosed {
                            market: lane_key.market,
                            channel: lane_key.channel,
                        });
                    }
                }
            }
        }

        self.drain_available_outputs()?;
        self.clock.start()?;
        self.clock_started = true;
        Ok(())
    }

    fn drain_available_outputs(&mut self) -> Result<()> {
        let lane_keys: Vec<LaneKey> = self.lanes.keys().copied().collect();
        for lane_key in lane_keys {
            loop {
                let lane_runtime = self
                    .lanes
                    .get_mut(&lane_key)
                    .expect("lane must exist while draining outputs");
                match lane_runtime.receiver.try_recv() {
                    Ok(output) => {
                        Self::apply_lane_output_to_runtime(lane_runtime, lane_key, output)?
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        if lane_runtime.finished {
                            break;
                        }
                        return Err(ReplayCoordinatorError::LaneReceiverClosed {
                            market: lane_key.market,
                            channel: lane_key.channel,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_lane_output_to_runtime(
        lane_runtime: &mut LaneRuntime,
        expected_lane_key: LaneKey,
        output: LaneOutput,
    ) -> Result<()> {
        match output {
            LaneOutput::ReadyBatch {
                lane_key,
                events,
                watermark_ms,
            } => {
                Self::validate_lane_key(expected_lane_key, lane_key)?;
                lane_runtime.ready_events.extend(events);
                Self::sort_ready_events_by_market_time(lane_runtime);
                lane_runtime.watermark_ms = watermark_ms;
                lane_runtime.warmed_up = true;
            }
            LaneOutput::Progress {
                lane_key,
                watermark_ms,
            } => {
                Self::validate_lane_key(expected_lane_key, lane_key)?;
                lane_runtime.watermark_ms = watermark_ms;
                lane_runtime.warmed_up = true;
            }
            LaneOutput::Finished { lane_key } => {
                Self::validate_lane_key(expected_lane_key, lane_key)?;
                lane_runtime.finished = true;
                lane_runtime.warmed_up = true;
            }
        }

        Ok(())
    }

    fn sort_ready_events_by_market_time(lane_runtime: &mut LaneRuntime) {
        lane_runtime
            .ready_events
            .make_contiguous()
            .sort_by_key(|event| (event.timestamp_ms(), event.message_number()));
    }

    fn validate_lane_key(expected: LaneKey, actual: LaneKey) -> Result<()> {
        if expected == actual {
            return Ok(());
        }

        Err(ReplayCoordinatorError::LaneOutputKeyMismatch {
            expected_market: expected.market,
            expected_channel: expected.channel,
            actual_market: actual.market,
            actual_channel: actual.channel,
        })
    }

    fn compute_safe_emit_time(&self) -> Option<i64> {
        let mut min_upper_bound: Option<i64> = None;

        for lane_runtime in self.lanes.values() {
            if lane_runtime.finished {
                continue;
            }

            let Some(upper_bound) = lane_runtime.watermark_ms else {
                return self.last_emitted_timestamp_ms;
            };

            min_upper_bound = Some(match min_upper_bound {
                Some(current) => current.min(upper_bound),
                None => upper_bound,
            });
        }

        min_upper_bound.or(Some(i64::MAX))
    }

    fn emit_events_until(&mut self, emit_until: Option<i64>) -> Vec<ReplayEvent> {
        let Some(emit_until) = emit_until else {
            return Vec::new();
        };

        let mut heap = self.build_head_heap();
        let mut emitted_events = Vec::new();

        while let Some(head) = heap.pop() {
            if head.timestamp_ms > emit_until {
                break;
            }

            let lane_runtime = self
                .lanes
                .get_mut(&head.lane_key)
                .expect("lane must exist for heap head");
            let Some(event) = lane_runtime.ready_events.pop_front() else {
                continue;
            };
            self.last_emitted_timestamp_ms = Some(event.timestamp_ms());
            emitted_events.push(event);

            if let Some(next_event) = lane_runtime.ready_events.front() {
                heap.push(HeapHead {
                    timestamp_ms: next_event.timestamp_ms(),
                    lane_key: head.lane_key,
                    message_number: next_event.message_number(),
                });
            }
        }

        emitted_events
    }

    fn build_head_heap(&self) -> BinaryHeap<HeapHead> {
        let mut heap = BinaryHeap::new();

        for (lane_key, lane_runtime) in &self.lanes {
            if let Some(event) = lane_runtime.ready_events.front() {
                heap.push(HeapHead {
                    timestamp_ms: event.timestamp_ms(),
                    lane_key: *lane_key,
                    message_number: event.message_number(),
                });
            }
        }

        heap
    }
}

#[cfg(test)]
mod tests {
    use super::{LaneKey, LaneRuntime, ReplayCoordinator};
    use crate::common::{L2Order, Market, OrderDirection, OrderType};
    use crate::replay::event::ReplayEvent;
    use crate::replay::producer::{LaneOutput, LaneReceiver};
    use crate::sim_clock::SimClock;
    use std::collections::{BTreeMap, VecDeque};
    use tokio::sync::mpsc;

    fn order_event(channel: i64, message_number: i64, timestamp_ms: i64) -> ReplayEvent {
        ReplayEvent::Order(L2Order {
            market: Market::XSHG,
            channel,
            message_number,
            code: format!("SH{channel}"),
            price: 1,
            volume: 1,
            direction: OrderDirection::Buy,
            order_type: OrderType::Limit,
            timestamp_ms,
            order_number: 0,
        })
    }

    #[tokio::test]
    async fn bootstrap_waits_until_every_lane_receives_first_output() {
        let (sender_a, receiver_a) = mpsc::channel(4);
        let (sender_b, receiver_b) = mpsc::channel(4);
        let lane_a = LaneKey::new(Market::XSHG, 1);
        let lane_b = LaneKey::new(Market::XSHE, 2011);

        sender_a
            .send(LaneOutput::ReadyBatch {
                lane_key: lane_a,
                events: vec![order_event(1, 10, 1_100)],
                watermark_ms: Some(1_100),
            })
            .await
            .unwrap();
        sender_b
            .send(LaneOutput::Finished { lane_key: lane_b })
            .await
            .unwrap();

        let clock = SimClock::new(1_000, 2_000, 1.0, false).unwrap();
        let mut coordinator = ReplayCoordinator::new(
            vec![
                LaneReceiver {
                    lane_key: lane_a,
                    receiver: receiver_a,
                },
                LaneReceiver {
                    lane_key: lane_b,
                    receiver: receiver_b,
                },
            ],
            clock,
            100,
        )
        .unwrap();

        coordinator.bootstrap().await.unwrap();

        assert!(coordinator.clock_started);
        assert!(
            coordinator
                .lanes
                .values()
                .all(|lane_runtime| lane_runtime.warmed_up)
        );
    }

    #[test]
    fn emits_globally_sorted_events_from_lane_buffers() {
        let lane_a = LaneRuntime {
            receiver: mpsc::channel(1).1,
            ready_events: VecDeque::from(vec![
                order_event(1, 10, 1_500),
                order_event(1, 11, 1_800),
            ]),
            watermark_ms: Some(1_800),
            warmed_up: true,
            finished: false,
        };
        let lane_b = LaneRuntime {
            receiver: mpsc::channel(1).1,
            ready_events: VecDeque::from(vec![
                order_event(2, 20, 1_500),
                order_event(2, 21, 1_600),
            ]),
            watermark_ms: Some(1_600),
            warmed_up: true,
            finished: false,
        };

        let mut coordinator = ReplayCoordinator {
            clock: SimClock::new(1_000, 2_000, 1.0, false).unwrap(),
            tick_interval_ms: 100,
            lanes: BTreeMap::from([
                (LaneKey::new(Market::XSHG, 1), lane_a),
                (LaneKey::new(Market::XSHG, 2), lane_b),
            ]),
            clock_started: true,
            last_emitted_timestamp_ms: None,
        };

        let events = coordinator.emit_events_until(Some(1_600));
        let ordering: Vec<(i64, i64)> = events
            .into_iter()
            .map(|event| (event.timestamp_ms(), event.message_number()))
            .collect();

        assert_eq!(ordering, vec![(1_500, 10), (1_500, 20), (1_600, 21)]);
    }

    #[tokio::test]
    async fn bootstrap_accepts_progress_as_first_output() {
        let (sender, receiver) = mpsc::channel(4);
        let lane = LaneKey::new(Market::XSHG, 1);

        sender
            .send(LaneOutput::Progress {
                lane_key: lane,
                watermark_ms: Some(1_900),
            })
            .await
            .unwrap();

        let clock = SimClock::new(1_000, 2_000, 1.0, false).unwrap();
        let mut coordinator = ReplayCoordinator::new(
            vec![LaneReceiver {
                lane_key: lane,
                receiver,
            }],
            clock,
            100,
        )
        .unwrap();

        coordinator.bootstrap().await.unwrap();

        let lane_runtime = coordinator.lanes.get(&lane).unwrap();
        assert!(lane_runtime.warmed_up);
        assert_eq!(lane_runtime.watermark_ms, Some(1_900));
    }

    #[tokio::test]
    async fn drain_available_outputs_consumes_multiple_ready_batches_per_lane() {
        let (sender, receiver) = mpsc::channel(4);
        let lane = LaneKey::new(Market::XSHG, 1);

        sender
            .send(LaneOutput::ReadyBatch {
                lane_key: lane,
                events: vec![order_event(1, 10, 1_100)],
                watermark_ms: Some(1_100),
            })
            .await
            .unwrap();
        sender
            .send(LaneOutput::ReadyBatch {
                lane_key: lane,
                events: vec![order_event(1, 11, 1_200)],
                watermark_ms: Some(1_200),
            })
            .await
            .unwrap();

        let mut coordinator = ReplayCoordinator {
            clock: SimClock::new(1_000, 2_000, 1.0, false).unwrap(),
            tick_interval_ms: 100,
            lanes: BTreeMap::from([(
                lane,
                LaneRuntime {
                    receiver,
                    ready_events: VecDeque::new(),
                    watermark_ms: None,
                    warmed_up: true,
                    finished: false,
                },
            )]),
            clock_started: true,
            last_emitted_timestamp_ms: None,
        };

        coordinator.drain_available_outputs().unwrap();
        let lane_runtime = coordinator.lanes.get(&lane).unwrap();
        assert_eq!(lane_runtime.ready_events.len(), 2);
        assert_eq!(
            coordinator
                .lanes
                .get(&lane)
                .and_then(|lane_runtime| lane_runtime.watermark_ms),
            Some(1_200)
        );
    }

    #[test]
    fn uses_lane_watermark_even_when_ready_queue_is_empty() {
        let lane_a = LaneRuntime {
            receiver: mpsc::channel(1).1,
            ready_events: VecDeque::new(),
            watermark_ms: Some(1_900),
            warmed_up: true,
            finished: false,
        };
        let lane_b = LaneRuntime {
            receiver: mpsc::channel(1).1,
            ready_events: VecDeque::from(vec![order_event(2, 20, 1_500)]),
            watermark_ms: Some(2_000),
            warmed_up: true,
            finished: false,
        };

        let coordinator = ReplayCoordinator {
            clock: SimClock::new(1_000, 2_000, 1.0, false).unwrap(),
            tick_interval_ms: 100,
            lanes: BTreeMap::from([
                (LaneKey::new(Market::XSHG, 1), lane_a),
                (LaneKey::new(Market::XSHG, 2), lane_b),
            ]),
            clock_started: true,
            last_emitted_timestamp_ms: None,
        };

        assert_eq!(coordinator.compute_safe_emit_time(), Some(1_900));
    }
}
