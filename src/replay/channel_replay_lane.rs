//! 负责单个 channel 内的委托和成交合流。
//! 两类数据分别缓冲，并按照 `message_number` 合并成统一的 `ReplayEvent` 序列。
//! 只有委托和成交的读取范围都覆盖到安全边界后，事件才会向上游释放。
use std::collections::VecDeque;

use thiserror::Error;

use crate::common::{L2Order, L2Transaction};

use super::db_reader::FetchedBatch;
use super::event::ReplayEvent;
use super::reader_cursor::ReplayDataKind;

pub type Result<T> = std::result::Result<T, ChannelReplayLaneError>;

#[derive(Debug, Error)]
pub enum ChannelReplayLaneError {
    #[error("channel replay lane must enable at least one data kind")]
    NoEnabledDataKind,
    #[error("fetched batch day mismatch: expected={expected}, actual={actual}")]
    DayMismatch { expected: String, actual: String },
    #[error("fetched batch channel mismatch: expected={expected}, actual={actual}")]
    ChannelMismatch { expected: i64, actual: i64 },
    #[error("fetched batch data kind is not enabled for this lane: data_kind={data_kind:?}")]
    DisabledDataKind { data_kind: ReplayDataKind },
    #[error(
        "fetched batch message range did not advance for {data_kind:?}: begin={begin_message_number}, previous_covered_until={previous_covered_until}"
    )]
    NonAdvancingRange {
        data_kind: ReplayDataKind,
        begin_message_number: i64,
        previous_covered_until: i64,
    },
    #[error("fetched batch contained mismatched event kind: expected={expected:?}")]
    EventKindMismatch { expected: ReplayDataKind },
    #[error(
        "fetched batch contained mismatched event channel: expected={expected}, actual={actual}"
    )]
    EventChannelMismatch { expected: i64, actual: i64 },
    #[error(
        "fetched batch contained out-of-range message number: begin={begin_message_number}, end={end_message_number}, actual={actual}"
    )]
    EventOutsideBatchRange {
        begin_message_number: i64,
        end_message_number: i64,
        actual: i64,
    },
    #[error(
        "fetched batch message numbers are not strictly increasing for {data_kind:?}: previous={previous}, current={current}"
    )]
    NonMonotonicEvents {
        data_kind: ReplayDataKind,
        previous: i64,
        current: i64,
    },
}

#[derive(Debug, Clone)]
pub struct ChannelReplayLane {
    day: String,
    channel: i64,
    has_order: bool,
    has_transaction: bool,
    order_finished: bool,
    transaction_finished: bool,
    order_buffer: VecDeque<L2Order>,
    transaction_buffer: VecDeque<L2Transaction>,
    order_covered_until: Option<i64>,
    transaction_covered_until: Option<i64>,
}

impl ChannelReplayLane {
    pub fn new(
        day: impl Into<String>,
        channel: i64,
        has_order: bool,
        has_transaction: bool,
    ) -> Result<Self> {
        if !has_order && !has_transaction {
            return Err(ChannelReplayLaneError::NoEnabledDataKind);
        }

        Ok(Self {
            day: day.into(),
            channel,
            has_order,
            has_transaction,
            order_finished: false,
            transaction_finished: false,
            order_buffer: VecDeque::new(),
            transaction_buffer: VecDeque::new(),
            order_covered_until: None,
            transaction_covered_until: None,
        })
    }

    pub fn day(&self) -> &str {
        &self.day
    }

    pub fn channel(&self) -> i64 {
        self.channel
    }

    pub fn has_order(&self) -> bool {
        self.has_order
    }

    pub fn has_transaction(&self) -> bool {
        self.has_transaction
    }

    pub fn order_finished(&self) -> bool {
        self.order_finished
    }

    pub fn transaction_finished(&self) -> bool {
        self.transaction_finished
    }

    pub fn order_covered_until(&self) -> Option<i64> {
        self.order_covered_until
    }

    pub fn transaction_covered_until(&self) -> Option<i64> {
        self.transaction_covered_until
    }

    pub fn mark_finished(&mut self, data_kind: ReplayDataKind) {
        match data_kind {
            ReplayDataKind::Order => self.order_finished = true,
            ReplayDataKind::Transaction => self.transaction_finished = true,
        }
    }

    pub fn safe_message_number_exclusive(&self) -> Option<i64> {
        match (self.has_order, self.has_transaction) {
            (true, true) => {
                if self.order_finished && self.transaction_finished {
                    match (self.order_covered_until, self.transaction_covered_until) {
                        (Some(order_covered_until), Some(transaction_covered_until)) => {
                            Some(order_covered_until.max(transaction_covered_until))
                        }
                        (Some(order_covered_until), None) => Some(order_covered_until),
                        (None, Some(transaction_covered_until)) => Some(transaction_covered_until),
                        (None, None) => None,
                    }
                } else if self.order_finished {
                    self.transaction_covered_until
                } else if self.transaction_finished {
                    self.order_covered_until
                } else {
                    Some(
                        self.order_covered_until?
                            .min(self.transaction_covered_until?),
                    )
                }
            }
            (true, false) => self.order_covered_until,
            (false, true) => self.transaction_covered_until,
            (false, false) => None,
        }
    }

    pub fn order_buffer_len(&self) -> usize {
        self.order_buffer.len()
    }

    pub fn transaction_buffer_len(&self) -> usize {
        self.transaction_buffer.len()
    }

    pub fn next_buffered_event_timestamp_ms(&self) -> Option<i64> {
        match (self.order_buffer.front(), self.transaction_buffer.front()) {
            (Some(order), Some(transaction)) => {
                if order.message_number < transaction.message_number {
                    Some(order.timestamp_ms)
                } else {
                    Some(transaction.timestamp_ms)
                }
            }
            (Some(order), None) => Some(order.timestamp_ms),
            (None, Some(transaction)) => Some(transaction.timestamp_ms),
            (None, None) => None,
        }
    }

    pub fn push_batch(&mut self, batch: FetchedBatch) -> Result<()> {
        self.validate_batch_identity(&batch)?;

        match batch.data_kind {
            ReplayDataKind::Order => self.push_order_batch(batch),
            ReplayDataKind::Transaction => self.push_transaction_batch(batch),
        }
    }

    pub fn pop_ready_events(&mut self) -> Vec<ReplayEvent> {
        let Some(safe_seq_exclusive) = self.safe_message_number_exclusive() else {
            return Vec::new();
        };

        let mut ready_events = Vec::new();

        loop {
            let next_kind = match (self.order_buffer.front(), self.transaction_buffer.front()) {
                (Some(order), Some(transaction)) => {
                    if order.message_number < transaction.message_number {
                        Some(ReplayDataKind::Order)
                    } else {
                        Some(ReplayDataKind::Transaction)
                    }
                }
                (Some(order), None) => {
                    if order.message_number < safe_seq_exclusive {
                        Some(ReplayDataKind::Order)
                    } else {
                        None
                    }
                }
                (None, Some(transaction)) => {
                    if transaction.message_number < safe_seq_exclusive {
                        Some(ReplayDataKind::Transaction)
                    } else {
                        None
                    }
                }
                (None, None) => None,
            };

            let Some(next_kind) = next_kind else {
                break;
            };

            let next_message_number = match next_kind {
                ReplayDataKind::Order => {
                    self.order_buffer.front().map(|event| event.message_number)
                }
                ReplayDataKind::Transaction => self
                    .transaction_buffer
                    .front()
                    .map(|event| event.message_number),
            };

            if next_message_number.is_none_or(|message_number| message_number >= safe_seq_exclusive)
            {
                break;
            }

            match next_kind {
                ReplayDataKind::Order => {
                    if let Some(order) = self.order_buffer.pop_front() {
                        ready_events.push(ReplayEvent::Order(order));
                    }
                }
                ReplayDataKind::Transaction => {
                    if let Some(transaction) = self.transaction_buffer.pop_front() {
                        ready_events.push(ReplayEvent::Transaction(transaction));
                    }
                }
            }
        }

        ready_events
    }

    fn validate_batch_identity(&self, batch: &FetchedBatch) -> Result<()> {
        if batch.day != self.day {
            return Err(ChannelReplayLaneError::DayMismatch {
                expected: self.day.clone(),
                actual: batch.day.clone(),
            });
        }

        if batch.channel != self.channel {
            return Err(ChannelReplayLaneError::ChannelMismatch {
                expected: self.channel,
                actual: batch.channel,
            });
        }

        if !self.supports_data_kind(batch.data_kind) {
            return Err(ChannelReplayLaneError::DisabledDataKind {
                data_kind: batch.data_kind,
            });
        }

        Ok(())
    }

    fn supports_data_kind(&self, data_kind: ReplayDataKind) -> bool {
        match data_kind {
            ReplayDataKind::Order => self.has_order,
            ReplayDataKind::Transaction => self.has_transaction,
        }
    }

    fn push_order_batch(&mut self, batch: FetchedBatch) -> Result<()> {
        let end_message_number = batch.end_message_number;
        self.validate_batch_progress(
            ReplayDataKind::Order,
            batch.begin_message_number,
            self.order_covered_until,
        )?;

        let events = Self::collect_order_events(batch)?;
        self.order_buffer.extend(events);
        self.order_covered_until = Some(end_message_number);
        Ok(())
    }

    fn push_transaction_batch(&mut self, batch: FetchedBatch) -> Result<()> {
        let end_message_number = batch.end_message_number;
        self.validate_batch_progress(
            ReplayDataKind::Transaction,
            batch.begin_message_number,
            self.transaction_covered_until,
        )?;

        let events = Self::collect_transaction_events(batch)?;
        self.transaction_buffer.extend(events);
        self.transaction_covered_until = Some(end_message_number);
        Ok(())
    }

    fn validate_batch_progress(
        &self,
        data_kind: ReplayDataKind,
        begin_message_number: i64,
        previous_covered_until: Option<i64>,
    ) -> Result<()> {
        if let Some(previous_covered_until) = previous_covered_until {
            if begin_message_number < previous_covered_until {
                return Err(ChannelReplayLaneError::NonAdvancingRange {
                    data_kind,
                    begin_message_number,
                    previous_covered_until,
                });
            }
        }

        Ok(())
    }

    fn collect_order_events(batch: FetchedBatch) -> Result<Vec<L2Order>> {
        let mut previous_message_number = None;
        let mut events = Vec::with_capacity(batch.events.len());

        for event in batch.events {
            let ReplayEvent::Order(order) = event else {
                return Err(ChannelReplayLaneError::EventKindMismatch {
                    expected: ReplayDataKind::Order,
                });
            };

            Self::validate_event(
                ReplayDataKind::Order,
                batch.channel,
                batch.begin_message_number,
                batch.end_message_number,
                previous_message_number,
                order.channel,
                order.message_number,
            )?;

            previous_message_number = Some(order.message_number);
            events.push(order);
        }

        Ok(events)
    }

    fn collect_transaction_events(batch: FetchedBatch) -> Result<Vec<L2Transaction>> {
        let mut previous_message_number = None;
        let mut events = Vec::with_capacity(batch.events.len());

        for event in batch.events {
            let ReplayEvent::Transaction(transaction) = event else {
                return Err(ChannelReplayLaneError::EventKindMismatch {
                    expected: ReplayDataKind::Transaction,
                });
            };

            Self::validate_event(
                ReplayDataKind::Transaction,
                batch.channel,
                batch.begin_message_number,
                batch.end_message_number,
                previous_message_number,
                transaction.channel,
                transaction.message_number,
            )?;

            previous_message_number = Some(transaction.message_number);
            events.push(transaction);
        }

        Ok(events)
    }

    fn validate_event(
        data_kind: ReplayDataKind,
        expected_channel: i64,
        begin_message_number: i64,
        end_message_number: i64,
        previous_message_number: Option<i64>,
        actual_channel: i64,
        actual_message_number: i64,
    ) -> Result<()> {
        if actual_channel != expected_channel {
            return Err(ChannelReplayLaneError::EventChannelMismatch {
                expected: expected_channel,
                actual: actual_channel,
            });
        }

        if actual_message_number < begin_message_number
            || actual_message_number >= end_message_number
        {
            return Err(ChannelReplayLaneError::EventOutsideBatchRange {
                begin_message_number,
                end_message_number,
                actual: actual_message_number,
            });
        }

        if let Some(previous_message_number) = previous_message_number {
            if actual_message_number <= previous_message_number {
                return Err(ChannelReplayLaneError::NonMonotonicEvents {
                    data_kind,
                    previous: previous_message_number,
                    current: actual_message_number,
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ChannelReplayLane;
    use crate::common::{L2Order, L2Transaction, Market, OrderDirection, OrderType};
    use crate::replay::ReplayEvent;
    use crate::replay::db_reader::FetchedBatch;
    use crate::replay::reader_cursor::ReplayDataKind;

    fn order_event(channel: i64, channel_number: i64, timestamp_ms: i64) -> ReplayEvent {
        ReplayEvent::Order(L2Order {
            market: Market::XSHG,
            channel,
            message_number: channel_number,
            code: "600000.XSHG".to_string(),
            price: 100_000,
            volume: 100,
            direction: OrderDirection::Buy,
            order_type: OrderType::Limit,
            timestamp_ms,
            order_number: 1,
        })
    }

    fn transaction_event(channel: i64, channel_number: i64, timestamp_ms: i64) -> ReplayEvent {
        ReplayEvent::Transaction(L2Transaction {
            market: Market::XSHG,
            channel,
            message_number: channel_number,
            code: "600000.XSHG".to_string(),
            timestamp_ms,
            price: 100_000,
            volume: 100,
            buy_number: 1,
            sell_number: 2,
            deal_type: "0".to_string(),
        })
    }

    #[test]
    fn merges_order_and_transaction_by_message_number() {
        let mut lane = ChannelReplayLane::new("2026-05-12", 3, true, true).unwrap();

        lane.push_batch(FetchedBatch {
            data_kind: ReplayDataKind::Order,
            market: Market::XSHG,
            channel: 3,
            day: "2026-05-12".to_string(),
            begin_message_number: 100,
            end_message_number: 110,
            events: vec![order_event(3, 100, 1_000), order_event(3, 103, 1_003)],
        })
        .unwrap();

        assert!(lane.pop_ready_events().is_empty());

        lane.push_batch(FetchedBatch {
            data_kind: ReplayDataKind::Transaction,
            market: Market::Unknown,
            channel: 3,
            day: "2026-05-12".to_string(),
            begin_message_number: 100,
            end_message_number: 110,
            events: vec![
                transaction_event(3, 101, 1_001),
                transaction_event(3, 108, 1_008),
            ],
        })
        .unwrap();

        let ready_events = lane.pop_ready_events();
        let message_numbers: Vec<i64> = ready_events
            .iter()
            .map(|event| match event {
                ReplayEvent::Order(order) => order.message_number,
                ReplayEvent::Transaction(transaction) => transaction.message_number,
            })
            .collect();

        assert_eq!(message_numbers, vec![100, 101, 103, 108]);
    }

    #[test]
    fn supports_gaps_after_code_filtering() {
        let mut lane = ChannelReplayLane::new("2026-05-12", 3, false, true).unwrap();

        lane.push_batch(FetchedBatch {
            data_kind: ReplayDataKind::Transaction,
            market: Market::Unknown,
            channel: 3,
            day: "2026-05-12".to_string(),
            begin_message_number: 200,
            end_message_number: 210,
            events: vec![
                transaction_event(3, 201, 2_001),
                transaction_event(3, 208, 2_008),
            ],
        })
        .unwrap();

        let ready_events = lane.pop_ready_events();
        let message_numbers: Vec<i64> = ready_events
            .iter()
            .map(|event| match event {
                ReplayEvent::Transaction(transaction) => transaction.message_number,
                ReplayEvent::Order(_) => unreachable!(),
            })
            .collect();

        assert_eq!(message_numbers, vec![201, 208]);
        assert_eq!(lane.safe_message_number_exclusive(), Some(210));
    }

    #[test]
    fn waits_until_both_streams_have_coverage() {
        let mut lane = ChannelReplayLane::new("2026-05-12", 3, true, true).unwrap();

        lane.push_batch(FetchedBatch {
            data_kind: ReplayDataKind::Order,
            market: Market::XSHG,
            channel: 3,
            day: "2026-05-12".to_string(),
            begin_message_number: 300,
            end_message_number: 305,
            events: vec![order_event(3, 300, 3_000)],
        })
        .unwrap();

        assert_eq!(lane.safe_message_number_exclusive(), None);
        assert!(lane.pop_ready_events().is_empty());
    }

    #[test]
    fn releases_remaining_events_after_counterpart_stream_finishes() {
        let mut lane = ChannelReplayLane::new("2026-05-12", 3, true, true).unwrap();

        lane.push_batch(FetchedBatch {
            data_kind: ReplayDataKind::Order,
            market: Market::XSHG,
            channel: 3,
            day: "2026-05-12".to_string(),
            begin_message_number: 400,
            end_message_number: 410,
            events: vec![order_event(3, 400, 4_000), order_event(3, 408, 4_008)],
        })
        .unwrap();

        lane.push_batch(FetchedBatch {
            data_kind: ReplayDataKind::Transaction,
            market: Market::Unknown,
            channel: 3,
            day: "2026-05-12".to_string(),
            begin_message_number: 400,
            end_message_number: 405,
            events: vec![transaction_event(3, 401, 4_001)],
        })
        .unwrap();

        let ready_before_finish = lane.pop_ready_events();
        let message_numbers_before_finish: Vec<i64> = ready_before_finish
            .iter()
            .map(|event| match event {
                ReplayEvent::Order(order) => order.message_number,
                ReplayEvent::Transaction(transaction) => transaction.message_number,
            })
            .collect();
        assert_eq!(message_numbers_before_finish, vec![400, 401]);

        lane.mark_finished(ReplayDataKind::Transaction);

        let ready_after_finish = lane.pop_ready_events();
        let message_numbers_after_finish: Vec<i64> = ready_after_finish
            .iter()
            .map(|event| match event {
                ReplayEvent::Order(order) => order.message_number,
                ReplayEvent::Transaction(transaction) => transaction.message_number,
            })
            .collect();
        assert_eq!(message_numbers_after_finish, vec![408]);
    }
}
