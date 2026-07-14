//! replay 链路使用的统一事件类型。
//! `ReplayEvent` 包装逐笔委托和逐笔成交，并提供时间、市场、channel 和消息号等公共字段。
//! `SequencedReplayEvent` 在事件进入并行 worker 前附加本次回放的内部序号。
use super::reader_cursor::ReplayDataKind;
use crate::common::Market;
use crate::common::{L2Order, L2Transaction};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ReplayEvent {
    Order(L2Order),
    Transaction(L2Transaction),
}

#[derive(Debug, Clone)]
pub struct SequencedReplayEvent {
    pub sequence: u64,
    pub event: ReplayEvent,
}

impl SequencedReplayEvent {
    pub fn new(sequence: u64, event: ReplayEvent) -> Self {
        Self { sequence, event }
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.event.timestamp_ms()
    }
}

impl ReplayEvent {
    pub fn data_kind(&self) -> ReplayDataKind {
        match self {
            Self::Order(_) => ReplayDataKind::Order,
            Self::Transaction(_) => ReplayDataKind::Transaction,
        }
    }

    pub fn market(&self) -> Market {
        match self {
            Self::Order(order) => order.market,
            Self::Transaction(transaction) => transaction.market,
        }
    }

    pub fn channel(&self) -> i64 {
        match self {
            Self::Order(order) => order.channel,
            Self::Transaction(transaction) => transaction.channel,
        }
    }

    pub fn message_number(&self) -> i64 {
        match self {
            Self::Order(order) => order.message_number,
            Self::Transaction(transaction) => transaction.message_number,
        }
    }

    pub fn timestamp_ms(&self) -> i64 {
        match self {
            Self::Order(order) => order.timestamp_ms,
            Self::Transaction(transaction) => transaction.timestamp_ms,
        }
    }
}
