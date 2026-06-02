//!
//! replay统一事件定义模块。
//! 1. 输入：
//!    - 原始逐笔委托 `L2Order`
//!    - 原始逐笔成交 `L2Transaction`
//!
//! 2. 输出：
//!    - 统一后的 `ReplayEvent`
//!    - 上层可以不再区分底层来自哪张表，而是按统一接口读取时间、市场、channel 和 message number
//!
//! 3. 逻辑：
//!    - 为 replay 全链路提供统一事件类型
//!    - 统一暴露排序和调度所需的公共字段，例如 `timestamp_ms`、`message_number`
//!    - 让 lane / coordinator / handler 可以在不关心底层表结构差异的前提下处理事件
//!
use super::reader_cursor::ReplayDataKind;
use crate::common::Market;
use crate::common::{L2Order, L2Transaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayEvent {
    Order(L2Order),
    Transaction(L2Transaction),
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
