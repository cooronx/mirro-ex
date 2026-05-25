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

    pub fn channel_number(&self) -> i64 {
        match self {
            Self::Order(order) => order.channel_number,
            Self::Transaction(transaction) => transaction.channel_number,
        }
    }

    pub fn timestamp_ms(&self) -> i64 {
        match self {
            Self::Order(order) => order.timestamp_ms,
            Self::Transaction(transaction) => transaction.timestamp_ms,
        }
    }
}
