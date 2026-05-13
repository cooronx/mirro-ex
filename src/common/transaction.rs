use serde::{Deserialize, Serialize};

use crate::common::Market;


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct L2Transaction {
    pub market: Market,
    pub channel: i64,
    pub channel_number: i64,
    pub code: String,
    pub timestamp_ms: i64,
    pub price: i64,
    pub volume: i64,
    pub buy_order_number: i64,
    pub sell_order_number: i64,
    pub deal_type: String,
}
