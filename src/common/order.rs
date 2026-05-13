use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Market {
    XSHG,
    XSHE,
    #[default]
    Unknown,
}

// Unified order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OrderDirection {
    Buy,
    Sell,
    #[default]
    Unknown,
}

// Unified order type across Shanghai/Shenzhen feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OrderType {
    Limit,
    Market,
    BestOwn,
    Cancel,
    #[default]
    Unknown,
}


/**
 * 统一的逐笔order结构体
 * `price` 被放大了10000倍（四位小数），`volume` 强制转换为整数
 * `timestamp_ms` unix毫秒时间戳
 * `channel_number` 序列号，分频道，每个频道内递增
 */
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct L2Order {
    pub market: Market,
    pub channel: i64,
    pub channel_number: i64,
    pub code: String,
    pub price: i64,
    pub volume: i64,
    pub direction: OrderDirection,
    pub order_type: OrderType,
    pub timestamp_ms: i64,
    pub extra_message_number: i64,
}
