use serde::{Deserialize, Serialize};

/// 交易所枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Market {
    /// 上交所
    XSHG,
    /// 深交所
    XSHE,
    /// 未知市场
    #[default]
    Unknown,
}

/// 委托方向枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OrderDirection {
    /// 买入
    Buy,
    /// 卖出
    Sell,
    /// 未知方向
    #[default]
    Unknown,
}

/// 委托类型枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OrderType {
    /// 限价
    Limit,
    /// 市价
    Market,
    /// 本方最优（深交所独有）
    BestOwn,
    /// 撤单
    Cancel,
    /// 未知类型
    #[default]
    Unknown,
}

/// 统一的逐笔委托结构体。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct L2Order {
    /// 交易所
    pub market: Market,
    /// 频道号
    pub channel: i64,
    /// 频道内消息序号，在同一个 channel 里面的 order 和 transaction 都共用，从 1 开始递增
    pub message_number: i64,
    /// 标的代码
    pub code: String,
    /// 委托价格（被放大了 10000 倍，四位小数）
    pub price: i64,
    /// 委托量（强制转换为了整数）
    pub volume: i64,
    /// 买卖方向
    pub direction: OrderDirection,
    /// 委托类型
    pub order_type: OrderType,
    /// Unix 毫秒时间戳
    pub timestamp_ms: i64,
    /// 原始委托单号。深市当前数据固定为 0，沪市使用真实委托单号。
    pub order_number: i64,
}
