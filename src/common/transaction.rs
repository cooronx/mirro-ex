use serde::{Deserialize, Serialize};

use crate::common::Market;

/// 统一的逐笔成交结构体
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct L2Transaction {
    /// 交易所
    pub market: Market,
    /// 频道号
    pub channel: i64,
    /// 频道内序号，在同一个channel里面的order和transaction都共用，从1开始递增
    pub channel_number: i64,
    /// 标的代码
    pub code: String,
    /// unix毫秒时间戳
    pub timestamp_ms: i64,
    /// 成交价格（被放大了 10000 倍（四位小数））
    pub price: i64,
    /// 成交量 (强制转换为了整数)
    pub volume: i64,
    /// 对应买入order的channel_number (如果为撤单，则为0)
    pub buy_order_number: i64,
    /// 对应卖出order的channel_number (如果为撤单，则为0)
    pub sell_order_number: i64,
    /// 交易所原始成交类型枚举值 (深交所: F = 成交，4 = 撤单，注意上交所的撤单是放在委托表里面的,上交所: B = 主动买入，S = 主动卖出，N = 未知)
    /// 其实主动买入主动卖出的数据完全可以自己推算，也就是buy_order_number > sell_order_number ---> 主动买入,反之为主动卖出
    pub deal_type: String,
}
