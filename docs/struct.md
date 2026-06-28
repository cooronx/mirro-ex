# 模拟撮合原理

# 项目中的结构

```jsx
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
```

```jsx
/// 逐笔成交结构体
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct L2Transaction {
    /// 交易所
    pub market: Market,
    /// 频道号
    pub channel: i64,
    /// 频道内消息序号，在同一个channel里面的order和transaction都共用，从1开始递增
    pub message_number: i64,
    /// 标的代码
    pub code: String,
    /// unix毫秒时间戳
    pub timestamp_ms: i64,
    /// 成交价格（被放大了 10000 倍（四位小数））
    pub price: i64,
    /// 成交量 (强制转换为了整数)
    pub volume: i64,
    /// 对应买入委托单号 (如果为撤单，则为0)
    pub buy_number: i64,
    /// 对应卖出委托单号 (如果为撤单，则为0)
    pub sell_number: i64,
    /// 交易所原始成交类型枚举值 (深交所: F = 成交，4 = 撤单，注意上交所的撤单是放在委托表里面的,上交所: B = 主动买入，S = 主动卖出，N = 未知)
    /// 其实主动买入主动卖出的数据完全可以自己推算，也就是buy_number > sell_number ---> 主动买入,反之为主动卖出
    pub deal_type: String,
}
```

```jsx
/// 逐笔委托结构体。
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
```
这是项目中使用的数据结构，基本上字段定义和交易所的保持一致

## 沪深交易所逐笔数据之间的区别

<aside>
📖

上交所逐笔委托数据中的委托数量代表着**委托报入立即成交后剩余的数量**~~（为什么要这样设计我请问）~~

</aside>

这句话实际上是在说：订单发送到的撮合平台能**立即成交**，不需要在订单簿队列中排队，比如 市价单，限价单的主动买入（大于等于当前盘口的卖一价的买入）或主动卖出（小于等于买一价的卖出）

这会导致什么问题呢？这会导致一个很奇怪的逻辑

1. 如果**立即全部成交**，那么没有委托回报，只有成交回报，**此时成交回报中的成交量就是原始委托的委托量**
2. 如果**立即部分成交**，那么会先返回一个成交回报，然后再返回一个委托回报，**此时委托回报的委托量加上成交回报的成交量就是原始委托的委托量**（太神人了）
3. 如果没有立即部分成交，那么会返回委托回报，没有成交回报（等到后续撮合成交后才会推送成交回报），此时**委托回报的委托数量为原始委托的委托量**

这个逻辑还会导致我们回放的时候的一个问题：

若主动买（买方订单号>卖方订单号）或deal_type='B'，将**无法在逐笔委托中查找到买方的原始委托**，

若主动卖（买方订单号< 卖方订单号）或deal_type = 'S'，将**无法在逐笔委托中查找到卖方的原始委托**