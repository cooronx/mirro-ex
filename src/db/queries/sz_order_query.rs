use clickhouse::{Row, sql::Identifier};
use serde::Deserialize;
use thiserror::Error;

use crate::common::{L2Order, Market, OrderDirection, OrderType};
use crate::db::dbpool::{DbPool, DbPoolError};

pub use super::order_query_common::ChannelMessageRange;
use super::order_query_common::{
    RawOrderMessageRange, build_message_range, validate_message_range, validate_time_range,
};

pub type Result<T> = std::result::Result<T, SZOrderQueryError>;

#[derive(Debug, Error)]
pub enum SZOrderQueryError {
    #[error("failed to acquire db client from pool")]
    AcquireClient(#[from] DbPoolError),
    #[error(
        "invalid shenzhen order time range: start_time_ms={start_time_ms}, end_time_ms={end_time_ms}"
    )]
    InvalidTimeRange {
        start_time_ms: i64,
        end_time_ms: i64,
    },
    #[error(
        "invalid shenzhen order message range: begin_message_number={begin_message_number}, end_message_number={end_message_number}"
    )]
    InvalidMessageRange {
        begin_message_number: i64,
        end_message_number: i64,
    },
    #[error("shenzhen order message range overflow: channel={channel}, max_seq={max_seq}")]
    MessageRangeOverflow { channel: i64, max_seq: i64 },
    #[error("failed to execute clickhouse order query")]
    Query(#[source] clickhouse::error::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SZOrderRangeQuery {
    pub day: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub table_name: String,
}

impl SZOrderRangeQuery {
    pub fn new(
        day: impl Into<String>,
        start_time_ms: i64,
        end_time_ms: i64,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            day: day.into(),
            start_time_ms,
            end_time_ms,
            table_name: table_name.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_time_range(
            self.start_time_ms,
            self.end_time_ms,
            |start_time_ms, end_time_ms| SZOrderQueryError::InvalidTimeRange {
                start_time_ms,
                end_time_ms,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SZOrderByRangeQuery {
    pub day: String,
    pub channel: i64,
    pub begin_message_number: i64,
    pub end_message_number: i64,
    pub table_name: String,
}

impl SZOrderByRangeQuery {
    pub fn new(
        day: impl Into<String>,
        channel: i64,
        begin_message_number: i64,
        end_message_number: i64,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            day: day.into(),
            channel,
            begin_message_number,
            end_message_number,
            table_name: table_name.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_message_range(
            self.begin_message_number,
            self.end_message_number,
            |begin_message_number, end_message_number| SZOrderQueryError::InvalidMessageRange {
                begin_message_number,
                end_message_number,
            },
        )
    }
}

/// 查询指定时间窗口内，深市逐笔委托数据在各个 channel 上的消息号范围。
///
/// # 参数
/// - `pool`: ClickHouse 连接池，用于执行范围查询。
/// - `query`: 查询条件，包含交易日、时间窗口以及表名。
///
/// # 返回
/// - `Ok(Vec<ChannelMessageRange>)`: 每个活跃 channel 一条消息号范围记录，结果按 `channel` 升序返回。
/// - `Err(SZOrderQueryError)`: 查询参数非法、连接池取连接失败或 ClickHouse 查询失败时返回错误。
///
/// # 注意事项
/// - 时间窗口语义为 `[start_time_ms, end_time_ms)`，也就是左闭右开。
/// - 返回的 `ChannelMessageRange` 也采用半开区间 `[begin_message_number, end_message_number)`。
/// - `end_message_number` 是排他上界，等于该 channel 在窗口内最后一条消息号加一，后续按批次读取时不会重复读取尾部消息。
pub async fn query_sz_order_message_ranges(
    pool: &DbPool,
    query: &SZOrderRangeQuery,
) -> Result<Vec<ChannelMessageRange>> {
    query.validate()?;

    let client = pool.get_one().await?;
    let sql = r#"
        SELECT
            MIN(message_number) AS min_seq,
            MAX(message_number) AS max_seq,
            channel
        FROM ?
        WHERE EventDate = toDate(?)
          AND commision_time >= fromUnixTimestamp64Milli(?)
          AND commision_time < fromUnixTimestamp64Milli(?)
        GROUP BY channel
        ORDER BY channel
    "#;

    let rows = client
        .query(sql)
        .bind(Identifier(&query.table_name))
        .bind(&query.day)
        .bind(query.start_time_ms)
        .bind(query.end_time_ms)
        .fetch_all::<RawOrderMessageRange>()
        .await
        .map_err(SZOrderQueryError::Query)?;

    rows.into_iter()
        .map(|row| {
            build_message_range(row, |channel, max_seq| {
                SZOrderQueryError::MessageRangeOverflow { channel, max_seq }
            })
        })
        .collect()
}

/// 查询指定 message range 内，深市某个 channel 的逐笔委托明细。
///
/// # 参数
/// - `pool`: ClickHouse 连接池，用于执行明细查询。
/// - `query`: 查询条件，包含交易日、channel、消息号范围以及表名。
///
/// # 返回
/// - `Ok(Vec<L2Order>)`: 该 channel 在指定消息号范围内的逐笔委托明细，结果按 `message_number` 升序返回。
/// - `Err(SZOrderQueryError)`: 查询参数非法、连接池取连接失败或 ClickHouse 查询失败时返回错误。
///
/// # 注意事项
/// - 消息号范围语义为 `[begin_message_number, end_message_number)`，也就是左闭右开。
/// - 结果按 `message_number` 排序，适合直接用于后续按 channel 的顺序回放。
/// - 返回的 `L2Order.channel_number` 对应原始 `message_number` 字段。
pub async fn query_sz_orders_by_range(
    pool: &DbPool,
    query: &SZOrderByRangeQuery,
) -> Result<Vec<L2Order>> {
    query.validate()?;

    let client = pool.get_one().await?;
    let sql = r#"
        SELECT
            channel,
            message_number,
            code,
            toUnixTimestamp64Milli(commision_time) AS timestamp_ms,
            toInt64(commission_price * 10000) AS price,
            toInt64(commission_volume) AS volume,
            direction,
            toString(order_type) AS order_type,
            extra_message_number
        FROM ?
        WHERE EventDate = toDate(?)
          AND message_number >= ?
          AND message_number < ?
          AND channel = ?
        ORDER BY message_number
    "#;

    let rows = client
        .query(sql)
        .bind(Identifier(&query.table_name))
        .bind(&query.day)
        .bind(query.begin_message_number)
        .bind(query.end_message_number)
        .bind(query.channel)
        .fetch_all::<RawSZOrder>()
        .await
        .map_err(SZOrderQueryError::Query)?;

    Ok(rows.into_iter().map(L2Order::from).collect())
}

#[derive(Debug, Row, Deserialize)]
struct RawSZOrder {
    channel: i64,
    message_number: i64,
    code: String,
    timestamp_ms: i64,
    price: i64,
    volume: i64,
    direction: i8,
    order_type: String,
    extra_message_number: i64,
}

impl From<RawSZOrder> for L2Order {
    fn from(value: RawSZOrder) -> Self {
        Self {
            market: Market::XSHE,
            channel: value.channel,
            channel_number: value.message_number,
            code: value.code,
            price: value.price,
            volume: value.volume,
            direction: normalize_sz_order_direction(value.direction),
            order_type: normalize_sz_order_type(&value.order_type),
            timestamp_ms: value.timestamp_ms,
            extra_message_number: value.extra_message_number,
        }
    }
}

fn normalize_sz_order_direction(direction: i8) -> OrderDirection {
    match direction {
        1 => OrderDirection::Buy,
        2 => OrderDirection::Sell,
        _ => OrderDirection::Unknown,
    }
}

fn normalize_sz_order_type(order_type: &str) -> OrderType {
    match order_type.trim() {
        "1" => OrderType::Market,
        "2" => OrderType::Limit,
        "U" | "u" => OrderType::BestOwn,
        _ => OrderType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::{RawSZOrder, normalize_sz_order_direction, normalize_sz_order_type};
    use crate::common::{Market, OrderDirection, OrderType};

    #[test]
    fn normalizes_sz_order_values() {
        let order = crate::common::L2Order::from(RawSZOrder {
            channel: 12,
            message_number: 34,
            code: "000001".to_string(),
            timestamp_ms: 1_700_000_000_123,
            price: 123_450,
            volume: 900,
            direction: 1,
            order_type: "U".to_string(),
            extra_message_number: 88,
        });

        assert_eq!(order.market, Market::XSHE);
        assert_eq!(order.channel_number, 34);
        assert_eq!(order.direction, OrderDirection::Buy);
        assert_eq!(order.order_type, OrderType::BestOwn);
    }
}
