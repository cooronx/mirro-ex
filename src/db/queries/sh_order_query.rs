use clickhouse::{Row, sql::Identifier};
use serde::Deserialize;
use thiserror::Error;

use crate::common::{L2Order, Market, OrderDirection, OrderType};
use crate::db::dbpool::{DbPool, DbPoolError};

pub use super::order_query_common::ChannelMessageRange;
use super::order_query_common::{
    RawOrderMessageRange, build_message_range, validate_message_range,
    validate_time_range,
};

const DEFAULT_SH_ORDER_TABLE: &str = "L2_shorder_rt_distributed";

pub type Result<T> = std::result::Result<T, SHOrderQueryError>;

#[derive(Debug, Error)]
pub enum SHOrderQueryError {
    #[error("failed to acquire db client from pool")]
    AcquireClient(#[from] DbPoolError),
    #[error("invalid shanghai order time range: start_time_ms={start_time_ms}, end_time_ms={end_time_ms}")]
    InvalidTimeRange {
        start_time_ms: i64,
        end_time_ms: i64,
    },
    #[error("invalid shanghai order message range: begin_message_number={begin_message_number}, end_message_number={end_message_number}")]
    InvalidMessageRange {
        begin_message_number: i64,
        end_message_number: i64,
    },
    #[error("shanghai order message range overflow: channel={channel}, max_seq={max_seq}")]
    MessageRangeOverflow {
        channel: i64,
        max_seq: i64,
    },
    #[error("failed to execute clickhouse order query")]
    Query(#[source] clickhouse::error::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SHOrderRangeQuery {
    pub day: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub table_name: String,
}

impl SHOrderRangeQuery {
    pub fn new(day: impl Into<String>, start_time_ms: i64, end_time_ms: i64) -> Self {
        Self {
            day: day.into(),
            start_time_ms,
            end_time_ms,
            table_name: DEFAULT_SH_ORDER_TABLE.to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    fn validate(&self) -> Result<()> {
        validate_time_range(self.start_time_ms, self.end_time_ms, |start_time_ms, end_time_ms| {
            SHOrderQueryError::InvalidTimeRange {
                start_time_ms,
                end_time_ms,
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SHOrderByRangeQuery {
    pub day: String,
    pub channel: i64,
    pub begin_message_number: i64,
    pub end_message_number: i64,
    pub table_name: String,
}

impl SHOrderByRangeQuery {
    pub fn new(
        day: impl Into<String>,
        channel: i64,
        begin_message_number: i64,
        end_message_number: i64,
    ) -> Self {
        Self {
            day: day.into(),
            channel,
            begin_message_number,
            end_message_number,
            table_name: DEFAULT_SH_ORDER_TABLE.to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    fn validate(&self) -> Result<()> {
        validate_message_range(
            self.begin_message_number,
            self.end_message_number,
            |begin_message_number, end_message_number| SHOrderQueryError::InvalidMessageRange {
                begin_message_number,
                end_message_number,
            },
        )
    }
}

pub async fn query_sh_order_message_ranges(
    pool: &DbPool,
    query: &SHOrderRangeQuery,
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
          AND time >= fromUnixTimestamp64Milli(?)
          AND time < fromUnixTimestamp64Milli(?)
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
        .map_err(SHOrderQueryError::Query)?;

    rows.into_iter()
        .map(|row| {
            build_message_range(row, |channel, max_seq| {
                SHOrderQueryError::MessageRangeOverflow { channel, max_seq }
            })
        })
        .collect()
}

pub async fn query_sh_orders_by_range(
    pool: &DbPool,
    query: &SHOrderByRangeQuery,
) -> Result<Vec<L2Order>> {
    query.validate()?;

    let client = pool.get_one().await?;
    let sql = r#"
        SELECT
            channel,
            message_number,
            code,
            toUnixTimestamp64Milli(time) AS timestamp_ms,
            toInt64(price * 10000) AS price,
            toInt64(volume) AS volume,
            bs_flag,
            order_type,
            order_number
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
        .fetch_all::<RawSHOrder>()
        .await
        .map_err(SHOrderQueryError::Query)?;

    Ok(rows.into_iter().map(L2Order::from).collect())
}

#[derive(Debug, Row, Deserialize)]
struct RawSHOrder {
    channel: i64,
    message_number: i64,
    code: String,
    timestamp_ms: i64,
    price: i64,
    volume: i64,
    bs_flag: i8,
    order_type: i8,
    order_number: i64,
}

impl From<RawSHOrder> for L2Order {
    fn from(value: RawSHOrder) -> Self {
        Self {
            market: Market::XSHG,
            channel: value.channel,
            channel_number: value.message_number,
            code: value.code,
            price: value.price,
            volume: value.volume,
            direction: normalize_sh_order_direction(value.bs_flag),
            order_type: normalize_sh_order_type(value.order_type),
            timestamp_ms: value.timestamp_ms,
            extra_message_number: value.order_number,
        }
    }
}

fn normalize_sh_order_direction(bs_flag: i8) -> OrderDirection {
    match bs_flag {
        2 => OrderDirection::Buy,
        3 => OrderDirection::Sell,
        _ => OrderDirection::Unknown,
    }
}

fn normalize_sh_order_type(order_type: i8) -> OrderType {
    match order_type {
        0 => OrderType::Limit,
        1 => OrderType::Cancel,
        _ => OrderType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RawSHOrder,
        normalize_sh_order_direction, normalize_sh_order_type,
    };
    use crate::common::{Market, OrderDirection, OrderType};

    #[test]
    fn normalizes_sh_order_values() {
        let order = crate::common::L2Order::from(RawSHOrder {
            channel: 3,
            message_number: 668_434,
            code: "600000".to_string(),
            timestamp_ms: 1_700_000_000_123,
            price: 123_450,
            volume: 900,
            bs_flag: 2,
            order_type: 1,
            order_number: 88,
        });

        assert_eq!(order.market, Market::XSHG);
        assert_eq!(order.channel_number, 668_434);
        assert_eq!(order.direction, OrderDirection::Buy);
        assert_eq!(order.order_type, OrderType::Cancel);
        assert_eq!(order.extra_message_number, 88);
    }

    #[test]
    fn maps_unknown_sh_values_to_unknown() {
        assert_eq!(normalize_sh_order_direction(1), OrderDirection::Unknown);
        assert_eq!(normalize_sh_order_type(9), OrderType::Unknown);
    }
}
