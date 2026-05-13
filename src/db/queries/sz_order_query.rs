use clickhouse::{Row, sql::Identifier};
use serde::Deserialize;
use thiserror::Error;

use crate::common::{L2Order, Market, OrderDirection, OrderType};
use crate::db::dbpool::{DbPool, DbPoolError};

pub use super::order_query_common::ChannelMessageRange;
use super::order_query_common::{
    RawOrderMessageRange, build_right_open_message_range, validate_message_range,
    validate_time_range,
};

const DEFAULT_SZ_ORDER_TABLE: &str = "L2_order_rt_distributed";

pub type Result<T> = std::result::Result<T, SZOrderQueryError>;

#[derive(Debug, Error)]
pub enum SZOrderQueryError {
    #[error("failed to acquire db client from pool")]
    AcquireClient(#[from] DbPoolError),
    #[error("invalid shenzhen order time range: start_time_ms={start_time_ms}, end_time_ms={end_time_ms}")]
    InvalidTimeRange {
        start_time_ms: i64,
        end_time_ms: i64,
    },
    #[error("invalid shenzhen order message range: begin_message_number={begin_message_number}, end_message_number={end_message_number}")]
    InvalidMessageRange {
        begin_message_number: i64,
        end_message_number: i64,
    },
    #[error("message number overflow while converting channel {channel} max_seq={max_seq} into right-open range end")]
    MessageNumberOverflow { channel: i64, max_seq: i64 },
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
    pub fn new(day: impl Into<String>, start_time_ms: i64, end_time_ms: i64) -> Self {
        Self {
            day: day.into(),
            start_time_ms,
            end_time_ms,
            table_name: DEFAULT_SZ_ORDER_TABLE.to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    fn validate(&self) -> Result<()> {
        validate_time_range(self.start_time_ms, self.end_time_ms, |start_time_ms, end_time_ms| {
            SZOrderQueryError::InvalidTimeRange {
                start_time_ms,
                end_time_ms,
            }
        })
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
    ) -> Self {
        Self {
            day: day.into(),
            channel,
            begin_message_number,
            end_message_number,
            table_name: DEFAULT_SZ_ORDER_TABLE.to_string(),
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
            |begin_message_number, end_message_number| SZOrderQueryError::InvalidMessageRange {
                begin_message_number,
                end_message_number,
            },
        )
    }
}

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
            build_right_open_message_range(row, |channel, max_seq| {
                SZOrderQueryError::MessageNumberOverflow { channel, max_seq }
            })
        })
        .collect()
}

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
    use super::{
        RawSZOrder, SZOrderByRangeQuery, SZOrderQueryError, SZOrderRangeQuery,
        normalize_sz_order_direction,
        normalize_sz_order_type,
    };
    use crate::common::{Market, OrderDirection, OrderType};
    use crate::db::queries::order_query_common::{
        RawOrderMessageRange, build_right_open_message_range,
    };

    #[test]
    fn converts_range_into_right_open_interval() {
        let raw = RawOrderMessageRange {
            min_seq: 10,
            max_seq: 15,
            channel: 7,
        };

        let range = build_right_open_message_range(raw, |channel, max_seq| {
            SZOrderQueryError::MessageNumberOverflow { channel, max_seq }
        })
        .unwrap();

        assert_eq!(range.channel, 7);
        assert_eq!(range.begin_message_number, 10);
        assert_eq!(range.end_message_number, 16);
    }

    #[test]
    fn rejects_overflow_when_expanding_range_end() {
        let raw = RawOrderMessageRange {
            min_seq: 10,
            max_seq: i64::MAX,
            channel: 7,
        };

        let err = build_right_open_message_range(raw, |channel, max_seq| {
            SZOrderQueryError::MessageNumberOverflow { channel, max_seq }
        })
        .unwrap_err();

        assert!(matches!(
            err,
            SZOrderQueryError::MessageNumberOverflow {
                channel: 7,
                max_seq: i64::MAX
            }
        ));
    }

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

    #[test]
    fn maps_unknown_direction_and_order_type_to_unknown() {
        assert_eq!(
            normalize_sz_order_direction(9),
            OrderDirection::Unknown
        );
        assert_eq!(normalize_sz_order_type("X"), OrderType::Unknown);
    }

    #[test]
    fn validates_time_query_range() {
        let err = SZOrderRangeQuery::new("2026-05-12", 20, 20)
            .validate()
            .unwrap_err();

        assert!(matches!(
            err,
            SZOrderQueryError::InvalidTimeRange {
                start_time_ms: 20,
                end_time_ms: 20
            }
        ));
    }

    #[test]
    fn validates_message_query_range() {
        let err = SZOrderByRangeQuery::new("2026-05-12", 1, 10, 10)
            .validate()
            .unwrap_err();

        assert!(matches!(
            err,
            SZOrderQueryError::InvalidMessageRange {
                begin_message_number: 10,
                end_message_number: 10
            }
        ));
    }
}
