use clickhouse::{Row, sql::Identifier};
use serde::Deserialize;
use thiserror::Error;

use crate::common::{L2Transaction, Market};
use crate::db::dbpool::{DbPool, DbPoolError};

pub use super::order_query_common::ChannelMessageRange;
use super::order_query_common::{
    RawOrderMessageRange, build_message_range, validate_message_range, validate_time_range,
};

pub type Result<T> = std::result::Result<T, TransactionQueryError>;

#[derive(Debug, Error)]
pub enum TransactionQueryError {
    #[error("failed to acquire db client from pool")]
    AcquireClient(#[from] DbPoolError),
    #[error("invalid transaction time range: start_time_ms={start_time_ms}, end_time_ms={end_time_ms}")]
    InvalidTimeRange {
        start_time_ms: i64,
        end_time_ms: i64,
    },
    #[error("invalid transaction message range: begin_message_number={begin_message_number}, end_message_number={end_message_number}")]
    InvalidMessageRange {
        begin_message_number: i64,
        end_message_number: i64,
    },
    #[error("transaction message range overflow: channel={channel}, max_seq={max_seq}")]
    MessageRangeOverflow {
        channel: i64,
        max_seq: i64,
    },
    #[error("failed to execute clickhouse transaction query")]
    Query(#[source] clickhouse::error::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRangeQuery {
    pub day: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub table_name: String,
}

impl TransactionRangeQuery {
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
        validate_time_range(self.start_time_ms, self.end_time_ms, |start_time_ms, end_time_ms| {
            TransactionQueryError::InvalidTimeRange {
                start_time_ms,
                end_time_ms,
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionByRangeQuery {
    pub day: String,
    pub channel: i64,
    pub begin_message_number: i64,
    pub end_message_number: i64,
    pub codes: Vec<String>,
    pub table_name: String,
}

impl TransactionByRangeQuery {
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
            codes: Vec::new(),
            table_name: table_name.into(),
        }
    }

    pub fn with_codes<I, S>(mut self, codes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.codes = codes.into_iter().map(Into::into).collect();
        self
    }

    fn validate(&self) -> Result<()> {
        validate_message_range(
            self.begin_message_number,
            self.end_message_number,
            |begin_message_number, end_message_number| {
                TransactionQueryError::InvalidMessageRange {
                    begin_message_number,
                    end_message_number,
                }
            },
        )
    }
}

/// 查询指定时间窗口内，逐笔成交数据在各个 channel 上的消息号范围。
///
/// # 参数
/// - `pool`: ClickHouse 连接池，用于执行范围查询。
/// - `query`: 查询条件，包含交易日、时间窗口以及表名。
///
/// # 返回
/// - `Ok(Vec<ChannelMessageRange>)`: 每个活跃 channel 一条消息号范围记录，结果按 `channel` 升序返回。
/// - `Err(TransactionQueryError)`: 查询参数非法、连接池取连接失败或 ClickHouse 查询失败时返回错误。
///
/// # 注意事项
/// - 时间窗口语义为 `[start_time_ms, end_time_ms)`，也就是左闭右开。
/// - 返回的 `ChannelMessageRange` 也采用半开区间 `[begin_message_number, end_message_number)`。
/// - `end_message_number` 是排他上界，等于该 channel 在窗口内最后一条消息号加一，后续按批次读取时不会重复读取尾部消息。
pub async fn query_transaction_message_ranges(
    pool: &DbPool,
    query: &TransactionRangeQuery,
) -> Result<Vec<ChannelMessageRange>> {
    query.validate()?;

    let client = pool.get_one().await?;
    let sql = r#"
        SELECT
            MIN(transaction_number) AS min_seq,
            MAX(transaction_number) AS max_seq,
            channel_id AS channel
        FROM ?
        WHERE EventDate = toDate(?)
          AND deal_time >= fromUnixTimestamp64Milli(?)
          AND deal_time < fromUnixTimestamp64Milli(?)
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
        .map_err(TransactionQueryError::Query)?;

    rows.into_iter()
        .map(|row| {
            build_message_range(row, |channel, max_seq| {
                TransactionQueryError::MessageRangeOverflow { channel, max_seq }
            })
        })
        .collect()
}

/// 查询指定 message range 内，某个 channel 的逐笔成交明细。
///
/// # 参数
/// - `pool`: ClickHouse 连接池，用于执行明细查询。
/// - `query`: 查询条件，包含交易日、channel、消息号范围、表名以及可选的代码过滤列表。
///
/// # 返回
/// - `Ok(Vec<L2Transaction>)`: 该 channel 在指定消息号范围内的逐笔成交明细，结果按 `transaction_number` 升序返回。
/// - `Err(TransactionQueryError)`: 查询参数非法、连接池取连接失败或 ClickHouse 查询失败时返回错误。
///
/// # 注意事项
/// - 消息号范围语义为 `[begin_message_number, end_message_number)`，也就是左闭右开。
/// - 结果按 `transaction_number` 排序，适合直接用于后续按 channel 的顺序回放。
/// - 如果设置了 `codes` 过滤条件，返回结果允许出现消息号缺口，但顺序保持不变。
/// - 返回的 `L2Transaction.channel_number` 对应原始 `transaction_number` 字段。
pub async fn query_transactions_by_range(
    pool: &DbPool,
    query: &TransactionByRangeQuery,
) -> Result<Vec<L2Transaction>> {
    query.validate()?;

    let client = pool.get_one().await?;
    let mut sql = String::from(
        r#"
        SELECT
            channel_id AS channel,
            transaction_number AS message_number,
            code,
            toUnixTimestamp64Milli(deal_time) AS timestamp_ms,
            toInt64(deal_price * 10000) AS price,
            toInt64(deal_volume) AS volume,
            buy_syh,
            sell_syh,
            toString(deal_type) AS deal_type
        FROM ?
        WHERE EventDate = toDate(?)
          AND transaction_number >= ?
          AND transaction_number < ?
          AND channel_id = ?
    "#,
    );

    if !query.codes.is_empty() {
        sql.push_str(" AND code IN (");
        for index in 0..query.codes.len() {
            if index > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push(')');
    }

    sql.push_str(" ORDER BY transaction_number");

    let mut db_query = client
        .query(&sql)
        .bind(Identifier(&query.table_name))
        .bind(&query.day)
        .bind(query.begin_message_number)
        .bind(query.end_message_number)
        .bind(query.channel);

    for code in &query.codes {
        db_query = db_query.bind(code);
    }

    let rows = db_query
        .fetch_all::<RawTransaction>()
        .await
        .map_err(TransactionQueryError::Query)?;

    Ok(rows.into_iter().map(L2Transaction::from).collect())
}

#[derive(Debug, Row, Deserialize)]
struct RawTransaction {
    channel: i64,
    message_number: i64,
    code: String,
    timestamp_ms: i64,
    price: i64,
    volume: i64,
    buy_syh: i64,
    sell_syh: i64,
    deal_type: String,
}

impl From<RawTransaction> for L2Transaction {
    fn from(value: RawTransaction) -> Self {
        Self {
            market: normalize_transaction_market(&value.code),
            channel: value.channel,
            channel_number: value.message_number,
            code: value.code,
            timestamp_ms: value.timestamp_ms,
            price: value.price,
            volume: value.volume,
            buy_order_number: value.buy_syh,
            sell_order_number: value.sell_syh,
            deal_type: value.deal_type,
        }
    }
}

fn normalize_transaction_market(code: &str) -> Market {
    match () {
        _ if code.starts_with("SH") => Market::XSHG,
        _ if code.starts_with("SZ") => Market::XSHE,
        _ => Market::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::{RawTransaction, normalize_transaction_market};
    use crate::common::Market;

    #[test]
    fn normalizes_transaction_values() {
        let tx = crate::common::L2Transaction::from(RawTransaction {
            channel: 3,
            message_number: 674_296,
            code: "SH600588".to_string(),
            timestamp_ms: 1_700_000_000_123,
            price: 123_450,
            volume: 900,
            buy_syh: 1001,
            sell_syh: 1002,
            deal_type: "0".to_string(),
        });

        assert_eq!(tx.market, Market::XSHG);
        assert_eq!(tx.channel, 3);
        assert_eq!(tx.channel_number, 674_296);
        assert_eq!(tx.buy_order_number, 1001);
        assert_eq!(tx.sell_order_number, 1002);
        assert_eq!(tx.code, "SH600588");
    }
}
