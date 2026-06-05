//!
//! 数据库批量读取模块。
//! 1. 输入：
//!    - ClickHouse 连接池 `DbPool`
//!    - 各市场各表的 message range 查询结果
//!    - 每条 source 当前的读取游标 `ReaderCursor`
//!    - 批大小 `batch_size`
//!
//! 2. 输出：
//!    - 若干个 `FetchedBatch`
//!    - 每个 batch 对应一个 source 在某个 message range 内的统一事件列表
//!
//! 3. 逻辑：
//!    - 把查询阶段得到的 channel/message range 转成 `ReaderCursor`
//!    - 维护每条 source 当前已经读取到哪里
//!    - 按 round-robin 方式为多个 cursor 规划下一批读取任务
//!    - 从 ClickHouse 拉取委托/成交明细，并统一封装成 `ReplayEvent`
//!
use clickhouse::{Row, sql::Identifier};
use serde::Deserialize;
use thiserror::Error;
use tokio::task::JoinError;

use crate::common::{L2Order, Market};
use crate::db::dbpool::{DbPool, DbPoolError};
use crate::db::queries::sh_order_query::{
    SHOrderByRangeQuery, SHOrderQueryError, SHOrderRangeQuery, query_sh_order_message_ranges,
    query_sh_orders_by_range,
};
use crate::db::queries::sz_order_query::{
    SZOrderByRangeQuery, SZOrderQueryError, SZOrderRangeQuery, query_sz_order_message_ranges,
    query_sz_orders_by_range,
};
use crate::db::queries::transaction_query::{
    TransactionByRangeQuery, TransactionQueryError, TransactionRangeQuery,
    query_transaction_message_ranges, query_transactions_by_range,
};

use super::event::ReplayEvent;
use super::reader_cursor::{ChannelRange, ReaderCursor, ReplayDataKind};

pub type Result<T> = std::result::Result<T, ReplayDbReaderError>;

#[derive(Debug, Error)]
pub enum ReplayDbReaderError {
    #[error("batch_size must be greater than 0")]
    InvalidBatchSize,
    #[error("max_batches must be greater than 0")]
    InvalidMaxBatches,
    #[error("failed to acquire db client from pool")]
    AcquireClient(#[from] DbPoolError),
    #[error("sh order query failed")]
    SHOrderQuery(#[from] SHOrderQueryError),
    #[error("sz order query failed")]
    SZOrderQuery(#[from] SZOrderQueryError),
    #[error("transaction query failed")]
    TransactionQuery(#[from] TransactionQueryError),
    #[error("failed to join replay batch task")]
    JoinTask(#[source] JoinError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBatch {
    pub data_kind: ReplayDataKind,
    pub market: Market,
    pub channel: i64,
    pub day: String,
    pub begin_message_number: i64,
    pub end_message_number: i64,
    pub events: Vec<ReplayEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CursorBatchSpec {
    cursor_index: usize,
    range: ChannelRange,
    begin_message_number: i64,
    end_message_number: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Row, Deserialize)]
struct RawNextTimestamp {
    timestamp_ms: i64,
}

/// 数据库重放器
#[derive(Clone)]
pub struct ReplayDbReader {
    pool: DbPool,
    batch_size: i64,
    cursors: Vec<ReaderCursor>,
    /// 下一个游标的序号
    /// 因为 max_batches 通常会小于 channel 数，如果每次就单纯的按顺序循环数组的话，会出现一种情况：只能拿到前几个channel
    /// 导致前几个channel被推进的非常快，而后面几个甚至有可能一直不推进
    ///
    /// 所以我们需要记录一下游标的序号，这样确保每一个channel都可以被正常循环读取到 (其实就是round-robin)
    next_cursor_index: usize,
}

impl ReplayDbReader {
    pub fn new(pool: DbPool, batch_size: i64, cursors: Vec<ReaderCursor>) -> Result<Self> {
        if batch_size <= 0 {
            return Err(ReplayDbReaderError::InvalidBatchSize);
        }

        Ok(Self {
            pool,
            batch_size,
            cursors,
            next_cursor_index: 0,
        })
    }

    pub async fn from_range_queries(
        pool: DbPool,
        batch_size: i64,
        sh_query: Option<&SHOrderRangeQuery>,
        sz_query: Option<&SZOrderRangeQuery>,
        transaction_query: Option<&TransactionRangeQuery>,
    ) -> Result<Self> {
        let mut cursors = Vec::new();

        if let Some(query) = sh_query {
            let ranges = query_sh_order_message_ranges(&pool, query).await?;
            cursors.extend(
                ranges
                    .into_iter()
                    .map(|range| {
                        ChannelRange::new(
                            &query.day,
                            query.start_time_ms,
                            query.end_time_ms,
                            ReplayDataKind::Order,
                            Market::XSHG,
                            range.channel,
                            range.begin_message_number,
                            range.end_message_number,
                            query.codes.clone(),
                            &query.table_name,
                        )
                    })
                    .map(ReaderCursor::new),
            );
        }

        if let Some(query) = sz_query {
            let ranges = query_sz_order_message_ranges(&pool, query).await?;
            cursors.extend(
                ranges
                    .into_iter()
                    .map(|range| {
                        ChannelRange::new(
                            &query.day,
                            query.start_time_ms,
                            query.end_time_ms,
                            ReplayDataKind::Order,
                            Market::XSHE,
                            range.channel,
                            range.begin_message_number,
                            range.end_message_number,
                            query.codes.clone(),
                            &query.table_name,
                        )
                    })
                    .map(ReaderCursor::new),
            );
        }

        if let Some(query) = transaction_query {
            let ranges = query_transaction_message_ranges(&pool, query).await?;
            cursors.extend(
                ranges
                    .into_iter()
                    .map(|range| {
                        ChannelRange::new(
                            &query.day,
                            query.start_time_ms,
                            query.end_time_ms,
                            ReplayDataKind::Transaction,
                            Market::Unknown,
                            range.channel,
                            range.begin_message_number,
                            range.end_message_number,
                            query.codes.clone(),
                            &query.table_name,
                        )
                    })
                    .map(ReaderCursor::new),
            );
        }

        Self::new(pool, batch_size, cursors)
    }

    pub fn cursors(&self) -> &[ReaderCursor] {
        &self.cursors
    }

    pub fn has_unfinished(&self) -> bool {
        self.cursors.iter().any(|cursor| !cursor.finished)
    }

    pub fn into_parts(self) -> (DbPool, i64, Vec<ReaderCursor>) {
        (self.pool, self.batch_size, self.cursors)
    }

    pub async fn fetch_batch_for_cursor(
        pool: &DbPool,
        batch_size: i64,
        cursor: &ReaderCursor,
    ) -> Result<FetchedBatch> {
        let batch_spec = CursorBatchSpec {
            cursor_index: 0,
            range: cursor.range.clone(),
            begin_message_number: cursor.next_message_number,
            end_message_number: cursor.current_batch_end(batch_size),
        };

        Self::fetch_batch_for_spec(pool.clone(), batch_spec).await
    }

    pub async fn peek_next_event_timestamp_for_cursor(
        pool: &DbPool,
        cursor: &ReaderCursor,
    ) -> Result<Option<i64>> {
        match cursor.range.data_kind {
            ReplayDataKind::Order => Self::peek_next_order_timestamp_for_cursor(pool, cursor).await,
            ReplayDataKind::Transaction => {
                Self::peek_next_transaction_timestamp_for_cursor(pool, cursor).await
            }
        }
    }

    pub async fn fetch_next_batches(&mut self, max_batches: usize) -> Result<Vec<FetchedBatch>> {
        if max_batches == 0 {
            return Err(ReplayDbReaderError::InvalidMaxBatches);
        }

        let (batch_specs, next_cursor_index) = self.plan_next_batch_specs(max_batches);
        if batch_specs.is_empty() {
            return Ok(Vec::new());
        }

        let mut handles = Vec::with_capacity(batch_specs.len());
        for batch_spec in &batch_specs {
            let pool = self.pool.clone();
            let batch_spec = batch_spec.clone();
            handles.push(tokio::spawn(async move {
                ReplayDbReader::fetch_batch_for_spec(pool, batch_spec).await
            }));
        }

        let mut fetched_batches = Vec::with_capacity(handles.len());
        let mut first_error = None;

        for handle in handles {
            match handle.await {
                Ok(Ok(batch)) => fetched_batches.push(batch),
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(ReplayDbReaderError::JoinTask(err));
                    }
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        self.commit_batch_specs(&batch_specs, next_cursor_index);
        Ok(fetched_batches)
    }

    fn plan_next_batch_specs(&self, max_batches: usize) -> (Vec<CursorBatchSpec>, usize) {
        if self.cursors.is_empty() || !self.has_unfinished() {
            return (Vec::new(), self.next_cursor_index);
        }

        let cursor_count = self.cursors.len();
        let mut cursor_index = self.next_cursor_index % cursor_count;
        let mut planned_batches = Vec::with_capacity(max_batches.min(cursor_count));
        let mut inspected = 0;

        while inspected < cursor_count && planned_batches.len() < max_batches {
            let cursor = &self.cursors[cursor_index];
            if !cursor.finished {
                planned_batches.push(CursorBatchSpec {
                    cursor_index,
                    range: cursor.range.clone(),
                    begin_message_number: cursor.next_message_number,
                    end_message_number: cursor.current_batch_end(self.batch_size),
                });
            }

            cursor_index = (cursor_index + 1) % cursor_count;
            inspected += 1;
        }

        (planned_batches, cursor_index)
    }

    fn commit_batch_specs(&mut self, batch_specs: &[CursorBatchSpec], next_cursor_index: usize) {
        for batch_spec in batch_specs {
            self.cursors[batch_spec.cursor_index].advance_to(batch_spec.end_message_number);
        }
        self.next_cursor_index = next_cursor_index;
    }

    async fn fetch_batch_for_spec(
        pool: DbPool,
        batch_spec: CursorBatchSpec,
    ) -> Result<FetchedBatch> {
        let events = Self::fetch_events_for_spec(&pool, &batch_spec).await?;

        Ok(FetchedBatch {
            data_kind: batch_spec.range.data_kind,
            market: batch_spec.range.market,
            channel: batch_spec.range.channel,
            day: batch_spec.range.day.clone(),
            begin_message_number: batch_spec.begin_message_number,
            end_message_number: batch_spec.end_message_number,
            events,
        })
    }

    async fn fetch_events_for_spec(
        pool: &DbPool,
        batch_spec: &CursorBatchSpec,
    ) -> Result<Vec<ReplayEvent>> {
        match batch_spec.range.data_kind {
            ReplayDataKind::Order => {
                let orders = Self::fetch_orders_for_spec(pool, batch_spec).await?;
                Ok(orders.into_iter().map(ReplayEvent::Order).collect())
            }
            ReplayDataKind::Transaction => {
                let transactions = Self::fetch_transactions_for_spec(pool, batch_spec).await?;
                Ok(transactions
                    .into_iter()
                    .map(ReplayEvent::Transaction)
                    .collect())
            }
        }
    }

    async fn fetch_orders_for_spec(
        pool: &DbPool,
        batch_spec: &CursorBatchSpec,
    ) -> Result<Vec<L2Order>> {
        match batch_spec.range.market {
            Market::XSHG => {
                let query = SHOrderByRangeQuery::new(
                    &batch_spec.range.day,
                    batch_spec.range.channel,
                    batch_spec.begin_message_number,
                    batch_spec.end_message_number,
                    &batch_spec.range.table_name,
                )
                .with_codes(batch_spec.range.codes.clone());
                Ok(query_sh_orders_by_range(pool, &query).await?)
            }
            Market::XSHE => {
                let query = SZOrderByRangeQuery::new(
                    &batch_spec.range.day,
                    batch_spec.range.channel,
                    batch_spec.begin_message_number,
                    batch_spec.end_message_number,
                    &batch_spec.range.table_name,
                )
                .with_codes(batch_spec.range.codes.clone());
                Ok(query_sz_orders_by_range(pool, &query).await?)
            }
            Market::Unknown => Ok(Vec::new()),
        }
    }

    async fn fetch_transactions_for_spec(
        pool: &DbPool,
        batch_spec: &CursorBatchSpec,
    ) -> Result<Vec<crate::common::L2Transaction>> {
        let query = TransactionByRangeQuery::new(
            &batch_spec.range.day,
            batch_spec.range.channel,
            batch_spec.begin_message_number,
            batch_spec.end_message_number,
            &batch_spec.range.table_name,
        )
        .with_codes(batch_spec.range.codes.clone());
        Ok(query_transactions_by_range(pool, &query).await?)
    }

    async fn peek_next_order_timestamp_for_cursor(
        pool: &DbPool,
        cursor: &ReaderCursor,
    ) -> Result<Option<i64>> {
        let client = pool.get_one().await?;

        let rows = match cursor.range.market {
            Market::XSHG => {
                let mut sql = String::from(
                    r#"
                SELECT
                    toUnixTimestamp64Milli(time) AS timestamp_ms
                FROM ?
                WHERE EventDate = toDate(?)
                  AND time >= fromUnixTimestamp64Milli(?)
                  AND time < fromUnixTimestamp64Milli(?)
                  AND message_number >= ?
                  AND message_number < ?
                  AND channel = ?
            "#,
                );

                if !cursor.range.codes.is_empty() {
                    sql.push_str(" AND code IN (");
                    for index in 0..cursor.range.codes.len() {
                        if index > 0 {
                            sql.push_str(", ");
                        }
                        sql.push('?');
                    }
                    sql.push(')');
                }

                sql.push_str(" ORDER BY message_number LIMIT 1");

                let mut db_query = client
                    .query(&sql)
                    .bind(Identifier(&cursor.range.table_name))
                    .bind(&cursor.range.day)
                    .bind(cursor.range.start_time_ms)
                    .bind(cursor.range.end_time_ms)
                    .bind(cursor.next_message_number)
                    .bind(cursor.range.end_message_number)
                    .bind(cursor.range.channel);

                for code in &cursor.range.codes {
                    db_query = db_query.bind(code);
                }

                db_query
                    .fetch_all::<RawNextTimestamp>()
                    .await
                    .map_err(|err| {
                        ReplayDbReaderError::SHOrderQuery(SHOrderQueryError::Query(err))
                    })?
            }
            Market::XSHE => {
                let mut sql = String::from(
                    r#"
                SELECT
                    toUnixTimestamp64Milli(time) AS timestamp_ms
                FROM ?
                WHERE EventDate = toDate(?)
                  AND time >= fromUnixTimestamp64Milli(?)
                  AND time < fromUnixTimestamp64Milli(?)
                  AND message_number >= ?
                  AND message_number < ?
                  AND channel = ?
            "#,
                );

                if !cursor.range.codes.is_empty() {
                    sql.push_str(" AND code IN (");
                    for index in 0..cursor.range.codes.len() {
                        if index > 0 {
                            sql.push_str(", ");
                        }
                        sql.push('?');
                    }
                    sql.push(')');
                }

                sql.push_str(" ORDER BY message_number LIMIT 1");

                let mut db_query = client
                    .query(&sql)
                    .bind(Identifier(&cursor.range.table_name))
                    .bind(&cursor.range.day)
                    .bind(cursor.range.start_time_ms)
                    .bind(cursor.range.end_time_ms)
                    .bind(cursor.next_message_number)
                    .bind(cursor.range.end_message_number)
                    .bind(cursor.range.channel);

                for code in &cursor.range.codes {
                    db_query = db_query.bind(code);
                }

                db_query
                    .fetch_all::<RawNextTimestamp>()
                    .await
                    .map_err(|err| {
                        ReplayDbReaderError::SZOrderQuery(SZOrderQueryError::Query(err))
                    })?
            }
            Market::Unknown => return Ok(None),
        };

        Ok(rows.into_iter().next().map(|row| row.timestamp_ms))
    }

    async fn peek_next_transaction_timestamp_for_cursor(
        pool: &DbPool,
        cursor: &ReaderCursor,
    ) -> Result<Option<i64>> {
        let client = pool.get_one().await?;
        let mut sql = String::from(
            r#"
            SELECT
                toUnixTimestamp64Milli(time) AS timestamp_ms
            FROM ?
            WHERE EventDate = toDate(?)
              AND time >= fromUnixTimestamp64Milli(?)
              AND time < fromUnixTimestamp64Milli(?)
              AND message_number >= ?
              AND message_number < ?
              AND channel = ?
        "#,
        );

        if !cursor.range.codes.is_empty() {
            sql.push_str(" AND code IN (");
            for index in 0..cursor.range.codes.len() {
                if index > 0 {
                    sql.push_str(", ");
                }
                sql.push('?');
            }
            sql.push(')');
        }

        sql.push_str(" ORDER BY message_number LIMIT 1");

        let mut db_query = client
            .query(&sql)
            .bind(Identifier(&cursor.range.table_name))
            .bind(&cursor.range.day)
            .bind(cursor.range.start_time_ms)
            .bind(cursor.range.end_time_ms)
            .bind(cursor.next_message_number)
            .bind(cursor.range.end_message_number)
            .bind(cursor.range.channel);

        for code in &cursor.range.codes {
            db_query = db_query.bind(code);
        }

        let rows = db_query
            .fetch_all::<RawNextTimestamp>()
            .await
            .map_err(|err| {
                ReplayDbReaderError::TransactionQuery(TransactionQueryError::Query(err))
            })?;

        Ok(rows.into_iter().next().map(|row| row.timestamp_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::{FetchedBatch, ReplayDbReader};
    use crate::common::Market;
    use crate::config::{DbConfig, DbSchemaConfig, DbTableConfig};
    use crate::db::dbpool::build_client;
    use crate::replay::reader_cursor::{ChannelRange, ReaderCursor, ReplayDataKind};

    fn test_pool() -> crate::db::dbpool::DbPool {
        let config = DbConfig {
            url: "http://127.0.0.1:8123".to_string(),
            user: "user".to_string(),
            password: "password".to_string(),
            database: "db".to_string(),
            pool_size: 1,
            tables: DbTableConfig {
                sh_order: "sh_table".to_string(),
                sz_order: "sz_table".to_string(),
                transaction: "tx_table".to_string(),
            },
            schema: DbSchemaConfig::default(),
        };

        crate::db::dbpool::DbPool::with_client(1, build_client(&config)).unwrap()
    }

    #[tokio::test]
    async fn fetches_multiple_batches_and_advances_cursors() {
        let pool = test_pool();
        let cursors = vec![
            ReaderCursor::new(ChannelRange::new(
                "2026-05-12",
                1_000,
                2_000,
                ReplayDataKind::Order,
                Market::Unknown,
                1,
                10,
                15,
                Vec::new(),
                "unknown",
            )),
            ReaderCursor::new(ChannelRange::new(
                "2026-05-12",
                1_000,
                2_000,
                ReplayDataKind::Order,
                Market::Unknown,
                2,
                20,
                27,
                Vec::new(),
                "unknown",
            )),
            ReaderCursor::new(ChannelRange::new(
                "2026-05-12",
                1_000,
                2_000,
                ReplayDataKind::Order,
                Market::Unknown,
                3,
                30,
                30,
                Vec::new(),
                "unknown",
            )),
        ];
        let mut reader = ReplayDbReader::new(pool, 3, cursors).unwrap();

        let batches = reader.fetch_next_batches(2).await.unwrap();

        assert_eq!(
            batches,
            vec![
                FetchedBatch {
                    data_kind: ReplayDataKind::Order,
                    market: Market::Unknown,
                    channel: 1,
                    day: "2026-05-12".to_string(),
                    begin_message_number: 10,
                    end_message_number: 13,
                    events: Vec::new(),
                },
                FetchedBatch {
                    data_kind: ReplayDataKind::Order,
                    market: Market::Unknown,
                    channel: 2,
                    day: "2026-05-12".to_string(),
                    begin_message_number: 20,
                    end_message_number: 23,
                    events: Vec::new(),
                },
            ]
        );
        assert_eq!(reader.cursors()[0].next_message_number, 13);
        assert_eq!(reader.cursors()[1].next_message_number, 23);
        assert_eq!(reader.cursors()[2].next_message_number, 30);
        assert_eq!(reader.next_cursor_index, 2);
    }

    #[tokio::test]
    async fn continues_round_robin_across_calls() {
        let pool = test_pool();
        let cursors = vec![
            ReaderCursor::new(ChannelRange::new(
                "2026-05-12",
                1_000,
                2_000,
                ReplayDataKind::Order,
                Market::Unknown,
                1,
                10,
                14,
                Vec::new(),
                "unknown",
            )),
            ReaderCursor::new(ChannelRange::new(
                "2026-05-12",
                1_000,
                2_000,
                ReplayDataKind::Order,
                Market::Unknown,
                2,
                20,
                24,
                Vec::new(),
                "unknown",
            )),
            ReaderCursor::new(ChannelRange::new(
                "2026-05-12",
                1_000,
                2_000,
                ReplayDataKind::Order,
                Market::Unknown,
                3,
                30,
                34,
                Vec::new(),
                "unknown",
            )),
        ];
        let mut reader = ReplayDbReader::new(pool, 2, cursors).unwrap();

        let first_batches = reader.fetch_next_batches(2).await.unwrap();
        let second_batches = reader.fetch_next_batches(2).await.unwrap();

        assert_eq!(first_batches[0].channel, 1);
        assert_eq!(first_batches[1].channel, 2);
        assert_eq!(second_batches[0].channel, 3);
        assert_eq!(second_batches[1].channel, 1);
    }
}
