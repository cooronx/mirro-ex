//!
//! lane生产者模块。
//! 1. 输入：
//!    - `ReplayDbReader` 提供的批量读取能力
//!    - 一组 `ReaderCursor`
//!    - 每个 lane 对应的 `ChannelReplayLane`
//!
//! 2. 输出：
//!    - 按 lane 切分的 `LaneOutput`
//!    - 每个 lane 通过独立的 mpsc channel 把 ready batch / watermark / finished 状态发给 coordinator
//!
//! 3. 逻辑：
//!    - 根据 cursor 集合构建 lane，并把 source 归并到正确的 lane key
//!    - 后台不断从 `ReplayDbReader` 拉取下一批数据
//!    - 把批次喂给对应 `ChannelReplayLane`，让 lane 产出当前 ready 的事件
//!    - 把每个 lane 的 ready 事件、水位和完成状态异步发送给 `ReplayCoordinator`
//!
use std::cmp::Ordering;
use std::collections::BTreeMap;

use thiserror::Error;
use tokio::sync::mpsc;
use tracing::error;

use crate::common::Market;

use super::channel_replay_lane::{ChannelReplayLane, ChannelReplayLaneError};
use super::db_reader::{FetchedBatch, ReplayDbReader, ReplayDbReaderError};
use super::event::ReplayEvent;
use super::reader_cursor::{ReaderCursor, ReplayDataKind};

pub type Result<T> = std::result::Result<T, LaneProducerError>;

#[derive(Debug, Error)]
pub enum LaneProducerError {
    #[error("lane queue capacity must be greater than 0")]
    InvalidQueueCapacity,
    #[error("replay db reader failed")]
    Reader(#[from] ReplayDbReaderError),
    #[error("channel replay lane failed")]
    Lane(#[from] ChannelReplayLaneError),
    #[error("ambiguous order lanes for transaction channel={channel}")]
    AmbiguousTransactionLane { channel: i64 },
    #[error(
        "duplicate source for lane market={market:?} channel={channel} data_kind={data_kind:?}"
    )]
    DuplicateSourceForLane {
        market: Market,
        channel: i64,
        data_kind: ReplayDataKind,
    },
    #[error("transaction batch market could not be resolved for channel={channel}")]
    UnresolvedTransactionMarket { channel: i64 },
    #[error(
        "transaction batch contains inconsistent markets for channel={channel}: expected={expected:?}, actual={actual:?}"
    )]
    InconsistentTransactionMarket {
        channel: i64,
        expected: Market,
        actual: Market,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneKey {
    pub(crate) market: Market,
    pub(crate) channel: i64,
}

impl LaneKey {
    pub(crate) fn new(market: Market, channel: i64) -> Self {
        Self { market, channel }
    }

    pub(crate) fn market_rank(self) -> u8 {
        match self.market {
            Market::XSHG => 0,
            Market::XSHE => 1,
            Market::Unknown => 2,
        }
    }
}

impl Ord for LaneKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.market_rank(), self.channel).cmp(&(other.market_rank(), other.channel))
    }
}

impl PartialOrd for LaneKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub enum LaneOutput {
    ReadyBatch {
        lane_key: LaneKey,
        events: Vec<ReplayEvent>,
        watermark_ms: Option<i64>,
    },
    Progress {
        lane_key: LaneKey,
        watermark_ms: Option<i64>,
    },
    Finished {
        lane_key: LaneKey,
    },
}

pub struct LaneReceiver {
    pub(crate) lane_key: LaneKey,
    pub(crate) receiver: mpsc::Receiver<LaneOutput>,
}

#[derive(Debug)]
struct LaneProducerSpec {
    lane_key: LaneKey,
    day: String,
    order_cursor: Option<ReaderCursor>,
    transaction_cursor: Option<ReaderCursor>,
    initial_transaction_batch: Option<FetchedBatch>,
}

struct LaneProducer {
    lane_key: LaneKey,
    pool: crate::db::dbpool::DbPool,
    batch_size: i64,
    lane: ChannelReplayLane,
    order_cursor: Option<ReaderCursor>,
    transaction_cursor: Option<ReaderCursor>,
    pending_transaction_batch: Option<FetchedBatch>,
    sender: mpsc::Sender<LaneOutput>,
    last_sent_watermark_ms: Option<i64>,
}

pub async fn spawn_lane_producers(
    reader: ReplayDbReader,
    queue_capacity: usize,
) -> Result<Vec<LaneReceiver>> {
    if queue_capacity == 0 {
        return Err(LaneProducerError::InvalidQueueCapacity);
    }

    let (pool, batch_size, cursors) = reader.into_parts();
    let specs = build_lane_specs(&pool, batch_size, cursors).await?;
    let mut receivers = Vec::with_capacity(specs.len());

    for spec in specs.into_values() {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let lane_key = spec.lane_key;
        let producer = LaneProducer::new(pool.clone(), batch_size, spec, sender)?;

        tokio::spawn(async move {
            if let Err(err) = producer.run().await {
                let mut error_chain = err.to_string();
                let mut source = std::error::Error::source(&err);
                while let Some(cause) = source {
                    error_chain.push_str(": ");
                    error_chain.push_str(&cause.to_string());
                    source = cause.source();
                }
                error!(
                    market = ?lane_key.market,
                    channel = lane_key.channel,
                    error_chain = %error_chain,
                    "lane producer failed"
                );
            }
        });

        receivers.push(LaneReceiver { lane_key, receiver });
    }

    Ok(receivers)
}

async fn build_lane_specs(
    pool: &crate::db::dbpool::DbPool,
    batch_size: i64,
    cursors: Vec<ReaderCursor>,
) -> Result<BTreeMap<LaneKey, LaneProducerSpec>> {
    let mut specs = BTreeMap::<LaneKey, LaneProducerSpec>::new();
    let mut order_lane_by_channel = BTreeMap::<i64, LaneKey>::new();
    let mut transaction_cursors = Vec::new();

    for cursor in cursors {
        match cursor.range.data_kind {
            ReplayDataKind::Order => {
                let lane_key = LaneKey::new(cursor.range.market, cursor.range.channel);
                if order_lane_by_channel
                    .insert(cursor.range.channel, lane_key)
                    .is_some()
                {
                    return Err(LaneProducerError::AmbiguousTransactionLane {
                        channel: cursor.range.channel,
                    });
                }

                let spec = specs.entry(lane_key).or_insert(LaneProducerSpec {
                    lane_key,
                    day: cursor.range.day.clone(),
                    order_cursor: None,
                    transaction_cursor: None,
                    initial_transaction_batch: None,
                });

                if spec.order_cursor.replace(cursor).is_some() {
                    return Err(LaneProducerError::DuplicateSourceForLane {
                        market: lane_key.market,
                        channel: lane_key.channel,
                        data_kind: ReplayDataKind::Order,
                    });
                }
            }
            ReplayDataKind::Transaction => transaction_cursors.push(cursor),
        }
    }

    for mut cursor in transaction_cursors {
        if let Some(lane_key) = order_lane_by_channel.get(&cursor.range.channel).copied() {
            let spec = specs.entry(lane_key).or_insert(LaneProducerSpec {
                lane_key,
                day: cursor.range.day.clone(),
                order_cursor: None,
                transaction_cursor: None,
                initial_transaction_batch: None,
            });

            if spec.transaction_cursor.replace(cursor).is_some() {
                return Err(LaneProducerError::DuplicateSourceForLane {
                    market: lane_key.market,
                    channel: lane_key.channel,
                    data_kind: ReplayDataKind::Transaction,
                });
            }
            continue;
        }

        let initial_batch =
            ReplayDbReader::fetch_batch_for_cursor(pool, batch_size, &cursor).await?;
        let market = resolve_transaction_batch_market(&initial_batch)?;
        let lane_key = LaneKey::new(market, cursor.range.channel);
        cursor.advance_to(initial_batch.end_message_number);

        let spec = specs.entry(lane_key).or_insert(LaneProducerSpec {
            lane_key,
            day: cursor.range.day.clone(),
            order_cursor: None,
            transaction_cursor: None,
            initial_transaction_batch: None,
        });

        if spec.transaction_cursor.replace(cursor).is_some() {
            return Err(LaneProducerError::DuplicateSourceForLane {
                market: lane_key.market,
                channel: lane_key.channel,
                data_kind: ReplayDataKind::Transaction,
            });
        }
        spec.initial_transaction_batch = Some(initial_batch);
    }

    Ok(specs)
}

fn resolve_transaction_batch_market(batch: &FetchedBatch) -> Result<Market> {
    let mut resolved_market = None;

    for event in &batch.events {
        let event_market = event.market();
        if let Some(current_market) = resolved_market {
            if current_market != event_market {
                return Err(LaneProducerError::InconsistentTransactionMarket {
                    channel: batch.channel,
                    expected: current_market,
                    actual: event_market,
                });
            }
        } else {
            resolved_market = Some(event_market);
        }
    }

    resolved_market.ok_or(LaneProducerError::UnresolvedTransactionMarket {
        channel: batch.channel,
    })
}

impl LaneProducer {
    fn new(
        pool: crate::db::dbpool::DbPool,
        batch_size: i64,
        spec: LaneProducerSpec,
        sender: mpsc::Sender<LaneOutput>,
    ) -> Result<Self> {
        let lane = ChannelReplayLane::new(
            spec.day,
            spec.lane_key.channel,
            spec.order_cursor.is_some(),
            spec.transaction_cursor.is_some(),
        )?;

        Ok(Self {
            lane_key: spec.lane_key,
            pool,
            batch_size,
            lane,
            order_cursor: spec.order_cursor,
            transaction_cursor: spec.transaction_cursor,
            pending_transaction_batch: spec.initial_transaction_batch,
            sender,
            last_sent_watermark_ms: None,
        })
    }

    async fn run(mut self) -> Result<()> {
        loop {
            self.sync_finished_markers();

            if let Some(batch) = self.pending_transaction_batch.take() {
                self.ingest_batch(batch)?;
            } else if let Some(data_kind) = self.select_next_fetch_kind() {
                self.fetch_and_ingest(data_kind).await?;
            }

            self.sync_finished_markers();
            let ready_events = self.lane.pop_ready_events();
            let last_ready_timestamp_ms = ready_events.last().map(ReplayEvent::timestamp_ms);
            let watermark_ms = self
                .compute_progress_watermark_ms(last_ready_timestamp_ms)
                .await?;
            if !ready_events.is_empty() {
                if !self.send_ready_batch(ready_events, watermark_ms).await? {
                    return Ok(());
                }
            } else if !self.send_progress_if_advanced(watermark_ms).await? {
                return Ok(());
            }

            if self.is_finished() {
                let _ = self
                    .sender
                    .send(LaneOutput::Finished {
                        lane_key: self.lane_key,
                    })
                    .await;
                return Ok(());
            }
        }
    }

    fn select_next_fetch_kind(&self) -> Option<ReplayDataKind> {
        let order_unfinished = self
            .order_cursor
            .as_ref()
            .is_some_and(|cursor| !cursor.finished);
        let transaction_unfinished = self
            .transaction_cursor
            .as_ref()
            .is_some_and(|cursor| !cursor.finished);

        match (order_unfinished, transaction_unfinished) {
            (false, false) => None,
            (true, false) => Some(ReplayDataKind::Order),
            (false, true) => Some(ReplayDataKind::Transaction),
            (true, true) => match (
                self.lane.order_covered_until(),
                self.lane.transaction_covered_until(),
            ) {
                (None, None) => Some(ReplayDataKind::Order),
                (None, Some(_)) => Some(ReplayDataKind::Order),
                (Some(_), None) => Some(ReplayDataKind::Transaction),
                (Some(order_covered_until), Some(transaction_covered_until)) => {
                    if order_covered_until <= transaction_covered_until {
                        Some(ReplayDataKind::Order)
                    } else {
                        Some(ReplayDataKind::Transaction)
                    }
                }
            },
        }
    }

    async fn fetch_and_ingest(&mut self, data_kind: ReplayDataKind) -> Result<()> {
        let cursor = match data_kind {
            ReplayDataKind::Order => self.order_cursor.as_mut(),
            ReplayDataKind::Transaction => self.transaction_cursor.as_mut(),
        }
        .expect("lane producer must have cursor for selected data kind");

        let batch =
            ReplayDbReader::fetch_batch_for_cursor(&self.pool, self.batch_size, cursor).await?;
        cursor.advance_to(batch.end_message_number);
        self.ingest_batch(batch)
    }

    fn ingest_batch(&mut self, batch: FetchedBatch) -> Result<()> {
        if batch.data_kind == ReplayDataKind::Transaction && !batch.events.is_empty() {
            let batch_market = resolve_transaction_batch_market(&batch)?;
            if batch_market != self.lane_key.market {
                return Err(LaneProducerError::InconsistentTransactionMarket {
                    channel: batch.channel,
                    expected: self.lane_key.market,
                    actual: batch_market,
                });
            }
        }

        self.lane.push_batch(batch)?;
        Ok(())
    }

    fn sync_finished_markers(&mut self) {
        if self
            .order_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.finished)
            && !self.lane.order_finished()
        {
            self.lane.mark_finished(ReplayDataKind::Order);
        }

        if self
            .transaction_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.finished)
            && !self.lane.transaction_finished()
        {
            self.lane.mark_finished(ReplayDataKind::Transaction);
        }
    }

    fn is_finished(&self) -> bool {
        let order_done = self
            .order_cursor
            .as_ref()
            .is_none_or(|cursor| cursor.finished);
        let transaction_done = self
            .transaction_cursor
            .as_ref()
            .is_none_or(|cursor| cursor.finished);

        order_done
            && transaction_done
            && self.pending_transaction_batch.is_none()
            && self.lane.order_buffer_len() == 0
            && self.lane.transaction_buffer_len() == 0
    }

    async fn send_ready_batch(
        &mut self,
        events: Vec<ReplayEvent>,
        watermark_ms: Option<i64>,
    ) -> Result<bool> {
        if self
            .sender
            .send(LaneOutput::ReadyBatch {
                lane_key: self.lane_key,
                events,
                watermark_ms,
            })
            .await
            .is_err()
        {
            return Ok(false);
        }

        if let Some(watermark_ms) = watermark_ms {
            self.last_sent_watermark_ms = Some(watermark_ms);
        }

        Ok(true)
    }

    async fn send_progress_if_advanced(&mut self, watermark_ms: Option<i64>) -> Result<bool> {
        let Some(watermark_ms) = watermark_ms else {
            return Ok(true);
        };

        if self
            .last_sent_watermark_ms
            .is_some_and(|last_sent_watermark_ms| watermark_ms <= last_sent_watermark_ms)
        {
            return Ok(true);
        }

        if self
            .sender
            .send(LaneOutput::Progress {
                lane_key: self.lane_key,
                watermark_ms: Some(watermark_ms),
            })
            .await
            .is_err()
        {
            return Ok(false);
        }

        self.last_sent_watermark_ms = Some(watermark_ms);
        Ok(true)
    }

    async fn compute_progress_watermark_ms(
        &mut self,
        last_ready_timestamp_ms: Option<i64>,
    ) -> Result<Option<i64>> {
        if let Some(buffered_timestamp_ms) = self.lane.next_buffered_event_timestamp_ms() {
            return Ok(Some(buffered_timestamp_ms));
        }

        let next_future_timestamp_ms = self.peek_next_lane_timestamp_ms().await?;
        self.sync_finished_markers();
        Ok(next_future_timestamp_ms.or(last_ready_timestamp_ms))
    }

    async fn peek_next_lane_timestamp_ms(&mut self) -> Result<Option<i64>> {
        let mut next_timestamp_ms: Option<i64> = None;

        if let Some(cursor) = self.order_cursor.as_mut() {
            if !cursor.finished {
                let peeked_timestamp_ms =
                    ReplayDbReader::peek_next_event_timestamp_for_cursor(&self.pool, cursor)
                        .await?;
                if peeked_timestamp_ms.is_none() {
                    cursor.advance_to(cursor.range.end_message_number);
                }
                next_timestamp_ms = match (next_timestamp_ms, peeked_timestamp_ms) {
                    (Some(current), Some(candidate)) => Some(current.min(candidate)),
                    (None, Some(candidate)) => Some(candidate),
                    (current, None) => current,
                };
            }
        }

        if let Some(cursor) = self.transaction_cursor.as_mut() {
            if !cursor.finished {
                let peeked_timestamp_ms =
                    ReplayDbReader::peek_next_event_timestamp_for_cursor(&self.pool, cursor)
                        .await?;
                if peeked_timestamp_ms.is_none() {
                    cursor.advance_to(cursor.range.end_message_number);
                }
                next_timestamp_ms = match (next_timestamp_ms, peeked_timestamp_ms) {
                    (Some(current), Some(candidate)) => Some(current.min(candidate)),
                    (None, Some(candidate)) => Some(candidate),
                    (current, None) => current,
                };
            }
        }

        Ok(next_timestamp_ms)
    }
}
