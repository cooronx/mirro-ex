use crate::marketdata::proto;
use crate::matcher::order_book;
use prost::Message;

impl From<order_book::LevelSnapshot> for proto::PriceLevel {
    fn from(level: order_book::LevelSnapshot) -> Self {
        Self {
            price: level.price,
            quantity: level.total_qty,
        }
    }
}

impl proto::OrderBookSnapshot {
    pub fn from_book_snapshot(
        event_ts_ms: i64,
        code: impl Into<String>,
        snapshot: order_book::OrderBookSnapshot,
    ) -> Self {
        Self {
            event_ts_ms,
            code: code.into(),
            bids: snapshot.bids.into_iter().map(Into::into).collect(),
            asks: snapshot.asks.into_iter().map(Into::into).collect(),
        }
    }
}

impl proto::Envelope {
    pub fn from_snapshot(
        sequence: u64,
        publish_ts_ms: i64,
        replay_run_id: impl Into<String>,
        replay_seq: u64,
        snapshot: proto::OrderBookSnapshot,
    ) -> Self {
        Self {
            sequence,
            publish_ts_ms,
            replay_run_id: replay_run_id.into(),
            event_ts_ms: snapshot.event_ts_ms,
            replay_seq,
            payload: Some(proto::envelope::Payload::Snapshot(snapshot)),
        }
    }

    pub fn from_raw_event(
        sequence: u64,
        publish_ts_ms: i64,
        replay_run_id: impl Into<String>,
        event_ts_ms: i64,
        replay_seq: u64,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            sequence,
            publish_ts_ms,
            replay_run_id: replay_run_id.into(),
            event_ts_ms,
            replay_seq,
            payload: Some(proto::envelope::Payload::RawEventJson(payload)),
        }
    }

    pub fn from_watermark(
        sequence: u64,
        publish_ts_ms: i64,
        replay_run_id: impl Into<String>,
        completed_through_ms: i64,
    ) -> Self {
        Self {
            sequence,
            publish_ts_ms,
            replay_run_id: replay_run_id.into(),
            event_ts_ms: completed_through_ms,
            replay_seq: 0,
            payload: Some(proto::envelope::Payload::Watermark(proto::Watermark {
                completed_through_ms,
            })),
        }
    }
}

pub fn encode_snapshot_envelope(
    sequence: u64,
    publish_ts_ms: i64,
    replay_run_id: impl Into<String>,
    replay_seq: u64,
    event_ts_ms: i64,
    code: impl Into<String>,
    snapshot: order_book::OrderBookSnapshot,
) -> Result<Vec<u8>, prost::EncodeError> {
    let snapshot = proto::OrderBookSnapshot::from_book_snapshot(event_ts_ms, code, snapshot);
    let envelope = proto::Envelope::from_snapshot(
        sequence,
        publish_ts_ms,
        replay_run_id,
        replay_seq,
        snapshot,
    );
    let mut bytes = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut bytes)?;
    Ok(bytes)
}
