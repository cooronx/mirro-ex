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
        snapshot: proto::OrderBookSnapshot,
    ) -> Self {
        Self {
            sequence,
            publish_ts_ms,
            snapshot: Some(snapshot),
        }
    }
}

pub fn encode_snapshot_envelope(
    sequence: u64,
    publish_ts_ms: i64,
    event_ts_ms: i64,
    code: impl Into<String>,
    snapshot: order_book::OrderBookSnapshot,
) -> Result<Vec<u8>, prost::EncodeError> {
    let snapshot = proto::OrderBookSnapshot::from_book_snapshot(event_ts_ms, code, snapshot);
    let envelope = proto::Envelope::from_snapshot(sequence, publish_ts_ms, snapshot);
    let mut bytes = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut bytes)?;
    Ok(bytes)
}
