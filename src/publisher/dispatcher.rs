use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tracing::info;

use crate::config::NatsConfig;
use crate::matcher::order_book::OrderBookSnapshot;
use crate::replay::ReplayEvent;

use super::snapshot::encode_snapshot_envelope;

pub type Result<T> = std::result::Result<T, NatsDispatcherError>;

#[derive(Debug, Error)]
pub enum NatsDispatcherError {
    #[error("failed to connect to nats at {url}")]
    Connect {
        url: String,
        #[source]
        source: async_nats::ConnectError,
    },
    #[error("failed to encode snapshot envelope")]
    Encode(#[from] prost::EncodeError),
    #[error("failed to encode raw replay event: {error}")]
    EncodeJson { error: String },
    #[error("failed to publish snapshot to nats subject {subject}")]
    Publish {
        subject: String,
        #[source]
        source: async_nats::PublishError,
    },
    #[error("failed to flush replay events to nats")]
    Flush(#[source] async_nats::client::FlushError),
    #[error("system clock is earlier than unix epoch")]
    InvalidSystemClock,
}

pub struct NatsDispatcher {
    client: async_nats::Client,
    subject: String,
    replay_run_id: String,
    next_sequence: u64,
}

impl NatsDispatcher {
    pub async fn new(config: &NatsConfig, replay_run_id: impl Into<String>) -> Result<Self> {
        let client = async_nats::connect(&config.url).await.map_err(|source| {
            NatsDispatcherError::Connect {
                url: config.url.clone(),
                source,
            }
        })?;

        info!(url = %config.url, subject = %config.subject, "connected to nats");

        Ok(Self {
            client,
            subject: config.subject.clone(),
            replay_run_id: replay_run_id.into(),
            next_sequence: 1,
        })
    }

    pub async fn publish_raw_event(&mut self, replay_seq: u64, event: &ReplayEvent) -> Result<()> {
        let event_ts_ms = event.timestamp_ms();
        let payload =
            serde_json::to_vec(event).map_err(|error| NatsDispatcherError::EncodeJson {
                error: error.to_string(),
            })?;
        let envelope = crate::marketdata::proto::Envelope::from_raw_event(
            self.next_sequence,
            current_unix_timestamp_ms()?,
            self.replay_run_id.clone(),
            event_ts_ms,
            replay_seq,
            payload,
        );
        self.publish_envelope(envelope).await
    }

    pub async fn publish_snapshot(
        &mut self,
        replay_seq: u64,
        event_ts_ms: i64,
        code: &str,
        snapshot: OrderBookSnapshot,
    ) -> Result<()> {
        let publish_ts_ms = current_unix_timestamp_ms()?;
        let payload = encode_snapshot_envelope(
            self.next_sequence,
            publish_ts_ms,
            self.replay_run_id.clone(),
            replay_seq,
            event_ts_ms,
            code,
            snapshot,
        )?;

        self.publish_payload(payload).await
    }

    pub async fn publish_watermark(&mut self, completed_through_ms: i64) -> Result<()> {
        let envelope = crate::marketdata::proto::Envelope::from_watermark(
            self.next_sequence,
            current_unix_timestamp_ms()?,
            self.replay_run_id.clone(),
            completed_through_ms,
        );
        self.publish_envelope(envelope).await?;
        self.client
            .flush()
            .await
            .map_err(NatsDispatcherError::Flush)
    }

    async fn publish_envelope(
        &mut self,
        envelope: crate::marketdata::proto::Envelope,
    ) -> Result<()> {
        let mut payload = Vec::with_capacity(prost::Message::encoded_len(&envelope));
        prost::Message::encode(&envelope, &mut payload)?;
        self.publish_payload(payload).await
    }

    async fn publish_payload(&mut self, payload: Vec<u8>) -> Result<()> {
        self.client
            .publish(self.subject.clone(), payload.into())
            .await
            .map_err(|source| NatsDispatcherError::Publish {
                subject: self.subject.clone(),
                source,
            })?;
        self.next_sequence += 1;
        Ok(())
    }
}

fn current_unix_timestamp_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NatsDispatcherError::InvalidSystemClock)?;

    Ok(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}
