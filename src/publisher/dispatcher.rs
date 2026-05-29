use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::config::NatsConfig;
use crate::matcher::order_book::OrderBookSnapshot;

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
    #[error("failed to publish snapshot to nats subject {subject}")]
    Publish {
        subject: String,
        #[source]
        source: async_nats::PublishError,
    },
    #[error("system clock is earlier than unix epoch")]
    InvalidSystemClock,
}

pub struct NatsDispatcher {
    client: async_nats::Client,
    subject: String,
    next_sequence: u64,
}

impl NatsDispatcher {
    pub async fn new(config: &NatsConfig) -> Result<Self> {
        let client = async_nats::connect(&config.url).await.map_err(|source| {
            NatsDispatcherError::Connect {
                url: config.url.clone(),
                source,
            }
        })?;

        Ok(Self {
            client,
            subject: config.subject.clone(),
            next_sequence: 1,
        })
    }

    pub async fn publish_snapshot(
        &mut self,
        event_ts_ms: i64,
        code: &str,
        snapshot: OrderBookSnapshot,
    ) -> Result<()> {
        let publish_ts_ms = current_unix_timestamp_ms()?;
        let payload = encode_snapshot_envelope(
            self.next_sequence,
            publish_ts_ms,
            event_ts_ms,
            code,
            snapshot,
        )?;

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
