use serde::Serialize;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppEvent {
    ReplayChanged,
    MarketChanged { code: String },
    TradingChanged { user_id: Option<String> },
}

impl AppEvent {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ReplayChanged => "replay_changed",
            Self::MarketChanged { .. } => "market_changed",
            Self::TradingChanged { .. } => "trading_changed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventBus {
    tx: broadcast::Sender<AppEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: AppEvent) {
        let _ = self.tx.send(event);
    }
}
