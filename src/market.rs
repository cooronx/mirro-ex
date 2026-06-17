use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::matcher::order_book::OrderBookSnapshot;

#[derive(Debug, Clone, Serialize)]
pub struct MarketSnapshotView {
    pub code: String,
    pub timestamp_ms: i64,
    pub last_price: Option<i64>,
    pub bid1_price: Option<i64>,
    pub bid1_qty: Option<i64>,
    pub ask1_price: Option<i64>,
    pub ask1_qty: Option<i64>,
}

#[derive(Debug, Clone)]
struct MarketSnapshot {
    timestamp_ms: i64,
    last_price: Option<i64>,
    snapshot: OrderBookSnapshot,
}

#[derive(Debug, Clone, Default)]
pub struct MarketState {
    snapshots: Arc<RwLock<HashMap<String, MarketSnapshot>>>,
}

impl MarketState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(
        &self,
        code: &str,
        timestamp_ms: i64,
        last_price: Option<i64>,
        snapshot: &OrderBookSnapshot,
    ) {
        let market_snapshot = MarketSnapshot {
            timestamp_ms,
            last_price,
            snapshot: snapshot.clone(),
        };
        self.snapshots
            .write()
            .expect("market state lock poisoned")
            .insert(code.to_string(), market_snapshot);
    }

    pub fn get(&self, code: &str) -> Option<MarketSnapshotView> {
        let snapshots = self.snapshots.read().expect("market state lock poisoned");
        let snapshot = snapshots.get(code)?;
        let bid1 = snapshot.snapshot.bids.first();
        let ask1 = snapshot.snapshot.asks.first();
        Some(MarketSnapshotView {
            code: code.to_string(),
            timestamp_ms: snapshot.timestamp_ms,
            last_price: snapshot.last_price,
            bid1_price: bid1.map(|level| level.price),
            bid1_qty: bid1.map(|level| level.total_qty),
            ask1_price: ask1.map(|level| level.price),
            ask1_qty: ask1.map(|level| level.total_qty),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::MarketState;
    use crate::matcher::order_book::{LevelSnapshot, OrderBookSnapshot};

    #[tokio::test]
    async fn stores_latest_market_snapshot_by_code() {
        let state = MarketState::new();
        state.update(
            "300274.XSHE",
            1_000,
            Some(101_000),
            &OrderBookSnapshot {
                bids: vec![LevelSnapshot {
                    price: 100_000,
                    total_qty: 200,
                    order_count: 2,
                }],
                asks: vec![LevelSnapshot {
                    price: 101_000,
                    total_qty: 300,
                    order_count: 3,
                }],
            },
        );

        let snapshot = state.get("300274.XSHE").unwrap();
        assert_eq!(snapshot.timestamp_ms, 1_000);
        assert_eq!(snapshot.last_price, Some(101_000));
        assert_eq!(snapshot.bid1_price, Some(100_000));
        assert_eq!(snapshot.bid1_qty, Some(200));
        assert_eq!(snapshot.ask1_price, Some(101_000));
        assert_eq!(snapshot.ask1_qty, Some(300));
        assert!(state.get("600000.XSHG").is_none());
    }
}
