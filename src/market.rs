use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::event_bus::{AppEvent, EventBus};
use crate::matcher::order_book::{LevelSnapshot, OrderBookSnapshot};

const INTRADAY_BUCKET_MS: i64 = 3_000;
const MAX_INTRADAY_POINTS: usize = 2_000;

#[derive(Debug, Clone, Serialize)]
pub struct MarketSnapshotView {
    pub code: String,
    pub timestamp_ms: i64,
    pub last_price: Option<i64>,
    pub auction_price: Option<i64>,
    pub auction_qty: Option<i64>,
    pub bids: Vec<MarketLevelView>,
    pub asks: Vec<MarketLevelView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketIntradayView {
    pub code: String,
    pub points: Vec<MarketPricePoint>,
    pub next_seq: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketLevelView {
    pub price: i64,
    pub qty: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketPricePoint {
    pub seq: u64,
    pub timestamp_ms: i64,
    pub price: i64,
}

#[derive(Debug, Clone)]
struct MarketSnapshot {
    timestamp_ms: i64,
    last_price: Option<i64>,
    auction_price: Option<i64>,
    auction_qty: Option<i64>,
    snapshot: OrderBookSnapshot,
    intraday_points: Vec<MarketPricePoint>,
    next_intraday_seq: u64,
}

#[derive(Debug, Clone, Default)]
pub struct MarketState {
    snapshots: Arc<RwLock<HashMap<String, MarketSnapshot>>>,
    event_bus: Option<EventBus>,
}

impl MarketState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_event_bus(event_bus: EventBus) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            event_bus: Some(event_bus),
        }
    }

    pub fn update(
        &self,
        code: &str,
        timestamp_ms: i64,
        last_price: Option<i64>,
        is_call_auction: bool,
        snapshot: &OrderBookSnapshot,
    ) {
        {
            let mut snapshots = self.snapshots.write().expect("market state lock poisoned");
            let existing_points = snapshots
                .get(code)
                .map(|snapshot| snapshot.intraday_points.clone())
                .unwrap_or_default();
            let existing_next_seq = snapshots
                .get(code)
                .map(|snapshot| snapshot.next_intraday_seq)
                .unwrap_or(1);
            let (intraday_points, next_intraday_seq) =
                next_intraday_points(existing_points, existing_next_seq, timestamp_ms, last_price);
            snapshots.insert(
                code.to_string(),
                MarketSnapshot {
                    timestamp_ms,
                    last_price,
                    auction_price: auction_price(snapshot, is_call_auction),
                    auction_qty: auction_qty(snapshot, is_call_auction),
                    snapshot: snapshot.clone(),
                    intraday_points,
                    next_intraday_seq,
                },
            );
        }

        if let Some(event_bus) = &self.event_bus {
            event_bus.publish(AppEvent::MarketChanged {
                code: code.to_string(),
            });
        }
    }

    pub fn get(&self, code: &str) -> Option<MarketSnapshotView> {
        let snapshots = self.snapshots.read().expect("market state lock poisoned");
        let snapshot = snapshots.get(code)?;
        Some(MarketSnapshotView {
            code: code.to_string(),
            timestamp_ms: snapshot.timestamp_ms,
            last_price: snapshot.last_price,
            auction_price: snapshot.auction_price,
            auction_qty: snapshot.auction_qty,
            bids: levels_to_view(&snapshot.snapshot.bids),
            asks: levels_to_view(&snapshot.snapshot.asks),
        })
    }

    pub fn intraday(&self, code: &str, from_seq: u64) -> Option<MarketIntradayView> {
        let snapshots = self.snapshots.read().expect("market state lock poisoned");
        let snapshot = snapshots.get(code)?;
        let points = snapshot
            .intraday_points
            .iter()
            .filter(|point| point.seq >= from_seq)
            .cloned()
            .collect();
        Some(MarketIntradayView {
            code: code.to_string(),
            points,
            next_seq: snapshot.next_intraday_seq,
        })
    }
}

fn auction_price(snapshot: &OrderBookSnapshot, is_call_auction: bool) -> Option<i64> {
    if !is_call_auction {
        return None;
    }
    let bid = snapshot.bids.first()?;
    let ask = snapshot.asks.first()?;
    (bid.price == ask.price).then_some(bid.price)
}

fn auction_qty(snapshot: &OrderBookSnapshot, is_call_auction: bool) -> Option<i64> {
    if !is_call_auction {
        return None;
    }
    let bid = snapshot.bids.first()?;
    let ask = snapshot.asks.first()?;
    (bid.price == ask.price).then_some(bid.total_qty.min(ask.total_qty))
}

fn levels_to_view(levels: &[LevelSnapshot]) -> Vec<MarketLevelView> {
    levels
        .iter()
        .take(5)
        .map(|level| MarketLevelView {
            price: level.price,
            qty: level.total_qty,
        })
        .collect()
}

fn next_intraday_points(
    mut points: Vec<MarketPricePoint>,
    mut next_seq: u64,
    timestamp_ms: i64,
    last_price: Option<i64>,
) -> (Vec<MarketPricePoint>, u64) {
    let Some(price) = last_price else {
        return (points, next_seq);
    };
    if let Some(last_point) = points.last_mut() {
        let last_bucket = last_point.timestamp_ms.div_euclid(INTRADAY_BUCKET_MS);
        let next_bucket = timestamp_ms.div_euclid(INTRADAY_BUCKET_MS);
        if last_bucket == next_bucket {
            last_point.seq = next_seq;
            last_point.timestamp_ms = timestamp_ms;
            last_point.price = price;
            next_seq += 1;
            return (points, next_seq);
        }
    }

    points.push(MarketPricePoint {
        seq: next_seq,
        timestamp_ms,
        price,
    });
    next_seq += 1;
    if points.len() > MAX_INTRADAY_POINTS {
        let drop_count = points.len() - MAX_INTRADAY_POINTS;
        points.drain(0..drop_count);
    }
    (points, next_seq)
}

#[cfg(test)]
mod tests {
    use super::{INTRADAY_BUCKET_MS, MarketState};
    use crate::matcher::order_book::{LevelSnapshot, OrderBookSnapshot};

    #[tokio::test]
    async fn stores_latest_market_snapshot_by_code() {
        let state = MarketState::new();
        state.update(
            "300274.XSHE",
            1_000,
            Some(101_000),
            true,
            &OrderBookSnapshot {
                bids: vec![LevelSnapshot {
                    price: 100_000,
                    total_qty: 200,
                    order_count: 2,
                }],
                asks: vec![LevelSnapshot {
                    price: 100_000,
                    total_qty: 300,
                    order_count: 3,
                }],
            },
        );

        let snapshot = state.get("300274.XSHE").unwrap();
        assert_eq!(snapshot.timestamp_ms, 1_000);
        assert_eq!(snapshot.last_price, Some(101_000));
        assert_eq!(snapshot.auction_price, Some(100_000));
        assert_eq!(snapshot.auction_qty, Some(200));
        assert_eq!(snapshot.bids[0].price, 100_000);
        assert_eq!(snapshot.bids[0].qty, 200);
        assert_eq!(snapshot.asks[0].price, 100_000);
        assert_eq!(snapshot.asks[0].qty, 300);
        let intraday = state.intraday("300274.XSHE", 0).unwrap();
        assert_eq!(intraday.points[0].price, 101_000);
        assert!(state.get("600000.XSHG").is_none());
    }

    #[tokio::test]
    async fn buckets_intraday_points_by_thirty_seconds() {
        let state = MarketState::new();
        let snapshot = OrderBookSnapshot::default();
        state.update("300274.XSHE", 1_000, Some(100_000), false, &snapshot);
        state.update(
            "300274.XSHE",
            INTRADAY_BUCKET_MS - 1,
            Some(101_000),
            false,
            &snapshot,
        );
        state.update(
            "300274.XSHE",
            INTRADAY_BUCKET_MS + 1,
            Some(102_000),
            false,
            &snapshot,
        );

        let view = state.intraday("300274.XSHE", 0).unwrap();
        assert_eq!(view.points.len(), 2);
        assert_eq!(view.points[0].seq, 2);
        assert_eq!(view.points[0].timestamp_ms, INTRADAY_BUCKET_MS - 1);
        assert_eq!(view.points[0].price, 101_000);
        assert_eq!(view.points[1].seq, 3);
        assert_eq!(view.points[1].timestamp_ms, INTRADAY_BUCKET_MS + 1);
        assert_eq!(view.points[1].price, 102_000);
        assert_eq!(view.next_seq, 4);
    }

    #[tokio::test]
    async fn returns_intraday_points_from_requested_sequence() {
        let state = MarketState::new();
        let snapshot = OrderBookSnapshot::default();
        state.update("300274.XSHE", 1_000, Some(100_000), false, &snapshot);
        state.update(
            "300274.XSHE",
            INTRADAY_BUCKET_MS + 1,
            Some(101_000),
            false,
            &snapshot,
        );

        let view = state.intraday("300274.XSHE", 2).unwrap();
        assert_eq!(view.points.len(), 1);
        assert_eq!(view.points[0].seq, 2);
        assert_eq!(view.points[0].price, 101_000);
        assert_eq!(view.next_seq, 3);
    }
}
