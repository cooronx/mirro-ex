use std::collections::{BTreeMap, HashMap, VecDeque};

use thiserror::Error;

use crate::common::{L2Order, L2Transaction, Market, OrderDirection, OrderType};

type OrderId = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PriceLevel {
    orders: VecDeque<OrderId>,
    total_qty: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelSnapshot {
    pub price: i64,
    pub total_qty: i64,
    pub order_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderBookSnapshot {
    pub bids: Vec<LevelSnapshot>,
    pub asks: Vec<LevelSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderBookStats {
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub bid_slots: usize,
    pub ask_slots: usize,
    pub live_orders: usize,
    pub stale_slots: usize,
    pub pending_cancels: usize,
    pub pending_reductions: usize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OrderBookError {
    #[error("unsupported order type: {0:?}")]
    UnsupportedOrderType(OrderType),
    #[error("unsupported order direction: {0:?}")]
    UnsupportedDirection(OrderDirection),
    #[error("cancel order is not expected from order stream for market: {0:?}")]
    UnexpectedOrderStreamCancel(Market),
    #[error("cancel transaction is not expected from transaction stream for market: {0:?}")]
    UnexpectedTransactionStreamCancel(Market),
    #[error("non-positive order volume: {0}")]
    InvalidOrderVolume(i64),
    #[error("non-positive transaction volume: {0}")]
    InvalidTransactionVolume(i64),
    #[error("duplicate order id in book: {0}")]
    DuplicateOrderId(OrderId),
}

pub type Result<T> = std::result::Result<T, OrderBookError>;

#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<i64, PriceLevel>,
    asks: BTreeMap<i64, PriceLevel>,
    order_hash: HashMap<OrderId, L2Order>,
    pending_cancels: HashMap<OrderId, i64>,
    pending_reductions: HashMap<OrderId, i64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_order(&mut self, order: L2Order) -> Result<()> {
        match order.order_type {
            OrderType::Limit => self.submit_limit_order(order),
            OrderType::Cancel => {
                if order.market != Market::XSHG {
                    return Err(OrderBookError::UnexpectedOrderStreamCancel(order.market));
                }

                self.cancel_order_volume(
                    Self::cancel_target_order_id(&order),
                    Self::cancel_qty_for_order_event(&order),
                );
                Ok(())
            }
            other => Err(OrderBookError::UnsupportedOrderType(other)),
        }
    }

    pub fn apply_transaction(&mut self, transaction: L2Transaction) -> Result<()> {
        if transaction.volume <= 0 {
            return Err(OrderBookError::InvalidTransactionVolume(transaction.volume));
        }

        match transaction.market {
            Market::XSHG => match transaction.deal_type.trim() {
                "B" | "S" | "N" => {
                    if transaction.buy_order_number > 0 {
                        self.reduce_order_if_present(
                            transaction.buy_order_number,
                            transaction.volume,
                        );
                    }
                    if transaction.sell_order_number > 0 {
                        self.reduce_order_if_present(
                            transaction.sell_order_number,
                            transaction.volume,
                        );
                    }
                }
                _ => {
                    return Err(OrderBookError::UnexpectedTransactionStreamCancel(
                        Market::XSHG,
                    ));
                }
            },
            Market::XSHE => match transaction.deal_type.trim() {
                "4" => {
                    self.cancel_transaction_orders(&transaction);
                }
                "F" => {
                    if transaction.buy_order_number > 0 {
                        self.reduce_order(transaction.buy_order_number, transaction.volume);
                    }
                    if transaction.sell_order_number > 0 {
                        self.reduce_order(transaction.sell_order_number, transaction.volume);
                    }
                }
                _ => {
                    return Err(OrderBookError::UnexpectedTransactionStreamCancel(
                        Market::XSHE,
                    ));
                }
            },
            Market::Unknown => {
                if transaction.buy_order_number > 0 {
                    self.reduce_order(transaction.buy_order_number, transaction.volume);
                }
                if transaction.sell_order_number > 0 {
                    self.reduce_order(transaction.sell_order_number, transaction.volume);
                }
            }
        }

        Ok(())
    }

    pub fn cancel_order(&mut self, order_id: OrderId) -> bool {
        self.cancel_order_volume(order_id, i64::MAX) > 0
    }

    pub fn snapshot(&mut self, depth: usize) -> OrderBookSnapshot {
        self.cleanup_all_levels();

        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .filter_map(|(&price, level)| self.level_snapshot(price, level))
            .collect();
        let asks = self
            .asks
            .iter()
            .take(depth)
            .filter_map(|(&price, level)| self.level_snapshot(price, level))
            .collect();

        OrderBookSnapshot { bids, asks }
    }

    pub fn best_bid_price(&mut self) -> Option<i64> {
        self.best_price(BookSide::Bid)
    }

    pub fn best_ask_price(&mut self) -> Option<i64> {
        self.best_price(BookSide::Ask)
    }

    pub fn stats(&self) -> OrderBookStats {
        let bid_slots = Self::side_slots(&self.bids);
        let ask_slots = Self::side_slots(&self.asks);
        let live_orders = self.order_hash.len();

        OrderBookStats {
            bid_levels: self.bids.len(),
            ask_levels: self.asks.len(),
            bid_slots,
            ask_slots,
            live_orders,
            stale_slots: bid_slots
                .saturating_add(ask_slots)
                .saturating_sub(live_orders),
            pending_cancels: self.pending_cancels.len(),
            pending_reductions: self.pending_reductions.len(),
        }
    }

    pub fn verify_invariants(&self) -> std::result::Result<(), String> {
        let mut seen_live_orders: HashMap<OrderId, (BookSide, i64, usize)> = HashMap::new();

        self.verify_side_invariants(BookSide::Bid, &self.bids, &mut seen_live_orders)?;
        self.verify_side_invariants(BookSide::Ask, &self.asks, &mut seen_live_orders)?;

        for (&order_id, order) in &self.order_hash {
            if order.volume <= 0 {
                return Err(format!(
                    "live order has non-positive volume: order_id={} volume={}",
                    order_id, order.volume
                ));
            }

            let expected_side = Self::book_side_for_direction(order.direction).map_err(|err| {
                format!(
                    "live order has invalid direction: order_id={} error={err}",
                    order_id
                )
            })?;

            let Some((actual_side, actual_price, occurrences)) = seen_live_orders.get(&order_id)
            else {
                return Err(format!(
                    "live order is missing from price levels: order_id={} expected_side={:?} expected_price={}",
                    order_id, expected_side, order.price
                ));
            };

            if *occurrences != 1 {
                return Err(format!(
                    "live order appears multiple times in price levels: order_id={} occurrences={}",
                    order_id, occurrences
                ));
            }

            if *actual_side != expected_side || *actual_price != order.price {
                return Err(format!(
                    "live order is attached to wrong level: order_id={} expected_side={:?} expected_price={} actual_side={:?} actual_price={}",
                    order_id, expected_side, order.price, actual_side, actual_price
                ));
            }
        }

        for (&order_id, &qty) in &self.pending_cancels {
            if qty <= 0 {
                return Err(format!(
                    "pending cancel has non-positive quantity: order_id={} qty={}",
                    order_id, qty
                ));
            }
        }

        for (&order_id, &qty) in &self.pending_reductions {
            if qty <= 0 {
                return Err(format!(
                    "pending reduction has non-positive quantity: order_id={} qty={}",
                    order_id, qty
                ));
            }
        }

        Ok(())
    }

    fn submit_limit_order(&mut self, order: L2Order) -> Result<()> {
        let order_id = Self::order_id(&order);
        let mut order = order;
        let pending_cancel = self.pending_cancels.remove(&order_id).unwrap_or(0);
        let pending_reduction = if order.market == Market::XSHG {
            0
        } else {
            self.pending_reductions.remove(&order_id).unwrap_or(0)
        };
        let total_pending = pending_cancel.saturating_add(pending_reduction);
        if total_pending > 0 {
            if total_pending >= order.volume {
                return Ok(());
            }
            order.volume -= total_pending;
        }

        if order.volume <= 0 {
            return Err(OrderBookError::InvalidOrderVolume(order.volume));
        }

        let side = Self::book_side_for_direction(order.direction)?;
        if self.order_hash.contains_key(&order_id) {
            return Err(OrderBookError::DuplicateOrderId(order_id));
        }

        let level = self.book_side_mut(side).entry(order.price).or_default();
        level.orders.push_back(order_id);
        level.total_qty += order.volume;
        self.order_hash.insert(order_id, order);
        Ok(())
    }

    fn reduce_order(&mut self, order_id: OrderId, matched_qty: i64) -> i64 {
        let Some((side, price, remaining_qty)) = self.order_hash.get(&order_id).map(|order| {
            (
                Self::book_side_for_direction(order.direction).ok(),
                order.price,
                order.volume,
            )
        }) else {
            self.pending_reductions
                .entry(order_id)
                .and_modify(|value| *value = value.saturating_add(matched_qty))
                .or_insert(matched_qty);
            return 0;
        };

        let Some(side) = side else {
            return 0;
        };

        let reduced_qty = remaining_qty.min(matched_qty);
        if reduced_qty <= 0 {
            return 0;
        }

        let remove_order = if let Some(order) = self.order_hash.get_mut(&order_id) {
            order.volume -= reduced_qty;
            order.volume <= 0
        } else {
            false
        };

        if remove_order {
            self.order_hash.remove(&order_id);
        }

        self.adjust_level_total_qty(side, price, reduced_qty);
        self.remove_empty_level_if_drained(side, price);
        reduced_qty
    }

    fn reduce_order_if_present(&mut self, order_id: OrderId, matched_qty: i64) -> i64 {
        if self.order_hash.contains_key(&order_id) {
            self.reduce_order(order_id, matched_qty)
        } else {
            0
        }
    }

    fn book_side_mut(&mut self, side: BookSide) -> &mut BTreeMap<i64, PriceLevel> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn book_side_for_direction(direction: OrderDirection) -> Result<BookSide> {
        match direction {
            OrderDirection::Buy => Ok(BookSide::Bid),
            OrderDirection::Sell => Ok(BookSide::Ask),
            other => Err(OrderBookError::UnsupportedDirection(other)),
        }
    }

    fn order_id(order: &L2Order) -> OrderId {
        if order.extra_message_number > 0 {
            order.extra_message_number
        } else {
            order.channel_number
        }
    }

    fn cancel_target_order_id(order: &L2Order) -> OrderId {
        Self::order_id(order)
    }

    fn cancel_qty_for_order_event(order: &L2Order) -> i64 {
        if order.volume > 0 {
            order.volume
        } else {
            i64::MAX
        }
    }

    fn cancel_transaction_orders(&mut self, transaction: &L2Transaction) {
        let cancel_qty = if transaction.volume > 0 {
            transaction.volume
        } else {
            i64::MAX
        };

        if transaction.buy_order_number > 0 {
            self.cancel_order_volume(transaction.buy_order_number, cancel_qty);
        }
        if transaction.sell_order_number > 0
            && transaction.sell_order_number != transaction.buy_order_number
        {
            self.cancel_order_volume(transaction.sell_order_number, cancel_qty);
        }
    }

    fn cancel_order_volume(&mut self, order_id: OrderId, cancel_qty: i64) -> i64 {
        let Some((side, price, remaining_qty)) = self.order_hash.get(&order_id).map(|order| {
            (
                Self::book_side_for_direction(order.direction).ok(),
                order.price,
                order.volume,
            )
        }) else {
            self.pending_cancels
                .entry(order_id)
                .and_modify(|value| *value = value.saturating_add(cancel_qty))
                .or_insert(cancel_qty);
            return 0;
        };

        let Some(side) = side else {
            return 0;
        };

        let reduced_qty = remaining_qty.min(cancel_qty);
        if reduced_qty <= 0 {
            return 0;
        }

        let remove_order = if let Some(order) = self.order_hash.get_mut(&order_id) {
            order.volume -= reduced_qty;
            order.volume <= 0
        } else {
            false
        };

        if remove_order {
            self.order_hash.remove(&order_id);
        }

        self.adjust_level_total_qty(side, price, reduced_qty);
        self.remove_empty_level_if_drained(side, price);
        reduced_qty
    }

    fn adjust_level_total_qty(&mut self, side: BookSide, price: i64, delta: i64) {
        if let Some(level) = self.book_side_mut(side).get_mut(&price) {
            level.total_qty = level.total_qty.saturating_sub(delta);
        }
    }

    fn remove_empty_level_if_drained(&mut self, side: BookSide, price: i64) {
        let should_remove = self.compact_level_front(side, price);
        if should_remove {
            self.book_side_mut(side).remove(&price);
        }
    }

    fn compact_level_front(&mut self, side: BookSide, price: i64) -> bool {
        loop {
            let front_order_id = {
                let Some(level) = self.book_side_mut(side).get_mut(&price) else {
                    return true;
                };
                level.orders.front().copied()
            };

            let Some(order_id) = front_order_id else {
                break;
            };

            if self.order_hash.contains_key(&order_id) {
                break;
            }

            if let Some(level) = self.book_side_mut(side).get_mut(&price) {
                level.orders.pop_front();
            } else {
                return true;
            }
        }

        let Some(level) = self.book_side_mut(side).get_mut(&price) else {
            return true;
        };

        level.orders.is_empty() || level.total_qty <= 0
    }

    fn cleanup_all_levels(&mut self) {
        self.cleanup_side(BookSide::Bid);
        self.cleanup_side(BookSide::Ask);
    }

    fn cleanup_side(&mut self, side: BookSide) {
        let prices: Vec<i64> = match side {
            BookSide::Bid => self.bids.keys().copied().collect(),
            BookSide::Ask => self.asks.keys().copied().collect(),
        };

        for price in prices {
            if self.compact_level_front(side, price) {
                self.book_side_mut(side).remove(&price);
            }
        }
    }

    fn best_price(&mut self, side: BookSide) -> Option<i64> {
        loop {
            let price = match side {
                BookSide::Bid => self.bids.last_key_value().map(|(&price, _)| price),
                BookSide::Ask => self.asks.first_key_value().map(|(&price, _)| price),
            }?;

            if self.compact_level_front(side, price) {
                self.book_side_mut(side).remove(&price);
                continue;
            }

            return Some(price);
        }
    }

    fn level_snapshot(&self, price: i64, level: &PriceLevel) -> Option<LevelSnapshot> {
        let order_count = level
            .orders
            .iter()
            .filter(|order_id| self.order_hash.contains_key(order_id))
            .count();

        if order_count == 0 || level.total_qty <= 0 {
            return None;
        }

        Some(LevelSnapshot {
            price,
            total_qty: level.total_qty,
            order_count,
        })
    }

    fn side_slots(levels: &BTreeMap<i64, PriceLevel>) -> usize {
        levels.values().map(|level| level.orders.len()).sum()
    }

    fn verify_side_invariants(
        &self,
        side: BookSide,
        levels: &BTreeMap<i64, PriceLevel>,
        seen_live_orders: &mut HashMap<OrderId, (BookSide, i64, usize)>,
    ) -> std::result::Result<(), String> {
        for (&price, level) in levels {
            if level.total_qty < 0 {
                return Err(format!(
                    "price level has negative total quantity: side={:?} price={} total_qty={}",
                    side, price, level.total_qty
                ));
            }

            let mut computed_total_qty = 0_i64;
            let mut live_order_count = 0_usize;

            for &order_id in &level.orders {
                let Some(order) = self.order_hash.get(&order_id) else {
                    continue;
                };

                let order_side = Self::book_side_for_direction(order.direction).map_err(|err| {
                    format!(
                        "live order in level has invalid direction: order_id={} side={:?} price={} error={err}",
                        order_id, side, price
                    )
                })?;

                if order_side != side || order.price != price {
                    return Err(format!(
                        "live order stale-matched into wrong level: order_id={} level_side={:?} level_price={} order_side={:?} order_price={}",
                        order_id, side, price, order_side, order.price
                    ));
                }

                computed_total_qty += order.volume;
                live_order_count += 1;

                let entry = seen_live_orders.entry(order_id).or_insert((side, price, 0));
                if entry.0 != side || entry.1 != price {
                    return Err(format!(
                        "live order appears in multiple levels: order_id={} first_side={:?} first_price={} duplicate_side={:?} duplicate_price={}",
                        order_id, entry.0, entry.1, side, price
                    ));
                }
                entry.2 += 1;
            }

            if live_order_count == 0 {
                if level.total_qty != 0 {
                    return Err(format!(
                        "empty level still carries quantity: side={:?} price={} total_qty={}",
                        side, price, level.total_qty
                    ));
                }
                continue;
            }

            if computed_total_qty != level.total_qty {
                return Err(format!(
                    "level total quantity mismatch: side={:?} price={} stored_total={} computed_total={} live_order_count={}",
                    side, price, level.total_qty, computed_total_qty, live_order_count
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderBook, OrderBookError};
    use crate::common::{L2Order, L2Transaction, Market, OrderDirection, OrderType};

    fn limit_order(order_id: i64, direction: OrderDirection, price: i64, volume: i64) -> L2Order {
        L2Order {
            market: Market::XSHG,
            channel: 1,
            channel_number: order_id,
            code: "SH600000".to_string(),
            price,
            volume,
            direction,
            order_type: OrderType::Limit,
            timestamp_ms: 1_000,
            extra_message_number: 0,
        }
    }

    fn cancel_order(event_id: i64, target_order_id: i64) -> L2Order {
        L2Order {
            market: Market::XSHG,
            channel: 1,
            channel_number: event_id,
            code: "SH600000".to_string(),
            price: 0,
            volume: 0,
            direction: OrderDirection::Unknown,
            order_type: OrderType::Cancel,
            timestamp_ms: 1_001,
            extra_message_number: target_order_id,
        }
    }

    fn transaction(buy_order_number: i64, sell_order_number: i64, volume: i64) -> L2Transaction {
        L2Transaction {
            market: Market::XSHG,
            channel: 1,
            channel_number: 10,
            code: "SH600000".to_string(),
            timestamp_ms: 1_100,
            price: 100_000,
            volume,
            buy_order_number,
            sell_order_number,
            deal_type: "F".to_string(),
        }
    }

    fn xshg_transaction(
        buy_order_number: i64,
        sell_order_number: i64,
        volume: i64,
        deal_type: &str,
    ) -> L2Transaction {
        let mut tx = transaction(buy_order_number, sell_order_number, volume);
        tx.deal_type = deal_type.to_string();
        tx
    }

    fn sz_cancel_transaction(order_number: i64) -> L2Transaction {
        L2Transaction {
            market: Market::XSHE,
            channel: 1,
            channel_number: 11,
            code: "SZ000001".to_string(),
            timestamp_ms: 1_101,
            price: 0,
            volume: 0,
            buy_order_number: order_number,
            sell_order_number: 0,
            deal_type: "4".to_string(),
        }
    }

    #[test]
    fn inserts_limit_orders_and_keeps_fifo() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Buy, 100_000, 20))
            .unwrap();
        book.apply_order(limit_order(3, OrderDirection::Sell, 101_000, 30))
            .unwrap();

        let snapshot = book.snapshot(10);

        assert_eq!(book.best_bid_price(), Some(100_000));
        assert_eq!(book.best_ask_price(), Some(101_000));
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.bids[0].price, 100_000);
        assert_eq!(snapshot.bids[0].total_qty, 30);
        assert_eq!(snapshot.bids[0].order_count, 2);
        assert_eq!(snapshot.asks[0].price, 101_000);
        assert_eq!(snapshot.asks[0].total_qty, 30);
        assert_eq!(
            book.bids
                .get(&100_000)
                .unwrap()
                .orders
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn cancel_uses_lazy_delete_and_updates_snapshot() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Buy, 100_000, 20))
            .unwrap();
        book.apply_order(cancel_order(3, 1)).unwrap();

        let snapshot = book.snapshot(10);

        assert!(!book.order_hash.contains_key(&1));
        assert_eq!(snapshot.bids[0].total_qty, 20);
        assert_eq!(snapshot.bids[0].order_count, 1);
        assert_eq!(book.best_bid_price(), Some(100_000));
        assert_eq!(
            book.bids
                .get(&100_000)
                .unwrap()
                .orders
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn cancel_reduces_only_cancelled_quantity() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();

        let mut cancel = cancel_order(2, 1);
        cancel.volume = 4;
        book.apply_order(cancel).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 6);
        assert_eq!(snapshot.bids[0].order_count, 1);
        assert_eq!(book.order_hash.get(&1).unwrap().volume, 6);
    }

    #[test]
    fn rejects_shenzhen_cancel_from_order_stream() {
        let mut book = OrderBook::new();
        let mut order = cancel_order(3, 1);
        order.market = Market::XSHE;

        let err = book.apply_order(order).unwrap_err();
        assert_eq!(
            err,
            OrderBookError::UnexpectedOrderStreamCancel(Market::XSHE)
        );
    }

    #[test]
    fn transaction_reduces_orders_and_removes_filled_ones() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Sell, 101_000, 12))
            .unwrap();

        book.apply_transaction(transaction(1, 2, 4)).unwrap();
        assert_eq!(book.order_hash.get(&1).unwrap().volume, 6);
        assert_eq!(book.order_hash.get(&2).unwrap().volume, 8);

        book.apply_transaction(transaction(1, 2, 8)).unwrap();

        let snapshot = book.snapshot(10);
        assert!(!book.order_hash.contains_key(&1));
        assert!(!book.order_hash.contains_key(&2));
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert_eq!(book.best_bid_price(), None);
        assert_eq!(book.best_ask_price(), None);
    }

    #[test]
    fn xshg_buy_aggressor_trade_reduces_both_sides() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Sell, 101_000, 12))
            .unwrap();

        book.apply_transaction(xshg_transaction(1, 2, 4, "B"))
            .unwrap();

        assert_eq!(book.order_hash.get(&1).unwrap().volume, 6);
        assert_eq!(book.order_hash.get(&2).unwrap().volume, 8);
    }

    #[test]
    fn xshg_sell_aggressor_trade_reduces_both_sides() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Sell, 101_000, 12))
            .unwrap();

        book.apply_transaction(xshg_transaction(1, 2, 4, "S"))
            .unwrap();

        assert_eq!(book.order_hash.get(&1).unwrap().volume, 6);
        assert_eq!(book.order_hash.get(&2).unwrap().volume, 8);
    }

    #[test]
    fn xshg_missing_buy_side_still_reduces_present_sell_side() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(2, OrderDirection::Sell, 101_000, 12))
            .unwrap();

        book.apply_transaction(xshg_transaction(1, 2, 4, "B"))
            .unwrap();
        assert_eq!(book.order_hash.get(&2).unwrap().volume, 8);
        assert!(book.pending_reductions.is_empty());
    }

    #[test]
    fn xshg_missing_both_sides_does_not_create_pending_reduction() {
        let mut book = OrderBook::new();

        book.apply_transaction(xshg_transaction(1, 2, 4, "B"))
            .unwrap();

        assert!(book.pending_reductions.is_empty());
    }

    #[test]
    fn xshg_n_trade_reduces_both_sides() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Sell, 101_000, 12))
            .unwrap();

        book.apply_transaction(xshg_transaction(1, 2, 4, "N"))
            .unwrap();

        assert_eq!(book.order_hash.get(&1).unwrap().volume, 6);
        assert_eq!(book.order_hash.get(&2).unwrap().volume, 8);
    }

    #[test]
    fn shenzhen_cancel_transaction_removes_order_from_book() {
        let mut book = OrderBook::new();
        let mut order = limit_order(1, OrderDirection::Buy, 100_000, 10);
        order.market = Market::XSHE;

        book.apply_order(order).unwrap();
        book.apply_transaction(sz_cancel_transaction(1)).unwrap();

        let snapshot = book.snapshot(10);
        assert!(snapshot.bids.is_empty());
        assert!(!book.order_hash.contains_key(&1));
    }

    #[test]
    fn transaction_references_exchange_order_number_not_message_number() {
        let mut book = OrderBook::new();
        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.extra_message_number = 88;

        book.apply_order(order).unwrap();
        book.apply_transaction(transaction(88, 0, 4)).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 6);
        assert_eq!(snapshot.bids[0].order_count, 1);
    }

    #[test]
    fn pending_cancel_removes_late_arriving_order() {
        let mut book = OrderBook::new();

        assert!(!book.cancel_order(88));

        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.extra_message_number = 88;
        book.apply_order(order).unwrap();

        let snapshot = book.snapshot(10);
        assert!(snapshot.bids.is_empty());
        assert!(!book.order_hash.contains_key(&88));
    }

    #[test]
    fn pending_partial_cancel_applies_to_late_arriving_order() {
        let mut book = OrderBook::new();

        let mut cancel = cancel_order(3, 88);
        cancel.volume = 4;
        book.apply_order(cancel).unwrap();

        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.extra_message_number = 88;
        book.apply_order(order).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 6);
        assert_eq!(snapshot.bids[0].order_count, 1);
        assert_eq!(book.order_hash.get(&88).unwrap().volume, 6);
    }

    #[test]
    fn pending_transaction_reduction_applies_to_late_arriving_order() {
        let mut book = OrderBook::new();

        let mut tx = transaction(88, 0, 4);
        tx.market = Market::XSHE;
        tx.deal_type = "F".to_string();
        book.apply_transaction(tx).unwrap();

        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.market = Market::XSHE;
        order.extra_message_number = 88;
        book.apply_order(order).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 6);
        assert_eq!(snapshot.bids[0].order_count, 1);
    }

    #[test]
    fn shanghai_late_arriving_order_ignores_pending_transaction_reduction() {
        let mut book = OrderBook::new();

        book.apply_transaction(transaction(88, 0, 4)).unwrap();

        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.extra_message_number = 88;
        book.apply_order(order).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 10);
        assert_eq!(snapshot.bids[0].order_count, 1);
    }

    #[test]
    fn ignores_missing_transaction_references() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_transaction(transaction(1, 999, 3)).unwrap();

        assert_eq!(book.order_hash.get(&1).unwrap().volume, 7);
        assert_eq!(book.snapshot(10).bids[0].total_qty, 7);
    }

    #[test]
    fn rejects_unsupported_order_type() {
        let mut book = OrderBook::new();
        let mut order = limit_order(1, OrderDirection::Buy, 100_000, 10);
        order.order_type = OrderType::Market;

        let err = book.apply_order(order).unwrap_err();
        assert_eq!(err, OrderBookError::UnsupportedOrderType(OrderType::Market));
    }
}
