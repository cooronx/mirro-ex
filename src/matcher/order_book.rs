use std::collections::{BTreeMap, HashMap};

use thiserror::Error;
use tracing::warn;

use crate::common::{L2Order, L2Transaction, Market, OrderDirection, OrderType};

type OrderId = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PriceLevel {
    total_qty: i64,
    active_order_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HoldingOrder {
    order: L2Order,
    remaining_volume: i64,
    resting_price: Option<i64>,
    has_trade: bool,
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
    holding_orders: HashMap<OrderId, HoldingOrder>,
    pending_cancels: HashMap<OrderId, i64>,
    pending_reductions: HashMap<OrderId, i64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_order(&mut self, order: L2Order) -> Result<()> {
        if order.market == Market::XSHE {
            self.finalize_traded_holdings()?;
        }

        match order.order_type {
            OrderType::Limit => self.submit_limit_order(order),
            OrderType::BestOwn => self.submit_best_own_order(order),
            OrderType::Market => self.submit_holding_market_order(order),
            OrderType::Cancel => {
                if order.market != Market::XSHG {
                    return Err(OrderBookError::UnexpectedOrderStreamCancel(order.market));
                }

                self.cancel_order_volume(
                    Self::order_id(&order),
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
                    if transaction.buy_number > 0 {
                        self.reduce_visible_order(transaction.buy_number, transaction.volume);
                    }
                    if transaction.sell_number > 0 {
                        self.reduce_visible_order(transaction.sell_number, transaction.volume);
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
                    self.process_shenzhen_trade(&transaction);
                }
                _ => {
                    return Err(OrderBookError::UnexpectedTransactionStreamCancel(
                        Market::XSHE,
                    ));
                }
            },
            Market::Unknown => {
                if transaction.buy_number > 0 {
                    self.reduce_order(transaction.buy_number, transaction.volume);
                }
                if transaction.sell_number > 0 {
                    self.reduce_order(transaction.sell_number, transaction.volume);
                }
            }
        }

        Ok(())
    }

    pub fn cancel_order(&mut self, order_id: OrderId) -> bool {
        self.cancel_order_volume(order_id, i64::MAX) > 0
    }

    pub fn snapshot(&mut self, depth: usize) -> OrderBookSnapshot {
        let bids = self
            .bids
            .iter()
            .rev()
            .filter_map(|(&price, level)| self.level_snapshot(price, level))
            .take(depth)
            .collect();
        let asks = self
            .asks
            .iter()
            .filter_map(|(&price, level)| self.level_snapshot(price, level))
            .take(depth)
            .collect();

        OrderBookSnapshot { bids, asks }
    }

    pub fn best_bid_price(&mut self) -> Option<i64> {
        self.best_price(BookSide::Bid)
    }

    pub fn best_ask_price(&mut self) -> Option<i64> {
        self.best_price(BookSide::Ask)
    }

    pub fn has_unsettled_holdings(&self) -> bool {
        !self.holding_orders.is_empty()
    }

    pub fn finalize_all_holdings(&mut self) -> Result<bool> {
        let holding_ids = self.holding_orders.keys().copied().collect::<Vec<_>>();
        self.finalize_holdings(&holding_ids)
    }

    fn finalize_traded_holdings(&mut self) -> Result<bool> {
        let holding_ids = self
            .holding_orders
            .iter()
            .filter_map(|(&order_id, holding)| holding.has_trade.then_some(order_id))
            .collect::<Vec<_>>();
        self.finalize_holdings(&holding_ids)
    }

    fn submit_limit_order(&mut self, mut order: L2Order) -> Result<()> {
        let order_id = Self::order_id(&order);
        if self.apply_pending_to_order(order_id, &mut order)? {
            return Ok(());
        }

        self.insert_visible_order(order)
    }

    fn insert_visible_order(&mut self, order: L2Order) -> Result<()> {
        let order_id = Self::order_id(&order);
        let side = Self::book_side_for_direction(order.direction)?;
        if self.contains_order_id(order_id) {
            return Err(OrderBookError::DuplicateOrderId(order_id));
        }

        let level = self.book_side_mut(side).entry(order.price).or_default();
        level.total_qty += order.volume;
        level.active_order_count += 1;
        self.order_hash.insert(order_id, order);
        Ok(())
    }

    fn submit_best_own_order(&mut self, mut order: L2Order) -> Result<()> {
        let Some(price) = self.best_own_price(order.direction)? else {
            return Ok(());
        };

        order.price = price;
        order.order_type = OrderType::Limit;
        self.submit_limit_order(order)
    }

    fn submit_holding_market_order(&mut self, mut order: L2Order) -> Result<()> {
        let order_id = Self::order_id(&order);
        if self.apply_pending_to_order(order_id, &mut order)? {
            return Ok(());
        }

        if self.contains_order_id(order_id) {
            return Err(OrderBookError::DuplicateOrderId(order_id));
        }

        self.holding_orders.insert(
            order_id,
            HoldingOrder {
                remaining_volume: order.volume,
                order,
                resting_price: None,
                has_trade: false,
            },
        );
        Ok(())
    }

    fn apply_pending_to_order(&mut self, order_id: OrderId, order: &mut L2Order) -> Result<bool> {
        let pending_cancel = self.pending_cancels.remove(&order_id).unwrap_or(0);
        let pending_reduction = if order.market == Market::XSHG {
            0
        } else {
            self.pending_reductions.remove(&order_id).unwrap_or(0)
        };
        let total_pending = pending_cancel.saturating_add(pending_reduction);

        if total_pending > 0 && total_pending >= order.volume {
            return Ok(true);
        }
        order.volume -= total_pending;

        if order.volume <= 0 {
            return Err(OrderBookError::InvalidOrderVolume(order.volume));
        }
        Ok(false)
    }

    fn reduce_order(&mut self, order_id: OrderId, matched_qty: i64) -> i64 {
        if let Some(reduced_qty) = self.reduce_visible_order(order_id, matched_qty) {
            return reduced_qty;
        }

        self.pending_reductions
            .entry(order_id)
            .and_modify(|value| *value = value.saturating_add(matched_qty))
            .or_insert(matched_qty);
        0
    }

    fn reduce_visible_order(&mut self, order_id: OrderId, qty: i64) -> Option<i64> {
        let (side, price, remaining_qty) = self.order_hash.get(&order_id).and_then(|order| {
            Some((
                Self::book_side_for_direction(order.direction).ok()?,
                order.price,
                order.volume,
            ))
        })?;
        let reduced_qty = remaining_qty.min(qty);
        if reduced_qty <= 0 {
            return Some(0);
        }

        let remove_order = if let Some(order) = self.order_hash.get_mut(&order_id) {
            order.volume -= reduced_qty;
            order.volume <= 0
        } else {
            false
        };
        if remove_order {
            self.order_hash.remove(&order_id);
            self.decrement_level_order_count(side, price);
        }

        self.adjust_level_total_qty(side, price, reduced_qty);
        self.remove_empty_level_if_drained(side, price);
        Some(reduced_qty)
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
        if order.order_number > 0 {
            order.order_number
        } else {
            order.message_number
        }
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

        if transaction.buy_number > 0 {
            self.cancel_order_volume(transaction.buy_number, cancel_qty);
        }
        if transaction.sell_number > 0 && transaction.sell_number != transaction.buy_number {
            self.cancel_order_volume(transaction.sell_number, cancel_qty);
        }
    }

    fn cancel_order_volume(&mut self, order_id: OrderId, cancel_qty: i64) -> i64 {
        if let Some(reduced_qty) = self.reduce_visible_order(order_id, cancel_qty) {
            return reduced_qty;
        }

        if let Some(holding) = self.holding_orders.get_mut(&order_id) {
            let reduced_qty = holding.remaining_volume.min(cancel_qty);
            if reduced_qty <= 0 {
                return 0;
            }

            holding.remaining_volume -= reduced_qty;
            if holding.remaining_volume <= 0 {
                self.holding_orders.remove(&order_id);
            }
            return reduced_qty;
        }

        self.pending_cancels
            .entry(order_id)
            .and_modify(|value| *value = value.saturating_add(cancel_qty))
            .or_insert(cancel_qty);
        0
    }

    fn adjust_level_total_qty(&mut self, side: BookSide, price: i64, delta: i64) {
        if let Some(level) = self.book_side_mut(side).get_mut(&price) {
            level.total_qty = level.total_qty.saturating_sub(delta);
        }
    }

    fn decrement_level_order_count(&mut self, side: BookSide, price: i64) {
        if let Some(level) = self.book_side_mut(side).get_mut(&price) {
            level.active_order_count = level.active_order_count.saturating_sub(1);
        }
    }

    fn remove_empty_level_if_drained(&mut self, side: BookSide, price: i64) {
        let should_remove = self
            .book_side_mut(side)
            .get(&price)
            .is_none_or(|level| level.active_order_count == 0 || level.total_qty <= 0);
        if should_remove {
            self.book_side_mut(side).remove(&price);
        }
    }

    fn best_price(&mut self, side: BookSide) -> Option<i64> {
        loop {
            let (price, is_empty) = match side {
                BookSide::Bid => self
                    .bids
                    .last_key_value()
                    .map(|(&price, level)| (price, Self::level_is_empty(level))),
                BookSide::Ask => self
                    .asks
                    .first_key_value()
                    .map(|(&price, level)| (price, Self::level_is_empty(level))),
            }?;

            if is_empty {
                self.book_side_mut(side).remove(&price);
                continue;
            }

            return Some(price);
        }
    }

    fn level_is_empty(level: &PriceLevel) -> bool {
        level.active_order_count == 0 || level.total_qty <= 0
    }

    fn level_snapshot(&self, price: i64, level: &PriceLevel) -> Option<LevelSnapshot> {
        if level.active_order_count == 0 || level.total_qty <= 0 {
            return None;
        }

        Some(LevelSnapshot {
            price,
            total_qty: level.total_qty,
            order_count: level.active_order_count,
        })
    }

    fn contains_order_id(&self, order_id: OrderId) -> bool {
        self.order_hash.contains_key(&order_id) || self.holding_orders.contains_key(&order_id)
    }

    fn best_own_price(&mut self, direction: OrderDirection) -> Result<Option<i64>> {
        match direction {
            OrderDirection::Buy => Ok(self.best_bid_price()),
            OrderDirection::Sell => Ok(self.best_ask_price()),
            other => Err(OrderBookError::UnsupportedDirection(other)),
        }
    }

    fn process_shenzhen_trade(&mut self, transaction: &L2Transaction) {
        let referenced_ids = Self::referenced_order_ids(transaction);
        let holding_ids = self.holding_orders.keys().copied().collect::<Vec<_>>();
        let finalized_ids = holding_ids
            .into_iter()
            .filter(|order_id| !referenced_ids.contains(order_id))
            .collect::<Vec<_>>();
        let _ = self.finalize_holdings(&finalized_ids);

        if transaction.buy_number > 0 {
            self.reduce_order_with_trade_price(
                transaction.buy_number,
                transaction.volume,
                transaction.price,
            );
        }
        if transaction.sell_number > 0 {
            self.reduce_order_with_trade_price(
                transaction.sell_number,
                transaction.volume,
                transaction.price,
            );
        }
    }

    fn reduce_order_with_trade_price(
        &mut self,
        order_id: OrderId,
        matched_qty: i64,
        trade_price: i64,
    ) -> i64 {
        if let Some(reduced_qty) = self.reduce_visible_order(order_id, matched_qty) {
            return reduced_qty;
        }

        if let Some(holding) = self.holding_orders.get_mut(&order_id) {
            let reduced_qty = holding.remaining_volume.min(matched_qty);
            if reduced_qty <= 0 {
                return 0;
            }

            if holding.resting_price.is_none() {
                holding.resting_price = Some(trade_price);
            }
            holding.has_trade = true;
            holding.remaining_volume -= reduced_qty;
            if holding.remaining_volume <= 0 {
                self.holding_orders.remove(&order_id);
            }
            return reduced_qty;
        }

        self.pending_reductions
            .entry(order_id)
            .and_modify(|value| *value = value.saturating_add(matched_qty))
            .or_insert(matched_qty);
        0
    }

    fn finalize_holdings(&mut self, holding_ids: &[OrderId]) -> Result<bool> {
        let mut changed = false;

        for order_id in holding_ids {
            let Some(holding) = self.holding_orders.remove(order_id) else {
                continue;
            };

            if holding.remaining_volume <= 0 {
                continue;
            }

            if !holding.has_trade {
                warn!(
                    order_id = *order_id,
                    code = %holding.order.code,
                    market = ?holding.order.market,
                    "dropping shenzhen market order without any following matching trades"
                );
                continue;
            }

            let Some(resting_price) = holding.resting_price else {
                warn!(
                    order_id = *order_id,
                    code = %holding.order.code,
                    market = ?holding.order.market,
                    "dropping shenzhen market order with residual volume but no resting price"
                );
                continue;
            };

            let mut visible_order = holding.order;
            visible_order.price = resting_price;
            visible_order.volume = holding.remaining_volume;
            visible_order.order_type = OrderType::Limit;
            self.insert_visible_order(visible_order)?;
            changed = true;
        }

        Ok(changed)
    }

    fn referenced_order_ids(transaction: &L2Transaction) -> Vec<OrderId> {
        let mut order_ids = Vec::with_capacity(2);
        if transaction.buy_number > 0 {
            order_ids.push(transaction.buy_number);
        }
        if transaction.sell_number > 0 && transaction.sell_number != transaction.buy_number {
            order_ids.push(transaction.sell_number);
        }
        order_ids
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
            message_number: order_id,
            code: "600000.XSHG".to_string(),
            price,
            volume,
            direction,
            order_type: OrderType::Limit,
            timestamp_ms: 1_000,
            order_number: 0,
        }
    }

    fn cancel_order(event_id: i64, target_order_id: i64) -> L2Order {
        L2Order {
            market: Market::XSHG,
            channel: 1,
            message_number: event_id,
            code: "600000.XSHG".to_string(),
            price: 0,
            volume: 0,
            direction: OrderDirection::Unknown,
            order_type: OrderType::Cancel,
            timestamp_ms: 1_001,
            order_number: target_order_id,
        }
    }

    fn transaction(buy_number: i64, sell_number: i64, volume: i64) -> L2Transaction {
        L2Transaction {
            market: Market::XSHG,
            channel: 1,
            message_number: 10,
            code: "600000.XSHG".to_string(),
            timestamp_ms: 1_100,
            price: 100_000,
            volume,
            buy_number,
            sell_number,
            deal_type: "F".to_string(),
        }
    }

    fn xshg_transaction(
        buy_number: i64,
        sell_number: i64,
        volume: i64,
        deal_type: &str,
    ) -> L2Transaction {
        let mut tx = transaction(buy_number, sell_number, volume);
        tx.deal_type = deal_type.to_string();
        tx
    }

    fn sz_cancel_transaction(order_number: i64) -> L2Transaction {
        L2Transaction {
            market: Market::XSHE,
            channel: 1,
            message_number: 11,
            code: "000001.XSHE".to_string(),
            timestamp_ms: 1_101,
            price: 0,
            volume: 10,
            buy_number: order_number,
            sell_number: 0,
            deal_type: "4".to_string(),
        }
    }

    fn shenzhen_order(
        order_id: i64,
        direction: OrderDirection,
        price: i64,
        volume: i64,
        order_type: OrderType,
    ) -> L2Order {
        L2Order {
            market: Market::XSHE,
            channel: 1,
            message_number: order_id,
            code: "000001.XSHE".to_string(),
            price,
            volume,
            direction,
            order_type,
            timestamp_ms: 1_000,
            order_number: order_id,
        }
    }

    #[test]
    fn inserts_limit_orders_and_aggregates_price_levels() {
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
    }

    #[test]
    fn xshg_cancel_updates_book() {
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
    }

    #[test]
    fn xshg_trade_reduces_both_sides_and_removes_filled_orders() {
        let mut book = OrderBook::new();

        book.apply_order(limit_order(1, OrderDirection::Buy, 100_000, 10))
            .unwrap();
        book.apply_order(limit_order(2, OrderDirection::Sell, 101_000, 12))
            .unwrap();

        book.apply_transaction(xshg_transaction(1, 2, 4, "N"))
            .unwrap();
        assert_eq!(book.order_hash.get(&1).unwrap().volume, 6);
        assert_eq!(book.order_hash.get(&2).unwrap().volume, 8);

        book.apply_transaction(xshg_transaction(1, 2, 8, "B"))
            .unwrap();

        let snapshot = book.snapshot(10);
        assert!(!book.order_hash.contains_key(&1));
        assert!(!book.order_hash.contains_key(&2));
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert_eq!(book.best_bid_price(), None);
        assert_eq!(book.best_ask_price(), None);
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
    fn shenzhen_late_order_consumes_pending_trade_reduction() {
        let mut book = OrderBook::new();

        let mut tx = transaction(88, 0, 4);
        tx.market = Market::XSHE;
        tx.deal_type = "F".to_string();
        book.apply_transaction(tx).unwrap();

        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.market = Market::XSHE;
        order.order_number = 88;
        book.apply_order(order).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 6);
        assert_eq!(snapshot.bids[0].order_count, 1);
    }

    #[test]
    fn shanghai_late_order_ignores_pending_trade_reduction() {
        let mut book = OrderBook::new();

        book.apply_transaction(xshg_transaction(88, 0, 4, "N"))
            .unwrap();

        let mut order = limit_order(668_434, OrderDirection::Buy, 100_000, 10);
        order.order_number = 88;
        book.apply_order(order).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].total_qty, 10);
        assert_eq!(snapshot.bids[0].order_count, 1);
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
    fn shenzhen_best_own_buy_joins_bid_one_tail() {
        let mut book = OrderBook::new();

        let mut bid_one = shenzhen_order(1, OrderDirection::Buy, 100_000, 10, OrderType::Limit);
        bid_one.order_number = 1;
        book.apply_order(bid_one).unwrap();

        let best_own = shenzhen_order(2, OrderDirection::Buy, 0, 7, OrderType::BestOwn);
        book.apply_order(best_own).unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].price, 100_000);
        assert_eq!(snapshot.bids[0].total_qty, 17);
        assert_eq!(snapshot.bids[0].order_count, 2);
    }

    #[test]
    fn shenzhen_best_own_without_same_side_quote_is_ignored() {
        let mut book = OrderBook::new();

        let best_own = shenzhen_order(2, OrderDirection::Buy, 0, 7, OrderType::BestOwn);
        book.apply_order(best_own).unwrap();

        let snapshot = book.snapshot(10);
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
    }

    #[test]
    fn shenzhen_market_order_is_held_until_trade_chain_ends() {
        let mut book = OrderBook::new();

        let mut bid_one = shenzhen_order(1, OrderDirection::Buy, 100_000, 10, OrderType::Limit);
        bid_one.order_number = 1;
        book.apply_order(bid_one).unwrap();
        book.apply_order(shenzhen_order(
            2,
            OrderDirection::Buy,
            0,
            7,
            OrderType::Market,
        ))
        .unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.bids[0].price, 100_000);
        assert_eq!(snapshot.bids[0].total_qty, 10);
        assert!(book.has_unsettled_holdings());
        assert_eq!(book.holding_orders.get(&2).unwrap().remaining_volume, 7);

        let mut trade = transaction(2, 0, 4);
        trade.market = Market::XSHE;
        trade.deal_type = "F".to_string();
        trade.price = 101_000;
        book.apply_transaction(trade).unwrap();

        assert!(book.has_unsettled_holdings());
        assert_eq!(book.holding_orders.get(&2).unwrap().remaining_volume, 3);
        assert_eq!(
            book.holding_orders.get(&2).unwrap().resting_price,
            Some(101_000)
        );

        let mut unrelated_trade = transaction(99, 100, 1);
        unrelated_trade.market = Market::XSHE;
        unrelated_trade.deal_type = "F".to_string();
        unrelated_trade.price = 99_500;
        book.apply_transaction(unrelated_trade).unwrap();

        let snapshot = book.snapshot(10);
        assert!(!book.has_unsettled_holdings());
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.bids[0].price, 101_000);
        assert_eq!(snapshot.bids[0].total_qty, 3);
        assert_eq!(snapshot.bids[1].price, 100_000);
        assert_eq!(snapshot.bids[1].total_qty, 10);
    }

    #[test]
    fn shenzhen_market_order_can_be_cancelled_while_held() {
        let mut book = OrderBook::new();

        book.apply_order(shenzhen_order(
            88,
            OrderDirection::Buy,
            0,
            10,
            OrderType::Market,
        ))
        .unwrap();

        book.apply_transaction(sz_cancel_transaction(88)).unwrap();
        assert!(!book.holding_orders.contains_key(&88));
        assert!(!book.has_unsettled_holdings());
    }

    #[test]
    fn shenzhen_market_order_without_following_trade_is_dropped() {
        let mut book = OrderBook::new();

        book.apply_order(shenzhen_order(
            88,
            OrderDirection::Buy,
            0,
            10,
            OrderType::Market,
        ))
        .unwrap();

        let mut unrelated_trade = transaction(99, 100, 1);
        unrelated_trade.market = Market::XSHE;
        unrelated_trade.deal_type = "F".to_string();
        unrelated_trade.price = 99_500;
        book.apply_transaction(unrelated_trade).unwrap();

        let snapshot = book.snapshot(10);
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert!(!book.has_unsettled_holdings());
    }

    #[test]
    fn shenzhen_best_own_sees_residual_from_traded_market_order_before_applying() {
        let mut book = OrderBook::new();

        let mut old_best = shenzhen_order(1, OrderDirection::Buy, 100_000, 10, OrderType::Limit);
        old_best.order_number = 1;
        book.apply_order(old_best).unwrap();

        book.apply_order(shenzhen_order(
            88,
            OrderDirection::Buy,
            0,
            10,
            OrderType::Market,
        ))
        .unwrap();

        let mut trade = transaction(88, 0, 4);
        trade.market = Market::XSHE;
        trade.deal_type = "F".to_string();
        trade.price = 101_000;
        book.apply_transaction(trade).unwrap();

        book.apply_order(shenzhen_order(
            99,
            OrderDirection::Buy,
            0,
            7,
            OrderType::BestOwn,
        ))
        .unwrap();

        let snapshot = book.snapshot(10);
        assert_eq!(snapshot.bids[0].price, 101_000);
        assert_eq!(snapshot.bids[0].total_qty, 13);
        assert_eq!(snapshot.bids[1].price, 100_000);
        assert_eq!(snapshot.bids[1].total_qty, 10);
    }
}
