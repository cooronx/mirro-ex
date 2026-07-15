use std::collections::HashMap;
use std::path::PathBuf;

use rusqlite::Connection;

use crate::db::queries::trading_account_query::{
    freeze_cash, insert_account, query_account_by_user_id, query_account_by_username, release_cash,
};
use crate::db::queries::trading_fill_query::{insert_fill, query_fills_by_user_id};
use crate::db::queries::trading_order_query::{
    insert_order, query_new_orders_by_code, query_order_by_id, query_orders_by_user_id,
    query_working_orders_by_code_price_side, update_order_fill, update_order_status,
};
use crate::db::queries::trading_position_query::{
    freeze_position, query_position, query_positions_by_user_id, release_position,
};
use crate::matcher::order_book::LevelSnapshot;
use crate::webdata::{AppEvent, EventBus};

use super::error::{StoreResult, TradingStoreError};
use super::matching::{apply_fill, planned_fills_from_levels};
use super::model::{
    Account, CancelOrderRequest, CreateAccountRequest, CreateLimitOrderRequest, Fill, LoginRequest,
    ORDER_TYPE_LIMIT, Position, SIDE_BUY, SIDE_SELL, STATUS_CANCELED, STATUS_FILLED, STATUS_NEW,
    STATUS_PARTIALLY_FILLED, STATUS_WORKING, TradingOrder,
};
use super::util::{
    FILL_COUNTER, ORDER_ACTIVITY_EPOCH, ORDER_COUNTER, checked_amount, current_unix_timestamp_ms,
    next_id, normalize_side,
};

#[derive(Clone)]
pub struct TradingStore {
    db_path: PathBuf,
    event_bus: Option<EventBus>,
}

impl TradingStore {
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            event_bus: None,
        }
    }

    pub fn with_event_bus(db_path: impl Into<PathBuf>, event_bus: EventBus) -> Self {
        Self {
            db_path: db_path.into(),
            event_bus: Some(event_bus),
        }
    }

    pub fn create_account(&self, request: CreateAccountRequest) -> StoreResult<Account> {
        let username = request.username.trim().to_string();
        let password = request.password.trim().to_string();
        if username.is_empty() {
            return Err(TradingStoreError::EmptyUsername);
        }
        if password.is_empty() {
            return Err(TradingStoreError::EmptyPassword);
        }
        if request.initial_cash < 0 {
            return Err(TradingStoreError::InvalidInitialCash);
        }

        let now_ms = current_unix_timestamp_ms();
        let connection = self.open_connection()?;
        match insert_account(
            &connection,
            &username,
            &password,
            request.initial_cash,
            now_ms,
        ) {
            Ok(user_id) => Ok(Account {
                user_id,
                username,
                password,
                cash_balance: request.initial_cash,
                available_cash: request.initial_cash,
                frozen_cash: 0,
                created_at: now_ms,
                updated_at: now_ms,
            }),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(TradingStoreError::AccountAlreadyExists { username })
            }
            Err(source) => Err(TradingStoreError::CreateAccount { username, source }),
        }
    }

    pub fn login(&self, request: LoginRequest) -> StoreResult<Account> {
        let username = request.username.trim().to_string();
        let password = request.password.trim().to_string();
        if username.is_empty() {
            return Err(TradingStoreError::EmptyUsername);
        }
        if password.is_empty() {
            return Err(TradingStoreError::EmptyPassword);
        }

        let connection = self.open_connection()?;
        let account = query_account_by_username(&connection, &username)
            .map_err(|source| TradingStoreError::QueryAccountByUsername { username, source })?;

        match account {
            Some(account) if account.password == password => Ok(account),
            _ => Err(TradingStoreError::InvalidCredentials),
        }
    }

    pub fn get_account(&self, user_id: i64) -> StoreResult<Account> {
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }

        let connection = self.open_connection()?;
        let account = query_account_by_user_id(&connection, user_id)
            .map_err(|source| TradingStoreError::QueryAccount { user_id, source })?;

        account.ok_or(TradingStoreError::AccountNotFound { user_id })
    }

    pub fn create_limit_order(
        &self,
        request: CreateLimitOrderRequest,
        sim_now_ms: i64,
    ) -> StoreResult<TradingOrder> {
        let user_id = request.user_id;
        let code = request.code.trim().to_string();
        let side = normalize_side(&request.side)?;
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }
        if code.is_empty() {
            return Err(TradingStoreError::EmptyCode);
        }
        if request.price <= 0 {
            return Err(TradingStoreError::InvalidPrice);
        }
        if request.qty <= 0 {
            return Err(TradingStoreError::InvalidQty);
        }

        let order = TradingOrder {
            order_id: next_id("ord", sim_now_ms, &ORDER_COUNTER),
            user_id: user_id.clone(),
            code: code.clone(),
            side: side.clone(),
            order_type: ORDER_TYPE_LIMIT.to_string(),
            price: request.price,
            qty: request.qty,
            filled_qty: 0,
            status: STATUS_NEW.to_string(),
            created_at: sim_now_ms,
            updated_at: sim_now_ms,
        };

        let mut connection = self.open_connection()?;
        let tx = connection
            .transaction()
            .map_err(|source| TradingStoreError::CreateOrder {
                user_id: user_id.clone(),
                code: code.clone(),
                source,
            })?;

        match side.as_str() {
            SIDE_BUY => {
                let frozen_cash = checked_amount(order.price, order.qty)?;
                if freeze_cash(&tx, user_id, frozen_cash, sim_now_ms).map_err(|source| {
                    TradingStoreError::CreateOrder {
                        user_id: user_id.clone(),
                        code: code.clone(),
                        source,
                    }
                })? == 0
                {
                    return Err(TradingStoreError::InsufficientCash { user_id });
                }
            }
            SIDE_SELL => {
                if freeze_position(&tx, user_id, &code, order.qty, sim_now_ms).map_err(
                    |source| TradingStoreError::CreateOrder {
                        user_id: user_id.clone(),
                        code: code.clone(),
                        source,
                    },
                )? == 0
                {
                    return Err(TradingStoreError::InsufficientPosition { user_id, code });
                }
            }
            _ => unreachable!("side was normalized"),
        }

        insert_order(&tx, &order).map_err(|source| TradingStoreError::CreateOrder {
            user_id: order.user_id.clone(),
            code: order.code.clone(),
            source,
        })?;
        tx.commit()
            .map_err(|source| TradingStoreError::CreateOrder {
                user_id: order.user_id.clone(),
                code: order.code.clone(),
                source,
            })?;

        bump_order_activity_epoch();
        self.publish_trading_changed(Some(order.user_id));
        Ok(order)
    }

    pub fn list_orders(&self, user_id: i64) -> StoreResult<Vec<TradingOrder>> {
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }

        let connection = self.open_connection()?;
        query_orders_by_user_id(&connection, user_id)
            .map_err(|source| TradingStoreError::QueryOrders { user_id, source })
    }

    pub fn get_order(&self, user_id: i64, order_id: &str) -> StoreResult<TradingOrder> {
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }
        let order_id = order_id.trim();
        let connection = self.open_connection()?;
        query_order_by_id(&connection, order_id)
            .map_err(|source| TradingStoreError::QueryOrders { user_id, source })?
            .filter(|order| order.user_id == user_id)
            .ok_or_else(|| TradingStoreError::OrderNotFound {
                user_id,
                order_id: order_id.to_string(),
            })
    }

    pub fn list_fills(&self, user_id: i64) -> StoreResult<Vec<Fill>> {
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }

        let connection = self.open_connection()?;
        query_fills_by_user_id(&connection, user_id)
            .map_err(|source| TradingStoreError::QueryFills { user_id, source })
    }

    pub fn list_positions(&self, user_id: i64, code: Option<&str>) -> StoreResult<Vec<Position>> {
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }

        let connection = self.open_connection()?;
        match code.map(str::trim).filter(|code| !code.is_empty()) {
            Some(code) => query_position(&connection, user_id, code)
                .map(|position| position.into_iter().collect())
                .map_err(|source| TradingStoreError::QueryPositions { user_id, source }),
            None => query_positions_by_user_id(&connection, user_id)
                .map_err(|source| TradingStoreError::QueryPositions { user_id, source }),
        }
    }

    pub fn cancel_order(
        &self,
        request: CancelOrderRequest,
        sim_now_ms: i64,
    ) -> StoreResult<TradingOrder> {
        let user_id = request.user_id;
        let order_id = request.order_id.trim().to_string();
        if user_id <= 0 {
            return Err(TradingStoreError::InvalidUserId);
        }
        if order_id.is_empty() {
            return Err(TradingStoreError::OrderNotFound { user_id, order_id });
        }

        let mut connection = self.open_connection()?;
        let tx = connection
            .transaction()
            .map_err(|source| TradingStoreError::CancelOrder {
                user_id: user_id.clone(),
                order_id: order_id.clone(),
                source,
            })?;
        let order = query_order_by_id(&tx, &order_id)
            .map_err(|source| TradingStoreError::CancelOrder {
                user_id: user_id.clone(),
                order_id: order_id.clone(),
                source,
            })?
            .filter(|order| order.user_id == user_id)
            .ok_or_else(|| TradingStoreError::OrderNotFound {
                user_id: user_id.clone(),
                order_id: order_id.clone(),
            })?;

        if !is_cancelable_status(&order.status) || order.filled_qty >= order.qty {
            return Err(TradingStoreError::OrderNotCancelable {
                order_id,
                status: order.status,
            });
        }

        let open_qty = order.qty - order.filled_qty;
        match order.side.as_str() {
            SIDE_BUY => {
                let release_amount = checked_amount(order.price, open_qty)?;
                if release_cash(&tx, user_id, release_amount, sim_now_ms).map_err(|source| {
                    TradingStoreError::CancelOrder {
                        user_id: user_id.clone(),
                        order_id: order.order_id.clone(),
                        source,
                    }
                })? == 0
                {
                    return Err(TradingStoreError::InsufficientCash { user_id });
                }
            }
            SIDE_SELL => {
                if release_position(&tx, user_id, &order.code, open_qty, sim_now_ms).map_err(
                    |source| TradingStoreError::CancelOrder {
                        user_id: user_id.clone(),
                        order_id: order.order_id.clone(),
                        source,
                    },
                )? == 0
                {
                    return Err(TradingStoreError::InsufficientPosition {
                        user_id,
                        code: order.code,
                    });
                }
            }
            _ => {
                return Err(TradingStoreError::UnsupportedSide {
                    side: order.side.clone(),
                });
            }
        }

        update_order_status(&tx, &order.order_id, STATUS_CANCELED, sim_now_ms).map_err(
            |source| TradingStoreError::CancelOrder {
                user_id: order.user_id.clone(),
                order_id: order.order_id.clone(),
                source,
            },
        )?;
        let canceled_order = query_order_by_id(&tx, &order.order_id)
            .map_err(|source| TradingStoreError::CancelOrder {
                user_id: order.user_id.clone(),
                order_id: order.order_id.clone(),
                source,
            })?
            .expect("order exists after status update");
        tx.commit()
            .map_err(|source| TradingStoreError::CancelOrder {
                user_id: canceled_order.user_id.clone(),
                order_id: canceled_order.order_id.clone(),
                source,
            })?;

        bump_order_activity_epoch();
        self.publish_trading_changed(Some(canceled_order.user_id));
        Ok(canceled_order)
    }

    pub fn order_activity_epoch(&self) -> u64 {
        ORDER_ACTIVITY_EPOCH.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn new_limit_orders(&self, code: &str) -> StoreResult<Vec<TradingOrder>> {
        let connection = self.open_connection()?;
        query_new_orders_by_code(&connection, code).map_err(|source| {
            TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            }
        })
    }

    pub fn initialize_limit_order_queue(
        &self,
        order: &TradingOrder,
        marketable_levels: &[LevelSnapshot],
        queue_ahead_qty: i64,
        timestamp_ms: i64,
    ) -> StoreResult<(usize, Option<i64>)> {
        let mut connection = self.open_connection()?;
        let tx = connection
            .transaction()
            .map_err(|source| TradingStoreError::MatchOrders {
                code: order.code.clone(),
                source,
            })?;

        let mut fill_count = 0;
        let mut filled_qty = order.filled_qty;
        for (fill_price, fill_qty) in planned_fills_from_levels(order, marketable_levels) {
            let fill = Fill {
                fill_id: next_id("fill", timestamp_ms, &FILL_COUNTER),
                order_id: order.order_id.clone(),
                user_id: order.user_id,
                code: order.code.clone(),
                side: order.side.clone(),
                price: fill_price,
                qty: fill_qty,
                filled_at: timestamp_ms,
            };
            apply_fill(&tx, order, &fill)?;
            insert_fill(&tx, &fill).map_err(|source| TradingStoreError::MatchOrders {
                code: order.code.clone(),
                source,
            })?;
            filled_qty += fill_qty;
            fill_count += 1;
        }

        let status = if filled_qty >= order.qty {
            STATUS_FILLED
        } else if filled_qty > 0 {
            STATUS_PARTIALLY_FILLED
        } else {
            STATUS_WORKING
        };
        update_order_fill(&tx, &order.order_id, filled_qty, status, timestamp_ms).map_err(
            |source| TradingStoreError::MatchOrders {
                code: order.code.clone(),
                source,
            },
        )?;
        tx.commit()
            .map_err(|source| TradingStoreError::MatchOrders {
                code: order.code.clone(),
                source,
            })?;
        if fill_count > 0 {
            self.publish_trading_changed(Some(order.user_id));
        }
        let queue_ahead_qty = (filled_qty < order.qty).then_some(queue_ahead_qty);
        Ok((fill_count, queue_ahead_qty))
    }

    pub fn match_queued_limit_orders(
        &self,
        code: &str,
        resting_side: &str,
        price: i64,
        volume: i64,
        timestamp_ms: i64,
        order_queues: &mut HashMap<String, i64>,
    ) -> StoreResult<usize> {
        if volume <= 0 {
            return Ok(0);
        }
        let mut connection = self.open_connection()?;
        let tx = connection
            .transaction()
            .map_err(|source| TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            })?;
        let orders = query_working_orders_by_code_price_side(&tx, code, price, resting_side)
            .map_err(|source| TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            })?;

        let mut fill_count = 0;
        let mut remaining_trade_volume = volume;
        for queued in orders {
            if remaining_trade_volume <= 0 {
                break;
            }
            let order = queued;
            let Some(current_queue_ahead_qty) = order_queues.get(&order.order_id).copied() else {
                continue;
            };
            let queue_consumed = current_queue_ahead_qty.min(remaining_trade_volume);
            let queue_ahead_qty = current_queue_ahead_qty - queue_consumed;
            remaining_trade_volume -= queue_consumed;
            if queue_ahead_qty > 0 {
                order_queues.insert(order.order_id.clone(), queue_ahead_qty);
                continue;
            }
            order_queues.insert(order.order_id.clone(), 0);

            let open_qty = order.qty - order.filled_qty;
            let fill_qty = open_qty.min(remaining_trade_volume);
            let mut filled_qty = order.filled_qty;
            if fill_qty > 0 {
                let fill = Fill {
                    fill_id: next_id("fill", timestamp_ms, &FILL_COUNTER),
                    order_id: order.order_id.clone(),
                    user_id: order.user_id,
                    code: order.code.clone(),
                    side: order.side.clone(),
                    price,
                    qty: fill_qty,
                    filled_at: timestamp_ms,
                };
                apply_fill(&tx, &order, &fill)?;
                insert_fill(&tx, &fill).map_err(|source| TradingStoreError::MatchOrders {
                    code: code.to_string(),
                    source,
                })?;
                filled_qty += fill_qty;
                remaining_trade_volume -= fill_qty;
                fill_count += 1;
            }

            let status = if filled_qty >= order.qty {
                STATUS_FILLED
            } else {
                STATUS_PARTIALLY_FILLED
            };
            update_order_fill(&tx, &order.order_id, filled_qty, status, timestamp_ms).map_err(
                |source| TradingStoreError::MatchOrders {
                    code: code.to_string(),
                    source,
                },
            )?;
            if filled_qty >= order.qty {
                order_queues.remove(&order.order_id);
            }
        }

        tx.commit()
            .map_err(|source| TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            })?;
        if fill_count > 0 {
            self.publish_trading_changed(None);
        }
        Ok(fill_count)
    }

    fn open_connection(&self) -> StoreResult<Connection> {
        Connection::open(&self.db_path).map_err(|source| TradingStoreError::OpenConnection {
            path: self.db_path.display().to_string(),
            source,
        })
    }

    fn publish_trading_changed(&self, user_id: Option<i64>) {
        if let Some(event_bus) = &self.event_bus {
            event_bus.publish(AppEvent::TradingChanged { user_id });
        }
    }
}

fn bump_order_activity_epoch() {
    ORDER_ACTIVITY_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn is_cancelable_status(status: &str) -> bool {
    matches!(
        status,
        STATUS_NEW | STATUS_WORKING | STATUS_PARTIALLY_FILLED
    )
}
