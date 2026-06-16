use std::path::PathBuf;

use rusqlite::Connection;

use crate::db::queries::trading_account_query::{
    freeze_cash, insert_account, query_account_by_user_id,
};
use crate::db::queries::trading_fill_query::insert_fill;
use crate::db::queries::trading_order_query::{
    insert_order, query_matchable_orders_by_code, query_orders_by_user_id, update_order_fill,
};
use crate::db::queries::trading_position_query::freeze_position;
use crate::matcher::order_book::OrderBookSnapshot;

use super::error::{StoreResult, TradingStoreError};
use super::matching::{apply_fill, planned_fills};
use super::model::{
    Account, CreateAccountRequest, CreateLimitOrderRequest, Fill, ORDER_TYPE_LIMIT, SIDE_BUY,
    SIDE_SELL, STATUS_FILLED, STATUS_PARTIALLY_FILLED, STATUS_WORKING, TradingOrder,
};
use super::util::{
    FILL_COUNTER, ORDER_COUNTER, checked_amount, current_unix_timestamp_ms, next_id, normalize_side,
};

#[derive(Clone)]
pub struct TradingStore {
    db_path: PathBuf,
}

impl TradingStore {
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }

    pub fn create_account(&self, request: CreateAccountRequest) -> StoreResult<Account> {
        let user_id = request.user_id;
        if user_id.is_empty() {
            return Err(TradingStoreError::EmptyUserId);
        }
        if request.initial_cash < 0 {
            return Err(TradingStoreError::InvalidInitialCash);
        }

        let now_ms = current_unix_timestamp_ms();
        let account = Account {
            user_id: user_id.clone(),
            cash_balance: request.initial_cash,
            available_cash: request.initial_cash,
            frozen_cash: 0,
            created_at: now_ms,
            updated_at: now_ms,
        };

        let connection = self.open_connection()?;
        match insert_account(&connection, &account) {
            Ok(()) => Ok(account),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(TradingStoreError::AccountAlreadyExists { user_id })
            }
            Err(source) => Err(TradingStoreError::CreateAccount { user_id, source }),
        }
    }

    pub fn get_account(&self, user_id: &str) -> StoreResult<Account> {
        if user_id.is_empty() {
            return Err(TradingStoreError::EmptyUserId);
        }

        let connection = self.open_connection()?;
        let account = query_account_by_user_id(&connection, user_id).map_err(|source| {
            TradingStoreError::QueryAccount {
                user_id: user_id.to_string(),
                source,
            }
        })?;

        account.ok_or_else(|| TradingStoreError::AccountNotFound {
            user_id: user_id.to_string(),
        })
    }

    pub fn create_limit_order(
        &self,
        request: CreateLimitOrderRequest,
        sim_now_ms: i64,
    ) -> StoreResult<TradingOrder> {
        let user_id = request.user_id.trim().to_string();
        let code = request.code.trim().to_string();
        let side = normalize_side(&request.side)?;
        if user_id.is_empty() {
            return Err(TradingStoreError::EmptyUserId);
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
            status: STATUS_WORKING.to_string(),
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
                if freeze_cash(&tx, &user_id, frozen_cash, sim_now_ms).map_err(|source| {
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
                if freeze_position(&tx, &user_id, &code, order.qty, sim_now_ms).map_err(
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

        Ok(order)
    }

    pub fn list_orders(&self, user_id: &str) -> StoreResult<Vec<TradingOrder>> {
        if user_id.is_empty() {
            return Err(TradingStoreError::EmptyUserId);
        }

        let connection = self.open_connection()?;
        query_orders_by_user_id(&connection, user_id).map_err(|source| {
            TradingStoreError::QueryOrders {
                user_id: user_id.to_string(),
                source,
            }
        })
    }

    pub fn match_limit_orders(
        &self,
        code: &str,
        snapshot: &OrderBookSnapshot,
        timestamp_ms: i64,
    ) -> StoreResult<usize> {
        let mut connection = self.open_connection()?;
        let tx = connection
            .transaction()
            .map_err(|source| TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            })?;
        let orders = query_matchable_orders_by_code(&tx, code).map_err(|source| {
            TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            }
        })?;

        let mut fill_count = 0;
        for order in orders {
            let fills = planned_fills(&order, snapshot);
            if fills.is_empty() {
                continue;
            }

            let mut filled_qty = order.filled_qty;
            for (fill_price, fill_qty) in fills {
                let fill = Fill {
                    fill_id: next_id("fill", timestamp_ms, &FILL_COUNTER),
                    order_id: order.order_id.clone(),
                    user_id: order.user_id.clone(),
                    code: order.code.clone(),
                    side: order.side.clone(),
                    price: fill_price,
                    qty: fill_qty,
                    filled_at: timestamp_ms,
                };
                apply_fill(&tx, &order, &fill)?;
                insert_fill(&tx, &fill).map_err(|source| TradingStoreError::MatchOrders {
                    code: code.to_string(),
                    source,
                })?;
                filled_qty += fill_qty;
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
        }

        tx.commit()
            .map_err(|source| TradingStoreError::MatchOrders {
                code: code.to_string(),
                source,
            })?;
        Ok(fill_count)
    }

    fn open_connection(&self) -> StoreResult<Connection> {
        Connection::open(&self.db_path).map_err(|source| TradingStoreError::OpenConnection {
            path: self.db_path.display().to_string(),
            source,
        })
    }
}
