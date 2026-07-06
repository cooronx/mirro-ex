use serde::{Deserialize, Serialize};

pub const SIDE_BUY: &str = "buy";
pub const SIDE_SELL: &str = "sell";
pub const ORDER_TYPE_LIMIT: &str = "limit";
pub const STATUS_NEW: &str = "new";
pub const STATUS_WORKING: &str = "working";
pub const STATUS_PARTIALLY_FILLED: &str = "partially_filled";
pub const STATUS_FILLED: &str = "filled";
pub const STATUS_CANCELED: &str = "canceled";

#[derive(Debug, Clone, Serialize)]
pub struct Account {
    pub user_id: i64,
    pub username: String,
    pub password: String,
    pub cash_balance: i64,
    pub available_cash: i64,
    pub frozen_cash: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Position {
    pub user_id: i64,
    pub code: String,
    pub long_qty: i64,
    pub available_qty: i64,
    pub frozen_qty: i64,
    pub avg_price: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradingOrder {
    pub order_id: String,
    pub user_id: i64,
    pub code: String,
    pub side: String,
    pub order_type: String,
    pub price: i64,
    pub qty: i64,
    pub filled_qty: i64,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Fill {
    pub fill_id: String,
    pub order_id: String,
    pub user_id: i64,
    pub code: String,
    pub side: String,
    pub price: i64,
    pub qty: i64,
    pub filled_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateAccountRequest {
    pub username: String,
    pub password: String,
    pub initial_cash: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateLimitOrderRequest {
    pub user_id: i64,
    pub code: String,
    pub side: String,
    pub price: i64,
    pub qty: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CancelOrderRequest {
    pub user_id: i64,
    pub order_id: String,
}
