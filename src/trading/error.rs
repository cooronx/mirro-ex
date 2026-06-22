use thiserror::Error;

#[derive(Debug, Error)]
pub enum TradingStoreError {
    #[error("user_id must not be empty")]
    EmptyUserId,
    #[error("code must not be empty")]
    EmptyCode,
    #[error("initial_cash must be greater than or equal to 0")]
    InvalidInitialCash,
    #[error("price must be greater than 0")]
    InvalidPrice,
    #[error("qty must be greater than 0")]
    InvalidQty,
    #[error("unsupported side: {side}")]
    UnsupportedSide { side: String },
    #[error("insufficient available cash for user_id={user_id}")]
    InsufficientCash { user_id: String },
    #[error("insufficient available position for user_id={user_id} code={code}")]
    InsufficientPosition { user_id: String, code: String },
    #[error("amount overflow")]
    AmountOverflow,
    #[error("account already exists for user_id={user_id}")]
    AccountAlreadyExists { user_id: String },
    #[error("account not found for user_id={user_id}")]
    AccountNotFound { user_id: String },
    #[error("order not found for user_id={user_id} order_id={order_id}")]
    OrderNotFound { user_id: String, order_id: String },
    #[error("order is not cancelable: order_id={order_id} status={status}")]
    OrderNotCancelable { order_id: String, status: String },
    #[error("failed to open sqlite trading database at {path}")]
    OpenConnection {
        path: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to create account for user_id={user_id}")]
    CreateAccount {
        user_id: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to query account for user_id={user_id}")]
    QueryAccount {
        user_id: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to create order for user_id={user_id} code={code}")]
    CreateOrder {
        user_id: String,
        code: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to cancel order for user_id={user_id} order_id={order_id}")]
    CancelOrder {
        user_id: String,
        order_id: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to query orders for user_id={user_id}")]
    QueryOrders {
        user_id: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to query positions for user_id={user_id}")]
    QueryPositions {
        user_id: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to match orders for code={code}")]
    MatchOrders {
        code: String,
        #[source]
        source: rusqlite::Error,
    },
}

pub type StoreResult<T> = std::result::Result<T, TradingStoreError>;
