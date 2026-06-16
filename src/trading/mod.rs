use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::queries::trading_account_query::{insert_account, query_account_by_user_id};

#[derive(Debug, Clone, Serialize)]
pub struct Account {
    pub user_id: String,
    pub cash_balance: i64,
    pub available_cash: i64,
    pub frozen_cash: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateAccountRequest {
    pub user_id: String,
    pub initial_cash: i64,
}

#[derive(Debug, Error)]
pub enum TradingStoreError {
    #[error("user_id must not be empty")]
    EmptyUserId,
    #[error("initial_cash must be greater than or equal to 0")]
    InvalidInitialCash,
    #[error("account already exists for user_id={user_id}")]
    AccountAlreadyExists { user_id: String },
    #[error("account not found for user_id={user_id}")]
    AccountNotFound { user_id: String },
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
}

pub type StoreResult<T> = std::result::Result<T, TradingStoreError>;

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

    fn open_connection(&self) -> StoreResult<Connection> {
        Connection::open(&self.db_path).map_err(|source| TradingStoreError::OpenConnection {
            path: self.db_path.display().to_string(),
            source,
        })
    }
}

fn current_unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub fn trading_db_path_from_config(db_path: &str) -> Result<PathBuf> {
    let path = PathBuf::from(db_path);
    path.canonicalize().or_else(|_| {
        let current_dir = std::env::current_dir().context("failed to resolve current directory")?;
        Ok(current_dir.join(path))
    })
}
