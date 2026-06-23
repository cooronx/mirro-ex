use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use super::error::{StoreResult, TradingStoreError};
use super::model::{SIDE_BUY, SIDE_SELL};

pub(super) static ORDER_COUNTER: AtomicU64 = AtomicU64::new(1);
pub(super) static FILL_COUNTER: AtomicU64 = AtomicU64::new(1);
pub(super) static ORDER_ACTIVITY_EPOCH: AtomicU64 = AtomicU64::new(0);

pub(super) fn normalize_side(side: &str) -> StoreResult<String> {
    match side.trim().to_ascii_lowercase().as_str() {
        SIDE_BUY => Ok(SIDE_BUY.to_string()),
        SIDE_SELL => Ok(SIDE_SELL.to_string()),
        _ => Err(TradingStoreError::UnsupportedSide {
            side: side.to_string(),
        }),
    }
}

pub(super) fn checked_amount(price: i64, qty: i64) -> StoreResult<i64> {
    price
        .checked_mul(qty)
        .ok_or(TradingStoreError::AmountOverflow)
}

pub(super) fn next_id(prefix: &str, timestamp_ms: i64, counter: &AtomicU64) -> String {
    let sequence = counter.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{timestamp_ms}-{sequence}")
}

pub(super) fn current_unix_timestamp_ms() -> i64 {
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
