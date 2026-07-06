use rusqlite::Connection;

use crate::db::queries::trading_account_query::{settle_buy_cash, settle_sell_cash};
use crate::db::queries::trading_position_query::{apply_buy_fill, apply_sell_fill};
use crate::matcher::order_book::LevelSnapshot;

use super::error::{StoreResult, TradingStoreError};
use super::model::{Fill, SIDE_BUY, SIDE_SELL, TradingOrder};
use super::util::checked_amount;

pub(super) fn planned_fills_from_levels(
    order: &TradingOrder,
    levels: &[LevelSnapshot],
) -> Vec<(i64, i64)> {
    let mut remaining = order.qty - order.filled_qty;
    if remaining <= 0 {
        return Vec::new();
    }

    let mut fills = Vec::new();
    for level in levels {
        if remaining <= 0 {
            break;
        }
        if level.total_qty <= 0 {
            continue;
        }
        let fill_qty = remaining.min(level.total_qty);
        fills.push((level.price, fill_qty));
        remaining -= fill_qty;
    }
    fills
}

pub(super) fn apply_fill(
    connection: &Connection,
    order: &TradingOrder,
    fill: &Fill,
) -> StoreResult<()> {
    match order.side.as_str() {
        SIDE_BUY => apply_buy_side_fill(connection, order, fill),
        SIDE_SELL => apply_sell_side_fill(connection, order, fill),
        _ => Err(TradingStoreError::UnsupportedSide {
            side: order.side.clone(),
        }),
    }
}

fn apply_buy_side_fill(
    connection: &Connection,
    order: &TradingOrder,
    fill: &Fill,
) -> StoreResult<()> {
    let frozen_release = checked_amount(order.price, fill.qty)?;
    let cash_cost = checked_amount(fill.price, fill.qty)?;
    settle_buy_cash(
        connection,
        order.user_id,
        frozen_release,
        cash_cost,
        fill.filled_at,
    )
    .map_err(|source| TradingStoreError::MatchOrders {
        code: order.code.clone(),
        source,
    })?;
    apply_buy_fill(
        connection,
        order.user_id,
        &order.code,
        fill.price,
        fill.qty,
        fill.filled_at,
    )
    .map_err(|source| TradingStoreError::MatchOrders {
        code: order.code.clone(),
        source,
    })?;
    Ok(())
}

fn apply_sell_side_fill(
    connection: &Connection,
    order: &TradingOrder,
    fill: &Fill,
) -> StoreResult<()> {
    let proceeds = checked_amount(fill.price, fill.qty)?;
    if apply_sell_fill(
        connection,
        order.user_id,
        &order.code,
        fill.qty,
        fill.filled_at,
    )
    .map_err(|source| TradingStoreError::MatchOrders {
        code: order.code.clone(),
        source,
    })? == 0
    {
        return Err(TradingStoreError::InsufficientPosition {
            user_id: order.user_id,
            code: order.code.clone(),
        });
    }
    settle_sell_cash(connection, order.user_id, proceeds, fill.filled_at).map_err(|source| {
        TradingStoreError::MatchOrders {
            code: order.code.clone(),
            source,
        }
    })?;
    Ok(())
}
