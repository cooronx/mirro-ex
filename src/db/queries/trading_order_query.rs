use rusqlite::{Connection, params};

use crate::trading::TradingOrder;

pub fn insert_order(connection: &Connection, order: &TradingOrder) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO orders (
            order_id,
            user_id,
            code,
            side,
            order_type,
            price,
            qty,
            filled_qty,
            status,
            created_at,
            updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            &order.order_id,
            &order.user_id,
            &order.code,
            &order.side,
            &order.order_type,
            order.price,
            order.qty,
            order.filled_qty,
            &order.status,
            order.created_at,
            order.updated_at,
        ],
    )?;
    Ok(())
}

pub fn query_orders_by_user_id(
    connection: &Connection,
    user_id: &str,
) -> rusqlite::Result<Vec<TradingOrder>> {
    let mut statement = connection.prepare(
        "SELECT
            order_id,
            user_id,
            code,
            side,
            order_type,
            price,
            qty,
            filled_qty,
            status,
            created_at,
            updated_at
         FROM orders
         WHERE user_id = ?1
         ORDER BY created_at DESC",
    )?;
    let rows = statement.query_map(params![user_id], order_from_row)?;
    rows.collect()
}

pub fn query_new_orders_by_code(
    connection: &Connection,
    code: &str,
) -> rusqlite::Result<Vec<TradingOrder>> {
    let mut statement = connection.prepare(
        "SELECT
            order_id,
            user_id,
            code,
            side,
            order_type,
            price,
            qty,
            filled_qty,
            status,
            created_at,
            updated_at
         FROM orders
         WHERE code = ?1
           AND order_type = 'limit'
           AND status = 'new'
           AND filled_qty < qty
         ORDER BY created_at ASC",
    )?;
    let rows = statement.query_map(params![code], order_from_row)?;
    rows.collect()
}

pub fn query_working_orders_by_code_price_side(
    connection: &Connection,
    code: &str,
    price: i64,
    side: &str,
) -> rusqlite::Result<Vec<TradingOrder>> {
    let mut statement = connection.prepare(
        "SELECT
            order_id,
            user_id,
            code,
            side,
            order_type,
            price,
            qty,
            filled_qty,
            status,
            created_at,
            updated_at
         FROM orders
         WHERE code = ?1
           AND price = ?2
           AND side = ?3
           AND order_type = 'limit'
           AND status IN ('working', 'partially_filled')
           AND filled_qty < qty
         ORDER BY created_at ASC",
    )?;
    let rows = statement.query_map(params![code, price, side], order_from_row)?;
    rows.collect()
}

pub fn update_order_fill(
    connection: &Connection,
    order_id: &str,
    filled_qty: i64,
    status: &str,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE orders
         SET filled_qty = ?1,
             status = ?2,
             updated_at = ?3
         WHERE order_id = ?4",
        params![filled_qty, status, updated_at, order_id],
    )
}

fn order_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradingOrder> {
    Ok(TradingOrder {
        order_id: row.get(0)?,
        user_id: row.get(1)?,
        code: row.get(2)?,
        side: row.get(3)?,
        order_type: row.get(4)?,
        price: row.get(5)?,
        qty: row.get(6)?,
        filled_qty: row.get(7)?,
        status: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}
