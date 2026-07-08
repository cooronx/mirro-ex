use rusqlite::{Connection, params};

use crate::trading::Fill;

pub fn insert_fill(connection: &Connection, fill: &Fill) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO fills (
            fill_id,
            order_id,
            user_id,
            code,
            side,
            price,
            qty,
            filled_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            &fill.fill_id,
            &fill.order_id,
            &fill.user_id,
            &fill.code,
            &fill.side,
            fill.price,
            fill.qty,
            fill.filled_at,
        ],
    )?;
    Ok(())
}

pub fn query_fills_by_user_id(
    connection: &Connection,
    user_id: i64,
) -> rusqlite::Result<Vec<Fill>> {
    let mut statement = connection.prepare(
        "SELECT
            fill_id,
            order_id,
            user_id,
            code,
            side,
            price,
            qty,
            filled_at
         FROM fills
         WHERE user_id = ?1
         ORDER BY filled_at DESC",
    )?;
    let rows = statement.query_map(params![user_id], fill_from_row)?;
    rows.collect()
}

fn fill_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Fill> {
    Ok(Fill {
        fill_id: row.get(0)?,
        order_id: row.get(1)?,
        user_id: row.get(2)?,
        code: row.get(3)?,
        side: row.get(4)?,
        price: row.get(5)?,
        qty: row.get(6)?,
        filled_at: row.get(7)?,
    })
}
