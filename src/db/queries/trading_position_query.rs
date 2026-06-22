use rusqlite::{Connection, OptionalExtension, params};

use crate::trading::Position;

pub fn query_positions_by_user_id(
    connection: &Connection,
    user_id: &str,
) -> rusqlite::Result<Vec<Position>> {
    let mut statement = connection.prepare(
        "SELECT
            user_id,
            code,
            long_qty,
            available_qty,
            frozen_qty,
            avg_price,
            updated_at
         FROM positions
         WHERE user_id = ?1
         ORDER BY code ASC",
    )?;
    let rows = statement.query_map(params![user_id], position_from_row)?;
    rows.collect()
}

pub fn query_position(
    connection: &Connection,
    user_id: &str,
    code: &str,
) -> rusqlite::Result<Option<Position>> {
    connection
        .query_row(
            "SELECT
                user_id,
                code,
                long_qty,
                available_qty,
                frozen_qty,
                avg_price,
                updated_at
             FROM positions
             WHERE user_id = ?1
               AND code = ?2",
            params![user_id, code],
            position_from_row,
        )
        .optional()
}

pub fn freeze_position(
    connection: &Connection,
    user_id: &str,
    code: &str,
    qty: i64,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE positions
         SET available_qty = available_qty - ?1,
             frozen_qty = frozen_qty + ?1,
             updated_at = ?2
         WHERE user_id = ?3
           AND code = ?4
           AND available_qty >= ?1",
        params![qty, updated_at, user_id, code],
    )
}

pub fn apply_buy_fill(
    connection: &Connection,
    user_id: &str,
    code: &str,
    price: i64,
    qty: i64,
    updated_at: i64,
) -> rusqlite::Result<()> {
    let position = query_position(connection, user_id, code)?;
    match position {
        Some(position) => {
            let new_long_qty = position.long_qty + qty;
            let old_cost = position.long_qty * position.avg_price;
            let fill_cost = qty * price;
            let avg_price = if new_long_qty > 0 {
                (old_cost + fill_cost) / new_long_qty
            } else {
                0
            };
            connection.execute(
                "UPDATE positions
                 SET long_qty = ?1,
                     available_qty = available_qty + ?2,
                     avg_price = ?3,
                     updated_at = ?4
                 WHERE user_id = ?5
                   AND code = ?6",
                params![new_long_qty, qty, avg_price, updated_at, user_id, code],
            )?;
        }
        None => {
            connection.execute(
                "INSERT INTO positions (
                    user_id,
                    code,
                    long_qty,
                    available_qty,
                    frozen_qty,
                    avg_price,
                    updated_at
                ) VALUES (?1, ?2, ?3, ?3, 0, ?4, ?5)",
                params![user_id, code, qty, price, updated_at],
            )?;
        }
    }
    Ok(())
}

pub fn apply_sell_fill(
    connection: &Connection,
    user_id: &str,
    code: &str,
    qty: i64,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE positions
         SET long_qty = long_qty - ?1,
             frozen_qty = frozen_qty - ?1,
             updated_at = ?2
         WHERE user_id = ?3
           AND code = ?4
           AND long_qty >= ?1
           AND frozen_qty >= ?1",
        params![qty, updated_at, user_id, code],
    )
}

pub fn release_position(
    connection: &Connection,
    user_id: &str,
    code: &str,
    qty: i64,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE positions
         SET available_qty = available_qty + ?1,
             frozen_qty = frozen_qty - ?1,
             updated_at = ?2
         WHERE user_id = ?3
           AND code = ?4
           AND frozen_qty >= ?1",
        params![qty, updated_at, user_id, code],
    )
}

fn position_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Position> {
    Ok(Position {
        user_id: row.get(0)?,
        code: row.get(1)?,
        long_qty: row.get(2)?,
        available_qty: row.get(3)?,
        frozen_qty: row.get(4)?,
        avg_price: row.get(5)?,
        updated_at: row.get(6)?,
    })
}
