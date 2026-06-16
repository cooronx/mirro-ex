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
