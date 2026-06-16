use rusqlite::{Connection, OptionalExtension, params};

use crate::trading::Account;

pub fn insert_account(connection: &Connection, account: &Account) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO accounts (
            user_id,
            cash_balance,
            available_cash,
            frozen_cash,
            created_at,
            updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            &account.user_id,
            account.cash_balance,
            account.available_cash,
            account.frozen_cash,
            account.created_at,
            account.updated_at,
        ],
    )?;
    Ok(())
}

pub fn query_account_by_user_id(
    connection: &Connection,
    user_id: &str,
) -> rusqlite::Result<Option<Account>> {
    connection
        .query_row(
            "SELECT
                user_id,
                cash_balance,
                available_cash,
                frozen_cash,
                created_at,
                updated_at
             FROM accounts
             WHERE user_id = ?1",
            params![user_id],
            |row| {
                Ok(Account {
                    user_id: row.get(0)?,
                    cash_balance: row.get(1)?,
                    available_cash: row.get(2)?,
                    frozen_cash: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            },
        )
        .optional()
}

pub fn freeze_cash(
    connection: &Connection,
    user_id: &str,
    amount: i64,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE accounts
         SET available_cash = available_cash - ?1,
             frozen_cash = frozen_cash + ?1,
             updated_at = ?2
         WHERE user_id = ?3
           AND available_cash >= ?1",
        params![amount, updated_at, user_id],
    )
}

pub fn settle_buy_cash(
    connection: &Connection,
    user_id: &str,
    frozen_release: i64,
    cash_cost: i64,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE accounts
         SET cash_balance = cash_balance - ?1,
             frozen_cash = frozen_cash - ?2,
             available_cash = available_cash + (?2 - ?1),
             updated_at = ?3
         WHERE user_id = ?4
           AND frozen_cash >= ?2",
        params![cash_cost, frozen_release, updated_at, user_id],
    )
}

pub fn settle_sell_cash(
    connection: &Connection,
    user_id: &str,
    proceeds: i64,
    updated_at: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE accounts
         SET cash_balance = cash_balance + ?1,
             available_cash = available_cash + ?1,
             updated_at = ?2
         WHERE user_id = ?3",
        params![proceeds, updated_at, user_id],
    )
}
