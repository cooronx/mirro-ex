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
