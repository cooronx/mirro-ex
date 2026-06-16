use std::fs;

use rusqlite::{Connection, params};

use super::{CreateAccountRequest, CreateLimitOrderRequest, TradingStore, TradingStoreError};
use crate::matcher::order_book::{LevelSnapshot, OrderBookSnapshot};

fn test_store(name: &str) -> (TradingStore, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "mirro-ex-{name}-{}-{}.db",
        std::process::id(),
        super::util::ORDER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(include_str!(
            "../../scripts/create_trading_sqlite_schema.sql"
        ))
        .unwrap();
    (TradingStore::new(path.clone()), path)
}

fn account_cash(path: &std::path::Path, user_id: &str) -> (i64, i64, i64) {
    let connection = Connection::open(path).unwrap();
    connection
        .query_row(
            "SELECT cash_balance, available_cash, frozen_cash FROM accounts WHERE user_id = ?1",
            params![user_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

fn position_qty(path: &std::path::Path, user_id: &str, code: &str) -> (i64, i64, i64, i64) {
    let connection = Connection::open(path).unwrap();
    connection
        .query_row(
            "SELECT long_qty, available_qty, frozen_qty, avg_price
             FROM positions
             WHERE user_id = ?1 AND code = ?2",
            params![user_id, code],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
}

#[test]
fn create_buy_limit_order_freezes_cash() {
    let (store, path) = test_store("freeze-cash");
    store
        .create_account(CreateAccountRequest {
            user_id: "u1".to_string(),
            initial_cash: 10_000,
        })
        .unwrap();

    let order = store
        .create_limit_order(
            CreateLimitOrderRequest {
                user_id: "u1".to_string(),
                code: "300274.XSHE".to_string(),
                side: "buy".to_string(),
                price: 100,
                qty: 20,
            },
            1_000,
        )
        .unwrap();

    assert_eq!(order.status, "working");
    assert_eq!(account_cash(&path, "u1"), (10_000, 8_000, 2_000));
    let _ = fs::remove_file(path);
}

#[test]
fn buy_limit_order_matches_ask_and_updates_account_position() {
    let (store, path) = test_store("buy-fill");
    store
        .create_account(CreateAccountRequest {
            user_id: "u1".to_string(),
            initial_cash: 10_000,
        })
        .unwrap();
    let order = store
        .create_limit_order(
            CreateLimitOrderRequest {
                user_id: "u1".to_string(),
                code: "300274.XSHE".to_string(),
                side: "buy".to_string(),
                price: 100,
                qty: 10,
            },
            1_000,
        )
        .unwrap();

    let snapshot = OrderBookSnapshot {
        bids: vec![],
        asks: vec![LevelSnapshot {
            price: 90,
            total_qty: 5,
            order_count: 1,
        }],
    };

    assert_eq!(
        store
            .match_limit_orders("300274.XSHE", &snapshot, 1_100)
            .unwrap(),
        1
    );
    let orders = store.list_orders("u1").unwrap();
    assert_eq!(orders[0].order_id, order.order_id);
    assert_eq!(orders[0].filled_qty, 5);
    assert_eq!(orders[0].status, "partially_filled");
    assert_eq!(account_cash(&path, "u1"), (9_550, 9_050, 500));
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (5, 5, 0, 90));
    let _ = fs::remove_file(path);
}

#[test]
fn sell_limit_order_freezes_position_and_settles_fill() {
    let (store, path) = test_store("sell-fill");
    store
        .create_account(CreateAccountRequest {
            user_id: "u1".to_string(),
            initial_cash: 1_000,
        })
        .unwrap();
    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO positions (
                    user_id, code, long_qty, available_qty, frozen_qty, avg_price, updated_at
                ) VALUES (?1, ?2, 10, 10, 0, 80, 1)",
                params!["u1", "300274.XSHE"],
            )
            .unwrap();
    }

    store
        .create_limit_order(
            CreateLimitOrderRequest {
                user_id: "u1".to_string(),
                code: "300274.XSHE".to_string(),
                side: "sell".to_string(),
                price: 95,
                qty: 6,
            },
            1_000,
        )
        .unwrap();
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (10, 4, 6, 80));

    let snapshot = OrderBookSnapshot {
        bids: vec![LevelSnapshot {
            price: 100,
            total_qty: 6,
            order_count: 1,
        }],
        asks: vec![],
    };
    store
        .match_limit_orders("300274.XSHE", &snapshot, 1_100)
        .unwrap();

    let orders = store.list_orders("u1").unwrap();
    assert_eq!(orders[0].filled_qty, 6);
    assert_eq!(orders[0].status, "filled");
    assert_eq!(account_cash(&path, "u1"), (1_600, 1_600, 0));
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (4, 4, 0, 80));
    let _ = fs::remove_file(path);
}

#[test]
fn rejects_buy_order_when_cash_is_insufficient() {
    let (store, path) = test_store("cash-reject");
    store
        .create_account(CreateAccountRequest {
            user_id: "u1".to_string(),
            initial_cash: 100,
        })
        .unwrap();

    let error = store
        .create_limit_order(
            CreateLimitOrderRequest {
                user_id: "u1".to_string(),
                code: "300274.XSHE".to_string(),
                side: "buy".to_string(),
                price: 100,
                qty: 2,
            },
            1_000,
        )
        .unwrap_err();

    assert!(matches!(error, TradingStoreError::InsufficientCash { .. }));
    let _ = fs::remove_file(path);
}
