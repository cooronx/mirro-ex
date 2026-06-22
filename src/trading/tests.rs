use std::collections::HashMap;
use std::fs;

use rusqlite::{Connection, params};

use super::{
    CancelOrderRequest, CreateAccountRequest, CreateLimitOrderRequest, SIDE_BUY, TradingStore,
    TradingStoreError,
};
use crate::matcher::order_book::LevelSnapshot;

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

    assert_eq!(order.status, "new");
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

    let marketable_levels = vec![LevelSnapshot {
        price: 90,
        total_qty: 5,
        order_count: 1,
    }];

    let (fill_count, queue_ahead_qty) = store
        .initialize_limit_order_queue(&order, &marketable_levels, 0, 1_100)
        .unwrap();
    assert_eq!(fill_count, 1);
    assert_eq!(queue_ahead_qty, Some(0));
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

    let order = store
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

    let marketable_levels = vec![LevelSnapshot {
        price: 100,
        total_qty: 6,
        order_count: 1,
    }];
    store
        .initialize_limit_order_queue(&order, &marketable_levels, 0, 1_100)
        .unwrap();

    let orders = store.list_orders("u1").unwrap();
    assert_eq!(orders[0].filled_qty, 6);
    assert_eq!(orders[0].status, "filled");
    assert_eq!(account_cash(&path, "u1"), (1_600, 1_600, 0));
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (4, 4, 0, 80));
    let _ = fs::remove_file(path);
}

#[test]
fn queued_buy_order_waits_for_real_trade_volume_before_filling() {
    let (store, path) = test_store("queue-fill");
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

    let (fill_count, queue_ahead_qty) = store
        .initialize_limit_order_queue(&order, &[], 8, 1_050)
        .unwrap();
    assert_eq!(fill_count, 0);
    assert_eq!(queue_ahead_qty, Some(8));
    let mut order_queues = HashMap::from([(order.order_id.clone(), queue_ahead_qty.unwrap())]);

    assert_eq!(
        store
            .match_queued_limit_orders("300274.XSHE", SIDE_BUY, 100, 5, 1_100, &mut order_queues)
            .unwrap(),
        0
    );
    assert_eq!(order_queues.get(&order.order_id), Some(&3));

    assert_eq!(
        store
            .match_queued_limit_orders("300274.XSHE", SIDE_BUY, 100, 6, 1_200, &mut order_queues)
            .unwrap(),
        1
    );
    let orders = store.list_orders("u1").unwrap();
    assert_eq!(orders[0].filled_qty, 3);
    assert_eq!(orders[0].status, "partially_filled");
    assert_eq!(order_queues.get(&order.order_id), Some(&0));
    assert_eq!(account_cash(&path, "u1"), (9_700, 9_000, 700));
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (3, 3, 0, 100));
    let _ = fs::remove_file(path);
}

#[test]
fn cancel_new_buy_order_releases_frozen_cash() {
    let (store, path) = test_store("cancel-new-buy");
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

    let canceled = store
        .cancel_order(
            CancelOrderRequest {
                user_id: "u1".to_string(),
                order_id: order.order_id,
            },
            1_100,
        )
        .unwrap();

    assert_eq!(canceled.status, "canceled");
    assert_eq!(canceled.updated_at, 1_100);
    assert_eq!(account_cash(&path, "u1"), (10_000, 10_000, 0));
    let _ = fs::remove_file(path);
}

#[test]
fn cancel_partially_filled_buy_order_releases_remaining_cash() {
    let (store, path) = test_store("cancel-partial-buy");
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
    store
        .initialize_limit_order_queue(
            &order,
            &[LevelSnapshot {
                price: 90,
                total_qty: 4,
                order_count: 1,
            }],
            0,
            1_050,
        )
        .unwrap();

    let canceled = store
        .cancel_order(
            CancelOrderRequest {
                user_id: "u1".to_string(),
                order_id: order.order_id,
            },
            1_100,
        )
        .unwrap();

    assert_eq!(canceled.filled_qty, 4);
    assert_eq!(canceled.status, "canceled");
    assert_eq!(account_cash(&path, "u1"), (9_640, 9_640, 0));
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (4, 4, 0, 90));
    let _ = fs::remove_file(path);
}

#[test]
fn cancel_sell_order_releases_remaining_position() {
    let (store, path) = test_store("cancel-sell");
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
    let order = store
        .create_limit_order(
            CreateLimitOrderRequest {
                user_id: "u1".to_string(),
                code: "300274.XSHE".to_string(),
                side: "sell".to_string(),
                price: 100,
                qty: 6,
            },
            1_000,
        )
        .unwrap();

    let canceled = store
        .cancel_order(
            CancelOrderRequest {
                user_id: "u1".to_string(),
                order_id: order.order_id,
            },
            1_100,
        )
        .unwrap();

    assert_eq!(canceled.status, "canceled");
    assert_eq!(position_qty(&path, "u1", "300274.XSHE"), (10, 10, 0, 80));
    let _ = fs::remove_file(path);
}

#[test]
fn filled_order_cannot_be_cancelled() {
    let (store, path) = test_store("cancel-filled");
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
                qty: 5,
            },
            1_000,
        )
        .unwrap();
    store
        .initialize_limit_order_queue(
            &order,
            &[LevelSnapshot {
                price: 100,
                total_qty: 5,
                order_count: 1,
            }],
            0,
            1_050,
        )
        .unwrap();

    let error = store
        .cancel_order(
            CancelOrderRequest {
                user_id: "u1".to_string(),
                order_id: order.order_id,
            },
            1_100,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        TradingStoreError::OrderNotCancelable { .. }
    ));
    let _ = fs::remove_file(path);
}

#[test]
fn canceled_queued_order_is_not_matched_later() {
    let (store, path) = test_store("cancel-queued");
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
    let (_, queue_ahead_qty) = store
        .initialize_limit_order_queue(&order, &[], 5, 1_050)
        .unwrap();
    let mut order_queues = HashMap::from([(order.order_id.clone(), queue_ahead_qty.unwrap())]);

    store
        .cancel_order(
            CancelOrderRequest {
                user_id: "u1".to_string(),
                order_id: order.order_id.clone(),
            },
            1_100,
        )
        .unwrap();
    assert_eq!(
        store
            .match_queued_limit_orders("300274.XSHE", SIDE_BUY, 100, 20, 1_200, &mut order_queues)
            .unwrap(),
        0
    );
    let orders = store.list_orders("u1").unwrap();
    assert_eq!(orders[0].status, "canceled");
    assert_eq!(orders[0].filled_qty, 0);
    assert_eq!(account_cash(&path, "u1"), (10_000, 10_000, 0));
    let _ = fs::remove_file(path);
}

#[test]
fn list_positions_returns_user_positions_and_optional_code_filter() {
    let (store, path) = test_store("list-positions");
    store
        .create_account(CreateAccountRequest {
            user_id: "u1".to_string(),
            initial_cash: 10_000,
        })
        .unwrap();
    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO positions (
                    user_id,
                    code,
                    long_qty,
                    available_qty,
                    frozen_qty,
                    avg_price,
                    updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params!["u1", "300274.XSHE", 10, 8, 2, 100, 1_000],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO positions (
                    user_id,
                    code,
                    long_qty,
                    available_qty,
                    frozen_qty,
                    avg_price,
                    updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params!["u1", "600000.XSHG", 20, 20, 0, 200, 1_100],
            )
            .unwrap();
    }

    let positions = store.list_positions("u1", None).unwrap();
    assert_eq!(positions.len(), 2);
    assert_eq!(positions[0].code, "300274.XSHE");
    assert_eq!(positions[1].code, "600000.XSHG");

    let positions = store.list_positions("u1", Some("300274.XSHE")).unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].long_qty, 10);
    assert_eq!(positions[0].available_qty, 8);
    assert_eq!(positions[0].frozen_qty, 2);

    let positions = store.list_positions("u1", Some("000001.XSHE")).unwrap();
    assert!(positions.is_empty());
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
