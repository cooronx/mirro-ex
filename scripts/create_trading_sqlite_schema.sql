PRAGMA foreign_keys = ON;

BEGIN;

-- accounts:
-- 每个用户一行，保存当前资金状态。
CREATE TABLE IF NOT EXISTS accounts (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    cash_balance INTEGER NOT NULL CHECK (cash_balance >= 0),
    available_cash INTEGER NOT NULL CHECK (available_cash >= 0),
    frozen_cash INTEGER NOT NULL CHECK (frozen_cash >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (cash_balance = available_cash + frozen_cash)
);

-- positions:
-- 每个用户、每个标的一行，保存当前持仓状态。
-- 数量字段统一使用“股/份”整数，价格字段统一使用 1e-4 精度整数。
CREATE TABLE IF NOT EXISTS positions (
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    long_qty INTEGER NOT NULL CHECK (long_qty >= 0),
    available_qty INTEGER NOT NULL CHECK (available_qty >= 0),
    frozen_qty INTEGER NOT NULL CHECK (frozen_qty >= 0),
    avg_price INTEGER NOT NULL CHECK (avg_price >= 0),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, code),
    FOREIGN KEY (user_id) REFERENCES accounts(user_id) ON DELETE CASCADE,
    CHECK (long_qty = available_qty + frozen_qty)
);

-- orders:
-- 保存用户委托及其当前状态。
-- status 最小集合：new / working / partially_filled / filled / canceled / rejected
-- order_type 第一版最小集合：limit / market
-- side 第一版最小集合：buy / sell
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    order_type TEXT NOT NULL CHECK (order_type IN ('limit', 'market')),
    price INTEGER NOT NULL CHECK (price >= 0),
    qty INTEGER NOT NULL CHECK (qty > 0),
    filled_qty INTEGER NOT NULL DEFAULT 0 CHECK (filled_qty >= 0 AND filled_qty <= qty),
    status TEXT NOT NULL CHECK (
        status IN ('new', 'working', 'partially_filled', 'filled', 'canceled', 'rejected')
    ),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES accounts(user_id) ON DELETE CASCADE
);

-- fills:
-- 保存成交记录。一张订单可以对应多条成交。
CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    price INTEGER NOT NULL CHECK (price >= 0),
    qty INTEGER NOT NULL CHECK (qty > 0),
    filled_at INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES accounts(user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_positions_user ON positions(user_id);
CREATE INDEX IF NOT EXISTS idx_accounts_username ON accounts(username);
CREATE INDEX IF NOT EXISTS idx_orders_user_status_created_at
    ON orders(user_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_code_status_created_at
    ON orders(code, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fills_user_filled_at
    ON fills(user_id, filled_at DESC);
CREATE INDEX IF NOT EXISTS idx_fills_order_filled_at
    ON fills(order_id, filled_at DESC);

COMMIT;
