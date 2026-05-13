pub mod order;
pub mod transaction;

pub use order::{L2Order, Market, OrderDirection, OrderType};
pub use transaction::L2Transaction;

// Database decimals are Decimal(20, 4); replay types store them as scaled integers.
pub const DECIMAL_SCALE_4: i64 = 10_000;
