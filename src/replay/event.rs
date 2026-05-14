use crate::common::{L2Order, L2Transaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayEvent {
    Order(L2Order),
    Transaction(L2Transaction),
}
