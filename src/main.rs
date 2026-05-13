mod config;
mod common;
mod db;
mod sim_clock;


use core::range;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clickhouse::Row;
use serde::Deserialize;

use crate::config::{AppConfig, DbConfig};
use crate::db::dbpool::DbPool;
use crate::db::sh_order_query::{SHOrderByRangeQuery, SHOrderRangeQuery, query_sh_order_message_ranges, query_sh_orders_by_range};
use crate::sim_clock::SimClock;

#[derive(Row, Deserialize, Debug)]
struct MyRow {
    min_seq: i64,
    max_seq: i64,
    channel: i64,
}

async fn test_hell(config: &DbConfig) -> Result<()> {
    let db_pool = DbPool::new(config)?;
    let query = SHOrderRangeQuery::new("2026-05-12", 1778549940000, 1778550120000);
    let ranges = query_sh_order_message_ranges(&db_pool, &query).await?;
    for range in ranges {
        // println!("{:?}",range);
        let query = SHOrderByRangeQuery::new("2026-05-12", range.channel, range.begin_message_number, range.end_message_number);
        let thing = query_sh_orders_by_range(&db_pool, &query).await?;
        println!("{}",thing.len());
    }

    Ok(())
}



#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let db_config = config.db;
    test_hell(&db_config).await?;
    Ok(())
}
