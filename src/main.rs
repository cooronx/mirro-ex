mod config;
mod common;
mod db;
mod sim_clock;


use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clickhouse::Row;
use serde::Deserialize;

use crate::config::{AppConfig, DbConfig};
use crate::sim_clock::SimClock;

#[derive(Row, Deserialize, Debug)]
struct MyRow {
    min_seq: i64,
    max_seq: i64,
    channel: i64,
}

async fn test_hell(config: &DbConfig) -> Result<()> {
    let client = crate::db::dbpool::build_client(config);

    let sql_str = r#"SELECT 
                            MIN(message_number) AS min_seq,
                            MAX(message_number) AS max_seq,
                            channel
                            FROM L2_order_rt_distributed
                            WHERE EventDate = toDate('2026-05-12')
                            GROUP BY channel
                            ORDER BY channel"#;
    let mut row_s = client
        .query(sql_str)
        .fetch::<MyRow>()
        .context("failed to start clickhouse query")?;
    while let Some(row) = row_s
        .next()
        .await
        .context("failed to fetch next clickhouse row")?
    {
        println!("{:?}", row);
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
