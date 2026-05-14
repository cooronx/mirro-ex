mod config;
mod common;
mod db;
mod replay;
mod sim_clock;


use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Result;

use crate::config::{AppConfig, DbConfig};
use crate::db::dbpool::DbPool;
use crate::db::sz_order_query::SZOrderRangeQuery;
use crate::replay::ReplayDbReader;

#[derive(Debug)]
struct FetchStat {
    channel: i64,
    batches: usize,
    rows: usize,
}

async fn test_hell(config: &DbConfig) -> Result<()> {
    let total_start = Instant::now();
    let db_pool = DbPool::new(config)?;

    println!("db_pool_size={}", db_pool.size());

    let query = SZOrderRangeQuery::new("2026-05-12", 1778549940000, 1778550000000);
    let reader_build_start = Instant::now();
    let mut reader = ReplayDbReader::from_order_range_queries(
        db_pool.clone(),
        100_000,
        None,
        Some(&query),
    )
    .await?;
    let reader_build_elapsed = reader_build_start.elapsed();

    println!(
        "reader_ready cursors={} batch_size={} elapsed_ms={}",
        reader.cursors().len(),
        reader.batch_size(),
        reader_build_elapsed.as_millis()
    );

    let fetch_start = Instant::now();
    let mut stats_by_channel = BTreeMap::<i64, FetchStat>::new();
    let mut total_batches = 0usize;
    let mut round = 0usize;

    loop {
        let round_start = Instant::now();
        let fetched_batches = reader.fetch_next_batches(db_pool.size()).await?;
        if fetched_batches.is_empty() {
            break;
        }

        round += 1;
        let round_rows: usize = fetched_batches
            .iter()
            .map(|batch| batch.events.len())
            .sum();

        println!(
            "round={} channels={} rows={} elapsed_ms={}",
            round,
            fetched_batches.len(),
            round_rows,
            round_start.elapsed().as_millis()
        );

        total_batches += fetched_batches.len();
        for batch in fetched_batches {
            let stat = stats_by_channel.entry(batch.channel).or_insert(FetchStat {
                channel: batch.channel,
                batches: 0,
                rows: 0,
            });
            stat.batches += 1;
            stat.rows += batch.events.len();
        }
    }
    let fetch_elapsed = fetch_start.elapsed();

    let stats: Vec<FetchStat> = stats_by_channel.into_values().collect();
    let total_rows: usize = stats.iter().map(|stat| stat.rows).sum();
    let max_batches_per_channel = stats.iter().map(|stat| stat.batches).max().unwrap_or(0);
    let min_batches_per_channel = stats.iter().map(|stat| stat.batches).min().unwrap_or(0);
    let avg_rows_per_channel = if stats.is_empty() {
        0
    } else {
        total_rows / stats.len()
    };

    for stat in &stats {
        println!(
            "channel={} batches={} rows={}",
            stat.channel, stat.batches, stat.rows
        );
    }

    println!(
        "fetch_done channels={} total_batches={} total_rows={} wall_elapsed_ms={} max_batches_per_channel={} min_batches_per_channel={} avg_rows_per_channel={}",
        stats.len(),
        total_batches,
        total_rows,
        fetch_elapsed.as_millis(),
        max_batches_per_channel,
        min_batches_per_channel,
        avg_rows_per_channel
    );
    println!("total_elapsed_ms={}", total_start.elapsed().as_millis());

    Ok(())
}



#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let db_config = config.db;
    test_hell(&db_config).await?;
    Ok(())
}
