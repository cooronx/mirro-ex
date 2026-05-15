mod config;
mod common;
mod db;
mod replay;
mod sim_clock;


use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Result;
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::config::{AppConfig, DbConfig, ReplayConfig};
use crate::db::sh_order_query::SHOrderRangeQuery;
use crate::db::dbpool::DbPool;
use crate::db::sz_order_query::SZOrderRangeQuery;
use crate::db::transaction_query::TransactionRangeQuery;
use crate::replay::{ReplayCoordinator, ReplayDataKind, ReplayDbReader, ReplayEvent};
use crate::sim_clock::SimClock;

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

    let query = SZOrderRangeQuery::new(
        "2026-05-12",
        1778549940000,
        1778550000000,
        &config.tables.sz_order,
    );
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

async fn test_replay_0930_0931(config: &DbConfig) -> Result<()> {
    const DAY: &str = "2026-05-12";
    const START_TIME_MS: i64 = 1_778_549_400_000;
    const END_TIME_MS: i64 = 1_778_549_460_000;
    const BATCH_SIZE: i64 = 100_0000;

    let total_start = Instant::now();
    let db_pool = DbPool::new(config)?;

    println!("db_pool_size={}", db_pool.size());
    println!(
        "replay_window day={} start_time_ms={} end_time_ms={}",
        DAY, START_TIME_MS, END_TIME_MS
    );

    let sh_query = SHOrderRangeQuery::new(
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        &config.tables.sh_order,
    );
    let sz_query = SZOrderRangeQuery::new(
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        &config.tables.sz_order,
    );
    let transaction_query = TransactionRangeQuery::new(
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        &config.tables.transaction,
    );

    let reader_build_start = Instant::now();
    let mut reader = ReplayDbReader::from_range_queries(
        db_pool.clone(),
        BATCH_SIZE,
        Some(&sh_query),
        Some(&sz_query),
        Some(&transaction_query),
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
    let mut total_order_rows = 0usize;
    let mut total_transaction_rows = 0usize;
    let mut round = 0usize;

    loop {
        let round_start = Instant::now();
        let fetched_batches = reader.fetch_next_batches(db_pool.size()).await?;
        if fetched_batches.is_empty() {
            break;
        }

        round += 1;
        let mut round_order_rows = 0usize;
        let mut round_transaction_rows = 0usize;

        for batch in &fetched_batches {
            for event in &batch.events {
                match event {
                    ReplayEvent::Order(_) => round_order_rows += 1,
                    ReplayEvent::Transaction(_) => round_transaction_rows += 1,
                }
            }
        }

        println!(
            "round={} channels={} order_rows={} transaction_rows={} elapsed_ms={}",
            round,
            fetched_batches.len(),
            round_order_rows,
            round_transaction_rows,
            round_start.elapsed().as_millis()
        );

        total_batches += fetched_batches.len();
        total_order_rows += round_order_rows;
        total_transaction_rows += round_transaction_rows;

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
    let total_rows = total_order_rows + total_transaction_rows;

    for stat in &stats {
        println!(
            "channel={} batches={} rows={}",
            stat.channel, stat.batches, stat.rows
        );
    }

    println!(
        "replay_done channels={} total_batches={} total_order_rows={} total_transaction_rows={} total_rows={} wall_elapsed_ms={}",
        stats.len(),
        total_batches,
        total_order_rows,
        total_transaction_rows,
        total_rows,
        fetch_elapsed.as_millis()
    );
    println!("total_elapsed_ms={}", total_start.elapsed().as_millis());

    Ok(())
}

async fn test_replay_coordinator_0930_0931(
    db_config: &DbConfig,
    replay_config: &ReplayConfig,
) -> Result<()> {
    const DAY: &str = "2026-05-12";
    const START_TIME_MS: i64 = 1_778_549_400_000;
    const END_TIME_MS: i64 = 1_778_549_460_000;
    const BATCH_SIZE: i64 = 1_000_000;
    const TICK_INTERVAL_MS: u64 = 100;
    const REPLAY_SPEED: f64 = 1.0;
    const STALL_TICK_LIMIT: usize = 20;

    let total_start = Instant::now();
    let db_pool = DbPool::new(db_config)?;

    println!("db_pool_size={}", db_pool.size());
    println!(
        "coordinator_window day={} start_time_ms={} end_time_ms={} batch_size={} tick_interval_ms={} replay_speed={}",
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        BATCH_SIZE,
        TICK_INTERVAL_MS,
        REPLAY_SPEED
    );

    let sh_query = SHOrderRangeQuery::new(
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        &db_config.tables.sh_order,
    );
    let sz_query = SZOrderRangeQuery::new(
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        &db_config.tables.sz_order,
    );
    let transaction_query = TransactionRangeQuery::new(
        DAY,
        START_TIME_MS,
        END_TIME_MS,
        &db_config.tables.transaction,
    );

    let reader_build_start = Instant::now();
    let reader = ReplayDbReader::from_range_queries(
        db_pool,
        BATCH_SIZE,
        Some(&sh_query),
        Some(&sz_query),
        Some(&transaction_query),
    )
    .await?;
    let reader_build_elapsed = reader_build_start.elapsed();
    let cursor_count = reader.cursors().len();

    let mut order_channels = BTreeMap::<i64, usize>::new();
    let mut transaction_channels = BTreeMap::<i64, usize>::new();
    for cursor in reader.cursors() {
        match cursor.range.data_kind {
            ReplayDataKind::Order => {
                *order_channels.entry(cursor.range.channel).or_default() += 1;
            }
            ReplayDataKind::Transaction => {
                *transaction_channels.entry(cursor.range.channel).or_default() += 1;
            }
        }
    }
    let duplicate_order_channels: Vec<(i64, usize)> = order_channels
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .collect();
    let duplicate_transaction_channels: Vec<(i64, usize)> = transaction_channels
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .collect();

    let clock = SimClock::new(START_TIME_MS as u64, END_TIME_MS as u64, REPLAY_SPEED)?;
    let mut coordinator = ReplayCoordinator::from_reader(
        reader,
        clock,
        TICK_INTERVAL_MS,
        replay_config.lane_queue_capacity,
    )
    .await?;
    let bootstrap_start = Instant::now();
    coordinator.bootstrap().await?;
    let bootstrap_elapsed = bootstrap_start.elapsed();

    println!(
        "coordinator_ready cursors={} duplicate_order_channels={:?} duplicate_transaction_channels={:?} reader_elapsed_ms={} bootstrap_elapsed_ms={}",
        cursor_count,
        duplicate_order_channels,
        duplicate_transaction_channels,
        reader_build_elapsed.as_millis(),
        bootstrap_elapsed.as_millis()
    );

    let run_start = Instant::now();
    let mut tick = interval(Duration::from_millis(coordinator.tick_interval_ms()));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut tick_count = 0usize;
    let mut total_events = 0usize;
    let mut total_order_rows = 0usize;
    let mut total_transaction_rows = 0usize;
    let mut consecutive_empty_ticks = 0usize;

    loop {
        tick.tick().await;
        let tick_start = Instant::now();
        let result = coordinator.poll_ready_events().await?;
        tick_count += 1;

        let mut order_rows = 0usize;
        let mut transaction_rows = 0usize;
        for event in &result.events {
            match event {
                ReplayEvent::Order(order) => {
                    println!("{:?}",order);
                },
                ReplayEvent::Transaction(transaction) => {
                    println!("{:?}",transaction);
                },
            }
        }

        total_events += result.events.len();
        total_order_rows += order_rows;
        total_transaction_rows += transaction_rows;

        if result.events.is_empty() {
            consecutive_empty_ticks += 1;
        } else {
            consecutive_empty_ticks = 0;
        }

        // println!(
        //     "tick={} sim_now_ms={} safe_emit_time_ms={} lag_ms={} events={} order_rows={} transaction_rows={} finished={} elapsed_ms={}",
        //     tick_count,
        //     result.sim_now_ms,
        //     result
        //         .safe_emit_time_ms
        //         .map_or_else(|| "none".to_string(), |value| value.to_string()),
        //     result.lag_ms,
        //     result.events.len(),
        //     order_rows,
        //     transaction_rows,
        //     result.finished,
        //     tick_start.elapsed().as_millis()
        // );

        if result.finished {
            break;
        }

        if consecutive_empty_ticks >= STALL_TICK_LIMIT {
            println!(
                "coordinator_stalled ticks_without_events={} total_events={} total_order_rows={} total_transaction_rows={}",
                consecutive_empty_ticks,
                total_events,
                total_order_rows,
                total_transaction_rows
            );
            for lane_summary in coordinator.debug_frontier_lanes(8) {
                println!("stall_frontier {}", lane_summary);
            }
            break;
        }
    }

    println!(
        "coordinator_done ticks={} total_events={} total_order_rows={} total_transaction_rows={} wall_elapsed_ms={} total_elapsed_ms={}",
        tick_count,
        total_events,
        total_order_rows,
        total_transaction_rows,
        run_start.elapsed().as_millis(),
        total_start.elapsed().as_millis()
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let db_config = config.db;
    let replay_config = config.replay;
    test_replay_coordinator_0930_0931(&db_config, &replay_config).await?;
    Ok(())
}
