use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use clickhouse::Client;
use rusqlite::Connection;
use tracing::info;

use crate::config::DbConfig;

use super::dbpool::build_client;

pub async fn initialize(config: &DbConfig) -> Result<()> {
    initialize_market_schema(config).await?;
    initialize_trading_schema(config)?;
    Ok(())
}

async fn initialize_market_schema(config: &DbConfig) -> Result<()> {
    let schema_path = Path::new(&config.schema.market_schema_path);
    let raw = fs::read_to_string(schema_path).with_context(|| {
        format!(
            "failed to read clickhouse schema file at {}",
            schema_path.display()
        )
    })?;

    let client = build_client(config);
    for statement in split_sql_statements(&raw) {
        execute_clickhouse_statement(&client, statement).await?;
    }

    info!(
        schema_path = %schema_path.display(),
        database = %config.database,
        "initialized clickhouse market schema"
    );
    Ok(())
}

fn initialize_trading_schema(config: &DbConfig) -> Result<()> {
    let db_path = Path::new(&config.schema.trading_db_path);
    if let Some(parent) = db_path.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create trading database directory at {}",
                parent.display()
            )
        })?;
    }

    let schema_path = Path::new(&config.schema.trading_schema_path);
    let schema_sql = fs::read_to_string(schema_path).with_context(|| {
        format!(
            "failed to read trading sqlite schema file at {}",
            schema_path.display()
        )
    })?;

    let connection = Connection::open(db_path).with_context(|| {
        format!(
            "failed to open sqlite trading database at {}",
            db_path.display()
        )
    })?;

    connection.execute_batch(&schema_sql).with_context(|| {
        format!(
            "failed to initialize trading sqlite schema at {}",
            db_path.display()
        )
    })?;

    info!(
        db_path = %db_path.display(),
        schema_path = %schema_path.display(),
        "initialized sqlite trading schema"
    );
    Ok(())
}

async fn execute_clickhouse_statement(client: &Client, statement: &str) -> Result<()> {
    client
        .query(statement)
        .execute()
        .await
        .with_context(|| format!("failed to execute clickhouse schema statement: {statement}"))
}

fn split_sql_statements(raw: &str) -> Vec<&str> {
    raw.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .collect()
}
