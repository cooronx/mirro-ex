use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

pub const DEFAULT_CONFIG_PATH: &str = "config/conf.toml";

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub db: DbConfig,
    pub replay: ReplayConfig,
    pub nats: NatsConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: usize,
    pub tables: DbTableConfig,
    #[serde(default)]
    pub schema: DbSchemaConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbTableConfig {
    pub sh_order: String,
    pub sz_order: String,
    pub transaction: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct DbSchemaConfig {
    pub market_schema_path: String,
    pub trading_db_path: String,
    pub trading_schema_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ReplayConfig {
    pub lane_queue_capacity: usize,
    #[serde(default = "default_orderbook_workers")]
    pub orderbook_workers: usize,
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    #[serde(default = "default_replay_batch_size")]
    pub batch_size: i64,
    #[serde(default = "default_snapshot_depth")]
    pub snapshot_depth: usize,
    #[serde(default = "default_write_snapshot_parquet")]
    pub write_snapshot_parquet: bool,
    #[serde(default = "default_snapshot_parquet_dir")]
    pub snapshot_parquet_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsConfig {
    pub url: String,
    pub subject: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct WebConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub directory: String,
    pub file_prefix: String,
    pub to_stdout: bool,
    pub to_file: bool,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        Self::from_path(DEFAULT_CONFIG_PATH)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).with_context(|| {
            format!(
                "failed to read config file at {}. Create it from config/db.toml.example first",
                path.display()
            )
        })?;

        toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file at {}", path.display()))
    }
}

fn default_tick_interval_ms() -> u64 {
    5
}

fn default_orderbook_workers() -> usize {
    6
}

fn default_replay_batch_size() -> i64 {
    100_000
}

fn default_snapshot_depth() -> usize {
    10
}

fn default_write_snapshot_parquet() -> bool {
    true
}

fn default_snapshot_parquet_dir() -> String {
    "data/order_book_snapshot".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_web_host() -> String {
    "127.0.0.1".to_string()
}

fn default_market_schema_path() -> String {
    "scripts/create_local_clickhouse_tables.sql".to_string()
}

fn default_trading_db_path() -> String {
    "data/trading.db".to_string()
}

fn default_trading_schema_path() -> String {
    "scripts/create_trading_sqlite_schema.sql".to_string()
}

fn default_web_port() -> u16 {
    5800
}

fn default_log_directory() -> String {
    "logs".to_string()
}

fn default_log_file_prefix() -> String {
    "mirro-ex".to_string()
}

fn default_true() -> bool {
    true
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            directory: default_log_directory(),
            file_prefix: default_log_file_prefix(),
            to_stdout: default_true(),
            to_file: default_true(),
        }
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            host: default_web_host(),
            port: default_web_port(),
        }
    }
}

impl Default for DbSchemaConfig {
    fn default() -> Self {
        Self {
            market_schema_path: default_market_schema_path(),
            trading_db_path: default_trading_db_path(),
            trading_schema_path: default_trading_schema_path(),
        }
    }
}
