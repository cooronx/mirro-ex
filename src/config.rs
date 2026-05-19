use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{FixedOffset, NaiveDateTime, TimeZone};
use serde::Deserialize;
use serde::de::{self, Deserializer};

const DEFAULT_CONFIG_PATH: &str = "config/conf.toml";
const REPLAY_TIME_FORMAT_WITH_MILLISECONDS: &str = "%Y-%m-%d %H:%M:%S%.3f";
const REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub db: DbConfig,
    pub replay: ReplayConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: usize,
    pub tables: DbTableConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbTableConfig {
    pub sh_order: String,
    pub sz_order: String,
    pub transaction: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ReplayConfig {
    pub lane_queue_capacity: usize,
    #[serde(deserialize_with = "deserialize_east8_timestamp_ms")]
    pub replay_start_time: i64,
    #[serde(deserialize_with = "deserialize_east8_timestamp_ms")]
    pub replay_end_time: i64,
    #[serde(default = "default_replay_speed")]
    pub replay_speed: f64,
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

fn default_replay_speed() -> f64 {
    1.0
}

fn asia_shanghai_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("Asia/Shanghai offset should be valid")
}

fn deserialize_east8_timestamp_ms<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_east8_timestamp_ms(&raw).map_err(de::Error::custom)
}

fn parse_east8_timestamp_ms(raw: &str) -> std::result::Result<i64, String> {
    let trimmed = raw.trim();
    let naive = NaiveDateTime::parse_from_str(trimmed, REPLAY_TIME_FORMAT_WITH_MILLISECONDS)
        .or_else(|_| NaiveDateTime::parse_from_str(trimmed, REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS))
        .map_err(|_| {
            format!(
                "invalid replay time format: {raw}, expected YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.sss"
            )
        })?;
    let datetime = asia_shanghai_offset()
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| format!("replay time is ambiguous or invalid in Asia/Shanghai: {raw}"))?;

    Ok(datetime.timestamp_millis())
}