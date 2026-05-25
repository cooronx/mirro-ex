use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveTime};
use serde::Deserialize;
use serde::de::{self, Deserializer};

const DEFAULT_CONFIG_PATH: &str = "config/conf.toml";
const REPLAY_DATE_FORMAT: &str = "%Y-%m-%d";
const REPLAY_TIME_FORMAT_WITH_MILLISECONDS: &str = "%H:%M:%S%.3f";
const REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS: &str = "%H:%M:%S";

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
    #[serde(default = "default_replay_batch_size")]
    pub batch_size: i64,
    #[serde(deserialize_with = "deserialize_replay_date")]
    pub replay_start_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_replay_date")]
    pub replay_end_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_replay_time")]
    pub replay_start_time: NaiveTime,
    #[serde(deserialize_with = "deserialize_replay_time")]
    pub replay_end_time: NaiveTime,
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

fn default_replay_batch_size() -> i64 {
    100_000
}

fn deserialize_replay_date<'de, D>(deserializer: D) -> std::result::Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_replay_date(&raw).map_err(de::Error::custom)
}

fn deserialize_replay_time<'de, D>(deserializer: D) -> std::result::Result<NaiveTime, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_replay_time(&raw).map_err(de::Error::custom)
}

fn parse_replay_date(raw: &str) -> std::result::Result<NaiveDate, String> {
    let trimmed = raw.trim();
    NaiveDate::parse_from_str(trimmed, REPLAY_DATE_FORMAT)
        .map_err(|_| format!("invalid replay date format: {raw}, expected YYYY-MM-DD"))
}

fn parse_replay_time(raw: &str) -> std::result::Result<NaiveTime, String> {
    let trimmed = raw.trim();
    NaiveTime::parse_from_str(trimmed, REPLAY_TIME_FORMAT_WITH_MILLISECONDS)
        .or_else(|_| NaiveTime::parse_from_str(trimmed, REPLAY_TIME_FORMAT_WITHOUT_MILLISECONDS))
        .map_err(|_| {
            format!("invalid replay time format: {raw}, expected HH:MM:SS or HH:MM:SS.sss")
        })
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    #[test]
    fn replay_batch_size_defaults_when_omitted() {
        let raw = r#"
[db]
url = "http://127.0.0.1:8123"
user = "user"
password = "password"
database = "db"
pool_size = 5

[db.tables]
sh_order = "sh"
sz_order = "sz"
transaction = "tx"

[replay]
lane_queue_capacity = 1
replay_start_date = "2026-05-12"
replay_end_date = "2026-05-12"
replay_start_time = "09:30:00.000"
replay_end_time = "09:31:00.000"
"#;

        let config: AppConfig = toml::from_str(raw).unwrap();

        assert_eq!(config.replay.batch_size, 100_000);
    }
}
