use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "config/conf.toml";

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub db: DbConfig,
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
