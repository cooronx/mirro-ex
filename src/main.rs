mod app;
mod common;
mod config;
mod db;
mod logging;
mod marketdata;
mod matcher;
mod publisher;
mod replay;
mod replay_manager;
mod sim_clock;
mod trading;
mod web;

use anyhow::Result;
use tracing::error;

use crate::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let _logging_guard = logging::init(&config.logging)?;
    db::init::initialize(&config.db).await?;

    if let Err(err) = web::serve(config).await {
        let mut chain = err.to_string();
        let mut source = err.source();
        while let Some(cause) = source {
            chain.push_str(": ");
            chain.push_str(&cause.to_string());
            source = cause.source();
        }
        error!(error_chain = %chain, "mirro-ex exited with error");
        return Err(err);
    }

    Ok(())
}
