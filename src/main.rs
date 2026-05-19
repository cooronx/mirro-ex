mod config;
mod common;
mod db;
mod replay;
mod sim_clock;

use anyhow::Result;

use crate::config::AppConfig;
use crate::replay::{PrintReplayHandler, ReplayController, ReplayRequest};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let request = ReplayRequest {
        start_time_ms: config.replay.replay_start_time,
        end_time_ms: config.replay.replay_end_time,
        replay_speed: config.replay.replay_speed,
    };
    let controller = ReplayController::new(config.db, config.replay);
    let mut handler = PrintReplayHandler;

    let report = controller.replay(request, &mut handler).await?;
    println!("{report:#?}");
    Ok(())
}
