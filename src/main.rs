mod config;
mod common;
mod db;
mod replay;
mod sim_clock;

use anyhow::{Context, Result, bail};

use crate::config::AppConfig;
use crate::replay::{PrintReplayHandler, ReplayController, ReplayRequest};

fn parse_replay_request_from_args() -> Result<ReplayRequest> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: mirro-ex <start_time_ms> <end_time_ms> [replay_speed]";

    let start_time_ms = args
        .next()
        .context(usage)?
        .parse::<i64>()
        .context("failed to parse start_time_ms as i64")?;
    let end_time_ms = args
        .next()
        .context(usage)?
        .parse::<i64>()
        .context("failed to parse end_time_ms as i64")?;
    let replay_speed = match args.next() {
        Some(speed) => speed
            .parse::<f64>()
            .context("failed to parse replay_speed as f64")?,
        None => 1.0,
    };

    if args.next().is_some() {
        bail!("{usage}");
    }

    Ok(ReplayRequest {
        start_time_ms,
        end_time_ms,
        replay_speed,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    let request = parse_replay_request_from_args()?;
    let controller = ReplayController::new(config.db, config.replay);
    let mut handler = PrintReplayHandler;

    let report = controller.replay(request, &mut handler).await?;
    println!("{report:#?}");
    Ok(())
}
