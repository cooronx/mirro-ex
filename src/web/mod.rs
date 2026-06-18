use std::sync::Arc;

use anyhow::Result;
use salvo::conn::tcp::TcpListener;
use salvo::prelude::*;
use tracing::info;

use crate::config::AppConfig;
use crate::replay_manager::ReplayManager;
use crate::trading::{TradingStore, trading_db_path_from_config};
use crate::webdata::EventBus;
use crate::webdata::MarketState;

mod common;
mod events;
mod market;
mod replay;
mod static_files;
mod trading;

pub async fn serve(config: AppConfig) -> Result<()> {
    let host = config.web.host.clone();
    let port = config.web.port;
    let trading_db_path = trading_db_path_from_config(&config.db.schema.trading_db_path)?;
    let event_bus = EventBus::new(4096);
    let trading_store = Arc::new(TradingStore::with_event_bus(
        trading_db_path,
        event_bus.clone(),
    ));
    let market_state = MarketState::with_event_bus(event_bus.clone());
    let manager = Arc::new(ReplayManager::with_event_bus(
        config,
        market_state.clone(),
        event_bus.clone(),
    ));
    let router = Router::new()
        .push(events::router(event_bus))
        .push(replay::router(manager.clone()))
        .push(trading::router(trading_store, manager))
        .push(market::router(market_state))
        .push(static_files::router());

    info!(host = %host, port = port, "starting salvo web server");

    let acceptor = TcpListener::new(format!("{host}:{port}")).bind().await;
    Server::new(acceptor).serve(router).await;
    Ok(())
}
