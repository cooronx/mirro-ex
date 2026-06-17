use std::sync::Arc;

use anyhow::Result;
use salvo::conn::tcp::TcpListener;
use salvo::prelude::*;
use tracing::info;

use crate::config::AppConfig;
use crate::market::MarketState;
use crate::replay_manager::ReplayManager;
use crate::trading::{TradingStore, trading_db_path_from_config};

mod common;
mod market;
mod replay;
mod static_files;
mod trading;

pub async fn serve(config: AppConfig) -> Result<()> {
    let host = config.web.host.clone();
    let port = config.web.port;
    let trading_db_path = trading_db_path_from_config(&config.db.schema.trading_db_path)?;
    let trading_store = Arc::new(TradingStore::new(trading_db_path));
    let market_state = MarketState::new();
    let manager = Arc::new(ReplayManager::new(config, market_state.clone()));
    let router = Router::new()
        .push(replay::router(manager.clone()))
        .push(trading::router(trading_store, manager))
        .push(market::router(market_state))
        .push(static_files::router());

    info!(host = %host, port = port, "starting salvo web server");

    let acceptor = TcpListener::new(format!("{host}:{port}")).bind().await;
    Server::new(acceptor).serve(router).await;
    Ok(())
}
