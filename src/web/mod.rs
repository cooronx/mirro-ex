use std::sync::Arc;

use anyhow::Result;
use salvo::conn::tcp::TcpListener;
use salvo::prelude::*;
use tracing::info;

use crate::config::AppConfig;
use crate::replay_manager::ReplayManager;

mod routes;

pub async fn serve(config: AppConfig) -> Result<()> {
    let host = config.web.host.clone();
    let port = config.web.port;
    let manager = Arc::new(ReplayManager::new(config));
    let router = routes::router(manager);

    info!(host = %host, port = port, "starting salvo web server");

    let acceptor = TcpListener::new(format!("{host}:{port}")).bind().await;
    Server::new(acceptor).serve(router).await;
    Ok(())
}
