use std::sync::Arc;

use async_trait::async_trait;
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Serialize;

use crate::replay_manager::{ReplayManager, ReplayManagerError, ReplayStartRequest};

pub fn router(manager: Arc<ReplayManager>) -> Router {
    Router::new()
        .push(Router::with_path("replay/start").post(StartReplayHandler {
            manager: manager.clone(),
        }))
        .push(Router::with_path("replay/pause").post(PauseReplayHandler {
            manager: manager.clone(),
        }))
        .push(
            Router::with_path("replay/resume").post(ResumeReplayHandler {
                manager: manager.clone(),
            }),
        )
        .push(Router::with_path("replay/stop").post(StopReplayHandler {
            manager: manager.clone(),
        }))
        .push(
            Router::with_path("replay/config").get(GetReplayConfigHandler {
                manager: manager.clone(),
            }),
        )
        .push(Router::with_path("replay/status").get(GetReplayStatusHandler { manager }))
}

#[derive(Debug, Serialize)]
struct ApiErrorResponse {
    error: String,
}

struct StartReplayHandler {
    manager: Arc<ReplayManager>,
}

struct PauseReplayHandler {
    manager: Arc<ReplayManager>,
}

struct ResumeReplayHandler {
    manager: Arc<ReplayManager>,
}

struct StopReplayHandler {
    manager: Arc<ReplayManager>,
}

struct GetReplayStatusHandler {
    manager: Arc<ReplayManager>,
}

struct GetReplayConfigHandler {
    manager: Arc<ReplayManager>,
}

#[async_trait]
impl Handler for StartReplayHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let request = match req.parse_json::<ReplayStartRequest>().await {
            Ok(request) => request,
            Err(_) => ReplayStartRequest::default(),
        };

        match self.manager.start(request).await {
            Ok(status) => {
                res.status_code(StatusCode::ACCEPTED);
                res.render(Json(status));
            }
            Err(err) => render_manager_error(res, err),
        }
    }
}

#[async_trait]
impl Handler for PauseReplayHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        match self.manager.pause().await {
            Ok(status) => res.render(Json(status)),
            Err(err) => render_manager_error(res, err),
        }
    }
}

#[async_trait]
impl Handler for ResumeReplayHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        match self.manager.resume().await {
            Ok(status) => res.render(Json(status)),
            Err(err) => render_manager_error(res, err),
        }
    }
}

#[async_trait]
impl Handler for StopReplayHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        match self.manager.stop().await {
            Ok(status) => res.render(Json(status)),
            Err(err) => render_manager_error(res, err),
        }
    }
}

#[async_trait]
impl Handler for GetReplayStatusHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let status = self.manager.status().await;
        res.render(Json(status));
    }
}

#[async_trait]
impl Handler for GetReplayConfigHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let config = self.manager.config().await;
        res.render(Json(config));
    }
}

fn render_manager_error(res: &mut Response, err: ReplayManagerError) {
    let status = match err {
        ReplayManagerError::ActiveReplayExists
        | ReplayManagerError::InvalidPauseState(_)
        | ReplayManagerError::InvalidResumeState(_)
        | ReplayManagerError::InvalidStopState(_) => StatusCode::CONFLICT,
        ReplayManagerError::MissingCommandChannel | ReplayManagerError::SendCommand => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        ReplayManagerError::InvalidReplayStartDate(_)
        | ReplayManagerError::InvalidReplayEndDate(_)
        | ReplayManagerError::InvalidReplayStartTime(_)
        | ReplayManagerError::InvalidReplayEndTime(_) => StatusCode::BAD_REQUEST,
    };

    res.status_code(status);
    res.render(Json(ApiErrorResponse {
        error: err.to_string(),
    }));
}
