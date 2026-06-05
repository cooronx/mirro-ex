use std::sync::Arc;

use async_trait::async_trait;
use salvo::http::StatusCode;
use salvo::prelude::*;

use crate::replay_manager::{ReplayManager, ReplayManagerError, ReplayStartRequest};

use super::common::ApiResponse;

const REPLAY_ACTIVE_EXISTS_CODE: i32 = 1001;
const REPLAY_INVALID_PAUSE_STATE_CODE: i32 = 1002;
const REPLAY_INVALID_RESUME_STATE_CODE: i32 = 1003;
const REPLAY_INVALID_STOP_STATE_CODE: i32 = 1004;
const REPLAY_INVALID_START_DATE_CODE: i32 = 1101;
const REPLAY_INVALID_END_DATE_CODE: i32 = 1102;
const REPLAY_INVALID_START_TIME_CODE: i32 = 1103;
const REPLAY_INVALID_END_TIME_CODE: i32 = 1104;
const REPLAY_INTERNAL_COMMAND_CODE: i32 = 1500;

pub fn router(manager: Arc<ReplayManager>) -> Router {
    Router::with_path("replay")
        .push(Router::with_path("start").post(StartReplayHandler {
            manager: manager.clone(),
        }))
        .push(Router::with_path("pause").post(PauseReplayHandler {
            manager: manager.clone(),
        }))
        .push(Router::with_path("resume").post(ResumeReplayHandler {
            manager: manager.clone(),
        }))
        .push(Router::with_path("stop").post(StopReplayHandler {
            manager: manager.clone(),
        }))
        .push(Router::with_path("status").get(GetReplayStatusHandler {
            manager: manager.clone(),
        }))
        .push(Router::with_path("config").get(GetReplayConfigHandler { manager }))
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
                res.render(Json(ApiResponse::success(status)));
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
            Ok(status) => res.render(Json(ApiResponse::success(status))),
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
            Ok(status) => res.render(Json(ApiResponse::success(status))),
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
            Ok(status) => res.render(Json(ApiResponse::success(status))),
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
        res.render(Json(ApiResponse::success(status)));
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
        res.render(Json(ApiResponse::success(config)));
    }
}

fn render_manager_error(res: &mut Response, err: ReplayManagerError) {
    let (status, code) = match err {
        ReplayManagerError::ActiveReplayExists => (StatusCode::CONFLICT, REPLAY_ACTIVE_EXISTS_CODE),
        ReplayManagerError::InvalidPauseState(_) => {
            (StatusCode::CONFLICT, REPLAY_INVALID_PAUSE_STATE_CODE)
        }
        ReplayManagerError::InvalidResumeState(_) => {
            (StatusCode::CONFLICT, REPLAY_INVALID_RESUME_STATE_CODE)
        }
        ReplayManagerError::InvalidStopState(_) => {
            (StatusCode::CONFLICT, REPLAY_INVALID_STOP_STATE_CODE)
        }
        ReplayManagerError::MissingCommandChannel | ReplayManagerError::SendCommand => (
            StatusCode::INTERNAL_SERVER_ERROR,
            REPLAY_INTERNAL_COMMAND_CODE,
        ),
        ReplayManagerError::InvalidReplayStartDate(_) => {
            (StatusCode::BAD_REQUEST, REPLAY_INVALID_START_DATE_CODE)
        }
        ReplayManagerError::InvalidReplayEndDate(_) => {
            (StatusCode::BAD_REQUEST, REPLAY_INVALID_END_DATE_CODE)
        }
        ReplayManagerError::InvalidReplayStartTime(_) => {
            (StatusCode::BAD_REQUEST, REPLAY_INVALID_START_TIME_CODE)
        }
        ReplayManagerError::InvalidReplayEndTime(_) => {
            (StatusCode::BAD_REQUEST, REPLAY_INVALID_END_TIME_CODE)
        }
    };

    res.status_code(status);
    res.render(Json(ApiResponse::error(code, err.to_string())));
}
