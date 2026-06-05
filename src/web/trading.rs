use std::sync::Arc;

use async_trait::async_trait;
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Deserialize;
use tokio::task;

use crate::trading::{CreateAccountRequest, TradingStore, TradingStoreError};

use super::common::ApiResponse;

const TRADING_INVALID_ACCOUNT_REQUEST_CODE: i32 = 2001;
const TRADING_INVALID_ACCOUNT_QUERY_CODE: i32 = 2002;
const TRADING_EMPTY_USER_ID_CODE: i32 = 2101;
const TRADING_INVALID_INITIAL_CASH_CODE: i32 = 2102;
const TRADING_ACCOUNT_ALREADY_EXISTS_CODE: i32 = 2409;
const TRADING_ACCOUNT_NOT_FOUND_CODE: i32 = 2404;
const TRADING_INTERNAL_STORE_CODE: i32 = 2500;
const TRADING_INTERNAL_TASK_CODE: i32 = 2501;

pub fn router(trading_store: Arc<TradingStore>) -> Router {
    Router::with_path("trading")
        .push(Router::with_path("accounts").post(CreateAccountHandler {
            trading_store: trading_store.clone(),
        }))
        .push(Router::with_path("accounts").get(GetAccountHandler { trading_store }))
}

#[derive(Debug, Deserialize)]
struct GetAccountQuery {
    user_id: String,
}

struct CreateAccountHandler {
    trading_store: Arc<TradingStore>,
}

struct GetAccountHandler {
    trading_store: Arc<TradingStore>,
}

#[async_trait]
impl Handler for CreateAccountHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let request = match req.parse_json::<CreateAccountRequest>().await {
            Ok(request) => request,
            Err(err) => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(Json(ApiResponse::error(
                    TRADING_INVALID_ACCOUNT_REQUEST_CODE,
                    format!("invalid account create request: {err}"),
                )));
                return;
            }
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || trading_store.create_account(request)).await {
            Ok(Ok(account)) => {
                res.status_code(StatusCode::CREATED);
                res.render(Json(ApiResponse::success(account)));
            }
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.render(Json(ApiResponse::error(
                    TRADING_INTERNAL_TASK_CODE,
                    format!("account create task join failed: {err}"),
                )));
            }
        }
    }
}

#[async_trait]
impl Handler for GetAccountHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let query = match req.parse_queries::<GetAccountQuery>() {
            Ok(query) => query,
            Err(err) => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(Json(ApiResponse::error(
                    TRADING_INVALID_ACCOUNT_QUERY_CODE,
                    format!("invalid account query: {err}"),
                )));
                return;
            }
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || trading_store.get_account(&query.user_id)).await {
            Ok(Ok(account)) => res.render(Json(ApiResponse::success(account))),
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.render(Json(ApiResponse::error(
                    TRADING_INTERNAL_TASK_CODE,
                    format!("account query task join failed: {err}"),
                )));
            }
        }
    }
}

fn render_trading_error(res: &mut Response, err: TradingStoreError) {
    let (status, code) = match err {
        TradingStoreError::EmptyUserId => (StatusCode::BAD_REQUEST, TRADING_EMPTY_USER_ID_CODE),
        TradingStoreError::InvalidInitialCash => {
            (StatusCode::BAD_REQUEST, TRADING_INVALID_INITIAL_CASH_CODE)
        }
        TradingStoreError::AccountAlreadyExists { .. } => {
            (StatusCode::CONFLICT, TRADING_ACCOUNT_ALREADY_EXISTS_CODE)
        }
        TradingStoreError::AccountNotFound { .. } => {
            (StatusCode::NOT_FOUND, TRADING_ACCOUNT_NOT_FOUND_CODE)
        }
        TradingStoreError::OpenConnection { .. }
        | TradingStoreError::CreateAccount { .. }
        | TradingStoreError::QueryAccount { .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            TRADING_INTERNAL_STORE_CODE,
        ),
    };

    res.status_code(status);
    res.render(Json(ApiResponse::error(code, err.to_string())));
}
