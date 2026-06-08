use std::sync::Arc;

use async_trait::async_trait;
use salvo::prelude::*;
use serde::Deserialize;
use tokio::task;

use crate::trading::{CreateAccountRequest, TradingStore, TradingStoreError};

use super::common::{ApiResponse, parse_json_body, parse_query, render_api_error};

/// 无效的创建账户请求
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
        let Some(request) = parse_json_body::<CreateAccountRequest>(
            req,
            res,
            TRADING_INVALID_ACCOUNT_REQUEST_CODE,
            "invalid account create request",
        )
        .await
        else {
            return;
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || trading_store.create_account(request)).await {
            Ok(Ok(account)) => res.render(Json(ApiResponse::success(account))),
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                render_api_error(
                    res,
                    TRADING_INTERNAL_TASK_CODE,
                    format!("account create task join failed: {err}"),
                );
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
        let Some(query) = parse_query::<GetAccountQuery>(
            req,
            res,
            TRADING_INVALID_ACCOUNT_QUERY_CODE,
            "invalid account query",
        ) else {
            return;
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || trading_store.get_account(&query.user_id)).await {
            Ok(Ok(account)) => res.render(Json(ApiResponse::success(account))),
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                render_api_error(
                    res,
                    TRADING_INTERNAL_TASK_CODE,
                    format!("account query task join failed: {err}"),
                );
            }
        }
    }
}

fn render_trading_error(res: &mut Response, err: TradingStoreError) {
    let code = match err {
        TradingStoreError::EmptyUserId => TRADING_EMPTY_USER_ID_CODE,
        TradingStoreError::InvalidInitialCash => TRADING_INVALID_INITIAL_CASH_CODE,
        TradingStoreError::AccountAlreadyExists { .. } => TRADING_ACCOUNT_ALREADY_EXISTS_CODE,
        TradingStoreError::AccountNotFound { .. } => TRADING_ACCOUNT_NOT_FOUND_CODE,
        TradingStoreError::OpenConnection { .. }
        | TradingStoreError::CreateAccount { .. }
        | TradingStoreError::QueryAccount { .. } => TRADING_INTERNAL_STORE_CODE,
    };

    render_api_error(res, code, err.to_string());
}
