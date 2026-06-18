use std::sync::Arc;

use async_trait::async_trait;
use salvo::prelude::*;
use serde::Deserialize;
use tokio::task;

use crate::replay::ReplayRuntimeState;
use crate::replay_manager::ReplayManager;
use crate::trading::{
    CancelOrderRequest, CreateAccountRequest, CreateLimitOrderRequest, TradingStore,
    TradingStoreError,
};

use super::common::{ApiResponse, parse_json_body, parse_query, render_api_error};

/// 无效的创建账户请求
const TRADING_INVALID_ACCOUNT_REQUEST_CODE: i32 = 2001;
const TRADING_INVALID_ACCOUNT_QUERY_CODE: i32 = 2002;
const TRADING_INVALID_ORDER_REQUEST_CODE: i32 = 2003;
const TRADING_INVALID_ORDER_QUERY_CODE: i32 = 2004;
const TRADING_INVALID_CANCEL_ORDER_REQUEST_CODE: i32 = 2005;
const TRADING_EMPTY_USER_ID_CODE: i32 = 2101;
const TRADING_INVALID_INITIAL_CASH_CODE: i32 = 2102;
const TRADING_INVALID_ORDER_CODE: i32 = 2103;
const TRADING_INVALID_PRICE_CODE: i32 = 2104;
const TRADING_INVALID_QTY_CODE: i32 = 2105;
const TRADING_UNSUPPORTED_SIDE_CODE: i32 = 2106;
const TRADING_REPLAY_NOT_RUNNING_CODE: i32 = 2201;
const TRADING_INSUFFICIENT_CASH_CODE: i32 = 2301;
const TRADING_INSUFFICIENT_POSITION_CODE: i32 = 2302;
const TRADING_ACCOUNT_ALREADY_EXISTS_CODE: i32 = 2409;
const TRADING_ACCOUNT_NOT_FOUND_CODE: i32 = 2404;
const TRADING_ORDER_NOT_FOUND_CODE: i32 = 2405;
const TRADING_ORDER_NOT_CANCELABLE_CODE: i32 = 2406;
const TRADING_INTERNAL_STORE_CODE: i32 = 2500;
const TRADING_INTERNAL_TASK_CODE: i32 = 2501;

pub fn router(trading_store: Arc<TradingStore>, replay_manager: Arc<ReplayManager>) -> Router {
    Router::with_path("trading")
        .push(Router::with_path("accounts").post(CreateAccountHandler {
            trading_store: trading_store.clone(),
        }))
        .push(Router::with_path("accounts").get(GetAccountHandler {
            trading_store: trading_store.clone(),
        }))
        .push(Router::with_path("orders").post(CreateOrderHandler {
            trading_store: trading_store.clone(),
            replay_manager: replay_manager.clone(),
        }))
        .push(Router::with_path("orders/cancel").post(CancelOrderHandler {
            trading_store: trading_store.clone(),
            replay_manager,
        }))
        .push(Router::with_path("orders").get(GetOrdersHandler { trading_store }))
}

#[derive(Debug, Deserialize)]
struct GetAccountQuery {
    user_id: String,
}

#[derive(Debug, Deserialize)]
struct GetOrdersQuery {
    user_id: String,
}

struct CreateAccountHandler {
    trading_store: Arc<TradingStore>,
}

struct GetAccountHandler {
    trading_store: Arc<TradingStore>,
}

struct CreateOrderHandler {
    trading_store: Arc<TradingStore>,
    replay_manager: Arc<ReplayManager>,
}

struct CancelOrderHandler {
    trading_store: Arc<TradingStore>,
    replay_manager: Arc<ReplayManager>,
}

struct GetOrdersHandler {
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

#[async_trait]
impl Handler for CreateOrderHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Some(request) = parse_json_body::<CreateLimitOrderRequest>(
            req,
            res,
            TRADING_INVALID_ORDER_REQUEST_CODE,
            "invalid order create request",
        )
        .await
        else {
            return;
        };

        let status = self.replay_manager.status().await;
        if status.state != ReplayRuntimeState::Running {
            render_api_error(
                res,
                TRADING_REPLAY_NOT_RUNNING_CODE,
                "replay is not running",
            );
            return;
        }
        let Some(sim_now_ms) = status.sim_now_ms else {
            render_api_error(
                res,
                TRADING_REPLAY_NOT_RUNNING_CODE,
                "replay simulated time is not available",
            );
            return;
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || {
            trading_store.create_limit_order(request, sim_now_ms as i64)
        })
        .await
        {
            Ok(Ok(order)) => res.render(Json(ApiResponse::success(order))),
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                render_api_error(
                    res,
                    TRADING_INTERNAL_TASK_CODE,
                    format!("order create task join failed: {err}"),
                );
            }
        }
    }
}

#[async_trait]
impl Handler for GetOrdersHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Some(query) = parse_query::<GetOrdersQuery>(
            req,
            res,
            TRADING_INVALID_ORDER_QUERY_CODE,
            "invalid order query",
        ) else {
            return;
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || trading_store.list_orders(&query.user_id)).await {
            Ok(Ok(orders)) => res.render(Json(ApiResponse::success(orders))),
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                render_api_error(
                    res,
                    TRADING_INTERNAL_TASK_CODE,
                    format!("order query task join failed: {err}"),
                );
            }
        }
    }
}

#[async_trait]
impl Handler for CancelOrderHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Some(request) = parse_json_body::<CancelOrderRequest>(
            req,
            res,
            TRADING_INVALID_CANCEL_ORDER_REQUEST_CODE,
            "invalid order cancel request",
        )
        .await
        else {
            return;
        };

        let status = self.replay_manager.status().await;
        if status.state != ReplayRuntimeState::Running {
            render_api_error(
                res,
                TRADING_REPLAY_NOT_RUNNING_CODE,
                "replay is not running",
            );
            return;
        }
        let Some(sim_now_ms) = status.sim_now_ms else {
            render_api_error(
                res,
                TRADING_REPLAY_NOT_RUNNING_CODE,
                "replay simulated time is not available",
            );
            return;
        };

        let trading_store = self.trading_store.clone();
        match task::spawn_blocking(move || trading_store.cancel_order(request, sim_now_ms as i64))
            .await
        {
            Ok(Ok(order)) => res.render(Json(ApiResponse::success(order))),
            Ok(Err(err)) => render_trading_error(res, err),
            Err(err) => {
                render_api_error(
                    res,
                    TRADING_INTERNAL_TASK_CODE,
                    format!("order cancel task join failed: {err}"),
                );
            }
        }
    }
}

fn render_trading_error(res: &mut Response, err: TradingStoreError) {
    let code = match err {
        TradingStoreError::EmptyUserId => TRADING_EMPTY_USER_ID_CODE,
        TradingStoreError::EmptyCode => TRADING_INVALID_ORDER_CODE,
        TradingStoreError::InvalidInitialCash => TRADING_INVALID_INITIAL_CASH_CODE,
        TradingStoreError::InvalidPrice => TRADING_INVALID_PRICE_CODE,
        TradingStoreError::InvalidQty => TRADING_INVALID_QTY_CODE,
        TradingStoreError::UnsupportedSide { .. } => TRADING_UNSUPPORTED_SIDE_CODE,
        TradingStoreError::InsufficientCash { .. } => TRADING_INSUFFICIENT_CASH_CODE,
        TradingStoreError::InsufficientPosition { .. } => TRADING_INSUFFICIENT_POSITION_CODE,
        TradingStoreError::AccountAlreadyExists { .. } => TRADING_ACCOUNT_ALREADY_EXISTS_CODE,
        TradingStoreError::AccountNotFound { .. } => TRADING_ACCOUNT_NOT_FOUND_CODE,
        TradingStoreError::OrderNotFound { .. } => TRADING_ORDER_NOT_FOUND_CODE,
        TradingStoreError::OrderNotCancelable { .. } => TRADING_ORDER_NOT_CANCELABLE_CODE,
        TradingStoreError::AmountOverflow => TRADING_INTERNAL_STORE_CODE,
        TradingStoreError::OpenConnection { .. }
        | TradingStoreError::CreateAccount { .. }
        | TradingStoreError::QueryAccount { .. }
        | TradingStoreError::CreateOrder { .. }
        | TradingStoreError::CancelOrder { .. }
        | TradingStoreError::QueryOrders { .. }
        | TradingStoreError::MatchOrders { .. } => TRADING_INTERNAL_STORE_CODE,
    };

    render_api_error(res, code, err.to_string());
}
