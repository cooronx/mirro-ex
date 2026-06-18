use crate::webdata::MarketState;

use std::sync::Arc;

use async_trait::async_trait;
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Deserialize;

use super::common::{ApiResponse, parse_query, render_api_error_with_status};

const MARKET_INVALID_SNAPSHOT_QUERY_CODE: i32 = 3001;
const MARKET_INVALID_INTRADAY_QUERY_CODE: i32 = 3002;
const MARKET_SNAPSHOT_NOT_FOUND_CODE: i32 = 3404;
const MARKET_INTRADAY_NOT_FOUND_CODE: i32 = 3405;

pub fn router(market_state: MarketState) -> Router {
    let market_state = Arc::new(market_state);
    Router::with_path("market")
        .push(Router::with_path("snapshot").get(GetSnapshotHandler {
            market_state: market_state.clone(),
        }))
        .push(Router::with_path("intraday").get(GetIntradayHandler { market_state }))
}

#[derive(Debug, Deserialize)]
struct SnapshotQuery {
    code: String,
}

#[derive(Debug, Deserialize)]
struct IntradayQuery {
    code: String,
    #[serde(default)]
    from_seq: u64,
}

struct GetSnapshotHandler {
    market_state: Arc<MarketState>,
}

struct GetIntradayHandler {
    market_state: Arc<MarketState>,
}

#[async_trait]
impl Handler for GetSnapshotHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Some(query) = parse_query::<SnapshotQuery>(
            req,
            res,
            MARKET_INVALID_SNAPSHOT_QUERY_CODE,
            "invalid market snapshot query",
        ) else {
            return;
        };

        let code = query.code.trim();
        if code.is_empty() {
            render_api_error_with_status(
                res,
                StatusCode::BAD_REQUEST,
                MARKET_INVALID_SNAPSHOT_QUERY_CODE,
                "code is required",
            );
            return;
        }

        match self.market_state.get(code) {
            Some(snapshot) => res.render(Json(ApiResponse::success(snapshot))),
            None => render_api_error_with_status(
                res,
                StatusCode::NOT_FOUND,
                MARKET_SNAPSHOT_NOT_FOUND_CODE,
                format!("market snapshot not found for code={code}"),
            ),
        }
    }
}

#[async_trait]
impl Handler for GetIntradayHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Some(query) = parse_query::<IntradayQuery>(
            req,
            res,
            MARKET_INVALID_INTRADAY_QUERY_CODE,
            "invalid market intraday query",
        ) else {
            return;
        };

        let code = query.code.trim();
        if code.is_empty() {
            render_api_error_with_status(
                res,
                StatusCode::BAD_REQUEST,
                MARKET_INVALID_INTRADAY_QUERY_CODE,
                "code is required",
            );
            return;
        }

        match self.market_state.intraday(code, query.from_seq) {
            Some(intraday) => res.render(Json(ApiResponse::success(intraday))),
            None => render_api_error_with_status(
                res,
                StatusCode::NOT_FOUND,
                MARKET_INTRADAY_NOT_FOUND_CODE,
                format!("market intraday not found for code={code}"),
            ),
        }
    }
}
