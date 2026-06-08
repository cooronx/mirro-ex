use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub const SUCCESS_CODE: i32 = 1;

#[derive(Debug, Serialize)]
pub struct ApiResponse<T>
where
    T: Serialize,
{
    pub code: i32,
    pub msg: String,
    pub data: Option<T>,
}

impl<T> ApiResponse<T>
where
    T: Serialize,
{
    pub fn success(data: T) -> Self {
        Self {
            code: SUCCESS_CODE,
            msg: "ok".to_string(),
            data: Some(data),
        }
    }
}

impl ApiResponse<()> {
    pub fn error(code: i32, msg: impl Into<String>) -> Self {
        Self {
            code,
            msg: msg.into(),
            data: None,
        }
    }
}

pub fn render_api_error(res: &mut Response, code: i32, msg: impl Into<String>) {
    res.render(Json(ApiResponse::error(code, msg)));
}

pub fn render_api_error_with_status(
    res: &mut Response,
    status: StatusCode,
    code: i32,
    msg: impl Into<String>,
) {
    res.status_code(status);
    res.render(Json(ApiResponse::error(code, msg)));
}

pub async fn parse_json_body<T>(
    req: &mut Request,
    res: &mut Response,
    error_code: i32,
    error_context: &str,
) -> Option<T>
where
    T: DeserializeOwned,
{
    match req.parse_json::<T>().await {
        Ok(value) => Some(value),
        Err(err) => {
            render_api_error_with_status(
                res,
                StatusCode::BAD_REQUEST,
                error_code,
                format!("{error_context}: {err}"),
            );
            None
        }
    }
}

pub fn parse_query<T>(
    req: &mut Request,
    res: &mut Response,
    error_code: i32,
    error_context: &str,
) -> Option<T>
where
    T: DeserializeOwned,
{
    match req.parse_queries::<T>() {
        Ok(value) => Some(value),
        Err(err) => {
            render_api_error_with_status(
                res,
                StatusCode::BAD_REQUEST,
                error_code,
                format!("{error_context}: {err}"),
            );
            None
        }
    }
}
