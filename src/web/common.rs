use serde::Serialize;

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
