use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use salvo::http::StatusError;
use salvo::prelude::*;

pub fn router() -> Router {
    Router::with_path("{**path}").get(StaticUiHandler {
        root: PathBuf::from("webui/dist"),
    })
}

struct StaticUiHandler {
    root: PathBuf,
}

#[async_trait]
impl Handler for StaticUiHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Some(path) = static_path(&self.root, req.uri().path()) else {
            res.render(StatusError::not_found());
            return;
        };

        let fallback = self.root.join("index.html");
        let path = if path.is_file() { path } else { fallback };
        res.send_file(path, req.headers()).await;
    }
}

fn static_path(root: &Path, request_path: &str) -> Option<PathBuf> {
    let relative_path = request_path.trim_start_matches('/');
    if relative_path.is_empty() {
        return Some(root.join("index.html"));
    }

    let mut result = root.to_path_buf();
    for component in Path::new(relative_path).components() {
        match component {
            Component::Normal(value) => result.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::static_path;

    #[test]
    fn builds_safe_static_paths() {
        assert_eq!(
            static_path(Path::new("webui/dist"), "/assets/app.js").unwrap(),
            Path::new("webui/dist/assets/app.js")
        );
        assert_eq!(
            static_path(Path::new("webui/dist"), "/").unwrap(),
            Path::new("webui/dist/index.html")
        );
        assert!(static_path(Path::new("webui/dist"), "/../Cargo.toml").is_none());
    }
}
