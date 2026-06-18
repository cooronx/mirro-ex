use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use salvo::prelude::*;
use salvo::sse::{SseEvent, SseKeepAlive};
use tokio::sync::broadcast;

use crate::webdata::EventBus;

pub fn router(event_bus: EventBus) -> Router {
    Router::with_path("events").get(EventsHandler { event_bus })
}

struct EventsHandler {
    event_bus: EventBus,
}

#[async_trait]
impl Handler for EventsHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let rx = self.event_bus.subscribe();
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => return Some((event, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .map(|event| SseEvent::default().name(event.name()).json(event));

        SseKeepAlive::new(stream)
            .comment("keep-alive")
            .max_interval(Duration::from_secs(10))
            .stream(res);
    }
}
