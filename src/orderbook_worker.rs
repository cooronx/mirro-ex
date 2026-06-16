//! 多 worker 盘口处理模块。
//! 1. 输入：
//!    - replay controller 产生的 `ReplayEvent` 批次
//!    - worker 数量、快照深度和 Parquet 输出配置
//! 2. 输出：
//!    - 每个标的独立维护的盘口
//!    - `输出目录/交易日/标的.parquet` 格式的十档快照文件
//! 3. 逻辑：
//!    - 使用稳定哈希将同一标的固定分配给同一个 worker
//!    - 每个 worker 串行处理自己负责的标的，保证单标的事件顺序
//!    - 每批事件等待全部 worker 完成后返回，保持回放进度与实际处理进度一致

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::oneshot;

use crate::common::{L2Order, Market, OrderType};
use crate::matcher::order_book::OrderBook;
use crate::replay::ReplayEvent;
use crate::snapshot_exporter::SnapshotParquetExporter;

type WorkerReply = oneshot::Sender<Result<()>>;

enum WorkerCommand {
    StartDay {
        day: String,
        reply: WorkerReply,
    },
    Events {
        events: Vec<ReplayEvent>,
        reply: WorkerReply,
    },
    EndDay {
        reply: WorkerReply,
    },
    Shutdown,
}

struct WorkerHandle {
    sender: SyncSender<WorkerCommand>,
    thread: Option<JoinHandle<()>>,
}

struct CodeState {
    book: OrderBook,
    last_event_timestamp: Option<i64>,
    exporter: Option<SnapshotParquetExporter>,
}

impl CodeState {
    fn new() -> Self {
        Self {
            book: OrderBook::new(),
            last_event_timestamp: None,
            exporter: None,
        }
    }
}

struct WorkerState {
    worker_id: usize,
    snapshot_depth: usize,
    write_snapshot_parquet: bool,
    snapshot_parquet_dir: PathBuf,
    current_day: Option<String>,
    codes: HashMap<String, CodeState>,
}

impl WorkerState {
    fn start_day(&mut self, day: String) -> Result<()> {
        self.close_day()?;
        self.current_day = Some(day);
        self.codes.clear();
        Ok(())
    }

    fn process_events(&mut self, events: Vec<ReplayEvent>) -> Result<()> {
        for event in events {
            self.process_event(event)?;
        }
        Ok(())
    }

    fn process_event(&mut self, event: ReplayEvent) -> Result<()> {
        let code = canonical_event_code(&event);
        let timestamp_ms = event.timestamp_ms();
        let state = self
            .codes
            .entry(code.clone())
            .or_insert_with(CodeState::new);
        state.last_event_timestamp = Some(timestamp_ms);

        match event {
            ReplayEvent::Order(order) => {
                if !should_track_order(&order) {
                    return Ok(());
                }
                let order_context = format!(
                    "failed to apply order for code={} channel={} message_number={}",
                    order.code, order.channel, order.message_number
                );
                state
                    .book
                    .apply_order(order)
                    .with_context(|| order_context)?;
            }
            ReplayEvent::Transaction(transaction) => {
                let transaction_context = format!(
                    "failed to apply transaction for code={} channel={} message_number={}",
                    transaction.code, transaction.channel, transaction.message_number
                );
                state
                    .book
                    .apply_transaction(transaction)
                    .with_context(|| transaction_context)?;
            }
        }

        if !state.book.has_unsettled_holdings() {
            self.record_snapshot(&code, timestamp_ms)?;
        }
        Ok(())
    }

    fn record_snapshot(&mut self, code: &str, timestamp_ms: i64) -> Result<()> {
        let day = self
            .current_day
            .as_deref()
            .context("worker received events before replay day started")?;
        let state = self
            .codes
            .get_mut(code)
            .context("missing order book state for snapshot")?;
        let snapshot = if is_closing_call_auction_time(timestamp_ms) {
            state
                .book
                .closing_call_auction_snapshot(self.snapshot_depth)
        } else {
            state.book.snapshot(self.snapshot_depth)
        };

        if self.write_snapshot_parquet {
            if state.exporter.is_none() {
                let mut exporter = SnapshotParquetExporter::new(&self.snapshot_parquet_dir);
                exporter
                    .start_code_day(day, code)
                    .with_context(|| format!("failed to start parquet exporter for code={code}"))?;
                state.exporter = Some(exporter);
            }
            state
                .exporter
                .as_mut()
                .expect("exporter was initialized")
                .write_snapshot(timestamp_ms, code, &snapshot)
                .with_context(|| {
                    format!(
                        "failed to write order book snapshot for code={code} worker={}",
                        self.worker_id
                    )
                })?;
        }
        Ok(())
    }

    fn close_day(&mut self) -> Result<()> {
        let codes = self.codes.keys().cloned().collect::<Vec<_>>();
        for code in codes {
            let (timestamp_ms, changed) = {
                let state = self
                    .codes
                    .get_mut(&code)
                    .context("missing order book state during day close")?;
                let changed = state
                    .book
                    .finalize_all_holdings()
                    .with_context(|| format!("failed to finalize holdings for code={code}"))?;
                (state.last_event_timestamp, changed)
            };

            if changed {
                if let Some(timestamp_ms) = timestamp_ms {
                    self.record_snapshot(&code, timestamp_ms)?;
                }
            }

            if let Some(exporter) = self
                .codes
                .get_mut(&code)
                .and_then(|state| state.exporter.as_mut())
            {
                exporter
                    .close()
                    .with_context(|| format!("failed to close parquet exporter for code={code}"))?;
            }
        }
        self.current_day = None;
        Ok(())
    }
}

pub struct OrderBookWorkerPool {
    workers: Vec<WorkerHandle>,
    tracked_codes: Option<HashSet<String>>,
}

impl OrderBookWorkerPool {
    pub fn new(
        worker_count: usize,
        tracked_codes: Option<HashSet<String>>,
        snapshot_depth: usize,
        write_snapshot_parquet: bool,
        snapshot_parquet_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        if worker_count == 0 {
            bail!("orderbook_workers must be greater than zero");
        }

        let snapshot_parquet_dir = snapshot_parquet_dir.into();
        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (sender, receiver) = sync_channel(1);
            let mut state = WorkerState {
                worker_id,
                snapshot_depth,
                write_snapshot_parquet,
                snapshot_parquet_dir: snapshot_parquet_dir.clone(),
                current_day: None,
                codes: HashMap::new(),
            };
            let worker_thread = thread::Builder::new()
                .name(format!("orderbook-worker-{worker_id}"))
                .spawn(move || {
                    while let Ok(command) = receiver.recv() {
                        match command {
                            WorkerCommand::StartDay { day, reply } => {
                                let _ = reply.send(state.start_day(day));
                            }
                            WorkerCommand::Events { events, reply } => {
                                let _ = reply.send(state.process_events(events));
                            }
                            WorkerCommand::EndDay { reply } => {
                                let _ = reply.send(state.close_day());
                            }
                            WorkerCommand::Shutdown => {
                                let _ = state.close_day();
                                break;
                            }
                        }
                    }
                })
                .with_context(|| format!("failed to spawn orderbook worker {worker_id}"))?;
            workers.push(WorkerHandle {
                sender,
                thread: Some(worker_thread),
            });
        }

        Ok(Self {
            workers,
            tracked_codes,
        })
    }

    pub async fn start_day(&self, day: &str) -> Result<()> {
        self.broadcast(|reply| WorkerCommand::StartDay {
            day: day.to_string(),
            reply,
        })
        .await
    }

    pub async fn process_events(&self, events: Vec<ReplayEvent>) -> Result<()> {
        let mut batches = (0..self.workers.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for event in events {
            let code = canonical_event_code(&event);
            if !self.should_track_code(&code) {
                continue;
            }
            let worker_id = stable_worker_index(&code, self.workers.len());
            batches[worker_id].push(event);
        }

        let mut replies = Vec::new();
        for (worker_id, events) in batches.into_iter().enumerate() {
            if events.is_empty() {
                continue;
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            self.workers[worker_id]
                .sender
                .send(WorkerCommand::Events {
                    events,
                    reply: reply_tx,
                })
                .map_err(|_| anyhow!("orderbook worker {worker_id} command channel closed"))?;
            replies.push((worker_id, reply_rx));
        }
        wait_for_replies(replies).await
    }

    pub async fn end_day(&self) -> Result<()> {
        self.broadcast(|reply| WorkerCommand::EndDay { reply })
            .await
    }

    async fn broadcast(&self, command: impl Fn(WorkerReply) -> WorkerCommand) -> Result<()> {
        let mut replies = Vec::with_capacity(self.workers.len());
        for (worker_id, worker) in self.workers.iter().enumerate() {
            let (reply_tx, reply_rx) = oneshot::channel();
            worker
                .sender
                .send(command(reply_tx))
                .map_err(|_| anyhow!("orderbook worker {worker_id} command channel closed"))?;
            replies.push((worker_id, reply_rx));
        }
        wait_for_replies(replies).await
    }

    fn should_track_code(&self, code: &str) -> bool {
        self.tracked_codes
            .as_ref()
            .is_none_or(|tracked_codes| tracked_codes.contains(code))
    }
}

impl Drop for OrderBookWorkerPool {
    fn drop(&mut self) {
        for worker in &self.workers {
            let _ = worker.sender.send(WorkerCommand::Shutdown);
        }
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                let _ = handle.join();
            }
        }
    }
}

async fn wait_for_replies(replies: Vec<(usize, oneshot::Receiver<Result<()>>)>) -> Result<()> {
    for (worker_id, reply) in replies {
        reply
            .await
            .map_err(|_| anyhow!("orderbook worker {worker_id} stopped without replying"))?
            .with_context(|| format!("orderbook worker {worker_id} failed"))?;
    }
    Ok(())
}

fn should_track_order(order: &L2Order) -> bool {
    matches!(
        order.order_type,
        OrderType::Limit | OrderType::Market | OrderType::BestOwn
    ) || (matches!(order.order_type, OrderType::Cancel) && matches!(order.market, Market::XSHG))
}

fn canonical_event_code(event: &ReplayEvent) -> String {
    match event {
        ReplayEvent::Order(order) => canonical_code(&order.code, order.market),
        ReplayEvent::Transaction(transaction) => {
            canonical_code(&transaction.code, transaction.market)
        }
    }
}

fn canonical_code(code: &str, market: Market) -> String {
    if code.ends_with(".XSHG") || code.ends_with(".XSHE") {
        return code.to_string();
    }
    match market {
        Market::XSHG => format!("{code}.XSHG"),
        Market::XSHE => format!("{code}.XSHE"),
        Market::Unknown => code.to_string(),
    }
}

fn stable_worker_index(code: &str, worker_count: usize) -> usize {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let hash = code.as_bytes().iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    (hash % worker_count as u64) as usize
}

fn is_closing_call_auction_time(timestamp_ms: i64) -> bool {
    const SHANGHAI_OFFSET_MS: i64 = 8 * 60 * 60 * 1_000;
    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    const START_MS: i64 = (14 * 60 * 60 + 57 * 60) * 1_000;
    const END_MS: i64 = 15 * 60 * 60 * 1_000;

    let local_ms = (timestamp_ms + SHANGHAI_OFFSET_MS).rem_euclid(DAY_MS);
    (START_MS..END_MS).contains(&local_ms)
}

#[cfg(test)]
mod tests {
    use super::{is_closing_call_auction_time, stable_worker_index};

    #[test]
    fn stable_hash_routes_same_code_to_same_worker() {
        let first = stable_worker_index("600410.XSHG", 6);
        assert_eq!(first, stable_worker_index("600410.XSHG", 6));
    }

    #[test]
    fn detects_closing_call_auction_time() {
        assert!(!is_closing_call_auction_time(1_778_741_819_999));
        assert!(is_closing_call_auction_time(1_778_741_820_000));
        assert!(is_closing_call_auction_time(1_778_741_999_999));
        assert!(!is_closing_call_auction_time(1_778_742_000_000));
    }
}
