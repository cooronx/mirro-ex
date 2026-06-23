export interface ApiResponse<T> {
  code: number;
  msg: string;
  data: T | null;
}

export type ReplayRuntimeState = 'idle' | 'running' | 'paused' | 'stopping' | 'finished' | 'failed';

export interface ReplayStatus {
  state: ReplayRuntimeState;
  current_day?: string | null;
  sim_now_ms?: number | null;
  progress?: number;
  replay_speed?: number | null;
  ticks?: number;
  total_events?: number;
  max_lag_ms?: number;
  final_lag_ms?: number | null;
  perf?: ReplayPerfSnapshot;
  debug?: ReplayDebugSnapshot;
  error?: string | null;
}

export interface ReplayDebugSnapshot {
  unfinished_lanes: ReplayLaneDebugSnapshot[];
}

export interface ReplayLaneDebugSnapshot {
  market: string;
  channel: number;
  ready_events: number;
  watermark_ms?: number | null;
  warmed_up: boolean;
  finished: boolean;
}

export interface ReplayPerfSnapshot {
  last_tick_events: number;
  last_poll_elapsed_ms: number;
  last_handler_elapsed_ms: number;
  last_tick_elapsed_ms: number;
  max_poll_elapsed_ms: number;
  max_handler_elapsed_ms: number;
  max_tick_elapsed_ms: number;
  last_safe_emit_time_ms?: number | null;
  last_emitted_min_ts_ms?: number | null;
  last_emitted_max_ts_ms?: number | null;
  handler_detail?: ReplayHandlerPerfSnapshot | null;
}

export interface ReplayHandlerPerfSnapshot {
  worker_count: number;
  active_workers: number;
  worker_max_events: number;
  worker_max_elapsed_ms: number;
  worker_total_elapsed_ms: number;
  apply_elapsed_ms: number;
  snapshot_elapsed_ms: number;
  record_snapshot_elapsed_ms: number;
  market_queue_elapsed_ms: number;
  trading_init_elapsed_ms: number;
  trading_match_elapsed_ms: number;
}

export interface ReplayStartRequest {
  replay_start_date: string;
  replay_end_date: string;
  replay_start_time: string;
  replay_end_time: string;
  replay_codes: string[];
  replay_speed: number;
  skip_intraday_breaks: boolean;
}

export interface ReplayConfig {
  active_replay_task: ReplayStartRequest | null;
}

export interface MarketSnapshot {
  code: string;
  timestamp_ms: number;
  last_price: number | null;
  auction_price: number | null;
  auction_qty: number | null;
  bids: MarketLevel[];
  asks: MarketLevel[];
}

export interface MarketIntraday {
  code: string;
  points: MarketPricePoint[];
  next_seq: number;
}

export interface MarketLevel {
  price: number;
  qty: number;
}

export interface MarketPricePoint {
  seq: number;
  timestamp_ms: number;
  price: number;
}

export interface Account {
  user_id: string;
  cash_balance: number;
  available_cash: number;
  frozen_cash: number;
  created_at: number;
  updated_at: number;
}

export interface TradingOrder {
  order_id: string;
  user_id: string;
  code: string;
  side: 'buy' | 'sell' | string;
  order_type: string;
  price: number;
  qty: number;
  filled_qty: number;
  status: string;
  created_at: number;
  updated_at: number;
}

export interface TradingPosition {
  user_id: string;
  code: string;
  long_qty: number;
  available_qty: number;
  frozen_qty: number;
  avg_price: number;
  updated_at: number;
}

export type AppEvent =
  | { type: 'replay_changed' }
  | { type: 'market_changed'; code: string }
  | { type: 'trading_changed'; user_id: string | null };

const API_ERROR_MESSAGES: Record<number, string> = {
  1000: '回放启动参数无效',
  1001: '已有回放任务正在运行',
  1002: '当前回放状态不能暂停',
  1003: '当前回放状态不能恢复',
  1004: '当前回放状态不能停止',
  1005: '当前回放状态不能调整速度',
  1006: '回放速度请求无效',
  1101: '开始日期格式无效，请使用 YYYY-MM-DD',
  1102: '结束日期格式无效，请使用 YYYY-MM-DD',
  1103: '开始时间格式无效，请使用 HH:MM:SS',
  1104: '结束时间格式无效，请使用 HH:MM:SS',
  1105: '回放速度必须大于等于 1',
  1500: '回放命令执行失败',

  2001: '创建账户请求无效',
  2002: '账户查询请求无效',
  2003: '下单请求无效',
  2004: '订单查询请求无效',
  2005: '撤单请求无效',
  2006: '持仓查询请求无效',
  2101: '请输入 user_id',
  2102: '初始资金必须大于 0',
  2103: '请输入标的代码',
  2104: '价格必须大于 0',
  2105: '数量必须大于 0',
  2106: '订单方向不支持',
  2201: '回放运行中才能下单或撤单',
  2301: '可用资金不足',
  2302: '可用持仓不足',
  2404: '资金账户不存在',
  2405: '订单不存在',
  2406: '订单当前状态不可撤单',
  2409: '资金账户已存在',
  2500: '交易存储操作失败',
  2501: '交易任务执行失败',

  3001: '行情快照查询请求无效',
  3002: '分时行情查询请求无效',
  3003: '行情列表查询请求无效',
  3404: '当前标的暂无盘口快照',
  3405: '当前标的暂无分时行情'
};

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url, init);
  } catch {
    throw new Error('网络请求失败，请检查后端服务是否已启动');
  }

  let payload: ApiResponse<T>;
  try {
    payload = (await response.json()) as ApiResponse<T>;
  } catch {
    throw new Error(response.ok ? '服务器返回格式异常' : httpErrorMessage(response.status));
  }

  if (!response.ok || payload.code !== 1 || payload.data === null) {
    throw new Error(apiErrorMessage(payload.code, response.status));
  }
  return payload.data;
}

function apiErrorMessage(code: number, status: number) {
  return API_ERROR_MESSAGES[code] ?? httpErrorMessage(status, code);
}

function httpErrorMessage(status: number, code?: number) {
  if (status === 400) return code ? `请求参数无效（错误码 ${code}）` : '请求参数无效';
  if (status === 404) return code ? `请求的数据不存在（错误码 ${code}）` : '请求的数据不存在';
  if (status >= 500) return code ? `服务器处理失败（错误码 ${code}）` : '服务器处理失败';
  return code ? `请求失败（错误码 ${code}）` : '请求失败';
}

export function getReplayStatus() {
  return request<ReplayStatus>('/replay/status');
}

export function getReplayConfig() {
  return request<ReplayConfig>('/replay/config');
}

export function startReplay(payload: ReplayStartRequest) {
  return request<ReplayStatus>('/replay/start', jsonPost(payload));
}

export function pauseReplay() {
  return request<ReplayStatus>('/replay/pause', jsonPost({}));
}

export function resumeReplay() {
  return request<ReplayStatus>('/replay/resume', jsonPost({}));
}

export function stopReplay() {
  return request<ReplayStatus>('/replay/stop', jsonPost({}));
}

export function setReplaySpeed(replay_speed: number) {
  return request<ReplayStatus>('/replay/speed', jsonPost({ replay_speed }));
}

export function getMarketSnapshot(code: string) {
  return request<MarketSnapshot>(`/market/snapshot?code=${encodeURIComponent(code)}`);
}

export function getMarketSnapshots(limit = 50) {
  return request<MarketSnapshot[]>(`/market/snapshots?limit=${encodeURIComponent(String(limit))}`);
}

export function getMarketIntraday(code: string, fromSeq: number) {
  return request<MarketIntraday>(
    `/market/intraday?code=${encodeURIComponent(code)}&from_seq=${encodeURIComponent(String(fromSeq))}`
  );
}

export function getAccount(userId: string) {
  return request<Account>(`/trading/accounts?user_id=${encodeURIComponent(userId)}`);
}

export function createAccount(payload: { user_id: string; initial_cash: number }) {
  return request<Account>('/trading/accounts', jsonPost(payload));
}

export function getOrders(userId: string) {
  return request<TradingOrder[]>(`/trading/orders?user_id=${encodeURIComponent(userId)}`);
}

export function getPositions(userId: string, code?: string) {
  const params = new URLSearchParams({ user_id: userId });
  if (code?.trim()) {
    params.set('code', code.trim());
  }
  return request<TradingPosition[]>(`/trading/positions?${params.toString()}`);
}

export function createOrder(payload: {
  user_id: string;
  code: string;
  side: string;
  price: number;
  qty: number;
}) {
  return request<TradingOrder>('/trading/orders', jsonPost(payload));
}

export function cancelOrder(payload: { user_id: string; order_id: string }) {
  return request<TradingOrder>('/trading/orders/cancel', jsonPost(payload));
}

export function connectEvents() {
  return new EventSource('/events');
}

function jsonPost(payload: unknown): RequestInit {
  return {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  };
}
