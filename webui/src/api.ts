export interface ApiResponse<T> {
  code: number;
  msg: string;
  data: T | null;
}

export interface ReplayStatus {
  state: string;
  current_day?: string | null;
  sim_now_ms?: number | null;
  progress?: number;
  replay_speed?: number | null;
  ticks?: number;
  total_events?: number;
  max_lag_ms?: number;
  final_lag_ms?: number | null;
  error?: string | null;
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

export interface MarketSnapshot {
  code: string;
  timestamp_ms: number;
  last_price: number | null;
  auction_price: number | null;
  auction_qty: number | null;
  bids: MarketLevel[];
  asks: MarketLevel[];
  intraday_points: MarketPricePoint[];
}

export interface MarketLevel {
  price: number;
  qty: number;
}

export interface MarketPricePoint {
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

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const payload = (await response.json()) as ApiResponse<T>;
  if (!response.ok || payload.code !== 1 || payload.data === null) {
    throw new Error(payload.msg || `request failed: ${response.status}`);
  }
  return payload.data;
}

export function getReplayStatus() {
  return request<ReplayStatus>('/replay/status');
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

export function getAccount(userId: string) {
  return request<Account>(`/trading/accounts?user_id=${encodeURIComponent(userId)}`);
}

export function getOrders(userId: string) {
  return request<TradingOrder[]>(`/trading/orders?user_id=${encodeURIComponent(userId)}`);
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

function jsonPost(payload: unknown): RequestInit {
  return {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  };
}
