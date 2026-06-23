<template>
  <main class="app-shell">
    <header class="topbar">
      <div>
        <h1>Mirro EX</h1>
        <p>回放交易控制台</p>
      </div>
      <div class="topbar-controls">
        <t-tag :theme="statusTheme" variant="light">{{ replayStatus?.state ?? 'Unknown' }}</t-tag>
      </div>
    </header>

    <section class="grid">
      <t-card title="回放控制" bordered class="panel replay-panel">
        <t-form label-align="top" class="form-grid">
          <t-form-item label="回放标的" class="full-form-item">
            <t-textarea
              v-model="replayCodesInput"
              :disabled="replayInputsLocked"
              :autosize="{ minRows: 2, maxRows: 4 }"
              placeholder="多个代码用逗号、空格或换行分隔；留空播放全部"
            />
          </t-form-item>
          <t-form-item label="开始日期">
            <t-date-picker
              v-model="replayForm.replay_start_date"
              clearable
              :disabled="replayInputsLocked"
              format="YYYY-MM-DD"
              value-type="YYYY-MM-DD"
              placeholder="选择开始日期"
            />
          </t-form-item>
          <t-form-item label="结束日期">
            <t-date-picker
              v-model="replayForm.replay_end_date"
              clearable
              :disabled="replayInputsLocked"
              format="YYYY-MM-DD"
              value-type="YYYY-MM-DD"
              placeholder="选择结束日期"
            />
          </t-form-item>
          <t-form-item label="开始时间">
            <t-time-picker
              v-model="replayForm.replay_start_time"
              clearable
              :disabled="replayInputsLocked"
              format="HH:mm:ss"
              placeholder="选择开始时间"
            />
          </t-form-item>
          <t-form-item label="结束时间">
            <t-time-picker
              v-model="replayForm.replay_end_time"
              clearable
              :disabled="replayInputsLocked"
              format="HH:mm:ss"
              placeholder="选择结束时间"
            />
          </t-form-item>
          <t-form-item label="回放速度">
            <t-input-number v-model="replayForm.replay_speed" :min="1" theme="normal" :disabled="replayInputsLocked" />
          </t-form-item>
          <t-form-item label="跳过午休">
            <t-switch v-model="replayForm.skip_intraday_breaks" :disabled="replayInputsLocked" />
          </t-form-item>
        </t-form>

        <div class="button-row">
          <t-button theme="primary" :loading="busy.replay" :disabled="replayInputsLocked" @click="handleStartReplay">开始</t-button>
          <t-button :loading="busy.replay" @click="runReplayAction('pause')">暂停</t-button>
          <t-button :loading="busy.replay" @click="runReplayAction('resume')">恢复</t-button>
          <t-button theme="danger" variant="outline" :loading="busy.replay" @click="runReplayAction('stop')">停止</t-button>
        </div>

        <div class="speed-row">
          <t-input-number v-model="speedValue" :min="1" theme="normal" />
          <t-button :loading="busy.speed" @click="handleSetSpeed">设置速度</t-button>
        </div>

        <dl class="status-grid">
          <div>
            <dt>模拟时间</dt>
            <dd>{{ formatDateTime(replayStatus?.sim_now_ms) }}</dd>
          </div>
          <div>
            <dt>行情时间</dt>
            <dd>{{ formatDateTime(marketSnapshot?.timestamp_ms) }}</dd>
          </div>
          <div>
            <dt>行情延迟</dt>
            <dd :class="marketLagClass">{{ marketLagText }}</dd>
          </div>
          <div>
            <dt>当前速度</dt>
            <dd>{{ replayStatus?.replay_speed ?? '-' }}</dd>
          </div>
          <div>
            <dt>进度</dt>
            <dd>{{ formatPercent(replayStatus?.progress) }}</dd>
          </div>
          <div>
            <dt>事件数</dt>
            <dd>{{ replayStatus?.total_events ?? '-' }}</dd>
          </div>
        </dl>
      </t-card>

      <t-card title="行情" bordered class="panel market-panel">
        <div class="market-layout">
          <aside class="market-list">
            <div class="market-list-header">
              <strong>最近活跃 {{ activeSnapshots.length }}</strong>
              <span>最多 50</span>
            </div>
            <t-input
              v-model="marketFilter"
              clearable
              placeholder="搜索或输入标的代码"
              @enter="handleMarketSearchEnter"
            />
            <div class="market-list-body">
              <button
                v-for="snapshot in filteredActiveSnapshots"
                :key="snapshot.code"
                type="button"
                class="market-list-row"
                :class="{ active: snapshot.code === selectedCode }"
                @click="selectMarketCode(snapshot.code)"
              >
                <span class="market-list-code">{{ snapshot.code }}</span>
                <strong>{{ formatPrice(snapshotDisplayPrice(snapshot)) }}</strong>
                <span>买一 {{ formatPrice(snapshot.bids[0]?.price) }} / {{ snapshot.bids[0]?.qty ?? '-' }}</span>
                <span>卖一 {{ formatPrice(snapshot.asks[0]?.price) }} / {{ snapshot.asks[0]?.qty ?? '-' }}</span>
                <em>{{ formatTimeOnly(snapshot.timestamp_ms) }}</em>
              </button>
              <div v-if="filteredActiveSnapshots.length === 0" class="market-list-empty">暂无活跃标的</div>
            </div>
          </aside>

          <div class="market-detail">
            <div class="market-detail-title">
              <strong>{{ selectedCode || '未选择标的' }}</strong>
              <span>{{ formatDateTime(marketSnapshot?.timestamp_ms) }}</span>
            </div>
            <div class="market-detail-layout">
              <div class="intraday-chart">
                <div class="chart-header">
                  <div>
                    <dt>最新价</dt>
                    <dd>{{ formatPrice(displayPrice) }}</dd>
                  </div>
                  <span>{{ formatDateTime(marketSnapshot?.timestamp_ms) }}</span>
                </div>
                <div ref="chartContainer" class="price-chart">
                  <div v-if="chartPoints.length === 0" class="chart-empty">暂无日内成交价格</div>
                </div>
                <div class="chart-axis">
                  <span>{{ formatTimeOnly(chartStartTime) }}</span>
                  <span>{{ chartPriceRange }}</span>
                  <span>{{ formatTimeOnly(chartEndTime) }}</span>
                </div>
              </div>

              <div class="orderbook">
                <div class="orderbook-title">五档盘口</div>
                <div class="book-side asks">
                  <div v-for="(level, index) in askLevels" :key="`ask-${index}`" class="book-row ask-row">
                    <span>卖{{ 5 - index }}</span>
                    <strong>{{ formatPrice(level?.price) }}</strong>
                    <em>{{ level?.qty ?? '-' }}</em>
                  </div>
                </div>
                <div class="last-price">
                  {{ formatPrice(displayPrice) }}
                </div>
                <div class="book-side bids">
                  <div v-for="(level, index) in bidLevels" :key="`bid-${index}`" class="book-row bid-row">
                    <span>买{{ index + 1 }}</span>
                    <strong>{{ formatPrice(level?.price) }}</strong>
                    <em>{{ level?.qty ?? '-' }}</em>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        <t-alert v-if="marketError" theme="warning" :message="marketError" />
      </t-card>

      <t-card title="账户" bordered class="panel account-panel">
        <div class="inline-form">
          <t-input v-model="userId" placeholder="user_id" />
          <t-button :loading="busy.account" @click="refreshAccountAndOrders">查询</t-button>
        </div>
        <div class="inline-form account-create-form">
          <t-input v-model="accountForm.initial_cash" placeholder="初始资金 1000000.0000" />
          <t-button theme="primary" :loading="busy.createAccount" @click="handleCreateAccount">创建账户</t-button>
        </div>
        <dl class="status-grid">
          <div>
            <dt>总资金</dt>
            <dd>{{ formatMoney(account?.cash_balance) }}</dd>
          </div>
          <div>
            <dt>可用资金</dt>
            <dd>{{ formatMoney(account?.available_cash) }}</dd>
          </div>
          <div>
            <dt>冻结资金</dt>
            <dd>{{ formatMoney(account?.frozen_cash) }}</dd>
          </div>
        </dl>
        <t-alert v-if="accountError" theme="warning" :message="accountError" />
      </t-card>

      <t-card title="下单" bordered class="panel order-panel">
        <t-form label-align="top" class="order-form">
          <t-form-item label="方向">
            <t-radio-group v-model="orderForm.side" variant="default-filled">
              <t-radio-button value="buy">买入</t-radio-button>
              <t-radio-button value="sell">卖出</t-radio-button>
            </t-radio-group>
          </t-form-item>
          <t-form-item label="价格">
            <t-input v-model="orderForm.price" placeholder="10.0000" />
          </t-form-item>
          <t-form-item label="数量">
            <t-input-number v-model="orderForm.qty" :min="1" theme="normal" />
          </t-form-item>
          <t-button block theme="primary" :loading="busy.order" @click="handleCreateOrder">提交限价单</t-button>
        </t-form>
        <t-alert v-if="orderMessage" :theme="orderMessageTheme" :message="orderMessage" />
      </t-card>
    </section>

    <t-card title="持仓" bordered class="positions-panel">
      <t-table
        row-key="code"
        size="small"
        :data="positions"
        :columns="positionColumns"
        :loading="busy.positions"
        :pagination="positionPagination"
      />
    </t-card>

    <t-card title="订单" bordered class="orders-panel">
      <t-table
        row-key="order_id"
        size="small"
        :data="orders"
        :columns="orderColumns"
        :loading="busy.orders"
        :pagination="orderPagination"
      />
    </t-card>
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, reactive, ref, h, watch } from 'vue';
import { Button, MessagePlugin, type TableProps } from 'tdesign-vue-next';
import {
  ColorType,
  CrosshairMode,
  LineSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type Time,
  type UTCTimestamp
} from 'lightweight-charts';
import {
  Account,
  AppEvent,
  MarketIntraday,
  MarketPricePoint,
  MarketSnapshot,
  ReplayConfig,
  ReplayStatus,
  TradingOrder,
  TradingPosition,
  cancelOrder,
  connectEvents,
  createAccount,
  createOrder,
  getAccount,
  getMarketIntraday,
  getMarketSnapshot,
  getMarketSnapshots,
  getOrders,
  getPositions,
  getReplayConfig,
  getReplayStatus,
  pauseReplay,
  resumeReplay,
  setReplaySpeed,
  startReplay,
  stopReplay
} from './api';

const INTRADAY_BUCKET_MS = 3_000;
const MARKET_SNAPSHOT_LIMIT = 50;

const replayCodesInput = ref('');
const selectedCode = ref('');
const marketFilter = ref('');
const userId = ref('');
const replayStatus = ref<ReplayStatus | null>(null);
const replayConfig = ref<ReplayConfig | null>(null);
const marketSnapshot = ref<MarketSnapshot | null>(null);
const activeSnapshots = ref<MarketSnapshot[]>([]);
const marketError = ref('');
const account = ref<Account | null>(null);
const accountError = ref('');
const positions = ref<TradingPosition[]>([]);
const orders = ref<TradingOrder[]>([]);
const orderMessage = ref('');
const orderMessageTheme = ref<'success' | 'error'>('success');
const speedValue = ref(1);
const cancelingOrderId = ref<string | null>(null);
const chartContainer = ref<HTMLDivElement | null>(null);
let chart: IChartApi | null = null;
let lineSeries: ISeriesApi<'Line'> | null = null;
let chartResizeObserver: ResizeObserver | null = null;
let eventSource: EventSource | null = null;
let replayRefreshTimer: number | null = null;
let marketSnapshotsRefreshTimer: number | null = null;
let marketRefreshTimer: number | null = null;
let tradingRefreshTimer: number | null = null;
let pendingReplayConfigRefresh = false;
let marketRequestSeq = 0;

type IntradayCache = {
  points: MarketPricePoint[];
  nextSeq: number;
};

const intradayCaches = reactive<Record<string, IntradayCache>>({});

const replayForm = reactive({
  replay_start_date: '',
  replay_end_date: '',
  replay_start_time: '',
  replay_end_time: '',
  replay_speed: 1,
  skip_intraday_breaks: true
});

const orderForm = reactive({
  side: 'buy',
  price: '',
  qty: 100
});

const accountForm = reactive({
  initial_cash: ''
});

const busy = reactive({
  replay: false,
  speed: false,
  account: false,
  createAccount: false,
  order: false,
  positions: false,
  orders: false,
  cancelOrder: false
});

const statusTheme = computed(() => {
  const state = replayStatus.value?.state;
  if (state === 'running') return 'success';
  if (state === 'paused') return 'warning';
  if (state === 'failed') return 'danger';
  return 'default';
});

const replayInputsLocked = computed(() => {
  const state = replayStatus.value?.state;
  return Boolean(replayConfig.value?.active_replay_task) || state === 'running' || state === 'paused' || state === 'stopping';
});

const bidLevels = computed(() => padLevels(marketSnapshot.value?.bids ?? []));
const askLevels = computed(() => padLevels(marketSnapshot.value?.asks ?? []).reverse());
const displayPrice = computed(() => marketSnapshot.value?.auction_price ?? marketSnapshot.value?.last_price ?? null);
const filteredActiveSnapshots = computed(() => {
  const keyword = marketFilter.value.trim().toUpperCase();
  if (!keyword) return activeSnapshots.value;
  return activeSnapshots.value.filter((snapshot) => snapshot.code.toUpperCase().includes(keyword));
});

const chartPoints = computed(() => intradayCaches[selectedCode.value.trim()]?.points ?? []);
const chartStartTime = computed(() => chartPoints.value[0]?.timestamp_ms ?? null);
const chartEndTime = computed(() => chartPoints.value[chartPoints.value.length - 1]?.timestamp_ms ?? null);
const chartPriceRange = computed(() => {
  if (chartPoints.value.length === 0) return '-';
  const prices = chartPoints.value.map((point) => point.price);
  return `${formatPrice(Math.min(...prices))} - ${formatPrice(Math.max(...prices))}`;
});
const marketLagMs = computed(() => {
  const simNow = replayStatus.value?.sim_now_ms;
  const marketNow = marketSnapshot.value?.timestamp_ms;
  if (simNow === undefined || simNow === null || marketNow === undefined || marketNow === null) return null;
  return Math.max(0, simNow - marketNow);
});
const marketLagText = computed(() => formatDuration(marketLagMs.value));
const marketLagClass = computed(() => {
  const lag = marketLagMs.value;
  if (lag === null) return '';
  if (lag >= 60_000) return 'lag-danger';
  if (lag >= 10_000) return 'lag-warning';
  return 'lag-ok';
});
const positionPagination = computed(() => ({
  defaultPageSize: 10,
  showJumper: true,
  total: positions.value.length
}));
const orderPagination = computed(() => ({
  defaultPageSize: 10,
  showJumper: true,
  total: orders.value.length
}));

const orderColumns: TableProps['columns'] = [
  { colKey: 'order_id', title: '订单ID', width: 230, ellipsis: true },
  { colKey: 'code', title: '标的', width: 120 },
  { colKey: 'side', title: '方向', width: 80 },
  {
    colKey: 'price',
    title: '价格',
    width: 110,
    cell: (_h, { row }) => formatPrice((row as TradingOrder).price)
  },
  { colKey: 'qty', title: '数量', width: 90 },
  { colKey: 'filled_qty', title: '已成交', width: 90 },
  {
    colKey: 'status',
    title: '状态',
    width: 130,
    cell: (_h, { row }) => h('span', { class: ['status-pill', `status-${(row as TradingOrder).status}`] }, (row as TradingOrder).status)
  },
  {
    colKey: 'created_at',
    title: '创建时间',
    width: 180,
    cell: (_h, { row }) => formatDateTime((row as TradingOrder).created_at)
  },
  {
    colKey: 'updated_at',
    title: '更新时间',
    width: 180,
    cell: (_h, { row }) => formatDateTime((row as TradingOrder).updated_at)
  },
  {
    colKey: 'actions',
    title: '操作',
    width: 90,
    fixed: 'right',
    cell: (_h, { row }) => {
      const order = row as TradingOrder;
      if (!isCancelableOrder(order)) return '';
      return h(
        Button,
        {
          size: 'small',
          theme: 'danger',
          variant: 'outline',
          disabled: replayStatus.value?.state !== 'running',
          loading: busy.cancelOrder && cancelingOrderId.value === order.order_id,
          onClick: () => handleCancelOrder(order)
        },
        () => '撤单'
      );
    }
  }
];

const positionColumns: TableProps['columns'] = [
  { colKey: 'code', title: '标的', width: 140 },
  { colKey: 'long_qty', title: '总持仓', width: 110 },
  { colKey: 'available_qty', title: '可用', width: 110 },
  { colKey: 'frozen_qty', title: '冻结', width: 110 },
  {
    colKey: 'avg_price',
    title: '成本价',
    width: 120,
    cell: (_h, { row }) => formatPrice((row as TradingPosition).avg_price)
  },
  {
    colKey: 'market_value',
    title: '持仓成本',
    width: 130,
    cell: (_h, { row }) => formatMoney(positionCost(row as TradingPosition))
  },
  {
    colKey: 'updated_at',
    title: '更新时间',
    width: 180,
    cell: (_h, { row }) => formatDateTime((row as TradingPosition).updated_at)
  }
];

onMounted(() => {
  initPriceChart();
  refreshAll();
  connectEventStream();
});

onUnmounted(() => {
  closeEventStream();
  clearScheduledRefreshes();
  chartResizeObserver?.disconnect();
  chartResizeObserver = null;
  chart?.remove();
  chart = null;
  lineSeries = null;
});

watch(chartPoints, () => {
  updatePriceChart();
});

watch(selectedCode, () => {
  marketSnapshot.value = null;
  marketError.value = '';
  updatePriceChart();
  refreshMarket();
});

async function refreshAll() {
  await refreshReplayStatus();
  await refreshReplayConfig();
  await refreshMarketSnapshots();
  await refreshMarket();
  if (userId.value.trim()) {
    await refreshAccountAndOrders(false);
  }
}

function connectEventStream() {
  closeEventStream();
  eventSource = connectEvents();
  eventSource.addEventListener('replay_changed', (event) => {
    if (!parseAppEvent(event)) return;
    scheduleReplayRefresh(true);
    scheduleMarketSnapshotsRefresh();
  });
  eventSource.addEventListener('market_changed', (event) => {
    const payload = parseAppEvent(event);
    if (!payload || payload.type !== 'market_changed') return;
    scheduleMarketSnapshotsRefresh();
    if (payload.code.trim() === selectedCode.value.trim()) {
      scheduleMarketRefresh();
    }
    scheduleReplayRefresh(false);
  });
  eventSource.addEventListener('trading_changed', (event) => {
    const payload = parseAppEvent(event);
    if (!payload || payload.type !== 'trading_changed') return;
    const normalizedUserId = userId.value.trim();
    if (!normalizedUserId) return;
    if (payload.user_id && payload.user_id !== normalizedUserId) return;
    scheduleTradingRefresh();
  });
  eventSource.onerror = () => {
    scheduleReplayRefresh(false);
  };
}

function closeEventStream() {
  eventSource?.close();
  eventSource = null;
}

function parseAppEvent(event: Event) {
  try {
    return JSON.parse((event as MessageEvent).data) as AppEvent;
  } catch {
    return null;
  }
}

function scheduleReplayRefresh(includeConfig: boolean) {
  pendingReplayConfigRefresh = pendingReplayConfigRefresh || includeConfig;
  if (replayRefreshTimer !== null) return;
  replayRefreshTimer = window.setTimeout(async () => {
    replayRefreshTimer = null;
    const shouldRefreshConfig = pendingReplayConfigRefresh;
    pendingReplayConfigRefresh = false;
    await refreshReplayStatus();
    if (shouldRefreshConfig) {
      await refreshReplayConfig();
    }
  }, 250);
}

function scheduleMarketRefresh() {
  if (marketRefreshTimer !== null) return;
  marketRefreshTimer = window.setTimeout(async () => {
    marketRefreshTimer = null;
    await refreshMarket();
  }, 250);
}

function scheduleMarketSnapshotsRefresh() {
  if (marketSnapshotsRefreshTimer !== null) return;
  marketSnapshotsRefreshTimer = window.setTimeout(async () => {
    marketSnapshotsRefreshTimer = null;
    await refreshMarketSnapshots();
  }, 400);
}

function scheduleTradingRefresh() {
  if (tradingRefreshTimer !== null) return;
  tradingRefreshTimer = window.setTimeout(async () => {
    tradingRefreshTimer = null;
    if (userId.value.trim()) {
      await refreshAccountAndOrders(false);
    }
  }, 300);
}

function clearScheduledRefreshes() {
  if (replayRefreshTimer !== null) window.clearTimeout(replayRefreshTimer);
  if (marketSnapshotsRefreshTimer !== null) window.clearTimeout(marketSnapshotsRefreshTimer);
  if (marketRefreshTimer !== null) window.clearTimeout(marketRefreshTimer);
  if (tradingRefreshTimer !== null) window.clearTimeout(tradingRefreshTimer);
  replayRefreshTimer = null;
  marketSnapshotsRefreshTimer = null;
  marketRefreshTimer = null;
  tradingRefreshTimer = null;
  pendingReplayConfigRefresh = false;
}

async function refreshReplayStatus() {
  try {
    replayStatus.value = await getReplayStatus();
  } catch {
    // The top-level status is intentionally quiet during server startup.
  }
}

async function refreshReplayConfig() {
  try {
    replayConfig.value = await getReplayConfig();
    applyActiveReplayTask(replayConfig.value.active_replay_task);
  } catch {
    // The config request is best-effort so the rest of the dashboard can keep updating.
  }
}

function applyActiveReplayTask(task: ReplayConfig['active_replay_task']) {
  if (!task) return;

  replayForm.replay_start_date = task.replay_start_date;
  replayForm.replay_end_date = task.replay_end_date;
  replayForm.replay_start_time = normalizeReplayTime(task.replay_start_time);
  replayForm.replay_end_time = normalizeReplayTime(task.replay_end_time);
  replayForm.replay_speed = task.replay_speed;
  replayForm.skip_intraday_breaks = task.skip_intraday_breaks;
  speedValue.value = task.replay_speed;
  replayCodesInput.value = task.replay_codes.join('\n');

  if (!selectedCode.value && task.replay_codes.length > 0) {
    selectedCode.value = task.replay_codes[0];
  }
}

async function refreshMarket() {
  const normalizedCode = selectedCode.value.trim();
  const requestSeq = ++marketRequestSeq;
  if (!normalizedCode) {
    marketSnapshot.value = null;
    marketError.value = '';
    return;
  }
  try {
    const snapshot = await getMarketSnapshot(normalizedCode);
    if (requestSeq !== marketRequestSeq) return;
    if (selectedCode.value.trim() !== normalizedCode) return;
    marketSnapshot.value = snapshot;
    marketError.value = '';
    upsertActiveSnapshot(snapshot);
    await refreshIntraday(normalizedCode, requestSeq);
  } catch (error) {
    if (requestSeq !== marketRequestSeq) return;
    if (selectedCode.value.trim() !== normalizedCode) return;
    marketSnapshot.value = null;
    marketError.value = messageOf(error);
  }
}

async function refreshMarketSnapshots() {
  try {
    activeSnapshots.value = mergeStableSnapshots(
      activeSnapshots.value,
      await getMarketSnapshots(MARKET_SNAPSHOT_LIMIT)
    );
    if (!selectedCode.value && activeSnapshots.value.length > 0) {
      selectedCode.value = activeSnapshots.value[0].code;
    }
  } catch (error) {
    marketError.value = messageOf(error);
  }
}

async function refreshIntraday(normalizedCode: string, requestSeq?: number) {
  const cache = intradayCaches[normalizedCode] ?? { points: [], nextSeq: 0 };
  const intraday = await getMarketIntraday(normalizedCode, cache.nextSeq);
  if (requestSeq !== undefined && requestSeq !== marketRequestSeq) return;
  if (selectedCode.value.trim() !== normalizedCode) return;
  intradayCaches[normalizedCode] = mergeIntraday(cache, intraday);
}

function mergeIntraday(cache: IntradayCache, intraday: MarketIntraday): IntradayCache {
  const points = [...cache.points];
  for (const point of intraday.points) {
    const lastPoint = points[points.length - 1];
    if (lastPoint && sameIntradayBucket(lastPoint, point)) {
      points[points.length - 1] = point;
    } else if (!points.some((existing) => existing.seq === point.seq)) {
      points.push(point);
    }
  }
  return {
    points,
    nextSeq: intraday.next_seq
  };
}

function sameIntradayBucket(left: MarketPricePoint, right: MarketPricePoint) {
  return Math.floor(left.timestamp_ms / INTRADAY_BUCKET_MS) === Math.floor(right.timestamp_ms / INTRADAY_BUCKET_MS);
}

function initPriceChart() {
  if (!chartContainer.value) return;

  chart = createChart(chartContainer.value, {
    autoSize: true,
    layout: {
      background: { type: ColorType.Solid, color: '#ffffff' },
      textColor: '#64748b',
      fontSize: 12
    },
    grid: {
      vertLines: { color: '#eef2f7' },
      horzLines: { color: '#e5eaf2' }
    },
    crosshair: {
      mode: CrosshairMode.Normal,
      vertLine: { color: '#94a3b8', labelBackgroundColor: '#334155' },
      horzLine: { color: '#94a3b8', labelBackgroundColor: '#334155' }
    },
    rightPriceScale: {
      borderColor: '#e2e8f0',
      scaleMargins: { top: 0.12, bottom: 0.16 }
    },
    timeScale: {
      borderColor: '#e2e8f0',
      timeVisible: true,
      secondsVisible: false,
      rightOffset: 2,
      barSpacing: 8,
      fixLeftEdge: true,
      fixRightEdge: true,
      tickMarkFormatter: (time: Time) => formatChartTime(chartTimeToMs(time))
    },
    localization: {
      priceFormatter: formatChartPrice,
      tickmarksPriceFormatter: (prices: number[]) => prices.map(formatChartPrice),
      timeFormatter: (time: UTCTimestamp) => formatTimeOnly(Number(time) * 1000)
    },
    handleScale: {
      axisPressedMouseMove: true,
      mouseWheel: true,
      pinch: true
    },
    handleScroll: {
      horzTouchDrag: true,
      mouseWheel: true,
      pressedMouseMove: true
    }
  });

  lineSeries = chart.addSeries(LineSeries, {
    color: '#2563eb',
    lineWidth: 2,
    lastValueVisible: true,
    priceLineVisible: true,
    priceLineColor: '#2563eb',
    priceLineWidth: 1,
    priceFormat: {
      type: 'price',
      precision: 2,
      minMove: 0.01
    }
  });

  chartResizeObserver = new ResizeObserver(() => {
    nextTick(updatePriceChart);
  });
  chartResizeObserver.observe(chartContainer.value);
  updatePriceChart();
}

function updatePriceChart() {
  if (!chart || !lineSeries) return;

  const data: LineData<UTCTimestamp>[] = chartPoints.value.map((point) => ({
    time: Math.floor(point.timestamp_ms / 1000) as UTCTimestamp,
    value: rawPriceToHuman(point.price)
  }));

  lineSeries.setData(data);
  if (data.length > 0) {
    chart.timeScale().fitContent();
  }
}

async function refreshAccountAndOrders(showLoading = true) {
  const normalizedUserId = userId.value.trim();
  if (!normalizedUserId) {
    accountError.value = '请输入 user_id';
    return;
  }
  if (showLoading) {
    busy.account = true;
  }
  busy.positions = true;
  busy.orders = true;
  try {
    const [nextAccount, nextPositions, nextOrders] = await Promise.all([
      getAccount(normalizedUserId),
      getPositions(normalizedUserId),
      getOrders(normalizedUserId)
    ]);
    account.value = nextAccount;
    positions.value = nextPositions;
    orders.value = nextOrders;
    accountError.value = '';
  } catch (error) {
    account.value = null;
    positions.value = [];
    orders.value = [];
    accountError.value = messageOf(error);
  } finally {
    busy.account = false;
    busy.positions = false;
    busy.orders = false;
  }
}

async function handleCreateAccount() {
  const normalizedUserId = userId.value.trim();
  const initialCash = humanPriceToRaw(accountForm.initial_cash);
  if (!normalizedUserId || initialCash === null) {
    showError('请填写 user_id 和有效初始资金');
    return;
  }

  busy.createAccount = true;
  accountError.value = '';
  try {
    const nextAccount = await createAccount({
      user_id: normalizedUserId,
      initial_cash: initialCash
    });
    account.value = nextAccount;
    accountError.value = '';
    const [nextPositions, nextOrders] = await Promise.all([
      getPositions(normalizedUserId),
      getOrders(normalizedUserId)
    ]);
    positions.value = nextPositions;
    orders.value = nextOrders;
    showSuccess(`账户已创建：${nextAccount.user_id}`);
  } catch (error) {
    showError(error);
  } finally {
    busy.createAccount = false;
  }
}

async function handleStartReplay() {
  const replayCodes = parseReplayCodes(replayCodesInput.value);
  const missing = requiredReplayFields();
  if (missing.length > 0) {
    showError(`请填写：${missing.join('、')}`);
    return;
  }
  busy.replay = true;
  try {
    replayStatus.value = await startReplay({
      replay_start_date: replayForm.replay_start_date.trim(),
      replay_end_date: replayForm.replay_end_date.trim(),
      replay_start_time: replayForm.replay_start_time.trim(),
      replay_end_time: replayForm.replay_end_time.trim(),
      replay_codes: replayCodes,
      replay_speed: Number(replayForm.replay_speed),
      skip_intraday_breaks: replayForm.skip_intraday_breaks
    });
    resetMarketViewForReplayStart(replayCodes);
    await refreshReplayConfig();
    showSuccess('回放已开始');
  } catch (error) {
    showError(error);
  } finally {
    busy.replay = false;
  }
}

function resetMarketViewForReplayStart(replayCodes: string[]) {
  marketRequestSeq += 1;
  activeSnapshots.value = [];
  selectedCode.value = replayCodes[0] ?? '';
  marketSnapshot.value = null;
  marketError.value = '';
  for (const cacheKey of Object.keys(intradayCaches)) {
    delete intradayCaches[cacheKey];
  }
  updatePriceChart();
}

async function runReplayAction(action: 'pause' | 'resume' | 'stop') {
  busy.replay = true;
  try {
    const request = action === 'pause' ? pauseReplay : action === 'resume' ? resumeReplay : stopReplay;
    replayStatus.value = await request();
    await refreshReplayConfig();
    showSuccess(action === 'pause' ? '回放已暂停' : action === 'resume' ? '回放已恢复' : '回放停止中');
  } catch (error) {
    showError(error);
  } finally {
    busy.replay = false;
  }
}

async function handleSetSpeed() {
  const nextSpeed = Number(speedValue.value);
  if (!Number.isFinite(nextSpeed) || nextSpeed < 1) {
    showError('回放速度必须大于等于 1');
    return;
  }
  busy.speed = true;
  try {
    replayStatus.value = await setReplaySpeed(nextSpeed);
    speedValue.value = replayStatus.value.replay_speed ?? nextSpeed;
    showSuccess(`回放速度已设置为 ${speedValue.value}`);
  } catch (error) {
    showError(error);
  } finally {
    busy.speed = false;
  }
}

async function handleCreateOrder() {
  const normalizedUserId = userId.value.trim();
  const normalizedCode = selectedCode.value.trim();
  const price = humanPriceToRaw(orderForm.price);
  orderMessage.value = '';
  if (!normalizedUserId || !normalizedCode || price === null || !orderForm.qty) {
    showError('请填写 user_id、选择标的、价格和数量');
    return;
  }

  busy.order = true;
  try {
    const order = await createOrder({
      user_id: normalizedUserId,
      code: normalizedCode,
      side: orderForm.side,
      price,
      qty: Number(orderForm.qty)
    });
    orderMessageTheme.value = 'success';
    orderMessage.value = `下单成功：${order.order_id}`;
    showSuccess(`下单成功：${order.order_id}`);
    await refreshAccountAndOrders(false);
  } catch (error) {
    showError(error);
  } finally {
    busy.order = false;
  }
}

async function handleCancelOrder(order: TradingOrder) {
  const normalizedUserId = userId.value.trim();
  orderMessage.value = '';
  if (!normalizedUserId) {
    showError('请输入 user_id');
    return;
  }
  if (!window.confirm(`确认撤单 ${order.order_id}？`)) {
    return;
  }

  busy.cancelOrder = true;
  cancelingOrderId.value = order.order_id;
  try {
    const canceled = await cancelOrder({
      user_id: normalizedUserId,
      order_id: order.order_id
    });
    orderMessageTheme.value = 'success';
    orderMessage.value = `撤单成功：${canceled.order_id}`;
    showSuccess(`撤单成功：${canceled.order_id}`);
    await refreshAccountAndOrders(false);
  } catch (error) {
    showError(error);
  } finally {
    busy.cancelOrder = false;
    cancelingOrderId.value = null;
  }
}

function requiredReplayFields() {
  const fields: string[] = [];
  if (!replayForm.replay_start_date.trim()) fields.push('开始日期');
  if (!replayForm.replay_end_date.trim()) fields.push('结束日期');
  if (!replayForm.replay_start_time.trim()) fields.push('开始时间');
  if (!replayForm.replay_end_time.trim()) fields.push('结束时间');
  if (!Number.isFinite(Number(replayForm.replay_speed)) || Number(replayForm.replay_speed) < 1) {
    fields.push('回放速度');
  }
  return fields;
}

function parseReplayCodes(value: string) {
  const seen = new Set<string>();
  return value
    .split(/[\s,，;；]+/)
    .map((code) => code.trim())
    .filter(Boolean)
    .filter((code) => {
      const key = code.toUpperCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function selectMarketCode(nextCode: string) {
  const normalizedCode = nextCode.trim();
  if (!normalizedCode) return;
  selectedCode.value = normalizedCode;
}

async function handleMarketSearchEnter() {
  const normalizedCode = marketFilter.value.trim();
  if (!normalizedCode) return;
  selectMarketCode(normalizedCode);
}

function upsertActiveSnapshot(snapshot: MarketSnapshot) {
  activeSnapshots.value = mergeStableSnapshots(activeSnapshots.value, [snapshot]);
}

function mergeStableSnapshots(current: MarketSnapshot[], incoming: MarketSnapshot[]) {
  const nextSnapshots = [...current];
  const knownCodes = new Set(nextSnapshots.map((snapshot) => snapshot.code));

  for (const snapshot of incoming) {
    const existingIndex = nextSnapshots.findIndex((item) => item.code === snapshot.code);
    if (existingIndex >= 0) {
      nextSnapshots[existingIndex] = snapshot;
      continue;
    }
    if (nextSnapshots.length >= MARKET_SNAPSHOT_LIMIT || knownCodes.has(snapshot.code)) {
      continue;
    }
    knownCodes.add(snapshot.code);
    nextSnapshots.push(snapshot);
  }

  return nextSnapshots;
}

function snapshotDisplayPrice(snapshot: MarketSnapshot) {
  return snapshot.auction_price ?? snapshot.last_price ?? null;
}

function isCancelableOrder(order: TradingOrder) {
  return ['new', 'working', 'partially_filled'].includes(order.status);
}

function normalizeReplayTime(value: string) {
  return value.trim().split('.')[0];
}

function humanPriceToRaw(value: string) {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) {
    return null;
  }
  return Math.round(price * 10000);
}

function rawPriceToHuman(value: number) {
  return value / 10000;
}

function positionCost(position: TradingPosition) {
  return position.avg_price * position.long_qty;
}

function formatPrice(value?: number | null) {
  if (value === null || value === undefined) return '-';
  return rawPriceToHuman(value).toFixed(4);
}

function formatMoney(value?: number | null) {
  if (value === null || value === undefined) return '-';
  return formatPrice(value);
}

function formatDateTime(value?: number | null) {
  if (!value) return '-';
  return new Date(value).toLocaleString('zh-CN', { hour12: false });
}

function formatTimeOnly(value?: number | null) {
  if (!value) return '-';
  return new Date(value).toLocaleTimeString('zh-CN', { hour12: false });
}

function formatChartPrice(value: number) {
  return value.toFixed(2);
}

function formatChartTime(value: number) {
  return new Date(value).toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
}

function chartTimeToMs(time: Time) {
  if (typeof time === 'number') return time * 1000;
  if (typeof time === 'string') return new Date(time).getTime();
  return new Date(time.year, time.month - 1, time.day).getTime();
}

function formatPercent(value?: number) {
  if (value === undefined || value === null) return '-';
  return `${(value * 100).toFixed(2)}%`;
}

function formatDuration(value?: number | null) {
  if (value === undefined || value === null) return '-';
  const totalSeconds = Math.floor(value / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${seconds}s`;
}

function messageOf(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function showError(error: unknown) {
  const message = messageOf(error);
  MessagePlugin.error({
    content: message,
    placement: 'top-right',
    duration: 5000
  });
}

function showSuccess(message: string) {
  MessagePlugin.success({
    content: message,
    placement: 'top-right',
    duration: 2500
  });
}

function padLevels<T>(levels: T[]): Array<T | null> {
  return Array.from({ length: 5 }, (_, index) => levels[index] ?? null);
}
</script>
