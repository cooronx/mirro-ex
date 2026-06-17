<template>
  <main class="app-shell">
    <header class="topbar">
      <div>
        <h1>Mirro EX</h1>
        <p>回放交易控制台</p>
      </div>
      <div class="topbar-controls">
        <t-input v-model="code" class="code-input" placeholder="标的代码，例如 300274.XSHE" />
        <t-tag :theme="statusTheme" variant="light">{{ replayStatus?.state ?? 'Unknown' }}</t-tag>
      </div>
    </header>

    <section class="grid">
      <t-card title="回放控制" bordered class="panel replay-panel">
        <t-form label-align="top" class="form-grid">
          <t-form-item label="开始日期">
            <t-date-picker
              v-model="replayForm.replay_start_date"
              clearable
              format="YYYY-MM-DD"
              value-type="YYYY-MM-DD"
              placeholder="选择开始日期"
            />
          </t-form-item>
          <t-form-item label="结束日期">
            <t-date-picker
              v-model="replayForm.replay_end_date"
              clearable
              format="YYYY-MM-DD"
              value-type="YYYY-MM-DD"
              placeholder="选择结束日期"
            />
          </t-form-item>
          <t-form-item label="开始时间">
            <t-time-picker
              v-model="replayForm.replay_start_time"
              clearable
              format="HH:mm:ss"
              placeholder="选择开始时间"
            />
          </t-form-item>
          <t-form-item label="结束时间">
            <t-time-picker
              v-model="replayForm.replay_end_time"
              clearable
              format="HH:mm:ss"
              placeholder="选择结束时间"
            />
          </t-form-item>
          <t-form-item label="回放速度">
            <t-input-number v-model="replayForm.replay_speed" :min="1" theme="normal" />
          </t-form-item>
          <t-form-item label="跳过午休">
            <t-switch v-model="replayForm.skip_intraday_breaks" />
          </t-form-item>
        </t-form>

        <div class="button-row">
          <t-button theme="primary" :loading="busy.replay" @click="handleStartReplay">开始</t-button>
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
        <dl class="quote-grid">
          <div>
            <dt>最新价</dt>
            <dd>{{ formatPrice(marketSnapshot?.last_price) }}</dd>
          </div>
          <div>
            <dt>卖一</dt>
            <dd>{{ formatPrice(marketSnapshot?.ask1_price) }} / {{ marketSnapshot?.ask1_qty ?? '-' }}</dd>
          </div>
          <div>
            <dt>买一</dt>
            <dd>{{ formatPrice(marketSnapshot?.bid1_price) }} / {{ marketSnapshot?.bid1_qty ?? '-' }}</dd>
          </div>
          <div>
            <dt>更新时间</dt>
            <dd>{{ formatDateTime(marketSnapshot?.timestamp_ms) }}</dd>
          </div>
        </dl>
        <t-alert v-if="marketError" theme="warning" :message="marketError" />
      </t-card>

      <t-card title="账户" bordered class="panel account-panel">
        <div class="inline-form">
          <t-input v-model="userId" placeholder="user_id" />
          <t-button :loading="busy.account" @click="refreshAccountAndOrders">查询</t-button>
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

    <t-card title="订单" bordered class="orders-panel">
      <t-table
        row-key="order_id"
        size="small"
        :data="orders"
        :columns="orderColumns"
        :loading="busy.orders"
        :pagination="{ defaultPageSize: 10, showJumper: true }"
      />
    </t-card>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref, h } from 'vue';
import { MessagePlugin, type TableProps } from 'tdesign-vue-next';
import {
  Account,
  MarketSnapshot,
  ReplayStatus,
  TradingOrder,
  createOrder,
  getAccount,
  getMarketSnapshot,
  getOrders,
  getReplayStatus,
  pauseReplay,
  resumeReplay,
  setReplaySpeed,
  startReplay,
  stopReplay
} from './api';

const code = ref('');
const userId = ref('');
const replayStatus = ref<ReplayStatus | null>(null);
const marketSnapshot = ref<MarketSnapshot | null>(null);
const marketError = ref('');
const account = ref<Account | null>(null);
const accountError = ref('');
const orders = ref<TradingOrder[]>([]);
const orderMessage = ref('');
const orderMessageTheme = ref<'success' | 'error'>('success');
const speedValue = ref(1);
const refreshTimer = ref<number | null>(null);

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

const busy = reactive({
  replay: false,
  speed: false,
  account: false,
  order: false,
  orders: false
});

const statusTheme = computed(() => {
  const state = replayStatus.value?.state;
  if (state === 'Running') return 'success';
  if (state === 'Paused') return 'warning';
  if (state === 'Failed') return 'danger';
  return 'default';
});

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
  }
];

onMounted(() => {
  refreshAll();
  refreshTimer.value = window.setInterval(refreshAll, 1000);
});

onUnmounted(() => {
  if (refreshTimer.value !== null) {
    window.clearInterval(refreshTimer.value);
  }
});

async function refreshAll() {
  await refreshReplayStatus();
  await refreshMarket();
  if (userId.value.trim()) {
    await refreshAccountAndOrders(false);
  }
}

async function refreshReplayStatus() {
  try {
    replayStatus.value = await getReplayStatus();
  } catch {
    // The top-level status is intentionally quiet during server startup.
  }
}

async function refreshMarket() {
  const normalizedCode = code.value.trim();
  if (!normalizedCode) {
    marketSnapshot.value = null;
    marketError.value = '';
    return;
  }
  try {
    marketSnapshot.value = await getMarketSnapshot(normalizedCode);
    marketError.value = '';
  } catch (error) {
    marketSnapshot.value = null;
    marketError.value = messageOf(error);
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
  busy.orders = true;
  try {
    const [nextAccount, nextOrders] = await Promise.all([
      getAccount(normalizedUserId),
      getOrders(normalizedUserId)
    ]);
    account.value = nextAccount;
    orders.value = nextOrders;
    accountError.value = '';
  } catch (error) {
    account.value = null;
    orders.value = [];
    accountError.value = messageOf(error);
  } finally {
    busy.account = false;
    busy.orders = false;
  }
}

async function handleStartReplay() {
  const normalizedCode = code.value.trim();
  const missing = requiredReplayFields();
  if (!normalizedCode) missing.push('标的代码');
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
      replay_codes: [normalizedCode],
      replay_speed: Number(replayForm.replay_speed),
      skip_intraday_breaks: replayForm.skip_intraday_breaks
    });
    showSuccess('回放已开始');
  } catch (error) {
    showError(error);
  } finally {
    busy.replay = false;
  }
}

async function runReplayAction(action: 'pause' | 'resume' | 'stop') {
  busy.replay = true;
  try {
    const request = action === 'pause' ? pauseReplay : action === 'resume' ? resumeReplay : stopReplay;
    replayStatus.value = await request();
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
  const normalizedCode = code.value.trim();
  const price = humanPriceToRaw(orderForm.price);
  if (!normalizedUserId || !normalizedCode || price === null || !orderForm.qty) {
    showError('请填写 user_id、标的代码、价格和数量');
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

function humanPriceToRaw(value: string) {
  const price = Number(value);
  if (!Number.isFinite(price) || price <= 0) {
    return null;
  }
  return Math.round(price * 10000);
}

function formatPrice(value?: number | null) {
  if (value === null || value === undefined) return '-';
  return (value / 10000).toFixed(4);
}

function formatMoney(value?: number | null) {
  if (value === null || value === undefined) return '-';
  return formatPrice(value);
}

function formatDateTime(value?: number | null) {
  if (!value) return '-';
  return new Date(value).toLocaleString('zh-CN', { hour12: false });
}

function formatPercent(value?: number) {
  if (value === undefined || value === null) return '-';
  return `${(value * 100).toFixed(2)}%`;
}

function messageOf(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function showError(error: unknown) {
  const message = messageOf(error);
  orderMessageTheme.value = 'error';
  orderMessage.value = message;
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
</script>
