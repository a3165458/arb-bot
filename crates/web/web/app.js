'use strict';

// 面板不自己算净年化：所有排名、强平、规则评估都由服务端用与 CLI 完全相同的那份代码
// 算出来。前端复算一遍等于在两处各写一份口径，迟早漂开 —— 旧项目就出现过前端硬编码
// 费率表、四家与实测不符，而扫描器早就带回了真值。前端只做单位换算和排版。

const $ = (id) => document.getElementById(id);
const REFRESH_MS = 30000;
const STRATEGY_PARAMS_KEY = 'arb-web-strategy-params';
// 上次用的场所、金额、杠杆（state 初始化要用，所以放在最前面）。
const STORED_STRATEGY = (() => {
  try {
    const parsed = JSON.parse(localStorage.getItem(STRATEGY_PARAMS_KEY) || '{}');
    const text = (value) => (typeof value === 'string' && value.trim() ? value.trim() : null);
    return { a: text(parsed.a), b: text(parsed.b), size: text(parsed.size), leverage: text(parsed.leverage), marginMode: parsed.marginMode === 'cross' ? 'cross' : 'isolated' };
  } catch {
    return {};
  }
})();
// 有候选排队预检时多久再取一次表。
const PRECHECK_POLL_MS = 8000;
// 选中的那一行在等重查时多久再取一次。
const FOCUS_POLL_MS = 4000;
const PAGES = ['opps', 'strategy', 'positions', 'rhspread', 'chart'];
// RH 价差页：服务端每秒算一次，页面每 3 秒取一次。
const RH_REFRESH_MS = 3000;
// 交易模式的本地存储键。要在 state 初始化之前定义：storedMode() 在初始化时就会用到。
const MODE_KEY = 'arb-web-mode';

let state = {
  page: 'opps',
  rh: null,
  rhTimer: null,
  rhAuto: { data: null, form: rhAutoForm({}), dirty: false, busy: false, message: null },
  // 价差图表页。form 在 wireChart() 里才初始化：它用到的常量定义在后面。
  chart: { form: null, data: null, fund: null, error: null, fundError: null, loading: false, seq: 0, main: null, resize: null, loadedAt: 0, timer: null, tick: null },
  livePendingTimer: null,
  config: null,
  data: null,
  selected: null,
  pair: null,
  depthToken: 0,
  measure: 0,
  measureNote: null,
  scanSeq: 0,
  // loadConfig 会回填输入框，那会触发 input。回填期间不要再打一轮扫描。
  booting: true,
  timer: null,
  // 两条策略共用同一份双腿结构，服务端一次返回两份榜；这里只决定看哪一份。
  view: 'funding',
  // 场所、金额、杠杆记在本浏览器里：再打开时参数与上次一样，服务端按这组参数一直在
  // 后台预检，结论是现成的。
  strategy: {
    // 场所是不是用户自己选的（下拉框、交换、机会页的「在策略页打开」）。自己选的就按选的来，
    // 实盘没连的场所也不悄悄换掉，由下单区说明为什么下不了；只有默认选的才限制在已连接实盘的场所里。
    userPicked: false,
    a: STORED_STRATEGY.a || null,
    b: STORED_STRATEGY.b || null,
    board: null,
    norm: 'apr',
    search: '',
    selected: null,
    plan: null,
    planSeq: 0,
    pairSeq: 0,
    form: {
      size: STORED_STRATEGY.size || '1000',
      leverage: STORED_STRATEGY.leverage || null,
      marginMode: STORED_STRATEGY.marginMode || 'isolated',
      // 资金费套利默认带上费差自动平仓：费差反转后不再一直持有。按最近 6 小时平均判断。
      minApr: { on: true, value: '5' },
      protect: { on: false, value: '10' },
      mismatch: { on: false, value: '1' },
      // 价差套利的退出条件：标记价基差收敛到它以内就整笔平仓。
      basisExit: { on: true, value: '0.1' },
      takeProfit: { on: false, value: '10' },
      autoMargin: { on: false, value: '15', max: '100' },
      timeout: { on: false, value: '4', max: '0.5' },
      stopLoss: { on: false, value: '2' },
    },
    // 策略：funding（按费率定方向、赚资金费）或 spread（按价格定方向、赚基差收敛）。
    view: storedStrategyView(),
    // 服务端后台预检的结论（随 /api/strategy 一起返回）：按当前金额、杠杆和模式，
    // 对排名靠前的候选现拉盘口走一遍预览的检查。
    precheck: null,
    // 只显示按当前金额、杠杆、模式能下单的（后台预检通过的）。默认开，记在本浏览器里。
    pcOnly: storedOrderableOnly(),
    pcTimer: null,
    pairsDebounce: null,
  },
  positions: null,
  // 交易模式：纸面 / 实盘。只在右上角「令牌」面板里选一次，策略页下单与持仓页都按它，
  // 不再各自切换。记在本浏览器里。
  mode: storedMode(),
  // 服务端的交易能力（/api/trade/config）：有没有配令牌、实盘开没开、连了哪几家。
  tradeConfig: null,
  // 策略页右侧的下单区。预览绑定在当时的参数上：参数一变，预览作废，要重新预览才能下单。
  trade: {
    dailyPnl: '',
    confirm: '',
    preview: null,
    result: null,
    error: null,
    busy: false,
    // 下过一次单（无论成败）就锁住按钮：失败后不重发，以台账与对账为准。
    done: false,
    seq: 0,
    attempts: {},
  },
  pos: {
    // 正在确认的那笔仓位 id 与操作（'close' 平仓 / 'add' 补仓）；addSize 是补仓输入框里的每腿名义。
    confirming: null,
    confirmKind: null,
    addSize: '',
    // 补仓确认时的价差对比：价差监控快照（/api/rh-spread），确认期间每 2 秒刷新一次。
    addQuote: null,
    addQuoteTimer: null,
    addQuoteSeq: 0,
    busy: false,
    round: null,
    account: null,
    // 交易所凭据识别（/api/trade/accounts）：没开实盘时撑起「实盘」页，开着时附在账户卡片里。
    credentials: null,
    flash: null,
    ruleDrafts: new Map(),
    modeSeq: 0,
    loadSeq: 0,
    // 折叠了的持仓卡片（仓位 id）；首次用到时从浏览器读出，见 collapsedSet()。
    collapsed: null,
    // 「最近结束」里展开了双边明细的仓位 id。
    closedOpen: new Set(),
    // 「最近结束」看到第几页（从 1 起）；自动刷新停在这一页。
    closedPage: 1,
    // 「最近结束」连被移除（隐藏）的也列出来。
    // 「交易统计」看哪个时间段（today / 7d / 30d / all）。
    statsWindow: '7d',
  },
};

// `Number(null)` 是 0、`Number('')` 也是 0 —— 直接转会把「没有这个字段」渲染成
// 「这个字段等于零」。持仓量/成交额缺数据时显示 0 会被当成「这个合约没有持仓」，
// 那是和「我们没取到」完全相反的结论。
const num = (value) => {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

// 费率类字段（daily_spread / funding_apr / round_trip_fee …）在 API 里是**小数**。
const pct = (value, digits = 3) => {
  const parsed = num(value);
  if (parsed === null) return '—';
  const shown = (parsed * 100).toFixed(digits);
  return `${parsed > 0 ? '+' : ''}${shown}%`;
};

// `*_pct` 字段已经是百分数（字段名就带 pct），不能再乘 100 ——
// 再乘一次会把 -1.55% 渲染成 -155%，而这种「大得离谱」的基差看起来像是数据错误。
const pctRaw = (value, digits = 3) => {
  const parsed = num(value);
  if (parsed === null) return '—';
  return `${parsed > 0 ? '+' : ''}${parsed.toFixed(digits)}%`;
};

const money = (value) => {
  const parsed = num(value);
  if (parsed === null) return '—';
  if (Math.abs(parsed) >= 1e9) return `${(parsed / 1e9).toFixed(2)}B`;
  if (Math.abs(parsed) >= 1e6) return `${(parsed / 1e6).toFixed(1)}M`;
  if (Math.abs(parsed) >= 1e3) return `${(parsed / 1e3).toFixed(1)}K`;
  return parsed.toFixed(0);
};

const usd = (value, digits = 2) => {
  const parsed = num(value);
  if (parsed === null) return '—';
  return `${parsed < 0 ? '−' : ''}$${Math.abs(parsed).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
};

// 盈亏金额：正数带「+」，负数由 usd() 给「−」。不到 1 美元时多给两位小数，小仓位才看得出变化。
const pnlUsd = (value) => {
  const digits = Math.abs(num(value) ?? 0) < 1 ? 4 : 2;
  return num(value) > 0 ? `+${usd(value, digits)}` : usd(value, digits);
};

const price = (value) => {
  const parsed = num(value);
  if (parsed === null) return '—';
  const abs = Math.abs(parsed);
  return parsed.toFixed(abs >= 1000 ? 2 : abs >= 1 ? 4 : 6);
};

const cls = (value) => {
  const parsed = num(value);
  if (parsed === null) return 'dim';
  return parsed > 0 ? 'pos' : parsed < 0 ? 'neg' : 'dim';
};

const esc = (text) =>
  String(text).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

const when = (iso) => {
  if (!iso) return '—';
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString('zh-CN', { hour12: false });
};

// 费差列的显示口径：服务端给的是日化费差，这里只做单位换算，不改变排名。
const NORMS = {
  day: { label: '日化费差', divisor: 1 },
  '8h': { label: '每 8h 费差', divisor: 3 },
  '1h': { label: '每 1h 费差', divisor: 24 },
};

// 策略页的年化口径：年化 ÷ 1095 = 每 8 小时，÷ 8760 = 每小时。同样只是换算。
const APR_NORMS = {
  apr: { label: '年化', divisor: 1, digits: 1 },
  '8h': { label: '每 8h', divisor: 1095, digits: 4 },
  '1h': { label: '每 1h', divisor: 8760, digits: 5 },
};

const rate = (apr) => {
  const norm = APR_NORMS[state.strategy.norm] || APR_NORMS.apr;
  const parsed = num(apr);
  return parsed === null ? '—' : pct(parsed / norm.divisor, norm.digits);
};

const HEALTH_LABEL = { healthy: '健康', caution: '注意', danger: '危险' };

function healthText(distance, health) {
  if (distance == null) return '<span class="muted" title="场所不公开维持保证金率，或缺保证金记录">未知</span>';
  return `<span class="h-${health}">${num(distance).toFixed(2)}% ${HEALTH_LABEL[health] || ''}</span>`;
}

function healthBar(distance, health) {
  const value = num(distance);
  const width = value === null ? 100 : Math.max(3, Math.min(100, (value / 40) * 100));
  return `<div class="bar" title="满格 = 40%"><i class="${value === null ? 'unknown' : health}" style="width:${width}%"></i></div>`;
}

function legRiskText(label, leg) {
  if (!leg) return `${label} —`;
  const max = leg.max_leverage == null ? '上限未知' : `上限 ${num(leg.max_leverage)}x`;
  const capped = leg.leverage_capped ? '，已压到上限' : '';
  const distance = leg.liq_distance_pct == null
    ? '强平距离未知（场所不公开维持保证金率）'
    : `强平距离 ${num(leg.liq_distance_pct).toFixed(2)}%`;
  return `${label} ${leg.venue} ${num(leg.leverage)}x（${max}${capped}）${distance}`;
}

// 两腿里更近的那个强平距离。任一腿未知就显示未知 —— 只看算得出的那条会把风险看轻。
function riskCell(row) {
  const risk = row.risk;
  if (!risk) return '<span class="muted">—</span>';
  const title = esc(`${legRiskText('多腿', risk.long)}\n${legRiskText('空腿', risk.short)}`);
  if (risk.liq_distance_pct == null) {
    return `<span class="muted" title="${title}">未知</span>`;
  }
  const health = risk.health || 'danger';
  const capped = risk.long?.leverage_capped || risk.short?.leverage_capped ? '<span class="tag warn">压杠杆</span>' : '';
  return `<span class="h-${health}" title="${title}">${num(risk.liq_distance_pct).toFixed(1)}% ${HEALTH_LABEL[health] || ''}</span>${capped}`;
}

function countdown(iso) {
  if (!iso) return '—';
  const target = Date.parse(iso);
  if (!Number.isFinite(target)) return '—';
  const seconds = Math.round((target - Date.now()) / 1000);
  if (seconds <= 0) return '已到期';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return h > 0 ? `${h}h${String(m).padStart(2, '0')}m` : `${m}m`;
}

function setAge(ageMs, stale) {
  const seconds = Math.round((ageMs || 0) / 1000);
  $('age').textContent = `快照 ${seconds}s 前${stale ? '·已过期' : ''}`;
  $('age').className = `age${stale ? ' stale' : ''}`;
}

// auth：有令牌就带上（服务端据此决定给不给账户状态，没有令牌也照常返回公开数据）。
async function getJson(url, { auth = false } = {}) {
  const headers = auth && token() ? { Authorization: `Bearer ${token()}` } : {};
  const response = await fetch(url, { headers });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}

// ───────────────────────────── 交易令牌与请求 ─────────────────────────────
//
// 令牌只存在这个浏览器里，每个交易请求放在 Authorization 头。浏览器不会在跨站请求里
// 自动带上它，所以别的网页伪造不了下单。反过来，页面里任何一处没转义的字符串都可能
// 把它偷走：凡是来自交易所或服务端的文字，一律过 esc()。

const TOKEN_KEY = 'arb-web-token';

function getToken() {
  try {
    return localStorage.getItem(TOKEN_KEY) || '';
  } catch {
    return '';
  }
}

function setToken(value) {
  try {
    if (value) localStorage.setItem(TOKEN_KEY, value);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    // 隐私模式等情况下存不了：本次会话里仍然可以用内存里的值。
  }
  state.tokenMemory = value || '';
  renderTokenButton();
}

const token = () => getToken() || state.tokenMemory || '';

// 返回 { ok, status, body }，不抛错：交易接口的错误响应里常带着对账结果，调用方要看。
async function api(url, { method = 'GET', body, auth = false } = {}) {
  const headers = {};
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (auth && token()) headers.Authorization = `Bearer ${token()}`;
  let response;
  try {
    response = await fetch(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  } catch (error) {
    return { ok: false, status: 0, body: { error: `网络错误：${error.message}` } };
  }
  const parsed = await response.json().catch(() => ({}));
  if (!response.ok && !parsed.error) parsed.error = `HTTP ${response.status}`;
  if (response.status === 401) parsed.error = `${parsed.error}（点右上角「令牌」重新填写）`;
  return { ok: response.ok, status: response.status, body: parsed };
}

// 实盘没连上时多久重新取一次交易配置（看后台是否已经重连成功）。
const LIVE_PENDING_POLL_MS = 30000;

async function loadTradeConfig() {
  const { ok, body } = await api('/api/trade/config');
  if (!ok) return;
  const wasPending = Boolean(state.tradeConfig?.live_pending);
  state.tradeConfig = body;
  renderModeControls();
  renderLivePending();
  clearTimeout(state.livePendingTimer);
  if (body.live_pending) {
    state.livePendingTimer = setTimeout(() => loadTradeConfig().catch(() => {}), LIVE_PENDING_POLL_MS);
  } else if (wasPending && body.live) {
    // 刚刚重连成功：当前页按实盘重新取数。
    refresh();
  }
  // 实盘模式下策略页只列已连接实盘的场所：配置到了再按它重取一次表。
  if (state.page === 'strategy' && state.mode === 'live') reloadPairsSoon(0);
}

// ───────────────────────────── 交易模式 ─────────────────────────────

function saveStrategyParams() {
  const s = state.strategy;
  try {
    localStorage.setItem(
      STRATEGY_PARAMS_KEY,
      JSON.stringify({ a: s.a, b: s.b, size: s.form.size, leverage: s.form.leverage, marginMode: s.form.marginMode }),
    );
  } catch {
    // 存不了就只在本次会话里生效。
  }
}

function storedOrderableOnly() {
  try {
    return localStorage.getItem('arb-web-orderable-only') !== 'off';
  } catch {
    return true;
  }
}

function storedStrategyView() {
  try {
    return localStorage.getItem('arb-web-strategy-view') === 'spread' ? 'spread' : 'funding';
  } catch {
    return 'funding';
  }
}

function storedMode() {
  try {
    return localStorage.getItem(MODE_KEY) === 'live' ? 'live' : 'paper';
  } catch {
    return 'paper';
  }
}

// 顶栏徽章、令牌按钮、面板里的模式选项：都跟着同一个模式走。
function renderModeControls() {
  const live = state.tradeConfig?.live;
  const badge = $('mode-badge');
  if (state.mode === 'paper') {
    badge.textContent = 'paper';
    badge.className = 'badge';
    badge.title = '纸面交易：真实盘口、纸面成交，不碰真实资金';
  } else if (!live && state.tradeConfig?.live_pending) {
    badge.textContent = 'live·down';
    badge.className = 'badge live-trade';
    badge.title = '实盘开着，但交易所账户暂时连不上：下单、规则与对账暂停，后台每分钟自动重连';
  } else if (!live) {
    badge.textContent = 'live·off';
    badge.className = 'badge live-ro';
    badge.title = '选了实盘，但看板没有开启实盘（ARB_WEB_LIVE=off）';
  } else if (live.mode === 'trade') {
    badge.textContent = 'live';
    badge.className = 'badge live-trade';
    badge.title = `实盘可下单：${live.venues.join(', ')}；价格保护 ${live.market_slippage}`;
  } else {
    badge.textContent = 'live·ro';
    badge.className = 'badge live-ro';
    badge.title = `实盘只读：${live.venues.join(', ')}；只出计划，不下单`;
  }
  for (const button of $('mode-seg').querySelectorAll('button')) {
    const on = button.getAttribute('data-mode') === state.mode;
    button.classList.toggle('on', on);
    button.classList.toggle('live', on && state.mode === 'live');
  }
  $('mode-note').textContent = state.mode === 'paper'
    ? '纸面：真实盘口、纸面成交，写进纸面台账，不碰真实资金。'
    : !live
      ? '看板没有开启实盘（ARB_WEB_LIVE=off）：持仓页显示交易所账户识别，不能下单。'
      : live.mode === 'trade'
        ? `实盘可下单（${live.venues.join('、')}）：真实资金，下单要手输合约名确认。`
        : `实盘只读（${live.venues.join('、')}）：能预览、看账户与对账，不能下单。`;
  renderTokenButton();
}

// 切换模式。有操作在进行时不切：那笔的结果必须显示在它发起的模式下。
function setMode(mode) {
  if (!['paper', 'live'].includes(mode) || mode === state.mode || state.trade.busy || state.pos.busy) return;
  const t = state.trade;
  if (t.done) t.attempts[state.mode] = { result: t.result, error: t.error };
  state.mode = mode;
  try {
    localStorage.setItem(MODE_KEY, mode);
  } catch {
    // 存不了就只在本次会话里生效。
  }
  // 预览只属于当前模式；已提交结果和锁按模式保留，切换不能变相重发失败订单。
  const attempt = t.attempts[mode];
  t.seq += 1;
  t.preview = null;
  t.result = attempt?.result || null;
  t.error = attempt?.error || null;
  t.confirm = '';
  t.done = Boolean(attempt);
  const p = state.pos;
  p.modeSeq += 1;
  p.loadSeq += 1;
  p.closedPage = 1;
  for (const draft of p.ruleDrafts.values()) {
    draft.wouldTrigger = null;
    draft.force = false;
  }
  state.strategy.pairSeq += 1;
  state.strategy.planSeq += 1;
  stopAddQuote();
  p.confirming = null;
  p.confirmKind = null;
  p.flash = null;
  p.round = null;
  p.account = null;
  p.accountTriedAt = 0;
  p.accountStale = null;
  p.credentials = null;
  state.positions = null;
  $('pos-open').innerHTML = '';
  renderModeControls();
  renderTradeBox();
  if ($('plan-auto-margin-note')) {
    const s = state.strategy;
    $('plan-auto-margin-note').innerHTML = autoMarginNote([s.selected?.long, s.selected?.short], s.form.autoMargin.on);
  }
  // 实盘与纸面的杠杆口径、可用场所不同：策略页按新模式重新预检。
  if (state.page === 'strategy') reloadPairsSoon(0);
  if (state.page === 'positions') {
    renderPositions();
    refresh();
    if (mode === 'live' && state.tradeConfig?.live) loadAccount();
  }
}

function renderTokenButton() {
  const button = $('token-btn');
  const configured = state.tradeConfig?.auth_configured;
  button.classList.toggle('set', Boolean(token()));
  button.classList.toggle('live', state.mode === 'live');
  button.textContent = `${state.mode === 'live' ? '实盘' : '纸面'} · ${token() ? '令牌 ✓' : '令牌'}`;
  $('token-note').textContent = configured === false
    ? '服务端没有配置 ARB_WEB_TOKEN：交易接口已关闭，填了也用不了。'
    : token()
      ? '已保存在本浏览器。'
      : '未设置：只能看，不能下单。';
}

// ───────────────────────────── 白天 / 夜间 ─────────────────────────────
//
// index.html 头部的脚本已经在首帧前定好了 data-theme；这里只管按钮和切换。
// 没手动选过时跟随系统；选过一次就记在本浏览器里，不再跟系统走。

const THEME_KEY = 'arb-web-theme';
const SUN_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
const MOON_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>';

let themeChosen = (() => {
  try {
    return ['light', 'dark'].includes(localStorage.getItem(THEME_KEY));
  } catch {
    return false;
  }
})();

const currentTheme = () => (document.documentElement.dataset.theme === 'light' ? 'light' : 'dark');

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  renderThemeButton();
}

function renderThemeButton() {
  const light = currentTheme() === 'light';
  const button = $('theme-btn');
  // 图标画的是「点了会切到哪」，与常见网站一致。
  button.innerHTML = light ? MOON_ICON : SUN_ICON;
  button.title = light ? '切换到夜间模式' : '切换到白天模式';
  button.setAttribute('aria-label', button.title);
}

function toggleTheme() {
  const next = currentTheme() === 'light' ? 'dark' : 'light';
  themeChosen = true;
  try {
    localStorage.setItem(THEME_KEY, next);
  } catch {
    // 存不了就只在本次会话里生效。
  }
  applyTheme(next);
}

function followSystemTheme() {
  const query = window.matchMedia?.('(prefers-color-scheme: light)');
  query?.addEventListener?.('change', (event) => {
    if (!themeChosen) applyTheme(event.matches ? 'light' : 'dark');
  });
}

// ───────────────────────────── 机会页 ─────────────────────────────

async function loadConfig() {
  const config = await (await fetch('/api/config')).json();
  state.config = config;
  if (!$('fee').value && config.fee_per_side != null) $('fee').value = config.fee_per_side;
  if (!$('basis-gate').value && config.max_entry_basis_pct != null) {
    $('basis-gate').value = config.max_entry_basis_pct;
  }
  if (config.amortize_days) $('amortize').value = String(Math.round(num(config.amortize_days) ?? 7));
  const leverage = num(config.leverage);
  if (leverage !== null) {
    const select = $('leverage');
    if (![...select.options].some((option) => num(option.value) === leverage)) {
      select.add(new Option(`${leverage}x`, String(leverage)));
    }
    select.value = String(leverage);
    state.strategy.form.leverage = String(leverage);
  }
}

async function loadScan() {
  const seq = ++state.scanSeq;
  // 在发出请求时记下这次是不是实测。响应回来时 state.measure 可能已经被下一次点击清掉，
  // 用发出时的值，才不会把「普通刷新」的结果写成「没拿到 K 线」。
  const measuring = state.measure;
  const params = new URLSearchParams();
  const symbols = $('symbols').value.trim();
  if (symbols) params.set('symbols', symbols);
  const fee = $('fee').value.trim();
  if (fee) params.set('fee', fee);
  params.set('amortize_days', $('amortize').value);
  params.set('spread_hold_days', $('spread-hold').value);
  const gate = $('basis-gate').value.trim();
  if (gate) params.set('max_entry_basis_pct', gate);
  params.set('top', $('top').value || '50');
  params.set('leverage', $('leverage').value || '3');
  if (measuring) params.set('measure_convergence', String(measuring));

  const response = await fetch(`/api/board?${params}`);
  if (seq !== state.scanSeq) return;
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${response.status}`);
  }
  state.data = await response.json();
  if (measuring) {
    const pairs = state.data.measured_pairs ?? 0;
    state.measureNote = pairs
      ? `${pairs} 条配对标出了基差半衰期，表示这条价差衰减一半要多久。净收益仍是这一笔的金额，不会按半衰期重复折年。自动刷新已暂停，避免把实测结果冲掉。`
      : '跳过没有公开 K 线的场所后，前 20 条里没有拟合出半衰期。两腿历史对不齐，或者基差不是均值回归。排名仍按配置的持有期。自动刷新已暂停。';
    $('auto').checked = false;
    schedule();
  } else {
    state.measureNote = null;
  }
  render();
}

function renderChips(data) {
  const totals = data.totals || {};
  const chips = [
    ['场所', `${totals.venues_ok}/${totals.venues_ok + totals.venues_failed}`, totals.venues_failed > 0],
    ['合约', totals.symbols, false],
    ['资金费配对', totals.profitable_pairs, false],
    ['价差配对', totals.spread_pairs, false],
    ['杠杆', `${num(data.leverage) ?? '—'}x`, false],
  ];
  $('chips').innerHTML = chips
    .map(([label, value, bad]) => `<span class="chip${bad ? ' bad' : ''}">${label} <b>${esc(value)}</b></span>`)
    .join('');
}

function render() {
  const data = state.data;
  if (!data) return;
  renderChips(data);
  if (state.page === 'opps') setAge(data.age_ms, data.stale);
  const gate = data.max_entry_basis_pct;
  $('caliber').textContent =
    state.view === 'funding'
      ? `资金费套利：净年化 = (日化费差 − 往返成本 ÷ ${data.amortize_days}) × 365，假设入场基差保持不变；` +
        `至少 ${data.min_venues} 家场所才成对；入场基差门槛 ${gate == null ? '关闭' : `不利不超过 −${gate}%`}。` +
        ` 基差扩大是无界风险。`
      : `跨所价差套利：这一笔的净收益 = 可成交价差 − 平仓穿价 − 往返手续费。` +
        `可成交价差 = (空腿买一 − 多腿卖一) / 中间价。收敛到 0 就拿到这一笔，不会按 ${data.spread_hold_days} 天重复折年。` +
        `没有盘口的场所不进这张榜。不含持有期内的资金费。`;

  renderNotices(data);
  renderRows(data);
}

function renderNotices(data) {
  const failed = (data.venues || []).filter((v) => !v.ok);
  const excluded = [...(data.suspicious || []), ...(data.unverified || [])];
  const gated = data.gated || [];
  const blocks = [];
  if (state.measureNote) {
    blocks.push(`<div class="notice">${esc(state.measureNote)}</div>`);
  }

  if (failed.length) {
    blocks.push(
      `<div class="notice error">这些场所本轮取数失败，<b>没有</b>参与排名：<ul>` +
        failed.map((v) => `<li><b>${esc(v.venue)}</b> ${esc(v.error || '未知原因')}</li>`).join('') +
        `</ul></div>`,
    );
  }
  if (excluded.length) {
    const shown = excluded.slice(0, 6);
    blocks.push(
      `<div class="notice">${excluded.length} 条读数被排除在配对之外（仍在上方表格里展示）：<ul>` +
        shown
          .map((row) => `<li><b>${esc(row.venue)}</b> ${esc(row.symbol)} — ${esc(row.reason)}</li>`)
          .join('') +
        (excluded.length > shown.length ? `<li>… 其余 ${excluded.length - shown.length} 条见 /api/scan</li>` : '') +
        `</ul></div>`,
    );
  }
  if (gated.length) {
    const shown = gated.slice(0, 5);
    blocks.push(
      `<div class="notice">${gated.length} 条配对读数没问题，但按当前门槛不该进场（进场即逆风）：<ul>` +
        shown
          .map(
            (row) =>
              `<li><b>${esc(row.symbol)}</b> ${esc(row.long)}→${esc(row.short)} — ${esc(row.reason)}</li>`,
          )
          .join('') +
        (gated.length > shown.length ? `<li>… 其余 ${gated.length - shown.length} 条见 /api/scan</li>` : '') +
        `</ul></div>`,
    );
  }
  $('notices').innerHTML = blocks.join('');
}

function renderRows(data) {
  const rows = (state.view === 'funding' ? data.funding : data.spread) || [];
  const norm = NORMS[$('norm').value] || NORMS.day;
  $('spread-head').textContent = norm.label;
  $('empty').textContent = rows.length
    ? ''
    : state.view === 'funding'
      ? '当前条件下没有正费差的配对。'
      : '当前条件下没有正基差的配对。';

  $('rows').innerHTML = rows
    .map((row, index) => {
      const mismatch = (row.quote_mismatch
        ? '<span class="tag" title="两腿计价资产不同，换汇成本未计入成本模型">跨计价</span>'
        : '') +
        (row.oi_capped
          ? '<span class="tag warn" title="至少一条腿的场所报告该合约已触及持仓量上限：只能减仓，这笔现在开不出来">OI 上限</span>'
          : '');
      const dailySpread = num(row.daily_spread);
      const shownSpread = dailySpread === null ? null : dailySpread / norm.divisor;
      const basisRaw = state.view === 'spread' ? row.executable_basis_pct : row.entry_basis_pct;
      const basisTitle = state.view === 'spread'
        ? '可成交价差：(空腿买一 − 多腿卖一) / 中间价'
        : '标记价基差，用来看资金费进场是不是已经逆风';
      const basis = basisRaw == null
        ? '<span class="muted">缺价</span>'
        : `<span title="${basisTitle}">${pctRaw(basisRaw, 3)}</span>`;
      // 穿价未知时不能显示 0 —— 那等于宣称「这笔交易没有价差成本」。
      const crossing = row.spread_unknown
        ? '<span class="muted" title="至少一条腿拿不到盘口：成本是下界，净收益是上界">未知</span>'
        : pct(row.round_trip_spread, 4);
      const hold = row.hold_measured
        ? `<span class="tag" title="持有期是两腿 K 线拟合出的半衰期，不是配置里的默认天数">半衰期 ${Number(row.spread_hold_days).toFixed(2)}天</span>`
        : '';
      const samePair =
        state.pair &&
        state.pair.symbol === String(row.symbol) &&
        state.pair.long === row.long &&
        state.pair.short === row.short;
      return `<tr${samePair ? ' class="open"' : ''} data-symbol="${esc(row.symbol)}" data-long="${esc(row.long)}" data-short="${esc(row.short)}">
        <td class="num dim">${index + 1}</td>
        <td>${esc(row.symbol)}${mismatch}</td>
        <td class="long-c">${esc(row.long)}<span class="tag">${row.long_interval_h}h</span></td>
        <td class="short-c">${esc(row.short)}<span class="tag">${row.short_interval_h}h</span></td>
        <td class="num">${pct(shownSpread, norm.divisor === 1 ? 4 : 5)}</td>
        <td class="num">${pct(row.apr, 1)}</td>
        <td class="num dim">${pct(row.round_trip_fee, 4)}</td>
        <td class="num dim">${crossing}</td>
        <td class="num ${cls(basisRaw)}">${basis}</td>
        <td class="num ${cls(row.funding_apr)}">${state.view === 'funding' ? `<b>${pct(row.funding_apr, 1)}</b>` : pct(row.funding_apr, 1)}</td>
        <td class="num ${cls(row.spread_net)}">${state.view === 'spread' ? `<b>${pct(row.spread_net, 3)}</b>` : pct(row.spread_net, 3)}${hold}</td>
        <td class="num">${riskCell(row)}</td>
        <td class="num ${cls(row.risk?.margin_apr)}">${row.risk ? pct(row.risk.margin_apr, 1) : '—'}</td>
        <td class="dim">${countdown(row.next_funding_at)}</td>
      </tr>`;
    })
    .join('');

  for (const row of $('rows').querySelectorAll('tr')) {
    row.addEventListener('click', () => {
      const symbol = row.getAttribute('data-symbol');
      const long = row.getAttribute('data-long');
      const short = row.getAttribute('data-short');
      const same =
        state.pair &&
        state.pair.symbol === symbol &&
        state.pair.long === long &&
        state.pair.short === short;
      state.pair = same ? null : { symbol, long, short };
      state.selected = state.pair ? symbol : null;
      renderRows(state.data);
    });
  }
  renderDetail();
}

// 相对买卖价差 = (ask − bid) / mid。这是**穿价成本**的单价：市价买要吃 ask、
// 市价卖只能拿 bid。缺一边就如实显示未知，不要用标记价顶替。
function spreadCell(rate) {
  const bid = num(rate.best_bid);
  const ask = num(rate.best_ask);
  if (bid === null || ask === null || bid <= 0 || ask < bid) {
    return '<span class="muted" title="该场所的批量行情不提供盘口">未知</span>';
  }
  const mid = (bid + ask) / 2;
  return mid > 0 ? pct((ask - bid) / mid, 4) : '—';
}

function renderDetail() {
  const box = $('detail');
  if (!state.selected) {
    box.classList.add('hidden');
    return;
  }
  const book = (state.data.details || {})[state.selected];
  if (!book) {
    box.classList.add('hidden');
    return;
  }
  const view = { symbol: state.selected, rates: book };
  const excluded = new Set(
    [...(state.data.suspicious || []), ...(state.data.unverified || [])]
      .filter((row) => row.symbol === view.symbol)
      .map((row) => row.venue),
  );
  const rates = [...(view.rates || [])].sort((a, b) => a.venue.localeCompare(b.venue));

  const pair = state.pair;
  const depth = pair
    ? `<p class="hint" id="depth-box">正在按 1000 USDT 拉 ${esc(pair.long)} 买 / ${esc(pair.short)} 卖的多档深度…</p>`
    : '';
  const toStrategy = pair
    ? `<button class="btn ghost" type="button" id="open-in-strategy">在策略页打开这对腿 →</button>`
    : '';
  box.innerHTML = `<h2>${esc(view.symbol)} 各场所读数 <span class="close" id="close-detail">关闭</span></h2>
    ${depth}
    <div class="table-wrap"><table>
      <thead><tr>
        <th>场所</th><th class="num">每期费率</th><th class="num">周期</th>
        <th class="num">日化</th><th class="num">吃单费率</th>
        <th class="num">买一</th><th class="num">卖一</th><th class="num">相对价差</th>
        <th class="num">标记价</th><th class="num">指数价</th>
        <th class="num">持仓量</th><th class="num">24h 成交额</th>
        <th class="num">最大杠杆</th><th class="num">维持保证金</th><th>下次结算</th>
      </tr></thead>
      <tbody>
        ${rates
          .map((rate) => {
            const daily = num(rate.period_rate) === null ? null : num(rate.period_rate) * (24 / (rate.interval_h || 8));
            const flags = [
              rate.interval_assumed ? '<span class="tag" title="结算周期不是该场所响应里的字段，用了默认值">周期假定</span>' : '',
              rate.next_funding_estimated ? '<span class="tag" title="结算时刻是按周期推算的">时刻推算</span>' : '',
              excluded.has(rate.venue) ? '<span class="tag" title="与同币种其它场所差异过大，不参与配对">已排除</span>' : '',
              rate.oi_capped ? '<span class="tag warn" title="场所报告已触及持仓量上限，只能减仓">OI 上限</span>' : '',
            ].join('');
            const maxLeverage = rate.max_leverage == null
              ? '<span class="muted" title="批量接口不提供">未知</span>'
              : `${Number(num(rate.max_leverage).toFixed(2))}x`;
            const maintenance = rate.maintenance_margin == null
              ? '<span class="muted" title="批量接口不提供">未知</span>'
              : pct(rate.maintenance_margin, 2).replace('+', '');
            const fee = rate.taker_fee == null
              ? '<span class="muted" title="该场所公共接口不提供吃单费率，排名回落到配置值">未知</span>'
              : pct(rate.taker_fee, 4);
            return `<tr>
              <td>${esc(rate.venue)}${flags}</td>
              <td class="num">${pct(rate.period_rate, 5)}</td>
              <td class="num dim">${rate.interval_h}h</td>
              <td class="num ${cls(daily)}">${daily == null ? '—' : pct(daily, 4)}</td>
              <td class="num">${fee}</td>
              <td class="num">${rate.best_bid ?? '—'}</td>
              <td class="num">${rate.best_ask ?? '—'}</td>
              <td class="num dim">${spreadCell(rate)}</td>
              <td class="num">${rate.mark_price ?? '—'}</td>
              <td class="num dim">${rate.index_price ?? '—'}</td>
              <td class="num">${money(rate.open_interest_usdt)}</td>
              <td class="num dim">${money(rate.quote_volume_24h)}</td>
              <td class="num">${maxLeverage}</td>
              <td class="num dim">${maintenance}</td>
              <td class="dim">${countdown(rate.next_funding_at)}</td>
            </tr>`;
          })
          .join('')}
      </tbody>
    </table></div>
    <div class="plan-actions">${toStrategy}</div>`;
  box.classList.remove('hidden');
  $('close-detail').addEventListener('click', () => {
    state.selected = null;
    state.pair = null;
    state.depthToken += 1;
    renderRows(state.data);
  });
  if (pair) {
    $('open-in-strategy').addEventListener('click', () => {
      const s = state.strategy;
      s.userPicked = true;
      s.a = pair.long;
      s.b = pair.short;
      // 机会页看的是哪个榜，策略页就切到同一个视角：资金费榜按费率定方向、价差榜按价格定方向，
      // 不切的话选中的方向会和榜上的对不上。
      if (s.view !== state.view) {
        s.view = state.view;
        try {
          localStorage.setItem('arb-web-strategy-view', s.view);
        } catch {
          // 存不了就只在本次会话里生效。
        }
        s.plan = null;
      }
      s.selected = { symbol: pair.symbol, long: pair.long, short: pair.short };
      showPage('strategy');
    });
    loadDepth(pair, 'depth-box', 1000);
  }
}

function depthLeg(leg, label) {
  if (!leg || !leg.ok) {
    return `<li><b>${esc(label)}</b> ${esc(leg?.venue || '')} — ${esc(leg?.error || '没有结果')}</li>`;
  }
  const gap = leg.exhausted ? `，深度不够，只成交 ${money(leg.filled_usdt)} USDT` : '';
  return `<li><b>${esc(label)}</b> ${esc(leg.venue)} ${esc(leg.side)} — 滑点 ${pct(leg.slippage, 3)}，均价 ${esc(leg.average_price)}，${leg.levels} 档${gap}</li>`;
}

async function loadDepth(pair, boxId, size) {
  const token = ++state.depthToken;
  const box = $(boxId);
  if (!box) return;
  box.textContent = `正在按 ${size} USDT 拉 ${pair.long} 买 / ${pair.short} 卖的多档深度…`;
  const params = new URLSearchParams({
    symbol: pair.symbol,
    long: pair.long,
    short: pair.short,
    size: String(size),
    levels: '20',
  });
  try {
    const body = await getJson(`/api/depth?${params}`);
    if (token !== state.depthToken) return;
    const current = $(boxId);
    if (!current) return;
    current.innerHTML =
      `${size} USDT 的多档滑点（超出一档之后多付的部分，不含已经计入排名的一档穿价）：<ul>` +
      depthLeg(body.long, '做多') +
      depthLeg(body.short, '做空') +
      `</ul>`;
  } catch (error) {
    if (token !== state.depthToken) return;
    const current = $(boxId);
    if (current) current.textContent = `深度体检失败：${error.message}`;
  }
}

// ───────────────────────────── 策略页 ─────────────────────────────

const RULE_FIELDS = [
  { key: 'minApr', id: 'rule-apr', field: 'min_funding_apr', source: 'min_funding_apr', label: '费差自动平仓', hint: '最近 6 小时平均毛费差年化跌破它就整笔平仓；转负立即平，仍为正但平仓太贵则继续持有', unit: '%', value: '5', min: '0', step: '0.5' },
  { key: 'basisExit', id: 'rule-basis', field: 'basis_exit', source: 'basis_exit_pct', label: '基差收敛平仓', hint: '标记价基差收敛到目标以内后，服务端按盘口核对整笔净收益再决定平仓；目标可为负（-5 ~ 5）', unit: '%', value: '0.1', min: '-5', max: '5', step: '0.01' },
  { key: 'protect', id: 'rule-protect', field: 'liq_protection', source: 'liq_protection_pct', label: '爆仓保护', hint: '任一腿强平距离低于它就两腿等比例减仓', unit: '%', value: '10', min: '0', step: '0.5' },
  { key: 'mismatch', id: 'rule-mismatch', field: 'size_mismatch', source: 'size_mismatch_pct', label: '数量失衡平仓', hint: '两腿数量偏差超过它就整笔平仓（0.5 ~ 100）', unit: '%', value: '1', min: '0.5', step: '0.5' },
  { key: 'takeProfit', id: 'rule-take-profit', field: 'take_profit', source: 'take_profit_usdt', label: '净收益止盈', hint: '含已结算资金费的净盈利达到目标后，服务端按盘口扣除退出成本再核对；另外后台逐秒盯两边盘口，盘口整笔平掉（含资金费）先到目标也会核对后平；资金费未知时不评估', unit: 'USDT', value: '10', min: '0', step: '0.01' },
  { key: 'tpBook', id: 'rule-tp-book', field: 'take_profit_book_min', source: 'take_profit_book_min_usdt', label: '止盈盘口门槛', hint: '配合净收益止盈：标记价到止盈后，按两边盘口平掉的整笔净额（含资金费、平仓手续费与穿价）为正且 ≥ 它就平，不再要求盘口也到止盈金额；0 = 为正即平。不开 = 盘口也要到止盈金额。下单前照样重拉盘口复核，成交价可能比预估差', unit: 'USDT', value: '0', min: '0', step: '0.01' },
  { key: 'autoMargin', id: 'rule-auto-margin', field: 'auto_margin', source: 'auto_margin_pct', label: '自动追加保证金', hint: '仅逐仓：任一腿强平距离低于触发线时补保证金；两腿共用整笔累计额度，结果未知也占额度，不重发', unit: '%', value: '15', min: '0', step: '0.5',
    pair: { field: 'auto_margin_max', source: 'auto_margin_max_usdt', valueLabel: '触发线', label: '累计上限', unit: 'USDT', value: '100', step: '0.01', min: '0' } },
  { key: 'stopLoss', id: 'rule-stop-loss', field: 'stop_loss', source: 'stop_loss_usdt', label: '止损', hint: '按两边盘口现在平掉的整笔净额（含资金费、平仓手续费与穿价）亏到 −它就整笔平仓；盘口拿不到时不按标记价平', unit: 'USDT', value: '2', min: '0', step: '0.1' },
  { key: 'timeout', id: 'rule-timeout', field: 'timeout_hours', source: 'timeout_hours', label: '超时退出', hint: '持仓超过这么多小时、且按两边盘口现在平掉的亏损（含资金费）不超过右边的金额就整笔平仓，释放名额；0 = 保本才平。按盘口核对，不按标记价', unit: '小时', value: '4', min: '0', step: '0.5',
    pair: { field: 'timeout_max_loss', source: 'timeout_max_loss_usdt', valueLabel: '持仓时长', label: '最多亏', unit: 'USDT', value: '0.5', step: '0.01', min: '0' } },
];

function ruleControl(def, rule, id = def.id, showState = false, mode = 'isolated') {
  const hint = mode === 'cross' && def.key === 'protect' ? '全仓仅按交易所有效强平价判断，触发时整笔退出；缺数据会提示无法评估，不使用逐仓减仓公式' : def.hint;
  const disabled = rule.on ? '' : ' disabled';
  const pair = def.pair;
  return `<div class="rule${pair ? ' rule-pair' : ''}">
    <input type="checkbox" id="${id}-on" data-rule-field="${def.key}" data-rule-part="on"${rule.on ? ' checked' : ''} aria-label="启用${def.label}" />
    <label class="desc" for="${id}-on">${def.label}${showState ? ` <span class="tag ${rule.on ? 'sky' : ''}" data-rule-state="${def.key}">${rule.on ? '已启用' : '已关闭'}</span>` : ''}<small>${hint}</small></label>
    <span class="rule-values"><label><span class="sr-only">${def.label}${pair ? pair.valueLabel : '阈值'}</span><input type="number" id="${id}-value" data-rule-field="${def.key}" data-rule-part="value" value="${esc(rule.value)}" step="${def.step}" min="${def.min}"${def.max ? ` max="${def.max}"` : ''}${disabled} /> ${def.unit}</label>
      ${pair ? `<label>${pair.label} <input type="number" id="${id}-max" data-rule-field="${def.key}" data-rule-part="max" value="${esc(rule.max)}" step="${pair.step}" min="${pair.min}"${disabled} /> ${pair.unit}</label>` : ''}
    </span>
  </div>`;
}

// 开仓按策略视角选择费差 / 基差规则；改持仓规则则整套发送，off 明确关闭。
function ruleFields(form, view = null) {
  const fields = {};
  for (const def of RULE_FIELDS) {
    const rule = form[def.key] || { on: false, value: def.value, max: def.pair?.value };
    const active = rule.on && !(view === 'spread' && def.key === 'minApr') && !(view === 'funding' && def.key === 'basisExit');
    fields[def.field] = active ? rule.value : 'off';
    if (def.pair) fields[def.pair.field] = active ? rule.max : 'off';
  }
  return fields;
}

function ruleFormError(form, view = null) {
  if (form.marginMode === 'cross' && form.autoMargin.on) return '全仓不能自动追加逐仓保证金，请明确关闭此规则。';
  const fields = ruleFields(form, view);
  for (const def of RULE_FIELDS) {
    if (fields[def.field] !== 'off' && num(fields[def.field]) === null) return `${def.label}已启用，请填写有效数值或明确关闭。`;
  }
  for (const def of RULE_FIELDS) {
    if (def.pair && fields[def.field] !== 'off' && num(fields[def.pair.field]) === null) {
      return `${def.label}已启用，请同时填写${def.pair.valueLabel}和${def.pair.label}。`;
    }
  }
  if (fields.take_profit_book_min !== 'off') {
    if (fields.take_profit === 'off') return '止盈盘口门槛要和净收益止盈一起开：它只在止盈按标记价达标后才用。';
    if (num(fields.take_profit_book_min) < 0) return '止盈盘口门槛不能为负（0 = 为正即平）。';
    if (num(fields.take_profit_book_min) > num(fields.take_profit)) return '止盈盘口门槛不能高于止盈金额。';
  }
  return null;
}

const marginModeLabel = (mode) => mode === 'cross' ? '全仓' : '逐仓';

function marginModeError(venues, form) {
  if (state.mode !== 'live') return null;
  const modes = state.tradeConfig?.margin_modes;
  if (form.marginMode !== 'cross') {
    // 只在能力清单明确说没有逐仓时拦（币安统一账户只有全仓）；清单缺失时由服务端核对。
    const crossOnly = venues.filter((venue) => modes?.[venue] && !modes[venue].includes('isolated'));
    return crossOnly.length ? `${crossOnly.join(' / ')} 只支持全仓（统一账户），请选择全仓或更换场所。` : null;
  }
  if (!modes) return '尚未取得交易所保证金模式能力清单，请稍后重试。';
  const unsupported = venues.filter((venue) => !modes[venue]?.includes('cross'));
  return unsupported.length ? `${unsupported.join(' / ')} 暂不支持本机器人全仓开仓，请选择逐仓或更换场所。` : null;
}

function marginModeNote(venues, form) {
  const error = marginModeError(venues, form);
  if (error) return `<div class="alert error">${esc(error)}</div>`;
  if (form.marginMode !== 'cross') return '';
  const noLiq = venues.filter((venue) => ['arcus', 'mexc'].includes(venue));
  const noLiqNote = noLiq.length ? `<b>${noLiq.map(esc).join(' / ')} 当前没有可用的全仓强平价接口口径，无法执行该腿的爆仓保护；请在交易所自行监控账户风险。</b> ` : '';
  return `<div class="alert warn">${noLiqNote}<b>全仓：</b>同一账户的仓位共用权益，其他仓位亏损也会影响本笔。初始占用仅为估算，强平价以交易所为准；缺少有效数据时爆仓保护无法执行。触发保护会整笔退出，不保证整个账户恢复安全。Bybit UTA 请先在交易所设置全仓，机器人不会修改账户级模式。已有持仓不会自动切换；不支持自动追加逐仓保证金。</div>`;
}

function autoMarginNote(venues, enabled) {
  if (!enabled) return '';
  if (state.mode === 'paper') return '<div class="alert info">自动追加保证金仅模拟逐仓，不会向交易所转入真实资金。</div>';
  const supported = state.tradeConfig?.live?.auto_margin_venues;
  if (!Array.isArray(supported)) return '<div class="alert warn">尚未取得实盘自动追加保证金能力清单，无法确认这两条腿支持。规则仍保留，预览 / 保存由服务端校验；不支持全仓。</div>';
  const unsupported = venues.filter((venue) => !supported.includes(venue));
  if (!unsupported.length) return '<div class="alert info">两腿支持自动追加保证金，但仅限逐仓；全仓不支持，服务端会核对实际保证金模式。</div>';
  return `<div class="alert warn"><b>${unsupported.map(esc).join(' / ')} 不支持自动追加保证金。</b>已选规则不会被悄悄关闭；请主动关闭此规则或换支持的腿。支持场所：${supported.length ? supported.map(esc).join('、') : '无'}。仅逐仓，不支持全仓。</div>`;
}

// quiet：只为刷新预检结论重取表，不重算右边的开仓计划。
async function loadPairs({ quiet = false } = {}) {
  const s = state.strategy;
  const seq = ++s.pairSeq;
  const params = new URLSearchParams();
  // 实盘模式只在已连接实盘的场所里选：没连的那一家怎么都下不了单。
  const allowed = liveVenueList();
  if (allowed && allowed.length >= 2 && !s.userPicked) {
    if (!allowed.includes(s.a)) s.a = allowed[0];
    if (!allowed.includes(s.b) || s.b === s.a) s.b = allowed.find((venue) => venue !== s.a);
  }
  if (s.a) params.set('a', s.a);
  if (s.b) params.set('b', s.b);
  params.set('view', s.view);
  // 预检按右边表单里的金额、杠杆与平仓规则、右上角的模式查；选中的那一行最先查。
  const f = s.form;
  if (f.size) params.set('size', f.size);
  params.set('leverage', f.leverage || '3');
  params.set('margin_mode', f.marginMode || 'isolated');
  params.set('mode', state.mode);
  if (s.selected) params.set('focus', s.selected.symbol);
  for (const [key, value] of Object.entries(ruleFields(f, s.view))) params.set(key, value);
  // 带上令牌：实盘的持仓数、对账这类账户级的关只在有令牌时才核对。
  const body = await getJson(`/api/strategy?${params}`, { auth: true });
  if (seq !== s.pairSeq) return;
  s.board = body.board;
  s.precheck = body.precheck || null;
  schedulePrecheckPoll();
  s.a = body.board.a;
  s.b = body.board.b;
  saveStrategyParams();
  if (state.page === 'strategy') setAge(body.age_ms, false);
  fillVenueSelects();
  // 选中的合约：这对场所还是原来那对就保留（方向以服务端为准）。行不在榜里（被截断、被排除）
  // 也保留 —— 开仓计划按合约现算，不依赖它在不在表里；场所换了才清掉。
  if (s.selected) {
    const row = s.board.rows.find((r) => String(r.symbol) === s.selected.symbol);
    if (s.selected.fromRh) {
      // 从 RH 价差页带来的方向是按两边深度、对照正常基差选的，不被标记价方向覆盖；场所换了才清掉。
      if (![s.a, s.b].includes(s.selected.long) || ![s.a, s.b].includes(s.selected.short)) s.selected = null;
    } else if (row && row.long && row.short) {
      s.selected = { symbol: String(row.symbol), long: row.long, short: row.short };
    } else if (![s.a, s.b].includes(s.selected.long) || ![s.a, s.b].includes(s.selected.short)) {
      s.selected = null;
    }
  }
  renderStrategy();
  if (s.selected && !quiet) loadPlan();
}

// 还有候选在排队预检、或选中的那一行在等重查时，隔几秒再取一次表，结论出来就显示；
// 不在策略页或页面不可见时不取。
function schedulePrecheckPoll() {
  const s = state.strategy;
  clearTimeout(s.pcTimer);
  const pc = s.precheck;
  if (!pc || !(pc.pending > 0 || pc.focus_refreshing)) return;
  s.pcTimer = setTimeout(() => {
    if (state.page !== 'strategy' || document.hidden) return;
    loadPairs({ quiet: true }).catch(() => {});
  }, pc.focus_refreshing ? FOCUS_POLL_MS : PRECHECK_POLL_MS);
}

// 金额、杠杆、模式或选中的行变了：稍等一下再重取表，让服务端按新参数预检。
function reloadPairsSoon(delay = 800) {
  const s = state.strategy;
  clearTimeout(s.pairsDebounce);
  s.pairsDebounce = setTimeout(() => {
    if (state.page !== 'strategy') return;
    loadPairs({ quiet: true }).catch(() => {});
  }, delay);
}

function precheckOf(row) {
  return state.strategy.precheck?.rows?.[String(row.symbol)] || null;
}

// 实盘模式下已连接实盘的场所；纸面模式或还不知道时为 null（不限制）。
function liveVenueList() {
  if (state.mode !== 'live') return null;
  return state.tradeConfig?.live?.venues || null;
}

// 这一行按当前参数能不能下单：预检通过，且没有挡住整张表的账户级原因。
function orderable(row) {
  const pc = state.strategy.precheck;
  if (!pc || pc.paused || pc.account_block) return false;
  return precheckOf(row)?.status === 'pass';
}

function precheckCounts() {
  const counts = { pass: 0, blocked: 0, unknown: 0, pending: 0 };
  for (const check of Object.values(state.strategy.precheck?.rows || {})) {
    if (check.status in counts) counts[check.status] += 1;
  }
  return counts;
}

// 「只显示能下单的」时表格为空的原因：暂停、账户级的关、还在查、或者确实都不过。
function orderableEmptyText() {
  const pc = state.strategy.precheck;
  if (!pc) return '正在取预检结论…';
  if (pc.paused) return `这对场所现在下不了单：${pc.paused}`;
  if (pc.account_block) return `现在哪一行都下不了单：${pc.account_block}`;
  const counts = precheckCounts();
  if (pc.pending > 0) {
    return `正在预检（已查 ${pc.checked} / ${pc.covered}），能下单的会陆续出现在这里。`;
  }
  return `按 ${precheckScope()} 这对场所目前没有能下单的合约（✗ ${counts.blocked} 个${counts.unknown ? `、? ${counts.unknown} 个没查成` : ''}；取消勾选「只显示能下单的」可以看每一行的原因）。可以调小金额、换杠杆或换一对场所。`;
}

function ageText(seconds) {
  if (seconds == null) return '';
  if (seconds < 60) return `${seconds} 秒前`;
  return `${Math.round(seconds / 60)} 分钟前`;
}

const PRECHECK_BADGE = {
  pass: { cls: 'pc-pass', text: '✓ 可下单', title: '能下单（预检通过）' },
  blocked: { cls: 'pc-blocked', text: '✗ 预检', title: '预检不过' },
  unknown: { cls: 'pc-unknown', text: '? 预检', title: '未能预检（不代表能下，也不代表不能下）' },
  pending: { cls: 'pc-pending', text: '… 预检', title: '排队预检中' },
};

function precheckBadge(row) {
  const check = precheckOf(row);
  const badge = check && PRECHECK_BADGE[check.status];
  if (!badge) return '';
  const age = check.age_s != null ? `（${ageText(check.age_s)}）` : '';
  const title = `${badge.title}${age}${check.reason ? `：${check.reason}` : ''}`;
  return `<span class="tag ${badge.cls}" title="${esc(title)}">${badge.text}</span>`;
}

function precheckScope() {
  const pc = state.strategy.precheck;
  if (!pc) return '';
  return `${num(pc.size)} USDT · ${num(pc.leverage)}x · ${marginModeLabel(pc.margin_mode)} · ${pc.live ? '实盘' : '纸面'}`;
}

function renderPrecheckCaption() {
  const pc = state.strategy.precheck;
  const box = $('precheck-caption');
  if (!pc) {
    box.textContent = '';
    return;
  }
  if (pc.paused) {
    box.innerHTML = `<span class="pc-warning">这对场所现在下不了单</span>：${esc(pc.paused)}`;
    return;
  }
  const counts = precheckCounts();
  const banners = [];
  if (pc.account_block) banners.push(`<span class="pc-warning">⚠ 现在哪一行都下不了单：${esc(pc.account_block)}</span>`);
  if (pc.note) banners.push(`<span class="pc-note">${esc(pc.note)}</span>`);
  const progress = pc.pending
    ? `已查 ${pc.checked} / ${pc.covered}，${pc.pending} 个排队中`
    : `${pc.covered} 个候选都查过了`;
  box.innerHTML =
    banners.map((banner) => `${banner}<br />`).join('') +
    `<b>后台预检</b>（${esc(precheckScope())}）：服务端对这对场所所有看上去能做的合约现拉两边盘口，走一遍和「预览」相同的检查（含平仓规则），${progress}；` +
    `✓ 能下单 ${counts.pass} · ✗ 不过 ${counts.blocked}${counts.unknown ? ` · ? 没查成 ${counts.unknown}` : ''}。` +
    `<span class="muted">选中的那一行 30 秒、能下单的 90 秒、其余 2 ~ 5 分钟重查一次；当日亏损只在下单时核对，下单前的预览为准。</span>`;
}

// 右边计划卡里给选中那一行的预检结论：点进来不用先预览就知道会不会被拦。
function renderPlanPrecheck() {
  const box = $('plan-precheck');
  if (!box) return;
  const s = state.strategy;
  const row = s.selected && s.board?.rows.find((r) => String(r.symbol) === s.selected.symbol);
  const check = row && precheckOf(row);
  if (!check || s.precheck?.paused) {
    box.innerHTML = '';
    return;
  }
  const badge = PRECHECK_BADGE[check.status];
  const cls = { pass: 'ok', blocked: 'error', unknown: 'warn', pending: 'info' }[check.status] || 'info';
  const age = check.age_s != null ? `，${ageText(check.age_s)}` : '';
  box.innerHTML =
    `<div class="alert ${cls} pc-alert"><b>${badge.title}</b>（${esc(precheckScope())}${age}）${check.reason ? `：${esc(check.reason)}` : ''}</div>` +
    (s.precheck?.account_block ? `<div class="alert error pc-alert">现在哪一行都下不了单：${esc(s.precheck.account_block)}</div>` : '') +
    (s.precheck?.note ? `<div class="alert warn pc-alert">${esc(s.precheck.note)}</div>` : '');
}

function fillVenueSelects() {
  const s = state.strategy;
  const allowed = liveVenueList();
  // 实盘模式默认只列已连接的场所；当前选着的（比如从机会页跳过来的）即使没连也要留在下拉里，
  // 标上「未连实盘」，不然选中的值会凭空消失。
  const venues = (s.board?.venues || []).filter(
    (venue) => !allowed || allowed.includes(venue) || venue === s.a || venue === s.b,
  );
  for (const [id, value] of [['venue-a', s.a], ['venue-b', s.b]]) {
    const select = $(id);
    select.innerHTML = venues
      .map((v) => `<option value="${esc(v)}">${esc(v)}${allowed && !allowed.includes(v) ? '（未连实盘）' : ''}</option>`)
      .join('');
    select.value = value;
  }
}

function pairTags(row) {
  const tags = [];
  const badge = precheckBadge(row);
  if (badge) tags.push(badge);
  if (row.excluded) tags.push(`<span class="tag coral" title="${esc(row.excluded)}">已排除</span>`);
  if (row.gated) tags.push(`<span class="tag warn" title="${esc(row.gated)}">基差门槛</span>`);
  if (row.a.oi_capped || row.b.oi_capped) tags.push('<span class="tag warn" title="至少一条腿触及持仓量上限，只能减仓">OI 上限</span>');
  if (row.opportunity?.spread_unknown) tags.push('<span class="tag" title="至少一条腿拿不到盘口：成本是下界，净收益是上界">穿价未知</span>');
  return tags.join('');
}

function legsText(long, short) {
  if (!long || !short) return `<span class="muted">${state.strategy.view === 'spread' ? '价格相同' : '费率相同'}</span>`;
  return `<span class="long-c">多 ${esc(long)}</span> · <span class="short-c">空 ${esc(short)}</span>`;
}

function renderPairDiagram() {
  const s = state.strategy;
  const long = s.selected?.long || s.a;
  const short = s.selected?.short || s.b;
  $('pair-diagram').innerHTML = `
    <div class="venue-node"><span class="name">${esc(long || '—')}</span><span class="side-pill long">做多</span></div>
    <span class="rail"></span>
    <span class="delta">Δ ≈ 0</span>
    <span class="rail rev"></span>
    <div class="venue-node right"><span class="name">${esc(short || '—')}</span><span class="side-pill short">做空</span></div>
    <p class="pair-caption">${s.view === 'spread'
      ? '两腿等额对冲价格；便宜的一侧做多、贵的一侧做空，两边价格收敛时赚到这段价差。'
      : '两腿等额，价格涨跌互相抵消；低费率一侧做多、高费率一侧做空，费差就是收益。'}${s.selected ? `当前：${esc(s.selected.symbol)}` : `选一个合约，方向会跟着${s.view === 'spread' ? '价格' : '费率'}高低自动定。`}</p>`;
}

function renderStrategy() {
  const s = state.strategy;
  const board = s.board;
  if (!board) return;
  const norm = APR_NORMS[s.norm];
  for (const button of $('apr-seg').querySelectorAll('button')) {
    button.classList.toggle('on', button.getAttribute('data-norm') === s.norm);
  }
  const spreadView = s.view === 'spread';
  for (const button of $('strategy-view').querySelectorAll('button')) {
    button.classList.toggle('on', button.getAttribute('data-view') === s.view);
  }
  $('strategy-title').textContent = spreadView ? '跨场所价差套利' : '跨场所资金费套利';
  $('strategy-intro').textContent = spreadView
    ? '同一资产在两家的价格不一样时，买便宜的一边、卖贵的一边，两边价格收敛就赚到这段价差；持有期间照常收付资金费。批量行情不给买一卖一的场所（Hyperliquid、Lighter、Arcus 等 DEX）先按标记价估算，预览时现拉两边盘口核算。纸面还是实盘在右上角「令牌」里选。'
    : '一家做多、一家做空，等额两腿对冲掉价格，费差就是收益。选两家场所，挑一个合约，右边给出开仓计划；配置了令牌时可以直接在右边预览并下单，纸面还是实盘在右上角「令牌」里选。';
  // 价差视角表头压短：场所名本身就长，完整含义放在 title 里，免得最右一列被挤出卡片。
  $('head-a').textContent = spreadView ? board.a : `${board.a} ${norm.label}`;
  $('head-a').title = spreadView ? `${board.a} 标记价` : '';
  $('head-b').textContent = spreadView ? board.b : `${board.b} ${norm.label}`;
  $('head-b').title = spreadView ? `${board.b} 标记价` : '';
  $('head-c').textContent = spreadView ? '价差' : '费差';
  $('head-c').title = spreadView ? '标记价基差：(贵的 − 便宜的) / 中间价' : '两边费率之差，不扣成本';
  $('head-d').textContent = spreadView ? '资金费/年' : '摊费后';
  $('head-d').title = spreadView ? '按价格方向持有时的资金费年化（空腿 − 多腿）；为负表示要付' : '扣掉往返手续费与穿价（按持有天数摊销）之后';
  renderPairDiagram();

  const passed = (row) => !s.pcOnly || orderable(row);
  const ranked = (spreadView
    ? board.rows.filter((row) => row.long && row.short && !row.excluded && num(row.basis_pct) > 0)
    : board.rows.filter((row) => row.opportunity && !row.excluded)
  ).filter(passed);
  renderPrecheckCaption();
  $('top-caption').textContent = spreadView
    ? `共 ${board.total} 个合约同时在两家上市，按标记价差排序`
    : `共 ${board.total} 个合约同时在两家上市，按摊费后净${norm.label}排序`;
  const top = ranked.slice(0, 3);
  $('top-cards').innerHTML = top.length
    ? top
        .map((row) => {
          const op = row.opportunity;
          const selected = s.selected && s.selected.symbol === String(row.symbol);
          const figure = spreadView
            ? `<span class="big pos">${pctRaw(row.basis_pct, 2)}</span>
              <span class="sub">标记价差 · 持有期资金费 ${rate(row.carry_apr)}</span>`
            : `<span class="big ${cls(op.funding_apr)}">${rate(op.funding_apr)}</span>
              <span class="sub">摊费后净${norm.label} · 毛 ${rate(row.gross_apr)} · 往返 ${pct(op.round_trip_cost, 3)}</span>`;
          return `<button type="button" class="top-card${selected ? ' selected' : ''}" data-symbol="${esc(row.symbol)}">
            <span class="sym">${esc(row.symbol)}${pairTags(row)}</span>
            <span class="legs">${legsText(row.long, row.short)}</span>
            ${figure}
          </button>`;
        })
        .join('')
    : s.pcOnly
      ? `<div class="top-empty">${esc(orderableEmptyText())}</div>`
      : `<div class="top-empty">${spreadView ? '这两家场所之间没有价格差可做。' : '这两家场所之间没有可配对的正费差合约。'}换一对场所试试。</div>`;
  for (const card of $('top-cards').querySelectorAll('.top-card')) {
    card.addEventListener('click', () => selectMarket(card.getAttribute('data-symbol')));
  }

  const needle = s.search.trim().toUpperCase();
  const isSelected = (row) => s.selected && s.selected.symbol === String(row.symbol);
  const filtered = board.rows.filter((row) => (!needle || String(row.symbol).includes(needle)) && passed(row));
  // 选中的那一行永远留在表里（钉在最上面）：从机会页跳过来的合约不能因为「只显示能下单的」
  // 或预检还没跑完就整行消失，看起来像没跳过来。
  const pinned = board.rows.find(isSelected);
  const pinnedExtra = pinned && !filtered.includes(pinned) ? 1 : 0;
  const shown = (pinnedExtra ? [pinned, ...filtered] : filtered).slice(0, 150);
  $('market-count').textContent = `${s.pcOnly ? `能下单 ${filtered.length} 个 / 共 ${board.total} 个` : `${filtered.length} 个`}${pinnedExtra ? '（另有选中的一行，现在不能下单）' : ''}${filtered.length > shown.length ? `，显示前 ${shown.length}` : ''}${board.total > board.rows.length ? `（服务端截断到 ${board.rows.length}）` : ''}`;
  $('market-empty').textContent = shown.length
    ? ''
    : s.pcOnly
      ? needle
        ? `没有匹配 ${needle} 且能下单的合约。`
        : orderableEmptyText()
      : needle
        ? `没有匹配 ${needle} 的合约。`
        : '这两家场所没有同时上市的合约。';
  $('market-rows').innerHTML = shown
    .map((row) => {
      const op = row.opportunity;
      const selected = s.selected && s.selected.symbol === String(row.symbol);
      const cells = spreadView
        ? `<td class="num">${price(row.a.mark_price)}</td>
        <td class="num">${price(row.b.mark_price)}</td>
        <td>${legsText(row.long, row.short)}</td>
        <td class="num"><b>${pctRaw(row.basis_pct, 3)}</b></td>
        <td class="num ${cls(row.carry_apr)}">${rate(row.carry_apr)}</td>`
        : `<td class="num ${cls(row.a.apr)}">${rate(row.a.apr)}<span class="tag">${row.a.interval_h}h</span></td>
        <td class="num ${cls(row.b.apr)}">${rate(row.b.apr)}<span class="tag">${row.b.interval_h}h</span></td>
        <td>${legsText(row.long, row.short)}</td>
        <td class="num">${rate(row.gross_apr)}</td>
        <td class="num ${cls(op?.funding_apr)}">${op ? `<b>${rate(op.funding_apr)}</b>` : '—'}</td>`;
      return `<tr class="${selected ? 'selected' : ''}" data-symbol="${esc(row.symbol)}">
        <td><b>${esc(row.symbol)}</b></td>
        ${cells}
        <td>${pairTags(row) || '<span class="muted">—</span>'}</td>
      </tr>`;
    })
    .join('');
  for (const tr of $('market-rows').querySelectorAll('tr')) {
    tr.addEventListener('click', () => selectMarket(tr.getAttribute('data-symbol')));
  }
  renderPlanShell();
  renderPlanPrecheck();
}

function selectMarket(symbol) {
  const s = state.strategy;
  const row = s.board?.rows.find((r) => String(r.symbol) === symbol);
  if (!row) return;
  s.selected = { symbol, long: row.long, short: row.short };
  s.plan = null;
  renderStrategy();
  loadPlan();
  // 告诉服务端选中了哪一行：它排到预检最前面，结论不够新就马上重查。
  reloadPairsSoon(0);
}

// 表单只在换合约时整体重画；输入时只刷新结果区，避免打字时丢焦点。
function renderPlanShell() {
  const s = state.strategy;
  const card = $('plan-card');
  if (!s.selected) {
    card.dataset.key = '';
    card.innerHTML = `<div class="plan-empty"><b>开仓计划</b>在左边选一个合约，这里会给出两腿的保证金、强平价、每日资金费、成本和规则校验。</div>`;
    return;
  }
  const key = `${s.view}|${s.selected.symbol}|${s.selected.long}|${s.selected.short}|${s.selected.fromRh ? `rh:${s.selected.fromRh.target}` : ''}`;
  if (card.dataset.key === key) return;
  card.dataset.key = key;
  const f = s.form;
  const leverageOptions = ['1', '2', '3', '5', '10', '20'];
  if (f.leverage && !leverageOptions.includes(f.leverage)) leverageOptions.push(f.leverage);
  const visibleRules = RULE_FIELDS.filter((def) => def.key !== (s.view === 'spread' ? 'minApr' : 'basisExit'));
  card.innerHTML = `
    <div class="plan-head">
      <span class="sym">${esc(s.selected.symbol)}</span>
      <span class="small">${legsText(s.selected.long, s.selected.short)}</span>
    </div>
    ${rhOriginNote(s.selected.fromRh)}
    <div class="form-grid">
      <label class="field"><span>单腿名义 (USDT)</span><input type="number" id="plan-size" value="${esc(f.size)}" min="1" step="100" /></label>
      <label class="field"><span>杠杆（两腿相同）</span><select id="plan-leverage">
        ${leverageOptions.map((v) => `<option value="${v}"${v === (f.leverage || '3') ? ' selected' : ''}>${v}x</option>`).join('')}
      </select></label>
      <label class="field"><span>保证金模式（两腿相同）</span><select id="plan-margin-mode">
        <option value="isolated"${f.marginMode !== 'cross' ? ' selected' : ''}>逐仓</option>
        <option value="cross"${f.marginMode === 'cross' ? ' selected' : ''}>全仓</option>
      </select></label>
    </div>
    <div id="plan-margin-mode-note">${marginModeNote([s.selected.long, s.selected.short], f)}</div>
    <div class="rules">
      ${visibleRules.map((def) => ruleControl(def, f[def.key], def.id, false, f.marginMode)).join('')}
    </div>
    <div id="plan-auto-margin-note">${autoMarginNote([s.selected.long, s.selected.short], f.autoMargin.on)}</div>
    <div id="plan-precheck"></div>
    <div id="plan-result"><p class="muted small">计算中…</p></div>
    <div id="trade-box"></div>`;
  resetTrade();
  let debounce = null;
  const sync = () => {
    s.planSeq += 1;
    s.pairSeq += 1;
    f.size = $('plan-size').value;
    f.leverage = $('plan-leverage').value;
    const changedMode = f.marginMode !== $('plan-margin-mode').value;
    f.marginMode = $('plan-margin-mode').value;
    saveStrategyParams();
    for (const def of visibleRules) {
      f[def.key].on = $(`${def.id}-on`).checked;
      f[def.key].value = $(`${def.id}-value`).value;
      $(`${def.id}-value`).disabled = !f[def.key].on;
      if (def.pair) {
        f[def.key].max = $(`${def.id}-max`).value;
        $(`${def.id}-max`).disabled = !f[def.key].on;
      }
    }
    $('plan-auto-margin-note').innerHTML = autoMarginNote([s.selected.long, s.selected.short], f.autoMargin.on);
    $('plan-margin-mode-note').innerHTML = marginModeNote([s.selected.long, s.selected.short], f);
    const protectHint = $('rule-protect-on').nextElementSibling.querySelector('small');
    protectHint.textContent = f.marginMode === 'cross' ? '全仓仅按交易所有效强平价判断，触发时整笔退出；缺数据会提示无法评估' : RULE_FIELDS.find((def) => def.key === 'protect').hint;
    resetTrade();
    if (changedMode) renderTradeBox();
    clearTimeout(debounce);
    debounce = setTimeout(loadPlan, 300);
    // 金额、杠杆、规则变了：按新参数重新预检（规则只在服务端现比，不会重新拉盘口）。
    reloadPairsSoon();
  };
  for (const input of card.querySelectorAll('.form-grid input, .form-grid select, .rules input')) {
    input.addEventListener('input', sync);
    input.addEventListener('change', sync);
  }
  renderTradeBox();
}

async function loadPlan() {
  const s = state.strategy;
  if (!s.selected) return;
  const seq = ++s.planSeq;
  const box = () => $('plan-result');
  if (!s.selected.long || !s.selected.short) {
    if (box()) box().innerHTML = `<div class="alert warn">两家${s.view === 'spread' ? '标记价' : '费率'}相同，没有方向可做。</div>`;
    return;
  }
  const f = s.form;
  const params = new URLSearchParams({
    symbol: s.selected.symbol,
    long: s.selected.long,
    short: s.selected.short,
    size: f.size || '1000',
    leverage: f.leverage || '3',
    margin_mode: f.marginMode || 'isolated',
  });
  params.set('view', s.view);
  for (const [key, value] of Object.entries(ruleFields(f, s.view))) params.set(key, value);
  try {
    const plan = await getJson(`/api/plan?${params}`);
    if (seq !== s.planSeq) return;
    s.plan = plan;
    renderPlanResult(plan);
  } catch (error) {
    if (seq !== s.planSeq) return;
    s.plan = null;
    if (box()) box().innerHTML = `<div class="alert error">${esc(error.message)}</div>`;
  }
}

function planLeg(leg, label, mode = 'isolated') {
  const sideClass = leg.side === 'buy' ? 'long' : 'short';
  const max = leg.max_leverage == null ? '上限未知' : `上限 ${Number(num(leg.max_leverage).toFixed(2))}x`;
  return `<div class="leg-card">
    <div class="top"><span class="venue">${esc(leg.venue)}</span><span class="side-pill ${sideClass}">${label}</span></div>
    <div class="kv"><span>杠杆</span><span>${num(leg.leverage)}x <span class="muted">${max}</span></span></div>
    <div class="kv"><span>${mode === 'cross' ? '预计初始占用（非独立保证金）' : '逐仓保证金'}</span><span>${usd(leg.margin_usdt)}</span></div>
    <div class="kv"><span>标记价</span><span>${price(leg.mark_price)}</span></div>
    <div class="kv"><span>强平价</span><span>${price(leg.liquidation_price)}</span></div>
    <div class="kv"><span>强平距离</span><span>${healthText(leg.liq_distance_pct, leg.health)}</span></div>
    ${healthBar(leg.liq_distance_pct, leg.health)}
  </div>`;
}

function renderPlanResult(plan) {
  const box = $('plan-result');
  if (!box) return;
  const op = plan.opportunity;
  const alerts = [];
  if (plan.rules_error) alerts.push(`<div class="alert error">规则不成立：${esc(plan.rules_error)}</div>`);
  if (plan.gated) alerts.push(`<div class="alert warn">入场基差门槛：${esc(plan.gated)}</div>`);
  for (const warning of plan.warnings || []) alerts.push(`<div class="alert warn">${esc(warning)}</div>`);
  if (plan.protection_trim_pct != null) {
    alerts.push(
      `<div class="alert info">爆仓保护：任一腿强平距离跌到 ${num(plan.rules.liq_protection_pct)}% 时，两腿各减约 ${num(plan.protection_trim_pct).toFixed(1)}%，把距离拉回 ${num(plan.protection_target_pct)}%。</div>`,
    );
  }
  const lowerBound = plan.cost_is_lower_bound ? ' <span class="muted" title="至少一条腿拿不到盘口">下界</span>' : '';
  const sp = plan.spread;
  const upper = sp?.estimated_from_marks ? ' <span class="tag warn" title="至少一条腿批量接口没给买一卖一：按标记价估，是上界；预览时按现拉盘口核算">估·上界</span>' : '';
  // 设了收敛目标：收敛到目标就平，只赚得到入场与目标之间那一段 —— 主数字按它算。
  const targeted = sp?.target_pct != null;
  const collateralLabel = plan.margin_mode === 'cross' ? '两腿预计初始占用合计（非独立保证金）' : '两腿保证金合计';
  const summary = sp
    ? `${targeted
        ? `<div class="kv hero"><span>收敛到目标 ${num(sp.target_pct)}% 平仓时净收益</span><span class="${cls(sp.target_net_usdt)}">${pnlUsd(sp.target_net_usdt)}（${pct(sp.target_net_pct, 3)}）${upper}</span></div>
      <div class="kv"><span title="目标低于它，收敛到目标平仓时扣完手续费和平仓穿价还有得赚">保本目标</span><span>低于 ${pctRaw(sp.break_even_target_pct, 3)}</span></div>
      <div class="kv"><span>收敛到 0 时净收益</span><span class="${cls(sp.net_usdt)}">${pnlUsd(sp.net_usdt)}（${pct(sp.net_pct, 3)}）</span></div>`
        : `<div class="kv hero"><span>收敛后一次性净收益</span><span class="${cls(sp.net_usdt)}">${pnlUsd(sp.net_usdt)}（${pct(sp.net_pct, 3)}）${upper}</span></div>`}
      <div class="kv"><span>价差（标记价）</span><span>${pctRaw(sp.mark_basis_pct, 3)}</span></div>
      <div class="kv"><span>可成交价差（买一卖一）</span><span>${pctRaw(sp.executable_basis_pct, 3)}${upper}</span></div>
      <div class="kv"><span>持有期资金费（每天）</span><span class="${cls(sp.carry_daily_usdt)}">${pnlUsd(sp.carry_daily_usdt)}</span></div>
      <div class="kv"><span>预计持有（价差半衰期）</span><span>${num(sp.hold_days).toFixed(1)} 天${sp.hold_measured ? '（实测）' : '（配置值）'}</span></div>
      <div class="kv"><span>往返成本（手续费 + 穿价）</span><span>${usd(plan.round_trip_cost_usdt)}${lowerBound}</span></div>
      <div class="kv"><span>${collateralLabel}</span><span>${usd(plan.margin_total_usdt)}</span></div>
      <div class="kv"><span>两腿里更近的强平距离</span><span>${healthText(plan.risk.liq_distance_pct, plan.risk.health)}</span></div>`
    : `<div class="kv hero"><span>摊费后净年化</span><span class="${cls(op.funding_apr)}">${pct(op.funding_apr, 2)}</span></div>
      <div class="kv"><span>${plan.margin_mode === 'cross' ? '预计占用年化（非账户收益率）' : '保证金年化'}</span><span class="${cls(plan.risk.margin_apr)}">${pct(plan.risk.margin_apr, 2)}</span></div>
      <div class="kv"><span>毛费差年化</span><span>${pct(op.apr, 2)}</span></div>
      <div class="kv"><span>每日资金费（毛 / 摊费后）</span><span>${usd(plan.daily_gross_usdt)} / ${usd(plan.daily_net_usdt)}</span></div>
      <div class="kv"><span>往返成本（手续费 + 穿价）</span><span>${usd(plan.round_trip_cost_usdt)}${lowerBound}</span></div>
      <div class="kv"><span>${collateralLabel}</span><span>${usd(plan.margin_total_usdt)}</span></div>
      <div class="kv"><span>入场基差（标记价）</span><span class="${cls(op.entry_basis_pct)}">${pctRaw(op.entry_basis_pct, 3)}</span></div>
      <div class="kv"><span>两腿里更近的强平距离</span><span>${healthText(plan.risk.liq_distance_pct, plan.risk.health)}</span></div>`;
  box.innerHTML = `
    <div class="legs-grid">${planLeg(plan.long, '做多', plan.margin_mode)}${planLeg(plan.short, '做空', plan.margin_mode)}</div>
    <div class="summary">${summary}
    </div>
    ${alerts.join('')}
    <div class="plan-actions">
      <button type="button" class="btn ghost" id="plan-depth">按 ${usd(plan.size_usdt, 0)} 做深度体检</button>
    </div>
    <div class="depth-out" id="plan-depth-out"></div>
    <div class="cmd"><code id="plan-cmd">${esc(plan.command)}</code><button type="button" class="btn ghost" id="plan-copy">复制</button></div>
    <p class="muted small">上面是估算。真正下单前要按当前盘口逐档核价、过闸门：在下面「下单」里先预览，确认后再下。命令行的等价做法是上面这条 <code>arb-paper</code> 命令。</p>`;
  $('plan-depth').addEventListener('click', () =>
    loadDepth(state.strategy.selected, 'plan-depth-out', Math.round(num(plan.size_usdt) || 1000)),
  );
  $('plan-copy').addEventListener('click', async () => {
    const text = plan.command;
    try {
      await navigator.clipboard.writeText(text);
      $('plan-copy').textContent = '已复制';
    } catch {
      const range = document.createRange();
      range.selectNodeContents($('plan-cmd'));
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      $('plan-copy').textContent = '已选中';
    }
    setTimeout(() => {
      if ($('plan-copy')) $('plan-copy').textContent = '复制';
    }, 1500);
  });
}

// ───────────────────────────── 下单 ─────────────────────────────
//
// 两步：先「预览」（服务端走完扫描、规则、深度体检与闸门，不下单），再「下单」（服务端
// 重新走一遍同样的流程后执行）。实盘还要手输合约名二次确认，服务端同样校验。

function resetTrade() {
  const t = state.trade;
  // 已提交的单不能靠改表单、换合约或自动刷新解锁。只有明确成功后的显式「再开一笔」可重置。
  if (t.done) return;
  // 预览在途时参数变了，让响应作废。
  t.seq += 1;
  if (t.busy) return;
  const dirty = t.preview || t.result || t.error || t.confirm;
  t.preview = null;
  t.result = null;
  t.error = null;
  t.confirm = '';
  t.done = false;
  // 状态本来就干净时不重画：改完输入框直接去点「预览」时，失焦触发的 change 会走到这里，
  // 这时重画会把按钮换掉，那一下点击就丢了。
  if (dirty) {
    t.daily = null; // 刚有过下单 / 预览：当日盈亏重读一次
    renderTradeBox();
  }
}

function tradeSucceeded(result) {
  const position = result?.position;
  if (!position || position.status !== 'open' || !position.long || !position.short || result.error || result.reconciliation_error) return false;
  // 对账不一致只冻结出问题的币：别的币有不一致不影响这一笔（归不到币的仍算）。
  const symbol = position.symbol;
  if ((result.reconciliation?.divergences || []).some((d) => !d.symbol || d.symbol === symbol)) return false;
  return state.mode === 'paper' || Array.isArray(result.reconciliation?.divergences);
}

function startAnotherTrade() {
  const t = state.trade;
  if (t.busy || !t.done || !tradeSucceeded(t.result)) return;
  delete t.attempts[state.mode];
  t.done = false;
  t.dailyPnl = '';
  resetTrade();
}

// 选中的这对腿在配对表里的那一行（带门槛、排除与机会信息）。
function selectedRow() {
  const s = state.strategy;
  const sel = s.selected;
  return sel ? (s.board?.rows || []).find((r) => r.symbol === sel.symbol && r.long === sel.long && r.short === sel.short) : null;
}

// 这对腿为什么不能下单：服务端按同一份扫描结果做同样的判断，这里提前说出来，不让人点了预览才碰壁。
function tradeBlocked(row) {
  const selected = state.strategy.selected;
  const marginError = selected && marginModeError([selected.long, selected.short], state.strategy.form);
  if (marginError) return { title: '保证金模式不可用', detail: marginError };
  if (!row) return null;
  if (row.excluded) return { title: '读数被可信度筛查排除', detail: row.excluded };
  if (state.strategy.view === 'spread') {
    // 价差视角不看入场基差门槛；DEX 没有批量买一卖一，机会要到预览时现拉盘口才算得出。
    if (!row.long || !row.short) return { title: '没有方向', detail: '两边标记价相同，没有价差可做。' };
    return null;
  }
  if (row.gated) {
    return {
      title: '被入场基差门槛挡下',
      detail: `${row.gated}。空腿比多腿便宜太多，两边价格一收敛这笔就先亏掉这部分，要很久的资金费才补得回来。确实要做，在 .env 里调大 ARB_MAX_ENTRY_BASIS_PCT（或设为 off）后重启看板。`,
    };
  }
  if (!row.opportunity) return { title: '算不出机会', detail: '费差方向不成立，或扣掉往返成本后不划算。' };
  return null;
}

function liveUsable() {
  const live = state.tradeConfig?.live;
  const sel = state.strategy.selected;
  return Boolean(live && sel && live.venues.includes(sel.long) && live.venues.includes(sel.short));
}

// 读一次台账算出的当日盈亏（不联网）。每次开仓预览前重读，别用旧的。
async function ensureDaily(force = false) {
  const t = state.trade;
  if (t.dailyLoading || (t.daily && !force)) return;
  t.dailyLoading = true;
  const { ok, body } = await api('/api/trade/daily', { auth: true });
  t.dailyLoading = false;
  t.daily = ok ? body : { error: body.error || '读取失败' };
  if ($('daily-hint')) renderTradeBox();
}

function tradeBody(extra = {}) {
  const s = state.strategy;
  const f = s.form;
  const t = state.trade;
  const body = {
    mode: state.mode,
    symbol: s.selected.symbol,
    long: s.selected.long,
    short: s.selected.short,
    size: f.size || '1000',
    leverage: f.leverage || '3',
    margin_mode: f.marginMode || 'isolated',
    view: s.view,
    ...extra,
  };
  if (state.mode === 'live') body.daily_pnl = t.dailyPnl;
  Object.assign(body, ruleFields(f, s.view));
  return body;
}

async function previewTrade() {
  const t = state.trade;
  if (t.busy || t.done || !token()) return;
  const selected = state.strategy.selected;
  const error = ruleFormError(state.strategy.form, state.strategy.view) || (selected && marginModeError([selected.long, selected.short], state.strategy.form));
  if (error) {
    t.preview = null;
    t.error = { error };
    renderTradeBox();
    return;
  }
  const seq = ++t.seq;
  t.busy = true;
  t.preview = null;
  t.result = null;
  t.error = null;
  renderTradeBox();
  const { ok, body } = await api('/api/trade/preview', { method: 'POST', body: tradeBody(), auth: true });
  t.busy = false;
  // 请求期间参数或合约变了：这份预览不代表当前参数，丢掉。
  if (seq === t.seq) {
    if (ok) t.preview = body;
    else t.error = body;
  }
  renderTradeBox();
}

async function executeTrade() {
  const t = state.trade;
  if (t.busy || t.done || !t.preview || !token()) return;
  if (state.mode === 'live' && state.tradeConfig?.live?.mode !== 'trade') return;
  const base = state.strategy.selected.symbol.split('/')[0];
  if (state.mode === 'live' && t.confirm.trim().toUpperCase() !== base.toUpperCase()) return;
  t.busy = true;
  t.done = true;
  renderTradeBox();
  const extra = state.mode === 'live' ? { confirm: t.confirm.trim() } : {};
  const { ok, body } = await api('/api/trade/open', { method: 'POST', body: tradeBody(extra), auth: true });
  t.busy = false;
  if (ok && body.position) t.result = body;
  else t.error = { ...body, error: body.error || '下单响应未提供可核对的仓位，结果未知', executed: true };
  t.attempts[state.mode] = { result: t.result, error: t.error };
  renderTradeBox();
}

function reconciliationText(reconciliation) {
  if (!reconciliation) return '';
  if (!reconciliation.divergences?.length) {
    return `<div class="alert info">对账干净：核对 ${reconciliation.checked_positions} 个仓位 / ${reconciliation.checked_venues} 个场所。</div>`;
  }
  const divergences = reconciliation.divergences;
  const hints = [];
  if (divergences.some((d) => d.kind === 'position_mismatch' && /台账没有持仓/.test(d.detail))) {
    hints.push('「台账之外的持仓」不是本程序开的（手动或其它程序）。它在的时候这个币的对账不会干净，这个币的实盘开仓会被拦住、规则不执行：平掉它，或给本程序单独用一个子账户 / API key。');
  }
  if (divergences.some((d) => d.kind === 'unverified')) {
    hints.push('「未核对」是查询失败（限频或交易所临时故障），不代表有问题也不代表没问题；稍后点「刷新实盘账户」重试。');
  }
  return `<div class="alert error">对账有 ${divergences.length} 处不一致：<ul class="round-list">${divergences
    .map((d) => `<li><b>${esc(d.venue || '—')}</b> · ${esc(DIVERGENCE_LABEL[d.kind] || d.kind)}：${esc(d.detail.replace(/\*\*/g, ''))}</li>`)
    .join('')}</ul>${hints.map((h) => `<div class="small">${esc(h)}</div>`).join('')}</div>`;
}

const DIVERGENCE_LABEL = {
  position_missing_on_venue: '场所上没有台账里的仓位',
  naked_leg: '只有一条腿',
  unknown_open_order: '交易所有台账不知道的挂单',
  position_mismatch: '持仓与台账不符',
  order_missing_on_venue: '台账里的挂单交易所上没有',
  unverified: '未核对',
};

function previewHtml(preview) {
  const p = preview.prepared;
  const legs = [p.plan.long, p.plan.short];
  const rows = legs
    .map(
      (leg) => `<tr>
        <td>${esc(leg.venue)}</td>
        <td class="${leg.side === 'buy' ? 'long-c' : 'short-c'}">${leg.side === 'buy' ? '买入' : '卖出'}</td>
        <td class="num">${usd(leg.notional_usdt)}</td>
        <td class="num">${price(leg.limit_price)}</td>
        <td class="num">${price(leg.expected_price)}</td>
        <td class="num">${pct(leg.slippage, 4).replace('+', '')}</td>
      </tr>`,
    )
    .join('');
  const capped = num(p.leverage) < num(p.requested_leverage)
    ? `<div class="alert warn">请求 ${num(p.requested_leverage)}x 超过两腿共同上限，按 ${num(p.leverage)}x 开仓。</div>`
    : '';
  return `
    <table class="trade-legs">
      <thead><tr><th>场所</th><th>方向</th><th class="num">名义</th><th class="num">限价</th><th class="num">预估均价</th><th class="num">滑点</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="kv"><span>预期成本（两腿相对中间价）</span><span>${pct(p.plan.expected_cost, 4).replace('+', '')}</span></div>
    ${p.spread
      ? `<div class="kv"><span>可成交价差（买一卖一 / 吃完深度）</span><span>${pctRaw(p.spread.top_basis_pct, 3)} / ${pctRaw(p.spread.depth_basis_pct, 3)}</span></div>
        ${p.spread.target_net != null
          ? `<div class="kv"><span>收敛到目标 ${num(p.spread.target_basis_pct)}% 平仓时净收益（按现拉盘口）</span><span class="${cls(p.spread.target_net)}">${pct(p.spread.target_net, 3)}（≈ ${pnlUsd(num(p.spread.target_net) * num(p.size_usdt))}）</span></div>
        <div class="kv"><span>保本目标</span><span>低于 ${pctRaw(p.spread.break_even_target_pct, 3)}</span></div>`
          : ''}
        <div class="kv"><span>收敛到 0 时净收益（按现拉盘口）</span><span class="${cls(p.spread.depth_net)}">${pct(p.spread.depth_net, 3)}（≈ ${pnlUsd(num(p.spread.depth_net) * num(p.size_usdt))}）</span></div>`
      : ''}
    ${p.stability
      ? `<div class="kv"><span title="回看两腿最近的逐小时资金费：24 小时均值为正、至少 60% 的小时为正、最近 6 小时没有反转才算稳定">费差稳定性（近 24 小时）</span><span class="${p.stability.stable ? 'pos' : 'neg'}">${p.stability.stable ? '稳定' : '不稳'} · 均值年化 ${pctRaw(num(p.stability.mean_apr_24h) * 100, 1)} · ${pctRaw(num(p.stability.positive_share) * 100, 0)} 的小时为正${p.stability.mean_apr_6h == null ? '' : ` · 近 6 小时 ${pctRaw(num(p.stability.mean_apr_6h) * 100, 1)}`}</span></div>`
      : p.strategy === 'funding' ? '<div class="kv"><span>费差稳定性</span><span class="muted">这两家里有场所没接入资金费历史，未核对</span></div>' : ''}
    <div class="kv"><span>杠杆（两腿相同）</span><span>${num(p.leverage)}x</span></div>
    <div class="kv"><span>两腿里更近的强平距离</span><span>${healthText(p.risk.liq_distance_pct, p.risk.health)}</span></div>
    ${p.spread ? '' : `<div class="kv"><span>摊费后净年化</span><span class="${cls(p.opportunity.funding_apr)}">${pct(p.opportunity.funding_apr, 2)}</span></div>`}
    <div class="kv"><span>保证金模式</span><span>${marginModeLabel(p.margin_mode)}</span></div>
    ${p.margin_mode === 'cross' ? marginModeNote([p.opportunity.long, p.opportunity.short], { marginMode: 'cross' }) : ''}
    <div class="kv"><span>规则</span><span>${rulesText(p.rules, p.margin_mode)}</span></div>
    ${capped}
    ${(preview.warnings || []).map((w) => `<div class="alert warn">${esc(w)}</div>`).join('')}
    ${preview.phases_ms ? `<div class="small muted" title="实盘预览会走和下单一样的前置步骤（只读）；嫌慢时看哪一步最长">这次预览用时 ${secondsText(preview.phases_ms.total)}：对账 ${secondsText(preview.phases_ms.reconcile)} · 现扫 ${secondsText(preview.phases_ms.scan)} · 盘口与保证金 ${secondsText(preview.phases_ms.prepare_and_collateral)}</div>` : ''}
    ${reconciliationText(preview.reconciliation)}`;
}

// 开仓执行报告：下单时的计划与实际成交的对照。偏差按「不利为正」，所以正数是坏事（红）、负数是好事。
function adverseCls(value) {
  const parsed = num(value);
  if (parsed === null || Math.abs(parsed) < 0.00005) return 'dim';
  return parsed > 0 ? 'neg' : 'pos';
}

function secondsText(ms) {
  const parsed = num(ms);
  return parsed === null ? '—' : `${(parsed / 1000).toFixed(1)} 秒`;
}

function openReportHtml(report) {
  if (!report) return '';
  const row = (leg, label) => `<tr>
      <td>${label}</td>
      <td>${esc(leg.venue)}</td>
      <td class="${leg.side === 'buy' ? 'long-c' : 'short-c'}">${leg.side === 'buy' ? '买入' : '卖出'}</td>
      <td class="num">${price(leg.best_price)}</td>
      <td class="num">${price(leg.expected_price)}</td>
      <td class="num"><b>${price(leg.actual_price)}</b></td>
      <td class="num ${adverseCls(leg.vs_expected)}">${pct(leg.vs_expected, 3)}</td>
      <td class="num ${adverseCls(leg.vs_best)}">${pct(leg.vs_best, 3)}</td>
      <td class="num">${leg.quote_to_fill_ms == null ? '—' : secondsText(leg.quote_to_fill_ms)}</td>
    </tr>`;
  const usdAdverse = (value) => `<span class="${adverseCls(value)}">${pnlUsd(-num(value))}</span>`;
  return `<div class="open-report">
    <table class="trade-legs">
      <thead><tr><th>顺序</th><th>场所</th><th>方向</th><th class="num">下单时最优价</th><th class="num">预估均价</th><th class="num">实际均价</th><th class="num" title="正 = 比预估差">比预估</th><th class="num" title="正 = 比最优价差">比最优价</th><th class="num" title="从取盘口到这条腿成交">报价→成交</th></tr></thead>
      <tbody>${row(report.first, '先')}${row(report.second, '后')}</tbody>
    </table>
    <div class="kv"><span title="(空腿价 − 多腿价) / 多腿价">锁定价差：最优价 / 预估 / 实际</span><span>${pctRaw(num(report.best_basis) * 100, 3)} / ${pctRaw(num(report.expected_basis) * 100, 3)} / <b>${pctRaw(num(report.actual_basis) * 100, 3)}</b></span></div>
    <div class="kv"><span title="按两腿实际成交名义折算；正数 = 实际比参考价更划算">两腿合计相对预估 / 相对最优价</span><span>${usdAdverse(report.vs_expected_usdt)} / ${usdAdverse(report.vs_best_usdt)}</span></div>
    <div class="kv"><span>开仓手续费</span><span>${usd(report.fees_usdt, 2)}</span></div>
    <div class="kv"><span title="第一腿成交到第二腿成交：这段时间里手上只有一条腿">两腿间隔（只有一条腿的窗口）</span><span>${secondsText(report.unhedged_ms)}</span></div>
    <div class="kv"><span>执行器总耗时</span><span>${secondsText(report.total_ms)}</span></div>
    <div class="small muted">预估均价 = 下单前按这笔名义吃盘口算出的均价；盘口只有一档很薄时，吃穿的几档已经算在预估里。</div>
  </div>`;
}

function resultHtml(result) {
  const position = result.position;
  const hedged = position.status === 'open';
  const naked = position.status !== 'open' && ((position.long && !position.short) || (!position.long && position.short));
  const tone = hedged ? 'info' : 'warn';
  const note = position.note ? `（${esc(position.note)}）` : '';
  let html = `<div class="alert ${tone}"><b>${esc(position.id)}</b>：${STATUS_LABEL[position.status] || esc(position.status)}${note}</div>`;
  if (naked) {
    html += '<div class="alert error">仍有单腿敞口：去「持仓」页对这笔仓位点「重试退出」，不要重新开仓。</div>';
  }
  if (position.open_report) html += `<details class="open-report-box" open><summary>开仓执行：预估 vs 实际</summary>${openReportHtml(position.open_report)}</details>`;
  if (result.phases_ms) {
    const ph = result.phases_ms;
    html += `<div class="small muted">点击到出结果 ${secondsText(ph.total)}：对账 ${secondsText(ph.reconcile)} · 现扫 ${secondsText(ph.scan)} · 盘口与保证金 ${secondsText(ph.prepare_and_collateral)} · 下单 ${secondsText(ph.execute)}</div>`;
  }
  if (result.reconciliation) html += reconciliationText(result.reconciliation);
  if (result.reconciliation_error) html += `<div class="alert error">下单后对账失败：${esc(result.reconciliation_error)}</div>`;
  html += `<div class="plan-actions"><button type="button" class="btn ghost" id="trade-goto">去持仓页查看 →</button></div>`;
  return html;
}

function renderTradeBox() {
  const box = $('trade-box');
  if (!box) return;
  const s = state.strategy;
  const cfg = state.tradeConfig;
  const t = state.trade;
  if (t.done) {
    const success = !t.busy && tradeSucceeded(t.result);
    box.innerHTML = `<div class="trade-box">
      <div class="row-head"><h3>上一次${state.mode === 'live' ? '实盘' : '纸面'}开仓</h3></div>
      ${t.busy ? '<div class="alert info">下单处理中，请等待结果；不要重复提交。</div>' : t.result ? resultHtml(t.result) : `<div class="alert error">下单未成功或结果未知：${esc(t.error?.error || '未取得结果')}</div>${reconciliationText(t.error?.reconciliation)}<div class="plan-actions"><button type="button" class="btn ghost" id="trade-goto">去持仓页核对 →</button></div>`}
      ${t.busy ? '' : success
        ? '<p class="muted small">本笔已成功建立双腿仓位。若要新开另一笔，请显式重置，然后按最新参数重新预览。</p><div class="plan-actions"><button type="button" class="btn ghost" id="trade-new">再开一笔</button></div>'
        : '<div class="alert warn">开仓流程已锁定：先去持仓页查看台账并对账，确认是否成交及是否有单腿敞口。不要重发；修改参数或切换模式不会解除本次提交锁。</div>'}
    </div>`;
    $('trade-goto')?.addEventListener('click', () => showPage('positions'));
    $('trade-new')?.addEventListener('click', startAnotherTrade);
    return;
  }
  if (!cfg || !s.selected?.long || !s.selected?.short) {
    box.innerHTML = '';
    return;
  }
  if (!cfg.auth_configured) {
    box.innerHTML = `<div class="trade-box"><div class="row-head"><h3>下单</h3></div>
      <div class="alert info">服务端没有配置 <code>ARB_WEB_TOKEN</code>，交易接口已关闭。设置令牌并重启 <code>arb-web</code> 后，可以在这里预览并下单。</div></div>`;
    return;
  }
  const live = cfg.live;
  const liveMode = state.mode === 'live';
  const base = s.selected.symbol.split('/')[0];
  const parts = [];
  parts.push(`<div class="row-head"><h3>下单</h3><span class="grow"></span>
    <span class="tag ${liveMode ? 'coral' : ''}" title="在右上角「令牌」里切换">${liveMode ? '实盘' : '纸面'}</span></div>`);
  // 选了实盘但这对腿下不了：说清楚原因，不悄悄换成纸面。
  if (liveMode && !liveUsable()) {
    parts.push(
      !live
        ? '<div class="alert warn">当前是<b>实盘</b>模式，但看板没有开启实盘（<code>ARB_WEB_LIVE=off</code>），不能预览或下单。要纸面下单，在右上角「令牌」里切到纸面。</div>'
        : `<div class="alert warn">当前是<b>实盘</b>模式，但实盘只连接了 ${esc(live.venues.join('、'))}，这对腿（${esc(s.selected.long)} / ${esc(s.selected.short)}）不在里面。换一对腿，或在右上角「令牌」里切到纸面。</div>`,
    );
    box.innerHTML = `<div class="trade-box">${parts.join('')}</div>`;
    return;
  }
  const blocked = tradeBlocked(selectedRow());
  if (blocked) {
    parts.push(`<div class="alert warn"><b>不能下单：${esc(blocked.title)}</b><br />${esc(blocked.detail)}</div>`);
    box.innerHTML = `<div class="trade-box">${parts.join('')}</div>`;
    return;
  }
  if (liveMode) {
    parts.push(
      live.mode === 'trade'
        ? `<div class="alert error"><b>实盘</b>：会用真实资金在 ${esc(s.selected.long)} / ${esc(s.selected.short)} 下单。开仓前服务端会先对账、现扫一轮行情；杠杆必须是整数，超过两腿上限直接拒绝。价格保护 ${esc(live.market_slippage)}。</div>`
        : '<div class="alert warn"><b>实盘只读</b>（ARB_WEB_LIVE=readonly）：可以预览计划，不能下单。</div>',
    );
    // 当日已实现盈亏：留空时服务端用台账算出的值（只算看板台账里今天结束的仓位）。
    ensureDaily();
    const daily = t.daily;
    const known = daily?.daily?.net_usdt != null;
    const hint = !daily
      ? '正在读台账…'
      : daily.error
        ? `台账读不出来：${esc(daily.error)}，请手填。`
        : known
          ? `留空 = 用台账算出的今天（UTC）已实现盈亏 <b class="${cls(daily.daily.net_usdt)}">${pnlUsd(daily.daily.net_usdt)}</b>（今天结束 ${daily.daily.closed} 笔；价格 − 手续费 + 资金费）。只算看板台账里的平仓，交易所里手动交易不在内；亏到 −${num(daily.max_daily_loss_usdt)} USDT 会拒绝开新仓。`
          : `台账里今天结束的 ${daily.daily.closed} 笔仓位有 ${daily.daily.unknown.length} 笔没有盈亏记录，算不出合计 —— <b>必须手填</b>（不知道的不按 0 算）。`;
    parts.push(`<label class="field"><span>当日已实现盈亏（USDT，亏损为负${known ? '，可留空' : '，必填'}）</span>
      <input id="trade-pnl" type="number" step="1" value="${esc(t.dailyPnl)}" placeholder="${known ? '留空用台账的值' : '例如 0 或 -35'}" /></label>
      <p class="muted small" id="daily-hint">${hint}</p>`);
  } else {
    parts.push('<p class="muted small">纸面：用真实盘口逐档吃单算成交价与手续费，写进纸面台账，不碰真实资金。</p>');
  }
  if (!token()) {
    parts.push('<div class="alert warn">还没有填令牌：点右上角「令牌」。</div>');
  }
  parts.push(`<div class="plan-actions"><button type="button" class="btn ghost" id="trade-preview"${t.busy || !token() ? ' disabled' : ''}>${t.busy && !t.done ? '核价中…' : '预览下单计划'}</button></div>`);

  if (t.error) {
    parts.push(`<div class="alert error">不能下单：${esc(t.error.error || '未知错误')}</div>`);
    if (t.error.reconciliation) parts.push(reconciliationText(t.error.reconciliation));
  }
  if (t.preview && !t.result) {
    parts.push(previewHtml(t.preview));
    const canTrade = !liveMode || live?.mode === 'trade';
    if (!canTrade) {
      // 只读：只有计划。
    } else if (!liveMode) {
      parts.push(`<div class="plan-actions"><button type="button" class="btn primary" id="trade-exec"${t.busy || t.done ? ' disabled' : ''}>${t.busy ? '下单中…' : '按此计划纸面开仓'}</button></div>`);
    } else {
      const matched = t.confirm.trim().toUpperCase() === base.toUpperCase();
      parts.push(`<div class="confirm-row">
        <input id="trade-confirm" autocomplete="off" placeholder="输入 ${esc(base)} 确认实盘下单" value="${esc(t.confirm)}"${t.done ? ' disabled' : ''} />
        <button type="button" class="btn danger" id="trade-exec"${!matched || t.busy || t.done ? ' disabled' : ''}>${t.busy ? '下单中…' : '实盘下单'}</button>
      </div>`);
    }
    parts.push('<p class="muted small">下单时服务端会按最新盘口重新核价、重新过闸门，成交价可能与预览不同；过不了闸门就不会下单。</p>');
  }

  box.innerHTML = `<div class="trade-box">${parts.join('')}</div>`;

  $('trade-pnl')?.addEventListener('input', (event) => {
    t.dailyPnl = event.target.value;
    if (t.done) return;
    t.seq += 1;
    t.preview = null;
    t.error = null;
    t.confirm = '';
    if ($('trade-exec')) $('trade-exec').disabled = true;
    if ($('trade-confirm')) $('trade-confirm').value = '';
  });
  $('trade-pnl')?.addEventListener('change', () => {
    // 当日盈亏会进闸门：改了它，旧预览就不再代表这次下单，要重新预览。
    if (!t.busy && !t.done) renderTradeBox();
  });
  $('trade-preview')?.addEventListener('click', previewTrade);
  $('trade-exec')?.addEventListener('click', executeTrade);
  $('trade-confirm')?.addEventListener('input', (event) => {
    t.confirm = event.target.value;
    const matched = t.confirm.trim().toUpperCase() === base.toUpperCase();
    $('trade-exec').disabled = !matched || t.busy || t.done;
  });
  $('trade-goto')?.addEventListener('click', () => showPage('positions'));
}

// ───────────────────────────── 持仓页 ─────────────────────────────

const ACTION = {
  hold: { cls: 'hold', label: '保持' },
  close: { cls: 'close', label: '建议平仓' },
  trim: { cls: 'trim', label: '建议减仓' },
  add_margin: { cls: 'trim', label: '建议追加保证金' },
};
const STATUS_LABEL = {
  opening: '建仓中',
  open: '持仓中',
  unwinding: '回滚中',
  unwound: '已回滚',
  closing: '平仓中',
  closed: '已平仓',
};

function rulesText(rules, mode = 'isolated') {
  const parts = [];
  if (rules?.min_funding_apr != null) parts.push(`费差 < ${pct(rules.min_funding_apr, 2).replace('+', '')} 平仓`);
  if (rules?.liq_protection_pct != null) parts.push(`强平距离 < ${num(rules.liq_protection_pct)}% ${mode === 'cross' ? '整笔退出（全仓）' : '减仓'}`);
  if (rules?.size_mismatch_pct != null) parts.push(`数量偏差 > ${num(rules.size_mismatch_pct)}% 平仓`);
  if (rules?.basis_exit_pct != null) parts.push(`基差收敛到 ≤ ${num(rules.basis_exit_pct)}% 平仓`);
  if (rules?.take_profit_usdt != null) parts.push(`含资金费净收益 ≥ ${usd(rules.take_profit_usdt)} 止盈`);
  if (rules?.take_profit_book_min_usdt != null) parts.push(num(rules.take_profit_book_min_usdt) === 0 ? '止盈触发后盘口为正即平' : `止盈触发后盘口 ≥ ${usd(rules.take_profit_book_min_usdt)} 即平`);
  if (rules?.auto_margin_pct != null) parts.push(`强平距离 < ${num(rules.auto_margin_pct)}% 追加逐仓保证金，累计上限 ${usd(rules.auto_margin_max_usdt)}`);
  if (rules?.stop_loss_usdt != null) parts.push(`按盘口亏到 −${usd(rules.stop_loss_usdt)} 止损`);
  if (rules?.timeout_hours != null) parts.push(`持仓超过 ${num(rules.timeout_hours)} 小时、按盘口亏损 ≤ ${usd(rules.timeout_max_loss_usdt)} 就平仓`);
  return parts.length ? parts.map((p) => `<span class="tag sky">${esc(p)}</span>`).join('') : '<span class="muted">无规则</span>';
}

const ruleDraftKey = (id) => `${state.mode}:${id}`;
const canEditRules = () => Boolean(state.tradeConfig?.auth_configured && token() && (state.mode === 'paper' || state.tradeConfig?.live?.mode === 'trade'));

// Decimal 字符串移两位转百分数，避免未改的费差阈值因二进制浮点回填而变化。
function ruleAprPercent(value) {
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(String(value));
  if (!match) return String(num(value) * 100);
  const [, sign, whole, fraction = ''] = match;
  const padded = fraction.padEnd(2, '0');
  const integer = `${whole}${padded.slice(0, 2)}`.replace(/^0+(?=\d)/, '');
  const rest = padded.slice(2).replace(/0+$/, '');
  return `${sign}${integer}${rest ? `.${rest}` : ''}`;
}

const rulePreferenceKey = (id) => `arb-web-position-rule-values:${ruleDraftKey(id)}`;

// 本地只记阈值，不记开关；是否启用始终以台账为准，不自动恢复已关闭的保护。
function positionRuleForm(position) {
  let remembered = {};
  try {
    remembered = JSON.parse(localStorage.getItem(rulePreferenceKey(position.id)) || '{}') || {};
  } catch {
    // 本地存储不可用时仍可查看、修改和保存服务端规则。
  }
  const text = (value, fallback) => typeof value === 'string' && value.trim() ? value : fallback;
  const form = { marginMode: position.margin_mode || 'isolated' };
  for (const def of RULE_FIELDS) {
    const value = position.rules?.[def.source];
    form[def.key] = {
      on: value != null || (def.pair != null && position.rules?.[def.pair.source] != null),
      value: value == null ? text(remembered[def.key]?.value, def.value) : def.key === 'minApr' ? ruleAprPercent(value) : String(value),
    };
    if (def.pair) form[def.key].max = position.rules?.[def.pair.source] == null
      ? text(remembered[def.key]?.max, def.pair.value) : String(position.rules[def.pair.source]);
  }
  return form;
}

function rememberPositionRuleValues(id, form) {
  const values = {};
  for (const def of RULE_FIELDS) {
    values[def.key] = { value: form[def.key].value };
    if (def.pair) values[def.key].max = form[def.key].max;
  }
  try {
    localStorage.setItem(rulePreferenceKey(id), JSON.stringify(values));
  } catch {
    // 阈值记忆是便利功能，不影响台账保存结果。
  }
}

function positionRulesEditor(position) {
  if (position.status !== 'open' || !position.long || !position.short) return '';
  const savedDraft = state.pos.ruleDrafts.get(ruleDraftKey(position.id));
  const draft = savedDraft || { form: positionRuleForm(position) };
  const allowed = canEditRules();
  return `<section class="pos-rules" data-rule-editor="${esc(position.id)}" aria-label="${esc(position.id)} 持仓规则">
    <h3>持仓保护与自动平仓 <span class="muted small" data-rule-dirty>${savedDraft ? '有未保存修改' : '当前已保存设置'}</span></h3>
    <p class="muted small">持仓期间可随时开启、关闭或调整阈值，点击「保存规则」后生效。关闭后本浏览器记住上次保存的阈值；保存后后台下一轮可能平仓、减仓或追加保证金。</p>
    <fieldset class="rules"${!allowed || state.pos.busy ? ' disabled' : ''}>
      ${RULE_FIELDS.map((def) => ruleControl(def, draft.form[def.key], `pos-${encodeURIComponent(position.id)}-${def.id}`, true, position.margin_mode)).join('')}
    </fieldset>
    ${position.margin_mode === 'cross' ? marginModeNote([position.long.venue, position.short.venue], draft.form) : ''}
    <div data-rule-margin-note>${autoMarginNote([position.long.venue, position.short.venue], draft.form.autoMargin.on)}</div>
    <div data-rule-feedback role="status">
      ${draft.error ? `<div class="alert error">${esc(draft.error)}</div>` : ''}
      ${draft.wouldTrigger ? `<div class="alert warn"><b>保存后会立即满足触发条件：</b>${esc(draft.wouldTrigger)}
        <label class="force-rule"><input type="checkbox" data-rule-force${draft.force ? ' checked' : ''}${!allowed || state.pos.busy ? ' disabled' : ''} /> 我已了解可能立即执行，仍然保存</label>
      </div>` : ''}
    </div>
    ${allowed ? '' : `<div class="alert warn">当前仅可查看：修改需要令牌，且实盘必须为可交易模式。${savedDraft ? '草稿已保留。' : ''}</div>`}
    <div class="pos-actions">
      <button type="button" class="btn ${draft.wouldTrigger ? 'danger' : 'primary'}" data-save-rules="${esc(position.id)}"${!savedDraft || !allowed || state.pos.busy || (draft.wouldTrigger && !draft.force) ? ' disabled' : ''}>${state.pos.busy ? '操作中…' : draft.wouldTrigger ? '仍然保存规则' : '保存规则'}</button>
      <button type="button" class="btn ghost" data-cancel-rules="${esc(position.id)}"${!savedDraft || state.pos.busy ? ' disabled' : ''}>撤销修改</button>
    </div>
  </section>`;
}

async function savePositionRules(id) {
  const p = state.pos;
  const key = ruleDraftKey(id);
  const draft = p.ruleDrafts.get(key);
  const position = state.positions?.open?.find((position) => position.id === id);
  if (!draft || !canEditRules() || p.busy || position?.status !== 'open' || !position.long || !position.short) return;
  if (draft.wouldTrigger && !draft.force) return;
  draft.error = ruleFormError(draft.form);
  if (draft.error) {
    renderPositions();
    return;
  }
  const mode = state.mode;
  const modeSeq = p.modeSeq;
  const force = Boolean(draft.wouldTrigger && draft.force);
  p.busy = true;
  p.loadSeq += 1;
  renderPositions();
  const response = await api('/api/trade/rules', {
    method: 'POST',
    body: { mode, position_id: id, ...ruleFields(draft.form), force },
    auth: true,
  });
  p.busy = false;
  if (mode !== state.mode || modeSeq !== p.modeSeq || p.ruleDrafts.get(key) !== draft) return;
  draft.force = false;
  draft.wouldTrigger = null;
  if (!response.ok) {
    draft.error = response.body.error || '保存失败';
    if (response.status === 409 && response.body.would_trigger) {
      draft.wouldTrigger = response.body.would_trigger;
    } else if (response.status === 0 || response.status >= 500) {
      draft.error += '；保存结果可能未知，请刷新持仓核对当前规则。';
    }
    renderPositions();
    return;
  }
  rememberPositionRuleValues(id, draft.form);
  p.ruleDrafts.delete(key);
  const updated = response.body.position;
  if (updated?.id === id) Object.assign(position, updated);
  p.flash = { error: false, html: `<b>${esc(id)}</b> ${response.body.changed ? '整套规则已保存' : '规则未改变'}${response.body.forced ? '（已明确确认即时触发）' : ''}。` };
  renderPositions();
  await loadPositions().catch((error) => {
    if (mode !== state.mode || modeSeq !== p.modeSeq) return;
    p.flash = { error: true, html: `规则已保存，但刷新失败：${esc(error.message)}。请刷新持仓核对。` };
    renderFlash();
  });
}

function wirePositionRules() {
  const box = $('pos-open');
  for (const editor of box.querySelectorAll('[data-rule-editor]')) {
    const id = editor.dataset.ruleEditor;
    const position = state.positions.open.find((position) => position.id === id);
    let draft = state.pos.ruleDrafts.get(ruleDraftKey(id));
    for (const input of editor.querySelectorAll('[data-rule-field]')) {
      input.addEventListener('input', () => {
        if (!canEditRules() || state.pos.busy) return;
        if (!draft) {
          draft = { form: positionRuleForm(position), error: null, wouldTrigger: null, force: false };
          state.pos.ruleDrafts.set(ruleDraftKey(id), draft);
        }
        const rule = draft.form[input.dataset.ruleField];
        rule[input.dataset.rulePart] = input.dataset.rulePart === 'on' ? input.checked : input.value;
        draft.error = null;
        draft.wouldTrigger = null;
        draft.force = false;
        editor.querySelector('[data-rule-feedback]').innerHTML = '';
        editor.querySelector('[data-rule-dirty]').textContent = '有未保存修改';
        editor.querySelector('[data-cancel-rules]').disabled = false;
        for (const badge of editor.querySelectorAll('[data-rule-state]')) {
          const on = draft.form[badge.dataset.ruleState].on;
          badge.textContent = on ? '已启用（待保存）' : '已关闭（待保存）';
          badge.className = `tag${on ? ' sky' : ''}`;
        }
        const save = editor.querySelector('[data-save-rules]');
        save.disabled = false;
        save.textContent = '保存规则';
        save.className = 'btn primary';
        for (const field of editor.querySelectorAll('[data-rule-field]')) {
          if (field.dataset.rulePart !== 'on') field.disabled = !draft.form[field.dataset.ruleField].on;
        }
        editor.querySelector('[data-rule-margin-note]').innerHTML = autoMarginNote([position.long.venue, position.short.venue], draft.form.autoMargin.on);
      });
    }
    editor.querySelector('[data-rule-force]')?.addEventListener('change', (event) => {
      draft.force = event.target.checked;
      editor.querySelector('[data-save-rules]').disabled = !draft.force || !canEditRules() || state.pos.busy;
    });
    editor.querySelector('[data-save-rules]').addEventListener('click', () => savePositionRules(id));
    editor.querySelector('[data-cancel-rules]').addEventListener('click', () => {
      if (state.pos.busy) return;
      state.pos.ruleDrafts.delete(ruleDraftKey(id));
      renderPositions();
    });
  }
}

// 看的是实盘、但看板没开实盘：没有台账可看，改为展示交易所账户识别。
// 实盘开着但暂时没连上（live_pending）不算「没开」：页面显示横幅，持仓接口返回 503 原因。
const liveSetup = () => state.mode === 'live' && !state.tradeConfig?.live && !state.tradeConfig?.live_pending;

// 实盘开着但交易所账户暂时连不上：每个页面顶部都显示，直到后台重连成功。
function livePendingText(pending) {
  if (!pending) return '';
  const since = when(pending.since);
  return `⚠️ 实盘账户暂时连不上（${esc(since)} 起，已重试 ${Number(pending.attempts) || 0} 次）：实盘下单、持仓规则与对账暂停，后台每分钟自动重连。行情、价差监控、纸面不受影响。<span class="muted small" title="${esc(pending.error || '')}">原因：${esc(String(pending.error || '').slice(0, 140))}</span>`;
}

function renderLivePending() {
  const el = $('live-pending');
  if (!el) return;
  const pending = state.tradeConfig?.live_pending;
  el.classList.toggle('hidden', !pending);
  el.innerHTML = livePendingText(pending);
}

async function loadCredentials() {
  const { ok, body } = await api('/api/trade/accounts', { auth: true });
  return ok ? body : { error: body.error };
}

// 「最近结束」每页几笔：多了整页会拖得很长。
const CLOSED_PER_PAGE = 10;

// 「最近结束」的翻页条：一页放得下就不显示。
function closedPagerHtml(data) {
  const total = data?.closed_total || 0;
  const per = data?.closed_per_page || CLOSED_PER_PAGE;
  const pages = Math.max(1, Math.ceil(total / per));
  if (pages <= 1) return '';
  const page = Math.min(Math.max(1, data.closed_page || 1), pages);
  const from = (page - 1) * per + 1;
  const to = Math.min(total, page * per);
  return `<button class="btn ghost" type="button" data-closed-page="${page - 1}"${page <= 1 ? ' disabled' : ''}>上一页</button>
    <span class="small">第 ${page} / ${pages} 页 · 第 ${from}–${to} 笔，共 ${total} 笔</span>
    <button class="btn ghost" type="button" data-closed-page="${page + 1}"${page >= pages ? ' disabled' : ''}>下一页</button>`;
}

// 正在进行的仓位（建仓中 / 平仓中 / 回滚中）。币安只挂单那几秒，开仓会以「建仓中、没有腿」出现在持仓里：
// 说清楚在等什么、等了多久，正常几秒就结束；等太久才提醒可能卡住了，和真的卡住分得开。
const IN_FLIGHT = new Set(['opening', 'closing', 'unwinding']);
const IN_FLIGHT_STUCK_SEC = 60;
const IN_FLIGHT_REFRESH_MS = 3000;

function inFlightNotice(position, nowMs = Date.now()) {
  if (position.status !== 'opening' || position.long || position.short) return null;
  const opened = Date.parse(position.opened_at);
  const sec = Number.isFinite(opened) ? Math.max(0, Math.round((nowMs - opened) / 1000)) : null;
  if (sec !== null && sec >= IN_FLIGHT_STUCK_SEC) {
    return {
      tone: 'warn',
      text: `建仓已经 ${sec} 秒还没结束，可能卡住了：看下面的备注与对账；第一腿是否成交以交易所为准。`,
    };
  }
  return {
    tone: 'info',
    text: `正在建仓：第一腿已下单，等交易所成交（只挂单最多挂 20 秒，没成交会自动撤掉、不留仓位）${sec === null ? '' : `，已 ${sec} 秒`}。`,
  };
}

// 有进行中的仓位时，3 秒后再拉一次持仓：它结束后很快从列表里消失，不用等 30 秒的自动刷新。
function scheduleInFlightRefresh() {
  const p = state.pos;
  clearTimeout(p.inFlightTimer);
  const inFlight = (state.positions?.open || []).some((position) => IN_FLIGHT.has(position.status));
  if (!inFlight || state.page !== 'positions') return;
  p.inFlightTimer = setTimeout(() => {
    if (document.hidden || state.page !== 'positions' || p.busy || p.confirming) return;
    loadPositions().catch(() => {});
  }, IN_FLIGHT_REFRESH_MS);
}

async function loadPositions() {
  const p = state.pos;
  const mode = state.mode;
  const seq = ++p.loadSeq;
  const modeSeq = p.modeSeq;
  const live = mode === 'live';
  if (liveSetup()) {
    const credentials = await loadCredentials();
    if (mode !== state.mode || seq !== p.loadSeq || modeSeq !== p.modeSeq) return;
    p.credentials = credentials;
    renderPositions();
    return;
  }
  const [positions, round] = await Promise.all([
    api(`/api/positions?mode=${mode}&closed_page=${p.closedPage}&closed_per_page=${CLOSED_PER_PAGE}`, { auth: live }),
    api(`/api/trade/round?mode=${mode}`, { auth: live }),
  ]);
  if (mode !== state.mode || seq !== p.loadSeq || modeSeq !== p.modeSeq) return;
  if (!positions.ok) throw new Error(positions.body.error);
  state.positions = positions.body;
  // 结束的笔数变少、请求的页超出末页时，后端给的是末页：跟着它走。
  if (positions.body.closed_page) p.closedPage = positions.body.closed_page;
  p.round = round.ok ? round.body : null;
  renderPositions();
  scheduleInFlightRefresh();
}

// 实盘账户（真实持仓 + 对账）要打各家私有接口：进入实盘页、点按钮时立刻拉；停在实盘页时再每 60 秒
// 在后台拉一次（页面不可见、或关掉「自动刷新」就不拉）。比行情的 30 秒慢一倍：Lighter RH 的限频按
// 出口 IP 算、额度很紧，一分钟一轮（每家几次查询）压力很小。
const ACCOUNT_REFRESH_MS = 60000;

async function loadAccount({ quiet = false } = {}) {
  const p = state.pos;
  // 已经有一次在路上：等它回来后再补一次，不并发打私有接口。
  if (p.accountLoading) {
    p.accountAgain = true;
    return;
  }
  p.accountLoading = true;
  p.accountTriedAt = Date.now();
  // 后台刷新保留旧数据，不闪「正在查询」。
  const keep = quiet && p.account && !p.account.loading && !p.account.error;
  if (!keep) {
    p.account = { loading: true };
    renderAccount();
  }
  const [{ ok, body }, credentials] = await Promise.all([
    api('/api/trade/live/status', { auth: true }),
    loadCredentials(),
  ]);
  p.accountLoading = false;
  if (ok) {
    p.account = body;
    p.accountAt = Date.now();
    p.accountStale = null;
  } else if (keep) {
    // 自动刷新失败：继续显示上一次的数据，并注明失败原因与数据时刻。
    p.accountStale = body.error;
  } else {
    p.account = { error: body.error };
  }
  p.credentials = credentials;
  renderAccount();
  if (p.accountAgain) {
    p.accountAgain = false;
    loadAccount({ quiet: true });
  }
}

// 自动刷新时顺带刷新实盘账户：只在实盘页、没有正在确认或执行的操作、且距上次拉取满 60 秒时。
function maybeRefreshAccount() {
  const p = state.pos;
  if (state.mode !== 'live' || !state.tradeConfig?.live || p.busy || p.confirming) return;
  if (Date.now() - (p.accountTriedAt || 0) < ACCOUNT_REFRESH_MS) return;
  loadAccount({ quiet: true });
}

const clockTime = (ms) => new Date(ms).toLocaleTimeString('zh-CN', { hour12: false });

// 各场所的可用保证金（能拿来开新仓的部分）。只显示交易所给的数：没给就说没给，不当成 0。
// 不给跨场所合计：Hyperliquid 统一账户下几个 dex 共用同一笔钱，加起来会重复算。
function accountBalancesHtml(accounts) {
  const list = accounts || [];
  if (!list.length) return '';
  const cell = (a) => {
    if (a.balance_error) return `<span class="neg" title="${esc(a.balance_error)}">没查成</span>`;
    if (a.free_collateral == null) return '<span class="muted">交易所没给</span>';
    return `<b>${usd(a.free_collateral)}</b>`;
  };
  const hl = list.filter((a) => String(a.venue).startsWith('hyperliquid') && num(a.free_collateral) !== null);
  const shared = hl.length > 1 && hl.every((a) => num(a.free_collateral) === num(hl[0].free_collateral));
  const note = shared
    ? '<span class="muted small">Hyperliquid 几个 dex 是同一个数：统一账户共用一笔余额，不要相加。</span>'
    : '';
  return `<div class="pnl-row balance-row" title="交易所报告的可用保证金：能拿来开新仓的部分，不含已经押在持仓上的保证金与浮动盈亏">
    <span class="small">可用保证金（USDT）</span>
    ${list.map((a) => `<span>${esc(a.venue)} ${cell(a)}</span>`).join('')}
    ${note}
  </div>`;
}

function renderAccount() {
  const p = state.pos;
  const box = $('pos-account');
  $('pos-account-btn').classList.toggle('hidden', state.mode !== 'live');
  if (state.mode !== 'live' || !p.account) {
    box.innerHTML = '';
    return;
  }
  const account = p.account;
  if (account.loading) {
    box.innerHTML = '<div class="card account-card muted small">正在查询实盘账户与对账…</div>';
    return;
  }
  if (account.error) {
    box.innerHTML = `<div class="notice error">实盘账户查询失败：${esc(account.error)}</div>`;
    return;
  }
  const rows = (account.accounts || [])
    .flatMap((a) => {
      if (a.error) return [`<tr><td>${esc(a.venue)}</td><td colspan="4" class="neg">查询失败：${esc(a.error)}</td></tr>`];
      if (!a.positions?.length) return [`<tr><td>${esc(a.venue)}</td><td colspan="3" class="muted">无持仓</td><td class="num dim">${pct(a.fee_per_side, 4).replace('+', '')}</td></tr>`];
      return a.positions.map(
        (v) => `<tr>
          <td>${esc(a.venue)}</td>
          <td>${esc(v.symbol)}</td>
          <td class="num ${cls(v.net_quantity)}">${esc(v.net_quantity)}</td>
          <td class="num">${usd(v.notional_usdt)}</td>
          <td class="num dim">${pct(a.fee_per_side, 4).replace('+', '')}</td>
        </tr>`,
      );
    })
    .join('');
  const recon = account.reconciliation
    ? reconciliationText(account.reconciliation)
    : `<div class="alert error">对账失败：${esc(account.reconciliation_error || '未知')}</div>`;
  const venues = (account.venues || []).map(esc).join('、');
  const how = account.auto_venues ? '按凭据自动识别' : '按 ARB_LIVE_VENUES 指定';
  const creds = p.credentials && !p.credentials.error
    ? `<details class="cred-details"><summary>交易所凭据识别</summary>${credentialsTable(p.credentials)}</details>`
    : '';
  const updated = p.accountAt
    ? `更新于 ${clockTime(p.accountAt)}${$('auto').checked ? ' · 每 60 秒自动刷新' : ' · 自动刷新已关'}`
    : '';
  const stale = p.accountStale
    ? `<div class="alert warn">自动刷新失败：${esc(p.accountStale)}。下面仍是 ${clockTime(p.accountAt)} 的数据。</div>`
    : '';
  box.innerHTML = `<section class="card account-card">
    <div class="row-head"><h3>实盘账户</h3><span class="muted small">${account.mode === 'trade' ? '可下单' : '只读'} · ${venues}（${how}）</span><span class="grow"></span><span class="muted small" id="account-updated">${updated}</span></div>
    ${stale}${recon}
    ${accountBalancesHtml(account.accounts)}
    <div class="table-wrap"><table>
      <thead><tr><th>场所</th><th>合约</th><th class="num">净数量</th><th class="num">名义</th><th class="num">吃单费率</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>
    ${creds}
  </section>`;
}

// ───────────────────────────── 交易所账户识别 ─────────────────────────────
//
// 服务端只告诉我们每家「填了哪些变量、缺哪些、格式对不对」，不含任何值。

const CRED_STATE = {
  ready: ['pos', '✓ 已识别'],
  invalid: ['neg', '✗ 格式不对'],
  partial: ['h-caution', '⚠ 没填全'],
  missing: ['muted', '未配置'],
};
const CRED_ORDER = { ready: 0, invalid: 1, partial: 2, missing: 3 };
const varList = (vars) => (vars || []).map((v) => `<code>${esc(v)}</code>`).join('、');

function credentialsTable(data) {
  const connected = data.live?.venues || [];
  const planned = data.selection?.venues || [];
  // 自动与否看设置本身：只识别出一家时 selection 是个错误，里面没有 auto。
  const auto = Boolean(data.auto);
  const rows = [...(data.accounts || [])]
    .sort((a, b) => (CRED_ORDER[a.state] ?? 9) - (CRED_ORDER[b.state] ?? 9))
    .map((a) => {
      const [c, label] = CRED_STATE[a.state] || ['', esc(a.state)];
      let note;
      if (a.state === 'invalid') note = `<span class="neg">${(a.problems || []).map(esc).join('；')}</span>`;
      else if (a.state === 'partial') note = `缺 ${varList(a.missing)}`;
      else if (a.state === 'missing') note = `<span class="muted">需要 ${varList(a.vars)}</span>`;
      else if (connected.includes(a.venue)) note = '<span class="pos">实盘已连接</span>';
      else if (planned.includes(a.venue)) note = '开启实盘时会连接';
      else if (!auto) note = '<span class="muted">没列在 ARB_LIVE_VENUES 里</span>';
      else note = '<span class="h-caution">会被自动选入；还差一家凭据齐全的交易所</span>';
      return `<tr><td>${esc(a.venue)}</td><td class="${c}">${label}</td><td class="small wrap">${note}</td></tr>`;
    })
    .join('');
  return `<div class="table-wrap"><table class="cred-table">
    <thead><tr><th>交易所</th><th>凭据</th><th>说明</th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

function setupHtml(data) {
  if (!data) return '<div class="card account-card muted small">正在识别交易所账户…</div>';
  if (data.error) return `<div class="notice error">交易所账户识别失败：${esc(data.error)}</div>`;
  const sel = data.selection || {};
  const states = Object.fromEntries((data.accounts || []).map((a) => [a.venue, a.state]));
  const how = data.auto ? '按凭据自动识别' : `按 <code>ARB_LIVE_VENUES=${esc(data.venues_setting)}</code> 指定`;
  const broken = (sel.venues || []).filter((v) => states[v] !== 'ready');
  let summary;
  if (sel.error) {
    summary = `<div class="alert warn">${esc(sel.error)}</div>`;
  } else if (broken.length) {
    summary = `<div class="alert warn">${how}了 ${sel.venues.map(esc).join('、')}，但 <b>${broken.map(esc).join('、')}</b> 的凭据还没填全或格式不对：现在开启实盘，看板会启动失败。</div>`;
  } else {
    summary = `<div class="alert info">开启实盘时会连接：<b>${sel.venues.map(esc).join('、')}</b>（${how}）。<br />
      下一步：把 <code>.env</code> 里的 <code>ARB_WEB_LIVE</code> 改成 <code>readonly</code>（只连接账户、对账，不下单），执行 <code>pm2 restart arb-web</code>；对账干净后再改成 <code>trade</code>。</div>`;
  }
  const explicit = data.auto
    ? ''
    : '<p class="hint">当前 <code>ARB_LIVE_VENUES</code> 是显式列表；删掉这一行或改成 <code>auto</code>，就按下表自动识别。</p>';
  return `<section class="card account-card">
    <div class="row-head"><h3>交易所账户识别</h3><span class="muted small">按 .env 里填的凭据判断；不显示任何密钥，也不联网</span></div>
    ${summary}${explicit}
    ${credentialsTable(data)}
    <p class="hint">识别读的是看板启动时的环境：改了 <code>.env</code> 之后执行 <code>pm2 restart arb-web</code> 再刷新本页。这里只检查填没填、格式对不对，能不能真的连上要等开启实盘后看「实盘账户」。</p>
  </section>`;
}

function renderRound() {
  const p = state.pos;
  const info = p.round;
  const cfg = state.tradeConfig;
  const live = state.mode === 'live';
  const canRun = cfg?.auth_configured && token() && (!live || cfg?.live?.mode === 'trade');
  $('pos-monitor').disabled = !canRun || p.busy;
  $('pos-monitor').title = canRun ? '立刻按规则评估每笔仓位并执行（平仓 / 减仓 / 追加保证金 / 重试退出）' : '需要令牌；实盘还需要 ARB_WEB_LIVE=trade';
  const auto = info?.watch_sec
    ? `规则每 ${info.watch_sec}s 自动执行一次`
    : live
      ? '<span class="h-caution">实盘规则没有自动执行（ARB_WEB_LIVE_WATCH_SEC=0 或只读）</span>'
      : '纸面规则不在看板里自动执行：用 <code>arb-paper --watch</code>，或点「执行一轮规则」（两者别同时跑）';
  const last = info?.last_round;
  let lastText = '尚未跑过';
  if (last) {
    const executed = (last.reports || []).filter((r) => r.executed).length;
    const failed = (last.reports || []).filter((r) => r.error).length;
    lastText = `上一轮 ${when(last.at)}（${last.trigger === 'auto' ? '自动' : '手动'}）：${(last.reports || []).length} 笔，执行 ${executed} 笔${failed ? `，<span class="neg">失败 ${failed} 笔</span>` : ''}`;
    if (last.error) lastText += `，<span class="neg">${esc(last.error)}</span>`;
  }
  $('pos-round').innerHTML = `${auto} · ${lastText}`;
}

function renderFlash() {
  const flash = state.pos.flash;
  $('pos-flash').innerHTML = flash ? `<div class="notice${flash.error ? ' error' : ''}">${flash.html}</div>` : '';
}

// 平仓 / 补仓的结果提示在持仓区顶部，按钮常在下面好几屏：不滚过去就像「点了没反应」
// （某次实盘 补仓被拒，提示出在屏幕外）。
function showActionFlash() {
  renderFlash();
  const box = $('pos-flash');
  if (box && state.pos.flash) box.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

async function runMonitor() {
  const p = state.pos;
  if (p.busy) return;
  p.busy = true;
  renderRound();
  const { ok, body } = await api('/api/trade/monitor', { method: 'POST', body: { mode: state.mode }, auth: true });
  p.busy = false;
  if (ok) {
    const lines = (body.reports || [])
      .filter((r) => r.executed || r.error || r.skipped)
      .map((r) => {
        const what = r.resumed
          ? '续上中断的建仓'
          : r.retried_exit
          ? '重试退出'
          : r.evaluation?.action?.action === 'close'
            ? '平仓'
            : r.evaluation?.action?.action === 'trim'
              ? '减仓'
              : r.evaluation?.action?.action === 'add_margin'
                ? '追加保证金'
                : '未执行';
        const tail = r.error ? `<span class="neg">失败：${esc(r.error)}</span>` : r.skipped ? esc(r.skipped) : STATUS_LABEL[r.status] || esc(r.status);
        return `<li><b>${esc(r.position_id)}</b> ${esc(r.symbol)} — ${what}：${tail}</li>`;
      });
    p.flash = {
      error: Boolean(body.error) || (body.reports || []).some((r) => r.error),
      html: `规则跑完一轮：${(body.reports || []).length} 笔仓位${body.error ? `；${esc(body.error)}` : ''}${lines.length ? `<ul>${lines.join('')}</ul>` : '，全部保持。'}`,
    };
  } else {
    p.flash = { error: true, html: `执行一轮规则失败：${esc(body.error)}` };
  }
  renderFlash();
  await loadPositions().catch((error) => {
    p.flash = { error: true, html: `刷新持仓失败：${esc(error.message)}` };
    renderFlash();
  });
}

async function closePosition(id) {
  const p = state.pos;
  if (p.busy) return;
  const live = state.mode === 'live';
  if (p.confirming !== id || p.confirmKind !== 'close') return;
  p.busy = true;
  renderPositions();
  // 实盘的二次确认就是页面上那一下「确认平仓」：服务端要求 confirm = 仓位 id，这里自动带上。
  const body = { mode: state.mode, position_id: id };
  if (live) body.confirm = id;
  const response = await api('/api/trade/close', { method: 'POST', body, auth: true });
  p.busy = false;
  p.confirming = null;
  p.confirmKind = null;
  if (response.ok) {
    const position = response.body.position;
    const status = STATUS_LABEL[position.status] || esc(position.status);
    const error = response.body.error;
    p.flash = {
      error: Boolean(error),
      html: error
        ? `<b>${esc(id)}</b> 平仓未完成（${status}）：${esc(error)}。不要反复点：先看对账，再对这笔点「重试退出」。`
        : `<b>${esc(id)}</b> ${status}。${position.note ? esc(position.note) : ''}`,
    };
    if (response.body.reconciliation_error) {
      p.flash.html += `<br />平仓后对账失败：${esc(response.body.reconciliation_error)}`;
      p.flash.error = true;
    }
  } else {
    p.flash = { error: true, html: `<b>${esc(id)}</b> 平仓失败：${esc(response.body.error)}` };
  }
  showActionFlash();
  await loadPositions().catch(() => {});
  if (live) loadAccount();
}

// 补仓输入框的默认每腿名义：自动交易的单腿名义（自动补仓也按它），没加载过就按这笔开仓时的单腿名义。
function defaultAddSize(position) {
  const auto = num(state.rhAuto?.data?.settings?.size_usdt);
  if (auto && auto > 0) return String(auto);
  const leg = position.long || position.short;
  const notional = num(leg?.notional_usdt);
  if (!notional) return '';
  return String(Math.max(10, Math.round(notional / (1 + (num(leg.adds) || 0)) / 10) * 10));
}

// 能不能补仓：两腿齐全、持仓中的价差单；实盘还要记了杠杆（服务端同样会核，没记杠杆的实盘补仓直接拒绝）。
function canAdd(position) {
  if (state.mode === 'live' && position.leverage == null) return false;
  return position.status === 'open' && position.strategy === 'spread' && Boolean(position.long && position.short);
}

function positionActions(position) {
  const p = state.pos;
  const cfg = state.tradeConfig;
  const live = state.mode === 'live';
  if (!cfg?.auth_configured || !token()) return '';
  if (live && cfg.live?.mode !== 'trade') return '<span class="muted small">实盘只读，不能平仓 / 补仓</span>';
  const retry = position.status !== 'open';
  const label = retry ? '重试退出' : '平仓';
  const where = live ? '实盘' : '纸面';
  const disabled = p.busy ? ' disabled' : '';
  const cancel = `<button type="button" class="btn ghost" data-pos-cancel="1"${disabled}>取消</button>`;
  if (p.confirming === position.id && p.confirmKind === 'close') {
    return `<span class="small">确认${where}${label} <b>${esc(position.symbol)}</b>（${esc(legsTextPlain(position))}）？两腿一起按市价平。</span>
      <button type="button" class="btn ${live ? 'danger' : 'primary'}" data-close-go="${esc(position.id)}"${disabled}>${p.busy ? '执行中…' : `确认${label}`}</button>
      ${cancel}`;
  }
  if (p.confirming === position.id && p.confirmKind === 'add') {
    const size = num(p.addSize);
    const ok = size !== null && size > 0;
    const lev = position.leverage == null ? '' : `，杠杆 ${num(position.leverage)}x`;
    return `<label class="add-size small">每腿补 <input type="number" min="0" step="10" id="add-size" inputmode="decimal" value="${esc(p.addSize)}" /> USDT</label>
      <span class="small muted">按原方向（${esc(legsTextPlain(position))}${lev}）两腿各补这么多名义；对账、现拉盘口、保证金、单笔上限照常检查</span>
      <div class="add-spread small" id="add-spread">${addSpreadHtml(position)}</div>
      <button type="button" class="btn ${live ? 'danger' : 'primary'}" data-add-go="${esc(position.id)}"${!ok || p.busy ? ' disabled' : ''}>${p.busy ? '执行中…' : `确认${where}补仓`}</button>
      ${cancel}`;
  }
  const others = p.confirming ? ' disabled' : disabled;
  return `<button type="button" class="btn ghost" data-close="${esc(position.id)}"${others}>${label}</button>${
    canAdd(position) ? `<button type="button" class="btn ghost" data-add="${esc(position.id)}"${others}>补仓</button>` : ''
  }`;
}

// 原仓位实际成交的开仓价差（%）：(空腿均价 − 多腿均价) / 两者均值 × 100，与自动补仓比较的口径相同。
function positionEntrySpread(position) {
  const long = num(position.long?.average_price);
  const short = num(position.short?.average_price);
  if (!long || !short) return null;
  return ((short - long) / ((short + long) / 2)) * 100;
}

// 价差监控里这笔仓位那一组、同方向（多原多腿 / 空原空腿）的报价。
function addSpreadQuote(position, view) {
  const base = String(position.symbol || '').split('/')[0].toUpperCase();
  const long = position.long?.venue;
  const short = position.short?.venue;
  const line = (view?.lines || []).find((l) => String(l.base).toUpperCase() === base
    && ((l.a === long && l.b === short) || (l.a === short && l.b === long)));
  if (!line) return { line: null, leg: null };
  return { line, leg: line.a === long ? line.long_a : line.long_b };
}

// 补仓确认行里的价差对比：现在可成交价差 vs 原仓位开仓价差。只是参考，下单时服务端照样按现拉盘口重算。
function addSpreadHtml(position) {
  const p = state.pos;
  const entry = positionEntrySpread(position);
  const entryText = entry === null ? '原仓位开仓价差 <span class="muted">未知</span>' : `原仓位开仓价差 <b>${pctRaw(entry, 3)}</b>`;
  const q = p.addQuote;
  if (!q || (!q.view && !q.error)) return `${entryText} · 现在价差读取中…`;
  if (q.error && !q.view) return `${entryText} · <span class="neg">读不到价差监控：${esc(q.error)}</span>`;
  const { line, leg } = addSpreadQuote(position, q.view);
  if (!line) return `${entryText} · <span class="muted">价差监控里没有这一组，看不到现在的价差（补仓时照样按现拉盘口检查）</span>`;
  if (line.note) return `${entryText} · <span class="muted">现在：${esc(line.note)}</span>`;
  if (!leg) return `${entryText} · <span class="muted">这个方向现在没有可成交报价</span>`;
  // 监控按估算名义吃深度；补得比它大时，有逐档数据（有信号的方向才有）就取够这笔的那一档。
  let now = num(leg.entry_pct);
  let at = num(q.view.size_usdt);
  const size = num(p.addSize);
  const ladder = leg.ladder || [];
  let partial = false;
  if (size && at && size > at) {
    const rung = ladder.find((r) => num(r.size_usdt) >= size) || ladder[ladder.length - 1];
    if (rung && num(rung.size_usdt) > at) {
      now = num(rung.entry_pct);
      at = num(rung.size_usdt);
    }
    partial = at < size;
  }
  const atText = at ? `按 ${usd(at, 0)} 吃深度${partial ? '，比这笔小、偏乐观' : ''}` : '';
  const age = num(line.age_sec);
  const ageText = age !== null && age > 5 ? `，盘口 ${age} 秒没更新` : '';
  const parts = [`现在可成交价差 <b>${pctRaw(now, 3)}</b> <span class="muted">（${atText}${ageText}）</span>`, entryText];
  if (entry !== null && now !== null) {
    const diff = now - entry;
    const verdict = diff > 0 ? '比原仓位好' : diff < 0 ? '比原仓位差' : '和原仓位一样';
    parts.push(`<b class="${cls(diff)}">${verdict} ${pctRaw(diff, 3)}</b>`);
    const min = num(state.rhAuto?.data?.settings?.add_min_improve_pct);
    if (min !== null) parts.push(`<span class="muted">自动补仓门槛 +${min}%（${diff >= min ? '够' : '不够'}）</span>`);
  }
  const net = num(leg.net_to_normal_pct);
  if (net !== null) parts.push(`回到正常净收益 <b class="${cls(net)}">${pctRaw(net, 3)}</b>`);
  return parts.join(' · ');
}

// 补仓确认期间每 2 秒刷新价差对比（只重画对比那一块，不动输入框）；确认关掉就停。
async function refreshAddQuote() {
  const p = state.pos;
  clearTimeout(p.addQuoteTimer);
  if (p.confirmKind !== 'add' || !p.confirming) return;
  const seq = p.addQuoteSeq;
  const id = p.confirming;
  // 只取这个合约的行（各组加起来几行），不拉整份快照（两千多行）。
  const position = (state.positions?.open || []).find((x) => x.id === id);
  const base = String(position?.symbol || '').split('/')[0];
  const result = await api(`/api/rh-spread?${new URLSearchParams({ base, limit: '100' })}`).catch((error) => ({ ok: false, body: { error: error.message } }));
  if (seq !== p.addQuoteSeq || p.confirmKind !== 'add' || p.confirming !== id) return;
  p.addQuote = result.ok
    ? { view: result.body, error: null }
    : { view: p.addQuote?.view || null, error: result.body?.error || `HTTP ${result.status}` };
  paintAddSpread();
  p.addQuoteTimer = setTimeout(refreshAddQuote, 2000);
}

function paintAddSpread() {
  const p = state.pos;
  const box = $('add-spread');
  const position = (state.positions?.open || []).find((x) => x.id === p.confirming);
  if (box && position) box.innerHTML = addSpreadHtml(position);
}

function stopAddQuote() {
  const p = state.pos;
  clearTimeout(p.addQuoteTimer);
  p.addQuoteTimer = null;
  p.addQuoteSeq += 1;
  p.addQuote = null;
}

function legsTextPlain(position) {
  return `多 ${position.long?.venue || '—'} / 空 ${position.short?.venue || '—'}`;
}

async function addToPosition(id) {
  const p = state.pos;
  if (p.busy || p.confirming !== id || p.confirmKind !== 'add') return;
  const size = String(p.addSize).trim();
  if (!(num(size) > 0)) return;
  const live = state.mode === 'live';
  stopAddQuote();
  p.busy = true;
  renderPositions();
  const body = { mode: state.mode, position_id: id, size };
  if (live) body.confirm = id;
  const response = await api('/api/trade/add', { method: 'POST', body, auth: true });
  p.busy = false;
  p.confirming = null;
  p.confirmKind = null;
  if (response.ok) {
    const position = response.body.position;
    const note = position?.note ? esc(position.note) : '';
    p.flash = response.body.added
      ? { error: false, html: `<b>${esc(id)}</b> 已补仓：两腿各约 ${usd(response.body.prepared?.size_usdt ?? size)}。${note}` }
      : { error: true, html: `<b>${esc(id)}</b> 补仓没补成（台账未变或已回滚）。${note}` };
    if (response.body.reconciliation_error) {
      p.flash.html += `<br />补仓后对账失败：${esc(response.body.reconciliation_error)}`;
      p.flash.error = true;
    }
  } else {
    p.flash = { error: true, html: `<b>${esc(id)}</b> 补仓失败：${esc(response.body.error)}` };
  }
  showActionFlash();
  await loadPositions().catch(() => {});
  if (live) loadAccount();
}

// 一条腿开仓以来实际收付的资金费（交易所结算流水）。
function legFundingText(leg, label) {
  if (!leg) return '';
  if (leg.usdt == null) return `${label} ${esc(leg.venue)} <span class="muted" title="${esc(leg.note || '')}">未知</span>`;
  const last = leg.last_at ? `，最近 ${when(leg.last_at)}` : '';
  return `${label} ${esc(leg.venue)} <b class="${cls(leg.usdt)}">${pnlUsd(leg.usdt)}</b><span class="muted">（${leg.payments} 次${last}）</span>`;
}

// 上一轮后台规则按两边盘口估的「现在平掉整笔」含资金费净额。与标记价净额口径不同，分开展示。
// 上一轮后台规则按两边盘口估的「现在整笔平掉」含资金费净额（数）。页面上所有「净收益」都用它：
// 标记价会偏离真实成交价（某次实盘：Lighter 标记价比盘口高约 4%，标记价净收益为正、盘口净额为负），
// 按标记价估值的浮亏 / 浮盈拿不到。拿不到（没核对过、盘口用不了、资金费未知）为 null。
function bookNet(report) {
  const obs = report?.evaluation?.observation;
  const exit = obs?.exit;
  const funding = num(obs?.funding_usdt);
  if (!exit || num(exit.net_usdt) === null || funding === null) return null;
  return num(exit.net_usdt) + funding;
}

function bookExitText(report) {
  if (!report) return '<span class="muted" title="等后台规则轮核对">待核对</span>';
  const obs = report.evaluation?.observation;
  const exit = obs?.exit;
  const net = bookNet(report);
  if (net !== null) {
    const title = `多腿卖 ${exit.long_price}、空腿买回 ${exit.short_price}；已扣已付与预估平仓手续费、含已结算资金费`;
    return `<b class="${cls(net)}" title="${esc(title)}">${pnlUsd(net)}</b>`;
  }
  if (obs?.exit_unavailable) {
    // 原因直接写在页面上：触屏没有悬停提示，只放 title 的话看到的只是一句「用不了」。
    const why = String(obs.exit_unavailable);
    const shown = why.length > 80 ? `${why.slice(0, 80)}…` : why;
    return `<span class="muted" title="${esc(why)}">盘口用不了（${esc(shown)}）</span>`;
  }
  return '<span class="muted">未知</span>';
}

// 顶部合计：算得出来的几笔加起来，缺数据的（缺行情 / 盘口还没核对）单独标明有几笔没计入，一笔都算不出才显示「—」。
// 不能因为一笔缺行情就整个合计消失（某次实盘 在 HL xyz 的价格比 CEX 低 5% 以上，被价差扫描当成另一个资产剔出快照，
// 这一笔评估不了，每日资金费合计就一直是「—」）；也不能悄悄少算一笔：数量写在合计后面，名单写在悬停提示里。
function positionTotals(open, reports) {
  const totalOf = (valueOf) => {
    const values = open.map((p) => ({ symbol: p.symbol, value: valueOf(p) }));
    const known = values.filter((v) => v.value !== null);
    return {
      sum: known.length ? known.reduce((a, v) => a + v.value, 0) : null,
      missing: values.filter((v) => v.value === null).map((v) => v.symbol),
    };
  };
  const shown = (total) => {
    if (!open.length || total.sum === null) return { text: '—', title: total.missing.length ? `算不出：${total.missing.join('、')}` : '', sum: total.sum };
    return total.missing.length
      ? { text: `${pnlUsd(total.sum)}（${total.missing.length} 笔缺数据未计入）`, title: `没计入：${total.missing.join('、')}（缺行情或盘口还没核对）`, sum: total.sum }
      : { text: pnlUsd(total.sum), title: '', sum: total.sum };
  };
  return {
    net: shown(totalOf((p) => bookNet((reports || []).find((r) => r.position_id === p.id)))),
    daily: shown(totalOf((p) => num(p.evaluation?.observation?.pnl?.funding_daily_usdt))),
  };
}

function pnlSummary(position, observation, report) {
  const pnl = observation?.pnl;
  const cell = (label, value) => `<span>${label} <b class="${cls(value)}">${num(value) === null ? '未知' : pnlUsd(value)}</b></span>`;
  const funding = position.funding;
  const net = observation?.net_with_funding_usdt;
  const target = position.rules?.take_profit_usdt;
  const bookMin = position.rules?.take_profit_book_min_usdt;
  const bookRule = bookMin == null ? `盘口也要 ≥ ${usd(target)}` : num(bookMin) === 0 ? '盘口为正即平' : `盘口 ≥ ${usd(bookMin)} 即平`;
  const scope = '整笔净收益按两边盘口现在平掉的预估（后台规则每轮核对），已扣已付与预估平仓手续费、含已减仓部分已实现盈亏与已结算资金费；上面的价格盈亏按服务端标记价，标记价和盘口偏离时两者对不上';
  return `${pnl ? `<div class="pnl-row">
    ${cell('剩余两腿价格盈亏', pnl.price_pnl_usdt)}
    <span>剩余两腿已付手续费 <b>${usd(pnl.fees_usdt, 4)}</b></span>
    ${cell('剩余两腿净额（不含资金费 / 已退出部分）', pnl.net_usdt)}
    <span>按当前费率每日资金费 ≈ <b class="${cls(pnl.funding_daily_usdt)}">${pnl.funding_daily_usdt == null ? '未知' : `${pnlUsd(pnl.funding_daily_usdt)} / 天`}</b></span>
  </div>` : ''}
  <div class="pnl-row funding-row">
    ${cell(state.mode === 'paper' ? '已结算资金费（纸面为 0）' : '已结算资金费', observation?.funding_usdt)}
    <span>整笔净收益（按盘口，含资金费） ${bookExitText(report)}</span>
    <span class="muted small">${scope}</span>
    ${funding ? `<span class="small" title="交易所流水按账户及合约记录，同账户同合约的其他仓位也可能计入">${[legFundingText(funding.long, '多'), legFundingText(funding.short, '空')].filter(Boolean).join(' · ')}</span>` : ''}
  </div>
  <div class="pnl-row rule-progress">
    <span data-take-profit-progress>止盈进度（标记价）：${target == null ? '未启用' : `<b class="${cls(net)}">${num(net) === null ? '未知' : pnlUsd(net)}</b> / 目标 <b>${usd(target)}</b>（USDT；触发后核对盘口：${bookRule}）`}</span>
    ${target == null ? '' : `<span data-book-exit title="实际止盈以这个数为准：按两边盘口整笔吃单均价、扣平仓手续费">按盘口现在平仓可得：${bookExitText(report)}</span>`}
    <span data-margin-used title="整笔仓位累计尝试追加额；结果未知也占额度，明确拒绝不占额度。关闭规则不清零。">累计追加保证金额度已用 <b>${usd(position.margin_added_usdt)}</b> / 上限 <b>${position.rules?.auto_margin_max_usdt == null ? '未启用' : usd(position.rules.auto_margin_max_usdt)}</b> USDT</span>
  </div>`;
}

function positionLeg(leg, status, label, pnl, mode = 'isolated') {
  if (!leg) return `<div class="leg-card"><span class="muted">${label}未成交</span></div>`;
  const sideClass = leg.side === 'buy' ? 'long' : 'short';
  const legPnl = pnl?.price_pnl_usdt == null
    ? '<span class="muted">—</span>'
    : `<span class="${cls(pnl.price_pnl_usdt)}">${pnlUsd(pnl.price_pnl_usdt)}（${pctRaw(pnl.price_pnl_pct, 2)}）</span>`;
  return `<div class="leg-card">
    <div class="top"><span class="venue">${esc(leg.venue)}</span><span class="side-pill ${sideClass}">${label}</span></div>
    <div class="kv"><span>入场名义</span><span>${usd(leg.notional_usdt)}</span></div>
    <div class="kv"><span>数量</span><span>${pnl?.quantity == null ? '—' : esc(num(pnl.quantity).toPrecision(6).replace(/\.?0+$/, ''))}</span></div>
    <div class="kv"><span>入场均价</span><span>${price(leg.average_price)}</span></div>
    <div class="kv"><span>${mode === 'cross' ? '保证金模式' : '逐仓保证金'}</span><span>${mode === 'cross' ? '全仓（共用账户权益）' : status?.margin_usdt == null && leg.margin_usdt == null ? '<span class="muted">未记录</span>' : usd(status?.margin_usdt ?? leg.margin_usdt)}${mode !== 'cross' && status?.margin_from_venue ? ' <span class="tag sky" title="交易所里这个仓位实际占用的保证金（含你后来补进去的）。台账里只记着开仓时的数。">交易所</span>' : ''}</span></div>
    <div class="kv"><span>标记价</span><span>${price(status?.mark_price)}</span></div>
    <div class="kv"><span>浮动盈亏</span>${legPnl}</div>
    <div class="kv"><span>已付手续费</span><span>${usd(leg.fee_usdt, 4)}</span></div>
    <div class="kv"><span>强平价</span><span>${price(status?.liquidation_price)}${status?.liquidation_from_venue ? ' <span class="tag sky" title="交易所报告的强平价">交易所</span>' : status?.margin_from_venue ? ' <span class="tag sky" title="按交易所里实际的保证金与维持保证金率算的">按实际保证金</span>' : ''}</span></div>
    <div class="kv"><span>强平距离</span><span>${healthText(status?.distance_pct, status?.health)}</span></div>
    ${healthBar(status?.distance_pct, status?.health)}
  </div>`;
}

// 已平仓的已实现盈亏：价格盈亏 − 手续费 + 资金费。没记录的如实说没记录，不显示成 0。
// ───────────── 持仓卡片折叠 ─────────────
//
// 折叠状态存在 state 里（每轮自动刷新会整体重写 innerHTML），并记进浏览器：刷新页面不丢。
// 浏览器存储读写可能抛错（隐私模式、被禁用），一律吞掉，只是不记住而已。
const COLLAPSED_KEY = 'arb.positions.collapsed';
const COLLAPSED_MAX = 200;

function collapsedSet() {
  if (!state.pos.collapsed) {
    let saved = [];
    try {
      saved = JSON.parse(localStorage.getItem(COLLAPSED_KEY) || '[]');
    } catch {
      saved = [];
    }
    state.pos.collapsed = new Set(Array.isArray(saved) ? saved.map(String) : []);
  }
  return state.pos.collapsed;
}

function saveCollapsed() {
  try {
    localStorage.setItem(COLLAPSED_KEY, JSON.stringify([...collapsedSet()].slice(-COLLAPSED_MAX)));
  } catch {
    // 记不住就算了：折叠在本次页面里照样有效。
  }
}

// 正在平仓确认的那笔永远展开：折叠了就看不到确认输入框。
function isCollapsed(id) {
  return state.pos.confirming !== id && collapsedSet().has(id);
}

function toggleCollapsed(id) {
  const set = collapsedSet();
  if (set.has(id)) set.delete(id);
  else set.add(id);
  saveCollapsed();
}

function setAllCollapsed(collapsed) {
  const set = collapsedSet();
  for (const position of state.positions?.open || []) {
    if (collapsed) set.add(position.id);
    else set.delete(position.id);
  }
  saveCollapsed();
}

// 需要人看一眼的事有几条：折叠后也在头部数出来，问题不会被折叠藏起来。
function positionAlertCount(position, external = []) {
  const evaluation = position.evaluation;
  return (
    external.length +
    (position.quotes_missing ? 1 : 0) +
    (evaluation?.skipped || []).length +
    (position.note ? 1 : 0) +
    (evaluation && evaluation.action?.action !== 'hold' ? 1 : 0) +
    (position.status !== 'open' ? 1 : 0)
  );
}

// 折叠后头部的一行摘要：整笔净收益（按盘口现在平仓，含资金费）。
function collapsedSummary(position, observation, report) {
  return `<span class="pos-brief small"><span title="按两边盘口现在整笔平掉的预估，已扣手续费与穿价、含已结算资金费">净收益 ${bookExitText(report)}</span></span>`;
}

function realizedCell(p) {
  if (p.realized_source) {
    const price = num(p.realized_pnl_usdt);
    const fee = num(p.realized_fee_usdt);
    const funding = p.realized_funding_usdt == null ? null : num(p.realized_funding_usdt);
    const net = price - fee + (funding ?? 0);
    const detail = `价格盈亏 ${pnlUsd(price)} · 手续费 −${usd(fee, 4)} · 资金费 ${funding == null ? '没查到，未计入' : pnlUsd(funding)}` +
      (p.realized_source === 'venue_fills' ? ' · 按交易所成交记录核算' : ' · 看板平仓的逐笔成交');
    return `<b class="${cls(net)}" title="${esc(detail)}">${pnlUsd(net)}</b>${funding == null ? ' <span class="muted small">不含资金费</span>' : ''}`;
  }
  if (p.status === 'closed') {
    return p.pnl_unattributed
      ? `<span class="muted" title="${esc(p.pnl_unattributed)}">核不出</span>`
      : '<span class="muted">核算中…</span>';
  }
  return '<span class="muted">—</span>';
}

// 成交数量：去掉末尾的 0，最多 10 位有效数字（0.4911802 这种要原样看到）。
const qtyText = (value) => {
  const parsed = num(value);
  return parsed === null ? '—' : String(Number(parsed.toPrecision(10)));
};

function orderRoleText(role, status) {
  if (role?.kind === 'trim') return `第 ${role.n} 次减仓`;
  if (role?.kind === 'undo') return role.n === 0 ? '开仓零头退回' : `第 ${role.n} 次补仓退回`;
  if (role?.kind === 'exit') return `${status === 'unwound' ? '回滚' : '平仓'}${role.n > 1 ? ` #${role.n}` : ''}`;
  return '开仓';
}

// 一笔已结束仓位的双边明细（服务端从台账订单还原，口径与执行器入账相同）：每条腿开仓、减仓、
// 平仓各成交多少、什么价、多少手续费，这条腿的价格盈亏。合计与「已实现盈亏」那一列同一口径。
function closedDetailHtml(position) {
  const b = position.breakdown;
  if (!b) return '<div class="muted small">台账里没有这笔的成交记录，拆不出双边明细。</div>';
  const legRows = (leg, label) => {
    if (!leg) return `<tr><td>${label}</td><td colspan="7" class="muted">没有成交</td></tr>`;
    const sideCls = leg.side === 'buy' ? 'long-c' : 'short-c';
    const entry = `<tr>
        <td class="${sideCls}">${label}</td><td>${esc(leg.venue)}</td><td>开仓${leg.adds ? `（含 ${leg.adds} 次补仓，均价加权）` : ''}</td>
        <td class="num">${leg.entry_price == null ? '<span class="muted">台账里没有开仓单</span>' : qtyText(leg.entry_quantity)}</td>
        <td class="num">${price(leg.entry_price)}</td>
        <td class="num">${usd(leg.entry_notional_usdt)}</td>
        <td class="num">${usd(leg.entry_fee_usdt, 4)}</td>
        <td class="num dim">—</td>
      </tr>`;
    const exits = (leg.exits || []).map((exit) => `<tr>
        <td></td><td></td><td>${orderRoleText(exit.role, position.status)}</td>
        <td class="num">${qtyText(exit.quantity)}</td>
        <td class="num">${price(exit.average_price)}</td>
        <td class="num">${usd(exit.notional_usdt)}</td>
        <td class="num">${usd(exit.fee_usdt, 4)}</td>
        <td class="num ${cls(exit.price_pnl_usdt)}">${exit.price_pnl_usdt == null ? '—' : pnlUsd(exit.price_pnl_usdt)}</td>
      </tr>`).join('');
    const legNet = num(leg.price_pnl_usdt) - num(leg.fee_usdt);
    const subtotal = `<tr class="subtotal">
        <td colspan="6" class="small">${label}小计：价格盈亏 <b class="${cls(leg.price_pnl_usdt)}">${pnlUsd(leg.price_pnl_usdt)}</b> − 手续费 <b>${usd(leg.fee_usdt, 4)}</b></td>
        <td class="num">${usd(leg.fee_usdt, 4)}</td>
        <td class="num"><b class="${cls(legNet)}">${pnlUsd(legNet)}</b></td>
      </tr>`;
    return entry + exits + subtotal;
  };
  const funding = position.realized_funding_usdt == null ? null : num(position.realized_funding_usdt);
  const notes = [];
  if (b.external) {
    notes.push('<div class="alert info">这笔是在交易所里平掉的：台账里只有开仓侧（和期间的减仓），平仓按交易所成交记录核算，合计以「已实现盈亏」那一列为准。</div>');
  }
  if (b.pnl_gap_usdt != null || b.fee_gap_usdt != null) {
    const gaps = [
      b.pnl_gap_usdt != null ? `价格盈亏差 ${pnlUsd(b.pnl_gap_usdt)}` : '',
      b.fee_gap_usdt != null ? `手续费差 ${pnlUsd(b.fee_gap_usdt)}` : '',
    ].filter(Boolean).join('，');
    notes.push(`<div class="alert warn">两腿明细之和与台账汇总对不上（${gaps}）：以「已实现盈亏」为准，这里如实列出差额。</div>`);
  }
  const total = b.external
    ? ''
    : `<div class="pnl-row">
        <span>两腿价格盈亏 <b class="${cls(b.price_pnl_usdt)}">${pnlUsd(b.price_pnl_usdt)}</b></span>
        <span>手续费合计 <b>−${usd(b.fee_usdt, 4)}</b></span>
        <span>资金费 <b class="${cls(funding)}">${funding == null ? '没查到' : pnlUsd(funding)}</b></span>
        <span>已实现净额 ${realizedCell(position)}</span>
      </div>`;
  const report = position.open_report
    ? `<details class="open-report-box"><summary>开仓执行：预估 vs 实际</summary>${openReportHtml(position.open_report)}</details>`
    : '';
  return `<div class="closed-detail-box">
    <div class="table-wrap"><table class="breakdown-table">
      <thead><tr><th>腿</th><th>场所</th><th>动作</th><th class="num">数量</th><th class="num">成交均价</th><th class="num">名义</th><th class="num">手续费</th><th class="num">价格盈亏</th></tr></thead>
      <tbody>${legRows(b.long, '多腿')}${legRows(b.short, '空腿')}</tbody>
    </table></div>
    ${total}${notes.join('')}${report}
  </div>`;
}

// ───────── 「最近结束」的移除 ─────────
// 移除只是把仓位从这个列表里隐藏：台账原样保留，对账、当日盈亏、熔断都照旧把它算进去（后端见 dismissed.rs）。
// 没成交过的（被交易所拒了、第一腿没吃到、准备阶段就没过）什么都没发生，一点就移除，还能一键清理；
// 有成交的是真实交易记录，移除前要确认。面板上没有恢复入口；要找回得改 .dismissed.json。

// 每行最右边的按钮。要有令牌才显示（移除要鉴权）。
function closedActionHtml(p) {
  if (!state.tradeConfig?.auth_configured || !token()) return '';
  const noTrade = p.traded === false;
  const hint = noTrade
    ? '什么都没成交：从列表里移除（台账保留）'
    : '这笔有成交：只是从列表里隐藏，台账和盈亏统计不变（移除前会让你确认）';
  return `<button type="button" class="btn ghost tiny" data-closed-dismiss="${esc(p.id)}" data-traded="${noTrade ? '0' : '1'}" title="${esc(hint)}">移除</button>`;
}

// 状态旁边的小标签：没成交的一眼能认出来。
function closedTagsHtml(p) {
  const tags = [];
  if (p.traded === false) tags.push('<span class="pill tag-muted" title="这笔的订单一笔都没成交：被交易所拒了、第一腿没吃到，或者准备阶段就没过。什么都没发生。">没成交</span>');
  return tags.length ? ` ${tags.join(' ')}` : '';
}

// 标题行右侧：一键清理没成交的。
function closedToolsHtml(data) {
  if (!state.tradeConfig?.auth_configured || !token()) return '';
  const parts = [];
  const failed = Number(data?.failed_count) || 0;
  if (failed > 0) {
    parts.push(`<button type="button" class="btn ghost tiny" data-closed-clean title="一次移除所有一笔都没成交的已结束仓位（被拒、没吃到、准备没过）；有成交的不动">清理 ${failed} 笔没成交的</button>`);
  }
  return parts.join('');
}

// 移除 / 一键清理。ids 是仓位 id 列表；traded = 其中有成交的（要确认）。
async function dismissClosed(ids, { traded = false, allFailed = false } = {}) {
  const p = state.pos;
  if (p.busy) return;
  if (allFailed) {
    const count = Number(state.positions?.failed_count) || 0;
    if (!window.confirm(`清理 ${count} 笔没成交的已结束仓位？\n它们一笔都没成交，什么都没发生；只是从「最近结束」里隐藏，台账原样保留。`)) return;
  } else if (traded) {
    if (!window.confirm(`${ids[0]} 有成交，这是一笔真实交易的记录。\n移除只是从「最近结束」列表里隐藏：台账原样保留，当日盈亏、对账、熔断仍把它算进去。\n确定移除？`)) return;
  }
  p.busy = true;
  const body = { mode: state.mode, ids, restore: false, all_failed: allFailed, allow_traded: traded && !allFailed };
  const response = await api('/api/trade/dismiss', { method: 'POST', body, auth: true });
  p.busy = false;
  if (response.ok) {
    const changed = (response.body.changed || []).length;
    const skipped = response.body.skipped || [];
    const why = skipped.length ? `；没处理 ${skipped.length} 笔：${skipped.map(([id, reason]) => `${esc(id)}（${esc(reason)}）`).join('、')}` : '';
    p.flash = {
      error: changed === 0 && skipped.length > 0,
      html: `已移除 ${changed} 笔${why}。台账原样保留、统计照旧。`,
    };
  } else {
    p.flash = { error: true, html: `移除失败：${esc(response.body.error)}` };
  }
  renderFlash();
  await loadPositions().catch(() => {});
}

// 交易统计（服务端按全量台账算，见 crates/web/src/stats.rs）：选中时间段的汇总、按合约、按日。
// 盈亏没核出来的只计笔数、不当 0 加进金额；外部平仓的平仓侧成交额是估算的，都在脚注里说明。
function tradeStatsParts(stats, windowKey) {
  const windows = stats?.windows || [];
  const chosen = windows.find((w) => w.key === windowKey) || windows[0];
  if (!chosen) return null;
  const seg = windows
    .map((w) => `<button type="button" data-stats-window="${esc(w.key)}" class="${w.key === chosen.key ? 'on' : ''}">${esc(w.label)}</button>`)
    .join('');
  const s = chosen.summary;
  const decided = s.wins + s.losses;
  const winRate = decided ? `${((s.wins / decided) * 100).toFixed(0)}%` : '—';
  const avg = s.settled ? num(s.net_usdt) / s.settled : null;
  const tiles = [
    ['已实现净盈亏', s.settled ? pnlUsd(s.net_usdt) : '—', s.settled ? cls(s.net_usdt) : 'dim', `价格盈亏 ${pnlUsd(s.price_pnl_usdt)} − 手续费 ${usd(s.fee_usdt, 4)} + 资金费 ${pnlUsd(s.funding_usdt)}`],
    ['结束笔数', `${s.ended} 笔`, '', `平仓 ${s.closed} 笔 · 回滚 ${s.unwound} 笔`],
    ['胜率', winRate, '', `盈利 ${s.wins} 笔 · 亏损 ${s.losses} 笔（净额为 0 的不计）`],
    ['每笔平均', avg === null ? '—' : pnlUsd(avg), cls(avg), '核出盈亏的那些笔的平均净额'],
    ['成交额（两腿）', usd(s.volume_usdt, 0), '', '两条腿开仓、补仓、减仓、平仓、回滚的成交名义合计'],
    ['手续费', usd(s.fee_usdt, 2), '', '已结束仓位付过的全部手续费（开仓 + 平仓）'],
    ['资金费', pnlUsd(s.funding_usdt), cls(s.funding_usdt), '持仓期间实际收付的资金费合计（正 = 收到）'],
    ['最好 / 最差', s.best_usdt == null ? '—' : `${pnlUsd(s.best_usdt)} / ${pnlUsd(s.worst_usdt)}`, '', '单笔净额最高与最低'],
  ]
    .map(([label, value, tone, title]) => `<div class="stat" title="${esc(title)}"><span>${esc(label)}</span><b class="${tone}">${esc(value)}</b></div>`)
    .join('');
  const foot = [
    `这段时间新开 ${s.opened} 笔（含还在持有的）`,
    s.unwound ? `回滚 ${s.unwound} 笔合计 ${pnlUsd(s.unwound_net_usdt)}` : '',
    s.unknown ? `${s.unknown} 笔盈亏没核出来，未计入金额` : '',
    s.funding_missing ? `${s.funding_missing} 笔资金费没查到，按 0 计` : '',
    s.volume_estimated ? `${s.volume_estimated} 笔在交易所手动平仓，平仓侧成交额按开仓名义估算` : '',
    chosen.key === 'today' ? '「今日」按 UTC 日，与当日亏损上限同一日界' : '',
    stats.excluded ? `按你的要求有 ${stats.excluded} 笔整笔不计入（盈亏、笔数、成交额都不算；当日亏损上限照算）` : '',
  ].filter(Boolean).join('；') + '。';
  const winLoss = (row) => `${row.wins} / ${row.losses}`;
  const unknownNote = (n) => (n ? ` <span class="muted small">另 ${n} 笔没核出</span>` : '');
  const rows = chosen.by_symbol || [];
  const symbols = rows.length
    ? rows
        .map(
          (row) => `<tr><td>${esc(row.symbol)}</td><td class="num">${row.ended}</td><td class="num">${winLoss(row)}</td>
            <td class="num"><b class="${row.ended > row.unknown ? cls(row.net_usdt) : 'dim'}">${row.ended > row.unknown ? pnlUsd(row.net_usdt) : '—'}</b>${unknownNote(row.unknown)}</td>
            <td class="num">${usd(row.fee_usdt, 4)}</td><td class="num">${usd(row.volume_usdt, 0)}</td></tr>`,
        )
        .join('')
    : '<tr><td colspan="6" class="muted">这段时间没有交易。</td></tr>';
  const daily = (stats.daily || [])
    .map(({ date, summary: d }) => `<tr><td>${esc(date)}</td><td class="num">${d.ended}</td><td class="num">${winLoss(d)}</td>
      <td class="num"><b class="${d.settled ? cls(d.net_usdt) : 'dim'}">${d.settled ? pnlUsd(d.net_usdt) : '—'}</b>${unknownNote(d.unknown)}</td>
      <td class="num">${usd(d.fee_usdt, 4)}</td><td class="num ${cls(d.funding_usdt)}">${pnlUsd(d.funding_usdt)}</td><td class="num">${d.opened}</td><td class="num">${usd(d.volume_usdt, 0)}</td></tr>`)
    .join('');
  return { key: chosen.key, seg, tiles, foot, symbols, venues: venueStatsRows(chosen), daily };
}

// 按交易所：成交额（含外部平仓估算）、占比、挂单部分、订单数、仓位笔数、订单回报的手续费与折合费率。
function venueStatsRows(chosen) {
  const rows = chosen.by_venue || [];
  if (!rows.length) return '<tr><td colspan="7" class="muted">这段时间没有成交。</td></tr>';
  const total = rows.reduce((sum, row) => sum + (num(row.volume_usdt) || 0), 0);
  return rows
    .map((row) => {
      const volume = num(row.volume_usdt) || 0;
      const share = total > 0 ? `${((volume / total) * 100).toFixed(1)}%` : '—';
      const maker = num(row.maker_volume_usdt) || 0;
      const estimated = num(row.estimated_usdt) || 0;
      const fee = num(row.fee_usdt) || 0;
      const rate = volume > 0 ? `${((fee / volume) * 100).toFixed(4)}%` : '—';
      const estimatedNote = estimated > 0 ? ` <span class="muted small" title="在交易所里手动平掉的平仓侧，按开仓名义估算">含估算 ${usd(estimated, 0)}</span>` : '';
      return `<tr class="${volume > 0 ? '' : 'dim'}"><td>${esc(venueLabel(row.venue))}</td><td class="num">${usd(volume, 0)}${estimatedNote}</td><td class="num">${share}</td>
        <td class="num">${maker > 0 ? usd(maker, 0) : '—'}</td><td class="num">${row.orders}</td><td class="num">${row.positions}</td>
        <td class="num">${usd(fee, 4)} <span class="muted small">${rate}</span></td></tr>`;
    })
    .join('');
}

function renderTradeStats(stats) {
  const parts = tradeStatsParts(stats, state.pos.statsWindow);
  if (!parts) return;
  $('trade-stats-seg').innerHTML = parts.seg;
  $('trade-stats-tiles').innerHTML = parts.tiles;
  $('trade-stats-foot').textContent = parts.foot;
  $('trade-stats-symbols').innerHTML = parts.symbols;
  $('trade-stats-venues').innerHTML = parts.venues;
  $('trade-stats-daily').innerHTML = parts.daily;
  for (const button of $('trade-stats-seg').querySelectorAll('[data-stats-window]')) {
    button.addEventListener('click', () => {
      state.pos.statsWindow = button.getAttribute('data-stats-window');
      renderTradeStats(stats);
    });
  }
}

function renderPositions() {
  const data = state.positions;
  const p = state.pos;
  const focusedRule = document.activeElement?.closest('[data-rule-editor]') ? document.activeElement.id : null;
  const live = state.mode === 'live';
  const setup = liveSetup();
  $('pos-title').textContent = setup ? '实盘（未开启）' : live ? '实盘持仓' : '纸面持仓';
  $('pos-intro').innerHTML = setup
    ? '按 .env 里填的凭据，自动识别你在哪几家交易所有可用账户；凑齐两家就能开启实盘。'
    : live
      ? '实盘台账里的双腿仓位（真实资金）。平仓 / 补仓点按钮后再点一下确认；规则由看板后台执行，对账不一致时只跳过出问题的币；建仓中断的会自动核实、补第二腿。'
      : '纸面台账里的双腿仓位，按当前行情评估每笔仓位的规则。配置了令牌时可以在这里平仓、手动跑一轮规则。';
  $('pos-closed-wrap').classList.toggle('hidden', setup);
  $('trade-stats').classList.toggle('hidden', setup || !data?.stats);
  $('pos-monitor').classList.toggle('hidden', setup);
  if (setup) {
    $('pos-round').innerHTML = '<span class="h-caution">实盘未开启（ARB_WEB_LIVE=off）</span>';
    $('pos-account-btn').classList.add('hidden');
    $('pos-collapse-all').classList.add('hidden');
    $('pos-expand-all').classList.add('hidden');
    $('pos-account').innerHTML = '';
    $('pos-stats').innerHTML = '';
    renderFlash();
    $('pos-open').innerHTML = setupHtml(p.credentials);
    return;
  }
  renderRound();
  renderAccount();
  renderFlash();
  if (!data) return;
  const open = data.open || [];
  const notional = open.reduce((sum, p) => sum + (num(p.long?.notional_usdt) || 0) + (num(p.short?.notional_usdt) || 0), 0);
  const distances = open
    .flatMap((p) => [p.evaluation?.observation?.long?.distance_pct, p.evaluation?.observation?.short?.distance_pct])
    .map(num)
    .filter((v) => v !== null);
  const pending = open.filter((p) => p.evaluation && p.evaluation.action.action !== 'hold').length;
  const totals = positionTotals(open, state.pos.round?.last_round?.reports);
  $('pos-stats').innerHTML = [
    ['持仓', `${open.length} 笔`, '', ''],
    ['两腿名义合计', usd(notional, 0), '', ''],
    ['整笔净收益（按盘口，含资金费）', totals.net.text, cls(totals.net.sum), totals.net.title],
    ['每日资金费（估）', totals.daily.text, cls(totals.daily.sum), totals.daily.title],
    ['最近的强平距离', distances.length ? `${Math.min(...distances).toFixed(2)}%` : '—', '', ''],
    ['规则触发', `${pending} 笔`, '', ''],
  ]
    .map(([label, value, tone, title]) => `<div class="stat"${title ? ` title="${esc(title)}"` : ''}><span>${label}</span><b class="${tone}">${esc(value)}</b></div>`)
    .join('');
  renderTradeStats(data.stats);

  const broken = data.broken_lines ? `<div class="notice">台账里有 ${data.broken_lines} 行无法解析，已跳过。</div>` : '';
  $('pos-open').innerHTML =
    broken +
    (open.length
      ? open
          .map((p) => {
            const evaluation = p.evaluation;
            const action = evaluation ? ACTION[evaluation.action.action] : null;
            const reason = evaluation?.action?.reason ? `：${esc(evaluation.action.reason)}` : '';
            const trim = evaluation?.action?.fraction != null ? ` ${pct(evaluation.action.fraction, 1).replace('+', '')}` : '';
            const obs = evaluation?.observation;
            // 上一轮后台规则对这笔按盘口核对过平仓：把结果带过来（持仓页自己不拉盘口）。
            const lastReport = (state.pos.round?.last_round?.reports || []).find((r) => r.position_id === p.id);
            const exit = lastReport?.evaluation?.observation?.exit;
            const exitFunding = num(lastReport?.evaluation?.observation?.funding_usdt);
            const exitWithFunding = num(exit?.net_usdt) !== null && exitFunding !== null ? num(exit.net_usdt) + exitFunding : null;
            // 对账发现的「不是看板平掉的」情况：只平了一条腿（裸敞口）、或等第二次确认。
            const external = (state.pos.round?.last_round?.external || []).filter((n) => n.position_id === p.id);
            const inFlight = inFlightNotice(p);
            const notes = [
              inFlight ? `<div class="alert ${inFlight.tone}">${esc(inFlight.text)}</div>` : '',
              ...external.map((n) => `<div class="alert warn"><b>对账发现：</b>${esc(n.message)}</div>`),
              exit
                ? `<div class="alert info">上一轮（${when(state.pos.round.last_round.at)}）按盘口核对平仓：多腿卖 ${price(exit.long_price)}、空腿买回 ${price(exit.short_price)}，平仓价差 ${pctRaw(exit.exit_basis_pct, 3)}，整笔预估 <b class="${cls(exit.net_usdt)}">${pnlUsd(exit.net_usdt)}</b>（资金费前，已扣已付与预估平仓手续费）；含该轮已结算资金费合计 <b class="${cls(exitWithFunding)}">${exitWithFunding === null ? '未知' : pnlUsd(exitWithFunding)}</b>。是否执行以服务端本轮规则报告为准。</div>`
                : '',
              p.quotes_missing ? '<div class="alert warn">本轮快照里缺至少一条腿的行情，评估不了。</div>' : '',
              ...(evaluation?.skipped || []).map((s) => `<div class="alert warn">未评估：${esc(s)}</div>`),
              p.note ? `<div class="alert info">${esc(p.note)}</div>` : '',
            ].join('');
            const collapsed = isCollapsed(p.id);
            const alerts = positionAlertCount(p, external);
            const head = `<div class="pos-head">
                <button type="button" class="collapse-btn" data-collapse="${esc(p.id)}" aria-expanded="${!collapsed}" title="${collapsed ? '展开这笔' : '折叠这笔'}">${collapsed ? '▸' : '▾'}</button>
                <span class="sym">${esc(p.symbol)}</span>
                <span class="small">${legsText(p.long?.venue, p.short?.venue)}</span>
                <span class="pill status">${STATUS_LABEL[p.status] || esc(p.status)}</span>
                <span class="tag">${p.leverage == null ? '杠杆未记录' : `${num(p.leverage)}x`}</span>
                <span class="tag sky">${marginModeLabel(p.margin_mode)}</span>
                ${p.strategy === 'spread' ? '<span class="tag sky">价差套利</span>' : ''}
                ${p.trims ? `<span class="tag warn">已减仓 ${p.trims} 次</span>` : ''}
                ${collapsed ? collapsedSummary(p, obs, lastReport) : ''}
                ${collapsed && alerts ? `<span class="tag warn" title="展开查看">${alerts} 条提醒</span>` : ''}
                <span class="grow"></span>
                <span class="muted small">${esc(p.id)} · 开仓 ${when(p.opened_at)}</span>
              </div>`;
            if (collapsed) return `<div class="card pos-card collapsed">${head}</div>`;
            return `<div class="card pos-card">
              ${head}
              <div class="legs-grid">${positionLeg(p.long, obs?.long, '做多', obs?.pnl?.long, p.margin_mode)}${positionLeg(p.short, obs?.short, '做空', obs?.pnl?.short, p.margin_mode)}</div>
              ${pnlSummary(p, obs, lastReport)}
              ${p.open_report ? `<details class="open-report-box"><summary>开仓执行：预估 vs 实际（锁定价差 ${pctRaw(num(p.open_report.actual_basis) * 100, 3)}，两腿间隔 ${secondsText(p.open_report.unhedged_ms)}）</summary>${openReportHtml(p.open_report)}</details>` : ''}
              <div class="pos-foot">
                ${p.strategy === 'spread'
                  ? `<span title="标记价基差只用来触发「基差收敛平仓」；触发后后台现拉两边盘口，整笔扣完手续费为正才平">标记价基差（触发参考）入场 <b>${pctRaw(p.entry_basis_pct, 3)}</b> → 当前 <b>${pctRaw(obs?.current_basis_pct, 3)}</b>${p.rules?.basis_exit_pct != null ? `（目标 ≤ ${num(p.rules.basis_exit_pct)}%）` : ''}</span>`
                  : ''}
                <span title="费差自动平仓按最近 6 小时的平均判断，一次瞬时波动不会平仓">当前费差年化 <b class="${cls(obs?.funding_apr)}">${obs ? pct(obs.funding_apr, 2) : '—'}</b>${obs?.funding_avg_apr != null ? `（近 6 小时均值 <b class="${cls(obs.funding_avg_apr)}">${pct(obs.funding_avg_apr, 2)}</b>）` : ''}</span>
                <span>两腿数量偏差 <b>${obs?.size_mismatch_pct == null ? '—' : `${num(obs.size_mismatch_pct).toFixed(3)}%`}</b></span>
                <span>${rulesText(p.rules, p.margin_mode)}</span>
                <span class="grow"></span>
                ${action ? `<span class="pill ${action.cls}">${action.label}${trim}</span><span class="small">${reason.replace(/^：/, '')}</span>` : ''}
              </div>
              ${notes}
              <div class="pos-actions">${positionActions(p)}</div>
              ${positionRulesEditor(p)}
            </div>`;
          })
          .join('')
      : `<div class="card pos-empty"><b>台账里没有持仓</b>在「策略」页选一对腿，在右侧「下单」里预览并开仓（或复制生成的 <code>arb-paper</code> 命令）。<br />台账：<code>${esc(data.ledger)}</code></div>`);
  wirePositionRules();
  if (focusedRule) $(focusedRule)?.focus({ preventScroll: true });

  // 全部折叠 / 展开：有两笔以上才有意义。
  $('pos-collapse-all').classList.toggle('hidden', open.length < 2);
  $('pos-expand-all').classList.toggle('hidden', open.length < 2);
  for (const button of $('pos-open').querySelectorAll('[data-collapse]')) {
    button.addEventListener('click', () => {
      toggleCollapsed(button.getAttribute('data-collapse'));
      renderPositions();
    });
  }

  for (const button of $('pos-open').querySelectorAll('[data-close]')) {
    button.addEventListener('click', () => {
      stopAddQuote();
      p.confirming = button.getAttribute('data-close');
      p.confirmKind = 'close';
      renderPositions();
    });
  }
  for (const button of $('pos-open').querySelectorAll('[data-add]')) {
    button.addEventListener('click', () => {
      const id = button.getAttribute('data-add');
      p.confirming = id;
      p.confirmKind = 'add';
      p.addSize = defaultAddSize((state.positions?.open || []).find((x) => x.id === id) || {});
      stopAddQuote();
      renderPositions();
      $('add-size')?.focus();
      refreshAddQuote();
    });
  }
  for (const button of $('pos-open').querySelectorAll('[data-close-go]')) {
    button.addEventListener('click', () => closePosition(button.getAttribute('data-close-go')));
  }
  for (const button of $('pos-open').querySelectorAll('[data-add-go]')) {
    button.addEventListener('click', () => addToPosition(button.getAttribute('data-add-go')));
  }
  for (const button of $('pos-open').querySelectorAll('[data-pos-cancel]')) {
    button.addEventListener('click', () => {
      stopAddQuote();
      p.confirming = null;
      p.confirmKind = null;
      renderPositions();
    });
  }
  $('add-size')?.addEventListener('input', (event) => {
    p.addSize = event.target.value;
    const go = $('pos-open').querySelector('[data-add-go]');
    const size = num(p.addSize);
    if (go) go.disabled = !(size !== null && size > 0) || p.busy;
    paintAddSpread();
  });
  $('add-size')?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') addToPosition(p.confirming);
  });

  const closed = data.closed || [];
  // 点一行展开它的双边明细；展开了哪几笔存在 state 里，自动刷新重画后不收起。
  const expanded = state.pos.closedOpen;
  $('pos-closed').innerHTML = closed
    .map(
      (p) => `<tr class="closed-row" data-closed-toggle="${esc(p.id)}" tabindex="0" aria-expanded="${expanded.has(p.id)}" title="点开看双边下单盈亏与手续费">
        <td class="dim"><span class="caret">${expanded.has(p.id) ? '▾' : '▸'}</span> ${esc(p.id)}</td>
        <td>${esc(p.symbol)}</td>
        <td>${p.long || p.short ? legsText(p.long?.venue, p.short?.venue) : '<span class="muted">两腿已退出</span>'}</td>
        <td><span class="pill status">${STATUS_LABEL[p.status] || esc(p.status)}</span>${closedTagsHtml(p)}</td>
        <td class="dim">${when(p.opened_at)}</td>
        <td class="dim">${when(p.closed_at)}</td>
        <td class="num">${realizedCell(p)}</td>
        <td class="small">${esc(p.note || '')}${p.pnl_unattributed ? esc(` 实际盈亏没能核出：${p.pnl_unattributed}`) : ''}</td>
        <td class="closed-action">${closedActionHtml(p)}</td>
      </tr>${expanded.has(p.id) ? `<tr class="closed-detail"><td colspan="9">${closedDetailHtml(p)}</td></tr>` : ''}`,
    )
    .join('');
  $('pos-closed-empty').textContent = closed.length ? '' : '还没有结束的仓位。';
  const tools = $('pos-closed-tools');
  tools.innerHTML = closedToolsHtml(data);
  tools.querySelector('[data-closed-clean]')?.addEventListener('click', () => dismissClosed([], { allFailed: true }));
  for (const button of $('pos-closed').querySelectorAll('[data-closed-dismiss]')) {
    // 按钮在可点开的行里：不能让点击 / 回车冒泡成「展开明细」。
    const stop = (event) => event.stopPropagation();
    button.addEventListener('keydown', stop);
    button.addEventListener('click', (event) => {
      stop(event);
      const id = button.getAttribute('data-closed-dismiss');
      dismissClosed([id], { traded: button.getAttribute('data-traded') === '1' });
    });
  }
  const pager = $('pos-closed-pager');
  pager.innerHTML = closedPagerHtml(data);
  pager.classList.toggle('hidden', !pager.innerHTML);
  for (const button of pager.querySelectorAll('[data-closed-page]')) {
    button.addEventListener('click', () => {
      state.pos.closedPage = Number(button.getAttribute('data-closed-page'));
      for (const b of pager.querySelectorAll('button')) b.disabled = true;
      loadPositions().catch((error) => {
        state.pos.flash = { error: true, html: `翻页没成功：${esc(error.message)}` };
        renderPositions();
      });
    });
  }
  for (const row of $('pos-closed').querySelectorAll('[data-closed-toggle]')) {
    const toggle = () => {
      const id = row.getAttribute('data-closed-toggle');
      if (expanded.has(id)) expanded.delete(id);
      else expanded.add(id);
      renderPositions();
    };
    row.addEventListener('click', toggle);
    row.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        toggle();
      }
    });
  }
}

// ───────────────────────────── 价差图表（参照 zuudot trading 页） ─────────────────────────────
//
// 一张图、两个窗格（lightweight-charts v5 的 pane，十字线与时间轴天然同步）：
// - 主图：两家 K 线（左轴）+ 价差（右轴，空腿 − 多腿，与 zuudot 的 long1_short2 同口径）+ 开 / 关仓参考线。
// - 资金费：两家已结算的资金费率（每周期原始费率 × 100，%）+ 结算点 + 按方向算的净资金费结算柱（左轴）。
// K 线来自 GET /api/spread-chart，资金费来自 GET /api/spread-chart/funding，各拉各的：一个失败不拖累另一个。
// 价差是成交价（K 线收盘）算的，不是盘口可成交价差。参考线与 zuudot 同一个公式：
// 关 = 价差的 SMA，开 = 关 + 2 × (两腿手续费) + 最小利润。

const CHART_VENUES = ['okx', 'binance', 'bybit', 'hyperliquid', 'hyperliquid-xyz', 'hyperliquid-io', 'lighter', 'lighter-rh', 'arcus', 'mexc', 'bitget', 'aster'];
const CHART_VENUE_NAME = {
  okx: 'OKX', binance: 'Binance', bybit: 'Bybit', hyperliquid: 'Hyperliquid', 'hyperliquid-xyz': 'HL-xyz',
  'hyperliquid-io': 'HL-io', lighter: 'Lighter', 'lighter-rh': 'Lighter RH', arcus: 'Arcus', mexc: 'MEXC', bitget: 'Bitget', aster: 'Aster',
};
const chartVenueName = (venue) => CHART_VENUE_NAME[venue] || venue;
// zuudot 链接里的交易所名：Hyperliquid 的子交易所只靠 instId 前缀（xyz: / io:）区分。
const ZUU_EXCHANGE = {
  okx: 'OKX', binance: 'Binance', bybit: 'Bybit', hyperliquid: 'Hyperliquid', 'hyperliquid-xyz': 'Hyperliquid',
  'hyperliquid-io': 'Hyperliquid', lighter: 'Lighter', 'lighter-rh': 'lighter-rh', arcus: 'Arcus',
};
// 周期：默认天数、最多天数、「再加载」一次加几天（与 zuudot 一致；它的 10 分钟这里是 15 分钟）。
const CHART_INTERVALS = [
  { name: '1m', label: '1分', minutes: 1, days: 16 / 24, max: 2, step: 1 },
  { name: '5m', label: '5分', minutes: 5, days: 3, max: 8, step: 2 },
  { name: '15m', label: '15分', minutes: 15, days: 6, max: 12, step: 3 },
  { name: '30m', label: '30分', minutes: 30, days: 12, max: 24, step: 6 },
  { name: '1h', label: '1小时', minutes: 60, days: 24, max: 48, step: 12 },
  { name: '4h', label: '4小时', minutes: 240, days: 48, max: 96, step: 24 },
];
const CHART_PARAMS_KEY = 'arb-web-chart-params';
// zuudot 的默认值：两腿手续费 0.05% / 0.055%、最小利润 0.5%、SMA 1000 分钟。
const CHART_FEE1 = 0.05;
const CHART_FEE2 = 0.055;
const CHART_MIN_PROFIT = 0.5;
const CHART_SMA_MINUTES = 1000;
const CHART_REFRESH_MS = 60000;
// 颜色与 zuudot 一致。
const CHART_COLOR = {
  spread: '#2563eb', openLine: '#2dd4bf', closeLine: '#fb923c', levels: '#f5a623',
  klineA: { up: 'rgba(22, 163, 74, 0.6)', down: 'rgba(220, 38, 38, 0.6)', line: 'rgba(22, 163, 74, 0.66)', dot: '#16a34a' },
  klineB: { up: 'rgba(14, 165, 233, 0.6)', down: 'rgba(147, 51, 234, 0.6)', line: 'rgba(14, 165, 233, 0.66)', dot: '#0ea5e9' },
  fundA: '#16a34a', fundB: '#0ea5e9', netPos: 'rgba(22, 163, 74, 0.85)', netNeg: 'rgba(220, 38, 38, 0.85)',
};
// 图层开关（图例上点一下切换）。
const CHART_LAYERS = [
  { key: 'spread', pane: 0, label: () => '价差', color: CHART_COLOR.spread },
  { key: 'openLine', pane: 0, label: () => '开仓参考线', color: CHART_COLOR.openLine },
  { key: 'closeLine', pane: 0, label: () => '关仓参考线', color: CHART_COLOR.closeLine },
  { key: 'levels', pane: 0, label: () => '最高 / 中位 / 最低', color: CHART_COLOR.levels },
  { key: 'klineA', pane: 0, label: (n) => `K线 · ${n.a}`, color: CHART_COLOR.klineA.dot },
  { key: 'klineB', pane: 0, label: (n) => `K线 · ${n.b}`, color: CHART_COLOR.klineB.dot },
  { key: 'fundA', pane: 1, label: (n) => `资金费 · ${n.a}`, color: CHART_COLOR.fundA },
  { key: 'fundB', pane: 1, label: (n) => `资金费 · ${n.b}`, color: CHART_COLOR.fundB },
  { key: 'net', pane: 1, label: () => '净资金费结算柱', color: CHART_COLOR.netPos, square: true },
  { key: 'marks', pane: 1, label: () => 'Funding 结算点', color: '#cbd5e1' },
];
const chartLayerDefaults = () => Object.fromEntries(CHART_LAYERS.map((l) => [l.key, l.key !== 'levels']));

function chartDefaults() {
  let saved = {};
  try {
    saved = JSON.parse(localStorage.getItem(CHART_PARAMS_KEY) || '{}') || {};
  } catch {
    saved = {};
  }
  const text = (v, fallback) => (typeof v === 'string' && v.trim() ? v.trim() : fallback);
  const layers = chartLayerDefaults();
  for (const key of Object.keys(layers)) if (typeof saved.layers?.[key] === 'boolean') layers[key] = saved.layers[key];
  return {
    symbol: text(saved.symbol, 'OPENAI'),
    a: CHART_VENUES.includes(saved.a) ? saved.a : 'okx',
    b: CHART_VENUES.includes(saved.b) ? saved.b : 'binance',
    ratio1: text(saved.ratio1, '1'),
    ratio2: text(saved.ratio2, '1'),
    interval: CHART_INTERVALS.some((i) => i.name === saved.interval) ? saved.interval : '5m',
    days: text(saved.days, ''),
    unit: saved.unit === 'diff' ? 'diff' : 'pct',
    kline: saved.kline === 'line' ? 'line' : 'candle',
    sma: text(saved.sma, ''),
    fee1: text(saved.fee1, ''),
    fee2: text(saved.fee2, ''),
    minProfit: text(saved.minProfit, ''),
    layers,
  };
}

const chartInterval = (name) => CHART_INTERVALS.find((i) => i.name === name) || CHART_INTERVALS[1];

// 这次要拉多少天：留空 = 这个周期的默认天数；超过上限夹到上限。
function chartDays(f) {
  const interval = chartInterval(f.interval);
  const days = num(f.days);
  return days && days > 0 ? Math.min(days, interval.max) : interval.days;
}

// 请求参数（纯函数，便于测试）。
function chartQuery(f) {
  const interval = chartInterval(f.interval);
  const q = new URLSearchParams({
    symbol: String(f.symbol || '').trim().toUpperCase(),
    exchange1: f.a,
    exchange2: f.b,
    interval: interval.name,
    days: String(chartDays(f)),
    ratio1: String(num(f.ratio1) || 1),
    ratio2: String(num(f.ratio2) || 1),
    direction: 'long1_short2',
  });
  return `/api/spread-chart?${q}`;
}

function fundingQuery(f) {
  const q = new URLSearchParams({
    symbol: String(f.symbol || '').trim().toUpperCase(),
    exchange1: f.a,
    exchange2: f.b,
    days: String(chartDays(f)),
    direction: 'long1_short2',
  });
  return `/api/spread-chart/funding?${q}`;
}

// 「再加载 N 天」「加载至 M 天」两个按钮（纯函数）：到了上限就不能再加。
function chartRangeButtons(f) {
  const interval = chartInterval(f.interval);
  const days = chartDays(f);
  const more = Math.min(interval.max, days + interval.step);
  const atMax = days >= interval.max - 1e-9;
  return [
    { days: more, label: `再加载 ${interval.step} 天`, disabled: atMax },
    { days: interval.max, label: `加载至 ${interval.max} 天`, disabled: atMax },
  ];
}

// zuudot 链接的交易所名 → 这里的场所。也认这里自己的短名（hyperliquid-xyz 等）。
function zuuVenue(exchange, instId) {
  const e = String(exchange || '').trim().toLowerCase().replace(/[\s_]+/g, '-');
  const inst = String(instId || '').trim();
  if (e === 'hyperliquid' || e === 'hl') {
    if (/^xyz:/i.test(inst)) return 'hyperliquid-xyz';
    if (/^io:/i.test(inst)) return 'hyperliquid-io';
    return 'hyperliquid';
  }
  if (e === 'lighterrh' || e === 'lighter-rh' || e === 'rh') return 'lighter-rh';
  return CHART_VENUES.includes(e) ? e : null;
}

// 从 instId 猜合约名：去掉 `xyz:` 这类前缀和 USDT / -USDT-SWAP / -USD 后缀。
const symbolFromInst = (inst) => String(inst || '').trim().replace(/^[a-z0-9]+:/i, '').replace(/(-USDT-SWAP|-USDT|-USD|USDT|USDC)$/i, '').toUpperCase();

// 解析 zuudot 风格的链接参数（/trading?symbol=…&exchange1=Binance&exchange2=Hyperliquid&instId2=xyz:…）。
// 这里的图永远是「A 多、B 空」：short1_long2 就把两边对调。认不出来返回 null。
function chartParamsFromUrl(search) {
  const q = new URLSearchParams(search || '');
  let a = zuuVenue(q.get('exchange1'), q.get('instId1'));
  let b = zuuVenue(q.get('exchange2'), q.get('instId2'));
  if (!a || !b) return null;
  const symbol = (q.get('symbol') || q.get('assetId') || symbolFromInst(q.get('instId1')) || symbolFromInst(q.get('instId2'))).trim().toUpperCase();
  if (!/^[A-Z0-9]+$/.test(symbol)) return null;
  let ratio1 = num(q.get('ratio1')) ? q.get('ratio1') : '1';
  let ratio2 = num(q.get('ratio2')) ? q.get('ratio2') : '1';
  if (q.get('direction') === 'short1_long2') {
    [a, b] = [b, a];
    [ratio1, ratio2] = [ratio2, ratio1];
  }
  const out = { symbol, a, b, ratio1, ratio2 };
  const interval = q.get('interval');
  if (CHART_INTERVALS.some((i) => i.name === interval)) out.interval = interval;
  return out;
}

// 这一家这个合约在接口里的名字（服务端回查失败时的猜测）。
function chartInstGuess(venue, base) {
  if (venue === 'binance' || venue === 'bybit') return `${base}USDT`;
  if (venue === 'okx') return `${base}-USDT-SWAP`;
  if (venue === 'hyperliquid-xyz') return `xyz:${base}`;
  if (venue === 'hyperliquid-io') return `io:${base}`;
  if (venue === 'arcus') return `${base}-USD`;
  return base;
}

// 当前图的分享链接（zuudot 同款参数，可以原样贴到 zuudot 或这里打开）。
function chartShareUrl(f, insts = []) {
  const base = String(f.symbol || '').trim().toUpperCase();
  const q = new URLSearchParams({
    symbol: base,
    exchange1: ZUU_EXCHANGE[f.a] || f.a,
    exchange2: ZUU_EXCHANGE[f.b] || f.b,
    instId1: insts[0] || chartInstGuess(f.a, base),
    instId2: insts[1] || chartInstGuess(f.b, base),
    ratio1: String(num(f.ratio1) || 1),
    ratio2: String(num(f.ratio2) || 1),
    direction: 'long1_short2',
    interval: chartInterval(f.interval).name,
  });
  return `/trading?${q}`;
}

// 开 / 关仓参考线（纯函数）：关 = 价差的 SMA（窗口按分钟换算成点数，开头不够就用已有的点），
// 开 = 关 + 2 × (费 1 + 费 2) + 最小利润。费率、利润都是 %；价格差口径按每点两边均价换算。
function chartRefLines(points, { unit = 'pct', intervalMinutes = 5, smaMinutes = CHART_SMA_MINUTES, fee1 = CHART_FEE1, fee2 = CHART_FEE2, minProfit = CHART_MIN_PROFIT } = {}) {
  const window = Math.max(1, Math.round(smaMinutes / intervalMinutes));
  const cost = (2 * (fee1 + fee2) + minProfit) / 100;
  const pick = (p) => (unit === 'diff' ? p.diff : p.pct);
  const close = [];
  const open = [];
  let sum = 0;
  points.forEach((p, i) => {
    sum += pick(p);
    if (i >= window) sum -= pick(points[i - window]);
    const sma = sum / Math.min(i + 1, window);
    const add = unit === 'diff' ? cost * (p.a + p.b) / 2 : cost * 100;
    close.push({ time: p.time, value: sma });
    open.push({ time: p.time, value: sma + add });
  });
  return { open, close, window, cost };
}

// 资金费副图的数据（纯函数）：结算点对齐到图的周期（同一根 K 线里留最后一次），末尾接上当前预测费率；
// 净结算柱同一根里求和。值都是 %（每周期原始费率 × 100）。
function chartFundingSeries(fund, intervalSec, nowSec) {
  const step = Math.max(60, intervalSec);
  const bucket = (sec) => sec - (((sec % step) + step) % step);
  const legs = (fund?.legs || []).map((leg) => {
    const byT = new Map();
    for (const s of leg.settlements || []) {
      if (Number.isFinite(s.rate)) byT.set(bucket(Math.floor(s.t / 1000)), s.rate * 100);
    }
    const marks = [...byT.keys()].sort((x, y) => x - y);
    const line = marks.map((time) => ({ time, value: byT.get(time) }));
    const rate = leg.current?.rate;
    if (Number.isFinite(rate)) {
      const time = bucket(nowSec);
      const last = line[line.length - 1];
      if (last && last.time === time) last.value = rate * 100;
      else if (!last || last.time < time) line.push({ time, value: rate * 100 });
    }
    return { venue: leg.venue, line, marks };
  });
  const netBy = new Map();
  for (const p of fund?.net || []) {
    if (!Number.isFinite(p.net)) continue;
    const time = bucket(Math.floor(p.t / 1000));
    netBy.set(time, (netBy.get(time) || 0) + p.net * 100);
  }
  const net = [...netBy.entries()]
    .sort((x, y) => x[0] - y[0])
    .map(([time, value]) => ({ time, value, color: value >= 0 ? CHART_COLOR.netPos : CHART_COLOR.netNeg }));
  return { legs, net };
}

const fmtTime = (sec) => new Date(sec * 1000).toISOString().slice(5, 16).replace('T', ' ');
const fmtFundPct = (rate, digits = 4) => (Number.isFinite(rate) ? `${rate >= 0 ? '' : '−'}${Math.abs(rate * 100).toFixed(digits)}%` : '—');
const signTone = (v) => (v > 0 ? 'pos' : v < 0 ? 'neg' : '');

// 434.3K / 1.2M 这种短写法（美元名义）。
function compactUsd(v) {
  const a = Math.abs(v);
  if (a >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (a >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return v.toFixed(0);
}

// 距结算还有多久：3h16m / 16m / 45s。
function fmtCountdown(ms) {
  if (!Number.isFinite(ms)) return '—';
  if (ms <= 0) return '结算中';
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  if (h > 0) return `${h}h${String(m).padStart(2, '0')}m`;
  if (m > 0) return `${m}m`;
  return `${total}s`;
}

// 顶部资金费条：每家现在的费率、周期、距结算；最后是快照多久前（纯函数，nowMs 由调用方给）。
function chartFundingStripHtml(fund, nowMs, loadedAtMs = nowMs) {
  if (!fund) return '<span class="muted small">资金费 —</span>';
  const legs = (fund.legs || []).map((leg) => {
    const c = leg.current;
    const meta = [leg.interval_h ? `${leg.interval_h}h` : null, c ? `结算${fmtCountdown(c.next_ms - nowMs)}` : null].filter(Boolean).join(' · ');
    return `<span class="zc-fund-leg"><b>${esc(chartVenueName(leg.venue))}</b> <b class="mono ${c ? signTone(c.rate) : ''}">${c ? fmtFundPct(c.rate) : '—'}</b> <span class="muted mono">${esc(meta)}</span></span>`;
  });
  const ages = (fund.legs || []).map((l) => l.current?.age_sec).filter(Number.isFinite);
  const money = (v) => (Number.isFinite(v) ? compactUsd(v) : '—');
  const books = (fund.legs || []).filter((l) => Number.isFinite(l.current?.volume_24h) || Number.isFinite(l.current?.open_interest))
    .map((l) => `<span class="zc-fund-leg muted small"><b>${esc(chartVenueName(l.venue))}</b> 24h ${money(l.current.volume_24h)} · 持仓 ${money(l.current.open_interest)}</span>`);
  const age = ages.length ? Math.max(...ages) + Math.max(0, Math.round((nowMs - loadedAtMs) / 1000)) : null;
  return `<span class="muted small">资金费</span>${legs.join('')}${age === null ? '' : `<span class="muted small">${age} 秒前</span>`}`
    + (books.length ? `<span class="zc-break"></span><span class="muted small">成交额 / 持仓量</span>${books.join('')}` : '');
}

// 资金费副图下面的合计（纯函数）。
function chartFundingSummaryHtml(fund) {
  if (!fund) return '';
  const name = (venue) => esc(chartVenueName(venue));
  const out = [];
  for (const leg of fund.legs || []) {
    if (leg.error) out.push(`<div class="notice error">${name(leg.venue)} 资金费历史拉不到：${esc(leg.error)}</div>`);
  }
  const s = fund.summary;
  const kind = (k) => (k === 'both' ? '两家同时结算' : k === 'long' ? `仅 ${name(fund.long)} 结算` : `仅 ${name(fund.short)} 结算`);
  if (s) {
    const days = s.days >= 1 ? `${Number(s.days.toFixed(2))} 天` : `${Math.round(s.days * 24)} 小时`;
    // 一家历史更短（上新不久、OKX 只给约 3 个月）：只算两家都有数据的那段。
    const span = Number.isFinite(fund.days) && s.days < fund.days * 0.99 ? `两家都有数据的 ${days}` : `所选 ${days}`;
    out.push(`<div>${span}内结算 <b>${s.count}</b> 次（同时 ${s.both} · 仅 ${name(fund.long)} ${s.long_only} · 仅 ${name(fund.short)} ${s.short_only}），`
      + `按「${name(fund.long)} 多 / ${name(fund.short)} 空」净资金费合计 <b class="mono ${signTone(s.total)}">${fmtFundPct(s.total)}</b>`
      + `（年化 <b class="mono ${signTone(s.apr)}">${fmtFundPct(s.apr, 2)}</b>，单利、相对单腿名义）</div>`);
  }
  const n = fund.next;
  if (n) {
    out.push(`<div>下次结算 ${esc(fmtTime(n.t / 1000))} UTC：${kind(n.kind)}，按现在的预测费率净 <b class="mono ${signTone(n.net)}">${fmtFundPct(n.net)}</b></div>`);
  }
  const notes = ['历史点是各家已结算的费率（每个结算周期的原始费率，不折算），最后一点是现在的预测费率（下次结算适用）。', ...(fund.notes || [])];
  out.push(`<div class="muted">${notes.map(esc).join(' ')}</div>`);
  return out.join('');
}

// 统计卡片：最高、最低、中位数、当前与当前分位。按显示单位给数。
function chartStatsHtml(data, unit) {
  const c = data.chart;
  const s = c.stats;
  if (!s) return '<div class="stat"><span>历史</span><b class="dim">两家没有能对上的 K 线</b></div>';
  const at = (sec) => c.points.find((p) => p.time === sec);
  const show = (pctValue, sec) => {
    const p = at(sec);
    if (unit === 'diff' && p) return `${p.diff >= 0 ? '+' : ''}${p.diff.toFixed(4)}`;
    return `${pctValue >= 0 ? '+' : ''}${pctValue.toFixed(3)}%`;
  };
  const range = unit === 'diff'
    ? (() => { const vals = c.points.map((p) => p.diff); return `${(Math.max(...vals) - Math.min(...vals)).toFixed(4)}`; })()
    : `${(s.max_pct - s.min_pct).toFixed(3)}%`;
  const tone = s.last_percentile >= 80 ? 'pos' : s.last_percentile <= 20 ? 'neg' : '';
  return [
    ['最高', `${show(s.max_pct, s.max_at)}`, `${fmtTime(s.max_at)}`],
    ['最低', `${show(s.min_pct, s.min_at)}`, `${fmtTime(s.min_at)}`],
    ['高低差', range, `${s.points} 根`],
    ['中位数', `${s.median_pct >= 0 ? '+' : ''}${s.median_pct.toFixed(3)}%`, `p10 ${s.p10_pct.toFixed(3)}% · p90 ${s.p90_pct.toFixed(3)}%`],
    ['当前', show(s.last_pct, s.last_at), fmtTime(s.last_at)],
    ['当前分位', `${s.last_percentile.toFixed(0)}%`, s.last_percentile >= 80 ? '价差偏高：适合开' : s.last_percentile <= 20 ? '价差偏低：开了没空间' : '半山腰'],
  ].map(([label, value, sub], i) => `<div class="stat"><span>${label}</span><b class="${i === 5 ? tone : ''}">${esc(value)}</b><span class="small muted">${esc(sub)}</span></div>`).join('');
}

// 图例：一排可点的图层开关，下面一行是十字线所在时刻的读数（纯函数）。
function chartLegendHtml(pane, layers, names, values = []) {
  const items = CHART_LAYERS.filter((l) => l.pane === pane).map((l) => {
    const on = layers[l.key] !== false;
    return `<button type="button" class="zc-leg${on ? '' : ' off'}" data-layer="${l.key}" title="${on ? '隐藏' : '显示'}"><i class="${l.square ? 'sq' : ''}" style="background:${l.color}"></i>${esc(l.label(names))}</button>`;
  });
  const vals = values.filter(Boolean).map(([label, value, tone, color]) => `<span><i style="background:${color || 'transparent'}"></i>${esc(label)} <b class="mono ${tone || ''}">${esc(value)}</b></span>`);
  return `<div class="zc-leg-row">${items.join('')}</div>${vals.length ? `<div class="zc-vals">${vals.join('')}</div>` : ''}`;
}

// 价格精度：按价格量级给小数位。
function chartPricePrecision(price) {
  const p = Math.abs(price || 0);
  if (p >= 1000) return 2;
  if (p >= 10) return 3;
  if (p >= 1) return 4;
  if (p >= 0.01) return 6;
  return 8;
}

// 时间上 ≤ t 的最后一个点（数据按时间升序）。
function chartValueAt(series, t) {
  let lo = 0;
  let hi = series.length - 1;
  let found = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (series[mid].time <= t) {
      found = series[mid];
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return found;
}

function chartFormFromInputs() {
  const f = state.chart.form;
  f.symbol = $('chart-symbol').value.trim().toUpperCase() || f.symbol;
  f.a = $('chart-a').value;
  f.b = $('chart-b').value;
  f.ratio1 = $('chart-ratio1').value;
  f.ratio2 = $('chart-ratio2').value;
  f.sma = $('chart-sma').value;
  f.fee1 = $('chart-fee1').value;
  f.fee2 = $('chart-fee2').value;
  f.minProfit = $('chart-min-profit').value;
  return f;
}

function saveChartForm() {
  try {
    localStorage.setItem(CHART_PARAMS_KEY, JSON.stringify(state.chart.form));
  } catch {
    // 存不了就只在本次会话里生效。
  }
}

function fillChartControls(f = state.chart.form) {
  const opt = (v) => `<option value="${esc(v)}">${esc(chartVenueName(v))}</option>`;
  for (const id of ['chart-a', 'chart-b']) {
    if (!$(id).options.length) $(id).innerHTML = CHART_VENUES.map(opt).join('');
  }
  $('chart-symbol').value = f.symbol;
  $('chart-a').value = f.a;
  $('chart-b').value = f.b;
  $('chart-ratio1').value = f.ratio1;
  $('chart-ratio2').value = f.ratio2;
  $('chart-sma').value = f.sma;
  $('chart-fee1').value = f.fee1;
  $('chart-fee2').value = f.fee2;
  $('chart-min-profit').value = f.minProfit;
  renderChartToolbar();
}

// 工具栏：方向、历史范围、周期、K 线样式、单位。
function renderChartToolbar() {
  const f = state.chart.form;
  $('chart-dir').innerHTML = `<span class="muted small">价差 方向</span> <b class="pos">${esc(chartVenueName(f.a))} 多</b> <span class="muted">/</span> <b class="neg">${esc(chartVenueName(f.b))} 空</b>`;
  const days = chartDays(f);
  $('chart-range').innerHTML = `<span class="muted small">当前 ${days >= 1 ? `${Number(days.toFixed(2))} 天` : `${Math.round(days * 24)} 小时`}</span>`
    + chartRangeButtons(f).map((b) => `<button type="button" data-days="${b.days}"${b.disabled ? ' disabled' : ''}>${esc(b.label)}</button>`).join('');
  $('chart-interval').innerHTML = CHART_INTERVALS.map((i) => `<button type="button" data-interval="${i.name}" class="${i.name === f.interval ? 'on' : ''}">${esc(i.label)}</button>`).join('');
  for (const button of $('chart-kline').querySelectorAll('button')) button.classList.toggle('on', button.dataset.kline === f.kline);
  for (const button of $('chart-unit').querySelectorAll('button')) button.classList.toggle('on', button.dataset.unit === f.unit);
  const fee = (i) => state.chart.fund?.legs?.[i]?.current?.taker_fee;
  $('chart-fee1').placeholder = String(Number.isFinite(fee(0)) ? Number((fee(0) * 100).toFixed(4)) : CHART_FEE1);
  $('chart-fee2').placeholder = String(Number.isFinite(fee(1)) ? Number((fee(1) * 100).toFixed(4)) : CHART_FEE2);
  $('chart-sma').placeholder = String(CHART_SMA_MINUTES);
  $('chart-min-profit').placeholder = String(CHART_MIN_PROFIT);
  $('chart-fee1-label').textContent = `${chartVenueName(f.a)} 手续费 %`;
  $('chart-fee2-label').textContent = `${chartVenueName(f.b)} 手续费 %`;
}

// 参考线用的参数：没填就用扫描快照里的吃单费率，再没有用 zuudot 的默认值。
function chartRefParams(f) {
  const fee = (i, fallback) => {
    const typed = num(f[`fee${i + 1}`]);
    if (typed !== null && typed >= 0) return typed;
    const snap = state.chart.fund?.legs?.[i]?.current?.taker_fee;
    return Number.isFinite(snap) ? snap * 100 : fallback;
  };
  const sma = num(f.sma);
  const minProfit = num(f.minProfit);
  return {
    unit: f.unit,
    intervalMinutes: chartInterval(f.interval).minutes,
    smaMinutes: sma && sma > 0 ? sma : CHART_SMA_MINUTES,
    fee1: fee(0, CHART_FEE1),
    fee2: fee(1, CHART_FEE2),
    minProfit: minProfit !== null && minProfit >= 0 ? minProfit : CHART_MIN_PROFIT,
  };
}

async function loadChart({ silent = false, keepRange = false } = {}) {
  const c = state.chart;
  // 后台刷新只重拉已加载的那组，不读输入框：用户可能正改到一半还没点「加载」。
  const f = silent ? c.form : chartFormFromInputs();
  if (!silent) saveChartForm();
  if (f.a === f.b) {
    $('chart-status').textContent = '两边不能是同一家';
    return;
  }
  const seq = ++c.seq;
  c.loading = true;
  if (!silent) $('chart-status').textContent = '加载中…（两家 K 线和资金费各拉一遍，几秒钟）';
  const [k, fu] = await Promise.all([api(chartQuery(f)), api(fundingQuery(f))]);
  if (seq !== c.seq) return;
  c.loading = false;
  c.key = `${f.symbol}|${f.a}|${f.b}|${f.interval}|${chartDays(f)}|${f.ratio1}|${f.ratio2}`;
  c.data = k.ok ? k.body : null;
  c.error = k.ok ? null : k.body.error || '加载失败';
  c.fund = fu.ok ? fu.body : null;
  c.fundError = fu.ok ? null : fu.body.error || '资金费加载失败';
  c.loadedAt = Date.now();
  const auto = chartAutoRefreshMs(f) === null ? ' · 含 Lighter：不自动刷新，点「加载」更新' : '';
  $('chart-status').textContent = c.data
    ? `${chartVenueName(c.data.long)} ${c.data.chart.a_bars} 根 · ${chartVenueName(c.data.short)} ${c.data.chart.b_bars} 根 · 对上 ${c.data.chart.points.length} 根 · ${new Date(c.loadedAt).toLocaleTimeString()} 更新${auto}`
    : '';
  if (state.page === 'chart') {
    const insts = (c.fund?.legs || []).map((l) => l.inst_id);
    history.replaceState(null, '', `${chartShareUrl(f, insts)}#chart`);
  }
  renderChartToolbar();
  renderChart({ keepRange });
}

function renderChartHead() {
  const c = state.chart;
  const f = c.form;
  const base = String(f.symbol || '').toUpperCase();
  const insts = [0, 1].map((i) => c.fund?.legs?.[i]?.inst_id || chartInstGuess(i ? f.b : f.a, base));
  $('chart-title').textContent = base || '—';
  $('chart-insts').textContent = `${chartVenueName(f.a)}: ${insts[0]} / ${chartVenueName(f.b)}: ${insts[1]}`;
  $('chart-funding-strip').innerHTML = c.fundError
    ? `<span class="muted small">资金费</span><span class="neg small">${esc(c.fundError)}</span>`
    : chartFundingStripHtml(c.fund, Date.now(), c.loadedAt || Date.now());
}

// 净资金费结算柱（lightweight-charts 自定义序列）：柱宽不随 K 线间距变窄（至少 6px，否则 5 分钟线上
// 每小时一根的结算柱只有 1px，看不见），并画一条淡淡的零线。与 zuudot 的画法一致。
class ChartNetBars {
  constructor(LW) {
    this.LW = LW;
    this.data = null;
  }

  renderer() {
    return { draw: (target, priceToCoordinate) => this.draw(target, priceToCoordinate) };
  }

  update(data) {
    this.data = data;
  }

  priceValueBuilder(item) {
    return [0, item.value];
  }

  isWhitespace(item) {
    return !Number.isFinite(item.value);
  }

  defaultOptions() {
    return { ...this.LW.customSeriesDefaultOptions, color: CHART_COLOR.netPos };
  }

  draw(target, priceToCoordinate) {
    const zero = priceToCoordinate(0);
    if (zero === null || !this.data?.visibleRange) return;
    target.useBitmapCoordinateSpace(({ context, bitmapSize, horizontalPixelRatio: hr, verticalPixelRatio: vr }) => {
      const z = Math.round(zero * vr);
      context.save();
      context.globalAlpha = 0.4;
      context.fillStyle = '#94a3b8';
      context.fillRect(0, z, bitmapSize.width, Math.max(1, Math.round(vr)));
      context.restore();
      const half = (Math.max(6, this.data.barSpacing * 0.88) * hr) / 2;
      const { from, to } = this.data.visibleRange;
      for (let i = from; i < to; i += 1) {
        const bar = this.data.bars[i];
        const y = priceToCoordinate(bar.originalData.value);
        if (y === null) continue;
        const top = Math.round(Math.min(y, zero) * vr);
        const height = Math.max(1, Math.round(Math.abs(y - zero) * vr));
        // 按正负取色：lightweight-charts 不把自定义序列数据里的 color 原样留在 originalData 里。
        context.fillStyle = bar.originalData.value >= 0 ? CHART_COLOR.netPos : CHART_COLOR.netNeg;
        context.fillRect(Math.round(bar.x * hr - half), top, Math.max(1, Math.round(half * 2)), height);
      }
    });
  }
}

function renderChart({ keepRange = false } = {}) {
  const c = state.chart;
  const f = c.form;
  const data = c.data;
  const fund = c.fund;
  renderChartHead();
  $('chart-stats').innerHTML = data ? chartStatsHtml(data, f.unit) : '';
  $('chart-notes').innerHTML = [
    c.error ? `<div class="notice error">K 线：${esc(c.error)}</div>` : '',
    ...(data?.chart?.notes || []).map((n) => `<div class="notice">${esc(n)}</div>`),
  ].join('');
  $('chart-funding-summary').innerHTML = c.fundError ? `<div class="notice error">资金费：${esc(c.fundError)}</div>` : chartFundingSummaryHtml(fund);
  let range = null;
  if (c.main) {
    if (keepRange) range = c.main.timeScale().getVisibleLogicalRange();
    c.main.remove();
    c.main = null;
  }
  if (c.resize) {
    c.resize.disconnect();
    c.resize = null;
  }
  $('chart-legend').innerHTML = '';
  $('chart-funding-legend').innerHTML = '';
  if (!data && !fund) return;
  const LW = globalThis.LightweightCharts;
  if (!LW) {
    $('chart-notes').innerHTML += '<div class="notice error">图表库没加载上（/vendor/lightweight-charts.js）</div>';
    return;
  }
  const layers = f.layers;
  const names = { a: chartVenueName(data?.long || fund?.long || f.a), b: chartVenueName(data?.short || fund?.short || f.b) };
  const root = getComputedStyle(document.documentElement);
  const textColor = root.getPropertyValue('--text-3').trim() || '#9aa4b2';
  const gridColor = 'rgba(128, 128, 128, 0.12)';
  const chart = LW.createChart($('chart-main'), {
    autoSize: true,
    layout: {
      background: { color: 'transparent' },
      textColor,
      fontSize: 11,
      panes: { separatorColor: 'rgba(128, 128, 128, 0.3)', separatorHoverColor: 'rgba(74, 159, 200, 0.35)', enableResize: true },
    },
    grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
    crosshair: { mode: LW.CrosshairMode.Normal },
    timeScale: { timeVisible: true, secondsVisible: false, rightOffset: 4, borderVisible: false },
    rightPriceScale: { visible: true, borderVisible: false },
    leftPriceScale: { visible: true, borderVisible: false },
  });
  c.main = chart;

  const pts = data?.chart?.points || [];
  const unitFmt = f.unit === 'diff' ? (v) => v.toFixed(4) : (v) => `${v.toFixed(3)}%`;
  const spreadFormat = { type: 'custom', formatter: unitFmt, minMove: f.unit === 'diff' ? 0.0001 : 0.001 };
  const pricePrecision = chartPricePrecision(pts[pts.length - 1]?.a);
  const priceFormat = { type: 'price', precision: pricePrecision, minMove: 10 ** -pricePrecision };
  const lookups = [];

  // 主图：两家 K 线（左轴）。
  for (const [key, candles, color] of [['klineA', data?.candles_a, CHART_COLOR.klineA], ['klineB', data?.candles_b, CHART_COLOR.klineB]]) {
    if (!candles?.length || !layers[key]) continue;
    const label = key === 'klineA' ? names.a : names.b;
    if (f.kline === 'candle') {
      const s = chart.addSeries(LW.CandlestickSeries, {
        priceScaleId: 'left', upColor: color.up, downColor: color.down, borderVisible: false,
        wickUpColor: color.up, wickDownColor: color.down, priceFormat, lastValueVisible: true, priceLineVisible: false, title: label,
      }, 0);
      s.setData(candles);
      lookups.push({ pane: 0, label, color: color.dot, data: candles, value: (p) => p.close, fmt: (v) => v.toFixed(pricePrecision), exact: true });
    } else {
      const line = candles.map((x) => ({ time: x.time, value: x.close }));
      const s = chart.addSeries(LW.LineSeries, { priceScaleId: 'left', color: color.line, lineWidth: 1, priceFormat, priceLineVisible: false, title: label }, 0);
      s.setData(line);
      lookups.push({ pane: 0, label, color: color.dot, data: line, value: (p) => p.value, fmt: (v) => v.toFixed(pricePrecision), exact: true });
    }
  }
  // 主图：价差与参考线（右轴）。
  if (pts.length) {
    const pick = (p) => (f.unit === 'diff' ? p.diff : p.pct);
    const ref = chartRefLines(pts, chartRefParams(f));
    if (layers.spread) {
      const line = pts.map((p) => ({ time: p.time, value: pick(p) }));
      const s = chart.addSeries(LW.LineSeries, { priceScaleId: 'right', color: CHART_COLOR.spread, lineWidth: 2, priceFormat: spreadFormat, priceLineVisible: false, title: '价差' }, 0);
      s.setData(line);
      const st = data.chart.stats;
      if (layers.levels && st) {
        const at = (sec) => pts.find((p) => p.time === sec);
        const level = (pctValue, sec) => (f.unit === 'diff' ? (at(sec)?.diff ?? 0) : pctValue);
        const median = f.unit === 'diff' ? (() => { const v = pts.map((p) => p.diff).sort((x, y) => x - y); return v[Math.floor(v.length / 2)]; })() : st.median_pct;
        for (const [price, title, color] of [[level(st.max_pct, st.max_at), '最高', '#26a69a'], [median, '中位数', CHART_COLOR.levels], [level(st.min_pct, st.min_at), '最低', '#ef5350']]) {
          s.createPriceLine({ price, color, lineWidth: 1, lineStyle: LW.LineStyle.Dashed, axisLabelVisible: true, title });
        }
      }
      lookups.push({ pane: 0, label: '价差', color: CHART_COLOR.spread, data: line, value: (p) => p.value, fmt: unitFmt, exact: true, tone: true });
    }
    for (const [key, series, title] of [['openLine', ref.open, '开'], ['closeLine', ref.close, '关']]) {
      if (!layers[key]) continue;
      const s = chart.addSeries(LW.LineSeries, { priceScaleId: 'right', color: CHART_COLOR[key], lineWidth: 1, priceFormat: spreadFormat, priceLineVisible: false, crosshairMarkerVisible: false, title }, 0);
      s.setData(series);
      lookups.push({ pane: 0, label: key === 'openLine' ? '开仓参考' : '关仓参考', color: CHART_COLOR[key], data: series, value: (p) => p.value, fmt: unitFmt, exact: true });
    }
  }
  // 资金费窗格：两家费率（右轴）、结算点、净结算柱（左轴）。
  const intervalSec = chartInterval(f.interval).minutes * 60;
  const fs = chartFundingSeries(fund, intervalSec, Math.floor((c.loadedAt || Date.now()) / 1000));
  const fundFmt = (v) => `${v.toFixed(4)}%`;
  const fundFormat = { type: 'custom', formatter: fundFmt, minMove: 0.0001 };
  let fundingPane = false;
  fs.legs.forEach((leg, i) => {
    const key = i === 0 ? 'fundA' : 'fundB';
    if (!layers[key] || !leg.line.length) return;
    const color = CHART_COLOR[key];
    const s = chart.addSeries(LW.LineSeries, { priceScaleId: 'right', color, lineWidth: 2, priceFormat: fundFormat, priceLineVisible: false, title: '' }, 1);
    s.setData(leg.line);
    if (layers.marks && leg.marks.length) {
      LW.createSeriesMarkers(s, leg.marks.map((time) => ({ time, position: 'inBar', shape: 'circle', color, size: 0.5 })));
    }
    fundingPane = true;
    lookups.push({ pane: 1, label: i === 0 ? names.a : names.b, color, data: leg.line, value: (p) => p.value, fmt: fundFmt, exact: false, tone: true });
  });
  if (layers.net && fs.net.length) {
    const s = typeof chart.addCustomSeries === 'function'
      ? chart.addCustomSeries(new ChartNetBars(LW), { priceScaleId: 'left', priceFormat: fundFormat, priceLineVisible: false, lastValueVisible: false }, 1)
      : chart.addSeries(LW.HistogramSeries, { priceScaleId: 'left', priceFormat: fundFormat, priceLineVisible: false, lastValueVisible: false, base: 0 }, 1);
    s.setData(fs.net);
    fundingPane = true;
    lookups.push({ pane: 1, label: '净', color: CHART_COLOR.netPos, data: fs.net, value: (p) => p.value, fmt: fundFmt, exact: true, tone: true });
  }
  const panes = chart.panes();
  if (panes.length > 1) {
    panes[0].setStretchFactor(3);
    panes[1].setStretchFactor(2);
  }
  if (range) chart.timeScale().setVisibleLogicalRange(range);
  else chart.timeScale().fitContent();

  // 图例与读数。
  const lastTime = Math.max(0, ...lookups.map((l) => l.data[l.data.length - 1]?.time || 0));
  const legend = (time) => {
    const t = time ?? lastTime;
    const values = (pane) => [
      pane === 0 ? ['', `${fmtTime(t)} UTC`, '', null] : null,
      ...lookups.filter((l) => l.pane === pane).map((l) => {
        const p = l.exact ? l.data.find((x) => x.time === t) || (time === undefined || time === null ? l.data[l.data.length - 1] : null) : chartValueAt(l.data, t);
        if (!p) return null;
        const v = l.value(p);
        return [l.label, l.fmt(v), l.tone ? signTone(v) : '', l.color];
      }),
    ];
    $('chart-legend').innerHTML = chartLegendHtml(0, layers, names, values(0));
    $('chart-funding-legend').innerHTML = fund || c.fundError ? chartLegendHtml(1, layers, names, values(1)) : '';
    placeFundingLegend();
  };
  const placeFundingLegend = () => {
    const ps = chart.panes();
    const el = $('chart-funding-legend');
    el.hidden = ps.length < 2 && !fund;
    el.style.top = ps.length > 1 ? `${ps[0].getHeight() + 6}px` : 'auto';
    el.style.bottom = ps.length > 1 ? 'auto' : '30px';
  };
  chart.subscribeCrosshairMove((param) => legend(param.time));
  legend(null);
  if (!fundingPane && fund) placeFundingLegend();
  c.resize = new ResizeObserver(() => placeFundingLegend());
  c.resize.observe($('chart-main'));
}

// 图例上的图层开关。
function toggleChartLayer(key) {
  const layers = state.chart.form.layers;
  if (!(key in layers)) return;
  layers[key] = !layers[key];
  saveChartForm();
  renderChart({ keepRange: true });
}

// 自动重拉的间隔（纯函数）：至少 1 分钟、至少一根 K 线；有一腿是 Lighter 就不自动重拉（null）——
// 它的公开额度很紧，而且和实盘交易共用同一个 IP，K 线一拉就是好几页。
function chartAutoRefreshMs(f) {
  if ([f.a, f.b].some((v) => v === 'lighter' || v === 'lighter-rh')) return null;
  return Math.max(CHART_REFRESH_MS, chartInterval(f.interval).minutes * 60000);
}

// 图表页：按 chartAutoRefreshMs 重拉（服务端 K 线缓存 1 分钟、资金费缓存 5 分钟），资金费条每秒走一次倒计时。
function scheduleChart() {
  const c = state.chart;
  clearInterval(c.timer);
  clearInterval(c.tick);
  c.timer = null;
  c.tick = null;
  if (state.page !== 'chart') return;
  c.timer = setInterval(() => {
    const every = c.form ? chartAutoRefreshMs(c.form) : null;
    if (every === null || document.hidden || state.page !== 'chart' || c.loading || !(c.data || c.fund)) return;
    if (Date.now() - c.loadedAt < every - 1000) return;
    loadChart({ silent: true, keepRange: true }).catch(() => {});
  }, CHART_REFRESH_MS);
  c.tick = setInterval(() => {
    if (!document.hidden && state.page === 'chart' && c.fund) renderChartHead();
  }, 1000);
}

function wireChart() {
  state.chart.form = state.chart.form || chartDefaults();
  fillChartControls(state.chart.form);
  $('chart-load').addEventListener('click', () => loadChart());
  $('chart-symbol').addEventListener('keydown', (event) => {
    if (event.key === 'Enter') loadChart();
  });
  $('chart-swap').addEventListener('click', () => {
    const f = chartFormFromInputs();
    [f.a, f.b] = [f.b, f.a];
    [f.ratio1, f.ratio2] = [f.ratio2, f.ratio1];
    [f.fee1, f.fee2] = [f.fee2, f.fee1];
    fillChartControls(f);
    loadChart();
  });
  $('chart-interval').addEventListener('click', (event) => {
    const button = event.target.closest('button[data-interval]');
    if (!button) return;
    state.chart.form.interval = button.dataset.interval;
    state.chart.form.days = '';
    renderChartToolbar();
    loadChart();
  });
  $('chart-range').addEventListener('click', (event) => {
    const button = event.target.closest('button[data-days]');
    if (!button || button.disabled) return;
    state.chart.form.days = button.dataset.days;
    renderChartToolbar();
    loadChart();
  });
  for (const [id, field] of [['chart-kline', 'kline'], ['chart-unit', 'unit']]) {
    $(id).addEventListener('click', (event) => {
      const button = event.target.closest('button');
      if (!button) return;
      state.chart.form[field] = button.dataset[field];
      saveChartForm();
      renderChartToolbar();
      renderChart({ keepRange: true });
    });
  }
  for (const id of ['chart-sma', 'chart-fee1', 'chart-fee2', 'chart-min-profit']) {
    $(id).addEventListener('change', () => {
      chartFormFromInputs();
      saveChartForm();
      renderChart({ keepRange: true });
    });
  }
  $('chart-share').addEventListener('click', async () => {
    const f = chartFormFromInputs();
    const url = new URL(chartShareUrl(f, (state.chart.fund?.legs || []).map((l) => l.inst_id)), location.href).href;
    try {
      await navigator.clipboard.writeText(url);
      $('chart-status').textContent = '链接已复制（zuudot 同款参数）';
    } catch {
      $('chart-status').textContent = url;
    }
  });
  for (const id of ['chart-legend', 'chart-funding-legend']) {
    $(id).addEventListener('click', (event) => {
      const button = event.target.closest('button[data-layer]');
      if (button) toggleChartLayer(button.dataset.layer);
    });
  }
}

// 从价差监控页的一行跳过来：带上这组的两家与合约，多 = 现在建议做多的那家。
function openChartFor(line) {
  const legs = rhLegs(line) || { long: line.a, short: line.b };
  Object.assign(state.chart.form, { symbol: line.base, a: legs.long, b: legs.short, ratio1: '1', ratio2: '1', fee1: '', fee2: '' });
  fillChartControls(state.chart.form);
  state.chart.data = null;
  state.chart.fund = null;
  showPage('chart');
}

// ───────────────────────────── 价差监控（RH 价差页） ─────────────────────────────
//
// 按「组」监控：每组两家 a / b，基差 = (a − b) / 均值，方向 long_a = 多 a 空 b、long_b = 多 b 空 a。

const VENUE_LABEL = {
  arcus: 'Arcus', 'lighter-rh': 'RH', hyperliquid: 'HL', 'hyperliquid-xyz': 'HL-xyz', 'hyperliquid-io': 'HL-io', binance: 'BN',
  lighter: 'Lighter', gate: 'Gate', bitget: 'Bitget', ondo: 'Ondo', popdex: 'PopDEX', mexc: 'MEXC', aster: 'Aster',
};
const venueLabel = (venue) => VENUE_LABEL[venue] || venue;
const pairLabel = (line) => `${venueLabel(line.a)} ↔ ${venueLabel(line.b)}`;
// 自动交易做哪些组：两家都是实盘已连接的场所（纸面模式下全部组）。与服务端同一口径。
function rhAutoTradable(pair) {
  const mode = state.rhAuto?.data?.settings?.mode;
  if (mode === 'paper') return true;
  const venues = state.tradeConfig?.live?.venues || [];
  return venues.includes(pair.a) && venues.includes(pair.b);
}

// 一个方向的两条腿：{ long, short }。
function rhLegs(line, direction = line.best?.direction) {
  if (direction === 'long_a') return { long: line.a, short: line.b };
  if (direction === 'long_b') return { long: line.b, short: line.a };
  return null;
}

function rhDirectionText(line) {
  const legs = rhLegs(line);
  return legs ? `多 ${venueLabel(legs.long)} / 空 ${venueLabel(legs.short)}` : null;
}

// 一个方向的数字：可成交价差、收敛到 0、回到正常。
function rhBestLeg(line) {
  const direction = line.best?.direction;
  return direction ? line[direction] : null;
}

// 只观察的行（Binance）：挂单测算里更好的那个方向（按「回到正常」吃单退出的净收益，没有就按入场价差）。
function rhMakerBest(line) {
  const maker = line.maker;
  if (!maker) return null;
  const score = (leg) => (leg ? num(leg.net_to_normal_taker_exit_pct) ?? num(leg.entry_pct) : null);
  const options = [['long_a', maker.long_a], ['long_b', maker.long_b]].filter(([, leg]) => leg);
  if (!options.length) return null;
  options.sort((x, y) => (score(y[1]) ?? -Infinity) - (score(x[1]) ?? -Infinity));
  const [direction, leg] = options[0];
  const legs = rhLegs(line, direction);
  const side = legs.long === maker.venue ? '多' : '空';
  return { direction, leg, legs, side, maker };
}

function rhMakerText(line) {
  const best = rhMakerBest(line);
  if (!best) return '';
  const { leg, side, maker } = best;
  const nets = leg.net_to_normal_taker_exit_pct == null
    ? '回到正常：等正常基差样本'
    : `回到正常 ${pctRaw(leg.net_to_normal_taker_exit_pct, 3)}（吃单平，费 ${num(maker.fee_taker_exit_pct)}%）/ ${pctRaw(leg.net_to_normal_maker_exit_pct, 3)}（挂单平，费 ${num(maker.fee_maker_exit_pct)}%）`;
  return `<div class="small muted" title="假设在 ${esc(venueLabel(maker.venue))} 挂单（做多挂买一、做空挂卖一）、另一家按深度吃单对冲；只是测算，不下单">挂单测算：${side} ${esc(venueLabel(maker.venue))} @${esc(String(num(leg.price)))}，入场 ${pctRaw(leg.entry_pct, 3)}；${nets}</div>`;
}

function rhStatus(line, view) {
  if (line.watch_only) {
    const base = '<span class="tag pc-pending" title="观察组：这家还没有实盘账户，只攒价差历史，不提醒、不交易">观察</span>';
    if (line.note) return `${base} <span class="muted small">${esc(line.note)}</span>`;
    if (line.normal == null) return `${base} <span class="muted small">攒样本 还差 ${line.normal_missing_minutes} 分钟</span>`;
    return line.best?.signal ? `${base} <span class="pos small" title="达到提醒门槛（观察组不推送）">够门槛</span>` : base;
  }
  if (line.observe_only) {
    const base = '<span class="tag pc-pending" title="Binance 的组只观察：不提醒、不自动交易；按挂单（0 费率）测算，并记录虚拟挂单被成交后的对冲收益">仅观察</span>';
    return line.note ? `${base} <span class="muted small">${esc(line.note)}</span>` : base;
  }
  if (line.note) return `<span class="muted">${esc(line.note)}</span>`;
  if (line.normal == null) {
    return `<span class="tag pc-pending" title="同一时段至少要 ${view.min_minutes} 分钟样本才给正常水平；在那之前不提醒">攒样本 还差 ${line.normal_missing_minutes} 分钟</span>`;
  }
  if (line.best?.signal) {
    const held = line.best.signal_sec ?? 0;
    return `<span class="tag pc-pass" title="回到正常水平的预估净收益超过提醒门槛；连续 10 秒才推送 Telegram">信号 ${held}s</span>`;
  }
  return '<span class="muted">—</span>';
}

function rhRow(line, view) {
  const leg = rhBestLeg(line);
  const normal = line.normal;
  const normalText = normal
    ? `<span title="样本 ${normal.minutes} 分钟">${pctRaw(normal.median, 3)}</span> <span class="muted small">(${pctRaw(normal.p10, 2)} ~ ${pctRaw(normal.p90, 2)})</span>`
    : '<span class="muted">—</span>';
  const z = num(line.z);
  const zText = z === null ? '<span class="muted">—</span>' : `<span class="${Math.abs(z) >= 3 ? 'neg' : 'muted'}">${z > 0 ? '+' : ''}${z.toFixed(1)}</span>`;
  const net = leg?.net_to_normal_pct;
  const usdText = line.best?.net_usdt == null ? '' : ` <span class="muted small">≈ ${pnlUsd(line.best.net_usdt)}</span>`;
  const fee = line.fee_round_trip_pct == null ? '' : ` · 往返费 ${num(line.fee_round_trip_pct)}%`;
  const signal = line.best?.signal && !line.observe_only && !line.watch_only;
  const title = line.watch_only
    ? '观察组：这家还没有实盘账户，不从这里下单'
    : line.observe_only
      ? '只观察：Binance 的组按挂单测算，不从这里下单'
      : '点击打开下单界面：带上方向和回到正常基差的收敛目标（仍需预览确认）';
  return `<tr class="${signal ? 'rh-signal' : ''}" data-rh-pair="${esc(line.pair)}" data-rh-base="${esc(line.base)}" title="${esc(title)}">
    <td><b>${esc(line.base)}</b> <span class="muted small">${esc((line.category || '').toLowerCase())}</span>${CHART_VENUES.includes(line.a) && CHART_VENUES.includes(line.b) ? ' <a href="#chart" class="small" data-rh-chart title="看这组的历史价差 K 线">历史价差</a>' : ''}<br><span class="muted small">${esc(pairLabel(line))}${esc(fee)}</span></td>
    <td>${esc(RH_SESSION[line.session] || line.session)}</td>
    <td class="num ${cls(line.basis_pct)}">${pctRaw(line.basis_pct, 3)}</td>
    <td class="num">${normalText}</td>
    <td class="num">${zText}</td>
    <td>${line.best ? esc(rhDirectionText(line) || line.best.direction) : '<span class="muted">—</span>'}</td>
    <td class="num">${leg ? `${pctRaw(leg.entry_pct, 3)} <span class="muted small" title="平仓穿价">−${pctRaw(leg.exit_cross_pct, 3).replace('+', '')}</span>` : '—'}</td>
    <td class="num ${cls(leg?.net_to_zero_pct)}">${leg ? pctRaw(leg.net_to_zero_pct, 3) : '—'}</td>
    <td class="num ${cls(net)}">${net == null ? '<span class="muted">—</span>' : pctRaw(net, 3)}${usdText}${rhMakerText(line)}</td>
    <td>${rhStatus(line, view)}</td>
  </tr>`;
}

const RH_SESSION = { rth: '盘中', off: '盘后', weekend: '周末', all: '全天' };

// 价差页的一行 → 策略页（价差视角）的下单参数。
// 基差口径不同：价差页是 (a − b) / 均值；持仓规则是 (空腿 − 多腿) / 均值。
// 多 a / 空 b 时持仓基差 = −页面基差，所以「回到正常」的收敛目标 = −正常中位数；反方向就是中位数本身。
function rhOrderTarget(line) {
  const direction = line.best?.direction;
  const legs = rhLegs(line, direction);
  const long = legs?.long ?? null;
  const short = legs?.short ?? null;
  const leg = direction ? line[direction] : null;
  const median = num(line.normal?.median);
  let target = null;
  if (long && median !== null) {
    const raw = direction === 'long_a' ? -median : median;
    // 规则只接受 ±5%；超出就不带目标（不截断成一个意思不同的数）。
    if (Math.abs(raw) <= 5) target = (Math.round(raw * 1000) / 1000).toFixed(3);
  }
  return {
    symbol: `${line.base}/USDT`,
    long,
    short,
    target,
    direction,
    entryPct: leg?.entry_pct ?? null,
    netToNormalPct: leg?.net_to_normal_pct ?? null,
    session: line.session,
    a: line.a,
    b: line.b,
    pairLabel: pairLabel(line),
    // 价差单要求空腿卖得出的价高于多腿要买的价：可成交价差不为正时预览会被拒绝。
    negativeEntry: leg ? num(leg.entry_pct) <= 0 : false,
  };
}

function rhOriginNote(origin) {
  if (!origin) return '';
  const parts = [`来自价差监控页（${esc(origin.pairLabel || '')}，${esc(RH_SESSION[origin.session] || origin.session || '')}）：方向按两边深度、对照同时段正常基差选出`];
  parts.push(origin.target != null
    ? `基差收敛目标已设为 <b>${esc(origin.target)}%</b>（回到正常水平就平仓）`
    : '还没有正常基差样本，收敛目标沿用你原来的设置');
  if (origin.netToNormalPct != null) parts.push(`RH 页估算回到正常净收益 ${pctRaw(origin.netToNormalPct, 3)}`);
  const warn = origin.negativeEntry
    ? `<br><b>注意：</b>这个方向当前可成交价差为 ${pctRaw(origin.entryPct, 3)}（不为正），价差单要求空腿卖价高于多腿买价，预览会被拒绝。`
    : '';
  return `<div class="alert ${origin.negativeEntry ? 'warn' : 'info'} small">${parts.join('；')}。下单前仍需预览并确认。${warn}</div>`;
}

// 点价差页的一行：切到策略页价差视角，选好这一组的两家、合约、方向和收敛目标。不下单。
function openRhInStrategy(line) {
  const order = rhOrderTarget(line);
  const s = state.strategy;
  s.userPicked = true;
  s.a = line.a;
  s.b = line.b;
  if (s.view !== 'spread') {
    s.view = 'spread';
    try {
      localStorage.setItem('arb-web-strategy-view', 'spread');
    } catch {
      // 存不了就只在本次会话里生效。
    }
  }
  if (order.target != null) s.form.basisExit = { ...s.form.basisExit, on: true, value: order.target };
  s.selected = order.long
    ? { symbol: order.symbol, long: order.long, short: order.short, fromRh: order }
    : { symbol: order.symbol, long: null, short: null };
  s.plan = null;
  const card = $('plan-card');
  if (card) card.dataset.key = '';
  showPage('strategy');
}

// 「显示观察组」：存在本机，默认不显示。
function rhShowWatch() {
  try {
    return localStorage.getItem('arb-web-rh-watch') === '1';
  } catch {
    return Boolean(state.rhWatch);
  }
}

function setRhShowWatch(on) {
  state.rhWatch = on;
  try {
    localStorage.setItem('arb-web-rh-watch', on ? '1' : '0');
  } catch {
    // 存不了就只在本次会话里生效。
  }
  reloadRhSpread();
}

// 「显示行数」：只取净收益最高的前 N 行（0 = 全部）。存在本机，默认 100 —— 整份两千多行每 3 秒重画一次会把页面拖卡。
const RH_LIMITS = ['50', '100', '300', '0'];
function rhLimit() {
  let value = state.rhLimit;
  try {
    value = localStorage.getItem('arb-web-rh-limit') || value;
  } catch {
    // 读不了就用本次会话里的。
  }
  return RH_LIMITS.includes(value) ? value : '100';
}

function setRhLimit(value) {
  state.rhLimit = value;
  try {
    localStorage.setItem('arb-web-rh-limit', value);
  } catch {
    // 存不了就只在本次会话里生效。
  }
  reloadRhSpread();
}

// 筛选变了：马上按新条件取一次（筛选在服务端做）。
function reloadRhSpread() {
  loadRhSpread().catch((error) => {
    $('rh-notice').__html = null;
    $('rh-notice').innerHTML = `<div class="notice error">取数失败：${esc(error.message)}</div>`;
  });
}

// 这次取数带的筛选参数。选了某一组时总是带上观察组的行（选中的可能就是观察组；不是的话服务端按组筛完也没有）。
function rhQuery() {
  const params = new URLSearchParams();
  const pair = rhPairFilter();
  if (pair !== 'all') params.set('pair', pair);
  if ($('rh-signal-only').checked) params.set('signal', '1');
  if (rhShowWatch() || pair !== 'all') params.set('watch', '1');
  params.set('limit', rhLimit());
  return params;
}

// 组选择：全部 / 某一组。存在本机。
function rhPairFilter() {
  try {
    return localStorage.getItem('arb-web-rh-pair') || 'all';
  } catch {
    return state.rhPair || 'all';
  }
}

function setRhPairFilter(value) {
  state.rhPair = value;
  try {
    localStorage.setItem('arb-web-rh-pair', value);
  } catch {
    // 存不了就只在本次会话里生效。
  }
  reloadRhSpread();
}

function renderRhSpread() {
  const view = state.rh;
  if (!view) return;
  const conn = view.connected || {};
  const dot = (ok, name) => `<span class="${ok ? 'pos' : 'neg'}">${ok ? '●' : '○'} ${esc(name)}</span>`;
  const venues = Object.entries(conn.venues || {});
  $('rh-status').innerHTML = `${venues.map(([venue, up]) => dot(up, venueLabel(venue))).join(' · ')} · 更新于 ${when(view.updated_at)}${conn.reconnects ? ` · 重连 ${conn.reconnects} 次` : ''}`;
  const tradingPairs = view.pairs || [];
  const watchPairs = view.watch?.pairs || [];
  const showWatch = rhShowWatch();
  const watchBox = $('rh-watch');
  if (watchBox) {
    watchBox.checked = showWatch;
    watchBox.closest('label').hidden = !watchPairs.length;
  }
  const pairs = tradingPairs.concat(watchPairs);
  const filter = pairs.some((p) => p.id === rhPairFilter()) ? rhPairFilter() : 'all';
  const watchFilter = watchPairs.some((p) => p.id === filter);
  const select = $('rh-pair');
  if (select) {
    const option = (p) => `<option value="${esc(p.id)}"${p.id === filter ? ' selected' : ''}>${esc(`${venueLabel(p.a)} ↔ ${venueLabel(p.b)}`)}（${p.markets}）${p.watch_only ? ' · 观察' : ''}</option>`;
    select.innerHTML = [`<option value="all">全部组（${tradingPairs.length}${showWatch && watchPairs.length ? ` + 观察 ${watchPairs.length}` : ''}）</option>`]
      .concat(tradingPairs.map(option))
      .concat(watchPairs.length ? [`<optgroup label="观察组（没有实盘账户）">${watchPairs.map(option).join('')}</optgroup>`] : [])
      .join('');
    select.value = filter;
  }
  const limitSelect = $('rh-limit');
  if (limitSelect) limitSelect.value = rhLimit();
  // 组、只看信号、观察组、行数都在服务端筛好了（见 rhQuery）：这里拿到的就是要显示的行，按净收益从高到低。
  // 观察组的行默认不带：勾了「显示观察组」或选中某个观察组才有。
  // 服务端本来就不带观察组的行；这里再挡一次（老接口、缓存的旧响应也不会让观察组悄悄冒出来）。
  // 选中了某一组就只留这一组的行（同上：服务端已经筛过，这里是兜底）。
  const shown = (view.lines || [])
    .concat(showWatch || watchFilter ? view.watch?.lines || [] : [])
    .filter((l) => filter === 'all' || l.pair === filter);
  // 总数由服务端数（只观察的组和观察组不算信号）；老接口没有 counts 时退回数返回的行。
  const counts = view.counts || { lines: shown.length, signals: shown.filter((l) => l.best?.signal && !l.observe_only && !l.watch_only).length, shown: shown.length };
  $('rh-stats').innerHTML = [
    ['合约', String(counts.lines), ''],
    ['当前信号', String(counts.signals), counts.signals ? 'pos' : 'dim'],
    ['估算名义', `$${num(view.size_usdt).toLocaleString('en-US')}`, ''],
    ['提醒门槛', `≥ ${num(view.alert_net_pct)}%`, ''],
  ].map(([label, value, tone]) => `<div class="stat"><span>${label}</span><b class="${tone}">${esc(value)}</b></div>`).join('');
  const shownPairs = filter === 'all' ? tradingPairs : tradingPairs.filter((p) => p.id === filter);
  const feeText = (p) => p.fee_min_pct == null ? '—' : num(p.fee_min_pct) === num(p.fee_max_pct) ? `${num(p.fee_min_pct)}%` : `${num(p.fee_min_pct)}% ~ ${num(p.fee_max_pct)}%`;
  const pairRows = shownPairs.map((p) => `<li><b>${esc(`${venueLabel(p.a)} ↔ ${venueLabel(p.b)}`)}</b>：${p.markets} 个合约，往返手续费 ${esc(feeText(p))}，已攒历史 ${(p.history_minutes / 60).toFixed(1)} 小时${p.observe_only ? '，<span class="muted">只观察（挂单测算，不提醒、不自动交易）</span>' : rhAutoTradable(p) ? '，<span class="pos">可自动交易</span>' : '，只监控（实盘没连这两家）'}${p.note ? ` <span class="muted">（${esc(p.note)}）</span>` : ''}</li>`).join('');
  const notices = [];
  if (view.error) notices.push(`<div class="notice error">${esc(view.error)}</div>`);
  const shownWatch = filter === 'all' ? watchPairs : watchPairs.filter((p) => p.id === filter);
  if (shownWatch.length && (showWatch || watchFilter)) {
    const watchRows = shownWatch.map((p) => `<li>${esc(`${venueLabel(p.a)} ↔ ${venueLabel(p.b)}`)}：${p.markets} 个合约，往返手续费 ${esc(feeText(p))}，已攒历史 ${(p.history_minutes / 60).toFixed(1)} 小时${p.note ? ` <span class="muted">（${esc(p.note)}）</span>` : ''}</li>`).join('');
    const total = shownWatch.reduce((n, p) => n + p.markets, 0);
    notices.push(`<div class="notice"><details><summary><b>观察组</b> ${shownWatch.length} 组 · ${total} 个合约：这些场所还没有实盘账户，只攒价差历史（攒够一周后按历史统计信号次数和扣费后的净额，再决定要不要接），<b>不提醒、不交易</b>。每秒算一次。</summary><ul>${watchRows}</ul></details></div>`);
  }
  if (pairRows) notices.push(`<div class="notice"><ul>${pairRows}</ul><span class="small muted">基差 = (左 − 右) / 均值。每个合约每个时段至少 ${view.min_minutes} 分钟样本才算出「正常基差」并开始提醒（窗口 ${view.window_days} 天）；在那之前只显示「收敛到 0」的估算，对股票类合约通常偏乐观。往返费按基础档上限算（HL 子交易所含 growth mode 折扣与 Entropy 返佣设置）。</span></div>`);
  const makerHtml = rhMakerSummaryHtml(view.maker, filter, view.maker_fills);
  if (makerHtml) notices.push(makerHtml);
  setHtmlIfChanged($('rh-notice'), notices.join(''));
  setHtmlIfChanged($('rh-rows'), shown.map((line) => rhRow(line, view)).join(''));
  const matching = $('rh-signal-only').checked ? counts.signals : counts.lines;
  $('rh-empty').textContent = shown.length
    ? (shown.length < matching ? `只显示净收益最高的前 ${shown.length} 行（共 ${matching} 行）。要看其余的：选某一组，或把「显示行数」调大。` : '')
    : (counts.lines ? '当前没有信号。' : '等待行情…');
}

// 内容没变就不动 DOM：每 3 秒整表重写会打断选中、滚动和悬停提示，也白白重排。
function setHtmlIfChanged(node, html) {
  if (!node || node.__html === html) return;
  node.__html = html;
  node.innerHTML = html;
}

// 价差表的点击（整张表只绑一次）：点「历史价差」去图表页，点行去策略页的下单界面。
function onRhRowClick(event) {
  const tr = event.target?.closest?.('tr[data-rh-base]');
  if (!tr) return;
  const lines = (state.rh?.lines || []).concat(state.rh?.watch?.lines || []);
  const line = lines.find((l) => l.base === tr.getAttribute('data-rh-base') && l.pair === tr.getAttribute('data-rh-pair'));
  if (!line) return;
  if (event.target?.closest?.('[data-rh-chart]')) {
    event.preventDefault();
    openChartFor(line);
    return;
  }
  // 只观察的组按挂单测算，不按吃单下单；观察组没有实盘账户。
  if (line.observe_only || line.watch_only) return;
  openRhInStrategy(line);
}

// 挂单测算的汇总：虚拟挂单被（严格穿价的）成交吃到之后，对面吃单对冲在各时刻能拿到的「回到正常」净收益。
function rhMakerSummaryHtml(maker, filter, fills) {
  if (!maker) return '';
  const stats = (maker.stats || []).filter((s) => filter === 'all' || s.pair === filter);
  // 统计只返回前 15 条：总笔数用服务端数好的（fills），没有就数返回的。
  const head = `<b>挂单测算（只观察）</b>：现在虚拟挂着 ${maker.quoting} 个；最近记录 ${fills ?? stats.reduce((n, s) => n + s.fills, 0)} 笔虚拟成交${maker.since ? `（自 ${when(maker.since)}）` : ''}。`;
  const explain = '<span class="small muted">挂单价 = Binance 自己这一侧的最优价；只有逐笔成交<b>严格穿过</b>挂单价才算成交（排队位置不知道，同价不算）；成交后在 0 / 0.3 / 1 / 5 秒按对面盘口吃单对冲，换算成「回到正常」净收益（吃单平仓）。报价时的净收益与之后实际能拿到的差就是逆向选择。同一合约同一方向 60 秒最多记一笔。</span>';
  if (!stats.length) return `<div class="notice">${head}<br>${explain}</div>`;
  const pctCell = (value) => (value == null ? '<span class="muted">—</span>' : `<span class="${cls(value)}">${pctRaw(value, 3)}</span>`);
  const at = (s, horizon) => (s.median_net_pct || []).find(([h]) => h === horizon)?.[1] ?? null;
  const rows = stats.slice(0, 15).map((s) => {
    const legs = { long_a: s.pair.split(':')[0], long_b: s.pair.split(':')[1] };
    const long = legs[s.direction];
    const short = s.direction === 'long_a' ? legs.long_b : legs.long_a;
    return `<tr><td><b>${esc(s.base)}</b> <span class="muted small">${esc(venueLabel(long))} 多 / ${esc(venueLabel(short))} 空</span></td><td class="num">${s.fills}</td><td class="num">${pctCell(s.median_quoted_net_pct)}</td><td class="num">${pctCell(at(s, 0))}</td><td class="num">${pctCell(at(s, 300))}</td><td class="num">${pctCell(at(s, 1000))}</td><td class="num">${pctCell(at(s, 5000))}</td><td class="num">${s.positive_share_1s == null ? '—' : `${num(s.positive_share_1s)}%`}</td><td class="small muted">${when(s.last_at)}</td></tr>`;
  }).join('');
  return `<div class="notice">${head}<br>${explain}<div class="table-wrap"><table class="small"><thead><tr><th>合约 / 方向</th><th class="num">笔数</th><th class="num">报价时</th><th class="num">成交时</th><th class="num">0.3 秒</th><th class="num">1 秒</th><th class="num">5 秒</th><th class="num">1 秒为正</th><th>最近</th></tr></thead><tbody>${rows}</tbody></table></div><span class="small muted">数字是中位数（%）。</span></div>`;
}

async function loadRhSpread() {
  // 只认最后发出的那次：筛选刚改时，旧条件的响应可能晚到。
  const seq = (state.rhSeq = (state.rhSeq || 0) + 1);
  const result = await api(`/api/rh-spread?${rhQuery()}`);
  if (seq !== state.rhSeq) return;
  if (!result.ok) throw new Error(result.body?.error || `HTTP ${result.status}`);
  const view = result.body;
  // 存着的组已经不在了（场所下线、配置变了）：退回「全部组」重取，不然一行都没有。
  const pair = rhPairFilter();
  if (pair !== 'all' && !(view.pairs || []).concat(view.watch?.pairs || []).some((p) => p.id === pair)) {
    setRhPairFilter('all');
    return;
  }
  state.rh = view;
  renderRhSpread();
  loadRhAuto().catch(() => {});
}

// ───────────────────────────── RH 价差自动交易 ─────────────────────────────
//
// 设置存在服务端（auto.json）。表单只在没有未保存改动时跟着服务端刷新，免得打字时被冲掉。

const RH_AUTO_FIELDS = [
  ['size_usdt', '单腿名义 (USDT)', 'number', '10', '每笔两腿各这么多名义'],
  ['size_max_usdt', '动态单笔上限 (USDT)', 'number', '100', '按盘口放大单笔：信号那一刻按两边盘口逐档（每 100 USDT）算回到正常的净收益，在「单腿名义 ～ 这个上限」之间挑预估净赚 USDT 最多、且每档都过门槛的那一档。只给稳的组合放大：预估净收益不到基差日常波动（MAD）的照原名义（防 PRL 那种乱跳的币对）。放大后的单笔仍按盘口深度往下缩、全部闸门照旧；按比例的止盈跟着放大；补仓照原名义；放大后保证金不够时这个合约 10 分钟内按原名义做。最多 1000；留空 = 固定单笔'],
  ['leverage', '杠杆（整数）', 'number', '1', '两腿相同，逐仓'],
  ['min_net_pct', '触发门槛 (%)', 'number', '0.01', '「回到正常净收益」≥ 它才下单（已扣手续费与平仓穿价）'],
  ['hold_sec', '信号保持 (秒)', 'number', '0.1', '连续达标这么久才下单，过滤一闪而过的挂单；0 ～ 600，可填小数（如 0.5）；0 = 检测到信号就下'],
  ['instant_multiple', '强信号不等保持 (门槛倍数)', 'number', '0.5', '扣掉滑点缓冲后的「回到正常净收益」≥ 门槛 × 它，就不等信号保持、立刻下单（历史分位、冷却、限额照旧；下单前仍按现拉盘口重算）；如 2；留空 = 一律等保持'],
  ['slippage_buffer_pct', '滑点缓冲 (%)', 'number', '0.01', '「回到正常净收益」先扣掉它再和门槛比；实盘统计预估与实际平均差约 0.05%。0 = 不扣'],
  ['max_fee_share_pct', '手续费占毛利上限 (%)', 'number', '5', '往返手续费超过预估毛利（回到正常净收益 + 往返费）这个比例的组合 / 合约不做；如 50；留空不过滤'],
  ['fast_move_pct', '急行情不开仓 (%)', 'number', '0.01', '两条腿所在的两家里，任何一家中间价最近 5 秒的波动（最高 − 最低 ÷ 最低）达到它就不开仓 / 不补仓，等稳住（保持计时也清零）。价差常是一边先动、另一边报价滞后，等第二腿到的时候已经追上，第一腿裸着被行情带走；如 0.15；刚启动 / 重连没有 5 秒历史时放行；留空不过滤'],
  ['depth_fit_pct', '按盘口深度定单笔 (占深度 %)', 'number', '5', '「单腿名义」当上限：盘口在限价内吃不下时往下缩，单笔不超过「第一腿限价内可吃深度」和「第二腿保本价内可吃深度」的这个比例（如 50；给下单到第二腿到达的两秒留余量）。只在吃不到时缩，不为凑利润而缩；缩完全部闸门照旧再过一遍；按 USDT 计的止盈 / 止损随单笔同比例缩小；留空 = 固定单笔'],
  ['force_hedge_pct', '强制补腿：最差锁定价差 (%)', 'number', '0.05', '第一腿成交后，第二腿在保本价内没吃满（一点没吃到或只吃到一部分）时不回滚，把剩下的数量按「两腿锁定价差不低于 −这个数」的最差价追上（如 0.3 = 价差最差到 −0.3%）；追不上才回滚。它不消除亏损，只是把回滚兑现的亏损换成一笔起点就是负价差的对冲仓位——收敛到目标不一定赚钱、又没有止损，可能长期挂着，建议同时设超时退出；只对两腿都吃单的新开仓起作用；留空 = 关（照旧回滚）'],
  ['depth_min_usdt', '按深度的最小单笔 (USDT)', 'number', '10', '按盘口深度缩到这个数以下就不下单（只在上一项开着时起作用）'],
  ['take_profit_usdt', '止盈 (USDT)', 'number', '0.01', '含资金费的净盈利达到它就平仓（按盘口核对）；与下一项都填时取大的；留空不设'],
  ['take_profit_pct', '止盈 (% 名义)', 'number', '0.01', '按单腿名义的比例止盈，如 0.05 = 800U 时 0.4U；留空不设'],
  ['take_profit_book_min_usdt', '止盈盘口门槛 (USDT)', 'number', '0.01', '标记价到止盈后，按两边盘口平掉的整笔净额（含资金费、平仓手续费与穿价）为正且 ≥ 它就平，不再要求盘口也到止盈金额；0 = 为正即平；高于某笔的止盈金额时按止盈金额算；留空 = 盘口也要到止盈金额（以前的做法）。只作用于之后新开的仓；下单前照样重拉盘口复核，成交可能比预估差'],
  ['recent_gate_pct', '相对近期偏离门槛 (%)', 'number', '0.01', '开仓 / 补仓时，这个方向的可成交价差要比近 12 小时的中位数至少高这么多才做。「回到正常」按 7 天中位算，价差整体换了水平之后要几天才跟上，这期间每个读数都像大偏离（实盘遇到过：开在近期中位上，价差继续走宽而亏损）；回测里 0.05 拦掉的合计为负；近期样本不够时放行；留空不过滤'],
  ['history_gate_pct', '历史分位前 (%)', 'number', '5', '按「历史价差」页同口径：这个方向现在的价差要排在过去一段时间最大的前 X% 才开，避免开在半山腰；如 20；历史拉不到时不开；留空不用'],
  ['history_days', '历史天数', 'number', '1', '历史分位用多少天的 K 线（1 小时线最多 48 天）'],
  ['stop_loss_usdt', '止损 (USDT)', 'number', '0.1', '按盘口现在平掉的整笔净额（含资金费）亏到 −它就平仓；开「相对正常基差」时必填；留空不设'],
  ['liq_protection_pct', '爆仓保护 (%)', 'number', '1', '强平距离低于它两腿等比例减仓；留空不设'],
  ['auto_margin_pct', '自动加保证金 (%)', 'number', '0.5', '任一腿强平距离低于它就往这条腿补逐仓保证金，拉回 1.5 倍；要同时填上限，留空不设（须高于爆仓保护）'],
  ['auto_margin_max_usdt', '加保证金上限 (USDT)', 'number', '1', '每笔仓位最多累计补这么多；留空不设'],
  ['timeout_hours', '超时退出 (小时)', 'number', '0.5', '持仓超过这么多小时、按盘口平掉的亏损不超过右边的金额就平仓，释放名额；要同时填最多亏，留空不设'],
  ['timeout_max_loss_usdt', '超时最多亏 (USDT)', 'number', '0.01', '超时后可接受的按盘口亏损（含资金费）；0 = 保本才平'],
  ['add_min_improve_pct', '补仓门槛 (%)', 'number', '0.01', '同一合约、同方向的新信号，开仓价差比原仓位实际成交的价差至少好这么多才往上补；留空不补仓'],
  ['add_max_times', '每笔最多补 (次)', 'number', '1', '每笔仓位最多补几次（发过单就算）'],
  ['add_max_multiple', '补完总名义 ≤ 单笔 (倍)', 'number', '0.5', '补完后单腿名义最多是单笔名义的几倍；同时不超过单笔上限'],
  ['max_positions', '同时最多 (笔)', 'number', '1', '自动开的仓位同时最多几笔（补仓不占名额）；0 = 不限（仍受账户级持仓上限 ARB_MAX_OPEN_POSITIONS 约束）'],
  ['daily_max_opens', '每日最多 (笔)', 'number', '1', '每个 UTC 日最多自动开几笔（补仓也算）；0 = 不限'],
  ['open_gap_sec', '开仓间隔 (秒)', 'number', '1', '两次自动开仓之间至少隔多少秒（全局，不分合约）；5 ～ 600，默认 60'],
];
// 留空 = 不设的可选项。
const RH_AUTO_OPTIONAL = new Set(['size_max_usdt', 'instant_multiple', 'recent_gate_pct', 'history_gate_pct', 'take_profit_usdt', 'take_profit_pct', 'take_profit_book_min_usdt', 'stop_loss_usdt', 'max_fee_share_pct', 'fast_move_pct', 'depth_fit_pct', 'force_hedge_pct', 'liq_protection_pct', 'auto_margin_pct', 'auto_margin_max_usdt', 'timeout_hours', 'timeout_max_loss_usdt', 'add_min_improve_pct']);

// 信号保持：整秒且 ≥ 5 照旧只写 hold_sec；小数或不到 5 秒写 hold_ms（毫秒），hold_sec 填最接近的合法整秒，
// 给旧版本读回、也是信号保持影子的「现行规则」对照。
// 空值、非数字、负数、超过 600：不转成 0（那是最激进的「见信号就下」），两个字段都给 null，保存时报错 ——
// 清空输入框不能悄悄变成 0 秒。
function rhAutoHold(value) {
  const text = String(value).trim();
  const sec = Number(text);
  if (text === '' || !Number.isFinite(sec) || sec < 0 || sec > 600) return { hold_sec: null, hold_ms: null };
  const whole = Number.isInteger(sec) && sec >= 5;
  return {
    hold_sec: Math.min(600, Math.max(5, Math.round(sec))),
    hold_ms: whole ? null : Math.round(sec * 1000),
  };
}

function rhAutoForm(settings) {
  return {
    enabled: Boolean(settings.enabled),
    mode: settings.mode || 'paper',
    size_usdt: String(settings.size_usdt ?? '500'),
    size_max_usdt: settings.size_max_usdt == null ? '' : String(settings.size_max_usdt),
    leverage: String(settings.leverage ?? '3'),
    min_net_pct: String(settings.min_net_pct ?? '0.05'),
    hold_sec: String(settings.hold_ms != null ? settings.hold_ms / 1000 : (settings.hold_sec ?? '10')),
    instant_multiple: settings.instant_multiple == null ? '' : String(settings.instant_multiple),
    back_to_normal: settings.back_to_normal !== false,
    take_profit_usdt: settings.take_profit_usdt == null ? '' : String(settings.take_profit_usdt),
    take_profit_pct: settings.take_profit_pct == null ? '' : String(settings.take_profit_pct),
    take_profit_book_min_usdt: settings.take_profit_book_min_usdt == null ? '' : String(settings.take_profit_book_min_usdt),
    stop_loss_usdt: settings.stop_loss_usdt == null ? '' : String(settings.stop_loss_usdt),
    slippage_buffer_pct: String(settings.slippage_buffer_pct ?? '0'),
    max_fee_share_pct: settings.max_fee_share_pct == null ? '' : String(settings.max_fee_share_pct),
    fast_move_pct: settings.fast_move_pct == null ? '' : String(settings.fast_move_pct),
    depth_fit_pct: settings.depth_fit_pct == null ? '' : String(settings.depth_fit_pct),
    force_hedge_pct: settings.force_hedge_pct == null ? '' : String(settings.force_hedge_pct),
    depth_min_usdt: String(settings.depth_min_usdt ?? '50'),
    relative_to_normal: Boolean(settings.relative_to_normal),
    cheap_leg_first: settings.cheap_leg_first !== false,
    history_gate_pct: settings.history_gate_pct == null ? '' : String(settings.history_gate_pct),
    recent_gate_pct: settings.recent_gate_pct == null ? '' : String(settings.recent_gate_pct),
    deduct_funding: settings.deduct_funding !== false,
    history_interval: settings.history_interval || '1h',
    history_days: String(settings.history_days ?? '24'),
    liq_protection_pct: settings.liq_protection_pct == null ? '' : String(settings.liq_protection_pct),
    auto_margin_pct: settings.auto_margin_pct == null ? '' : String(settings.auto_margin_pct),
    auto_margin_max_usdt: settings.auto_margin_max_usdt == null ? '' : String(settings.auto_margin_max_usdt),
    timeout_hours: settings.timeout_hours == null ? '' : String(settings.timeout_hours),
    timeout_max_loss_usdt: settings.timeout_max_loss_usdt == null ? '' : String(settings.timeout_max_loss_usdt),
    add_min_improve_pct: settings.add_min_improve_pct == null ? '' : String(settings.add_min_improve_pct),
    add_max_times: String(settings.add_max_times ?? '2'),
    add_max_multiple: String(settings.add_max_multiple ?? '3'),
    max_positions: String(settings.max_positions ?? '1'),
    daily_max_opens: String(settings.daily_max_opens ?? '3'),
    open_gap_sec: String(settings.open_gap_sec ?? '60'),
    symbols: (settings.symbols || []).join(','),
    exclude_venues: (settings.exclude_venues || []).join(','),
    exclude_symbols: (settings.exclude_symbols || []).join(','),
  };
}

// 表单 → 请求体。空的可选项发 null（关闭），数字按字符串发（服务端按十进制解析）。
function rhAutoBody(form, enabled) {
  const optional = (value) => (String(value).trim() === '' ? null : String(value).trim());
  return {
    enabled,
    mode: form.mode,
    size_usdt: String(form.size_usdt).trim(),
    size_max_usdt: optional(form.size_max_usdt),
    leverage: String(form.leverage).trim(),
    min_net_pct: String(form.min_net_pct).trim(),
    ...rhAutoHold(form.hold_sec),
    instant_multiple: optional(form.instant_multiple),
    back_to_normal: Boolean(form.back_to_normal),
    take_profit_usdt: optional(form.take_profit_usdt),
    take_profit_pct: optional(form.take_profit_pct),
    take_profit_book_min_usdt: optional(form.take_profit_book_min_usdt),
    stop_loss_usdt: optional(form.stop_loss_usdt),
    slippage_buffer_pct: String(form.slippage_buffer_pct).trim() || '0',
    max_fee_share_pct: optional(form.max_fee_share_pct),
    fast_move_pct: optional(form.fast_move_pct),
    depth_fit_pct: optional(form.depth_fit_pct),
    force_hedge_pct: optional(form.force_hedge_pct),
    depth_min_usdt: String(form.depth_min_usdt).trim() || '50',
    relative_to_normal: Boolean(form.relative_to_normal),
    cheap_leg_first: Boolean(form.cheap_leg_first),
    history_gate_pct: optional(form.history_gate_pct),
    recent_gate_pct: optional(form.recent_gate_pct),
    deduct_funding: Boolean(form.deduct_funding),
    history_interval: form.history_interval || '1h',
    history_days: String(form.history_days).trim() || '24',
    liq_protection_pct: optional(form.liq_protection_pct),
    auto_margin_pct: optional(form.auto_margin_pct),
    auto_margin_max_usdt: optional(form.auto_margin_max_usdt),
    timeout_hours: optional(form.timeout_hours),
    timeout_max_loss_usdt: optional(form.timeout_max_loss_usdt),
    add_min_improve_pct: optional(form.add_min_improve_pct),
    add_max_times: Number(form.add_max_times),
    add_max_multiple: String(form.add_max_multiple).trim(),
    max_positions: Number(form.max_positions),
    daily_max_opens: Number(form.daily_max_opens),
    open_gap_sec: Number(form.open_gap_sec ?? 60),
    symbols: String(form.symbols).split(/[,，\s]+/).map((s) => s.trim().toUpperCase()).filter(Boolean),
    exclude_venues: String(form.exclude_venues ?? '').split(/[,，\s]+/).map((s) => s.trim().toLowerCase()).filter(Boolean),
    exclude_symbols: String(form.exclude_symbols ?? '').split(/[,，\s]+/).map((s) => s.trim().toUpperCase()).filter(Boolean),
  };
}

async function loadRhAuto() {
  const a = state.rhAuto;
  if (!state.tradeConfig?.auth_configured || !token()) {
    a.data = null;
    renderRhAuto();
    return;
  }
  const { ok, body } = await api('/api/rh-spread/auto', { auth: true });
  a.data = ok ? body : { error: body.error };
  if (ok && !a.dirty) a.form = rhAutoForm(body.settings);
  renderRhAuto();
}

async function saveRhAuto(enabled) {
  const a = state.rhAuto;
  const body = rhAutoBody(a.form, enabled);
  if (body.hold_sec === null) {
    a.message = { tone: 'error', text: '信号保持必须是 0 ～ 600 秒的数字（可小数，0 = 检测到信号就下单），不能留空。' };
    renderRhAuto();
    return;
  }
  if (enabled && body.mode === 'live' && !(a.data?.settings?.enabled && a.data?.settings?.mode === 'live')) {
    const typed = window.prompt(`开启【实盘】自动交易：信号出现时机器人会用真实资金自动下单（每笔两腿各 ${body.size_usdt} USDT）。\n确认请输入 LIVE`);
    if ((typed || '').trim() !== 'LIVE') return;
    body.confirm = 'LIVE';
  }
  a.busy = true;
  a.message = null;
  renderRhAuto();
  const { ok, body: result } = await api('/api/rh-spread/auto', { method: 'POST', body, auth: true });
  a.busy = false;
  if (ok) {
    a.dirty = false;
    a.message = { tone: 'info', text: enabled ? '已保存并开启。' : '已保存（自动交易关闭）。' };
  } else {
    a.message = { tone: 'error', text: result.error || '保存失败' };
  }
  await loadRhAuto().catch(() => {});
  renderRhAuto();
}

const RH_AUTO_EVENT = { opened: '开仓', added: '补仓', unwound: '回滚', maker_unfilled: '挂单没成交', untraded: '第一腿没成交', rejected: '被拒', paused: '暂停', disabled: '关闭', settings: '设置' };

function rhAutoHtml(a, cfg) {
  if (!cfg?.auth_configured) return '<h3>自动交易</h3><p class="muted small">看板没有配置 ARB_WEB_TOKEN，自动交易不可用。</p>';
  if (!token()) return '<h3>自动交易</h3><p class="muted small">点右上角「令牌」填写后才能查看和设置自动交易。</p>';
  const data = a.data;
  if (!data) return '<h3>自动交易</h3><p class="muted small">读取中…</p>';
  if (data.error) return `<h3>自动交易</h3><div class="notice error">${esc(data.error)}</div>`;
  const f = a.form;
  const on = Boolean(data.settings.enabled);
  const liveMode = data.settings.mode === 'live';
  const badge = on
    ? `<span class="tag ${liveMode ? 'coral' : 'pc-pass'}">${liveMode ? '实盘自动交易中' : '纸面自动交易中'}</span>`
    : '<span class="tag">已关闭</span>';
  const field = ([key, label, type, step, hint]) => `<label class="field" title="${esc(hint)}"><span>${esc(label)}</span><input type="${type}" step="${step}" data-rh-auto="${key}" value="${esc(f[key])}"${RH_AUTO_OPTIONAL.has(key) ? ` placeholder="${key === 'add_min_improve_pct' ? '不补仓' : '不设'}"` : ''} /></label>`;
  const maxSize = data.max_position_usdt;
  const warnings = [];
  if (f.mode === 'live' && !data.live_can_trade) warnings.push('实盘没连上或是只读模式：不能开启实盘自动交易。');
  if (f.mode === 'paper' && !data.paper_watch_sec) warnings.push('纸面规则没有在看板后台运行（ARB_WEB_PAPER_WATCH_SEC=0）：纸面自动仓位不会被自动平仓，只能在持仓页手动「执行一轮规则」。');
  const blank = (key) => String(f[key] ?? '').trim() === '';
  if (!f.back_to_normal && blank('take_profit_usdt') && blank('take_profit_pct')) warnings.push('至少开启一条退出规则（回到正常基差平仓 / 止盈），否则不能保存。');
  if (!blank('take_profit_book_min_usdt') && blank('take_profit_usdt') && blank('take_profit_pct')) warnings.push('填了止盈盘口门槛但没设止盈：门槛只在止盈按标记价达标后才用，否则不能保存。');
  if (!blank('take_profit_book_min_usdt') && !(blank('take_profit_usdt') && blank('take_profit_pct'))) warnings.push(`已开止盈盘口门槛：标记价到止盈后，盘口算下来${num(f.take_profit_book_min_usdt) === 0 ? '为正' : ` ≥ ${f.take_profit_book_min_usdt} USDT`}就平。盘口预估到成交之间常有滑点（10-12 起规则平仓的成交比预估平均差约 0.08%，200U 仓位约 0.16U），门槛太低时实际可能是小亏。`);
  if (f.relative_to_normal && (!f.back_to_normal || blank('stop_loss_usdt'))) warnings.push('「相对正常基差」必须同时勾选「回到正常基差就平仓」并填止损，否则不能保存。');
  if (!blank('history_gate_pct')) warnings.push(`已开历史分位门槛：只在价差排在过去 ${f.history_days} 天（${f.history_interval} 线）最大的前 ${f.history_gate_pct}% 时开。K 线是成交价（Arcus 是预言机价），和实时可成交价差不完全一样；历史拉不到时不开。`);
  if (f.relative_to_normal) warnings.push('已开「相对正常基差」：开仓可成交价差可以为负（两家长期有固定差价时也会做），赚的是回到正常水平那一段；正常水平本身会慢慢变，偏离继续拉大时只有止损兜底。');
  const half = (a, b) => (String(f[a]).trim() === '') !== (String(f[b]).trim() === '');
  if (half('auto_margin_pct', 'auto_margin_max_usdt')) warnings.push('自动加保证金要同时填触发线和上限，否则不能保存。');
  if (half('timeout_hours', 'timeout_max_loss_usdt')) warnings.push('超时退出要同时填小时数和最多亏多少，否则不能保存。');
  if (String(f.add_min_improve_pct).trim() !== '') warnings.push(`已开补仓：同一合约、同方向的新信号比原仓位实际成交价差好 ≥ ${f.add_min_improve_pct}% 就往上加 ${f.size_usdt} USDT，每笔最多 ${f.add_max_times} 次、总名义 ≤ 单笔 ${f.add_max_multiple} 倍。补仓是在还没回归的仓位上加码，基差继续走远时亏损也成倍放大。`);
  if (num(f.size_usdt) !== null && num(data.monitor_size_usdt) !== null && num(f.size_usdt) > num(data.monitor_size_usdt)) {
    warnings.push(`表格里的净收益按 ${num(data.monitor_size_usdt)} USDT 估算；你的单笔更大，吃得更深，实际价差会更差（下单前按你的金额重算，不够会被拒）。`);
  }
  const opened = data.open_positions || [];
  const events = (data.events || []).slice(0, 12);
  const stats = data.stats || {};
  const count = (value) => Number(value) || 0;
  // 单腿名义。整数不带小数；有小数最多两位。缺字段按 0，旧看板不会显示成空白。
  const usdtText = (value) => {
    const parsed = num(value);
    const amount = parsed === null ? 0 : parsed;
    const sign = amount < 0 ? '−' : '';
    const [whole, frac] = Math.abs(amount).toFixed(2).split('.');
    const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    const tail = frac === '00' ? '' : `.${frac.replace(/0$/, '')}`;
    return `${sign}${grouped}${tail} USDT`;
  };
  const statCards = [
    ['累计开仓', String(count(stats.opened)), usdtText(stats.opened_usdt), ''],
    ['今日开仓', String(count(stats.opened_today)), usdtText(stats.opened_today_usdt), count(stats.opened_today) ? 'pos' : 'dim'],
    ['仍持有', String(count(stats.open_now)), usdtText(stats.open_now_usdt), count(stats.open_now) ? 'pos' : 'dim'],
    ['回滚', String(count(stats.unwound)), usdtText(stats.unwound_usdt), count(stats.unwound) ? 'neg' : 'dim'],
    ['被拒', String(count(stats.rejected)), '', count(stats.rejected) ? 'neg' : 'dim'],
    ['连续失败', `${count(stats.failures)}/${Number(data.max_failures) || 2}`, '', count(stats.failures) ? 'neg' : 'dim'],
  ].map(([label, value, amount, tone]) => `<div class="stat"><span>${label}</span><b class="${tone}">${esc(value)}</b>${amount ? `<small>${esc(amount)}</small>` : ''}</div>`).join('');
  const symbols = Array.isArray(stats.symbols) ? stats.symbols : [];
  const days = Array.isArray(stats.days) ? stats.days : [];
  const symbolLine = symbols.length
    ? symbols.map((row) => `${row.symbol} ${count(row.opened)} 笔 · ${usdtText(row.usdt)}${count(row.open) ? `（持有 ${count(row.open)} · ${usdtText(row.open_usdt)}）` : ''}`).join(' · ')
    : '还没有自动开仓记录';
  const dayLine = days.map((row) => `${row.day} ${count(row.opened)} 笔 · ${usdtText(row.usdt)}`).join(' · ');
  return `
    <div class="row-head">
      <h3>自动交易（全部组）</h3> ${badge}
      <span class="grow"></span>
      <span class="small muted">${esc(data.status || '')}</span>
    </div>
    ${data.disabled_reason && !on ? `<div class="notice error">上次自动关闭的原因：${esc(data.disabled_reason)}</div>` : ''}
    <div class="stats rh-auto-stats">${statCards}</div>
    <p class="small muted">金额是开仓当时的单腿名义，和上面「单腿名义」同一口径，价差另一腿金额相同。累计金额写在本地，开仓记录只留最近 300 笔，裁掉以后累计金额还在。这次更新之前的开仓，金额按 0。</p>
    <p class="small muted">按合约（最近 ${count(stats.recorded)} 笔记录）：${esc(symbolLine)}</p>
    ${dayLine ? `<p class="small muted">近 7 日开仓（UTC）：${esc(dayLine)}</p>` : ''}
    <p class="small muted">所有你配了 API 的交易所两两组成的组都参与（实盘：两家都已连接）。任何一组的信号「回到正常净收益」（扣掉滑点缓冲）达到门槛、手续费占比不超上限、且开仓可成交价差与收敛到 0 净收益都为正（开了「相对正常基差」则不要求这一条）、连续保持够久后，按下面的参数自动下一笔价差单。每一笔都走和手动下单同一套检查：对账干净、按你的金额现拉盘口重算、单笔上限${maxSize ? `（${esc(String(maxSize))} USDT）` : ''}、持仓数上限、当日亏损、Telegram /pause 与熔断。同一合约已有仓位不再新开（开了补仓时，同方向更好的信号会往那笔上补）；每次尝试后该合约冷却 10 分钟；执行中断（结果未知）或连续 ${data.max_failures} 次回滚会自动关闭。</p>
    <div class="rh-auto-grid">
      <label class="field"><span>账户</span><select data-rh-auto="mode">
        <option value="paper"${f.mode === 'paper' ? ' selected' : ''}>纸面（不碰真实资金）</option>
        <option value="live"${f.mode === 'live' ? ' selected' : ''}>实盘（真实资金）</option>
      </select></label>
      ${RH_AUTO_FIELDS.map(field).join('')}
      <label class="field" title="历史分位用的 K 线周期"><span>历史 K 线周期</span><select data-rh-auto="history_interval">${CHART_INTERVALS.map((i) => `<option value="${i.name}"${f.history_interval === i.name ? ' selected' : ''}>${esc(i.label)}</option>`).join('')}</select></label>
      <label class="field" title="逗号分隔，如 NVDA,SPY；留空 = 全部合约"><span>只做这些合约</span><input type="text" data-rh-auto="symbols" value="${esc(f.symbols)}" placeholder="全部" /></label>
      <label class="field" title="逗号分隔的交易所短名，如 bitget,lighter：两条腿里有它的组合不自动开仓、不补仓；已有仓位的止盈止损等规则照常。留空 = 不排除"><span>不自动交易的交易所</span><input type="text" data-rh-auto="exclude_venues" value="${esc(f.exclude_venues)}" placeholder="不排除" /></label>
      <label class="field" title="逗号分隔的合约名，如 ANTHROPIC,OPENAI：所有组合都不自动开仓、不补仓；已有仓位的止盈止损等规则照常，提醒照推。留空 = 不排除"><span>不自动交易的合约</span><input type="text" data-rh-auto="exclude_symbols" value="${esc(f.exclude_symbols)}" placeholder="不排除" /></label>
    </div>
    <label class="small rh-auto-check"><input type="checkbox" data-rh-auto="back_to_normal"${f.back_to_normal ? ' checked' : ''} /> 回到同时段正常基差就平仓（按方向换算成「基差收敛平仓」目标，按盘口核对净收益为正才平）</label>
    <label class="small rh-auto-check"><input type="checkbox" data-rh-auto="relative_to_normal"${f.relative_to_normal ? ' checked' : ''} /> 相对正常基差（不要求开仓价差为正、不算「收敛到 0」；只看回到正常水平扣成本后的净收益。必须同时开上一项并设止损）</label>
    <label class="small rh-auto-check"><input type="checkbox" data-rh-auto="cheap_leg_first"${f.cheap_leg_first ? ' checked' : ''} /> 先下费率明显低的那条腿（如 Arcus ↔ RH 先下 RH：没成交什么都不花，成交了再去盘口厚的 Arcus 对冲；不勾 = 按预估滑点大的先下）</label>
    <label class="small rh-auto-check"><input type="checkbox" data-rh-auto="deduct_funding"${f.deduct_funding ? ' checked' : ''} /> 预估收益扣持仓期资金费：按两家当前资金费率，持仓按「超时退出」的小时数算（没设按 24 小时），只扣要付的、不加能收的；拿不到费率时不扣</label>
    ${warnings.map((w) => `<div class="alert warn small">${esc(w)}</div>`).join('')}
    ${a.message ? `<div class="alert ${a.message.tone === 'error' ? 'error' : 'info'} small">${esc(a.message.text)}</div>` : ''}
    <div class="plan-actions">
      ${on
        ? `<button type="button" class="btn ghost" data-rh-auto-save${a.busy ? ' disabled' : ''}>保存参数（保持开启）</button><button type="button" class="btn primary" data-rh-auto-off${a.busy ? ' disabled' : ''}>关闭自动交易</button>`
        : `<button type="button" class="btn ghost" data-rh-auto-save${a.busy ? ' disabled' : ''}>只保存参数</button><button type="button" class="btn primary" data-rh-auto-on${a.busy ? ' disabled' : ''}>${f.mode === 'live' ? '开启实盘自动交易' : '开启纸面自动交易'}</button>`}
      ${a.dirty ? '<span class="small muted">有未保存的改动</span>' : ''}
    </div>
    <div class="small muted">今天（UTC）已自动开 ${Number(data.opened_today) || 0} 笔；自动仓位持有中 ${opened.length} 笔${opened.length ? `：${opened.map((o) => esc(`${o.id} ${o.symbol}`)).join('、')}` : ''}${(data.cooldown || []).length ? `；冷却中：${data.cooldown.map((c) => esc(`${c.symbol} ${c.sec}s`)).join('、')}` : ''}</div>
    ${events.length ? `<details class="rh-auto-events"><summary>最近动作（${events.length}）</summary><ul>${events.map((e) => `<li><span class="muted">${when(e.at)}</span> <b>${esc(RH_AUTO_EVENT[e.kind] || e.kind)}</b> ${esc(e.text)}</li>`).join('')}</ul></details>` : ''}`;
}

function renderRhAuto() {
  const box = $('rh-auto');
  if (!box) return;
  // 正在输入时不重画：只更新状态文字。
  if (box.contains(document.activeElement) && document.activeElement.matches('input, select')) return;
  box.innerHTML = rhAutoHtml(state.rhAuto, state.tradeConfig);
  for (const input of box.querySelectorAll('[data-rh-auto]')) {
    const key = input.getAttribute('data-rh-auto');
    const update = () => {
      state.rhAuto.form[key] = input.type === 'checkbox' ? input.checked : input.value;
      state.rhAuto.dirty = true;
    };
    input.addEventListener('input', update);
    input.addEventListener('change', () => {
      update();
      renderRhAuto();
    });
  }
  box.querySelector('[data-rh-auto-on]')?.addEventListener('click', () => saveRhAuto(true));
  box.querySelector('[data-rh-auto-off]')?.addEventListener('click', () => saveRhAuto(false));
  box.querySelector('[data-rh-auto-save]')?.addEventListener('click', () => saveRhAuto(Boolean(state.rhAuto.data?.settings?.enabled)));
}

function scheduleRh() {
  clearInterval(state.rhTimer);
  state.rhTimer = null;
  if (state.page !== 'rhspread') return;
  state.rhTimer = setInterval(() => {
    // 上一次还没回来就不再发：连接慢时不让请求越堆越多。
    if (document.hidden || state.page !== 'rhspread' || state.rhLoading) return;
    state.rhLoading = true;
    loadRhSpread().catch(() => {}).finally(() => {
      state.rhLoading = false;
    });
  }, RH_REFRESH_MS);
}

// ───────────────────────────── 路由与刷新 ─────────────────────────────

function showPage(page) {
  if (!PAGES.includes(page)) page = 'opps';
  state.page = page;
  for (const id of PAGES) $(`page-${id}`).classList.toggle('hidden', id !== page);
  for (const item of document.querySelectorAll('.nav-item')) {
    item.classList.toggle('active', item.getAttribute('data-page') === page);
  }
  // 价差图在 zuudot 风格的 /trading?… 链接上（loadChart 会写回参数）；离开它就回到根路径，免得刷新又落回图表页。
  const target = page === 'chart' && location.pathname === '/trading' ? `${location.pathname}${location.search}#chart` : `/#${page}`;
  if (`${location.pathname}${location.search}${location.hash}` !== target) history.replaceState(null, '', target);
  scheduleRh();
  scheduleChart();
  refresh();
}

function schedule() {
  clearInterval(state.timer);
  if ($('auto').checked) {
    state.timer = setInterval(() => {
      // 页面不可见时不要继续打接口 —— 后台标签页刷新只会白白消耗上游配额。
      if (!document.hidden) refresh();
    }, REFRESH_MS);
  }
}

async function refresh() {
  try {
    if (state.page === 'strategy') {
      await loadPairs();
    } else if (state.page === 'rhspread') {
      await loadRhSpread();
    } else if (state.page === 'chart') {
      // 价差图首次进入时拉一次；之后由 scheduleChart() 每分钟刷新，不跟着全局刷新打。
      if (!state.chart.data && !state.chart.fund && !state.chart.loading) await loadChart();
    } else if (state.page === 'positions') {
      // 正在确认或执行平仓时不重画：会把确认输入框冲掉。
      if (state.pos.confirming || state.pos.busy) return;
      maybeRefreshAccount();
      await loadPositions();
    } else {
      await loadScan();
    }
  } catch (error) {
    const target = state.page === 'strategy' ? 'market-empty' : state.page === 'positions' ? 'pos-open' : state.page === 'rhspread' ? 'rh-notice' : state.page === 'chart' ? 'chart-notes' : 'notices';
    $(target).__html = null;
    $(target).innerHTML = `<div class="notice error">取数失败：${esc(error.message)}</div>`;
  }
}

function wire() {
  for (const item of document.querySelectorAll('.nav-item')) {
    item.addEventListener('click', () => showPage(item.getAttribute('data-page')));
  }
  $('refresh').addEventListener('click', refresh);
  $('measure').addEventListener('click', async () => {
    state.view = 'spread';
    for (const tab of document.querySelectorAll('.view-tab')) {
      tab.classList.toggle('active', tab.getAttribute('data-view') === 'spread');
    }
    state.measure = 20;
    $('measure').disabled = true;
    $('measure').textContent = '实测中…';
    try {
      await loadScan();
    } catch (error) {
      $('notices').innerHTML = `<div class="notice error">实测收敛失败：${esc(error.message)}</div>`;
    } finally {
      state.measure = 0;
      $('measure').disabled = false;
      $('measure').textContent = '实测收敛';
    }
  });
  $('auto').addEventListener('change', schedule);
  for (const tab of document.querySelectorAll('.view-tab')) {
    tab.addEventListener('click', () => {
      state.view = tab.getAttribute('data-view');
      state.selected = null;
      for (const other of document.querySelectorAll('.view-tab')) {
        other.classList.toggle('active', other === tab);
      }
      // 两个视角用同一份数据，切换不需要重新取数。
      render();
    });
  }
  $('spread-hold').addEventListener('change', refresh);
  // 「持有天数」会改变排名，不是纯展示参数 —— 必须重新向服务端要数据。
  $('amortize').addEventListener('change', refresh);
  $('top').addEventListener('change', refresh);
  // 杠杆只改风险列，但风险是服务端算的，仍要重新取数；口径切换是纯展示，本地重画即可。
  $('leverage').addEventListener('change', refresh);
  $('norm').addEventListener('change', () => state.data && renderRows(state.data));
  let debounce = null;
  for (const id of ['symbols', 'fee', 'basis-gate']) {
    $(id).addEventListener('input', () => {
      if (state.booting) return;
      clearTimeout(debounce);
      debounce = setTimeout(refresh, 400);
    });
  }

  // 策略页
  const s = state.strategy;
  $('venue-a').addEventListener('change', () => {
    s.userPicked = true;
    s.a = $('venue-a').value;
    if (s.a === s.b) s.b = null;
    loadPairs().catch((error) => ($('market-empty').textContent = error.message));
  });
  $('venue-b').addEventListener('change', () => {
    s.userPicked = true;
    s.b = $('venue-b').value;
    if (s.a === s.b) s.a = null;
    loadPairs().catch((error) => ($('market-empty').textContent = error.message));
  });
  $('swap').addEventListener('click', () => {
    s.userPicked = true;
    [s.a, s.b] = [s.b, s.a];
    loadPairs().catch((error) => ($('market-empty').textContent = error.message));
  });
  $('market-search').addEventListener('input', () => {
    s.search = $('market-search').value;
    renderStrategy();
  });
  $('pc-only').checked = s.pcOnly;
  $('pc-only').addEventListener('change', () => {
    s.pcOnly = $('pc-only').checked;
    try {
      localStorage.setItem('arb-web-orderable-only', s.pcOnly ? 'on' : 'off');
    } catch {
      // 存不了就只在本次会话里生效。
    }
    renderStrategy();
  });
  for (const button of $('strategy-view').querySelectorAll('button')) {
    button.addEventListener('click', () => {
      const view = button.getAttribute('data-view');
      if (view === s.view || state.trade.busy) return;
      s.view = view;
      try {
        localStorage.setItem('arb-web-strategy-view', view);
      } catch {
        // 存不了就只在本次会话里生效。
      }
      // 两个视角的方向定法不同：选中的方向作废，按新视角重新取表。
      s.selected = null;
      s.plan = null;
      resetTrade();
      loadPairs().catch((error) => ($('market-empty').textContent = error.message));
    });
  }
  for (const button of $('apr-seg').querySelectorAll('button')) {
    button.addEventListener('click', () => {
      s.norm = button.getAttribute('data-norm');
      renderStrategy();
    });
  }

  // 主题
  renderThemeButton();
  $('theme-btn').addEventListener('click', toggleTheme);
  followSystemTheme();

  // 令牌
  $('token-btn').addEventListener('click', () => {
    $('token-panel').classList.toggle('hidden');
    $('token-input').value = '';
    renderTokenButton();
    if (!$('token-panel').classList.contains('hidden')) $('token-input').focus();
  });
  $('token-save').addEventListener('click', () => {
    const value = $('token-input').value.trim();
    if (!value) return;
    setToken(value);
    $('token-input').value = '';
    $('token-panel').classList.add('hidden');
    renderTradeBox();
    if (state.page === 'positions') {
      renderPositions();
      refresh();
    }
  });
  $('token-input').addEventListener('keydown', (event) => {
    if (event.key === 'Enter') $('token-save').click();
  });
  $('token-clear').addEventListener('click', () => {
    setToken('');
    $('token-input').value = '';
    renderTradeBox();
    state.pos.account = null;
    if (state.page === 'positions') {
      renderPositions();
      refresh();
    }
  });

  // 交易模式（令牌面板里）
  for (const button of $('mode-seg').querySelectorAll('button')) {
    button.addEventListener('click', () => setMode(button.getAttribute('data-mode')));
  }
  renderModeControls();

  // 持仓页
  $('pos-monitor').addEventListener('click', runMonitor);
  $('pos-account-btn').addEventListener('click', () => loadAccount());
  $('pos-collapse-all').addEventListener('click', () => {
    setAllCollapsed(true);
    renderPositions();
  });
  $('pos-expand-all').addEventListener('click', () => {
    setAllCollapsed(false);
    renderPositions();
  });

  // RH 价差页
  $('rh-signal-only').addEventListener('change', reloadRhSpread);
  $('rh-limit')?.addEventListener('change', (event) => setRhLimit(event.target.value));
  $('rh-rows').addEventListener('click', onRhRowClick);
  $('rh-watch')?.addEventListener('change', (event) => setRhShowWatch(event.target.checked));
  // 价差图表
  wireChart();
  $('rh-pair').addEventListener('change', (event) => setRhPairFilter(event.target.value));
}

(async function main() {
  wire();
  try {
    await loadConfig();
  } catch (error) {
    console.warn('配置加载失败', error);
  }
  try {
    await loadTradeConfig();
  } catch (error) {
    console.warn('交易配置加载失败', error);
  }
  state.booting = false;
  // 顶栏的场所/合约计数来自机会榜；其它页也先拉一次，让顶栏有数。
  let initial = location.hash.replace('#', '');
  // zuudot 风格的链接（/trading?symbol=…&exchange1=…&exchange2=…）直接打开价差图。
  const fromUrl = location.pathname === '/trading' || initial === 'chart' ? chartParamsFromUrl(location.search) : null;
  if (location.pathname === '/trading') initial = 'chart';
  if (fromUrl) {
    Object.assign(state.chart.form, fromUrl, { days: '', fee1: '', fee2: '' });
    fillChartControls(state.chart.form);
  }
  if (initial && initial !== 'opps') loadScan().catch(() => {});
  showPage(initial || 'opps');
  schedule();
})();
