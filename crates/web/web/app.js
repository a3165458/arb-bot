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
const PAGES = ['opps', 'strategy', 'positions', 'rhspread'];
// RH 价差页：服务端每秒算一次，页面每 3 秒取一次。
const RH_REFRESH_MS = 3000;
// 交易模式的本地存储键。要在 state 初始化之前定义：storedMode() 在初始化时就会用到。
const MODE_KEY = 'arb-web-mode';

let state = {
  page: 'opps',
  rh: null,
  rhTimer: null,
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
    confirming: null,
    confirmText: '',
    busy: false,
    round: null,
    account: null,
    // 交易所凭据识别（/api/trade/accounts）：没开实盘时撑起「实盘」页，开着时附在账户卡片里。
    credentials: null,
    flash: null,
    ruleDrafts: new Map(),
    modeSeq: 0,
    loadSeq: 0,
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
  for (const draft of p.ruleDrafts.values()) {
    draft.wouldTrigger = null;
    draft.force = false;
  }
  state.strategy.pairSeq += 1;
  state.strategy.planSeq += 1;
  p.confirming = null;
  p.confirmText = '';
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
  { key: 'takeProfit', id: 'rule-take-profit', field: 'take_profit', source: 'take_profit_usdt', label: '净收益止盈', hint: '含已结算资金费的净盈利达到目标后，服务端按盘口扣除退出成本再核对；资金费未知时不评估', unit: 'USDT', value: '10', min: '0', step: '0.01' },
  { key: 'autoMargin', id: 'rule-auto-margin', field: 'auto_margin', source: 'auto_margin_pct', label: '自动追加保证金', hint: '仅逐仓：任一腿强平距离低于触发线时补保证金；两腿共用整笔累计额度，结果未知也占额度，不重发', unit: '%', value: '15', min: '0', step: '0.5' },
];

function ruleControl(def, rule, id = def.id, showState = false, mode = 'isolated') {
  const hint = mode === 'cross' && def.key === 'protect' ? '全仓仅按交易所有效强平价判断，触发时整笔退出；缺数据会提示无法评估，不使用逐仓减仓公式' : def.hint;
  const disabled = rule.on ? '' : ' disabled';
  return `<div class="rule${def.key === 'autoMargin' ? ' rule-pair' : ''}">
    <input type="checkbox" id="${id}-on" data-rule-field="${def.key}" data-rule-part="on"${rule.on ? ' checked' : ''} aria-label="启用${def.label}" />
    <label class="desc" for="${id}-on">${def.label}${showState ? ` <span class="tag ${rule.on ? 'sky' : ''}" data-rule-state="${def.key}">${rule.on ? '已启用' : '已关闭'}</span>` : ''}<small>${hint}</small></label>
    <span class="rule-values"><label><span class="sr-only">${def.label}${def.key === 'autoMargin' ? '触发线' : '阈值'}</span><input type="number" id="${id}-value" data-rule-field="${def.key}" data-rule-part="value" value="${esc(rule.value)}" step="${def.step}" min="${def.min}"${def.max ? ` max="${def.max}"` : ''}${disabled} /> ${def.unit}</label>
      ${def.key === 'autoMargin' ? `<label>累计上限 <input type="number" id="${id}-max" data-rule-field="${def.key}" data-rule-part="max" value="${esc(rule.max)}" step="0.01" min="0"${disabled} /> USDT</label>` : ''}
    </span>
  </div>`;
}

// 开仓按策略视角选择费差 / 基差规则；改持仓规则则整套发送，off 明确关闭。
function ruleFields(form, view = null) {
  const fields = {};
  for (const def of RULE_FIELDS) {
    const rule = form[def.key];
    const active = rule.on && !(view === 'spread' && def.key === 'minApr') && !(view === 'funding' && def.key === 'basisExit');
    fields[def.field] = active ? rule.value : 'off';
    if (def.key === 'autoMargin') fields.auto_margin_max = active ? rule.max : 'off';
  }
  return fields;
}

function ruleFormError(form, view = null) {
  if (form.marginMode === 'cross' && form.autoMargin.on) return '全仓不能自动追加逐仓保证金，请明确关闭此规则。';
  const fields = ruleFields(form, view);
  for (const def of RULE_FIELDS) {
    if (fields[def.field] !== 'off' && num(fields[def.field]) === null) return `${def.label}已启用，请填写有效数值或明确关闭。`;
  }
  if (form.autoMargin.on && num(fields.auto_margin_max) === null) return '自动追加保证金已启用，请同时填写触发线和累计上限。';
  return null;
}

const marginModeLabel = (mode) => mode === 'cross' ? '全仓' : '逐仓';

function marginModeError(venues, form) {
  if (form.marginMode !== 'cross' || state.mode !== 'live') return null;
  const modes = state.tradeConfig?.margin_modes;
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
      if (def.key === 'autoMargin') {
        f.autoMargin.max = $('rule-auto-margin-max').value;
        $('rule-auto-margin-max').disabled = !f.autoMargin.on;
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
  if (result.reconciliation?.divergences?.length) return false;
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
    hints.push('「台账之外的持仓」不是本程序开的（手动或其它程序）。它在的时候对账不会干净，实盘开仓会被拦住：平掉它，或给本程序单独用一个子账户 / API key。');
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
  if (rules?.auto_margin_pct != null) parts.push(`强平距离 < ${num(rules.auto_margin_pct)}% 追加逐仓保证金，累计上限 ${usd(rules.auto_margin_max_usdt)}`);
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
      on: value != null || (def.key === 'autoMargin' && position.rules?.auto_margin_max_usdt != null),
      value: value == null ? text(remembered[def.key]?.value, def.value) : def.key === 'minApr' ? ruleAprPercent(value) : String(value),
    };
    if (def.key === 'autoMargin') form.autoMargin.max = position.rules?.auto_margin_max_usdt == null
      ? text(remembered.autoMargin?.max, '100') : String(position.rules.auto_margin_max_usdt);
  }
  return form;
}

function rememberPositionRuleValues(id, form) {
  const values = {};
  for (const def of RULE_FIELDS) {
    values[def.key] = { value: form[def.key].value };
    if (def.key === 'autoMargin') values.autoMargin.max = form.autoMargin.max;
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
    api(`/api/positions?mode=${mode}`, { auth: live }),
    api(`/api/trade/round?mode=${mode}`, { auth: live }),
  ]);
  if (mode !== state.mode || seq !== p.loadSeq || modeSeq !== p.modeSeq) return;
  if (!positions.ok) throw new Error(positions.body.error);
  state.positions = positions.body;
  p.round = round.ok ? round.body : null;
  renderPositions();
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
        const what = r.retried_exit
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
  if (live && p.confirmText.trim() !== id) return;
  p.busy = true;
  renderPositions();
  const body = { mode: state.mode, position_id: id };
  if (live) body.confirm = p.confirmText.trim();
  const response = await api('/api/trade/close', { method: 'POST', body, auth: true });
  p.busy = false;
  p.confirming = null;
  p.confirmText = '';
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
  renderFlash();
  await loadPositions().catch(() => {});
  if (live) loadAccount();
}

function positionActions(position) {
  const p = state.pos;
  const cfg = state.tradeConfig;
  const live = state.mode === 'live';
  if (!cfg?.auth_configured || !token()) return '';
  if (live && cfg.live?.mode !== 'trade') return '<span class="muted small">实盘只读，不能平仓</span>';
  const retry = position.status !== 'open';
  const label = retry ? '重试退出' : '平仓';
  if (p.confirming !== position.id) {
    return `<button type="button" class="btn ghost" data-close="${esc(position.id)}"${p.busy ? ' disabled' : ''}>${label}</button>`;
  }
  if (!live) {
    return `<span class="small">确认纸面${label}？</span>
      <button type="button" class="btn primary" data-close-go="${esc(position.id)}"${p.busy ? ' disabled' : ''}>${p.busy ? '执行中…' : '确认'}</button>
      <button type="button" class="btn ghost" data-close-cancel="1"${p.busy ? ' disabled' : ''}>取消</button>`;
  }
  const matched = p.confirmText.trim() === position.id;
  return `<input class="close-confirm" id="close-confirm" autocomplete="off" placeholder="输入 ${esc(position.id)} 确认" value="${esc(p.confirmText)}" />
    <button type="button" class="btn danger" data-close-go="${esc(position.id)}"${!matched || p.busy ? ' disabled' : ''}>${p.busy ? '执行中…' : `实盘${label}`}</button>
    <button type="button" class="btn ghost" data-close-cancel="1"${p.busy ? ' disabled' : ''}>取消</button>`;
}

// 一条腿开仓以来实际收付的资金费（交易所结算流水）。
function legFundingText(leg, label) {
  if (!leg) return '';
  if (leg.usdt == null) return `${label} ${esc(leg.venue)} <span class="muted" title="${esc(leg.note || '')}">未知</span>`;
  const last = leg.last_at ? `，最近 ${when(leg.last_at)}` : '';
  return `${label} ${esc(leg.venue)} <b class="${cls(leg.usdt)}">${pnlUsd(leg.usdt)}</b><span class="muted">（${leg.payments} 次${last}）</span>`;
}

// 上一轮后台规则按两边盘口估的「现在平掉整笔」含资金费净额。与标记价净额口径不同，分开展示。
function bookExitText(report) {
  if (!report) return '<span class="muted" title="等后台规则轮核对">待核对</span>';
  const obs = report.evaluation?.observation;
  const exit = obs?.exit;
  const funding = num(obs?.funding_usdt);
  if (exit && num(exit.net_usdt) !== null && funding !== null) {
    const net = num(exit.net_usdt) + funding;
    const title = `多腿卖 ${exit.long_price}、空腿买回 ${exit.short_price}；已扣已付与预估平仓手续费、含已结算资金费`;
    return `<b class="${cls(net)}" title="${esc(title)}">${pnlUsd(net)}</b>`;
  }
  if (obs?.exit_unavailable) return `<span class="muted" title="${esc(obs.exit_unavailable)}">盘口用不了</span>`;
  return '<span class="muted">未知</span>';
}

function pnlSummary(position, observation, report) {
  const pnl = observation?.pnl;
  const cell = (label, value) => `<span>${label} <b class="${cls(value)}">${num(value) === null ? '未知' : pnlUsd(value)}</b></span>`;
  const funding = position.funding;
  const net = observation?.net_with_funding_usdt;
  const target = position.rules?.take_profit_usdt;
  const scope = '按服务端标记价计，含已减仓部分已实现价格盈亏、已结算资金费及已付手续费；未扣预估平仓手续费与穿价';
  return `${pnl ? `<div class="pnl-row">
    ${cell('剩余两腿价格盈亏', pnl.price_pnl_usdt)}
    <span>剩余两腿已付手续费 <b>${usd(pnl.fees_usdt, 4)}</b></span>
    ${cell('剩余两腿净额（不含资金费 / 已退出部分）', pnl.net_usdt)}
    <span>按当前费率每日资金费 ≈ <b class="${cls(pnl.funding_daily_usdt)}">${pnl.funding_daily_usdt == null ? '未知' : `${pnlUsd(pnl.funding_daily_usdt)} / 天`}</b></span>
  </div>` : ''}
  <div class="pnl-row funding-row">
    ${cell(state.mode === 'paper' ? '已结算资金费（纸面为 0）' : '已结算资金费', observation?.funding_usdt)}
    ${cell('整笔净收益（含资金费）', net)}
    <span class="muted small">${scope}</span>
    ${funding ? `<span class="small" title="交易所流水按账户及合约记录，同账户同合约的其他仓位也可能计入">${[legFundingText(funding.long, '多'), legFundingText(funding.short, '空')].filter(Boolean).join(' · ')}</span>` : ''}
  </div>
  <div class="pnl-row rule-progress">
    <span data-take-profit-progress>止盈进度（标记价）：${target == null ? '未启用' : `<b class="${cls(net)}">${num(net) === null ? '未知' : pnlUsd(net)}</b> / 目标 <b>${usd(target)}</b>（USDT；触发后仍须核对盘口）`}</span>
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
      ? '实盘台账里的双腿仓位（真实资金）。平仓要输入仓位 id 二次确认；规则由看板后台执行，对账不干净时自动跳过。'
      : '纸面台账里的双腿仓位，按当前行情评估每笔仓位的规则。配置了令牌时可以在这里平仓、手动跑一轮规则。';
  $('pos-closed-wrap').classList.toggle('hidden', setup);
  $('pos-monitor').classList.toggle('hidden', setup);
  if (setup) {
    $('pos-round').innerHTML = '<span class="h-caution">实盘未开启（ARB_WEB_LIVE=off）</span>';
    $('pos-account-btn').classList.add('hidden');
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
  // 合计只在每一笔都算得出来时才给；有一笔缺行情就显示「—」，不拿部分和冒充全部。
  const sumOf = (pick) => {
    const values = open.map((p) => num(pick(p.evaluation?.observation)));
    return values.length && values.every((v) => v !== null) ? values.reduce((a, b) => a + b, 0) : null;
  };
  const netTotal = sumOf((obs) => obs?.net_with_funding_usdt);
  const fundingTotal = sumOf((obs) => obs?.pnl?.funding_daily_usdt);
  $('pos-stats').innerHTML = [
    ['持仓', `${open.length} 笔`, ''],
    ['两腿名义合计', usd(notional, 0), ''],
    ['整笔净收益（含资金费）', open.length ? pnlUsd(netTotal) : '—', cls(netTotal)],
    ['每日资金费（估）', open.length ? pnlUsd(fundingTotal) : '—', cls(fundingTotal)],
    ['最近的强平距离', distances.length ? `${Math.min(...distances).toFixed(2)}%` : '—', ''],
    ['规则触发', `${pending} 笔`, ''],
  ]
    .map(([label, value, tone]) => `<div class="stat"><span>${label}</span><b class="${tone}">${esc(value)}</b></div>`)
    .join('');

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
            const notes = [
              ...external.map((n) => `<div class="alert warn"><b>对账发现：</b>${esc(n.message)}</div>`),
              exit
                ? `<div class="alert info">上一轮（${when(state.pos.round.last_round.at)}）按盘口核对平仓：多腿卖 ${price(exit.long_price)}、空腿买回 ${price(exit.short_price)}，平仓价差 ${pctRaw(exit.exit_basis_pct, 3)}，整笔预估 <b class="${cls(exit.net_usdt)}">${pnlUsd(exit.net_usdt)}</b>（资金费前，已扣已付与预估平仓手续费）；含该轮已结算资金费合计 <b class="${cls(exitWithFunding)}">${exitWithFunding === null ? '未知' : pnlUsd(exitWithFunding)}</b>。是否执行以服务端本轮规则报告为准。</div>`
                : '',
              p.quotes_missing ? '<div class="alert warn">本轮快照里缺至少一条腿的行情，评估不了。</div>' : '',
              ...(evaluation?.skipped || []).map((s) => `<div class="alert warn">未评估：${esc(s)}</div>`),
              p.note ? `<div class="alert info">${esc(p.note)}</div>` : '',
            ].join('');
            return `<div class="card pos-card">
              <div class="pos-head">
                <span class="sym">${esc(p.symbol)}</span>
                <span class="small">${legsText(p.long?.venue, p.short?.venue)}</span>
                <span class="pill status">${STATUS_LABEL[p.status] || esc(p.status)}</span>
                <span class="tag">${p.leverage == null ? '杠杆未记录' : `${num(p.leverage)}x`}</span>
                <span class="tag sky">${marginModeLabel(p.margin_mode)}</span>
                ${p.strategy === 'spread' ? '<span class="tag sky">价差套利</span>' : ''}
                ${p.trims ? `<span class="tag warn">已减仓 ${p.trims} 次</span>` : ''}
                <span class="grow"></span>
                <span class="muted small">${esc(p.id)} · 开仓 ${when(p.opened_at)}</span>
              </div>
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

  for (const button of $('pos-open').querySelectorAll('[data-close]')) {
    button.addEventListener('click', () => {
      p.confirming = button.getAttribute('data-close');
      p.confirmText = '';
      renderPositions();
      $('close-confirm')?.focus();
    });
  }
  for (const button of $('pos-open').querySelectorAll('[data-close-go]')) {
    button.addEventListener('click', () => closePosition(button.getAttribute('data-close-go')));
  }
  for (const button of $('pos-open').querySelectorAll('[data-close-cancel]')) {
    button.addEventListener('click', () => {
      p.confirming = null;
      p.confirmText = '';
      renderPositions();
    });
  }
  $('close-confirm')?.addEventListener('input', (event) => {
    p.confirmText = event.target.value;
    const go = $('pos-open').querySelector('[data-close-go]');
    if (go) go.disabled = p.confirmText.trim() !== p.confirming || p.busy;
  });

  const closed = data.closed || [];
  $('pos-closed').innerHTML = closed
    .map(
      (p) => `<tr>
        <td class="dim">${esc(p.id)}</td>
        <td>${esc(p.symbol)}</td>
        <td>${p.long || p.short ? legsText(p.long?.venue, p.short?.venue) : '<span class="muted">两腿已退出</span>'}</td>
        <td><span class="pill status">${STATUS_LABEL[p.status] || esc(p.status)}</span></td>
        <td class="dim">${when(p.opened_at)}</td>
        <td class="dim">${when(p.closed_at)}</td>
        <td class="num">${realizedCell(p)}</td>
        <td class="small">${esc(p.note || '')}${p.pnl_unattributed ? esc(` 实际盈亏没能核出：${p.pnl_unattributed}`) : ''}</td>
      </tr>`,
    )
    .join('');
  $('pos-closed-empty').textContent = closed.length ? '' : '还没有结束的仓位。';
}

// ───────────────────────────── RH 价差（只读） ─────────────────────────────

const RH_DIRECTION = { long_arcus: '多 Arcus / 空 RH', long_lighter: '多 RH / 空 Arcus' };

// 一个方向的数字：可成交价差、收敛到 0、回到正常。
function rhBestLeg(line) {
  const direction = line.best?.direction;
  return direction ? line[direction] : null;
}

function rhStatus(line, view) {
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
  return `<tr class="${line.best?.signal ? 'rh-signal' : ''}" data-rh-base="${esc(line.base)}" title="点击打开下单界面：带上方向和回到正常基差的收敛目标（仍需预览确认）">
    <td><b>${esc(line.base)}</b> <span class="muted small">${esc((line.category || '').toLowerCase())}</span></td>
    <td>${esc(RH_SESSION[line.session] || line.session)}</td>
    <td class="num ${cls(line.basis_pct)}">${pctRaw(line.basis_pct, 3)}</td>
    <td class="num">${normalText}</td>
    <td class="num">${zText}</td>
    <td>${line.best ? esc(RH_DIRECTION[line.best.direction] || line.best.direction) : '<span class="muted">—</span>'}</td>
    <td class="num">${leg ? `${pctRaw(leg.entry_pct, 3)} <span class="muted small" title="平仓穿价">−${pctRaw(leg.exit_cross_pct, 3).replace('+', '')}</span>` : '—'}</td>
    <td class="num ${cls(leg?.net_to_zero_pct)}">${leg ? pctRaw(leg.net_to_zero_pct, 3) : '—'}</td>
    <td class="num ${cls(net)}">${net == null ? '<span class="muted">—</span>' : pctRaw(net, 3)}${usdText}</td>
    <td>${rhStatus(line, view)}</td>
  </tr>`;
}

const RH_SESSION = { rth: '盘中', off: '盘后', weekend: '周末', all: '全天' };

// RH 价差页的一行 → 策略页（价差视角）的下单参数。
// 基差口径不同：RH 页是 (Arcus − RH) / 均值；持仓规则是 (空腿 − 多腿) / 均值。
// 多 Arcus / 空 RH 时持仓基差 = −RH 页基差，所以「回到正常」的收敛目标 = −正常中位数；反方向就是中位数本身。
function rhOrderTarget(line) {
  const direction = line.best?.direction;
  const long = direction === 'long_arcus' ? 'arcus' : direction === 'long_lighter' ? 'lighter-rh' : null;
  const short = long === 'arcus' ? 'lighter-rh' : long ? 'arcus' : null;
  const leg = direction ? line[direction] : null;
  const median = num(line.normal?.median);
  let target = null;
  if (long && median !== null) {
    const raw = direction === 'long_arcus' ? -median : median;
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
    // 价差单要求空腿卖得出的价高于多腿要买的价：可成交价差不为正时预览会被拒绝。
    negativeEntry: leg ? num(leg.entry_pct) <= 0 : false,
  };
}

function rhOriginNote(origin) {
  if (!origin) return '';
  const parts = [`来自 RH 价差页（${esc(RH_SESSION[origin.session] || origin.session || '')}）：方向按两边深度、对照同时段正常基差选出`];
  parts.push(origin.target != null
    ? `基差收敛目标已设为 <b>${esc(origin.target)}%</b>（回到正常水平就平仓）`
    : '还没有正常基差样本，收敛目标沿用你原来的设置');
  if (origin.netToNormalPct != null) parts.push(`RH 页估算回到正常净收益 ${pctRaw(origin.netToNormalPct, 3)}`);
  const warn = origin.negativeEntry
    ? `<br><b>注意：</b>这个方向当前可成交价差为 ${pctRaw(origin.entryPct, 3)}（不为正），价差单要求空腿卖价高于多腿买价，预览会被拒绝。`
    : '';
  return `<div class="alert ${origin.negativeEntry ? 'warn' : 'info'} small">${parts.join('；')}。下单前仍需预览并确认。${warn}</div>`;
}

// 点 RH 价差页的一行：切到策略页价差视角，选好 Arcus ↔ Lighter RH、合约、方向和收敛目标。不下单。
function openRhInStrategy(line) {
  const order = rhOrderTarget(line);
  const s = state.strategy;
  s.userPicked = true;
  s.a = 'arcus';
  s.b = 'lighter-rh';
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

function renderRhSpread() {
  const view = state.rh;
  if (!view) return;
  const conn = view.connected || {};
  const dot = (ok, name) => `<span class="${ok ? 'pos' : 'neg'}">${ok ? '●' : '○'} ${name}</span>`;
  $('rh-status').innerHTML = `${dot(conn.lighter, 'Lighter RH')} · ${dot(conn.arcus, 'Arcus')} · 更新于 ${when(view.updated_at)}${conn.reconnects ? ` · 重连 ${conn.reconnects} 次` : ''}`;
  const lines = view.lines || [];
  const signals = lines.filter((l) => l.best?.signal);
  const hours = (view.history_minutes / 60).toFixed(1);
  $('rh-stats').innerHTML = [
    ['合约', String(lines.length), ''],
    ['当前信号', String(signals.length), signals.length ? 'pos' : 'dim'],
    ['估算名义', `$${num(view.size_usdt).toLocaleString('en-US')}`, ''],
    ['提醒门槛', `≥ ${num(view.alert_net_pct)}%`, ''],
    ['往返手续费', view.fee_round_trip_pct == null ? '—' : `${num(view.fee_round_trip_pct)}%`, ''],
    ['已攒历史', `${hours} 小时`, ''],
  ].map(([label, value, tone]) => `<div class="stat"><span>${label}</span><b class="${tone}">${esc(value)}</b></div>`).join('');
  const notices = [];
  if (view.error) notices.push(`<div class="notice error">${esc(view.error)}</div>`);
  if (view.history_minutes < view.min_minutes) {
    notices.push(`<div class="notice">正在积累历史：每个合约每个时段至少 ${view.min_minutes} 分钟样本才算出「正常基差」并开始提醒（窗口 ${view.window_days} 天）。在那之前只显示「收敛到 0」的估算 —— 对股票类合约这通常偏乐观。</div>`);
  }
  $('rh-notice').innerHTML = notices.join('');
  const shown = $('rh-signal-only').checked ? signals : lines;
  $('rh-rows').innerHTML = shown.map((line) => rhRow(line, view)).join('');
  for (const tr of $('rh-rows').querySelectorAll('tr[data-rh-base]')) {
    tr.addEventListener('click', () => {
      const line = (state.rh?.lines || []).find((l) => l.base === tr.getAttribute('data-rh-base'));
      if (line) openRhInStrategy(line);
    });
  }
  $('rh-empty').textContent = shown.length ? '' : (lines.length ? '当前没有信号。' : '等待行情…');
}

async function loadRhSpread() {
  const result = await api('/api/rh-spread');
  if (!result.ok) throw new Error(result.body?.error || `HTTP ${result.status}`);
  state.rh = result.body;
  renderRhSpread();
}

function scheduleRh() {
  clearInterval(state.rhTimer);
  state.rhTimer = null;
  if (state.page !== 'rhspread') return;
  state.rhTimer = setInterval(() => {
    if (!document.hidden && state.page === 'rhspread') loadRhSpread().catch(() => {});
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
  if (location.hash !== `#${page}`) history.replaceState(null, '', `#${page}`);
  scheduleRh();
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
    } else if (state.page === 'positions') {
      // 正在确认或执行平仓时不重画：会把确认输入框冲掉。
      if (state.pos.confirming || state.pos.busy) return;
      maybeRefreshAccount();
      await loadPositions();
    } else {
      await loadScan();
    }
  } catch (error) {
    const target = state.page === 'strategy' ? 'market-empty' : state.page === 'positions' ? 'pos-open' : state.page === 'rhspread' ? 'rh-notice' : 'notices';
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

  // RH 价差页
  $('rh-signal-only').addEventListener('change', renderRhSpread);
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
  const initial = location.hash.replace('#', '');
  if (initial && initial !== 'opps') loadScan().catch(() => {});
  showPage(initial || 'opps');
  schedule();
})();
