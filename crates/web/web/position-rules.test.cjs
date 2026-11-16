'use strict';

// Run: node --test crates/web/web/position-rules.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const vm = require('node:vm');

const source = readFileSync(`${__dirname}/app.js`, 'utf8');
const boot = source.indexOf('(async function main() {');
assert.ok(boot > 0);

function harness(storage = new Map()) {
  const context = vm.createContext({
    localStorage: {
      getItem: (key) => storage.get(key) ?? null,
      setItem: (key, value) => storage.set(key, value),
    },
    document: { getElementById: () => null },
    URLSearchParams,
    // 不跑真定时器：补仓价差对比的 2 秒轮询在测试里不排下一轮。
    setTimeout: () => 0,
    clearTimeout: () => {},
  });
  // Load the real application, omitting only browser startup/network requests.
  vm.runInContext(source.slice(0, boot) + `
    globalThis.app = {
      state, RULE_FIELDS, positionRuleForm, positionRulesEditor, ruleFields,
      rememberPositionRuleValues, rulePreferenceKey, ruleDraftKey, wirePositionRules,
      savePositionRules, saveStrategyParams, tradeBody, marginModeError, ruleFormError,
      pnlSummary, bookExitText, bookNet, positionTotals, tradeStatsParts, inFlightNotice, closedDetailHtml, closedPagerHtml, CLOSED_PER_PAGE, accountBalancesHtml,
      closedActionHtml, closedTagsHtml, closedToolsHtml, dismissClosed,
      positionActions, defaultAddSize, closePosition, addToPosition, positionEntrySpread, addSpreadHtml, refreshAddQuote, stopAddQuote,
      collapsedSet, toggleCollapsed, setAllCollapsed, isCollapsed, positionAlertCount, collapsedSummary,
      rhRow, rhStatus, rhMakerText, rhMakerSummaryHtml, renderRhSpread, rhOrderTarget, rhOriginNote, openRhInStrategy, rhAutoForm, rhAutoBody, rhAutoHtml, saveRhAuto,
      chartQuery, chartStatsHtml, chartDefaults, CHART_INTERVALS,
      fundingQuery, chartRangeButtons, chartParamsFromUrl, chartShareUrl, chartRefLines, chartFundingSeries,
      chartFundingStripHtml, chartFundingSummaryHtml, chartLegendHtml, fmtCountdown, chartValueAt, chartAutoRefreshMs,
      mockPrompt(fn) { globalThis.window = { prompt: fn }; },
      mockWindow(win) { globalThis.window = win; },
      mockShowPage(fn) { showPage = fn; },
      mockApi(fn) { api = fn; },
      mockRefresh(fn) { loadPositions = fn; },
    };
    renderPositions = () => {};
    renderFlash = () => {};
    loadPositions = async () => {};
  `, context);
  const app = context.app;
  app.state.mode = 'paper';
  app.state.tokenMemory = 'test-token';
  app.state.tradeConfig = { auth_configured: true, live: { mode: 'trade' } };
  const position = {
    id: 'position-1', status: 'open', rules: {},
    long: { venue: 'binance' }, short: { venue: 'okx' },
  };
  app.state.positions = { open: [position] };
  return { app, context, storage, position };
}

const plain = (value) => JSON.parse(JSON.stringify(value));

function element(dataset = {}) {
  const listeners = {};
  return {
    dataset, disabled: false, checked: false, value: '',
    addEventListener: (name, fn) => { listeners[name] = fn; },
    emit: (name) => listeners[name]?.(),
  };
}

function wire(h) {
  const { app, context, position } = h;
  const form = app.positionRuleForm(position);
  const fields = app.RULE_FIELDS.flatMap((def) => {
    const parts = def.key === 'autoMargin' ? ['on', 'value', 'max'] : ['on', 'value'];
    return parts.map((part) => Object.assign(element({ ruleField: def.key, rulePart: part }), {
      checked: form[def.key].on, value: form[def.key][part],
      disabled: part !== 'on' && !form[def.key].on,
    }));
  });
  const badges = app.RULE_FIELDS.map((def) => element({ ruleState: def.key }));
  const controls = Object.fromEntries([
    'data-rule-feedback', 'data-rule-dirty', 'data-save-rules',
    'data-cancel-rules', 'data-rule-margin-note', 'data-rule-force',
  ].map((name) => [`[${name}]`, element()]));
  const editor = {
    dataset: { ruleEditor: position.id },
    querySelector: (selector) => controls[selector],
    querySelectorAll: (selector) => selector === '[data-rule-field]' ? fields : badges,
  };
  context.document.getElementById = () => ({ querySelectorAll: () => [editor] });
  app.wirePositionRules();
  return {
    controls,
    input(key, part, value) {
      const field = fields.find((f) => f.dataset.ruleField === key && f.dataset.rulePart === part);
      if (part === 'on') field.checked = value;
      else field.value = value;
      field.emit('input');
    },
    field: (key, part) => fields.find((f) => f.dataset.ruleField === key && f.dataset.rulePart === part),
  };
}

test('all six rules are visible without opening an editor; viewing creates no stale draft', () => {
  const { app, position } = harness();
  position.rules = { min_funding_apr: '0.0525', liq_protection_pct: '8' };
  const html = app.positionRulesEditor(position);
  for (const def of app.RULE_FIELDS) assert.ok(html.includes(def.label));
  assert.match(html, /已启用/);
  assert.match(html, /已关闭/);
  assert.match(html, /value="5.25"/);
  assert.match(html, /data-save-rules="position-1" disabled/);
  assert.equal(app.state.pos.ruleDrafts.size, 0);
  position.rules.min_funding_apr = '0.071234567890123456';
  assert.equal(app.positionRuleForm(position).minApr.value, '7.1234567890123456');
});

test('only complete Open positions expose controls; unauthorized/readonly users can view but not edit', () => {
  const { app, position } = harness();
  for (const status of ['opening', 'closing', 'closed', 'unwinding', 'unwound']) {
    assert.equal(app.positionRulesEditor({ ...position, status }), '');
  }
  assert.equal(app.positionRulesEditor({ ...position, short: null }), '');
  app.state.tokenMemory = '';
  assert.match(app.positionRulesEditor(position), /fieldset class="rules" disabled/);
  app.state.tokenMemory = 'test-token';
  app.state.mode = 'live';
  app.state.tradeConfig.live.mode = 'readonly';
  assert.match(app.positionRulesEditor(position), /当前仅可查看/);
  assert.match(app.positionRulesEditor(position), /fieldset class="rules" disabled/);
});

test('remembered thresholds survive a new page session, without restoring enabled state', () => {
  const h = harness();
  h.position.rules = { min_funding_apr: '0.07625', auto_margin_pct: '17', auto_margin_max_usdt: '321.45' };
  h.app.rememberPositionRuleValues(h.position.id, h.app.positionRuleForm(h.position));
  const fresh = harness(h.storage);
  const form = fresh.app.positionRuleForm(fresh.position);
  assert.deepEqual(plain(form.minApr), { on: false, value: '7.625' });
  assert.deepEqual(plain(form.autoMargin), { on: false, value: '17', max: '321.45' });
  const saved = JSON.parse(h.storage.get(h.app.rulePreferenceKey(h.position.id)));
  assert.equal(saved.minApr.on, undefined);
  fresh.position.rules.min_funding_apr = '0.035';
  assert.equal(fresh.app.positionRuleForm(fresh.position).minApr.value, '3.5');
  fresh.app.state.mode = 'live';
  assert.equal(fresh.app.positionRuleForm(fresh.position).autoMargin.max, '100');
  assert.equal(h.app.positionRuleForm({ ...h.position, id: 'other', rules: {} }).minApr.value, '5');
});

test('corrupt or unavailable browser storage does not prevent rule editing', () => {
  const h = harness();
  h.storage.set(h.app.rulePreferenceKey(h.position.id), '{broken');
  assert.equal(h.app.positionRuleForm(h.position).protect.value, '10');
  h.context.localStorage.getItem = () => { throw new Error('storage blocked'); };
  h.context.localStorage.setItem = () => { throw new Error('storage blocked'); };
  assert.doesNotThrow(() => h.app.rememberPositionRuleValues(h.position.id, h.app.positionRuleForm(h.position)));
  assert.equal(h.app.positionRuleForm(h.position).mismatch.value, '1');
});

test('changing a control creates a draft, enables its value, survives refresh and can be cancelled', () => {
  const h = harness();
  const ui = wire(h);
  ui.input('protect', 'on', true);
  ui.input('protect', 'value', '7.5');
  const draft = h.app.state.pos.ruleDrafts.get(h.app.ruleDraftKey(h.position.id));
  assert.deepEqual(plain(draft.form.protect), { on: true, value: '7.5' });
  assert.equal(ui.field('protect', 'value').disabled, false);
  assert.equal(ui.controls['[data-save-rules]'].disabled, false);
  assert.match(h.app.positionRulesEditor(h.position), /value="7.5"/);
  assert.match(h.app.positionRulesEditor(h.position), /有未保存修改/);
  assert.equal(h.position.rules.liq_protection_pct, undefined);
  ui.controls['[data-cancel-rules]'].emit('click');
  assert.equal(h.app.state.pos.ruleDrafts.size, 0);
  assert.match(h.app.positionRulesEditor(h.position), /当前已保存设置/);
});

test('successful save sends the complete rule set and retains a disabled custom threshold', async () => {
  const h = harness();
  h.position.rules = { liq_protection_pct: '7.5', size_mismatch_pct: '2' };
  const ui = wire(h);
  ui.input('protect', 'on', false);
  let submitted;
  h.app.mockApi(async (path, options) => {
    assert.equal(path, '/api/trade/rules');
    assert.equal(options.auth, true);
    submitted = plain(options.body);
    return { ok: true, body: { changed: true, position: { ...h.position, rules: { size_mismatch_pct: '2' } } } };
  });
  await h.app.savePositionRules(h.position.id);
  assert.deepEqual(submitted, {
    mode: 'paper', position_id: h.position.id, force: false,
    min_funding_apr: 'off', basis_exit: 'off', liq_protection: 'off',
    size_mismatch: '2', take_profit: 'off', take_profit_book_min: 'off', auto_margin: 'off', auto_margin_max: 'off',
    stop_loss: 'off', timeout_hours: 'off', timeout_max_loss: 'off',
  });
  assert.equal(h.app.state.pos.ruleDrafts.size, 0);
  assert.deepEqual(plain(h.app.positionRuleForm(h.position).protect), { on: false, value: '7.5' });
  const reopened = wire(h);
  reopened.input('protect', 'on', true);
  const draft = h.app.state.pos.ruleDrafts.get(h.app.ruleDraftKey(h.position.id));
  assert.equal(h.app.ruleFields(draft.form).liq_protection, '7.5');
});

test('immediate triggers require explicit confirmation; any edit invalidates confirmation', async () => {
  const h = harness();
  const ui = wire(h);
  ui.input('protect', 'on', true);
  let calls = 0;
  h.app.mockApi(async () => {
    calls++;
    return { ok: false, status: 409, body: { error: '需要确认', would_trigger: '会减仓' } };
  });
  await h.app.savePositionRules(h.position.id);
  const draft = h.app.state.pos.ruleDrafts.get(h.app.ruleDraftKey(h.position.id));
  assert.equal(draft.wouldTrigger, '会减仓');
  assert.match(h.app.positionRulesEditor(h.position), /data-rule-force/);
  await h.app.savePositionRules(h.position.id);
  assert.equal(calls, 1);
  assert.equal(h.storage.has(h.app.rulePreferenceKey(h.position.id)), false);
  draft.force = true;
  ui.input('protect', 'value', '8');
  assert.equal(draft.force, false);
  assert.equal(draft.wouldTrigger, null);
  await h.app.savePositionRules(h.position.id);
  draft.force = true;
  h.app.mockApi(async (_path, options) => {
    assert.equal(options.body.force, true);
    return { ok: true, body: { changed: true, forced: true } };
  });
  await h.app.savePositionRules(h.position.id);
  assert.equal(h.app.state.pos.ruleDrafts.size, 0);
});

test('failed/unknown saves keep drafts and do not replace remembered values', async () => {
  const h = harness();
  const ui = wire(h);
  ui.input('mismatch', 'on', true);
  ui.input('mismatch', 'value', '3');
  h.app.mockApi(async () => ({ ok: false, status: 0, body: { error: '网络错误' } }));
  await h.app.savePositionRules(h.position.id);
  const draft = h.app.state.pos.ruleDrafts.get(h.app.ruleDraftKey(h.position.id));
  assert.match(draft.error, /保存结果可能未知/);
  assert.equal(h.storage.has(h.app.rulePreferenceKey(h.position.id)), false);
  assert.equal(draft.form.mismatch.value, '3');
});

test('busy and unauthorized input events cannot create or change a draft', () => {
  const h = harness();
  const ui = wire(h);
  h.app.state.pos.busy = true;
  ui.input('protect', 'on', true);
  assert.equal(h.app.state.pos.ruleDrafts.size, 0);
  h.app.state.pos.busy = false;
  h.app.state.tokenMemory = '';
  ui.input('protect', 'on', true);
  assert.equal(h.app.state.pos.ruleDrafts.size, 0);
});

test('margin selection persists and is included in preview/open bodies, with safe legacy defaults', () => {
  const h = harness();
  assert.equal(h.app.state.strategy.form.marginMode, 'isolated');
  h.app.state.strategy.form.marginMode = 'cross';
  h.app.state.strategy.selected = { symbol: 'BTC/USDT', long: 'binance', short: 'okx' };
  assert.equal(h.app.tradeBody().margin_mode, 'cross');
  h.app.saveStrategyParams();
  const next = harness(h.storage);
  assert.equal(next.app.state.strategy.form.marginMode, 'cross');
  const invalid = harness(new Map([['arb-web-strategy-params', JSON.stringify({marginMode: 'portfolio'})]]));
  assert.equal(invalid.app.state.strategy.form.marginMode, 'isolated');
});

test('live cross selection fails closed without capabilities or with an isolated-only leg', () => {
  const h = harness();
  h.app.state.mode = 'live';
  const form = { marginMode: 'cross' };
  assert.match(h.app.marginModeError(['binance', 'okx'], form), /尚未取得/);
  h.app.state.tradeConfig.margin_modes = {binance: ['isolated', 'cross'], okx: ['isolated', 'cross'], 'hyperliquid-io': ['isolated']};
  assert.equal(h.app.marginModeError(['binance', 'okx'], form), null);
  assert.match(h.app.marginModeError(['binance', 'hyperliquid-io'], form), /不支持/);
  assert.equal(h.app.marginModeError(['unknown'], {marginMode: 'isolated'}), null);
  // 币安统一账户只有全仓：选逐仓要拦，选全仓照常。
  h.app.state.tradeConfig.margin_modes.binance = ['cross'];
  assert.match(h.app.marginModeError(['binance', 'okx'], {marginMode: 'isolated'}), /只支持全仓/);
  assert.equal(h.app.marginModeError(['binance', 'okx'], form), null);
  assert.equal(h.app.marginModeError(['okx', 'hyperliquid-io'], {marginMode: 'isolated'}), null);
});

test('cross position rules keep thresholds but reject isolated auto-topups until explicitly disabled', () => {
  const h = harness();
  h.position.margin_mode = 'cross';
  h.position.rules = { liq_protection_pct: '12' };
  const form = h.app.positionRuleForm(h.position);
  assert.equal(form.marginMode, 'cross');
  assert.equal(form.protect.value, '12');
  form.autoMargin.on = true;
  assert.match(h.app.ruleFormError(form), /全仓不能/);
  form.autoMargin.on = false;
  assert.equal(h.app.ruleFormError(form), null);
  const html = h.app.positionRulesEditor(h.position);
  assert.match(html, /全仓/);
  assert.match(html, /整笔退出/);
  assert.match(html, /不支持自动追加逐仓保证金/);
});

test('take-profit progress shows the order-book exit estimate beside the mark-price figure', () => {
  const { app, position } = harness();
  position.rules = { take_profit_usdt: '2' };
  const obs = { net_with_funding_usdt: '2.3', funding_usdt: '1.1' };
  const report = (observation) => ({ position_id: position.id, evaluation: { observation } });
  const quoted = app.pnlSummary(position, obs, report({ ...obs, exit: { net_usdt: '-5.9', long_price: '287.23', short_price: '287.4' } }));
  assert.match(quoted, /止盈进度（标记价）/);
  assert.match(quoted, /按盘口现在平仓可得/);
  assert.match(quoted, /class="neg"[^>]*>−\$4\.80</);
  assert.match(app.bookExitText(report({ ...obs, exit_unavailable: 'arcus 卖盘不够' })), /盘口用不了/);
  assert.match(app.bookExitText(report({ ...obs, exit_unavailable: 'arcus 卖盘不够' })), /arcus 卖盘不够/);
  // 原因要写在可见文字里（触屏没有悬停提示），过长的截断，完整原因仍在 title。
  assert.match(app.bookExitText(report({ ...obs, exit_unavailable: 'arcus 卖盘不够' })), />盘口用不了（arcus 卖盘不够）</);
  const long = 'x'.repeat(200);
  const clipped = app.bookExitText(report({ ...obs, exit_unavailable: long }));
  assert.match(clipped, new RegExp(`>盘口用不了（x{80}…）<`));
  assert.match(clipped, new RegExp(`title="x{200}"`));
  assert.match(app.bookExitText(null), /待核对/);
  assert.match(app.bookExitText(report({ exit: { net_usdt: '1' } })), /未知/);
  assert.doesNotMatch(app.pnlSummary({ ...position, rules: {} }, obs, null), /按盘口现在平仓可得/);
});

test('RH spread rows show normal basis, direction, both net figures and honest warm-up state', () => {
  const { app } = harness();
  const view = { min_minutes: 120, window_days: 7 };
  const leg = { entry_pct: '0.149', exit_cross_pct: '0.002', net_to_zero_pct: '0.102', net_to_normal_pct: '0.061' };
  const line = {
    base: 'SPY', category: 'INDICES', session: 'off', basis_pct: '-0.15254',
    normal: { median: -0.05, p10: -0.08, p90: -0.02, mad: 0.01, minutes: 900 }, normal_missing_minutes: 0, z: -10.3,
    pair: 'arcus:lighter-rh', a: 'arcus', b: 'lighter-rh', fee_round_trip_pct: '0.045',
    long_a: leg, long_b: null, best: { direction: 'long_a', signal: true, signal_sec: 12, net_usdt: '1.22' }, note: null,
  };
  const row = app.rhRow(line, view);
  for (const part of ['SPY', '盘后', '多 Arcus / 空 RH', 'Arcus ↔ RH', '往返费 0.045%', '-0.050%', '+0.061%', '+0.102%', '+$1.22', '信号 12s', 'rh-signal']) {
    assert.ok(row.includes(part), `${part} missing in ${row}`);
  }
  const warming = app.rhStatus({ ...line, normal: null, normal_missing_minutes: 37, best: { ...line.best, signal: false } }, view);
  assert.match(warming, /还差 37 分钟/);
  assert.match(app.rhStatus({ ...line, note: 'Arcus 盘口 20 秒没更新，不计算' }, view), /没更新/);
  const noNormal = app.rhRow({ ...line, normal: null, z: null, long_a: { ...leg, net_to_normal_pct: null }, best: { ...line.best, signal: false, net_usdt: null } }, view);
  assert.doesNotMatch(noNormal, /rh-signal/);
});

test('Binance rows are observe-only: no signal highlight, a maker estimate and a simulated-fill summary', () => {
  const { app } = harness();
  const view = { min_minutes: 120, window_days: 7 };
  const taker = { entry_pct: '0.2', exit_cross_pct: '0.01', net_to_zero_pct: '0.06', net_to_normal_pct: '0.08' };
  const makerLeg = (price, net) => ({
    price, entry_pct: '0.02', exit_cross_taker_pct: '0.015', exit_cross_maker_pct: '0.005',
    net_to_normal_taker_exit_pct: net, net_to_normal_maker_exit_pct: net == null ? null : '0.075',
  });
  const line = {
    base: 'NVDA', category: 'EQUITIES', session: 'rth', basis_pct: '0.02',
    normal: { median: -0.06, p10: -0.08, p90: -0.02, mad: 0.01, minutes: 900 }, normal_missing_minutes: 0, z: 2,
    pair: 'lighter-rh:binance', a: 'lighter-rh', b: 'binance', fee_round_trip_pct: '0.08',
    long_a: null, long_b: taker, best: { direction: 'long_b', signal: true, signal_sec: 30, net_usdt: '1.6' }, note: null,
    observe_only: true,
    maker: { venue: 'binance', fee_taker_exit_pct: '0.04', fee_maker_exit_pct: '0', long_a: makerLeg('99.99', '-0.145'), long_b: makerLeg('99.98', '0.025') },
  };
  const row = app.rhRow(line, view);
  assert.doesNotMatch(row, /rh-signal/, '只观察的行不高亮成信号');
  for (const part of ['仅观察', 'RH ↔ BN', '挂单测算：多 BN @99.98', '+0.025%（吃单平，费 0.04%）', '+0.075%（挂单平，费 0%）']) {
    assert.ok(row.includes(part), `${part} missing in ${row}`);
  }
  // 没有正常基差时：说清楚在等样本。
  const warming = app.rhMakerText({ ...line, maker: { ...line.maker, long_a: null, long_b: makerLeg('99.98', null) } });
  assert.match(warming, /等正常基差样本/);
  assert.equal(app.rhMakerText({ ...line, maker: null }), '');
  // 汇总：按组过滤，中位数与 1 秒为正比例都显示。
  const summary = {
    quoting: 3, fills: 2, since: '2026-10-12T14:00:00Z',
    stats: [
      { pair: 'lighter-rh:binance', base: 'NVDA', direction: 'long_b', fills: 2, last_at: '2026-10-12T15:00:00Z',
        median_quoted_net_pct: '0.065', median_net_pct: [[0, '0.065'], [300, '0.055'], [1000, '-0.01'], [5000, null]], positive_share_1s: '50.0' },
    ],
  };
  const html = app.rhMakerSummaryHtml(summary, 'all');
  for (const part of ['挂单测算（只观察）', '虚拟挂着 3 个', '2 笔虚拟成交', 'RH 空', 'BN 多', '+0.065%', '+0.055%', '-0.010%', '50%']) {
    assert.ok(html.includes(part), `${part} missing in ${html}`);
  }
  assert.match(app.rhMakerSummaryHtml(summary, 'arcus:binance'), /0 笔虚拟成交/);
  assert.equal(app.rhMakerSummaryHtml(null, 'all'), '');
});

test('clicking an RH spread row opens the spread order form with the depth-chosen direction and a back-to-normal target', () => {
  const { app } = harness();
  const leg = { entry_pct: '0.17324', exit_cross_pct: '0.018', net_to_zero_pct: '0.11', net_to_normal_pct: '0.04' };
  const line = {
    base: 'NVDA', session: 'rth', basis_pct: '-0.196',
    pair: 'arcus:lighter-rh', a: 'arcus', b: 'lighter-rh',
    normal: { median: -0.111645, minutes: 1297 }, long_a: leg, long_b: null,
    best: { direction: 'long_a', signal: false },
  };
  // 多 Arcus / 空 RH：持仓基差 = 空 − 多 = RH − Arcus = −(RH 页基差)，所以目标是 −中位数。
  const order = plain(app.rhOrderTarget(line));
  assert.deepEqual(
    [order.symbol, order.long, order.short, order.target, order.negativeEntry],
    ['NVDA/USDT', 'arcus', 'lighter-rh', '0.112', false],
  );
  const reverse = app.rhOrderTarget({ ...line, long_a: null, long_b: leg, best: { direction: 'long_b' } });
  assert.deepEqual([reverse.long, reverse.short, reverse.target], ['lighter-rh', 'arcus', '-0.112']);
  // 没有正常样本：不带目标；目标超出规则允许的 ±5% 也不带（不截断）。
  assert.equal(app.rhOrderTarget({ ...line, normal: null }).target, null);
  assert.equal(app.rhOrderTarget({ ...line, normal: { median: 7 } }).target, null);
  // 可成交价差不为正：提前说明预览会被拒绝。
  const negative = app.rhOrderTarget({ ...line, long_a: { ...leg, entry_pct: '-0.05' } });
  assert.ok(negative.negativeEntry);
  assert.match(app.rhOriginNote(negative), /预览会被拒绝/);

  let shown = null;
  app.mockShowPage((page) => { shown = page; });
  app.state.strategy.view = 'funding';
  app.state.strategy.form.basisExit = { on: false, value: '0.1' };
  app.openRhInStrategy(line);
  const s = app.state.strategy;
  assert.equal(shown, 'strategy');
  assert.deepEqual([s.view, s.a, s.b, s.userPicked], ['spread', 'arcus', 'lighter-rh', true]);
  assert.deepEqual([s.selected.symbol, s.selected.long, s.selected.short], ['NVDA/USDT', 'arcus', 'lighter-rh']);
  assert.deepEqual(plain(s.form.basisExit), { on: true, value: '0.112' });
  assert.equal(app.ruleFields(s.form, 'spread').basis_exit, '0.112');
  assert.match(app.rhOriginNote(s.selected.fromRh), /0\.112%/);
  // 打开下单界面不会发任何请求、不会下单：只切页面和表单。
  assert.equal(app.state.trade.preview, null);
});

test('RH auto-trading panel round-trips settings, needs a token, and makes live opt-in explicit', async () => {
  const { app, context } = harness();
  const settings = {
    enabled: false, mode: 'paper', size_usdt: '500', leverage: '3', min_net_pct: '0.05', hold_sec: 10,
    back_to_normal: true, take_profit_usdt: null, liq_protection_pct: null, max_positions: 1, daily_max_opens: 3, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.equal(form.take_profit_usdt, '');
  const body = plain(app.rhAutoBody({ ...form, take_profit_usdt: ' 1.5 ', symbols: 'nvda， spy' }, true));
  assert.deepEqual(
    [body.enabled, body.take_profit_usdt, body.liq_protection_pct, body.symbols, body.hold_sec],
    [true, '1.5', null, ['NVDA', 'SPY'], 10],
  );
  assert.equal(body.hold_ms, null, '整秒 ≥ 5：只写 hold_sec');
  // 0.5 秒：写 hold_ms，hold_sec 留合法整秒给旧版本；读回时显示 0.5。
  const half = plain(app.rhAutoBody({ ...form, hold_sec: '0.5' }, true));
  assert.deepEqual([half.hold_sec, half.hold_ms], [5, 500]);
  assert.equal(app.rhAutoForm({ ...settings, hold_sec: 5, hold_ms: 500 }).hold_sec, '0.5');
  const now = plain(app.rhAutoBody({ ...form, hold_sec: '0' }, true));
  assert.deepEqual([now.hold_sec, now.hold_ms], [5, 0]);
  assert.equal(app.rhAutoForm({ ...settings, hold_sec: 5, hold_ms: 0 }).hold_sec, '0');
  // 空值、非数字、负数、超上限：不转成 0（最激进的设置），而是 null，保存时报错。
  for (const bad of ['', '  ', 'abc', '-1', '601', '9999', 'NaN', 'Infinity']) {
    const invalid = plain(app.rhAutoBody({ ...form, hold_sec: bad }, true));
    assert.deepEqual([invalid.hold_sec, invalid.hold_ms], [null, null], JSON.stringify(bad));
  }
  const odd = plain(app.rhAutoBody({ ...form, hold_sec: '7.5' }, true));
  assert.deepEqual([odd.hold_sec, odd.hold_ms], [8, 7500]);
  // 开仓间隔：旧设置没有这一项时按 60 显示、保存时写回 60（不能因为面板保存一次就丢掉）；填了照填的。
  assert.equal(form.open_gap_sec, '60');
  assert.equal(body.open_gap_sec, 60);
  assert.equal(app.rhAutoForm({ ...settings, open_gap_sec: 15 }).open_gap_sec, '15');
  assert.equal(plain(app.rhAutoBody({ ...form, open_gap_sec: '15' }, true)).open_gap_sec, 15);

  const cfg = { auth_configured: true };
  const a = { data: { settings, status: '监控中', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 0, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, cfg);
  for (const part of ['已关闭', '开启纸面自动交易', '纸面规则没有在看板后台运行', '监控中', '累计开仓', '今日开仓', '还没有自动开仓记录', '0 USDT']) assert.ok(html.includes(part), part);
  a.data.stats = {
    opened: 9, unwound: 2, rejected: 4, failures: 1, recorded: 3,
    opened_today: 2, open_now: 1,
    opened_usdt: '4500', unwound_usdt: '1000', opened_today_usdt: '1300', open_now_usdt: '500',
    symbols: [{ symbol: 'NVDA/USDT', opened: 2, open: 1, usdt: '1300', open_usdt: '500' }],
    days: [{ day: '10-18', opened: 2, usdt: '1300' }],
  };
  const withStats = app.rhAutoHtml(a, cfg);
  assert.ok(withStats.includes('>9<'), withStats);
  assert.match(withStats, />4,500 USDT</);
  assert.match(withStats, />1,300 USDT</);
  assert.match(withStats, />500 USDT</);
  assert.match(withStats, />1,000 USDT</);
  assert.match(withStats, /NVDA\/USDT 2 笔 · 1,300 USDT（持有 1 · 500 USDT）/);
  assert.match(withStats, /近 7 日开仓（UTC）：10-18 2 笔 · 1,300 USDT/);
  assert.match(withStats, /连续失败/);
  assert.match(withStats, />1\/2</);
  delete a.data.stats;
  const noExit = app.rhAutoHtml({ ...a, form: { ...form, back_to_normal: false } }, cfg);
  assert.match(noExit, /至少开启一条退出规则/);
  const bigger = app.rhAutoHtml({ ...a, form: { ...form, size_usdt: '5000' } }, cfg);
  assert.match(bigger, /实际价差会更差/);
  assert.match(app.rhAutoHtml(a, { auth_configured: false }), /ARB_WEB_TOKEN/);

  // 开启实盘：不输入 LIVE 就不发请求；输入了才带上 confirm。
  const sent = [];
  app.mockApi(async (url, opts = {}) => {
    if (opts.method === 'POST') sent.push(plain(opts.body));
    return { ok: true, status: 200, body: { settings } };
  });
  context.document.getElementById = () => null;
  app.state.rhAuto = { data: a.data, form: { ...form, mode: 'live' }, dirty: true, busy: false, message: null };
  app.mockPrompt(() => 'live');
  await app.saveRhAuto(true);
  assert.equal(sent.length, 0, '没输入 LIVE 不发请求');
  app.mockPrompt(() => 'LIVE');
  await app.saveRhAuto(true);
  assert.equal(sent.length, 1);
  assert.deepEqual([sent[0].enabled, sent[0].mode, sent[0].confirm], [true, 'live', 'LIVE']);
  // 纸面开启不需要确认。
  app.state.rhAuto.form = { ...form, mode: 'paper' };
  app.mockPrompt(() => { throw new Error('纸面不该弹确认'); });
  await app.saveRhAuto(true);
  assert.equal(sent[1].confirm, undefined);
});

test('other venue pairs open the order form with their own venues and sign-correct targets', () => {
  const { app } = harness();
  let shown = null;
  app.mockShowPage((page) => { shown = page; });
  const leg = { entry_pct: '0.12', exit_cross_pct: '0.01', net_to_zero_pct: '0.09', net_to_normal_pct: '0.07' };
  // HL-xyz ↔ RH，多 RH / 空 HL-xyz（long_b）：持仓基差 = 空 − 多 = a − b = 页面基差 → 目标 = 中位数本身。
  const line = {
    pair: 'hyperliquid-xyz:lighter-rh', a: 'hyperliquid-xyz', b: 'lighter-rh', base: 'TSLA', session: 'rth',
    normal: { median: 0.031 }, long_a: null, long_b: leg, best: { direction: 'long_b', signal: true },
  };
  const order = plain(app.rhOrderTarget(line));
  assert.deepEqual([order.long, order.short, order.target], ['lighter-rh', 'hyperliquid-xyz', '0.031']);
  assert.match(app.rhOriginNote(order), /HL-xyz ↔ RH/);
  app.openRhInStrategy(line);
  const s = app.state.strategy;
  assert.equal(shown, 'strategy');
  assert.deepEqual([s.a, s.b, s.selected.long, s.selected.short, s.selected.symbol], ['hyperliquid-xyz', 'lighter-rh', 'lighter-rh', 'hyperliquid-xyz', 'TSLA/USDT']);
  assert.match(app.rhRow(line, { min_minutes: 120 }), /多 RH \/ 空 HL-xyz/);
});

test('position cards fold one by one or all at once, remember it, and never hide alerts or a pending close', () => {
  const storage = new Map();
  const { app } = harness(storage);
  app.state.positions = { open: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] };
  assert.equal(app.isCollapsed('a'), false);
  app.toggleCollapsed('a');
  assert.equal(app.isCollapsed('a'), true);
  assert.deepEqual(JSON.parse(storage.get('arb.positions.collapsed')), ['a']);
  // 刷新页面（新的 state）后从浏览器记住的读回来。
  const reloaded = harness(storage).app;
  assert.equal(reloaded.isCollapsed('a'), true);
  assert.equal(reloaded.isCollapsed('b'), false);
  // 正在平仓确认的那笔强制展开，否则看不到确认框。
  reloaded.state.pos.confirming = 'a';
  assert.equal(reloaded.isCollapsed('a'), false);
  reloaded.state.pos.confirming = null;
  app.setAllCollapsed(true);
  assert.deepEqual(['a', 'b', 'c'].map(app.isCollapsed), [true, true, true]);
  app.setAllCollapsed(false);
  assert.deepEqual(['a', 'b', 'c'].map(app.isCollapsed), [false, false, false]);

  // 提醒数：对账发现、未评估、备注、规则触发、非持仓中状态都算。
  const quiet = { status: 'open', evaluation: { action: { action: 'hold' }, skipped: [] } };
  assert.equal(app.positionAlertCount(quiet), 0);
  const noisy = {
    status: 'closing', note: '平仓未完成', quotes_missing: true,
    evaluation: { action: { action: 'close' }, skipped: ['止盈达标但盘口核对未通过'] },
  };
  assert.equal(app.positionAlertCount(noisy, [{ message: '只平了一条腿' }]), 6);

  // 折叠摘要：净收益按盘口（含资金费），不是标记价；没核对过就写待核对，不拿标记价顶替。
  const unavailable = { evaluation: { observation: { exit_unavailable: 'arcus 卖盘不够' } } };
  const quoted = { evaluation: { observation: { exit: { net_usdt: '-0.5', long_price: '98.6', short_price: '98.7' }, funding_usdt: '0.02' } } };
  const brief = app.collapsedSummary({ rules: { take_profit_usdt: '0.05' } }, { net_with_funding_usdt: '-8.47' }, quoted);
  assert.match(brief, /净收益 <b class="neg"[^>]*>−\$0\.4800</);
  assert.doesNotMatch(brief, /8\.47/);
  assert.match(app.collapsedSummary({ rules: {} }, { net_with_funding_usdt: '1' }, null), /净收益 <span class="muted"[^>]*>待核对</);
  assert.doesNotMatch(app.collapsedSummary({ rules: {} }, { net_with_funding_usdt: '1' }, null), /\$1\.0000/);
  assert.match(app.collapsedSummary({ rules: {} }, { net_with_funding_usdt: '1' }, unavailable), /盘口用不了（arcus 卖盘不够）/);
});

test('whole-trade net profit is the order-book figure everywhere, never the mark-price one', () => {
  const { app, position } = harness();
  position.rules = {};
  const obs = { net_with_funding_usdt: '-8.47', funding_usdt: '0.02' };
  const report = { position_id: position.id, evaluation: { observation: { ...obs, exit: { net_usdt: '-0.5', long_price: '98.6', short_price: '98.7' } } } };
  assert.equal(app.bookNet(report), -0.48);
  assert.equal(app.bookNet(null), null);
  // 资金费未知：盘口净额不完整，不当成 0 给出。
  assert.equal(app.bookNet({ evaluation: { observation: { exit: { net_usdt: '-0.5' } } } }), null);
  const html = app.pnlSummary(position, obs, report);
  assert.match(html, /整笔净收益（按盘口，含资金费） <b class="neg"[^>]*>−\$0\.4800</);
  assert.doesNotMatch(html, /整笔净收益（含资金费）/);
  assert.doesNotMatch(html, /8\.47/);
});

test('folding still works when the browser refuses storage', () => {
  class Refusing extends Map {
    get() { throw new Error('denied'); }
    set() { throw new Error('denied'); }
  }
  const { app } = harness(new Refusing());
  app.toggleCollapsed('a');
  assert.equal(app.isCollapsed('a'), true, '本页仍然折叠，只是记不住');
});

test('a closed trade expands into both legs: fills, fees and price P&L that add up to the realized column', () => {
  const { app } = harness();
  const position = {
    id: 'live-1', status: 'closed', realized_source: 'executor',
    realized_pnl_usdt: '0.544262118', realized_fee_usdt: '0.342307243', realized_funding_usdt: '0',
    breakdown: {
      long: {
        venue: 'lighter-rh', side: 'buy', entry_quantity: '5.4432', entry_price: '147.13',
        entry_notional_usdt: '800.858016', entry_fee_usdt: '0',
        exits: [{ role: { kind: 'exit', n: 1 }, quantity: '5.4432', average_price: '147.3', notional_usdt: '801.78', fee_usdt: '0', price_pnl_usdt: '0.925344' }],
        exited_quantity: '5.4432', exit_price: '147.3', price_pnl_usdt: '0.925344', fee_usdt: '0',
      },
      short: {
        venue: 'arcus', side: 'sell', entry_quantity: '5.4432877', entry_price: '147.29',
        entry_notional_usdt: '801.741845333', entry_fee_usdt: '0.170771013',
        exits: [
          { role: { kind: 'trim', n: 1 }, quantity: '1', average_price: '147.3', notional_usdt: '147.3', fee_usdt: '0.03', price_pnl_usdt: '-0.01' },
          { role: { kind: 'exit', n: 1 }, quantity: '4.4432877', average_price: '147.37', notional_usdt: '654.8', fee_usdt: '0.141536230', price_pnl_usdt: '-0.371081882' },
        ],
        exited_quantity: '5.4432877', exit_price: '147.36', price_pnl_usdt: '-0.381081882', fee_usdt: '0.342307243',
      },
      price_pnl_usdt: '0.544262118', fee_usdt: '0.342307243', pnl_gap_usdt: null, fee_gap_usdt: null, external: false,
    },
  };
  const html = app.closedDetailHtml(position);
  assert.match(html, /多腿/);
  assert.match(html, /空腿/);
  assert.match(html, /lighter-rh/);
  assert.match(html, /arcus/);
  assert.match(html, /第 1 次减仓/);
  assert.match(html, />平仓</);
  assert.match(html, /5\.4432877/, '数量原样显示');
  assert.match(html, /两腿价格盈亏 <b class="pos">\+\$0\.5443</);
  assert.match(html, /手续费合计 <b>−\$0\.3423</);
  // 已实现净额与「已实现盈亏」那一列同一个函数、同一口径。
  assert.match(html, /已实现净额 <b class="pos"[^>]*>\+\$0\.2020</);
  assert.doesNotMatch(html, /对不上/);

  // 回滚单叫回滚；差额要如实列出；外部平仓只说开仓侧。
  const unwound = { ...position, status: 'unwound', breakdown: { ...position.breakdown, pnl_gap_usdt: '0.01' } };
  const unwoundHtml = app.closedDetailHtml(unwound);
  assert.match(unwoundHtml, />回滚</);
  assert.match(unwoundHtml, /对不上（价格盈亏差 \+\$0\.0100）/);
  const external = { ...position, realized_source: 'venue_fills', breakdown: { ...position.breakdown, external: true } };
  const externalHtml = app.closedDetailHtml(external);
  assert.match(externalHtml, /在交易所里平掉的/);
  assert.doesNotMatch(externalHtml, /两腿价格盈亏/);
  assert.match(app.closedDetailHtml({ ...position, breakdown: null }), /拆不出双边明细/);
});

test('the live account card shows each venue\'s free collateral without inventing zeros or a double-counted total', () => {
  const { app } = harness();
  const html = app.accountBalancesHtml([
    { venue: 'arcus', free_collateral: '1234.5' },
    { venue: 'lighter-rh', free_collateral: null },
    { venue: 'hyperliquid', free_collateral: '900' },
    { venue: 'hyperliquid-io', free_collateral: '900' },
    { venue: 'hyperliquid-xyz', balance_error: 'HTTP 429' },
  ]);
  assert.match(html, /arcus <b>\$1,234\.50<\/b>/);
  assert.match(html, /lighter-rh <span class="muted">交易所没给/);
  assert.match(html, /title="HTTP 429">没查成/);
  assert.match(html, /统一账户共用一笔余额，不要相加/);
  assert.doesNotMatch(html, /合计/);
  assert.doesNotMatch(app.accountBalancesHtml([
    { venue: 'hyperliquid', free_collateral: '900' },
    { venue: 'hyperliquid-io', free_collateral: '100' },
  ]), /不要相加/, '各 dex 余额不同时（标准账户）不提示');
  assert.equal(app.accountBalancesHtml([]), '');
});

test('the timeout exit is a paired rule like auto margin, and the auto panel round-trips margin, timeout and add settings', () => {
  const { app } = harness();
  // 持仓上已设超时：表单读出小时数与最多亏，保存时成对发出。
  const position = { id: 'p-timeout', status: 'open', rules: { timeout_hours: '4', timeout_max_loss_usdt: '0.5' }, long: { venue: 'arcus' }, short: { venue: 'lighter-rh' } };
  const form = app.positionRuleForm(position);
  assert.deepEqual(plain(form.timeout), { on: true, value: '4', max: '0.5' });
  const fields = app.ruleFields(form);
  assert.equal(fields.timeout_hours, '4');
  assert.equal(fields.timeout_max_loss, '0.5');
  // 只填了一半：拒绝，说清楚要填哪两个。
  form.timeout.max = '';
  assert.match(app.ruleFormError(form), /超时退出已启用，请同时填写持仓时长和最多亏/);
  // 关掉：两个都发 off。
  form.timeout.on = false;
  const off = app.ruleFields(form);
  assert.equal(off.timeout_hours, 'off');
  assert.equal(off.timeout_max_loss, 'off');

  const settings = {
    enabled: true, mode: 'live', size_usdt: '800', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: false, take_profit_usdt: '0.05', liq_protection_pct: null,
    auto_margin_pct: '15', auto_margin_max_usdt: '100', timeout_hours: '4', timeout_max_loss_usdt: '0.5',
    add_min_improve_pct: '0.05', add_max_times: 2, add_max_multiple: '3', max_positions: 5, daily_max_opens: 20, symbols: [],
  };
  const body = app.rhAutoBody(app.rhAutoForm(settings), true);
  for (const key of ['auto_margin_pct', 'auto_margin_max_usdt', 'timeout_hours', 'timeout_max_loss_usdt', 'add_min_improve_pct', 'add_max_multiple']) {
    assert.equal(body[key], String(settings[key]), key);
  }
  assert.equal(body.add_max_times, 2);
  // 旧设置没有这些字段：留空 = 关闭（发 null），补仓次数与倍数取默认。
  const old = app.rhAutoBody(app.rhAutoForm({ ...settings, auto_margin_pct: null, auto_margin_max_usdt: null, timeout_hours: null, timeout_max_loss_usdt: null, add_min_improve_pct: null, add_max_times: undefined, add_max_multiple: undefined }), true);
  assert.equal(old.add_min_improve_pct, null);
  assert.equal(old.timeout_hours, null);
  assert.equal(old.auto_margin_pct, null);
  assert.equal(old.add_max_times, 2);
  assert.equal(old.add_max_multiple, '3');

  // 明细：补仓并进开仓均价，补仓退回单独一行。
  const html = app.closedDetailHtml({
    id: 'x', status: 'closed', realized_source: 'executor', realized_pnl_usdt: '0', realized_fee_usdt: '0', realized_funding_usdt: '0',
    breakdown: {
      long: { venue: 'arcus', side: 'buy', entry_quantity: '15', entry_price: '99.6', entry_notional_usdt: '1494', entry_fee_usdt: '0', adds: 1,
        exits: [{ role: { kind: 'undo', n: 1 }, quantity: '5', average_price: '99', notional_usdt: '495', fee_usdt: '0', price_pnl_usdt: '-3' }],
        exited_quantity: '5', exit_price: '99', price_pnl_usdt: '-3', fee_usdt: '0' },
      short: null, price_pnl_usdt: '-3', fee_usdt: '0', pnl_gap_usdt: null, fee_gap_usdt: null, external: false,
    },
  });
  assert.match(html, /含 1 次补仓，均价加权/);
  assert.match(html, /第 1 次补仓退回/);
});

test('auto-trading panel carries the new cost filters, percentage take-profit, stop-loss and the relative-to-normal opt-in', () => {
  const { app } = harness();
  const settings = {
    enabled: true, mode: 'live', size_usdt: '800', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: true, take_profit_usdt: null, take_profit_pct: '0.05', stop_loss_usdt: '4',
    slippage_buffer_pct: '0.05', max_fee_share_pct: '50', relative_to_normal: true,
    liq_protection_pct: null, max_positions: 0, daily_max_opens: 0, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.deepEqual(
    [form.take_profit_pct, form.stop_loss_usdt, form.slippage_buffer_pct, form.max_fee_share_pct, form.relative_to_normal],
    ['0.05', '4', '0.05', '50', true],
  );
  const body = plain(app.rhAutoBody({ ...form, max_fee_share_pct: '', stop_loss_usdt: ' 3 ' }, true));
  assert.deepEqual(
    [body.take_profit_pct, body.stop_loss_usdt, body.slippage_buffer_pct, body.max_fee_share_pct, body.relative_to_normal],
    ['0.05', '3', '0.05', null, true],
  );
  // 旧设置（没有新字段）：缓冲 0、不过滤、关着。
  const legacy = app.rhAutoForm({ ...settings, take_profit_pct: undefined, stop_loss_usdt: undefined, slippage_buffer_pct: undefined, max_fee_share_pct: undefined, relative_to_normal: undefined });
  assert.deepEqual([legacy.take_profit_pct, legacy.stop_loss_usdt, legacy.slippage_buffer_pct, legacy.max_fee_share_pct, legacy.relative_to_normal], ['', '', '0', '', false]);

  const a = { data: { settings, status: '监控中', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 0, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, { auth_configured: true });
  for (const part of ['滑点缓冲', '手续费占毛利上限', '止盈 (% 名义)', '止损 (USDT)', 'data-rh-auto="relative_to_normal" checked', '已开「相对正常基差」']) {
    assert.ok(html.includes(part), part);
  }
  // 相对模式缺止损 / 没开回到正常：明确提示存不了。
  const noStop = app.rhAutoHtml({ ...a, form: { ...form, stop_loss_usdt: '' } }, { auth_configured: true });
  assert.match(noStop, /必须同时勾选「回到正常基差就平仓」并填止损/);
  // 只有按比例止盈也算退出规则。
  const pctOnly = app.rhAutoHtml({ ...a, form: { ...form, back_to_normal: false, relative_to_normal: false, take_profit_usdt: '', take_profit_pct: '0.05' } }, { auth_configured: true });
  assert.doesNotMatch(pctOnly, /至少开启一条退出规则/);
});

test('auto-trading panel keeps the recent-level gate and the funding deduction through load and save', () => {
  const { app } = harness();
  const settings = {
    enabled: true, mode: 'live', size_usdt: '200', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: true, recent_gate_pct: '0.05', deduct_funding: true,
    slippage_buffer_pct: '0.05', max_positions: 20, daily_max_opens: 20, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.equal(form.recent_gate_pct, '0.05');
  assert.equal(form.deduct_funding, true);
  const body = plain(app.rhAutoBody(form, true));
  assert.equal(body.recent_gate_pct, '0.05');
  assert.equal(body.deduct_funding, true);
  // 留空 = 关（发 null）；取消勾选 = 不扣资金费。
  assert.equal(plain(app.rhAutoBody({ ...form, recent_gate_pct: ' ' }, true)).recent_gate_pct, null);
  assert.equal(plain(app.rhAutoBody({ ...form, deduct_funding: false }, true)).deduct_funding, false);
  // 服务端没给（旧版本）：资金费照默认扣，门槛表单留空。
  const legacy = app.rhAutoForm({ ...settings, recent_gate_pct: undefined, deduct_funding: undefined });
  assert.equal(legacy.recent_gate_pct, '');
  assert.equal(legacy.deduct_funding, true);
});

test('auto-trading panel keeps the fast-market limit through load, edit and save (a save must never silently reset it)', () => {
  const { app } = harness();
  const settings = {
    enabled: true, mode: 'live', size_usdt: '800', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: true, take_profit_usdt: '1', take_profit_pct: '0.1', fast_move_pct: '0.15',
    slippage_buffer_pct: '0.05', max_positions: 20, daily_max_opens: 20, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.equal(form.fast_move_pct, '0.15');
  assert.equal(plain(app.rhAutoBody(form, true)).fast_move_pct, '0.15');
  // 留空 = 关（发 null），改数字原样发。
  assert.equal(plain(app.rhAutoBody({ ...form, fast_move_pct: ' ' }, true)).fast_move_pct, null);
  assert.equal(plain(app.rhAutoBody({ ...form, fast_move_pct: ' 0.2 ' }, true)).fast_move_pct, '0.2');
  // 旧设置没有这个字段：表单留空、保存时发 null（关着）。
  const legacy = app.rhAutoForm({ ...settings, fast_move_pct: undefined });
  assert.equal(legacy.fast_move_pct, '');
  assert.equal(plain(app.rhAutoBody(legacy, true)).fast_move_pct, null);
  const a = { data: { settings, status: '监控中', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 0, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, { auth_configured: true });
  assert.ok(html.includes('急行情不开仓'), '表单里要有这一项');
  assert.ok(html.includes('data-rh-auto="fast_move_pct"'), '字段名要对上');
});

test('auto-trading panel keeps the depth-sizing switch through load, edit and save (a save must never silently turn it off)', () => {
  const { app } = harness();
  const settings = {
    enabled: true, mode: 'live', size_usdt: '200', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: true, take_profit_usdt: '0.3', depth_fit_pct: '50', depth_min_usdt: '80',
    slippage_buffer_pct: '0.05', max_positions: 20, daily_max_opens: 20, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.deepEqual([form.depth_fit_pct, form.depth_min_usdt], ['50', '80']);
  const body = plain(app.rhAutoBody(form, true));
  assert.deepEqual([body.depth_fit_pct, body.depth_min_usdt], ['50', '80']);
  // 留空 = 固定单笔（发 null）；最小单笔留空回到 50。
  const off = plain(app.rhAutoBody({ ...form, depth_fit_pct: ' ', depth_min_usdt: '' }, true));
  assert.deepEqual([off.depth_fit_pct, off.depth_min_usdt], [null, '50']);
  // 旧设置没有这两个字段：表单留空 / 50，保存时发 null（关着）。
  const legacy = app.rhAutoForm({ ...settings, depth_fit_pct: undefined, depth_min_usdt: undefined });
  assert.deepEqual([legacy.depth_fit_pct, legacy.depth_min_usdt], ['', '50']);
  assert.equal(plain(app.rhAutoBody(legacy, true)).depth_fit_pct, null);
  const a = { data: { settings, status: '监控中', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 0, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, { auth_configured: true });
  assert.ok(html.includes('按盘口深度定单笔'), '表单里要有这一项');
  assert.ok(html.includes('data-rh-auto="depth_fit_pct"') && html.includes('data-rh-auto="depth_min_usdt"'), '字段名要对上');
});

test('auto-trading panel keeps the forced-hedge limit through load, edit and save (a save must never silently turn it off)', () => {
  const { app } = harness();
  const settings = {
    enabled: true, mode: 'live', size_usdt: '200', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: true, take_profit_usdt: '0.25', force_hedge_pct: '0.3',
    slippage_buffer_pct: '0.05', max_positions: 20, daily_max_opens: 20, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.equal(form.force_hedge_pct, '0.3');
  assert.equal(plain(app.rhAutoBody(form, true)).force_hedge_pct, '0.3');
  assert.equal(plain(app.rhAutoBody({ ...form, force_hedge_pct: ' ' }, true)).force_hedge_pct, null);
  assert.equal(plain(app.rhAutoBody({ ...form, force_hedge_pct: ' 0.2 ' }, true)).force_hedge_pct, '0.2');
  const legacy = app.rhAutoForm({ ...settings, force_hedge_pct: undefined });
  assert.equal(legacy.force_hedge_pct, '');
  assert.equal(plain(app.rhAutoBody(legacy, true)).force_hedge_pct, null);
  const a = { data: { settings, status: '监控中', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 0, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, { auth_configured: true });
  assert.ok(html.includes('强制补腿') && html.includes('data-rh-auto="force_hedge_pct"'), '表单里要有这一项且字段名对得上');
});

test('history spread chart: request params follow the reference site defaults and stats explain the percentile', () => {
  const { app } = harness();
  const d = app.chartDefaults();
  assert.deepEqual([d.symbol, d.a, d.b, d.interval, d.unit], ['OPENAI', 'okx', 'binance', '5m', 'pct']);
  const q = new URL(app.chartQuery({ ...d, symbol: ' openai ', days: '' }), 'http://x').searchParams;
  assert.deepEqual(
    [q.get('symbol'), q.get('exchange1'), q.get('exchange2'), q.get('interval'), q.get('days'), q.get('direction')],
    ['OPENAI', 'okx', 'binance', '5m', '3', 'long1_short2'],
  );
  // 超过这个周期的上限：夹到上限。
  assert.equal(new URL(app.chartQuery({ ...d, interval: '1h', days: '999' }), 'http://x').searchParams.get('days'), '48');
  assert.equal(new URL(app.chartQuery({ ...d, ratio1: '10' }), 'http://x').searchParams.get('ratio1'), '10');

  // 用户的例子：9-25 币安 1597.3 / OK 1612.5（差 15.2），9-30 币安 1604.1 / OK 1653.8（差 49.7）。
  const t1 = Date.UTC(2026, 8, 25) / 1000;
  const t2 = Date.UTC(2026, 8, 30) / 1000;
  const p1 = { time: t1, a: 1597.3, b: 1612.5, diff: 15.2, pct: 15.2 / 1604.9 * 100 };
  const p2 = { time: t2, a: 1604.1, b: 1653.8, diff: 49.7, pct: 49.7 / 1628.95 * 100 };
  const data = { chart: { points: [p1, p2], stats: {
    points: 2, max_pct: p2.pct, max_at: t2, min_pct: p1.pct, min_at: t1, median_pct: (p1.pct + p2.pct) / 2,
    p10_pct: p1.pct, p90_pct: p2.pct, last_pct: p2.pct, last_at: t2, last_percentile: 90,
  } } };
  const abs = app.chartStatsHtml(data, 'diff');
  for (const part of ['+49.7000', '+15.2000', '34.5000', '90%', '适合开']) assert.ok(abs.includes(part), part);
  const pct = app.chartStatsHtml(data, 'pct');
  assert.ok(pct.includes(`+${p2.pct.toFixed(3)}%`));
  assert.match(app.chartStatsHtml({ chart: { points: [], stats: null } }, 'pct'), /没有能对上的 K 线/);
});

test('spread chart opens zuudot links 1:1 and writes the same parameters back', () => {
  const { app } = harness();
  // 用户给的 zuudot 链接：Binance 多、Hyperliquid xyz 空。
  const zuu = '?symbol=GIGADEV&exchange1=Binance&exchange2=Hyperliquid&instId1=GIGADEVUSDT&instId2=xyz%3AGIGADEV&ratio1=1&ratio2=1&direction=long1_short2&assetId=GIGADEV';
  const f = app.chartParamsFromUrl(zuu);
  assert.deepEqual(plain(f), { symbol: 'GIGADEV', a: 'binance', b: 'hyperliquid-xyz', ratio1: '1', ratio2: '1' });
  // short1_long2：这里的图永远是 A 多 B 空，两边对调（比例跟着走）。
  const flipped = app.chartParamsFromUrl('?symbol=OPENAI&exchange1=OKX&exchange2=Hyperliquid&instId2=io%3AOPENAI&ratio1=2&direction=short1_long2&interval=1h');
  assert.deepEqual(plain(flipped), { symbol: 'OPENAI', a: 'hyperliquid-io', b: 'okx', ratio1: '1', ratio2: '2', interval: '1h' });
  // 没有 symbol 就从 instId 猜；认不出的交易所给 null。
  assert.equal(app.chartParamsFromUrl('?exchange1=bybit&exchange2=lighter-rh&instId1=AAPLUSDT').symbol, 'AAPL');
  assert.equal(app.chartParamsFromUrl('?symbol=X&exchange1=Kraken&exchange2=OKX'), null);
  assert.equal(app.chartParamsFromUrl(''), null);
  // 写回去的链接和 zuudot 同款参数，再解析一遍得到同一组。
  const url = app.chartShareUrl({ ...f, interval: '5m' }, ['GIGADEVUSDT', 'xyz:GIGADEV']);
  const q = new URL(url, 'http://x').searchParams;
  assert.equal(new URL(url, 'http://x').pathname, '/trading');
  assert.deepEqual(
    [q.get('symbol'), q.get('exchange1'), q.get('exchange2'), q.get('instId1'), q.get('instId2'), q.get('direction')],
    ['GIGADEV', 'Binance', 'Hyperliquid', 'GIGADEVUSDT', 'xyz:GIGADEV', 'long1_short2'],
  );
  assert.deepEqual(plain(app.chartParamsFromUrl(new URL(url, 'http://x').search)), { ...plain(f), interval: '5m' });
  // 资金费请求跟价差图同一组天数。
  const fq = new URL(app.fundingQuery({ ...app.chartDefaults(), ...f, interval: '1h', days: '' }), 'http://x');
  assert.equal(fq.pathname, '/api/spread-chart/funding');
  assert.deepEqual([fq.searchParams.get('exchange2'), fq.searchParams.get('days')], ['hyperliquid-xyz', '24']);
});

test('spread chart reference lines use the zuudot formula: open − close = 2 × (fee1 + fee2) + min profit', () => {
  const { app } = harness();
  const pts = [-3.4389, -3.4217, -3.4183, -3.5].map((v, i) => ({ time: i * 300, a: 49.2, b: 47.5, pct: v, diff: v * 0.48 }));
  // zuudot 默认：0.05% + 0.055%、最小利润 0.5% → 开比关高 0.71%（它的数据里 lol − lcl 恒为 0.0071）。
  const ref = app.chartRefLines(pts, { unit: 'pct', intervalMinutes: 5, smaMinutes: 10 });
  assert.equal(ref.window, 2);
  assert.ok(Math.abs(ref.cost - 0.0071) < 1e-12);
  assert.ok(Math.abs(ref.close[0].value - pts[0].pct) < 1e-12, '开头只有一点：均线就是它自己');
  assert.ok(Math.abs(ref.close[3].value - (pts[2].pct + pts[3].pct) / 2) < 1e-12, '窗口满了就滚动');
  for (let i = 0; i < pts.length; i += 1) assert.ok(Math.abs(ref.open[i].value - ref.close[i].value - 0.71) < 1e-9);
  // 价格差口径：成本按两边均价换算。
  const diff = app.chartRefLines(pts, { unit: 'diff', intervalMinutes: 5, smaMinutes: 5, fee1: 0, fee2: 0, minProfit: 1 });
  assert.ok(Math.abs(diff.open[0].value - diff.close[0].value - 0.01 * (49.2 + 47.5) / 2) < 1e-9);
});

test('funding pane buckets settlements to the bar, appends the live rate and sums net bars per bar', () => {
  const { app } = harness();
  const h = 3600;
  const fund = {
    long: 'binance', short: 'hyperliquid-xyz',
    legs: [
      { venue: 'binance', settlements: [{ t: 0, rate: 0.0003 }, { t: 4 * h * 1000, rate: 0 }], current: { rate: 0.0001, next_ms: 8 * h * 1000 } },
      { venue: 'hyperliquid-xyz', settlements: [1, 2, 3, 4, 5].map((i) => ({ t: i * h * 1000, rate: -0.0001 })), current: null },
    ],
    net: [{ t: 0, net: -0.0003 }, { t: 1 * h * 1000, net: -0.0001 }, { t: 2 * h * 1000, net: -0.0001 }, { t: 4 * h * 1000, net: -0.0001 }],
  };
  // 4 小时线：HL 每小时的结算落进同一根，线上留最后一次，净结算柱求和。
  const s = app.chartFundingSeries(fund, 4 * h, 5 * h + 120);
  assert.deepEqual(plain(s.legs[0].marks), [0, 4 * h]);
  assert.deepEqual(plain(s.legs[0].line).map((p) => p.time), [0, 4 * h], '现在落在最后一根里：替换成当前预测费率');
  assert.ok(Math.abs(s.legs[0].line[1].value - 0.01) < 1e-12, '× 100 = %');
  assert.deepEqual(plain(s.legs[1].marks), [0, 4 * h]);
  assert.equal(s.net.length, 2);
  assert.ok(Math.abs(s.net[0].value - -0.05) < 1e-12, '0 点那根：−0.03% − 0.01% − 0.01%');
  assert.match(s.net[0].color, /220, 38, 38/, '负的是红柱');
  // 5 分钟线：每次结算各自一根，当前费率接在最后。
  const fine = app.chartFundingSeries(fund, 300, 5 * h + 120);
  assert.deepEqual(plain(fine.legs[0].line).map((p) => p.time), [0, 4 * h, 5 * h]);
  assert.equal(app.chartFundingSeries(null, 300, 0).legs.length, 0);
  assert.equal(app.chartValueAt([{ time: 1 }, { time: 5 }, { time: 9 }], 6).time, 5);
  assert.equal(app.chartValueAt([{ time: 1 }], 0), null);
});

test('funding strip and summary read like the reference site', () => {
  const { app } = harness();
  assert.deepEqual([app.fmtCountdown(3 * 3600e3 + 16 * 60e3), app.fmtCountdown(16 * 60e3 + 5e3), app.fmtCountdown(45e3), app.fmtCountdown(-1)], ['3h16m', '16m', '45s', '结算中']);
  const now = 1_000_000_000;
  const fund = {
    long: 'binance', short: 'hyperliquid-xyz', notes: ['Lighter 的历史费率只精确到 0.0001%（每小时）'],
    legs: [
      { venue: 'binance', interval_h: 4, current: { rate: 0, next_ms: now + (3 * 60 + 16) * 60e3, age_sec: 6 } },
      { venue: 'hyperliquid-xyz', interval_h: 1, current: { rate: -0.000053, next_ms: now + 16 * 60e3, age_sec: 4 } },
    ],
    summary: { count: 48, both: 12, long_only: 0, short_only: 36, total: -0.006412, days: 2, apr: -1.17 },
    next: { t: now + 16 * 60e3, kind: 'short', net: -0.000053 },
  };
  const strip = app.chartFundingStripHtml(fund, now + 2000, now);
  for (const part of ['Binance', '0.0000%', '4h · 结算3h15m', 'HL-xyz', '−0.0053%', '1h · 结算15m', '8 秒前']) assert.ok(strip.includes(part), part);
  const sum = app.chartFundingSummaryHtml(fund);
  for (const part of ['结算 <b>48</b> 次', '同时 12', '仅 HL-xyz 36', '−0.6412%', '年化', '仅 HL-xyz 结算', '−0.0053%', '已结算的费率', '0.0001%']) assert.ok(sum.includes(part), part);
  // 一家拉不到：报错，不算净额。
  const broken = app.chartFundingSummaryHtml({ ...fund, summary: null, next: null, legs: [{ venue: 'okx', error: 'HTTP 429' }, fund.legs[1]] });
  assert.match(broken, /OKX 资金费历史拉不到：HTTP 429/);
  assert.doesNotMatch(broken, /净资金费合计/);
  assert.match(app.chartFundingStripHtml(null, now), /资金费 —/);
  // 一家历史更短：写明只算了两家都有数据的那段。
  assert.match(app.chartFundingSummaryHtml({ ...fund, days: 48 }), /两家都有数据的 2 天内结算/);
  assert.match(app.chartFundingSummaryHtml({ ...fund, days: 2 }), /所选 2 天内结算/);
});

test('spread chart auto-refresh: at least a minute and a bar, never with a Lighter leg', () => {
  const { app } = harness();
  const d = app.chartDefaults();
  assert.equal(app.chartAutoRefreshMs({ ...d, a: 'binance', b: 'hyperliquid-xyz', interval: '5m' }), 300000);
  assert.equal(app.chartAutoRefreshMs({ ...d, a: 'okx', b: 'binance', interval: '1m' }), 60000);
  assert.equal(app.chartAutoRefreshMs({ ...d, a: 'okx', b: 'binance', interval: '4h' }), 4 * 3600000);
  assert.equal(app.chartAutoRefreshMs({ ...d, a: 'lighter-rh', b: 'hyperliquid-io', interval: '1m' }), null);
  assert.equal(app.chartAutoRefreshMs({ ...d, a: 'arcus', b: 'lighter', interval: '1h' }), null);
});

test('spread chart range buttons step like zuudot and stop at the cap; legend toggles show their state', () => {
  const { app } = harness();
  const d = app.chartDefaults();
  assert.deepEqual([d.kline, d.layers.spread, d.layers.levels, d.layers.net], ['candle', true, false, true]);
  assert.deepEqual(plain(app.chartRangeButtons({ ...d, interval: '5m', days: '' })).map((b) => [b.label, b.days, b.disabled]), [['再加载 2 天', 5, false], ['加载至 8 天', 8, false]]);
  assert.ok(app.chartRangeButtons({ ...d, interval: '5m', days: '8' }).every((b) => b.disabled));
  assert.deepEqual(plain(app.chartRangeButtons({ ...d, interval: '4h', days: '' })).map((b) => b.days), [72, 96]);
  const legend = app.chartLegendHtml(1, { ...d.layers, fundB: false }, { a: 'Binance', b: 'HL-xyz' }, [['Binance', '0.0000%', '', '#16a34a']]);
  assert.match(legend, /data-layer="fundA"[^>]*>.*资金费 · Binance/);
  assert.match(legend, /class="zc-leg off" data-layer="fundB"/);
  assert.match(legend, /净资金费结算柱/);
  assert.match(legend, /0\.0000%/);
  // 保存过的图层开关读得回来。
  const storage = new Map([['arb-web-chart-params', JSON.stringify({ layers: { levels: true, net: false }, kline: 'line' })]]);
  const saved = harness(storage).app.chartDefaults();
  assert.deepEqual([saved.layers.levels, saved.layers.net, saved.kline], [true, false, 'line']);
});

test('auto-trading panel exposes the history-percentile gate', () => {
  const { app } = harness();
  const form = app.rhAutoForm({ history_gate_pct: '20', history_interval: '1h', history_days: '24' });
  assert.deepEqual([form.history_gate_pct, form.history_interval, form.history_days], ['20', '1h', '24']);
  const body = plain(app.rhAutoBody({ ...form, history_gate_pct: '' }, true));
  assert.deepEqual([body.history_gate_pct, body.history_interval, body.history_days], [null, '1h', '24']);
  const legacy = app.rhAutoForm({});
  assert.deepEqual([legacy.history_gate_pct, legacy.history_interval, legacy.history_days], ['', '1h', '24']);
  const a = { data: { settings: {}, status: '', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 60, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, { auth_configured: true });
  for (const part of ['历史分位前 (%)', '历史 K 线周期', '已开历史分位门槛']) assert.ok(html.includes(part), part);
});

test('recently closed positions page by ten and the pager only shows when needed', () => {
  const { app } = harness();
  assert.equal(app.CLOSED_PER_PAGE, 10);
  assert.equal(app.closedPagerHtml({ closed_total: 10, closed_page: 1, closed_per_page: 10 }), '', '一页放得下不显示');
  assert.equal(app.closedPagerHtml(null), '');
  const first = app.closedPagerHtml({ closed_total: 28, closed_page: 1, closed_per_page: 10 });
  assert.match(first, /第 1 \/ 3 页 · 第 1–10 笔，共 28 笔/);
  assert.match(first, /data-closed-page="0" disabled>上一页/);
  assert.match(first, /data-closed-page="2">下一页/);
  const last = app.closedPagerHtml({ closed_total: 28, closed_page: 3, closed_per_page: 10 });
  assert.match(last, /第 21–28 笔/);
  assert.match(last, /data-closed-page="4" disabled>下一页/);
  assert.match(app.closedPagerHtml({ closed_total: 28, closed_page: 9, closed_per_page: 10 }), /第 3 \/ 3 页/, '页码夹回范围内');
});

test('auto-trading panel exposes the strong-signal multiple and keeps it on save', () => {
  const { app } = harness();
  const form = app.rhAutoForm({ instant_multiple: '2', hold_sec: 5 });
  assert.equal(form.instant_multiple, '2');
  assert.equal(plain(app.rhAutoBody(form, true)).instant_multiple, '2');
  assert.equal(plain(app.rhAutoBody({ ...form, instant_multiple: ' ' }, true)).instant_multiple, null, '留空 = 关闭');
  assert.equal(app.rhAutoForm({}).instant_multiple, '', '旧设置没有这一项：关闭');
  // 先下费率低的腿：旧设置没有这一项时照开；取消勾选会发 false。
  assert.equal(app.rhAutoForm({}).cheap_leg_first, true);
  assert.equal(plain(app.rhAutoBody(app.rhAutoForm({ cheap_leg_first: false }), true)).cheap_leg_first, false);
  assert.equal(plain(app.rhAutoBody(form, true)).cheap_leg_first, true);
  const a = { data: { settings: {}, status: '', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 60, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  assert.ok(app.rhAutoHtml(a, { auth_configured: true }).includes('强信号不等保持'));
});

test('auto-trading panel round-trips the excluded venues as lowercase short names', () => {
  const { app } = harness();
  // 旧设置没有这个字段：不排除，发回去也是空的（整套替换不能把它弄丢）。
  const legacy = app.rhAutoForm({});
  assert.equal(legacy.exclude_venues, '');
  assert.deepEqual(plain(app.rhAutoBody(legacy, false)).exclude_venues, []);
  const form = app.rhAutoForm({ exclude_venues: ['bitget'] });
  assert.equal(form.exclude_venues, 'bitget');
  assert.deepEqual(plain(app.rhAutoBody(form, true)).exclude_venues, ['bitget']);
  const typed = plain(app.rhAutoBody({ ...form, exclude_venues: 'Bitget， lighter ' }, true));
  assert.deepEqual(typed.exclude_venues, ['bitget', 'lighter']);
});

test('auto-trading panel no longer lists Binance separately: no Binance switches, nothing Binance-only sent', () => {
  const { app } = harness();
  // 旧设置里还开着币安挂单、只做币安：面板不再显示、也不再发这些字段（服务端读到旧值也一律关掉）。
  const form = app.rhAutoForm({ binance_only: true, binance_maker: true, binance_symbols: ['OPENAI'], binance_size_usdt: '200', maker_rest_sec: 8 });
  const body = plain(app.rhAutoBody(form, true));
  for (const key of Object.keys(body)) assert.ok(!key.startsWith('binance_') && !key.startsWith('maker_'), key);
  const a = { data: { settings: {}, status: '', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 60, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, { auth_configured: true });
  assert.ok(!html.includes('币安'), '面板里不再单独列币安');
  assert.ok(html.includes('不自动交易的交易所'), '想排除币安就和别家一样填在这里');
});

test('recently ended orders: a never-traded one is marked, removable in one click, and the tools offer clean-up', () => {
  const { app } = harness();
  const failed = { id: 'live-1', status: 'unwound', traded: false };
  const real = { id: 'live-2', status: 'closed', traded: true };
  // 每行的按钮：没成交 -> 移除（不用确认，data-traded=0）；有成交 -> 移除（要确认，data-traded=1）。
  assert.match(app.closedActionHtml(failed), /data-closed-dismiss="live-1" data-traded="0"/);
  assert.match(app.closedActionHtml(real), /data-closed-dismiss="live-2" data-traded="1"/);
  // 状态旁边的标签：只有没成交的有。
  assert.match(app.closedTagsHtml(failed), /没成交/);
  assert.equal(app.closedTagsHtml(real), '');
  // 没有令牌就没有任何移除入口（后端也要令牌）。
  app.state.tokenMemory = '';
  assert.equal(app.closedActionHtml(failed), '');
  assert.equal(app.closedToolsHtml({ failed_count: 2 }), '');
  app.state.tokenMemory = 'test-token';
  // 工具条：有没成交的才有一键清理，不再有「显示已隐藏」开关。
  const tools = app.closedToolsHtml({ failed_count: 2, dismissed_count: 1 });
  assert.match(tools, /data-closed-clean[^>]*>清理 2 笔没成交的/);
  assert.doesNotMatch(tools, /显示已隐藏|data-closed-show/);
  assert.equal(app.closedToolsHtml({ failed_count: 0, dismissed_count: 1 }), '', '没东西可清理时不占地方');
});

test('removing a recently ended order confirms only for real trades and sends exactly what was confirmed', async () => {
  const { app } = harness();
  const calls = [];
  const asked = [];
  app.mockApi(async (url, options) => {
    calls.push({ url, body: plain(options.body) });
    return { ok: true, status: 200, body: { changed: options.body.ids, skipped: [] } };
  });
  app.mockRefresh(async () => {});
  app.mockWindow({ confirm: (text) => { asked.push(text); return true; } });
  app.state.mode = 'live';
  // 没成交的：不问，直接发，不带 allow_traded。
  await app.dismissClosed(['live-1'], { traded: false });
  assert.equal(asked.length, 0);
  assert.deepEqual(calls[0].body, { mode: 'live', ids: ['live-1'], restore: false, all_failed: false, allow_traded: false });
  assert.equal(calls[0].url, '/api/trade/dismiss');
  // 有成交的：先问（说清楚只是隐藏、统计照旧），同意了才带 allow_traded。
  await app.dismissClosed(['live-2'], { traded: true });
  assert.equal(asked.length, 1);
  assert.match(asked[0], /只是从「最近结束」列表里隐藏/);
  assert.match(asked[0], /仍把它算进去/);
  assert.equal(calls[1].body.allow_traded, true);
  // 不同意：什么都不发。
  app.mockWindow({ confirm: () => false });
  await app.dismissClosed(['live-2'], { traded: true });
  assert.equal(calls.length, 2);
  // 一键清理：问一次，发 all_failed，且永远不带 allow_traded。
  app.state.positions = { failed_count: 3 };
  app.mockWindow({ confirm: (text) => { asked.push(text); return true; } });
  await app.dismissClosed([], { allFailed: true, traded: true });
  assert.match(asked[asked.length - 1], /清理 3 笔没成交/);
  assert.deepEqual(calls[2].body, { mode: 'live', ids: [], restore: false, all_failed: true, allow_traded: false });
});

test('take-profit book threshold: rides with take-profit, sent as its own field, shown in progress', () => {
  const h = harness();
  h.position.rules = { take_profit_usdt: '0.3', take_profit_book_min_usdt: '0' };
  const form = h.app.positionRuleForm(h.position);
  assert.deepEqual(plain(form.tpBook), { on: true, value: '0' });
  assert.equal(h.app.ruleFields(form).take_profit_book_min, '0');
  assert.equal(h.app.ruleFormError(form), null);
  form.takeProfit.on = false;
  assert.match(h.app.ruleFormError(form), /要和净收益止盈一起开/);
  form.takeProfit.on = true;
  form.tpBook.value = '0.5';
  assert.match(h.app.ruleFormError(form), /不能高于止盈金额/);
  form.tpBook.value = '0.1';
  assert.equal(h.app.ruleFormError(form), null);
  const shown = h.app.pnlSummary(h.position, { net_with_funding_usdt: '0.4', funding_usdt: '0.4' }, null);
  assert.match(shown, /盘口为正即平/);
  h.position.rules = { take_profit_usdt: '0.3' };
  assert.match(h.app.pnlSummary(h.position, { net_with_funding_usdt: '0.4', funding_usdt: '0.4' }, null), /盘口也要 ≥ \$0\.30/);
  assert.equal(h.app.positionRuleForm(h.position).tpBook.on, false);
});

test('trade stats: chosen window, unknown pnl not shown as zero, notes for estimates', () => {
  const { app } = harness();
  const summary = (over) => ({
    ended: 0, closed: 0, unwound: 0, settled: 0, wins: 0, losses: 0, net_usdt: '0', price_pnl_usdt: '0', fee_usdt: '0',
    funding_usdt: '0', unwound_net_usdt: '0', best_usdt: null, worst_usdt: null, unknown: 0, funding_missing: 0,
    opened: 0, volume_usdt: '0', volume_estimated: 0, ...over,
  });
  const stats = {
    windows: [
      { key: 'today', label: '今日（UTC）', summary: summary({ ended: 1, closed: 1, unknown: 1 }), by_symbol: [{ symbol: 'NVDA/USDT', ended: 1, wins: 0, losses: 0, net_usdt: '0', fee_usdt: '0', unknown: 1, volume_usdt: '200' }] },
      { key: '7d', label: '近 7 天', summary: summary({ ended: 3, closed: 2, unwound: 1, settled: 3, wins: 2, losses: 1, net_usdt: '0.5', price_pnl_usdt: '1', fee_usdt: '0.6', funding_usdt: '0.1', unwound_net_usdt: '-0.2', best_usdt: '0.5', worst_usdt: '-0.2', opened: 4, volume_usdt: '3200', volume_estimated: 1 }), by_symbol: [],
        by_venue: [
          { venue: 'lighter-rh', positions: 3, orders: 5, volume_usdt: '2400', maker_volume_usdt: '0', estimated_usdt: '200', fee_usdt: '0' },
          { venue: 'binance', positions: 1, orders: 2, volume_usdt: '800', maker_volume_usdt: '400', estimated_usdt: '0', fee_usdt: '0.16' },
          { venue: 'hyperliquid', positions: 0, orders: 0, volume_usdt: '0', maker_volume_usdt: '0', estimated_usdt: '0', fee_usdt: '0' },
        ] },
    ],
    daily: [{ date: '2026-10-15', summary: summary({ ended: 1, unknown: 1, volume_usdt: '200' }) }],
  };
  const week = app.tradeStatsParts(stats, '7d');
  assert.equal(week.key, '7d');
  assert.match(week.seg, /data-stats-window="7d" class="on"/);
  assert.match(week.tiles, /\+\$0\.5000/);
  assert.match(week.tiles, /67%/);
  assert.match(week.tiles, /\$3,200/);
  assert.match(week.foot, /回滚 1 笔合计 −\$0\.2000/);
  assert.match(week.foot, /1 笔在交易所手动平仓/);
  assert.match(week.symbols, /这段时间没有交易/);
  // 按交易所：占比、挂单、估算、折合费率；连着但没成交的显示成灰行。
  assert.match(week.venues, /RH<\/td><td class="num">\$2,400 <span[^>]*>含估算 \$200<\/span><\/td><td class="num">75\.0%/);
  assert.match(week.venues, /BN<\/td><td class="num">\$800<\/td><td class="num">25\.0%<\/td>\s*<td class="num">\$400<\/td>/);
  assert.match(week.venues, /0\.0200%/);
  assert.match(week.venues, /<tr class="dim"><td>HL<\/td>/);
  const today = app.tradeStatsParts(stats, 'today');
  assert.match(today.venues, /这段时间没有成交/);
  // 只有一笔、还没核出：净盈亏显示「—」，不显示 $0。
  assert.match(today.tiles, /已实现净盈亏<\/span><b class="dim">—</);
  assert.match(today.foot, /1 笔盈亏没核出来/);
  assert.match(today.foot, /UTC 日/);
  assert.match(today.symbols, /另 1 笔没核出/);
  assert.match(today.daily, /2026-10-15/);
  assert.equal(app.tradeStatsParts({ windows: [] }, '7d'), null);
  assert.equal(app.tradeStatsParts(stats, 'nope').key, 'today', '没有这个时间段就退回第一个');
});

test('a leg-less opening position says it is waiting for the first leg, and flags a long wait', () => {
  const { app } = harness();
  const opened = Date.parse('2026-10-16T10:41:58Z');
  const opening = { status: 'opening', long: null, short: null, opened_at: '2026-10-16T10:41:58Z' };
  const waiting = app.inFlightNotice(opening, opened + 5_000);
  assert.equal(waiting.tone, 'info');
  assert.match(waiting.text, /正在建仓/);
  assert.match(waiting.text, /已 5 秒/);
  assert.match(waiting.text, /不留仓位/);
  const stuck = app.inFlightNotice(opening, opened + 75_000);
  assert.equal(stuck.tone, 'warn');
  assert.match(stuck.text, /75 秒/);
  assert.match(stuck.text, /可能卡住/);
  // 有腿的、或不是建仓中的，不提示。
  assert.equal(app.inFlightNotice({ ...opening, long: { venue: 'binance' } }, opened), null);
  assert.equal(app.inFlightNotice({ ...opening, status: 'open' }, opened), null);
  assert.equal(app.inFlightNotice({ ...opening, status: 'closing' }, opened), null);
  // 开仓时间读不出来也不报错。
  assert.doesNotMatch(app.inFlightNotice({ ...opening, opened_at: null }, opened).text, /NaN/);
});

test('spread panel: watch pairs are hidden by default, never count as signals and cannot be clicked to order', () => {
  const storage = new Map();
  const { app, context } = harness(storage);
  const nodes = {};
  const node = (id) => {
    if (!nodes[id]) {
      const label = { hidden: false };
      nodes[id] = { id, innerHTML: '', textContent: '', checked: false, value: '', label, closest: () => label, querySelectorAll: () => [], addEventListener() {} };
    }
    return nodes[id];
  };
  context.document.getElementById = node;
  const leg = { entry_pct: '0.5', exit_cross_pct: '0.02', net_to_zero_pct: '0.4', net_to_normal_pct: '0.3' };
  const line = (pair, a, b, base, extra = {}) => ({
    pair, a, b, base, category: 'EQUITIES', session: 'rth', basis_pct: '0.4', fee_round_trip_pct: '0.1',
    normal: { median: 0.1, p10: 0, p90: 0.2, minutes: 500 }, normal_missing_minutes: 0, z: 1,
    long_a: leg, long_b: null, best: { direction: 'long_a', signal: true, signal_sec: 3, net_usdt: '6' },
    note: null, observe_only: false, watch_only: false, ...extra,
  });
  const pairView = (id, a, b, watch) => ({ id, a, b, markets: 3, history_minutes: 60, fee_min_pct: '0.1', fee_max_pct: '0.1', note: null, observe_only: false, watch_only: watch });
  app.state.rh = {
    size_usdt: '2000', alert_net_pct: '0.1', min_minutes: 120, window_days: 7,
    connected: { venues: { arcus: true, gate: true }, reconnects: 0 },
    pairs: [pairView('arcus:lighter-rh', 'arcus', 'lighter-rh', false)],
    lines: [line('arcus:lighter-rh', 'arcus', 'lighter-rh', 'SPY')],
    watch: {
      pairs: [pairView('arcus:gate', 'arcus', 'gate', true)],
      lines: [line('arcus:gate', 'arcus', 'gate', 'OPENAI', { watch_only: true })],
    },
  };
  app.renderRhSpread();
  assert.doesNotMatch(nodes['rh-rows'].innerHTML, /OPENAI/, '默认不显示观察组');
  assert.match(nodes['rh-pair'].innerHTML, /<optgroup label="观察组（没有实盘账户）">.*Arcus ↔ Gate（3） · 观察/);
  assert.match(nodes['rh-stats'].innerHTML, /当前信号<\/span><b class="pos">1</);
  // 勾上「显示观察组」：行出来了，但不算信号、打的是「观察」标签、点了也不下单。
  storage.set('arb-web-rh-watch', '1');
  app.renderRhSpread();
  assert.match(nodes['rh-rows'].innerHTML, /OPENAI/);
  assert.match(nodes['rh-stats'].innerHTML, /当前信号<\/span><b class="pos">1</, '观察组的信号不算');
  assert.match(nodes['rh-notice'].innerHTML, /观察组<\/b> 1 组 · 3 个合约/);
  assert.match(nodes['rh-pair'].innerHTML, /全部组（1 \+ 观察 1）/);
  const watchRow = app.rhRow(app.state.rh.watch.lines[0], app.state.rh);
  assert.match(watchRow, /class=""/, '观察组的行不高亮成信号');
  assert.match(watchRow, /不从这里下单/);
  assert.doesNotMatch(watchRow, /data-rh-chart/, 'Gate 没有历史价差图');
  assert.match(app.rhRow(app.state.rh.lines[0], app.state.rh), /data-rh-chart/);
  assert.match(app.rhStatus(app.state.rh.watch.lines[0], app.state.rh), /观察<\/span>.*够门槛/);
  // 不勾、但选中了某个观察组：照样显示那一组。
  storage.set('arb-web-rh-watch', '0');
  storage.set('arb-web-rh-pair', 'arcus:gate');
  app.renderRhSpread();
  assert.match(nodes['rh-rows'].innerHTML, /OPENAI/);
  assert.doesNotMatch(nodes['rh-rows'].innerHTML, /SPY/);
});

test('RH auto-trading: dynamic size cap round-trips, blank = fixed size', () => {
  const { app } = harness();
  const settings = {
    enabled: true, mode: 'live', size_usdt: '200', size_max_usdt: '600', leverage: '3', min_net_pct: '0.05', hold_sec: 5,
    back_to_normal: false, take_profit_usdt: '0.25', max_positions: 20, daily_max_opens: 100, symbols: [],
  };
  const form = app.rhAutoForm(settings);
  assert.equal(form.size_max_usdt, '600');
  assert.equal(plain(app.rhAutoBody(form, true)).size_max_usdt, '600');
  // 留空 = 固定单笔（发 null）；服务端没给（旧版本）表单留空。
  assert.equal(plain(app.rhAutoBody({ ...form, size_max_usdt: ' ' }, true)).size_max_usdt, null);
  assert.equal(app.rhAutoForm({ ...settings, size_max_usdt: undefined }).size_max_usdt, '');
});

test('top totals keep the known positions and say how many were left out, instead of vanishing', () => {
  const { app } = harness();
  const pos = (symbol, daily) => ({ id: symbol, symbol, evaluation: daily === null ? null : { observation: { pnl: { funding_daily_usdt: daily } } } });
  const report = (id, net) => ({ position_id: id, evaluation: { observation: { exit: { net_usdt: String(net) }, funding_usdt: '0' } } });
  // 一笔缺行情（评估是 null）：其余照加，写明 1 笔没计入，名单在悬停提示里。
  const open = [pos('UNITREE', '0.31'), pos('GIGADEV', null), pos('LIT', '0.10')];
  const totals = app.positionTotals(open, [report('UNITREE', -0.5), report('GIGADEV', -1), report('LIT', 0.2)]);
  assert.match(totals.daily.text, /\+\$0\.4100（1 笔缺数据未计入）/);
  assert.match(totals.daily.title, /GIGADEV/);
  // 净收益按盘口：三笔都核对过，没有缺。
  assert.match(totals.net.text, /−\$1\.30/);
  assert.doesNotMatch(totals.net.text, /未计入/);
  // 盘口还没核对的那笔也要说出来。
  const partial = app.positionTotals(open, [report('UNITREE', -0.5)]);
  assert.match(partial.net.text, /2 笔缺数据未计入/);
  // 全都算不出才显示「—」；没有持仓也是「—」。
  assert.equal(app.positionTotals([pos('A', null)], []).daily.text, '—');
  assert.equal(app.positionTotals([], []).daily.text, '—');
  // 都齐全：不带任何说明。
  const full = app.positionTotals([pos('UNITREE', '0.31')], [report('UNITREE', -0.5)]);
  assert.equal(full.daily.text, '+$0.3100');
  assert.equal(full.daily.title, '');
});

test('live close needs no typed id: one confirm click sends the position id itself', async () => {
  const h = harness();
  const { app } = h;
  app.state.mode = 'live';
  const position = { id: 'live-1792699812006', symbol: 'DRAM/USDT', status: 'open', strategy: 'spread', leverage: '3',
    long: { venue: 'lighter', notional_usdt: '400' }, short: { venue: 'hyperliquid-xyz', notional_usdt: '400' } };
  app.state.positions = { open: [position] };
  const idle = app.positionActions(position);
  assert.match(idle, /data-close="live-1792699812006"/);
  assert.match(idle, /data-add="live-1792699812006"/);
  assert.doesNotMatch(idle, /<input/);

  app.state.pos.confirming = position.id;
  app.state.pos.confirmKind = 'close';
  const confirm = app.positionActions(position);
  assert.doesNotMatch(confirm, /<input/, '不再要求输入仓位 id');
  assert.match(confirm, /data-close-go="live-1792699812006"/);
  assert.doesNotMatch(confirm, /data-close-go="[^"]*" disabled/);

  const calls = [];
  app.mockApi(async (url, opts) => { calls.push([url, plain(opts.body)]); return { ok: true, body: { position: { status: 'closed' } } }; });
  h.context.loadAccount = () => {};
  await app.closePosition(position.id);
  assert.deepEqual(calls, [['/api/trade/close', { mode: 'live', position_id: position.id, confirm: position.id }]]);
  assert.equal(app.state.pos.confirming, null);
});

test('manual add: button only on open two-leg spread positions, sends id + size + auto confirm', async () => {
  const h = harness();
  const { app } = h;
  app.state.mode = 'live';
  const position = { id: 'live-1', symbol: 'SKHY/USDT', status: 'open', strategy: 'spread', leverage: '3',
    long: { venue: 'bitget', notional_usdt: '600', adds: 2 }, short: { venue: 'lighter', notional_usdt: '600' } };
  assert.doesNotMatch(app.positionActions({ ...position, status: 'closing' }), /data-add=/);
  assert.doesNotMatch(app.positionActions({ ...position, strategy: 'funding' }), /data-add=/);
  assert.doesNotMatch(app.positionActions({ ...position, short: null }), /data-add=/);
  assert.doesNotMatch(app.positionActions({ ...position, leverage: null }), /data-add=/, '实盘没记杠杆的不能补');

  assert.equal(app.defaultAddSize(position), '200', '没加载自动交易设置时按开仓单腿名义');
  app.state.rhAuto.data = { settings: { size_usdt: '400' } };
  assert.equal(app.defaultAddSize(position), '400', '优先用自动交易的单腿名义');
  assert.equal(app.defaultAddSize({ ...position, long: { venue: 'binance', notional_usdt: '200' } }), '400', '币安和别家一样用通用单腿名义');

  app.state.pos.confirming = position.id;
  app.state.pos.confirmKind = 'add';
  app.state.pos.addSize = '';
  assert.match(app.positionActions(position), /data-add-go="live-1"[^>]*disabled/, '没填名义不能点');
  app.state.pos.addSize = '150';
  const ready = app.positionActions(position);
  assert.match(ready, /id="add-size"[^>]*value="150"/);
  assert.match(ready, /多 bitget \/ 空 lighter，杠杆 3x/);
  assert.doesNotMatch(ready, /data-add-go="live-1"[^>]*disabled/);

  const calls = [];
  app.mockApi(async (url, opts) => { calls.push([url, plain(opts.body)]); return { ok: true, body: { added: true, prepared: { size_usdt: '150' }, position: {} } }; });
  h.context.loadAccount = () => {};
  await app.addToPosition('live-1');
  assert.deepEqual(calls, [['/api/trade/add', { mode: 'live', position_id: 'live-1', size: '150', confirm: 'live-1' }]]);
  assert.equal(app.state.pos.flash.error, false);
  assert.match(app.state.pos.flash.html, /已补仓/);
});

test('manual add shows the live spread against the position entry spread', async () => {
  const h = harness();
  const { app } = h;
  app.state.mode = 'live';
  const position = { id: 'live-9', symbol: 'DRAM/USDT', status: 'open', strategy: 'spread', leverage: '3',
    long: { venue: 'lighter', average_price: '100', notional_usdt: '400' },
    short: { venue: 'hyperliquid-xyz', average_price: '100.5', notional_usdt: '400' } };
  app.state.positions = { open: [position] };
  assert.ok(Math.abs(app.positionEntrySpread(position) - 0.49875) < 1e-4);

  app.state.pos.confirming = position.id;
  app.state.pos.confirmKind = 'add';
  app.state.pos.addSize = '400';
  assert.match(app.addSpreadHtml(position), /原仓位开仓价差 <b>\+0\.499%<\/b> · 现在价差读取中/);

  const view = { size_usdt: '400', lines: [
    { base: 'DRAM', a: 'lighter', b: 'arcus', long_a: { entry_pct: '9' } },
    { base: 'DRAM', a: 'hyperliquid-xyz', b: 'lighter', age_sec: 1,
      long_a: { entry_pct: '-0.9' },
      long_b: { entry_pct: '0.7', net_to_normal_pct: '0.2', ladder: [{ size_usdt: '500', entry_pct: '0.65' }] } },
  ] };
  const calls = [];
  app.mockApi(async (url) => { calls.push(url); return { ok: true, body: view }; });
  app.state.rhAuto.data = { settings: { add_min_improve_pct: '0.05' } };
  await app.refreshAddQuote();
  app.stopAddQuote();
  // 补仓确认只要这一个合约的行（?base=…&limit=100），不拉整份快照。
  assert.deepEqual(calls, ['/api/rh-spread?base=DRAM&limit=100']);
  app.state.pos.addQuote = { view, error: null };
  const html = app.addSpreadHtml(position);
  assert.match(html, /现在可成交价差 <b>\+0\.700%<\/b>/, '取多 lighter / 空 xyz 那个方向，不取别的组');
  assert.match(html, /比原仓位好 \+0\.201%/);
  assert.match(html, /自动补仓门槛 \+0\.05%（够）/);
  assert.match(html, /回到正常净收益 <b class="pos">\+0\.200%<\/b>/);

  app.state.pos.addSize = '600';
  const bigger = app.addSpreadHtml(position);
  assert.match(bigger, /现在可成交价差 <b>\+0\.650%<\/b>/, '补得比估算名义大时取逐档里最大的那档');
  assert.match(bigger, /比这笔小、偏乐观/);

  app.state.pos.addQuote = { view: { size_usdt: '400', lines: [] }, error: null };
  assert.match(app.addSpreadHtml(position), /价差监控里没有这一组/);
  app.state.pos.addQuote = { view: null, error: 'HTTP 502' };
  assert.match(app.addSpreadHtml(position), /读不到价差监控：HTTP 502/);
});
