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
  });
  // Load the real application, omitting only browser startup/network requests.
  vm.runInContext(source.slice(0, boot) + `
    globalThis.app = {
      state, RULE_FIELDS, positionRuleForm, positionRulesEditor, ruleFields,
      rememberPositionRuleValues, rulePreferenceKey, ruleDraftKey, wirePositionRules,
      savePositionRules, saveStrategyParams, tradeBody, marginModeError, ruleFormError,
      pnlSummary, bookExitText, rhRow, rhStatus, renderRhSpread, rhOrderTarget, rhOriginNote, openRhInStrategy, rhAutoForm, rhAutoBody, rhAutoHtml, saveRhAuto,
      mockPrompt(fn) { globalThis.window = { prompt: fn }; },
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
    size_mismatch: '2', take_profit: 'off', auto_margin: 'off', auto_margin_max: 'off',
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

  const cfg = { auth_configured: true };
  const a = { data: { settings, status: '监控中', max_failures: 2, events: [], open_positions: [], cooldown: [], paper_watch_sec: 0, live_can_trade: true, monitor_size_usdt: '2000' }, form, dirty: false };
  const html = app.rhAutoHtml(a, cfg);
  for (const part of ['已关闭', '开启纸面自动交易', '纸面规则没有在看板后台运行', '监控中']) assert.ok(html.includes(part), part);
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
