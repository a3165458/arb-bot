//! 价差提醒下面的「下单…」按钮：两步确认的手动下单。
//!
//! - **第一下**（提醒上的按钮）只回一条确认消息。里面的数字是**点按钮这一刻**从价差监控重新读的，
//!   不是提醒发出时的；
//! - **第二下**（确认消息上的「确认」）才下单。确认票据 [`CONFIRM_TTL`] 内有效、只能用一次；
//! - 下单走和网页、自动交易同一条路径（[`crate::trade::open_for_auto`] / [`crate::trade::add_for_auto`]），
//!   对账、现扫、闸门、当日亏损、暂停开仓、交易台锁一个不少；
//! - 数量、杠杆、退出规则用面板上「价差自动交易」的设置（不要求自动交易开着）；自动交易自己的门槛
//!   （净收益、保持时间、名单、历史分位）只作提示、不拦 —— 点按钮就是人自己拿的主意；
//! - 同一合约已有同方向（同样的多腿、空腿）价差仓位时是**补仓**到那笔上；有别的仓位就不做；
//! - 确认时重读一遍：要做的事变了（比如自动交易刚开了这个合约）、或可成交价差比确认消息里的差了
//!   超过 [`max_drift_pct`]，就不下单。
//!
//! 票据只在内存里：重启后旧消息上的按钮回「已过期」。

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arb_core::{Decimal, Symbol, Venue};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::rh_auto::{AddTarget, Outcome, Settings};
use crate::rh_spread::{View, venue_label};
use crate::trade::Mode;

/// 提醒上的「下单…」按钮多久内能点。点了也只是看确认，数字按点的那一刻重算。
pub const ALERT_TICKET_TTL: Duration = Duration::from_secs(2 * 3600);
/// 确认消息上的「确认」多久内有效。
pub const CONFIRM_TTL: Duration = Duration::from_secs(90);
/// 票据表最多留多少张（满了丢最旧的）。
const MAX_TICKETS: usize = 256;

/// 确认时可成交价差最多比确认消息里差多少（百分点）。
pub fn max_drift_pct() -> Decimal {
    Decimal::new(1, 1)
}

/// 提醒按钮指向的那一行：组、合约、方向（`long_a` / `long_b`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub pair: String,
    pub base: String,
    pub direction: &'static str,
}

/// 内存里的票据：短 id → 内容。Telegram 的 `callback_data` 最多 64 字节，参数不往里塞。
pub struct Tickets<T> {
    ttl: Duration,
    map: HashMap<String, (Instant, T)>,
}

impl<T: Clone> Tickets<T> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            map: HashMap::new(),
        }
    }

    pub fn issue(&mut self, value: T, now: Instant) -> String {
        let ttl = self.ttl;
        self.map
            .retain(|_, (at, _)| now.saturating_duration_since(*at) < ttl);
        while self.map.len() >= MAX_TICKETS {
            let Some(oldest) = self
                .map
                .iter()
                .min_by_key(|(_, (at, _))| *at)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            self.map.remove(&oldest);
        }
        let id = new_id();
        self.map.insert(id.clone(), (now, value));
        id
    }

    /// 还没过期就给一份（不作废）。
    pub fn get(&self, id: &str, now: Instant) -> Option<T> {
        self.map
            .get(id)
            .filter(|(at, _)| now.saturating_duration_since(*at) < self.ttl)
            .map(|(_, value)| value.clone())
    }

    /// 取出并作废：只能用一次。过期的也作废、返回 `None`。
    pub fn take(&mut self, id: &str, now: Instant) -> Option<T> {
        let (at, value) = self.map.remove(id)?;
        (now.saturating_duration_since(at) < self.ttl).then_some(value)
    }

    pub fn remove(&mut self, id: &str) {
        self.map.remove(id);
    }
}

/// 12 位十六进制的票据 id。只要求不重复、不好猜（授权另有管理员私聊那道关）。
pub fn new_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    );
    format!("{:012x}", hasher.finish() & 0xffff_ffff_ffff)
}

/// 要做的事。
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// 新开一笔，带上这些退出规则。
    Open { rules: arb_exec::TaskRules },
    /// 补到这笔已有仓位上（杠杆、保证金模式、规则沿用原仓位）。
    Add(AddTarget),
}

/// 确认消息里说好的一单：确认后按这些参数下，不再读设置。
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub intent: Intent,
    pub long: Venue,
    pub short: Venue,
    pub action: Action,
    pub mode: Mode,
    pub size: Decimal,
    pub leverage: Decimal,
    pub relative_to_normal: bool,
    /// 补完后的单腿名义上限（补仓用）。
    pub max_total: Decimal,
    /// 确认消息里的开仓可成交价差（%）。
    pub entry_pct: Decimal,
    /// 确认消息正文。
    pub prompt: String,
    /// 「确认」按钮上的字。
    pub button: String,
    /// 一行摘要（日志、结果消息用）。
    pub summary: String,
}

pub fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Live => "实盘",
        Mode::Paper => "纸面",
    }
}

/// 按现在的价差监控、台账和自动交易设置，算出这张票据要下的单（纯函数）。`Err` 是为什么不能下。
///
/// `exposed`：台账里有敞口的仓位；`targets`：其中可以补仓的价差仓位；`tradable`：这家场所现在能不能下单。
#[allow(clippy::too_many_arguments)]
pub fn plan(
    view: &View,
    intent: &Intent,
    settings: &Settings,
    exposed: &[(String, Symbol)],
    targets: &[AddTarget],
    max_size: Option<Decimal>,
    tradable: &dyn Fn(Venue) -> bool,
    now: DateTime<Utc>,
) -> Result<Plan, String> {
    match view.updated_at {
        Some(at) if (now - at).num_seconds() <= crate::rh_auto::VIEW_STALE_SEC => {}
        _ => return Err("价差监控的快照不新鲜（行情可能断了），这一刻不下单".into()),
    }
    let line = view
        .lines
        .iter()
        .find(|line| line.pair == intent.pair && line.base == intent.base)
        .ok_or_else(|| {
            format!(
                "价差监控里现在没有 {}（{}）这一行",
                intent.base, intent.pair
            )
        })?;
    if let Some(note) = &line.note {
        return Err(format!("{} 现在不能做：{note}", line.base));
    }
    if line.observe_only {
        return Err(format!(
            "{}（{}）只观察：Binance 的组只做挂单测算，不按吃单下单",
            line.base, line.pair
        ));
    }
    if line.watch_only {
        return Err(format!(
            "{}（{}）是观察组：还没有实盘账户，不下单",
            line.base, line.pair
        ));
    }
    let (long, short, leg) = line
        .legs(intent.direction)
        .ok_or("这个方向现在没有可成交报价（盘口断了或太旧）")?;
    if !view.connected.up(long) || !view.connected.up(short) {
        return Err("有一家的行情连接断了，这一刻不下单".into());
    }
    let mode = mode_label(settings.mode);
    if !tradable(long) || !tradable(short) {
        return Err(format!(
            "{} / {} 不都是{mode}已连接的场所",
            venue_label(long),
            venue_label(short)
        ));
    }
    // 下单路径的硬条件：可成交价差为正、收敛到 0 也赚钱（开了「相对正常基差」时由下单路径按目标核对）。
    if !settings.relative_to_normal
        && let Some(reason) = leg.auto_entry_block_reason()
    {
        return Err(format!("不满足开仓硬条件：{reason}"));
    }
    let size = settings.size_usdt;
    let multiple = size * settings.add_max_multiple;
    let max_total = max_size.map_or(multiple, |max| max.min(multiple));
    let entry = leg.quote.entry_pct;
    let legs_text = format!(
        "{}：多 {} / 空 {}",
        line.base,
        venue_label(long),
        venue_label(short)
    );
    let same: Vec<&str> = exposed
        .iter()
        .filter(|(_, symbol)| symbol.base.eq_ignore_ascii_case(&line.base))
        .map(|(id, _)| id.as_str())
        .collect();
    let mut lines = Vec::new();
    let (action, verb) = if same.is_empty() {
        let target_pct = if settings.back_to_normal {
            let normal = line
                .normal
                .as_ref()
                .ok_or("还没有正常基差，设不了「回到正常基差平仓」的目标")?;
            Some(
                crate::rh_auto::target_for(intent.direction, normal.median)
                    .ok_or("正常基差超出规则允许的 ±5%，设不了「回到正常基差平仓」的目标")?,
            )
        } else {
            None
        };
        let rules = settings.rules(target_pct);
        lines.push(format!("⚠️ 确认{mode}开仓？点下面的「确认」才会下单。"));
        lines.push(legs_text.clone());
        lines.push(format!(
            "单腿 {} USDT × {}x，逐仓（按面板「价差自动交易」的设置；提醒里的 {} USDT 只是监控的估算口径）",
            size.normalize(),
            settings.leverage.normalize(),
            view.size_usdt.normalize()
        ));
        lines.push(format!(
            "平仓规则：{}",
            arb_exec::cli::describe_rules(&rules)
        ));
        (Action::Open { rules }, "开仓")
    } else {
        let Some(target) = targets.iter().find(|target| {
            target.base.eq_ignore_ascii_case(&line.base)
                && target.long == long
                && target.short == short
        }) else {
            return Err(format!(
                "已有 {} 的仓位 {}，方向 / 场所和这条提醒不一样（或不是持仓中的价差仓位）：Telegram 上不叠加，去面板处理",
                line.base,
                same.join("、")
            ));
        };
        if target.adds >= settings.add_max_times {
            return Err(format!(
                "仓位 {} 已补 {} 次，到上限 {} 次",
                target.id, target.adds, settings.add_max_times
            ));
        }
        let after = target.notional_usdt + size;
        if after > max_total {
            return Err(format!(
                "仓位 {} 补完单腿约 {} USDT，超过上限 {} USDT",
                target.id,
                after.round_dp(0),
                max_total.round_dp(0)
            ));
        }
        let improve = entry - target.entry_spread_pct;
        lines.push(format!("⚠️ 确认{mode}补仓？点下面的「确认」才会下单。"));
        lines.push(format!("{legs_text} → 补到仓位 {}", target.id));
        lines.push(format!(
            "原仓位开仓价差 {}%，现在可成交 {}%（{} {}%{}）",
            target.entry_spread_pct.round_dp(3),
            entry.round_dp(3),
            if improve.is_sign_negative() {
                "差"
            } else {
                "好"
            },
            improve.abs().round_dp(3),
            settings
                .add_min_improve_pct
                .map_or(String::new(), |min| format!(
                    "；自动补仓门槛 {}%",
                    min.normalize()
                ))
        ));
        lines.push(format!(
            "补 {} USDT（第 {} 次，上限 {} 次；补完单腿约 {} / 上限 {} USDT）；杠杆与保证金模式沿用原仓位，原仓位的规则不变",
            size.normalize(),
            target.adds + 1,
            settings.add_max_times,
            after.round_dp(0),
            max_total.round_dp(0)
        ));
        if improve.is_sign_negative() {
            lines.push("⚠️ 现在的价差比原仓位差：补了会拉低整笔的开仓价差".into());
        }
        (Action::Add(target.clone()), "补仓")
    };
    lines.push(format!(
        "现在盘口：可成交价差 {}%，回到正常预估净赚 {}（已扣手续费与平仓穿价，未扣滑点缓冲），收敛到 0 净赚 {}%",
        entry.round_dp(3),
        leg.net_to_normal_pct
            .map_or("未知".to_string(), |net| format!("{}%", net.round_dp(3))),
        leg.net_to_zero_pct.round_dp(3)
    ));
    match (
        leg.net_to_normal_pct,
        crate::rh_auto::auto_skip_reason(line, leg, settings),
    ) {
        (None, _) => lines.push("提示：没有正常水平，自动交易不会下这单。".into()),
        (Some(_), Some(reason)) => {
            lines.push(format!("提示：自动交易此刻不会下这单（{reason}）。"));
        }
        (Some(_), None) => lines.push("提示：这一刻也过了自动交易的净收益门槛。".into()),
    }
    if let Some(gate) = settings.history_gate_pct {
        lines.push(format!(
            "提示：历史价差分位门槛（前 {}%）这里不检查。",
            gate.normalize()
        ));
    }
    lines.push(format!(
        "确认后照常对账、现扫一轮、过闸门和当日亏损上限；{} 秒内有效、只能点一次；确认时可成交价差比现在差超过 {}% 就不下。",
        CONFIRM_TTL.as_secs(),
        max_drift_pct()
    ));
    // 提醒按钮 2 小时内都能点：信号没了要放在第一行，手机上一眼就能看到。
    match leg.net_to_normal_pct {
        Some(net) if net <= Decimal::ZERO => lines.insert(
            0,
            format!(
                "⚠️ 信号已经消失：现在回到正常预估净赚 {}%（≤ 0），按现在的价格下大概率亏手续费。",
                net.round_dp(3)
            ),
        ),
        Some(net) if net < view.alert_net_pct => lines.insert(
            0,
            format!(
                "⚠️ 信号已经减弱：现在回到正常预估净赚 {}%，低于提醒门槛 {}%。",
                net.round_dp(3),
                view.alert_net_pct.normalize()
            ),
        ),
        _ => {}
    }
    Ok(Plan {
        intent: intent.clone(),
        long,
        short,
        mode: settings.mode,
        size,
        leverage: settings.leverage,
        relative_to_normal: settings.relative_to_normal,
        max_total,
        entry_pct: entry,
        prompt: lines.join("\n"),
        button: format!("✅ 确认{mode}{verb} {} {}U", line.base, size.normalize()),
        summary: format!(
            "{mode}{verb} {legs_text}{}，单腿 {} USDT × {}x，确认时可成交价差 {}%",
            match &action {
                Action::Add(target) => format!(" → {}", target.id),
                Action::Open { .. } => String::new(),
            },
            size.normalize(),
            settings.leverage.normalize(),
            entry.round_dp(3)
        ),
        action,
    })
}

/// 确认那一刻重算出来的 `fresh` 和确认消息里的 `confirmed` 还是不是同一件事。
pub fn still_valid(confirmed: &Plan, fresh: &Plan) -> Result<(), String> {
    // 复核是按此刻的设置查的台账（实盘 / 纸面）：设置变了，复核的就不是要下的那一单。
    if (confirmed.mode, confirmed.size, confirmed.leverage)
        != (fresh.mode, fresh.size, fresh.leverage)
    {
        return Err(
            "价差自动交易的模式 / 数量 / 杠杆刚改过，和确认消息里的不一样了；要下请重新点提醒下面的「下单…」"
                .into(),
        );
    }
    let same = match (&confirmed.action, &fresh.action) {
        (Action::Open { .. }, Action::Open { .. }) => true,
        (Action::Add(a), Action::Add(b)) => a.id == b.id,
        _ => false,
    };
    if !same {
        return Err(
            "这个合约的持仓刚有变化（比如自动交易刚开了一笔），和确认消息里说的不一样了；要下请重新点提醒下面的「下单…」"
                .into(),
        );
    }
    let worse = confirmed.entry_pct - fresh.entry_pct;
    if worse > max_drift_pct() {
        return Err(format!(
            "可成交价差从确认消息里的 {}% 变成了 {}%，差了 {}%（超过 {}%）",
            confirmed.entry_pct.round_dp(3),
            fresh.entry_pct.round_dp(3),
            worse.round_dp(3),
            max_drift_pct()
        ));
    }
    Ok(())
}

/// 开仓接口的返回 → 给人看的结果。
pub fn open_outcome(mode: Mode, status: StatusCode, body: &Value) -> String {
    let reason = body["error"].as_str().unwrap_or("没有返回原因");
    match crate::rh_auto::classify(status, body) {
        Outcome::Opened(id) => match mode {
            Mode::Live => {
                format!("✅ 已开仓 {id}。成交明细见下单路径发的开仓通知，平仓规则已随仓位生效。")
            }
            Mode::Paper => format!("✅ 纸面已开仓 {id}。"),
        },
        Outcome::Unwound(id) => {
            format!("⚠️ 开仓以回滚收场（{id}）：有一条腿没成交，已把成交的那条平掉。去持仓页核对。")
        }
        Outcome::NoFill(id) => format!("挂单没成交，已撤（{id}），什么都没发生。"),
        Outcome::Untraded(id) => format!("第一腿没成交（{id}），什么都没发生。"),
        Outcome::Busy => {
            "没下单：交易台正忙（规则轮或自动交易在下单），稍后重新点提醒下面的「下单…」。".into()
        }
        Outcome::Paused => format!("没下单：{reason}"),
        Outcome::Rejected => format!("没下单，下单前检查没通过：{reason}"),
        Outcome::Interrupted => {
            format!("⛔ 执行中断、结果未知：{reason}。以台账与对账为准，去持仓页核对。")
        }
        Outcome::Misconfigured => format!("没下单（HTTP {}）：{reason}", status.as_u16()),
    }
}

/// 补仓接口的返回 → 给人看的结果。
pub fn add_outcome(position: &str, status: StatusCode, body: &Value) -> String {
    let reason = body["error"].as_str().unwrap_or("没有返回原因");
    if status.is_success() {
        let note = body["position"]["note"].as_str().unwrap_or_default();
        return if body["added"].as_bool().unwrap_or(false) {
            format!("✅ 已补仓到 {position}。{note}")
        } else {
            format!("没补成（{position}）。{note}")
        };
    }
    match crate::rh_auto::classify(status, body) {
        Outcome::Busy => {
            "没补仓：交易台正忙（规则轮或自动交易在下单），稍后重新点提醒下面的「下单…」。".into()
        }
        Outcome::Interrupted => {
            format!("⛔ 补仓中断、结果未知：{reason}。以台账与对账为准，去持仓页核对。")
        }
        Outcome::Paused => format!("没补仓：{reason}"),
        _ => format!("没补仓，补仓前检查没通过：{reason}"),
    }
}

#[cfg(test)]
mod tests;
