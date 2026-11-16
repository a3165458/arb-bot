//! 策略工作区：选两家场所，看同时上市的合约、两边费率、方向与净年化；对选中的一对腿
//! 给出纸面开仓计划；只读展示纸面台账里的持仓与监控评估。
//!
//! 全部数字都来自 `arb-scanner` / `arb-exec` 的同一份计算：配对与净年化走 `rank::rank`，
//! 强平走 `leverage`，规则走 `monitor`。面板只展示，不自己复算。

use std::collections::HashMap;

use arb_core::{Decimal, MarketSnapshot, Side, Symbol, Venue};
use arb_exec::monitor::PROTECTION_TARGET_MULTIPLE;
use arb_exec::monitor::{Inputs, evaluate_full};
use arb_exec::{Evaluation, PairPosition, Replayed, TaskRules, VenueLegState};
use arb_scanner::leverage::{Health, liquidation_price, trim_fraction};
use arb_scanner::rank::{self, RankConfig};
use arb_scanner::{Opportunity, PairRisk, ScanReport, SymbolView, pair_risk, to_apr, to_daily};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// 配对榜最多返回多少行。两家大所同时上市的合约可能有几百个，全量下发没有意义。
pub const MAX_PAIR_ROWS: usize = 300;
/// 「最近结束」不带分页参数时每页几笔（Telegram、旧请求）。
pub const MAX_CLOSED_POSITIONS: usize = 20;
/// 「最近结束」每页最多几笔。
pub const MAX_CLOSED_PER_PAGE: usize = 100;

/// 「最近结束」取哪一页（从 1 起）。超出末页的按末页给。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedPage {
    pub page: usize,
    pub per_page: usize,
    /// 连用户移除（隐藏）了的也列出来（带 `dismissed` 标记，页面上可以恢复）。
    pub show_dismissed: bool,
}

impl Default for ClosedPage {
    fn default() -> Self {
        Self {
            page: 1,
            per_page: MAX_CLOSED_POSITIONS,
            show_dismissed: false,
        }
    }
}

impl ClosedPage {
    /// 请求参数：缺省用默认，页码至少 1，每页 1 ～ [`MAX_CLOSED_PER_PAGE`]。
    pub fn from_query(page: Option<usize>, per_page: Option<usize>) -> Self {
        let default = Self::default();
        Self {
            page: page.unwrap_or(default.page).max(1),
            per_page: per_page
                .unwrap_or(default.per_page)
                .clamp(1, MAX_CLOSED_PER_PAGE),
            show_dismissed: false,
        }
    }

    /// 连已移除的也列出来。
    pub fn with_dismissed(mut self, show: bool) -> Self {
        self.show_dismissed = show;
        self
    }
}
/// 开仓计划允许的单腿名义上限。
pub const MAX_PLAN_SIZE_USDT: u32 = 1_000_000;

/// 一条腿在某家场所的报价。
#[derive(Debug, Clone, Serialize)]
pub struct LegQuote {
    pub venue: Venue,
    /// 这家场所这一腿的毛年化：每期费率折成年，不扣任何成本。
    pub apr: Decimal,
    pub interval_h: u32,
    pub mark_price: Option<Decimal>,
    pub max_leverage: Option<Decimal>,
    pub oi_capped: bool,
}

#[derive(Debug, Serialize)]
pub struct PairRow {
    pub symbol: Symbol,
    pub a: LegQuote,
    pub b: LegQuote,
    /// 低费率一侧做多、高费率一侧做空。两边费率相等时没有方向。
    pub long: Option<Venue>,
    pub short: Option<Venue>,
    /// 毛费差年化（不扣成本）。
    pub gross_apr: Decimal,
    /// 这个方向的完整机会（含摊费后净年化）。读数被排除或没有方向时为 `None`。
    pub opportunity: Option<Opportunity>,
    /// 被入场基差门槛挡下的原因。
    pub gated: Option<String>,
    /// 至少一条腿被可信度筛查排除的原因。被排除的读数不参与配对。
    pub excluded: Option<String>,
    /// 这个方向的标记价基差（%）：(空腿标记价 − 多腿标记价) / 中间价 × 100。
    pub basis_pct: Option<Decimal>,
    /// 这个方向持有期间的资金费年化（空腿 − 多腿，不扣成本）。资金费视角里恒为正；
    /// 价差视角按价格定方向，可能为负（持有要付资金费）。
    pub carry_apr: Option<Decimal>,
    /// 这个方向开仓能锁住的价差（%）：(空腿买一 − 多腿卖一) / 两者中间价 × 100。
    /// 任一条腿的批量行情不给买一卖一（DEX）时为 `None`。
    pub executable_basis_pct: Option<Decimal>,
}

#[derive(Debug, Serialize)]
pub struct PairBoard {
    /// `funding` 或 `spread`：方向按费率定，还是按价格定。
    pub view: String,
    pub a: Venue,
    pub b: Venue,
    /// 本轮取数成功、可供选择的场所。
    pub venues: Vec<Venue>,
    /// 两家同时上市的合约总数（`rows` 可能被截断）。
    pub total: usize,
    pub rows: Vec<PairRow>,
}

fn rank_config(report: &ScanReport) -> RankConfig {
    report.rank_config()
}

fn leg(view: &SymbolView, venue: Venue) -> Option<&MarketSnapshot> {
    view.rates.iter().find(|rate| rate.venue == venue)
}

fn quote(snapshot: &MarketSnapshot) -> LegQuote {
    LegQuote {
        venue: snapshot.venue,
        apr: to_apr(to_daily(snapshot.period_rate, snapshot.interval_h)),
        interval_h: snapshot.interval_h,
        mark_price: snapshot.mark_price,
        max_leverage: snapshot.max_leverage,
        oi_capped: snapshot.oi_capped,
    }
}

/// 被可信度筛查排除的读数：`(场所, 合约)` → 原因。
fn exclusions(report: &ScanReport) -> HashMap<(Venue, String), String> {
    report
        .suspicious
        .iter()
        .chain(report.unverified.iter())
        .map(|row| ((row.venue, row.symbol.to_string()), row.reason.clone()))
        .collect()
}

/// 本轮可选的场所：取数成功的那些。
pub fn live_venues(report: &ScanReport) -> Vec<Venue> {
    report
        .venues
        .iter()
        .filter(|venue| venue.ok)
        .map(|venue| venue.venue)
        .collect()
}

/// 标记价基差（%）：(空腿 − 多腿) / 中间价 × 100。任一腿缺标记价时为 `None`。
fn mark_basis_pct(long: &MarketSnapshot, short: &MarketSnapshot) -> Option<Decimal> {
    let (long, short) = (long.mark_price?, short.mark_price?);
    let mid = (long + short) / Decimal::TWO;
    (mid > Decimal::ZERO).then(|| ((short - long) / mid * Decimal::ONE_HUNDRED).round_dp(4))
}

/// 开仓能锁住的价差（%），与扫描器同一个公式：做空吃买一，做多吃卖一。
fn executable_basis_pct(long: &MarketSnapshot, short: &MarketSnapshot) -> Option<Decimal> {
    let (bid, ask) = (short.best_bid?, long.best_ask?);
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO {
        return None;
    }
    let mid = (bid + ask) / Decimal::TWO;
    Some(((bid - ask) / mid * Decimal::ONE_HUNDRED).round_dp(4))
}

/// 两家场所同时上市的合约。
///
/// - `funding`：低费率一侧做多、高费率一侧做空，按摊费后净年化倒序。
/// - `spread`：便宜的一侧做多、贵的一侧做空（按标记价），按标记价基差倒序。批量接口
///   不给买一卖一的场所（DEX）算不出可成交价差，选中后由开仓计划 / 预览现拉盘口核算。
pub fn pair_board(report: &ScanReport, a: Venue, b: Venue, view_name: &str) -> PairBoard {
    let spread_view = view_name == "spread";
    let config = rank_config(report);
    let excluded = exclusions(report);
    let mut rows = Vec::new();
    for view in &report.symbols {
        let (Some(sa), Some(sb)) = (leg(view, a), leg(view, b)) else {
            continue;
        };
        let (qa, qb) = (quote(sa), quote(sb));
        let order = if spread_view {
            match (sa.mark_price, sb.mark_price) {
                (Some(pa), Some(pb)) => pa.cmp(&pb),
                _ => std::cmp::Ordering::Equal,
            }
        } else {
            qa.apr.cmp(&qb.apr)
        };
        let (long, short) = match order {
            std::cmp::Ordering::Less => (Some(sa), Some(sb)),
            std::cmp::Ordering::Greater => (Some(sb), Some(sa)),
            std::cmp::Ordering::Equal => (None, None),
        };
        let basis_pct = long
            .zip(short)
            .and_then(|(long, short)| mark_basis_pct(long, short));
        let carry_apr = long
            .zip(short)
            .map(|(long, short)| quote(short).apr - quote(long).apr);
        let executable_basis_pct = long
            .zip(short)
            .and_then(|(long, short)| executable_basis_pct(long, short));
        let key = view.symbol.to_string();
        let reason = excluded
            .get(&(a, key.clone()))
            .or_else(|| excluded.get(&(b, key)))
            .cloned();
        let (opportunity, gated) = match (long, short, &reason) {
            (Some(long), Some(short), None) => {
                let opportunity = rank::rank(&[long, short], &config)
                    .into_iter()
                    .find(|op| op.long == long.venue && op.short == short.venue);
                // 入场基差门槛只管资金费视角：价差视角进场时基差本来就是正的。
                let gated = opportunity
                    .as_ref()
                    .filter(|_| !spread_view)
                    .and_then(|op| rank::basis_gate(op, config.max_entry_basis_pct).err());
                (opportunity, gated)
            }
            _ => (None, None),
        };
        rows.push(PairRow {
            symbol: view.symbol.clone(),
            gross_apr: (qa.apr - qb.apr).abs(),
            a: qa,
            b: qb,
            long: long.map(|snapshot| snapshot.venue),
            short: short.map(|snapshot| snapshot.venue),
            opportunity,
            gated,
            excluded: reason,
            basis_pct,
            carry_apr,
            executable_basis_pct,
        });
    }
    rows.sort_by(|left, right| {
        let key = |row: &PairRow| {
            if spread_view {
                row.basis_pct
            } else {
                row.opportunity.as_ref().map(|op| op.funding_apr)
            }
        };
        key(right)
            .cmp(&key(left))
            .then_with(|| right.gross_apr.cmp(&left.gross_apr))
            .then_with(|| left.symbol.base.cmp(&right.symbol.base))
    });
    let total = rows.len();
    rows.truncate(MAX_PAIR_ROWS);
    PairBoard {
        view: if spread_view { "spread" } else { "funding" }.into(),
        a,
        b,
        venues: live_venues(report),
        total,
        rows,
    }
}

/// 开仓计划的输入。
#[derive(Debug, Clone)]
pub struct PlanInput {
    pub margin_mode: arb_exec::MarginMode,
    pub symbol: Symbol,
    pub long: Venue,
    pub short: Venue,
    pub size_usdt: Decimal,
    pub leverage: Decimal,
    /// `funding` 或 `spread`。
    pub view: String,
    pub rules: TaskRules,
}

#[derive(Debug, Serialize)]
pub struct LegPlanView {
    pub venue: Venue,
    pub side: Side,
    pub mark_price: Option<Decimal>,
    pub leverage: Decimal,
    pub max_leverage: Option<Decimal>,
    pub maintenance_margin: Option<Decimal>,
    pub margin_usdt: Decimal,
    /// 按当前标记价入场时的强平价。缺维持保证金率时为 `None`。
    pub liquidation_price: Option<Decimal>,
    pub liq_distance_pct: Option<Decimal>,
    pub health: Option<Health>,
}

#[derive(Debug, Serialize)]
pub struct PlanView {
    pub margin_mode: arb_exec::MarginMode,
    pub symbol: Symbol,
    pub size_usdt: Decimal,
    pub requested_leverage: Decimal,
    /// 实际按多少倍算：请求值与两腿场所上限的较小者，两腿相同。
    pub leverage: Decimal,
    pub opportunity: Opportunity,
    pub risk: PairRisk,
    pub long: LegPlanView,
    pub short: LegPlanView,
    pub margin_total_usdt: Decimal,
    /// 每日毛资金费收入（单腿名义 × 日化费差）。
    pub daily_gross_usdt: Decimal,
    /// 每日摊费后净收入（单腿名义 × 摊费后日化）。
    pub daily_net_usdt: Decimal,
    /// 一次往返的成本（手续费 + 穿价）。
    pub round_trip_cost_usdt: Decimal,
    /// 至少一条腿拿不到盘口：成本是下界。
    pub cost_is_lower_bound: bool,
    pub rules: TaskRules,
    /// 规则不成立的原因（开仓就会触发、或缺数据无从执行）。
    pub rules_error: Option<String>,
    /// 爆仓保护触发时两腿各减仓的比例（%），以及要拉回的距离（%）。
    pub protection_trim_pct: Option<Decimal>,
    pub protection_target_pct: Option<Decimal>,
    /// 被入场基差门槛挡下的原因。
    pub gated: Option<String>,
    pub warnings: Vec<String>,
    /// 用纸面执行器开这一笔的命令。
    pub command: String,
    /// `funding` 或 `spread`。
    pub view: String,
    /// 价差视角才有：价差账。
    pub spread: Option<SpreadPlan>,
}

/// 价差套利的开仓估算。
#[derive(Debug, Serialize)]
pub struct SpreadPlan {
    /// 标记价基差（%）。「基差收敛平仓」按它判断。
    pub mark_basis_pct: Option<Decimal>,
    /// 可成交价差（%）：(空腿买一 − 多腿卖一) / 中间价 × 100。
    pub executable_basis_pct: Option<Decimal>,
    /// 收敛到 0 时这一笔的一次性净收益（小数）与金额：扣掉平仓穿价与往返手续费。
    pub net_pct: Decimal,
    pub net_usdt: Decimal,
    /// 设了「基差收敛平仓」时：收敛到目标就平仓的净收益（小数）与金额。只赚得到入场基差
    /// 与目标之间那一段，下单闸门按它判断。
    pub target_pct: Option<Decimal>,
    pub target_net_pct: Option<Decimal>,
    pub target_net_usdt: Option<Decimal>,
    /// 保本的收敛目标（%）：目标低于它才有得赚。
    pub break_even_target_pct: Decimal,
    /// 至少一条腿批量接口没给买一卖一：按「买一 = 卖一 = 标记价」估，是上界。
    /// 真正的可成交价差在预览时按现拉盘口核算。
    pub estimated_from_marks: bool,
    /// 持有期间每天的资金费（正 = 收、负 = 付）：名义 × 日化费差（空腿 − 多腿）。
    pub carry_daily_usdt: Decimal,
    /// 预计持有天数（价差半衰期；没实测时是配置值）。
    pub hold_days: Decimal,
    pub hold_measured: bool,
}

/// 对一对腿给出纸面开仓计划。拒绝的情况返回原因，由调用方回 400。
pub fn plan(report: &ScanReport, input: &PlanInput) -> Result<PlanView, String> {
    if input.long == input.short {
        return Err("两腿不能是同一场所".into());
    }
    if input.size_usdt <= Decimal::ZERO || input.size_usdt > Decimal::from(MAX_PLAN_SIZE_USDT) {
        return Err(format!("单腿名义必须在 0 ~ {MAX_PLAN_SIZE_USDT} 之间"));
    }
    // 同名合约可能被身份判定拆成几个簇：找同时含这两条腿的那个，不取第一个同名的。
    let view = report
        .symbols
        .iter()
        .find(|view| {
            view.symbol == input.symbol
                && leg(view, input.long).is_some()
                && leg(view, input.short).is_some()
        })
        .or_else(|| {
            report
                .symbols
                .iter()
                .find(|view| view.symbol == input.symbol)
        })
        .ok_or_else(|| format!("本轮扫描里没有 {}", input.symbol))?;
    let long = leg(view, input.long)
        .ok_or_else(|| format!("{} 没有 {} 的读数", input.long, input.symbol))?;
    let short = leg(view, input.short)
        .ok_or_else(|| format!("{} 没有 {} 的读数", input.short, input.symbol))?;
    let excluded = exclusions(report);
    for venue in [input.long, input.short] {
        if let Some(reason) = excluded.get(&(venue, input.symbol.to_string())) {
            return Err(format!("{venue} 的读数被排除在配对之外：{reason}"));
        }
    }
    let config = rank_config(report);
    let spread_view = input.view == "spread";
    let (opportunity, estimated_from_marks) = if spread_view {
        // 批量接口不给买一卖一的腿（DEX），按「买一 = 卖一 = 标记价」补上：零宽盘口，
        // 算出来的净收益是上界。公式仍是扫描器那一套，不另写一份。
        let fill = |snapshot: &MarketSnapshot| {
            if snapshot.best_bid.is_some() && snapshot.best_ask.is_some() {
                (snapshot.clone(), false)
            } else {
                let mut filled = snapshot.clone();
                filled.best_bid = filled.mark_price;
                filled.best_ask = filled.mark_price;
                (filled, true)
            }
        };
        let ((long_filled, long_est), (short_filled, short_est)) = (fill(long), fill(short));
        let estimated = long_est || short_est;
        let opportunity = rank::rank(&[&long_filled, &short_filled], &config)
            .into_iter()
            .find(|op| {
                op.long == input.long
                    && op.short == input.short
                    && op
                        .executable_basis_pct
                        .is_some_and(|basis| basis > Decimal::ZERO)
            })
            .ok_or_else(|| {
                format!(
                    "按{}，{} 并不比 {} 便宜：价差方向不成立；试试反过来",
                    if estimated {
                        "标记价"
                    } else {
                        "买一卖一"
                    },
                    input.long,
                    input.short
                )
            })?;
        (opportunity, estimated)
    } else {
        let opportunity = rank::rank(&[long, short], &config)
            .into_iter()
            .find(|op| op.long == input.long && op.short == input.short)
            .ok_or_else(|| {
                format!(
                    "{} 做多、{} 做空收不到费差，价差方向也不成立；试试反过来",
                    input.long, input.short
                )
            })?;
        (opportunity, false)
    };

    let requested = input.leverage;
    let pair_max = long
        .max_leverage
        .zip(short.max_leverage)
        .map(|(a, b)| a.min(b));
    let leverage = pair_max.map_or(requested, |max| requested.min(max));
    let mut risk = pair_risk(&opportunity, long, short, leverage);
    arb_exec::margin::display_risk(input.margin_mode, &mut risk);
    let margin = input.size_usdt / leverage;
    let leg_view = |snapshot: &MarketSnapshot, side: Side, leg: &arb_scanner::LegRisk| {
        let liquidation = if input.margin_mode.is_cross() {
            None
        } else {
            snapshot
                .mark_price
                .zip(snapshot.maintenance_margin)
                .and_then(|(mark, mmr)| liquidation_price(mark, input.size_usdt, margin, mmr, side))
        };
        LegPlanView {
            venue: snapshot.venue,
            side,
            mark_price: snapshot.mark_price,
            leverage,
            max_leverage: snapshot.max_leverage,
            maintenance_margin: snapshot.maintenance_margin,
            margin_usdt: margin,
            liquidation_price: liquidation,
            liq_distance_pct: leg.liq_distance_pct,
            health: leg.health,
        }
    };
    let long_view = leg_view(long, Side::Buy, &risk.long);
    let short_view = leg_view(short, Side::Sell, &risk.short);

    let rules_error = arb_exec::margin::validate_open_rules(
        input.margin_mode,
        &input.rules,
        risk.liq_distance_pct,
    )
    .err()
    .or_else(|| {
        let target = input.rules.basis_exit_pct?;
        match opportunity.entry_basis_pct {
            Some(entry) if target >= entry => Some(format!(
                "基差收敛目标 {target}% 不低于当前标记价基差 {}%，开仓就会触发平仓",
                entry.round_dp(3)
            )),
            None => Some("缺两腿标记价，基差收敛无从评估".into()),
            Some(_) => None,
        }
    });
    let protection_target_pct = (!input.margin_mode.is_cross())
        .then_some(&input.rules)
        .and_then(|rules| rules.liq_protection_pct)
        .map(|threshold| threshold * PROTECTION_TARGET_MULTIPLE);
    let protection_trim_pct = input
        .rules
        .liq_protection_pct
        .zip(protection_target_pct)
        .and_then(|(threshold, target)| {
            [(long, Side::Buy), (short, Side::Sell)]
                .into_iter()
                .filter_map(|(snapshot, side)| {
                    trim_fraction(threshold, target, snapshot.maintenance_margin?, side)
                })
                .max()
        })
        .map(|fraction| fraction * Decimal::ONE_HUNDRED);
    // 入场基差门槛只管资金费视角。
    let gated = if spread_view {
        None
    } else {
        rank::basis_gate(&opportunity, config.max_entry_basis_pct).err()
    };

    let mut warnings = Vec::new();
    if let Some(warning) = arb_exec::margin::warning(input.margin_mode) {
        warnings.push(warning.into());
        for venue in [input.long, input.short] {
            if matches!(venue, Venue::Arcus | Venue::Mexc) {
                warnings.push(format!("{venue} 当前没有接入有效全仓强平价，无法执行该腿的爆仓保护；请在交易所自行监控账户风险"));
            }
        }
    }
    if opportunity.oi_capped {
        warnings
            .push("至少一条腿的场所已触及该合约的持仓量上限：只能减仓，这笔现在开不出来".into());
    }
    if leverage < requested {
        warnings.push(format!(
            "请求 {}x 超过两腿共同上限，按 {}x 算",
            requested.normalize(),
            leverage.normalize()
        ));
    }
    match risk.health {
        None if !input.margin_mode.is_cross() => {
            warnings.push("至少一条腿不公开维持保证金率，强平距离未知".into())
        }
        Some(Health::Danger) => {
            warnings.push("强平距离落在危险档（< 8%），纸面闸门会拒绝开仓".into())
        }
        _ => {}
    }
    let carry_daily_usdt = input.size_usdt * opportunity.daily_spread;
    let spread = spread_view.then(|| SpreadPlan {
        mark_basis_pct: opportunity.entry_basis_pct.map(|pct| pct.round_dp(4)),
        executable_basis_pct: opportunity.executable_basis_pct.map(|pct| pct.round_dp(4)),
        net_pct: opportunity.spread_net,
        net_usdt: input.size_usdt * opportunity.spread_net,
        target_pct: input.rules.basis_exit_pct,
        target_net_pct: input
            .rules
            .basis_exit_pct
            .map(|target| opportunity.spread_net - target / Decimal::ONE_HUNDRED),
        target_net_usdt: input.rules.basis_exit_pct.map(|target| {
            input.size_usdt * (opportunity.spread_net - target / Decimal::ONE_HUNDRED)
        }),
        break_even_target_pct: (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(4),
        estimated_from_marks,
        carry_daily_usdt,
        hold_days: opportunity.spread_hold_days,
        hold_measured: opportunity.hold_measured,
    });
    if let Some(spread) = &spread {
        if spread.estimated_from_marks {
            warnings.push(
                "至少一条腿批量接口没给买一卖一：价差按标记价估算，是上界；预览时按现拉盘口核算"
                    .into(),
            );
        }
        if let Some(reason) = spread
            .target_pct
            .and_then(|target| arb_exec::desk::spread_target_shortfall(spread.net_pct, target))
        {
            warnings.push(format!(
                "{reason}{}",
                if spread.estimated_from_marks {
                    "（按标记价估算，预览时按现拉盘口核对）"
                } else {
                    ""
                }
            ));
        } else if spread.net_usdt <= Decimal::ZERO {
            warnings.push("价差扣掉平仓穿价与往返手续费后不为正".into());
        } else if spread.carry_daily_usdt < Decimal::ZERO
            && -spread.carry_daily_usdt * spread.hold_days >= spread.net_usdt
        {
            warnings.push(format!(
                "按预计持有 {} 天，资金费支出约 {} USDT，会吃掉 {} USDT 的价差收益",
                spread.hold_days.round_dp(1),
                (-spread.carry_daily_usdt * spread.hold_days).round_dp(2),
                spread.net_usdt.round_dp(2)
            ));
        }
    } else if opportunity.funding_daily <= Decimal::ZERO {
        warnings.push(format!(
            "摊费后日化不为正：按 {} 天摊销，费差收不回往返成本",
            report.amortize_days.normalize()
        ));
    }
    if opportunity.spread_unknown {
        warnings.push("至少一条腿拿不到盘口：往返成本是下界，净收益是上界".into());
    }
    if opportunity.quote_mismatch {
        warnings.push("两腿计价资产不同，换汇成本没有计入".into());
    }

    let command = command_for(input, leverage);
    Ok(PlanView {
        margin_mode: input.margin_mode,
        symbol: input.symbol.clone(),
        size_usdt: input.size_usdt,
        requested_leverage: requested,
        leverage,
        margin_total_usdt: margin * Decimal::TWO,
        daily_gross_usdt: input.size_usdt * opportunity.daily_spread,
        daily_net_usdt: input.size_usdt * opportunity.funding_daily,
        round_trip_cost_usdt: input.size_usdt * opportunity.round_trip_cost,
        cost_is_lower_bound: opportunity.spread_unknown,
        long: long_view,
        short: short_view,
        risk,
        opportunity,
        rules: input.rules.clone(),
        rules_error,
        protection_trim_pct,
        protection_target_pct,
        gated,
        warnings,
        command,
        view: if spread_view { "spread" } else { "funding" }.into(),
        spread,
    })
}

fn command_for(input: &PlanInput, leverage: Decimal) -> String {
    let mut parts = vec![
        "arb-paper".to_string(),
        input.symbol.base.clone(),
        format!("--long {}", input.long),
        format!("--short {}", input.short),
        format!("--size {}", input.size_usdt.normalize()),
        format!("--leverage {}", leverage.normalize()),
        format!("--margin-mode {}", input.margin_mode),
    ];
    if input.view == "spread" {
        parts.push("--view spread".into());
    }
    if let Some(apr) = input.rules.min_funding_apr {
        parts.push(format!(
            "--min-funding-apr {}",
            (apr * Decimal::ONE_HUNDRED).normalize()
        ));
    }
    if let Some(pct) = input.rules.liq_protection_pct {
        parts.push(format!("--liq-protection {}", pct.normalize()));
    }
    if let Some(pct) = input.rules.size_mismatch_pct {
        parts.push(format!("--size-mismatch {}", pct.normalize()));
    }
    if let Some(pct) = input.rules.basis_exit_pct {
        parts.push(format!("--basis-exit {}", pct.normalize()));
    }
    if let Some(usdt) = input.rules.take_profit_usdt {
        parts.push(format!("--take-profit {}", usdt.normalize()));
    }
    if let Some((pct, max)) = input.rules.auto_margin() {
        parts.push(format!(
            "--auto-margin {} --auto-margin-max {}",
            pct.normalize(),
            max.normalize()
        ));
    }
    parts.join(" ")
}

#[derive(Debug, Serialize)]
pub struct PositionView {
    #[serde(flatten)]
    pub position: PairPosition,
    /// 按当前快照做的监控评估。这里只展示；执行走 [`crate::trade`] 的一轮规则或 `arb-paper --watch`。
    pub evaluation: Option<Evaluation>,
    /// 本轮快照里缺至少一条腿的行情，评估不了。
    pub quotes_missing: bool,
    /// 实盘：开仓以来两条腿实际收付的资金费（交易所结算流水）。纸面不结算资金费，为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub funding: Option<crate::trade::PositionFunding>,
    /// 已结束的仓位：从台账订单还原的双边明细（开仓、减仓、平仓的成交与手续费）。持仓中为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdown: Option<arb_exec::breakdown::PositionBreakdown>,
    /// 已结束的仓位：它的订单有没有任何成交。`false` = 什么都没发生（被拒、第一腿没吃到、准备阶段就没过），
    /// 页面据此标「没成交」、允许一键清理；持仓中为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traded: Option<bool>,
    /// 用户已经把它从「最近结束」里移除（只在「显示已隐藏」时才会列出）。
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub dismissed: bool,
}

#[derive(Debug, Serialize)]
pub struct Positions {
    pub ledger: String,
    pub broken_lines: usize,
    /// 还有敞口的仓位，最新的在前。
    pub open: Vec<PositionView>,
    /// 最近结束的仓位（已平仓 / 已回滚）里的这一页，最近结束的在前。
    pub closed: Vec<PositionView>,
    /// 列表里一共有几笔结束的（不含被移除的，除非请求了「显示已隐藏」）。
    pub closed_total: usize,
    /// 被用户移除（隐藏）的有几笔；它们仍在台账里、仍计入所有统计。
    pub dismissed_count: usize,
    /// 列表里「没成交过」、可以一键清理的有几笔（不含已移除的）。
    pub failed_count: usize,
    /// `closed` 是第几页（从 1 起；请求超出末页时是末页）、每页几笔。
    pub closed_page: usize,
    pub closed_per_page: usize,
    /// 全量台账的交易统计（笔数、净盈亏、成交额），不受分页与隐藏影响。
    pub stats: crate::stats::TradeStats,
}

/// 台账里的持仓 + 按当前快照的监控评估。
/// 每笔仓位两条腿在交易所的实际保证金状态（仓位 id → （多腿，空腿））。纸面或查不到时为空。
pub type LegStates = HashMap<String, (Option<VenueLegState>, Option<VenueLegState>)>;

/// 这些仓位里，扫描快照没有的腿所在的场所（要实时拉行情顶上，见 [`positions_page_full`]）。
pub(crate) fn venues_missing_from_scan(
    positions: &[&arb_exec::PairPosition],
    report: &ScanReport,
) -> std::collections::BTreeSet<Venue> {
    positions
        .iter()
        .filter(|position| position.status.has_exposure())
        .flat_map(|position| {
            [position.long.as_ref(), position.short.as_ref()]
                .into_iter()
                .flatten()
                .filter(|leg| leg_snapshot(report, leg.venue, &position.symbol).is_none())
                .map(|leg| leg.venue)
        })
        .collect()
}

/// 同名合约可能拆成多个身份簇；只取包含指定场所的快照。
pub(crate) fn leg_snapshot<'a>(
    report: &'a ScanReport,
    venue: Venue,
    symbol: &Symbol,
) -> Option<&'a MarketSnapshot> {
    report
        .symbols
        .iter()
        .filter(|view| &view.symbol == symbol)
        .find_map(|view| leg(view, venue))
}

#[cfg(test)]
pub fn positions(
    replayed: &Replayed,
    broken_lines: usize,
    ledger: &str,
    report: &ScanReport,
    states: &LegStates,
    funding: Option<HashMap<String, crate::trade::PositionFunding>>,
) -> Positions {
    positions_page(
        replayed,
        broken_lines,
        ledger,
        report,
        states,
        funding,
        ClosedPage::default(),
    )
}

/// [`positions`]，「最近结束」取第 `page` 页（不带「已移除」清单；线上走 [`positions_page_with`]）。
#[cfg(test)]
pub fn positions_page(
    replayed: &Replayed,
    broken_lines: usize,
    ledger: &str,
    report: &ScanReport,
    states: &LegStates,
    funding: Option<HashMap<String, crate::trade::PositionFunding>>,
    page: ClosedPage,
) -> Positions {
    positions_page_with(
        replayed,
        broken_lines,
        ledger,
        report,
        states,
        funding,
        page,
        &crate::dismissed::Dismissed::new(),
    )
}

/// [`positions_page`]，并按用户的「已移除」清单（见 [`crate::dismissed`]）隐藏「最近结束」里的仓位。
/// 移除只影响这个列表：台账、对账、当日盈亏、熔断都不看它。
#[allow(clippy::too_many_arguments)]
pub fn positions_page_with(
    replayed: &Replayed,
    broken_lines: usize,
    ledger: &str,
    report: &ScanReport,
    states: &LegStates,
    funding: Option<HashMap<String, crate::trade::PositionFunding>>,
    page: ClosedPage,
    dismissed: &crate::dismissed::Dismissed,
) -> Positions {
    positions_page_full(
        replayed,
        broken_lines,
        ledger,
        report,
        &HashMap::new(),
        states,
        funding,
        page,
        dismissed,
    )
}

/// [`positions_page_with`]；`fallback` 是扫描快照里**没有**的腿的实时行情（场所、合约 → 快照）。
///
/// 价差扫描只收「同一资产、参考价相差 5% 以内」的行，一条腿的价格偏离其它场所超过 5% 就被排除在快照外
/// （某次实盘：HL xyz 43.5、CEX 45.8），持仓页对这笔就评估不了：标记价、浮动盈亏、强平距离、每日资金费
/// 全空。持仓是真实在手的，不能因为扫描把它当成别的资产就没数据：扫描里找不到的腿用实时行情顶上。
#[allow(clippy::too_many_arguments)]
pub fn positions_page_full(
    replayed: &Replayed,
    broken_lines: usize,
    ledger: &str,
    report: &ScanReport,
    fallback: &HashMap<(Venue, Symbol), MarketSnapshot>,
    states: &LegStates,
    mut funding: Option<HashMap<String, crate::trade::PositionFunding>>,
    page: ClosedPage,
    dismissed: &crate::dismissed::Dismissed,
) -> Positions {
    let mut open = Vec::new();
    let mut closed = Vec::new();
    let (mut dismissed_count, mut failed_count) = (0usize, 0usize);
    for position in replayed.positions.values() {
        if position.status.has_exposure() {
            // `None` 是纸面（不结算，按 0）；实盘映射里没查到的仍是未知。
            let live = funding.is_some();
            let position_funding = funding
                .as_mut()
                .and_then(|funding| funding.remove(&position.id));
            let funding_usdt = if live {
                position_funding
                    .as_ref()
                    .and_then(|funding| funding.total_usdt)
            } else {
                Some(Decimal::ZERO)
            };
            let quotes =
                position
                    .long
                    .as_ref()
                    .zip(position.short.as_ref())
                    .map(|(long, short)| {
                        let find = |venue: Venue| {
                            leg_snapshot(report, venue, &position.symbol)
                                .or_else(|| fallback.get(&(venue, position.symbol.clone())))
                        };
                        (find(long.venue), find(short.venue))
                    });
            let (evaluation, quotes_missing) = match quotes {
                Some((Some(long), Some(short))) => {
                    let (long_state, short_state) = states
                        .get(&position.id)
                        .map_or((None, None), |(l, s)| (l.as_ref(), s.as_ref()));
                    (
                        evaluate_full(
                            position,
                            long,
                            short,
                            Inputs {
                                long_state,
                                short_state,
                                funding_usdt,
                                ..Inputs::default()
                            },
                        ),
                        false,
                    )
                }
                Some(_) => (None, true),
                None => (None, false),
            };
            open.push(PositionView {
                position: position.clone(),
                evaluation,
                quotes_missing,
                funding: position_funding,
                breakdown: None,
                traded: None,
                dismissed: false,
            });
        } else {
            let hidden = dismissed.contains_key(&position.id);
            if hidden {
                dismissed_count += 1;
                if !page.show_dismissed {
                    continue;
                }
            }
            let traded = crate::dismissed::traded(position, &replayed.orders);
            if !traded && !hidden {
                failed_count += 1;
            }
            closed.push(PositionView {
                position: position.clone(),
                evaluation: None,
                quotes_missing: false,
                funding: None,
                breakdown: None,
                traded: Some(traded),
                dismissed: hidden,
            });
        }
    }
    open.sort_by_key(|view| std::cmp::Reverse(view.position.opened_at));
    let ended = |view: &PositionView| -> DateTime<Utc> {
        view.position.closed_at.unwrap_or(view.position.opened_at)
    };
    closed.sort_by_key(|view| std::cmp::Reverse(ended(view)));
    let closed_total = closed.len();
    let last_page = closed_total.div_ceil(page.per_page).max(1);
    let closed_page = page.page.min(last_page);
    let mut closed: Vec<PositionView> = closed
        .into_iter()
        .skip((closed_page - 1) * page.per_page)
        .take(page.per_page)
        .collect();
    // 只给要展示的那一页还原明细：台账里结束的仓位只会越来越多。
    for view in &mut closed {
        view.breakdown = arb_exec::breakdown::position_breakdown(&view.position, &replayed.orders);
    }
    Positions {
        ledger: ledger.to_string(),
        broken_lines,
        open,
        closed,
        closed_total,
        dismissed_count,
        failed_count,
        closed_page,
        closed_per_page: page.per_page,
        stats: crate::stats::trade_stats_excluding(
            replayed,
            Utc::now(),
            &crate::stats::load_excluded(ledger),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_exec::{ClientOrderId, LegFill, PositionStatus, Strategy};
    use arb_scanner::{ExcludedReading, Totals, VenueReport};
    use rust_decimal_macros::dec;

    fn snapshot(
        venue: Venue,
        base: &str,
        period_rate: Decimal,
        mmr: Option<Decimal>,
    ) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: Symbol::perp(base, "USDT"),
            period_rate,
            interval_h: 1,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: true,
            taker_fee: Some(Decimal::ZERO),
            mark_price: Some(dec!(100)),
            index_price: Some(dec!(100)),
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: Some(dec!(20)),
            maintenance_margin: mmr,
            oi_capped: false,
        }
    }

    fn report(views: Vec<Vec<MarketSnapshot>>) -> ScanReport {
        ScanReport {
            generated_at: Utc::now(),
            fee_per_side: dec!(0.0005),
            amortize_days: dec!(7),
            spread_hold_days: dec!(3),
            max_entry_basis_pct: Some(dec!(0.5)),
            min_venues: 2,
            venues: [Venue::Hyperliquid, Venue::Lighter, Venue::Binance]
                .into_iter()
                .map(|venue| VenueReport {
                    venue,
                    ok: true,
                    rates: 1,
                    error: None,
                    elapsed_ms: 1,
                    reused_age_ms: None,
                })
                .collect(),
            symbols: views
                .into_iter()
                .map(|rates| SymbolView {
                    symbol: rates[0].symbol.clone(),
                    rates,
                    funding: Vec::new(),
                    spread: Vec::new(),
                })
                .collect(),
            unverified: Vec::new(),
            suspicious: Vec::new(),
            gated: Vec::new(),
            totals: Totals::default(),
        }
    }

    /// 价差：Lighter 便宜（100）、Hyperliquid 贵（102），都没有买一卖一（DEX）。
    /// 费率上 Lighter 更高：按价格方向做（多 Lighter / 空 Hyperliquid）要付资金费。
    fn spread_pair() -> ScanReport {
        let mut cheap = snapshot(Venue::Lighter, "ANTHROPIC", dec!(0.00003), Some(dec!(0.01)));
        cheap.mark_price = Some(dec!(100));
        let mut rich = snapshot(
            Venue::Hyperliquid,
            "ANTHROPIC",
            dec!(0.00001),
            Some(dec!(0.01)),
        );
        rich.mark_price = Some(dec!(102));
        report(vec![vec![rich, cheap]])
    }

    fn spread_input(basis_exit: Option<Decimal>) -> PlanInput {
        PlanInput {
            margin_mode: arb_exec::MarginMode::Isolated,
            symbol: Symbol::perp("ANTHROPIC", "USDT"),
            long: Venue::Lighter,
            short: Venue::Hyperliquid,
            size_usdt: dec!(1000),
            leverage: dec!(2),
            view: "spread".into(),
            rules: TaskRules {
                basis_exit_pct: basis_exit,
                ..TaskRules::default()
            },
        }
    }

    #[test]
    fn cross_plan_has_no_isolated_liquidation_or_trim_and_keeps_mode_in_cli() {
        let mut input = spread_input(Some(dec!(0.1)));
        input.margin_mode = arb_exec::MarginMode::Cross;
        input.rules.liq_protection_pct = Some(dec!(10));
        let view = plan(&spread_pair(), &input).unwrap();
        assert_eq!(view.margin_mode, arb_exec::MarginMode::Cross);
        assert_eq!(view.rules_error, None);
        assert_eq!(view.risk.liq_distance_pct, None);
        assert_eq!(view.risk.health, None);
        assert_eq!(view.long.liquidation_price, None);
        assert_eq!(view.short.liquidation_price, None);
        assert_eq!(view.protection_trim_pct, None);
        assert_eq!(view.protection_target_pct, None);
        assert!(view.command.contains("--margin-mode cross"));
        assert!(view.warnings.iter().any(|w| w.contains("全仓")));
        input.rules.auto_margin_pct = Some(dec!(15));
        input.rules.auto_margin_max_usdt = Some(dec!(100));
        assert!(
            plan(&spread_pair(), &input)
                .unwrap()
                .rules_error
                .unwrap()
                .contains("全仓")
        );
    }

    #[test]
    fn the_spread_board_goes_long_the_cheap_leg_without_the_basis_gate() {
        let board = pair_board(&spread_pair(), Venue::Hyperliquid, Venue::Lighter, "spread");
        assert_eq!(board.view, "spread");
        let row = &board.rows[0];
        assert_eq!(
            (row.long, row.short),
            (Some(Venue::Lighter), Some(Venue::Hyperliquid))
        );
        // (102 − 100) / 101 = 1.9802%
        assert_eq!(row.basis_pct, Some(dec!(1.9802)));
        assert!(
            row.carry_apr.unwrap() < Decimal::ZERO,
            "按价格方向持有要付资金费"
        );
        assert_eq!(row.gated, None);
        // 同一对腿在资金费视角里方向相反（按费率：多 Hyperliquid / 空 Lighter）。
        let funding = pair_board(
            &spread_pair(),
            Venue::Hyperliquid,
            Venue::Lighter,
            "funding",
        );
        assert_eq!(funding.rows[0].long, Some(Venue::Hyperliquid));
    }

    #[test]
    fn a_spread_plan_estimates_dex_legs_from_marks_and_says_so() {
        let view = plan(&spread_pair(), &spread_input(Some(dec!(0.1)))).unwrap();
        let spread = view.spread.as_ref().unwrap();
        assert!(spread.estimated_from_marks);
        assert_eq!(spread.executable_basis_pct, Some(dec!(1.9802)));
        assert!(spread.net_usdt > Decimal::ZERO);
        assert!(spread.carry_daily_usdt < Decimal::ZERO);
        assert!(view.warnings.iter().any(|w| w.contains("上界")));
        assert_eq!(view.gated, None);
        assert_eq!(view.rules_error, None);
        // 反方向：多贵的、空便宜的，价差方向不成立。
        let mut reversed = spread_input(None);
        (reversed.long, reversed.short) = (Venue::Hyperliquid, Venue::Lighter);
        assert!(plan_err(&spread_pair(), &reversed).contains("价差方向不成立"));
        // 收敛目标不低于当前基差：开仓就会触发。
        let eager = plan(&spread_pair(), &spread_input(Some(dec!(2)))).unwrap();
        assert!(eager.rules_error.unwrap().contains("开仓就会触发"));
    }

    fn plan_err(report: &ScanReport, input: &PlanInput) -> String {
        match plan(report, input) {
            Ok(_) => panic!("应当拒绝"),
            Err(error) => error,
        }
    }

    fn two_symbols() -> ScanReport {
        report(vec![
            vec![
                snapshot(Venue::Hyperliquid, "BTC", dec!(0.00002), Some(dec!(0.0125))),
                snapshot(Venue::Lighter, "BTC", dec!(0.00001), Some(dec!(0.012))),
            ],
            vec![
                snapshot(Venue::Hyperliquid, "ETH", dec!(0.00001), Some(dec!(0.02))),
                snapshot(Venue::Lighter, "ETH", dec!(0.00005), Some(dec!(0.012))),
                snapshot(Venue::Binance, "ETH", dec!(0.0001), None),
            ],
        ])
    }

    #[test]
    fn the_pair_board_shorts_the_higher_rate_and_ranks_by_net_apr() {
        let board = pair_board(
            &two_symbols(),
            Venue::Hyperliquid,
            Venue::Lighter,
            "funding",
        );
        assert_eq!(board.total, 2);
        let eth = &board.rows[0];
        assert_eq!(eth.symbol.base, "ETH", "ETH 的费差更大，排第一");
        assert_eq!(eth.long, Some(Venue::Hyperliquid));
        assert_eq!(eth.short, Some(Venue::Lighter));
        assert!(eth.opportunity.as_ref().unwrap().funding_apr > Decimal::ZERO);
        // 每小时 0.00004 的费差 → 毛年化 35.04%
        assert_eq!(eth.gross_apr, dec!(0.3504));
        let btc = &board.rows[1];
        assert_eq!(btc.long, Some(Venue::Lighter), "BTC 反过来：Lighter 更便宜");
    }

    #[test]
    fn an_excluded_reading_is_shown_but_not_paired() {
        let mut report = two_symbols();
        report.suspicious.push(ExcludedReading {
            venue: Venue::Lighter,
            symbol: Symbol::perp("ETH", "USDT"),
            reason: "离群".into(),
        });
        let board = pair_board(&report, Venue::Hyperliquid, Venue::Lighter, "funding");
        let eth = board
            .rows
            .iter()
            .find(|row| row.symbol.base == "ETH")
            .unwrap();
        assert_eq!(eth.excluded.as_deref(), Some("离群"));
        assert!(eth.opportunity.is_none());
        let input = PlanInput {
            margin_mode: arb_exec::MarginMode::Isolated,
            symbol: Symbol::perp("ETH", "USDT"),
            long: Venue::Hyperliquid,
            short: Venue::Lighter,
            size_usdt: dec!(1000),
            leverage: dec!(3),
            view: "funding".into(),
            rules: TaskRules::default(),
        };
        assert!(plan(&report, &input).unwrap_err().contains("排除"));
    }

    #[test]
    fn a_plan_caps_leverage_and_prices_both_legs() {
        let mut report = two_symbols();
        report.symbols[1].rates[0].max_leverage = Some(dec!(5));
        let input = PlanInput {
            margin_mode: arb_exec::MarginMode::Isolated,
            symbol: Symbol::perp("ETH", "USDT"),
            long: Venue::Hyperliquid,
            short: Venue::Lighter,
            size_usdt: dec!(1000),
            leverage: dec!(10),
            view: "funding".into(),
            rules: TaskRules {
                min_funding_apr: Some(dec!(0.05)),
                liq_protection_pct: Some(dec!(10)),
                size_mismatch_pct: Some(dec!(1)),
                basis_exit_pct: None,
                ..TaskRules::default()
            },
        };
        let view = plan(&report, &input).unwrap();
        assert_eq!(view.leverage, dec!(5), "hyperliquid 这条腿最多 5 倍");
        assert_eq!(view.long.margin_usdt, dec!(200));
        assert_eq!(view.margin_total_usdt, dec!(400));
        assert!(view.long.liquidation_price.unwrap() < dec!(100));
        assert!(view.short.liquidation_price.unwrap() > dec!(100));
        // 单腿 1000 × 每日 0.00096 = 0.96 美元
        assert_eq!(view.daily_gross_usdt, dec!(0.96));
        assert!(view.rules_error.is_none(), "{:?}", view.rules_error);
        assert_eq!(view.protection_target_pct, Some(dec!(15)));
        assert!(view.protection_trim_pct.unwrap() > Decimal::ZERO);
        assert!(view.warnings.iter().any(|w| w.contains("共同上限")));
    }

    #[test]
    fn a_plan_refuses_the_wrong_direction_and_flags_rules_that_cannot_run() {
        let report = two_symbols();
        let wrong = PlanInput {
            margin_mode: arb_exec::MarginMode::Isolated,
            symbol: Symbol::perp("ETH", "USDT"),
            long: Venue::Lighter,
            short: Venue::Hyperliquid,
            size_usdt: dec!(1000),
            leverage: dec!(3),
            view: "funding".into(),
            rules: TaskRules::default(),
        };
        assert!(plan(&report, &wrong).unwrap_err().contains("反过来"));

        let unknown = PlanInput {
            margin_mode: arb_exec::MarginMode::Isolated,
            symbol: Symbol::perp("ETH", "USDT"),
            long: Venue::Hyperliquid,
            short: Venue::Binance,
            size_usdt: dec!(1000),
            leverage: dec!(3),
            view: "funding".into(),
            rules: TaskRules {
                liq_protection_pct: Some(dec!(10)),
                ..TaskRules::default()
            },
        };
        let view = plan(&report, &unknown).unwrap();
        assert!(view.risk.liq_distance_pct.is_none());
        assert!(
            view.rules_error.is_some(),
            "Binance 不公开维持保证金率，保护无从执行"
        );
        assert!(view.warnings.iter().any(|w| w.contains("强平距离未知")));
    }

    #[test]
    fn open_positions_come_with_an_evaluation_and_closed_ones_are_listed_newest_first() {
        let report = two_symbols();
        let leg = |venue, side| LegFill {
            venue,
            side,
            notional_usdt: dec!(1000),
            average_price: dec!(100),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("p", side, 0),
            margin_usdt: Some(dec!(200)),
        };
        let position = |id: &str, status| PairPosition {
            margin_mode: arb_exec::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("ETH", "USDT"),
            strategy: Strategy::Funding,
            long: Some(leg(Venue::Hyperliquid, Side::Buy)),
            short: Some(leg(Venue::Lighter, Side::Sell)),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        let mut replayed = Replayed::default();
        replayed
            .positions
            .insert("a".into(), position("a", PositionStatus::Open));
        replayed
            .positions
            .insert("b".into(), position("b", PositionStatus::Closed));
        let mut orphan = position("c", PositionStatus::Open);
        orphan.symbol = Symbol::perp("SOL", "USDT");
        replayed.positions.insert("c".into(), orphan);
        // 已结束那笔的平仓单：持仓页据此还原双边明细。
        replayed.orders.insert(
            "b-sell-exit1".into(),
            serde_json::from_value(serde_json::json!({
                "order": {
                    "client_order_id": "b-sell-exit1", "venue": "hyperliquid", "symbol": "ETH/USDT",
                    "side": "sell", "notional_usdt": "1000", "quantity": "10", "limit_price": null,
                    "reduce_only": true, "margin_mode": "isolated", "leverage": null
                },
                "venue_order_id": "1", "status": "filled", "filled_usdt": "1010",
                "average_price": "101", "fee_usdt": "0.5", "reject_reason": null
            }))
            .unwrap(),
        );

        let view = positions(
            &replayed,
            0,
            "ledger.jsonl",
            &report,
            &LegStates::new(),
            None,
        );
        assert_eq!(view.open.len(), 2);
        assert_eq!(view.closed.len(), 1);
        let eth = view.open.iter().find(|row| row.position.id == "a").unwrap();
        assert!(eth.evaluation.is_some());
        let sol = view.open.iter().find(|row| row.position.id == "c").unwrap();
        assert!(sol.quotes_missing, "快照里没有 SOL：评估不了要说出来");
        let json = serde_json::to_value(eth).unwrap();
        assert_eq!(json["id"], "a", "仓位字段平铺在外层");
        assert!(json["evaluation"]["observation"]["funding_apr"].is_string());
        assert!(json.get("breakdown").is_none(), "持仓中的仓位不带明细");
        let closed = serde_json::to_value(&view.closed[0]).unwrap();
        let exits = &closed["breakdown"]["long"]["exits"];
        assert_eq!(exits.as_array().unwrap().len(), 1);
        assert_eq!(
            exits[0]["role"],
            serde_json::json!({"kind": "exit", "n": 1})
        );
        assert_eq!(closed["breakdown"]["long"]["venue"], "hyperliquid");
    }

    /// 某次实盘：扫描快照因价格偏离 >5% 把一条腿排除在外时，持仓页用实时行情顶上，
    /// 不再整笔评估不了（标记价、浮动盈亏、强平距离、每日资金费全空）。没有顶上的行情才如实说缺行情。
    #[test]
    fn a_leg_the_scan_dropped_is_evaluated_from_live_quotes_instead_of_going_blank() {
        let report = two_symbols();
        let leg = |venue, side| LegFill {
            venue,
            side,
            notional_usdt: dec!(1000),
            average_price: dec!(100),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("g", side, 0),
            margin_usdt: Some(dec!(200)),
        };
        let mut replayed = Replayed::default();
        replayed.positions.insert(
            "g".into(),
            PairPosition {
                margin_mode: arb_exec::MarginMode::Isolated,
                id: "g".into(),
                symbol: Symbol::perp("SOL", "USDT"),
                strategy: Strategy::Spread,
                long: Some(leg(Venue::Hyperliquid, Side::Buy)),
                short: Some(leg(Venue::Lighter, Side::Sell)),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status: PositionStatus::Open,
                opened_at: Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(5)),
                rules: TaskRules::default(),
                trims: 0,
                exits: 0,
                margin_added_usdt: Decimal::ZERO,
                realized_pnl_usdt: Decimal::ZERO,
                realized_fee_usdt: Decimal::ZERO,
                realized_source: None,
                realized_funding_usdt: None,
                funding_checked_at: None,
                closed_externally: false,
                entry_legs: None,
                pnl_unattributed: None,
                open_report: None,
                planned: None,
            },
        );
        let exposed = replayed.exposed();
        // 扫描里没有 SOL：两条腿所在的场所都要实时拉。
        let missing = venues_missing_from_scan(&exposed, &report);
        assert_eq!(
            missing.into_iter().collect::<Vec<_>>(),
            vec![Venue::Hyperliquid, Venue::Lighter]
        );
        let page = |fallback: &HashMap<(Venue, Symbol), MarketSnapshot>| {
            positions_page_full(
                &replayed,
                0,
                "ledger.jsonl",
                &report,
                fallback,
                &LegStates::new(),
                None,
                ClosedPage::default(),
                &crate::dismissed::Dismissed::new(),
            )
        };
        let blank = page(&HashMap::new());
        assert!(blank.open[0].quotes_missing && blank.open[0].evaluation.is_none());

        let live: HashMap<(Venue, Symbol), MarketSnapshot> = [Venue::Hyperliquid, Venue::Lighter]
            .into_iter()
            .map(|venue| {
                (
                    (venue, Symbol::perp("SOL", "USDT")),
                    snapshot(venue, "SOL", dec!(0.0001), Some(dec!(0.02))),
                )
            })
            .collect();
        let filled = page(&live);
        assert!(!filled.open[0].quotes_missing);
        let observation = &filled.open[0].evaluation.as_ref().unwrap().observation;
        assert!(observation.pnl.funding_daily_usdt.is_some());
        // 只补缺的：快照里有的腿（ETH）不需要实时拉。
        let mut with_eth = replayed.clone();
        let mut eth = with_eth.positions["g"].clone();
        eth.id = "e".into();
        eth.symbol = Symbol::perp("ETH", "USDT");
        with_eth.positions.insert("e".into(), eth);
        let exposed = with_eth.exposed();
        let missing = venues_missing_from_scan(&exposed, &report);
        assert!(missing.contains(&Venue::Hyperliquid) && missing.contains(&Venue::Lighter));
        let only_eth: Vec<_> = exposed
            .iter()
            .filter(|position| position.symbol == Symbol::perp("ETH", "USDT"))
            .copied()
            .collect();
        assert!(venues_missing_from_scan(&only_eth, &report).is_empty());
    }

    /// 「最近结束」的移除：默认不列出被移除的，页码与总数只数列出来的；带 `show_dismissed` 才列出并标记；
    /// 没成交的标 `traded: false` 并计入可一键清理的数；持仓中的永远不受影响，也不带这两个字段。
    #[test]
    fn dismissed_positions_leave_the_closed_list_but_are_counted_and_can_be_shown_again() {
        use crate::dismissed::{Dismissed, Entry};
        let report = two_symbols();
        let mut replayed = Replayed::default();
        let start = Utc::now();
        let make = |id: &str, status: &str, minutes: i64| -> PairPosition {
            serde_json::from_value(serde_json::json!({
                "id": id, "symbol": "ETH/USDT", "strategy": "spread", "long": null, "short": null,
                "entry_basis_pct": "0", "expected_round_trip_cost": "0", "status": status,
                "opened_at": start, "closed_at": start + chrono::Duration::minutes(minutes),
                "note": null, "leverage": "3", "rules": {}, "trims": 0, "exits": 0
            }))
            .unwrap()
        };
        // fail1/fail2：没成交；real：开仓成交过；still：持仓中。
        for (id, status, minutes) in [
            ("fail1", "unwound", 1),
            ("fail2", "unwound", 2),
            ("real", "closed", 3),
            ("still", "open", 4),
        ] {
            replayed
                .positions
                .insert(id.into(), make(id, status, minutes));
        }
        replayed.orders.insert(
            "real-buy-0".into(),
            serde_json::from_value(serde_json::json!({
                "order": {
                    "client_order_id": "real-buy-0", "venue": "hyperliquid", "symbol": "ETH/USDT",
                    "side": "buy", "notional_usdt": "1000", "quantity": "10", "limit_price": null,
                    "reduce_only": false, "margin_mode": "isolated", "leverage": null
                },
                "venue_order_id": "1", "status": "filled", "filled_usdt": "1000",
                "average_price": "100", "fee_usdt": "0.5", "reject_reason": null
            }))
            .unwrap(),
        );
        let page = |show: bool, dismissed: &Dismissed| {
            positions_page_with(
                &replayed,
                0,
                "ledger.jsonl",
                &report,
                &LegStates::new(),
                None,
                ClosedPage::default().with_dismissed(show),
                dismissed,
            )
        };
        let none = Dismissed::new();
        let before = page(false, &none);
        assert_eq!(before.closed_total, 3);
        assert_eq!((before.dismissed_count, before.failed_count), (0, 2));
        let traded: Vec<(&str, Option<bool>)> = before
            .closed
            .iter()
            .map(|view| (view.position.id.as_str(), view.traded))
            .collect();
        assert_eq!(
            traded,
            vec![
                ("real", Some(true)),
                ("fail2", Some(false)),
                ("fail1", Some(false))
            ],
            "最近结束的在前，每笔标了有没有成交"
        );
        assert_eq!(before.open[0].traded, None, "持仓中的不带");

        let mut hidden = Dismissed::new();
        hidden.insert(
            "fail1".into(),
            Entry {
                at: Utc::now(),
                symbol: "ETH/USDT".into(),
                status: "unwound".into(),
                traded: false,
            },
        );
        let after = page(false, &hidden);
        assert_eq!(after.closed_total, 2, "被移除的不再算在列表里");
        assert_eq!((after.dismissed_count, after.failed_count), (1, 1));
        assert!(after.closed.iter().all(|view| view.position.id != "fail1"));
        assert_eq!(after.open.len(), 1, "持仓中的不受影响");

        let shown = page(true, &hidden);
        assert_eq!(shown.closed_total, 3);
        let flagged: Vec<(&str, bool)> = shown
            .closed
            .iter()
            .map(|view| (view.position.id.as_str(), view.dismissed))
            .collect();
        assert_eq!(
            flagged,
            vec![("real", false), ("fail2", false), ("fail1", true)]
        );
        // 已移除的不再算作「可一键清理」。
        assert_eq!(shown.failed_count, 1);
        // 没有移除过的字段不进 JSON（旧页面不受影响）。
        let json = serde_json::to_value(&shown.closed[0]).unwrap();
        assert!(json.get("dismissed").is_none());
        assert_eq!(json["traded"], true);
        assert_eq!(
            serde_json::to_value(&shown.closed[2]).unwrap()["dismissed"],
            true
        );
    }

    #[test]
    fn closed_positions_are_paged_newest_first_and_a_page_past_the_end_shows_the_last() {
        let report = two_symbols();
        let mut replayed = Replayed::default();
        let start = Utc::now();
        for n in 0..23i64 {
            let id = format!("p{n:02}");
            let position: PairPosition = serde_json::from_value(serde_json::json!({
                "id": id, "symbol": "ETH/USDT", "strategy": "spread", "long": null, "short": null,
                "entry_basis_pct": "0", "expected_round_trip_cost": "0", "status": "closed",
                "opened_at": start, "closed_at": start + chrono::Duration::minutes(n),
                "note": null, "leverage": "3", "rules": {}, "trims": 0, "exits": 0
            }))
            .unwrap();
            replayed.positions.insert(id, position);
        }
        let page = |page, per_page| {
            positions_page(
                &replayed,
                0,
                "ledger.jsonl",
                &report,
                &LegStates::new(),
                None,
                ClosedPage::from_query(page, per_page),
            )
        };
        let ids = |view: &Positions| {
            view.closed
                .iter()
                .map(|row| row.position.id.clone())
                .collect::<Vec<_>>()
        };

        let first = page(None, Some(10));
        assert_eq!(
            (first.closed_total, first.closed_page, first.closed_per_page),
            (23, 1, 10)
        );
        assert_eq!(
            ids(&first).first().map(String::as_str),
            Some("p22"),
            "最近结束的在前"
        );
        assert_eq!(ids(&first).len(), 10);
        let third = page(Some(3), Some(10));
        assert_eq!(ids(&third), ["p02", "p01", "p00"]);
        let past = page(Some(9), Some(10));
        assert_eq!(past.closed_page, 3, "超出末页按末页给");
        assert_eq!(ids(&past), ids(&third));

        let default = page(None, None);
        assert_eq!(
            default.closed.len(),
            MAX_CLOSED_POSITIONS,
            "不带参数和以前一样给 20 笔"
        );
        assert_eq!(
            page(Some(0), Some(0)).closed_per_page,
            1,
            "页码、每页至少 1"
        );
        assert_eq!(
            page(None, Some(10_000)).closed_per_page,
            MAX_CLOSED_PER_PAGE
        );

        let empty = positions_page(
            &Replayed::default(),
            0,
            "ledger.jsonl",
            &report,
            &LegStates::new(),
            None,
            ClosedPage::from_query(Some(4), Some(10)),
        );
        assert_eq!((empty.closed_total, empty.closed_page), (0, 1));
        assert!(empty.closed.is_empty());
    }
}
