//! `arb-paper` 与 `arb-live` 共用的命令行辅助：候选挑选、风险与盘口体检、持仓监控与打印。
//!
//! 两个命令只在「券商是谁」上不同；排名、闸门与规则评估必须是同一份代码，
//! 否则纸面验证过的行为和实盘行为会悄悄分叉。

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::Result;
use arb_core::{Decimal, MarketSnapshot, Settings, Symbol, Venue, money::to_pct};
use arb_scanner::leverage::CAUTION_DISTANCE_PCT;
use arb_scanner::{LegRisk, Opportunity, PairRisk, ScanReport, pair_risk};
use arb_venues::VenueApi;
use rust_decimal::prelude::FromPrimitive;
use tokio::task::JoinSet;
use tracing::warn;

use crate::{Action, Evaluation, Executor, Limits, PairPosition, Rejection, TaskRules};

pub async fn monitor_one(
    executor: &Executor,
    position: &mut PairPosition,
    snapshots: &HashMap<(Venue, Symbol), MarketSnapshot>,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ctx: &crate::desk::RoundCtx<'_>,
) {
    let (Some(long_leg), Some(short_leg)) = (position.long.clone(), position.short.clone()) else {
        return;
    };
    println!(
        "\n  [{}] {} 多 {} / 空 {}，杠杆 {}；规则：{}",
        position.id,
        position.symbol,
        long_leg.venue,
        short_leg.venue,
        position
            .leverage
            .map_or("未记录".into(), |leverage| format!(
                "{}x",
                leverage.round_dp(2)
            )),
        describe_rules(&position.rules)
    );
    let report = crate::desk::monitor_position(executor, position, snapshots, by_venue, ctx).await;
    print_monitor_report(&report);
}

/// 打印一轮监控里一笔仓位的结果。评估与执行在 [`crate::desk::monitor_position`]。
pub fn print_monitor_report(report: &crate::desk::MonitorReport) {
    if let Some(evaluation) = &report.evaluation {
        print_evaluation(evaluation);
        match &evaluation.action {
            Action::Hold => println!("    动作：保持"),
            Action::Close { reason } => println!("    动作：平仓 —— {reason}"),
            Action::Trim { fraction, reason } => println!(
                "    动作：两腿各减仓 {}% —— {reason}",
                to_pct(*fraction).round_dp(2)
            ),
            Action::AddMargin {
                venue,
                amount_usdt,
                reason,
                ..
            } => println!("    动作：往 {venue} 补保证金 {amount_usdt} USDT —— {reason}"),
        }
    }
    if let Some(skipped) = &report.skipped {
        println!("    ⚠ {skipped}");
    }
    if let Some(attention) = &report.attention {
        println!("    ⚠ {attention}");
    }
    if report.executed {
        match &report.error {
            None => println!(
                "    结果：{:?}{}",
                report.status,
                report
                    .note
                    .as_deref()
                    .map(|note| format!("（{note}）"))
                    .unwrap_or_default()
            ),
            Some(error) => println!("    ⚠ 执行失败：{error}"),
        }
    }
}

pub fn print_evaluation(evaluation: &Evaluation) {
    let observation = &evaluation.observation;
    println!(
        "    当前费差年化 {}%；两腿数量偏差 {}",
        to_pct(observation.funding_apr).round_dp(2),
        observation
            .size_mismatch_pct
            .map_or("—".into(), |pct| format!("{}%", pct.round_dp(3)))
    );
    if let Some(net) = observation.net_with_funding_usdt {
        println!(
            "    含资金费的净额（按标记价）{} USDT（其中资金费 {}）",
            net.round_dp(4),
            observation
                .funding_usdt
                .map_or("—".into(), |funding| funding.round_dp(4).to_string())
        );
    }
    for (label, leg) in [("多腿", &observation.long), ("空腿", &observation.short)] {
        println!(
            "    {label} {}：标记 {}，强平 {}，距离 {}",
            leg.venue,
            leg.mark_price.map_or("—".into(), |v| v.to_string()),
            leg.liquidation_price
                .map_or("—".into(), |v| v.round_dp(6).to_string()),
            match (leg.distance_pct, leg.health) {
                (Some(distance), Some(health)) =>
                    format!("{}%（{}）", distance.round_dp(2), health_label(health)),
                _ => "未知（缺保证金或维持保证金率）".into(),
            }
        );
    }
    for note in &evaluation.skipped {
        println!("    ⚠ 未评估：{note}");
    }
}

/// 只拉持仓涉及的场所。单家失败只让那家的持仓本轮跳过，并打出原因。
pub async fn fetch_snapshots(
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    venues: &BTreeSet<Venue>,
) -> HashMap<(Venue, Symbol), MarketSnapshot> {
    let mut set = JoinSet::new();
    for venue in venues {
        match by_venue.get(venue) {
            Some(api) => {
                let api = Arc::clone(api);
                set.spawn(async move { (api.venue(), api.fetch_all().await) });
            }
            None => println!("  ⚠ {venue} 不在 ARB_VENUES 里，它的持仓无法监控"),
        }
    }
    let mut out = HashMap::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_, Ok(rows))) => {
                for row in rows {
                    out.insert((row.venue, row.symbol.clone()), row);
                }
            }
            Ok((venue, Err(error))) => println!("  ⚠ {venue} 取数失败：{error}"),
            Err(error) => warn!(%error, "场所任务异常退出"),
        }
    }
    out
}

/// 用本轮扫描里两条腿的快照算风险。找不到快照时返回 `None`。
pub fn risk_of(
    report: &ScanReport,
    opportunity: &Opportunity,
    leverage: Decimal,
) -> Option<PairRisk> {
    // 同名合约可能被身份判定拆成几个簇（比如 OPENAI 的 DEX 簇与 CEX 簇价差 5%）：
    // 要找同时含这两条腿的那个，不能取第一个同名的。
    let has =
        |view: &&arb_scanner::SymbolView, venue| view.rates.iter().any(|rate| rate.venue == venue);
    let view = report.symbols.iter().find(|view| {
        view.symbol == opportunity.symbol
            && has(view, opportunity.long)
            && has(view, opportunity.short)
    })?;
    let long = view
        .rates
        .iter()
        .find(|rate| rate.venue == opportunity.long)?;
    let short = view
        .rates
        .iter()
        .find(|rate| rate.venue == opportunity.short)?;
    Some(pair_risk(opportunity, long, short, leverage))
}

pub fn print_risk(risk: &PairRisk) {
    let leg = |label: &str, leg: &LegRisk| {
        format!(
            "{label} {} {}x{}（上限 {}）强平距离 {}",
            leg.venue,
            leg.leverage.round_dp(2),
            if leg.leverage_capped {
                "·已压到上限"
            } else {
                ""
            },
            leg.max_leverage
                .map_or("未知".into(), |max| format!("{}x", max.round_dp(2))),
            leg.liq_distance_pct
                .map_or("未知".into(), |pct| format!("{}%", pct.round_dp(2)))
        )
    };
    println!(
        "  杠杆：{}；{}",
        leg("多腿", &risk.long),
        leg("空腿", &risk.short)
    );
    println!(
        "  保证金年化 {}%（净年化 × 名义 ÷ 两腿保证金）；健康度 {}",
        to_pct(risk.margin_apr).round_dp(2),
        risk.health.map_or("未知", health_label)
    );
}

pub fn describe_rules(rules: &TaskRules) -> String {
    if rules.is_empty() {
        return "无".into();
    }
    let mut parts = Vec::new();
    if let Some(apr) = rules.min_funding_apr {
        parts.push(format!("费差 < {}% 平仓", to_pct(apr).round_dp(2)));
    }
    if let Some(pct) = rules.liq_protection_pct {
        parts.push(format!("强平距离 < {pct}% 减仓"));
    }
    if let Some(pct) = rules.size_mismatch_pct {
        parts.push(format!("数量偏差 > {pct}% 平仓"));
    }
    if let Some(pct) = rules.basis_exit_pct {
        parts.push(format!("基差收敛到 ≤ {pct}% 平仓"));
    }
    if let Some(target) = rules.take_profit_usdt {
        parts.push(format!("含资金费净盈利 ≥ {target} USDT 止盈"));
        match rules.take_profit_book_min_usdt {
            Some(min) if min.is_zero() => parts.push("止盈触发后盘口为正即平".into()),
            Some(min) => parts.push(format!("止盈触发后盘口 ≥ {min} USDT 即平")),
            None => {}
        }
    }
    if let Some((trigger, cap)) = rules.auto_margin() {
        parts.push(format!(
            "强平距离 < {trigger}% 补逐仓保证金（累计上限 {cap} USDT）"
        ));
    }
    if let Some((hours, loss)) = rules.timeout_exit() {
        parts.push(format!(
            "持仓超过 {hours} 小时、按盘口亏损 ≤ {loss} USDT 就平仓"
        ));
    }
    if let Some(loss) = rules.stop_loss_usdt {
        parts.push(format!("按盘口亏到 −{loss} USDT 止损"));
    }
    parts.join("，")
}

pub fn health_label(health: arb_scanner::Health) -> &'static str {
    match health {
        arb_scanner::Health::Healthy => "健康",
        arb_scanner::Health::Caution => "注意",
        arb_scanner::Health::Danger => "危险",
    }
}

/// 两腿标记价基差（%）：(空 − 多) / 中间价 × 100。缺价时记 0 —— 只影响平仓备注里的
/// 价差盈亏，不影响是否平仓。
pub fn basis_pct(long_mark: Option<Decimal>, short_mark: Option<Decimal>) -> Decimal {
    match (long_mark, short_mark) {
        (Some(long), Some(short)) if long + short > Decimal::ZERO => {
            ((short - long) / ((short + long) / Decimal::TWO) * Decimal::ONE_HUNDRED).round_dp(4)
        }
        _ => Decimal::ZERO,
    }
}

pub fn limits_from(settings: &Settings) -> Limits {
    // 风控限额先取配置里的风险项；没有配的用保守默认值。
    Limits {
        // 不设上限时用一个够大的数：单笔多大只剩盘口深度、保证金与强平距离在管。
        max_position_usdt: settings.max_position_usdt.map_or(Decimal::MAX, dec),
        // 账户级持仓数上限：ARB_MAX_OPEN_POSITIONS（默认 5，off = 不限）。
        max_open_positions: settings.max_open_positions.unwrap_or(usize::MAX),
        max_daily_loss_usdt: dec(settings.max_daily_loss_usdt),
        max_slippage: Decimal::new(2, 3),
        max_entry_basis_pct: settings.max_entry_basis_pct.unwrap_or(Decimal::from(5u32)),
        // 危险档（强平距离 < 8%）直接拒绝；注意档照常放行，由用户自己看。
        min_liq_distance_pct: CAUTION_DISTANCE_PCT,
        // 资金费视角开仓前回看历史，费差不稳（近期已反转、只是偶尔为正）就拒绝。
        // ARB_REQUIRE_STABLE_FUNDING=off 关闭。
        require_stable_funding: !std::env::var("ARB_REQUIRE_STABLE_FUNDING")
            .is_ok_and(|raw| raw.trim().eq_ignore_ascii_case("off")),
    }
}

pub fn dec(value: f64) -> Decimal {
    Decimal::from_f64(value).unwrap_or(Decimal::ZERO)
}

pub async fn fetch_books(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    opportunity: &Opportunity,
    levels: u32,
) -> Result<(arb_core::OrderBook, arb_core::OrderBook)> {
    fetch_books_for(
        apis,
        (opportunity.long, &opportunity.symbol),
        (opportunity.short, &opportunity.symbol),
        levels,
    )
    .await
}

/// 同 [`fetch_books`]，但开仓核算（`desk::prepare`）用：速度优先，允许用本进程实时维护的盘口，
/// 放不下 `size_usdt` 的改拉整本（见 [`fetch_books_fast_for`]）。
pub async fn fetch_books_fast(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    opportunity: &Opportunity,
    levels: u32,
    size_usdt: Decimal,
) -> Result<(arb_core::OrderBook, arb_core::OrderBook)> {
    fetch_books_fast_for(
        apis,
        (opportunity.long, &opportunity.symbol),
        (opportunity.short, &opportunity.symbol),
        levels,
        size_usdt,
    )
    .await
}

/// 盘口没拉到（限频、超时、网络、场所报错）。它说的是「这次没查成」，不是「这对腿不能做」——
/// 调用方（比如后台预检）要能把它和闸门拒绝分开，所以给一个专门的类型，按类型判断而不是按文字。
#[derive(Debug)]
pub struct BookUnavailable {
    pub venue: Venue,
    pub detail: String,
}

impl std::fmt::Display for BookUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} 盘口没拉到：{}", self.venue, self.detail)
    }
}

impl std::error::Error for BookUnavailable {}

/// 两份盘口各自是什么时候到的（从同时发出请求起算，毫秒）。
///
/// 盘口接口都是 REST 现拉，没有各自的行情时间戳；两边到达的时刻差，是「这两份报价是不是同一时刻」
/// 能拿到的最好代理。差太大（一边在限频队列里排队、另一边立刻回来）时，由它们算出来的价差
/// 里混着一段时间的价格变动，不是任何一个时刻真能成交的价差。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookTiming {
    pub long_ms: u64,
    pub short_ms: u64,
}

impl BookTiming {
    /// 两份盘口到达时刻的差（毫秒）。
    pub fn skew_ms(&self) -> u64 {
        self.long_ms.abs_diff(self.short_ms)
    }
}

/// 两条腿各自的盘口。每条腿用它自己场所上的合约名去拉（计价资产可能不同）。
pub async fn fetch_books_for(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    long: (Venue, &arb_core::Symbol),
    short: (Venue, &arb_core::Symbol),
    levels: u32,
) -> Result<(arb_core::OrderBook, arb_core::OrderBook)> {
    let (long_book, short_book, _) = fetch_books_timed(apis, long, short, levels).await?;
    Ok((long_book, short_book))
}

/// 同 [`fetch_books_for`]，但开仓核算用：每条腿走 [`VenueApi::fetch_depth_fast`]
/// （Hyperliquid 可以用本进程实时维护的顶档代替一次 REST 往返；其它场所与 `fetch_depth` 相同）。
///
/// 快路径只给得出实时顶档（一档）。这笔 `size_usdt` 在这一侧放不下的，**改拉 REST 整本**，而不是让闸门按
/// 「深度不够」拒绝：手动开仓、资金费开仓的名义常常比顶档大，页面预检（走 REST）会说通过。
pub async fn fetch_books_fast_for(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    long: (Venue, &arb_core::Symbol),
    short: (Venue, &arb_core::Symbol),
    levels: u32,
    size_usdt: Decimal,
) -> Result<(arb_core::OrderBook, arb_core::OrderBook)> {
    let (long_book, short_book, _) =
        fetch_books_timed_with(apis, long, short, levels, Some(size_usdt)).await?;
    Ok((long_book, short_book))
}

/// 同 [`fetch_books_for`]，并带回两份盘口各自的到达时间（见 [`BookTiming`]）。永远走 REST。
pub async fn fetch_books_timed(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    long: (Venue, &arb_core::Symbol),
    short: (Venue, &arb_core::Symbol),
    levels: u32,
) -> Result<(arb_core::OrderBook, arb_core::OrderBook, BookTiming)> {
    fetch_books_timed_with(apis, long, short, levels, None).await
}

async fn fetch_books_timed_with(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    (long_venue, long_symbol): (Venue, &arb_core::Symbol),
    (short_venue, short_symbol): (Venue, &arb_core::Symbol),
    levels: u32,
    fast: Option<Decimal>,
) -> Result<(arb_core::OrderBook, arb_core::OrderBook, BookTiming)> {
    let started = std::time::Instant::now();
    // `need`：这条腿要吃的那一侧（多腿买入吃卖盘、空腿卖出吃买盘）。
    let fetch = |venue: Venue, symbol: &arb_core::Symbol, need: arb_core::Side| {
        let api = apis.get(&venue).cloned();
        let symbol = symbol.clone();
        async move {
            let api = api.ok_or_else(|| anyhow::anyhow!("{venue} 没有连接器"))?;
            let depth = match fast {
                Some(size_usdt) => match api.fetch_depth_fast(&symbol, levels).await {
                    // 快路径的盘口（实时顶档）这一侧放不下这笔金额：改拉整本。
                    Ok(book)
                        if book
                            .side(need)
                            .iter()
                            .map(|level| level.notional_usdt)
                            .sum::<Decimal>()
                            < size_usdt =>
                    {
                        api.fetch_depth(&symbol, levels).await
                    }
                    other => other,
                },
                None => api.fetch_depth(&symbol, levels).await,
            };
            let book = depth.map_err(|error| {
                anyhow::Error::new(BookUnavailable {
                    venue,
                    detail: error.to_string(),
                })
            })?;
            Ok::<_, anyhow::Error>((book, started.elapsed().as_millis() as u64))
        }
    };
    // 两个场所各拉各的，并发：串行时第二个盘口比第一个晚一整个往返，下单用的两份报价也就差了一段时间。
    let (long, short) = tokio::join!(
        fetch(long_venue, long_symbol, arb_core::Side::Buy),
        fetch(short_venue, short_symbol, arb_core::Side::Sell)
    );
    let ((long_book, long_ms), (short_book, short_ms)) = (long?, short?);
    Ok((long_book, short_book, BookTiming { long_ms, short_ms }))
}

/// 资金费历史没拉到（限频、网络）。和盘口一样属于「没查成」，不代表能下也不代表不能下。
#[derive(Debug)]
pub struct HistoryUnavailable {
    pub venue: Venue,
    pub detail: String,
}

impl std::fmt::Display for HistoryUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} 资金费历史没拉到：{}", self.venue, self.detail)
    }
}

impl std::error::Error for HistoryUnavailable {}

/// 这个错误（含它包着的上下文）是不是「行情数据没拉到」（盘口或资金费历史）：
/// 那不是闸门的结论，调用方要把它和拒绝分开。
pub fn is_book_unavailable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<BookUnavailable>().is_some()
            || cause.downcast_ref::<HistoryUnavailable>().is_some()
    })
}

/// 两腿最近的逐小时资金费历史。任何一家没实现历史接口返回 `Ok(None)`（不做判断）；
/// 实现了却拉失败返回 [`HistoryUnavailable`]。
pub async fn fetch_funding_histories(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    (long_venue, long_symbol): (Venue, &arb_core::Symbol),
    (short_venue, short_symbol): (Venue, &arb_core::Symbol),
    hours: u32,
) -> Result<Option<(Vec<arb_core::FundingPoint>, Vec<arb_core::FundingPoint>)>> {
    let (Some(long_api), Some(short_api)) = (apis.get(&long_venue), apis.get(&short_venue)) else {
        return Ok(None);
    };
    if !long_api.supports_funding_history() || !short_api.supports_funding_history() {
        return Ok(None);
    }
    let unavailable = |venue: Venue, error: arb_core::ArbError| {
        anyhow::Error::new(HistoryUnavailable {
            venue,
            detail: error.to_string(),
        })
    };
    let (long, short) = tokio::join!(
        long_api.fetch_funding_history(long_symbol, hours),
        short_api.fetch_funding_history(short_symbol, hours)
    );
    let long = long.map_err(|error| unavailable(long_venue, error))?;
    let short = short.map_err(|error| unavailable(short_venue, error))?;
    Ok(Some((long, short)))
}

pub fn parse_venue(raw: &str) -> Result<Venue, String> {
    Venue::parse(raw).ok_or_else(|| {
        format!(
            "未知场所 {raw:?}；可选：{}",
            Venue::ALL.map(Venue::as_str).join(", ")
        )
    })
}

pub fn pick<'a>(
    report: &'a ScanReport,
    view: &str,
    top: usize,
    long: Option<Venue>,
    short: Option<Venue>,
) -> Vec<&'a Opportunity> {
    let mut rows: Vec<&Opportunity> = report
        .symbols
        .iter()
        .flat_map(|symbol_view| {
            if view == "spread" {
                symbol_view.spread.iter()
            } else {
                symbol_view.funding.iter()
            }
        })
        .filter(|op| long.is_none_or(|venue| op.long == venue))
        .filter(|op| short.is_none_or(|venue| op.short == venue))
        .collect();
    rows.sort_by_key(|op| {
        std::cmp::Reverse(if view == "spread" {
            op.spread_net
        } else {
            op.funding_apr
        })
    });
    rows.into_iter().take(top).collect()
}

pub fn local_open_orders(replayed: &crate::Replayed) -> HashMap<Venue, Vec<String>> {
    let mut out: HashMap<Venue, Vec<String>> = HashMap::new();
    for order in replayed.orders.values() {
        if order.status.is_live() {
            out.entry(order.order.venue)
                .or_default()
                .push(order.order.client_order_id.0.clone());
        }
    }
    out
}

pub fn describe(rejection: &Rejection) -> String {
    rejection.to_string()
}

pub fn side_label(side: arb_core::Side) -> &'static str {
    match side {
        arb_core::Side::Buy => "买入",
        arb_core::Side::Sell => "卖出",
    }
}

#[cfg(test)]
mod fast_books_tests {
    use super::*;
    use arb_core::{ArbResult, Level, MarketSnapshot, OrderBook, Side, Symbol};
    use async_trait::async_trait;
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 快路径只给一档（实时顶档），REST 给整本；各自数调用次数。
    struct Api {
        venue: Venue,
        rest_calls: AtomicUsize,
        fast_calls: AtomicUsize,
    }

    fn book(venue: Venue, symbol: &Symbol, levels: usize) -> OrderBook {
        let side = |start: Decimal, step: Decimal| -> Vec<Level> {
            (0..levels)
                .map(|i| Level {
                    price: start + step * Decimal::from(i as u64),
                    notional_usdt: dec!(1000),
                })
                .collect()
        };
        OrderBook {
            venue,
            symbol: symbol.clone(),
            bids: side(dec!(99), dec!(-0.01)),
            asks: side(dec!(101), dec!(0.01)),
        }
    }

    #[async_trait]
    impl VenueApi for Api {
        fn venue(&self) -> Venue {
            self.venue
        }
        async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
            Ok(vec![])
        }
        async fn fetch_depth(&self, symbol: &Symbol, _: u32) -> ArbResult<OrderBook> {
            self.rest_calls.fetch_add(1, Ordering::SeqCst);
            Ok(book(self.venue, symbol, 10))
        }
        async fn fetch_depth_fast(&self, symbol: &Symbol, _: u32) -> ArbResult<OrderBook> {
            self.fast_calls.fetch_add(1, Ordering::SeqCst);
            Ok(book(self.venue, symbol, 1))
        }
    }

    type Fixture = (HashMap<Venue, Arc<dyn VenueApi>>, Arc<Api>, Arc<Api>);

    fn apis() -> Fixture {
        let make = |venue| {
            Arc::new(Api {
                venue,
                rest_calls: AtomicUsize::new(0),
                fast_calls: AtomicUsize::new(0),
            })
        };
        let (long, short) = (make(Venue::HyperliquidXyz), make(Venue::LighterRh));
        let map = HashMap::from([
            (
                Venue::HyperliquidXyz,
                Arc::clone(&long) as Arc<dyn VenueApi>,
            ),
            (Venue::LighterRh, Arc::clone(&short) as Arc<dyn VenueApi>),
        ]);
        (map, long, short)
    }

    #[tokio::test]
    async fn the_fast_book_is_used_only_when_it_covers_the_size_otherwise_the_full_book_is_fetched()
    {
        let symbol = Symbol::perp("ANTH", "USDT");
        let legs = |apis: &HashMap<Venue, Arc<dyn VenueApi>>, size| {
            let apis = apis.clone();
            let symbol = symbol.clone();
            async move {
                fetch_books_fast_for(
                    &apis,
                    (Venue::HyperliquidXyz, &symbol),
                    (Venue::LighterRh, &symbol),
                    20,
                    size,
                )
                .await
                .unwrap()
            }
        };
        // 顶档（1000 USDT）放得下 800：用快路径，不碰 REST。
        let (map, long, _) = apis();
        let (long_book, _) = legs(&map, dec!(800)).await;
        assert_eq!(long_book.side(Side::Buy).len(), 1);
        assert_eq!(long.fast_calls.load(Ordering::SeqCst), 1);
        assert_eq!(long.rest_calls.load(Ordering::SeqCst), 0);
        // 放不下 5000：改拉整本（10 档），而不是让闸门按「深度不够」拒绝。
        let (map, long, _) = apis();
        let (long_book, _) = legs(&map, dec!(5000)).await;
        assert_eq!(long_book.side(Side::Buy).len(), 10);
        assert_eq!(long.rest_calls.load(Ordering::SeqCst), 1);
        // 永远走 REST 的入口（页面预检、平仓核对、命令行）从不碰快路径。
        let (map, long, _) = apis();
        fetch_books_for(
            &map,
            (Venue::HyperliquidXyz, &symbol),
            (Venue::LighterRh, &symbol),
            20,
        )
        .await
        .unwrap();
        assert_eq!(long.fast_calls.load(Ordering::SeqCst), 0);
        assert_eq!(long.rest_calls.load(Ordering::SeqCst), 1);
    }
}
