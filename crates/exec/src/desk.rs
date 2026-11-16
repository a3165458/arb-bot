//! 交易台：开仓、平仓、对账与一轮规则监控的结构化流程。
//!
//! 这里**不打印**，只返回结构化结果。`arb-live` / `arb-paper` 在上面加打印，看板（`arb-web`）
//! 把它序列化成 JSON。三个入口必须走同一份流程：命令行验证过的闸门、杠杆校验与回滚，
//! 在网页上点一下按钮时一条都不能少。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result, bail};
use arb_core::{ArbError, ArbResult, Decimal, MarketSnapshot, OrderBook, Side, Symbol, Venue};
use arb_scanner::{Opportunity, PairRisk, ScanReport, SymbolView, rank};
use arb_venues::VenueApi;
use async_trait::async_trait;
use serde::Serialize;
use tracing::warn;

use crate::broker::{Broker, PaperBroker};
use crate::cli::{
    BookTiming, fetch_books_fast, fetch_books_fast_for, fetch_books_for, fetch_books_timed,
    fetch_funding_histories, fetch_snapshots, local_open_orders, pick, risk_of,
};
use crate::ledger::{Ledger, Record, Replayed};
use crate::monitor::{
    Action, Evaluation, ExitCheck, Inputs, MIN_TOP_UP_USDT, evaluate_full, exit_quote,
};
use crate::preflight::{Limits, Plan, Preflight, plan};
use crate::reconcile::{Reconciliation, reconcile};
use crate::types::{PairPosition, PositionStatus, Strategy, TaskRules};
use crate::{Executor, PositionSetup};

/// 杠杆超过两腿共同上限时怎么办。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeveragePolicy {
    /// 实盘：必须是整数，超过上限直接拒绝，不偷偷降档。
    Strict,
    /// 纸面：压到两腿共同上限。
    CapToPair,
}

/// 开一笔仓位的请求。
#[derive(Debug, Clone)]
pub struct OpenRequest {
    /// 合约 base，如 `ETH`。
    pub base: String,
    /// 计价资产。给了就必须完全一致；不给时取这个 base 下满足两腿的那一条。
    pub quote: Option<String>,
    pub long: Venue,
    pub short: Venue,
    /// 单腿名义额（USDT）。
    pub size: Decimal,
    pub margin_mode: crate::MarginMode,
    pub leverage: Decimal,
    /// 当日已实现盈亏（亏损为负）。实盘必须由操作者给出。
    pub daily_pnl: Decimal,
    /// `funding` 或 `spread`。
    pub view: String,
    pub depth_levels: u32,
    pub rules: TaskRules,
    /// 「相对正常基差」的价差单（只给自动交易用，默认关）：开仓可成交价差可以为负、不要求收敛到 0 也
    /// 赚钱，只要求**收敛到 `rules.basis_exit_pct`（回到正常水平）**时扣掉成本仍为正。为此必须同时带
    /// 基差收敛目标和止损（[`TaskRules::stop_loss_usdt`]），否则拒绝。
    pub relative_to_normal: bool,
    /// 价差单先下哪条腿（资金费单不看它，一律 [`crate::preflight::LegOrder::Slippage`]）。
    pub leg_order: crate::preflight::LegOrder,
    /// 其中一条腿用**只挂单**开仓（见 [`MakerLeg`]）。只有「相对正常基差」的价差单可以。
    pub maker: Option<MakerLeg>,
    /// 按盘口深度定单笔（只给自动交易用，默认关）：`size` 当作上限，盘口在限价内吃不下时往下缩，见
    /// [`crate::sizing`]。只对两腿都吃单的价差单起作用；资金费单、挂单开仓照旧用固定的 `size`。
    pub depth_fit: Option<crate::sizing::DepthFit>,
    /// 强制补腿（只给自动交易用，默认关）：最差锁定价差（%，正数，如 0.3 = 价差最差到 −0.3%）。第一腿成交后第二腿在
    /// 保本价内没吃满，不回滚，按这个最差价把剩余数量追上，见 [`crate::executor::PositionSetup::force_hedge`]。
    /// 只对两腿都吃单的价差单起作用。
    pub force_hedge_pct: Option<Decimal>,
}

/// 一条腿用只挂单（post-only）开仓：挂在这家场所自己的最优价上等人来吃，成交了对面才按成交量吃单对冲。
/// 开仓不付吃单费也不穿价；代价是不一定成交，成交时又往往是价格正在穿过挂单价（逆向选择）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MakerLeg {
    /// 挂单的场所，必须是两条腿之一。
    pub venue: Venue,
    /// 挂单开仓的费率（小数，币安 TradFi 为 0）。
    pub entry_fee: Decimal,
    /// 之后平仓（吃单）的费率（小数，币安 TradFi 为 0.0004）。回滚也按它。
    pub exit_fee: Decimal,
    /// 最长挂多久（毫秒）。
    pub rest_ms: u64,
}

impl MakerLeg {
    /// 按挂单的方式改写计划：`leg` 是这家场所的那条腿，挂在 `book` 自己这一侧的最优价（买挂买一、卖挂卖一）；
    /// 返回改写后的往返手续费（小数，占名义额）：对面开平两次吃单，这家挂单开、吃单平。
    ///
    /// 盘口空了或买卖一交叉（价格不可信）返回错误。
    fn apply(
        &self,
        leg: &mut crate::preflight::LegPlan,
        book: &OrderBook,
        other_fee: Decimal,
    ) -> Result<Decimal> {
        let (bid, ask) = match (book.best_bid(), book.best_ask()) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO && bid < ask => (bid, ask),
            _ => bail!("{} 的盘口为空或买卖一交叉，定不了挂单价", self.venue),
        };
        let price = match leg.side {
            Side::Buy => bid,
            Side::Sell => ask,
        };
        leg.limit_price = price;
        leg.best_price = price;
        leg.expected_price = price;
        leg.slippage = Decimal::ZERO;
        leg.fee = self.exit_fee;
        leg.maker_rest_ms = Some(self.rest_ms);
        Ok(other_fee * Decimal::TWO + self.entry_fee + self.exit_fee)
    }
}

/// 盘口顶端。给操作者看「计划是按什么价算的」。
#[derive(Debug, Clone, Serialize)]
pub struct BookTop {
    pub venue: Venue,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
}

impl BookTop {
    fn of(book: &OrderBook) -> Self {
        Self {
            venue: book.venue,
            best_bid: book.best_bid(),
            best_ask: book.best_ask(),
        }
    }
}

/// 过了全部校验与闸门、可以直接交给执行器的一笔仓位。
#[derive(Debug, Serialize)]
pub struct Prepared {
    pub opportunity: Opportunity,
    pub risk: PairRisk,
    pub requested_leverage: Decimal,
    pub margin_mode: crate::MarginMode,
    /// 实际使用的杠杆（两腿相同）。
    pub leverage: Decimal,
    pub plan: Plan,
    /// 取完两条腿盘口的时刻：计划是按这一刻的盘口算的。执行报告用它算「报价到成交」用了多久。
    pub quoted_at: chrono::DateTime<chrono::Utc>,
    pub strategy: Strategy,
    /// 开仓后的任务规则。按盘口深度缩小了单笔时，按 USDT 计的金额已同比例缩小（见 [`TaskRules::scaled`]）。
    pub rules: TaskRules,
    /// 实际单笔（按盘口深度缩小过就小于请求的 `size`）。
    pub size_usdt: Decimal,
    /// 按盘口深度定单笔时的依据（请求多少、实际多少、两条腿限价内的深度）；没开就是 `None`。
    pub fit: Option<crate::sizing::SizeFit>,
    /// 强制补腿的最差锁定价差（小数，0.003 = −0.3%）；没开 / 不适用就是 `None`。
    pub force_hedge: Option<Decimal>,
    /// 下单前已有的敞口仓位数（闸门用）。
    pub open_positions: usize,
    pub long_book: BookTop,
    pub short_book: BookTop,
    /// 价差套利才有：可成交价差与吃完深度后还剩多少。
    pub spread: Option<SpreadEdge>,
    /// 资金费套利才有：两腿最近的费差稳不稳。场所没接入历史时为 `None`（没法判断，不拦）。
    pub stability: Option<arb_scanner::FundingStability>,
    /// 调用方已经用 [`check_legs_flat`] 核对过两腿场所都没有这个合约的持仓（见
    /// [`PositionSetup::positions_checked`]）。只有和计划的两腿是同一个合约时才算数。
    #[serde(skip)]
    pub flat_checked: Option<Symbol>,
}

/// 价差套利下单时的价差账：按买一卖一算一次，再按这笔名义吃完深度后的均价算一次。
#[derive(Debug, Clone, Serialize)]
pub struct SpreadEdge {
    /// 可成交价差（%）：(空腿买一 − 多腿卖一) / 中间价 × 100。
    pub top_basis_pct: Decimal,
    /// 吃完深度后的价差（%）：(空腿预估均价 − 多腿预估均价) / 中间价 × 100。
    pub depth_basis_pct: Decimal,
    /// 按买一卖一、扣掉平仓穿价与往返手续费后的一次性净收益（小数）。
    pub top_net: Decimal,
    /// 同上，按吃完深度后的均价算。**闸门用这个**：它不为正就拒绝下单。
    pub depth_net: Decimal,
    /// 当前标记价基差（%）。「基差收敛平仓」按它触发，入场基差也记它。
    pub mark_basis_pct: Option<Decimal>,
    /// 设了「基差收敛平仓」时的目标（%，标记价口径）。
    pub target_basis_pct: Option<Decimal>,
    /// 收敛到目标就平仓时的一次性净收益（小数）：`depth_net − 目标 / 100`。
    /// 设了目标时**闸门用这个**：收敛到目标就平，只赚得到入场基差与目标之间那一段。
    pub target_net: Option<Decimal>,
    /// 保本的收敛目标（%）：目标低于它，收敛到目标平仓时扣完成本还有得赚。
    pub break_even_target_pct: Decimal,
}

/// 价差单两腿锁定价差 (空 − 多) / 中间价 的保本线（小数）：低于它，收敛到 `target_pct`（没设按收敛到 0）
/// 平仓时扣掉平仓穿价与往返手续费就不赚。和开仓前的判断（[`spread_edge`] 的 `target_net`、
/// [`spread_target_shortfall`]）是同一条线：`target_net = 吃完深度后的价差 − 保本线`。
pub fn locked_basis_floor(opportunity: &Opportunity, target_pct: Option<Decimal>) -> Decimal {
    opportunity.round_trip_spread.unwrap_or(Decimal::ZERO) / Decimal::TWO
        + opportunity.round_trip_fee
        + target_pct.unwrap_or(Decimal::ZERO) / Decimal::ONE_HUNDRED
}

/// 设了收敛目标的价差单，收敛到目标时的净收益不为正：说清楚差在哪、目标至少要设多低。
/// 预览、下单与策略页的后台预检共用这一句。
pub fn spread_target_shortfall(depth_net: Decimal, target_pct: Decimal) -> Option<String> {
    let net = depth_net - target_pct / Decimal::ONE_HUNDRED;
    (net <= Decimal::ZERO).then(|| {
        let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(4).normalize();
        format!(
            "基差收敛目标 {}% 留下的空间不够：收敛到目标就平仓时，扣掉平仓穿价与往返手续费净 {}%（不为正）。\
             目标至少要低于 {}%（保本线）才有得赚",
            target_pct.normalize(),
            pct(net),
            pct(depth_net)
        )
    })
}

/// 价差视角下的一对腿：两腿快照来自**同一个合约身份**（簇），排除掉不可信的读数；
/// 用现拉的两边盘口补上买一卖一，再交给 `rank::rank` 按扫描器同一套公式算可成交价差与
/// 一次性净收益。CEX 与 DEX 都走这里 —— 批量接口不给买一卖一的 DEX（Hyperliquid、Lighter、
/// Arcus）因此也能做价差单，而且 CEX 用的也是刚拉的盘口，不是一分钟前的快照。
pub async fn spread_opportunity(
    report: &ScanReport,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    base: &str,
    quote: Option<&str>,
    long: Venue,
    short: Venue,
    levels: u32,
) -> Result<(Opportunity, OrderBook, OrderBook)> {
    spread_opportunity_with(
        report, by_venue, base, quote, long, short, levels, false, None,
    )
    .await
}

/// [`spread_opportunity`]；`relative_to_normal` 为真时不要求可成交价差为正、也不要求收敛到 0 划算
/// （由调用方按「收敛到目标」判断，见 [`prepare`]）。身份、可信度、现拉盘口照旧。
#[allow(clippy::too_many_arguments)]
pub async fn spread_opportunity_with(
    report: &ScanReport,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    base: &str,
    quote: Option<&str>,
    long: Venue,
    short: Venue,
    levels: u32,
    relative_to_normal: bool,
    fast_size_usdt: Option<Decimal>,
) -> Result<(Opportunity, OrderBook, OrderBook)> {
    let pair = format!("{base} 多 {long} / 空 {short}");
    // 同名不同资产（QNT 股票 vs QNT 币）的价差看起来最「诱人」，也最致命：两腿必须在同一个簇里。
    let views: Vec<&SymbolView> = report
        .symbols
        .iter()
        .filter(|view| {
            view.symbol.base.eq_ignore_ascii_case(base)
                && quote.is_none_or(|quote| view.symbol.quote.eq_ignore_ascii_case(quote))
                && [long, short]
                    .iter()
                    .all(|venue| view.rates.iter().any(|rate| rate.venue == *venue))
        })
        .collect();
    let view = match views.as_slice() {
        [view] => *view,
        [] => {
            // 有一条腿被身份判定或筛查排除时直接说原因（补仓两腿没能成对时写明差了多少）。
            if let Some(row) = report
                .unverified
                .iter()
                .chain(report.suspicious.iter())
                .find(|row| {
                    [long, short].contains(&row.venue) && row.symbol.base.eq_ignore_ascii_case(base)
                })
            {
                bail!("{pair}：{} 的读数没有参与配对：{}", row.venue, row.reason);
            }
            bail!(
                "{pair}：本轮扫描里这两家没有落在同一个合约身份下（可能不是同一资产、两家价格相差太大，或有一家没上这个合约）"
            )
        }
        _ => bail!("{pair}：有多个同名的合约身份，无法确定是哪一个"),
    };
    let rate = |venue: Venue| view.rates.iter().find(|rate| rate.venue == venue);
    let (Some(long_rate), Some(short_rate)) = (rate(long), rate(short)) else {
        bail!("{pair}：本轮扫描缺至少一条腿的读数");
    };
    for leg in [long_rate, short_rate] {
        if let Some(row) = report
            .suspicious
            .iter()
            .chain(report.unverified.iter())
            .find(|row| row.venue == leg.venue && row.symbol == leg.symbol)
        {
            bail!(
                "{pair} 不能做：{} 上的读数被可信度筛查排除（{}）",
                leg.venue,
                row.reason
            );
        }
    }
    // 开仓核算（`fast_size_usdt` 有值）：Hyperliquid 可以读本进程实时维护的顶档代替 REST 往返；这笔金额在
    // 这一侧放不下的改拉整本（见 [`fetch_books_fast_for`]）。其它调用方（命令行、测试）永远走 REST。
    let (long_book, short_book) = match fast_size_usdt {
        Some(size_usdt) => {
            fetch_books_fast_for(
                by_venue,
                (long, &long_rate.symbol),
                (short, &short_rate.symbol),
                levels,
                size_usdt,
            )
            .await
        }
        None => {
            fetch_books_for(
                by_venue,
                (long, &long_rate.symbol),
                (short, &short_rate.symbol),
                levels,
            )
            .await
        }
    }
    .with_context(|| format!("{pair}：拉两边盘口失败"))?;
    // 只补买一卖一；标记价、指数价不动 —— 身份判定与入场基差都依赖它们。
    let patch = |rate: &MarketSnapshot, book: &OrderBook| {
        let mut patched = rate.clone();
        patched.best_bid = book.best_bid();
        patched.best_ask = book.best_ask();
        patched.bid_size_usdt = book.bids.first().map(|level| level.notional_usdt);
        patched.ask_size_usdt = book.asks.first().map(|level| level.notional_usdt);
        patched
    };
    let (long_patched, short_patched) =
        (patch(long_rate, &long_book), patch(short_rate, &short_book));
    if relative_to_normal {
        let mut opportunity =
            rank::pair_unfiltered(&long_patched, &short_patched, &report.rank_config())
                .filter(|op| op.executable_basis_pct.is_some() && !op.spread_unknown)
                .ok_or_else(|| anyhow::anyhow!("{pair}：至少一侧盘口为空，算不出可成交价差"))?;
        opportunity.symbol = view.symbol.clone();
        return Ok((opportunity, long_book, short_book));
    }
    let found = rank::rank(&[&long_patched, &short_patched], &report.rank_config())
        .into_iter()
        .find(|op| {
            op.long == long
                && op.short == short
                && op
                    .executable_basis_pct
                    .is_some_and(|basis| basis > Decimal::ZERO)
        });
    let Some(mut opportunity) = found else {
        let top = match (short_book.best_bid(), long_book.best_ask()) {
            (Some(bid), Some(ask)) => format!("{short} 买一 {bid}，{long} 卖一 {ask}"),
            _ => "至少一侧盘口为空".into(),
        };
        bail!(
            "{pair}：按现拉盘口，可成交价差不为正（{top}）。价差套利要空腿卖得出的价高于多腿要买的价"
        );
    };
    if !opportunity.spread_profitable() {
        bail!(
            "{pair}：可成交价差 {}% 扣掉平仓穿价与往返手续费后不划算（净 {}%）",
            opportunity
                .executable_basis_pct
                .unwrap_or(Decimal::ZERO)
                .round_dp(3),
            (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(3)
        );
    }
    // 风险评估（risk_of）按簇的合约名找两腿快照。
    opportunity.symbol = view.symbol.clone();
    Ok((opportunity, long_book, short_book))
}

/// 吃完深度后的价差账。两腿预估均价来自闸门刚算好的执行计划；`target_pct` 是
/// 「基差收敛平仓」的目标（没设为 `None`，按收敛到 0 算）。
pub fn spread_edge(
    opportunity: &Opportunity,
    execution: &Plan,
    target_pct: Option<Decimal>,
) -> Result<SpreadEdge> {
    let (long_price, short_price) = (
        execution.long.expected_price,
        execution.short.expected_price,
    );
    let mid = (long_price + short_price) / Decimal::TWO;
    if mid <= Decimal::ZERO {
        bail!("预估成交均价非正，算不出价差");
    }
    let depth_basis = (short_price - long_price) / mid;
    let exit_cross = opportunity.round_trip_spread.unwrap_or(Decimal::ZERO) / Decimal::TWO;
    let depth_net = depth_basis - exit_cross - opportunity.round_trip_fee;
    Ok(SpreadEdge {
        top_basis_pct: opportunity
            .executable_basis_pct
            .unwrap_or(Decimal::ZERO)
            .round_dp(4),
        depth_basis_pct: (depth_basis * Decimal::ONE_HUNDRED).round_dp(4),
        top_net: opportunity.spread_net.round_dp(6),
        depth_net: depth_net.round_dp(6),
        mark_basis_pct: opportunity.entry_basis_pct.map(|pct| pct.round_dp(4)),
        target_basis_pct: target_pct,
        target_net: target_pct
            .map(|target| (depth_net - target / Decimal::ONE_HUNDRED).round_dp(6)),
        break_even_target_pct: (depth_net * Decimal::ONE_HUNDRED).round_dp(4),
    })
}

/// 资金费单的稳定性检查。`require` 为假时只评估、不拦。历史不够判断时：要求稳定就拒绝
/// （不能把「不知道」当成稳定），不要求就放行。
pub fn stability_gate(
    long_history: &[arb_core::FundingPoint],
    short_history: &[arb_core::FundingPoint],
    now: chrono::DateTime<chrono::Utc>,
    require: bool,
) -> Result<Option<arb_scanner::FundingStability>> {
    match arb_scanner::stability::assess(long_history, short_history, now) {
        Ok(assessed) => {
            if require && !assessed.stable {
                bail!(
                    "费差不稳定：{}。当前费差可能是短时的，开仓后容易反转",
                    assessed.reason.as_deref().unwrap_or("")
                );
            }
            Ok(Some(assessed))
        }
        Err(why) if require => {
            bail!("{why}，不开新仓（ARB_REQUIRE_STABLE_FUNDING=off 可关闭这道检查）")
        }
        Err(_) => Ok(None),
    }
}

/// 价差单的盈利类闸门：这笔名义吃完深度后的价差，扣掉平仓穿价与往返手续费还得为正；设了收敛目标的，
/// 收敛到目标就平，只赚得到入场基差与目标之间那一段，按它判断。通过返回价差账。
fn spread_profit_gate(
    opportunity: &Opportunity,
    execution_plan: &Plan,
    request: &OpenRequest,
    size: Decimal,
) -> Result<SpreadEdge> {
    let edge = spread_edge(opportunity, execution_plan, request.rules.basis_exit_pct)?;
    if let Some(target) = request.rules.basis_exit_pct
        && let Some(reason) = spread_target_shortfall(edge.depth_net, target)
    {
        bail!(
            "按 {} USDT 吃完深度后价差 {}%（两腿预估均价 {} / {}）。{reason}",
            size.normalize(),
            edge.depth_basis_pct,
            execution_plan.long.expected_price.normalize(),
            execution_plan.short.expected_price.normalize()
        );
    }
    if request.rules.basis_exit_pct.is_none() && edge.depth_net <= Decimal::ZERO {
        bail!(
            "按 {} USDT 吃完深度后价差不够：两腿预估均价 {} / {}，价差 {}%，扣掉平仓穿价与往返手续费后净 {}%",
            size.normalize(),
            execution_plan.long.expected_price.normalize(),
            execution_plan.short.expected_price.normalize(),
            edge.depth_basis_pct,
            (edge.depth_net * Decimal::ONE_HUNDRED).round_dp(4)
        );
    }
    Ok(edge)
}

/// 扫描 → 规则与强平校验 → 深度体检 → 闸门。任何一步不过都返回原因，不下单。
///
/// `report` 必须是**新鲜**的：风险、持仓量上限、最大杠杆与入场基差都来自它，
/// 不来自刚拉的盘口。
pub async fn prepare(
    report: &ScanReport,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    request: &OpenRequest,
    open_positions: usize,
    limits: &Limits,
    policy: LeveragePolicy,
) -> Result<Prepared> {
    if request.long == request.short {
        bail!("两腿不能是同一场所");
    }
    if request.size <= Decimal::ZERO {
        bail!("单腿名义必须大于 0");
    }
    if request.leverage < Decimal::ONE {
        bail!("杠杆必须不小于 1");
    }
    if policy == LeveragePolicy::Strict && !request.leverage.fract().is_zero() {
        bail!("实盘杠杆必须是整数");
    }
    let base = request.base.trim().to_ascii_uppercase();
    let quote = request
        .quote
        .as_deref()
        .map(|quote| quote.trim().to_ascii_uppercase());
    let is_spread = request.view == "spread";
    let relative = is_spread && request.relative_to_normal;
    if let Some(maker) = &request.maker {
        if !relative {
            bail!("挂单开仓只用于「相对正常基差」的价差单");
        }
        if ![request.long, request.short].contains(&maker.venue) {
            bail!("挂单的场所 {} 不是这笔单的任何一条腿", maker.venue);
        }
        if maker.rest_ms == 0 {
            bail!("挂单时间必须大于 0");
        }
    }
    if request.relative_to_normal {
        if !is_spread {
            bail!("「相对正常基差」只适用于价差单");
        }
        if request.rules.basis_exit_pct.is_none() {
            bail!(
                "「相对正常基差」的价差单必须带基差收敛目标（回到正常水平平仓）：没有它就不知道要赚哪一段"
            );
        }
        if request.rules.stop_loss_usdt.is_none() {
            bail!("「相对正常基差」的价差单必须带止损：开仓价差可以为负，偏离继续拉大时要有出口");
        }
    }
    // 价差：按现拉盘口重新核算（CEX 与 DEX 同一条路）；资金费：取本轮扫描的机会榜。
    let (opportunity, prefetched) = if is_spread {
        let (opportunity, long_book, short_book) = spread_opportunity_with(
            report,
            by_venue,
            &base,
            quote.as_deref(),
            request.long,
            request.short,
            request.depth_levels,
            relative,
            Some(request.size),
        )
        .await?;
        (opportunity, Some((long_book, short_book)))
    } else {
        // `pick` 只按场所过滤；合约要按 base（和 quote）精确匹配，不能取「第一条同 base 的」。
        let Some(opportunity) = pick(
            report,
            &request.view,
            usize::MAX,
            Some(request.long),
            Some(request.short),
        )
        .into_iter()
        .find(|op| {
            op.symbol.base.eq_ignore_ascii_case(&base)
                && quote
                    .as_deref()
                    .is_none_or(|quote| op.symbol.quote.eq_ignore_ascii_case(quote))
        }) else {
            bail!(
                "{}",
                missing_opportunity(
                    report,
                    &request.view,
                    &base,
                    quote.as_deref(),
                    request.long,
                    request.short
                )
            );
        };
        (opportunity.clone(), None)
    };
    let opportunity = &opportunity;
    if let Some(target) = request.rules.basis_exit_pct {
        match opportunity.entry_basis_pct {
            Some(entry) if target >= entry => bail!(
                "规则不成立：基差收敛目标 {target}% 不低于当前标记价基差 {}%，开仓就会触发平仓",
                entry.round_dp(3)
            ),
            None => bail!("规则不成立：缺两腿标记价，基差收敛无从评估"),
            Some(_) => {}
        }
    }

    // 费差自动平仓的门槛不低于当前费差：开仓第一轮就会被平掉，那不是规则，是白付一趟成本。
    if !is_spread
        && let Some(min) = request.rules.min_funding_apr
        && min >= opportunity.apr
    {
        bail!(
            "规则不成立：费差自动平仓门槛 {}% 不低于当前毛费差年化 {}%，开仓第一轮就会被平掉",
            (min * Decimal::ONE_HUNDRED).round_dp(2).normalize(),
            (opportunity.apr * Decimal::ONE_HUNDRED)
                .round_dp(2)
                .normalize()
        );
    }

    crate::margin::validate_rules(request.margin_mode, &request.rules)
        .map_err(anyhow::Error::msg)?;
    if policy == LeveragePolicy::Strict {
        crate::margin::validate_venues(request.margin_mode, &[request.long, request.short])
            .map_err(anyhow::Error::msg)?;
    }
    let requested = request.leverage;
    let first = risk_of(report, opportunity, requested)
        .context("本轮扫描缺少两腿快照，无法评估强平风险")?;
    let leverage = match (first.max_pair_leverage, policy) {
        (Some(max), LeveragePolicy::Strict) if requested > max => {
            bail!("杠杆 {requested}x 超过两腿共同上限 {}x", max.round_dp(2))
        }
        (Some(max), LeveragePolicy::CapToPair) => requested.min(max),
        _ => requested,
    };
    let mut risk = if leverage == requested {
        first
    } else {
        risk_of(report, opportunity, leverage).context("本轮扫描缺少两腿快照")?
    };
    crate::margin::display_risk(request.margin_mode, &mut risk);
    if let Err(reason) = crate::margin::validate_open_rules(
        request.margin_mode,
        &request.rules,
        risk.liq_distance_pct,
    ) {
        bail!("规则不成立：{reason}");
    }

    let (long_book, short_book) = match prefetched {
        Some(books) => books,
        None => fetch_books_fast(by_venue, opportunity, request.depth_levels, request.size).await?,
    };
    let quoted_at = chrono::Utc::now();
    let ctx = Preflight {
        opportunity,
        size_usdt: request.size,
        long_book: &long_book,
        short_book: &short_book,
        open_positions,
        daily_pnl: request.daily_pnl,
        risk: Some(&risk),
    };
    // 「相对正常基差」的单子开仓价差本来就可能为负（两家长期有固定差价），不利入场基差的门槛
    // 管不了它：真正的风控是「收敛到目标要为正」（下面）和止损。其余闸门照旧。
    let relaxed;
    let limits = if relative {
        relaxed = Limits {
            max_entry_basis_pct: Decimal::MAX,
            ..limits.clone()
        };
        &relaxed
    } else {
        limits
    };
    // 按盘口深度定单笔：只在两腿都吃单的价差单上，只在「吃不到」时往下缩；缩完之后下面的全部闸门
    // （含吃完深度后的价差、收敛目标）在这个单笔上完整再跑一遍。
    let (ctx, fit) = match request.depth_fit {
        Some(depth_fit) if is_spread && request.maker.is_none() => {
            // 上限本身盘口吃得下、却不赚的，照今天的做法拒绝：只在「吃不到」时缩，不去找一个能赚的小单笔
            // （价差只在盘口顶上一小块报价上才有，往往是还没跟上的旧报价）。上限吃不下时看不到上限的价差账，
            // 缩小之后再由下面的闸门在缩小后的单笔上判断。
            if let Ok(capped) = plan(&ctx, limits) {
                spread_profit_gate(opportunity, &capped, request, ctx.size_usdt)?;
            }
            let floor = locked_basis_floor(opportunity, request.rules.basis_exit_pct);
            let fit = crate::sizing::fit_spread_size(
                &ctx,
                limits,
                floor,
                request.leg_order,
                request
                    .force_hedge_pct
                    .map(|pct| pct / Decimal::ONE_HUNDRED),
                depth_fit,
            )?;
            (
                Preflight {
                    size_usdt: fit.fitted,
                    ..ctx
                },
                Some(fit),
            )
        }
        _ => (ctx, None),
    };
    let size = ctx.size_usdt;
    let mut execution_plan =
        plan(&ctx, limits).map_err(|rejection| anyhow::anyhow!("闸门拒绝：{rejection}"))?;
    // 挂单开仓：这条腿改成挂在自己这一侧的最优价（现拉的盘口），手续费口径跟着改。之后的价差账、保本线、
    // 第二腿的限价都按改写后的计划与手续费算，和吃单的单子走同一套。
    let adjusted;
    let opportunity: &Opportunity = if let Some(maker) = &request.maker {
        let (leg, book, other_fee) = if maker.venue == request.long {
            (&mut execution_plan.long, &long_book, opportunity.short_fee)
        } else {
            (&mut execution_plan.short, &short_book, opportunity.long_fee)
        };
        let round_trip_fee = maker.apply(leg, book, other_fee)?;
        adjusted = Opportunity {
            round_trip_fee,
            round_trip_cost: round_trip_fee
                + opportunity.round_trip_spread.unwrap_or(Decimal::ZERO),
            ..opportunity.clone()
        };
        &adjusted
    } else {
        opportunity
    };
    // 价差单的最后一道：买一卖一只覆盖一个单位，这笔名义吃完深度、再扣平仓穿价与手续费还得为正。
    let spread = if is_spread {
        let edge = spread_profit_gate(opportunity, &execution_plan, request, size)?;
        // 第一腿成交后，第二腿的限价按这条线和第一腿的实际均价反推（见 `Plan::hedged_after`）。
        execution_plan.min_locked_basis = Some(locked_basis_floor(
            opportunity,
            request.rules.basis_exit_pct,
        ));
        execution_plan.leg_order = request.leg_order;
        Some(edge)
    } else {
        None
    };
    // 资金费单的最后一道：回看两腿最近的逐小时费率，费差不稳（近期已反转、只是偶尔为正）
    // 就拒绝。拉不到历史按「没查成」报错，不当成稳定。
    let stability = if is_spread {
        None
    } else {
        match fetch_funding_histories(
            by_venue,
            (opportunity.long, &opportunity.symbol),
            (opportunity.short, &opportunity.symbol),
            arb_scanner::stability::FETCH_HOURS,
        )
        .await?
        {
            Some((long_history, short_history)) => stability_gate(
                &long_history,
                &short_history,
                chrono::Utc::now(),
                limits.require_stable_funding,
            )?,
            None => None,
        }
    };
    let strategy = if is_spread {
        Strategy::Spread
    } else {
        Strategy::Funding
    };
    Ok(Prepared {
        opportunity: opportunity.clone(),
        risk,
        requested_leverage: requested,
        margin_mode: request.margin_mode,
        leverage,
        plan: execution_plan,
        quoted_at,
        strategy,
        rules: if size < request.size {
            request.rules.scaled(size / request.size)
        } else {
            request.rules.clone()
        },
        size_usdt: size,
        fit,
        force_hedge: request
            .force_hedge_pct
            .filter(|_| is_spread && request.maker.is_none())
            .map(|pct| pct / Decimal::ONE_HUNDRED),
        open_positions,
        long_book: BookTop::of(&long_book),
        short_book: BookTop::of(&short_book),
        spread,
        stability,
        flat_checked: None,
    })
}

/// 这笔开仓要交易的合约：本轮扫描里 base（与 quote）对得上、且两家场所都有它的那一个。
/// 找不到就是 `None`，预热直接跳过。
fn warm_symbol(report: &ScanReport, request: &OpenRequest) -> Option<Symbol> {
    let base = request.base.trim().to_ascii_uppercase();
    let quote = request
        .quote
        .as_deref()
        .map(|quote| quote.trim().to_ascii_uppercase());
    report
        .symbols
        .iter()
        .filter(|view| {
            view.symbol.base.eq_ignore_ascii_case(&base)
                && quote
                    .as_deref()
                    .is_none_or(|quote| view.symbol.quote.eq_ignore_ascii_case(quote))
        })
        .find(|view| {
            [request.long, request.short]
                .iter()
                .all(|venue| view.rates.iter().any(|rate| rate.venue == *venue))
        })
        .map(|view| view.symbol.clone())
}

/// 两条腿的券商**只读**预热（见 [`Broker::warm_reads`]）：和拉盘口、算计划并发做，
/// 把「市场列表、杠杆核对」的往返藏在这些步骤后面，第一单发出前不必再等。
/// 只读、不改设置，所以计划被闸门拒绝也没有副作用；预热失败不影响开仓（真正下单前执行器会再准备一遍，
/// 那时再失败才是失败），只记一条日志。
pub async fn warm_legs(
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    report: &ScanReport,
    request: &OpenRequest,
) {
    if request.margin_mode.is_cross() {
        return;
    }
    let Some(symbol) = warm_symbol(report, request) else {
        return;
    };
    let warm = |venue: Venue| {
        let broker = brokers.get(&venue).cloned();
        let symbol = symbol.clone();
        async move {
            if let Some(broker) = broker
                && let Err(error) = broker.warm_reads(&symbol, Some(request.leverage)).await
            {
                tracing::debug!(%venue, %symbol, "开仓前只读预热没成功：{error}");
            }
        }
    };
    tokio::join!(warm(request.long), warm(request.short));
}

/// 提前把一个合约在这家的逐仓杠杆设成目标值（[`Broker::prepare_open_mode`]，读过是目标值就不写）：
/// 自动交易空闲时在后台一个一个做（见 web 的 `rh_auto`）。
///
/// 为什么：杠杆没设过的合约第一次开仓时，执行器要在报价之后、第一单之前现设。2026-10-15～10-22 东京
/// 有一部分吃单开仓因此多等 0.6～1.7 秒，几乎都是那个合约在那家第一次开。交易所会记住杠杆设置，
/// 提前设一次，之后开仓只剩一次很快的核对读。
///
/// 有持仓的合约不动（改杠杆会动到保证金，同 [`crate::executor::ensure_flat`]），现在开不了的（休市、
/// 维护、查不到）也不动。返回 `false` = 跳过了，没有改任何设置。
pub async fn preset_leverage(
    broker: &dyn Broker,
    symbol: &Symbol,
    leverage: Decimal,
) -> ArbResult<bool> {
    if !matches!(broker.trading_halted(symbol, false).await, Ok(None)) {
        return Ok(false);
    }
    if broker
        .positions()
        .await?
        .iter()
        .any(|position| position.symbol == *symbol && position.net_quantity != Decimal::ZERO)
    {
        return Ok(false);
    }
    broker
        .prepare_open_mode(
            symbol,
            Side::Buy,
            Some(leverage),
            crate::MarginMode::Isolated,
        )
        .await?;
    Ok(true)
}

/// 两条腿的场所上都还没有这个合约的持仓（只读，见 [`crate::executor::ensure_flat`]），而且两家现在
/// 都能开这个合约（休市、维护的不开，见 [`Broker::trading_halted`]）。
///
/// 看板在拉盘口、算计划的同时做（同一把交易锁内）：执行器本来要在报价之后、第一单之前各查一次，
/// 挪到这里就藏在拉盘口后面。合约认不出来时返回 `Ok(None)`，执行器照常自己查。
pub async fn check_legs_flat(
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    report: &ScanReport,
    request: &OpenRequest,
) -> Result<Option<Symbol>> {
    let Some(symbol) = warm_symbol(report, request) else {
        return Ok(None);
    };
    let check = |venue: Venue| {
        let broker = brokers.get(&venue).cloned();
        let symbol = symbol.clone();
        async move {
            match broker {
                Some(broker) => {
                    let (flat, open) = tokio::join!(
                        crate::executor::ensure_flat(broker.as_ref(), &symbol),
                        leg_open_check(broker.as_ref(), venue, &symbol)
                    );
                    flat.and(open)
                }
                None => Err(arb_core::ArbError::config(format!(
                    "{venue} 没有配置券商，无法执行"
                ))),
            }
        }
    };
    let (long, short) = tokio::join!(check(request.long), check(request.short));
    long?;
    short?;
    Ok(Some(symbol))
}

/// 这家现在能不能开这个合约（见 [`Broker::trading_halted`]）。查不到状态也不开：两条腿只有一条
/// 下得去就是裸腿，开仓不急这一下。
async fn leg_open_check(broker: &dyn Broker, venue: Venue, symbol: &Symbol) -> ArbResult<()> {
    match broker.trading_halted(symbol, false).await {
        Ok(None) => Ok(()),
        Ok(Some(reason)) => Err(ArbError::venue(
            venue.as_str(),
            format!("{reason}，现在不开仓"),
        )),
        Err(error) => Err(ArbError::venue(
            venue.as_str(),
            format!("查不到这个合约现在能不能下单，不开仓：{error}"),
        )),
    }
}

/// 补仓前：两条腿的场所现在都能开这个合约（见 [`leg_open_check`]）。
pub async fn check_legs_tradable(
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    symbol: &Symbol,
    venues: [Venue; 2],
) -> Result<()> {
    let check = |venue: Venue| {
        let broker = brokers.get(&venue).cloned();
        async move {
            match broker {
                Some(broker) => leg_open_check(broker.as_ref(), venue, symbol).await,
                None => Err(ArbError::config(format!("{venue} 没有配置券商，无法执行"))),
            }
        }
    };
    let (a, b) = tokio::join!(check(venues[0]), check(venues[1]));
    a?;
    b?;
    Ok(())
}

/// 按计划建仓。返回台账里的最终状态（可能是 `Unwound`：失败并已回滚）。
pub async fn execute(
    executor: &Executor,
    prepared: &Prepared,
    position_id: &str,
) -> Result<PairPosition> {
    let checked = prepared.flat_checked.as_ref();
    let setup = PositionSetup {
        leverage: Some(prepared.leverage),
        margin_mode: prepared.margin_mode,
        rules: prepared.rules.clone(),
        quoted_at: Some(prepared.quoted_at),
        force_hedge: prepared.force_hedge,
        positions_checked: checked.is_some()
            && [prepared.plan.first_leg(), prepared.plan.second_leg()]
                .iter()
                .all(|leg| Some(&leg.symbol) == checked),
    };
    let outcome = executor
        .open(
            &prepared.plan,
            prepared.strategy,
            prepared
                .opportunity
                .entry_basis_pct
                .unwrap_or(Decimal::ZERO),
            position_id,
            &setup,
        )
        .await?;
    Ok(outcome.position)
}

/// 按计划往已有的仓位上补仓（见 [`Executor::increase`]）。`position` 原地更新成补完后的样子；
/// 返回是否真的补上了。
pub async fn execute_add(
    executor: &Executor,
    prepared: &Prepared,
    position: &mut PairPosition,
) -> Result<bool> {
    let basis = prepared
        .opportunity
        .entry_basis_pct
        .unwrap_or(position.entry_basis_pct);
    Ok(executor.increase(position, &prepared.plan, basis).await?)
}

/// 补仓前核对目标仓位：持仓中、两腿齐全、价差套利、合约与两腿场所和请求一致；补完后单腿名义
/// 不超过 `max_total`。通过时返回这笔仓位。
pub fn add_target(
    exposed: &[PairPosition],
    position_id: &str,
    request: &OpenRequest,
    max_total: Decimal,
) -> std::result::Result<PairPosition, String> {
    let position = exposed
        .iter()
        .find(|position| position.id == position_id)
        .ok_or_else(|| format!("台账里没有持仓中的 {position_id}"))?;
    if position.status != PositionStatus::Open || !position.is_hedged() {
        return Err(format!("{position_id} 不是两腿齐全的持仓中仓位，不能补仓"));
    }
    if position.strategy != Strategy::Spread {
        return Err(format!("{position_id} 不是价差套利仓位，不补仓"));
    }
    let (Some(long), Some(short)) = (position.long.as_ref(), position.short.as_ref()) else {
        return Err("双腿持仓缺腿".into());
    };
    if !position.symbol.base.eq_ignore_ascii_case(&request.base)
        || long.venue != request.long
        || short.venue != request.short
    {
        return Err(format!(
            "补仓方向与 {position_id} 不一致（原仓位 {} 多 {} / 空 {}）",
            position.symbol, long.venue, short.venue
        ));
    }
    let total = long.notional_usdt.max(short.notional_usdt) + request.size;
    if total > max_total {
        return Err(format!(
            "补完单腿名义约 {} USDT，超过上限 {} USDT",
            total.round_dp(2),
            max_total.round_dp(2)
        ));
    }
    Ok(position.clone())
}

static LAST_ID_MILLIS: AtomicI64 = AtomicI64::new(0);

/// 新仓位 id：`{prefix}-{毫秒}`，与命令行同一格式。同一进程内严格递增 ——
/// 同一毫秒里的第二笔顺延一毫秒，不会与上一笔撞号（撞号会让两笔仓位共用订单号）。
pub fn new_position_id(prefix: &str) -> String {
    let now = chrono::Utc::now().timestamp_millis();
    let mut last = LAST_ID_MILLIS.load(Ordering::SeqCst);
    loop {
        let next = now.max(last + 1);
        match LAST_ID_MILLIS.compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return format!("{prefix}-{next}"),
            Err(current) => last = current,
        }
    }
}

/// 这对腿不在机会榜上时，说清楚是被什么挡下的：入场门槛、可信度筛查，还是方向本身不成立。
fn missing_opportunity(
    report: &ScanReport,
    view: &str,
    base: &str,
    quote: Option<&str>,
    long: Venue,
    short: Venue,
) -> String {
    let same = |symbol: &Symbol| {
        symbol.base.eq_ignore_ascii_case(base)
            && quote.is_none_or(|quote| symbol.quote.eq_ignore_ascii_case(quote))
    };
    let pair = format!("{base} 多 {long} / 空 {short}");
    if let Some(row) = report
        .gated
        .iter()
        .find(|row| row.long == long && row.short == short && same(&row.symbol))
    {
        return format!(
            "{pair} 被入场基差门槛挡下：{}。空腿比多腿便宜太多，两边价格一收敛这笔就先亏掉这部分，\
             要很久的资金费才补得回来。确实要做，调大 ARB_MAX_ENTRY_BASIS_PCT（或设为 off）后重启",
            row.reason
        );
    }
    if let Some(row) = report
        .suspicious
        .iter()
        .chain(report.unverified.iter())
        .find(|row| (row.venue == long || row.venue == short) && same(&row.symbol))
    {
        return format!(
            "{pair} 不能做：{} 上的读数被可信度筛查排除（{}）",
            row.venue, row.reason
        );
    }
    format!(
        "{view} 视角下没有 {pair} 的机会：费差方向不成立（多腿费率不低于空腿），或扣掉成本后不划算"
    )
}

/// 实盘对账：台账坏行、或台账里有敞口落在没连接的场所上，都直接报错 ——
/// 那部分核对不了，不能当成一致。
pub async fn reconcile_ledger(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
) -> Result<Reconciliation> {
    reconcile_ledger_on(ledger, brokers, None).await
}

/// [`reconcile_ledger`]，只去交易所核 `only` 里的场所（`None` = 全部）：开仓前只核这笔的两条腿（见 web 的
/// `LiveDesk::reconcile_legs`）。「台账里的仓位都在连着的场所上」照旧按全部券商查。
pub async fn reconcile_ledger_on(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    only: Option<&[Venue]>,
) -> Result<Reconciliation> {
    let (replayed, broken) = ledger.replay().await?;
    if broken > 0 {
        bail!("台账有 {broken} 行无法解析：先人工核对，不在脏台账上交易");
    }
    let exposed = replayed.exposed();
    let unconnected: BTreeSet<Venue> = exposed
        .iter()
        .flat_map(|position| [position.long.as_ref(), position.short.as_ref()])
        .flatten()
        .map(|leg| leg.venue)
        .filter(|venue| !brokers.contains_key(venue))
        .collect();
    if !unconnected.is_empty() {
        bail!(
            "台账里有仓位在未连接的场所上（{}）：把它们加进连接的场所再运行",
            unconnected
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let checked: HashMap<Venue, Arc<dyn Broker>>;
    let brokers = match only {
        Some(only) => {
            checked = brokers
                .iter()
                .filter(|(venue, _)| only.contains(venue))
                .map(|(venue, broker)| (*venue, Arc::clone(broker)))
                .collect();
            &checked
        }
        None => brokers,
    };
    let mut reconciliation = reconcile(&exposed, &local_open_orders(&replayed), brokers).await?;
    // 只按订单号 / 仓位 id 报出来的不一致，按台账补上是哪个币：冻结只落在那个币上（见 `Divergence::blocks`）。
    for divergence in reconciliation
        .divergences
        .iter_mut()
        .filter(|divergence| divergence.symbol.is_none())
    {
        divergence.symbol = replayed
            .orders
            .get(&divergence.reference)
            .map(|state| state.order.symbol.clone())
            .or_else(|| {
                replayed
                    .positions
                    .get(&divergence.reference)
                    .map(|position| position.symbol.clone())
            });
    }
    Ok(reconciliation)
}

/// [`resolve_pending_orders`] 的结论。
#[derive(Debug, Default, Serialize)]
pub struct PendingResolution {
    /// 已在台账里补上终态的订单号（券商从没收到的记为 `Rejected`）。
    pub resolved: Vec<String>,
    /// 补上的终态里**有成交**的订单号：成交发生了、但仓位记录里没有这条腿，必须人工核对。
    pub filled_unrecorded: Vec<String>,
    /// 券商那边还挂着的订单号（IOC 单不该出现）：留给对账。
    pub still_open: Vec<String>,
    /// 查不到确切结论的订单号与原因：保持原样，留给对账与人工。
    pub unknown: Vec<(String, String)>,
}

impl PendingResolution {
    pub fn is_empty(&self) -> bool {
        self.resolved.is_empty() && self.still_open.is_empty() && self.unknown.is_empty()
    }
}

/// 重启后先把台账里停在 `Pending` / `Open` 的订单对到券商的权威记录上。
///
/// 执行器在发单**之前**先把意图（`Pending`）落盘，发单之后才落终态。进程恰好死在两者之间
/// （OOM、被 SIGKILL、断电）时，台账里那张单永远停在 `Pending`，而对账把它当成「本地记着挂单、
/// 交易所没有」—— 一处**永远消不掉**的不一致，所有开仓与规则从此被拒，只能手改 JSONL。
///
/// 这里按券商的订单日志（[`Broker::order_state`]：意图先落盘，所以它认得每一张发出过的单）
/// 逐张核实：
/// - 有终态 → 把终态追加进台账；
/// - 券商明确**从没收到**（`None`，按约定只在没提交过时才返回）→ 记 `Rejected`；
/// - 还挂着、或查询出错 → **不动**，留给对账（宁可继续报不一致，也不编一个终态）。
///
/// 终态里有成交时，仓位记录里却没有对应的腿（进程死在「第一腿成交」与「写入仓位」之间），
/// 这笔成交属于台账之外的敞口：列在 `filled_unrecorded` 里，由调用方告警，对账会如实报
/// 持仓不符。
pub async fn resolve_pending_orders(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
) -> Result<PendingResolution> {
    // 先撤掉遗留的只挂单（它们是唯一合法地挂在簿上的单子），再核实其余的。
    let mut cancelled = cancel_resting_post_only(ledger, brokers).await?;
    let (replayed, _) = ledger.replay().await?;
    let mut pending: Vec<&crate::types::OrderState> = replayed
        .orders
        .values()
        .filter(|order| order.status.is_live())
        .collect();
    pending.sort_by(|a, b| a.order.client_order_id.0.cmp(&b.order.client_order_id.0));
    let mut result = PendingResolution::default();
    for local in pending {
        let id = &local.order.client_order_id;
        let Some(broker) = brokers.get(&local.order.venue) else {
            result
                .unknown
                .push((id.0.clone(), format!("{} 没有连接", local.order.venue)));
            continue;
        };
        match broker.order_state(id).await {
            Ok(Some(state))
                if state.order.client_order_id == *id
                    && state.order.venue == local.order.venue
                    && state.order.symbol == local.order.symbol
                    && state.order.side == local.order.side
                    && state.order.reduce_only == local.order.reduce_only =>
            {
                if state.status.is_live() {
                    result.still_open.push(id.0.clone());
                    continue;
                }
                let filled = state.filled_usdt > Decimal::ZERO;
                ledger
                    .append(&crate::ledger::Record::Order(Box::new(state)))
                    .await?;
                if filled {
                    result.filled_unrecorded.push(id.0.clone());
                }
                result.resolved.push(id.0.clone());
            }
            Ok(Some(_)) => result
                .unknown
                .push((id.0.clone(), "券商返回的订单与台账里的意图不一致".into())),
            Ok(None) => {
                let mut rejected = local.clone();
                rejected.status = crate::types::OrderStatus::Rejected;
                rejected.reject_reason =
                    Some("进程在发单前后中断，券商没有收到这个订单号：按未提交处理".into());
                ledger
                    .append(&crate::ledger::Record::Order(Box::new(rejected)))
                    .await?;
                result.resolved.push(id.0.clone());
            }
            Err(error) => result.unknown.push((id.0.clone(), error.to_string())),
        }
    }
    // 不必再核对「仓位记录里有没有这条腿」：执行器在 `execute_order` 里先落订单终态、之后才由调用方
    // 更新仓位。一张订单到重启时还停在 `Pending`，说明它的终态都没来得及落盘，后面的仓位更新
    // 更不可能发生 —— 所以这里补出来的每一笔成交都是台账没有记过的。
    result.resolved.append(&mut cancelled.resolved);
    result
        .filled_unrecorded
        .append(&mut cancelled.filled_unrecorded);
    // 没撤掉的在上面的核实循环里还会再出现一次（它仍是活的）：不重复报。
    for id in cancelled.still_open {
        if !result.still_open.contains(&id) {
            result.still_open.push(id);
        }
    }
    for entry in cancelled.unknown {
        if !result.unknown.iter().any(|(id, _)| *id == entry.0) {
            result.unknown.push(entry);
        }
    }
    Ok(result)
}

/// 撤掉台账里还活着的**只挂单**，并把读回的终态追加进台账。
///
/// 只挂单是唯一会合法地挂在簿上的单子：执行器开仓时挂着它、轮询、到点撤掉，整个过程在交易台的锁里。它一旦
/// 在锁外还活着，就属于一次已经中断的开仓（进程在挂单期间死了，或轮询 / 撤单出错后开仓提前返回）—— 不会再有人
/// 来撤它，而它随时可能成交成一条没人对冲的腿。所以重启时（[`resolve_pending_orders`]）和每一轮持仓规则开头
/// （交易台的锁内，不会碰上正在挂的单子）都要调用它。
///
/// 撤不掉、或读不回终态的，原样留在 `still_open` / `unknown`：对账会因此一直报不一致，不会悄悄放过。
/// 撤掉前已经成交的部分（没有对应的仓位记录）列在 `filled_unrecorded`，调用方要告警。
pub async fn cancel_resting_post_only(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
) -> Result<PendingResolution> {
    let (replayed, _) = ledger.replay().await?;
    let mut resting: Vec<&crate::types::OrderState> = replayed
        .orders
        .values()
        .filter(|order| order.status.is_live() && order.order.post_only)
        .collect();
    resting.sort_by(|a, b| a.order.client_order_id.0.cmp(&b.order.client_order_id.0));
    let mut result = PendingResolution::default();
    for local in resting {
        let id = &local.order.client_order_id;
        let Some(broker) = brokers.get(&local.order.venue) else {
            result
                .unknown
                .push((id.0.clone(), format!("{} 没有连接", local.order.venue)));
            continue;
        };
        // 以券商的记录为准（台账里停在 `Pending` 的没有交易所单号）。
        let state = match broker.order_state(id).await {
            Ok(Some(state))
                if state.order.client_order_id == *id
                    && state.order.venue == local.order.venue
                    && state.order.symbol == local.order.symbol
                    && state.order.side == local.order.side
                    && state.order.post_only =>
            {
                state
            }
            // 券商没有这张单、或对不上、或查询出错：交给 `resolve_pending_orders` 的其余逻辑与对账。
            _ => continue,
        };
        let state = if state.status.is_live() {
            if let Some(venue_id) = state.venue_order_id.clone() {
                if let Err(error) = broker.cancel(&venue_id).await {
                    tracing::warn!(order = %id, %error, "撤遗留的只挂单报错，读回状态再判断");
                }
                match broker.order_state(id).await {
                    Ok(Some(after))
                        if after.order.client_order_id == *id
                            && after.order.venue == local.order.venue =>
                    {
                        after
                    }
                    _ => state,
                }
            } else {
                state
            }
        } else {
            state
        };
        if state.status.is_live() {
            result.still_open.push(id.0.clone());
            continue;
        }
        let filled = state.filled_usdt > Decimal::ZERO;
        ledger
            .append(&crate::ledger::Record::Order(Box::new(state)))
            .await?;
        if filled {
            result.filled_unrecorded.push(id.0.clone());
        }
        result.resolved.push(id.0.clone());
    }
    Ok(result)
}

/// 两次对账都发现「交易所已经没有这笔仓位」之间至少隔多久，才在台账里结束它。
/// 一次空的或出错的持仓响应不能让一笔还在的仓位被当成已平掉。
pub const EXTERNAL_CLOSE_CONFIRM: std::time::Duration = std::time::Duration::from_secs(60);
/// 单腿认领时，那家场所上台账里只有这一条腿（认不出「查询返回了空列表」）：多确认这么久才平剩下的腿。
pub const EXTERNAL_CLOSE_CONFIRM_SINGLE: std::time::Duration = std::time::Duration::from_secs(180);

/// 对账发现的、不是看板平掉的仓位：给操作者看的一句话。
#[derive(Debug, Clone, Serialize)]
pub struct ExternalNote {
    pub position_id: String,
    pub message: String,
}

/// [`check_external_closes`] 的结论。
#[derive(Debug, Default)]
pub struct ExternalCloseCheck {
    /// 剩下的腿都已在交易所归零、且连续两次对账都如此：可以在台账里结束这笔仓位。
    pub adopt: Vec<String>,
    /// 两腿持仓里只有一条腿在交易所归零（强平、ADL 或手动平掉）、另一条与台账一致，且连续两次对账都如此：
    /// （仓位 id, 归零那条腿的场所）。调用方用 [`adopt_lone_legs`] 记下，再由退出重试平掉剩下的腿。
    pub lone: Vec<(String, Venue)>,
    /// 只对上一部分（数量对不上、还有没核对的查询或挂单），或还在等第二次确认：只说明，不处理。
    pub notes: Vec<ExternalNote>,
}

/// 补仓单在台账里没终态，至少「放过」这么久才去核实：补仓整个过程在 `live.lock` 里，持锁时看到没终态的补仓单
/// 本来就是上一次遗留的；再放一阵，是让交易所那边的订单历史（Lighter 撮合后约 1.1 秒才查得到，偶尔更久）先稳定下来。
pub const ADD_SETTLE_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// 一张补仓单被核实的结果（给日志与告警用）。
#[derive(Debug, Clone)]
pub struct AddSettlement {
    pub position_id: String,
    pub symbol: Symbol,
    pub order_id: String,
    pub outcome: crate::AddSettle,
}

/// 核实两腿持仓里**没有终态的补仓单、减仓单、补仓退单**（见 [`Executor::settle_add_order`]、
/// [`Executor::settle_reduce_order`]）：补仓中断、结果不明时订单会停在
/// `Pending`，对账随即报「本地记着这笔挂单、交易所没有」并冻结这个币，此前只有重启能解。
///
/// 调用方必须持有 `live.lock`，并应在**对账之前**调用：了结之后同一轮的对账就是干净的，不必多冻一轮。
/// `seen` 记着每张订单第一次被看到没终态的时刻，满 `after` 才去核实；已经了结的会被清掉。
/// 只碰 `Open` 的双腿仓位上、订单号是 `{仓位}-{buy|sell}-{add|trim|undo}{n}` 的单，其余（建仓、退出、回滚）各有各的路径。
/// 单张订单核实出错（台账读写失败）只记日志、不影响其它。
pub async fn settle_stale_adds(
    executor: &Executor,
    ledger: &Ledger,
    seen: &mut HashMap<String, std::time::Instant>,
    now: std::time::Instant,
    after: std::time::Duration,
) -> Result<Vec<AddSettlement>> {
    let (replayed, _) = ledger.replay().await?;
    let mut pending: Vec<(PairPosition, crate::types::OrderState)> = Vec::new();
    for position in replayed.exposed() {
        if position.status != PositionStatus::Open || !position.is_hedged() {
            continue;
        }
        for state in replayed.orders.values().filter(|state| {
            state.status.is_live()
                && matches!(
                    state.order.client_order_id.parse_for(&position.id),
                    Some((
                        _,
                        crate::types::OrderRole::Add { .. }
                            | crate::types::OrderRole::Trim { .. }
                            | crate::types::OrderRole::Undo { .. }
                    ))
                )
        }) {
            pending.push((position.clone(), state.clone()));
        }
    }
    pending.sort_by(|a, b| {
        a.1.order
            .client_order_id
            .0
            .cmp(&b.1.order.client_order_id.0)
    });
    seen.retain(|id, _| {
        pending
            .iter()
            .any(|(_, state)| &state.order.client_order_id.0 == id)
    });
    let mut settled = Vec::new();
    for (mut position, local) in pending {
        let id = local.order.client_order_id.0.clone();
        let first_seen = *seen.entry(id.clone()).or_insert(now);
        if now.saturating_duration_since(first_seen) < after {
            continue;
        }
        let is_add = matches!(
            local.order.client_order_id.parse_for(&position.id),
            Some((_, crate::types::OrderRole::Add { .. }))
        );
        let settled_one = if is_add {
            executor.settle_add_order(&mut position, &local).await
        } else {
            executor.settle_reduce_order(&mut position, &local).await
        };
        match settled_one {
            Ok(outcome) => settled.push(AddSettlement {
                position_id: position.id.clone(),
                symbol: position.symbol.clone(),
                order_id: id,
                outcome,
            }),
            Err(error) => {
                warn!(position = %position.id, order = %id, "核实补仓单没做成，下一轮再试：{error}");
            }
        }
    }
    Ok(settled)
}

/// 判断哪些仓位是在交易所被外部（手动）平掉的。**纯判断，不下单、不改台账。**
///
/// 只在下面全部成立时才建议结束一笔仓位：
/// - 状态是 `Open`（两腿俱在）或 `Closing`；
/// - 它剩下的**每一条腿**在对应场所都是「台账有仓、场所净数量为 0」；
/// - 这些场所没有 `Unverified`（查询失败不是零）、也没有挂单方面的不一致（游离的挂单还可能成交）；
/// - 上一次对账（至少 [`EXTERNAL_CLOSE_CONFIRM`] 之前）也是这样。
///
/// 一条腿没了、另一条还在，是裸敞口（2026-10-31 起）：`Open` 的双腿仓位、归零那条腿的场所查询正常、
/// 两家都没有游离挂单或这笔自己没了结的单、剩下那条腿与台账一致，且连续两次对账（相隔至少
/// [`EXTERNAL_CLOSE_CONFIRM`]）都如此，才列进 `lone`，由调用方记下并平掉剩下那条。那家场所上台账里**每一条**腿
/// 都同时「没有了」（至少两条）时不算：更像是查询返回了空列表，不是真的被强平。
/// `seen` 记着每笔候选第一次被发现的时刻；不再是候选的会被清掉。
pub fn check_external_closes(
    reconciliation: &Reconciliation,
    exposed: &[&PairPosition],
    seen: &mut HashMap<String, std::time::Instant>,
    now: std::time::Instant,
) -> ExternalCloseCheck {
    use crate::reconcile::DivergenceKind as Kind;
    let mut check = ExternalCloseCheck::default();
    let mut candidates = BTreeSet::new();
    for position in exposed {
        if !matches!(
            position.status,
            PositionStatus::Open | PositionStatus::Closing
        ) {
            continue;
        }
        let legs: Vec<&crate::LegFill> = [position.long.as_ref(), position.short.as_ref()]
            .into_iter()
            .flatten()
            .collect();
        if legs.is_empty() || (position.status == PositionStatus::Open && legs.len() < 2) {
            continue;
        }
        let symbol = position.symbol.to_string();
        // 场所层面的不一致（不看是哪个合约）：查询没核对、挂单对不上、还有台账之外的持仓
        // （可能是这笔仓位换了个币名还在那儿）—— 都不能当成「这笔平掉了」。
        let on = |kind: Kind, leg: &crate::LegFill| {
            reconciliation
                .divergences
                .iter()
                .any(|d| d.kind == kind && d.venue == Some(leg.venue))
        };
        let missing: Vec<&crate::LegFill> = legs
            .iter()
            .copied()
            .filter(|leg| {
                reconciliation.divergences.iter().any(|d| {
                    d.kind == Kind::PositionMissingOnVenue
                        && d.venue == Some(leg.venue)
                        && d.reference == symbol
                })
            })
            .collect();
        let mismatched: Vec<&crate::LegFill> = legs
            .iter()
            .copied()
            .filter(|leg| {
                reconciliation.divergences.iter().any(|d| {
                    d.kind == Kind::PositionMismatch
                        && d.venue == Some(leg.venue)
                        && d.reference == symbol
                })
            })
            .collect();
        let note = |message: String| ExternalNote {
            position_id: position.id.clone(),
            message,
        };
        if missing.is_empty() && mismatched.is_empty() {
            continue;
        }
        if position.status == PositionStatus::Open
            && legs.len() == 2
            && missing.len() == 1
            && mismatched.is_empty()
        {
            let gone = missing[0];
            let rest = legs
                .iter()
                .copied()
                .find(|leg| leg.venue != gone.venue)
                .unwrap_or(gone);
            let blocked: Vec<String> = legs
                .iter()
                .filter(|leg| {
                    on(Kind::Unverified, leg)
                        || on(Kind::UnknownOpenOrder, leg)
                        || on(Kind::OrderMissingOnVenue, leg)
                        || reconciliation.divergences.iter().any(|d| {
                            d.kind == Kind::PositionMismatch
                                && d.venue == Some(leg.venue)
                                && d.reference != symbol
                        })
                })
                .map(|leg| leg.venue.to_string())
                .collect();
            // 那家场所上台账里的腿全都「没有了」（至少两条）：多半是查询返回了空列表。
            let venue_legs = exposed
                .iter()
                .flat_map(|p| [p.long.as_ref(), p.short.as_ref()])
                .flatten()
                .filter(|leg| leg.venue == gone.venue)
                .count();
            let venue_missing = reconciliation
                .divergences
                .iter()
                .filter(|d| d.kind == Kind::PositionMissingOnVenue && d.venue == Some(gone.venue))
                .count();
            let wiped = venue_legs >= 2 && venue_missing >= venue_legs;
            if !blocked.is_empty() || wiped {
                check.notes.push(note(format!(
                    "{} 腿在交易所已没有仓位，{} 腿还在：现在是裸敞口。{}，暂不自动平剩下的腿，请到交易所核对",
                    gone.venue,
                    rest.venue,
                    if wiped {
                        format!("{} 上台账里的仓位全都读不到，像是查询返回了空列表", gone.venue)
                    } else {
                        format!("{} 上还有没核对完的查询、挂单或台账之外的持仓", blocked.join("、"))
                    }
                )));
                continue;
            }
            let key = format!("{}:lone", position.id);
            candidates.insert(key.clone());
            let first = *seen.entry(key).or_insert(now);
            // 那家场所上台账里只有这一条腿：「全没了」认不出是查询返回了空列表（上面的 `wiped` 要至少两条），
            // 读节点连着落后一分钟就会把对冲腿当裸腿平掉。多确认一会儿（读节点落后通常是几秒到十几秒）。
            let confirm = if venue_legs <= 1 {
                EXTERNAL_CLOSE_CONFIRM_SINGLE
            } else {
                EXTERNAL_CLOSE_CONFIRM
            };
            if now.duration_since(first) >= confirm {
                check.lone.push((position.id.clone(), gone.venue));
            } else {
                check.notes.push(note(format!(
                    "{} 腿在交易所已没有仓位（可能被强平），{} 腿还在：再对账确认（约 {} 分钟）就自动平掉剩下的腿",
                    gone.venue,
                    rest.venue,
                    confirm.as_secs().div_ceil(60)
                )));
            }
            continue;
        }
        if missing.len() < legs.len() {
            let gone: Vec<String> = missing.iter().map(|leg| leg.venue.to_string()).collect();
            let rest: Vec<String> = legs
                .iter()
                .filter(|leg| !missing.iter().any(|m| m.venue == leg.venue))
                .map(|leg| leg.venue.to_string())
                .collect();
            check.notes.push(note(if gone.is_empty() {
                format!(
                    "{} 上的数量与台账不符（可能被部分平掉或加过仓）：看板不会自动处理，请到交易所核对",
                    mismatched
                        .iter()
                        .map(|leg| leg.venue.to_string())
                        .collect::<Vec<_>>()
                        .join("、")
                )
            } else {
                format!(
                    "{} 腿已在交易所被外部平掉，{} 腿还在：现在是裸敞口。看板不会自动平剩下的腿，请你决定（手动平掉，或在持仓页平仓）",
                    gone.join("、"),
                    rest.join("、")
                )
            }));
            continue;
        }
        // 剩下的每条腿都已归零。先排除「没核对」和游离挂单。
        let blocked: Vec<String> = legs
            .iter()
            .filter(|leg| {
                on(Kind::Unverified, leg)
                    || on(Kind::UnknownOpenOrder, leg)
                    || reconciliation.divergences.iter().any(|d| {
                        // 这笔仓位自己的未了结订单「在场所不在了」不算：两条腿都已经没有仓位，那张单（多半是
                        // 一直读不到的退出单）不可能再改变什么，结束仓位时会一并了结。别的单仍然要挡。
                        d.kind == Kind::OrderMissingOnVenue
                            && d.venue == Some(leg.venue)
                            && !d.reference.starts_with(&format!("{}-", position.id))
                    })
                    || reconciliation.divergences.iter().any(|d| {
                        d.kind == Kind::PositionMismatch
                            && d.venue == Some(leg.venue)
                            && d.reference != symbol
                    })
            })
            .map(|leg| leg.venue.to_string())
            .collect();
        if !blocked.is_empty() {
            check.notes.push(note(format!(
                "两腿在交易所都已没有仓位，但 {} 上还有没核对完的查询、挂单或台账之外的持仓不一致，暂不在台账里结束",
                blocked.join("、")
            )));
            continue;
        }
        candidates.insert(position.id.clone());
        let first = *seen.entry(position.id.clone()).or_insert(now);
        if now.duration_since(first) >= EXTERNAL_CLOSE_CONFIRM {
            check.adopt.push(position.id.clone());
        } else {
            check.notes.push(note(
                "交易所里这笔的两条腿都已经没有仓位（像是手动平掉了）：再对账确认一次（约 1 分钟）就在台账里结束它"
                    .into(),
            ));
        }
    }
    seen.retain(|id, _| candidates.contains(id));
    check
}

/// 在台账里结束被外部平掉的仓位：记 `Closed`、清掉腿，备注写清楚是怎么知道的。
///
/// **不向任何场所下单。** 成交价没有记录，实际盈亏由 [`settle_closed_from_fills`] 稍后按成交记录核算；开仓以来的资金费
/// 从交易所的结算流水里查（查不到也如实写）。
pub async fn adopt_external_closes(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    exposed: &[&PairPosition],
    ids: &[String],
) -> Result<Vec<PairPosition>> {
    let mut adopted = Vec::new();
    let replayed = if ids.is_empty() {
        None
    } else {
        Some(ledger.replay().await?.0)
    };
    for id in ids {
        let Some(position) = exposed.iter().find(|position| &position.id == id) else {
            continue;
        };
        let mut position = (*position).clone();
        // 腿在下面会被清掉：先把开仓记录存下来，事后按成交核算实际盈亏要用。
        position.entry_legs = position
            .long
            .clone()
            .zip(position.short.clone())
            .map(|(long, short)| crate::EntryLegs { long, short });
        position.closed_externally = true;
        // 资金费按开仓时的两条腿算：平仓重试中只剩一条腿时，另一条（已经平掉的）也要算。
        // 2026-10-02 PONS 就是只算了剩下的 arcus，漏掉了 lighter-rh 付出的 2.83 USDT。
        let venues = crate::executor::position_venues(ledger, &position).await?;
        let now = chrono::Utc::now();
        let funding = crate::executor::funding_window(
            brokers,
            &venues,
            &position.symbol,
            position.opened_at,
            now,
        )
        .await;
        let funding_known = funding.is_some();
        let funding = funding.unwrap_or_default();
        position.realized_funding_usdt = funding_known.then_some(funding);
        position.note = Some(format!(
            "在交易所外部平仓：{} 对账发现两腿都已没有仓位，台账据此结束这笔。实际盈亏稍后按交易所的成交记录核算；开仓以来资金费{}",
            now.format("%Y-%m-%d %H:%M UTC"),
            if funding_known {
                format!(
                    " {} USDT（交易所结算流水）",
                    funding.round_dp(4).normalize()
                )
            } else {
                "没有查到".into()
            }
        ));
        position.long = None;
        position.short = None;
        position.status = PositionStatus::Closed;
        position.closed_at = Some(now);
        ledger
            .append(&crate::Record::Position(Box::new(position.clone())))
            .await?;
        // 这笔自己还没了结的订单（读不到的退出单、没确认的补仓单）一并了结：仓位已经没了，留着只会让对账
        // 永远报「本地有、交易所没有」。真还挂在交易所的会被对账当成台账外挂单报出来，不会被藏起来。
        // 实际成交以交易所成交记录为准（见 settle_closed_from_fills），这里的状态只是台账收尾。
        for state in replayed
            .iter()
            .flat_map(|r| r.orders.values())
            .filter(|state| {
                state.status.is_live()
                    && state
                        .order
                        .client_order_id
                        .parse_for(&position.id)
                        .is_some()
            })
        {
            let mut closed = state.clone();
            closed.status = crate::types::OrderStatus::Rejected;
            closed.reject_reason = Some(
                "仓位已被外部平掉，台账收尾时把这张没了结的单记为未成交（实际成交以交易所成交记录为准）"
                    .into(),
            );
            ledger
                .append(&crate::Record::Order(Box::new(closed)))
                .await?;
        }
        warn!(position = %position.id, symbol = %position.symbol, "对账发现仓位已在交易所外部平掉，台账已结束这笔");
        adopted.push(position);
    }
    Ok(adopted)
}

/// 记下「一条腿在交易所已没有仓位」的仓位（见 [`check_external_closes`] 的 `lone`）：清掉那条腿、转为 `Closing`，
/// 开仓时的两条腿存进 `entry_legs`，标成外部平仓。**本身不下单**：下一次退出重试（规则轮里 `Closing` 的仓位
/// 照常重试，见 `round_positions`）用只减仓单平掉剩下那条腿；平完后实际盈亏（含被强平那条腿的亏损）
/// 按交易所成交记录核算（[`settle_closed_from_fills`]）。
pub async fn adopt_lone_legs(
    ledger: &Ledger,
    exposed: &[&PairPosition],
    lone: &[(String, Venue)],
) -> Result<Vec<PairPosition>> {
    let mut adopted = Vec::new();
    for (id, gone) in lone {
        let Some(position) = exposed.iter().find(|position| &position.id == id) else {
            continue;
        };
        if position.status != PositionStatus::Open || !position.is_hedged() {
            continue;
        }
        let mut position = (*position).clone();
        position.entry_legs = position
            .long
            .clone()
            .zip(position.short.clone())
            .map(|(long, short)| crate::EntryLegs { long, short });
        position.closed_externally = true;
        let rest = if position.long.as_ref().is_some_and(|leg| leg.venue == *gone) {
            position.long = None;
            position.short.as_ref().map(|leg| leg.venue)
        } else if position
            .short
            .as_ref()
            .is_some_and(|leg| leg.venue == *gone)
        {
            position.short = None;
            position.long.as_ref().map(|leg| leg.venue)
        } else {
            continue;
        };
        let rest = rest.map_or_else(|| "另一".to_string(), |venue| venue.to_string());
        position.status = PositionStatus::Closing;
        position.note = Some(format!(
            "{gone} 腿在交易所已没有仓位（强平、ADL 或手动平掉），{} 对账连续两次确认；看板接着用只减仓单平掉剩下的 {rest} 腿。实际盈亏平完后按交易所成交记录核算",
            chrono::Utc::now().format("%Y-%m-%d %H:%M UTC")
        ));
        ledger
            .append(&crate::Record::Position(Box::new(position.clone())))
            .await?;
        warn!(position = %position.id, symbol = %position.symbol, %gone, "一条腿在交易所已没有仓位，转为平仓剩下的腿");
        adopted.push(position);
    }
    Ok(adopted)
}

/// 占用保证金的余量：手续费、标记价波动、交易所自己的保证金口径差异。
const COLLATERAL_BUFFER: Decimal = Decimal::from_parts(11, 0, 0, false, 1);

/// 一条腿的保证金核对结论。
#[derive(Debug, PartialEq, Eq)]
pub enum CollateralVerdict {
    Enough,
    /// 可用保证金不够。
    Short {
        free: Decimal,
        required: Decimal,
    },
    /// 场所没给可用保证金：只警告，不拦。
    Unknown,
}

/// 这条腿要占多少保证金：名义 ÷ 杠杆，再乘余量。
pub fn required_margin(notional: Decimal, leverage: Decimal) -> Decimal {
    notional / leverage.max(Decimal::ONE) * COLLATERAL_BUFFER
}

pub fn judge_collateral(free: Option<Decimal>, required: Decimal) -> CollateralVerdict {
    match free {
        None => CollateralVerdict::Unknown,
        Some(free) if free >= required => CollateralVerdict::Enough,
        Some(free) => CollateralVerdict::Short { free, required },
    }
}

/// 实盘开仓前核对两条腿的账户撑不撑得住这笔的保证金（盘口检查之后）。返回警告（查不到的场所）；
/// 不够就报错。**只读，不下单。** 关掉单笔上限之后这是防止「盘口够、账户不够」的最后一道：
/// 那种情况第一条腿会成交、第二条失败，被迫付一次回滚的手续费和滑点。
pub async fn verify_free_collateral(
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    prepared: &Prepared,
) -> Result<Vec<String>> {
    check_legs_collateral(
        brokers,
        &[
            (prepared.plan.long.venue, prepared.plan.long.notional_usdt),
            (prepared.plan.short.venue, prepared.plan.short.notional_usdt),
        ],
        prepared.leverage,
    )
    .await
}

/// [`verify_free_collateral`] 的核心：每条腿（场所, 名义）核对一次。
pub async fn check_legs_collateral(
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    legs: &[(Venue, Decimal)],
    leverage: Decimal,
) -> Result<Vec<String>> {
    // 各条腿的账户并发查（两个场所各一次往返），再按腿的顺序判断，警告与报错的顺序不变。
    let frees = futures_util::future::join_all(legs.iter().map(|&(venue, _)| async move {
        match brokers.get(&venue) {
            Some(broker) => Some(broker.free_collateral().await),
            None => None,
        }
    }))
    .await;
    let mut warnings = Vec::new();
    for (&(venue, notional), free) in legs.iter().zip(frees) {
        let Some(free) = free else {
            continue;
        };
        let required = required_margin(notional, leverage);
        let free = match free {
            Ok(free) => free,
            Err(error) => {
                warnings.push(format!("{venue} 的可用保证金没查成，没核对：{error}"));
                continue;
            }
        };
        match judge_collateral(free, required) {
            CollateralVerdict::Enough => {}
            CollateralVerdict::Unknown => warnings.push(format!(
                "{venue} 不给可用保证金，没核对这笔要占的约 {} USDT",
                required.round_dp(2)
            )),
            CollateralVerdict::Short { free, required } => bail!(
                "{venue} 账户可用保证金 {} USDT，这笔要占约 {} USDT（名义 {} ÷ {}x，含 10% 余量），不够。\
                 先充值或减小金额，否则会一条腿成交、另一条失败，被迫回滚",
                free.round_dp(2),
                required.round_dp(2),
                notional.round_dp(2),
                leverage.normalize()
            ),
        }
    }
    Ok(warnings)
}

/// 某一天（UTC）已实现盈亏的汇总，来自台账里当天结束的仓位。
#[derive(Debug, Clone, Serialize)]
pub struct DailyPnl {
    pub date: chrono::NaiveDate,
    /// 当天结束的仓位数（平仓与回滚）。
    pub closed: usize,
    /// 合计（价格盈亏 − 手续费 + 资金费）。**只要有一笔当天结束的仓位没有已实现盈亏记录，
    /// 就是 `None`** —— 不知道的不能按 0 算，不然亏损闸门就是摆设。
    pub net_usdt: Option<Decimal>,
    /// 没有盈亏记录的仓位 id。
    pub unknown: Vec<String>,
    /// 其中资金费没查到、按 0 计入的仓位数（金额小，但要说清楚）。
    pub funding_missing: usize,
}

/// 汇总台账里 `day`（UTC）结束的仓位的已实现盈亏。
///
/// 只算看板台账里的：交易所里手动开平、别的程序做的交易不在内。仍持有的仓位减仓落袋的
/// 部分（爆仓保护）也不在内，那部分记在保证金里。
pub fn daily_realized(replayed: &Replayed, day: chrono::NaiveDate) -> DailyPnl {
    let mut out = DailyPnl {
        date: day,
        closed: 0,
        net_usdt: Some(Decimal::ZERO),
        unknown: Vec::new(),
        funding_missing: 0,
    };
    let mut ended: Vec<&PairPosition> = replayed
        .positions
        .values()
        .filter(|position| {
            matches!(
                position.status,
                PositionStatus::Closed | PositionStatus::Unwound
            ) && position.closed_at.is_some_and(|at| at.date_naive() == day)
        })
        .collect();
    ended.sort_by(|a, b| a.id.cmp(&b.id));
    for position in ended {
        out.closed += 1;
        // 回滚掉、但它的订单一笔都没成交过的（第一腿被拒 / 没吃到）：盈亏确定是 0。
        // 旧版本没给这种仓位记来源，不在这里认出来的话，当日合计会一直是「未知」、拦住自动交易。
        if position.realized_source.is_none()
            && position.status == PositionStatus::Unwound
            && position.long.is_none()
            && position.short.is_none()
            && never_filled(replayed, &position.id)
        {
            continue;
        }
        if position.realized_source.is_none() {
            out.unknown.push(position.id.clone());
            out.net_usdt = None;
            continue;
        }
        if position.realized_funding_usdt.is_none() {
            out.funding_missing += 1;
        }
        if let Some(net) = out.net_usdt.as_mut() {
            *net += position.realized_pnl_usdt - position.realized_fee_usdt
                + position.realized_funding_usdt.unwrap_or_default();
        }
    }
    out.net_usdt = out.net_usdt.map(|net| net.round_dp(8));
    out
}

/// 这笔仓位的订单（订单号前缀 `{仓位}-`）在台账里一笔成交都没有。
fn never_filled(replayed: &Replayed, position_id: &str) -> bool {
    let prefix = format!("{position_id}-");
    !replayed.orders.values().any(|state| {
        state.order.client_order_id.0.starts_with(&prefix) && state.filled_usdt > Decimal::ZERO
    })
}

/// 一笔外部平仓的实际盈亏这一次核算的结果。
#[derive(Debug)]
pub enum SettleOutcome {
    /// 核算出来了，已写进台账。
    Settled(Box<PairPosition>),
    /// 这次没核出来，可能是成交记录还没出现或还没对齐：过一会儿再试。
    Retry(String),
    /// 不可能核出来（场所没接入成交记录、缺开仓记录……），别再试了。
    Impossible(String),
}

/// 事后按交易所的成交记录核算一笔已平仓仓位的实际盈亏，写回台账。**只读交易所、不下任何单。**
/// 外部平仓的，和早于逐笔记账功能上线、当时没记下盈亏的（比如 NEAR）都走这里。
///
/// 与识别平仓（[`adopt_external_closes`]）解耦：识别不依赖成交记录能不能查到。成功就在
/// 台账追加一条同 id 的 `Closed` 记录（重放时取最新，敞口不变）；数量对不上等情况按
/// [`crate::settlement`] 的规则拒绝归因，如实返回原因。
pub async fn settle_closed_from_fills(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    position: &PairPosition,
) -> Result<SettleOutcome> {
    let entry = match &position.entry_legs {
        Some(entry) => entry.clone(),
        // 更早识别的外部平仓没有存开仓记录：回台账历史里找最后一条两腿俱在的。
        None => match ledger
            .position_history(&position.id)
            .await?
            .into_iter()
            .rev()
            .find_map(|record| record.long.zip(record.short))
        {
            Some((long, short)) => crate::EntryLegs { long, short },
            None => {
                return Ok(SettleOutcome::Impossible(
                    "台账里找不到这笔仓位开仓时的两条腿".into(),
                ));
            }
        },
    };
    let from = position.opened_at;
    let until = position.closed_at.unwrap_or_else(chrono::Utc::now);
    let mut fills = Vec::new();
    for leg in [&entry.long, &entry.short] {
        let Some(broker) = brokers.get(&leg.venue) else {
            return Ok(SettleOutcome::Retry(format!("{} 没有连接实盘", leg.venue)));
        };
        match broker.fills_between(&position.symbol, from, until).await {
            Ok(Some(rows)) => fills.push(rows),
            Ok(None) => {
                return Ok(SettleOutcome::Impossible(format!(
                    "{} 还没接入成交记录",
                    leg.venue
                )));
            }
            Err(error) => {
                return Ok(SettleOutcome::Retry(format!(
                    "{} 的成交记录没查成：{error}",
                    leg.venue
                )));
            }
        }
    }
    let settlement = match crate::settlement::settle(&entry, &fills[0], &fills[1], from, until) {
        Ok(settlement) => settlement,
        Err(reason) => return Ok(SettleOutcome::Retry(reason)),
    };
    // 资金费按开仓时的两条腿、[开仓, 平仓] 窗口重新查：不沿用识别外部平仓时记下的数
    // （那时可能只查了一条腿、或没有截止时间）。查不到时才退回那个数。
    let venues = [entry.long.venue, entry.short.venue];
    let funding = match crate::executor::funding_window(
        brokers,
        &venues,
        &position.symbol,
        position.opened_at,
        until,
    )
    .await
    {
        Some(funding) => Some(funding),
        None => position.realized_funding_usdt,
    };
    let mut settled = position.clone();
    settled.realized_pnl_usdt = settlement.price_pnl_usdt();
    settled.realized_fee_usdt = settlement.fees_usdt();
    settled.realized_funding_usdt = funding;
    settled.realized_source = Some(crate::RealizedSource::VenueFills);
    settled.pnl_unattributed = None;
    settled.note = Some(if position.is_external_close() {
        format!(
            "在交易所外部平仓（{}{}）；{}",
            until.format("%Y-%m-%d %H:%M UTC"),
            if position.realized_pnl_usdt.is_zero() && position.realized_fee_usdt.is_zero() {
                " 对账发现两腿都已没有仓位"
            } else {
                "：一条腿在交易所没了（强平、ADL 或手动），剩下的由看板平掉"
            },
            settlement.describe(funding)
        )
    } else {
        // 看板自己平的、但当时没有逐笔记账（早于记账功能上线）：事后按成交记录补上。
        format!(
            "已平仓（{}）；{}",
            until.format("%Y-%m-%d %H:%M UTC"),
            settlement.describe(funding)
        )
    });
    ledger
        .append(&crate::Record::Position(Box::new(settled.clone())))
        .await?;
    warn!(position = %settled.id, "外部平仓的实际盈亏已按成交记录核出并写进台账");
    Ok(SettleOutcome::Settled(Box::new(settled)))
}

/// 一张订单的手续费按交易所成交记录改正了（见 [`correct_estimated_fees`]）。
#[derive(Debug, Clone)]
pub struct FeeCorrection {
    pub position_id: String,
    pub symbol: Symbol,
    pub order_id: String,
    pub venue: Venue,
    pub recorded: Decimal,
    pub actual: Decimal,
}

/// 是不是要按成交记录改正手续费的订单：已有终态、有成交，且手续费是估的 —— 有 `fee_estimated` 标记，或者
/// （标记上线前的 Hyperliquid 系订单）手续费正好等于「成交额 × 配置的吃单费率」，那是按回包估的数。
fn fee_needs_check(state: &crate::types::OrderState, taker: Option<Decimal>) -> bool {
    if state.status.is_live() || state.filled_usdt <= Decimal::ZERO {
        return false;
    }
    state.fee_estimated
        || (matches!(
            state.order.venue,
            Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo
        ) && taker.is_some_and(|rate| {
            (state.fee_usdt - state.filled_usdt * rate).abs() < Decimal::new(1, 6)
        }))
}

/// 读回的成交额与台账差多少以内才认（比例）：回包记的均价往不利方向放过一个最小单位，成交额会差一点点。
const FEE_CHECK_NOTIONAL_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 2);

/// 把台账里**估算的手续费**按交易所成交记录改正（2026-10-31：Hyperliquid 按下单回包先记成交时按吃单费率 0.09%
/// 估，读节点落后读不到就一直留着；17 单实际只收 0.0086%，台账多记 3.86 USDT）。
///
/// 每次最多核 `limit` 张；`checked` 记着这次进程里核过、不用再核的（读回的费用与台账一样、或认不出属于哪笔仓位）。
/// 改正时追加订单的新状态（`fee_estimated` 清掉），再按这张单作用在哪条腿上改仓位：腿还在就改腿上的手续费，
/// 腿已经平掉就改已实现手续费；按交易所成交记录核算过的已平仓仓位（`VenueFills`）只改订单。
/// 正在执行中的仓位（建仓、平仓、回滚中）这次跳过，下次再核。**只读交易所、不下单。** 调用方必须持有交易台的锁。
pub async fn correct_estimated_fees(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    checked: &mut HashSet<String>,
    limit: usize,
) -> Result<Vec<FeeCorrection>> {
    let (replayed, _) = ledger.replay().await?;
    let mut candidates: Vec<&crate::types::OrderState> = replayed
        .orders
        .values()
        .filter(|state| !checked.contains(&state.order.client_order_id.0))
        .filter(|state| {
            let taker = brokers
                .get(&state.order.venue)
                .map(|broker| broker.fee_per_side());
            fee_needs_check(state, taker)
        })
        .collect();
    candidates.sort_by(|a, b| a.order.client_order_id.0.cmp(&b.order.client_order_id.0));
    let mut corrections = Vec::new();
    // 这一批里已经改过的仓位：同一笔仓位的两张单（两条腿都在 Hyperliquid 上）要在上一次改过的基础上改，
    // 不能都拿批次开头重放的旧仓位改 —— 后写的会把先写的改动盖掉（实盘遇到过，少改了几分钱）。
    let mut updated_positions: HashMap<String, PairPosition> = HashMap::new();
    for local in candidates.into_iter().take(limit) {
        let id = local.order.client_order_id.0.clone();
        let Some((position, (side, role))) = replayed.positions.values().find_map(|position| {
            local
                .order
                .client_order_id
                .parse_for(&position.id)
                .map(|parsed| (position, parsed))
        }) else {
            checked.insert(id);
            continue;
        };
        if !matches!(
            position.status,
            PositionStatus::Open | PositionStatus::Closed | PositionStatus::Unwound
        ) {
            continue;
        }
        let Some(broker) = brokers.get(&local.order.venue) else {
            continue;
        };
        let remote = match broker.order_state(&local.order.client_order_id).await {
            Ok(Some(remote)) => remote,
            Ok(None) => {
                warn!(order = %id, "改正估算的手续费：交易所查不到这张单");
                continue;
            }
            Err(error) => {
                warn!(order = %id, "改正估算的手续费：读回没成功，下次再试：{error}");
                continue;
            }
        };
        let close_enough = (remote.filled_usdt - local.filled_usdt).abs()
            <= local.filled_usdt * FEE_CHECK_NOTIONAL_TOLERANCE;
        if remote.fee_estimated || remote.status.is_live() || !close_enough {
            warn!(
                order = %id,
                local = %local.filled_usdt,
                remote = %remote.filled_usdt,
                estimated = remote.fee_estimated,
                "改正估算的手续费：读回的成交与台账对不上或还是估的，这次不改"
            );
            continue;
        }
        let delta = remote.fee_usdt - local.fee_usdt;
        let mut fixed = local.clone();
        fixed.fee_usdt = remote.fee_usdt;
        fixed.fee_estimated = false;
        ledger
            .append(&crate::Record::Order(Box::new(fixed)))
            .await?;
        checked.insert(id.clone());
        if delta.is_zero() {
            continue;
        }
        if position.realized_source != Some(crate::RealizedSource::VenueFills) {
            let mut updated = updated_positions
                .get(&position.id)
                .cloned()
                .unwrap_or_else(|| position.clone());
            let is_long = role.is_long_leg(side);
            let leg = if is_long {
                updated.long.as_mut()
            } else {
                updated.short.as_mut()
            }
            .filter(|leg| leg.venue == local.order.venue);
            match leg {
                Some(leg) => leg.fee_usdt += delta,
                None => updated.realized_fee_usdt += delta,
            }
            if updated.status != PositionStatus::Open {
                let line = format!(
                    "{} 订单 {id} 的手续费按交易所成交记录更正 {} → {} USDT",
                    local.order.venue,
                    local.fee_usdt.round_dp(4).normalize(),
                    remote.fee_usdt.round_dp(4).normalize()
                );
                updated.note = Some(match updated.note.take() {
                    Some(note) if !note.is_empty() => format!("{note}；{line}"),
                    _ => line,
                });
            }
            ledger
                .append(&crate::Record::Position(Box::new(updated.clone())))
                .await?;
            updated_positions.insert(updated.id.clone(), updated);
        }
        warn!(position = %position.id, order = %id, recorded = %local.fee_usdt, actual = %remote.fee_usdt, "估算的手续费已按交易所成交记录改正");
        corrections.push(FeeCorrection {
            position_id: position.id.clone(),
            symbol: position.symbol.clone(),
            order_id: id,
            venue: local.order.venue,
            recorded: local.fee_usdt,
            actual: remote.fee_usdt,
        });
    }
    Ok(corrections)
}

/// 最终核不出来：把原因记进台账，别再一遍遍试。
pub async fn mark_pnl_unattributed(
    ledger: &Ledger,
    position: &PairPosition,
    reason: &str,
) -> Result<()> {
    let mut marked = position.clone();
    marked.pnl_unattributed = Some(reason.to_string());
    ledger
        .append(&crate::Record::Position(Box::new(marked)))
        .await?;
    Ok(())
}

/// 平仓后等多久再核对资金费：两家结算流水出现的时间可能差几十秒到几分钟。
pub const FUNDING_RECHECK_AFTER: chrono::Duration = chrono::Duration::minutes(10);

/// 一次资金费核对的结果。
#[derive(Debug, Clone, PartialEq)]
pub enum FundingCheck {
    /// 还没到时候、已经核对过、或不是已平仓的实盘仓位。
    NotDue,
    /// 与台账一致，只记下核对时刻。
    Confirmed,
    /// 不一致，已更正：（旧值, 新值）。
    Corrected(Option<Decimal>, Decimal),
    /// 这次查不到（场所没连上 / 流水没查成），下次再试，台账不动。
    Unavailable,
}

/// 已平仓仓位的资金费事后核对：按开仓时的两条腿、[开仓, 平仓] 窗口重新查交易所流水。
///
/// 一致就只追加一条带 `funding_checked_at` 的记录；不一致就追加更正记录（重放取最新），
/// 并在备注里写明旧值、新值与修正后的净额。只追加，不改写任何旧行。**只读交易所、不下单。**
///
/// `force`：已经核对过的也再核一次（修正历史记录时用）。
pub async fn recheck_funding(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    position: &PairPosition,
    now: chrono::DateTime<chrono::Utc>,
    force: bool,
) -> Result<FundingCheck> {
    let Some(closed_at) = position.closed_at else {
        return Ok(FundingCheck::NotDue);
    };
    // 只核有两条腿成交的已平仓仓位：回滚掉的（Unwound）和外部平仓还没核算出价格盈亏的不核。
    if position.status != PositionStatus::Closed
        || position.realized_source.is_none()
        || (!force && position.funding_checked_at.is_some())
        || (!force && now < closed_at + FUNDING_RECHECK_AFTER)
    {
        return Ok(FundingCheck::NotDue);
    }
    let venues = crate::executor::position_venues(ledger, position).await?;
    let Some(actual) = crate::executor::funding_window(
        brokers,
        &venues,
        &position.symbol,
        position.opened_at,
        closed_at,
    )
    .await
    else {
        return Ok(FundingCheck::Unavailable);
    };
    let actual = actual.round_dp(9);
    let mut checked = position.clone();
    checked.funding_checked_at = Some(now);
    let previous = position.realized_funding_usdt;
    // 小于 0.0001 USDT 的差异是小数位舍入，不算不一致。
    let same = previous.is_some_and(|old| (old - actual).abs() < Decimal::new(1, 4));
    let outcome = if same {
        FundingCheck::Confirmed
    } else {
        checked.realized_funding_usdt = Some(actual);
        let net = |funding: Decimal| {
            (position.realized_pnl_usdt - position.realized_fee_usdt + funding)
                .round_dp(4)
                .normalize()
        };
        let correction = format!(
            "资金费已按交易所流水更正（{} 核对，两条腿、开仓至平仓）：{} → {} USDT，净额 {} → {} USDT",
            now.format("%Y-%m-%d %H:%M UTC"),
            previous.map_or("未知".to_string(), |old| old
                .round_dp(4)
                .normalize()
                .to_string()),
            actual.round_dp(4).normalize(),
            previous.map_or("未知".to_string(), |old| net(old).to_string()),
            net(actual),
        );
        checked.note = Some(match &position.note {
            Some(note) if !note.is_empty() => format!("{note}；{correction}"),
            _ => correction,
        });
        FundingCheck::Corrected(previous, actual)
    };
    ledger
        .append(&crate::Record::Position(Box::new(checked)))
        .await?;
    Ok(outcome)
}

/// 仓位两条腿涉及的场所。
fn position_venues(positions: &[PairPosition]) -> BTreeSet<Venue> {
    positions
        .iter()
        .flat_map(|position| [position.long.as_ref(), position.short.as_ref()])
        .flatten()
        .map(|leg| leg.venue)
        .collect()
}

/// 平掉台账里的一笔仓位；也用于重试停在 `Closing` / `Unwinding` 的仓位。
///
/// 执行出错时仍返回仓位的最新状态与错误，由调用方决定怎么展示 —— 不重试。
pub async fn close(
    executor: &Executor,
    ledger: &Ledger,
    position_id: &str,
) -> Result<(PairPosition, Option<String>)> {
    let (replayed, _) = ledger.replay().await?;
    let Some(mut position) = replayed.positions.get(position_id).cloned() else {
        bail!("台账里没有仓位 {position_id}");
    };
    if !position.status.has_exposure() {
        bail!("仓位 {position_id} 已是 {:?}，没有敞口", position.status);
    }
    let error = executor
        .close(&mut position)
        .await
        .err()
        .map(|error| error.to_string());
    Ok((position, error))
}

/// 改一笔**已开仓位**的规则（整套替换）。开仓时没勾的规则，开仓后也能加上。
///
/// 只有两腿都在的 `Open` 仓位能改：`Opening` / `Closing` / `Unwinding` 正在执行，规则不起作用。
/// 数值与开仓时共用校验；允许当前已经触发，调用方须先评估并取得显式确认。
/// `current_distance_pct` 拿不到传 `None`，强平类规则被拒；自动加保证金还要两腿场所都接入。
/// 规则没变就不写台账。返回（仓位，是否改了）。
///
/// 调用方必须持有交易台的锁：读台账、改、追加是三步，不能和一轮规则或下单交错。
pub async fn update_rules(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    position_id: &str,
    rules: TaskRules,
    current_distance_pct: Option<Decimal>,
) -> Result<(PairPosition, bool)> {
    let (replayed, _) = ledger.replay().await?;
    let Some(mut position) = replayed.positions.get(position_id).cloned() else {
        bail!("台账里没有仓位 {position_id}");
    };
    if position.status != PositionStatus::Open || !position.is_hedged() {
        bail!(
            "只有两腿都在的 Open 仓位能改规则（{position_id} 现在是 {:?}）",
            position.status
        );
    }
    crate::margin::validate_rules(position.margin_mode, &rules).map_err(anyhow::Error::msg)?;
    crate::monitor::validate_rules_update(&rules, current_distance_pct)
        .map_err(|reason| anyhow::anyhow!("规则不成立：{reason}"))?;
    if rules.auto_margin().is_some() {
        for leg in [position.long.as_ref(), position.short.as_ref()]
            .into_iter()
            .flatten()
        {
            let supported = brokers
                .get(&leg.venue)
                .is_some_and(|broker| broker.supports_add_margin());
            if !supported {
                bail!("{} 没有接入补保证金，不能开启自动加保证金", leg.venue);
            }
        }
    }
    if rules == position.rules {
        return Ok((position, false));
    }
    position.rules = rules;
    ledger
        .append(&Record::Position(Box::new(position.clone())))
        .await?;
    Ok((position, true))
}

/// 一笔仓位在一轮监控里发生了什么。
#[derive(Debug, Clone, Serialize)]
pub struct MonitorReport {
    pub position_id: String,
    pub symbol: Symbol,
    pub evaluation: Option<Evaluation>,
    /// 这一轮是否向券商发了单（平仓、减仓或重试退出），或续上了中断的建仓（见 `resumed`）。
    pub executed: bool,
    /// 上一次执行没走完（`Opening` / `Closing` / `Unwinding`），这一轮重试退出。
    pub retried_exit: bool,
    /// 没评估或没执行的原因。
    pub skipped: Option<String>,
    /// 执行失败的原因。
    pub error: Option<String>,
    /// 需要人看一眼的事（给告警用）：自动加保证金没补成 / 结果不明 / 上限用完仍在逼近强平。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attention: Option<String>,
    /// 平仓限价保护先发的那条腿没成交，两条腿都没动、继续持有（[`Executor::close_guarded`]）：原因。
    /// 发了单但不是「规则已执行」，也不是失败。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_held: Option<String>,
    pub status: PositionStatus,
    pub note: Option<String>,
    /// 中断的建仓这一轮续上收尾了（转为持仓、记为没成交、或补不齐回滚了，见 `status` 与 `note`）。
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub resumed: bool,
    /// 续上建仓这一轮刚把开仓单的结局、腿补进台账，还差第二腿：调用方马上再对一次账，再跑
    /// [`resume_openings`]。
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub reverify: bool,
}

impl MonitorReport {
    fn new(position: &PairPosition) -> Self {
        Self {
            position_id: position.id.clone(),
            symbol: position.symbol.clone(),
            evaluation: None,
            executed: false,
            retried_exit: false,
            skipped: None,
            error: None,
            attention: None,
            exit_held: None,
            status: position.status,
            note: position.note.clone(),
            resumed: false,
            reverify: false,
        }
    }

    fn finish(mut self, position: &PairPosition) -> Self {
        self.status = position.status;
        self.note = position.note.clone();
        self
    }
}

/// 基差收敛触发后，平仓前核对两边盘口时拉几档：与开仓一致。
const EXIT_DEPTH_LEVELS: u32 = 20;

/// 获利平仓（止盈、基差收敛）触发后，隔多久重拉盘口复核一次。
///
/// 触发用的是一次盘口快照，而盘口价差每秒都在跳：实盘观察到，实际成交的
/// 平仓价差几乎总是比触发时预估的差；触发时预估净赚的几笔，实际成交后可能净亏，
/// 其中有几笔触发前那些轮的预估均值本来就是负的，成交落回那个水平 ——
/// 在噪声的上沿开枪。隔一会儿重拉、仍然达标才平，能挡掉只持续一瞬的尖峰；真有的价差
/// （比如 某合约 那笔，预估 +1.05、成交也是 +1.05）不会因此丢。
pub const EXIT_CONFIRM_GAP: std::time::Duration = std::time::Duration::from_millis(700);

/// 平仓限价保护的限价：复核盘口上这笔数量要吃到的最差一档，再让出这么多（0.05%）。
///
/// 让出的是盘口在复核到下单之间的正常抖动、和交易所按 tick 保守取整（Hyperliquid 卖单向上、买单向下，最多一个 tick，
/// 5 位有效数字约 0.001%～0.01%）；挡的是 HL-xyz 腿上 −0.05% ～ −0.19% 的变差。
pub const EXIT_GUARD_BUFFER: Decimal = Decimal::from_parts(5, 0, 0, false, 4);

/// 获利平仓要不要先发一条腿的限价、按什么价（见 [`Executor::close_guarded`]）：两条腿里**只有一条**在 Hyperliquid
/// （主站、xyz、io）时保护它 —— 平仓滑点几乎都出在这几家（实盘观察：HL-xyz 居多，
/// Lighter、Arcus 不比预估差）。两条都在或都不在 Hyperliquid：不保护，照旧两条腿同时按市价平。
pub fn exit_guard(
    position: &PairPosition,
    quote: &crate::monitor::ExitQuote,
) -> Option<crate::executor::ExitGuard> {
    let hyperliquid = |leg: Option<&crate::types::LegFill>| {
        leg.is_some_and(|leg| {
            matches!(
                leg.venue,
                Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo
            )
        })
    };
    let (long, short) = (
        hyperliquid(position.long.as_ref()),
        hyperliquid(position.short.as_ref()),
    );
    let (side, limit_price) = match (long, short) {
        // 多腿卖出：不低于最差那一档再让 0.05%。
        (true, false) => (
            Side::Sell,
            quote.long_worst_price * (Decimal::ONE - EXIT_GUARD_BUFFER),
        ),
        // 空腿买回：不高于最差那一档再加 0.05%。
        (false, true) => (
            Side::Buy,
            quote.short_worst_price * (Decimal::ONE + EXIT_GUARD_BUFFER),
        ),
        _ => return None,
    };
    (limit_price > Decimal::ZERO).then_some(crate::executor::ExitGuard { side, limit_price })
}

/// 平仓成交后，实际净额比盘口预估差超过这个数（USDT）就按警告记。
const EXIT_SHORTFALL_WARN_USDT: Decimal = Decimal::from_parts(3, 0, 0, false, 1);

/// 复核时两份盘口到达时间差的上限（毫秒）。超过就说明一边在排队、另一边早回来了，
/// 两份报价不是同一时刻的，算出来的价差里混着一段时间的价格变动：不当作复核通过。
const EXIT_CONFIRM_MAX_SKEW_MS: u64 = 1500;

/// 开仓以来这笔仓位已收付的资金费（止盈要用）的来源。
#[async_trait]
pub trait FundingSource: Send + Sync {
    /// 合计（正 = 收到）。`None` = 查不到（场所没接入、查询失败）：调用方**不能**当 0 用。
    async fn funding_usdt(&self, position: &PairPosition) -> Option<Decimal>;
}

/// 纸面：不结算资金费，台账里的盈亏本来就不含它，按 0 算与台账一致。
pub struct PaperFunding;

#[async_trait]
impl FundingSource for PaperFunding {
    async fn funding_usdt(&self, _position: &PairPosition) -> Option<Decimal> {
        Some(Decimal::ZERO)
    }
}

/// 直接问各券商的结算流水（不缓存；命令行用。看板有自己带缓存的实现）。
#[async_trait]
impl FundingSource for Executor {
    async fn funding_usdt(&self, position: &PairPosition) -> Option<Decimal> {
        let (long, short) = (position.long.as_ref()?, position.short.as_ref()?);
        self.funding_total(
            &[long.venue, short.venue],
            &position.symbol,
            position.opened_at,
        )
        .await
    }
}

/// 一轮监控里各笔仓位共用的外部输入。
pub struct RoundCtx<'a> {
    /// 资金费流水来源（止盈要用）。
    pub funding: &'a dyn FundingSource,
    /// 退避状态：没走完的退出、补保证金。
    pub retries: &'a ExitRetries,
    /// 获利平仓下单前复核盘口的等待时间（见 [`EXIT_CONFIRM_GAP`]）。测试里给 0。
    pub exit_confirm_gap: std::time::Duration,
}

/// 补保证金成功（或结果不明）后多久内不再补同一笔：交易所的保证金读数要一会儿才更新，
/// 马上重算会把同一份缺口再补一遍。
const TOP_UP_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(90);
/// 补保证金时最多动用账户可用资金的多少：留一成，别把账户抽干。
const FREE_COLLATERAL_USE: Decimal = Decimal::from_parts(9, 0, 0, false, 1);

/// 一次自动加保证金的结局。
enum TopUp {
    /// 交易所确认到账。
    Applied,
    /// 结果不明：已按已补计入上限，下一轮按交易所的真实保证金核对。
    Unknown(String),
    /// 没补成（退避中、账户可用资金不够、被拒、没发出）：由调用方退回减仓。
    Unavailable(String),
}

/// 先看账户可用资金与退避，再补。补不了的原因原样带回去。
async fn top_up(
    executor: &Executor,
    position: &mut PairPosition,
    ctx: &RoundCtx<'_>,
    venue: Venue,
    wanted: Decimal,
    reason: &str,
) -> TopUp {
    use rust_decimal::RoundingStrategy::ToZero;
    let key = format!("{}:margin", position.id);
    let now = std::time::Instant::now();
    if !ctx.retries.due(&key, now) {
        let failures = ctx.retries.failures(&key);
        return TopUp::Unavailable(if failures == 0 {
            "刚补过一次，等交易所的保证金读数更新（冷却中）".to_string()
        } else {
            format!("上一次补保证金没成功（连续失败 {failures} 次），退避中")
        });
    }
    let mut amount = wanted;
    // 账户可用资金：知道不够就只补它的九成，连 1 USDT 都补不出来就不补了；查不到不拦（交易所自己会拒）。
    if let Some(free) = executor.free_collateral(venue).await {
        let usable = (free * FREE_COLLATERAL_USE).round_dp_with_strategy(2, ToZero);
        if usable < MIN_TOP_UP_USDT {
            ctx.retries.failed(&key, now);
            return TopUp::Unavailable(format!(
                "{venue} 账户可用保证金只有 {} USDT，不够补",
                free.round_dp(2)
            ));
        }
        amount = amount.min(usable);
    }
    match executor.add_margin(position, venue, amount, reason).await {
        Ok(crate::broker::MarginOutcome::Applied) => {
            ctx.retries.hold(&key, now, TOP_UP_COOLDOWN);
            TopUp::Applied
        }
        Ok(crate::broker::MarginOutcome::Unknown(why)) => {
            ctx.retries.hold(&key, now, TOP_UP_COOLDOWN);
            TopUp::Unknown(why)
        }
        Ok(crate::broker::MarginOutcome::Refused(why)) => {
            ctx.retries.failed(&key, now);
            TopUp::Unavailable(why)
        }
        Err(error) => {
            ctx.retries.failed(&key, now);
            TopUp::Unavailable(error.to_string())
        }
    }
}

/// 平仓决定当时，标记价口径与盘口口径之间的差额拆成哪几项（见 [`crate::monitor::ExitGap`]）。
/// 缺标记价或盘口中间价算不出来时说明算不出，不留空。
fn exit_gap_text(
    position: &PairPosition,
    observation: &crate::monitor::Observation,
    quote: &crate::monitor::ExitQuote,
) -> String {
    observation
        .long
        .mark_price
        .zip(observation.short.mark_price)
        .and_then(|(long_mark, short_mark)| quote.gap(position, long_mark, short_mark))
        .map_or_else(
            || "差额拆分算不出（缺标记价或盘口中间价）".to_string(),
            |gap| gap.describe(),
        )
}

/// 实际成交比盘口预估差在哪：多腿卖价、空腿买回价各差多少 USDT，手续费差多少。三项加起来就是「实际净 − 预估净」。
/// 台账里找不到这次平仓的成交记录时说明找不到。
fn exit_shortfall_text(
    quote: &crate::monitor::ExitQuote,
    fills: Option<&crate::breakdown::ExitFills>,
) -> String {
    let Some(fills) = fills else {
        return "差在哪：台账里没找到这次平仓的成交记录".to_string();
    };
    let r = |value: Decimal| value.round_dp(4).normalize();
    // 多腿卖出：成交价高于预估是好事；空腿买回：成交价低于预估是好事。
    let long = fills
        .long
        .map(|fill| (fill.price - quote.long_price) * fill.quantity);
    let short = fills
        .short
        .map(|fill| (quote.short_price - fill.price) * fill.quantity);
    let fee: Decimal = fills
        .long
        .iter()
        .chain(fills.short.iter())
        .map(|f| f.fee_usdt)
        .sum();
    let leg = |name: &str,
               est: Decimal,
               fill: Option<crate::breakdown::ExitFill>,
               delta: Option<Decimal>| {
        match (fill, delta) {
            (Some(fill), Some(delta)) => format!(
                "{name}预估 {} → 实际 {}（{} USDT）",
                est.normalize(),
                fill.price.normalize(),
                r(delta)
            ),
            _ => format!("{name}没有成交记录"),
        }
    };
    format!(
        "差在哪：{}；{}；平仓手续费预估 {} → 实际 {}（{} USDT）",
        leg("多腿卖价", quote.long_price, fills.long, long),
        leg("空腿买回价", quote.short_price, fills.short, short),
        r(quote.exit_fee_usdt),
        r(fee),
        r(quote.exit_fee_usdt - fee)
    )
}

/// 执行平仓 / 减仓。`None` = 做不了（减仓缺标记价）；`Some(Ok(Some(原因)))` = 平仓限价保护先发的那条腿
/// 没成交，两条腿都没动、继续持有（见 [`Executor::close_guarded`]）。
async fn run_exit_action(
    executor: &Executor,
    position: &mut PairPosition,
    observation: &crate::monitor::Observation,
    action: &Action,
    guard: Option<crate::executor::ExitGuard>,
) -> Option<ArbResult<Option<String>>> {
    match action {
        Action::Close { reason } => {
            // 平仓决定当时按盘口估的数，留到日志里：事后能逐笔比「估算 vs 实际成交」，查滑点出在哪。
            match observation.exit.as_ref() {
                Some(quote) => warn!(
                    position = %position.id,
                    symbol = %position.symbol,
                    est_long_price = %quote.long_price,
                    est_short_price = %quote.short_price,
                    est_exit_basis_pct = %quote.exit_basis_pct,
                    est_net_usdt = %quote.net_usdt.round_dp(4),
                    est_funding_usdt = ?observation.funding_usdt.map(|f| f.round_dp(4)),
                    est_gap = %exit_gap_text(position, observation, quote),
                    "规则平仓：按盘口估算（成交后对照台账的实际价）：{reason}"
                ),
                None => warn!(
                    position = %position.id,
                    symbol = %position.symbol,
                    "规则平仓（没有盘口估算）：{reason}"
                ),
            }
            let result = match guard {
                Some(guard) => match executor.close_guarded(position, guard).await {
                    Ok(crate::executor::GuardedExit::Held(why)) => {
                        warn!(
                            position = %position.id,
                            symbol = %position.symbol,
                            limit_price = %guard.limit_price,
                            "平仓限价保护：先发的那条腿没成交，两条腿都没动，继续持有：{why}"
                        );
                        return Some(Ok(Some(why)));
                    }
                    Ok(crate::executor::GuardedExit::Closed) => Ok(()),
                    Err(error) => Err(error),
                },
                None => executor.close(position).await,
            };
            // 成交后立刻对账：估算时的净额 vs 台账里实际落下的净额（都不含资金费）。差多少、差在哪条腿，
            // 是调平仓规则的唯一依据；只靠事后翻台账，要等亏了好几笔才看得出来。
            if result.is_ok()
                && position.status == PositionStatus::Closed
                && let Some(quote) = observation.exit.as_ref()
            {
                let actual = position.realized_pnl_usdt - position.realized_fee_usdt;
                let shortfall = actual - quote.net_usdt;
                let fills = executor.last_exit_fills(position).await;
                let message = format!(
                    "平仓对账：盘口预估净 {} USDT，实际成交净 {} USDT，差 {} USDT（负 = 比预估差）；{}；{}",
                    quote.net_usdt.round_dp(4),
                    actual.round_dp(4),
                    shortfall.round_dp(4),
                    exit_shortfall_text(quote, fills.as_ref()),
                    position.note.as_deref().unwrap_or("")
                );
                if shortfall < -EXIT_SHORTFALL_WARN_USDT {
                    warn!(position = %position.id, symbol = %position.symbol, "{message}");
                } else {
                    tracing::info!(position = %position.id, symbol = %position.symbol, "{message}");
                }
            }
            Some(result.map(|()| None))
        }
        Action::Trim { fraction, reason } => {
            match (observation.long.mark_price, observation.short.mark_price) {
                (Some(long_mark), Some(short_mark)) => Some(
                    executor
                        .trim(position, *fraction, long_mark, short_mark, reason)
                        .await
                        .map(|()| None),
                ),
                _ => None,
            }
        }
        Action::Hold | Action::AddMargin { .. } => None,
    }
}

/// 复核时重新评估要用的、第一次评估也用过的输入（资金费、强平状态不随盘口变）。
struct ConfirmInputs<'a> {
    funding_avg_apr: Option<Decimal>,
    funding_usdt: Option<Decimal>,
    long_state: Option<&'a crate::broker::VenueLegState>,
    short_state: Option<&'a crate::broker::VenueLegState>,
    /// 平仓手续费按什么费率估（与第一次评估同一个）。
    fees: crate::monitor::ExitFees,
}

/// 获利平仓下单前的复核：隔 `gap` 重拉两边盘口，用新报价把同一套评估再跑一遍。
///
/// 仍然判定平仓 → 返回新的评估（带上两次的数）；不再判定平仓 → 返回新的评估（继续持有），
/// 备注里写明第一次与复核的差别。盘口没拉到、或两份盘口不是同一时刻的，一律算没通过：
/// 获利平仓不急这一轮，下一轮再看；而一份来路不明的报价不能拿来下真单。
async fn confirm_profit_exit(
    position: &PairPosition,
    (long, short): (&MarketSnapshot, &MarketSnapshot),
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    gap: std::time::Duration,
    first_timing: Option<BookTiming>,
    first: Evaluation,
    inputs: ConfirmInputs<'_>,
) -> Evaluation {
    let (Some(long_leg), Some(short_leg)) = (position.long.as_ref(), position.short.as_ref())
    else {
        return first;
    };
    let first_net = first.observation.exit.as_ref().map(|quote| quote.net_usdt);
    let first_basis = first
        .observation
        .exit
        .as_ref()
        .map(|quote| quote.exit_basis_pct);
    if !gap.is_zero() {
        tokio::time::sleep(gap).await;
    }
    let (check, timing) = match fetch_books_timed(
        by_venue,
        (long_leg.venue, &position.symbol),
        (short_leg.venue, &position.symbol),
        EXIT_DEPTH_LEVELS,
    )
    .await
    {
        Ok((long_book, short_book, timing)) if timing.skew_ms() > EXIT_CONFIRM_MAX_SKEW_MS => {
            let _ = (long_book, short_book);
            (
                ExitCheck::Unavailable(format!(
                    "复核时两份盘口到达时间差 {} 毫秒（{} {} 毫秒、{} {} 毫秒），不是同一时刻的报价",
                    timing.skew_ms(),
                    long_leg.venue,
                    timing.long_ms,
                    short_leg.venue,
                    timing.short_ms
                )),
                Some(timing),
            )
        }
        Ok((long_book, short_book, timing)) => (
            match exit_quote(position, &long_book, &short_book, inputs.fees) {
                Ok(quote) => ExitCheck::Quoted(quote),
                Err(why) => ExitCheck::Unavailable(why),
            },
            Some(timing),
        ),
        Err(error) => (
            ExitCheck::Unavailable(format!("复核时盘口没拉到：{error:#}")),
            None,
        ),
    };
    let ConfirmInputs {
        funding_avg_apr,
        funding_usdt,
        long_state,
        short_state,
        fees: _,
    } = inputs;
    let Some(mut second) = evaluate_full(
        position,
        long,
        short,
        Inputs {
            exit: check,
            funding_avg_apr,
            funding_usdt,
            long_state,
            short_state,
            tp_buffer_usdt: crate::monitor::exit_buffer_usdt(chrono::Utc::now()),
        },
    ) else {
        return first;
    };
    let second_net = second.observation.exit.as_ref().map(|quote| quote.net_usdt);
    let second_basis = second
        .observation
        .exit
        .as_ref()
        .map(|quote| quote.exit_basis_pct);
    let fmt =
        |value: Option<Decimal>| value.map_or("无".to_string(), |v| v.round_dp(4).to_string());
    let timing_text = |timing: Option<BookTiming>| {
        timing.map_or("无".to_string(), |t| {
            format!("{}/{} 毫秒", t.long_ms, t.short_ms)
        })
    };
    if matches!(second.action, Action::Close { .. }) {
        tracing::info!(
            position = %position.id,
            symbol = %position.symbol,
            first_net_usdt = %fmt(first_net),
            second_net_usdt = %fmt(second_net),
            first_exit_basis_pct = %fmt(first_basis),
            second_exit_basis_pct = %fmt(second_basis),
            gap_ms = gap.as_millis() as u64,
            first_book_ms = %timing_text(first_timing),
            second_book_ms = %timing_text(timing),
            "平仓复核通过：隔 {} 毫秒重拉盘口仍然达标",
            gap.as_millis()
        );
        second.needs_confirm = false;
        return second;
    }
    let why = format!(
        "平仓复核没过：第一次盘口预估净赚 {} USDT（平仓价差 {}%），隔 {} 毫秒重拉盘口后不再达标（预估 {} USDT、价差 {}%），\
         第一次的达标只持续了一瞬，继续持有，下一轮再看",
        fmt(first_net),
        fmt(first_basis),
        gap.as_millis(),
        fmt(second_net),
        fmt(second_basis),
    );
    warn!(
        position = %position.id,
        symbol = %position.symbol,
        first_net_usdt = %fmt(first_net),
        second_net_usdt = %fmt(second_net),
        first_exit_basis_pct = %fmt(first_basis),
        second_exit_basis_pct = %fmt(second_basis),
        first_book_ms = %timing_text(first_timing),
        second_book_ms = %timing_text(timing),
        "{why}"
    );
    second.skipped.push(why);
    second
}

/// 对一笔双腿持仓评估规则，并把动作交给执行器。
///
/// 基差收敛（标记价）与止盈触发时，先现拉两边盘口算「现在平掉整笔」的预估盈亏，为正 / 达标才平
/// （见 [`crate::monitor`] 模块文档）。
pub async fn monitor_position(
    executor: &Executor,
    position: &mut PairPosition,
    snapshots: &HashMap<(Venue, Symbol), MarketSnapshot>,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ctx: &RoundCtx<'_>,
) -> MonitorReport {
    let mut report = MonitorReport::new(position);
    let (Some(long_leg), Some(short_leg)) = (position.long.clone(), position.short.clone()) else {
        report.skipped = Some("不是完整的双腿持仓".into());
        return report;
    };
    let long = snapshots.get(&(long_leg.venue, position.symbol.clone()));
    let short = snapshots.get(&(short_leg.venue, position.symbol.clone()));
    let (Some(long), Some(short)) = (long, short) else {
        report.skipped = Some("本轮缺少至少一条腿的行情，跳过评估".into());
        return report;
    };
    // 费差自动平仓按最近几小时的平均判断：拉两腿的逐小时历史（有缓存）。
    // 拉不到就退回当前读数，评估里会写明。
    let funding_avg_apr = if position.status == PositionStatus::Open
        && position.rules.min_funding_apr.is_some()
    {
        match fetch_funding_histories(
            by_venue,
            (long_leg.venue, &position.symbol),
            (short_leg.venue, &position.symbol),
            arb_scanner::stability::FETCH_HOURS,
        )
        .await
        {
            Ok(Some((long_history, short_history))) => arb_scanner::stability::recent_diff_apr(
                &long_history,
                &short_history,
                chrono::Utc::now(),
                crate::monitor::FUNDING_AVG_HOURS,
                arb_scanner::stability::MIN_RECENT_POINTS,
            ),
            Ok(None) => None,
            Err(error) => {
                warn!(position = %position.id, "资金费历史没拉到，费差规则按当前读数：{error:#}");
                None
            }
        }
    } else {
        None
    };
    // 每一笔持仓中的仓位都查结算流水、每轮按盘口估一次「现在平掉能得多少」：页面上所有「净收益」都按这个盘口净额显示
    // （标记价会偏离真实成交价，某次实盘：Lighter 标记价比盘口高约 4%，标记价净收益为正、盘口净额却为负），
    // 没设止盈等规则的仓位也不例外。不改变决策：用到盘口 / 资金费的规则只在各自触发后才看它，没设规则的仓位不会因此动作。
    let funding_usdt = if position.status == PositionStatus::Open {
        ctx.funding.funding_usdt(position).await
    } else {
        None
    };
    let mut first_timing: Option<BookTiming> = None;
    let exit = if position.status == PositionStatus::Open {
        match fetch_books_timed(
            by_venue,
            (long_leg.venue, &position.symbol),
            (short_leg.venue, &position.symbol),
            EXIT_DEPTH_LEVELS,
        )
        .await
        {
            Ok((long_book, short_book, timing)) => {
                first_timing = Some(timing);
                match exit_quote(
                    position,
                    &long_book,
                    &short_book,
                    executor.exit_fees(position),
                ) {
                    Ok(quote) => ExitCheck::Quoted(quote),
                    Err(why) => ExitCheck::Unavailable(why),
                }
            }
            Err(error) => ExitCheck::Unavailable(format!("{error:#}")),
        }
    } else {
        ExitCheck::NotFetched
    };
    // 仅供展示的估算用不了时，原因只会进页面的 report；留一条日志，事后才查得到是哪家、为什么。
    if let ExitCheck::Unavailable(why) = &exit {
        warn!(position = %position.id, symbol = %position.symbol, "按盘口估平仓用不了：{why}");
    }
    // 强平相关的规则要按交易所里**实际**的保证金判断：补过保证金后台账里的开仓时保证金早不对了，
    // 照旧算会在保证金充足时误判危险、误减仓（或重复补）。没设这些规则就不必多打一趟交易所。
    let (long_state, short_state) = if position.rules.liq_protection_pct.is_some()
        || position.rules.auto_margin_pct.is_some()
    {
        executor.leg_states(position).await
    } else {
        (None, None)
    };
    let Some(mut evaluation) = evaluate_full(
        position,
        long,
        short,
        Inputs {
            exit,
            funding_avg_apr,
            funding_usdt,
            long_state: long_state.as_ref(),
            short_state: short_state.as_ref(),
            tp_buffer_usdt: crate::monitor::exit_buffer_usdt(chrono::Utc::now()),
        },
    ) else {
        report.skipped = Some("这笔仓位不在可评估的状态".into());
        return report;
    };
    // 获利平仓只凭一次盘口快照触发：下单前隔一小会儿重拉盘口、再评估一遍，仍然成立才平。
    let profit_exit = evaluation.needs_confirm && matches!(evaluation.action, Action::Close { .. });
    // 休市（如 Bitget 美股合约周末）平不了：先判休市，不为一笔反正下不了的单每轮多拉两次盘口复核。
    if profit_exit && let Some(reason) = executor.exit_halted(position).await {
        report.skipped = Some(format!("{reason}，规则要平 / 减仓，等能下单了再执行"));
        report.evaluation = Some(evaluation);
        return report.finish(position);
    }
    if profit_exit {
        evaluation = confirm_profit_exit(
            position,
            (long, short),
            by_venue,
            ctx.exit_confirm_gap,
            first_timing,
            evaluation,
            ConfirmInputs {
                funding_avg_apr,
                funding_usdt,
                long_state: long_state.as_ref(),
                short_state: short_state.as_ref(),
                fees: executor.exit_fees(position),
            },
        )
        .await;
    }
    // 规则要平 / 减，但有一家现在下不了单（休市）：这一轮不动，不算失败、不退避，下一轮再看。
    if matches!(
        evaluation.action,
        Action::Close { .. } | Action::Trim { .. }
    ) && let Some(reason) = executor.exit_halted(position).await
    {
        report.skipped = Some(format!("{reason}，规则要平 / 减仓，等能下单了再执行"));
        report.evaluation = Some(evaluation);
        return report.finish(position);
    }
    // 复核过的获利平仓：只有一条腿在 Hyperliquid 时，先发那条腿的限价（见 [`exit_guard`]）。
    let guard = (profit_exit && matches!(evaluation.action, Action::Close { .. }))
        .then(|| exit_guard(position, evaluation.observation.exit.as_ref()?))
        .flatten();
    let observation = &evaluation.observation;
    let result = match &evaluation.action {
        Action::Hold => None,
        Action::AddMargin {
            venue,
            amount_usdt,
            reason,
            ..
        } => match top_up(executor, position, ctx, *venue, *amount_usdt, reason).await {
            TopUp::Applied => Some(Ok(None)),
            TopUp::Unknown(why) => {
                report.attention = Some(format!(
                    "自动加保证金往 {venue} 补的结果不明（{why}），已按补过计入上限"
                ));
                Some(Ok(None))
            }
            TopUp::Unavailable(why) => {
                // 补不了：退回爆仓保护算出来的减仓 / 平仓（没设爆仓保护就只能告警）。
                report.attention = Some(format!("自动加保证金没补成：{why}"));
                match &evaluation.fallback {
                    Some(fallback) => {
                        let done =
                            run_exit_action(executor, position, observation, fallback, None).await;
                        if done.is_none() {
                            report.skipped = Some("缺标记价，无法折算减仓名义".into());
                        }
                        done
                    }
                    None => {
                        report.skipped =
                            Some(format!("自动加保证金没补成，也没设爆仓保护可退回：{why}"));
                        None
                    }
                }
            }
        },
        action => {
            let done = run_exit_action(executor, position, observation, action, guard).await;
            if done.is_none() {
                report.skipped = Some("缺标记价，无法折算减仓名义".into());
            }
            done
        }
    };
    if let Some(result) = result {
        report.executed = true;
        match result {
            Ok(held) => report.exit_held = held,
            Err(error) => report.error = Some(error.to_string()),
        }
    }
    if report.attention.is_none() {
        // 评估里「自动加保证金：…」开头的都是问题（上限用完、算不出强平距离）：带出去给告警用。
        let problems: Vec<&str> = evaluation
            .skipped
            .iter()
            .filter(|note| note.starts_with("自动加保证金："))
            .map(String::as_str)
            .collect();
        if !problems.is_empty() {
            report.attention = Some(problems.join("；"));
        }
    }
    report.evaluation = Some(evaluation);
    report.finish(position)
}

/// 盘口触发止盈：价差监控逐秒看 WebSocket 盘口，发现一笔持仓「现在整笔平掉（含资金费）已到止盈」时调这里，
/// 不等下一个规则轮（约 60 秒一轮，而按盘口到止盈的窗口中位只有 2~3 秒）。
///
/// 为什么值得做（只记录不下单的测算）：已平的仓位里，测算第一次判定「该平」时的整笔净额比实际由规则轮平掉时更高，
/// 有的晚了几个小时。
///
/// 和规则轮的止盈只差一点：规则轮先要**标记价**到止盈才去核对盘口，这里**盘口本身**到止盈就算触发 —— 盘口才是
/// 真正平得出来的价，标记价只是参考（某次实盘：标记价比盘口中间价多算 1.76 USDT）。门槛是止盈金额本身，
/// 不用「止盈盘口门槛」放宽：那个门槛是给「标记价已经到了」的情形用的。其余全照规则轮：
///
/// - 只看开着、两腿都在、设了止盈的仓位；资金费没查到不评估（不拿 0 冒充）；有一家休市不动。
/// - 盘口用 REST 现拉（不用触发它的 WebSocket 盘口），两份盘口到达时间差太大不用；隔
///   [`RoundCtx::exit_confirm_gap`] 再拉一次复核，两次都到止盈才平。
/// - 同一份评估里规则要做别的（减仓、补保证金、别的平仓理由）就不插手，留给规则轮。
/// - 下单走同一条路：只有一条腿在 Hyperliquid 时先发那条腿的限价（[`exit_guard`]），成交后打「平仓对账」。
///
/// 返回 `None` = 这笔已经不在（平掉了、不是双腿、没设止盈），什么都没做。
pub async fn book_trigger_exit(
    executor: &Executor,
    ledger: &Ledger,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ctx: &RoundCtx<'_>,
    position_id: &str,
) -> Result<Option<MonitorReport>> {
    let (replayed, _) = ledger.replay().await?;
    let Some(mut position) = replayed.positions.get(position_id).cloned() else {
        return Ok(None);
    };
    if position.status != PositionStatus::Open || !position.is_hedged() {
        return Ok(None);
    }
    let Some(target) = position.rules.take_profit_usdt else {
        return Ok(None);
    };
    Ok(Some(
        book_trigger_position(executor, &mut position, None, by_venue, ctx, target).await,
    ))
}

/// 现拉两边盘口，按吃单费率算「现在平掉整笔」的预估（不含资金费）。两份盘口到达时间差太大不用。
async fn fresh_exit_quote(
    executor: &Executor,
    position: &PairPosition,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
) -> Result<crate::monitor::ExitQuote, String> {
    let (Some(long_leg), Some(short_leg)) = (position.long.as_ref(), position.short.as_ref())
    else {
        return Err("不是完整的双腿持仓".into());
    };
    let (long_book, short_book, timing) = fetch_books_timed(
        by_venue,
        (long_leg.venue, &position.symbol),
        (short_leg.venue, &position.symbol),
        EXIT_DEPTH_LEVELS,
    )
    .await
    .map_err(|error| format!("盘口没拉到：{error:#}"))?;
    if timing.skew_ms() > EXIT_CONFIRM_MAX_SKEW_MS {
        return Err(format!(
            "两份盘口到达时间差 {} 毫秒（{} {} 毫秒、{} {} 毫秒），不是同一时刻的报价",
            timing.skew_ms(),
            long_leg.venue,
            timing.long_ms,
            short_leg.venue,
            timing.short_ms
        ));
    }
    exit_quote(
        position,
        &long_book,
        &short_book,
        executor.exit_fees(position),
    )
}

/// `snapshots`（两腿的标记价）：`None` = 现拉盘口第一次到止盈之后才去拉。拉的是两家的全量行情
/// （Hyperliquid `metaAndAssetCtxs`、Lighter `orderBookDetails`，都不缓存），WebSocket 盘口说到了、REST 盘口
/// 却没到的那些触发（会是大多数）不该为它付两次全量扫描。测试直接给。
async fn book_trigger_position(
    executor: &Executor,
    position: &mut PairPosition,
    snapshots: Option<HashMap<(Venue, Symbol), MarketSnapshot>>,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ctx: &RoundCtx<'_>,
    target: Decimal,
) -> MonitorReport {
    // 美股开盘头一小时盘口乱跳、平仓滑点大：要多赚这么多才平（见 `monitor::exit_buffer_usdt`）。
    let target = target + crate::monitor::exit_buffer_usdt(chrono::Utc::now());
    let mut report = MonitorReport::new(position);
    let skip = |mut report: MonitorReport, position: &PairPosition, why: String| {
        tracing::info!(position = %position.id, symbol = %position.symbol, "盘口触发止盈：不平，{why}");
        report.skipped = Some(format!("盘口触发止盈：{why}"));
        report.finish(position)
    };
    let (Some(long_leg), Some(short_leg)) = (position.long.clone(), position.short.clone()) else {
        return skip(report, position, "不是完整的双腿持仓".into());
    };
    let Some(funding) = ctx.funding.funding_usdt(position).await else {
        return skip(report, position, "资金费流水没查到，不拿 0 冒充".into());
    };
    if let Some(reason) = executor.exit_halted(position).await {
        return skip(report, position, reason);
    }
    let first = match fresh_exit_quote(executor, position, by_venue).await {
        Ok(quote) => quote,
        Err(why) => return skip(report, position, why),
    };
    let first_net = first.net_usdt + funding;
    if first_net < target {
        return skip(
            report,
            position,
            format!(
                "现拉盘口整笔平掉只有 {} USDT（含资金费 {}），不到止盈 {target}",
                first_net.round_dp(4),
                funding.round_dp(4)
            ),
        );
    }
    let snapshots = match snapshots {
        Some(snapshots) => snapshots,
        None => fetch_snapshots(by_venue, &position_venues(std::slice::from_ref(&*position))).await,
    };
    let (Some(long), Some(short)) = (
        snapshots.get(&(long_leg.venue, position.symbol.clone())),
        snapshots.get(&(short_leg.venue, position.symbol.clone())),
    ) else {
        return skip(report, position, "缺至少一条腿的行情".into());
    };
    let (long_state, short_state) = if position.rules.liq_protection_pct.is_some()
        || position.rules.auto_margin_pct.is_some()
    {
        executor.leg_states(position).await
    } else {
        (None, None)
    };
    let Some(evaluation) = evaluate_full(
        position,
        long,
        short,
        Inputs {
            exit: ExitCheck::Quoted(first.clone()),
            funding_avg_apr: None,
            funding_usdt: Some(funding),
            long_state: long_state.as_ref(),
            short_state: short_state.as_ref(),
            tp_buffer_usdt: crate::monitor::exit_buffer_usdt(chrono::Utc::now()),
        },
    ) else {
        return skip(report, position, "这笔仓位不在可评估的状态".into());
    };
    if !matches!(evaluation.action, Action::Hold | Action::Close { .. }) {
        return skip(
            report,
            position,
            "规则这一轮要做别的（减仓 / 补保证金），交给规则轮".into(),
        );
    }
    if !ctx.exit_confirm_gap.is_zero() {
        tokio::time::sleep(ctx.exit_confirm_gap).await;
    }
    let second = match fresh_exit_quote(executor, position, by_venue).await {
        Ok(quote) => quote,
        Err(why) => return skip(report, position, format!("复核时{why}")),
    };
    let second_net = second.net_usdt + funding;
    if second_net < target {
        warn!(
            position = %position.id,
            symbol = %position.symbol,
            first_net_usdt = %first_net.round_dp(4),
            second_net_usdt = %second_net.round_dp(4),
            "盘口触发止盈复核没过：第一次现拉盘口到止盈，隔 {} 毫秒再拉不到了，继续持有",
            ctx.exit_confirm_gap.as_millis()
        );
        report.skipped = Some(format!(
            "盘口触发止盈复核没过：第一次 {} USDT，复核 {} USDT，不到止盈 {target}",
            first_net.round_dp(4),
            second_net.round_dp(4)
        ));
        return report.finish(position);
    }
    let mark_text = evaluation
        .observation
        .net_with_funding_usdt
        .map_or("未知".to_string(), |net| net.round_dp(4).to_string());
    let reason = format!(
        "盘口触发止盈：两边盘口现在整笔平掉预估净赚 {} USDT（含资金费 {}、平仓手续费与穿价），隔 {} 毫秒复核仍不低于止盈 {target} USDT（按标记价是 {mark_text} USDT）",
        second_net.round_dp(4),
        funding.round_dp(4),
        ctx.exit_confirm_gap.as_millis()
    );
    let guard = exit_guard(position, &second);
    let mut observation = evaluation.observation.clone();
    observation.exit = Some(second);
    let action = Action::Close { reason };
    report.executed = true;
    match run_exit_action(executor, position, &observation, &action, guard).await {
        Some(Ok(held)) => report.exit_held = held,
        Some(Err(error)) => report.error = Some(error.to_string()),
        None => {}
    }
    report.evaluation = Some(Evaluation {
        observation,
        action,
        needs_confirm: false,
        ..evaluation
    });
    report.finish(position)
}

/// 没走完的退出的重试节奏：每笔仓位各自记「连续失败几次、最早什么时候再试」。
///
/// 每次重试都是一张**新的** reduce-only 订单（旧订单号已有终态、不能复用）。场所持续超时或
/// 拒单时，每个规则轮（默认 60 秒）都发一张，既是在刷接口，也把日志与告警灌满。所以失败后
/// 指数退避：30 秒、1 分、2 分……封顶 30 分钟。**不放弃**：裸腿每过一段时间还是要再试，只是
/// 不再每轮都试。成功一次就清零。只放内存，重启后重新计时。
#[derive(Debug, Default)]
pub struct ExitRetries {
    inner: std::sync::Mutex<HashMap<String, (u32, std::time::Instant)>>,
}

impl ExitRetries {
    const BASE: std::time::Duration = std::time::Duration::from_secs(30);
    const CAP: std::time::Duration = std::time::Duration::from_secs(30 * 60);

    /// 现在能不能重试这笔仓位的退出。没失败过的永远可以。
    pub fn due(&self, position_id: &str, now: std::time::Instant) -> bool {
        self.lock()
            .get(position_id)
            .is_none_or(|(_, next_at)| now >= *next_at)
    }

    /// 连续失败了几次。
    pub fn failures(&self, position_id: &str) -> u32 {
        self.lock().get(position_id).map_or(0, |(count, _)| *count)
    }

    /// 记一次失败，返回连续失败次数。
    pub fn failed(&self, position_id: &str, now: std::time::Instant) -> u32 {
        let mut table = self.lock();
        let entry = table.entry(position_id.to_string()).or_insert((0, now));
        entry.0 = entry.0.saturating_add(1);
        let wait = Self::BASE
            .saturating_mul(1u32 << (entry.0 - 1).min(16))
            .min(Self::CAP);
        entry.1 = now + wait;
        entry.0
    }

    pub fn succeeded(&self, position_id: &str) {
        self.lock().remove(position_id);
    }

    /// 冷却：`wait` 之内 [`due`](Self::due) 都是假；不算失败（失败次数清零）。
    pub fn hold(&self, key: &str, now: std::time::Instant, wait: std::time::Duration) {
        self.lock().insert(key.to_string(), (0, now + wait));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (u32, std::time::Instant)>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// 这一轮哪些仓位被对账冻结（见 [`live_round`]）。
#[derive(Clone, Copy)]
pub enum Frozen<'a> {
    /// 对账干净：都不冻结。
    Nothing,
    /// 全部冻结。
    Everything,
    /// 按这次对账的不一致冻结：只冻结出问题的币、没核对的场所上的仓位（见 [`Reconciliation::blocks`]）。
    By(&'a Reconciliation),
}

impl Frozen<'_> {
    /// 这笔仓位是否被冻结。场所按它的两条腿、以及开仓计划里的两腿算。
    pub fn blocks(&self, position: &PairPosition) -> bool {
        match self {
            Frozen::Nothing => false,
            Frozen::Everything => true,
            Frozen::By(reconciliation) => {
                let mut venues: Vec<Venue> = [position.long.as_ref(), position.short.as_ref()]
                    .into_iter()
                    .flatten()
                    .map(|leg| leg.venue)
                    .collect();
                if let Some(planned) = position.planned {
                    venues.extend([planned.long, planned.short]);
                }
                reconciliation.blocks(&position.symbol, &venues)
            }
        }
    }
}

/// 实盘一轮监控：双腿持仓按规则评估执行；没走完的退出继续重试；中断的建仓续上。
///
/// `frozen`：对账不一致时**只冻结出问题的币**（见 [`Frozen`]），其余仓位照常评估规则。规则按台账评估，台账与
/// 账户对不上的那个币自动平仓 / 减仓可能作用在错误的数量上，所以它不评估规则；但仍然重试它**已经开始的退出**
/// （`Closing` / `Unwinding`）：那是在补一条裸腿，订单是 reduce-only，数量即使与账户对不上也不会反向开出新仓。
/// 只剩一条腿的 `Open` 被冻结时不自动退出：按台账退出可能把已经成交的对冲拆掉，必须先对账、由人判断。
///
/// `Opening`（建仓中断、结果不明）一律交给 [`Executor::resume_open`] 续上：读回开仓单的结局、把腿补进台账
/// （只读，冻结时也做），还差第二腿就补 —— 补单只在这个币没被冻结、且台账在补腿之后又对过一次账时才发。
pub async fn live_round(
    executor: &Executor,
    ledger: &Ledger,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ctx: &RoundCtx<'_>,
    frozen: Frozen<'_>,
) -> Result<Vec<MonitorReport>> {
    round_positions(executor, ledger, by_venue, ctx, frozen, false).await
}

/// 只续上中断的建仓（`Opening`）。规则轮里续上建仓刚把腿补进台账（[`MonitorReport::reverify`]）时，调用方在
/// 同一把锁里马上再对一次账、按新的对账结果跑这一遍：补第二腿不必等下一轮，少裸露一分钟。
pub async fn resume_openings(
    executor: &Executor,
    ledger: &Ledger,
    ctx: &RoundCtx<'_>,
    frozen: Frozen<'_>,
) -> Result<Vec<MonitorReport>> {
    round_positions(executor, ledger, &HashMap::new(), ctx, frozen, true).await
}

async fn round_positions(
    executor: &Executor,
    ledger: &Ledger,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ctx: &RoundCtx<'_>,
    frozen: Frozen<'_>,
    only_opening: bool,
) -> Result<Vec<MonitorReport>> {
    let retries = ctx.retries;
    let (replayed, _) = ledger.replay().await?;
    let mut positions: Vec<PairPosition> = replayed
        .exposed()
        .into_iter()
        .filter(|position| !only_opening || position.status == PositionStatus::Opening)
        .cloned()
        .collect();
    positions.sort_by(|a, b| a.id.cmp(&b.id));
    if positions.is_empty() {
        return Ok(Vec::new());
    }
    let evaluable: Vec<PairPosition> = positions
        .iter()
        .filter(|position| {
            position.status == PositionStatus::Open
                && position.is_hedged()
                && !frozen.blocks(position)
        })
        .cloned()
        .collect();
    let snapshots = if evaluable.is_empty() {
        HashMap::new()
    } else {
        fetch_snapshots(by_venue, &position_venues(&evaluable)).await
    };
    let mut reports = Vec::new();
    for mut position in positions {
        let blocked = frozen.blocks(&position);
        if position.status == PositionStatus::Open && position.is_hedged() {
            if !blocked {
                reports.push(
                    monitor_position(executor, &mut position, &snapshots, by_venue, ctx).await,
                );
            } else {
                let mut report = MonitorReport::new(&position);
                report.skipped = Some(format!(
                    "对账不一致（{}）：这个币本轮不评估规则，只重试没走完的退出",
                    position.symbol
                ));
                reports.push(report);
            }
            continue;
        }
        if position.status == PositionStatus::Opening {
            reports.push(resume_opening(executor, &mut position, !blocked).await);
            continue;
        }
        // Closing / Unwinding（上一次退出没走完）、只剩一条腿的 Open：继续退出剩下的腿。
        let mut report = MonitorReport::new(&position);
        report.retried_exit = true;
        if blocked
            && !matches!(
                position.status,
                PositionStatus::Closing | PositionStatus::Unwinding
            )
        {
            report.skipped = Some(format!(
                "对账不一致（{}）：这笔仓位只剩一条腿，不自动退出；请先处理对账不一致",
                position.symbol
            ));
            reports.push(report);
            continue;
        }
        let now = std::time::Instant::now();
        if !retries.due(&position.id, now) {
            report.skipped = Some(format!(
                "上一次退出没成功（连续失败 {} 次），退避中，稍后再试",
                retries.failures(&position.id)
            ));
            reports.push(report.finish(&position));
            continue;
        }
        // 剩下的腿所在的场所现在下不了单（休市）：不算失败、不退避，等它开了再平。
        if let Some(reason) = executor.exit_halted(&position).await {
            report.skipped = Some(format!("{reason}，等能下单了再平"));
            reports.push(report.finish(&position));
            continue;
        }
        report.executed = true;
        let outcome = executor.close(&mut position).await;
        match &outcome {
            Ok(()) => retries.succeeded(&position.id),
            Err(_) => {
                let failures = retries.failed(&position.id, now);
                warn!(position = %position.id, failures, "退出重试没成功，退避后再试");
            }
        }
        report.error = outcome.err().map(|error| error.to_string());
        reports.push(report.finish(&position));
    }
    Ok(reports)
}

/// 续上一笔中断的建仓（见 [`Executor::resume_open`]），写成这一轮的报告。`can_trade`：这个币没被对账冻结。
async fn resume_opening(
    executor: &Executor,
    position: &mut PairPosition,
    can_trade: bool,
) -> MonitorReport {
    // 不退避：裸着的第一腿每一轮（约 1 分钟）都要再试（用户 10-17 定的）。
    let mut report = MonitorReport::new(position);
    match executor.resume_open(position, can_trade).await {
        Ok(crate::Resume::Waiting(why)) => {
            report.skipped = Some(format!("建仓中断，还在核实开仓单：{why}"));
        }
        // 要人处理：记成错误，看板告警（同一笔仓位限频）；不算失败、不退避，下一轮照样再看。
        Ok(crate::Resume::Stuck(why)) => {
            report.error = Some(format!("建仓中断，需要人工处理：{why}"));
        }
        Ok(crate::Resume::Verified) => {
            report.reverify = true;
            report.skipped =
                Some("建仓中断：开仓单的结局已读回、腿已补进台账，对完账再补第二腿".into());
        }
        Ok(crate::Resume::Frozen) => {
            report.skipped = Some(format!(
                "建仓中断：还差第二腿，但 {} 的对账还不干净，先不补",
                position.symbol
            ));
        }
        // 补了还没补齐：发了单，第一腿还裸着，告警（同一笔限频），下一轮再补。
        Ok(crate::Resume::Retry(why)) => {
            report.executed = true;
            report.error = Some(format!("建仓中断，第二腿还没补齐：{why}"));
        }
        Ok(crate::Resume::Done) => {
            report.executed = true;
            report.resumed = true;
        }
        Err(error) => {
            warn!(position = %position.id, %error, "续上建仓没成功，下一轮再试");
            report.executed = true;
            report.error = Some(error.to_string());
        }
    }
    report.finish(position)
}

/// 纸面一轮监控：只看双腿持仓（纸面没有「没走完的退出」要重试）。
pub async fn paper_round(
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    fee_per_side: Decimal,
    depth_levels: u32,
    ledger: &Arc<Ledger>,
    ctx: &RoundCtx<'_>,
) -> Result<Vec<MonitorReport>> {
    let (replayed, _) = ledger.replay().await?;
    let mut positions: Vec<PairPosition> = replayed
        .positions
        .values()
        .filter(|position| position.status == PositionStatus::Open && position.is_hedged())
        .cloned()
        .collect();
    positions.sort_by(|a, b| a.id.cmp(&b.id));
    if positions.is_empty() {
        return Ok(Vec::new());
    }
    let snapshots = fetch_snapshots(by_venue, &position_venues(&positions)).await;
    let flat: Vec<MarketSnapshot> = snapshots.values().cloned().collect();
    let (executor, _) = paper_executor(
        by_venue,
        fee_per_side,
        depth_levels,
        ledger,
        &flat,
        &positions,
    );
    let mut reports = Vec::new();
    for mut position in positions {
        reports.push(monitor_position(&executor, &mut position, &snapshots, by_venue, ctx).await);
    }
    Ok(reports)
}

/// 纸面券商与执行器。费率表来自快照里的**真实**吃单费率（逐场所、逐合约）。
///
/// 拿不到费率的场所不进表，由 `PaperBroker` 回落到配置值 —— 那是刻意的：
/// 编一个费率比留白更危险。`positions` 用来重建各场所的纸面净持仓，平仓要靠它。
pub fn paper_executor(
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    fee_per_side: Decimal,
    depth_levels: u32,
    ledger: &Arc<Ledger>,
    snapshots: &[MarketSnapshot],
    positions: &[PairPosition],
) -> (Executor, HashMap<Venue, Arc<dyn Broker>>) {
    let mut fee_table = HashMap::new();
    for rate in snapshots {
        if let Some(fee) = rate.taker_fee {
            fee_table.insert((rate.venue, rate.symbol.to_string()), fee);
        }
    }
    let brokers: Vec<Arc<dyn Broker>> = by_venue
        .values()
        .map(|api| {
            Arc::new(
                PaperBroker::new(Arc::clone(api), fee_per_side, depth_levels)
                    .with_fee_table(fee_table.clone())
                    .with_positions(positions),
            ) as Arc<dyn Broker>
        })
        .collect();
    let executor = Executor::new(Arc::clone(ledger), brokers.clone());
    let broker_map = brokers
        .into_iter()
        .map(|broker| (broker.venue(), broker))
        .collect();
    (executor, broker_map)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod preset {
        use super::super::*;
        use crate::broker::VenuePosition;
        use crate::types::{ClientOrderId, OrderAck, OrderState};
        use async_trait::async_trait;
        use rust_decimal_macros::dec;
        use std::sync::Mutex;

        /// 记下 `prepare_open_mode` 调用的桩券商：可以给持仓、可以「休市」。
        struct Presettable {
            positions: Vec<VenuePosition>,
            halted: bool,
            prepared: Mutex<Vec<(Symbol, Option<Decimal>, crate::MarginMode)>>,
        }

        impl Presettable {
            fn new(positions: Vec<VenuePosition>, halted: bool) -> Self {
                Self {
                    positions,
                    halted,
                    prepared: Mutex::new(Vec::new()),
                }
            }
        }

        #[async_trait]
        impl Broker for Presettable {
            fn venue(&self) -> Venue {
                Venue::LighterRh
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("提前设杠杆不该下任何单");
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("提前设杠杆不该撤任何单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
                Ok(self.positions.clone())
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn trading_halted(&self, _: &Symbol, _: bool) -> ArbResult<Option<String>> {
                Ok(self.halted.then(|| "休市".to_string()))
            }
            async fn prepare_open_mode(
                &self,
                symbol: &Symbol,
                _: Side,
                leverage: Option<Decimal>,
                mode: crate::MarginMode,
            ) -> ArbResult<()> {
                self.prepared
                    .lock()
                    .unwrap()
                    .push((symbol.clone(), leverage, mode));
                Ok(())
            }
        }

        fn holding(base: &str) -> VenuePosition {
            VenuePosition {
                venue: Venue::LighterRh,
                symbol: Symbol::perp(base, "USDT"),
                net_quantity: dec!(-0.5),
                average_price: Some(dec!(100)),
                notional_usdt: dec!(50),
            }
        }

        #[tokio::test]
        async fn a_flat_open_market_gets_its_isolated_leverage_set_ahead_of_time() {
            let broker = Presettable::new(vec![holding("TSLA")], false);
            let symbol = Symbol::perp("IBM", "USDT");
            assert!(preset_leverage(&broker, &symbol, dec!(3)).await.unwrap());
            assert_eq!(
                *broker.prepared.lock().unwrap(),
                vec![(symbol, Some(dec!(3)), crate::MarginMode::Isolated)]
            );
        }

        #[tokio::test]
        async fn a_market_with_a_position_or_a_halt_is_left_alone() {
            let symbol = Symbol::perp("IBM", "USDT");
            let held = Presettable::new(vec![holding("IBM")], false);
            assert!(!preset_leverage(&held, &symbol, dec!(3)).await.unwrap());
            assert!(held.prepared.lock().unwrap().is_empty());
            let halted = Presettable::new(vec![], true);
            assert!(!preset_leverage(&halted, &symbol, dec!(3)).await.unwrap());
            assert!(halted.prepared.lock().unwrap().is_empty());
        }
    }

    mod spread {
        use super::super::*;
        use crate::preflight::LegPlan;
        use arb_core::{ArbResult, Level, Side};
        use arb_scanner::scan::{ExcludedReading, Totals};
        use async_trait::async_trait;
        use rust_decimal_macros::dec;

        /// 只给盘口的桩场所：一档买一卖一，深度足够。
        struct Book {
            venue: Venue,
            bid: Decimal,
            ask: Decimal,
        }

        #[async_trait]
        impl VenueApi for Book {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
                Ok(Vec::new())
            }
            async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
                let level = |price| Level {
                    price,
                    notional_usdt: dec!(100000),
                };
                Ok(OrderBook {
                    venue: self.venue,
                    symbol: symbol.clone(),
                    bids: vec![level(self.bid)],
                    asks: vec![level(self.ask)],
                })
            }
        }

        /// 逐档挂量的桩场所（价格, 名义）：用来摆「保本价以内只有一点量」的薄盘口。
        struct Ladder {
            venue: Venue,
            bids: Vec<(Decimal, Decimal)>,
            asks: Vec<(Decimal, Decimal)>,
        }

        #[async_trait]
        impl VenueApi for Ladder {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
                Ok(Vec::new())
            }
            async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
                let levels = |side: &[(Decimal, Decimal)]| {
                    side.iter()
                        .map(|(price, notional)| Level {
                            price: *price,
                            notional_usdt: *notional,
                        })
                        .collect()
                };
                Ok(OrderBook {
                    venue: self.venue,
                    symbol: symbol.clone(),
                    bids: levels(&self.bids),
                    asks: levels(&self.asks),
                })
            }
        }

        /// 多 lighter-rh（买，卖盘 `long_asks`）/ 空 hyperliquid-io（卖，买盘 `short_bids`）。
        fn ladders(
            long_asks: Vec<(Decimal, Decimal)>,
            short_bids: Vec<(Decimal, Decimal)>,
        ) -> HashMap<Venue, Arc<dyn VenueApi>> {
            HashMap::from([
                (
                    Venue::LighterRh,
                    Arc::new(Ladder {
                        venue: Venue::LighterRh,
                        bids: vec![(dec!(99.9), dec!(100000))],
                        asks: long_asks,
                    }) as Arc<dyn VenueApi>,
                ),
                (
                    Venue::HyperliquidIo,
                    Arc::new(Ladder {
                        venue: Venue::HyperliquidIo,
                        asks: vec![(short_bids[0].0 + dec!(0.1), dec!(100000))],
                        bids: short_bids,
                    }) as Arc<dyn VenueApi>,
                ),
            ])
        }

        fn fit_limits() -> Limits {
            Limits {
                max_position_usdt: dec!(5000),
                max_open_positions: 5,
                max_daily_loss_usdt: dec!(100),
                max_slippage: dec!(0.002),
                max_entry_basis_pct: dec!(5),
                min_liq_distance_pct: dec!(8),
                require_stable_funding: false,
            }
        }

        fn fit_request(
            size: Decimal,
            target: Decimal,
            fit: Option<crate::sizing::DepthFit>,
        ) -> OpenRequest {
            OpenRequest {
                maker: None,
                margin_mode: crate::MarginMode::Isolated,
                base: "ANTHROPIC".into(),
                quote: Some("USDT".into()),
                long: Venue::LighterRh,
                short: Venue::HyperliquidIo,
                size,
                leverage: dec!(2),
                daily_pnl: Decimal::ZERO,
                view: "spread".into(),
                depth_levels: 20,
                rules: TaskRules {
                    basis_exit_pct: Some(target),
                    take_profit_usdt: Some(dec!(10)),
                    stop_loss_usdt: Some(dec!(20)),
                    ..TaskRules::default()
                },
                relative_to_normal: false,
                leg_order: crate::preflight::LegOrder::Slippage,
                depth_fit: fit,
                force_hedge_pct: None,
            }
        }

        fn half_depth(min_size: Decimal) -> Option<crate::sizing::DepthFit> {
            Some(crate::sizing::DepthFit {
                fraction: dec!(0.5),
                min_size,
            })
        }

        /// 多腿卖盘很厚；空腿买盘在保本价（约 100.7）以内只有 100.8 与 101 两档共 240 USDT，再往下 100 是远价。
        fn thin_short_side() -> HashMap<Venue, Arc<dyn VenueApi>> {
            ladders(
                vec![(dec!(100), dec!(100000))],
                vec![
                    (dec!(101), dec!(120)),
                    (dec!(100.8), dec!(120)),
                    (dec!(100), dec!(100000)),
                ],
            )
        }

        /// 某次实盘：第二腿保本价以内的深度只够一小块。按深度定单笔时，单笔缩到深度的
        /// 一半以内；金额类规则同比例缩小；没开时仍按今天的做法（整笔被盘口太薄拒绝）。
        #[tokio::test]
        async fn a_thin_second_leg_shrinks_the_size_to_a_fraction_of_the_depth_inside_the_breakeven_price()
         {
            let (report, apis, limits) = (dex_pair(), thin_short_side(), fit_limits());
            // 没开：1000 在限价内吃不满，整笔拒绝。
            let err = prepare(
                &report,
                &apis,
                &fit_request(dec!(1000), dec!(0.5), None),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(err.contains("可吃到"), "{err}");
            // 开了：缩小到深度 240 的一半以内，下面全部闸门在这个单笔上再过一遍。
            let prepared = prepare(
                &report,
                &apis,
                &fit_request(dec!(1000), dec!(0.5), half_depth(dec!(50))),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap();
            let fit = prepared.fit.clone().expect("开了就带依据");
            assert!(fit.shrunk());
            assert_eq!(fit.requested, dec!(1000));
            assert!(
                fit.fitted <= dec!(120) && fit.fitted > dec!(100),
                "每档 ×0.9 往下试，第一个不超过 240 × 50% 的是 118：{}",
                fit.fitted
            );
            assert_eq!(fit.second_venue, Venue::HyperliquidIo);
            assert_eq!(fit.second_depth_usdt, dec!(240));
            assert_eq!(prepared.size_usdt, fit.fitted);
            assert_eq!(prepared.plan.long.notional_usdt, fit.fitted);
            assert_eq!(prepared.plan.short.notional_usdt, fit.fitted);
            // 按 USDT 计的规则同比例缩小，占名义的比例不变。
            let ratio = fit.fitted / dec!(1000);
            assert_eq!(
                prepared.rules.take_profit_usdt,
                Some((dec!(10) * ratio).round_dp(4))
            );
            assert_eq!(
                prepared.rules.stop_loss_usdt,
                Some((dec!(20) * ratio).round_dp(4))
            );
            assert_eq!(
                prepared.rules.basis_exit_pct,
                Some(dec!(0.5)),
                "百分比规则不动"
            );
            // 价差账是按缩小后的单笔算的。
            assert!(prepared.spread.unwrap().target_net.unwrap() > Decimal::ZERO);
        }

        /// 开了强制补腿，第二腿还能追到最差限价（−X% 锁定价差）以内的量：按深度定单笔时第二腿的深度按追单限价算，
        /// 不再被保本价那一档的一点点量卡住；`Prepared.force_hedge` 带着追单的最差价差（小数）。
        ///
        /// 盘口：整笔 1000 的均价 100.31 够保本线（0.28%：往返穿价的一半 0.1% + 往返手续费 0.04% + 收敛目标 0.14%
        /// → 保本价 100.28），但保本价以内只有第一档 400；
        /// 第二档 600 在 100.25，低于保本价，高于「最优价 − 0.2%」的原限价（所以闸门里的「限价内吃得满」过得去）。
        #[tokio::test]
        async fn the_forced_hedge_widens_the_second_leg_depth_the_size_may_use() {
            let apis = ladders(
                vec![(dec!(100), dec!(100000))],
                vec![
                    (dec!(100.4), dec!(400)),
                    (dec!(100.25), dec!(600)),
                    (dec!(100.0), dec!(100000)),
                ],
            );
            // lighter-rh 免费、hyperliquid-io 0.02%：费率差够大，先下免费的 lighter-rh，第二腿就是盘口薄的 io。
            let mut free = snap(Venue::LighterRh, dec!(100));
            free.taker_fee = Some(Decimal::ZERO);
            let report = report(vec![view(vec![
                free,
                snap(Venue::HyperliquidIo, dec!(101)),
            ])]);
            let limits = fit_limits();
            let mut request = fit_request(dec!(1000), dec!(0.14), half_depth(dec!(50)));
            request.leg_order = crate::preflight::LegOrder::CheapFirst;
            // 没开强制补腿：保本价内只有 400，按一半最多 200。
            let plain = prepare(&report, &apis, &request, 0, &limits, LeveragePolicy::Strict)
                .await
                .unwrap();
            let fit = plain.fit.clone().unwrap();
            assert!(
                fit.shrunk() && fit.fitted <= dec!(200),
                "{}",
                fit.describe()
            );
            assert_eq!(fit.second_depth_usdt, dec!(400));
            assert_eq!(plain.force_hedge, None);
            // 开了：追单限价（锁定价差 ≥ −0.5%）以内连第二档、第三档都算，不缩。
            request.force_hedge_pct = Some(dec!(0.5));
            let prepared = prepare(&report, &apis, &request, 0, &limits, LeveragePolicy::Strict)
                .await
                .unwrap();
            assert_eq!(prepared.force_hedge, Some(dec!(0.005)));
            let fit = prepared.fit.clone().unwrap();
            assert!(!fit.shrunk(), "{}", fit.describe());
            assert!(fit.second_depth_usdt > dec!(100000));
            assert_eq!(prepared.size_usdt, dec!(1000));
            // 资金费单 / 挂单开仓不适用。
            request.view = "funding".into();
            request.rules = TaskRules::default();
            if let Ok(funding) =
                prepare(&report, &apis, &request, 0, &limits, LeveragePolicy::Strict).await
            {
                assert_eq!(funding.force_hedge, None, "资金费单不适用");
            }
        }

        #[tokio::test]
        async fn a_book_that_is_deep_enough_keeps_the_requested_size_and_the_rules_untouched() {
            let (report, apis, limits) = (
                dex_pair(),
                books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1))),
                fit_limits(),
            );
            let prepared = prepare(
                &report,
                &apis,
                &fit_request(dec!(1000), dec!(0.5), half_depth(dec!(50))),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap();
            let fit = prepared.fit.clone().unwrap();
            assert!(!fit.shrunk());
            assert_eq!(prepared.size_usdt, dec!(1000));
            assert_eq!(prepared.rules.take_profit_usdt, Some(dec!(10)));
            assert_eq!(prepared.rules.stop_loss_usdt, Some(dec!(20)));
            // 没开：没有依据字段。
            let plain = prepare(
                &report,
                &apis,
                &fit_request(dec!(1000), dec!(0.5), None),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap();
            assert!(plain.fit.is_none());
            assert_eq!(plain.size_usdt, dec!(1000));
        }

        #[tokio::test]
        async fn below_the_minimum_size_the_open_is_refused_with_the_depth_numbers() {
            let (report, apis, limits) = (dex_pair(), thin_short_side(), fit_limits());
            // 深度 240 的一半是 120，不到最小单笔 150。
            let err = prepare(
                &report,
                &apis,
                &fit_request(dec!(1000), dec!(0.5), half_depth(dec!(150))),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("盘口深度不够") && err.contains("最小单笔 150") && err.contains("240"),
                "{err}"
            );
        }

        /// 只在「吃不到」时才缩：上限本身执行得了、只是不赚的，和以前一样被拒（保本线），
        /// 不会缩成一小笔去找能赚的价位；和大小无关的拒绝（单笔上限）也原样拒绝。
        #[tokio::test]
        async fn the_size_only_shrinks_for_depth_never_to_find_a_profitable_price_or_dodge_other_gates()
         {
            let (report, limits) = (dex_pair(), fit_limits());
            // 深盘口 + 目标 0.9%（收敛空间不够）：被保本线拒绝，不缩。
            let deep = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let err = prepare(
                &report,
                &deep,
                &fit_request(dec!(1000), dec!(0.9), half_depth(dec!(50))),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(err.contains("保本线"), "{err}");
            // 薄盘口 + 单笔超过仓位上限：TooLarge 与大小有关吗？有关但不是盘口的事 —— 原样拒绝。
            let tight = Limits {
                max_position_usdt: dec!(500),
                ..fit_limits()
            };
            let err = prepare(
                &report,
                &thin_short_side(),
                &fit_request(dec!(1000), dec!(0.5), half_depth(dec!(50))),
                0,
                &tight,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(err.contains("超过上限"), "{err}");
            // 持仓数已满同理。
            let err = prepare(
                &report,
                &thin_short_side(),
                &fit_request(dec!(1000), dec!(0.5), half_depth(dec!(50))),
                5,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(err.contains("达到上限"), "{err}");
        }

        /// 资金费单不按深度缩（只有价差单、两腿都吃单的才起作用）。
        #[tokio::test]
        async fn funding_orders_ignore_the_depth_sizing_switch() {
            let (report, apis, limits) = (dex_pair(), thin_short_side(), fit_limits());
            let mut request = fit_request(dec!(1000), dec!(0.5), half_depth(dec!(50)));
            request.view = "funding".into();
            request.rules = TaskRules::default();
            // 不论成败，都不会带按深度定单笔的依据。
            if let Ok(prepared) =
                prepare(&report, &apis, &request, 0, &limits, LeveragePolicy::Strict).await
            {
                assert!(prepared.fit.is_none());
                assert_eq!(prepared.size_usdt, dec!(1000));
            }
        }

        fn symbol() -> Symbol {
            Symbol::perp("ANTHROPIC", "USDT")
        }

        /// DEX 的批量快照：有标记价、没有买一卖一。
        fn snap(venue: Venue, mark: Decimal) -> MarketSnapshot {
            MarketSnapshot {
                venue,
                symbol: symbol(),
                period_rate: dec!(0.00001),
                interval_h: 1,
                interval_assumed: true,
                next_funding_at: chrono::Utc::now(),
                next_funding_estimated: true,
                taker_fee: Some(dec!(0.0002)),
                mark_price: Some(mark),
                index_price: Some(mark),
                best_bid: None,
                best_ask: None,
                bid_size_usdt: None,
                ask_size_usdt: None,
                open_interest_usdt: None,
                quote_volume_24h: None,
                max_leverage: Some(dec!(10)),
                maintenance_margin: Some(dec!(0.01)),
                oi_capped: false,
            }
        }

        fn view(rates: Vec<MarketSnapshot>) -> SymbolView {
            SymbolView {
                symbol: symbol(),
                rates,
                funding: Vec::new(),
                spread: Vec::new(),
            }
        }

        fn report(symbols: Vec<SymbolView>) -> ScanReport {
            ScanReport {
                generated_at: chrono::Utc::now(),
                fee_per_side: dec!(0.0005),
                amortize_days: dec!(7),
                spread_hold_days: dec!(3),
                max_entry_basis_pct: Some(dec!(0.5)),
                min_venues: 2,
                venues: Vec::new(),
                symbols,
                unverified: Vec::new(),
                suspicious: Vec::new(),
                gated: Vec::new(),
                totals: Totals::default(),
            }
        }

        fn books(
            long: (Decimal, Decimal),
            short: (Decimal, Decimal),
        ) -> HashMap<Venue, Arc<dyn VenueApi>> {
            HashMap::from([
                (
                    Venue::LighterRh,
                    Arc::new(Book {
                        venue: Venue::LighterRh,
                        bid: long.0,
                        ask: long.1,
                    }) as Arc<dyn VenueApi>,
                ),
                (
                    Venue::HyperliquidIo,
                    Arc::new(Book {
                        venue: Venue::HyperliquidIo,
                        bid: short.0,
                        ask: short.1,
                    }) as Arc<dyn VenueApi>,
                ),
            ])
        }

        /// lighter-rh 便宜（100）、hyperliquid-io 贵（101）：多便宜的、空贵的。
        fn dex_pair() -> ScanReport {
            report(vec![view(vec![
                snap(Venue::LighterRh, dec!(100)),
                snap(Venue::HyperliquidIo, dec!(101)),
            ])])
        }

        async fn run(
            report: &ScanReport,
            apis: &HashMap<Venue, Arc<dyn VenueApi>>,
        ) -> Result<(Opportunity, OrderBook, OrderBook)> {
            spread_opportunity(
                report,
                apis,
                "ANTHROPIC",
                Some("USDT"),
                Venue::LighterRh,
                Venue::HyperliquidIo,
                20,
            )
            .await
        }

        use crate::types::{ClientOrderId, OrderAck, OrderState};

        /// 只读预热的桩：记下被预热的合约与杠杆；下单、改设置一律 panic。`holding`：这家场所上
        /// 这个合约已有的净持仓（`None` = 没有）。
        struct WarmOnly {
            venue: Venue,
            warmed: Arc<std::sync::Mutex<Vec<String>>>,
            holding: Option<Decimal>,
        }

        #[async_trait]
        impl Broker for WarmOnly {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("只读预热不该下单");
            }
            async fn prepare_open(&self, _: &Symbol, _: Option<Decimal>) -> ArbResult<()> {
                panic!("只读预热不该改任何设置");
            }
            async fn warm_reads(
                &self,
                symbol: &Symbol,
                leverage: Option<Decimal>,
            ) -> ArbResult<()> {
                self.warmed
                    .lock()
                    .unwrap()
                    .push(format!("{}:{symbol}:{leverage:?}", self.venue));
                Ok(())
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("只读预热不该撤单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                self.warmed
                    .lock()
                    .unwrap()
                    .push(format!("{}:positions", self.venue));
                Ok(self
                    .holding
                    .map(|quantity| crate::broker::VenuePosition {
                        venue: self.venue,
                        symbol: symbol(),
                        net_quantity: quantity,
                        average_price: None,
                        notional_usdt: Decimal::ZERO,
                    })
                    .into_iter()
                    .collect())
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
        }

        fn open_request(quote: Option<&str>) -> OpenRequest {
            OpenRequest {
                maker: None,
                margin_mode: crate::MarginMode::Isolated,
                base: "anthropic".into(),
                quote: quote.map(str::to_string),
                long: Venue::LighterRh,
                short: Venue::HyperliquidIo,
                size: dec!(1000),
                leverage: dec!(3),
                daily_pnl: Decimal::ZERO,
                view: "funding".into(),
                depth_levels: 20,
                rules: TaskRules::default(),
                relative_to_normal: false,
                leg_order: crate::preflight::LegOrder::Slippage,
                depth_fit: None,
                force_hedge_pct: None,
            }
        }

        #[tokio::test]
        async fn the_pre_warm_only_reads_and_only_for_the_symbol_both_venues_list() {
            let warmed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let brokers: HashMap<Venue, Arc<dyn Broker>> = [Venue::LighterRh, Venue::HyperliquidIo]
                .into_iter()
                .map(|venue| {
                    (
                        venue,
                        Arc::new(WarmOnly {
                            venue,
                            warmed: Arc::clone(&warmed),
                            holding: None,
                        }) as Arc<dyn Broker>,
                    )
                })
                .collect();
            warm_legs(&brokers, &dex_pair(), &open_request(Some("usdt"))).await;
            let mut seen = warmed.lock().unwrap().clone();
            seen.sort();
            assert_eq!(
                seen,
                [
                    "hyperliquid-io:ANTHROPIC/USDT:Some(3)",
                    "lighter-rh:ANTHROPIC/USDT:Some(3)"
                ]
            );

            // 只有一家场所有这个合约、或 quote 对不上：不预热（也不报错）。
            warmed.lock().unwrap().clear();
            let one_venue = report(vec![view(vec![snap(Venue::LighterRh, dec!(100))])]);
            warm_legs(&brokers, &one_venue, &open_request(None)).await;
            warm_legs(&brokers, &dex_pair(), &open_request(Some("USDC"))).await;
            assert!(warmed.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn the_early_flat_check_blocks_an_existing_position_and_names_the_symbol_it_checked()
        {
            let warmed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let brokers = |held: Option<Decimal>| -> HashMap<Venue, Arc<dyn Broker>> {
                [Venue::LighterRh, Venue::HyperliquidIo]
                    .into_iter()
                    .map(|venue| {
                        (
                            venue,
                            Arc::new(WarmOnly {
                                venue,
                                warmed: Arc::clone(&warmed),
                                holding: held.filter(|_| venue == Venue::HyperliquidIo),
                            }) as Arc<dyn Broker>,
                        )
                    })
                    .collect()
            };
            let request = open_request(Some("usdt"));
            let checked = check_legs_flat(&brokers(None), &dex_pair(), &request)
                .await
                .unwrap();
            assert_eq!(checked, Some(symbol()));
            let mut seen = warmed.lock().unwrap().clone();
            seen.sort();
            assert_eq!(seen, ["hyperliquid-io:positions", "lighter-rh:positions"]);

            // 空腿场所上已经有这个合约：照旧拒绝（和执行器下单前的检查同一句话）。
            let err = check_legs_flat(&brokers(Some(dec!(-0.5))), &dex_pair(), &request)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("该合约已有持仓"), "{err}");
            // 净持仓为 0 不算。
            assert!(
                check_legs_flat(&brokers(Some(Decimal::ZERO)), &dex_pair(), &request)
                    .await
                    .is_ok()
            );
            // 认不出合约：不查，交给执行器自己查。
            let none = check_legs_flat(
                &brokers(Some(dec!(1))),
                &dex_pair(),
                &open_request(Some("USDC")),
            )
            .await
            .unwrap();
            assert_eq!(none, None);
        }

        #[tokio::test]
        async fn dex_pairs_are_priced_from_fresh_books() {
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let (op, long_book, short_book) = run(&dex_pair(), &apis).await.unwrap();
            // (101 − 100) / 100.5 = 0.995%
            let basis = op.executable_basis_pct.unwrap();
            assert!(basis > dec!(0.99) && basis < dec!(1.0), "{basis}");
            assert!(!op.spread_unknown && op.spread_profitable());
            assert_eq!(op.symbol, symbol());
            assert_eq!(long_book.best_ask(), Some(dec!(100)));
            assert_eq!(short_book.best_bid(), Some(dec!(101)));
        }

        #[tokio::test]
        async fn risk_lookup_skips_a_same_named_identity_without_the_legs() {
            // 同名合约被拆成两个簇（OPENAI 实测：DEX 簇 1720、CEX 簇 1630），第一个里没有这两条腿。
            let report = report(vec![
                view(vec![snap(Venue::Lighter, dec!(150))]),
                view(vec![
                    snap(Venue::LighterRh, dec!(100)),
                    snap(Venue::HyperliquidIo, dec!(101)),
                ]),
            ]);
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let (op, _, _) = run(&report, &apis).await.unwrap();
            assert!(crate::cli::risk_of(&report, &op, dec!(2)).is_some());
        }

        #[tokio::test]
        async fn inverted_books_are_rejected() {
            // 标记价上空腿更贵，但盘口上空腿买一低于多腿卖一：开仓当时就是亏的。
            let apis = books((dec!(99.9), dec!(100)), (dec!(99.5), dec!(99.6)));
            let error = run(&dex_pair(), &apis).await.unwrap_err().to_string();
            assert!(error.contains("不为正"), "{error}");
        }

        #[tokio::test]
        async fn a_positive_gap_that_does_not_cover_costs_is_rejected() {
            // 0.01% 的价差，往返手续费 0.08%。
            let apis = books((dec!(99.99), dec!(100)), (dec!(100.01), dec!(100.02)));
            let error = run(&dex_pair(), &apis).await.unwrap_err().to_string();
            assert!(error.contains("不划算"), "{error}");
        }

        #[tokio::test]
        async fn an_excluded_leg_is_rejected() {
            let mut report = dex_pair();
            report.suspicious.push(ExcludedReading {
                venue: Venue::HyperliquidIo,
                symbol: symbol(),
                reason: "与同币种其它场所差异过大".into(),
            });
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let error = run(&report, &apis).await.unwrap_err().to_string();
            assert!(error.contains("可信度筛查"), "{error}");
        }

        #[tokio::test]
        async fn legs_from_different_identities_are_rejected() {
            // 同名、但身份判定拆成了两个簇（比如股票与同名币）。
            let report = report(vec![
                view(vec![snap(Venue::LighterRh, dec!(100))]),
                view(vec![snap(Venue::HyperliquidIo, dec!(150))]),
            ]);
            let apis = books((dec!(99.9), dec!(100)), (dec!(150), dec!(150.1)));
            let error = run(&report, &apis).await.unwrap_err().to_string();
            assert!(error.contains("同一个合约身份"), "{error}");
        }

        #[tokio::test]
        async fn the_gate_uses_depth_walked_prices() {
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let (op, _, _) = run(&dex_pair(), &apis).await.unwrap();
            let leg = |venue, side, expected| LegPlan {
                symbol: symbol(),
                venue,
                side,
                notional_usdt: dec!(1000),
                limit_price: expected,
                best_price: expected,
                expected_price: expected,
                slippage: Decimal::ZERO,
                book_notional: dec!(100000),
                fee: Decimal::ZERO,
                maker_rest_ms: None,
            };
            let plan = |long_price, short_price| Plan {
                symbol: symbol(),
                long: leg(Venue::LighterRh, Side::Buy, long_price),
                short: leg(Venue::HyperliquidIo, Side::Sell, short_price),
                expected_cost: Decimal::ZERO,
                min_locked_basis: None,
                leg_order: crate::preflight::LegOrder::Slippage,
            };
            // 一档就吃完：与买一卖一一致，净额为正。
            let edge = spread_edge(&op, &plan(dec!(100), dec!(101)), None).unwrap();
            assert!(edge.depth_net > Decimal::ZERO);
            assert_eq!(edge.depth_basis_pct, edge.top_basis_pct);
            // 吃穿好几档后两腿均价几乎贴在一起：买一卖一看着赚，吃完深度就不够了。
            let thin = spread_edge(&op, &plan(dec!(100.45), dec!(100.55)), None).unwrap();
            assert!(thin.top_net > Decimal::ZERO && thin.depth_net <= Decimal::ZERO);
            // 设了收敛目标：只赚得到入场与目标之间那一段。
            let targeted = spread_edge(&op, &plan(dec!(100), dec!(101)), Some(dec!(0.5))).unwrap();
            assert_eq!(targeted.target_net, Some(targeted.depth_net - dec!(0.005)));
            assert_eq!(
                targeted.break_even_target_pct,
                (targeted.depth_net * dec!(100)).round_dp(4)
            );
        }

        /// 挂单开仓：多腿（io）挂在自己的买一上，不穿价；手续费口径改成「这家挂单开、吃单平，对面吃单开平」；
        /// 挂单腿是第一腿；只有相对正常基差的单子可以挂单，挂单的场所必须是两条腿之一。
        #[tokio::test]
        async fn a_maker_leg_rests_at_its_own_best_price_and_the_fees_follow() {
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let report = dex_pair();
            let limits = Limits {
                max_position_usdt: dec!(5000),
                max_open_positions: 5,
                max_daily_loss_usdt: dec!(100),
                max_slippage: dec!(0.002),
                max_entry_basis_pct: dec!(0.5),
                min_liq_distance_pct: dec!(8),
                require_stable_funding: false,
            };
            let request = |maker: Option<MakerLeg>, relative: bool| OpenRequest {
                maker,
                margin_mode: crate::MarginMode::Isolated,
                base: "ANTHROPIC".into(),
                quote: Some("USDT".into()),
                long: Venue::HyperliquidIo,
                short: Venue::LighterRh,
                size: dec!(1000),
                leverage: dec!(2),
                daily_pnl: Decimal::ZERO,
                view: "spread".into(),
                depth_levels: 20,
                rules: TaskRules {
                    basis_exit_pct: Some(dec!(-1.5)),
                    stop_loss_usdt: Some(dec!(5)),
                    ..TaskRules::default()
                },
                relative_to_normal: relative,
                leg_order: crate::preflight::LegOrder::Slippage,
                depth_fit: None,
                force_hedge_pct: None,
            };
            let maker = MakerLeg {
                venue: Venue::HyperliquidIo,
                entry_fee: Decimal::ZERO,
                exit_fee: dec!(0.0004),
                rest_ms: 8_000,
            };
            let go = |r: OpenRequest| {
                let (report, apis, limits) = (&report, &apis, &limits);
                async move {
                    prepare(report, apis, &r, 0, limits, LeveragePolicy::Strict)
                        .await
                        .map_err(|e| e.to_string())
                }
            };
            let taker = go(request(None, true)).await.unwrap();
            let resting = go(request(Some(maker), true)).await.unwrap();
            // 挂在 io 自己的买一（101）上：买腿不再吃 101.1 的卖一。
            let leg = &resting.plan.long;
            assert_eq!(
                (leg.limit_price, leg.expected_price, leg.best_price),
                (dec!(101), dec!(101), dec!(101))
            );
            assert_eq!(leg.maker_rest_ms, Some(8_000));
            assert_eq!(leg.fee, dec!(0.0004), "回滚 / 平仓按吃单费率");
            assert_eq!(resting.plan.short.maker_rest_ms, None);
            assert!(std::ptr::eq(resting.plan.first_leg(), &resting.plan.long));
            assert_eq!(taker.plan.long.maker_rest_ms, None);
            // 开仓少付了穿价，价差账更好：入场价差（持仓口径 空 − 多）更接近 0。
            let (resting_edge, taker_edge) = (
                resting.spread.clone().unwrap(),
                taker.spread.clone().unwrap(),
            );
            assert!(resting_edge.depth_basis_pct > taker_edge.depth_basis_pct);
            // 手续费口径：对面（lighter-rh）吃单开平两次 + 这家挂单开（0）+ 吃单平（0.0004）。
            let other_fee = resting.opportunity.short_fee;
            assert_eq!(
                resting.opportunity.round_trip_fee,
                other_fee * Decimal::TWO + dec!(0.0004)
            );
            // 保本线用的就是这个手续费：和两腿都吃单的相比，只差这家的费用（挂单开 + 吃单平 − 吃单开平）。
            assert_eq!(
                resting.plan.min_locked_basis.unwrap() - taker.plan.min_locked_basis.unwrap(),
                dec!(0.0004) - taker.opportunity.long_fee * Decimal::TWO
            );

            // 不是相对正常基差的单子、挂单场所不是任何一条腿、挂单时间为 0：都拒绝。
            assert!(
                go(request(Some(maker), false))
                    .await
                    .unwrap_err()
                    .contains("相对正常基差")
            );
            let elsewhere = MakerLeg {
                venue: Venue::Binance,
                ..maker
            };
            assert!(
                go(request(Some(elsewhere), true))
                    .await
                    .unwrap_err()
                    .contains("不是这笔单的任何一条腿")
            );
            let zero = MakerLeg {
                rest_ms: 0,
                ..maker
            };
            assert!(
                go(request(Some(zero), true))
                    .await
                    .unwrap_err()
                    .contains("挂单时间")
            );
        }

        /// 一笔已对冲的价差仓位：多 lighter-rh 10 个 @ 100、空 hyperliquid-io 10 个 @ 101，
        /// 收敛目标 0.1%。
        fn hedged_spread() -> PairPosition {
            let leg = |venue, side, price: Decimal| crate::LegFill {
                venue,
                side,
                notional_usdt: price * dec!(10),
                average_price: price,
                fee_usdt: Decimal::ZERO,
                client_order_id: crate::types::ClientOrderId::for_leg("wired", side, 0),
                margin_usdt: Some(dec!(500)),
            };
            PairPosition {
                margin_mode: crate::MarginMode::Isolated,
                id: "wired".into(),
                symbol: symbol(),
                strategy: Strategy::Spread,
                long: Some(leg(Venue::LighterRh, Side::Buy, dec!(100))),
                short: Some(leg(Venue::HyperliquidIo, Side::Sell, dec!(101))),
                entry_basis_pct: dec!(0.995),
                expected_round_trip_cost: Decimal::ZERO,
                status: PositionStatus::Open,
                opened_at: chrono::Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(2)),
                rules: TaskRules {
                    basis_exit_pct: Some(dec!(0.1)),
                    ..TaskRules::default()
                },
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
            }
        }

        /// 整条实盘平仓路径：标记价收敛触发 → 经连接器现拉两边盘口 → 算「现在平掉整笔」
        /// 的预估 → 不为正就持有、为正才交给执行器。
        #[tokio::test]
        async fn a_converged_position_is_checked_against_fresh_books_before_closing() {
            let path = std::env::temp_dir().join(format!("arb-wired-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            let ledger = Arc::new(Ledger::open(&path).await.unwrap());
            // 没有券商：真要下单就会报错，正好用来证明走到了平仓这一步。
            let executor = Executor::new(Arc::clone(&ledger), Vec::new());
            // 标记价：100 对 100.05，基差 ≈ 0.05% ≤ 目标 0.1%，触发。
            let snapshots = HashMap::from([
                (
                    (Venue::LighterRh, symbol()),
                    snap(Venue::LighterRh, dec!(100)),
                ),
                (
                    (Venue::HyperliquidIo, symbol()),
                    snap(Venue::HyperliquidIo, dec!(100.05)),
                ),
            ]);

            // 盘口不配合：多腿只能卖 99、空腿要 102 才买得回 —— 整笔亏 20，持有。
            let losing = books((dec!(99), dec!(99.1)), (dec!(101.9), dec!(102)));
            let mut position = hedged_spread();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &ExitRetries::default(),
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report =
                monitor_position(&executor, &mut position, &snapshots, &losing, &ctx).await;
            let evaluation = report.evaluation.expect("应当评估");
            assert_eq!(evaluation.action, Action::Hold);
            assert!(!report.executed && report.error.is_none());
            assert!(
                evaluation.skipped.iter().any(|why| why.contains("不为正")),
                "{:?}",
                evaluation.skipped
            );
            let quote = evaluation.observation.exit.expect("应当按盘口核对过");
            assert_eq!(quote.net_usdt, dec!(-20));
            assert_eq!(position.status, PositionStatus::Open);

            // 盘口配合：多腿卖 100.5、空腿 100.4 买回 —— 整笔赚 11，交给执行器平仓。
            let paying = books((dec!(100.5), dec!(100.6)), (dec!(100.3), dec!(100.4)));
            let mut position = hedged_spread();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &ExitRetries::default(),
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report =
                monitor_position(&executor, &mut position, &snapshots, &paying, &ctx).await;
            let evaluation = report.evaluation.expect("应当评估");
            assert!(
                matches!(evaluation.action, Action::Close { .. }),
                "{:?}",
                evaluation.action
            );
            assert_eq!(evaluation.observation.exit.unwrap().net_usdt, dec!(11));
            assert!(
                report.executed && report.error.is_some(),
                "没有券商，平仓应当报错"
            );
            let _ = tokio::fs::remove_file(&path).await;
        }

        /// 2026-09-30 实盘 NEAR：吃完深度后价差 0.1137%，平仓穿价约 0.00425%，往返手续费
        /// 0.045%（arcus 0.0225% × 2，lighter-rh 免费）→ 收敛到 0 净 0.06445%。目标 0.1%
        /// 只留下 0.044% 的空间，不够成本：必须拒绝，并说出保本线。
        #[test]
        fn the_near_target_leaves_no_room_after_costs() {
            let depth_net = dec!(0.001137) - dec!(0.0000425) - dec!(0.00045);
            let reason = spread_target_shortfall(depth_net, dec!(0.1)).unwrap();
            assert!(
                reason.contains("0.0644") && reason.contains("保本线"),
                "{reason}"
            );
            assert!(spread_target_shortfall(depth_net, dec!(0.05)).is_none());
        }

        #[tokio::test]
        async fn prepare_rejects_a_spread_whose_target_leaves_no_room() {
            // 标记价基差 ≈ 0.995%；两边一档价差各 0.1 → 平仓穿价约 0.1%，手续费 0.08%：
            // 收敛到 0 净约 0.815%。目标 0.9% 不够，0.5% 够。
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let request = |target| OpenRequest {
                maker: None,
                margin_mode: crate::MarginMode::Isolated,
                base: "ANTHROPIC".into(),
                quote: Some("USDT".into()),
                long: Venue::LighterRh,
                short: Venue::HyperliquidIo,
                size: dec!(1000),
                leverage: dec!(2),
                daily_pnl: Decimal::ZERO,
                view: "spread".into(),
                depth_levels: 20,
                rules: TaskRules {
                    basis_exit_pct: Some(target),
                    ..TaskRules::default()
                },
                relative_to_normal: false,
                leg_order: crate::preflight::LegOrder::Slippage,
                depth_fit: None,
                force_hedge_pct: None,
            };
            let limits = Limits {
                max_position_usdt: dec!(5000),
                max_open_positions: 5,
                max_daily_loss_usdt: dec!(100),
                max_slippage: dec!(0.002),
                max_entry_basis_pct: dec!(5),
                min_liq_distance_pct: dec!(8),
                require_stable_funding: false,
            };
            let report = dex_pair();
            let error = prepare(
                &report,
                &apis,
                &request(dec!(0.9)),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(error.contains("保本线"), "{error}");
            let prepared = prepare(
                &report,
                &apis,
                &request(dec!(0.5)),
                0,
                &limits,
                LeveragePolicy::Strict,
            )
            .await
            .unwrap();
            let edge = prepared.spread.clone().unwrap();
            assert!(edge.target_net.unwrap() > Decimal::ZERO);
            // 第二腿的保本线与这里的判断是同一条线：吃完深度后的价差 − 保本线 = 收敛到目标的净收益。
            let floor = prepared.plan.min_locked_basis.expect("价差单要带保本线");
            let gap = edge.depth_basis_pct / dec!(100) - floor - edge.target_net.unwrap();
            assert!(gap.abs() < dec!(0.000002), "{gap}");
        }

        /// 「相对正常基差」：多 hyperliquid-io（贵，101）/ 空 lighter-rh（便宜，100），开仓可成交价差约 −1.1%
        /// （持仓口径 空 − 多），原口径一定拒绝。假设这两家「正常」就差 −1.5%：从 −1.1% 收敛到 −1.5% 时空腿
        /// 相对又便宜了约 0.4%，赚的是这一段。只有带上收敛目标和止损、且收敛到目标扣成本仍为正才放行。
        #[tokio::test]
        async fn relative_to_normal_spreads_open_only_with_a_target_that_pays_and_a_stop_loss() {
            let apis = books((dec!(99.9), dec!(100)), (dec!(101), dec!(101.1)));
            let report = dex_pair();
            let limits = Limits {
                max_position_usdt: dec!(5000),
                max_open_positions: 5,
                max_daily_loss_usdt: dec!(100),
                max_slippage: dec!(0.002),
                max_entry_basis_pct: dec!(0.5),
                min_liq_distance_pct: dec!(8),
                require_stable_funding: false,
            };
            // 反方向：多贵的 io、空便宜的 rh。
            let request = |relative, target: Option<Decimal>, stop: Option<Decimal>| OpenRequest {
                maker: None,
                margin_mode: crate::MarginMode::Isolated,
                base: "ANTHROPIC".into(),
                quote: Some("USDT".into()),
                long: Venue::HyperliquidIo,
                short: Venue::LighterRh,
                size: dec!(1000),
                leverage: dec!(2),
                daily_pnl: Decimal::ZERO,
                view: "spread".into(),
                depth_levels: 20,
                rules: TaskRules {
                    basis_exit_pct: target,
                    stop_loss_usdt: stop,
                    ..TaskRules::default()
                },
                relative_to_normal: relative,
                leg_order: crate::preflight::LegOrder::Slippage,
                depth_fit: None,
                force_hedge_pct: None,
            };
            let go = |r: OpenRequest| {
                let (report, apis, limits) = (&report, &apis, &limits);
                async move {
                    prepare(report, apis, &r, 0, limits, LeveragePolicy::Strict)
                        .await
                        .map_err(|e| e.to_string())
                }
            };
            // 默认口径：可成交价差为负，拒绝（这条规则对手动下单与默认自动交易不变）。
            let err = go(request(false, Some(dec!(-1.5)), Some(dec!(5))))
                .await
                .unwrap_err();
            assert!(err.contains("可成交价差不为正"), "{err}");
            // 相对模式缺目标 / 缺止损：拒绝。
            assert!(
                go(request(true, None, Some(dec!(5))))
                    .await
                    .unwrap_err()
                    .contains("基差收敛目标")
            );
            assert!(
                go(request(true, Some(dec!(-1.5)), None))
                    .await
                    .unwrap_err()
                    .contains("止损")
            );
            // 目标 −1.2%：只比入场低 ~0.1%，扣掉穿价与手续费不够 → 拒绝并给保本线。
            let err = go(request(true, Some(dec!(-1.2)), Some(dec!(5))))
                .await
                .unwrap_err();
            assert!(err.contains("保本线"), "{err}");
            // 目标 −1.5%：收敛到它扣成本仍为正 → 放行（不利入场基差门槛 0.5% 对它不适用）。
            let prepared = go(request(true, Some(dec!(-1.5)), Some(dec!(5))))
                .await
                .unwrap();
            let edge = prepared.spread.clone().unwrap();
            assert!(
                edge.depth_basis_pct < Decimal::ZERO,
                "{}",
                edge.depth_basis_pct
            );
            assert!(edge.target_net.unwrap() > Decimal::ZERO);
            // 保本线跟着目标走：目标 −1.5%，锁定价差可以为负，但不能低于「−1.5% + 穿价 + 手续费」。
            let floor = prepared.plan.min_locked_basis.unwrap();
            assert!(floor < Decimal::ZERO && floor > dec!(-0.015), "{floor}");
            let gap = edge.depth_basis_pct / dec!(100) - floor - edge.target_net.unwrap();
            assert!(gap.abs() < dec!(0.000002), "{gap}");
        }
    }

    fn history(hours: usize, rate: Decimal) -> Vec<arb_core::FundingPoint> {
        let top = chrono::Utc::now().timestamp() / 3600 * 3600;
        (0..hours)
            .rev()
            .map(|back| arb_core::FundingPoint {
                at: chrono::DateTime::from_timestamp(top - back as i64 * 3600, 0).unwrap(),
                rate,
            })
            .collect()
    }

    /// 「平仓对账」说清楚实际比预估差在哪：多腿卖价、空腿买回价各差多少 USDT，手续费差多少 ——
    /// 三项加起来就是「实际净 − 预估净」。
    #[test]
    fn the_close_reconciliation_names_which_leg_and_how_much() {
        use crate::breakdown::{ExitFill, ExitFills};
        use rust_decimal_macros::dec;
        let quote = crate::monitor::ExitQuote {
            long_price: dec!(100),
            short_price: dec!(100.05),
            exit_basis_pct: dec!(0.05),
            exit_fee_usdt: dec!(0.2),
            net_usdt: dec!(1),
            net_pct: dec!(0.1),
            cost: None,
            fraction: Decimal::ONE,
            long_worst_price: dec!(100),
            short_worst_price: dec!(100.05),
        };
        let fills = ExitFills {
            long: Some(ExitFill {
                price: dec!(99.9),
                quantity: dec!(10),
                fee_usdt: dec!(0.1),
            }),
            short: Some(ExitFill {
                price: dec!(100.2),
                quantity: dec!(10),
                fee_usdt: dec!(0.15),
            }),
        };
        let text = exit_shortfall_text(&quote, Some(&fills));
        // 多腿少卖 0.1 × 10 = −1；空腿多付 0.15 × 10 = −1.5；手续费 0.2 → 0.25 多付 0.05。
        assert!(
            text.contains("多腿卖价预估 100 → 实际 99.9（-1 USDT）"),
            "{text}"
        );
        assert!(
            text.contains("空腿买回价预估 100.05 → 实际 100.2（-1.5 USDT）"),
            "{text}"
        );
        assert!(
            text.contains("手续费预估 0.2 → 实际 0.25（-0.05 USDT）"),
            "{text}"
        );
        // 找不到成交记录 / 缺一条腿：如实说，不编。
        assert!(exit_shortfall_text(&quote, None).contains("没找到"));
        let only_long = ExitFills {
            short: None,
            ..fills
        };
        assert!(exit_shortfall_text(&quote, Some(&only_long)).contains("空腿买回价没有成交记录"));
    }

    #[test]
    fn the_stability_gate_rejects_reversed_or_unknown_funding_only_when_required() {
        use rust_decimal_macros::dec;
        let now = chrono::Utc::now();
        // 稳定：放行并带上评估。
        let stable = stability_gate(
            &history(30, dec!(0.00001)),
            &history(30, dec!(0.00005)),
            now,
            true,
        )
        .unwrap();
        assert!(stable.unwrap().stable);
        // 已经反转（费差为负）：要求稳定就拒绝，说明原因；不要求就放行。
        let (long, short) = (history(30, dec!(0.00005)), history(30, dec!(0.00001)));
        let error = stability_gate(&long, &short, now, true)
            .unwrap_err()
            .to_string();
        assert!(error.contains("费差不稳定"), "{error}");
        assert!(
            !stability_gate(&long, &short, now, false)
                .unwrap()
                .unwrap()
                .stable
        );
        // 历史不够：不能当成稳定。
        let error = stability_gate(
            &history(3, dec!(0.00001)),
            &history(3, dec!(0.00005)),
            now,
            true,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("历史不够"), "{error}");
        assert!(
            stability_gate(
                &history(3, dec!(0.00001)),
                &history(3, dec!(0.00005)),
                now,
                false
            )
            .unwrap()
            .is_none()
        );
    }

    mod external {
        use super::super::*;
        use crate::reconcile::{Divergence, DivergenceKind as Kind};
        use crate::types::{ClientOrderId, OrderAck, OrderState};
        use arb_core::{ArbResult, Side};
        use async_trait::async_trait;
        use rust_decimal_macros::dec;
        use std::time::{Duration, Instant};

        fn leg(venue: Venue, side: Side, price: Decimal) -> crate::LegFill {
            crate::LegFill {
                venue,
                side,
                notional_usdt: price * dec!(10),
                average_price: price,
                fee_usdt: Decimal::ZERO,
                client_order_id: ClientOrderId::for_leg("ext", side, 0),
                margin_usdt: Some(dec!(300)),
            }
        }

        fn position(status: PositionStatus) -> PairPosition {
            PairPosition {
                margin_mode: crate::MarginMode::Isolated,
                id: "ext-1".into(),
                symbol: Symbol::perp("PONS", "USDT"),
                strategy: Strategy::Funding,
                long: Some(leg(Venue::LighterRh, Side::Buy, dec!(0.555))),
                short: Some(leg(Venue::Arcus, Side::Sell, dec!(0.554))),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status,
                opened_at: chrono::Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(3)),
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
            }
        }

        fn divergence(kind: Kind, venue: Venue, reference: &str) -> Divergence {
            Divergence {
                kind,
                venue: Some(venue),
                symbol: None,
                reference: reference.into(),
                detail: "测试".into(),
            }
        }

        fn reconciliation(divergences: Vec<Divergence>) -> Reconciliation {
            Reconciliation {
                divergences,
                checked_positions: 2,
                checked_venues: 2,
                elapsed_ms: Vec::new(),
            }
        }

        fn both_missing() -> Vec<Divergence> {
            vec![
                divergence(Kind::PositionMissingOnVenue, Venue::LighterRh, "PONS/USDT"),
                divergence(Kind::PositionMissingOnVenue, Venue::Arcus, "PONS/USDT"),
            ]
        }

        #[test]
        fn a_position_gone_from_both_venues_is_adopted_only_on_the_second_sighting() {
            let position = position(PositionStatus::Open);
            let mut seen = HashMap::new();
            let start = Instant::now();
            let first = check_external_closes(
                &reconciliation(both_missing()),
                &[&position],
                &mut seen,
                start,
            );
            assert!(first.adopt.is_empty(), "第一次只记下，不结束");
            assert!(first.notes[0].message.contains("再对账确认一次"));
            // 太快的第二次（比如连点两下「执行一轮规则」）也不算。
            let soon = check_external_closes(
                &reconciliation(both_missing()),
                &[&position],
                &mut seen,
                start + Duration::from_secs(10),
            );
            assert!(soon.adopt.is_empty());
            let second = check_external_closes(
                &reconciliation(both_missing()),
                &[&position],
                &mut seen,
                start + EXTERNAL_CLOSE_CONFIRM,
            );
            assert_eq!(second.adopt, vec!["ext-1".to_string()]);
        }

        #[test]
        fn a_glitch_that_clears_resets_the_clock() {
            let position = position(PositionStatus::Open);
            let mut seen = HashMap::new();
            let start = Instant::now();
            check_external_closes(
                &reconciliation(both_missing()),
                &[&position],
                &mut seen,
                start,
            );
            // 下一轮仓位又对上了（上一次是接口抽风）：清掉，重新计时。
            check_external_closes(
                &reconciliation(Vec::new()),
                &[&position],
                &mut seen,
                start + Duration::from_secs(30),
            );
            assert!(seen.is_empty());
            let again = check_external_closes(
                &reconciliation(both_missing()),
                &[&position],
                &mut seen,
                start + Duration::from_secs(70),
            );
            assert!(again.adopt.is_empty(), "从头计时，不沿用抽风之前那次");
        }

        #[test]
        fn one_leg_gone_from_the_venue_is_confirmed_twice_then_handed_to_the_exit() {
            let position = position(PositionStatus::Open);
            let mut seen = HashMap::new();
            let divergences = vec![divergence(
                Kind::PositionMissingOnVenue,
                Venue::Arcus,
                "PONS/USDT",
            )];
            let start = Instant::now();
            let first = check_external_closes(
                &reconciliation(divergences.clone()),
                &[&position],
                &mut seen,
                start,
            );
            assert!(first.adopt.is_empty() && first.lone.is_empty());
            let message = &first.notes[0].message;
            assert!(
                message.contains("arcus 腿在交易所已没有仓位")
                    && message.contains("lighter-rh 腿还在")
                    && message.contains("再对账确认（约 3 分钟）"),
                "{message}"
            );
            let soon = check_external_closes(
                &reconciliation(divergences.clone()),
                &[&position],
                &mut seen,
                start + Duration::from_secs(10),
            );
            assert!(soon.lone.is_empty(), "太快的第二次不算");
            let second = check_external_closes(
                &reconciliation(divergences.clone()),
                &[&position],
                &mut seen,
                start + EXTERNAL_CLOSE_CONFIRM,
            );
            assert!(
                second.lone.is_empty(),
                "那家只有这一条腿：一分钟不够（认不出查询返回了空列表），要等更久"
            );
            let second = check_external_closes(
                &reconciliation(divergences),
                &[&position],
                &mut seen,
                start + EXTERNAL_CLOSE_CONFIRM_SINGLE,
            );
            assert!(second.adopt.is_empty());
            assert_eq!(second.lone, vec![("ext-1".to_string(), Venue::Arcus)]);
        }

        /// 只剩一条腿时要下单，所以比两腿都没了更严：这笔自己的没了结的单也挡；那家全部仓位都读不到（像是查询返回了空）
        /// 也挡；已经在平仓中的、剩下那条数量也对不上的都不碰。
        #[test]
        fn a_lone_leg_is_not_closed_on_doubtful_evidence() {
            let start = Instant::now();
            let gone = || divergence(Kind::PositionMissingOnVenue, Venue::Arcus, "PONS/USDT");
            let own = ClientOrderId::for_exit("ext-1", Side::Sell, 1).0;
            let mut other = position(PositionStatus::Open);
            other.id = "ext-2".into();
            other.symbol = Symbol::perp("CRCL", "USDT");
            let cases: Vec<(&str, PositionStatus, Vec<Divergence>, bool)> = vec![
                (
                    "没核对",
                    PositionStatus::Open,
                    vec![
                        gone(),
                        divergence(Kind::Unverified, Venue::LighterRh, "lighter-rh"),
                    ],
                    false,
                ),
                (
                    "游离挂单",
                    PositionStatus::Open,
                    vec![
                        gone(),
                        divergence(Kind::UnknownOpenOrder, Venue::Arcus, "x"),
                    ],
                    false,
                ),
                (
                    "自己的单没了结",
                    PositionStatus::Open,
                    vec![
                        gone(),
                        divergence(Kind::OrderMissingOnVenue, Venue::Arcus, &own),
                    ],
                    false,
                ),
                (
                    "剩下那条数量也不对",
                    PositionStatus::Open,
                    vec![
                        gone(),
                        divergence(Kind::PositionMismatch, Venue::LighterRh, "PONS/USDT"),
                    ],
                    false,
                ),
                (
                    "那家全部读不到",
                    PositionStatus::Open,
                    vec![
                        gone(),
                        divergence(Kind::PositionMissingOnVenue, Venue::Arcus, "CRCL/USDT"),
                    ],
                    true,
                ),
                ("平仓中", PositionStatus::Closing, vec![gone()], false),
            ];
            for (name, status, divergences, with_other) in cases {
                let position = position(status);
                let exposed: Vec<&PairPosition> = if with_other {
                    vec![&position, &other]
                } else {
                    vec![&position]
                };
                let mut seen = HashMap::new();
                check_external_closes(
                    &reconciliation(divergences.clone()),
                    &exposed,
                    &mut seen,
                    start,
                );
                let check = check_external_closes(
                    &reconciliation(divergences),
                    &exposed,
                    &mut seen,
                    start + Duration::from_secs(300),
                );
                assert!(check.lone.is_empty() && check.adopt.is_empty(), "{name}");
                assert!(
                    check.notes.iter().any(|note| note.position_id == "ext-1"
                        && (note.message.contains("裸敞口")
                            || note.message.contains("数量与台账不符"))),
                    "{name}: {:?}",
                    check.notes
                );
            }
            // 同一家上别的仓位还读得到：这一条是真没了。
            let position = position(PositionStatus::Open);
            let exposed = vec![&position, &other];
            let mut seen = HashMap::new();
            check_external_closes(&reconciliation(vec![gone()]), &exposed, &mut seen, start);
            let check = check_external_closes(
                &reconciliation(vec![gone()]),
                &exposed,
                &mut seen,
                start + EXTERNAL_CLOSE_CONFIRM,
            );
            assert_eq!(check.lone, vec![("ext-1".to_string(), Venue::Arcus)]);
        }

        #[tokio::test]
        async fn adopting_a_lone_leg_books_it_as_closing_and_the_reconcile_is_clean() {
            let path = std::env::temp_dir().join(format!("arb-lone-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            let ledger = Ledger::open(&path).await.unwrap();
            let open = position(PositionStatus::Open);
            ledger
                .append(&crate::Record::Position(Box::new(open.clone())))
                .await
                .unwrap();
            let lighter_qty = open.long.as_ref().unwrap().quantity().unwrap();
            let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
                (
                    Venue::LighterRh,
                    Arc::new(Holding(Venue::LighterRh, lighter_qty)) as Arc<dyn Broker>,
                ),
                (
                    Venue::Arcus,
                    Arc::new(Flat(Venue::Arcus)) as Arc<dyn Broker>,
                ),
            ]);
            let before = reconcile_ledger(&ledger, &brokers).await.unwrap();
            let (replayed, _) = ledger.replay().await.unwrap();
            let exposed = replayed.exposed();
            let mut seen = HashMap::new();
            let start = Instant::now();
            check_external_closes(&before, &exposed, &mut seen, start);
            let check = check_external_closes(
                &before,
                &exposed,
                &mut seen,
                start + EXTERNAL_CLOSE_CONFIRM_SINGLE,
            );
            assert_eq!(
                check.lone,
                vec![("ext-1".to_string(), Venue::Arcus)],
                "{:?}",
                before.divergences
            );
            let adopted = adopt_lone_legs(&ledger, &exposed, &check.lone)
                .await
                .unwrap();
            assert_eq!(adopted.len(), 1);
            let (replayed, _) = ledger.replay().await.unwrap();
            let booked = &replayed.positions["ext-1"];
            assert_eq!(booked.status, PositionStatus::Closing);
            assert!(booked.short.is_none() && booked.long.is_some());
            assert!(booked.closed_externally);
            let entry = booked.entry_legs.as_ref().unwrap();
            assert_eq!(entry.short.venue, Venue::Arcus);
            let note = booked.note.as_deref().unwrap();
            assert!(
                note.contains("arcus 腿在交易所已没有仓位") && note.contains("lighter-rh"),
                "{note}"
            );
            let after = reconcile_ledger(&ledger, &brokers).await.unwrap();
            assert!(after.is_clean(), "{:?}", after.divergences);
            tokio::fs::remove_file(&path).await.unwrap();
        }

        #[test]
        fn a_reduced_quantity_is_reported_not_adopted() {
            let position = position(PositionStatus::Open);
            let mismatch = vec![divergence(
                Kind::PositionMismatch,
                Venue::Arcus,
                "PONS/USDT",
            )];
            let check = check_external_closes(
                &reconciliation(mismatch),
                &[&position],
                &mut HashMap::new(),
                Instant::now(),
            );
            assert!(check.adopt.is_empty());
            assert!(check.notes[0].message.contains("数量与台账不符"));
        }

        #[test]
        fn an_unverified_venue_or_a_stray_order_blocks_adoption() {
            let position = position(PositionStatus::Open);
            let start = Instant::now();
            for blocker in [
                divergence(Kind::Unverified, Venue::Arcus, "arcus"),
                divergence(Kind::UnknownOpenOrder, Venue::LighterRh, "x"),
                // 台账之外的持仓：可能是这笔换了币名还在。
                divergence(Kind::PositionMismatch, Venue::LighterRh, "OTHER/USDT"),
            ] {
                let mut divergences = both_missing();
                divergences.push(blocker);
                let mut seen = HashMap::new();
                check_external_closes(
                    &reconciliation(divergences.clone()),
                    &[&position],
                    &mut seen,
                    start,
                );
                let check = check_external_closes(
                    &reconciliation(divergences),
                    &[&position],
                    &mut seen,
                    start + Duration::from_secs(300),
                );
                assert!(check.adopt.is_empty(), "{:?}", check.notes);
                assert!(check.notes[0].message.contains("暂不在台账里结束"));
            }
        }

        #[test]
        fn the_positions_own_unresolved_exit_order_does_not_block_but_another_positions_does() {
            // 某次实盘：退出单一直读不到（OrderMissingOnVenue），两条腿又都被手动平了，
            // 外部平仓识别被这张单自己挡住，只能手工改台账。
            let position = position(PositionStatus::Closing);
            let own = ClientOrderId::for_exit("ext-1", Side::Buy, 3).0;
            let start = Instant::now();
            for (reference, adopted) in [(own.as_str(), true), ("live-other-sell-exit1", false)] {
                let mut divergences = both_missing();
                divergences.push(divergence(
                    Kind::OrderMissingOnVenue,
                    Venue::Arcus,
                    reference,
                ));
                let mut seen = HashMap::new();
                check_external_closes(
                    &reconciliation(divergences.clone()),
                    &[&position],
                    &mut seen,
                    start,
                );
                let check = check_external_closes(
                    &reconciliation(divergences),
                    &[&position],
                    &mut seen,
                    start + EXTERNAL_CLOSE_CONFIRM,
                );
                assert_eq!(check.adopt.len(), usize::from(adopted), "{reference}");
            }
        }

        #[test]
        fn an_unrelated_divergence_does_not_block_this_position() {
            // 别的场所上无关的一处不一致（比如手动开的 io:ANTH 空单）不该拦住 PONS。
            let position = position(PositionStatus::Open);
            let mut divergences = both_missing();
            divergences.push(divergence(
                Kind::PositionMismatch,
                Venue::HyperliquidIo,
                "ANTH/USDT",
            ));
            let mut seen = HashMap::new();
            let start = Instant::now();
            check_external_closes(
                &reconciliation(divergences.clone()),
                &[&position],
                &mut seen,
                start,
            );
            let check = check_external_closes(
                &reconciliation(divergences),
                &[&position],
                &mut seen,
                start + EXTERNAL_CLOSE_CONFIRM,
            );
            assert_eq!(check.adopt, vec!["ext-1".to_string()]);
        }

        #[test]
        fn only_open_or_closing_positions_are_candidates() {
            for status in [PositionStatus::Opening, PositionStatus::Unwinding] {
                let position = position(status);
                let mut seen = HashMap::new();
                let start = Instant::now();
                check_external_closes(
                    &reconciliation(both_missing()),
                    &[&position],
                    &mut seen,
                    start,
                );
                let check = check_external_closes(
                    &reconciliation(both_missing()),
                    &[&position],
                    &mut seen,
                    start + Duration::from_secs(300),
                );
                assert!(check.adopt.is_empty(), "{status:?}");
            }
        }

        /// 什么仓位都没有的账户：与「交易所里已经平掉」一致。
        struct Flat(Venue);

        #[async_trait]
        impl Broker for Flat {
            fn venue(&self) -> Venue {
                self.0
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("识别外部平仓不该下任何单");
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("识别外部平仓不该撤任何单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
        }

        /// 只拿着一个币多仓的账户。
        struct Holding(Venue, Decimal);

        #[async_trait]
        impl Broker for Holding {
            fn venue(&self) -> Venue {
                self.0
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("识别外部平仓不该下任何单");
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("识别外部平仓不该撤任何单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![crate::broker::VenuePosition {
                    venue: self.0,
                    symbol: Symbol::perp("PONS", "USDT"),
                    net_quantity: self.1,
                    average_price: None,
                    notional_usdt: self.1 * dec!(0.555),
                }])
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
        }

        #[tokio::test]
        async fn adopting_ends_the_position_in_the_ledger_and_the_next_reconcile_is_clean() {
            let path =
                std::env::temp_dir().join(format!("arb-external-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            let ledger = Ledger::open(&path).await.unwrap();
            let open = position(PositionStatus::Open);
            ledger
                .append(&crate::Record::Position(Box::new(open.clone())))
                .await
                .unwrap();
            // 一张一直读不到的退出单停在 Pending：结束仓位时一并了结，否则对账永远报「本地有、交易所没有」。
            let exit = crate::types::NewOrder {
                post_only: false,
                client_order_id: ClientOrderId::for_exit("ext-1", Side::Buy, 2),
                venue: Venue::Arcus,
                symbol: open.symbol.clone(),
                side: Side::Buy,
                notional_usdt: dec!(100),
                quantity: Some(dec!(10)),
                limit_price: None,
                reduce_only: true,
                leverage: None,
                margin_mode: crate::MarginMode::Isolated,
            };
            ledger
                .append(&crate::Record::Order(Box::new(OrderState::new(exit))))
                .await
                .unwrap();
            let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
                (
                    Venue::LighterRh,
                    Arc::new(Flat(Venue::LighterRh)) as Arc<dyn Broker>,
                ),
                (
                    Venue::Arcus,
                    Arc::new(Flat(Venue::Arcus)) as Arc<dyn Broker>,
                ),
            ]);
            // 真实对账：台账有仓、两家账户都是空的。
            let before = reconcile_ledger(&ledger, &brokers).await.unwrap();
            assert_eq!(
                before
                    .divergences
                    .iter()
                    .filter(|d| d.kind == Kind::PositionMissingOnVenue)
                    .count(),
                2
            );
            let (replayed, _) = ledger.replay().await.unwrap();
            let exposed = replayed.exposed();
            let mut seen = HashMap::new();
            let start = Instant::now();
            check_external_closes(&before, &exposed, &mut seen, start);
            let check =
                check_external_closes(&before, &exposed, &mut seen, start + EXTERNAL_CLOSE_CONFIRM);
            let adopted = adopt_external_closes(&ledger, &brokers, &exposed, &check.adopt)
                .await
                .unwrap();
            assert_eq!(adopted.len(), 1);
            let note = adopted[0].note.clone().unwrap();
            assert!(
                note.contains("在交易所外部平仓") && note.contains("稍后按交易所的成交记录核算"),
                "{note}"
            );
            // 台账回放：已平仓、没有敞口；再对账，这笔不再有任何不一致。
            let (replayed, _) = ledger.replay().await.unwrap();
            let stored = &replayed.positions["ext-1"];
            assert_eq!(stored.status, PositionStatus::Closed);
            assert!(stored.long.is_none() && stored.short.is_none() && stored.closed_at.is_some());
            assert!(replayed.exposed().is_empty());
            assert!(
                replayed
                    .orders
                    .values()
                    .all(|state| !state.status.is_live()),
                "没了结的退出单也要收尾"
            );
            let after = reconcile_ledger(&ledger, &brokers).await.unwrap();
            assert!(after.is_clean(), "{:?}", after.divergences);
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    mod fees {
        use super::super::*;
        use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState};
        use arb_core::{ArbResult, Side};
        use async_trait::async_trait;
        use rust_decimal_macros::dec;

        /// 读回订单时给出 `remote` 里的状态；吃单费率 0.09%（同 Hyperliquid 的配置）。
        struct Readback {
            venue: Venue,
            remote: HashMap<String, OrderState>,
        }

        #[async_trait]
        impl Broker for Readback {
            fn venue(&self) -> Venue {
                self.venue
            }
            fn fee_per_side(&self) -> Decimal {
                dec!(0.0009)
            }
            async fn place(&self, _: &NewOrder) -> ArbResult<OrderAck> {
                panic!("改正手续费不该下单");
            }
            async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(self.remote.get(&id.0).cloned())
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("改正手续费不该撤单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
        }

        fn leg(venue: Venue, side: Side, fee: Decimal) -> crate::LegFill {
            crate::LegFill {
                venue,
                side,
                notional_usdt: dec!(200),
                average_price: dec!(100),
                fee_usdt: fee,
                client_order_id: ClientOrderId::for_leg("f", side, 0),
                margin_usdt: Some(dec!(66)),
            }
        }

        fn position(status: PositionStatus) -> PairPosition {
            PairPosition {
                margin_mode: crate::MarginMode::Isolated,
                id: "f".into(),
                symbol: Symbol::perp("BX", "USDT"),
                strategy: Strategy::Spread,
                long: Some(leg(Venue::HyperliquidXyz, Side::Buy, dec!(0.18))),
                short: Some(leg(Venue::Bitget, Side::Sell, dec!(0.12))),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status,
                opened_at: chrono::Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(3)),
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
            }
        }

        fn order(
            id: &str,
            side: Side,
            filled: Decimal,
            fee: Decimal,
            estimated: bool,
        ) -> OrderState {
            let mut state = OrderState::new(NewOrder {
                post_only: false,
                client_order_id: ClientOrderId(id.into()),
                venue: Venue::HyperliquidXyz,
                symbol: Symbol::perp("BX", "USDT"),
                side,
                notional_usdt: filled,
                quantity: Some(dec!(2)),
                limit_price: Some(dec!(100)),
                reduce_only: false,
                leverage: None,
                margin_mode: crate::MarginMode::Isolated,
            });
            state.venue_order_id = Some("1".into());
            state.status = crate::types::OrderStatus::Filled;
            state.filled_usdt = filled;
            state.average_price = Some(dec!(100));
            state.fee_usdt = fee;
            state.fee_estimated = estimated;
            state
        }

        async fn ledger_with(
            name: &str,
            position: &PairPosition,
            orders: &[OrderState],
        ) -> (Ledger, std::path::PathBuf) {
            let path =
                std::env::temp_dir().join(format!("arb-fees-{name}-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            let ledger = Ledger::open(&path).await.unwrap();
            ledger
                .append(&crate::Record::Position(Box::new(position.clone())))
                .await
                .unwrap();
            for state in orders {
                ledger
                    .append(&crate::Record::Order(Box::new(state.clone())))
                    .await
                    .unwrap();
            }
            (ledger, path)
        }

        fn brokers(remote: Vec<OrderState>) -> HashMap<Venue, Arc<dyn Broker>> {
            HashMap::from([(
                Venue::HyperliquidXyz,
                Arc::new(Readback {
                    venue: Venue::HyperliquidXyz,
                    remote: remote
                        .into_iter()
                        .map(|state| (state.order.client_order_id.0.clone(), state))
                        .collect(),
                }) as Arc<dyn Broker>,
            )])
        }

        /// 标记上线前的单：手续费正好是「成交额 × 0.09%」就是估的。读回实际 0.0086%：改订单，腿还在就改腿上的手续费。
        #[tokio::test]
        async fn a_legacy_estimated_entry_fee_is_corrected_on_the_leg_still_open() {
            let open = position(PositionStatus::Open);
            let local = order("f-buy-0", Side::Buy, dec!(200), dec!(0.18), false);
            let actual = order("f-buy-0", Side::Buy, dec!(200.01), dec!(0.0172), false);
            let (ledger, path) = ledger_with("open", &open, &[local]).await;
            let brokers = brokers(vec![actual]);
            let mut checked = HashSet::new();
            let fixes = correct_estimated_fees(&ledger, &brokers, &mut checked, 6)
                .await
                .unwrap();
            assert_eq!(fixes.len(), 1);
            assert_eq!(
                (fixes[0].recorded, fixes[0].actual),
                (dec!(0.18), dec!(0.0172))
            );
            let (replayed, _) = ledger.replay().await.unwrap();
            let fixed = &replayed.orders["f-buy-0"];
            assert_eq!(fixed.fee_usdt, dec!(0.0172));
            assert!(!fixed.fee_estimated);
            assert_eq!(fixed.filled_usdt, dec!(200), "只改手续费，成交额照旧");
            let position = &replayed.positions["f"];
            assert_eq!(position.long.as_ref().unwrap().fee_usdt, dec!(0.0172));
            assert_eq!(position.short.as_ref().unwrap().fee_usdt, dec!(0.12));
            assert_eq!(position.realized_fee_usdt, Decimal::ZERO);
            // 改过的不再是候选（手续费不再等于 0.09%），核过的也记下了。
            let again = correct_estimated_fees(&ledger, &brokers, &mut HashSet::new(), 6)
                .await
                .unwrap();
            assert!(again.is_empty());
            tokio::fs::remove_file(&path).await.unwrap();
        }

        /// 已平仓的：腿已经没了，改已实现手续费，备注里写一句。
        #[tokio::test]
        async fn a_flagged_exit_fee_of_a_closed_position_corrects_the_realized_fee() {
            let mut closed = position(PositionStatus::Closed);
            closed.long = None;
            closed.short = None;
            closed.realized_fee_usdt = dec!(0.4378);
            closed.realized_source = Some(crate::RealizedSource::Executor);
            closed.note = Some("已实现盈亏 -0.168 USDT".into());
            let mut local = order("f-sell-exit1", Side::Sell, dec!(200.24), dec!(0.1802), true);
            local.order.reduce_only = true;
            let mut actual = local.clone();
            actual.fee_usdt = dec!(0.0173);
            actual.fee_estimated = false;
            let (ledger, path) = ledger_with("closed", &closed, &[local]).await;
            let fixes =
                correct_estimated_fees(&ledger, &brokers(vec![actual]), &mut HashSet::new(), 6)
                    .await
                    .unwrap();
            assert_eq!(fixes.len(), 1);
            let (replayed, _) = ledger.replay().await.unwrap();
            let position = &replayed.positions["f"];
            assert_eq!(position.realized_fee_usdt, dec!(0.2749));
            let note = position.note.as_deref().unwrap();
            assert!(
                note.starts_with("已实现盈亏") && note.contains("0.1802 → 0.0173"),
                "{note}"
            );
            tokio::fs::remove_file(&path).await.unwrap();
        }

        /// 同一笔仓位两条腿的单在同一批里改：两次改动都要留下，不能后一次盖掉前一次。
        #[tokio::test]
        async fn two_estimated_orders_of_one_closed_position_both_reach_its_realized_fee() {
            let mut closed = position(PositionStatus::Closed);
            closed.long = None;
            closed.short = None;
            closed.realized_fee_usdt = dec!(1);
            closed.realized_source = Some(crate::RealizedSource::Executor);
            let entry = order("f-buy-0", Side::Buy, dec!(200), dec!(0.18), false);
            let mut exit = order("f-sell-exit1", Side::Sell, dec!(200), dec!(0.18), true);
            exit.order.reduce_only = true;
            let mut entry_actual = entry.clone();
            entry_actual.fee_usdt = dec!(0.0172);
            let mut exit_actual = exit.clone();
            exit_actual.fee_usdt = dec!(0.0173);
            exit_actual.fee_estimated = false;
            let (ledger, path) = ledger_with("two", &closed, &[entry, exit]).await;
            let fixes = correct_estimated_fees(
                &ledger,
                &brokers(vec![entry_actual, exit_actual]),
                &mut HashSet::new(),
                6,
            )
            .await
            .unwrap();
            assert_eq!(fixes.len(), 2);
            let (replayed, _) = ledger.replay().await.unwrap();
            let position = &replayed.positions["f"];
            assert_eq!(
                position.realized_fee_usdt,
                dec!(1) - dec!(0.1628) - dec!(0.1627)
            );
            let note = position.note.as_deref().unwrap();
            assert!(
                note.contains("0.18 → 0.0172") && note.contains("0.18 → 0.0173"),
                "{note}"
            );
            tokio::fs::remove_file(&path).await.unwrap();
        }

        /// 读回对不上（成交额差太多、还是估的、查不到）都不改，下次再核；按成交记录核算过的已平仓仓位只改订单。
        #[tokio::test]
        async fn doubtful_readbacks_change_nothing_and_venue_settled_positions_keep_their_pnl() {
            let open = position(PositionStatus::Open);
            let local = order("f-buy-0", Side::Buy, dec!(200), dec!(0.18), false);
            for (name, remote) in [
                (
                    "差太多",
                    vec![order("f-buy-0", Side::Buy, dec!(150), dec!(0.0129), false)],
                ),
                (
                    "还是估的",
                    vec![order("f-buy-0", Side::Buy, dec!(200), dec!(0.18), true)],
                ),
                ("查不到", vec![]),
            ] {
                let (ledger, path) =
                    ledger_with("doubt", &open, std::slice::from_ref(&local)).await;
                let mut checked = HashSet::new();
                let fixes = correct_estimated_fees(&ledger, &brokers(remote), &mut checked, 6)
                    .await
                    .unwrap();
                assert!(fixes.is_empty(), "{name}");
                assert!(checked.is_empty(), "{name}：下次还要核");
                let (replayed, _) = ledger.replay().await.unwrap();
                assert_eq!(replayed.orders["f-buy-0"].fee_usdt, dec!(0.18), "{name}");
                tokio::fs::remove_file(&path).await.unwrap();
            }
            let mut settled = position(PositionStatus::Closed);
            settled.long = None;
            settled.short = None;
            settled.realized_fee_usdt = dec!(0.3);
            settled.realized_source = Some(crate::RealizedSource::VenueFills);
            let (ledger, path) = ledger_with("venue", &settled, std::slice::from_ref(&local)).await;
            let actual = order("f-buy-0", Side::Buy, dec!(200), dec!(0.0172), false);
            let fixes =
                correct_estimated_fees(&ledger, &brokers(vec![actual]), &mut HashSet::new(), 6)
                    .await
                    .unwrap();
            assert_eq!(fixes.len(), 1);
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(replayed.orders["f-buy-0"].fee_usdt, dec!(0.0172));
            assert_eq!(
                replayed.positions["f"].realized_fee_usdt,
                dec!(0.3),
                "按成交记录核算的不动"
            );
            tokio::fs::remove_file(&path).await.unwrap();
        }

        /// 不是 Hyperliquid 系、又没有标记的，手续费碰巧等于费率 × 成交额也不核；真实收了别的费率的（STRC 0.0864%）也不核。
        #[test]
        fn only_flagged_or_hyperliquid_taker_rate_fees_are_checked() {
            let hl = order("f-buy-0", Side::Buy, dec!(200), dec!(0.18), false);
            assert!(fee_needs_check(&hl, Some(dec!(0.0009))));
            let mut strc = hl.clone();
            strc.fee_usdt = dec!(0.1728);
            assert!(!fee_needs_check(&strc, Some(dec!(0.0009))));
            let mut lighter = hl.clone();
            lighter.order.venue = Venue::Lighter;
            assert!(!fee_needs_check(&lighter, Some(dec!(0.0009))));
            lighter.fee_estimated = true;
            assert!(fee_needs_check(&lighter, Some(dec!(0.0009))));
            let mut live = hl.clone();
            live.status = crate::types::OrderStatus::Pending;
            assert!(!fee_needs_check(&live, Some(dec!(0.0009))));
        }
    }

    mod settle {
        use super::super::*;
        use crate::settlement::{FillEffect, VenueFill};
        use crate::types::{ClientOrderId, OrderAck, OrderState};
        use arb_core::{ArbResult, Side};
        use async_trait::async_trait;
        use chrono::TimeZone;
        use rust_decimal_macros::dec;

        fn at(second: u32) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc
                .with_ymd_and_hms(2026, 9, 30, 6, 22, second)
                .unwrap()
        }

        fn fill(
            sec: u32,
            side: Side,
            qty: Decimal,
            price: Decimal,
            fee: Decimal,
            effect: FillEffect,
        ) -> VenueFill {
            VenueFill {
                at: at(sec),
                side,
                quantity: qty,
                price,
                fee_usdt: fee,
                effect,
            }
        }

        /// 交易所给什么就回什么的桩：成交记录（`None` = 没接入）与资金费。
        struct Venue_ {
            venue: Venue,
            fills: Option<Vec<VenueFill>>,
            /// 资金费流水（时刻, 金额）。旧用例只给一个总数：放在开仓后第 30 秒一笔。
            funding: Vec<(chrono::DateTime<chrono::Utc>, Decimal)>,
            /// 资金费流水查询失败。
            funding_down: bool,
        }

        fn flow(total: Decimal) -> Vec<(chrono::DateTime<chrono::Utc>, Decimal)> {
            vec![(at(30), total)]
        }

        #[async_trait]
        impl Broker for Venue_ {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("核算盈亏不该下单");
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("核算盈亏不该撤单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn funding_since(
                &self,
                _: &Symbol,
                since: chrono::DateTime<chrono::Utc>,
            ) -> ArbResult<Option<crate::broker::FundingTotal>> {
                if self.funding_down {
                    return Err(arb_core::ArbError::venue(
                        self.venue.as_str(),
                        "流水查询超时",
                    ));
                }
                Ok(Some(crate::broker::FundingTotal::from_rows(
                    self.funding.clone(),
                    since,
                )))
            }
            async fn fills_between(
                &self,
                _: &Symbol,
                _: chrono::DateTime<chrono::Utc>,
                _: chrono::DateTime<chrono::Utc>,
            ) -> ArbResult<Option<Vec<VenueFill>>> {
                Ok(self.fills.clone())
            }
        }

        fn leg(venue: Venue, side: Side, price: Decimal, fee: Decimal) -> crate::LegFill {
            crate::LegFill {
                venue,
                side,
                notional_usdt: price * dec!(1797.9),
                average_price: price,
                fee_usdt: fee,
                client_order_id: ClientOrderId::for_leg("pons", side, 0),
                margin_usdt: None,
            }
        }

        /// PONS：多 lighter-rh 0.55507（免费）、空 arcus 0.5547（吃单费 0.224391404）。
        fn open_position() -> PairPosition {
            PairPosition {
                margin_mode: crate::MarginMode::Isolated,
                id: "pons".into(),
                symbol: Symbol::perp("PONS", "USDT"),
                strategy: Strategy::Funding,
                long: Some(leg(
                    Venue::LighterRh,
                    Side::Buy,
                    dec!(0.55507),
                    Decimal::ZERO,
                )),
                short: Some(leg(
                    Venue::Arcus,
                    Side::Sell,
                    dec!(0.5547),
                    dec!(0.224391404),
                )),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status: PositionStatus::Open,
                opened_at: at(0),
                closed_at: None,
                note: None,
                leverage: Some(dec!(3)),
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
            }
        }

        /// 外部平仓后被识别结束的记录：两腿已清掉，只有 `closed_externally`。
        fn ended(entry_legs: bool) -> PairPosition {
            let open = open_position();
            let mut ended = open.clone();
            if entry_legs {
                ended.entry_legs = open
                    .long
                    .clone()
                    .zip(open.short.clone())
                    .map(|(long, short)| crate::EntryLegs { long, short });
            }
            ended.long = None;
            ended.short = None;
            ended.status = PositionStatus::Closed;
            ended.closed_at = Some(at(59));
            ended.closed_externally = true;
            ended.note = Some("在交易所外部平仓".into());
            ended
        }

        fn brokers(
            long_fills: Option<Vec<VenueFill>>,
            short_fills: Option<Vec<VenueFill>>,
        ) -> HashMap<Venue, Arc<dyn Broker>> {
            HashMap::from([
                (
                    Venue::LighterRh,
                    Arc::new(Venue_ {
                        venue: Venue::LighterRh,
                        fills: long_fills,
                        funding: flow(dec!(-0.5410)),
                        funding_down: false,
                    }) as Arc<dyn Broker>,
                ),
                (
                    Venue::Arcus,
                    Arc::new(Venue_ {
                        venue: Venue::Arcus,
                        fills: short_fills,
                        funding: flow(dec!(0.7096)),
                        funding_down: false,
                    }) as Arc<dyn Broker>,
                ),
            ])
        }

        fn matching_fills() -> (Vec<VenueFill>, Vec<VenueFill>) {
            (
                vec![
                    fill(
                        2,
                        Side::Buy,
                        dec!(1797.9),
                        dec!(0.55507),
                        dec!(0),
                        FillEffect::Unknown,
                    ),
                    fill(
                        52,
                        Side::Sell,
                        dec!(1797.9),
                        dec!(0.5645),
                        dec!(0),
                        FillEffect::Unknown,
                    ),
                ],
                vec![
                    fill(
                        2,
                        Side::Sell,
                        dec!(1797.9),
                        dec!(0.5547),
                        dec!(0.224391404),
                        FillEffect::Open,
                    ),
                    fill(
                        50,
                        Side::Buy,
                        dec!(35.4258),
                        dec!(0.56456),
                        dec!(0),
                        FillEffect::Close,
                    ),
                    fill(
                        51,
                        Side::Buy,
                        dec!(1762.4742),
                        dec!(0.56456),
                        dec!(0),
                        FillEffect::Close,
                    ),
                ],
            )
        }

        async fn ledger(name: &str) -> (Ledger, std::path::PathBuf) {
            let path = std::env::temp_dir()
                .join(format!("arb-settle-{name}-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            (Ledger::open(&path).await.unwrap(), path)
        }

        #[tokio::test]
        async fn an_external_close_is_settled_from_the_venues_fills_and_written_back() {
            let (ledger, path) = ledger("ok").await;
            let (long, short) = matching_fills();
            let brokers = brokers(Some(long), Some(short));
            let ended = ended(true);
            ledger
                .append(&crate::Record::Position(Box::new(ended.clone())))
                .await
                .unwrap();
            let SettleOutcome::Settled(settled) =
                settle_closed_from_fills(&ledger, &brokers, &ended)
                    .await
                    .unwrap()
            else {
                panic!("应当核出来");
            };
            // 价格：多腿 +16.954197、空腿 −17.727294；手续费只有空腿开仓那笔；资金费 −0.5410 + 0.7096。
            assert_eq!(settled.realized_pnl_usdt, dec!(-0.773097));
            assert_eq!(settled.realized_fee_usdt, dec!(0.224391404));
            assert_eq!(settled.realized_funding_usdt, Some(dec!(0.1686)));
            assert_eq!(
                settled.realized_source,
                Some(crate::RealizedSource::VenueFills)
            );
            assert!(
                settled.note.as_ref().unwrap().contains("实际盈亏 -0.8289"),
                "{:?}",
                settled.note
            );
            // 台账回放取到最新一条：已核算、仍是已平仓、没有敞口。
            let (replayed, _) = ledger.replay().await.unwrap();
            let stored = &replayed.positions["pons"];
            assert_eq!(
                stored.realized_source,
                Some(crate::RealizedSource::VenueFills)
            );
            assert_eq!(stored.status, PositionStatus::Closed);
            assert!(replayed.exposed().is_empty());
            let _ = tokio::fs::remove_file(&path).await;
        }

        #[tokio::test]
        async fn the_entry_legs_are_recovered_from_the_ledger_history_when_they_were_not_stored() {
            // 更早识别的外部平仓（没存开仓记录）：台账历史里还有开仓时的那条记录。
            let (ledger, path) = ledger("history").await;
            ledger
                .append(&crate::Record::Position(Box::new(open_position())))
                .await
                .unwrap();
            let ended = ended(false);
            ledger
                .append(&crate::Record::Position(Box::new(ended.clone())))
                .await
                .unwrap();
            let (long, short) = matching_fills();
            let brokers = brokers(Some(long), Some(short));
            let outcome = settle_closed_from_fills(&ledger, &brokers, &ended)
                .await
                .unwrap();
            assert!(matches!(outcome, SettleOutcome::Settled(_)), "{outcome:?}");
            let _ = tokio::fs::remove_file(&path).await;
        }

        #[tokio::test]
        async fn fills_that_do_not_balance_are_not_attributed_and_nothing_is_written() {
            let (ledger, path) = ledger("mismatch").await;
            let (long, mut short) = matching_fills();
            // 交易所记录里只有一部分平仓（另一部分还没出现）。
            short.pop();
            let brokers = brokers(Some(long), Some(short));
            let ended = ended(true);
            ledger
                .append(&crate::Record::Position(Box::new(ended.clone())))
                .await
                .unwrap();
            let before = tokio::fs::read_to_string(&path).await.unwrap();
            match settle_closed_from_fills(&ledger, &brokers, &ended)
                .await
                .unwrap()
            {
                SettleOutcome::Retry(reason) => assert!(reason.contains("平仓数量"), "{reason}"),
                other => panic!("应当稍后重试，实际 {other:?}"),
            }
            assert_eq!(
                tokio::fs::read_to_string(&path).await.unwrap(),
                before,
                "没核出来就不动台账"
            );
            let _ = tokio::fs::remove_file(&path).await;
        }

        #[tokio::test]
        async fn the_daily_total_counts_todays_closes_and_refuses_to_guess_unknowns() {
            let (ledger, path) = ledger("daily").await;
            let day = chrono::NaiveDate::from_ymd_opt(2026, 9, 30).unwrap();
            let closed = |id: &str,
                          closed_at,
                          source: Option<crate::RealizedSource>,
                          pnl: Decimal,
                          fee: Decimal,
                          funding: Option<Decimal>| {
                let mut position = ended(true);
                position.id = id.into();
                position.closed_at = Some(closed_at);
                position.realized_source = source;
                position.realized_pnl_usdt = pnl;
                position.realized_fee_usdt = fee;
                position.realized_funding_usdt = funding;
                position
            };
            let today = at(30);
            let yesterday = today - chrono::Duration::days(1);
            let src = Some(crate::RealizedSource::Executor);
            for position in [
                closed("a", today, src, dec!(3.47), dec!(0.22), Some(dec!(0.17))),
                closed("b", today, src, dec!(-1.29), dec!(0.45), None),
                // 昨天的不算。
                closed("old", yesterday, src, dec!(-999), dec!(1), None),
            ] {
                ledger
                    .append(&crate::Record::Position(Box::new(position)))
                    .await
                    .unwrap();
            }
            let (replayed, _) = ledger.replay().await.unwrap();
            let daily = daily_realized(&replayed, day);
            assert_eq!(daily.closed, 2);
            // (3.47 − 0.22 + 0.17) + (−1.29 − 0.45) = 1.68
            assert_eq!(daily.net_usdt, Some(dec!(1.68)));
            assert_eq!(daily.funding_missing, 1);
            // 今天结束的仓位里有一笔没有盈亏记录：合计算不出来，不能当成 0。
            ledger
                .append(&crate::Record::Position(Box::new(closed(
                    "c",
                    today,
                    None,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    None,
                ))))
                .await
                .unwrap();
            let (replayed, _) = ledger.replay().await.unwrap();
            let daily = daily_realized(&replayed, day);
            assert_eq!(daily.net_usdt, None);
            assert_eq!(daily.unknown, vec!["c".to_string()]);
            let _ = tokio::fs::remove_file(&path).await;
        }

        #[tokio::test]
        async fn an_unwind_without_any_fill_counts_as_zero_not_unknown() {
            // 某次实盘：第一腿被拒（canceled-margin-not-allowed），旧版本没记盈亏来源，
            // 当日合计变成「未知」，自动交易的补仓被拦。
            let (ledger, path) = ledger("unwound-unfilled").await;
            let day = chrono::NaiveDate::from_ymd_opt(2026, 9, 30).unwrap();
            let mut rejected = ended(true);
            rejected.id = "zec".into();
            rejected.status = PositionStatus::Unwound;
            rejected.long = None;
            rejected.short = None;
            rejected.closed_at = Some(at(30));
            rejected.realized_source = None;
            rejected.realized_pnl_usdt = Decimal::ZERO;
            rejected.realized_fee_usdt = Decimal::ZERO;
            ledger
                .append(&crate::Record::Position(Box::new(rejected.clone())))
                .await
                .unwrap();
            let (replayed, _) = ledger.replay().await.unwrap();
            let daily = daily_realized(&replayed, day);
            assert_eq!((daily.closed, daily.net_usdt), (1, Some(Decimal::ZERO)));
            assert!(daily.unknown.is_empty());
            // 但它的订单要是有过成交，就不能当 0：照旧算不出合计。
            let mut order = crate::OrderState::new(crate::NewOrder {
                post_only: false,
                client_order_id: crate::ClientOrderId("zec-sell-0".into()),
                venue: Venue::LighterRh,
                symbol: rejected.symbol.clone(),
                side: Side::Sell,
                notional_usdt: dec!(800),
                quantity: None,
                limit_price: None,
                reduce_only: false,
                leverage: None,
                margin_mode: rejected.margin_mode,
            });
            order.filled_usdt = dec!(10);
            ledger
                .append(&crate::Record::Order(Box::new(order)))
                .await
                .unwrap();
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(daily_realized(&replayed, day).net_usdt, None);
            // 没有任何平仓的一天：确定是 0。
            let empty = daily_realized(
                &replayed,
                chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            );
            assert_eq!((empty.closed, empty.net_usdt), (0, Some(Decimal::ZERO)));
            let _ = tokio::fs::remove_file(&path).await;
        }

        #[test]
        fn collateral_is_judged_with_a_buffer_and_unknown_never_blocks() {
            // 1000 名义、3 倍：333.33 × 1.1 = 366.67
            let required = required_margin(dec!(1000), dec!(3));
            assert_eq!(required.round_dp(2), dec!(366.67));
            assert_eq!(
                judge_collateral(Some(dec!(400)), required),
                CollateralVerdict::Enough
            );
            assert!(matches!(
                judge_collateral(Some(dec!(360)), required),
                CollateralVerdict::Short { .. }
            ));
            assert_eq!(judge_collateral(None, required), CollateralVerdict::Unknown);
            // 杠杆小于 1 按 1 算，不会把要占的保证金放大。
            assert_eq!(required_margin(dec!(100), dec!(0.5)), dec!(110.0));
        }

        /// 只回可用保证金的桩：下单 / 撤单就 panic，证明核对是只读的。
        struct Margin {
            venue: Venue,
            free: Option<Decimal>,
        }

        #[async_trait]
        impl Broker for Margin {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("核对保证金不该下单");
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("核对保证金不该撤单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
                Ok(self.free)
            }
        }

        #[tokio::test]
        async fn an_account_that_cannot_carry_the_margin_blocks_the_open_and_unknown_only_warns() {
            let brokers = |long_free, short_free| -> HashMap<Venue, Arc<dyn Broker>> {
                HashMap::from([
                    (
                        Venue::LighterRh,
                        Arc::new(Margin {
                            venue: Venue::LighterRh,
                            free: long_free,
                        }) as Arc<dyn Broker>,
                    ),
                    (
                        Venue::Arcus,
                        Arc::new(Margin {
                            venue: Venue::Arcus,
                            free: short_free,
                        }) as Arc<dyn Broker>,
                    ),
                ])
            };
            let legs = [(Venue::LighterRh, dec!(1000)), (Venue::Arcus, dec!(1000))];
            // 两边都够：无警告。
            let ok =
                check_legs_collateral(&brokers(Some(dec!(500)), Some(dec!(500))), &legs, dec!(3))
                    .await;
            assert!(ok.unwrap().is_empty());
            // 一边不够：拒绝，说清楚是哪家、差多少。
            let error =
                check_legs_collateral(&brokers(Some(dec!(500)), Some(dec!(100))), &legs, dec!(3))
                    .await
                    .unwrap_err()
                    .to_string();
            assert!(
                error.contains("arcus 账户可用保证金 100") && error.contains("不够"),
                "{error}"
            );
            // 一边查不到：只警告，不拦。
            let warned = check_legs_collateral(&brokers(None, Some(dec!(500))), &legs, dec!(3))
                .await
                .unwrap();
            assert_eq!(warned.len(), 1);
            assert!(
                warned[0].contains("lighter-rh") && warned[0].contains("没核对"),
                "{warned:?}"
            );
        }

        /// 查一次可用保证金要 200ms 的桩。
        struct SlowMargin(Venue);

        #[async_trait]
        impl Broker for SlowMargin {
            fn venue(&self) -> Venue {
                self.0
            }
            async fn place(&self, _: &crate::types::NewOrder) -> ArbResult<OrderAck> {
                panic!("核对保证金不该下单");
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                panic!("核对保证金不该撤单");
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                Ok(Some(dec!(10_000)))
            }
        }

        #[tokio::test]
        async fn the_two_accounts_are_checked_at_the_same_time() {
            let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
                (
                    Venue::LighterRh,
                    Arc::new(SlowMargin(Venue::LighterRh)) as Arc<dyn Broker>,
                ),
                (
                    Venue::Arcus,
                    Arc::new(SlowMargin(Venue::Arcus)) as Arc<dyn Broker>,
                ),
            ]);
            let legs = [(Venue::LighterRh, dec!(1000)), (Venue::Arcus, dec!(1000))];
            let started = std::time::Instant::now();
            let warnings = check_legs_collateral(&brokers, &legs, dec!(3))
                .await
                .unwrap();
            assert!(warnings.is_empty());
            // 串行要 400ms；并发约 200ms。留足余量避免机器慢时误报。
            assert!(
                started.elapsed() < std::time::Duration::from_millis(380),
                "两个账户应并发核对，用了 {:?}",
                started.elapsed()
            );
        }

        #[test]
        fn a_legacy_external_close_is_recognised_by_its_note() {
            // 更早版本识别出来的记录没有 closed_externally 字段。
            let mut legacy = ended(false);
            legacy.closed_externally = false;
            legacy.note =
                Some("在交易所外部平仓：2026-09-30 13:26 UTC 对账发现两腿都已没有仓位".into());
            assert!(legacy.is_external_close());
            legacy.note = Some("按退出基差 0.1% 平仓".into());
            assert!(!legacy.is_external_close());
        }

        #[tokio::test]
        async fn a_venue_without_fill_history_can_never_be_settled() {
            let (ledger, path) = ledger("none").await;
            let (long, _) = matching_fills();
            let brokers = brokers(Some(long), None);
            let ended = ended(true);
            match settle_closed_from_fills(&ledger, &brokers, &ended)
                .await
                .unwrap()
            {
                SettleOutcome::Impossible(reason) => {
                    assert!(reason.contains("还没接入成交记录"), "{reason}")
                }
                other => panic!("应当放弃，实际 {other:?}"),
            }
            let _ = tokio::fs::remove_file(&path).await;
        }

        /// 资金费流水：开仓前、窗口内、平仓后（下一笔同合约仓位）各有结算。
        fn windowed(venue: Venue, rows: &[(u32, Decimal)], down: bool) -> (Venue, Arc<dyn Broker>) {
            (
                venue,
                Arc::new(Venue_ {
                    venue,
                    fills: None,
                    funding: rows
                        .iter()
                        .map(|(sec, amount)| (at(*sec), *amount))
                        .collect(),
                    funding_down: down,
                }) as Arc<dyn Broker>,
            )
        }

        #[tokio::test]
        async fn funding_between_counts_only_the_window_including_its_last_instant() {
            let (_, broker) = windowed(
                Venue::Arcus,
                &[(0, dec!(100)), (10, dec!(1)), (20, dec!(2)), (21, dec!(50))],
                false,
            );
            let symbol = Symbol::perp("PONS", "USDT");
            let total = broker
                .funding_between(&symbol, at(5), at(20))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (total.usdt, total.payments),
                (dec!(3), 2),
                "开仓前与平仓后的都不算，恰好在平仓那一刻的算"
            );
            assert_eq!(
                total.last_at, None,
                "窗口之后还有结算时，窗口内最后一笔的时刻未知"
            );
            let tail = broker
                .funding_between(&symbol, at(15), at(59))
                .await
                .unwrap()
                .unwrap();
            assert_eq!((tail.usdt, tail.last_at), (dec!(52), Some(at(21))));
        }

        /// 2026-10-02 PONS：平仓重试时台账里只剩 arcus 一条腿，lighter-rh 已经平掉。资金费必须仍按两条腿算，
        /// 而且不能把之后同合约仓位的结算算进来。
        #[tokio::test]
        async fn funding_uses_both_entry_legs_and_stops_at_the_close() {
            let (ledger, path) = ledger("funding-legs").await;
            let open = open_position();
            ledger
                .append(&crate::Record::Position(Box::new(open.clone())))
                .await
                .unwrap();
            let mut closing = open.clone();
            closing.long = None; // lighter-rh 已平，arcus 还在重试
            closing.status = PositionStatus::Closing;
            ledger
                .append(&crate::Record::Position(Box::new(closing.clone())))
                .await
                .unwrap();
            let brokers = HashMap::from([
                windowed(
                    Venue::LighterRh,
                    &[(30, dec!(-2.829355)), (58, dec!(-9))],
                    false,
                ),
                windowed(Venue::Arcus, &[(30, dec!(6.622370)), (58, dec!(9))], false),
            ]);
            let venues = crate::executor::position_venues(&ledger, &closing)
                .await
                .unwrap();
            assert_eq!(venues.len(), 2, "已平掉的腿也要从历史里找回来：{venues:?}");
            let funding = crate::executor::funding_window(
                &brokers,
                &venues,
                &closing.symbol,
                closing.opened_at,
                at(50),
            )
            .await;
            assert_eq!(funding, Some(dec!(3.793015)));
            // 只有一家的流水：不知道就是不知道，不拿一条腿冒充合计。
            let one = crate::executor::funding_window(
                &brokers,
                &[Venue::Arcus],
                &closing.symbol,
                at(0),
                at(50),
            )
            .await;
            assert_eq!(one, None);
            // 外部平仓识别：两条腿、截止到识别时刻之前的结算都算上（这里窗口到「现在」之前的 58 秒那笔也在内，
            // 识别时还不知道真正的平仓时刻；事后核对按平仓时刻截）。
            let adopted =
                adopt_external_closes(&ledger, &brokers, &[&closing], &[closing.id.clone()])
                    .await
                    .unwrap();
            assert!(
                adopted[0].realized_funding_usdt.is_some(),
                "两家都查得到就不该是未知"
            );
            let _ = tokio::fs::remove_file(&path).await;
        }

        /// 2026-09-30 LIT：平仓那一刻 arcus 已经出了最后一笔、lighter-rh 还没出。事后按同一窗口重查，
        /// 不一致就追加更正（只追加、不改旧行），一致就只记核对时刻；查不到就什么都不写。
        #[tokio::test]
        async fn a_closed_position_is_rechecked_once_and_corrected_append_only() {
            let (ledger, path) = ledger("funding-recheck").await;
            let mut closed = open_position();
            closed.status = PositionStatus::Closed;
            closed.closed_at = Some(at(40));
            closed.realized_source = Some(crate::RealizedSource::Executor);
            closed.realized_pnl_usdt = dec!(0.915308);
            closed.realized_fee_usdt = dec!(1.352440077);
            closed.realized_funding_usdt = Some(dec!(0.586956679));
            closed.note = Some("已实现盈亏 0.1498 USDT".into());
            ledger
                .append(&crate::Record::Position(Box::new(open_position())))
                .await
                .unwrap();
            ledger
                .append(&crate::Record::Position(Box::new(closed.clone())))
                .await
                .unwrap();
            let brokers = HashMap::from([
                windowed(
                    Venue::LighterRh,
                    &[(10, dec!(-1.078742)), (39, dec!(-0.207642))],
                    false,
                ),
                windowed(
                    Venue::Arcus,
                    &[(10, dec!(1.547093388)), (39, dec!(0.118605612))],
                    false,
                ),
            ]);
            let soon = at(40) + chrono::Duration::minutes(1);
            assert_eq!(
                recheck_funding(&ledger, &brokers, &closed, soon, false)
                    .await
                    .unwrap(),
                FundingCheck::NotDue,
                "平仓后要等一会儿，两家的流水才都出来"
            );
            let later = at(40) + FUNDING_RECHECK_AFTER;
            let outcome = recheck_funding(&ledger, &brokers, &closed, later, false)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                FundingCheck::Corrected(Some(dec!(0.586956679)), dec!(0.379315))
            );
            let lines = tokio::fs::read_to_string(&path).await.unwrap();
            assert_eq!(lines.lines().count(), 3, "只追加一行，旧行原样保留");
            assert!(lines.contains("0.586956679"));
            let (replayed, _) = ledger.replay().await.unwrap();
            let fixed = &replayed.positions["pons"];
            assert_eq!(fixed.realized_funding_usdt, Some(dec!(0.379315)));
            assert_eq!(fixed.funding_checked_at, Some(later));
            assert_eq!(
                (fixed.realized_pnl_usdt, fixed.realized_fee_usdt),
                (closed.realized_pnl_usdt, closed.realized_fee_usdt)
            );
            let note = fixed.note.as_deref().unwrap();
            assert!(
                note.starts_with("已实现盈亏 0.1498 USDT；资金费已按交易所流水更正"),
                "{note}"
            );
            assert!(
                note.contains("0.587 → 0.3793") && note.contains("0.1498 → -0.0578"),
                "{note}"
            );
            // 核过一次就不再核（除非 force）；force 时一致只记时刻。
            assert_eq!(
                recheck_funding(&ledger, &brokers, fixed, later, false)
                    .await
                    .unwrap(),
                FundingCheck::NotDue
            );
            assert_eq!(
                recheck_funding(&ledger, &brokers, fixed, later, true)
                    .await
                    .unwrap(),
                FundingCheck::Confirmed
            );
            // 流水查不到：不写任何东西，下次再试。
            let down = HashMap::from([
                windowed(Venue::LighterRh, &[], true),
                windowed(Venue::Arcus, &[(10, dec!(1))], false),
            ]);
            let before = tokio::fs::read_to_string(&path)
                .await
                .unwrap()
                .lines()
                .count();
            assert_eq!(
                recheck_funding(&ledger, &down, &closed, later, true)
                    .await
                    .unwrap(),
                FundingCheck::Unavailable
            );
            assert_eq!(
                tokio::fs::read_to_string(&path)
                    .await
                    .unwrap()
                    .lines()
                    .count(),
                before
            );
            // 回滚掉的、还没核算出价格盈亏的外部平仓：不核。
            let mut unattributed = closed.clone();
            unattributed.realized_source = None;
            assert_eq!(
                recheck_funding(&ledger, &brokers, &unattributed, later, true)
                    .await
                    .unwrap(),
                FundingCheck::NotDue
            );
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    #[test]
    fn a_missing_opportunity_names_what_blocked_it() {
        use arb_scanner::scan::{ExcludedReading, RejectedPair, Totals};
        let anthropic = Symbol::perp("ANTHROPIC", "USDT");
        let report = ScanReport {
            generated_at: chrono::Utc::now(),
            fee_per_side: Decimal::new(5, 4),
            amortize_days: Decimal::from(7),
            spread_hold_days: Decimal::from(3),
            max_entry_basis_pct: Some(Decimal::new(5, 1)),
            min_venues: 2,
            venues: Vec::new(),
            symbols: Vec::new(),
            unverified: Vec::new(),
            suspicious: vec![ExcludedReading {
                venue: Venue::Arcus,
                symbol: Symbol::perp("CASHCAT", "USDT"),
                reason: "与同币种其它场所差异过大".into(),
            }],
            // 2026-09-29 看板上的真实一行：lighter-rh 2115.9、hyperliquid-io 2081.1。
            gated: vec![RejectedPair {
                symbol: anthropic,
                long: Venue::LighterRh,
                short: Venue::HyperliquidIo,
                reason: "入场基差 -1.658% 低于门槛 −0.5%".into(),
            }],
            totals: Totals::default(),
        };
        let gated = missing_opportunity(
            &report,
            "funding",
            "ANTHROPIC",
            Some("USDT"),
            Venue::LighterRh,
            Venue::HyperliquidIo,
        );
        assert!(
            gated.contains("入场基差门槛") && gated.contains("-1.658%"),
            "{gated}"
        );
        assert!(gated.contains("ARB_MAX_ENTRY_BASIS_PCT"), "{gated}");
        // 方向反过来就不是这条门槛记录。
        let reversed = missing_opportunity(
            &report,
            "funding",
            "ANTHROPIC",
            Some("USDT"),
            Venue::HyperliquidIo,
            Venue::LighterRh,
        );
        assert!(reversed.contains("费差方向不成立"), "{reversed}");
        let suspicious = missing_opportunity(
            &report,
            "funding",
            "CASHCAT",
            Some("USDT"),
            Venue::Arcus,
            Venue::Lighter,
        );
        assert!(
            suspicious.contains("可信度筛查") && suspicious.contains("arcus"),
            "{suspicious}"
        );
    }

    #[test]
    fn position_ids_keep_the_cli_format_and_never_repeat() {
        let ids: Vec<String> = (0..50).map(|_| new_position_id("live")).collect();
        let unique: BTreeSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "同一毫秒里连开也不能撞号");
        for id in &ids {
            let millis = id.strip_prefix("live-").expect("前缀与命令行一致");
            assert!(millis.parse::<i64>().is_ok(), "{id}");
        }
    }

    mod recovery {
        use super::super::*;
        use crate::types::{ClientOrderId, LegFill, NewOrder, OrderState, OrderStatus};
        use arb_core::{ArbError, ArbResult, Side};
        use async_trait::async_trait;
        use rust_decimal_macros::dec;
        use std::time::{Duration, Instant};

        /// 按订单号返回预设答案的桩券商；`fills` 为真时下单一律按 100 全部成交。
        struct Scripted {
            venue: Venue,
            answers: HashMap<String, Result<Option<OrderState>, String>>,
            fills: bool,
            placed: tokio::sync::Mutex<HashMap<String, OrderState>>,
        }

        impl Scripted {
            fn new(venue: Venue) -> Self {
                Self {
                    venue,
                    answers: HashMap::new(),
                    fills: false,
                    placed: Default::default(),
                }
            }
        }

        #[async_trait]
        impl Broker for Scripted {
            fn venue(&self) -> Venue {
                self.venue
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
                if !self.fills {
                    return Err(ArbError::config("这个桩券商不下单"));
                }
                let mut state = OrderState::new(order.clone());
                state.venue_order_id = Some(order.client_order_id.0.clone());
                state.status = OrderStatus::Filled;
                state.average_price = Some(dec!(100));
                state.filled_usdt = order.quantity.unwrap_or_default() * dec!(100);
                self.placed
                    .lock()
                    .await
                    .insert(order.client_order_id.0.clone(), state);
                Ok(crate::types::OrderAck {
                    client_order_id: order.client_order_id.clone(),
                    venue_order_id: order.client_order_id.0.clone(),
                    status: OrderStatus::Filled,
                })
            }
            async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                if let Some(state) = self.placed.lock().await.get(&id.0) {
                    return Ok(Some(state.clone()));
                }
                match self.answers.get(&id.0) {
                    Some(Ok(state)) => Ok(state.clone()),
                    Some(Err(why)) => Err(ArbError::venue(self.venue.as_str(), why.clone())),
                    None => Ok(None),
                }
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                Ok(())
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
        }

        /// 查持仓一律报错的桩券商（看对账有没有去碰它）。
        struct Down(Venue);

        #[async_trait]
        impl Broker for Down {
            fn venue(&self) -> Venue {
                self.0
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn place(&self, _: &NewOrder) -> ArbResult<crate::types::OrderAck> {
                Err(ArbError::config("不下单"))
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                Ok(())
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Err(ArbError::venue(self.0.as_str(), "挂单查不了"))
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Err(ArbError::venue(self.0.as_str(), "持仓查不了"))
            }
        }

        /// 开仓前只核这笔两条腿的场所：别的场所查不了也不碍事（完整对账照旧由规则轮做）。
        #[tokio::test]
        async fn a_leg_only_reconcile_leaves_the_other_venues_alone() {
            let path =
                std::env::temp_dir().join(format!("arb-legs-only-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            let ledger = Ledger::open(&path).await.unwrap();
            let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Scripted::new(Venue::Lighter)) as Arc<dyn Broker>,
                ),
                (
                    Venue::HyperliquidXyz,
                    Arc::new(Scripted::new(Venue::HyperliquidXyz)) as Arc<dyn Broker>,
                ),
                (
                    Venue::Bitget,
                    Arc::new(Down(Venue::Bitget)) as Arc<dyn Broker>,
                ),
            ]);
            let full = reconcile_ledger(&ledger, &brokers).await.unwrap();
            assert!(
                full.divergences
                    .iter()
                    .any(|d| d.venue == Some(Venue::Bitget)),
                "完整对账会碰到 Bitget"
            );
            let legs = reconcile_ledger_on(
                &ledger,
                &brokers,
                Some(&[Venue::Lighter, Venue::HyperliquidXyz]),
            )
            .await
            .unwrap();
            assert!(legs.divergences.is_empty(), "{:?}", legs.divergences);
            assert_eq!(legs.checked_venues, 2);
            let _ = tokio::fs::remove_file(&path).await;
        }

        fn pending(id: &str, venue: Venue, side: Side, reduce_only: bool) -> OrderState {
            OrderState::new(NewOrder {
                post_only: false,
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: ClientOrderId(id.into()),
                venue,
                symbol: Symbol::perp("BTC", "USDT"),
                side,
                notional_usdt: dec!(1000),
                quantity: Some(dec!(10)),
                limit_price: None,
                reduce_only,
                leverage: None,
            })
        }

        async fn temp_ledger(name: &str) -> Arc<Ledger> {
            let path =
                std::env::temp_dir().join(format!("arb-desk-{name}-{}.jsonl", std::process::id()));
            let _ = tokio::fs::remove_file(&path).await;
            Arc::new(Ledger::open(&path).await.unwrap())
        }

        /// 进程死在「意图已落盘、终态未落盘」之间：台账里的 Pending 要按券商的权威记录补成终态，
        /// 否则对账把它们当成永远消不掉的「本地有挂单、交易所没有」，所有开仓与规则从此被拒。
        #[tokio::test]
        async fn orders_stuck_pending_after_a_crash_are_resolved_from_the_brokers_records() {
            let ledger = temp_ledger("pending").await;
            for (id, side) in [
                ("a-buy-0", Side::Buy),
                ("b-buy-0", Side::Buy),
                ("c-buy-0", Side::Buy),
                ("d-buy-0", Side::Buy),
            ] {
                ledger
                    .append(&crate::ledger::Record::Order(Box::new(pending(
                        id,
                        Venue::Binance,
                        side,
                        false,
                    ))))
                    .await
                    .unwrap();
            }
            let mut broker = Scripted::new(Venue::Binance);
            // a：券商说已成交；b：券商从没收到（None）；c：查询出错；d：还挂在簿上。
            let mut filled = pending("a-buy-0", Venue::Binance, Side::Buy, false);
            filled.status = OrderStatus::Filled;
            filled.filled_usdt = dec!(1000);
            filled.average_price = Some(dec!(100));
            broker.answers.insert("a-buy-0".into(), Ok(Some(filled)));
            broker.answers.insert("b-buy-0".into(), Ok(None));
            broker
                .answers
                .insert("c-buy-0".into(), Err("超时".to_string()));
            let mut resting = pending("d-buy-0", Venue::Binance, Side::Buy, false);
            resting.status = OrderStatus::Open;
            broker.answers.insert("d-buy-0".into(), Ok(Some(resting)));
            let brokers: HashMap<Venue, Arc<dyn Broker>> =
                HashMap::from([(Venue::Binance, Arc::new(broker) as Arc<dyn Broker>)]);

            let resolution = resolve_pending_orders(&ledger, &brokers).await.unwrap();

            assert_eq!(resolution.resolved, vec!["a-buy-0", "b-buy-0"]);
            assert_eq!(resolution.filled_unrecorded, vec!["a-buy-0"]);
            assert_eq!(resolution.still_open, vec!["d-buy-0"]);
            assert_eq!(resolution.unknown.len(), 1);
            assert_eq!(resolution.unknown[0].0, "c-buy-0");
            let (replayed, broken) = ledger.replay().await.unwrap();
            assert_eq!(broken, 0);
            assert_eq!(replayed.orders["a-buy-0"].status, OrderStatus::Filled);
            assert_eq!(replayed.orders["b-buy-0"].status, OrderStatus::Rejected);
            assert!(
                replayed.orders["b-buy-0"]
                    .reject_reason
                    .as_deref()
                    .unwrap()
                    .contains("没有收到")
            );
            // 没结论的不编终态：c 仍是 Pending，d 仍在场上 —— 只有它们还留给对账。
            let leftover = local_open_orders(&replayed);
            let mut ids = leftover[&Venue::Binance].clone();
            ids.sort();
            assert_eq!(ids, vec!["c-buy-0", "d-buy-0"]);
        }

        /// 只挂单的订单：挂在簿上、被读到之前没有人撤；撤单之后状态变成 `Cancelled`，可能带着已成交的一部分。
        struct Orphan {
            state: tokio::sync::Mutex<OrderState>,
            cancel_works: bool,
            filled_before_cancel: Decimal,
        }

        #[async_trait]
        impl Broker for Orphan {
            fn venue(&self) -> Venue {
                Venue::Binance
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn place(&self, _: &NewOrder) -> ArbResult<crate::types::OrderAck> {
                unreachable!("不下单")
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(Some(self.state.lock().await.clone()))
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                if !self.cancel_works {
                    return Err(ArbError::venue("binance", "撤单超时"));
                }
                let mut state = self.state.lock().await;
                state.status = OrderStatus::Cancelled;
                state.filled_usdt = self.filled_before_cancel;
                if self.filled_before_cancel > Decimal::ZERO {
                    state.average_price = Some(dec!(100));
                }
                Ok(())
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
        }

        /// 进程死在只挂单挂着的时候：重启后要把它撤掉；撤掉前已经成交的部分要报出来（没有对应的仓位记录）。
        #[tokio::test]
        async fn a_post_only_order_left_resting_by_a_crash_is_cancelled_on_restart() {
            for (name, cancel_works, filled, expect_resolved, expect_filled) in [
                ("orphan-idle", true, Decimal::ZERO, true, false),
                ("orphan-partial", true, dec!(300), true, true),
                ("orphan-stuck", false, Decimal::ZERO, false, false),
            ] {
                let ledger = temp_ledger(name).await;
                let mut local = pending("p-buy-0", Venue::Binance, Side::Buy, false);
                local.order.post_only = true;
                local.order.limit_price = Some(dec!(100));
                local.status = OrderStatus::Open;
                local.venue_order_id = Some("p-buy-0".into());
                ledger
                    .append(&crate::ledger::Record::Order(Box::new(local.clone())))
                    .await
                    .unwrap();
                let broker = Orphan {
                    state: tokio::sync::Mutex::new(local),
                    cancel_works,
                    filled_before_cancel: filled,
                };
                let brokers: HashMap<Venue, Arc<dyn Broker>> =
                    HashMap::from([(Venue::Binance, Arc::new(broker) as Arc<dyn Broker>)]);

                let resolution = resolve_pending_orders(&ledger, &brokers).await.unwrap();

                assert_eq!(resolution.resolved.len() == 1, expect_resolved, "{name}");
                assert_eq!(
                    resolution.filled_unrecorded.len() == 1,
                    expect_filled,
                    "{name}"
                );
                assert_eq!(resolution.still_open.len() == 1, !expect_resolved, "{name}");
                let (replayed, _) = ledger.replay().await.unwrap();
                let leftover = local_open_orders(&replayed);
                assert_eq!(
                    leftover
                        .get(&Venue::Binance)
                        .is_some_and(|ids| !ids.is_empty()),
                    !expect_resolved,
                    "{name}：撤成功的不该留在本地挂单里，没撤成的必须留给对账"
                );
            }
        }

        /// 券商返回的订单与台账里的意图对不上（方向 / 只减仓标记不同）：不信它，留给对账。
        #[tokio::test]
        async fn a_broker_record_that_contradicts_the_intent_is_not_trusted() {
            let ledger = temp_ledger("contradiction").await;
            ledger
                .append(&crate::ledger::Record::Order(Box::new(pending(
                    "x-buy-0",
                    Venue::Binance,
                    Side::Buy,
                    false,
                ))))
                .await
                .unwrap();
            let mut broker = Scripted::new(Venue::Binance);
            let mut other = pending("x-buy-0", Venue::Binance, Side::Sell, false);
            other.status = OrderStatus::Filled;
            broker.answers.insert("x-buy-0".into(), Ok(Some(other)));
            let brokers: HashMap<Venue, Arc<dyn Broker>> =
                HashMap::from([(Venue::Binance, Arc::new(broker) as Arc<dyn Broker>)]);

            let resolution = resolve_pending_orders(&ledger, &brokers).await.unwrap();

            assert!(resolution.resolved.is_empty());
            assert_eq!(resolution.unknown.len(), 1);
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(replayed.orders["x-buy-0"].status, OrderStatus::Pending);
        }

        /// 退出重试：失败后指数退避（30 秒、1 分、2 分……）封顶 30 分钟，成功清零；
        /// 各仓位互不影响。
        #[test]
        fn exit_retries_back_off_exponentially_up_to_a_cap_and_reset_on_success() {
            let retries = ExitRetries::default();
            let t0 = Instant::now();
            assert!(retries.due("p", t0), "没失败过的永远可以试");

            assert_eq!(retries.failed("p", t0), 1);
            assert!(!retries.due("p", t0 + Duration::from_secs(29)));
            assert!(retries.due("p", t0 + Duration::from_secs(30)));
            assert!(retries.due("other", t0), "别的仓位不受影响");

            let t1 = t0 + Duration::from_secs(30);
            assert_eq!(retries.failed("p", t1), 2);
            assert!(!retries.due("p", t1 + Duration::from_secs(59)));
            assert!(retries.due("p", t1 + Duration::from_secs(60)));

            let mut now = t1;
            for _ in 0..30 {
                retries.failed("p", now);
                now += Duration::from_secs(1);
            }
            let last = now - Duration::from_secs(1);
            assert!(!retries.due("p", last + Duration::from_secs(29 * 60)));
            assert!(
                retries.due("p", last + Duration::from_secs(30 * 60)),
                "封顶 30 分钟：再多次失败也不会等得更久"
            );

            retries.succeeded("p");
            assert!(retries.due("p", last));
            assert_eq!(retries.failures("p"), 0);
        }

        fn leg(venue: Venue, side: Side) -> LegFill {
            LegFill {
                venue,
                side,
                notional_usdt: dec!(1000),
                average_price: dec!(100),
                fee_usdt: Decimal::ZERO,
                client_order_id: ClientOrderId::for_leg("p", side, 0),
                margin_usdt: Some(dec!(300)),
            }
        }

        fn position(id: &str, status: PositionStatus, hedged: bool) -> PairPosition {
            PairPosition {
                margin_mode: crate::MarginMode::Isolated,
                id: id.into(),
                symbol: Symbol::perp("BTC", "USDT"),
                strategy: Strategy::Funding,
                long: Some(leg(Venue::Binance, Side::Buy)),
                short: hedged.then(|| leg(Venue::Bybit, Side::Sell)),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status,
                opened_at: chrono::Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(3)),
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
            }
        }

        /// 全部冻结的一轮：已经开始的退出（Closing）照常重试，补那条裸腿；规则不执行；开仓结果还没核实的
        /// Opening 交给续上建仓，只核实、不发单，更不按台账退出第一腿 —— 那会把已经成交的对冲拆掉、
        /// 留下第二腿裸奔。（这笔 Opening 记着一条腿、台账里却没有它的开仓单：留给人工。）
        #[tokio::test]
        async fn a_dirty_round_retries_started_exits_but_not_unverified_openings() {
            let ledger = temp_ledger("dirty-round").await;
            for position in [
                position("closing", PositionStatus::Closing, false),
                position("opening", PositionStatus::Opening, false),
                position("open", PositionStatus::Open, true),
            ] {
                ledger
                    .append(&crate::ledger::Record::Position(Box::new(position)))
                    .await
                    .unwrap();
            }
            let mut binance = Scripted::new(Venue::Binance);
            binance.fills = true;
            let executor = Executor::new(
                Arc::clone(&ledger),
                vec![Arc::new(binance) as Arc<dyn Broker>],
            );
            let retries = ExitRetries::default();

            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let reports = live_round(
                &executor,
                &ledger,
                &HashMap::new(),
                &ctx,
                Frozen::Everything,
            )
            .await
            .unwrap();

            let report = |id: &str| reports.iter().find(|r| r.position_id == id).unwrap();
            assert!(report("closing").executed, "已经开始的退出必须重试");
            assert_eq!(report("closing").status, PositionStatus::Closed);
            assert!(!report("opening").executed);
            assert!(
                report("opening")
                    .error
                    .as_deref()
                    .is_some_and(|why| why.contains("建仓中断，需要人工处理")),
                "{:?}",
                report("opening").error
            );
            assert!(!report("open").executed);
            assert!(report("open").evaluation.is_none(), "规则不评估");
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(
                replayed.positions["opening"].status,
                PositionStatus::Opening
            );
            assert_eq!(replayed.positions["open"].status, PositionStatus::Open);
            assert!(
                replayed.orders.keys().all(|id| id.starts_with("closing-")),
                "只有 closing 发过单：{:?}",
                replayed.orders.keys().collect::<Vec<_>>()
            );
        }

        fn divergence_on(
            kind: crate::reconcile::DivergenceKind,
            venue: Venue,
            symbol: Option<&str>,
        ) -> crate::reconcile::Divergence {
            crate::reconcile::Divergence {
                kind,
                venue: Some(venue),
                symbol: symbol.map(|base| Symbol::perp(base, "USDT")),
                reference: "x".into(),
                detail: "测试".into(),
            }
        }

        fn reconciliation_of(divergences: Vec<crate::reconcile::Divergence>) -> Reconciliation {
            Reconciliation {
                divergences,
                checked_positions: 0,
                checked_venues: 1,
                elapsed_ms: Vec::new(),
            }
        }

        /// 只冻结出问题的币；整个场所没核对的，冻结在它上面有腿的；归不到哪里的，全部冻结。
        #[test]
        fn a_divergence_freezes_only_what_it_belongs_to() {
            use crate::reconcile::DivergenceKind as Kind;
            let btc = position("btc", PositionStatus::Open, true); // Binance 多 / Bybit 空
            let mut eth = position("eth", PositionStatus::Open, true);
            eth.symbol = Symbol::perp("ETH", "USDT");
            let mut elsewhere = position("sol", PositionStatus::Open, false);
            elsewhere.symbol = Symbol::perp("SOL", "USDT");
            elsewhere.long.as_mut().unwrap().venue = Venue::Okx;

            let eth_only = reconciliation_of(vec![divergence_on(
                Kind::PositionMismatch,
                Venue::Bybit,
                Some("ETH"),
            )]);
            assert!(!Frozen::By(&eth_only).blocks(&btc));
            assert!(Frozen::By(&eth_only).blocks(&eth));
            assert_eq!(eth_only.frozen_scope(), ["ETH/USDT"]);

            let bybit_down =
                reconciliation_of(vec![divergence_on(Kind::Unverified, Venue::Bybit, None)]);
            assert!(Frozen::By(&bybit_down).blocks(&btc));
            assert!(Frozen::By(&bybit_down).blocks(&eth));
            assert!(!Frozen::By(&bybit_down).blocks(&elsewhere));
            // 开仓计划里有这个场所的也算（建仓中断、还没记腿）。
            let mut planned = bare_opening("plan");
            planned.planned = Some(crate::types::PlannedLegs {
                long: Venue::Okx,
                short: Venue::Bybit,
                first: Side::Buy,
                force_hedge: None,
            });
            assert!(Frozen::By(&bybit_down).blocks(&planned));

            let unknown = reconciliation_of(vec![divergence_on(
                Kind::OrderMissingOnVenue,
                Venue::Okx,
                None,
            )]);
            assert!(Frozen::By(&unknown).blocks(&btc));
            assert!(Frozen::By(&unknown).blocks(&elsewhere));
            assert_eq!(unknown.frozen_scope(), ["全部"]);

            // 开新仓按 base（quote 给了才比）与两腿场所判断。
            assert!(eth_only.blocks_open("eth", None, &[Venue::Okx, Venue::Binance]));
            assert!(eth_only.blocks_open("ETH", Some("usdt"), &[Venue::Okx, Venue::Binance]));
            assert!(!eth_only.blocks_open("ETH", Some("USDC"), &[Venue::Okx, Venue::Binance]));
            assert!(!eth_only.blocks_open("BTC", None, &[Venue::Bybit, Venue::Binance]));
            assert!(bybit_down.blocks_open("BTC", None, &[Venue::Bybit, Venue::Binance]));
            assert!(!bybit_down.blocks_open("BTC", None, &[Venue::Okx, Venue::Binance]));
        }

        fn bare_opening(id: &str) -> PairPosition {
            let mut position = position(id, PositionStatus::Opening, false);
            position.long = None;
            position
        }

        /// 一个币对账不一致，别的币照常评估规则（某次实盘 一笔卡住，冻结了全部持仓的规则约 2 小时）。
        #[tokio::test]
        async fn one_symbols_divergence_leaves_the_other_symbols_rules_running() {
            use crate::reconcile::DivergenceKind as Kind;
            let ledger = temp_ledger("frozen-symbol").await;
            let btc = position("btc", PositionStatus::Open, true);
            let mut eth = position("eth", PositionStatus::Open, true);
            eth.symbol = Symbol::perp("ETH", "USDT");
            for position in [btc, eth] {
                ledger
                    .append(&crate::ledger::Record::Position(Box::new(position)))
                    .await
                    .unwrap();
            }
            let executor = Executor::new(
                Arc::clone(&ledger),
                vec![Arc::new(Scripted::new(Venue::Binance)) as Arc<dyn Broker>],
            );
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let eth_only = reconciliation_of(vec![divergence_on(
                Kind::PositionMismatch,
                Venue::Bybit,
                Some("ETH"),
            )]);
            let reports = live_round(
                &executor,
                &ledger,
                &HashMap::new(),
                &ctx,
                Frozen::By(&eth_only),
            )
            .await
            .unwrap();
            let report = |id: &str| reports.iter().find(|r| r.position_id == id).unwrap();
            assert!(
                report("eth")
                    .skipped
                    .as_deref()
                    .is_some_and(|why| why.contains("对账不一致（ETH/USDT）")),
                "{:?}",
                report("eth").skipped
            );
            assert!(
                !report("btc")
                    .skipped
                    .as_deref()
                    .unwrap_or_default()
                    .contains("对账不一致"),
                "{:?}",
                report("btc").skipped
            );
        }

        /// 只按订单号报出来的不一致（本地记着、交易所没有的挂单），按台账补上是哪个币。
        #[tokio::test]
        async fn a_missing_order_is_attributed_to_its_symbol_from_the_ledger() {
            let ledger = temp_ledger("attribute-order").await;
            let order = crate::types::NewOrder {
                post_only: false,
                client_order_id: crate::types::ClientOrderId("eth-sell-0".into()),
                venue: Venue::Binance,
                symbol: Symbol::perp("ETH", "USDT"),
                side: Side::Sell,
                notional_usdt: dec!(100),
                quantity: None,
                limit_price: None,
                reduce_only: false,
                leverage: None,
                margin_mode: crate::MarginMode::Isolated,
            };
            ledger
                .append(&crate::ledger::Record::Order(Box::new(
                    crate::types::OrderState::new(order),
                )))
                .await
                .unwrap();
            let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([(
                Venue::Binance,
                Arc::new(Scripted::new(Venue::Binance)) as Arc<dyn Broker>,
            )]);
            let reconciliation = reconcile_ledger(&ledger, &brokers).await.unwrap();
            assert_eq!(reconciliation.divergences.len(), 1);
            assert_eq!(
                reconciliation.divergences[0].symbol,
                Some(Symbol::perp("ETH", "USDT"))
            );
            assert!(!reconciliation.blocks(&Symbol::perp("BTC", "USDT"), &[Venue::Binance]));
        }

        /// 退出失败后下一轮在退避期内不再发单（不刷接口），退避期过后才重试。
        #[tokio::test]
        async fn a_failed_exit_is_not_resent_every_round() {
            let ledger = temp_ledger("backoff").await;
            ledger
                .append(&crate::ledger::Record::Position(Box::new(position(
                    "closing",
                    PositionStatus::Closing,
                    false,
                ))))
                .await
                .unwrap();
            // 不下单的桩：每次退出都失败。
            let executor = Executor::new(
                Arc::clone(&ledger),
                vec![Arc::new(Scripted::new(Venue::Binance)) as Arc<dyn Broker>],
            );
            let retries = ExitRetries::default();

            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let first = live_round(
                &executor,
                &ledger,
                &HashMap::new(),
                &ctx,
                Frozen::Everything,
            )
            .await
            .unwrap();
            assert!(first[0].executed && first[0].error.is_some());
            assert_eq!(retries.failures("closing"), 1);

            let second = live_round(
                &executor,
                &ledger,
                &HashMap::new(),
                &ctx,
                Frozen::Everything,
            )
            .await
            .unwrap();
            assert!(!second[0].executed, "退避期内不能再发单");
            assert!(second[0].skipped.as_deref().unwrap().contains("退避"));
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(
                replayed.positions["closing"].exits, 1,
                "第二轮没有新的退出编号被消耗"
            );
        }
    }

    mod rules {
        use super::super::*;
        use crate::types::{ClientOrderId, LegFill, NewOrder, OrderState, OrderStatus};
        use arb_core::{Level, Side};
        use async_trait::async_trait;
        use rust_decimal_macros::dec;
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// 成交一律按 100、补保证金按队列里给定的结果应答的桩券商。
        struct Stub {
            venue: Venue,
            free: Option<Decimal>,
            supports_margin: bool,
            margin: std::sync::Mutex<VecDeque<ArbResult<crate::broker::MarginOutcome>>>,
            calls: AtomicUsize,
            placed: tokio::sync::Mutex<HashMap<String, OrderState>>,
            /// 账户吃单费率（[`Broker::fee_per_side`]）。
            fee: Decimal,
            /// 带限价的单一概没有对手盘（被拒，一点不成交）：平仓限价保护没成交的情形。
            reject_limits: bool,
        }

        impl Stub {
            fn new(venue: Venue) -> Self {
                Self {
                    venue,
                    free: Some(dec!(10_000)),
                    supports_margin: true,
                    margin: Default::default(),
                    calls: AtomicUsize::new(0),
                    placed: Default::default(),
                    fee: Decimal::ZERO,
                    reject_limits: false,
                }
            }

            fn answering(self, outcome: ArbResult<crate::broker::MarginOutcome>) -> Self {
                self.margin.lock().unwrap().push_back(outcome);
                self
            }
        }

        #[async_trait]
        impl Broker for Stub {
            fn venue(&self) -> Venue {
                self.venue
            }
            fn fee_per_side(&self) -> Decimal {
                self.fee
            }
            fn supports_add_margin(&self) -> bool {
                self.supports_margin
            }
            async fn add_margin(
                &self,
                _symbol: &Symbol,
                _amount: Decimal,
            ) -> ArbResult<crate::broker::MarginOutcome> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.margin
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("桩没有预设这一次的应答：不该走到这里")
            }
            async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
                Ok(self.free)
            }
            async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
                let mut state = OrderState::new(order.clone());
                state.venue_order_id = Some(order.client_order_id.0.clone());
                if self.reject_limits && order.limit_price.is_some() {
                    state.status = OrderStatus::Rejected;
                    state.reject_reason = Some("could not immediately match".into());
                } else {
                    state.status = OrderStatus::Filled;
                    state.average_price = Some(dec!(100));
                    state.filled_usdt = order.quantity.unwrap_or_default() * dec!(100);
                }
                let status = state.status;
                self.placed
                    .lock()
                    .await
                    .insert(order.client_order_id.0.clone(), state);
                Ok(crate::types::OrderAck {
                    client_order_id: order.client_order_id.clone(),
                    venue_order_id: order.client_order_id.0.clone(),
                    status,
                })
            }
            async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(self.placed.lock().await.get(&id.0).cloned())
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                Ok(())
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(vec![])
            }
            async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
                Ok(vec![])
            }
        }

        /// 只给盘口：一档买一卖一，深度足够。
        struct Book {
            venue: Venue,
            bid: Decimal,
            ask: Decimal,
        }

        #[async_trait]
        impl VenueApi for Book {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
                Ok(Vec::new())
            }
            async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
                let level = |price| Level {
                    price,
                    notional_usdt: dec!(1_000_000),
                };
                Ok(OrderBook {
                    venue: self.venue,
                    symbol: symbol.clone(),
                    bids: vec![level(self.bid)],
                    asks: vec![level(self.ask)],
                })
            }
        }

        /// 每次被问盘口按脚本里的下一份应答（最后一份重复）；记下被问了几次，可选地让某一次晚回来。
        struct ScriptedBook {
            venue: Venue,
            script: Vec<(Decimal, Decimal)>,
            slow_on_call: Option<(usize, std::time::Duration)>,
            calls: AtomicUsize,
        }

        impl ScriptedBook {
            fn new(venue: Venue, script: Vec<(Decimal, Decimal)>) -> Arc<Self> {
                Arc::new(Self {
                    venue,
                    script,
                    slow_on_call: None,
                    calls: AtomicUsize::new(0),
                })
            }

            fn slow_on(
                venue: Venue,
                script: Vec<(Decimal, Decimal)>,
                call: usize,
                delay: std::time::Duration,
            ) -> Arc<Self> {
                Arc::new(Self {
                    venue,
                    script,
                    slow_on_call: Some((call, delay)),
                    calls: AtomicUsize::new(0),
                })
            }

            fn asked(&self) -> usize {
                self.calls.load(Ordering::SeqCst)
            }
        }

        #[async_trait]
        impl VenueApi for ScriptedBook {
            fn venue(&self) -> Venue {
                self.venue
            }
            async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
                Ok(Vec::new())
            }
            async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some((slow, delay)) = self.slow_on_call
                    && slow == call
                {
                    tokio::time::sleep(delay).await;
                }
                let (bid, ask) = self.script[call.min(self.script.len() - 1)];
                let level = |price| Level {
                    price,
                    notional_usdt: dec!(1_000_000),
                };
                Ok(OrderBook {
                    venue: self.venue,
                    symbol: symbol.clone(),
                    bids: vec![level(bid)],
                    asks: vec![level(ask)],
                })
            }
        }

        fn scripted(
            long: &Arc<ScriptedBook>,
            short: &Arc<ScriptedBook>,
        ) -> HashMap<Venue, Arc<dyn VenueApi>> {
            HashMap::from([
                (Venue::Lighter, Arc::clone(long) as Arc<dyn VenueApi>),
                (Venue::Hyperliquid, Arc::clone(short) as Arc<dyn VenueApi>),
            ])
        }

        struct FixedFunding(Option<Decimal>);

        #[async_trait]
        impl FundingSource for FixedFunding {
            async fn funding_usdt(&self, _position: &PairPosition) -> Option<Decimal> {
                self.0
            }
        }

        fn symbol() -> Symbol {
            Symbol::perp("BTC", "USDT")
        }

        fn snapshot(venue: Venue, mark: Decimal) -> MarketSnapshot {
            MarketSnapshot {
                venue,
                symbol: symbol(),
                period_rate: dec!(0.00001),
                interval_h: 1,
                interval_assumed: false,
                next_funding_at: chrono::Utc::now(),
                next_funding_estimated: true,
                taker_fee: None,
                mark_price: Some(mark),
                index_price: Some(mark),
                best_bid: None,
                best_ask: None,
                bid_size_usdt: None,
                ask_size_usdt: None,
                open_interest_usdt: None,
                quote_volume_24h: None,
                max_leverage: Some(dec!(50)),
                maintenance_margin: Some(dec!(0.01)),
                oi_capped: false,
            }
        }

        /// 多腿 Lighter、空腿 Hyperliquid，入场价都是 100、各 10 个，5 倍（保证金各 200）。
        fn position(rules: TaskRules) -> PairPosition {
            let leg = |venue, side| LegFill {
                venue,
                side,
                notional_usdt: dec!(1000),
                average_price: dec!(100),
                fee_usdt: Decimal::ZERO,
                client_order_id: ClientOrderId::for_leg("p", side, 0),
                margin_usdt: Some(dec!(200)),
            };
            PairPosition {
                margin_mode: crate::MarginMode::Isolated,
                id: "p".into(),
                symbol: symbol(),
                strategy: Strategy::Funding,
                long: Some(leg(Venue::Lighter, Side::Buy)),
                short: Some(leg(Venue::Hyperliquid, Side::Sell)),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status: PositionStatus::Open,
                opened_at: chrono::Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(5)),
                rules,
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
            }
        }

        fn snapshots(
            long_mark: Decimal,
            short_mark: Decimal,
        ) -> HashMap<(Venue, Symbol), MarketSnapshot> {
            HashMap::from([
                (
                    (Venue::Lighter, symbol()),
                    snapshot(Venue::Lighter, long_mark),
                ),
                (
                    (Venue::Hyperliquid, symbol()),
                    snapshot(Venue::Hyperliquid, short_mark),
                ),
            ])
        }

        async fn temp_ledger(name: &str) -> Arc<Ledger> {
            let path = std::env::temp_dir().join(format!(
                "arb-desk-rules-{name}-{}.jsonl",
                std::process::id()
            ));
            let _ = tokio::fs::remove_file(&path).await;
            Arc::new(Ledger::open(&path).await.unwrap())
        }

        fn auto_margin(trigger: Decimal, cap: Decimal) -> TaskRules {
            TaskRules {
                auto_margin_pct: Some(trigger),
                auto_margin_max_usdt: Some(cap),
                ..TaskRules::default()
            }
        }

        fn executor_over(
            ledger: &Arc<Ledger>,
            long: Stub,
            short: Stub,
        ) -> (Executor, Arc<Stub>, Arc<Stub>) {
            let (long, short) = (Arc::new(long), Arc::new(short));
            let executor = Executor::new(
                Arc::clone(ledger),
                vec![
                    Arc::clone(&long) as Arc<dyn Broker>,
                    Arc::clone(&short) as Arc<dyn Broker>,
                ],
            );
            (executor, long, short)
        }

        /// 页面与 Telegram 的「净收益」都按盘口：没设任何规则的仓位也每轮给出盘口平仓估算和资金费，且不会因此动作。
        #[tokio::test]
        async fn a_position_without_rules_still_gets_the_book_net_and_never_acts_on_it() {
            let books: HashMap<Venue, Arc<dyn VenueApi>> = HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Book {
                        venue: Venue::Lighter,
                        bid: dec!(102),
                        ask: dec!(102.1),
                    }) as Arc<dyn VenueApi>,
                ),
                (
                    Venue::Hyperliquid,
                    Arc::new(Book {
                        venue: Venue::Hyperliquid,
                        bid: dec!(100.9),
                        ask: dec!(101),
                    }) as Arc<dyn VenueApi>,
                ),
            ]);
            let marks = snapshots(dec!(102), dec!(101));
            let retries = ExitRetries::default();
            let ledger = temp_ledger("book-net-no-rules").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(TaskRules::default());
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed, "没设规则不能动作");
            assert_eq!(held.status, PositionStatus::Open);
            let observation = report.evaluation.unwrap().observation;
            let quote = observation
                .exit
                .as_ref()
                .expect("没设规则也要给出盘口平仓估算");
            assert_eq!(observation.funding_usdt, Some(dec!(2)));
            assert_eq!(
                observation.book_net_with_funding_usdt(),
                Some(quote.net_usdt + dec!(2))
            );
        }

        /// 止盈：含资金费的净盈利达标、按盘口平掉后仍达标，才真的平。
        #[tokio::test]
        async fn take_profit_closes_a_profitable_pair_once_funding_is_counted() {
            let books: HashMap<Venue, Arc<dyn VenueApi>> = HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Book {
                        venue: Venue::Lighter,
                        bid: dec!(102),
                        ask: dec!(102.1),
                    }) as Arc<dyn VenueApi>,
                ),
                (
                    Venue::Hyperliquid,
                    Arc::new(Book {
                        venue: Venue::Hyperliquid,
                        bid: dec!(100.9),
                        ask: dec!(101),
                    }) as Arc<dyn VenueApi>,
                ),
            ]);
            // 多腿 100→102（+20）、空腿 100→101（−10）：价格 +10。止盈 11，要靠收到的资金费凑。
            let marks = snapshots(dec!(102), dec!(101));
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let retries = ExitRetries::default();

            // 没有资金费流水：不评估，更不能凭价格盈亏平仓。
            let ledger = temp_ledger("tp-unknown").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules.clone());
            let ctx = RoundCtx {
                funding: &FixedFunding(None),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed, "资金费没查到不能平");
            assert_eq!(held.status, PositionStatus::Open);
            let evaluation = report.evaluation.unwrap();
            assert!(
                evaluation
                    .skipped
                    .iter()
                    .any(|note| note.contains("资金费流水没查到"))
            );

            // 资金费 0：价格净 10 < 11，没到，不平；盘口估算只留给页面展示。
            let ledger = temp_ledger("tp-short").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules.clone());
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(0))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed);
            assert_eq!(held.status, PositionStatus::Open);
            let evaluation = report.evaluation.unwrap();
            assert!(
                evaluation.observation.exit.is_some(),
                "设了止盈每轮都给出盘口平仓估算"
            );
            assert!(evaluation.take_profit_hold.is_none());

            // 收到 2 的资金费：10 + 2 = 12 ≥ 11，按盘口平仓预估（10 + 2）仍达标：平。
            let ledger = temp_ledger("tp-hit").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(
                report.executed && report.error.is_none(),
                "{:?}",
                report.error
            );
            assert_eq!(held.status, PositionStatus::Closed);
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(replayed.positions["p"].status, PositionStatus::Closed);
        }

        /// 止盈的盘口核对：两条腿都是挂单开仓（开仓手续费 0），平仓照样要付吃单费，按场所账户的吃单费率估 ——
        /// 按开仓费率估成 0 会在其实不达标时平掉。
        #[tokio::test]
        async fn the_take_profit_book_check_charges_the_exit_at_the_taker_rate() {
            let books: HashMap<Venue, Arc<dyn VenueApi>> = HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Book {
                        venue: Venue::Lighter,
                        bid: dec!(102),
                        ask: dec!(102.1),
                    }) as Arc<dyn VenueApi>,
                ),
                (
                    Venue::Hyperliquid,
                    Arc::new(Book {
                        venue: Venue::Hyperliquid,
                        bid: dec!(100.9),
                        ask: dec!(101),
                    }) as Arc<dyn VenueApi>,
                ),
            ]);
            // 价格 +10、资金费 +2：按开仓费率（0）估平仓净 12 ≥ 11.9；按吃单 0.05% 估要扣
            // 10 × 102 × 0.05% + 10 × 101 × 0.05% = 1.015，只剩 10.985，不到止盈。
            let marks = snapshots(dec!(102), dec!(101));
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11.9)),
                ..TaskRules::default()
            };
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let ledger = temp_ledger("tp-taker-fee").await;
            let taker = |venue| Stub {
                fee: dec!(0.0005),
                ..Stub::new(venue)
            };
            let (executor, ..) =
                executor_over(&ledger, taker(Venue::Lighter), taker(Venue::Hyperliquid));
            let mut held = position(rules.clone());
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed, "按吃单费率估不到止盈，不能平");
            assert_eq!(held.status, PositionStatus::Open);
            let quote = report.evaluation.unwrap().observation.exit.unwrap();
            assert_eq!(quote.exit_fee_usdt, dec!(1.015));
            assert_eq!(quote.net_usdt, dec!(8.985));

            // 同样的盘口，账户吃单费率 0（Lighter 这类）：照旧平。
            let ledger = temp_ledger("tp-zero-fee").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(
                report.executed && report.error.is_none(),
                "{:?}",
                report.error
            );
            assert_eq!(held.status, PositionStatus::Closed);
        }

        /// 获利平仓的限价保护：只有一条腿在 Hyperliquid 时保护那条腿，限价 = 这笔数量要吃到的最差一档再让 0.05%。
        #[test]
        fn only_a_lone_hyperliquid_leg_is_guarded_at_its_worst_level_less_the_buffer() {
            let mut quote = crate::monitor::ExitQuote {
                long_price: dec!(100),
                short_price: dec!(101),
                exit_basis_pct: dec!(1),
                exit_fee_usdt: Decimal::ZERO,
                net_usdt: Decimal::ONE,
                net_pct: dec!(0.1),
                cost: None,
                fraction: Decimal::ONE,
                long_worst_price: dec!(99.9),
                short_worst_price: dec!(101.2),
            };
            // 多腿 Lighter、空腿 Hyperliquid：保护空腿的买回，不高于 101.2 × 1.0005。
            let mut held = position(TaskRules::default());
            let guard = exit_guard(&held, &quote).unwrap();
            assert_eq!(guard.side, Side::Buy);
            assert_eq!(guard.limit_price, dec!(101.2) * dec!(1.0005));
            // 多腿在 xyz：保护多腿的卖出，不低于 99.9 × 0.9995。
            held.long.as_mut().unwrap().venue = Venue::HyperliquidXyz;
            held.short.as_mut().unwrap().venue = Venue::Arcus;
            let guard = exit_guard(&held, &quote).unwrap();
            assert_eq!(guard.side, Side::Sell);
            assert_eq!(guard.limit_price, dec!(99.9) * dec!(0.9995));
            // 两条腿都在 Hyperliquid（xyz / io）、或都不在：不保护。
            held.short.as_mut().unwrap().venue = Venue::HyperliquidIo;
            assert!(exit_guard(&held, &quote).is_none());
            held.long.as_mut().unwrap().venue = Venue::Lighter;
            held.short.as_mut().unwrap().venue = Venue::Arcus;
            assert!(exit_guard(&held, &quote).is_none());
            quote.long_worst_price = Decimal::ZERO;
            held.long.as_mut().unwrap().venue = Venue::Hyperliquid;
            assert!(exit_guard(&held, &quote).is_none(), "算不出限价就不保护");
        }

        fn guarded_books() -> HashMap<Venue, Arc<dyn VenueApi>> {
            HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Book {
                        venue: Venue::Lighter,
                        bid: dec!(102),
                        ask: dec!(102.1),
                    }) as Arc<dyn VenueApi>,
                ),
                (
                    Venue::Hyperliquid,
                    Arc::new(Book {
                        venue: Venue::Hyperliquid,
                        bid: dec!(100.9),
                        ask: dec!(101),
                    }) as Arc<dyn VenueApi>,
                ),
            ])
        }

        /// 止盈：空腿在 Hyperliquid，先发它的限价买回（卖一 101 × 1.0005），成交了才按市价平 Lighter 那条腿。
        #[tokio::test]
        async fn a_take_profit_sends_the_hyperliquid_leg_first_with_a_limit() {
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let ledger = temp_ledger("tp-guarded").await;
            let (executor, long, short) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let marks = snapshots(dec!(102), dec!(101));
            let report =
                monitor_position(&executor, &mut held, &marks, &guarded_books(), &ctx).await;
            assert!(
                report.executed && report.error.is_none(),
                "{:?}",
                report.error
            );
            assert!(report.exit_held.is_none());
            assert_eq!(held.status, PositionStatus::Closed);
            let hl = short.placed.lock().await;
            let first = &hl["p-buy-exit1"].order;
            assert_eq!(first.limit_price, Some(dec!(101) * dec!(1.0005)));
            assert!(first.reduce_only);
            let lighter = long.placed.lock().await;
            assert_eq!(
                lighter["p-sell-exit2"].order.limit_price, None,
                "另一条腿按市价"
            );
            assert_eq!(lighter.len(), 1);
        }

        /// 止盈：先发的 Hyperliquid 限价单没有对手盘 —— 两条腿都没动、继续持有；不算失败、不推送「规则已执行」。
        #[tokio::test]
        async fn a_take_profit_whose_guarded_leg_finds_no_match_keeps_the_position() {
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let ledger = temp_ledger("tp-guard-held").await;
            let rejecting = Stub {
                reject_limits: true,
                ..Stub::new(Venue::Hyperliquid)
            };
            let (executor, long, short) =
                executor_over(&ledger, Stub::new(Venue::Lighter), rejecting);
            let mut held = position(rules);
            ledger
                .append(&Record::Position(Box::new(held.clone())))
                .await
                .unwrap();
            let marks = snapshots(dec!(102), dec!(101));
            let report =
                monitor_position(&executor, &mut held, &marks, &guarded_books(), &ctx).await;
            assert!(report.executed, "限价单发出去了");
            assert!(report.error.is_none(), "不是失败：{:?}", report.error);
            let why = report.exit_held.as_deref().expect("要写明为什么没平");
            assert!(why.contains("没成交"), "{why}");
            assert_eq!(report.status, PositionStatus::Open);
            assert_eq!(held.status, PositionStatus::Open);
            assert!(held.long.is_some() && held.short.is_some());
            assert!(long.placed.lock().await.is_empty(), "Lighter 那条腿不能动");
            assert_eq!(short.placed.lock().await.len(), 1);
            assert_eq!(retries.failures("p"), 0);
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(replayed.positions["p"].status, PositionStatus::Open);
        }

        /// 止盈只凭一次盘口快照触发会在噪声上沿开枪：第一次盘口达标、隔一会儿重拉就不达标了，
        /// 说明那是一瞬间的尖峰，不平，下一轮再看。
        #[tokio::test]
        async fn a_take_profit_spike_that_vanishes_on_the_second_look_is_not_taken() {
            // 第一次：多腿买盘 102（+20）、空腿卖盘 101（−10），价格 +10，加资金费 2 = 12 ≥ 止盈 11。
            // 复核：多腿买盘掉到 100.5（+5）、空腿不变（−10），价格 −5，加资金费 2 = −3，不再达标。
            let long = ScriptedBook::new(
                Venue::Lighter,
                vec![(dec!(102), dec!(102.1)), (dec!(100.5), dec!(100.6))],
            );
            let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
            let books = scripted(&long, &short);
            let marks = snapshots(dec!(102), dec!(101));
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let ledger = temp_ledger("tp-spike").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed, "尖峰过去了不能平");
            assert_eq!(held.status, PositionStatus::Open);
            assert_eq!(long.asked(), 2, "触发后要重拉一次盘口复核");
            let evaluation = report.evaluation.unwrap();
            assert!(
                evaluation
                    .skipped
                    .iter()
                    .any(|note| note.contains("平仓复核没过")),
                "{:?}",
                evaluation.skipped
            );
            // 复核用的是新报价：页面上看到的盘口估算也是新的。
            let quote = evaluation.observation.exit.as_ref().unwrap();
            assert_eq!(quote.long_price, dec!(100.5));
            assert!(evaluation.take_profit_hold.is_some());
            // 挡下的原因里带差额拆分：钱花在标记价 vs 盘口、买卖价差、吃深度、手续费哪一项上。
            let hold = evaluation.take_profit_hold.as_ref().unwrap();
            let gap = hold.gap.expect("两本盘口都有买卖两侧、两条腿都有标记价");
            assert!(
                hold.reason.contains("标记价与盘口的差额"),
                "{}",
                hold.reason
            );
            assert_eq!(hold.reason.matches("吃深度").count(), 1);
            assert!(gap.spread_usdt >= Decimal::ZERO);
        }

        /// 复核时盘口仍然达标：照常平，而且拿的是复核那一份报价；两次各拉一次盘口。
        #[tokio::test]
        async fn a_take_profit_that_holds_on_the_second_look_is_taken() {
            let long = ScriptedBook::new(
                Venue::Lighter,
                vec![(dec!(102), dec!(102.1)), (dec!(101.9), dec!(102))],
            );
            let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
            let books = scripted(&long, &short);
            let marks = snapshots(dec!(102), dec!(101));
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let ledger = temp_ledger("tp-confirmed").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(
                report.executed && report.error.is_none(),
                "{:?}",
                report.error
            );
            assert_eq!(held.status, PositionStatus::Closed);
            assert_eq!(long.asked(), 2);
            assert_eq!(short.asked(), 2);
            let quote = report.evaluation.unwrap().observation.exit.unwrap();
            assert_eq!(quote.long_price, dec!(101.9), "平仓依据是复核那份报价");
        }

        /// 盘口触发止盈：标记价离止盈还远（规则轮不会去核对盘口），但两边盘口本身整笔平掉已到止盈 ——
        /// 规则轮不平，盘口触发平；两次现拉盘口（第一次 + 复核），平仓依据是复核那份报价。
        #[tokio::test]
        async fn a_book_at_take_profit_closes_even_while_the_mark_price_lags() {
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            // 标记价：两腿都 101 → 价格 +10 −10 = 0，加资金费 2 = 2，远不到 11。
            let marks = snapshots(dec!(101), dec!(101));
            // 盘口：多腿买盘 102（+20）、空腿卖盘 101（−10）→ +10，加资金费 2 = 12 ≥ 11；复核 101.9 → 11。
            let pick = |name: &str| {
                let long = ScriptedBook::new(
                    Venue::Lighter,
                    vec![(dec!(102), dec!(102.1)), (dec!(101.9), dec!(102))],
                );
                let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
                (long, short, name.to_string())
            };
            let funding = FixedFunding(Some(dec!(2)));
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &funding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };

            let (long, short, name) = pick("book-trigger-round");
            let books = scripted(&long, &short);
            let ledger = temp_ledger(&name).await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules.clone());
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed, "规则轮先看标记价：没到止盈不平");
            assert_eq!(held.status, PositionStatus::Open);

            let (long, short, name) = pick("book-trigger");
            let books = scripted(&long, &short);
            let ledger = temp_ledger(&name).await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let report = book_trigger_position(
                &executor,
                &mut held,
                Some(marks.clone()),
                &books,
                &ctx,
                dec!(11),
            )
            .await;
            assert!(
                report.executed && report.error.is_none(),
                "{:?} {:?}",
                report.skipped,
                report.error
            );
            assert_eq!(held.status, PositionStatus::Closed);
            assert_eq!(
                (long.asked(), short.asked()),
                (2, 2),
                "第一次 + 复核各拉一次"
            );
            let evaluation = report.evaluation.unwrap();
            assert_eq!(
                evaluation.observation.exit.as_ref().unwrap().long_price,
                dec!(101.9),
                "平仓依据是复核那份报价"
            );
            match &evaluation.action {
                Action::Close { reason } => {
                    assert!(reason.starts_with("盘口触发止盈"), "{reason}");
                    assert!(reason.contains("按标记价是"), "{reason}");
                }
                other => panic!("应当平仓：{other:?}"),
            }
        }

        /// 盘口触发止盈：第一次现拉盘口到止盈、复核时掉下去了 —— 不平；现拉盘口一开始就不到止盈
        /// （触发它的 WebSocket 盘口和 REST 对不上）—— 只拉一次、不平。
        #[tokio::test]
        async fn a_book_trigger_needs_both_fresh_looks_at_take_profit() {
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let marks = snapshots(dec!(101), dec!(101));
            for (script, asked, note) in [
                (
                    vec![(dec!(102), dec!(102.1)), (dec!(100.5), dec!(100.6))],
                    2,
                    "复核没过",
                ),
                (vec![(dec!(101.5), dec!(101.6))], 1, "不到止盈"),
            ] {
                let long = ScriptedBook::new(Venue::Lighter, script);
                let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
                let books = scripted(&long, &short);
                let ledger = temp_ledger("book-trigger-miss").await;
                let (executor, ..) = executor_over(
                    &ledger,
                    Stub::new(Venue::Lighter),
                    Stub::new(Venue::Hyperliquid),
                );
                let mut held = position(rules.clone());
                let retries = ExitRetries::default();
                let ctx = RoundCtx {
                    funding: &FixedFunding(Some(dec!(2))),
                    retries: &retries,
                    exit_confirm_gap: std::time::Duration::ZERO,
                };
                let report = book_trigger_position(
                    &executor,
                    &mut held,
                    Some(marks.clone()),
                    &books,
                    &ctx,
                    dec!(11),
                )
                .await;
                assert!(!report.executed, "{note}");
                assert_eq!(held.status, PositionStatus::Open);
                assert_eq!(long.asked(), asked, "{note}");
                let skipped = report.skipped.unwrap_or_default();
                assert!(skipped.contains(note), "{skipped}");
            }

            // 资金费没查到：不评估，也不去拉盘口。
            let long = ScriptedBook::new(Venue::Lighter, vec![(dec!(102), dec!(102.1))]);
            let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
            let books = scripted(&long, &short);
            let ledger = temp_ledger("book-trigger-no-funding").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(None),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = book_trigger_position(
                &executor,
                &mut held,
                Some(marks.clone()),
                &books,
                &ctx,
                dec!(11),
            )
            .await;
            assert!(!report.executed);
            assert_eq!(long.asked(), 0);
        }

        /// 盘口触发止盈只认台账里开着、两腿都在、设了止盈的仓位：没设止盈的、不在台账里的，什么都不做。
        #[tokio::test]
        async fn a_book_trigger_ignores_positions_without_a_take_profit() {
            let long = ScriptedBook::new(Venue::Lighter, vec![(dec!(102), dec!(102.1))]);
            let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
            let books = scripted(&long, &short);
            let ledger = temp_ledger("book-trigger-no-tp").await;
            ledger
                .append(&crate::Record::Position(Box::new(position(
                    TaskRules::default(),
                ))))
                .await
                .unwrap();
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            for id in ["p", "unknown"] {
                let done = book_trigger_exit(&executor, &ledger, &books, &ctx, id)
                    .await
                    .unwrap();
                assert!(done.is_none(), "{id}");
            }
            assert_eq!(long.asked(), 0);
        }

        /// 止盈盘口门槛 0（为正即平）：标记价到 11 触发；盘口净额只有几 USDT、远不到 11，但为正就平 ——
        /// 下单前的复核用的也是同一个门槛：复核时仍为正就平，掉到不为正就不平。
        #[tokio::test]
        async fn a_lowered_book_threshold_also_governs_the_second_look() {
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                take_profit_book_min_usdt: Some(Decimal::ZERO),
                ..TaskRules::default()
            };
            // 第一次：多腿买盘 101.2（+12）、空腿卖盘 101（−10），价格 +2，加资金费 2 = 4。
            // 复核 A：101.1 → +1 + 2 = 3，仍为正，平。复核 B：100.6 → −4 + 2 = −2，不平。
            for (second, taken) in [(dec!(101.1), true), (dec!(100.6), false)] {
                let long = ScriptedBook::new(
                    Venue::Lighter,
                    vec![(dec!(101.2), dec!(101.3)), (second, second + dec!(0.1))],
                );
                let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
                let books = scripted(&long, &short);
                let marks = snapshots(dec!(102), dec!(101));
                let ledger = temp_ledger("tp-book-min").await;
                let (executor, ..) = executor_over(
                    &ledger,
                    Stub::new(Venue::Lighter),
                    Stub::new(Venue::Hyperliquid),
                );
                let mut held = position(rules.clone());
                let retries = ExitRetries::default();
                let ctx = RoundCtx {
                    funding: &FixedFunding(Some(dec!(2))),
                    retries: &retries,
                    exit_confirm_gap: std::time::Duration::ZERO,
                };
                let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
                assert_eq!(long.asked(), 2, "第一次为正就去复核");
                assert_eq!(
                    report.executed, taken,
                    "复核买盘 {second}：{:?}",
                    report.evaluation
                );
                if taken {
                    assert_eq!(held.status, PositionStatus::Closed);
                } else {
                    assert_eq!(held.status, PositionStatus::Open);
                    let evaluation = report.evaluation.unwrap();
                    assert!(
                        evaluation
                            .skipped
                            .iter()
                            .any(|note| note.contains("平仓复核没过")),
                        "{:?}",
                        evaluation.skipped
                    );
                }
            }
        }

        /// 复核时两份盘口不是同一时刻到的（一边排队晚回来很久）：报价来路不明，不拿来下真单。
        #[tokio::test]
        async fn a_confirmation_with_books_from_different_moments_is_refused() {
            let long = ScriptedBook::new(Venue::Lighter, vec![(dec!(102), dec!(102.1))]);
            // 第二次（复核那次）这一边晚 1.7 秒才回。
            let short = ScriptedBook::slow_on(
                Venue::Hyperliquid,
                vec![(dec!(100.9), dec!(101))],
                1,
                std::time::Duration::from_millis(1700),
            );
            let books = scripted(&long, &short);
            let marks = snapshots(dec!(102), dec!(101));
            let rules = TaskRules {
                take_profit_usdt: Some(dec!(11)),
                ..TaskRules::default()
            };
            let ledger = temp_ledger("tp-skew").await;
            let (executor, ..) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(2))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(!report.executed);
            assert_eq!(held.status, PositionStatus::Open);
            let evaluation = report.evaluation.unwrap();
            assert!(
                evaluation
                    .skipped
                    .iter()
                    .any(|note| note.contains("不是同一时刻的报价")),
                "{:?}",
                evaluation.skipped
            );
        }

        /// 保护性退出（止损）不加复核：不是在等一个好价，复核不过就拖着，风险比滑点大。
        #[tokio::test]
        async fn a_stop_loss_exit_is_not_delayed_by_a_confirmation() {
            // 多腿 100→98（−20）、空腿 100→101（−10）：价格 −30，止损 20。
            let long = ScriptedBook::new(Venue::Lighter, vec![(dec!(98), dec!(98.1))]);
            let short = ScriptedBook::new(Venue::Hyperliquid, vec![(dec!(100.9), dec!(101))]);
            let books = scripted(&long, &short);
            let marks = snapshots(dec!(98), dec!(101));
            let rules = TaskRules {
                stop_loss_usdt: Some(dec!(20)),
                ..TaskRules::default()
            };
            let ledger = temp_ledger("stop-no-confirm").await;
            let (executor, lighter, hyperliquid) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid),
            );
            let mut held = position(rules);
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &FixedFunding(Some(dec!(0))),
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };
            let report = monitor_position(&executor, &mut held, &marks, &books, &ctx).await;
            assert!(
                report.executed && report.error.is_none(),
                "{:?}",
                report.error
            );
            assert_eq!(held.status, PositionStatus::Closed);
            assert_eq!(long.asked(), 1, "止损不复核，只拉一次盘口");
            // 保护性退出不加限价保护：两条腿同一轮按市价平。
            for broker in [&lighter, &hyperliquid] {
                let placed = broker.placed.lock().await;
                assert_eq!(placed.len(), 1);
                assert!(
                    placed
                        .values()
                        .all(|state| state.order.limit_price.is_none()
                            && state.order.client_order_id.0.ends_with("-exit1"))
                );
            }
        }

        /// 空腿被逼到强平线附近：补保证金成功，台账里累计补了多少、腿上的保证金都更新；
        /// 距离回到目标后下一轮保持，不重复补。
        #[tokio::test]
        async fn a_stressed_leg_is_topped_up_once_and_then_left_alone() {
            let ledger = temp_ledger("topup-ok").await;
            let (executor, _, short) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid).answering(Ok(crate::broker::MarginOutcome::Applied)),
            );
            let marks = snapshots(dec!(100), dec!(108));
            let mut held = position(auto_margin(dec!(12), dec!(500)));
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };

            let first = monitor_position(&executor, &mut held, &marks, &HashMap::new(), &ctx).await;
            assert!(first.executed && first.error.is_none(), "{:?}", first.error);
            assert_eq!(short.calls.load(Ordering::SeqCst), 1);
            assert!(
                held.margin_added_usdt > dec!(80) && held.margin_added_usdt < dec!(95),
                "{}",
                held.margin_added_usdt
            );
            assert_eq!(
                held.short.as_ref().unwrap().margin_usdt,
                Some(dec!(200) + held.margin_added_usdt)
            );
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(
                replayed.positions["p"].margin_added_usdt,
                held.margin_added_usdt
            );

            let second =
                monitor_position(&executor, &mut held, &marks, &HashMap::new(), &ctx).await;
            assert!(!second.executed, "距离已回到目标，不再补");
            assert_eq!(short.calls.load(Ordering::SeqCst), 1);
        }

        /// 结果不明：保证金读数没变，下一轮还想补 —— 但冷却中，绝不重发（没有幂等键，重发会补两次）。
        #[tokio::test]
        async fn an_unknown_top_up_is_never_resent_while_cooling_down() {
            let ledger = temp_ledger("topup-unknown").await;
            let (executor, _, short) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid)
                    .answering(Ok(crate::broker::MarginOutcome::Unknown("超时".into()))),
            );
            let marks = snapshots(dec!(100), dec!(108));
            let mut held = position(auto_margin(dec!(12), dec!(500)));
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };

            let first = monitor_position(&executor, &mut held, &marks, &HashMap::new(), &ctx).await;
            assert!(first.executed);
            assert!(first.attention.as_deref().unwrap().contains("结果不明"));
            let counted = held.margin_added_usdt;
            assert!(counted > Decimal::ZERO, "结果不明按已补计入上限");
            assert_eq!(
                held.short.as_ref().unwrap().margin_usdt,
                Some(dec!(200)),
                "没确认就不改腿上的保证金"
            );

            let second =
                monitor_position(&executor, &mut held, &marks, &HashMap::new(), &ctx).await;
            assert!(!second.executed);
            assert_eq!(
                short.calls.load(Ordering::SeqCst),
                1,
                "冷却中不能再发写请求"
            );
            assert_eq!(held.margin_added_usdt, counted);
            assert!(second.attention.as_deref().unwrap().contains("冷却"));
        }

        /// 补不了（被拒）就退回爆仓保护的减仓；记一次失败退避，没动钱所以不占上限。
        #[tokio::test]
        async fn a_refused_top_up_falls_back_to_the_trim() {
            let ledger = temp_ledger("topup-refused").await;
            let (executor, _, short) = executor_over(
                &ledger,
                Stub::new(Venue::Lighter),
                Stub::new(Venue::Hyperliquid).answering(Ok(crate::broker::MarginOutcome::Refused(
                    "可用资金不足".into(),
                ))),
            );
            // 距离约 6%：补保证金线（12）与爆仓保护线（8）都破了。
            let marks = snapshots(dec!(100), dec!(112));
            let mut held = position(TaskRules {
                liq_protection_pct: Some(dec!(8)),
                ..auto_margin(dec!(12), dec!(500))
            });
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };

            let report =
                monitor_position(&executor, &mut held, &marks, &HashMap::new(), &ctx).await;

            assert_eq!(short.calls.load(Ordering::SeqCst), 1);
            assert!(
                report.executed && report.error.is_none(),
                "{:?}",
                report.error
            );
            assert_eq!(held.trims, 1, "退回减仓");
            assert_eq!(
                held.margin_added_usdt,
                Decimal::ZERO,
                "被拒没动钱，不占上限"
            );
            assert!(report.attention.as_deref().unwrap().contains("没补成"));
            assert_eq!(retries.failures("p:margin"), 1);
        }

        /// 账户可用资金连 1 USDT 都补不出来：不打交易所，退回处理。
        #[tokio::test]
        async fn a_top_up_the_account_cannot_cover_never_reaches_the_venue() {
            let ledger = temp_ledger("topup-broke").await;
            let mut broke = Stub::new(Venue::Hyperliquid);
            broke.free = Some(dec!(0.5));
            let (executor, _, short) = executor_over(&ledger, Stub::new(Venue::Lighter), broke);
            let marks = snapshots(dec!(100), dec!(108));
            let mut held = position(auto_margin(dec!(12), dec!(500)));
            let retries = ExitRetries::default();
            let ctx = RoundCtx {
                funding: &PaperFunding,
                retries: &retries,
                exit_confirm_gap: std::time::Duration::ZERO,
            };

            let report =
                monitor_position(&executor, &mut held, &marks, &HashMap::new(), &ctx).await;

            assert_eq!(short.calls.load(Ordering::SeqCst), 0);
            assert!(!report.executed, "没设爆仓保护、补不了：只能告警");
            assert!(report.attention.as_deref().unwrap().contains("可用保证金"));
            assert!(report.skipped.as_deref().unwrap().contains("没设爆仓保护"));
        }

        /// 开仓后改规则：开仓时没勾的规则能加上；没变不写台账；不合规的规则、不在 Open 的仓位、
        /// 没接入补保证金的场所都被拒。
        #[tokio::test]
        async fn rules_can_be_changed_after_opening_with_the_same_validation() {
            let ledger = temp_ledger("update-rules").await;
            ledger
                .append(&crate::ledger::Record::Position(Box::new(position(
                    TaskRules::default(),
                ))))
                .await
                .unwrap();
            let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Stub::new(Venue::Lighter)) as Arc<dyn Broker>,
                ),
                (
                    Venue::Hyperliquid,
                    Arc::new(Stub::new(Venue::Hyperliquid)) as Arc<dyn Broker>,
                ),
            ]);
            let lines = || async {
                tokio::fs::read_to_string(ledger.path())
                    .await
                    .unwrap()
                    .lines()
                    .count()
            };
            let before = lines().await;
            let distance = Some(dec!(18.81));

            let wanted = TaskRules {
                take_profit_usdt: Some(dec!(2)),
                liq_protection_pct: Some(dec!(8)),
                ..auto_margin(dec!(12), dec!(200))
            };
            let (updated, changed) = update_rules(&ledger, &brokers, "p", wanted.clone(), distance)
                .await
                .unwrap();
            assert!(changed);
            assert_eq!(updated.rules, wanted);
            let (replayed, _) = ledger.replay().await.unwrap();
            assert_eq!(replayed.positions["p"].rules, wanted, "重放台账读到新规则");
            assert_eq!(lines().await, before + 1);

            // 同样的规则再提交一次：不写台账。
            let (_, changed) = update_rules(&ledger, &brokers, "p", wanted.clone(), distance)
                .await
                .unwrap();
            assert!(!changed);
            assert_eq!(lines().await, before + 1);

            // 与开仓时同一份校验：没上限的自动加保证金、一生效就触发的门槛。
            let no_cap = TaskRules {
                auto_margin_pct: Some(dec!(12)),
                ..TaskRules::default()
            };
            let error = update_rules(&ledger, &brokers, "p", no_cap, distance)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("上限"), "{error}");
            let fires_now = TaskRules {
                liq_protection_pct: Some(dec!(25)),
                ..TaskRules::default()
            };
            let (armed, _) = update_rules(&ledger, &brokers, "p", fires_now.clone(), distance)
                .await
                .unwrap();
            assert_eq!(armed.rules, fires_now, "已开仓规则的立即触发确认由入口负责");
            // 空规则 = 全部关掉，合法。
            let (cleared, changed) =
                update_rules(&ledger, &brokers, "p", TaskRules::default(), distance)
                    .await
                    .unwrap();
            assert!(changed && cleared.rules.is_empty());

            // 场所没接入补保证金：不能开。
            let mut no_support = Stub::new(Venue::Hyperliquid);
            no_support.supports_margin = false;
            let partial: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
                (
                    Venue::Lighter,
                    Arc::new(Stub::new(Venue::Lighter)) as Arc<dyn Broker>,
                ),
                (Venue::Hyperliquid, Arc::new(no_support) as Arc<dyn Broker>),
            ]);
            let error = update_rules(
                &ledger,
                &partial,
                "p",
                auto_margin(dec!(12), dec!(100)),
                distance,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("没有接入补保证金"), "{error}");
            assert!(
                update_rules(&ledger, &brokers, "nope", TaskRules::default(), distance)
                    .await
                    .is_err()
            );

            // 正在平仓的仓位不能改。
            let mut closing = position(TaskRules::default());
            closing.id = "c".into();
            closing.status = PositionStatus::Closing;
            ledger
                .append(&crate::ledger::Record::Position(Box::new(closing)))
                .await
                .unwrap();
            assert!(
                update_rules(&ledger, &brokers, "c", take_profit_rules(), distance)
                    .await
                    .is_err()
            );
        }

        fn take_profit_rules() -> TaskRules {
            TaskRules {
                take_profit_usdt: Some(dec!(1)),
                ..TaskRules::default()
            }
        }
    }
}
