//! `arb-paper` —— 端到端的纸面交易：扫描 → 深度体检 → 闸门 → 双腿执行 → 对账，
//! 以及持仓期间的任务规则监控。
//!
//! ```text
//! arb-paper                      # 资金费视角，取榜首一条，默认 5000 USDT、3 倍
//! arb-paper --view spread --size 2000 --top 3
//! arb-paper --symbols BTC,ETH
//! arb-paper BTC --leverage 5 --min-funding-apr 5 --liq-protection 10 --size-mismatch 1
//! arb-paper TSLA --long lighter --short hyperliquid-xyz   # 指定两条腿的场所
//! arb-paper --watch              # 不开新仓，按台账里每笔仓位的规则持续监控
//! arb-paper --watch --once       # 只评估一轮
//! ```
//!
//! 这个命令把整条链路跑通，但**不会碰任何真实资金**：券商是 [`arb_exec::PaperBroker`]，
//! 它用各家的真实盘口逐档吃单算成交价与手续费。真实下单见 `arb-live`。

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arb_core::{Decimal, MarketSnapshot, OrderBook, Settings, Venue, logging, money::to_pct};
use arb_exec::broker::Broker;
use arb_exec::cli::{
    describe, describe_rules, fetch_books, fetch_snapshots, limits_from, local_open_orders,
    monitor_one, parse_venue, pick, print_risk, risk_of, side_label,
};
use arb_exec::desk::{self, spread_edge, spread_opportunity};
use arb_exec::{
    Executor, Ledger, PairPosition, PositionSetup, PositionStatus, Preflight, Strategy, TaskRules,
    plan, reconcile,
};
use arb_scanner::Opportunity;
use arb_scanner::{filter_by_base, scan};
use arb_venues::{VenueApi, build_all, build_client};
use clap::Parser;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "arb-paper",
    version,
    about = "纸面交易：扫描 → 体检 → 双腿执行 → 持仓监控"
)]
struct Cli {
    /// 只扫描这些 base（如 BTC ETH SOL）。
    symbols: Vec<String>,

    /// 看哪条策略的榜。
    #[arg(long, default_value = "funding")]
    view: String,

    /// 单腿名义额（USDT）。
    #[arg(long, default_value = "5000")]
    size: Decimal,

    /// 取榜单前几条来尝试。
    #[arg(long, default_value_t = 1)]
    top: usize,

    /// 只考虑做多腿在这个场所的配对。
    #[arg(long, value_parser = parse_venue)]
    long: Option<Venue>,

    /// 只考虑做空腿在这个场所的配对。
    #[arg(long, value_parser = parse_venue)]
    short: Option<Venue>,

    /// 台账文件。默认 `./arb-ledger.jsonl`。
    #[arg(long, default_value = "arb-ledger.jsonl")]
    ledger: String,

    /// 盘口请求多少档。
    #[arg(long, default_value_t = 20)]
    depth_levels: u32,

    /// 保证金模式。纸面全仓不模拟账户权益/强平距离。
    #[arg(long, default_value = "isolated")]
    margin_mode: arb_exec::MarginMode,

    /// 两腿的杠杆。默认取 `ARB_LEVERAGE`；超过任一腿的场所上限时压到上限。
    #[arg(long)]
    leverage: Option<Decimal>,

    /// 费差自动平仓：当前毛费差年化（%）跌破它就整笔平仓。
    #[arg(long)]
    min_funding_apr: Option<Decimal>,

    /// 爆仓保护：任一腿强平距离（%）低于它就两腿等比例减仓，拉回 1.5 倍。
    #[arg(long)]
    liq_protection: Option<Decimal>,

    /// 数量失衡自动平仓：两腿数量偏差（%）超过它就整笔平仓（0.5 ~ 100）。
    #[arg(long)]
    size_mismatch: Option<Decimal>,

    /// 基差收敛平仓（价差套利）：当前标记价基差（%）收敛到它以内就整笔平仓，兑现价差。
    #[arg(long, allow_hyphen_values = true)]
    basis_exit: Option<Decimal>,

    /// 止盈：含资金费的净盈利（USDT）达到它就整笔平仓（先按标记价估算，再按盘口核对）。
    /// 纸面不结算资金费，按 0 计。
    #[arg(long)]
    take_profit: Option<Decimal>,

    /// 自动加保证金：任一腿强平距离（%）低于它就往这条腿补保证金，拉回 1.5 倍。必须同时给
    /// `--auto-margin-max`。纸面只在台账里记补了多少。
    #[arg(long, requires = "auto_margin_max")]
    auto_margin: Option<Decimal>,

    /// 自动加保证金的累计上限（USDT）。
    #[arg(long, requires = "auto_margin")]
    auto_margin_max: Option<Decimal>,

    /// 监控模式：不开新仓，按台账里每笔仓位的规则评估并执行。
    #[arg(long)]
    watch: bool,

    /// 监控模式只跑一轮。
    #[arg(long, requires = "watch")]
    once: bool,

    /// 监控间隔（秒）。默认取 `ARB_SCAN_INTERVAL_SEC`。
    #[arg(long)]
    interval: Option<u64>,
}

impl Cli {
    fn rules(&self) -> TaskRules {
        TaskRules {
            min_funding_apr: self.min_funding_apr.map(|pct| pct / Decimal::ONE_HUNDRED),
            liq_protection_pct: self.liq_protection,
            size_mismatch_pct: self.size_mismatch,
            basis_exit_pct: self.basis_exit,
            take_profit_usdt: self.take_profit,
            auto_margin_pct: self.auto_margin,
            auto_margin_max_usdt: self.auto_margin_max,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut settings = Settings::from_env()?;
    if let Some(leverage) = cli.leverage {
        settings.leverage = leverage;
    }
    settings.validate()?;
    logging::init(&settings.log_filter);

    let client = build_client(settings.http_timeout_sec)?;
    let apis = build_all(&settings, &client);
    let by_venue: HashMap<Venue, Arc<dyn VenueApi>> = apis
        .iter()
        .map(|api| (api.venue(), Arc::clone(api)))
        .collect();

    let ledger = Arc::new(Ledger::open(&cli.ledger).await?);
    let (replayed, broken) = ledger.replay().await?;
    if broken > 0 {
        warn!(broken, "台账里有无法解析的行（已跳过）");
    }
    info!(
        ledger = %ledger.path().display(),
        open_positions = replayed.exposed().len(),
        "台账已加载"
    );

    if cli.watch {
        return watch(&cli, &settings, &by_venue, &ledger).await;
    }
    let existing: Vec<PairPosition> = replayed.exposed().into_iter().cloned().collect();
    open(&cli, &settings, &apis, &by_venue, &ledger, &existing).await
}

async fn open(
    cli: &Cli,
    settings: &Settings,
    apis: &[Arc<dyn VenueApi>],
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ledger: &Arc<Ledger>,
    existing: &[PairPosition],
) -> Result<()> {
    let mut report = scan(apis, settings).await;
    filter_by_base(&mut report, &cli.symbols);

    let candidates = pick(&report, &cli.view, cli.top, cli.long, cli.short);
    // 价差视角：每个候选都按现拉盘口重新核算（与看板同一条路，CEX 也一样）。明确给了
    // 两腿与一个合约时，不在价差榜上的 DEX 配对（批量接口不给买一卖一）也能做。
    let spread = cli.view == "spread";
    let mut owned: Vec<(Opportunity, Option<(OrderBook, OrderBook)>)> = Vec::new();
    if spread {
        let mut targets: Vec<(String, Venue, Venue)> = candidates
            .iter()
            .map(|op| (op.symbol.base.clone(), op.long, op.short))
            .collect();
        if targets.is_empty()
            && let (Some(long), Some(short), [base]) = (cli.long, cli.short, cli.symbols.as_slice())
        {
            targets.push((base.clone(), long, short));
        }
        for (base, long, short) in targets {
            match spread_opportunity(
                &report,
                by_venue,
                &base,
                None,
                long,
                short,
                cli.depth_levels,
            )
            .await
            {
                Ok((op, long_book, short_book)) => owned.push((op, Some((long_book, short_book)))),
                Err(error) => {
                    println!("\n=== 候选：{base} {long} → {short}（spread）\n  跳过：{error:#}")
                }
            }
        }
    } else {
        owned = candidates
            .into_iter()
            .map(|op| (op.clone(), None))
            .collect();
    }
    if owned.is_empty() {
        println!("这个视角下没有候选机会。");
        return Ok(());
    }

    let limits = limits_from(settings);
    let rules = cli.rules();
    println!("保证金模式：{}", cli.margin_mode);
    if let Some(warning) = arb_exec::margin::warning(cli.margin_mode) {
        println!("  ⚠ {warning}");
    }
    let mut open_positions = existing.len();
    // 费率表来自本轮扫描取到的**真实**吃单费率（逐场所、逐合约）。
    let snapshots: Vec<MarketSnapshot> = report
        .symbols
        .iter()
        .flat_map(|view| view.rates.iter().cloned())
        .collect();
    let (executor, broker_map) = paper_executor(
        by_venue,
        settings,
        cli.depth_levels,
        ledger,
        &snapshots,
        existing,
    );

    for (opportunity, books) in owned {
        let opportunity = &opportunity;
        println!(
            "\n=== 候选：{} {} → {}（{}）",
            opportunity.symbol, opportunity.long, opportunity.short, cli.view
        );

        // 两腿同杠杆：取请求值与两腿场所上限的较小者。
        let leverage = risk_of(&report, opportunity, settings.leverage)
            .and_then(|risk| risk.max_pair_leverage)
            .map_or(settings.leverage, |max| settings.leverage.min(max));
        if leverage < settings.leverage {
            println!(
                "  杠杆：请求 {}x 超过两腿共同上限，按 {}x 开仓",
                settings.leverage.round_dp(2),
                leverage.round_dp(2)
            );
        }
        let mut risk = risk_of(&report, opportunity, leverage);
        if let Some(risk) = &mut risk {
            arb_exec::margin::display_risk(cli.margin_mode, risk);
        }
        if let Some(risk) = &risk {
            print_risk(risk);
        }
        if let Err(reason) = arb_exec::margin::validate_open_rules(
            cli.margin_mode,
            &rules,
            risk.as_ref().and_then(|risk| risk.liq_distance_pct),
        ) {
            println!("  规则不成立，跳过：{reason}");
            continue;
        }

        // 深度体检：整轮排名只用批量端点给的一档价，真正下单前必须看多档。
        let (long_book, short_book) = match books {
            Some(books) => books,
            None => match fetch_books(by_venue, opportunity, cli.depth_levels).await {
                Ok(books) => books,
                Err(error) => {
                    println!("  深度不可用，跳过：{error}");
                    continue;
                }
            },
        };
        println!(
            "  盘口：{} 买一 {} / 卖一 {}；{} 买一 {} / 卖一 {}",
            opportunity.long,
            long_book.best_bid().map_or("—".into(), |v| v.to_string()),
            long_book.best_ask().map_or("—".into(), |v| v.to_string()),
            opportunity.short,
            short_book.best_bid().map_or("—".into(), |v| v.to_string()),
            short_book.best_ask().map_or("—".into(), |v| v.to_string()),
        );

        let ctx = Preflight {
            opportunity,
            size_usdt: cli.size,
            long_book: &long_book,
            short_book: &short_book,
            open_positions,
            daily_pnl: Decimal::ZERO,
            risk: risk.as_ref(),
        };
        let execution_plan = match plan(&ctx, &limits) {
            Ok(plan) => plan,
            Err(rejection) => {
                println!("  闸门拒绝：{}", describe(&rejection));
                continue;
            }
        };
        if spread {
            match spread_edge(opportunity, &execution_plan, cli.basis_exit) {
                Ok(edge) => match cli
                    .basis_exit
                    .and_then(|target| desk::spread_target_shortfall(edge.depth_net, target))
                {
                    Some(reason) => {
                        println!("  吃完深度后价差 {}%。{reason}，跳过", edge.depth_basis_pct);
                        continue;
                    }
                    None if cli.basis_exit.is_some() || edge.depth_net > Decimal::ZERO => println!(
                        "  价差：买一卖一 {}%，吃完深度 {}%，扣成本后净 {}%{}",
                        edge.top_basis_pct,
                        edge.depth_basis_pct,
                        to_pct(edge.depth_net).round_dp(4),
                        edge.target_net.map_or(String::new(), |net| format!(
                            "；收敛到目标 {}% 平仓时净 {}%",
                            cli.basis_exit.unwrap_or_default().normalize(),
                            to_pct(net).round_dp(4)
                        ))
                    ),
                    None => {
                        println!(
                            "  吃完深度后价差不够：{}%，扣成本后净 {}%，跳过",
                            edge.depth_basis_pct,
                            to_pct(edge.depth_net).round_dp(4)
                        );
                        continue;
                    }
                },
                Err(error) => {
                    println!("  价差核算失败，跳过：{error:#}");
                    continue;
                }
            }
        }

        // 资金费单：回看两腿最近的逐小时费率，费差不稳就跳过（与看板预览同一道检查）。
        if !spread {
            match arb_exec::cli::fetch_funding_histories(
                by_venue,
                (opportunity.long, &opportunity.symbol),
                (opportunity.short, &opportunity.symbol),
                arb_scanner::stability::FETCH_HOURS,
            )
            .await
            {
                Ok(Some((long_history, short_history))) => {
                    match desk::stability_gate(
                        &long_history,
                        &short_history,
                        chrono::Utc::now(),
                        limits.require_stable_funding,
                    ) {
                        Ok(Some(stability)) => println!(
                            "  费差稳定：近 24 小时均值年化 {}%，{}% 的小时为正",
                            to_pct(stability.mean_apr_24h).round_dp(1),
                            to_pct(stability.positive_share).round_dp(1)
                        ),
                        Ok(None) => {}
                        Err(error) => {
                            println!("  {error}，跳过");
                            continue;
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    println!("  资金费历史没拉到，跳过：{error:#}");
                    continue;
                }
            }
        }

        println!(
            "  计划：先做 {} {}（滑点 {}），再做 {} {}（滑点 {}）",
            execution_plan.first_leg().venue,
            side_label(execution_plan.first_leg().side),
            to_pct(execution_plan.first_leg().slippage).round_dp(4),
            execution_plan.second_leg().venue,
            side_label(execution_plan.second_leg().side),
            to_pct(execution_plan.second_leg().slippage).round_dp(4),
        );
        println!(
            "  预期成本（两腿相对中间价）：{}%",
            to_pct(execution_plan.expected_cost).round_dp(4)
        );
        println!(
            "  杠杆 {}x；规则：{}",
            leverage.round_dp(2),
            describe_rules(&rules)
        );

        let position_id = arb_exec::desk::new_position_id("paper");
        let strategy = if cli.view == "spread" {
            Strategy::Spread
        } else {
            Strategy::Funding
        };
        let basis = opportunity.entry_basis_pct.unwrap_or(Decimal::ZERO);
        let setup = PositionSetup {
            margin_mode: cli.margin_mode,
            leverage: Some(leverage),
            rules: rules.clone(),
            quoted_at: None,
        };
        let outcome = executor
            .open(&execution_plan, strategy, basis, &position_id, &setup)
            .await
            .context("执行失败")?;

        let position = outcome.position;
        println!(
            "  结果：{:?}{}",
            position.status,
            position
                .note
                .as_deref()
                .map(|note| format!("（{note}）"))
                .unwrap_or_default()
        );
        if position.is_hedged() {
            println!(
                "  建仓完成：两腿合计 {}，手续费 {}，入场基差 {}%",
                position.total_notional().round_dp(2),
                position.total_fee().round_dp(4),
                position.entry_basis_pct.round_dp(4)
            );
        } else if position.is_naked() {
            println!("  ⚠ 只有一条腿成交，仓位处于裸敞口状态");
        }
        if position.status.has_exposure() {
            open_positions += 1;
        }
    }

    // 对账：把本地台账与券商状态对齐。
    let (replayed, _) = ledger.replay().await?;
    let exposed: Vec<&PairPosition> = replayed.exposed();
    let local_orders = local_open_orders(&replayed);
    let reconciliation = reconcile(&exposed, &local_orders, &broker_map).await?;
    println!(
        "\n=== 对账：核对 {} 个仓位 / {} 个场所",
        reconciliation.checked_positions, reconciliation.checked_venues
    );
    if reconciliation.is_clean() {
        println!("  本地台账与券商状态一致。");
    } else {
        for divergence in &reconciliation.divergences {
            println!("  ⚠ [{:?}] {}", divergence.kind, divergence.detail);
        }
    }
    println!("\n台账：{}", ledger.path().display());
    Ok(())
}

/// 监控模式：每轮重放台账，只拉持仓涉及的场所，逐笔评估规则并执行。
async fn watch(
    cli: &Cli,
    settings: &Settings,
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    ledger: &Arc<Ledger>,
) -> Result<()> {
    let interval = Duration::from_secs(cli.interval.unwrap_or(settings.scan_interval_sec).max(1));
    loop {
        let (replayed, _) = ledger.replay().await?;
        let mut positions: Vec<PairPosition> = replayed
            .positions
            .values()
            .filter(|position| position.status == PositionStatus::Open && position.is_hedged())
            .cloned()
            .collect();
        positions.sort_by(|a, b| a.id.cmp(&b.id));

        println!(
            "\n=== {} 监控 {} 笔双腿持仓",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            positions.len()
        );
        if !positions.is_empty() {
            let venues: BTreeSet<Venue> = positions
                .iter()
                .flat_map(|position| [position.long.as_ref(), position.short.as_ref()])
                .flatten()
                .map(|leg| leg.venue)
                .collect();
            let snapshots = fetch_snapshots(by_venue, &venues).await;
            let flat: Vec<MarketSnapshot> = snapshots.values().cloned().collect();
            let (executor, _) = paper_executor(
                by_venue,
                settings,
                cli.depth_levels,
                ledger,
                &flat,
                &positions,
            );
            let retries = arb_exec::desk::ExitRetries::default();
            let ctx = arb_exec::desk::RoundCtx {
                funding: &arb_exec::desk::PaperFunding,
                retries: &retries,
            };
            for mut position in positions {
                monitor_one(&executor, &mut position, &snapshots, by_venue, &ctx).await;
            }
        }

        if cli.once {
            return Ok(());
        }
        tokio::time::sleep(interval).await;
    }
}

/// 纸面券商与执行器，见 [`arb_exec::desk::paper_executor`]。
fn paper_executor(
    by_venue: &HashMap<Venue, Arc<dyn VenueApi>>,
    settings: &Settings,
    depth_levels: u32,
    ledger: &Arc<Ledger>,
    snapshots: &[MarketSnapshot],
    positions: &[PairPosition],
) -> (Executor, HashMap<Venue, Arc<dyn Broker>>) {
    arb_exec::desk::paper_executor(
        by_venue,
        settings.fee_per_side,
        depth_levels,
        ledger,
        snapshots,
        positions,
    )
}
