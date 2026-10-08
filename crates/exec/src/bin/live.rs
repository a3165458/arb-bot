//! `arb-live` —— 真实双腿执行。场所由 `ARB_LIVE_VENUES`（或 `--venues`）指定，
//! 留空或 `auto` 时连接凭据齐全的每一家；每家的凭据见 README「实盘执行」。
//!
//! ```text
//! arb-live status                                   # 只读：连接各家账户，对账台账与真实持仓
//! arb-live open ETH --long hyperliquid --short lighter --size 500 --leverage 3 --daily-pnl 0
//!                                                   # 只读：出计划，不签名、不发单
//! arb-live --enable-trading --market-slippage 0.003 open ETH ...   # 真实下单
//! arb-live --enable-trading --market-slippage 0.003 close live-1789...
//! arb-live --enable-trading --market-slippage 0.003 watch [--once]
//! ```
//!
//! 与 `arb-paper`、看板共用同一份扫描、闸门、执行器与规则评估（[`arb_exec::desk`]），
//! 只把券商换成各家的私有接口。安全边界：
//!
//! - 不给 `--enable-trading` 时所有券商都拒绝一切写操作；这个命令任何路径都不签名写请求。
//! - 开启下单必须显式给 `--market-slippage`：平仓 / 回滚 / 减仓没有限价，
//!   没有它就会开出平不掉的仓位。
//! - 开仓前对账必须干净：台账之外的持仓或挂单都会拦下（请用专用账户）；台账里有仓位
//!   落在没连接的场所上时，对账不完整，同样拦下。
//! - 当日已实现盈亏必须由操作者给出（`--daily-pnl`）。各家的历史接口都有上限，
//!   这里不把「查不全」当成 0。
//! - 实盘台账与每家的订单意图日志是独立文件，与纸面台账分开；
//!   出现无法核实的订单时不要删日志重试，先对账。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arb_core::{Decimal, Settings, Venue, logging, money::to_pct};
use arb_exec::broker::Broker;
use arb_exec::cli::{describe_rules, parse_venue, print_monitor_report, print_risk, side_label};
use arb_exec::desk::{self, LeveragePolicy, OpenRequest};
use arb_exec::live_connect::{self, ConnectOptions};
use arb_exec::{Executor, Ledger, Reconciliation, TaskRules};
use arb_scanner::scan;
use arb_venues::{VenueApi, build_all, build_client};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "arb-live", version, about = "实盘：跨场所双腿执行（默认只读）")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// 实盘台账（仓位与订单）。与纸面台账分开。
    #[arg(long, global = true, default_value = "arb-live-ledger.jsonl")]
    ledger: PathBuf,

    /// 要连接的场所（逗号分隔）。默认取 `ARB_LIVE_VENUES`；留空或 `auto` 时按凭据自动识别。
    #[arg(long, global = true)]
    venues: Option<String>,

    /// 各场所订单意图日志所在目录：`<dir>/arb-live-<场所>-orders.jsonl`（独占锁，防重复提交）。
    #[arg(long, global = true, default_value = ".")]
    journal_dir: PathBuf,

    /// 真实下单开关。不给 = 只读：连接、对账、出计划，不签名、不发单。
    #[arg(long, global = true)]
    enable_trading: bool,

    /// 无限价单（平仓 / 回滚 / 减仓）的价格保护，小数（0.003 = 0.3%）。开启下单时必填。
    #[arg(long, global = true)]
    market_slippage: Option<Decimal>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// 对账：本地实盘台账 vs 各家账户的真实持仓与挂单。
    Status,
    /// 按指定两腿开一笔仓位。不带 `--enable-trading` 时只出计划。
    Open(Box<OpenArgs>),
    /// 平掉一笔仓位；也用于重试停在 Closing / Unwinding 的仓位。需要 `--enable-trading`。
    Close { position_id: String },
    /// 按台账里每笔仓位的规则监控并执行；重试未完成的退出。需要 `--enable-trading`。
    Watch {
        #[arg(long)]
        once: bool,
        /// 监控间隔（秒）。默认取 `ARB_SCAN_INTERVAL_SEC`。
        #[arg(long)]
        interval: Option<u64>,
    },
}

#[derive(clap::Args, Debug)]
struct OpenArgs {
    /// 合约 base，如 ETH。
    symbol: String,
    #[arg(long, value_parser = parse_venue)]
    long: Venue,
    #[arg(long, value_parser = parse_venue)]
    short: Venue,
    /// 单腿名义额（USDT）。
    #[arg(long)]
    size: Decimal,
    /// 保证金模式：isolated / cross。默认逐仓；账户/合约限制会明确拒绝，不静默切换。
    #[arg(long, default_value = "isolated")]
    margin_mode: arb_exec::MarginMode,
    /// 两腿杠杆（整数；Hyperliquid 只接受整数）。超过两腿共同上限直接拒绝。
    #[arg(long)]
    leverage: Decimal,
    /// 当日已实现盈亏（USDT，亏损为负）。必须由操作者给出，不回落为 0。
    #[arg(long, allow_hyphen_values = true)]
    daily_pnl: Decimal,
    #[arg(long, default_value = "funding")]
    view: String,
    #[arg(long, default_value_t = 20)]
    depth_levels: u32,
    #[arg(long)]
    min_funding_apr: Option<Decimal>,
    #[arg(long)]
    liq_protection: Option<Decimal>,
    #[arg(long)]
    size_mismatch: Option<Decimal>,
    /// 基差收敛平仓（价差套利）：当前标记价基差（%）收敛到它以内就整笔平仓。
    #[arg(long, allow_hyphen_values = true)]
    basis_exit: Option<Decimal>,
    /// 止盈：含资金费的净盈利（USDT）达到它就整笔平仓（先按标记价估算，再按盘口核对）。
    #[arg(long)]
    take_profit: Option<Decimal>,
    /// 自动加保证金：任一腿强平距离（%）低于它就往这条腿补保证金，拉回 1.5 倍。
    /// 必须同时给 `--auto-margin-max`。真实资金：补的是账户里的可用保证金。
    #[arg(long, requires = "auto_margin_max")]
    auto_margin: Option<Decimal>,
    /// 自动加保证金的累计上限（USDT，这笔仓位一生最多补这么多）。
    #[arg(long, requires = "auto_margin")]
    auto_margin_max: Option<Decimal>,
}

struct Live {
    settings: Settings,
    venues: Vec<Venue>,
    apis: Vec<Arc<dyn VenueApi>>,
    by_venue: HashMap<Venue, Arc<dyn VenueApi>>,
    brokers: HashMap<Venue, Arc<dyn Broker>>,
    ledger: Arc<Ledger>,
    retries: desk::ExitRetries,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut settings = Settings::from_env()?;
    let selection = live_connect::live_venues(cli.venues.as_deref())?;
    let venues = selection.venues;
    settings.venues = venues.clone();
    settings.validate()?;
    logging::init(&settings.log_filter);

    let options = ConnectOptions {
        journal_dir: cli.journal_dir.clone(),
        trading_enabled: cli.enable_trading,
        market_slippage: cli.market_slippage,
    };
    options
        .validate()
        .context("--enable-trading 必须同时给 --market-slippage")?;
    let requires_trading = matches!(cli.command, Command::Close { .. } | Command::Watch { .. });
    if requires_trading && !cli.enable_trading {
        bail!("close / watch 会下单，需要 --enable-trading 与 --market-slippage；只读请用 status");
    }

    let client = build_client(settings.http_timeout_sec)?;
    let apis = build_all(&settings, &client);
    let by_venue = apis
        .iter()
        .map(|api| (api.venue(), Arc::clone(api)))
        .collect();
    let brokers = live_connect::connect(&client, &venues, &options).await?;
    let ledger = Arc::new(Ledger::open(&cli.ledger).await?);
    // 上次进程若死在「意图已落盘、终态未落盘」之间，那张单永远停在 Pending，对账会报一处永远消不掉
    // 的不一致。先按券商的订单日志把它们核实成终态。
    match desk::resolve_pending_orders(&ledger, &brokers).await {
        Ok(resolution) if resolution.is_empty() => {}
        Ok(resolution) => {
            println!(
                "启动核实：补上终态 {} 张，仍在场上 {} 张，查不到结论 {} 张",
                resolution.resolved.len(),
                resolution.still_open.len(),
                resolution.unknown.len()
            );
            if !resolution.filled_unrecorded.is_empty() {
                println!(
                    "⚠ 这些订单在上次中断时已成交、台账没记：{}。请对照各场所账户人工处理",
                    resolution.filled_unrecorded.join("、")
                );
            }
        }
        Err(error) => println!("⚠ 启动核实台账订单失败（对账会如实报出）：{error:#}"),
    }
    println!(
        "实盘模式：{}；场所 {}{}；台账 {}",
        if cli.enable_trading {
            "允许下单"
        } else {
            "只读（不签名、不发单）"
        },
        venues
            .iter()
            .map(|venue| venue.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        if selection.auto {
            "（按凭据自动识别）"
        } else {
            ""
        },
        ledger.path().display()
    );
    let live = Live {
        settings,
        venues,
        apis,
        by_venue,
        brokers,
        ledger,
        retries: desk::ExitRetries::default(),
    };

    match cli.command {
        Command::Status => {
            let reconciliation = live.reconcile().await?;
            print_reconciliation(&reconciliation);
            print_venue_positions(&live.venues, &live.brokers).await;
            Ok(())
        }
        Command::Open(args) => {
            let OpenArgs {
                margin_mode,
                symbol,
                long,
                short,
                size,
                leverage,
                daily_pnl,
                view,
                depth_levels,
                min_funding_apr,
                liq_protection,
                size_mismatch,
                basis_exit,
                take_profit,
                auto_margin,
                auto_margin_max,
            } = *args;
            let request = OpenRequest {
                margin_mode,
                base: symbol,
                quote: None,
                long,
                short,
                size,
                leverage,
                daily_pnl,
                view,
                depth_levels,
                rules: TaskRules {
                    min_funding_apr: min_funding_apr.map(|pct| pct / Decimal::ONE_HUNDRED),
                    liq_protection_pct: liq_protection,
                    size_mismatch_pct: size_mismatch,
                    basis_exit_pct: basis_exit,
                    take_profit_usdt: take_profit,
                    auto_margin_pct: auto_margin,
                    auto_margin_max_usdt: auto_margin_max,
                },
            };
            live.open(&request, cli.enable_trading).await
        }
        Command::Close { position_id } => live.close(&position_id).await,
        Command::Watch { once, interval } => live.watch(once, interval).await,
    }
}

impl Live {
    fn executor(&self) -> Executor {
        Executor::new(
            Arc::clone(&self.ledger),
            self.brokers.values().cloned().collect(),
        )
    }

    async fn reconcile(&self) -> Result<Reconciliation> {
        desk::reconcile_ledger(&self.ledger, &self.brokers).await
    }

    /// 对账不干净时，看有没有仓位是在交易所被外部（手动）平掉的：连续两次对账（间隔 ≥ 60 秒）
    /// 两腿都已归零才在台账里结束它，然后重新对账。只识别、不下单；只平了一条腿的只提示。
    async fn adopt_external(
        &self,
        reconciliation: Reconciliation,
        seen: &mut HashMap<String, std::time::Instant>,
    ) -> Result<Reconciliation> {
        let (replayed, _) = self.ledger.replay().await?;
        let exposed = replayed.exposed();
        let check =
            desk::check_external_closes(&reconciliation, &exposed, seen, std::time::Instant::now());
        for note in &check.notes {
            println!("  ⚠ [{}] {}", note.position_id, note.message);
        }
        if check.adopt.is_empty() {
            return Ok(reconciliation);
        }
        for position in
            desk::adopt_external_closes(&self.ledger, &self.brokers, &exposed, &check.adopt).await?
        {
            println!(
                "  [{}] {}",
                position.id,
                position.note.as_deref().unwrap_or("已在台账里结束")
            );
        }
        self.reconcile().await
    }

    async fn open(&self, request: &OpenRequest, trading: bool) -> Result<()> {
        if request.long == request.short
            || !self.brokers.contains_key(&request.long)
            || !self.brokers.contains_key(&request.short)
        {
            bail!(
                "两腿必须是已连接的两个不同场所（当前连接：{}）",
                self.venue_list()
            );
        }

        let reconciliation = self.reconcile().await?;
        print_reconciliation(&reconciliation);
        if !reconciliation.is_clean() {
            bail!("对账不干净，拒绝开新仓");
        }
        let (replayed, _) = self.ledger.replay().await?;
        let open_positions = replayed.exposed().len();

        let report = scan(&self.apis, &self.settings).await;
        let prepared = desk::prepare(
            &report,
            &self.by_venue,
            request,
            open_positions,
            &arb_exec::cli::limits_from(&self.settings),
            LeveragePolicy::Strict,
        )
        .await?;
        // 两个账户撑不撑得住这笔的保证金（只读）。
        for warning in desk::verify_free_collateral(&self.brokers, &prepared).await? {
            println!("  ⚠ {warning}");
        }
        let opportunity = &prepared.opportunity;
        println!(
            "\n=== {} 多 {} / 空 {}（{}）",
            opportunity.symbol, opportunity.long, opportunity.short, request.view
        );
        println!("  保证金模式：{}", prepared.margin_mode);
        if let Some(warning) = arb_exec::margin::warning(prepared.margin_mode) {
            println!("  ⚠ {warning}");
        }
        print_risk(&prepared.risk);
        for leg in [prepared.plan.first_leg(), prepared.plan.second_leg()] {
            println!(
                "  {} {}：名义 {}，限价 {}，预估均价 {}，滑点 {}%",
                leg.venue,
                side_label(leg.side),
                leg.notional_usdt,
                leg.limit_price.round_dp(8),
                leg.expected_price,
                to_pct(leg.slippage).round_dp(4)
            );
        }
        println!(
            "  预期成本 {}%；杠杆 {}x；规则：{}",
            to_pct(prepared.plan.expected_cost).round_dp(4),
            prepared.leverage,
            describe_rules(&prepared.rules)
        );
        if !trading {
            println!(
                "\n只读模式：未签名、未发单。加 --enable-trading --market-slippage <小数> 才会真实下单。"
            );
            return Ok(());
        }

        let position_id = desk::new_position_id("live");
        let position = desk::execute(&self.executor(), &prepared, &position_id)
            .await
            .context("执行中断：以台账与对账结果为准，不要重跑同一条命令")?;
        println!(
            "\n结果 [{}]：{:?}{}",
            position.id,
            position.status,
            position
                .note
                .as_deref()
                .map(|note| format!("（{note}）"))
                .unwrap_or_default()
        );
        if position.is_naked() {
            println!(
                "⚠ 仍有单腿敞口：用 `arb-live --enable-trading ... close {}` 重试退出",
                position.id
            );
        }
        print_reconciliation(&self.reconcile().await?);
        Ok(())
    }

    async fn close(&self, position_id: &str) -> Result<()> {
        let (position, error) = desk::close(&self.executor(), &self.ledger, position_id).await?;
        println!(
            "[{}] {:?}{}",
            position.id,
            position.status,
            position
                .note
                .as_deref()
                .map(|note| format!("（{note}）"))
                .unwrap_or_default()
        );
        print_reconciliation(&self.reconcile().await?);
        match error {
            Some(error) => bail!(error),
            None => Ok(()),
        }
    }

    async fn watch(&self, once: bool, interval: Option<u64>) -> Result<()> {
        let interval =
            Duration::from_secs(interval.unwrap_or(self.settings.scan_interval_sec).max(1));
        // 「交易所里已经没有这笔仓位」的候选第一次被发现的时刻（连续两次对账才结束）。
        let mut seen = HashMap::new();
        loop {
            println!(
                "\n=== {}",
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
            );
            // 对账不干净时本轮不做任何自动动作：规则是按台账评估的，台账与账户对不上时
            // 自动平仓 / 减仓可能作用在错误的数量上。
            let reconciled = match self.reconcile().await {
                Ok(reconciliation) if !reconciliation.is_clean() => {
                    self.adopt_external(reconciliation, &mut seen).await
                }
                other => other,
            };
            match reconciled {
                Ok(reconciliation) if reconciliation.is_clean() => self.watch_round(true).await?,
                Ok(reconciliation) => {
                    print_reconciliation(&reconciliation);
                    println!(
                        "  ⚠ 对账不干净：不执行规则，只重试没走完的退出（Closing / Unwinding）"
                    );
                    self.watch_round(false).await?;
                }
                Err(error) => println!("  ⚠ 对账失败，本轮跳过自动动作：{error:#}"),
            }
            if once {
                return Ok(());
            }
            tokio::time::sleep(interval).await;
        }
    }

    async fn watch_round(&self, rules: bool) -> Result<()> {
        let executor = self.executor();
        let ctx = desk::RoundCtx {
            // 命令行直接问各券商的结算流水（不缓存）。
            funding: &executor,
            retries: &self.retries,
        };
        let reports =
            desk::live_round(&executor, &self.ledger, &self.by_venue, &ctx, rules).await?;
        println!("  监控 {} 笔有敞口的仓位", reports.len());
        for report in &reports {
            if report.retried_exit {
                println!(
                    "\n  [{}] 上一次执行没走完，重试退出剩余敞口",
                    report.position_id
                );
                match &report.error {
                    None => println!("    结果：{:?}", report.status),
                    Some(error) => println!("    ⚠ 退出未完成：{error}"),
                }
                continue;
            }
            println!("\n  [{}] {}", report.position_id, report.symbol);
            print_monitor_report(report);
        }
        Ok(())
    }

    fn venue_list(&self) -> String {
        self.venues
            .iter()
            .map(|venue| venue.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn print_reconciliation(reconciliation: &Reconciliation) {
    println!(
        "对账：{} 个仓位 / {} 个场所",
        reconciliation.checked_positions, reconciliation.checked_venues
    );
    if reconciliation.is_clean() {
        println!("  本地台账与各家账户一致。");
    }
    for divergence in &reconciliation.divergences {
        println!("  ⚠ [{:?}] {}", divergence.kind, divergence.detail);
    }
}

async fn print_venue_positions(venues: &[Venue], brokers: &HashMap<Venue, Arc<dyn Broker>>) {
    for &venue in venues {
        let Some(broker) = brokers.get(&venue) else {
            continue;
        };
        match broker.positions().await {
            Ok(positions) if positions.is_empty() => println!("{venue}：无持仓"),
            Ok(positions) => {
                for position in positions {
                    println!(
                        "{venue}：{} 净数量 {}，均价 {}，名义 {}",
                        position.symbol,
                        position.net_quantity,
                        position
                            .average_price
                            .map_or("—".into(), |price| price.to_string()),
                        position.notional_usdt.round_dp(2)
                    );
                }
            }
            Err(error) => println!("{venue}：持仓查询失败 {error}"),
        }
        println!("{venue}：账户吃单费率 {}", broker.fee_per_side());
    }
}
