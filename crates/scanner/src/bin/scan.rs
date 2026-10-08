//! `arb-scan` —— 跨场所资金费套利扫描器。
//!
//! ```text
//! arb-scan                       # 扫描全部合约，打印净年化最高的 20 条机会
//! arb-scan BTC ETH SOL           # 只看这几个 base
//! arb-scan --json | jq .totals   # 机器可读（日志走 stderr，stdout 只有 JSON）
//! arb-scan --fee 0.0002 --amortize-days 14
//! arb-scan --leverage 5          # 强平距离与保证金年化按 5 倍算（不影响排名）
//! ```

use anyhow::Result;
use arb_core::{Decimal, Settings, logging, money::to_pct};
use arb_scanner::{Opportunity, ScanReport, SymbolView, filter_by_base, pair_risk, scan};
use arb_venues::{build_all, build_client};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "arb-scan", version, about = "跨场所资金费套利扫描器")]
struct Cli {
    /// 只扫描这些 base（如 BTC ETH SOL）；不给则扫描全部合约。
    symbols: Vec<String>,

    /// 输出机器可读的 JSON。
    #[arg(long)]
    json: bool,

    /// 文本模式下打印的机会条数。
    #[arg(long, default_value_t = 20)]
    top: usize,

    /// 单边手续费率的回落值（覆盖 ARB_FEE_PER_SIDE）。
    #[arg(long)]
    fee: Option<Decimal>,

    /// 计划持有天数（覆盖 ARB_AMORTIZE_DAYS）。
    #[arg(long)]
    amortize_days: Option<Decimal>,

    /// 逗号分隔的场所白名单（覆盖 ARB_VENUES）。
    #[arg(long)]
    venues: Option<String>,

    /// 不利入场基差的上限（%），`off` 关闭（覆盖 ARB_MAX_ENTRY_BASIS_PCT）。
    #[arg(long)]
    max_entry_basis_pct: Option<String>,

    /// 看哪条策略的榜：`funding`（资金费套利，默认）或 `spread`（跨所价差套利）。
    #[arg(long, default_value = "funding")]
    view: String,

    /// 价差套利的计划持有天数（覆盖 ARB_SPREAD_HOLD_DAYS）。
    #[arg(long)]
    spread_hold_days: Option<Decimal>,

    /// 对价差榜前 N 条候选**实测**基差半衰期，并用它替代计划持有天数。
    /// 每条候选要拉两腿的 K 线（逐合约请求），所以只对少数候选做。
    #[arg(long)]
    measure_convergence: Option<usize>,

    /// 实测用的 K 线周期（分钟）。向上取到各场所支持的最近合法值。
    #[arg(long, default_value_t = 60)]
    candle_interval: u32,

    /// 实测用的 K 线条数。
    #[arg(long, default_value_t = 500)]
    candle_limit: u32,

    /// 两腿各自的逐仓杠杆（覆盖 ARB_LEVERAGE）。只影响强平距离与保证金年化列。
    #[arg(long)]
    leverage: Option<Decimal>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let settings = resolve_settings(&cli)?;
    logging::init(&settings.log_filter);

    let client = build_client(settings.http_timeout_sec)?;
    let apis = build_all(&settings, &client);

    let view = match cli.view.as_str() {
        "funding" => View::Funding,
        "spread" => View::Spread,
        other => anyhow::bail!("--view 只能是 funding 或 spread，收到 {other:?}"),
    };

    let mut report = scan(&apis, &settings).await;
    filter_by_base(&mut report, &cli.symbols);

    // 实测基差半衰期：替掉「计划持有 N 天」这个拍脑袋的参数。
    if let Some(top) = cli.measure_convergence {
        let api_map: std::collections::HashMap<_, _> = apis
            .iter()
            .map(|api| (api.venue(), std::sync::Arc::clone(api)))
            .collect();
        let holds = arb_scanner::convergence::measure_holds(
            &api_map,
            &report,
            top,
            cli.candle_interval,
            cli.candle_limit,
        )
        .await;
        if holds.is_empty() {
            println!(
                "\n实测收敛时间：两腿都有 K 线的前 {top} 条里没有拟合出半衰期，排名仍按配置的持有期。"
            );
        } else {
            println!(
                "\n实测收敛时间：{} 条配对拿到了半衰期（跳过没有公开 K 线的场所后，取前 {top} 条）",
                holds.len()
            );
            let mut rows: Vec<_> = holds.iter().collect();
            rows.sort_by_key(|(_, half_life)| half_life.days);
            for ((symbol, long, short), half_life) in rows.iter().take(8) {
                println!(
                    "  {:<14} {}→{} 半衰期 {:>7} 天（β={} R²={} n={}）",
                    symbol.to_string(),
                    long.as_str(),
                    short.as_str(),
                    half_life.days,
                    half_life.beta,
                    half_life.r_squared,
                    half_life.samples
                );
            }
        }

        let short_half_lives = holds
            .values()
            .filter(|half_life| half_life.days < Decimal::ONE)
            .count();
        if short_half_lives > 0 {
            println!(
                "  注意：{short_half_lives} 条配对的半衰期不足 1 天。年化会把「几小时收敛一次」\n  \
                 外推成「一年重复几千次」，那个数字只能当**相对**信号看；\n  \
                 单笔实际赚到的是「入场基差 − 一次性成本」那一列，与持有期无关。"
            );
        }

        let mut config = arb_scanner::scan::rank_config(&settings);
        config.measured_hold = holds
            .iter()
            .map(|((symbol, long, short), half_life)| {
                ((symbol.clone(), *long, *short), half_life.days)
            })
            .collect();
        arb_scanner::rerank(&mut report, &config);
    }

    if cli.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        print_text(&report, cli.top, view, settings.leverage);
    }
    Ok(())
}

/// 环境变量给出默认值，命令行参数覆盖，最后统一校验。
///
/// 校验放在覆盖之后：否则 `--fee 0.5` 会绕过 `from_env` 的范围检查，
/// 让一个荒谬的费率活到排名里。
fn resolve_settings(cli: &Cli) -> Result<Settings> {
    let mut settings = Settings::from_env()?;
    if let Some(fee) = cli.fee {
        settings.fee_per_side = fee;
    }
    if let Some(days) = cli.amortize_days {
        settings.amortize_days = days;
    }
    if let Some(days) = cli.spread_hold_days {
        settings.spread_hold_days = days;
    }
    if let Some(leverage) = cli.leverage {
        settings.leverage = leverage;
    }
    if let Some(list) = cli.venues.as_deref() {
        settings.venues = arb_core::config::parse_venue_list(list)?;
    }
    if let Some(raw) = cli.max_entry_basis_pct.as_deref() {
        settings.max_entry_basis_pct = match raw.trim() {
            "off" | "none" | "-" | "" => None,
            value => Some(
                value
                    .parse::<Decimal>()
                    .map_err(|_| anyhow::anyhow!("--max-entry-basis-pct 必须是十进制数或 off"))?,
            ),
        };
    }
    settings.validate()?;
    Ok(settings)
}

/// 看哪条策略的榜。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Funding,
    Spread,
}

impl View {
    fn label(self) -> &'static str {
        match self {
            View::Funding => "资金费套利",
            View::Spread => "跨所价差套利",
        }
    }
}

fn print_text(report: &ScanReport, top: usize, view: View, leverage: Decimal) {
    println!(
        "=== arb-bot · {} · {} ===",
        view.label(),
        report.generated_at.format("%Y-%m-%d %H:%M:%SZ")
    );
    println!(
        "场所 {} / {} 应答    合约 {}    读数 {}    资金费配对 {}    价差配对 {}",
        report.totals.venues_ok,
        report.totals.venues_ok + report.totals.venues_failed,
        report.totals.symbols,
        report.totals.rates,
        report.totals.profitable_pairs,
        report.totals.spread_pairs,
    );
    match view {
        View::Funding => println!(
            "口径：一次往返的成本（手续费 + 穿价）按 {} 天摊销；假设基差保持不变；\
             单边费率回落值 {}；入场基差门槛 {}",
            report.amortize_days,
            report.fee_per_side,
            report
                .max_entry_basis_pct
                .map_or("关闭".to_string(), |value| format!(
                    "不利不超过 −{value}%"
                )),
        ),
        View::Spread => println!(
            "口径：一次往返的成本（手续费 + 穿价）按 {} 天摊销；假设基差收敛到 0；\
             单边费率回落值 {}",
            report.spread_hold_days, report.fee_per_side,
        ),
    }

    let failed: Vec<&arb_scanner::VenueReport> = report.venues.iter().filter(|v| !v.ok).collect();
    if !failed.is_empty() {
        println!("\n取数失败的场所（这些**没有**参与排名）：");
        for venue in failed {
            println!(
                "  - {:<12} {}",
                venue.venue.as_str(),
                venue.error.as_deref().unwrap_or("未知原因")
            );
        }
    }

    let mut rows: Vec<(&SymbolView, &Opportunity)> = report
        .symbols
        .iter()
        .flat_map(|symbol_view| {
            let list = match view {
                View::Funding => &symbol_view.funding,
                View::Spread => &symbol_view.spread,
            };
            list.iter().map(move |op| (symbol_view, op))
        })
        .collect();
    // 稳定排序：`symbols` 已经按资金费视角排好，所以数值相同的两条机会仍按合约名
    // 保持顺序，两次运行的输出一致。
    rows.sort_by_key(|row| {
        std::cmp::Reverse(match view {
            View::Funding => row.1.funding_apr,
            View::Spread => row.1.spread_net,
        })
    });

    println!(
        "\nTop {} 机会（强平距离与保证金年化按 {}x 逐仓）",
        top.min(rows.len()),
        leverage
    );
    println!(
        "  {:<4} {:<12} {:<15} {:<15} {:>9} {:>9} {:>10} {:>10} {:>9} {:>10} {:>10} {:>12} {:>10}",
        "#",
        "合约",
        "做多",
        "做空",
        "日化%",
        "APR%",
        "往返费%",
        "穿价%",
        "基差%",
        "资金费APR%",
        "价差净%",
        "强平距离%",
        "保证金APR%"
    );
    for (idx, (symbol_view, op)) in rows.iter().take(top).enumerate() {
        let risk = leg(symbol_view, op.long)
            .zip(leg(symbol_view, op.short))
            .map(|(long, short)| pair_risk(op, long, short, leverage));
        // 任一腿缺维持保证金率就是未知：只看算得出的那条会把风险看轻。
        let distance = risk
            .as_ref()
            .and_then(|risk| risk.liq_distance_pct.zip(risk.health))
            .map_or("未知".to_string(), |(pct, health)| {
                format!("{:.1} {}", pct, health_label(health))
            });
        let capped = if op.oi_capped { " [OI上限]" } else { "" };
        println!(
            "  {:<4} {:<12} {:<15} {:<15} {:>9} {:>9} {:>10} {:>10} {:>9} {:>10} {:>10} {:>12} {:>10}{capped}",
            idx + 1,
            format!("{}/{}", symbol_view.symbol.base, op.symbol.quote),
            op.long.as_str(),
            op.short.as_str(),
            pct(op.daily_spread, 4),
            pct(op.apr, 2),
            pct(op.round_trip_fee, 4),
            // 穿价未知时不能显示 0 —— 那等于宣称「没有价差成本」
            op.round_trip_spread
                .map_or("未知".to_string(), |value| pct(value, 4)),
            // 价差榜显示能成交的价差；资金费榜显示标记价基差。都已经是百分数。
            (if view == View::Spread {
                op.executable_basis_pct
            } else {
                op.entry_basis_pct
            })
            .map_or("—".to_string(), |value| format!(
                "{:+}",
                value.round_dp(3)
            )),
            pct(op.funding_apr, 2),
            pct(op.spread_net, 3),
            distance,
            risk.map_or("—".to_string(), |risk| pct(risk.margin_apr, 1)),
        );
    }
    if rows.is_empty() {
        println!("  （这个视角下没有可排的配对）");
    }

    let capped = rows.iter().take(top).filter(|(_, op)| op.oi_capped).count();
    if capped > 0 {
        println!(
            "\n注意：{capped} 条配对标了 [OI上限]：至少一条腿的场所已触及该合约的持仓量上限，\
             只能减仓，现在开不出来。"
        );
    }

    let unknown = rows.iter().filter(|(_, op)| op.spread_unknown).count();
    if unknown > 0 {
        println!(
            "\n注意：{unknown} 条配对的**穿价成本未知**（至少一条腿拿不到盘口）。\
             它们的成本是下界，因此净收益是**上界**。"
        );
    }

    if view == View::Funding && !report.gated.is_empty() {
        println!(
            "\n被入场基差门槛挡下的配对 {} 条（读数与身份都没问题，只是进场即逆风）：",
            report.gated.len()
        );
        for row in report.gated.iter().take(5) {
            println!(
                "  - {:<12} {} -> {}  {}",
                row.symbol.to_string(),
                row.long.as_str(),
                row.short.as_str(),
                row.reason
            );
        }
        if report.gated.len() > 5 {
            println!("  … 其余 {} 条见 --json", report.gated.len() - 5);
        }
        println!("  （用 --max-entry-basis-pct off 可以关掉这个门槛）");
    }

    let excluded: Vec<&arb_scanner::ExcludedReading> = report
        .suspicious
        .iter()
        .chain(report.unverified.iter())
        .collect();
    if !excluded.is_empty() {
        println!(
            "\n被排除在配对之外的读数 {} 条（仍然展示，只是不参与排名）：",
            excluded.len()
        );
        for row in excluded.iter().take(5) {
            println!(
                "  - {:<12} {:<12} {}",
                row.venue.as_str(),
                row.symbol.to_string(),
                row.reason
            );
        }
        if excluded.len() > 5 {
            println!("  … 其余 {} 条见 --json", excluded.len() - 5);
        }
    }
}

fn leg(view: &SymbolView, venue: arb_core::Venue) -> Option<&arb_core::MarketSnapshot> {
    view.rates.iter().find(|rate| rate.venue == venue)
}

fn health_label(health: arb_scanner::Health) -> &'static str {
    match health {
        arb_scanner::Health::Healthy => "健康",
        arb_scanner::Health::Caution => "注意",
        arb_scanner::Health::Danger => "危险",
    }
}

fn pct(value: Decimal, dp: u32) -> String {
    format!(
        "{:+.width$}",
        to_pct(value).round_dp(dp),
        width = (dp as usize) + 3
    )
}
