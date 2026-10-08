//! 已平仓盈亏核对（手动、联网、只读）：按交易所自己的成交记录与资金费流水，独立重算台账里
//! 每笔已平仓仓位的价格盈亏、手续费、资金费，与台账记录逐项比较。
//!
//! ```text
//! ARB_AUDIT_LEDGER=arb-live-ledger.jsonl cargo test -p arb-exec --test pnl_audit -- --ignored --nocapture
//! ```
//!
//! - 下单开关关着连接（`trading_enabled = false`），券商拒绝一切写操作。
//! - 订单意图日志复制到临时目录再连接：运行中的看板对原文件持独占锁，这里不碰它。
//! - 输出只有仓位号、合约、场所与金额，不打印地址、账户号或密钥。

use std::path::PathBuf;

use arb_core::{Decimal, Side, Venue};
use arb_exec::live_connect::{ConnectOptions, connect};
use arb_exec::settlement::VenueFill;
use arb_exec::{Ledger, PairPosition, PositionStatus};
use chrono::Duration;

fn d(value: Decimal) -> String {
    format!("{:>9}", value.round_dp(4).to_string())
}

#[tokio::test]
#[ignore = "联网：读实盘账户的成交与资金费（只读）"]
async fn audit_closed_positions_against_venue_records() {
    let _ = dotenvy_load();
    let ledger_path = PathBuf::from(
        std::env::var("ARB_AUDIT_LEDGER").unwrap_or_else(|_| "../../arb-live-ledger.jsonl".into()),
    );
    let ledger = Ledger::open(&ledger_path).await.expect("打开台账");
    let (replayed, _) = ledger.replay().await.expect("重放台账");
    let mut closed: Vec<PairPosition> = replayed
        .positions
        .values()
        .filter(|p| p.status == PositionStatus::Closed)
        .cloned()
        .collect();
    closed.sort_by_key(|p| p.opened_at);

    // 用到哪些场所就连哪些；意图日志复制一份。
    let venues: Vec<Venue> = {
        let mut set = Vec::new();
        for p in &closed {
            for record in ledger.position_history(&p.id).await.unwrap() {
                for leg in [record.long, record.short].into_iter().flatten() {
                    if !set.contains(&leg.venue) {
                        set.push(leg.venue);
                    }
                }
            }
        }
        set
    };
    let source_dir = ledger_path.parent().map(PathBuf::from).unwrap_or_default();
    let temp = std::env::temp_dir().join(format!("pnl-audit-{}", std::process::id()));
    std::fs::create_dir_all(&temp).unwrap();
    for venue in &venues {
        let from = arb_exec::live_connect::journal_path(&source_dir, *venue);
        let to = arb_exec::live_connect::journal_path(&temp, *venue);
        if from.exists() {
            std::fs::copy(&from, &to).unwrap();
        }
    }
    let client = arb_venues::build_client(20).unwrap();
    let brokers = connect(
        &client,
        &venues,
        &ConnectOptions {
            journal_dir: temp.clone(),
            trading_enabled: false,
            market_slippage: None,
        },
    )
    .await
    .expect("只读连接券商");

    let mut totals = [Decimal::ZERO; 6];
    println!(
        "{:<20} {:<9} | {:>9} {:>9} | {:>9} {:>9} | {:>9} {:>9} | {:>9} {:>9} | 备注",
        "仓位",
        "合约",
        "价格·台账",
        "价格·所",
        "费·台账",
        "费·所",
        "资金·台账",
        "资金·所",
        "净·台账",
        "净·所"
    );
    for position in &closed {
        let history = ledger.position_history(&position.id).await.unwrap();
        let Some((long, short)) = history
            .iter()
            .rev()
            .find_map(|r| r.long.clone().zip(r.short.clone()))
        else {
            println!("{} 找不到两腿开仓记录", position.id);
            continue;
        };
        // 窗口前后各放宽 2 分钟：开仓单的成交时间可能略早于台账的 opened_at（台账先写意图）。
        let from = position.opened_at - Duration::minutes(2);
        let until = position.closed_at.unwrap() + Duration::minutes(2);
        let mut notes = Vec::new();
        let mut price = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        let mut funding = Decimal::ZERO;
        let mut complete = true;
        for leg in [&long, &short] {
            let broker = &brokers[&leg.venue];
            let fills: Vec<VenueFill> =
                match broker.fills_between(&position.symbol, from, until).await {
                    Ok(Some(fills)) => fills,
                    Ok(None) => {
                        notes.push(format!("{} 未接入成交", leg.venue));
                        complete = false;
                        continue;
                    }
                    Err(error) => {
                        notes.push(format!("{} 成交没查成：{error}", leg.venue));
                        complete = false;
                        continue;
                    }
                };
            // 不依赖台账的开仓价：开、平两侧都用交易所成交。
            let (open_side, close_side) = (
                leg.side,
                if leg.side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                },
            );
            let qty = |side: Side| {
                fills
                    .iter()
                    .filter(|f| f.side == side)
                    .map(|f| f.quantity)
                    .sum::<Decimal>()
            };
            let notional = |side: Side| {
                fills
                    .iter()
                    .filter(|f| f.side == side)
                    .map(|f| f.quantity * f.price)
                    .sum::<Decimal>()
            };
            let (q_open, q_close) = (qty(open_side), qty(close_side));
            if (q_open - q_close).abs() > q_open.max(q_close) * Decimal::new(1, 3) {
                notes.push(format!(
                    "{} 开 {} 平 {} 数量不等",
                    leg.venue,
                    q_open.normalize(),
                    q_close.normalize()
                ));
            }
            let ledger_qty = leg.quantity().unwrap_or_default();
            if (q_open - ledger_qty).abs() > ledger_qty * Decimal::new(1, 3) {
                notes.push(format!(
                    "{} 交易所开仓 {} ≠ 台账 {}",
                    leg.venue,
                    q_open.normalize(),
                    ledger_qty.round_dp(6).normalize()
                ));
            }
            price += match leg.side {
                Side::Buy => notional(close_side) - notional(open_side),
                Side::Sell => notional(open_side) - notional(close_side),
            };
            fees += fills.iter().map(|f| f.fee_usdt).sum::<Decimal>();
            // 资金费：按开仓时刻起算；结束时刻后（换了一笔仓位）的不算。
            // 资金费只算 [开仓, 平仓] 窗口：since(开仓) − since(平仓)。之后同合约的仓位不算进来。
            let closed_at = position.closed_at.unwrap();
            let window = match (
                broker
                    .funding_since(&position.symbol, position.opened_at)
                    .await,
                broker
                    .funding_since(&position.symbol, closed_at + Duration::seconds(1))
                    .await,
            ) {
                (Ok(Some(a)), Ok(Some(b))) => Ok(Some((a, b))),
                (Ok(None), _) | (_, Ok(None)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            };
            match window {
                Ok(Some((all, after))) => {
                    let leg_funding = all.usdt - after.usdt;
                    funding += leg_funding;
                    println!(
                        "    {} {} 资金费：窗口内 {} 笔 {}；平仓后 {} 笔 {}",
                        position.id,
                        leg.venue,
                        all.payments - after.payments,
                        leg_funding.round_dp(6),
                        after.payments,
                        after.usdt.round_dp(6)
                    );
                }
                Ok(None) => {
                    notes.push(format!("{} 未接入资金费", leg.venue));
                    complete = false;
                }
                Err(error) => {
                    notes.push(format!("{} 资金费没查成：{error}", leg.venue));
                    complete = false;
                }
            }
        }
        let ledger_funding = position.realized_funding_usdt.unwrap_or_default();
        let ledger_net = position.realized_pnl_usdt - position.realized_fee_usdt + ledger_funding;
        let venue_net = price - fees + funding;
        if complete {
            for (slot, value) in totals.iter_mut().zip([
                position.realized_pnl_usdt,
                price,
                position.realized_fee_usdt,
                fees,
                ledger_funding,
                funding,
            ]) {
                *slot += value;
            }
        }
        println!(
            "{:<20} {:<9} | {} {} | {} {} | {} {} | {} {} | {}",
            position.id,
            position.symbol.base,
            d(position.realized_pnl_usdt),
            d(price),
            d(position.realized_fee_usdt),
            d(fees),
            d(ledger_funding),
            d(funding),
            d(ledger_net),
            d(venue_net),
            notes.join("；")
        );
    }
    println!(
        "合计（两边都查全的）：价格 台账 {} / 交易所 {}；手续费 {} / {}；资金费 {} / {}；净 {} / {}",
        d(totals[0]),
        d(totals[1]),
        d(totals[2]),
        d(totals[3]),
        d(totals[4]),
        d(totals[5]),
        d(totals[0] - totals[2] + totals[4]),
        d(totals[1] - totals[3] + totals[5]),
    );
    let _ = std::fs::remove_dir_all(&temp);
}

/// 读 `.env`（与看板同一份凭据）。不打印任何值。
fn dotenvy_load() -> std::io::Result<()> {
    let path = std::env::var("ARB_AUDIT_ENV").unwrap_or_else(|_| "../../.env".into());
    for line in std::fs::read_to_string(path)?.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=')
            && std::env::var_os(key.trim()).is_none()
        {
            // SAFETY: 测试开始时单线程设置环境变量，之后才建运行时任务。
            unsafe {
                std::env::set_var(
                    key.trim(),
                    value.trim().trim_matches('"').trim_matches('\''),
                )
            };
        }
    }
    Ok(())
}

/// 演练历史更正（手动、联网、只读交易所）：把台账**复制**到临时文件，对每笔已平仓仓位跑一遍
/// 与看板后台相同的 [`arb_exec::desk::recheck_funding`]，打印会写进去的更正。原台账不动。
#[tokio::test]
#[ignore = "联网：读实盘账户的资金费（只读），只写临时台账副本"]
async fn rehearse_funding_corrections_on_a_copy() {
    let _ = dotenvy_load();
    let source = PathBuf::from(
        std::env::var("ARB_AUDIT_LEDGER").unwrap_or_else(|_| "../../arb-live-ledger.jsonl".into()),
    );
    let temp = std::env::temp_dir().join(format!("pnl-rehearse-{}", std::process::id()));
    std::fs::create_dir_all(&temp).unwrap();
    let copy = temp.join("ledger.jsonl");
    std::fs::copy(&source, &copy).unwrap();
    let ledger = Ledger::open(&copy).await.unwrap();
    let (replayed, _) = ledger.replay().await.unwrap();
    let mut closed: Vec<PairPosition> = replayed
        .positions
        .values()
        .filter(|p| p.status == PositionStatus::Closed)
        .cloned()
        .collect();
    closed.sort_by_key(|p| p.opened_at);
    let source_dir = source.parent().map(PathBuf::from).unwrap_or_default();
    let venues = [Venue::Arcus, Venue::LighterRh];
    for venue in venues {
        let from = arb_exec::live_connect::journal_path(&source_dir, venue);
        if from.exists() {
            std::fs::copy(&from, arb_exec::live_connect::journal_path(&temp, venue)).unwrap();
        }
    }
    let client = arb_venues::build_client(20).unwrap();
    let brokers = connect(
        &client,
        &venues,
        &ConnectOptions {
            journal_dir: temp.clone(),
            trading_enabled: false,
            market_slippage: None,
        },
    )
    .await
    .unwrap();
    let now = chrono::Utc::now();
    for position in &closed {
        let outcome = arb_exec::desk::recheck_funding(&ledger, &brokers, position, now, false)
            .await
            .unwrap();
        println!("{} {:<8} {:?}", position.id, position.symbol.base, outcome);
    }
    let (after, _) = ledger.replay().await.unwrap();
    let net: Decimal = after
        .positions
        .values()
        .filter(|p| p.status == PositionStatus::Closed)
        .map(|p| {
            p.realized_pnl_usdt - p.realized_fee_usdt + p.realized_funding_usdt.unwrap_or_default()
        })
        .sum();
    println!("更正后已平仓净额合计 {} USDT", net.round_dp(4));
    let _ = std::fs::remove_dir_all(&temp);
}
