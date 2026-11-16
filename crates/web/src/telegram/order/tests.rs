use super::*;
use crate::rh_spread::{Best, Connected, DirectionQuote, Leg, Line, Normal, Session};

fn dec(raw: &str) -> Decimal {
    raw.parse().unwrap()
}

fn leg(entry: &str, zero: &str, normal: &str) -> Leg {
    Leg {
        quote: DirectionQuote {
            entry_pct: dec(entry),
            exit_cross_pct: dec("0.02"),
        },
        net_to_zero_pct: dec(zero),
        net_to_normal_pct: Some(dec(normal)),
        funding_cost_pct: None,
        ladder: Vec::new(),
    }
}

/// Arcus ↔ Lighter RH 一行，只有多 Arcus / 空 Lighter RH（`long_a`）这个方向有报价。
fn line(base: &str, leg: Leg, median: f64) -> Line {
    Line {
        pair: crate::rh_spread::pairs::Pair::RH.id(),
        a: Venue::Arcus,
        b: Venue::LighterRh,
        fee_round_trip_pct: Some(dec("0.045")),
        base: base.into(),
        category: "EQUITIES".into(),
        session: Session::Rth,
        basis_pct: Some(dec("-0.4")),
        normal: Some(Normal {
            session: Session::Rth,
            median,
            p10: median - 0.03,
            p90: median + 0.03,
            mad: 0.01,
            minutes: 900,
        }),
        normal_missing_minutes: 0,
        z: Some(-10.0),
        long_a: Some(leg),
        long_b: None,
        best: Some(Best {
            direction: "long_a",
            signal: true,
            signal_sec: 30,
            net_usdt: None,
        }),
        age_sec: Some(0),
        note: None,
        observe_only: false,
        watch_only: false,
        maker: None,
        recent_move: None,
        recent_entry: None,
    }
}

fn view(lines: Vec<Line>) -> View {
    View {
        enabled: true,
        size_usdt: Decimal::from(2000),
        alert_net_pct: dec("0.05"),
        min_minutes: 120,
        window_days: 7,
        pairs: Vec::new(),
        connected: Connected {
            venues: [(Venue::Arcus, true), (Venue::LighterRh, true)].into(),
            reconnects: 0,
        },
        updated_at: Some(Utc::now()),
        lines: lines.into_iter().map(std::sync::Arc::new).collect(),
        error: None,
        maker: None,
    }
}

/// 线上的那组设置：实盘、单腿 800 × 3x、门槛 0.05% 再扣 0.05% 滑点缓冲、补仓 5 次 / 3 倍。
fn settings() -> Settings {
    Settings {
        enabled: true,
        mode: Mode::Live,
        size_usdt: Decimal::from(800),
        leverage: Decimal::from(3),
        min_net_pct: dec("0.05"),
        slippage_buffer_pct: dec("0.05"),
        hold_sec: 5,
        hold_ms: None,
        open_gap_sec: 60,
        add_min_improve_pct: Some(dec("0.2")),
        add_max_times: 5,
        add_max_multiple: Decimal::from(3),
        history_gate_pct: Some(Decimal::from(20)),
        ..Settings::default()
    }
}

fn intent(base: &str) -> Intent {
    Intent {
        pair: crate::rh_spread::pairs::Pair::RH.id(),
        base: base.into(),
        direction: "long_a",
    }
}

fn target(
    base: &str,
    long: Venue,
    short: Venue,
    entry: &str,
    notional: &str,
    adds: u32,
) -> AddTarget {
    AddTarget {
        id: format!("live-{base}"),
        base: base.into(),
        long,
        short,
        entry_spread_pct: dec(entry),
        notional_usdt: dec(notional),
        adds,
    }
}

fn all(_: Venue) -> bool {
    true
}

fn plan_for(
    view: &View,
    exposed: &[(String, Symbol)],
    targets: &[AddTarget],
) -> Result<Plan, String> {
    plan(
        view,
        &intent("TSLA"),
        &settings(),
        exposed,
        targets,
        None,
        &all,
        Utc::now(),
    )
}

#[test]
fn opens_with_the_auto_trade_size_and_rules_even_below_the_auto_threshold() {
    // 回到正常净收益 0.07%：响了提醒，但扣 0.05% 缓冲后不到 0.05% 门槛，自动交易不会下 —— 手动可以。
    let view = view(vec![line("TSLA", leg("0.40", "0.335", "0.07"), -0.3)]);
    let plan = plan_for(&view, &[], &[]).unwrap();
    assert_eq!((plan.long, plan.short), (Venue::Arcus, Venue::LighterRh));
    assert_eq!(
        (plan.size, plan.leverage, plan.mode),
        (Decimal::from(800), Decimal::from(3), Mode::Live)
    );
    let Action::Open { rules } = &plan.action else {
        panic!("没有仓位时是新开");
    };
    // 多 a / 空 b：持仓基差 = −页面基差，回到正常的目标 = −中位数。
    assert_eq!(rules.basis_exit_pct, Some(dec("0.3")));
    assert_eq!(plan.entry_pct, dec("0.40"));
    for expected in [
        "确认实盘开仓",
        "多 Arcus / 空 Lighter RH",
        "单腿 800 USDT × 3x",
        "提醒里的 2000 USDT",
        "自动交易此刻不会下这单",
        "历史价差分位门槛（前 20%）这里不检查",
        "90 秒内有效",
    ] {
        assert!(
            plan.prompt.contains(expected),
            "{expected}：{}",
            plan.prompt
        );
    }
    assert_eq!(plan.button, "✅ 确认实盘开仓 TSLA 800U");
}

#[test]
fn a_faded_signal_is_the_first_line_of_the_confirmation() {
    let prompt = |net: &str| {
        let view = view(vec![line("TSLA", leg("0.40", "0.335", net), -0.3)]);
        plan_for(&view, &[], &[]).unwrap().prompt
    };
    assert!(prompt("-0.02").starts_with("⚠️ 信号已经消失：现在回到正常预估净赚 -0.02%（≤ 0）"));
    assert!(prompt("0").starts_with("⚠️ 信号已经消失"));
    assert!(
        prompt("0.03")
            .starts_with("⚠️ 信号已经减弱：现在回到正常预估净赚 0.03%，低于提醒门槛 0.05%")
    );
    assert!(prompt("0.05").starts_with("⚠️ 确认实盘开仓"));
}

#[test]
fn a_same_direction_position_becomes_an_add_within_its_caps() {
    let view = view(vec![line("TSLA", leg("0.40", "0.335", "0.20"), -0.3)]);
    let exposed = vec![("live-TSLA".to_string(), Symbol::perp("TSLA", "USDT"))];
    let held = target("TSLA", Venue::Arcus, Venue::LighterRh, "0.3", "800", 1);
    let plan = plan_for(&view, &exposed, std::slice::from_ref(&held)).unwrap();
    assert_eq!(plan.action, Action::Add(held.clone()));
    assert_eq!(plan.max_total, Decimal::from(2400));
    for expected in [
        "确认实盘补仓",
        "补到仓位 live-TSLA",
        "原仓位开仓价差 0.3%，现在可成交 0.40%（好 0.10%；自动补仓门槛 0.2%）",
        "第 2 次，上限 5 次；补完单腿约 1600 / 上限 2400 USDT",
        "这一刻也过了自动交易的净收益门槛",
    ] {
        assert!(
            plan.prompt.contains(expected),
            "{expected}：{}",
            plan.prompt
        );
    }
    assert_eq!(plan.button, "✅ 确认实盘补仓 TSLA 800U");

    // 价差比原仓位差：照样能补（人拿主意），但要写明。
    let worse = target("TSLA", Venue::Arcus, Venue::LighterRh, "0.5", "800", 0);
    let plan = plan_for(&view, &exposed, &[worse]).unwrap();
    assert!(plan.prompt.contains("差 0.10%") && plan.prompt.contains("会拉低整笔的开仓价差"));

    // 次数、总名义、单笔上限都要挡住。
    let full = target("TSLA", Venue::Arcus, Venue::LighterRh, "0.3", "800", 5);
    assert!(
        plan_for(&view, &exposed, &[full])
            .unwrap_err()
            .contains("到上限 5 次")
    );
    let big = target("TSLA", Venue::Arcus, Venue::LighterRh, "0.3", "1700", 1);
    assert!(
        plan_for(&view, &exposed, &[big])
            .unwrap_err()
            .contains("超过上限 2400 USDT")
    );
    let capped = super::plan(
        &view,
        &intent("TSLA"),
        &settings(),
        &exposed,
        &[held],
        Some(Decimal::from(1000)),
        &all,
        Utc::now(),
    );
    assert!(capped.unwrap_err().contains("超过上限 1000 USDT"));
}

#[test]
fn other_positions_on_the_same_symbol_are_left_to_the_panel() {
    let view = view(vec![line("TSLA", leg("0.40", "0.335", "0.20"), -0.3)]);
    let exposed = vec![("live-TSLA".to_string(), Symbol::perp("TSLA", "USDT"))];
    // 反方向的价差仓位、或者根本不是可补仓的价差仓位（资金费仓位不在 targets 里）。
    let opposite = target("TSLA", Venue::LighterRh, Venue::Arcus, "0.3", "800", 0);
    for targets in [vec![opposite], Vec::new()] {
        let error = plan_for(&view, &exposed, &targets).unwrap_err();
        assert!(
            error.contains("已有 TSLA 的仓位 live-TSLA") && error.contains("去面板处理"),
            "{error}"
        );
    }
}

#[test]
fn hard_entry_conditions_stale_views_and_missing_quotes_refuse() {
    let good = || line("TSLA", leg("0.40", "0.335", "0.20"), -0.3);
    let refuse = |view: View, tradable: &dyn Fn(Venue) -> bool| {
        plan(
            &view,
            &intent("TSLA"),
            &settings(),
            &[],
            &[],
            None,
            tradable,
            Utc::now(),
        )
        .unwrap_err()
    };
    let negative = view(vec![line("TSLA", leg("-0.05", "-0.1", "0.20"), -0.3)]);
    assert!(refuse(negative, &all).contains("不满足开仓硬条件"));
    let mut stale = view(vec![good()]);
    stale.updated_at = Some(Utc::now() - chrono::Duration::seconds(60));
    assert!(refuse(stale, &all).contains("不新鲜"));
    assert!(
        refuse(view(vec![good()]), &|venue| venue != Venue::LighterRh).contains("不都是实盘已连接")
    );
    let mut down = view(vec![good()]);
    down.connected.venues.insert(Venue::LighterRh, false);
    assert!(refuse(down, &all).contains("行情连接断了"));
    let mut noted = good();
    noted.note = Some("Arcus 盘口太旧".into());
    assert!(refuse(view(vec![noted]), &all).contains("现在不能做：Arcus 盘口太旧"));
    let mut other = good();
    other.base = "NVDA".into();
    assert!(refuse(view(vec![other]), &all).contains("没有 TSLA"));
    let mut flipped = good();
    flipped.long_a = None;
    assert!(refuse(view(vec![flipped]), &all).contains("没有可成交报价"));
    // 正常基差离谱（超出规则 ±5%）：设不了回到正常的目标就不开。
    assert!(
        refuse(
            view(vec![line("TSLA", leg("0.40", "0.335", "0.20"), -7.0)]),
            &all
        )
        .contains("±5%")
    );
}

#[test]
fn confirmation_is_rechecked_against_a_fresh_plan() {
    let view_at = |entry: &str| view(vec![line("TSLA", leg(entry, "0.335", "0.20"), -0.3)]);
    let confirmed = plan_for(&view_at("0.40"), &[], &[]).unwrap();
    // 价差变好、或变差不超过 0.1%：照下。
    for entry in ["0.55", "0.40", "0.31"] {
        let fresh = plan_for(&view_at(entry), &[], &[]).unwrap();
        assert_eq!(still_valid(&confirmed, &fresh), Ok(()), "{entry}");
    }
    let fresh = plan_for(&view_at("0.25"), &[], &[]).unwrap();
    assert!(
        still_valid(&confirmed, &fresh)
            .unwrap_err()
            .contains("差了 0.15%")
    );
    // 确认消息说的是新开，这期间自动交易开了一笔：变成补仓了，不下。
    let exposed = vec![("live-TSLA".to_string(), Symbol::perp("TSLA", "USDT"))];
    let held = target("TSLA", Venue::Arcus, Venue::LighterRh, "0.3", "800", 0);
    let fresh = plan_for(&view_at("0.40"), &exposed, std::slice::from_ref(&held)).unwrap();
    assert!(
        still_valid(&confirmed, &fresh)
            .unwrap_err()
            .contains("持仓刚有变化")
    );
    let add = fresh.clone();
    let mut elsewhere = held;
    elsewhere.id = "live-TSLA-2".into();
    let moved = plan_for(&view_at("0.40"), &exposed, &[elsewhere]).unwrap();
    assert!(still_valid(&add, &moved).is_err());
    assert_eq!(still_valid(&add, &fresh), Ok(()));
    // 确认期间面板上改了设置（实盘 → 纸面、换了数量）：不按旧确认下。
    let confirmed = plan_for(&view_at("0.40"), &[], &[]).unwrap();
    for change in [
        |s: &mut Settings| s.mode = Mode::Paper,
        |s: &mut Settings| s.size_usdt = Decimal::from(500),
        |s: &mut Settings| s.leverage = Decimal::from(2),
    ] {
        let mut changed = settings();
        change(&mut changed);
        let fresh = plan(
            &view_at("0.40"),
            &intent("TSLA"),
            &changed,
            &[],
            &[],
            None,
            &all,
            Utc::now(),
        )
        .unwrap();
        assert!(
            still_valid(&confirmed, &fresh)
                .unwrap_err()
                .contains("刚改过")
        );
    }
}

#[test]
fn tickets_are_single_use_expire_and_stay_bounded() {
    let start = Instant::now();
    let mut tickets = Tickets::new(Duration::from_secs(90));
    let id = tickets.issue("plan", start);
    assert_eq!(id.len(), 12);
    assert_eq!(
        tickets.get(&id, start + Duration::from_secs(10)),
        Some("plan")
    );
    assert_eq!(
        tickets.take(&id, start + Duration::from_secs(10)),
        Some("plan")
    );
    assert_eq!(
        tickets.take(&id, start + Duration::from_secs(11)),
        None,
        "只能用一次"
    );
    let late = tickets.issue("late", start);
    assert_eq!(tickets.get(&late, start + Duration::from_secs(91)), None);
    assert_eq!(
        tickets.take(&late, start + Duration::from_secs(91)),
        None,
        "过期的也作废"
    );
    let ids: std::collections::HashSet<String> =
        (0..1000).map(|_| tickets.issue("x", start)).collect();
    assert_eq!(ids.len(), 1000, "票据 id 不重复");
    assert!(tickets.map.len() <= MAX_TICKETS);
    // `callback_data` 最多 64 字节：前缀 + 12 位 id 远在限内。
    assert!(format!("ok:{}", new_id()).len() <= 64);
}

#[test]
fn order_path_responses_become_plain_results() {
    let opened = serde_json::json!({"position": {"id": "live-7", "status": "open"}});
    assert!(open_outcome(Mode::Live, StatusCode::OK, &opened).contains("✅ 已开仓 live-7"));
    let unwound = serde_json::json!({"position": {"id": "live-8", "status": "closed"}});
    assert!(open_outcome(Mode::Live, StatusCode::OK, &unwound).contains("回滚收场（live-8）"));
    let busy = serde_json::json!({"error": "已有操作正在进行"});
    assert!(open_outcome(Mode::Live, StatusCode::CONFLICT, &busy).contains("交易台正忙"));
    let paused = serde_json::json!({"error": "开新仓已被暂停"});
    assert_eq!(
        open_outcome(Mode::Live, StatusCode::LOCKED, &paused),
        "没下单：开新仓已被暂停"
    );
    let gate = serde_json::json!({"error": "深度不够"});
    assert!(
        open_outcome(Mode::Live, StatusCode::UNPROCESSABLE_ENTITY, &gate)
            .contains("检查没通过：深度不够")
    );
    let broke = serde_json::json!({"error": "第二腿超时"});
    assert!(
        open_outcome(Mode::Live, StatusCode::INTERNAL_SERVER_ERROR, &broke).contains("结果未知")
    );
    let added = serde_json::json!({"added": true, "position": {"note": "补了 800"}});
    assert_eq!(
        add_outcome("live-7", StatusCode::OK, &added),
        "✅ 已补仓到 live-7。补了 800"
    );
    let skipped = serde_json::json!({"added": false, "position": {"note": "深度不够"}});
    assert!(add_outcome("live-7", StatusCode::OK, &skipped).starts_with("没补成"));
    assert!(add_outcome("live-7", StatusCode::INTERNAL_SERVER_ERROR, &broke).contains("补仓中断"));
}
