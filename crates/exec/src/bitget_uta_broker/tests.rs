use super::*;
use rust_decimal_macros::dec;

fn settings(
    mode: &str,
    hold: &str,
    level: &str,
    configs: &[(&str, &str, &str)],
) -> AccountSettings {
    AccountSettings {
        account_mode: Some(mode.into()),
        account_level: Some(level.into()),
        hold_mode: Some(hold.into()),
        delta_switch: Some("no".into()),
        symbol_config_list: configs
            .iter()
            .map(|(symbol, margin, leverage)| SymbolConfig {
                category: Some(CATEGORY.into()),
                symbol: Some((*symbol).into()),
                margin_mode: Some((*margin).into()),
                leverage: Some((*leverage).into()),
            })
            .collect(),
    }
}

#[test]
fn an_empty_list_written_as_null_reads_as_empty() {
    // Verbatim live replies of an empty unified account (10-21).
    let positions: Page<PositionRow> = data_of(json!({
        "code": "00000", "data": {"list": null}, "msg": "success", "requestTime": 1792588254224u64
    }))
    .unwrap();
    assert!(positions.list.is_empty());
    let fills: Page<FillRow> = data_of(json!({
        "code": "00000", "data": {"cursor": null, "list": null}, "msg": "success"
    }))
    .unwrap();
    assert!(fills.list.is_empty() && fills.cursor.is_none());
    let fill: FillRow = serde_json::from_value(json!({"execId": "1", "feeDetail": null})).unwrap();
    assert!(fill.fee_detail.is_empty());
    let settings: AccountSettings =
        serde_json::from_value(json!({"accountMode": "unified", "symbolConfigList": null}))
            .unwrap();
    assert!(settings.symbol_config_list.is_empty());
    let assets: Assets = serde_json::from_value(json!({"assets": null})).unwrap();
    assert!(assets.assets.is_empty());
    // A missing list is still empty, and a list that is not one is still malformed.
    let missing: Page<PositionRow> = serde_json::from_value(json!({})).unwrap();
    assert!(missing.list.is_empty());
    assert!(serde_json::from_value::<Page<PositionRow>>(json!({"list": "x"})).is_err());
}

#[test]
fn the_account_type_defaults_to_the_unified_account() {
    assert_eq!(BitgetAccount::parse(None), Ok(BitgetAccount::Uta));
    assert_eq!(BitgetAccount::parse(Some("")), Ok(BitgetAccount::Uta));
    assert_eq!(BitgetAccount::parse(Some(" UTA ")), Ok(BitgetAccount::Uta));
    assert_eq!(
        BitgetAccount::parse(Some("classic")),
        Ok(BitgetAccount::Classic)
    );
    assert!(BitgetAccount::parse(Some("futures")).is_err());
}

/// 统一账户、单向持仓才连；升级 / 切回中、双向持仓、Delta 中性模式都拒绝（不代改）。
#[test]
fn only_a_settled_one_way_unified_account_is_accepted() {
    assert!(check_account(&settings("unified", "one_way_mode", "basic", &[])).is_ok());
    assert!(check_account(&settings("unified", "one_way_mode", "isolated", &[])).is_ok());
    assert!(check_account(&settings("upgrading", "one_way_mode", "basic", &[])).is_err());
    assert!(check_account(&settings("switching", "one_way_mode", "basic", &[])).is_err());
    assert!(check_account(&settings("unified", "hedge_mode", "basic", &[])).is_err());
    assert!(check_account(&settings("unified", "one_way_mode", "delta", &[])).is_err());
    assert!(check_account(&settings("unified", "one_way_mode", "advanced", &[])).is_ok());
    let mut delta = settings("unified", "one_way_mode", "advanced", &[]);
    delta.delta_switch = Some("yes".into());
    assert!(check_account(&delta).is_err());
}

#[test]
fn leverage_in_force_is_read_from_the_symbol_configuration() {
    let account = settings(
        "unified",
        "one_way_mode",
        "basic",
        &[("NVDAUSDT", "isolated", "3"), ("SOXLUSDT", "crossed", "5")],
    );
    assert_eq!(
        mode_in_force(&account, "NVDAUSDT", dec!(3)).unwrap(),
        Some(crate::MarginMode::Isolated)
    );
    assert_eq!(mode_in_force(&account, "NVDAUSDT", dec!(5)).unwrap(), None);
    assert_eq!(
        mode_in_force(&account, "SOXLUSDT", dec!(5)).unwrap(),
        Some(crate::MarginMode::Cross)
    );
    assert_eq!(
        mode_in_force(&account, "TSLAUSDT", dec!(3)).unwrap(),
        None,
        "没配过的合约：要设"
    );
}

#[test]
fn v3_contract_status_names_decide_what_may_be_sent() {
    assert_eq!(status_halt(Some("online"), false), None);
    assert_eq!(status_halt(Some("limit_open"), true), None);
    assert!(status_halt(Some("limit_open"), false).is_some());
    // 开得出去、平不掉的腿比没有更糟：两边都停。
    assert!(status_halt(Some("limit_close"), false).is_some());
    assert!(status_halt(Some("limit_close"), true).is_some());
    for halted in ["offline", "restrictedAPI", "listed", "normal"] {
        assert!(status_halt(Some(halted), true).is_some(), "{halted}");
    }
    assert!(status_halt(None, true).is_some());
}

fn instrument(symbol: &str, base: &str, status: &str) -> InstrumentRow {
    InstrumentRow {
        symbol: symbol.into(),
        base_coin: base.into(),
        quote_coin: "USDT".into(),
        is_rwa: Some("YES".into()),
        kind: Some("perpetual".into()),
        status: Some(status.into()),
        min_order_qty: Some("0.01".into()),
        max_order_qty: Some("0".into()),
        price_multiplier: Some("0.01".into()),
        price_precision: Some("2".into()),
        quantity_multiplier: Some("0.01".into()),
        quantity_precision: Some("2".into()),
        min_order_amount: Some("5".into()),
        min_leverage: Some("1".into()),
        max_leverage: Some("20".into()),
        buy_limit_price_ratio: Some("0.02".into()),
        sell_limit_price_ratio: Some("0.02".into()),
    }
}

#[test]
fn instruments_follow_the_v3_specification_fields() {
    let nvda = Symbol::perp("NVDA", "USDT");
    let rows = vec![instrument("NVDAUSDT", "NVDA", "online")];
    let found = select_instrument(&rows, &nvda, false).unwrap();
    assert_eq!(found.native, "NVDAUSDT");
    assert!(found.rwa);
    assert_eq!((found.tick, found.step), (dec!(0.01), dec!(0.01)));
    assert_eq!((found.min_qty, found.min_notional), (dec!(0.01), dec!(5)));
    assert_eq!(found.max_qty, None, "0 = 不限");
    assert_eq!((found.min_lever, found.max_lever), (dec!(1), dec!(20)));
    assert_eq!(found.limit_ratio, Some(dec!(0.02)));
    let closing_only = vec![instrument("NVDAUSDT", "NVDA", "limit_open")];
    assert!(select_instrument(&closing_only, &nvda, false).is_err());
    assert!(select_instrument(&closing_only, &nvda, true).is_ok());
    let mut too_fine = instrument("NVDAUSDT", "NVDA", "online");
    too_fine.price_multiplier = Some("0.001".into());
    assert!(select_instrument(&[too_fine], &nvda, false).is_err());
    assert!(select_instrument(&rows, &Symbol::perp("TSLA", "USDT"), false).is_err());
}

/// 每张单都带保证金模式：v3 不带就开成全仓。平仓的只减仓单也一样。
#[test]
fn every_order_carries_its_margin_mode_and_is_a_limit_ioc() {
    let order = |side: Side, reduce_only: bool, mode: crate::MarginMode| NewOrder {
        post_only: false,
        margin_mode: mode,
        client_order_id: ClientOrderId("p1-0".into()),
        venue: Venue::Bitget,
        symbol: Symbol::perp("NVDA", "USDT"),
        side,
        notional_usdt: dec!(400),
        quantity: None,
        limit_price: None,
        reduce_only,
        leverage: Some(dec!(3)),
    };
    let open = order_body(
        "NVDAUSDT",
        &order(Side::Buy, false, crate::MarginMode::Isolated),
        dec!(2.2),
        dec!(180.50),
        "bgp10",
    );
    assert_eq!(open["marginMode"], "isolated");
    assert_eq!(open["reduceOnly"], "no");
    assert_eq!(open["timeInForce"], "ioc");
    assert_eq!(open["orderType"], "limit");
    assert_eq!(open["qty"], "2.2");
    assert_eq!(open["price"], "180.5");
    assert_eq!(open["category"], CATEGORY);
    assert!(open.get("posSide").is_none(), "单向持仓不带 posSide");
    let exit = order_body(
        "NVDAUSDT",
        &order(Side::Sell, true, crate::MarginMode::Isolated),
        dec!(2.2),
        dec!(181),
        "bgp11",
    );
    assert_eq!(exit["marginMode"], "isolated");
    assert_eq!(exit["reduceOnly"], "yes");
    assert_eq!(exit["side"], "sell");
    let cross = order_body(
        "NVDAUSDT",
        &order(Side::Buy, false, crate::MarginMode::Cross),
        dec!(1),
        dec!(1),
        "bgp12",
    );
    assert_eq!(cross["marginMode"], "crossed");
}

#[test]
fn order_statuses_and_refusals_follow_the_v3_codes() {
    assert_eq!(map_status("live").unwrap(), OrderStatus::Open);
    assert_eq!(map_status("new").unwrap(), OrderStatus::Open);
    assert_eq!(map_status("partially_filled").unwrap(), OrderStatus::Open);
    assert_eq!(map_status("filled").unwrap(), OrderStatus::Filled);
    assert_eq!(map_status("cancelled").unwrap(), OrderStatus::Cancelled);
    assert!(map_status("rejected").is_err());
    // 余额不足、价格越界、数量不够：明确拒绝。
    for refusal in ["25203", "25205", "25207", "25611", "40017"] {
        assert!(
            definitive_refusal(StatusCode::BAD_REQUEST, refusal),
            "{refusal}"
        );
    }
    // 系统错误、超时、限频、clientOid 重复（单已经在交易所了）：结果不明。
    for unknown in AMBIGUOUS_CODES {
        assert!(
            !definitive_refusal(StatusCode::BAD_REQUEST, unknown),
            "{unknown}"
        );
    }
    assert!(!definitive_refusal(StatusCode::BAD_REQUEST, "25212"));
    // 限频：网关在受理前就挡掉了（调用前已确认交易所没有这张单）。
    assert!(definitive_refusal(StatusCode::TOO_MANY_REQUESTS, "429"));
    assert!(definitive_refusal(StatusCode::TOO_MANY_REQUESTS, ""));
    assert!(!definitive_refusal(StatusCode::BAD_GATEWAY, "25203"));
    assert!(!definitive_refusal(StatusCode::OK, SUCCESS));
    assert!(!definitive_refusal(StatusCode::BAD_REQUEST, ""));
}

#[test]
fn margin_replies_separate_applied_refused_and_unknown() {
    let reply = |code: &str| json!({"code": code, "msg": "m", "data": "success"});
    assert_eq!(
        classify_margin_reply(StatusCode::OK, &reply(SUCCESS)),
        MarginOutcome::Applied
    );
    assert!(matches!(
        classify_margin_reply(StatusCode::BAD_REQUEST, &reply("25202")),
        MarginOutcome::Refused(_)
    ));
    assert!(matches!(
        classify_margin_reply(StatusCode::BAD_REQUEST, &reply("25001")),
        MarginOutcome::Unknown(_)
    ));
    assert!(matches!(
        classify_margin_reply(StatusCode::INTERNAL_SERVER_ERROR, &reply("25202")),
        MarginOutcome::Unknown(_)
    ));
}

#[test]
fn pagination_must_move_on() {
    assert_eq!(next_cursor(Some("9".into()), None).unwrap(), "9");
    assert_eq!(next_cursor(Some("8".into()), Some("9")).unwrap(), "8");
    assert!(next_cursor(Some("9".into()), Some("9")).is_err());
    assert!(next_cursor(Some(String::new()), None).is_err());
    assert!(next_cursor(None, None).is_err());
}

fn fill(id: &str, order: &str, at: &str, side: &str, qty: &str, price: &str, fee: &str) -> FillRow {
    FillRow {
        exec_id: Some(id.into()),
        order_id: Some(order.into()),
        symbol: Some("NVDAUSDT".into()),
        exec_price: Some(price.into()),
        exec_qty: Some(qty.into()),
        side: Some(side.into()),
        fee_detail: vec![FeeDetail {
            fee_coin: Some("USDT".into()),
            fee: Some(fee.into()),
        }],
        created_time: Some(at.into()),
    }
}

fn remote(order_id: &str, side: &str) -> RemoteOrder {
    RemoteOrder {
        order_id: Some(order_id.into()),
        client_oid: Some("bgp10".into()),
        symbol: Some("NVDAUSDT".into()),
        price: Some("181".into()),
        qty: Some("2".into()),
        cum_exec_qty: Some("2".into()),
        cum_exec_value: Some("361".into()),
        avg_price: Some("180.5".into()),
        order_status: Some("filled".into()),
        side: Some(side.into()),
        reduce_only: Some("NO".into()),
        margin_mode: Some("isolated".into()),
    }
}

/// 只下吃单：手续费不管字段正负，一律记成正的成本。
#[test]
fn fills_aggregate_to_the_order_with_fees_as_a_positive_cost() {
    let order = remote("7", "buy");
    let fills = vec![
        fill("a", "7", "1000", "buy", "1.5", "180", "-0.162"),
        fill("b", "7", "1001", "buy", "0.5", "182", "0.0546"),
    ];
    let (qty, notional, fee) = aggregate_fills(&fills, &order).unwrap();
    assert_eq!((qty, notional, fee), (dec!(2), dec!(361), dec!(0.2166)));
    let wrong_order = vec![fill("c", "8", "1000", "buy", "1", "180", "0.1")];
    assert!(aggregate_fills(&wrong_order, &order).is_err());
    let twice = vec![fills[0].clone_row(), fills[0].clone_row()];
    assert!(aggregate_fills(&twice, &order).is_err());
    let mut bgb = fill("d", "7", "1000", "buy", "1", "180", "0.1");
    bgb.fee_detail[0].fee_coin = Some("BGB".into());
    assert!(aggregate_fills(&[bgb], &order).is_err());
}

impl FillRow {
    fn clone_row(&self) -> FillRow {
        FillRow {
            exec_id: self.exec_id.clone(),
            order_id: self.order_id.clone(),
            symbol: self.symbol.clone(),
            exec_price: self.exec_price.clone(),
            exec_qty: self.exec_qty.clone(),
            side: self.side.clone(),
            fee_detail: self
                .fee_detail
                .iter()
                .map(|d| FeeDetail {
                    fee_coin: d.fee_coin.clone(),
                    fee: d.fee.clone(),
                })
                .collect(),
            created_time: self.created_time.clone(),
        }
    }
}

#[test]
fn fill_history_becomes_venue_fills_oldest_first() {
    let fills = venue_fills(vec![
        fill("b", "2", "2000", "Sell", "2", "181", "-0.217"),
        fill("a", "1", "1000", "buy", "2", "180", "0.216"),
    ])
    .unwrap();
    assert_eq!(fills[0].at.timestamp_millis(), 1000);
    assert_eq!((fills[0].side, fills[1].side), (Side::Buy, Side::Sell));
    assert_eq!(
        (fills[0].fee_usdt, fills[1].fee_usdt),
        (dec!(0.216), dec!(0.217))
    );
}

fn record(id: &str, kind: &str, symbol: &str, amount: &str, ts: &str) -> FinancialRecord {
    FinancialRecord {
        id: Some(id.into()),
        symbol: Some(symbol.into()),
        coin: Some("USDT".into()),
        kind: Some(kind.into()),
        amount: Some(amount.into()),
        ts: Some(ts.into()),
    }
}

/// 资金费：逐仓、全仓、RWA 现金股息四组都算；正负按 _IN / _OUT 定，不管 amount 本身带不带号。
#[test]
fn funding_records_of_every_type_keep_their_direction() {
    let window = (1_000, 2_000);
    let entry = |r: &FinancialRecord| funding_entry(r, "NVDAUSDT", window).unwrap();
    let paid = entry(&record(
        "1",
        "MARGIN_SETTLE_FEE_USER_OUT",
        "NVDAUSDT",
        "0.05",
        "1500",
    ))
    .unwrap();
    assert_eq!((paid.0.as_str(), paid.2), ("1", dec!(-0.05)));
    let paid_signed = entry(&record(
        "2",
        "MARGIN_SETTLE_FEE_USER_OUT",
        "NVDAUSDT",
        "-0.05",
        "1500",
    ))
    .unwrap();
    assert_eq!(paid_signed.2, dec!(-0.05));
    let received = entry(&record(
        "3",
        "CONTRACT_MAIN_SETTLE_FEE_USER_IN",
        "NVDAUSDT",
        "0.02",
        "1600",
    ))
    .unwrap();
    assert_eq!(received.2, dec!(0.02));
    let dividend = entry(&record(
        "4",
        "RWA_FIXED_SETTLE_FEE_USER_OUT",
        "NVDAUSDT",
        "0.31",
        "1700",
    ))
    .unwrap();
    assert_eq!(dividend.2, dec!(-0.31));
    assert!(entry(&record("5", "ORDER_PLF_FEE_OUT", "NVDAUSDT", "0.1", "1500")).is_none());
    assert!(
        entry(&record(
            "6",
            "MARGIN_SETTLE_FEE_USER_IN",
            "TSLAUSDT",
            "0.1",
            "1500"
        ))
        .is_none()
    );
    assert!(
        funding_entry(
            &record("7", "MARGIN_SETTLE_FEE_USER_IN", "NVDAUSDT", "0.1", "2001"),
            "NVDAUSDT",
            window
        )
        .is_err()
    );
    let mut bgb = record("8", "MARGIN_SETTLE_FEE_USER_IN", "NVDAUSDT", "0.1", "1500");
    bgb.coin = Some("BGB".into());
    assert!(funding_entry(&bgb, "NVDAUSDT", window).is_err());
}

#[test]
fn free_collateral_is_the_usdt_available_balance() {
    let assets = |coin: &str, available: &str| Assets {
        assets: vec![AssetRow {
            coin: Some(coin.into()),
            available: Some(available.into()),
        }],
    };
    assert_eq!(
        free_collateral_of(&assets("USDT", "812.4")).unwrap(),
        dec!(812.4)
    );
    assert_eq!(
        free_collateral_of(&assets("USDT", "-3")).unwrap(),
        Decimal::ZERO
    );
    assert_eq!(
        free_collateral_of(&assets("BTC", "1")).unwrap(),
        Decimal::ZERO
    );
}

fn position(symbol: &str, side: &str, total: &str, margin: &str, hold: &str) -> PositionRow {
    PositionRow {
        symbol: Some(symbol.into()),
        pos_side: Some(side.into()),
        total: Some(total.into()),
        avg_price: Some("180".into()),
        mark_price: Some("181".into()),
        margin_mode: Some(margin.into()),
        hold_mode: Some(hold.into()),
        liquidation_price: Some("120".into()),
        position_balance: Some("120.5".into()),
    }
}

#[test]
fn positions_are_net_one_way_and_unambiguous() {
    let rows = vec![
        position("NVDAUSDT", "short", "2", "isolated", "one_way_mode"),
        position("TSLAUSDT", "long", "0", "isolated", "one_way_mode"),
    ];
    let positions = positions_of(&rows).unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].net_quantity, dec!(-2));
    assert_eq!(positions[0].notional_usdt, dec!(362));
    assert!(positions_of(&[position("NVDAUSDT", "long", "1", "isolated", "hedge_mode")]).is_err());
    assert!(
        positions_of(&[
            position("NVDAUSDT", "long", "1", "isolated", "one_way_mode"),
            position("NVDAUSDT", "short", "1", "isolated", "one_way_mode"),
        ])
        .is_err()
    );
    assert_eq!(
        isolated_pos_side(&rows, "NVDAUSDT").unwrap(),
        "short",
        "补保证金带持仓方向"
    );
    assert!(
        isolated_pos_side(
            &[position("NVDAUSDT", "long", "1", "crossed", "one_way_mode")],
            "NVDAUSDT"
        )
        .is_err()
    );
    assert!(isolated_pos_side(&rows, "TSLAUSDT").is_err());
}

/// Read-only probe of the real unified account in `ARB_BITGET_API_KEY` / `ARB_BITGET_API_SECRET`
/// / `ARB_BITGET_PASSPHRASE`: no write request is possible (trading disabled). Runs the very
/// `connect` the live desk runs (all-or-nothing: if it fails here, the whole live desk would fail
/// to connect), then every read the bot relies on — even when `connect` failed. Journal in
/// `ARB_PROBE_DIR` (default: the system temp dir). Credentials come from the environment or, failing
/// that, the repo `.env`. Run with `cargo test -p arb-exec --lib
/// bitget_uta_broker::tests::live_read_only_probe -- --ignored --nocapture`.
#[test]
fn a_reduce_only_order_below_the_minimum_closes_the_whole_dust_position_or_nothing() {
    // 某次实盘：剩 0.001（约 0.82 USDT），每次退出都被本地 5 USDT 下限拦下。
    assert_eq!(
        dust_rule(true, dec!(0.001), dec!(0.001)),
        DustRule::CloseWhole
    );
    assert_eq!(
        dust_rule(true, dec!(0.001), dec!(-0.001)),
        DustRule::CloseWhole
    );
    assert_eq!(
        dust_rule(true, dec!(0.001), dec!(0.003)),
        DustRule::Refuse,
        "只平一部分会留下更小的零头"
    );
    assert_eq!(dust_rule(false, dec!(0.101), dec!(0.102)), DustRule::Place);

    let ok = StatusCode::OK;
    let placed = json!({"code": "00000", "msg": "success", "data": {"list": [
        {"orderId": "123", "clientOid": "venue-made", "code": "", "msg": ""}
    ]}});
    assert_eq!(dust_reply(ok, &placed), DustReply::Placed("123".into()));
    // 文档示例：外层成功，里面这一条失败。
    let failed = json!({"code": "00000", "msg": "success", "data": {"list": [
        {"orderId": "111111111111111111", "clientOid": "111111111111111111", "code": "24056", "msg": "notExisted"}
    ]}});
    assert_eq!(
        dust_reply(ok, &failed),
        DustReply::Refused("24056".into(), "notExisted".into())
    );
    let busy = json!({"code": "00000", "data": {"list": [{"orderId": "1", "code": "25003"}]}});
    assert_eq!(
        dust_reply(ok, &busy),
        DustReply::Unknown,
        "结果不明的码不当拒单"
    );
    assert_eq!(
        dust_reply(ok, &json!({"code": "00000", "data": {"list": []}})),
        DustReply::Unknown
    );
    assert_eq!(
        dust_reply(
            StatusCode::BAD_REQUEST,
            &json!({"code": "45110", "msg": "less than the minimum order amount"})
        ),
        DustReply::Refused("45110".into(), "less than the minimum order amount".into())
    );
    assert_eq!(
        dust_reply(StatusCode::BAD_GATEWAY, &json!({})),
        DustReply::Unknown
    );
}

#[tokio::test]
#[ignore = "needs live Bitget credentials"]
async fn live_read_only_probe() {
    use std::time::Instant;
    let _ = dotenvy::dotenv();
    let var = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    let credentials = || BitgetCredentials {
        api_key: var("ARB_BITGET_API_KEY").expect("ARB_BITGET_API_KEY"),
        api_secret: var("ARB_BITGET_API_SECRET").expect("ARB_BITGET_API_SECRET"),
        passphrase: var("ARB_BITGET_PASSPHRASE").expect("ARB_BITGET_PASSPHRASE"),
    };
    println!(
        "ARB_BITGET_ACCOUNT: {:?}",
        BitgetAccount::parse(var("ARB_BITGET_ACCOUNT").as_deref())
    );
    let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let options = LiveOptions {
        trading_enabled: false,
        market_slippage: None,
    };
    let client = reqwest::Client::new();
    let ms = |since: Instant| since.elapsed().as_millis();
    let started = Instant::now();
    let connected = BitgetUtaBroker::connect(
        client.clone(),
        credentials(),
        &dir.join("arb-live-bitget-uta-probe-orders.jsonl"),
        options.clone(),
    )
    .await;
    println!(
        "connect (what the live desk runs): {} ({} ms)",
        match &connected {
            Ok(broker) => format!("OK, account taker fee {}", broker.fee_per_side),
            Err(failure) => format!("FAILED — the live desk would not connect: {failure}"),
        },
        ms(started)
    );
    let broker = match connected {
        Ok(broker) => broker,
        Err(_) => {
            let journal = OrderJournal::open(
                &dir.join("arb-live-bitget-uta-probe-raw.jsonl"),
                "bitget-uta-probe-raw",
            )
            .unwrap();
            let mut broker = BitgetUtaBroker::assemble(client, credentials(), options, journal);
            broker.time_offset_ms = broker.server_offset().await.unwrap_or(0);
            broker
        }
    };
    println!("clock offset: {} ms", broker.time_offset_ms);
    // Needs no permission: shows the key's permission list and IP whitelist.
    let info: ArbResult<Value> = broker
        .signed_data(Method::GET, "/api/v3/account/info", &[], None)
        .await;
    match info {
        Ok(info) => println!(
            "key: permType {} permissions {} ips {}（需要 uta_trade + uta_mgt 读写，不要 withdraw）",
            info["permType"], info["permissions"], info["ips"]
        ),
        Err(failure) => println!("account info: {failure}"),
    }
    match broker.account_settings().await {
        Ok(s) => println!(
            "settings: accountMode {:?} (needs unified), accountLevel {:?} (basic / isolated), holdMode {:?} (needs one_way_mode), deltaSwitch {:?} (must not be yes), {} symbol configs",
            s.account_mode,
            s.account_level,
            s.hold_mode,
            s.delta_switch,
            s.symbol_config_list.len()
        ),
        Err(failure) => println!("settings: {failure}"),
    }
    let deduct: ArbResult<Value> = broker
        .signed_data(Method::GET, "/api/v3/account/deduct-info", &[], None)
        .await;
    println!("BGB fee deduction (must be off): {deduct:?}");
    let rates: ArbResult<Vec<TradeRate>> = broker
        .signed_data(
            Method::GET,
            PATH_ALL_FEE_RATE,
            &[("category", CATEGORY.to_string())],
            None,
        )
        .await;
    match rates {
        Ok(rows) => {
            let mut distinct: Vec<&str> =
                rows.iter().map(|row| row.taker_fee_rate.as_str()).collect();
            distinct.sort_unstable();
            distinct.dedup();
            println!(
                "fee rows: {} (distinct taker rates: {distinct:?})",
                rows.len()
            );
        }
        Err(failure) => println!("all-fee-rate: {failure}"),
    }
    println!(
        "free collateral (USDT available): {:?}",
        broker.free_collateral().await
    );
    broker.instruments.clear();
    let nvda = Symbol::perp("NVDA", "USDT");
    for round in ["cold", "cached"] {
        let started = Instant::now();
        let found = broker.instrument_for(&nvda, false).await;
        println!(
            "NVDAUSDT instrument ({round}): {:?} ({} ms)",
            found.map(|i| (i.native, i.rwa, i.tick, i.step, i.min_notional, i.max_lever)),
            ms(started)
        );
    }
    for round in ["first", "second"] {
        let started = Instant::now();
        let (positions, orders) = tokio::join!(broker.positions(), broker.open_orders());
        println!(
            "positions + open orders ({round}): {:?} / {:?} ({} ms)",
            positions.map(|rows| rows
                .iter()
                .map(|row| (row.symbol.base.clone(), row.net_quantity))
                .collect::<Vec<_>>()),
            orders.map(|rows| rows.len()),
            ms(started)
        );
    }
    let btc = Symbol::perp("BTC", "USDT");
    for symbol in [&nvda, &btc] {
        println!(
            "{} halted now (open / close): {:?} / {:?}",
            symbol.base,
            broker.trading_halted(symbol, false).await,
            broker.trading_halted(symbol, true).await
        );
    }
    for round in ["first", "second"] {
        let started = Instant::now();
        let warmed = broker.warm_reads(&nvda, Some(dec!(3))).await;
        println!(
            "warm_reads(NVDA, 3x) {round}: {warmed:?} ({} ms)",
            ms(started)
        );
    }
    // Raw shapes of the account lists (they carry no secret), so a parser mismatch shows itself.
    for (path, params) in [
        (PATH_POSITIONS, vec![("category", CATEGORY.to_string())]),
        (
            PATH_FILLS,
            vec![
                ("category", CATEGORY.to_string()),
                ("limit", "5".to_string()),
            ],
        ),
        (PATH_UNFILLED, vec![("category", CATEGORY.to_string())]),
    ] {
        let raw = broker
            .request_raw(true, Method::GET, path, &params, None)
            .await
            .map(|(status, body)| {
                let text = body.to_string();
                format!("{status} {}", &text[..text.len().min(800)])
            });
        println!("raw {path}: {raw:?}");
    }
    println!("NVDA leg state: {:?}", broker.leg_state(&nvda).await);
    println!(
        "order lookup of an unknown id: {:?}",
        broker
            .fetch_order("bgprobeunknown")
            .await
            .map(|row| row.is_some())
    );
    let now = Utc::now();
    let started = Instant::now();
    println!(
        "NVDA funding since 30 days: {:?} ({} ms)",
        broker
            .funding_since(&nvda, now - chrono::Duration::days(30))
            .await,
        ms(started)
    );
    for days in [1, 20] {
        let started = Instant::now();
        let fills = broker
            .fills_between(&nvda, now - chrono::Duration::days(days), now)
            .await;
        println!(
            "NVDA fills over {days} day(s): {:?} ({} ms)",
            fills.map(|rows| rows.map(|rows| rows.len())),
            ms(started)
        );
    }
    // `ARB_PROBE_FILLS=某合约,...`: every fill of these symbols over the last day, as the desk sees it.
    for base in var("ARB_PROBE_FILLS").unwrap_or_default().split(',') {
        let base = base.trim();
        if base.is_empty() {
            continue;
        }
        let symbol = Symbol::perp(base, "USDT");
        match broker
            .fills_between(&symbol, now - chrono::Duration::days(1), now)
            .await
        {
            Ok(Some(rows)) => {
                for fill in &rows {
                    println!(
                        "{base} fill {} {:?} {} @ {} fee {} {:?}",
                        fill.at, fill.side, fill.quantity, fill.price, fill.fee_usdt, fill.effect
                    );
                }
                println!("{base}: {} fill(s) in the last day", rows.len());
            }
            other => println!("{base} fills: {other:?}"),
        }
    }
    println!(
        "funding since 100 days (beyond retention, must refuse): {:?}",
        broker
            .funding_since(&nvda, now - chrono::Duration::days(100))
            .await
            .map(|total| total.map(|total| total.payments))
    );
}

/// 零头整笔平仓的交易所订单号写进旁路文件，重启后读得回来；坏行跳过。
#[test]
fn dust_close_order_ids_survive_a_restart() {
    let dir = std::env::temp_dir().join(format!("arb-bitget-dust-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let journal_path = dir.join("arb-live-bitget-orders.jsonl");
    let side = dust_path(&journal_path);
    let _ = std::fs::remove_file(&side);
    let _ = std::fs::remove_file(&journal_path);
    let journal = OrderJournal::open(&journal_path, "bitget:test").unwrap();
    let mut broker = BitgetUtaBroker::assemble(
        reqwest::Client::new(),
        BitgetCredentials {
            api_key: "k".into(),
            api_secret: "s".into(),
            passphrase: "p".into(),
        },
        LiveOptions {
            trading_enabled: false,
            market_slippage: None,
        },
        journal,
    );
    broker.dust_path = Some(side.clone());
    let id = ClientOrderId("live-1-sell-exit1".into());
    broker.remember_dust_order(&id, "1234567890");
    assert_eq!(broker.dust_order_id(&id).as_deref(), Some("1234567890"));
    std::fs::OpenOptions::new()
        .append(true)
        .open(&side)
        .and_then(|mut file| {
            use std::io::Write;
            writeln!(file, "not json")
        })
        .unwrap();
    let loaded = load_dust_orders(&side);
    assert_eq!(
        loaded.get("live-1-sell-exit1").map(String::as_str),
        Some("1234567890")
    );
    assert_eq!(loaded.len(), 1, "坏行跳过");
    assert!(load_dust_orders(&dir.join("missing")).is_empty());
}
