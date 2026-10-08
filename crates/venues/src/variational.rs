//! Variational Omni 的公共 RFQ 行情。
//!
//! `/metadata/stats` 的 `funding_rate` 是小数形式的年化单利，而不是每期费率。
//! 核实依据：官方 Pre-IPO 文档规定每 8h 收 0.005%，实测 OPENAI / ANTHROPIC
//! 返回 `0.05475`，恰好等于 `0.00005 × 3 × 365`。因此逐行按
//! `period_rate = funding_rate × interval_h / (365 × 24)` 还原，不能直接透传。
//!
//! 周期来自 `funding_interval_s`，实测永续同时有 1h / 4h / 8h；另有名称为
//! `Swap on ...`、周期为 0 的传统金融 swap，其双边融资机制不同，不能当永续。
//! API 没有下次结算时刻，按 UTC 周期边界估算并显式标记；报价更新时间不是结算时刻。
//!
//! # 为什么计价资产写 USDT，尽管结算资产是 USDC
//!
//! `Symbol` 的 quote 表示的是**报价单位**，不是抵押/结算资产。该场所的
//! `mark_price` 是美元价（实测 BTC `81220.81`，与 Binance 同刻的 `81241` 同一量纲），
//! 两条腿之间**没有换汇动作**，所以报价单位就是 USDT。
//!
//! 写成 USDC 会让这个场所配不上任何 USDT 场所，或者让**每一条**配对都被打上
//! `quote_mismatch` 标记 —— 标记一旦常态出现就不再是警告。USDC 抵押这件事是
//! 组合层面的风险（资金实际落在 USDC 上），记在这里，不塞进 Symbol。
//!
//! `mark_price` 可用，指数价与吃单费率字段不可用。RFQ spread 不是吃单手续费。
//! 持仓量的单边/双边口径未核实，不把它塞进 USDT 持仓量字段。

use arb_core::{
    ArbResult, DEFAULT_FUNDING_INTERVAL_H, Decimal, MarketSnapshot, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Variational;
const STATS_URL: &str = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats";
const QUOTE: &str = "USDT";

pub struct VariationalApi {
    client: Client,
}

impl VariationalApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[derive(Debug, Deserialize)]
struct Stats {
    // 单行坏字段不能让其它市场一起消失；顶层结构错误仍由 get_json 报错。
    listings: Vec<Value>,
}

#[async_trait]
impl VenueApi for VariationalApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let stats: Stats = get_json(self.client.get(STATS_URL), VENUE).await?;
        let now = Utc::now();
        let mut out = Vec::with_capacity(stats.listings.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for item in stats.listings {
            if is_swap(&item) {
                filtered += 1;
                continue;
            }
            match parse_listing(&item, now) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非永续的 swap 已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
        Ok(out)
    }
}

fn is_swap(item: &Value) -> bool {
    item.get("name")
        .and_then(Value::as_str)
        .is_some_and(|name| name.starts_with("Swap on "))
}

fn parse_listing(item: &Value, now: DateTime<Utc>) -> Option<MarketSnapshot> {
    if is_swap(item) {
        return None;
    }
    let base = item.get("ticker")?.as_str()?.trim();
    // RE_ETH / OPN_OPINION 是实测 ticker，不能照搬 CEX 的「下划线即交割」规则。
    if base.is_empty() || !base.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return None;
    }
    let apr = parse_decimal(item.get("funding_rate")?.as_str()?)?;
    let (interval_h, interval_assumed) = match item.get("funding_interval_s") {
        None | Some(Value::Null) => (DEFAULT_FUNDING_INTERVAL_H, true),
        Some(value) => {
            let seconds = value.as_u64()?;
            // 明确的 0 或不足整小时不是「没给」，不可回落成正常永续周期。
            if seconds == 0 || seconds % 3600 != 0 {
                return None;
            }
            (u32::try_from(seconds / 3600).ok()?, false)
        }
    };
    let period_rate = apr_to_period_rate(apr, interval_h)?;
    let interval_s = i64::from(interval_h).checked_mul(3600)?;
    // next = (floor(now / (interval_h × 3600)) + 1) × (interval_h × 3600)。
    // 这里只估算 UTC 周期边界，不把 now 或缓存的 quotes.updated_at 冒充结算时刻。
    let next_timestamp = now
        .timestamp()
        .div_euclid(interval_s)
        .checked_add(1)?
        .checked_mul(interval_s)?;
    let next_funding_at = DateTime::from_timestamp(next_timestamp, 0)?;

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: true,
        taker_fee: None,
        mark_price: item
            .get("mark_price")
            .and_then(Value::as_str)
            .and_then(parse_decimal),
        index_price: None,
        best_bid: None,
        best_ask: None,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt: None,
        quote_volume_24h: item
            .get("volume_24h")
            .and_then(Value::as_str)
            .and_then(parse_decimal),
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

fn apr_to_period_rate(apr: Decimal, interval_h: u32) -> Option<Decimal> {
    if interval_h == 0 {
        return None;
    }
    // 先乘后除保留精度；坏响应即使超出 Decimal 范围也只丢这一行，不 panic。
    apr.checked_mul(Decimal::from(interval_h))?
        .checked_div(Decimal::from(8760u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // 2026-09-19 实测 /metadata/stats 的原始字段片段，保留字符串精度。
    const FIXTURE: &str = r#"{"listings":[
        {"ticker":"SYRUP","name":"Maple Finance","funding_rate":"0.1095","funding_interval_s":14400,"mark_price":"0.2222116687245987","volume_24h":"152421.268196"},
        {"ticker":"BTC","name":"Bitcoin","funding_rate":"0.1095","funding_interval_s":28800,"mark_price":"81220.8119416714","volume_24h":"610323706.123309"},
        {"ticker":"ONE","name":"Harmony","funding_rate":"-78.663098","funding_interval_s":3600,"mark_price":"0.002452321797249536","volume_24h":"115965.165155"},
        {"ticker":"OPENAI","name":"OpenAI","funding_rate":"0.05475","funding_interval_s":28800,"mark_price":"1506.766180594784","volume_24h":"721581.985310"},
        {"ticker":"XAUS","name":"Swap on Gold Spot","funding_rate":"0","funding_interval_s":0,"mark_price":"4378.29","volume_24h":"153150177.885947"}
    ]}"#;

    fn fixture() -> Stats {
        serde_json::from_str(FIXTURE).unwrap()
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-19T13:17:08Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn decimal(raw: &str) -> Decimal {
        parse_decimal(raw).unwrap()
    }

    #[test]
    fn live_apr_is_converted_per_listing_not_used_as_a_period_rate() {
        let stats = fixture();
        let syrup = parse_listing(&stats.listings[0], now()).unwrap();
        let btc = parse_listing(&stats.listings[1], now()).unwrap();
        let one = parse_listing(&stats.listings[2], now()).unwrap();
        let pre_ipo = parse_listing(&stats.listings[3], now()).unwrap();
        assert_eq!(syrup.period_rate, decimal("0.00005"));
        assert_eq!(btc.period_rate, decimal("0.0001"));
        assert_eq!(one.period_rate, decimal("-0.0089798057077625570776255708"));
        assert_eq!(pre_ipo.period_rate, decimal("0.00005"));
        assert_eq!(
            (syrup.interval_h, btc.interval_h, one.interval_h),
            (4, 8, 1)
        );
        assert!(!syrup.interval_assumed && !btc.interval_assumed && !one.interval_assumed);
        assert_eq!(
            apr_to_period_rate(decimal("0.1095"), 1),
            Some(decimal("0.0000125"))
        );
        for hours in [1, 4, 8] {
            let apr = decimal("0.1095");
            let period = apr_to_period_rate(apr, hours).unwrap();
            assert_eq!(period * Decimal::from(8760u32) / Decimal::from(hours), apr);
        }
        assert_eq!(btc.symbol.quote, "USDT");
        assert_eq!(btc.taker_fee, None);
    }

    #[test]
    fn missing_or_bad_rate_drops_only_that_row_and_reported_zero_survives() {
        let mut row = fixture().listings.remove(0);
        row.as_object_mut().unwrap().remove("funding_rate");
        assert!(parse_listing(&row, now()).is_none());
        for bad in [Value::Null, json!(""), json!("n/a"), json!(0.1095)] {
            row["funding_rate"] = bad;
            assert!(parse_listing(&row, now()).is_none());
        }
        row["funding_rate"] = json!("0");
        assert_eq!(
            parse_listing(&row, now()).unwrap().period_rate,
            Decimal::ZERO
        );
    }

    #[test]
    fn absent_interval_is_flagged_but_invalid_reported_intervals_are_rejected() {
        let mut row = fixture().listings.remove(0);
        row.as_object_mut().unwrap().remove("funding_interval_s");
        let assumed = parse_listing(&row, now()).unwrap();
        assert!(assumed.interval_assumed);
        assert_eq!(assumed.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert_eq!(assumed.period_rate, decimal("0.0001"));
        for bad in [json!(0), json!(-3600), json!(1800), json!("14400")] {
            row["funding_interval_s"] = bad;
            assert!(parse_listing(&row, now()).is_none());
        }
    }

    #[test]
    fn swaps_are_not_zero_funding_perpetuals() {
        let row = fixture().listings.remove(4);
        assert!(is_swap(&row));
        assert!(parse_listing(&row, now()).is_none());
    }

    #[test]
    fn estimated_times_use_each_utc_window_and_advance_at_an_exact_boundary() {
        let stats = fixture();
        let four = parse_listing(&stats.listings[0], now()).unwrap();
        let one = parse_listing(&stats.listings[2], now()).unwrap();
        assert!(four.next_funding_estimated);
        for rate in [&four, &one] {
            assert!(rate.next_funding_at > now());
            assert_eq!(
                rate.next_funding_at.timestamp() % (i64::from(rate.interval_h) * 3600),
                0
            );
        }
        assert_eq!(
            four.next_funding_at.to_rfc3339(),
            "2026-09-19T16:00:00+00:00"
        );
        assert_eq!(
            one.next_funding_at.to_rfc3339(),
            "2026-09-19T14:00:00+00:00"
        );
        let next = parse_listing(&stats.listings[0], four.next_funding_at).unwrap();
        assert_eq!(
            next.next_funding_at.to_rfc3339(),
            "2026-09-19T20:00:00+00:00"
        );
    }

    #[test]
    fn missing_ticker_is_not_an_empty_symbol_and_overflow_drops_the_row() {
        let mut row = fixture().listings.remove(0);
        row.as_object_mut().unwrap().remove("ticker");
        assert!(parse_listing(&row, now()).is_none());
        row["ticker"] = json!("RE_ETH");
        assert_eq!(parse_listing(&row, now()).unwrap().symbol.base, "RE_ETH");
        row["funding_rate"] = json!(Decimal::MAX.to_string());
        assert!(parse_listing(&row, now()).is_none());
    }
}
