//! MEXC / Bitget 的真实 OHLC 历史；不能用只有收盘价的 Candle 伪造开高低。
//!
//! 官方规则：
//! - https://www.mexc.com/api-docs/futures/market-endpoints/get-candlestick-data
//!   列式 OHLC、秒时间戳、每次最多 2000 根，start/end 都是秒。
//! - https://www.bitget.com/docs/catalog/classic-contract-market/classic-contract-market#get-historical-candlestick
//!   history-candles 只含已完成 K 线、毫秒时间戳、每页最多 200 根；单次区间最多 90 天。

use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use arb_core::Venue;
use serde_json::Value;
use tokio::sync::Mutex;

use super::{Bar, MAX_BARS, array_bar, finish, get, interval_minutes, num};

/// 股票合约名不一定是标准代码（NVDA → NVIDIA_USDT）；必须和扫描、券商用同一套映射。
async fn mexc_symbol(base: &str) -> Result<String> {
    type Names = Option<(Instant, HashMap<String, String>)>;
    static NAMES: OnceLock<Mutex<Names>> = OnceLock::new();
    let mut cache = NAMES.get_or_init(|| Mutex::new(None)).lock().await;
    if cache
        .as_ref()
        .is_none_or(|(at, _)| at.elapsed() >= Duration::from_secs(600))
    {
        let names = arb_venues::mexc::fetch_bases(super::client()).await?;
        *cache = Some((Instant::now(), names));
    }
    native_mexc(&cache.as_ref().context("MEXC 合约名缓存为空")?.1, base)
}

fn native_mexc(names: &HashMap<String, String>, base: &str) -> Result<String> {
    let mut matches = names.iter().filter(|(_, name)| name.as_str() == base);
    let native = matches
        .next()
        .map(|(native, _)| native.clone())
        .ok_or_else(|| anyhow!("MEXC 没有 {base} USDT 永续"))?;
    ensure!(matches.next().is_none(), "MEXC {base} 对应不止一个合约");
    Ok(native)
}

fn timestamp(value: &Value) -> Result<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|t| *t > 0)
        .context("K 线时间戳无效")
}

fn positive(value: &Value) -> Result<f64> {
    num(value)
        .filter(|p| *p > 0.0)
        .context("K 线 OHLC 价格缺失、非有限或非正")
}

fn mexc_page(v: &Value) -> Result<Vec<Bar>> {
    ensure!(
        v["success"].as_bool() == Some(true) && v["code"].as_i64() == Some(0),
        "MEXC K 线业务失败：code={} message={}",
        v["code"],
        v["message"]
    );
    let data = &v["data"];
    let time = data["time"].as_array().context("MEXC 缺少 time 列")?;
    let columns: Vec<&Vec<Value>> = ["open", "high", "low", "close"]
        .into_iter()
        .map(|name| {
            let col = data[name]
                .as_array()
                .with_context(|| format!("MEXC 缺少 {name} 列"))?;
            ensure!(
                col.len() == time.len(),
                "MEXC K 线列长度不一致：time={} {name}={}",
                time.len(),
                col.len()
            );
            Ok(col)
        })
        .collect::<Result<_>>()?;
    time.iter()
        .enumerate()
        .map(|(i, t)| {
            Ok(Bar {
                t: timestamp(t)?
                    .checked_mul(1000)
                    .context("MEXC 秒时间戳溢出")?,
                o: positive(&columns[0][i])?,
                h: positive(&columns[1][i])?,
                l: positive(&columns[2][i])?,
                c: positive(&columns[3][i])?,
            })
        })
        .collect()
}

fn bitget_page(v: &Value) -> Result<Vec<Bar>> {
    ensure!(
        v["code"].as_str() == Some("00000"),
        "Bitget K 线业务失败：code={} msg={}",
        v["code"],
        v["msg"]
    );
    v["data"]
        .as_array()
        .context("Bitget K 线缺少 data 数组")?
        .iter()
        .map(|row| {
            let cells = row.as_array().context("Bitget K 线不是数组")?;
            let mut bar = array_bar(row).context("Bitget K 线缺少有效 OHLC")?;
            bar.t = timestamp(cells.first().context("Bitget K 线没有时间戳")?)?;
            for value in cells.iter().skip(1).take(4) {
                positive(value)?;
            }
            Ok(bar)
        })
        .collect()
}

fn request_start(start_ms: i64, end_ms: i64, step: i64) -> Result<i64> {
    ensure!(start_ms > 0 && start_ms <= end_ms, "K 线时间范围无效");
    // 只取末尾 MAX_BARS 根；即使调用者给了很长的范围，分页数仍然有界。
    Ok(start_ms
        .max(end_ms.saturating_sub(step * MAX_BARS as i64))
        .div_euclid(step)
        * step)
}

pub(super) async fn fetch(
    venue: Venue,
    base: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<Bar>> {
    match venue {
        Venue::Mexc => {
            let native = mexc_symbol(base).await?;
            mexc_with(&native, interval, start_ms, end_ms, |url| get(url, venue)).await
        }
        Venue::Bitget => bitget_with(base, interval, start_ms, end_ms, |url| get(url, venue)).await,
        _ => Err(anyhow!("不是 MEXC / Bitget")),
    }
}

async fn mexc_with<F, Fut>(
    native: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
    mut fetch: F,
) -> Result<Vec<Bar>>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let minutes = interval_minutes(interval).context("不支持的 MEXC K 线周期")?;
    let granularity = match minutes {
        1 => "Min1",
        5 => "Min5",
        15 => "Min15",
        30 => "Min30",
        60 => "Min60",
        240 => "Hour4",
        _ => return Err(anyhow!("不支持的 MEXC K 线周期")),
    };
    let step = i64::from(minutes) * 60_000;
    let mut from = request_start(start_ms, end_ms, step)?;
    let mut out = Vec::new();
    while from <= end_ms {
        let to = (from + step * 1999).min(end_ms);
        let v = fetch(format!("https://contract.mexc.com/api/v1/contract/kline/{native}?interval={granularity}&start={}&end={}", from / 1000, to / 1000)).await?;
        let page = mexc_page(&v)?;
        ensure!(
            page.iter().all(|bar| bar.t >= from && bar.t <= to),
            "MEXC K 线返回请求区间外的数据"
        );
        out.extend(page);
        from = (to.div_euclid(step) + 1) * step;
    }
    Ok(finish(out, start_ms, end_ms))
}

async fn bitget_with<F, Fut>(
    base: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
    mut fetch: F,
) -> Result<Vec<Bar>>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let minutes = interval_minutes(interval).context("不支持的 Bitget K 线周期")?;
    let granularity = match minutes {
        60 => "1H",
        240 => "4H",
        _ => interval,
    };
    let step = i64::from(minutes) * 60_000;
    let start = request_start(start_ms, end_ms, step)?;
    let mut to = end_ms.div_euclid(step) * step;
    let mut out = Vec::new();
    while to > start {
        // history-candles 的 endTime 是排他的：每页 [from, to)，下一页的 to 就是 from。
        // 多减一个周期会在每页边界漏一根，24 天小时线会少两根。
        let from = (to - step * 200).max(start);
        let v = fetch(format!("https://api.bitget.com/api/v2/mix/market/history-candles?symbol={base}USDT&productType=USDT-FUTURES&granularity={granularity}&startTime={from}&endTime={to}&limit=200")).await?;
        let page = bitget_page(&v)?;
        // 官方允许因端点取整多回前一根；除此之外不接受忽略时间范围的响应。
        ensure!(
            page.iter().all(|bar| bar.t >= from - step && bar.t <= to),
            "Bitget K 线返回请求区间外的数据"
        );
        out.extend(page.into_iter().filter(|bar| bar.t >= from));
        to = from;
        if to > start {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(finish(out, start_ms, end_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn mexc_columns_keep_real_ohlc_and_convert_seconds_to_milliseconds() {
        let v = json!({"success":true,"code":0,"data":{"time":[1700000000],"open":[100],"high":[120],"low":[90],"close":[110]}});
        let b = mexc_page(&v).unwrap()[0];
        assert_eq!(
            (b.t, b.o, b.h, b.l, b.c),
            (1700000000000, 100.0, 120.0, 90.0, 110.0)
        );
        for field in ["time", "open", "high", "low", "close"] {
            let mut bad = v.clone();
            bad["data"][field] = json!([]);
            assert!(mexc_page(&bad).is_err(), "{field}");
        }
        assert!(mexc_page(&json!({"success":false,"code":600,"message":"error"})).is_err());
        assert!(mexc_page(&json!({"success":true,"code":0})).is_err());
    }

    #[test]
    fn bitget_rows_validate_business_errors_and_do_not_fake_prices() {
        let v = json!({"code":"00000","data":[["1700000000000","100","120","90","110","42"]]});
        let b = bitget_page(&v).unwrap()[0];
        assert_eq!(
            (b.t, b.o, b.h, b.l, b.c),
            (1700000000000, 100.0, 120.0, 90.0, 110.0)
        );
        for raw in [
            json!({"code":"40017","data":[]}),
            json!({"code":"00000","data":null}),
            json!({"code":"00000","data":[["1","100","120","90"]]}),
        ] {
            assert!(bitget_page(&raw).is_err());
        }
        for value in ["0", "-1", "NaN", "inf"] {
            let mut bad = v.clone();
            bad["data"][0][2] = json!(value);
            assert!(bitget_page(&bad).is_err());
        }
    }

    #[test]
    fn mexc_stock_names_match_the_scanner_and_unknown_names_fail_closed() {
        let mut names = HashMap::from([
            ("NVIDIA_USDT".into(), "NVDA".into()),
            ("BTC_USDT".into(), "BTC".into()),
        ]);
        assert_eq!(native_mexc(&names, "NVDA").unwrap(), "NVIDIA_USDT");
        assert_eq!(native_mexc(&names, "BTC").unwrap(), "BTC_USDT");
        assert!(native_mexc(&names, "MISSING").is_err());
        names.insert("OTHER_USDT".into(), "NVDA".into());
        assert!(native_mexc(&names, "NVDA").is_err());
    }

    fn params(url: &str) -> HashMap<String, String> {
        reqwest::Url::parse(url)
            .unwrap()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }

    #[tokio::test]
    async fn mexc_paginates_over_2000_bars_with_second_units_and_no_gaps() {
        let step = 3_600_000i64;
        let start = 1_700_000_000_000i64 / step * step;
        let end = start + step * 2500;
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bars=mexc_with("NVIDIA_USDT","1h",start,end,|url| {
            let calls=Arc::clone(&calls);
            async move {
                assert!(url.contains("/NVIDIA_USDT?interval=Min60"));
                let q=params(&url);let from:i64=q["start"].parse()?;let to:i64=q["end"].parse()?;
                calls.lock().unwrap().push((from,to));
                let ts:Vec<i64>=(from..=to).step_by(3600).collect(); let prices=vec![100.0;ts.len()];
                Ok(json!({"success":true,"code":0,"data":{"time":ts,"open":prices,"high":prices,"low":prices,"close":prices}}))
            }
        }).await.unwrap();
        assert_eq!(bars.len(), 2501);
        assert_eq!(calls.lock().unwrap().len(), 2);
        assert_eq!((bars[0].t, bars.last().unwrap().t), (start, end));
        assert!(bars.windows(2).all(|w| w[1].t - w[0].t == step));
    }

    #[tokio::test]
    async fn bitget_paginates_in_either_order_and_keeps_24_days_not_200_hours() {
        let step = 3_600_000i64;
        let start = 1_700_000_000_000i64 / step * step;
        let end = start + step * 576;
        let calls = Arc::new(std::sync::Mutex::new(0usize));
        let bars = bitget_with("BTC", "1h", start, end, |url| {
            let calls = Arc::clone(&calls);
            async move {
                let q = params(&url);
                assert_eq!(q["granularity"], "1H");
                assert_eq!(q["limit"], "200");
                let from: i64 = q["startTime"].parse()?;
                let to: i64 = q["endTime"].parse()?;
                assert!((to - from) / step <= 200);
                let mut rows: Vec<Value> = (from..to)
                    .step_by(step as usize)
                    .map(|t| json!([t.to_string(), "100", "101", "99", "100"]))
                    .collect();
                let mut n = calls.lock().unwrap();
                *n += 1;
                if (*n).is_multiple_of(2) {
                    rows.reverse();
                }
                Ok(json!({"code":"00000","data":rows}))
            }
        })
        .await
        .unwrap();
        assert_eq!(bars.len(), 576);
        assert_eq!(*calls.lock().unwrap(), 3);
        assert_eq!((bars[0].t, bars.last().unwrap().t), (start, end - step));
        assert!(bars.windows(2).all(|w| w[1].t - w[0].t == step));
    }

    #[tokio::test]
    async fn repeated_out_of_window_pages_are_errors_not_false_history() {
        let step = 3_600_000i64;
        let start = 1_700_000_000_000i64 / step * step;
        let result=bitget_with("BTC","1h",start,start+step*400,|_|async move {
            Ok(json!({"code":"00000","data":[[(start+step*400).to_string(),"100","101","99","100"]]}))
        }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore = "只读公开 API 联网核验"]
    async fn live_contract_history() {
        let end = chrono::Utc::now().timestamp_millis();
        let start = end - 24 * 86_400_000;
        for (venue, base) in [
            (Venue::Mexc, "GIGADEV"),
            (Venue::Mexc, "NVDA"),
            (Venue::Bitget, "BTC"),
            (Venue::Bitget, "UNITREE"),
        ] {
            let bars = fetch(venue, base, "1h", start, end).await.unwrap();
            assert!(bars.len() >= 24, "{venue} {base}: {}", bars.len());
            assert!(end - bars.last().unwrap().t < 3 * 3_600_000);
            if base == "BTC" {
                assert!(
                    bars.windows(2).all(|w| w[1].t - w[0].t == 3_600_000),
                    "分页边界不能漏根"
                );
            }
            println!(
                "{venue} {base}: {} bars; first={} last={}",
                bars.len(),
                bars[0].t,
                bars.last().unwrap().t
            );
        }
    }
}
