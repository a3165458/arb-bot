//! HTTP 基础设施：客户端构造与带场所上下文的 JSON 取数，外加限频冷却。

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Venue};
use reqwest::header::RETRY_AFTER;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::de::DeserializeOwned;

/// 构造共享 HTTP 客户端。
///
/// 所有连接器复用同一个客户端：连接池、TLS 会话、DNS 缓存都是共享的，
/// 一次扫描要并发打十几家 API，每家各建一个客户端等于每次扫描重建一遍 TLS。
pub fn build_client(timeout_sec: u64) -> ArbResult<Client> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_sec))
        .user_agent(concat!("arb-bot/", env!("CARGO_PKG_VERSION")))
        .gzip(true)
        // 307/308 会自动重发 POST。共享客户端也用于签名写请求，不能隐式再提交一次。
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(ArbError::from)
}

/// 冷却时长的上限：`Retry-After` 写多长都不会让一家场所停摆超过这么久。
const MAX_COOLDOWN: Duration = Duration::from_secs(300);

/// 碰到限频后默认冷却多久（上游没给 `Retry-After` 时）。
///
/// Lighter RH 按出口 IP 限频、额度最紧（同机别的程序也在用），其余场所更宽松；Aster 429 之后
/// 继续打会升级成封 IP，所以也放长一点。冷却期间这家场所的行情请求直接失败、不发出去。
pub fn default_cooldown(venue: Venue) -> Duration {
    Duration::from_secs(match venue {
        Venue::LighterRh => 120,
        Venue::Lighter | Venue::Aster => 60,
        _ => 30,
    })
}

/// 限频冷却表：场所 → 冷却到什么时候。进程内共享，扫描、预检、盘口、K 线走的是同一张表。
static COOLDOWNS: LazyLock<Mutex<HashMap<Venue, Instant>>> = LazyLock::new(Default::default);

/// 这家场所还要冷却多久；没在冷却返回 `None`。
pub fn cooling(venue: Venue) -> Option<Duration> {
    let mut table = COOLDOWNS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let until = *table.get(&venue)?;
    match until.checked_duration_since(Instant::now()) {
        Some(left) if !left.is_zero() => Some(left),
        _ => {
            table.remove(&venue);
            None
        }
    }
}

/// 让这家场所冷却 `wait`（上限 [`MAX_COOLDOWN`]）。已有更长的冷却不会被缩短。
pub fn cool_down(venue: Venue, wait: Duration) {
    let until = Instant::now() + wait.min(MAX_COOLDOWN);
    let mut table = COOLDOWNS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = table.entry(venue).or_insert(until);
    if until > *entry {
        *entry = until;
    }
}

/// 这个错误像不像限频。上游错误的实际样子：
/// - HTTP 非 2xx：`… 返回了业务错误：HTTP 429 Too Many Requests：{响应片段}`；
/// - Lighter 200 里带业务码：`… 返回了业务错误：盘口深度 code=23000`；
/// - Binance 无视 429 继续打会升级成 `HTTP 418`（封 IP）；Gate 的 `TOO_MANY_REQUESTS`。
pub fn looks_rate_limited(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("http 429")
        || lower.contains("http 418")
        || lower.contains("too many")
        || lower.contains("too_many")
        || lower.contains("rate limit")
        || lower.contains("code=23000")
        || lower.contains("\"code\":23000")
}

/// 一次请求没成功的原因。`transient`：值得原样再试一次（连接失败、超时、5xx）。
struct Failure {
    error: ArbError,
    transient: bool,
}

/// 发请求并反序列化，失败时带上场所名与响应片段。
///
/// 响应体要截取进错误里：交易所拒绝时（429 / 403 / IP 限频）真正的原因只在 body 里，
/// 只报 `HTTP 429` 会让人去查错方向。
///
/// 所有连接器的行情请求都从这里出去，所以限频保护只做在这一处：
/// - **冷却中的场所不发请求**，直接报错说明还剩多久。429 / 418 按 `Retry-After`（没有就按
///   [`default_cooldown`]）让这家场所冷却：被限频后每一轮扫描都再打一次，只会把限频升级成封禁。
/// - **连接失败、超时、5xx 只重试一次**，间隔 200 ~ 500 ms 抖动：行情请求都是只读的，这类
///   瞬时故障（实测 `502`、`error sending request` 每天都有几次）不该让整家场所从这一轮排名里消失。
///   Lighter RH 的额度太紧，不重试；429 从不重试。
pub async fn get_json<T: DeserializeOwned>(request: RequestBuilder, venue: Venue) -> ArbResult<T> {
    if let Some(left) = cooling(venue) {
        return Err(ArbError::venue(
            venue.as_str(),
            format!("限频冷却中，还剩 {} 秒，不发请求", left.as_secs().max(1)),
        ));
    }
    let spare = request.try_clone().filter(|_| venue != Venue::LighterRh);
    let body = match fetch_body(request, venue).await {
        Ok(body) => body,
        Err(Failure {
            transient: true, ..
        }) if spare.is_some() => {
            tokio::time::sleep(retry_delay()).await;
            match spare {
                Some(spare) => fetch_body(spare, venue)
                    .await
                    .map_err(|failure| failure.error)?,
                None => unreachable!("上面的守卫保证有备用请求"),
            }
        }
        Err(failure) => return Err(failure.error),
    };
    serde_json::from_str(&body).map_err(|source| ArbError::Decode {
        venue: venue.as_str(),
        source,
    })
}

/// 发一次请求，取回响应体。碰到限频就登记冷却。
async fn fetch_body(request: RequestBuilder, venue: Venue) -> Result<String, Failure> {
    let transient = |error: reqwest::Error| Failure {
        error: ArbError::from(error),
        transient: true,
    };
    let response = request.send().await.map_err(transient)?;
    let status = response.status();
    let retry_after = retry_after(&response);
    let body = response.text().await.map_err(transient)?;
    if status.is_success() {
        return Ok(body);
    }
    let rate_limited = matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::IM_A_TEAPOT
    );
    if rate_limited {
        let wait = retry_after.unwrap_or_else(|| default_cooldown(venue));
        cool_down(venue, wait);
        tracing::warn!(%venue, cooldown_sec = wait.min(MAX_COOLDOWN).as_secs(), "碰到限频：这家场所暂停取数");
    }
    Err(Failure {
        error: ArbError::venue(venue.as_str(), format!("HTTP {status}：{}", snippet(&body))),
        transient: status.is_server_error(),
    })
}

/// `Retry-After` 的秒数写法（HTTP 日期写法很少见，按没有处理）。
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// 重试前的等待：200 ~ 500 ms。用系统时钟的亚秒部分做抖动，不为这点随机引入依赖。
fn retry_delay() -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    Duration::from_millis(200 + u64::from(nanos % 300))
}

/// 截取响应片段用于报错。按字符截断，避免在多字节边界上 panic。
fn snippet(body: &str) -> String {
    const MAX_CHARS: usize = 200;
    let trimmed = body.trim();
    if trimmed.chars().count() <= MAX_CHARS {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(MAX_CHARS).collect();
    format!("{head}…")
}

/// 记「这一轮有几个合约字段不可用而被跳过」。这些数字扫描一次报一次、几乎不变，逐轮 WARN
/// 只会把日志灌满、把真正的告警淹掉：
/// - 进程里第一次见到：INFO（只是基线 —— 币安有几十个已下架但还在 `premiumIndex` 里的合约，
///   重启一次就 WARN 一次没有意义）；
/// - 比上一轮**多**了：WARN（上游格式变了或场所出了问题）；
/// - 变少了：INFO；没变：DEBUG。
pub fn note_unusable(venue: arb_core::Venue, unusable: usize) {
    static LAST: LazyLock<Mutex<HashMap<Venue, usize>>> = LazyLock::new(Default::default);
    let previous = LAST
        .lock()
        .map(|mut last| last.insert(venue, unusable))
        .unwrap_or(None);
    match previous {
        None => tracing::info!(%venue, unusable, "字段不可用的合约已跳过（本进程首轮基线）"),
        Some(before) if unusable > before => {
            tracing::warn!(%venue, unusable, before, "字段不可用的合约变多了");
        }
        Some(before) if unusable < before => {
            tracing::info!(%venue, unusable, before, "字段不可用的合约变少了");
        }
        Some(_) => tracing::debug!(%venue, unusable, "字段不可用的合约已跳过"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn snippet_truncates_on_char_boundaries() {
        let body = "费率".repeat(200);
        let out = snippet(&body);
        assert!(out.ends_with('…'));
        assert_eq!(out.chars().count(), 201);
    }

    #[test]
    fn snippet_keeps_short_bodies_intact() {
        assert_eq!(snippet("  {\"code\":0}  "), "{\"code\":0}");
    }

    #[test]
    fn rate_limit_messages_are_recognised() {
        // 与 `get_json` 与 Lighter 业务码检查实际渲染出来的错误同一个格式。
        let http = ArbError::venue(
            "lighter-rh",
            format!(
                "HTTP {}：{}",
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"code":23000,"message":"Too Many Requests!"}"#
            ),
        );
        assert!(looks_rate_limited(&http.to_string()), "{http}");
        let business = ArbError::venue("lighter-rh", "盘口深度 code=23000");
        assert!(looks_rate_limited(&business.to_string()), "{business}");
        let banned = ArbError::venue("binance", format!("HTTP {}：", StatusCode::IM_A_TEAPOT));
        assert!(looks_rate_limited(&banned.to_string()), "{banned}");
        assert!(looks_rate_limited("gate 返回了业务错误：TOO_MANY_REQUESTS"));
        assert!(!looks_rate_limited(
            &ArbError::venue("okx", "HTTP 502 Bad Gateway：").to_string()
        ));
        // 价格里碰巧有 23000 不算。
        assert!(!looks_rate_limited(
            "盘口交叉：买一 0.23000 高于卖一 0.22999"
        ));
    }

    /// 依次用 `replies` 里的原始 HTTP 响应回答连接的本地服务；返回地址与收到的请求数。
    async fn serve(replies: Vec<&'static str>) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let reply = replies.get(n).or(replies.last()).copied().unwrap_or("");
                let mut buffer = [0u8; 2048];
                let _ = socket.read(&mut buffer).await;
                let _ = socket.write_all(reply.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (address, hits)
    }

    const OK: &str =
        "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}\n";
    const BAD_GATEWAY: &str =
        "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const LIMITED: &str = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 7\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    #[derive(serde::Deserialize)]
    struct Ok_ {
        ok: bool,
    }

    /// 每个测试用自己的场所：冷却表是进程级的，测试并行时共用一家会互相影响。
    #[tokio::test]
    async fn a_429_starts_a_cooldown_that_stops_further_requests() {
        let venue = Venue::Variational;
        let (address, hits) = serve(vec![LIMITED]).await;
        let client = Client::new();

        let first = get_json::<Ok_>(client.get(&address), venue).await;
        let message = first.err().expect("429 必须报错").to_string();
        assert!(looks_rate_limited(&message), "{message}");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "429 从不重试");

        // 冷却按 Retry-After（7 秒）算，期间不再发请求。
        let left = cooling(venue).expect("应当在冷却");
        assert!(left > Duration::from_secs(5) && left <= Duration::from_secs(7));
        let second = get_json::<Ok_>(client.get(&address), venue).await;
        assert!(second.err().unwrap().to_string().contains("限频冷却中"));
        assert_eq!(hits.load(Ordering::SeqCst), 1, "冷却中不能再打上游");
        COOLDOWNS.lock().unwrap().remove(&venue);
    }

    #[tokio::test]
    async fn a_transient_5xx_is_retried_exactly_once() {
        let (address, hits) = serve(vec![BAD_GATEWAY, OK]).await;
        let parsed: Ok_ = get_json(Client::new().get(&address), Venue::Okx)
            .await
            .expect("第二次应当成功");
        assert!(parsed.ok);
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        // 一直 502：只重试一次，不会无限打下去，也不触发冷却。
        let (address, hits) = serve(vec![BAD_GATEWAY]).await;
        let error = get_json::<Ok_>(Client::new().get(&address), Venue::Bitget)
            .await
            .err()
            .expect("两次都 502 必须报错");
        assert!(error.to_string().contains("502"), "{error}");
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert!(cooling(Venue::Bitget).is_none());
    }

    #[tokio::test]
    async fn the_tightest_venue_is_never_retried() {
        let (address, hits) = serve(vec![BAD_GATEWAY, OK]).await;
        let result = get_json::<Ok_>(Client::new().get(&address), Venue::LighterRh).await;
        assert!(result.is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_redirect_cannot_resubmit_a_signed_write() {
        let redirect = "HTTP/1.1 307 Temporary Redirect\r\nLocation: /again\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (address, hits) = serve(vec![redirect, OK]).await;
        let response = build_client(2)
            .unwrap()
            .post(address)
            .body("local-fixture-only")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_longer_cooldown_is_never_shortened() {
        let venue = Venue::Ourbit;
        cool_down(venue, Duration::from_secs(100));
        cool_down(venue, Duration::from_secs(5));
        assert!(cooling(venue).unwrap() > Duration::from_secs(90));
        // 上限：写多长的 Retry-After 都不超过 MAX_COOLDOWN。
        cool_down(venue, Duration::from_secs(86_400));
        assert!(cooling(venue).unwrap() <= MAX_COOLDOWN);
        COOLDOWNS.lock().unwrap().remove(&venue);
    }
}
