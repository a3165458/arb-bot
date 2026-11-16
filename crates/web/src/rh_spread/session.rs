//! 交易时段：股票 / 指数 / 商品永续的价差在盘中、盘后、周末往往不是同一个水平，
//! 「正常价差」要按时段分开统计。加密币 24/7，不分。
//!
//! 优先用 Arcus `/v1/markets` 的 `isOutsideRth`（它知道节假日与提前收盘）；拿不到时按
//! 纽约时间 周一至周五 9:30–16:00 推算（不含节假日）。

use chrono::{DateTime, Datelike, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Session {
    /// 美股常规交易时段。
    Rth,
    /// 工作日盘前 / 盘后 / 夜盘。
    Off,
    /// 纽约时间周六、周日。
    Weekend,
    /// 加密币：不分时段。
    All,
}

impl Session {
    pub fn label(self) -> &'static str {
        match self {
            Session::Rth => "盘中",
            Session::Off => "盘后",
            Session::Weekend => "周末",
            Session::All => "全天",
        }
    }
}

/// 某一时刻属于哪个时段。`crypto` 为真时恒为 [`Session::All`]。
/// `outside_rth` 是 Arcus 报的「当前是否在常规时段之外」，`None` = 不知道，按时间推算。
pub fn classify(at: DateTime<Utc>, crypto: bool, outside_rth: Option<bool>) -> Session {
    if crypto {
        return Session::All;
    }
    let local = arb_core::new_york::local(at);
    if matches!(local.weekday(), Weekday::Sat | Weekday::Sun) {
        return Session::Weekend;
    }
    // 时钟为主：Arcus 的标志 5 分钟才刷新一次（取不到时还会一直沿用旧值），16:00 以后几分钟还报「盘中」、
    // 9:30 以后几分钟还报「盘外」—— 正是价差跳变的时候，按错的时段比正常水平会出假信号，也污染分钟历史。
    // 所以时钟说盘外就是盘外；时钟说盘中而 Arcus 说盘外（节假日、提前收盘）时，开盘头几分钟不信它（可能是旧的）。
    let minutes = local.hour() * 60 + local.minute();
    if !(OPEN_MIN..CLOSE_MIN).contains(&minutes) {
        return Session::Off;
    }
    match outside_rth {
        Some(true) if minutes >= OPEN_MIN + FLAG_STALE_MIN => Session::Off,
        _ => Session::Rth,
    }
}

/// 纽约时间 9:30、16:00（分钟数）。
const OPEN_MIN: u32 = 9 * 60 + 30;
const CLOSE_MIN: u32 = 16 * 60;
/// Arcus 的 `isOutsideRth` 最多这么旧（目录 5 分钟刷新一次，留 1 分钟余量）。
const FLAG_STALE_MIN: u32 = 6;
