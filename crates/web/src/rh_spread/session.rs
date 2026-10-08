//! 交易时段：股票 / 指数 / 商品永续的价差在盘中、盘后、周末往往不是同一个水平，
//! 「正常价差」要按时段分开统计。加密币 24/7，不分。
//!
//! 优先用 Arcus `/v1/markets` 的 `isOutsideRth`（它知道节假日与提前收盘）；拿不到时按
//! 纽约时间 周一至周五 9:30–16:00 推算（不含节假日）。

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
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

/// 第 `nth` 个星期日（`nth` 从 1 开始）。
fn nth_sunday(year: i32, month: u32, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("合法月份");
    let offset = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + Duration::days(i64::from(offset + 7 * (nth - 1)))
}

/// 纽约相对 UTC 的小时偏移：夏令时 3 月第二个周日 2:00 起、11 月第一个周日 2:00 止。
pub fn new_york_offset_hours(at: DateTime<Utc>) -> i64 {
    let year = at.year();
    // 当地 2:00 = UTC 7:00（标准时间时）开始；当地 2:00 = UTC 6:00（夏令时时）结束。
    let start = Utc.from_utc_datetime(&nth_sunday(year, 3, 2).and_hms_opt(7, 0, 0).expect("时刻"));
    let end = Utc.from_utc_datetime(&nth_sunday(year, 11, 1).and_hms_opt(6, 0, 0).expect("时刻"));
    if at >= start && at < end { -4 } else { -5 }
}

/// 某一时刻属于哪个时段。`crypto` 为真时恒为 [`Session::All`]。
/// `outside_rth` 是 Arcus 报的「当前是否在常规时段之外」，`None` = 不知道，按时间推算。
pub fn classify(at: DateTime<Utc>, crypto: bool, outside_rth: Option<bool>) -> Session {
    if crypto {
        return Session::All;
    }
    let local = at + Duration::hours(new_york_offset_hours(at));
    if matches!(local.weekday(), Weekday::Sat | Weekday::Sun) {
        return Session::Weekend;
    }
    match outside_rth {
        Some(true) => Session::Off,
        Some(false) => Session::Rth,
        None => {
            let minutes = local.hour() * 60 + local.minute();
            if (9 * 60 + 30..16 * 60).contains(&minutes) {
                Session::Rth
            } else {
                Session::Off
            }
        }
    }
}
