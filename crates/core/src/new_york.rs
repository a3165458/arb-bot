//! 纽约时间：美股相关的时段（价差监控的盘中 / 盘后 / 周末、Bitget 股票类合约的休市）都按它算。

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};

/// 第 `nth` 个星期日（`nth` 从 1 开始）。
fn nth_sunday(year: i32, month: u32, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("合法月份");
    let offset = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + Duration::days(i64::from(offset + 7 * (nth - 1)))
}

/// 纽约相对 UTC 的小时偏移：夏令时 3 月第二个周日 2:00 起、11 月第一个周日 2:00 止。
pub fn offset_hours(at: DateTime<Utc>) -> i64 {
    let year = at.year();
    // 当地 2:00 = UTC 7:00（标准时间时）开始；当地 2:00 = UTC 6:00（夏令时时）结束。
    let start = Utc.from_utc_datetime(&nth_sunday(year, 3, 2).and_hms_opt(7, 0, 0).expect("时刻"));
    let end = Utc.from_utc_datetime(&nth_sunday(year, 11, 1).and_hms_opt(6, 0, 0).expect("时刻"));
    if at >= start && at < end { -4 } else { -5 }
}

/// 这一刻的纽约当地时间。
pub fn local(at: DateTime<Utc>) -> NaiveDateTime {
    (at + Duration::hours(offset_hours(at))).naive_utc()
}
