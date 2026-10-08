//! arb-bot 的核心：领域类型、错误、配置、十进制工具。
//!
//! 这一层**不做 IO、不做归一化、不做排名**。它的职责是把「一次交易所读数长什么样」
//! 和「一次启动需要哪些配置」定死，让上层可以放心地并发打十几家 API。

pub mod book;
pub mod config;
pub mod error;
pub mod logging;
pub mod money;
pub mod types;

pub use config::{
    DEFAULT_AMORTIZE_DAYS, DEFAULT_FEE_PER_SIDE, DEFAULT_LEVERAGE, DEFAULT_MAX_ENTRY_BASIS_PCT,
    DEFAULT_SPREAD_HOLD_DAYS, MAX_AMORTIZE_DAYS, MAX_FEE_PER_SIDE, MAX_LEVERAGE, Settings,
};
// 统一从 core 取 `Decimal`：全仓只允许一个 rust_decimal 版本，否则类型不互通。
pub use book::{
    Candle, FillEstimate, FundingPoint, Level, OrderBook, Side, estimate_fill,
    estimate_fill_limited,
};
pub use error::{ArbError, ArbResult};
pub use money::{from_json_f64, parse_decimal, to_pct};
pub use rust_decimal::Decimal;
pub use types::{
    DEFAULT_FUNDING_INTERVAL_H, MarketSnapshot, Symbol, Venue, family_display_quote,
    settlement_family,
};
