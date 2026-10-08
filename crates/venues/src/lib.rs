//! 场所连接器：公共行情，只读。
//!
//! 新增一家场所 = 新增一个模块 + 在 [`connector::build`] 里加一个分支。
//! 契约（[`VenueApi`]）只有 `fetch_all`，因为结算周期只在批量端点里。

pub mod arcus;
pub mod aster;
pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod connector;
pub mod gate;
pub mod http;
pub mod hyperliquid;
pub mod lighter;
pub mod mexc;
pub mod okx;
pub mod ourbit;
pub mod variational;

pub use connector::{HistoryCached, VenueApi, build, build_all};
pub use http::{build_client, get_json};
