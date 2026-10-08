//! 扫描与排名：归一化、身份分簇、可信度筛查、机会排序。
//!
//! 这一层是纯逻辑 + 一次并发取数，不持有状态、不下单。它同时被 CLI 与 HTTP 服务
//! 复用，保证「命令行看到的」和「面板看到的」是同一份计算。

pub mod convergence;
pub mod identity;
pub mod leverage;
pub mod normalize;
pub mod rank;
pub mod sanity;
pub mod scan;
pub mod stability;

pub use convergence::{BasisPoint, HalfLife, basis_series, half_life};
pub use identity::{Cluster, Clustering};
pub use leverage::{Health, LegRisk, PairRisk, pair_risk};
pub use normalize::{to_apr, to_daily};
pub use rank::{Opportunity, venue_fee};
pub use sanity::{Suspicion, SuspicionMap};
pub use scan::{
    ExcludedReading, ScanReport, SymbolView, Totals, VenueReport, filter_by_base, rerank, scan,
};
pub use stability::FundingStability;
