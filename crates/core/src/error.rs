//! 错误类型。
//!
//! 原则：**失败必须带上场所名**。一次扫描会并发打十几家 API，只有
//! `connection reset` 这种错误是无法定位的 —— 排查时得靠日志里的上下文猜是哪家。

use rust_decimal::Decimal;

pub type ArbResult<T> = Result<T, ArbError>;

#[derive(Debug, thiserror::Error)]
pub enum ArbError {
    #[error("HTTP 请求失败：{0}")]
    Http(#[from] reqwest::Error),

    #[error("{venue} 的响应无法解析：{source}")]
    Decode {
        venue: &'static str,
        #[source]
        source: serde_json::Error,
    },

    #[error("{venue} 返回了业务错误：{message}")]
    Venue {
        venue: &'static str,
        message: String,
    },

    #[error("配置错误：{0}")]
    Config(String),

    #[error("本地文件操作失败：{0}")]
    Io(#[from] std::io::Error),

    #[error("{what} 超出允许范围：{value}（要求 {expected}）")]
    OutOfRange {
        what: &'static str,
        value: Decimal,
        expected: &'static str,
    },
}

impl ArbError {
    /// 交易所返回了 HTTP 200 但业务失败时的构造器。
    ///
    /// 这一层不能省：多家 CEX 用 `200 + code != "0"` 表达失败，只看状态码会把
    /// 「下单/查询失败」当成成功。
    pub fn venue(venue: &'static str, message: impl Into<String>) -> Self {
        ArbError::Venue {
            venue,
            message: message.into(),
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        ArbError::Config(message.into())
    }
}
