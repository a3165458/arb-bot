//! 日志初始化。
//!
//! 一律写 **stderr**：CLI 的 `--json` 模式要保证 stdout 只有 JSON，
//! 否则下游 `| jq` 会被日志行打断。看板也是同样的道理。

use tracing_subscriber::EnvFilter;

/// 初始化全局订阅者。重复调用是安全的（第二次起静默忽略）。
pub fn init(filter: &str) {
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}
