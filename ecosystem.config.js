// pm2 配置：把看板 `arb-web` 交给 pm2 托管（自动拉起、日志、开机恢复）。
//
// 环境变量分两层：
//   1) 从启动 `pm2 start` 的那个 shell 继承全部 `ARB_*`，这样密钥（实盘券商私钥、
//      看板令牌）只存在于进程环境里，不落进这个会被 git 跟踪的文件。
//   2) 下面 `env` 里的显式值覆盖继承值，用来钉死「当前已验证可用」的口径。
//
// 改完执行 `pm2 restart arb-web --update-env`，再 `pm2 save` 才会写进开机恢复的快照。
const path = require('path');

const fromShell = Object.fromEntries(
  Object.entries(process.env).filter(([k]) => k.startsWith('ARB_')),
);

// 优雅停机的排空时限（秒），与 arb-web 读的是同一个变量：ARB_SHUTDOWN_GRACE_SEC，默认 120，
// 上限 300（超出 arb-web 启动即报错）。pm2 的 kill_timeout 由它推出，两者不可能对不上。
const graceSec = Number(fromShell.ARB_SHUTDOWN_GRACE_SEC || 120);
const drainSec = Number.isFinite(graceSec) && graceSec > 0 ? graceSec : 120;

// 日志目录：ARB_LOG_DIR，默认仓库下的 logs/。
const logDir = fromShell.ARB_LOG_DIR || path.join(__dirname, 'logs');

module.exports = {
  apps: [
    {
      name: 'arb-web',
      script: './target/release/arb-web',
      cwd: __dirname,
      interpreter: 'none',
      exec_mode: 'fork',
      watch: false,
      // 绑定端口失败会立刻退出；给足延迟，免得和上一个实例抢 8123 时打成快速重启循环。
      restart_delay: 3000,
      // 配置错误（比如实盘凭证填错）会让每次启动都失败。连续失败时重启间隔指数增长到 15 秒，
      // 免得每 3 秒就去连一遍各家交易所的账户接口；稳定运行 30 秒后自动复位。
      exp_backoff_restart_delay: 3000,
      max_memory_restart: '600M',
      // 优雅停机：收到 SIGINT/SIGTERM 后 arb-web 最多等排空时限，让进行中的下单做完再退出。
      // pm2 默认 1.6 秒就 SIGKILL，会把两条腿之间的下单劈开，所以 kill_timeout 取「排空时限 + 30 秒」
      // （给最后一条告警和进程收尾留余量）。
      kill_timeout: (drainSec + 30) * 1000,
      time: true,
      out_file: path.join(logDir, 'arb-web-out.log'),
      error_file: path.join(logDir, 'arb-web-error.log'),
      merge_logs: true,
      env: {
        ...fromShell,
        // 回环地址 + 8123 + 全部 15 家场所。lighter-rh 按出口 IP 限频、额度紧（同一出口 IP 上的其它程序也会占用），
        // 但实盘连着它，不扫就没法在策略页选它下单；偶尔被限频时那一轮标成取数失败，下一轮自动恢复。
        ARB_BIND_HOST: '127.0.0.1',
        ARB_HTTP_PORT: '8123',
        ARB_LOG: 'info',
        ARB_VENUES:
          'arcus,aster,binance,bitget,bybit,gate,hyperliquid,hyperliquid-io,hyperliquid-xyz,lighter,lighter-rh,mexc,okx,ourbit,variational',
        // 实盘默认关闭（`off`）。要开只读/下单得显式给 ARB_WEB_LIVE，且下单模式
        // 必须同时给 ARB_WEB_MARKET_SLIPPAGE；这里不设，继承值说了算。
      },
    },
  ],
};
