# arb-bot

English · [中文](README.md)

> ⚠️ **Disclaimer**: This project is for education and research only and is **not investment advice**.
> Perpetual futures and leveraged trading are extremely risky; you can lose all of your capital.
> The software may contain bugs, and exchange APIs, fees and rules can change at any time. Live order
> placement, automatic closing, trimming and margin top-ups all act on **your real funds**.
> You must review the code yourself and you bear all risks and losses; the authors and contributors are
> not liable for any direct or indirect loss. Comply with the laws of your jurisdiction and each exchange's
> terms of service — some exchanges do not serve users in certain countries or regions.
> **Never** commit private keys, API keys or your `.env` file to any repository, or share them with anyone.

Cross-venue perpetual-futures arbitrage toolkit: a Rust core plus a built-in web dashboard. Scanning is
read-only; paper trading simulates both legs against real order books; live execution is off by default
and must be enabled explicitly.

The full, detailed documentation (cost model, venue matrix, risk model, rules, deployment) is in
[README.md](README.md) (Chinese). This page is a summary.

## Strategies

Both strategies use the same two-leg (long one venue, short another) structure:

| Strategy | What it earns | Ranked by | Main risk |
| --- | --- | --- | --- |
| **Funding-rate arbitrage** | Funding spread accrued every settlement period | Annualised net yield after amortised costs (assumes basis unchanged) | Basis widening |
| **Cross-venue price spread** | Basis convergence (one-off) | Net basis gain (assumes convergence to 0) | Basis not converging |

## Features

- **Scanner** (`arb-scan`): 14 venues (Arcus, Aster, Binance, Bitget, Bybit, Gate, Hyperliquid incl. HIP-3
  `xyz`/`io`, Lighter, Lighter RH, MEXC, OKX, Ourbit, Variational). Normalises funding intervals per contract,
  clusters identical assets by price, flags suspicious readings, ranks by net yield after fees and spread.
- **Paper trading** (`arb-paper`): depth check → gates → two-leg execution with rollback → reconciliation, no real funds.
- **Live execution** (`arb-live`, dashboard): real two-leg execution on 12 venues; read-only unless explicitly
  enabled; append-only ledger and per-venue order-intent journals; isolated / cross margin modes.
- **Position rules**, editable while a position is open: funding-spread exit, liquidation protection (trim),
  size-mismatch exit, basis-convergence exit, take-profit including settled funding (confirmed against the
  live order books before closing), and capped automatic isolated-margin top-ups.
- **Dashboard** (`arb-web`): opportunities, strategy planner, positions, and a read-only
  **Lighter RH ↔ Arcus spread monitor** (WebSocket order books, per-session "normal basis", Telegram alerts).
- **Telegram**: alerts plus a private read-only command menu (status, positions, PnL, balances, pause/resume new opens).

## Quick start

```bash
cargo build --release

# Funding-rate board (default) / price-spread board
./target/release/arb-scan
./target/release/arb-scan --view spread

# Dashboard (listens on 127.0.0.1 only by default)
./target/release/arb-web

# Paper trading (no real funds)
./target/release/arb-paper BTC --size 1000

# Live: read-only by default — connects, reconciles and plans, but never signs or sends orders
./target/release/arb-live status
```

Configuration is via environment variables; copy `.env.example` to `.env` and fill in only what you need.
`.env` is git-ignored — keep it private (`chmod 600 .env`).

Live trading from the dashboard requires `ARB_WEB_TOKEN` (≥ 16 characters) and `ARB_WEB_LIVE=trade`.
Without a token, all trading endpoints are disabled. Do not expose the dashboard to the internet without
your own authentication layer in front of it.

## Safety notes

- Start with paper trading and small sizes. Test on your own account limits, fees and margin settings.
- Venue credentials should be trading-only keys; **never grant withdrawal permission**.
- Keep the live ledger (`arb-live-ledger.jsonl`) and order journals (`arb-live-<venue>-orders.jsonl`) backed up;
  they are the only record that an order ID was already sent.
- Spread/basis statistics are not guarantees: a "normal" basis may not revert, and funding can flip.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
node --test crates/web/web/position-rules.test.cjs
```

Tests do not touch the network or any account (network smoke tests are `#[ignore]`d).

## License

[MIT](LICENSE). Provided "as is", without warranty of any kind — see the license text and the disclaimer above.
