---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). All entry gates come from environment configuration. `MIN_MARKET_CAP_SOL` and `MAX_MARKET_CAP_SOL` are strict bounds with no tolerance; `MAX_MARKET_CAP_SOL=0` disables only the maximum. `MIN_VOLUME_BEFORE_BUY` and `MAX_VOLUME_BEFORE_BUY` are inclusive real bonding-curve SOL reserve bounds (defaults 0–1.5 SOL), and real SOL reserves are a liquidity proxy, never cumulative traded volume. In the RPC legacy path, verify reserves before admission and fail closed. In the WebSocket fast path, do not synchronously wait for or reject admission based on the reserve snapshot; keep reserve conformance diagnostic and separate from execution readiness. PumpPortal market cap may gate entry and signal that a mark should be refreshed, but it is never the position-price or P&L source; position marks require a fresh bonding-curve spot price from RPC.

**Why:** the user requires configured reserve and market-cap limits to remain strict, and clarified that PumpPortal market cap must not be treated as the position's actual price or P&L.

**How to apply:** require a positive, valid WebSocket market cap in fast mode; do not add a blocking reserve RPC read to that admission path. Keep legacy RPC reserve gates, and keep live quote freshness, slippage, and impact checks in execution preparation rather than merging them with candidate admission. Treat post-entry WebSocket events as refresh signals only; refresh and validate the spot price from RPC before updating P&L or considering exits. Use `PRICE_POLL_SECONDS` for RPC refreshes and fail closed when the price is stale or unavailable. Exclude subscription-control messages from raw logs. Preserve TP/SL, trailing/max-hold, financial limits, duplicate-entry prevention, reconciliation, and post-entry-only RugCheck.

Preserve TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close only flat or negative-return positions; keep positive-return positions open for take-profit or trailing-stop decisions, and keep positions without a valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check alerts but does not force-close. Live exits are confirmed from wallet balances, and market-cap estimates never count as confirmed live P&L.

**Why:** the user wants first-seconds Pump.fun sniping while preserving risk and settlement safeguards, and requires max hold to close only positions with `change <= 0`.

**How to apply:** delay the buy only for the configured entry gate, not for RugCheck. Keep WebSocket as the primary fast price source and `PRICE_POLL_SECONDS` as a stale-mark RPC fallback. Alert about RPC degradation only when an RPC operation is actually expected or attempted.

`TOKEN_MAX_AGE_SECONDS` defaults to 30 and is authoritative in both the strategy and monitor, with no tolerance multiplier or fixed override. A value of `0` disables only the token-age filter in simulation and live modes; positive values reject tokens at or above the configured age boundary. This cutoff is independent of the entry delay.

**Why:** the user explicitly requires `TOKEN_MAX_AGE_SECONDS` to be obeyed exactly, including disabling it with zero and not forcing it above the entry delay.

**How to apply:** pass the configured seconds to both `strategy.New` and `tokenmonitor.Settings.MaxTokenAge`; preserve `EvaluateFast`'s strict `age >= maxAge` check and skip monitor age checks when the configured duration is zero.
