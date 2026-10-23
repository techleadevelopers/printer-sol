---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). All entry gates come from environment configuration. `MIN_MARKET_CAP_SOL` and `MAX_MARKET_CAP_SOL` are strict bounds with no tolerance; `MAX_MARKET_CAP_SOL=0` disables only the maximum. `MIN_VOLUME_BEFORE_BUY` and `MAX_VOLUME_BEFORE_BUY` are inclusive real bonding-curve SOL reserve bounds in every mode (defaults 0–1.5 SOL), checked via RPC and fail closed if the curve cannot be verified. Never treat real SOL reserves as cumulative traded volume. With `USE_WS_MARKET_CAP=true`, use PumpPortal `marketCapSol` as the market-cap gate source and entry baseline; with it false, use RPC market cap.

**Why:** the user clarified that the real-reserve limits must never be ignored and that market-cap settings must not admit values above their configured limit, including in fast mode.

**How to apply:** require a positive, valid WebSocket market cap in fast mode and RPC verification of the reserve bounds in every mode. Continue using WebSocket updates as the primary post-entry mark source; use `PRICE_POLL_SECONDS` for stale-mark RPC fallback in fast mode and regular RPC marks in legacy mode. Exclude subscription-control messages from raw logs. Preserve TP/SL, trailing/max-hold, financial limits, duplicate-entry prevention, reconciliation, and post-entry-only RugCheck.

Preserve TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close only flat or negative-return positions; keep positive-return positions open for take-profit or trailing-stop decisions, and keep positions without a valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check alerts but does not force-close. Live exits are confirmed from wallet balances, and market-cap estimates never count as confirmed live P&L.

**Why:** the user wants first-seconds Pump.fun sniping while preserving risk and settlement safeguards, and requires max hold to close only positions with `change <= 0`.

**How to apply:** delay the buy only for the configured entry gate, not for RugCheck. Keep WebSocket as the primary fast price source and `PRICE_POLL_SECONDS` as a stale-mark RPC fallback. Alert about RPC degradation only when an RPC operation is actually expected or attempted.

`TOKEN_MAX_AGE_SECONDS` defaults to 30 and is authoritative in both the strategy and monitor, with no tolerance multiplier or fixed override. A value of `0` disables only the token-age filter in simulation and live modes; positive values reject tokens at or above the configured age boundary. This cutoff is independent of the entry delay.

**Why:** the user explicitly requires `TOKEN_MAX_AGE_SECONDS` to be obeyed exactly, including disabling it with zero and not forcing it above the entry delay.

**How to apply:** pass the configured seconds to both `strategy.New` and `tokenmonitor.Settings.MaxTokenAge`; preserve `EvaluateFast`'s strict `age >= maxAge` check and skip monitor age checks when the configured duration is zero.
