---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). All entry gates come from environment configuration. `MIN_MARKET_CAP_SOL` and `MAX_MARKET_CAP_SOL` are strict bounds with no tolerance; `MAX_MARKET_CAP_SOL=0` disables only the maximum. `MIN_VOLUME_BEFORE_BUY` and `MAX_VOLUME_BEFORE_BUY` are inclusive real bonding-curve SOL reserve bounds in every mode (defaults 0–1.5 SOL), checked via RPC and fail closed if the curve cannot be verified. Never treat real SOL reserves as cumulative traded volume. With `USE_WS_MARKET_CAP=true`, use PumpPortal `marketCapSol` as the market-cap gate source and entry baseline; with it false, use RPC market cap.

**Why:** the user clarified that the real-reserve limits must never be ignored and that market-cap settings must not admit values above their configured limit, including in fast mode.

**How to apply:** require a positive, valid WebSocket market cap in fast mode and RPC verification of the reserve bounds in every mode. Continue using WebSocket updates as the primary post-entry mark source; use `PRICE_POLL_SECONDS` for stale-mark RPC fallback in fast mode and regular RPC marks in legacy mode. Exclude subscription-control messages from raw logs. Preserve TP/SL, trailing/max-hold, financial limits, duplicate-entry prevention, reconciliation, and post-entry-only RugCheck.

Keep TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close only flat or negative-return positions; keep all positive-return positions open for take-profit or trailing-stop decisions, and keep positions with no valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check raises an alert but does not force-close the position. In fast mode, use positive PumpPortal WebSocket marks for open positions and do not fall back to RPC price polling; only legacy mode refreshes marks from RPC. Alert handling must not claim the RPC price feed is failing in fast mode merely because no entry/mark RPC calls are made.

**Why:** the user wants first-seconds Pump.fun sniping without entry-time market-data RPC latency, and specifically requires max hold to close only positions with `change <= 0` so positive positions remain eligible for take-profit or trailing stop. Live exits are confirmed from wallet balances.

**How to apply:** delay the buy only for the configured entry gate, not for RugCheck; keep TP/SL, trailing stops, financial limits, duplicate-entry prevention, and transaction reconciliation. Never use market-cap estimates as confirmed live P&L.
