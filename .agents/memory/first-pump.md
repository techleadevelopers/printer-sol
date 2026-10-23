---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). `USE_WS_MARKET_CAP=true` is the default fast-entry mode: use PumpPortal `marketCapSol` for the `MIN_MARKET_CAP_SOL`/`MAX_MARKET_CAP_SOL` filters and entry baseline, without an entry-time bonding-curve RPC call; do not apply the real-reserve window in this mode. `USE_WS_MARKET_CAP=false` restores the legacy RPC market-cap and inclusive `MIN_VOLUME_BEFORE_BUY`–`MAX_VOLUME_BEFORE_BUY` real-reserve gate (defaults 0–1.5 SOL). Never treat real SOL reserves as cumulative traded volume.

**Why:** the user specified PumpPortal market cap as the default low-latency entry source, with no RPC curve read during entry; the real-reserve gate remains available only in legacy RPC mode.

**How to apply:** require a positive, valid WebSocket market cap in fast mode; reserve RPC for legacy-mode gates and transaction execution/confirmation. Continue using WebSocket updates for post-entry price marks and raw-frame debugging, excluding subscription-control messages from raw logs. Preserve TP/SL, trailing/max-hold, financial limits, duplicate-entry prevention, reconciliation, and post-entry-only RugCheck.

Keep TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close at exact break-even to release a slot, or close a material move at +5% / -10%; keep intermediate dead-zone moves and positions with no valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check raises an alert but does not force-close the position. In fast mode, use positive PumpPortal WebSocket marks for open positions and do not fall back to RPC price polling; only legacy mode refreshes marks from RPC. Alert handling must not claim the RPC price feed is failing in fast mode merely because no entry/mark RPC calls are made.

**Why:** the user wants first-seconds Pump.fun sniping without entry-time market-data RPC latency. They also want exact-flat positions to release capacity after max hold without liquidating the intermediate dead zone, and live exits confirmed from wallet balances.

**How to apply:** delay the buy only for the configured entry gate, not for RugCheck; keep TP/SL, trailing stops, financial limits, duplicate-entry prevention, and transaction reconciliation. Never use market-cap estimates as confirmed live P&L.
