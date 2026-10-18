---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). Require two PumpPortal WebSocket buy/sell trades (configurable with `MIN_TXNS_BEFORE_BUY`) and a positive market cap. A positive market-cap value in those trades is sufficient without waiting for HTTP or RPC; if the WS mark is absent, read the Pump.fun bonding-curve account via Solana RPC before entering.

Keep TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close at exact break-even to release a slot, or close a material move at +5% / -10%; keep intermediate dead-zone moves and positions with no valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check raises an alert but does not force-close the position. Refresh open-position marks directly from the Solana RPC bonding-curve account, while applying positive PumpPortal WebSocket trade marks immediately; alert after three consecutive RPC failures.

**Why:** the user wants first-seconds Pump.fun sniping but explicitly rejects zero-price or inactive entries, wants exact-flat positions to release capacity after max hold without liquidating the intermediate dead zone, and wants live exits confirmed from wallet balances.

**How to apply:** delay the buy only for the activity/positive-mark gate, not for RugCheck; use WebSocket trades only as the fallback when public activity data is missing or unavailable; never use market-cap estimates as confirmed live P&L.
