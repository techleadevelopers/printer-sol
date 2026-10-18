---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). Require the configured transaction and volume thresholds (2 transactions and 0.5 SOL by default) plus a positive current market cap; if DexScreener activity data is unavailable or incomplete, require at least two WebSocket trades and still require a positive mark. Missing market-cap data may fall back from DexScreener to WebSocket and then the on-chain bonding curve.

Keep TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close at exact break-even to release a slot, or close a material move at +5% / -10%; keep intermediate dead-zone moves and positions with no valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check raises an alert but does not force-close the position. Refresh open-position marks via DexScreener, falling back to WebSocket/RPC bonding-curve marks; retain exponential HTTP 429 backoff and alert after three consecutive API failures.

**Why:** the user wants first-seconds Pump.fun sniping but explicitly rejects zero-price or inactive entries, wants exact-flat positions to release capacity after max hold without liquidating the intermediate dead zone, and wants live exits confirmed from wallet balances.

**How to apply:** delay the buy only for the activity/positive-mark gate, not for RugCheck; use WebSocket trades only as the fallback when public activity data is missing or unavailable; never use market-cap estimates as confirmed live P&L.
