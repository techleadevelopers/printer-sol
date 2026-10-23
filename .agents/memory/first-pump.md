---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Only consider newly created Pump.fun tokens after the configured entry delay (5 seconds by default). Approve entry from a Solana RPC bonding-curve read: real SOL reserves converted from lamports to SOL must be within the inclusive `MIN_VOLUME_BEFORE_BUY`–`MAX_VOLUME_BEFORE_BUY` range (defaults 0–1.5 SOL), and the selected market cap must meet `MIN_MARKET_CAP_SOL`. RPC must still confirm real reserves even when `USE_WS_MARKET_CAP=true`; that option selects the PumpPortal market cap only for market-cap filters and the entry baseline. Never require WebSocket trade counts for entry; real SOL reserves are a liquidity proxy, not cumulative traded volume.

**Why:** the user chose zero-to-1.5 SOL as the default real-reserve entry window, configurable through environment variables, and requested an optional WebSocket market-cap source without letting it bypass reserve validation.

**How to apply:** reject when the RPC cannot confirm the curve or reserves fall outside configured bounds; do not apply WebSocket market-cap prefilters unless `USE_WS_MARKET_CAP` is enabled. Continue using WebSocket updates for post-entry price marks and raw-frame debugging, excluding subscription-control messages from raw logs.

Keep TP/SL, trailing stop, financial limits, duplicate-entry prevention, and transaction reconciliation. After max hold, close at exact break-even to release a slot, or close a material move at +5% / -10%; keep intermediate dead-zone moves and positions with no valid mark open. Run RugCheck asynchronously after entry; a failed post-entry check raises an alert but does not force-close the position. Refresh open-position marks directly from the Solana RPC bonding-curve account, while applying positive PumpPortal WebSocket trade marks immediately; alert after three consecutive RPC failures.

**Why:** the user wants first-seconds Pump.fun sniping without depending on delayed WebSocket trades or adding RPC calls for volume; bonding-curve market cap and real SOL reserves are available in the same RPC read. They also want exact-flat positions to release capacity after max hold without liquidating the intermediate dead zone, and live exits confirmed from wallet balances.

**How to apply:** delay the buy only for the configured entry gate, not for RugCheck; keep TP/SL, trailing stops, financial limits, duplicate-entry prevention, and transaction reconciliation. Never use market-cap estimates as confirmed live P&L.
