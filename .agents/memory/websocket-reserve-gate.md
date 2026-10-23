---
name: WebSocket reserve-gate mismatch
description: The safety-policy mismatch between documented reserve bounds and the WebSocket market-cap entry path.
---

`MIN_VOLUME_BEFORE_BUY` and `MAX_VOLUME_BEFORE_BUY` describe real bonding-curve SOL reserves, a liquidity proxy rather than cumulative traded volume. Product behavior intentionally differs by admission path: the legacy RPC path verifies the inclusive range and fails closed; the WebSocket fast path must not wait for an RPC reserve snapshot or reject admission because it is delayed or divergent. Keep reserve conformance separate from candidate admission and live execution readiness.

**Why:** the user explicitly chose the low-latency WebSocket path without a blocking reserve read and asked that admission, target reserve policy, and live execution prerequisites remain independent.

**How to apply:** do not add a synchronous reserve gate to the WebSocket candidate path. Keep the legacy RPC path fail-closed, use a fresh RPC quote plus configured slippage/impact limits for live preparation, and add tests for RPC delay, unavailable data, source divergence, and boundary reserves.
