---
name: WebSocket reserve-gate mismatch
description: The safety-policy mismatch between documented reserve bounds and the WebSocket market-cap entry path.
---

The intended invariant is that `MIN_VOLUME_BEFORE_BUY` and `MAX_VOLUME_BEFORE_BUY` bound verified real bonding-curve SOL reserves in every entry mode; these reserves are a liquidity proxy, not cumulative traded volume. The current WebSocket market-cap candidate path skips the RPC entry-metrics check and disables the reserve gate. Live execution still needs an RPC bonding-curve quote later, but that is not equivalent to enforcing the reserve bounds before admission.

**Why:** the README and user-stated strategy require strict reserve bounds in every mode, so the current implementation can admit a candidate that violates the configured policy.

**How to apply:** if backend scope is authorized, validate real reserves and fail closed before enqueueing in WebSocket mode too, then add coverage for in-range, out-of-range, and RPC-unavailable cases. Do not silently reinterpret the limits as traded volume.
