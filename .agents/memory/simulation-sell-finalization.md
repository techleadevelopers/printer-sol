---
name: Simulation sell finalization
description: Paper-sale persistence, retry, and reporting invariants.
---

Paper sell finalization must be atomic across the position, trade, and transaction records. A failed sale must leave the position open and must not count toward realized P&L or effective fees. Some PostgreSQL schemas require numeric P&L fields to be non-null, so a failed row may store zero; success state plus a closed position, not a non-null/zero P&L field alone, defines a completed sale. Paper retries can use persisted failed-attempt timestamps and bounded backoff; stale prepared/unknown local sells may be reconciled only when the position is still open and no external transaction side effect exists.

**Why:** Treating a failed sale's schema-compatible zero as a realized result distorts dashboard/report metrics, while retrying an ambiguous live transaction can create a second sale.

**How to apply:** Keep paper finalization transactional, filter every realized-P&L and cost aggregate by successful terminal state and closed position, and preserve the stricter signature-reconciliation rule for live exits.
