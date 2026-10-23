---
name: Simulation sell finalization
description: Paper-sale persistence, retry, and reporting invariants.
---

Paper sell finalization must be atomic across the position, trade, and transaction records. If it fails definitively, mark the trade and transaction failed and close the paper position to release its slot; do not count it toward realized P&L or effective fees, and keep its entry cost reserved in simulated available balance. If persistence is ambiguous, keep the attempt blocked as unknown until reconciliation rather than creating a duplicate sell. `realized_pnl_sol` stays NULL until a sale is finalized successfully; success state plus a closed position, not a non-null/zero P&L field alone, defines a completed sale.

**Why:** The user requires failed paper finalization not to leave a position open and consume capacity, while an ambiguous live transaction must never be force-closed or resubmitted without signature reconciliation.

**How to apply:** Keep finalization transactional; classify the persisted snapshot before resolving errors; close definitive paper failures while reserving their entry cost against simulated cash; leave ambiguous outcomes blocked for reconciliation; filter realized-P&L and fee aggregates by successful terminal state and closed position; preserve signature reconciliation for live exits.
