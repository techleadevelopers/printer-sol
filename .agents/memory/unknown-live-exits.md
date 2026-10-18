---
name: Unknown live exits
description: Safety rule for recovering live transactions whose submission result is uncertain.
---

Treat a live transaction in an unknown state as possibly submitted, not failed. Reconcile the original persisted signature; do not submit a replacement while its outcome is uncertain. Retry only after the chain confirms failure, and stop retrying after confirmed success.

**Why:** A timeout or dropped response can happen after the chain accepts a transaction. Resending an uncertain sell can sell twice.

**How to apply:** Every live recovery path must preserve and query the original signature. If the signature is missing or the RPC cannot establish a terminal result, keep the operation blocked and surface it for manual reconciliation rather than sending another transaction.
