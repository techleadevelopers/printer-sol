---
name: First-pump strategy
description: User-stated trading behavior for newly launched tokens in this project.
---

Prioritize entering a newly created token within its first seconds. Keep TP/SL, trailing stop, and the max-hold dead-zone behavior; run RugCheck asynchronously after entry.

Keep financial limits, duplicate-entry prevention, and transaction reconciliation. A failed post-entry RugCheck raises an alert but does not force-close the position.

**Why:** the user explicitly chose fast entry without removing existing portfolio limits or transaction integrity checks.

**How to apply:** skip pre-entry RugCheck in the trade path; preserve exit triggers and controls while changing entry or price-monitoring behavior.
