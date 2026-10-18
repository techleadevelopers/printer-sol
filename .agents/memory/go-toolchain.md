---
name: Go toolchain verification
description: Toolchain mismatch and checksum policy that blocked local Go tests.
---

The project declares Go 1.24.0, while the local shell had Go 1.21.13. Automatic toolchain download was blocked because `GOSUMDB=off` prevents checksum verification.

**Why:** Source changes can be ready while local tests remain blocked by the available compiler rather than by code.

**How to apply:** Check the local Go version before testing. Use a compatible toolchain and approved checksum verification; do not change project or production settings just to bypass the mismatch.
