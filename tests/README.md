# Integration test notes

Keep a sanitized PumpPortal Local API serialized-transaction fixture here after verifying it was created for a disposable wallet. The real-execution compatibility test must decode it with the pinned `solana-go` version, verify the operational wallet is a required signer, sign it, serialize it, and assert a non-empty first signature. It must never submit the fixture to an RPC.
