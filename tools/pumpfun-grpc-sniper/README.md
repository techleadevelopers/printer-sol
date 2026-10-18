## Pumpfun Grpc Sniper and Bundler Bot (Solana)

High-performance listener and sniping toolkit for Pump.fun tokens on Solana. It subscribes to Yellowstone gRPC for near real-time detection of new Pump.fun token creations and prepares buy transactions using a lightweight SDK wrapper. Concurrency is bounded via an internal queue with TTL deduplication and per-task timeouts for stability under heavy throughput.

### Features
- **Low-latency stream**: Yellowstone gRPC `processed` commitment for fast detection.
- **Instruction decoding**: Detects Pump.fun `create` discriminator and extracts mint, creator, metadata.
- **Sniping pipeline**: Generates buy instructions via `PumpHelper`, simulates, then signs and sends.
- **Parallel processing**: Concurrency-limited queue with per-task timeout and mint deduplication.
- **Helpers**: PDA helpers, bonding curve math utilities, legacy/Jito executors, bundle buy scaffold, IDL types.

### Repository structure
- `src/index.ts`: gRPC subscriber, decoding, concurrency queue, dedupe + timeout, enqueues `SnipeToken`.
- `src/snipe.ts`: Sniping flow using `PumpHelper` to construct, simulate, sign, and send a buy tx.
- `src/pumpHelper/*`:
  - `helper.ts`: Anchor-based helper to build buy/sell/extend instructions, fetch PDAs and accounts.
  - `pda.ts`: PDA derivations for global, bonding curve, creator vault.
  - `calc.ts`: Bonding curve math utilities and pricing helpers.
  - `types.ts`: Program state interfaces (`Global`, `BondingCurve`).
- `src/executor/*`:
  - `legacy.ts`: Raw send/confirm helpers for `VersionedTransaction`.
  - `jito.ts`: Jito bundle sender to multiple block engines.
- `src/bundleBuy.ts`: Example to aggregate multiple wallet buys (bundle) and simulate.
- `src/buyersKeys.ts`: Placeholder array for wallet keypairs used in bundled buy.
- `src/config/index.ts`: RPC endpoints list and a shared `connection` helper.
- `IDL/pump.json` and `IDL/pumptypes.ts`: Program IDL and TS types for Pump.fun.

### Requirements
- Node.js 18+
- Solana RPC access (Helius/Alchemy/etc.)
- Yellowstone gRPC endpoint (e.g. Triton One)

### Installation
```bash
npm install
```

Optionally install ts-node globally for local runs:
```bash
npm install -D ts-node typescript
```

### Environment variables
Create a `.env` file in the project root.
```bash
# Yellowstone gRPC
GRPC_ENDPOINT=your_yellowstone_grpc_endpoint

# Trading keypair (base58 secret key)
PRIVATE_KEY=base58_secret_key

# Concurrency controls (optional)
SNIPE_CONCURRENCY=4           # max parallel snipes
SNIPE_TIMEOUT_MS=8000         # per-snipe timeout
DEDUPE_TTL_MS=60000           # skip same mint within TTL
```

Notes:
- `PRIVATE_KEY` is used by `src/snipe.ts` to sign and send transactions.
- Some modules also use `src/config/index.ts`’s `connection`; update RPCs as needed.

### Quick start: run the listener
Use ts-node (or your preferred TS runner) to start the gRPC listener.
```bash
npx ts-node src/index.ts
```

You should see:
- “Starting Pump.fun token listener…”
- “Listening for new Pump.fun tokens…”
- Logs for detected token creations and the parsed token metadata

When a new token is detected, the bot enqueues a snipe task. Tasks are executed in parallel up to `SNIPE_CONCURRENCY`, each with a timeout `SNIPE_TIMEOUT_MS`, and duplicate mints are skipped within `DEDUPE_TTL_MS`.

### Sniping flow overview
1. `src/index.ts` subscribes to Yellowstone and filters transactions containing the Pump.fun `create` discriminator.
2. It decodes args with `BorshInstructionCoder`, extracts `mint` and `creator`, then enqueues a call to `SnipeToken(mint, creator)`.
3. `src/snipe.ts` uses `PumpHelper` to build buy instructions, simulates, then signs and sends the transaction.
4. Alternatively, you can integrate `executor/legacy.ts` or `executor/jito.ts` if you prefer different execution flows/bundles.

### Executing transactions (optional)
- Legacy send helper:
  - See `src/executor/legacy.ts` (`createAndSendV0Tx`, `execute`).
- Jito bundles:
  - See `src/executor/jito.ts` to send a bundle to multiple block engines and confirm.

These are scaffolds; integrate them into the sniping step if you want to actually submit transactions instead of simulating.

### Bundled buys (optional)
`src/bundleBuy.ts` demonstrates aggregating multiple wallets’ buy instructions into a single v0 transaction (potentially with an Address Lookup Table).
- Populate `src/buyersKeys.ts` with `Keypair` instances to include.
- Provide `lookup_address` (ALT) when using address lookup tables.
- The file currently simulates; adapt to send on mainnet if desired.

### Configuration and tuning
- `SNIPE_CONCURRENCY`: Increase to process more snipes in parallel; reduce if you see resource contention.
- `SNIPE_TIMEOUT_MS`: Keep small to avoid stuck tasks under network stalls.
- `DEDUPE_TTL_MS`: Protects against reprocessing the same mint too frequently.
- Commitment level is set to `processed` in `src/index.ts` for low latency.

### Safety and production notes
- Always simulate before sending; handle failures and slippage controls.
- Secure your `.env` and never commit private keys.
- Consider distinct senders for bundles and rate-limit RPC calls.
- Monitor blockhash expiry and recompile/sign transactions close to send time.

### Scripts (suggested)
You may add these npm scripts for convenience:
```json
{
  "scripts": {
    "start": "ts-node src/index.ts"
  }
}
```

Then run:
```bash
npm run start
```

### License
MIT


