# Solana Sniper Bot — frontend console

Portuguese-language operations UI for the independently hosted Go API. This artifact is a viewer and request surface only: it contains no trading strategy, transaction construction, signing, private-key handling, or local business-data fixtures.

## Run locally

Install workspace dependencies with `pnpm install`, then start the dashboard with `pnpm --filter @workspace/solana-sniper-bot run dev`. Replit's managed web workflow starts it in the preview. Run `pnpm --filter @workspace/solana-sniper-bot run typecheck` to validate the frontend types.

Copy `.env.example` to a local `.env` only when configuring a separate Go API. `VITE_API_BASE_URL` is the API origin without the `/api` suffix; leave it empty for same-origin routing. Set `VITE_WS_URL` only when the event service is not reachable at the API host's `/ws/events`. Clerk publishable configuration is managed by Replit Auth; never place `CLERK_SECRET_KEY` in this file.

## Authentication and runtime

- Clerk is mounted in the web app with Wouter's `import.meta.env.BASE_URL` base path. The public landing page remains available signed out; authenticated visitors at `/` go to `/user-portal`; signed-out portal routes return to `/`.
- Clerk's sign-in and sign-up routes intentionally use the exact optional callback paths `/sign-in/*?` and `/sign-up/*?`. The browser uses the host-resolved publishable key and unconditionally passes `VITE_CLERK_PROXY_URL` to `ClerkProvider`.
- The API client base is initialized in `src/services/api.ts` from `VITE_API_BASE_URL`. When unset, generated API requests stay same-origin and use Clerk's browser session cookie. No bearer token, auth token getter, or browser storage is used.
- Configure `VITE_CLERK_PUBLISHABLE_KEY` and the deployment-managed `VITE_CLERK_PROXY_URL` as described by the Clerk integration. The parent workspace configures the server-side Clerk proxy.

## Go API contract expected by this UI

All HTTP operations go through generated hooks in `@workspace/api-client-react`; nullable fields remain visibly unavailable rather than being converted to zero. Errors show **Aguardando conexão com o back-end**. RPC provider labels may be surfaced as candidates, but no status, price, latency, health, or production readiness is assumed without API data.

| Method | Path | UI use |
| --- | --- | --- |
| GET | `/api/health` | API and event-monitor health |
| GET | `/api/dashboard` | Bot state, balances, aggregates, P&L, balance history, trades |
| GET | `/api/tokens` | Cursor-paginated token detections and analysis results |
| GET | `/api/positions` | Cursor-paginated positions |
| POST | `/api/positions/{positionId}/sell` | Manual sell request; UI waits for accepted response |
| GET | `/api/trades` | Cursor-paginated trade history and filters |
| GET | `/api/rpcs` | Configured endpoints and observed RPC metrics |
| POST | `/api/rpcs/test` | Backend RPC test request |
| POST | `/api/rpcs/select` | Primary, backup, or disabled role |
| GET | `/api/config` | Accepted strategy configuration |
| POST | `/api/config` | Submit validated strategy configuration |
| GET | `/api/wallet` | Public address, balances, holdings and sync status only |
| GET | `/api/logs` | Filtered operational logs |
| POST | `/api/bot/start`, `/stop`, `/pause`, `/resume` | Bot lifecycle requests |
| POST | `/api/bot/mode` | Simulation/real mode request; real mode asks confirmation |

Generated mutation success is surfaced only when the backend returns `accepted: true` (strategy save waits for the saved configuration response). Stop means stop new operations only and does not imply that open positions have been closed. Realized and estimated P&L are displayed independently. Valid transaction signatures link to Solscan; other values are not linked.

## WebSocket event contract

`src/services/websocket.ts` expects JSON text frames with a string `type` and optional ISO `timestamp` at `/ws/events`. It reconnects with bounded backoff and ignores malformed frames. Supported event names that trigger generated-query refreshes:

- `health`
- `bot.status`, `bot.mode`
- `token.detected`
- `position.updated`
- `trade.updated`
- `rpc.updated`
- `log.created`

The UI does not treat a WebSocket event as a transaction or action confirmation; the HTTP API response remains authoritative. Set `VITE_WS_URL` to the full `ws://` or `wss://` event URL if it differs from the API host's `/ws/events`.

## Pages

Public: `/`, `/sign-in/*?`, `/sign-up/*?`. Authenticated: `/user-portal`, `/user-portal/tokens`, `/user-portal/positions`, `/user-portal/trades`, `/user-portal/rpcs`, `/user-portal/strategy`, `/user-portal/wallet`, `/user-portal/logs`.
