# Solana Sniper Bot — frontend console

Portuguese-language operations UI for the independently hosted Go API. This artifact is a viewer and request surface only: it contains no trading strategy, transaction construction, signing, private-key handling, or local business-data fixtures.

## Run locally

Install dependencies with `npm install`, then start the dashboard with `npm run dev`. Run `npm run typecheck` to validate the frontend types.

`VITE_API_BASE_URL` is the Go API origin without the `/api` suffix. When unset, the frontend uses the production Railway API at `https://quantum-sniper-sol-production.up.railway.app`. Set it explicitly when connecting this frontend to another Go API. Vercel currently rewrites unmatched paths to `index.html`; it does not proxy same-origin `/api` requests. Set `VITE_WS_URL` only when the event service is not reachable at the API host's `/ws/events`.

## Authentication and runtime

- The active web entry point uses the administrator password form at `/sign-in` and stores the one-hour bearer session issued by `/api/auth/login` in browser local storage. Protected API and WebSocket requests send this bearer token.
- The source still contains an unmounted Clerk route implementation, but the active `App()` renders `MasterRoutes`; Clerk is not the current login flow.
- The API client base is initialized in `src/services/api.ts` from `VITE_API_BASE_URL` or the production API default above.

## Go API contract expected by this UI

All HTTP operations go through the local hooks in `src/api-client`; nullable fields remain visibly unavailable rather than being converted to zero. Errors show **Aguardando conexão com o back-end**. RPC provider labels may be surfaced as candidates, but no status, price, latency, health, or production readiness is assumed without API data.

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
