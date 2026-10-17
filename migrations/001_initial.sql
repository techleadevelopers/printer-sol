CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS token_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, mint TEXT NOT NULL, creator TEXT, received_at TEXT NOT NULL,
  raw_json TEXT NOT NULL, latency_ms INTEGER NOT NULL, UNIQUE(mint, received_at)
);
CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY, mint TEXT NOT NULL, side TEXT NOT NULL, quantity TEXT NOT NULL,
  requested_value_sol REAL NOT NULL, executed_value_sol REAL, fees_sol REAL, slippage_pct REAL NOT NULL,
  signature TEXT, detected_at TEXT, submitted_at TEXT, confirmed_at TEXT, state TEXT NOT NULL,
  entry_reason TEXT, exit_reason TEXT, realized_pnl_sol REAL, reconciliation_needed INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_mint_state ON trades(mint, state);
CREATE TABLE IF NOT EXISTS rpc_health (
  endpoint TEXT PRIMARY KEY, checked_at TEXT NOT NULL, latency_ms INTEGER, healthy INTEGER NOT NULL,
  http_status INTEGER, error TEXT, supports_read INTEGER NOT NULL DEFAULT 0
);
