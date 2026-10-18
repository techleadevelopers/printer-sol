CREATE TABLE IF NOT EXISTS simulation_sessions (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  started_at TEXT NOT NULL,
  balance_start_sol REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_simulated_sell_confirmed
  ON trades(state, side, confirmed_at);
