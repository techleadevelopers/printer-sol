CREATE TABLE IF NOT EXISTS simulation_sessions (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  started_at TIMESTAMPTZ NOT NULL,
  balance_start_sol DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_simulated_sell_confirmed
  ON trades(state, side, confirmed_at);
