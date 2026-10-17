CREATE TABLE IF NOT EXISTS paper_marks (
  mint TEXT PRIMARY KEY,
  entry_market_cap_sol DOUBLE PRECISION NOT NULL,
  current_market_cap_sol DOUBLE PRECISION NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
