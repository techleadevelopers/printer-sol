CREATE TABLE IF NOT EXISTS paper_marks (
  mint TEXT PRIMARY KEY,
  entry_market_cap_sol REAL NOT NULL,
  current_market_cap_sol REAL NOT NULL,
  updated_at TEXT NOT NULL
);
