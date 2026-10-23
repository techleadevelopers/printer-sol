CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_one_active_per_mint
  ON trades(mint)
  WHERE state IN ('prepared','signed','submitted','pending','unknown');
