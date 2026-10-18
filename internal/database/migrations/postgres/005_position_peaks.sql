ALTER TABLE paper_marks ADD COLUMN IF NOT EXISTS peak_market_cap_sol DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE live_marks ADD COLUMN IF NOT EXISTS peak_market_cap_sol DOUBLE PRECISION NOT NULL DEFAULT 0;
UPDATE paper_marks SET peak_market_cap_sol = current_market_cap_sol WHERE peak_market_cap_sol <= 0;
UPDATE live_marks SET peak_market_cap_sol = current_market_cap_sol WHERE peak_market_cap_sol <= 0;
