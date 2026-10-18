CREATE TABLE IF NOT EXISTS creator_blacklist (
  creator TEXT PRIMARY KEY,
  blocked_until TIMESTAMPTZ NOT NULL,
  source_mint TEXT NOT NULL,
  reason TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_creator_blacklist_until ON creator_blacklist(blocked_until);
