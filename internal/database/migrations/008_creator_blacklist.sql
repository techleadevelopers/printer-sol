CREATE TABLE IF NOT EXISTS creator_blacklist (
  creator TEXT PRIMARY KEY,
  blocked_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_creator_blacklist_blocked_at ON creator_blacklist(blocked_at);
