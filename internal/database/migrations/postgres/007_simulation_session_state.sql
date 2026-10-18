ALTER TABLE simulation_sessions
  ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'running';
