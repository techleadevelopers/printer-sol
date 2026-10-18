ALTER TABLE simulation_sessions
  ADD COLUMN state TEXT NOT NULL DEFAULT 'running';
