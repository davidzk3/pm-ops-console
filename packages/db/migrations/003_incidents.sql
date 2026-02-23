CREATE TABLE IF NOT EXISTS market_incidents (
  id BIGSERIAL PRIMARY KEY,
  market_id TEXT NOT NULL,
  day DATE NOT NULL,
  status TEXT NOT NULL DEFAULT 'OPEN', -- OPEN | MONITOR | RESOLVED
  note TEXT NOT NULL,
  created_by TEXT NOT NULL DEFAULT 'operator',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_incidents_market_day
  ON market_incidents (market_id, day);

-- optional: prevent duplicates if you want
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_market_incidents_unique
--   ON market_incidents (market_id, day, note);
