CREATE TABLE IF NOT EXISTS market_manual_overrides (
  id SERIAL PRIMARY KEY,
  market_id TEXT NOT NULL REFERENCES markets(market_id),
  day DATE NOT NULL,
  risk_score_override INT NULL,
  health_score_override INT NULL,
  note TEXT NULL,
  created_by TEXT NOT NULL DEFAULT 'operator',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (market_id, day)
);

-- Make sure note is nullable if table existed before
ALTER TABLE market_manual_overrides
  ALTER COLUMN note DROP NOT NULL;
