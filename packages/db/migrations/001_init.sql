CREATE TABLE IF NOT EXISTS markets (
  market_id TEXT PRIMARY KEY,
  protocol TEXT NOT NULL,
  chain TEXT NOT NULL,
  title TEXT NOT NULL,
  category TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  url TEXT
);

CREATE TABLE IF NOT EXISTS market_metrics_daily (
  market_id TEXT REFERENCES markets(market_id),
  day DATE NOT NULL,
  volume NUMERIC NOT NULL DEFAULT 0,
  trades INTEGER NOT NULL DEFAULT 0,
  unique_traders INTEGER NOT NULL DEFAULT 0,
  spread_median NUMERIC,
  depth_2pct_median NUMERIC,
  concentration_hhi NUMERIC,
  health_score NUMERIC,
  risk_score NUMERIC,
  PRIMARY KEY (market_id, day)
);

CREATE TABLE IF NOT EXISTS market_flags_daily (
  market_id TEXT REFERENCES markets(market_id),
  day DATE NOT NULL,
  flag_code TEXT NOT NULL,
  severity INTEGER NOT NULL,
  details JSONB,
  PRIMARY KEY (market_id, day, flag_code)
);
