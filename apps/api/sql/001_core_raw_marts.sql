-- 001_core_raw_marts.sql
-- Creates raw/core/marts schemas + minimal tables to support the UI.

BEGIN;

-- 1) Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;

-- 2) RAW: append-only event landing table (idempotent ingest)
CREATE TABLE IF NOT EXISTS raw.trade_events (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  payload JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- extracted helpers (optional but very useful)
  ts TIMESTAMPTZ NULL,
  market_id TEXT NULL,
  trader_id TEXT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_trade_events_source_event
  ON raw.trade_events (source, source_event_id);

CREATE INDEX IF NOT EXISTS ix_raw_trade_events_market_ts
  ON raw.trade_events (market_id, ts);

-- 3) CORE: markets
CREATE TABLE IF NOT EXISTS core.markets (
  market_id TEXT PRIMARY KEY,
  protocol TEXT NOT NULL,
  chain TEXT NOT NULL,
  title TEXT NOT NULL,
  category TEXT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4) CORE: traders
CREATE TABLE IF NOT EXISTS core.traders (
  trader_id TEXT PRIMARY KEY,
  first_seen_at TIMESTAMPTZ NULL,
  last_seen_at TIMESTAMPTZ NULL
);

-- 5) CORE: trades (normalized)
CREATE TABLE IF NOT EXISTS core.trades (
  trade_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL REFERENCES core.markets (market_id) ON DELETE CASCADE,
  trader_id TEXT NOT NULL REFERENCES core.traders (trader_id) ON DELETE CASCADE,

  side TEXT NULL,               -- BUY/SELL (optional for now)
  price NUMERIC NULL,
  size NUMERIC NULL,
  notional NUMERIC NULL,

  ts TIMESTAMPTZ NOT NULL,
  day DATE NOT NULL,
  source TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_core_trades_market_ts
  ON core.trades (market_id, ts);

CREATE INDEX IF NOT EXISTS ix_core_trades_trader_ts
  ON core.trades (trader_id, ts);

CREATE INDEX IF NOT EXISTS ix_core_trades_day
  ON core.trades (day);

-- 6) MARTS: daily market rollup that powers inbox + snapshot timeline
CREATE TABLE IF NOT EXISTS marts.market_day (
  market_id TEXT NOT NULL REFERENCES core.markets (market_id) ON DELETE CASCADE,
  day DATE NOT NULL,

  volume NUMERIC NOT NULL DEFAULT 0,
  trades INTEGER NOT NULL DEFAULT 0,
  unique_traders INTEGER NOT NULL DEFAULT 0,

  spread_median NUMERIC NULL,
  depth_2pct_median NUMERIC NULL,
  concentration_hhi NUMERIC NULL,

  health_score NUMERIC NULL,
  risk_score NUMERIC NULL,

  flags JSONB NOT NULL DEFAULT '[]'::jsonb,

  PRIMARY KEY (market_id, day)
);

CREATE INDEX IF NOT EXISTS ix_marts_market_day_day
  ON marts.market_day (day);

COMMIT;