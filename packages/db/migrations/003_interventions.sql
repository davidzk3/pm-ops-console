-- 003_interventions.sql
-- Interventions are concrete actions taken by ops, tied to a market and optionally an incident.

CREATE TABLE IF NOT EXISTS market_interventions (
  id            bigserial PRIMARY KEY,
  market_id     text NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
  incident_id   bigint NULL REFERENCES market_incidents(id) ON DELETE SET NULL,

  day           date NOT NULL DEFAULT CURRENT_DATE,

  action_code   text NOT NULL,
  title         text NOT NULL,
  status        text NOT NULL DEFAULT 'PLANNED', -- PLANNED | APPLIED | REVERTED | CANCELLED

  params        jsonb NOT NULL DEFAULT '{}'::jsonb,

  created_by    text NOT NULL DEFAULT 'operator',
  created_at    timestamptz NOT NULL DEFAULT now(),
  applied_at    timestamptz NULL
);

CREATE INDEX IF NOT EXISTS idx_market_interventions_market_day
  ON market_interventions(market_id, day DESC);

CREATE INDEX IF NOT EXISTS idx_market_interventions_incident
  ON market_interventions(incident_id);
