-- Create synthetic history for the last 13 days based on today's row
-- This gives you a believable deterioration / improvement curve for demos.

INSERT INTO market_metrics_daily (
  market_id,
  day,
  volume,
  trades,
  unique_traders,
  spread_median,
  depth_2pct_median,
  concentration_hhi,
  health_score,
  risk_score
)
SELECT
  md.market_id,
  CURRENT_DATE - s.day_offset AS day,

  -- volume/trades/users slowly decay as market gets worse
  GREATEST(0, (md.volume * (1 - s.day_offset * 0.04))::numeric)::numeric AS volume,
  GREATEST(0, (md.trades * (1 - s.day_offset * 0.03))::numeric)::numeric AS trades,
  GREATEST(0, (md.unique_traders * (1 - s.day_offset * 0.02))::numeric)::numeric AS unique_traders,

  -- spread increases as market degrades
  CASE
    WHEN md.spread_median IS NULL THEN NULL
    ELSE (md.spread_median * (1 + s.day_offset * 0.08))::numeric
  END AS spread_median,

  -- depth decreases as market degrades
  CASE
    WHEN md.depth_2pct_median IS NULL THEN NULL
    ELSE GREATEST(0, (md.depth_2pct_median * (1 - s.day_offset * 0.06))::numeric)
  END AS depth_2pct_median,

  -- concentration drifts upward slightly
  CASE
    WHEN md.concentration_hhi IS NULL THEN NULL
    ELSE LEAST(1, (md.concentration_hhi + s.day_offset * 0.01))::numeric
  END AS concentration_hhi,

  -- health down, risk up
  LEAST(100, GREATEST(0, md.health_score - s.day_offset * 3)) AS health_score,
  LEAST(100, GREATEST(0, md.risk_score + s.day_offset * 4)) AS risk_score

FROM market_metrics_daily md
CROSS JOIN (VALUES
  (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13)
) s(day_offset)

-- only use today's snapshot as the base
WHERE md.day = CURRENT_DATE

-- don't double insert if you re-run
ON CONFLICT (market_id, day) DO NOTHING;
