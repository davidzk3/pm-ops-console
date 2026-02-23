INSERT INTO market_incidents (market_id, day, status, note, created_by)
VALUES
  ('m2', CURRENT_DATE - 5, 'OPEN', 'Spread started widening. Possible LP pullback. Monitor depth.', 'ops'),
  ('m2', CURRENT_DATE - 3, 'MONITOR', 'Whale concentration rising. Consider tightening reward eligibility.', 'ops'),
  ('m3', CURRENT_DATE - 2, 'OPEN', 'Liquidity thin after new market launch. Consider bootstrapping incentives.', 'ops')
ON CONFLICT DO NOTHING;
