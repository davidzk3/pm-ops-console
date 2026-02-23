INSERT INTO markets (market_id, protocol, chain, title, category, status, url)
VALUES
  ('m1','polymarket','polygon','Will BTC be above 100k by Friday?','crypto','open',NULL),
  ('m2','polymarket','polygon','Will Candidate X win the election?','politics','open',NULL),
  ('m3','probable','bnb','Will ETH outperform BTC this week?','crypto','open',NULL)
ON CONFLICT (market_id) DO NOTHING;

INSERT INTO market_metrics_daily
(market_id, day, volume, trades, unique_traders, spread_median, depth_2pct_median, concentration_hhi, health_score, risk_score)
VALUES
  ('m1', CURRENT_DATE, 120000, 840, 420, 0.018, 5500, 0.12, 78, 22),
  ('m2', CURRENT_DATE, 90000, 620, 310, 0.045, 1800, 0.28, 49, 71),
  ('m3', CURRENT_DATE, 25000, 120, 80, 0.030, 900, 0.22, 55, 52)
ON CONFLICT (market_id, day) DO NOTHING;

INSERT INTO market_flags_daily (market_id, day, flag_code, severity, details)
VALUES
  ('m2', CURRENT_DATE, 'SPREAD_BLOWOUT', 4, '{"spread_pct":0.045,"target":0.02}'),
  ('m2', CURRENT_DATE, 'WHALE_DOMINANCE', 4, '{"hhi":0.28,"top1_share":0.41}'),
  ('m3', CURRENT_DATE, 'THIN_LIQUIDITY', 3, '{"depth_2pct":900,"target":2500}')
ON CONFLICT (market_id, day, flag_code) DO NOTHING;
