-- Seed raw.trade_events with deterministic demo data
-- Matches columns: source, source_event_id, payload, ts, market_id, trader_id
-- id is auto, ingested_at defaults to now()

BEGIN;

-- optional: keep reruns clean
DELETE FROM raw.trade_events WHERE source IN ('seed');

WITH
markets AS (
  SELECT * FROM (VALUES
    ('m1','polymarket','polygon','crypto','Will BTC be above 100k by Friday?'),
    ('m2','polymarket','polygon','politics','Will Candidate X win the election?'),
    ('m3','probable','bnb','crypto','Will ETH outperform BTC this week?')
  ) AS t(market_id, protocol, chain, category, title)
),
traders AS (
  -- small set, repeated across markets
  SELECT * FROM (VALUES
    ('t0016'),('t0063'),('t0068'),('t0007'),('t0028'),
    ('t0003'),('t0029'),('t0004'),('t0037'),('t0053'),
    ('t0009'),('t0042'),('t0033'),('t0031'),('t0026'),
    ('t0001'),('t0056'),('t0069'),('t0070'),('t0041')
  ) AS t(trader_id)
),
days AS (
  -- last 20 days including today
  SELECT (CURRENT_DATE - gs)::date AS day
  FROM generate_series(0, 19) gs
),
events AS (
  SELECT
    m.market_id,
    m.protocol,
    m.chain,
    m.category,
    m.title,
    t.trader_id,
    d.day,
    -- spread and depth shaped to create visible regimes across markets
    CASE
      WHEN m.market_id = 'm2' THEN 0.0700 + (random() * 0.0300)   -- stressed spreads
      WHEN m.market_id = 'm3' THEN 0.0300 + (random() * 0.0300)   -- medium
      ELSE 0.0150 + (random() * 0.0200)                           -- lower
    END AS spread_median,
    CASE
      WHEN m.market_id = 'm2' THEN 500 + (random() * 700)         -- thinner depth
      WHEN m.market_id = 'm3' THEN 1200 + (random() * 3500)       -- ok
      ELSE 2000 + (random() * 4000)                               -- better
    END AS depth_2pct_median,
    CASE
      WHEN m.market_id = 'm2' THEN 0.36 + (random() * 0.08)       -- higher concentration
      WHEN m.market_id = 'm3' THEN 0.28 + (random() * 0.08)
      ELSE 0.22 + (random() * 0.08)
    END AS concentration_hhi,
    -- market activity
    (1000 + floor(random() * 5000))::int AS volume,
    (20 + floor(random() * 400))::int AS trades,
    (20 + floor(random() * 350))::int AS unique_traders,
    -- trade level fields
    CASE WHEN random() < 0.5 THEN 'BUY' ELSE 'SELL' END AS side,
    round((0.05 + random() * 0.90)::numeric, 4) AS price,
    round((0.5 + random() * 12.0)::numeric, 2) AS size,
    -- ts placed within the day
    (d.day::timestamp
      + make_interval(hours => (random() * 23)::int)
      + make_interval(mins  => (random() * 59)::int)
      + make_interval(secs  => (random() * 59)::int)
    ) AT TIME ZONE 'UTC' AS ts_utc
  FROM markets m
  CROSS JOIN days d
  CROSS JOIN LATERAL (
    -- choose a handful of traders per market per day
    SELECT trader_id
    FROM traders
    ORDER BY random()
    LIMIT 8
  ) t
)
INSERT INTO raw.trade_events (source, source_event_id, payload, ts, market_id, trader_id)
SELECT
  'seed' AS source,
  -- stable unique id per row so reruns do not collide after DELETE
  ('seed:' || e.market_id || ':' || e.trader_id || ':' || e.day::text || ':' || row_number() OVER (PARTITION BY e.market_id, e.trader_id, e.day ORDER BY e.ts_utc)) AS source_event_id,
  jsonb_build_object(
    'protocol', e.protocol,
    'chain', e.chain,
    'category', e.category,
    'title', e.title,
    'side', e.side,
    'price', e.price,
    'size', e.size,
    'notional', round((e.price * e.size)::numeric, 4),
    'spread_median', round(e.spread_median::numeric, 4),
    'depth_2pct_median', round(e.depth_2pct_median::numeric, 0),
    'concentration_hhi', round(e.concentration_hhi::numeric, 4),
    'volume', e.volume,
    'trades', e.trades,
    'unique_traders', e.unique_traders
  ) AS payload,
  e.ts_utc AS ts,
  e.market_id,
  e.trader_id
FROM events e;

COMMIT;

-- quick sanity check
-- SELECT source, count(*) FROM raw.trade_events GROUP BY 1 ORDER BY 1;