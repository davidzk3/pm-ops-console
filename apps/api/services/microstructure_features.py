# apps/api/services/microstructure_features.py
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn

ENGINE_VERSION = "microstructure_features_v2_flags_array_2026_03_03"


def compute_microstructure_features_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 5000,
) -> Dict[str, Any]:
    if day is None:
        day = date.today()

    start_ts = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end_ts = start_ts + timedelta(days=1)

    sql = """
with base as (
  select
    m.market_id,
    m.day,
    m.window_hours,

    m.volume,
    m.trades,
    m.unique_traders,

    m.hhi,
    m.price_volatility,
    m.bbo_ticks,
    m.avg_spread,

    m.suspicious_burst_flag,
    m.burst_score,

    m.identity_blind
  from market_microstructure_daily m
  where m.day = %(day)s::date
    and m.window_hours = %(window_hours)s::int
  order by m.market_id
  limit %(limit_markets)s
),
scored as (
  select
    b.*,

    -- Activity score (0..1)
    (
      0.5 * greatest(
        0.05,
        least(1.0, ln(1 + coalesce(b.trades,0))::numeric / ln(1 + 500)::numeric)
      )
      +
      0.5 * least(1.0, ln(1 + coalesce(b.unique_traders,0))::numeric / ln(1 + 200)::numeric)
    )::double precision as activity_score,

    -- Spread score (higher is better) (0..1)
    (1.0 - least(1.0, (coalesce(b.avg_spread, 0)::numeric / 0.03)))::double precision as spread_score,

    -- Depth score proxy via bbo ticks (0..1)
    (least(1.0, ln(1 + coalesce(b.bbo_ticks, 0))::numeric / ln(1 + 1000)::numeric))::double precision as depth_score,

    -- Volatility risk (higher is worse) (0..1)
    (least(1.0, (coalesce(b.price_volatility, 0)::numeric / 2.0)))::double precision as volatility_risk_score,

    -- Burst risk (higher is worse) (0..1)
    (
      case
        when coalesce(b.suspicious_burst_flag, false) then 1.0
        else least(1.0, (coalesce(b.burst_score, 0)::numeric / 3.0))
      end
    )::double precision as burst_risk_score,

    -- Concentration risk (higher is worse) (0..1)
    (
      case
        when coalesce(b.identity_blind, false)
          then least(1.0, coalesce(b.hhi, 0)::numeric * 0.75)
        else least(1.0, coalesce(b.hhi, 0)::numeric)
      end
    )::double precision as concentration_risk_score
  from base b
),
final as (
  select
    s.market_id,
    s.day,
    s.window_hours,

    -- Liquidity health (0..1)
    (
      0.45 * s.spread_score
      +
      0.35 * s.depth_score
      +
      0.20 * s.activity_score
    )::double precision as liquidity_health_score,

    -- Trading activity score (explicit)
    s.activity_score::double precision as trading_activity_score,

    -- Spread quality score (explicit)
    s.spread_score::double precision as spread_quality_score,

    -- Market quality (0..1)
    (
      0.55 * (
        0.45 * s.spread_score
        +
        0.35 * s.depth_score
        +
        0.20 * s.activity_score
      )
      +
      0.25 * (1.0 - s.concentration_risk_score)
      +
      0.10 * (1.0 - s.volatility_risk_score)
      +
      0.10 * (1.0 - s.burst_risk_score)
    )::double precision as market_quality_score,

    s.concentration_risk_score,
    s.volatility_risk_score,
    s.burst_risk_score,

    -- Flags (booleans)
    (s.activity_score < 0.15)::boolean as low_activity_flag,
    (s.concentration_risk_score > 0.85)::boolean as high_concentration_flag,
    (s.spread_score < 0.25)::boolean as wide_spread_flag,
    (s.volatility_risk_score > 0.70)::boolean as high_volatility_flag,
    (s.burst_risk_score > 0.70)::boolean as burst_flag,

    -- Proper text[] flag arrays (NULL if empty)
    (
      case
        when cardinality(array_remove(array[
          case when (s.activity_score < 0.15) then 'low_activity' end,
          case when (s.spread_score < 0.25) then 'wide_spread' end,
          case when (s.burst_risk_score > 0.70) then 'burst' end,
          case when (s.volatility_risk_score > 0.70) then 'high_volatility' end
        ], null)) = 0
        then null
        else array_remove(array[
          case when (s.activity_score < 0.15) then 'low_activity' end,
          case when (s.spread_score < 0.25) then 'wide_spread' end,
          case when (s.burst_risk_score > 0.70) then 'burst' end,
          case when (s.volatility_risk_score > 0.70) then 'high_volatility' end
        ], null)
      end
    )::text[] as liquidity_flags,

    (
      case
        when cardinality(array_remove(array[
          case when (s.concentration_risk_score > 0.85) then 'high_concentration' end
        ], null)) = 0
        then null
        else array_remove(array[
          case when (s.concentration_risk_score > 0.85) then 'high_concentration' end
        ], null)
      end
    )::text[] as concentration_flags,

    (
      case
        when cardinality(array_remove(array[
          case when (s.concentration_risk_score > 0.85) then 'high_concentration' end,
          case when (s.burst_risk_score > 0.70) then 'burst' end,
          case when (s.volatility_risk_score > 0.70) then 'high_volatility' end
        ], null)) = 0
        then null
        else array_remove(array[
          case when (s.concentration_risk_score > 0.85) then 'high_concentration' end,
          case when (s.burst_risk_score > 0.70) then 'burst' end,
          case when (s.volatility_risk_score > 0.70) then 'high_volatility' end
        ], null)
      end
    )::text[] as quality_flags,

    -- Keep raw sub-scores too (your table has these columns)
    s.activity_score,
    s.spread_score,
    s.depth_score
  from scored s
)

insert into market_microstructure_features_daily (
  market_id,
  day,
  window_hours,

  engine_version,

  liquidity_health_score,
  trading_activity_score,
  concentration_risk_score,
  spread_quality_score,
  volatility_risk_score,
  burst_risk_score,
  market_quality_score,

  low_activity_flag,
  high_concentration_flag,
  wide_spread_flag,
  high_volatility_flag,
  burst_flag,

  quality_flags,
  liquidity_flags,
  concentration_flags,

  activity_score,
  spread_score,
  depth_score,

  created_at,
  updated_at
)
select
  f.market_id,
  f.day,
  f.window_hours,

  %(engine_version)s::text as engine_version,

  f.liquidity_health_score,
  f.trading_activity_score,
  f.concentration_risk_score,
  f.spread_quality_score,
  f.volatility_risk_score,
  f.burst_risk_score,
  f.market_quality_score,

  f.low_activity_flag,
  f.high_concentration_flag,
  f.wide_spread_flag,
  f.high_volatility_flag,
  f.burst_flag,

  f.quality_flags,
  f.liquidity_flags,
  f.concentration_flags,

  f.activity_score,
  f.spread_score,
  f.depth_score,

  now(),
  now()
from final f
on conflict (market_id, day, window_hours) do update set
  engine_version = excluded.engine_version,

  liquidity_health_score = excluded.liquidity_health_score,
  trading_activity_score = excluded.trading_activity_score,
  concentration_risk_score = excluded.concentration_risk_score,
  spread_quality_score = excluded.spread_quality_score,
  volatility_risk_score = excluded.volatility_risk_score,
  burst_risk_score = excluded.burst_risk_score,
  market_quality_score = excluded.market_quality_score,

  low_activity_flag = excluded.low_activity_flag,
  high_concentration_flag = excluded.high_concentration_flag,
  wide_spread_flag = excluded.wide_spread_flag,
  high_volatility_flag = excluded.high_volatility_flag,
  burst_flag = excluded.burst_flag,

  quality_flags = excluded.quality_flags,
  liquidity_flags = excluded.liquidity_flags,
  concentration_flags = excluded.concentration_flags,

  activity_score = excluded.activity_score,
  spread_score = excluded.spread_score,
  depth_score = excluded.depth_score,

  updated_at = now()
returning market_id;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "window_hours": window_hours,
                    "limit_markets": limit_markets,
                    "engine_version": ENGINE_VERSION,
                },
            )
            rows = cur.fetchall()
            conn.commit()

    return {
        "engine_version": ENGINE_VERSION,
        "day": str(day),
        "window_hours": window_hours,
        "markets_written": len(rows),
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "status": "ok",
    }