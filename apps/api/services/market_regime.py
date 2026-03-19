from datetime import date
from typing import Optional, Dict, Any

import psycopg
from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_regime_v1_2026_03_06"


def compute_market_regime_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    sql = """
with params as (
    select coalesce(%(day)s::date, current_date) as day
),

eligible_markets as (
    select m.market_id
    from public.markets m
    where m.protocol = 'polymarket'
    limit %(limit_markets)s
),

micro as (
    select
        mm.market_id,
        mm.day,
        mm.trades,
        mm.unique_traders,
        mm.volume,
        mm.avg_spread,
        mm.price_volatility,
        mm.bbo_ticks,
        mm.top1_trader_share,
        mm.top5_trader_share,
        mm.hhi,
        mm.burst_score
    from public.market_microstructure_daily mm
    join eligible_markets e
      on e.market_id = mm.market_id
    join params p
      on mm.day = p.day
),

feat as (
    select
        mf.market_id,
        mf.day,
        mf.market_quality_score,
        mf.liquidity_health_score,
        mf.concentration_risk_score
    from public.market_microstructure_features_daily mf
    join eligible_markets e
      on e.market_id = mf.market_id
    join params p
      on mf.day = p.day
),

trader_agg as (
    select
        tb.market_id,
        tb.day,
        count(*)::int as trader_count,
        sum(case when tb.is_large_participant then 1 else 0 end)::int as large_participant_count,
        sum(case when tb.is_one_sided then 1 else 0 end)::int as one_sided_trader_count,
        sum(case when tb.is_high_frequency then 1 else 0 end)::int as high_frequency_trader_count,
        coalesce(
            sum(case when tb.is_large_participant then tb.market_volume_share else 0 end),
            0
        )::double precision as whale_volume_share
    from public.trader_behavior_daily tb
    join eligible_markets e
      on e.market_id = tb.market_id
    join params p
      on tb.day = p.day
    group by tb.market_id, tb.day
),

final as (
    select
        m.market_id,
        m.day,

        m.trades,
        m.unique_traders,
        m.volume,
        m.avg_spread,
        m.price_volatility,
        m.bbo_ticks,
        m.top1_trader_share,
        m.top5_trader_share,
        m.hhi,
        m.burst_score,

        f.market_quality_score,
        f.liquidity_health_score,
        f.concentration_risk_score,

        coalesce(t.trader_count, 0) as trader_count,
        coalesce(t.large_participant_count, 0) as large_participant_count,
        coalesce(t.one_sided_trader_count, 0) as one_sided_trader_count,
        coalesce(t.high_frequency_trader_count, 0) as high_frequency_trader_count,
        coalesce(t.whale_volume_share, 0) as whale_volume_share,

        case
            when coalesce(m.trades, 0) = 0 and coalesce(m.bbo_ticks, 0) = 0
                then 'inactive'
            when coalesce(f.liquidity_health_score, 0) < 0.20
                 and coalesce(m.avg_spread, 0) > 0.05
                then 'liquidity_collapse'
            when coalesce(t.high_frequency_trader_count, 0) >= 3
                 and coalesce(m.unique_traders, 0) <= 10
                 and coalesce(m.trades, 0) >= 50
                then 'farming_dominated'
            when coalesce(t.whale_volume_share, 0) >= 0.50
                 or coalesce(m.top1_trader_share, 0) >= 0.35
                 or coalesce(m.hhi, 0) >= 0.25
                then 'whale_dominated'
            when coalesce(f.market_quality_score, 0) >= 0.60
                 and coalesce(f.liquidity_health_score, 0) >= 0.35
                 and coalesce(f.concentration_risk_score, 0) < 0.50
                then 'healthy'
            else 'mixed'
        end as regime,

        case
            when coalesce(m.trades, 0) = 0 and coalesce(m.bbo_ticks, 0) = 0
                then 'no trades and no orderbook activity'
            when coalesce(f.liquidity_health_score, 0) < 0.20
                 and coalesce(m.avg_spread, 0) > 0.05
                then 'thin orderbook and wide spreads'
            when coalesce(t.high_frequency_trader_count, 0) >= 3
                 and coalesce(m.unique_traders, 0) <= 10
                 and coalesce(m.trades, 0) >= 50
                then 'high trading intensity concentrated in few traders'
            when coalesce(t.whale_volume_share, 0) >= 0.50
                 or coalesce(m.top1_trader_share, 0) >= 0.35
                 or coalesce(m.hhi, 0) >= 0.25
                then 'large participants dominate market flow'
            when coalesce(f.market_quality_score, 0) >= 0.60
                 and coalesce(f.liquidity_health_score, 0) >= 0.35
                 and coalesce(f.concentration_risk_score, 0) < 0.50
                then 'good liquidity and manageable concentration'
            else 'mixed market structure signals'
        end as regime_reason

    from micro m
    left join feat f
      on f.market_id = m.market_id
     and f.day = m.day
    left join trader_agg t
      on t.market_id = m.market_id
     and t.day = m.day
),

upserted as (
    insert into public.market_regime_daily (
        market_id,
        day,
        trades,
        unique_traders,
        volume,
        avg_spread,
        price_volatility,
        bbo_ticks,
        top1_trader_share,
        top5_trader_share,
        hhi,
        burst_score,
        market_quality_score,
        liquidity_health_score,
        concentration_risk_score,
        trader_count,
        large_participant_count,
        one_sided_trader_count,
        high_frequency_trader_count,
        whale_volume_share,
        regime,
        regime_reason
    )
    select
        f.market_id,
        f.day,
        f.trades,
        f.unique_traders,
        f.volume,
        f.avg_spread,
        f.price_volatility,
        f.bbo_ticks,
        f.top1_trader_share,
        f.top5_trader_share,
        f.hhi,
        f.burst_score,
        f.market_quality_score,
        f.liquidity_health_score,
        f.concentration_risk_score,
        f.trader_count,
        f.large_participant_count,
        f.one_sided_trader_count,
        f.high_frequency_trader_count,
        f.whale_volume_share,
        f.regime,
        f.regime_reason
    from final f
    on conflict (market_id, day)
    do update set
        trades = excluded.trades,
        unique_traders = excluded.unique_traders,
        volume = excluded.volume,
        avg_spread = excluded.avg_spread,
        price_volatility = excluded.price_volatility,
        bbo_ticks = excluded.bbo_ticks,
        top1_trader_share = excluded.top1_trader_share,
        top5_trader_share = excluded.top5_trader_share,
        hhi = excluded.hhi,
        burst_score = excluded.burst_score,
        market_quality_score = excluded.market_quality_score,
        liquidity_health_score = excluded.liquidity_health_score,
        concentration_risk_score = excluded.concentration_risk_score,
        trader_count = excluded.trader_count,
        large_participant_count = excluded.large_participant_count,
        one_sided_trader_count = excluded.one_sided_trader_count,
        high_frequency_trader_count = excluded.high_frequency_trader_count,
        whale_volume_share = excluded.whale_volume_share,
        regime = excluded.regime,
        regime_reason = excluded.regime_reason
    returning 1
)

select count(*)::int as rows_written
from upserted;
"""

    effective_day = day or date.today()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "limit_markets": limit_markets,
                },
            )
            row = cur.fetchone()
            rows_written = row[0] if row else 0
            conn.commit()

    return {
        "engine_version": ENGINE_VERSION,
        "day": str(effective_day),
        "limit_markets": limit_markets,
        "rows_written": rows_written,
        "status": "ok",
    }