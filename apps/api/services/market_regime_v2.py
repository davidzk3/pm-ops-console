from datetime import date
from typing import Optional, Dict, Any

import psycopg
from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_regime_v2_2026_03_17_C"


def compute_market_regime_daily_v2(
    day: Optional[date] = None,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    resolve_day_sql = """
        select coalesce(%(day)s::date, max(day))::date
        from public.market_microstructure_daily;
    """

    sql = """
with params as (
    select %(effective_day)s::date as day
),

eligible_markets as (
    select m.market_id
    from public.markets m
    where m.protocol = 'polymarket'
    order by m.market_id
    limit %(limit_markets)s
),

micro as (
    select
        mm.market_id,
        mm.day,
        coalesce(mm.window_hours, 24)::int as window_hours,
        coalesce(mm.trades, 0)::int as trades,
        coalesce(mm.unique_traders, 0)::int as unique_traders,
        coalesce(mm.volume, 0)::double precision as volume,
        coalesce(mm.avg_spread, 0)::double precision as avg_spread,
        coalesce(mm.price_volatility, 0)::double precision as price_volatility,
        coalesce(mm.bbo_ticks, 0)::int as bbo_ticks,
        coalesce(mm.top1_trader_share, 0)::double precision as top1_trader_share,
        coalesce(mm.top5_trader_share, 0)::double precision as top5_trader_share,
        coalesce(mm.hhi, 0)::double precision as hhi,
        coalesce(mm.burst_score, 0)::double precision as burst_score
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
        coalesce(mf.window_hours, 24)::int as window_hours,
        mf.market_quality_score::double precision as market_quality_score,
        mf.liquidity_health_score::double precision as liquidity_health_score,
        mf.concentration_risk_score::double precision as concentration_risk_score
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

base as (
    select
        m.market_id,
        m.day,
        m.window_hours,

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

        coalesce(t.trader_count, 0)::int as trader_count,
        coalesce(t.large_participant_count, 0)::int as large_participant_count,
        coalesce(t.one_sided_trader_count, 0)::int as one_sided_trader_count,
        coalesce(t.high_frequency_trader_count, 0)::int as high_frequency_trader_count,
        coalesce(t.whale_volume_share, 0)::double precision as whale_volume_share,

        f.market_quality_score as feature_market_quality_score,
        f.liquidity_health_score as feature_liquidity_health_score,
        f.concentration_risk_score as feature_concentration_risk_score,

        (
            case
                when coalesce(m.avg_spread, 0) <= 0.005 then 1.0
                when coalesce(m.avg_spread, 0) <= 0.010 then 0.85
                when coalesce(m.avg_spread, 0) <= 0.020 then 0.65
                when coalesce(m.avg_spread, 0) <= 0.050 then 0.35
                else 0.10
            end
        )::double precision as spread_health_score,

        (
            case
                when coalesce(m.bbo_ticks, 0) >= 1000 then 1.0
                when coalesce(m.bbo_ticks, 0) >= 250 then 0.75
                when coalesce(m.bbo_ticks, 0) >= 50 then 0.50
                when coalesce(m.bbo_ticks, 0) >= 10 then 0.25
                else 0.0
            end
        )::double precision as orderbook_presence_score,

        (
            case
                when coalesce(m.trades, 0) >= 100 then 1.0
                when coalesce(m.trades, 0) >= 30 then 0.75
                when coalesce(m.trades, 0) >= 10 then 0.50
                when coalesce(m.trades, 0) >= 5 then 0.25
                else 0.0
            end
        )::double precision as activity_score,

        (
            case
                when coalesce(m.unique_traders, 0) >= 50 then 1.0
                when coalesce(m.unique_traders, 0) >= 20 then 0.75
                when coalesce(m.unique_traders, 0) >= 10 then 0.50
                when coalesce(m.unique_traders, 0) >= 5 then 0.25
                else 0.0
            end
        )::double precision as participation_score,

        least(
            1.0,
            greatest(
                0.0,
                (
                    (coalesce(m.hhi, 0) * 0.60)
                    + (coalesce(m.top1_trader_share, 0) * 0.25)
                    + (coalesce(m.top5_trader_share, 0) * 0.15)
                )
            )
        )::double precision as derived_concentration_risk_score,

        (
            coalesce(m.trades, 0) < 5
            or coalesce(m.unique_traders, 0) < 5
            or coalesce(t.trader_count, 0) < 5
        ) as is_thin_market,

        (
            coalesce(m.trades, 0) = 0
            and coalesce(m.bbo_ticks, 0) = 0
        ) as is_inactive

    from micro m
    left join feat f
      on f.market_id = m.market_id
     and f.day = m.day
     and f.window_hours = m.window_hours
    left join trader_agg t
      on t.market_id = m.market_id
     and t.day = m.day
),

scored as (
    select
        b.market_id,
        b.day,
        b.window_hours,

        b.trades,
        b.unique_traders,
        b.volume,
        b.avg_spread,
        b.price_volatility,
        b.bbo_ticks,
        b.top1_trader_share,
        b.top5_trader_share,
        b.hhi,
        b.burst_score,

        coalesce(
            b.feature_liquidity_health_score,
            round(
                (
                    (b.spread_health_score * 0.50)
                    + (b.orderbook_presence_score * 0.30)
                    + (b.activity_score * 0.20)
                )::numeric,
                6
            )::double precision
        ) as liquidity_health_score,

        coalesce(
            b.feature_concentration_risk_score,
            round(b.derived_concentration_risk_score::numeric, 6)::double precision
        ) as concentration_risk_score,

        b.trader_count,
        b.large_participant_count,
        b.one_sided_trader_count,
        b.high_frequency_trader_count,
        b.whale_volume_share,

        coalesce(
            b.feature_market_quality_score,
            round(
                (
                    (
                        (
                            (b.spread_health_score * 0.50)
                            + (b.orderbook_presence_score * 0.30)
                            + (b.activity_score * 0.20)
                        ) * 0.45
                    )
                    + (b.participation_score * 0.35)
                    + ((1.0 - b.derived_concentration_risk_score) * 0.20)
                )::numeric,
                6
            )::double precision
        ) as market_quality_score,

        b.is_thin_market,
        b.is_inactive
    from base b
),

final as (
    select
        s.market_id,
        s.day,

        s.trades,
        s.unique_traders,
        s.volume,
        s.avg_spread,
        s.price_volatility,
        s.bbo_ticks,
        s.top1_trader_share,
        s.top5_trader_share,
        s.hhi,
        s.burst_score,

        s.market_quality_score,
        s.liquidity_health_score,
        s.concentration_risk_score,

        s.trader_count,
        s.large_participant_count,
        s.one_sided_trader_count,
        s.high_frequency_trader_count,
        s.whale_volume_share,

        case
            when s.is_inactive then 'inactive'

            when s.is_thin_market then 'thin_market'

            when coalesce(s.liquidity_health_score, 0) < 0.20
                 and coalesce(s.avg_spread, 0) > 0.05
                then 'liquidity_collapse'

            when coalesce(s.high_frequency_trader_count, 0) >= 3
                 and coalesce(s.unique_traders, 0) <= 10
                 and coalesce(s.trades, 0) >= 50
                then 'farming_dominated'

            when not s.is_thin_market and (
                 coalesce(s.whale_volume_share, 0) >= 0.50
                 or coalesce(s.top1_trader_share, 0) >= 0.35
                 or coalesce(s.concentration_risk_score, 0) >= 0.25
            )
                then 'whale_dominated'

            when not s.is_thin_market
                 and coalesce(s.market_quality_score, 0) >= 0.60
                 and coalesce(s.liquidity_health_score, 0) >= 0.35
                 and coalesce(s.concentration_risk_score, 0) < 0.50
                 and coalesce(s.whale_volume_share, 0) < 0.50
                then 'mixed'

            else 'mixed'
        end as regime,

        case
            when s.is_inactive
                then 'no trades and no orderbook activity'

            when s.is_thin_market
                then 'market too thin for strong structural classification'

            when coalesce(s.liquidity_health_score, 0) < 0.20
                 and coalesce(s.avg_spread, 0) > 0.05
                then 'thin orderbook and wide spreads'

            when coalesce(s.high_frequency_trader_count, 0) >= 3
                 and coalesce(s.unique_traders, 0) <= 10
                 and coalesce(s.trades, 0) >= 50
                then 'high trading intensity concentrated in few traders'

            when not s.is_thin_market and (
                 coalesce(s.whale_volume_share, 0) >= 0.50
                 or coalesce(s.top1_trader_share, 0) >= 0.35
                 or coalesce(s.concentration_risk_score, 0) >= 0.25
            )
                then 'large participants dominate market flow'

            when not s.is_thin_market
                 and coalesce(s.market_quality_score, 0) >= 0.60
                 and coalesce(s.liquidity_health_score, 0) >= 0.35
                 and coalesce(s.concentration_risk_score, 0) < 0.50
                 and coalesce(s.whale_volume_share, 0) < 0.50
                then 'balanced participation with acceptable liquidity and manageable concentration'

            else 'mixed market structure signals'
        end as regime_reason

    from scored s
),

upserted as (
    insert into public.market_regime_daily_v2 (
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

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(resolve_day_sql, {"day": day})
            resolved = cur.fetchone()
            effective_day = resolved[0] if resolved else None

            if effective_day is None:
                return {
                    "engine_version": ENGINE_VERSION,
                    "day": None,
                    "limit_markets": limit_markets,
                    "rows_written": 0,
                    "status": "ok",
                }

            cur.execute(
                sql,
                {
                    "effective_day": effective_day,
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