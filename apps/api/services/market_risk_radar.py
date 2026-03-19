from datetime import date
from typing import Optional, Dict, Any

import psycopg
from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_risk_radar_v3_2026_03_17_D"


def compute_market_risk_radar_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    resolve_day_sql = """
        select coalesce(%(day)s::date, max(day))::date
        from public.market_regime_daily_v2;
    """

    delete_sql = """
        delete from public.market_risk_radar_daily
        where day = %(effective_day)s::date;
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

regime as (
    select
        r.market_id,
        r.day,
        r.regime,
        r.regime_reason,
        r.market_quality_score,
        r.liquidity_health_score,
        r.concentration_risk_score,
        r.whale_volume_share,
        coalesce(r.trades, 0)::int as trades,
        coalesce(r.unique_traders, 0)::int as unique_traders,
        coalesce(r.trader_count, 0)::int as trader_count
    from public.market_regime_daily_v2 r
    join eligible_markets e
      on e.market_id = r.market_id
    join params p
      on p.day = r.day
),

role_base as (
    select
        b.market_id,
        b.day,
        b.trader_id,
        coalesce(b.volume, 0)::double precision as volume,
        coalesce(b.trades, 0)::int as trades,
        coalesce(b.is_high_frequency, false) as is_high_frequency,
        coalesce(b.is_large_participant, false) as is_large_participant,
        coalesce(b.is_one_sided, false) as is_one_sided,
        coalesce(b.buy_ratio, 0.5)::double precision as buy_ratio,
        coalesce(b.avg_trade_size, 0)::double precision as avg_trade_size,
        coalesce(b.active_minutes, 0)::int as active_minutes,
        case
            when coalesce(b.is_high_frequency, false) and coalesce(b.avg_trade_size, 0) <= 2 then 'POSSIBLE_FARMER'
            when coalesce(b.is_large_participant, false) then 'WHALE'
            when (
                coalesce(b.is_one_sided, false) and coalesce(b.trades, 0) >= 2
            ) or (
                abs(coalesce(b.buy_ratio, 0.5) - 0.5) >= 0.25 and coalesce(b.trades, 0) >= 2
            ) then 'SPECULATOR'
            else 'NEUTRAL'
        end as cohort
    from public.trader_behavior_daily b
    join eligible_markets e
      on e.market_id = b.market_id
    join params p
      on p.day = b.day
),

roles as (
    select
        rb.market_id,
        rb.day,

        count(*)::int as total_role_rows,

        count(*) filter (where rb.cohort = 'WHALE')::int as whale_count,
        count(*) filter (where rb.cohort = 'SPECULATOR')::int as speculator_count,
        count(*) filter (where rb.cohort = 'NEUTRAL')::int as neutral_count,
        count(*) filter (where rb.is_high_frequency)::int as high_frequency_count,
        count(*) filter (where rb.cohort = 'POSSIBLE_FARMER')::int as possible_farmer_count,

        count(distinct rb.trader_id)::int as trader_count,

        coalesce(sum(rb.volume), 0)::double precision as total_volume,
        coalesce(sum(case when rb.cohort = 'WHALE' then rb.volume else 0 end), 0)::double precision as whale_volume,
        coalesce(sum(case when rb.cohort = 'SPECULATOR' then rb.volume else 0 end), 0)::double precision as speculator_volume,
        coalesce(sum(case when rb.cohort = 'NEUTRAL' then rb.volume else 0 end), 0)::double precision as neutral_volume,

        case
            when coalesce(sum(rb.volume), 0) > 0
                then coalesce(sum(case when rb.cohort = 'WHALE' then rb.volume else 0 end), 0)::double precision
                     / coalesce(sum(rb.volume), 0)::double precision
            else 0
        end as whale_role_share,

        case
            when coalesce(sum(rb.volume), 0) > 0
                then coalesce(sum(case when rb.cohort = 'SPECULATOR' then rb.volume else 0 end), 0)::double precision
                     / coalesce(sum(rb.volume), 0)::double precision
            else 0
        end as speculator_role_share,

        case
            when coalesce(sum(rb.volume), 0) > 0
                then coalesce(sum(case when rb.cohort = 'NEUTRAL' then rb.volume else 0 end), 0)::double precision
                     / coalesce(sum(rb.volume), 0)::double precision
            else 0
        end as neutral_role_share
    from role_base rb
    group by rb.market_id, rb.day
),

base as (
    select
        r.market_id,
        r.day,

        r.regime,
        r.regime_reason,

        r.market_quality_score,
        r.liquidity_health_score,
        r.concentration_risk_score,
        r.whale_volume_share,

        r.trades,
        r.unique_traders,
        coalesce(ro.trader_count, r.trader_count, 0)::int as trader_count,

        coalesce(ro.total_role_rows, 0)::int as total_role_rows,
        coalesce(ro.whale_count, 0)::int as whale_count,
        coalesce(ro.speculator_count, 0)::int as speculator_count,
        coalesce(ro.neutral_count, 0)::int as neutral_count,
        coalesce(ro.neutral_count, 0)::int as organic_count,
        coalesce(ro.high_frequency_count, 0)::int as high_frequency_count,
        coalesce(ro.possible_farmer_count, 0)::int as possible_farmer_count,

        coalesce(ro.total_volume, 0)::double precision as total_volume,
        coalesce(ro.whale_volume, 0)::double precision as trader_whale_volume,
        coalesce(ro.speculator_volume, 0)::double precision as trader_speculator_volume,
        coalesce(ro.neutral_volume, 0)::double precision as trader_neutral_volume,

        coalesce(ro.whale_role_share, 0)::double precision as whale_role_share,
        coalesce(ro.speculator_role_share, 0)::double precision as speculator_role_share,
        coalesce(ro.neutral_role_share, 0)::double precision as neutral_role_share

    from regime r
    left join roles ro
      on ro.market_id = r.market_id
     and ro.day = r.day
),

scored as (
    select
        b.*,

        (
            b.regime = 'thin_market'
            and coalesce(b.trades, 0) <= 1
            and coalesce(b.unique_traders, 0) <= 1
            and coalesce(b.trader_count, 0) <= 1
        ) as is_ultra_thin,

        (
            coalesce(b.trades, 0) >= 3
            or coalesce(b.unique_traders, 0) >= 3
            or coalesce(b.trader_count, 0) >= 3
        ) as has_real_participation,

        (
            b.regime = 'thin_market'
            and coalesce(b.trades, 0) <= 1
            and coalesce(b.unique_traders, 0) <= 1
            and coalesce(b.trader_count, 0) <= 1
            and coalesce(b.possible_farmer_count, 0) = 0
        ) as should_force_medium,

        (
            least(1.0, coalesce(b.concentration_risk_score, 0)) * 0.22
          + least(1.0, greatest(0.0, 1 - coalesce(b.liquidity_health_score, 0))) * 0.22
          + least(1.0, coalesce(b.whale_volume_share, 0)) * 0.14
          + least(1.0, coalesce(b.speculator_role_share, 0)) * 0.10
          + least(
                1.0,
                case
                    when b.possible_farmer_count >= 2 then 1
                    else b.possible_farmer_count::double precision / 2
                end
            ) * 0.10
          + case
                when b.regime = 'liquidity_collapse' then 0.12
                when b.regime = 'farming_dominated' then 0.10
                when b.regime = 'thin_market'
                     and coalesce(b.trades, 0) <= 1
                     and coalesce(b.unique_traders, 0) <= 1
                     and coalesce(b.trader_count, 0) <= 1
                    then 0.01
                when b.regime = 'thin_market'
                    then 0.03
                else 0.0
            end
        ) as risk_score_raw,

        case
            when b.whale_count > greatest(b.speculator_count, b.organic_count) then 'whale'
            when b.speculator_count > greatest(b.whale_count, b.organic_count) then 'speculator'
            when b.organic_count > greatest(b.whale_count, b.speculator_count) then 'neutral'
            when b.high_frequency_count > 0 then 'high_frequency'
            else 'unclear'
        end as dominant_role

    from base b
),

classified as (
    select
        s.*,

        round(s.risk_score_raw::numeric, 8) as risk_score,

        case
            when s.should_force_medium then 'medium'
            when s.regime = 'liquidity_collapse' then 'critical'
            when s.risk_score_raw >= 0.78 then 'critical'
            when s.risk_score_raw >= 0.56 then 'high'
            when s.risk_score_raw >= 0.30 then 'medium'
            else 'low'
        end as risk_tier,

        case
            when s.should_force_medium then
                array['thin_market', 'very_low_activity']::text[]
            else
                array_remove(array[
                    case when s.regime = 'thin_market' then 'thin_market' end,
                    case when s.regime = 'liquidity_collapse' then 'liquidity_collapse' end,
                    case when s.regime = 'farming_dominated' then 'farming_risk' end,
                    case when coalesce(s.whale_volume_share, 0) >= 0.50 and not s.is_ultra_thin then 'whale_dominance' end,
                    case when coalesce(s.concentration_risk_score, 0) >= 0.60 and s.has_real_participation then 'concentration_risk' end,
                    case when coalesce(s.liquidity_health_score, 0) < 0.30 then 'liquidity_weakness' end,
                    case when s.possible_farmer_count >= 2 then 'farmer_presence' end,
                    case when s.speculator_role_share >= 0.50 and s.has_real_participation then 'speculator_dominance' end,
                    case when s.is_ultra_thin then 'very_low_activity' end
                ]::text[], null)
        end as risk_labels,

        case
            when s.should_force_medium
                then 'market is very thin and should be monitored'
            when s.regime = 'liquidity_collapse'
                then 'liquidity has materially deteriorated'
            when s.regime = 'farming_dominated'
                then 'high frequency speculative flow may be dominating participation'
            when s.is_ultra_thin
                then 'market is very thin and should be monitored'
            when s.regime = 'thin_market'
                 and not s.has_real_participation
                then 'market is thin and structurally fragile'
            when coalesce(s.whale_volume_share, 0) >= 0.70 and s.has_real_participation
                then 'one or few large participants dominate market flow'
            when coalesce(s.concentration_risk_score, 0) >= 0.75 and s.has_real_participation
                then 'market participation is highly concentrated'
            when coalesce(s.liquidity_health_score, 0) < 0.30
                then 'liquidity conditions are weak'
            else 'no major structural alert'
        end as primary_risk_reason,

        case
            when s.should_force_medium
                then false
            else (
                s.regime in ('liquidity_collapse', 'farming_dominated')
                or (s.risk_score_raw >= 0.56)
                or (coalesce(s.whale_volume_share, 0) >= 0.70 and s.has_real_participation)
                or (coalesce(s.concentration_risk_score, 0) >= 0.75 and s.has_real_participation)
                or s.possible_farmer_count >= 2
            )
        end as needs_operator_review

    from scored s
),

upserted as (
    insert into public.market_risk_radar_daily (
        market_id,
        day,
        risk_score,
        risk_tier,
        primary_risk_reason,
        dominant_role,
        needs_operator_review,
        regime,
        regime_reason,
        market_quality_score,
        liquidity_health_score,
        concentration_risk_score,
        whale_volume_share,
        trades,
        unique_traders,
        trader_count,
        whale_count,
        speculator_count,
        organic_count,
        high_frequency_count,
        possible_farmer_count,
        whale_role_share,
        speculator_role_share,
        neutral_role_share,
        risk_labels
    )
    select
        c.market_id,
        c.day,
        c.risk_score,
        c.risk_tier,
        c.primary_risk_reason,
        c.dominant_role,
        c.needs_operator_review,
        c.regime,
        c.regime_reason,
        c.market_quality_score,
        c.liquidity_health_score,
        c.concentration_risk_score,
        c.whale_volume_share,
        c.trades,
        c.unique_traders,
        c.trader_count,
        c.whale_count,
        c.speculator_count,
        c.organic_count,
        c.high_frequency_count,
        c.possible_farmer_count,
        c.whale_role_share,
        c.speculator_role_share,
        c.neutral_role_share,
        c.risk_labels
    from classified c
    on conflict (market_id, day)
    do update set
        risk_score = excluded.risk_score,
        risk_tier = excluded.risk_tier,
        primary_risk_reason = excluded.primary_risk_reason,
        dominant_role = excluded.dominant_role,
        needs_operator_review = excluded.needs_operator_review,
        regime = excluded.regime,
        regime_reason = excluded.regime_reason,
        market_quality_score = excluded.market_quality_score,
        liquidity_health_score = excluded.liquidity_health_score,
        concentration_risk_score = excluded.concentration_risk_score,
        whale_volume_share = excluded.whale_volume_share,
        trades = excluded.trades,
        unique_traders = excluded.unique_traders,
        trader_count = excluded.trader_count,
        whale_count = excluded.whale_count,
        speculator_count = excluded.speculator_count,
        organic_count = excluded.organic_count,
        high_frequency_count = excluded.high_frequency_count,
        possible_farmer_count = excluded.possible_farmer_count,
        whale_role_share = excluded.whale_role_share,
        speculator_role_share = excluded.speculator_role_share,
        neutral_role_share = excluded.neutral_role_share,
        risk_labels = excluded.risk_labels
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
                delete_sql,
                {
                    "effective_day": effective_day,
                },
            )

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