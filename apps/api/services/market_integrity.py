from datetime import date
from typing import Optional, Dict, Any

import psycopg

from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_integrity_v2_2026_03_08"


def compute_market_integrity_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    sql = """
with anchor as (
    select
        m.market_id,
        m.day
    from public.market_microstructure_daily m
    where m.day = %(day)s
    order by m.market_id
    limit %(limit_markets)s
),

base as (
    select
        a.market_id,
        a.day,

        mk.title,
        mk.url,
        mk.category,

        coalesce(r2.regime, 'unknown') as regime,
        coalesce(r2.regime_reason, 'unknown') as regime_reason,

        coalesce(r2.market_quality_score, 0)::double precision as market_quality_score,
        coalesce(r2.liquidity_health_score, 0)::double precision as liquidity_health_score,
        coalesce(r2.concentration_risk_score, 0)::double precision as concentration_risk_score,
        coalesce(r2.whale_volume_share, 0)::double precision as whale_volume_share,
        coalesce(r2.trades, 0)::int as trades,
        coalesce(r2.unique_traders, 0)::int as unique_traders,

        coalesce(rr.risk_score, 0)::double precision as radar_risk_score,
        coalesce(rr.needs_operator_review, false) as radar_review,
        coalesce(rr.whale_role_share, 0)::double precision as whale_role_share,
        coalesce(rr.speculator_role_share, 0)::double precision as speculator_role_share,
        coalesce(rr.neutral_role_share, 0)::double precision as neutral_role_share,
        coalesce(rr.possible_farmer_count, 0)::int as possible_farmer_count,

        coalesce(mm.manipulation_score, 0)::double precision as manipulation_score,
        coalesce(mm.needs_operator_review, false) as manipulation_review,
        coalesce(mm.primary_signal, 'none') as manipulation_signal,

        (r2.market_id is not null)::boolean as has_regime_data,
        (rr.market_id is not null)::boolean as has_radar_data,
        (mm.market_id is not null)::boolean as has_manipulation_data

    from anchor a
    left join public.markets mk
      on mk.market_id = a.market_id
    left join public.market_regime_daily_v2 r2
      on r2.market_id = a.market_id
     and r2.day = a.day
    left join public.market_risk_radar_daily rr
      on rr.market_id = a.market_id
     and rr.day = a.day
    left join public.market_manipulation_daily mm
      on mm.market_id = a.market_id
     and mm.day = a.day
),

coverage as (
    select
        b.*,
        (
            (
                case when b.has_regime_data then 1 else 0 end
                + case when b.has_radar_data then 1 else 0 end
                + case when b.has_manipulation_data then 1 else 0 end
            ) / 3.0
        )::double precision as data_completeness_score,

        (
            not b.has_regime_data
            or not b.has_radar_data
            or not b.has_manipulation_data
        ) as is_partial_coverage
    from base b
),

scored as (
    select
        c.*,

        greatest(
            0,
            least(
                100,
                (
                    100
                    - (c.radar_risk_score * 35.0)
                    - (c.manipulation_score * 35.0)
                    - (c.concentration_risk_score * 15.0)
                    - (
                        case
                            when c.regime = 'thin_market' then 10.0
                            else 0.0
                        end
                    )
                    - (
                        case
                            when c.is_partial_coverage then (1.0 - c.data_completeness_score) * 25.0
                            else 0.0
                        end
                    )
                    + (c.neutral_role_share * 10.0)
                    + (c.market_quality_score * 10.0)
                )
            )
        )::double precision as integrity_score_raw

    from coverage c
),

final as (
    select
        s.*,

        round(s.integrity_score_raw::numeric, 4)::double precision as integrity_score,

        case
            when s.integrity_score_raw >= 85 then 'strong'
            when s.integrity_score_raw >= 70 then 'stable'
            when s.integrity_score_raw >= 50 then 'fragile'
            when s.integrity_score_raw >= 30 then 'review'
            else 'critical'
        end as integrity_band,

        case
            when s.manipulation_review then 'high'
            when s.is_partial_coverage then 'high'
            when s.radar_review then 'medium'
            when s.integrity_score_raw < 50 then 'medium'
            else 'low'
        end as review_priority,

        case
            when s.is_partial_coverage then 'partial downstream coverage'
            when s.manipulation_review then concat('manipulation signal: ', s.manipulation_signal)
            when s.regime = 'thin_market' then 'thin market structure'
            when s.whale_role_share >= 0.50 then 'whale dominated participation'
            when s.speculator_role_share >= 0.60 then 'speculator dominated participation'
            when s.concentration_risk_score >= 0.50 then 'high concentration risk'
            when s.neutral_role_share >= 0.60 then 'neutral participation base'
            when s.market_quality_score >= 0.65 then 'strong market quality'
            else 'mixed structural signals'
        end as primary_reason,

        (
            s.manipulation_review
            or s.radar_review
            or s.integrity_score_raw < 50
            or s.is_partial_coverage
        ) as needs_operator_review

    from scored s
),

upserted as (
    insert into public.market_integrity_score_daily (
        market_id,
        day,
        title,
        url,
        category,
        regime,
        regime_reason,
        trades,
        unique_traders,
        market_quality_score,
        liquidity_health_score,
        concentration_risk_score,
        whale_volume_share,
        radar_risk_score,
        manipulation_score,
        manipulation_signal,
        whale_role_share,
        speculator_role_share,
        neutral_role_share,
        possible_farmer_count,
        has_regime_data,
        has_radar_data,
        has_manipulation_data,
        data_completeness_score,
        is_partial_coverage,
        integrity_score,
        integrity_band,
        review_priority,
        primary_reason,
        needs_operator_review
    )
    select
        f.market_id,
        f.day,
        f.title,
        f.url,
        f.category,
        f.regime,
        f.regime_reason,
        f.trades,
        f.unique_traders,
        f.market_quality_score,
        f.liquidity_health_score,
        f.concentration_risk_score,
        f.whale_volume_share,
        f.radar_risk_score,
        f.manipulation_score,
        f.manipulation_signal,
        f.whale_role_share,
        f.speculator_role_share,
        f.neutral_role_share,
        f.possible_farmer_count,
        f.has_regime_data,
        f.has_radar_data,
        f.has_manipulation_data,
        f.data_completeness_score,
        f.is_partial_coverage,
        f.integrity_score,
        f.integrity_band,
        f.review_priority,
        f.primary_reason,
        f.needs_operator_review
    from final f
    on conflict (market_id, day)
    do update set
        title = excluded.title,
        url = excluded.url,
        category = excluded.category,
        regime = excluded.regime,
        regime_reason = excluded.regime_reason,
        trades = excluded.trades,
        unique_traders = excluded.unique_traders,
        market_quality_score = excluded.market_quality_score,
        liquidity_health_score = excluded.liquidity_health_score,
        concentration_risk_score = excluded.concentration_risk_score,
        whale_volume_share = excluded.whale_volume_share,
        radar_risk_score = excluded.radar_risk_score,
        manipulation_score = excluded.manipulation_score,
        manipulation_signal = excluded.manipulation_signal,
        whale_role_share = excluded.whale_role_share,
        speculator_role_share = excluded.speculator_role_share,
        neutral_role_share = excluded.neutral_role_share,
        possible_farmer_count = excluded.possible_farmer_count,
        has_regime_data = excluded.has_regime_data,
        has_radar_data = excluded.has_radar_data,
        has_manipulation_data = excluded.has_manipulation_data,
        data_completeness_score = excluded.data_completeness_score,
        is_partial_coverage = excluded.is_partial_coverage,
        integrity_score = excluded.integrity_score,
        integrity_band = excluded.integrity_band,
        review_priority = excluded.review_priority,
        primary_reason = excluded.primary_reason,
        needs_operator_review = excluded.needs_operator_review,
        updated_at = now()
    returning 1
)

select count(*)::int as rows_written
from upserted;
"""

    effective_day = day or date.today()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists public.market_integrity_score_daily (
                    market_id text not null,
                    day date not null,

                    title text null,
                    url text null,
                    category text null,

                    regime text null,
                    regime_reason text null,

                    trades integer null,
                    unique_traders integer null,

                    market_quality_score double precision null,
                    liquidity_health_score double precision null,
                    concentration_risk_score double precision null,
                    whale_volume_share double precision null,

                    radar_risk_score double precision null,
                    manipulation_score double precision null,
                    manipulation_signal text null,

                    whale_role_share double precision null,
                    speculator_role_share double precision null,
                    neutral_role_share double precision null,
                    possible_farmer_count integer null,

                    has_regime_data boolean not null default false,
                    has_radar_data boolean not null default false,
                    has_manipulation_data boolean not null default false,
                    data_completeness_score double precision null,
                    is_partial_coverage boolean not null default false,

                    integrity_score double precision null,
                    integrity_band text null,
                    review_priority text null,
                    primary_reason text null,
                    needs_operator_review boolean not null default false,

                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now(),

                    primary key (market_id, day)
                );
                """
            )

            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists whale_role_share double precision null;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists speculator_role_share double precision null;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists neutral_role_share double precision null;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists possible_farmer_count integer null;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists has_regime_data boolean not null default false;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists has_radar_data boolean not null default false;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists has_manipulation_data boolean not null default false;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists data_completeness_score double precision null;
                """
            )
            cur.execute(
                """
                alter table public.market_integrity_score_daily
                add column if not exists is_partial_coverage boolean not null default false;
                """
            )

            cur.execute(
                """
                create index if not exists idx_market_integrity_score_daily_day
                on public.market_integrity_score_daily (day desc);
                """
            )

            cur.execute(
                """
                create index if not exists idx_market_integrity_score_daily_review
                on public.market_integrity_score_daily (day desc, needs_operator_review, integrity_score asc);
                """
            )

            cur.execute(
                """
                create index if not exists idx_market_integrity_score_daily_band
                on public.market_integrity_score_daily (day desc, integrity_band);
                """
            )

            cur.execute(
                """
                create index if not exists idx_market_integrity_score_daily_partial
                on public.market_integrity_score_daily (day desc, is_partial_coverage, data_completeness_score);
                """
            )

            cur.execute(sql, {"day": effective_day, "limit_markets": limit_markets})
            row = cur.fetchone()
            conn.commit()

    return {
        "engine_version": ENGINE_VERSION,
        "day": str(effective_day),
        "limit_markets": limit_markets,
        "rows_written": row[0] if row else 0,
        "status": "ok",
    }