from datetime import date
from typing import Optional
import psycopg


def compute_market_social_intelligence_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
):
    dsn = get_db_dsn()

    q = """
    WITH target_day AS (
        SELECT COALESCE(%s::date, CURRENT_DATE) AS day
    ),

    base AS (
        SELECT
            li.market_id,
            li.day,

            -- -------------------------
            -- Demand / attention proxies
            -- -------------------------

            COALESCE(li.participation_quality_score, 0.5) AS attention_score,

            -- sentiment derived from structure (not flat)
            CASE
                WHEN li.manipulation_penalty > 0.25 THEN 0.35
                WHEN li.speculative_flow_penalty > 0.15 THEN 0.45
                WHEN li.launch_readiness_score > 0.6 THEN 0.65
                ELSE 0.55
            END AS sentiment_score,

            COALESCE(li.launch_readiness_score, 0.5) AS demand_score,
            COALESCE(li.liquidity_durability_score, 0.5) AS trend_velocity,

            -- -------------------------
            -- Synthetic but variable activity
            -- -------------------------

            -- mention_count scales with demand + participation
            GREATEST(
                5,
                FLOOR(20 * COALESCE(li.launch_readiness_score, 0.5)
                    + 15 * COALESCE(li.participation_quality_score, 0.5))
            )::int AS mention_count,

            -- source_count varies with confidence / breadth
            CASE
                WHEN li.launch_readiness_score > 0.65 THEN 5
                WHEN li.launch_readiness_score > 0.55 THEN 4
                WHEN li.launch_readiness_score > 0.45 THEN 3
                ELSE 2
            END AS source_count,

            -- confidence depends on penalties
            GREATEST(
                0.4,
                LEAST(
                    0.9,
                    0.75
                    - COALESCE(li.manipulation_penalty, 0)
                    - COALESCE(li.speculative_flow_penalty, 0)
                )
            ) AS confidence_score,

            -- -------------------------
            -- Recommendation
            -- -------------------------

            CASE
                WHEN li.launch_readiness_score > 0.6 THEN 'rising'
                WHEN li.launch_readiness_score > 0.5 THEN 'watch'
                ELSE 'weak'
            END AS recommendation,

            -- -------------------------
            -- Summary (rule-based, not static)
            -- -------------------------

            CASE
                WHEN li.launch_readiness_score > 0.6 AND li.liquidity_durability_score > 0.7
                    THEN 'strong structural signals with rising demand proxy'
                WHEN li.launch_readiness_score > 0.55
                    THEN 'demand proxy improving with stable participation'
                WHEN li.participation_quality_score > 0.7
                    THEN 'healthy participation but demand still forming'
                ELSE
                    'limited demand signals despite current activity'
            END AS summary,

            -- -------------------------
            -- Flags
            -- -------------------------

            ARRAY_REMOVE(ARRAY[
                CASE WHEN li.launch_readiness_score > 0.6 THEN 'DEMAND_PROXY_RISING' END,
                CASE WHEN li.participation_quality_score > 0.75 THEN 'STRONG_PARTICIPATION_BASE' END,
                CASE WHEN li.speculative_flow_penalty > 0.15 THEN 'SPECULATIVE_DEMAND_PRESENT' END,
                CASE WHEN li.manipulation_penalty > 0.25 THEN 'LOW_CONFIDENCE_PROXY' END
            ], NULL) AS flags

        FROM public.market_launch_intelligence_daily li
        JOIN target_day td ON li.day = td.day
        ORDER BY li.launch_readiness_score DESC
        LIMIT %s
    )

    INSERT INTO public.market_social_intelligence_daily (
        market_id,
        day,
        attention_score,
        sentiment_score,
        demand_score,
        trend_velocity,
        mention_count,
        source_count,
        confidence_score,
        recommendation,
        summary,
        flags,
        engine_version,
        created_at,
        updated_at
    )
    SELECT
        b.market_id,
        b.day,
        b.attention_score,
        b.sentiment_score,
        b.demand_score,
        b.trend_velocity,
        b.mention_count,
        b.source_count,
        b.confidence_score,
        b.recommendation,
        b.summary,
        b.flags,
        'market_social_intelligence_v2_proxy',
        NOW(),
        NOW()
    FROM base b
    ON CONFLICT (market_id, day)
    DO UPDATE SET
        attention_score = EXCLUDED.attention_score,
        sentiment_score = EXCLUDED.sentiment_score,
        demand_score = EXCLUDED.demand_score,
        trend_velocity = EXCLUDED.trend_velocity,
        mention_count = EXCLUDED.mention_count,
        source_count = EXCLUDED.source_count,
        confidence_score = EXCLUDED.confidence_score,
        recommendation = EXCLUDED.recommendation,
        summary = EXCLUDED.summary,
        flags = EXCLUDED.flags,
        engine_version = EXCLUDED.engine_version,
        updated_at = NOW();
    """

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (day, limit_markets))

    return {
        "engine_version": "market_social_intelligence_v2_proxy",
        "day": str(day) if day else None,
        "limit_markets": limit_markets,
        "status": "ok",
    }


# import placed at bottom to avoid circular import
from apps.api.db import get_db_dsn