from datetime import date
from typing import Optional, Dict, Any

import psycopg

from apps.api.db import get_db_dsn

ENGINE_VERSION = "market_launch_intelligence_v1_2026_03_18"


def compute_market_launch_intelligence_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    resolve_day_sql = """
        SELECT COALESCE(%(day)s::date, MAX(day))::date
        FROM public.market_integrity_score_daily;
    """

    delete_sql = """
        DELETE FROM public.market_launch_intelligence_daily
        WHERE day = %(effective_day)s::date;
    """

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.market_launch_intelligence_daily (
        market_id text NOT NULL,
        day date NOT NULL,
        launch_readiness_score double precision,
        launch_risk_score double precision,
        participation_quality_score double precision,
        liquidity_durability_score double precision,
        concentration_penalty double precision,
        speculative_flow_penalty double precision,
        manipulation_penalty double precision,
        recommendation text,
        recommendation_reason text,
        flags text[],
        engine_version text,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (market_id, day)
    );
    """

    sql = """
    WITH params AS (
        SELECT %(effective_day)s::date AS day
    ),

    eligible_markets AS (
        SELECT market_id
        FROM public.markets
        WHERE protocol = 'polymarket'
        ORDER BY market_id
        LIMIT %(limit_markets)s
    ),

    integrity AS (
        SELECT
            i.market_id,
            i.day,
            COALESCE(i.market_quality_score, 0.0) AS market_quality_score,
            COALESCE(i.liquidity_health_score, 0.0) AS liquidity_health_score,
            COALESCE(i.concentration_risk_score, 0.0) AS concentration_risk_score,
            COALESCE(i.manipulation_score, 0.0) AS manipulation_score,
            COALESCE(i.whale_role_share, 0.0) AS whale_role_share,
            COALESCE(i.speculator_role_share, 0.0) AS speculator_role_share,
            COALESCE(i.neutral_role_share, 0.0) AS neutral_role_share,
            COALESCE(i.possible_farmer_count, 0) AS possible_farmer_count,
            COALESCE(i.review_priority, 'low') AS review_priority,
            COALESCE(i.primary_reason, 'no_reason') AS primary_reason
        FROM public.market_integrity_score_daily i
        JOIN eligible_markets e
          ON e.market_id = i.market_id
        JOIN params p
          ON p.day = i.day
    ),

    micro AS (
        SELECT
            m.market_id,
            m.day,
            COALESCE(m.volume, 0.0) AS volume,
            COALESCE(m.trades, 0) AS trades,
            COALESCE(m.unique_traders, 0) AS unique_traders,
            COALESCE(m.avg_spread, 0.0) AS spread_median,
            COALESCE(m.hhi, 0.0) AS concentration_hhi
        FROM public.market_microstructure_daily m
        JOIN eligible_markets e
          ON e.market_id = m.market_id
        JOIN params p
          ON p.day = m.day
    ),

    impact AS (
        SELECT
            x.market_id,
            x.day,
            COALESCE(x.neutral_share_delta, 0.0) AS neutral_share_delta,
            COALESCE(x.whale_share_recent, 0.0) AS whale_share_recent,
            COALESCE(x.speculator_share_recent, 0.0) AS speculator_share_recent
        FROM (
            SELECT
                i.market_id,
                i.day,
                0.0::double precision AS neutral_share_delta,
                COALESCE(i.whale_role_share, 0.0) AS whale_share_recent,
                COALESCE(i.speculator_role_share, 0.0) AS speculator_share_recent
            FROM public.market_integrity_score_daily i
            JOIN eligible_markets e
              ON e.market_id = i.market_id
            JOIN params p
              ON p.day = i.day
        ) x
    ),

    scored AS (
        SELECT
            integ.market_id,
            integ.day,

            LEAST(
                1.0,
                GREATEST(
                    0.0,
                    (
                        0.35 * integ.neutral_role_share
                      + 0.25 * integ.liquidity_health_score
                      + 0.20 * integ.market_quality_score
                      + 0.10 * CASE
                            WHEN micro.unique_traders >= 20 THEN 1.0
                            WHEN micro.unique_traders >= 10 THEN 0.7
                            WHEN micro.unique_traders >= 5 THEN 0.4
                            ELSE 0.1
                        END
                      + 0.10 * CASE
                            WHEN micro.trades >= 40 THEN 1.0
                            WHEN micro.trades >= 20 THEN 0.7
                            WHEN micro.trades >= 10 THEN 0.4
                            ELSE 0.1
                        END
                    )
                )
            ) AS participation_quality_score,

            LEAST(
                1.0,
                GREATEST(
                    0.0,
                    (
                        0.45 * integ.liquidity_health_score
                      + 0.20 * integ.market_quality_score
                      + 0.15 * CASE
                            WHEN micro.spread_median <= 0.01 THEN 1.0
                            WHEN micro.spread_median <= 0.03 THEN 0.7
                            WHEN micro.spread_median <= 0.06 THEN 0.4
                            ELSE 0.1
                        END
                      + 0.20 * CASE
                            WHEN micro.volume >= 10000 THEN 1.0
                            WHEN micro.volume >= 2500 THEN 0.7
                            WHEN micro.volume >= 500 THEN 0.4
                            ELSE 0.1
                        END
                    )
                )
            ) AS liquidity_durability_score,

            LEAST(1.0, GREATEST(0.0, integ.concentration_risk_score)) AS concentration_penalty,
            LEAST(1.0, GREATEST(0.0, integ.speculator_role_share)) AS speculative_flow_penalty,
            LEAST(1.0, GREATEST(0.0, integ.manipulation_score)) AS manipulation_penalty,

            integ.neutral_role_share,
            integ.whale_role_share,
            integ.speculator_role_share,
            integ.possible_farmer_count,
            integ.review_priority,
            integ.primary_reason,
            micro.volume,
            micro.trades,
            micro.unique_traders,
            micro.spread_median,
            micro.concentration_hhi
        FROM integrity integ
        LEFT JOIN micro
          ON micro.market_id = integ.market_id
         AND micro.day = integ.day
        LEFT JOIN impact
          ON impact.market_id = integ.market_id
         AND impact.day = integ.day
    ),

    final AS (
        SELECT
            s.market_id,
            s.day,

            ROUND(
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        (
                            0.45 * s.participation_quality_score
                          + 0.35 * s.liquidity_durability_score
                          - 0.10 * s.concentration_penalty
                          - 0.05 * s.speculative_flow_penalty
                          - 0.05 * s.manipulation_penalty
                        )
                    )
                )::numeric,
                8
            )::double precision AS launch_readiness_score,

            ROUND(
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        (
                            0.40 * s.concentration_penalty
                          + 0.30 * s.speculative_flow_penalty
                          + 0.30 * s.manipulation_penalty
                        )
                    )
                )::numeric,
                8
            )::double precision AS launch_risk_score,

            ROUND(s.participation_quality_score::numeric, 8)::double precision AS participation_quality_score,
            ROUND(s.liquidity_durability_score::numeric, 8)::double precision AS liquidity_durability_score,
            ROUND(s.concentration_penalty::numeric, 8)::double precision AS concentration_penalty,
            ROUND(s.speculative_flow_penalty::numeric, 8)::double precision AS speculative_flow_penalty,
            ROUND(s.manipulation_penalty::numeric, 8)::double precision AS manipulation_penalty,

            CASE
                WHEN (
                    (
                        0.45 * s.participation_quality_score
                      + 0.35 * s.liquidity_durability_score
                      - 0.10 * s.concentration_penalty
                      - 0.05 * s.speculative_flow_penalty
                      - 0.05 * s.manipulation_penalty
                    ) >= 0.60
                    AND
                    (
                        0.40 * s.concentration_penalty
                      + 0.30 * s.speculative_flow_penalty
                      + 0.30 * s.manipulation_penalty
                    ) <= 0.18
                ) THEN 'launch_ready'
                WHEN (
                    (
                        0.45 * s.participation_quality_score
                      + 0.35 * s.liquidity_durability_score
                      - 0.10 * s.concentration_penalty
                      - 0.05 * s.speculative_flow_penalty
                      - 0.05 * s.manipulation_penalty
                    ) >= 0.50
                    AND
                    (
                        0.40 * s.concentration_penalty
                      + 0.30 * s.speculative_flow_penalty
                      + 0.30 * s.manipulation_penalty
                    ) <= 0.30
                ) THEN 'monitor_then_launch'
                ELSE 'not_ready'
            END AS recommendation,

            CASE
                WHEN s.manipulation_penalty >= 0.60 THEN 'high manipulation risk'
                WHEN s.concentration_penalty >= 0.60 THEN 'high concentration risk'
                WHEN s.participation_quality_score < 0.40 THEN 'weak participation quality'
                WHEN s.liquidity_durability_score < 0.40 THEN 'weak liquidity durability'
                WHEN s.speculative_flow_penalty >= 0.50 THEN 'speculative participation too high'
                WHEN s.participation_quality_score >= 0.70
                 AND s.liquidity_durability_score >= 0.60
                    THEN 'strong participation and liquidity foundation'
                WHEN s.neutral_role_share >= 0.60
                    THEN 'healthy neutral participant base'
                ELSE 'balanced structural launch profile'
            END AS recommendation_reason,

            ARRAY_REMOVE(ARRAY[
                CASE WHEN s.participation_quality_score < 0.40 THEN 'LOW_PARTICIPATION_QUALITY' END,
                CASE WHEN s.liquidity_durability_score < 0.40 THEN 'LOW_LIQUIDITY_DURABILITY' END,
                CASE WHEN s.concentration_penalty >= 0.60 THEN 'HIGH_CONCENTRATION_RISK' END,
                CASE WHEN s.speculative_flow_penalty >= 0.50 THEN 'HIGH_SPECULATIVE_FLOW' END,
                CASE WHEN s.manipulation_penalty >= 0.60 THEN 'HIGH_MANIPULATION_RISK' END,
                CASE WHEN s.neutral_role_share >= 0.60 THEN 'STRONG_NEUTRAL_BASE' END,
                CASE WHEN s.whale_role_share >= 0.25 THEN 'WHALE_DEPENDENCY_PRESENT' END,
                CASE WHEN s.possible_farmer_count > 0 THEN 'FARMER_ACTIVITY_PRESENT' END
            ], NULL) AS flags
        FROM scored s
    ),

    upserted AS (
        INSERT INTO public.market_launch_intelligence_daily (
            market_id,
            day,
            launch_readiness_score,
            launch_risk_score,
            participation_quality_score,
            liquidity_durability_score,
            concentration_penalty,
            speculative_flow_penalty,
            manipulation_penalty,
            recommendation,
            recommendation_reason,
            flags,
            engine_version,
            updated_at
        )
        SELECT
            market_id,
            day,
            launch_readiness_score,
            launch_risk_score,
            participation_quality_score,
            liquidity_durability_score,
            concentration_penalty,
            speculative_flow_penalty,
            manipulation_penalty,
            recommendation,
            recommendation_reason,
            flags,
            %(engine_version)s,
            now()
        FROM final
        ON CONFLICT (market_id, day)
        DO UPDATE SET
            launch_readiness_score = EXCLUDED.launch_readiness_score,
            launch_risk_score = EXCLUDED.launch_risk_score,
            participation_quality_score = EXCLUDED.participation_quality_score,
            liquidity_durability_score = EXCLUDED.liquidity_durability_score,
            concentration_penalty = EXCLUDED.concentration_penalty,
            speculative_flow_penalty = EXCLUDED.speculative_flow_penalty,
            manipulation_penalty = EXCLUDED.manipulation_penalty,
            recommendation = EXCLUDED.recommendation,
            recommendation_reason = EXCLUDED.recommendation_reason,
            flags = EXCLUDED.flags,
            engine_version = EXCLUDED.engine_version,
            updated_at = now()
        RETURNING 1
    )

    SELECT COUNT(*)::int
    FROM upserted;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)

            cur.execute(resolve_day_sql, {"day": day})
            row = cur.fetchone()
            effective_day = row[0] if row else None

            if effective_day is None:
                conn.commit()
                return {
                    "engine_version": ENGINE_VERSION,
                    "day": None,
                    "limit_markets": limit_markets,
                    "rows_written": 0,
                    "status": "ok",
                }

            cur.execute(delete_sql, {"effective_day": effective_day})

            cur.execute(
                sql,
                {
                    "effective_day": effective_day,
                    "limit_markets": limit_markets,
                    "engine_version": ENGINE_VERSION,
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