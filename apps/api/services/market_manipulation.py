from datetime import date
from typing import Optional, Dict, Any

import psycopg

from apps.api.db import get_db_dsn

ENGINE_VERSION = "market_manipulation_v1_2026_03_08_C"


def compute_market_manipulation_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    resolve_day_sql = """
        SELECT COALESCE(%(day)s::date, MAX(day))::date
        FROM public.market_microstructure_daily;
    """

    delete_sql = """
        DELETE FROM public.market_manipulation_daily
        WHERE day = %(effective_day)s::date;
    """

    sql = """
    WITH params AS (
        SELECT %(effective_day)s::date AS day
    ),

    eligible_markets AS (
        SELECT market_id
        FROM public.markets
        WHERE protocol = 'polymarket'
        LIMIT %(limit_markets)s
    ),

    micro AS (
        SELECT
            mm.market_id,
            mm.day,
            COALESCE(mm.trades, 0) AS trades,
            COALESCE(mm.unique_traders, 0) AS unique_traders,
            COALESCE(mm.volume, 0) AS total_volume
        FROM public.market_microstructure_daily mm
        JOIN eligible_markets e
            ON e.market_id = mm.market_id
        JOIN params p
            ON p.day = mm.day
    ),

    features AS (
        SELECT
            mf.market_id,
            mf.day,
            COALESCE(mf.concentration_risk_score, 0) AS concentration_risk_score
        FROM public.market_microstructure_features_daily mf
        JOIN eligible_markets e
            ON e.market_id = mf.market_id
        JOIN params p
            ON p.day = mf.day
    ),

    trader_base AS (
        SELECT
            tb.market_id,
            tb.day,
            tb.trader_id,
            COALESCE(tb.volume, 0) AS volume,
            COALESCE(tb.trades, 0) AS trades,
            COALESCE(tb.buy_ratio, 0.5) AS buy_ratio,
            COALESCE(tb.avg_trade_size, 0) AS avg_trade_size,
            COALESCE(tb.market_volume_share, 0) AS market_volume_share
        FROM public.trader_behavior_daily tb
        JOIN eligible_markets e
            ON e.market_id = tb.market_id
        JOIN params p
            ON p.day = tb.day
    ),

    trader_ranked AS (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY t.market_id, t.day
                ORDER BY t.market_volume_share DESC, t.volume DESC, t.trader_id
            ) AS rn
        FROM trader_base t
    ),

    trader_agg AS (
        SELECT
            market_id,
            day,
            MAX(CASE WHEN rn = 1 THEN market_volume_share END) AS largest_trader_share,
            SUM(CASE WHEN rn <= 2 THEN market_volume_share ELSE 0 END) AS top2_trader_share,
            AVG(avg_trade_size) AS avg_trade_size,
            PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY avg_trade_size) AS median_trade_size
        FROM trader_ranked
        GROUP BY market_id, day
    ),

    market_side_stats AS (
        SELECT
            market_id,
            day,
            SUM(volume * buy_ratio) / NULLIF(SUM(volume), 0) AS buy_volume_share,
            SUM(volume * (1 - buy_ratio)) / NULLIF(SUM(volume), 0) AS sell_volume_share
        FROM trader_base
        GROUP BY market_id, day
    ),

    scored AS (
        SELECT
            m.market_id,
            m.day,
            m.trades,
            m.unique_traders,
            m.total_volume,

            COALESCE(ss.buy_volume_share, 0.5) AS buy_volume_share,
            COALESCE(ss.sell_volume_share, 0.5) AS sell_volume_share,

            COALESCE(ta.largest_trader_share, 0) AS largest_trader_share,
            COALESCE(ta.top2_trader_share, 0) AS top2_trader_share,

            COALESCE(ta.avg_trade_size, 0) AS avg_trade_size,
            COALESCE(ta.median_trade_size, 0) AS median_trade_size,

            COALESCE(f.concentration_risk_score, 0) AS concentration_risk_score,

            CASE
                WHEN m.trades = 0 OR m.unique_traders = 0 THEN 0.05
                WHEN m.trades <= 2 AND m.unique_traders <= 2 THEN 0.55
                WHEN COALESCE(ta.largest_trader_share, 0) >= 0.75 THEN 0.75
                WHEN COALESCE(ss.buy_volume_share, 0.5) >= 0.90 THEN 0.70
                WHEN COALESCE(ss.sell_volume_share, 0.5) >= 0.90 THEN 0.70
                WHEN COALESCE(ta.top2_trader_share, 0) >= 0.90 THEN 0.65
                WHEN COALESCE(f.concentration_risk_score, 0) >= 0.60 THEN 0.60
                ELSE 0.20
            END AS manipulation_score_raw,

            CASE
                WHEN m.trades = 0 OR m.unique_traders = 0 THEN 'inactive_market'
                WHEN m.trades <= 2 AND m.unique_traders <= 2 THEN 'thin_market_dislocation'
                WHEN COALESCE(ta.largest_trader_share, 0) >= 0.75 THEN 'concentration_spike'
                WHEN COALESCE(ss.buy_volume_share, 0.5) >= 0.90 THEN 'one_sided_price_push'
                WHEN COALESCE(ss.sell_volume_share, 0.5) >= 0.90 THEN 'one_sided_price_push'
                WHEN COALESCE(ta.top2_trader_share, 0) >= 0.90 THEN 'wash_like_flow'
                WHEN COALESCE(f.concentration_risk_score, 0) >= 0.60 THEN 'concentration_spike'
                ELSE 'none'
            END AS primary_signal

        FROM micro m
        LEFT JOIN features f
            ON f.market_id = m.market_id
           AND f.day = m.day
        LEFT JOIN trader_agg ta
            ON ta.market_id = m.market_id
           AND ta.day = m.day
        LEFT JOIN market_side_stats ss
            ON ss.market_id = m.market_id
           AND ss.day = m.day
    ),

    classified AS (
        SELECT
            s.*,
            ROUND(s.manipulation_score_raw::numeric, 8) AS manipulation_score,

            CASE
                WHEN s.manipulation_score_raw >= 0.80 THEN 'critical'
                WHEN s.manipulation_score_raw >= 0.65 THEN 'high'
                WHEN s.manipulation_score_raw >= 0.35 THEN 'medium'
                ELSE 'low'
            END AS risk_tier,

            ARRAY_REMOVE(ARRAY[
                CASE WHEN s.primary_signal = 'wash_like_flow' THEN 'wash_like_flow' END,
                CASE WHEN s.primary_signal = 'one_sided_price_push' THEN 'one_sided_price_push' END,
                CASE WHEN s.primary_signal = 'concentration_spike' THEN 'concentration_spike' END,
                CASE WHEN s.primary_signal = 'thin_market_dislocation' THEN 'thin_market_dislocation' END
            ], NULL) AS signal_labels,
        
            (
                s.manipulation_score_raw >= 0.65
                AND s.primary_signal <> 'inactive_market'
            ) AS needs_operator_review
        
        FROM scored s
    ),

    upserted AS (
        INSERT INTO public.market_manipulation_daily (
            market_id,
            day,
            manipulation_score,
            risk_tier,
            primary_signal,
            signal_labels,
            needs_operator_review,
            trades,
            unique_traders,
            buy_volume_share,
            sell_volume_share,
            largest_trader_share,
            top2_trader_share,
            avg_trade_size,
            median_trade_size
        )
        SELECT
            market_id,
            day,
            manipulation_score,
            risk_tier,
            primary_signal,
            signal_labels,
            needs_operator_review,
            trades,
            unique_traders,
            buy_volume_share,
            sell_volume_share,
            largest_trader_share,
            top2_trader_share,
            avg_trade_size,
            median_trade_size
        FROM classified
        ON CONFLICT (market_id, day)
        DO UPDATE SET
            manipulation_score = EXCLUDED.manipulation_score,
            risk_tier = EXCLUDED.risk_tier,
            primary_signal = EXCLUDED.primary_signal,
            signal_labels = EXCLUDED.signal_labels,
            needs_operator_review = EXCLUDED.needs_operator_review,
            trades = EXCLUDED.trades,
            unique_traders = EXCLUDED.unique_traders,
            buy_volume_share = EXCLUDED.buy_volume_share,
            sell_volume_share = EXCLUDED.sell_volume_share,
            largest_trader_share = EXCLUDED.largest_trader_share,
            top2_trader_share = EXCLUDED.top2_trader_share,
            avg_trade_size = EXCLUDED.avg_trade_size,
            median_trade_size = EXCLUDED.median_trade_size
        RETURNING 1
    )

    SELECT COUNT(*)::int
    FROM upserted;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(resolve_day_sql, {"day": day})
            row = cur.fetchone()
            effective_day = row[0] if row else None

            if effective_day is None:
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