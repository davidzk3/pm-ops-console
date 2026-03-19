from __future__ import annotations

from typing import List, Dict, Any
import psycopg
from fastapi import APIRouter, Query

from apps.api.db import get_db_dsn


router = APIRouter(prefix="/ops/markets", tags=["ops"])


def fetch_integrity_history(
    market_id: str,
    limit_days: int = 60,
) -> List[Dict[str, Any]]:
    """
    Integrity time series for a single market.
    """

    sql = """
    SELECT
        day,
        market_id,

        integrity_score,
        integrity_band,

        radar_risk_score,
        manipulation_score,

        regime,
        regime_reason,

        whale_role_share,
        speculator_role_share,
        neutral_role_share,

        trades,
        unique_traders

    FROM market_integrity_score_daily
    WHERE market_id = %(market_id)s
    ORDER BY day ASC
    LIMIT %(limit_days)s
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                sql,
                {
                    "market_id": market_id,
                    "limit_days": limit_days,
                },
            )
            rows = cur.fetchall()

    return rows


@router.get("/integrity/history")
def integrity_history(
    market_id: str = Query(...),
    limit_days: int = Query(60),
):
    """
    Return structural integrity trend for a market.
    """

    rows = fetch_integrity_history(market_id, limit_days)

    return {
        "market_id": market_id,
        "points": rows,
        "count": len(rows),
    }