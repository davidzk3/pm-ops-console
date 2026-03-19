from datetime import date
from typing import Dict, Any, Optional

import psycopg

from apps.api.db import get_db_dsn
from apps.api.ops.universe import compute_market_universe_daily
from apps.api.services.microstructure import compute_microstructure_daily
from apps.api.services.microstructure_features import compute_microstructure_features_daily
from apps.api.services.trader_behavior import compute_trader_behavior_daily
from apps.api.services.trader_role import compute_trader_role_daily
from apps.api.services.market_regime import compute_market_regime_daily
from apps.api.services.market_regime_v2 import compute_market_regime_daily_v2
from apps.api.services.market_risk_radar import compute_market_risk_radar_daily
from apps.api.services.market_manipulation import compute_market_manipulation_daily
from apps.api.services.market_integrity import compute_market_integrity_daily
from apps.api.ops.resolution import (
    compute_market_resolution_raw_daily,
    compute_market_resolution_features_daily,
    compute_market_resolution_scores_daily,
)

PIPELINE_BUILD = "ops_pipeline_v6_2026_03_08"

def _resolve_pipeline_day(day: Optional[date]) -> Optional[date]:
    if day is not None:
        return day

    sql = """
        select max(day)::date
        from public.market_microstructure_daily;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()

    return row[0] if row and row[0] is not None else None


def run_ops_pipeline(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:

    effective_day = _resolve_pipeline_day(day)

    results = {
        "pipeline": PIPELINE_BUILD,
        "requested_day": str(day) if day else None,
        "effective_day": str(effective_day) if effective_day else None,
        "window_hours": window_hours,
    }

    if effective_day is None:
        return {
            "status": "ok",
            "results": {
                **results,
                "note": "No available microstructure day found yet, pipeline not executed.",
            },
        }

    universe = compute_market_universe_daily(
        day=effective_day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )
    results["universe"] = universe

    micro = compute_microstructure_daily(
        day=effective_day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )
    results["microstructure"] = micro

    features = compute_microstructure_features_daily(
        day=effective_day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )
    results["features"] = features

    trader_behavior = compute_trader_behavior_daily(
        day=effective_day,
        limit_markets=limit_markets,
    )
    results["trader_behavior"] = trader_behavior

    trader_role = compute_trader_role_daily(
        day=effective_day,
        limit_markets=limit_markets,
    )
    results["trader_role"] = trader_role

    market_regime = compute_market_regime_daily(
        day=effective_day,
        limit_markets=limit_markets,
    )
    results["market_regime"] = market_regime

    market_regime_v2 = compute_market_regime_daily_v2(
        day=effective_day,
        limit_markets=limit_markets,
    )
    results["market_regime_v2"] = market_regime_v2

    market_risk_radar = compute_market_risk_radar_daily(
        day=effective_day,
        limit_markets=limit_markets,
    )
    results["market_risk_radar"] = market_risk_radar

    manipulation = compute_market_manipulation_daily(
    day=effective_day,
    limit_markets=limit_markets,
    )
    results["market_manipulation"] = manipulation

    market_integrity = compute_market_integrity_daily(
        day=effective_day,
        limit_markets=limit_markets,
    )
    results["market_integrity"] = market_integrity

    resolution_raw = compute_market_resolution_raw_daily(
        day=effective_day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )
    results["resolution_raw"] = resolution_raw

    resolution_features = compute_market_resolution_features_daily(
        day=effective_day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )
    results["resolution_features"] = resolution_features

    resolution_scores = compute_market_resolution_scores_daily(
        day=effective_day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )
    results["resolution_scores"] = resolution_scores

    return {
        "status": "ok",
        "results": results,
    }