from datetime import date
from fastapi import FastAPI, HTTPException, Depends, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg.types.json import Json
from fastapi.encoders import jsonable_encoder

import os
import time
import json
import psycopg
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from apps.api.db import get_db_dsn

from apps.api.ingest.polymarket_trades_rest import ingest_polymarket_trades_rest_for_market
from apps.api.ingest.runner import ingest_polymarket_markets
from apps.api.ingest.runner import ingest_polymarket_trades_ws
from apps.api.ingest.runner import ingest_polymarket_trades_rest_job
from apps.api.ingest.runner import ingest_polymarket_bbo_ws_for_market
from apps.api.ingest.runner import ingest_polymarket_metrics_daily
from apps.api.ops.microstructure import compute_microstructure_daily
from apps.api.ops.traders import compute_trader_daily_stats, compute_trader_labels_daily
from apps.api.services.trader_behavior import compute_trader_behavior_daily
from apps.api.services.trader_role import compute_trader_role_daily
from apps.api.ingest.runner import ingest_polymarket_trades_rest_for_market_job
from apps.api.services.market_risk_radar import compute_market_risk_radar_daily
from apps.api.services.market_integrity import compute_market_integrity_daily
from apps.api.services.market_regime_v2 import compute_market_regime_daily_v2
from apps.api.services.market_manipulation import compute_market_manipulation_daily
from apps.api.services.market_launch_intelligence import compute_market_launch_intelligence_daily
from apps.api.services.market_social_intelligence import compute_market_social_intelligence_daily
from apps.api.ops.integrity_history import router as integrity_history_router
from apps.api.web.microstructure import router as microstructure_router
from .settings import CORS_ORIGINS
from .auth import require_operator, AuthUser

from fastapi.responses import ORJSONResponse
app = FastAPI(default_response_class=ORJSONResponse)

app.include_router(microstructure_router)
app.include_router(integrity_history_router)

# -----------------------------
# CORS (only add ONCE)
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Request logging middleware
# -----------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = (time.time() - start) * 1000
    print(f"{request.method} {request.url.path} -> {response.status_code} {duration:.1f}ms")
    return response

# -----------------------------
# Consistent error helpers
# -----------------------------
def error_response(code: str, message: str, status_code: int = 400, details=None):
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "details": details or {}}},
    )

# -----------------------------
# Global exception handlers
# -----------------------------
@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict):
        code = detail.get("code", "http_error")
        message = detail.get("message", "Request failed")
        details = detail.get("details", {})
    else:
        code = "http_error"
        message = str(detail)
        details = {}
    return error_response(code=code, message=message, status_code=exc.status_code, details=details)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError):
    return error_response(
        code="validation_error",
        message="Invalid request payload",
        status_code=422,
        details={"errors": exc.errors()},
    )

import traceback

@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    traceback.print_exc()
    return error_response(
        code="internal_error",
        message="Internal server error",
        status_code=500,
        details={"type": type(exc).__name__, "message": str(exc)},
    )

# -----------------------------
# DB helpers
# -----------------------------
def rows_as_dicts(cur):
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def market_exists(market_id: str) -> bool:
    q = """
    SELECT 1
    FROM (
        SELECT market_id FROM core.markets WHERE market_id = %s
        UNION
        SELECT market_id FROM marts.market_day WHERE market_id = %s
        UNION
        SELECT market_id FROM public.market_integrity_score_daily WHERE market_id = %s
    ) x
    LIMIT 1;
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, market_id))
            return cur.fetchone() is not None

def _ensure_metrics_row(cur, market_id: str, day):
    """
    Ensure marts.market_day has (market_id, day).
    If missing, clone the latest day.
    """
    cur.execute(
        """
        INSERT INTO marts.market_day (
            market_id, day, volume, trades, unique_traders,
            spread_median, depth_2pct_median, concentration_hhi,
            health_score, risk_score, flags
        )
        SELECT
            market_id,
            %s AS day,
            volume, trades, unique_traders,
            spread_median, depth_2pct_median, concentration_hhi,
            health_score, risk_score,
            COALESCE(flags, '[]'::jsonb) AS flags
        FROM marts.market_day
        WHERE market_id = %s
        ORDER BY day DESC
        LIMIT 1
        ON CONFLICT (market_id, day) DO NOTHING
        """,
        (day, market_id),
    )

def _parse_params(params_any):
    if not params_any:
        return {}
    if isinstance(params_any, dict):
        return params_any
    if isinstance(params_any, str):
        try:
            v = json.loads(params_any)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}

def _apply_action(cur, action_code: str, market_id: str, day, params: dict, direction: int):
    """
    direction: +1 = apply, -1 = revert
    Only LIQUIDITY_BOOST implemented for now.
    """
    if action_code == "LIQUIDITY_BOOST":
        spread_bps = float(params.get("spread_bps", 10))
        depth_delta = float(params.get("depth_delta", 500))
        health_delta = float(params.get("health_delta", 3))
        risk_delta = float(params.get("risk_delta", -2))

        spread_delta = -(spread_bps / 10000.0)

        cur.execute(
            """
            UPDATE marts.market_day
            SET
                spread_median = GREATEST(0, COALESCE(spread_median, 0) + (%s * %s)),
                depth_2pct_median = GREATEST(0, COALESCE(depth_2pct_median, 0) + (%s * %s)),
                health_score = LEAST(100, GREATEST(0, COALESCE(health_score, 0) + (%s * %s))),
                risk_score = LEAST(100, GREATEST(0, COALESCE(risk_score, 0) + (%s * %s)))
            WHERE market_id = %s AND day = %s
            """,
            (
                spread_delta,
                direction,
                depth_delta,
                direction,
                health_delta,
                direction,
                risk_delta,
                direction,
                market_id,
                day,
            ),
        )

def build_coverage_summary(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    timeline = snapshot.get("timeline") or []
    incidents = snapshot.get("incidents") or []
    interventions = snapshot.get("interventions") or []
    overrides = snapshot.get("overrides") or []

    traders = snapshot.get("traders") or {}
    same_day = traders.get("same_day") or {}
    rolling_window = traders.get("rolling_window") or {}

    same_day_summary = same_day.get("summary") or []
    same_day_cohort_summary = same_day.get("cohorts_summary") or []
    same_day_trader_intelligence = same_day.get("intelligence") or []

    rolling_summary = rolling_window.get("summary") or []
    rolling_cohort_summary = rolling_window.get("cohorts_summary") or []
    rolling_trader_intelligence = rolling_window.get("intelligence") or []

    market = snapshot.get("market") or {}
    errors = snapshot.get("errors") or []

    has_timeline = len(timeline) > 0
    has_integrity_history = has_timeline
    has_impact = not any(e.get("key") == "impact" for e in errors)

    has_same_day_trader_summary = len(same_day_summary) > 0
    has_same_day_cohort_summary = len(same_day_cohort_summary) > 0
    has_same_day_trader_intelligence = len(same_day_trader_intelligence) > 0

    has_rolling_trader_summary = len(rolling_summary) > 0
    has_rolling_cohort_summary = len(rolling_cohort_summary) > 0
    has_rolling_trader_intelligence = len(rolling_trader_intelligence) > 0

    has_trader_summary = has_same_day_trader_summary or has_rolling_trader_summary
    has_cohort_summary = has_same_day_cohort_summary or has_rolling_cohort_summary
    has_trader_intelligence = has_same_day_trader_intelligence or has_rolling_trader_intelligence

    has_incidents = len(incidents) > 0
    has_interventions = len(interventions) > 0
    has_overrides = len(overrides) > 0

    downstream_flags = [
        bool(market.get("has_regime_data")),
        bool(market.get("has_radar_data")),
        bool(market.get("has_manipulation_data")),
    ]
    downstream_coverage_count = sum(1 for x in downstream_flags if x)

    if (
        has_timeline
        and has_impact
        and has_trader_summary
        and has_cohort_summary
        and has_trader_intelligence
    ):
        coverage_level = "full"
    elif (
        downstream_coverage_count >= 1
        or has_timeline
        or has_trader_summary
        or has_cohort_summary
        or has_trader_intelligence
    ):
        coverage_level = "partial"
    else:
        coverage_level = "sparse"

    if coverage_level == "full":
        coverage_reason = "Full downstream coverage available for this market."
    elif market.get("is_partial_coverage") is True:
        coverage_reason = "Live market with partial downstream coverage."
    elif not has_timeline and not has_trader_summary and not has_impact:
        coverage_reason = "Market exists, but downstream analytics are still sparse."
    else:
        coverage_reason = "Some downstream analytics are available, but coverage is incomplete."

    return {
        "has_timeline": has_timeline,
        "has_integrity_history": has_integrity_history,
        "has_impact": has_impact,

        "has_trader_summary": has_trader_summary,
        "has_cohort_summary": has_cohort_summary,
        "has_trader_intelligence": has_trader_intelligence,

        "has_same_day_trader_summary": has_same_day_trader_summary,
        "has_same_day_cohort_summary": has_same_day_cohort_summary,
        "has_same_day_trader_intelligence": has_same_day_trader_intelligence,

        "has_rolling_trader_summary": has_rolling_trader_summary,
        "has_rolling_cohort_summary": has_rolling_cohort_summary,
        "has_rolling_trader_intelligence": has_rolling_trader_intelligence,

        "has_incidents": has_incidents,
        "has_interventions": has_interventions,
        "has_overrides": has_overrides,
        "coverage_level": coverage_level,
        "coverage_reason": coverage_reason,
        "downstream_coverage_count": downstream_coverage_count,
    }

# -----------------------------
# Health
# -----------------------------
@app.get("/health")
def health():
    try:
        with psycopg.connect(get_db_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        return {"ok": True, "db": "ok"}
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={"code": "db_unreachable", "message": "Database unreachable", "details": {}},
        )

# -----------------------------
# Ops Inbox (latest per market)
# -----------------------------
@app.get("/ops/inbox")
def ops_inbox():
    q = """
WITH latest_md AS (
  SELECT DISTINCT ON (md.market_id)
    md.market_id,
    md.day,
    md.volume,
    md.trades,
    md.unique_traders,
    md.spread_median,
    md.depth_2pct_median,
    md.concentration_hhi,
    md.health_score,
    md.risk_score,
    md.flags
  FROM marts.market_day md
  ORDER BY md.market_id, md.day DESC
),
base AS (
  SELECT
    m.market_id,
    m.protocol,
    m.chain,
    m.title,
    m.category,
    l.day,
    l.volume,
    l.trades,
    l.unique_traders,
    l.spread_median,
    l.depth_2pct_median,
    l.concentration_hhi,
    COALESCE(mo.health_score_override, l.health_score) AS health_score,
    COALESCE(mo.risk_score_override,  l.risk_score)   AS risk_score,
    (mo.id IS NOT NULL) AS has_manual_override,
    COALESCE(l.flags, '[]'::jsonb) AS flags
  FROM core.markets m
  JOIN latest_md l
    ON l.market_id = m.market_id
  LEFT JOIN market_manual_overrides mo
    ON mo.market_id = l.market_id AND mo.day = l.day
  WHERE m.is_active = true
)
SELECT
  b.market_id,
  b.protocol,
  b.chain,
  b.title,
  b.category,
  b.day,
  b.volume,
  b.trades,
  b.unique_traders,
  b.spread_median,
  b.depth_2pct_median,
  b.concentration_hhi,
  b.health_score,
  b.risk_score,
  b.has_manual_override,
  b.flags
FROM base b
ORDER BY b.risk_score DESC NULLS LAST, b.volume DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            return rows_as_dicts(cur)

# -----------------------------
# Trader Pillar: Summary + Cohorts + Intelligence
# -----------------------------
class TraderSummaryRow(BaseModel):
    trader_id: str
    days_active: int
    trades: int
    notional_total: float
    notional_buy: float
    notional_sell: float
    avg_trade_size: float
    first_ts: Optional[str] = None
    last_ts: Optional[str] = None

@app.get("/ops/markets/{market_id}/traders/summary", response_model=List[TraderSummaryRow])
def market_traders_summary(market_id: str, days: int = 30, top_n: int = 10):
    days = max(1, min(int(days), 365))
    top_n = max(1, min(int(top_n), 200))

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM public.trader_behavior_daily
  WHERE market_id = %s
),
w AS (
  SELECT *
  FROM public.trader_behavior_daily
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
)
SELECT
  trader_id,
  COUNT(DISTINCT day)::int AS days_active,
  COALESCE(SUM(trades), 0)::int AS trades,
  COALESCE(SUM(volume), 0)::float AS notional_total,
  COALESCE(SUM(volume * buy_ratio), 0)::float AS notional_buy,
  COALESCE(SUM(volume * (1 - buy_ratio)), 0)::float AS notional_sell,
  (COALESCE(SUM(volume), 0) / NULLIF(COALESCE(SUM(trades), 0), 0))::float AS avg_trade_size,
  MIN(first_trade_ts)::text AS first_ts,
  MAX(last_trade_ts)::text AS last_ts
FROM w
GROUP BY trader_id
ORDER BY notional_total DESC, trades DESC
LIMIT %s;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days, top_n))
            return rows_as_dicts(cur)

class CohortSummaryRow(BaseModel):
    cohort: str
    traders: int
    trades: int
    notional_total: float
    avg_trade_size: float
    days_covered: int

@app.get("/ops/markets/{market_id}/traders/cohorts/summary", response_model=List[CohortSummaryRow])
def market_trader_cohorts_summary(market_id: str, days: int = 1):
    days = max(1, min(int(days), 365))

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "market_not_found",
                "message": "Market not found",
                "details": {"market_id": market_id},
            },
        )

    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM public.trader_role_daily
  WHERE market_id = %s
),
w AS (
  SELECT
    r.market_id,
    r.day,
    r.trader_id,
    COALESCE(r.trades, 0) AS trades,
    COALESCE(r.volume, 0) AS volume,
    COALESCE(r.avg_trade_size, 0) AS avg_trade_size,
    COALESCE(r.buy_ratio, 0.5) AS buy_ratio,
    COALESCE(r.is_large_participant, false) AS is_large_participant,
    COALESCE(r.is_one_sided, false) AS is_one_sided,
    COALESCE(r.is_high_frequency, false) AS is_high_frequency
  FROM public.trader_role_daily r
  WHERE r.market_id = %s
    AND r.day >= (SELECT latest_day FROM latest) - %s
),
normalized AS (
  SELECT
    CASE
      WHEN is_high_frequency AND COALESCE(avg_trade_size, 0) <= 2 THEN 'POSSIBLE_FARMER'
      WHEN is_large_participant THEN 'WHALE'
      WHEN (
        is_one_sided AND COALESCE(trades, 0) >= 2
      ) OR (
        ABS(COALESCE(buy_ratio, 0.5) - 0.5) >= 0.25 AND COALESCE(trades, 0) >= 2
      ) THEN 'SPECULATOR'
      ELSE 'NEUTRAL'
    END AS cohort,
    trader_id,
    day,
    trades,
    volume,
    avg_trade_size
  FROM w
),
agg AS (
  SELECT
    cohort,
    COUNT(DISTINCT trader_id)::int AS traders,
    COALESCE(SUM(trades), 0)::int AS trades,
    COALESCE(SUM(volume), 0)::float AS notional_total,
    (
      COALESCE(SUM(volume), 0) / NULLIF(COALESCE(SUM(trades), 0), 0)
    )::float AS avg_trade_size,
    COUNT(DISTINCT day)::int AS days_covered
  FROM normalized
  GROUP BY cohort
)
SELECT
  cohort,
  traders,
  trades,
  notional_total,
  avg_trade_size,
  days_covered
FROM agg
ORDER BY notional_total DESC NULLS LAST, traders DESC, cohort;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)

class TraderIntelligenceRow(BaseModel):
    trader_id: str
    days_active: int
    trades: int
    notional_total: float
    avg_trade_size: float
    buy_ratio: float
    cohort: str
    operator_tag: str
    flags: Dict[str, Any] = {}


@app.get("/ops/markets/{market_id}/traders/intelligence", response_model=List[TraderIntelligenceRow])
def market_trader_intelligence(market_id: str, days: int = 1, top_n: int = 50):
    days = max(1, min(int(days), 365))
    top_n = max(1, min(int(top_n), 500))

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM public.trader_behavior_daily
  WHERE market_id = %s
),
behavior AS (
  SELECT
    b.trader_id,
    COUNT(DISTINCT b.day)::int AS days_active,
    COALESCE(SUM(b.trades), 0)::int AS trades,
    COALESCE(SUM(b.volume), 0)::float AS notional_total,
    (COALESCE(SUM(b.volume), 0) / NULLIF(COALESCE(SUM(b.trades), 0), 0))::float AS avg_trade_size,
    CASE
      WHEN COALESCE(SUM(b.trades), 0) > 0 AND COALESCE(SUM(b.volume), 0) > 0
        THEN COALESCE(SUM(b.volume * b.buy_ratio), 0)::float / COALESCE(SUM(b.volume), 0)::float
      ELSE 0.5
    END AS buy_ratio,
    BOOL_OR(COALESCE(b.is_large_participant, false)) AS is_large_participant,
    BOOL_OR(COALESCE(b.is_one_sided, false)) AS is_one_sided,
    BOOL_OR(COALESCE(b.is_high_frequency, false)) AS is_high_frequency,
    MAX(COALESCE(b.active_minutes, 0))::int AS active_minutes
  FROM public.trader_behavior_daily b
  WHERE b.market_id = %s
    AND b.day >= (SELECT latest_day FROM latest) - %s
  GROUP BY b.trader_id
),
ranked AS (
  SELECT
    b.trader_id,
    b.days_active,
    b.trades,
    b.notional_total,
    b.avg_trade_size,
    b.buy_ratio,
    b.is_large_participant,
    b.is_one_sided,
    b.is_high_frequency,
    b.active_minutes
  FROM behavior b
  ORDER BY b.notional_total DESC, b.trades DESC
  LIMIT %s
)
SELECT *
FROM ranked;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days, top_n))
            rows = rows_as_dicts(cur)

    out = []
    for r in rows:
        trades = int(r.get("trades") or 0)
        days_active = int(r.get("days_active") or 0)
        avg_size = float(r.get("avg_trade_size") or 0.0)
        buy_ratio = float(r.get("buy_ratio") or 0.5)

        imbalance = abs(buy_ratio - 0.5)
        is_large = bool(r.get("is_large_participant"))
        is_one_sided = bool(r.get("is_one_sided"))
        is_high_freq = bool(r.get("is_high_frequency"))
        active_minutes = int(r.get("active_minutes") or 0)

        if is_high_freq and avg_size <= 2.0:
            cohort = "POSSIBLE_FARMER"
        elif is_large:
            cohort = "WHALE"
        elif (is_one_sided and trades >= 2) or (imbalance >= 0.25 and trades >= 2):
            cohort = "SPECULATOR"
        else:
            cohort = "NEUTRAL"

        operator_tag = "RETAIL"
        flags: Dict[str, Any] = {
            "balanced_flow": imbalance <= 0.12,
            "imbalance": round(imbalance, 4),
            "confidence": 0.0,
            "supporting_flags": [],
            "is_large_participant": is_large,
            "is_one_sided": is_one_sided,
            "is_high_frequency": is_high_freq,
            "active_minutes": active_minutes,
            "large_avg_trade_size": avg_size >= 25.0,
        }

        if is_high_freq and avg_size <= 2.0:
            operator_tag = "INCENTIVE_FARMER"
            flags["reason"] = "high frequency small size flow"
        elif is_large:
            operator_tag = "WHALE"
            flags["reason"] = "large participation profile"
        elif days_active >= 2 and imbalance <= 0.12:
            operator_tag = "MAKER_LIKE"
            flags["reason"] = "balanced participation across time"
        elif (is_one_sided and trades >= 2) or (imbalance >= 0.25 and trades >= 2):
            operator_tag = "DIRECTIONAL"
            flags["reason"] = "repeated one sided speculative flow"
        elif trades >= 60:
            operator_tag = "ACTIVE_RETAIL"
            flags["reason"] = "high activity without stronger structural role"
        else:
            operator_tag = "RETAIL"
            flags["reason"] = "general market participation"

        out.append(
            {
                "trader_id": r.get("trader_id"),
                "days_active": days_active,
                "trades": trades,
                "notional_total": float(r.get("notional_total") or 0.0),
                "avg_trade_size": avg_size,
                "buy_ratio": buy_ratio,
                "cohort": cohort,
                "operator_tag": operator_tag,
                "flags": flags,
            }
        )

    return out

# -----------------------------
# Market Quality and Cohort Impact (Operational View)
# -----------------------------
from datetime import timedelta, date
from decimal import Decimal

class CohortShareRow(BaseModel):
    cohort: str
    notional_share: float
    trade_share: float

class CohortShareDeltaRow(BaseModel):
    cohort: str
    notional_share_delta: float
    trade_share_delta: float

class MarketImpactResponse(BaseModel):
    window_days: int
    recent_window: Dict[str, str]
    prior_window: Dict[str, str]
    market_quality_delta: Dict[str, float]

    recent_cohort_share: List[CohortShareRow]
    prior_cohort_share: List[CohortShareRow]
    cohort_share_delta: List[CohortShareDeltaRow]

    diagnosis: str
    market_regime: str
    cohort_risk_flags: List[str]

def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return 0.0

def _compute_cohort_share(cur, market_id: str, start_day, end_day) -> List[Dict[str, float]]:
    cur.execute(
        """
        WITH behavior_window AS (
            SELECT
                b.trader_id,
                b.day,
                COALESCE(b.trades, 0)::int AS trades,
                COALESCE(b.volume, 0)::double precision AS volume,
                COALESCE(b.avg_trade_size, 0)::double precision AS avg_trade_size,
                COALESCE(b.buy_ratio, 0.5)::double precision AS buy_ratio,
                COALESCE(b.is_large_participant, false) AS is_large_participant,
                COALESCE(b.is_one_sided, false) AS is_one_sided,
                COALESCE(b.is_high_frequency, false) AS is_high_frequency
            FROM public.trader_behavior_daily b
            WHERE b.market_id = %s
              AND b.day BETWEEN %s AND %s
        ),
        normalized AS (
            SELECT
                CASE
                    WHEN is_high_frequency AND avg_trade_size <= 2 THEN 'POSSIBLE_FARMER'
                    WHEN is_large_participant THEN 'WHALE'
                    WHEN (is_one_sided AND trades >= 2)
                      OR (ABS(buy_ratio - 0.5) >= 0.25 AND trades >= 2)
                      THEN 'SPECULATOR'
                    ELSE 'NEUTRAL'
                END AS cohort,
                volume,
                trades
            FROM behavior_window
        )
        SELECT
            cohort,
            COALESCE(SUM(volume), 0) AS notional_total,
            COALESCE(SUM(trades), 0) AS trades
        FROM normalized
        GROUP BY 1
        """,
        (market_id, start_day, end_day),
    )

    cohort_rows = cur.fetchall()

    total_notional = sum(_to_float(r[1]) for r in cohort_rows)
    total_trades = sum(_to_float(r[2]) for r in cohort_rows)

    if total_notional <= 0:
        total_notional = 1.0
    if total_trades <= 0:
        total_trades = 1.0

    out: List[Dict[str, float]] = []
    for r in cohort_rows:
        out.append(
            {
                "cohort": str(r[0] or "UNKNOWN").upper(),
                "notional_share": _to_float(r[1]) / total_notional,
                "trade_share": _to_float(r[2]) / total_trades,
            }
        )

    out.sort(key=lambda x: (-x["notional_share"], -x["trade_share"], x["cohort"]))
    return out

def _compute_cohort_share_delta(
    recent: List[Dict[str, float]],
    prior: List[Dict[str, float]],
) -> List[Dict[str, float]]:
    rmap = {c["cohort"].upper(): c for c in recent}
    pmap = {c["cohort"].upper(): c for c in prior}

    cohorts = sorted(set(rmap.keys()) | set(pmap.keys()))
    out: List[Dict[str, float]] = []
    for k in cohorts:
        r = rmap.get(k, {})
        p = pmap.get(k, {})
        out.append(
            {
                "cohort": k,
                "notional_share_delta": _to_float(r.get("notional_share")) - _to_float(p.get("notional_share")),
                "trade_share_delta": _to_float(r.get("trade_share")) - _to_float(p.get("trade_share")),
            }
        )

    out.sort(key=lambda x: (-abs(x["notional_share_delta"]), -abs(x["trade_share_delta"]), x["cohort"]))
    return out

def _normalize_trader_role(role: Optional[str]) -> str:
    r = (role or "").strip().lower()
    if r == "whale":
        return "WHALE"
    if r in ("one_sided_speculator", "high_frequency_trader"):
        return "SPECULATOR"
    if r == "possible_farmer":
        return "POSSIBLE_FARMER"
    return "NEUTRAL"

def _compute_cohort_risk_flags(
    recent_cohort_share: List[Dict[str, float]],
    cohort_share_delta: List[Dict[str, float]] | None = None,
) -> List[str]:
    flags: List[str] = []

    recent_map = {c["cohort"].upper(): c for c in (recent_cohort_share or [])}
    delta_map = {c["cohort"].upper(): c for c in (cohort_share_delta or [])}

    whale_notional = _to_float(recent_map.get("WHALE", {}).get("notional_share"))
    spec_trade = _to_float(recent_map.get("SPECULATOR", {}).get("trade_share"))
    neutral_trade = _to_float(recent_map.get("NEUTRAL", {}).get("trade_share"))
    farmer_trade = _to_float(recent_map.get("POSSIBLE_FARMER", {}).get("trade_share"))

    neutral_notional_delta = _to_float(delta_map.get("NEUTRAL", {}).get("notional_share_delta"))
    neutral_trade_delta = _to_float(delta_map.get("NEUTRAL", {}).get("trade_share_delta"))

    if whale_notional >= 0.35:
        flags.append("WHALE_DOMINANCE_RISK")

    if spec_trade >= 0.60:
        flags.append("SPECULATIVE_FLOW_DOMINANCE")

    if farmer_trade >= 0.20:
        flags.append("INCENTIVE_DISTORTION_RISK")

    if neutral_trade < 0.12 and whale_notional > 0.25:
        flags.append("THIN_ORGANIC_LAYER")

    if neutral_notional_delta <= -0.03 or neutral_trade_delta <= -0.015:
        flags.append("NEUTRAL_PARTICIPATION_EROSION")

    if whale_notional < 0.10 and neutral_trade < 0.20:
        flags.append("NO_DEEP_ORGANIC_LIQUIDITY")

    if not flags:
        flags.append("COHORT_STRUCTURE_STABLE")

    return flags


def _compute_market_regime(delta: Dict[str, float]) -> str:
    spread = _to_float(delta.get("spread_median_delta"))
    depth = _to_float(delta.get("depth_2pct_delta"))
    hhi = _to_float(delta.get("concentration_hhi_delta"))
    traders = _to_float(delta.get("unique_traders_delta"))
    health = _to_float(delta.get("health_score_delta"))

    exec_worse = (spread > 0) or (hhi > 0)
    exec_better = (spread < 0) and (hhi < 0)

    participation_down = (traders < 0) or (health < 0)
    participation_up = (traders > 0) and (health > 0)

    depth_up = depth > 0
    depth_down = depth < 0

    if exec_worse and participation_down:
        return "MICROSTRUCTURE_STRESS"

    if exec_worse:
        return "EXECUTION_DEGRADING"

    if depth_down:
        return "LIQUIDITY_THINNING"

    if participation_down:
        return "PARTICIPATION_DECAY"

    if exec_better and depth_up and not participation_down:
        return "STRUCTURALLY_HEALTHY"

    if exec_better and depth_up:
        return "LIQUIDITY_EXPANSION"

    if participation_up:
        return "PARTICIPATION_GROWTH"

    return "STABLE"

@app.get("/ops/markets/{market_id}/traders/impact", response_model=MarketImpactResponse)
def market_trader_impact(
    market_id: str,
    days: int = Query(14, ge=4, le=60),
    anchor_day: Optional[date] = Query(None),
):
    days = max(4, min(int(days), 60))

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "market_not_found",
                "message": "Market not found",
                "details": {"market_id": market_id},
            },
        )

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(day)
                FROM marts.market_day
                WHERE market_id = %s
                """,
                (market_id,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "no_metrics",
                        "message": "No metrics available for market",
                        "details": {"market_id": market_id},
                    },
                )

            db_max_day = row[0]

            if anchor_day is not None:
                max_day = anchor_day if anchor_day <= db_max_day else db_max_day
            else:
                max_day = db_max_day

            half = days // 2
            recent_start = max_day - timedelta(days=half - 1)
            prior_end = recent_start - timedelta(days=1)
            prior_start = prior_end - timedelta(days=half - 1)

            cur.execute(
                """
                SELECT
                    AVG(spread_median),
                    AVG(depth_2pct_median),
                    AVG(concentration_hhi),
                    AVG(unique_traders),
                    AVG(health_score)
                FROM marts.market_day
                WHERE market_id = %s
                  AND day BETWEEN %s AND %s
                """,
                (market_id, recent_start, max_day),
            )
            recent = cur.fetchone() or (None, None, None, None, None)

            cur.execute(
                """
                SELECT
                    AVG(spread_median),
                    AVG(depth_2pct_median),
                    AVG(concentration_hhi),
                    AVG(unique_traders),
                    AVG(health_score)
                FROM marts.market_day
                WHERE market_id = %s
                  AND day BETWEEN %s AND %s
                """,
                (market_id, prior_start, prior_end),
            )
            prior = cur.fetchone() or (None, None, None, None, None)

            def safe_delta(a, b):
                if a is None or b is None:
                    return 0.0
                return _to_float(a) - _to_float(b)

            delta = {
                "spread_median_delta": safe_delta(recent[0], prior[0]),
                "depth_2pct_delta": safe_delta(recent[1], prior[1]),
                "concentration_hhi_delta": safe_delta(recent[2], prior[2]),
                "unique_traders_delta": safe_delta(recent[3], prior[3]),
                "health_score_delta": safe_delta(recent[4], prior[4]),
            }

            recent_cohort_share = _compute_cohort_share(cur, market_id, recent_start, max_day)
            prior_cohort_share = _compute_cohort_share(cur, market_id, prior_start, prior_end)
            cohort_share_delta = _compute_cohort_share_delta(recent_cohort_share, prior_cohort_share)

            diagnosis = "STABLE"

            if (
                delta["spread_median_delta"] < 0
                and delta["depth_2pct_delta"] > 0
                and delta["concentration_hhi_delta"] < 0
            ):
                diagnosis = "LIQUIDITY_IMPROVING"
            elif (
                delta["spread_median_delta"] > 0
                and delta["concentration_hhi_delta"] > 0
            ):
                diagnosis = "CONCENTRATION_RISK"
            elif delta["unique_traders_delta"] > 0:
                diagnosis = "PARTICIPATION_EXPANDING"

            market_regime = _compute_market_regime(delta)
            cohort_risk_flags = _compute_cohort_risk_flags(recent_cohort_share, cohort_share_delta)

            return {
                "window_days": days,
                "recent_window": {"start": str(recent_start), "end": str(max_day)},
                "prior_window": {"start": str(prior_start), "end": str(prior_end)},
                "market_quality_delta": delta,
                "recent_cohort_share": recent_cohort_share,
                "prior_cohort_share": prior_cohort_share,
                "cohort_share_delta": cohort_share_delta,
                "diagnosis": diagnosis,
                "market_regime": market_regime,
                "cohort_risk_flags": cohort_risk_flags,
            }


class LaunchCandidateRow(BaseModel):
    market_id: str
    day: str
    launch_readiness_score: float
    launch_risk_score: float
    participation_quality_score: float
    liquidity_durability_score: float
    concentration_penalty: float
    speculative_flow_penalty: float
    manipulation_penalty: float
    recommendation: str
    recommendation_reason: str
    flags: List[str] = []
    title: Optional[str] = None
    category: Optional[str] = None
    url: Optional[str] = None


class LaunchCandidateDetailResponse(BaseModel):
    market_id: str
    day: str
    launch_readiness_score: float
    launch_risk_score: float
    participation_quality_score: float
    liquidity_durability_score: float
    concentration_penalty: float
    speculative_flow_penalty: float
    manipulation_penalty: float
    recommendation: str
    recommendation_reason: str
    flags: List[str] = []
    title: Optional[str] = None
    category: Optional[str] = None
    url: Optional[str] = None
    engine_version: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class SocialIntelligenceRow(BaseModel):
    market_id: str
    day: str
    attention_score: float
    sentiment_score: float
    demand_score: float
    trend_velocity: float
    mention_count: int
    source_count: int
    confidence_score: float
    recommendation: str
    summary: str
    flags: List[str] = []
    title: Optional[str] = None
    category: Optional[str] = None
    url: Optional[str] = None


class SocialIntelligenceDetailResponse(BaseModel):
    market_id: str
    day: str
    attention_score: float
    sentiment_score: float
    demand_score: float
    trend_velocity: float
    mention_count: int
    source_count: int
    confidence_score: float
    recommendation: str
    summary: str
    flags: List[str] = []
    title: Optional[str] = None
    category: Optional[str] = None
    url: Optional[str] = None
    engine_version: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@app.get("/ops/launch/candidates/{market_id}", response_model=LaunchCandidateDetailResponse)
def ops_launch_candidate_detail(market_id: str):
    return market_launch_intelligence(market_id)


@app.get("/ops/launch/candidates", response_model=List[LaunchCandidateRow])
def ops_launch_candidates(
    recommendation: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    day: Optional[date] = Query(default=None),
):
    recommendation_normalized = (recommendation or "").strip().lower()

    allowed = {"launch_ready", "monitor_then_launch", "not_ready"}
    if recommendation_normalized and recommendation_normalized not in allowed:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_recommendation",
                "message": "Invalid recommendation filter",
                "details": {"allowed": sorted(list(allowed))},
            },
        )

    q = """
    WITH latest_day AS (
      SELECT COALESCE(%s::date, (SELECT MAX(day) FROM public.market_launch_intelligence_daily)) AS day
    )
    SELECT
      li.market_id,
      li.day::text AS day,
      li.launch_readiness_score::float AS launch_readiness_score,
      li.launch_risk_score::float AS launch_risk_score,
      li.participation_quality_score::float AS participation_quality_score,
      li.liquidity_durability_score::float AS liquidity_durability_score,
      li.concentration_penalty::float AS concentration_penalty,
      li.speculative_flow_penalty::float AS speculative_flow_penalty,
      li.manipulation_penalty::float AS manipulation_penalty,
      li.recommendation,
      li.recommendation_reason,
      COALESCE(li.flags, ARRAY[]::text[]) AS flags,
      COALESCE(cm.title, i_latest.title, li.market_id) AS title,
      COALESCE(cm.category, i_latest.category) AS category,
      i_url.url AS url
    FROM public.market_launch_intelligence_daily li
    LEFT JOIN core.markets cm
      ON cm.market_id = li.market_id
    LEFT JOIN LATERAL (
      SELECT
        x.title,
        x.category
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = li.market_id
      ORDER BY x.day DESC
      LIMIT 1
    ) i_latest ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        x.url
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = li.market_id
        AND x.url IS NOT NULL
      ORDER BY x.day DESC
      LIMIT 1
    ) i_url ON TRUE
    WHERE li.day = (SELECT day FROM latest_day)
      AND (%s = '' OR li.recommendation = %s)
    ORDER BY
      li.launch_readiness_score DESC,
      li.launch_risk_score ASC,
      li.market_id ASC
    LIMIT %s;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (day, recommendation_normalized, recommendation_normalized, limit))
            return rows_as_dicts(cur)


@app.get("/ops/markets/{market_id}/launch-intelligence", response_model=LaunchCandidateDetailResponse)
def market_launch_intelligence(market_id: str):
    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "market_not_found",
                "message": "Market not found",
                "details": {"market_id": market_id},
            },
        )

    q = """
    SELECT
      li.market_id,
      li.day::text AS day,
      li.launch_readiness_score::float AS launch_readiness_score,
      li.launch_risk_score::float AS launch_risk_score,
      li.participation_quality_score::float AS participation_quality_score,
      li.liquidity_durability_score::float AS liquidity_durability_score,
      li.concentration_penalty::float AS concentration_penalty,
      li.speculative_flow_penalty::float AS speculative_flow_penalty,
      li.manipulation_penalty::float AS manipulation_penalty,
      li.recommendation,
      li.recommendation_reason,
      COALESCE(li.flags, ARRAY[]::text[]) AS flags,
      COALESCE(cm.title, i_latest.title, li.market_id) AS title,
      COALESCE(cm.category, i_latest.category) AS category,
      i_url.url AS url,
      li.engine_version,
      li.created_at::text AS created_at,
      li.updated_at::text AS updated_at
    FROM public.market_launch_intelligence_daily li
    LEFT JOIN core.markets cm
      ON cm.market_id = li.market_id
    LEFT JOIN LATERAL (
      SELECT
        x.title,
        x.category
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = li.market_id
      ORDER BY x.day DESC
      LIMIT 1
    ) i_latest ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        x.url
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = li.market_id
        AND x.url IS NOT NULL
      ORDER BY x.day DESC
      LIMIT 1
    ) i_url ON TRUE
    WHERE li.market_id = %s
    ORDER BY li.day DESC
    LIMIT 1;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "code": "launch_intelligence_not_found",
                        "message": "No launch intelligence found for market",
                        "details": {"market_id": market_id},
                    },
                )

            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))

@app.get("/ops/social/markets/{market_id}", response_model=SocialIntelligenceDetailResponse)
def market_social_intelligence(market_id: str):
    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "market_not_found",
                "message": "Market not found",
                "details": {"market_id": market_id},
            },
        )

    q = """
    SELECT
      si.market_id,
      si.day::text AS day,
      si.attention_score::float AS attention_score,
      si.sentiment_score::float AS sentiment_score,
      si.demand_score::float AS demand_score,
      si.trend_velocity::float AS trend_velocity,
      COALESCE(si.mention_count, 0)::int AS mention_count,
      COALESCE(si.source_count, 0)::int AS source_count,
      si.confidence_score::float AS confidence_score,
      si.recommendation,
      si.summary,
      COALESCE(si.flags, ARRAY[]::text[]) AS flags,
      COALESCE(cm.title, i_latest.title, si.market_id) AS title,
      COALESCE(cm.category, i_latest.category) AS category,
      i_url.url AS url,
      si.engine_version,
      si.created_at::text AS created_at,
      si.updated_at::text AS updated_at
    FROM public.market_social_intelligence_daily si
    LEFT JOIN core.markets cm
      ON cm.market_id = si.market_id
    LEFT JOIN LATERAL (
      SELECT
        x.title,
        x.category
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = si.market_id
      ORDER BY x.day DESC
      LIMIT 1
    ) i_latest ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        x.url
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = si.market_id
        AND x.url IS NOT NULL
      ORDER BY x.day DESC
      LIMIT 1
    ) i_url ON TRUE
    WHERE si.market_id = %s
    ORDER BY si.day DESC
    LIMIT 1;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "code": "social_intelligence_not_found",
                        "message": "No social intelligence found for market",
                        "details": {"market_id": market_id},
                    },
                )

            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))


@app.get("/ops/social/candidates", response_model=List[SocialIntelligenceRow])
def ops_social_candidates(
    recommendation: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    day: Optional[date] = Query(default=None),
):
    recommendation_normalized = (recommendation or "").strip().lower()

    allowed = {"rising", "watch", "weak"}
    if recommendation_normalized and recommendation_normalized not in allowed:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_recommendation",
                "message": "Invalid recommendation filter",
                "details": {"allowed": sorted(list(allowed))},
            },
        )

    q = """
    WITH latest_day AS (
      SELECT COALESCE(%s::date, (SELECT MAX(day) FROM public.market_social_intelligence_daily)) AS day
    )
    SELECT
      si.market_id,
      si.day::text AS day,
      si.attention_score::float AS attention_score,
      si.sentiment_score::float AS sentiment_score,
      si.demand_score::float AS demand_score,
      si.trend_velocity::float AS trend_velocity,
      COALESCE(si.mention_count, 0)::int AS mention_count,
      COALESCE(si.source_count, 0)::int AS source_count,
      si.confidence_score::float AS confidence_score,
      si.recommendation,
      si.summary,
      COALESCE(si.flags, ARRAY[]::text[]) AS flags,
      COALESCE(cm.title, i_latest.title, si.market_id) AS title,
      COALESCE(cm.category, i_latest.category) AS category,
      i_url.url AS url
    FROM public.market_social_intelligence_daily si
    LEFT JOIN core.markets cm
      ON cm.market_id = si.market_id
    LEFT JOIN LATERAL (
      SELECT
        x.title,
        x.category
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = si.market_id
      ORDER BY x.day DESC
      LIMIT 1
    ) i_latest ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        x.url
      FROM public.market_integrity_score_daily x
      WHERE x.market_id = si.market_id
        AND x.url IS NOT NULL
      ORDER BY x.day DESC
      LIMIT 1
    ) i_url ON TRUE
    WHERE si.day = (SELECT day FROM latest_day)
      AND (%s = '' OR si.recommendation = %s)
    ORDER BY
      si.demand_score DESC,
      si.attention_score DESC,
      si.market_id ASC
    LIMIT %s;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (day, recommendation_normalized, recommendation_normalized, limit))
            return rows_as_dicts(cur)



# -----------------------------
# Single market (latest view)
# Supports:
# - full markets with core.markets + marts.market_day
# - integrity-only live markets not yet present in core.markets
# -----------------------------
@app.get("/ops/markets/{market_id}")
def ops_market(market_id: str):
    q = """
WITH latest_md AS (
  SELECT
    md.market_id,
    md.day,
    md.volume,
    md.trades,
    md.unique_traders,
    md.spread_median,
    md.depth_2pct_median,
    md.concentration_hhi,
    md.health_score,
    md.risk_score,
    md.flags
  FROM marts.market_day md
  WHERE md.market_id = %s
  ORDER BY md.day DESC
  LIMIT 1
),
latest_integrity AS (
  SELECT
    i.market_id,
    i.day,
    i.title,
    i.category,
    i.regime,
    i.regime_reason,
    i.trades,
    i.unique_traders,
    i.market_quality_score,
    i.liquidity_health_score,
    i.concentration_risk_score,
    i.whale_volume_share,
    i.radar_risk_score,
    i.manipulation_score,
    i.manipulation_signal,
    i.whale_role_share,
    i.speculator_role_share,
    i.neutral_role_share,
    i.possible_farmer_count,
    i.integrity_score,
    i.integrity_band,
    i.review_priority,
    i.primary_reason,
    i.needs_operator_review,
    i.has_regime_data,
    i.has_radar_data,
    i.has_manipulation_data,
    i.data_completeness_score,
    i.is_partial_coverage
  FROM public.market_integrity_score_daily i
  WHERE i.market_id = %s
  ORDER BY i.day DESC
  LIMIT 1
),
latest_integrity_url AS (
  SELECT
    i.market_id,
    i.url
  FROM public.market_integrity_score_daily i
  WHERE i.market_id = %s
    AND i.url IS NOT NULL
  ORDER BY i.day DESC
  LIMIT 1
),
anchor AS (
  SELECT
    COALESCE(li.market_id, md.market_id) AS market_id,
    GREATEST(
      COALESCE(li.day, DATE '1900-01-01'),
      COALESCE(md.day, DATE '1900-01-01')
    ) AS day
  FROM latest_integrity li
  FULL OUTER JOIN latest_md md
    ON li.market_id = md.market_id
  LIMIT 1
),

base AS (
  SELECT
    a.market_id,
    COALESCE(m.protocol, 'polymarket') AS protocol,
    COALESCE(m.chain, 'polygon') AS chain,
    COALESCE(li.title, m.title, a.market_id) AS title,
    COALESCE(li.category, m.category) AS category,
    li_url.url AS url,
    a.day,

    md.volume,
    COALESCE(md.trades, li.trades) AS trades,
    COALESCE(md.unique_traders, li.unique_traders) AS unique_traders,

    md.spread_median,
    md.depth_2pct_median,
    md.concentration_hhi,

    COALESCE(mo.health_score_override, md.health_score) AS health_score,
    COALESCE(mo.risk_score_override, md.risk_score) AS risk_score,
    (mo.id IS NOT NULL) AS has_manual_override,
    COALESCE(md.flags, '[]'::jsonb) AS flags,

    li.regime,
    li.regime_reason,
    li.market_quality_score,
    li.liquidity_health_score,
    li.concentration_risk_score,
    li.whale_volume_share,
    li.radar_risk_score,
    li.manipulation_score,
    li.manipulation_signal,
    li.whale_role_share,
    li.speculator_role_share,
    li.neutral_role_share,
    li.possible_farmer_count,
    li.integrity_score,
    li.integrity_band,
    li.review_priority,
    li.primary_reason,
    li.needs_operator_review,
    li.has_regime_data,
    li.has_radar_data,
    li.has_manipulation_data,
    li.data_completeness_score,
    li.is_partial_coverage
  FROM anchor a
  LEFT JOIN core.markets m
    ON m.market_id = a.market_id
  LEFT JOIN latest_md md
    ON md.market_id = a.market_id
  LEFT JOIN latest_integrity li
    ON li.market_id = a.market_id
  LEFT JOIN latest_integrity_url li_url
    ON li_url.market_id = a.market_id
  LEFT JOIN market_manual_overrides mo
    ON mo.market_id = a.market_id AND mo.day = a.day
)
SELECT
  b.market_id,
  b.protocol,
  b.chain,
  b.title,
  b.category,
  b.url,
  b.day,
  b.volume,
  b.trades,
  b.unique_traders,
  b.spread_median,
  b.depth_2pct_median,
  b.concentration_hhi,
  b.health_score,
  b.risk_score,
  b.has_manual_override,
  b.flags,
  b.regime,
  b.regime_reason,
  b.market_quality_score,
  b.liquidity_health_score,
  b.concentration_risk_score,
  b.whale_volume_share,
  b.radar_risk_score,
  b.manipulation_score,
  b.manipulation_signal,
  b.whale_role_share,
  b.speculator_role_share,
  b.neutral_role_share,
  b.possible_farmer_count,
  b.integrity_score,
  b.integrity_band,
  b.review_priority,
  b.primary_reason,
  b.needs_operator_review,
  b.has_regime_data,
  b.has_radar_data,
  b.has_manipulation_data,
  b.data_completeness_score,
  b.is_partial_coverage
FROM base b
LIMIT 1;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, market_id))
            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "code": "market_not_found",
                        "message": "Market not found in analytics tables",
                        "details": {"market_id": market_id},
                    },
                )

            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))

# -----------------------------
# Incident effectiveness
# Option B: before = day before incident day
# after = incident day + after_days, capped at latest_day
# -----------------------------
@app.get("/ops/markets/{market_id}/incidents/effectiveness")
def incident_effectiveness(market_id: str, days: int = 30, after_days: int = 3, **_ignored):
    days = max(1, min(int(days), 180))
    after_days = max(0, min(int(after_days), 30))

    q = """
WITH latest AS (
  SELECT COALESCE(MAX(day), CURRENT_DATE) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
),
inc AS (
  SELECT
    i.id AS incident_id,
    i.market_id,
    i.day::date AS incident_day,
    i.status,
    i.note,
    i.created_by,
    i.created_at
  FROM market_incidents i
  WHERE i.market_id = %s
    AND i.day >= (SELECT latest_day FROM latest) - %s
),
base AS (
  SELECT
    inc.*,
    (inc.incident_day - INTERVAL '1 day')::date AS before_day,
    inc.incident_day AS after_day
  FROM inc
),
joined AS (
  SELECT
    base.*,

    mb.trades AS b_trades,
    mb.volume AS b_volume,
    mb.risk_score AS b_risk,
    mb.health_score AS b_health,
    mb.spread_median AS b_spread,
    mb.unique_traders AS b_unique,
    mb.concentration_hhi AS b_hhi,
    mb.depth_2pct_median AS b_depth,

    ma.trades AS a_trades,
    ma.volume AS a_volume,
    ma.risk_score AS a_risk,
    ma.health_score AS a_health,
    ma.spread_median AS a_spread,
    ma.unique_traders AS a_unique,
    ma.concentration_hhi AS a_hhi,
    ma.depth_2pct_median AS a_depth

  FROM base
  LEFT JOIN market_metrics_daily mb
    ON mb.market_id = base.market_id AND mb.day = base.before_day
  LEFT JOIN market_metrics_daily ma
    ON ma.market_id = base.market_id AND ma.day = base.after_day
),
scored AS (
  SELECT
    j.*,

    (j.a_trades - j.b_trades) AS d_trades,
    (j.a_volume - j.b_volume) AS d_volume,
    (j.a_risk - j.b_risk) AS d_risk,
    (j.a_health - j.b_health) AS d_health,
    (j.a_spread - j.b_spread) AS d_spread,
    (j.a_unique - j.b_unique) AS d_unique,
    (j.a_hhi - j.b_hhi) AS d_hhi,
    (j.a_depth - j.b_depth) AS d_depth,

    (
      COALESCE((j.b_risk - j.a_risk), 0) * 1.0 +
      COALESCE((j.a_health - j.b_health), 0) * 1.0 +
      COALESCE((j.b_spread - j.a_spread), 0) * 100.0 +
      COALESCE((j.a_depth - j.b_depth) / 100.0, 0) * 1.0
    ) AS delta_score

  FROM joined j
)
SELECT
  scored.incident_id,
  scored.market_id,
  scored.incident_day AS day,
  scored.status,
  scored.note,
  scored.created_by,
  scored.created_at,

  scored.before_day,
  scored.after_day,

  jsonb_build_object(
    'trades', scored.b_trades,
    'volume', scored.b_volume,
    'risk_score', scored.b_risk,
    'health_score', scored.b_health,
    'spread_median', scored.b_spread,
    'unique_traders', scored.b_unique,
    'concentration_hhi', scored.b_hhi,
    'depth_2pct_median', scored.b_depth
  ) AS before,

  jsonb_build_object(
    'trades', scored.a_trades,
    'volume', scored.a_volume,
    'risk_score', scored.a_risk,
    'health_score', scored.a_health,
    'spread_median', scored.a_spread,
    'unique_traders', scored.a_unique,
    'concentration_hhi', scored.a_hhi,
    'depth_2pct_median', scored.a_depth
  ) AS after,

  jsonb_build_object(
    'trades', scored.d_trades,
    'volume', scored.d_volume,
    'risk_score', scored.d_risk,
    'health_score', scored.d_health,
    'spread_median', scored.d_spread,
    'unique_traders', scored.d_unique,
    'concentration_hhi', scored.d_hhi,
    'depth_2pct_median', scored.d_depth
  ) AS delta,

  scored.delta_score
FROM scored
ORDER BY scored.incident_day DESC, scored.created_at DESC, scored.incident_id DESC;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)

# -----------------------------
# Market snapshot (one call for the whole market page)
# -----------------------------
@app.get("/ops/markets/{market_id}/snapshot")
def ops_market_snapshot(
    market_id: str,
    timeline_days: int = 30,
    lookback_days: int = 30,
    impact_days: int = 14,
):
    timeline_days = max(1, min(int(timeline_days), 60))
    lookback_days = max(1, min(int(lookback_days), 180))
    impact_days = max(4, min(int(impact_days), 60))

    INCIDENT_EFFECT_DAYS = 30
    INCIDENT_AFTER_DAYS = 3

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "market_not_found",
                "message": "Market not found",
                "details": {"market_id": market_id},
            },
        )

    errors: List[Dict[str, Any]] = []

    def safe_call(key: str, fn, default):
        try:
            v = fn()
            if v is None:
                return default
            return v
        except HTTPException as e:
            errors.append(
                {
                    "key": key,
                    "message": str(getattr(e, "detail", "")) or f"HTTPException: {str(e)}",
                    "status": int(getattr(e, "status_code", 500) or 500),
                }
            )
            return default
        except Exception as e:
            errors.append(
                {
                    "key": key,
                    "message": f"{type(e).__name__}: {str(e)}",
                    "status": 500,
                }
            )
            return default

    market = safe_call("market", lambda: ops_market(market_id), {})

    timeline = safe_call(
        "timeline",
        lambda: market_timeline(market_id, days=timeline_days),
        [],
    )

    incidents = safe_call(
        "incidents",
        lambda: market_incidents(market_id, days=lookback_days),
        [],
    )

    incident_events = safe_call(
        "incident_events",
        lambda: market_incident_events(market_id, days=lookback_days),
        [],
    )

    incident_effectiveness_rows = safe_call(
        "incident_effectiveness",
        lambda: incident_effectiveness(
            market_id,
            days=min(lookback_days, INCIDENT_EFFECT_DAYS),
            after_days=INCIDENT_AFTER_DAYS,
        ),
        [],
    )

    interventions = safe_call(
        "interventions",
        lambda: list_interventions_collapsed(market_id, days=lookback_days),
        [],
    )

    interventions_effectiveness_rows = safe_call(
        "interventions_effectiveness",
        lambda: interventions_effectiveness(market_id, days=lookback_days),
        [],
    )

    interventions_cumulative_row = safe_call(
        "intervention_cumulative",
        lambda: interventions_cumulative(market_id, days=lookback_days),
        {
            "days": lookback_days,
            "count_total": 0,
            "count_effective": 0,
            "risk_score": 0.0,
            "health_score": 0.0,
            "spread_median": 0.0,
            "depth_2pct_median": 0.0,
        },
    )

    overrides = safe_call(
        "overrides",
        lambda: list_overrides(market_id, days=lookback_days),
        [],
    )

    launch_intelligence = safe_call(
        "launch_intelligence",
        lambda: market_launch_intelligence(market_id),
        {},
    )

    social_intelligence = safe_call(
        "social_intelligence",
        lambda: market_social_intelligence(market_id),
        {},
    )

    # -----------------------------
    # Rolling trader context
    # -----------------------------
    traders_summary = safe_call(
        "traders_summary",
        lambda: market_traders_summary(market_id, days=lookback_days, top_n=10),
        [],
    )

    cohorts_summary = safe_call(
        "cohorts_summary",
        lambda: market_trader_cohorts_summary(market_id, days=lookback_days),
        [],
    )

    trader_intelligence = safe_call(
        "trader_intelligence",
        lambda: market_trader_intelligence(market_id, days=lookback_days, top_n=50),
        [],
    )

    # -----------------------------
    # Same day trader context
    # -----------------------------
    same_day_traders_summary = safe_call(
        "same_day_traders_summary",
        lambda: market_traders_summary(market_id, days=1, top_n=10),
        [],
    )

    same_day_cohorts_summary = safe_call(
        "same_day_cohorts_summary",
        lambda: market_trader_cohorts_summary(market_id, days=1),
        [],
    )

    same_day_trader_intelligence = safe_call(
        "same_day_trader_intelligence",
        lambda: market_trader_intelligence(market_id, days=1, top_n=50),
        [],
    )

    impact = safe_call(
        "impact",
        lambda: market_trader_impact(market_id, days=impact_days, anchor_day=None),
        {
            "window_days": impact_days,
            "recent_window": {"start": "", "end": ""},
            "prior_window": {"start": "", "end": ""},
            "market_quality_delta": {
                "spread_median_delta": 0.0,
                "depth_2pct_delta": 0.0,
                "concentration_hhi_delta": 0.0,
                "unique_traders_delta": 0.0,
                "health_score_delta": 0.0,
            },
            "recent_cohort_share": [],
            "prior_cohort_share": [],
            "cohort_share_delta": [],
            "diagnosis": "STABLE",
            "market_regime": "STABLE",
            "cohort_risk_flags": ["COHORT_STRUCTURE_STABLE"],
        },
    )

    recent_cohort_share = (impact or {}).get("recent_cohort_share") or []
    recent_share_map = {
        (row.get("cohort") or "").upper(): row
        for row in recent_cohort_share
    }

    opportunity_summary = {
        "structural_state": None,
        "launch_state": None,
        "social_state": None,
        "summary": "No combined opportunity view available.",
        "alignment": "unknown",
        "signals": [],
    }

    if market:
        structural_state = (
            (market.get("integrity_band") or market.get("regime") or "unknown")
            if isinstance(market, dict)
            else "unknown"
        )
        launch_state = (
            (launch_intelligence.get("recommendation") or "unknown")
            if isinstance(launch_intelligence, dict) and launch_intelligence
            else "unknown"
        )
        social_state = (
            (social_intelligence.get("recommendation") or "unknown")
            if isinstance(social_intelligence, dict) and social_intelligence
            else "unknown"
        )

        signals = []

        if (market.get("needs_operator_review") is True):
            signals.append("operator_review_needed")

        if isinstance(market.get("structural_divergence"), dict):
            if market["structural_divergence"].get("has_divergence") is True:
                signals.append("structural_divergence_present")

        if launch_state == "launch_ready":
            signals.append("launch_ready")
        elif launch_state == "monitor_then_launch":
            signals.append("monitor_before_launch")
        elif launch_state == "not_ready":
            signals.append("not_launch_ready")

        if social_state == "rising":
            signals.append("social_demand_rising")
        elif social_state == "watch":
            signals.append("social_demand_watch")
        elif social_state == "cold":
            signals.append("social_demand_cold")

        alignment = "mixed"
        summary = "Structural and demand signals are mixed."

        if launch_state == "launch_ready" and social_state == "rising":
            alignment = "strong"
            summary = "Strong structural quality and rising demand signals."
        elif launch_state == "monitor_then_launch" and social_state == "rising":
            alignment = "developing"
            summary = "Demand is improving, but structure still needs monitoring."
        elif launch_state == "launch_ready" and social_state in {"watch", "cold"}:
            alignment = "structural_only"
            summary = "Structure looks ready, but demand confirmation is weaker."
        elif launch_state == "not_ready":
            alignment = "weak"
            summary = "Structural quality is not ready for launch regardless of current demand."
        elif launch_state == "monitor_then_launch" and social_state == "watch":
            alignment = "mixed"
            summary = "Market is structurally promising but still needs more confirmation."
        elif launch_state == "unknown" and social_state != "unknown":
            alignment = "partial"
            summary = "Demand signal is available, but launch signal is missing."
        elif launch_state != "unknown" and social_state == "unknown":
            alignment = "partial"
            summary = "Launch signal is available, but demand signal is missing."

        opportunity_summary = {
            "structural_state": structural_state,
            "launch_state": launch_state,
            "social_state": social_state,
            "summary": summary,
            "alignment": alignment,
            "signals": signals,
        }

    if market:
        market["recent_whale_notional_share"] = float(
            (recent_share_map.get("WHALE") or {}).get("notional_share") or 0.0
        )
        market["recent_speculator_notional_share"] = float(
            (recent_share_map.get("SPECULATOR") or {}).get("notional_share") or 0.0
        )
        market["recent_neutral_notional_share"] = float(
            (recent_share_map.get("NEUTRAL") or {}).get("notional_share") or 0.0
        )

        market["recent_whale_trade_share"] = float(
            (recent_share_map.get("WHALE") or {}).get("trade_share") or 0.0
        )
        market["recent_speculator_trade_share"] = float(
            (recent_share_map.get("SPECULATOR") or {}).get("trade_share") or 0.0
        )
        market["recent_neutral_trade_share"] = float(
            (recent_share_map.get("NEUTRAL") or {}).get("trade_share") or 0.0
        )

        market["role_share_interpretation"] = {
            "latest_day": "Same day role shares from market integrity and radar.",
            "recent_window": "Rolling cohort shares derived from the impact window.",
        }

    interventions_effectiveness_ui = {
        "heat": {
            "good_up": ["health_score", "depth_2pct_median", "unique_traders", "volume", "trades"],
            "good_down": ["risk_score", "spread_median", "concentration_hhi"],
            "steps": {
                "risk_score": 1.0,
                "health_score": 1.0,
                "spread_median": 0.0005,
                "depth_2pct_median": 250.0,
                "unique_traders": 5.0,
                "volume": 1000.0,
                "trades": 10.0,
                "concentration_hhi": 0.01,
                "delta_score": 2.0,
                "roi_score": 0.25,
            },
            "precision": {
                "risk_score": 0,
                "health_score": 0,
                "spread_median": 5,
                "depth_2pct_median": 0,
                "unique_traders": 0,
                "volume": 0,
                "trades": 0,
                "concentration_hhi": 3,
                "delta_score": 2,
                "roi_score": 2,
            },
        }
    }

    anchor_day = (market or {}).get("day")

    if market:
        latest_whale_share = float((market or {}).get("whale_role_share") or 0.0)
        latest_speculator_share = float((market or {}).get("speculator_role_share") or 0.0)
        latest_neutral_share = float((market or {}).get("neutral_role_share") or 0.0)

        recent_whale_share = float((market or {}).get("recent_whale_notional_share") or 0.0)
        recent_speculator_share = float((market or {}).get("recent_speculator_notional_share") or 0.0)
        recent_neutral_share = float((market or {}).get("recent_neutral_notional_share") or 0.0)

        divergence_flags: List[str] = []

        if (
            (latest_whale_share <= 0.05 and recent_whale_share >= 0.15)
            or (latest_speculator_share <= 0.05 and recent_speculator_share >= 0.15)
            or ((latest_neutral_share - recent_neutral_share) >= 0.20)
        ):
            divergence_flags.append("LATEST_DAY_VS_RECENT_WINDOW_DIVERGENCE")

        if latest_whale_share <= 0.05 and recent_whale_share >= 0.15:
            divergence_flags.append("RECENT_WHALE_PARTICIPATION_PRESENT")

        if latest_speculator_share <= 0.05 and recent_speculator_share >= 0.15:
            divergence_flags.append("RECENT_SPECULATIVE_FLOW_PRESENT")

        if (latest_neutral_share - recent_neutral_share) >= 0.20:
            divergence_flags.append("NEUTRAL_SHARE_WEAKER_IN_RECENT_WINDOW")

        if divergence_flags:
            divergence_summary_parts: List[str] = []

            if "RECENT_WHALE_PARTICIPATION_PRESENT" in divergence_flags:
                divergence_summary_parts.append("recent whale participation is meaningful")

            if "RECENT_SPECULATIVE_FLOW_PRESENT" in divergence_flags:
                divergence_summary_parts.append("recent speculative flow is meaningful")

            if "NEUTRAL_SHARE_WEAKER_IN_RECENT_WINDOW" in divergence_flags:
                divergence_summary_parts.append("recent neutral participation is weaker than the latest day view")

            market["structural_divergence"] = {
                "has_divergence": True,
                "flags": divergence_flags,
                "summary": (
                    "Latest day appears more neutral than the recent window. "
                    + "; ".join(divergence_summary_parts)
                    + "."
                ),
                "latest_day": {
                    "whale_role_share": latest_whale_share,
                    "speculator_role_share": latest_speculator_share,
                    "neutral_role_share": latest_neutral_share,
                },
                "recent_window": {
                    "whale_notional_share": recent_whale_share,
                    "speculator_notional_share": recent_speculator_share,
                    "neutral_notional_share": recent_neutral_share,
                },
            }
        else:
            market["structural_divergence"] = {
                "has_divergence": False,
                "flags": [],
                "summary": "Latest day structure is broadly consistent with the recent window.",
                "latest_day": {
                    "whale_role_share": latest_whale_share,
                    "speculator_role_share": latest_speculator_share,
                    "neutral_role_share": latest_neutral_share,
                },
                "recent_window": {
                    "whale_notional_share": recent_whale_share,
                    "speculator_notional_share": recent_speculator_share,
                    "neutral_notional_share": recent_neutral_share,
                },
            }

    snapshot = {
        "market": {
            **(market or {}),
            "horizon": "latest_day",
        },
        "launch_intelligence": {
            **(launch_intelligence or {}),
            "horizon": "latest_day",
        } if launch_intelligence else {},
        "social_intelligence": {
            **(social_intelligence or {}),
            "horizon": "latest_day",
        } if social_intelligence else {},
        "opportunity_summary": opportunity_summary,
        "timeline": timeline or [],
        "incidents": incidents or [],
        "incident_events": incident_events or [],
        "incident_effectiveness": incident_effectiveness_rows or [],
        "interventions": interventions or [],
        "interventions_effectiveness": interventions_effectiveness_rows or [],
        "interventions_effectiveness_ui": interventions_effectiveness_ui,
        "intervention_cumulative": interventions_cumulative_row or {},
        "overrides": overrides or [],
        "traders": {
            "same_day": {
                "horizon": "same_day",
                "anchor_day": anchor_day,
                "summary": same_day_traders_summary or [],
                "cohorts_summary": same_day_cohorts_summary or [],
                "intelligence": same_day_trader_intelligence or [],
            },
            "rolling_window": {
                "horizon": f"last_{lookback_days}_days",
                "anchor_day": anchor_day,
                "summary": traders_summary or [],
                "cohorts_summary": cohorts_summary or [],
                "intelligence": trader_intelligence or [],
            },
        },
        "impact": {
            **(impact or {}),
            "horizon": f"last_{impact_days}_days",
        },
        "errors": errors,
        "snapshot_meta": {
            "market_horizon": "latest_day",
            "launch_horizon": "latest_day",
            "social_horizon": "latest_day",
            "traders_horizon": {
                "same_day": "same_day",
                "rolling_window": f"last_{lookback_days}_days",
            },
            "impact_horizon": f"last_{impact_days}_days",
            "interpretation": {
                "market": "Latest available daily state for this market.",
                "launch_intelligence": "Latest structural launch recommendation for this market.",
                "social_intelligence": "Latest demand and attention signal for this market.",
                "opportunity_summary": "Combined structural and demand interpretation for launch decision support.",
                "traders": {
                    "same_day": "Participant context for the latest market day only.",
                    "rolling_window": "Recent participant context aggregated over the trader lookback window.",
                },
                "impact": "Recent trend context comparing the recent and prior windows.",
            },
        },
    }

    snapshot["coverage_summary"] = build_coverage_summary(snapshot)

    snapshot["coverage_summary"]["has_launch_intelligence"] = bool(snapshot.get("launch_intelligence"))
    snapshot["coverage_summary"]["has_social_intelligence"] = bool(snapshot.get("social_intelligence"))
    snapshot["coverage_summary"]["has_opportunity_summary"] = bool(snapshot.get("opportunity_summary"))

    return snapshot

# -----------------------------
# Timeline (anchored to latest day)
# -----------------------------
@app.get("/ops/markets/{market_id}/timeline")
def market_timeline(market_id: str, days: int = 14):
    days = max(1, min(int(days), 60))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM marts.market_day
  WHERE market_id = %s
)
SELECT
  day,
  volume,
  trades,
  unique_traders,
  spread_median,
  depth_2pct_median,
  concentration_hhi,
  health_score,
  risk_score
FROM marts.market_day
WHERE market_id = %s
  AND day >= (SELECT latest_day FROM latest) - %s
ORDER BY day ASC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)

# -----------------------------
# Trader Pillar (Seed + Daily Metrics)
# -----------------------------
from datetime import datetime, timezone
import random
import hashlib

class SeedTradesRequest(BaseModel):
    days: int = 30
    markets: int = 5
    traders: int = 80
    trades_per_day_min: int = 200
    trades_per_day_max: int = 600
    seed: int = 42

@app.post("/dev/seed_trades")
def dev_seed_trades(payload: SeedTradesRequest, operator: AuthUser = Depends(require_operator)):
    days = max(1, min(int(payload.days), 90))
    markets_n = max(1, min(int(payload.markets), 25))
    traders_n = max(5, min(int(payload.traders), 500))
    tmin = max(10, min(int(payload.trades_per_day_min), 5000))
    tmax = max(tmin, min(int(payload.trades_per_day_max), 10000))

    rng = random.Random(int(payload.seed))

    q_markets = """
SELECT market_id
FROM core.markets
WHERE is_active = true
ORDER BY market_id
LIMIT %s;
"""

    q_latest = """
SELECT MAX(day) AS latest_day
FROM marts.market_day;
"""

    q_insert = """
INSERT INTO core.trades (
  trade_id, market_id, trader_id, side, price, size, notional, ts, day, source
)
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s::timestamptz, %s::date, 'seed'
);
"""

    q_clear_window = """
DELETE FROM core.trades
WHERE source = 'seed'
  AND day >= %s::date
  AND day <= %s::date;
"""

    q_build_user_market_daily = """
INSERT INTO user_market_daily (
  market_id, day, trader_id,
  trades_count, volume_notional, avg_trade_size,
  buy_count, sell_count,
  first_ts, last_ts
)
SELECT
  market_id,
  day,
  trader_id,
  COUNT(*)::int AS trades_count,
  SUM(notional) AS volume_notional,
  AVG(size) AS avg_trade_size,
  SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END)::int AS buy_count,
  SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END)::int AS sell_count,
  MIN(ts) AS first_ts,
  MAX(ts) AS last_ts
FROM core.trades
WHERE day >= %s::date AND day <= %s::date
GROUP BY market_id, day, trader_id
ON CONFLICT (market_id, day, trader_id)
DO UPDATE SET
  trades_count = EXCLUDED.trades_count,
  volume_notional = EXCLUDED.volume_notional,
  avg_trade_size = EXCLUDED.avg_trade_size,
  buy_count = EXCLUDED.buy_count,
  sell_count = EXCLUDED.sell_count,
  first_ts = EXCLUDED.first_ts,
  last_ts = EXCLUDED.last_ts;
"""

    q_clear_user_market_daily = """
DELETE FROM user_market_daily
WHERE day >= %s::date AND day <= %s::date;
"""

    q_clear_user_cohorts_daily = """
DELETE FROM user_cohorts_daily
WHERE day >= %s::date AND day <= %s::date;
"""

    q_build_cohorts = """
WITH base AS (
  SELECT
    market_id, day, trader_id,
    trades_count,
    volume_notional,
    avg_trade_size
  FROM user_market_daily
  WHERE day >= %s::date AND day <= %s::date
),
ranked AS (
  SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY market_id, day ORDER BY volume_notional) AS pr
  FROM base
)
INSERT INTO user_cohorts_daily (market_id, day, trader_id, cohort, score)
SELECT
  market_id,
  day,
  trader_id,
  CASE
    WHEN pr >= 0.95 THEN 'WHALE'
    WHEN trades_count >= 25 AND avg_trade_size <= 2 THEN 'FARMER'
    WHEN trades_count >= 10 THEN 'ACTIVE'
    ELSE 'CASUAL'
  END AS cohort,
  volume_notional AS score
FROM ranked
ON CONFLICT (market_id, day, trader_id)
DO UPDATE SET cohort = EXCLUDED.cohort, score = EXCLUDED.score;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q_latest)
            latest_day = cur.fetchone()[0]
            if not latest_day:
                raise HTTPException(status_code=400, detail={"code": "no_market_days", "message": "marts.market_day has no rows"})

            cur.execute(q_markets, (markets_n,))
            market_ids = [r[0] for r in cur.fetchall()]
            if not market_ids:
                raise HTTPException(status_code=400, detail={"code": "no_markets", "message": "No markets available to seed"})

            end_day = latest_day
            start_day = end_day - timedelta(days=days - 1)

            cur.execute(q_clear_window, (start_day, end_day))
            cur.execute(q_clear_user_market_daily, (start_day, end_day))
            cur.execute(q_clear_user_cohorts_daily, (start_day, end_day))

            trader_ids = [f"t{n:04d}" for n in range(1, traders_n + 1)]

            # Ensure traders exist (FK on core.trades.trader_id)
            cur.execute(
                """
                INSERT INTO core.traders (trader_id)
                SELECT UNNEST(%s::text[])
                ON CONFLICT (trader_id) DO NOTHING;
                """,
                (trader_ids,),
            )

            total = 0
            for d in range(days):
                day = start_day + timedelta(days=d)
                for mid in market_ids:
                    n_trades = rng.randint(tmin, tmax)
                    for _ in range(n_trades):
                        trader = rng.choice(trader_ids)
                        side = rng.choice(["BUY", "SELL"])

                        price = max(0.01, min(0.99, 0.5 + rng.uniform(-0.15, 0.15)))

                        if rng.random() < 0.03:
                            size = rng.uniform(50, 250)
                        else:
                            size = rng.uniform(0.2, 8.0)

                        notional = price * size

                        seconds = rng.randint(0, 86399)
                        ts = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(seconds=seconds)

                        trade_id = hashlib.md5(f"seed:{mid}:{trader}:{ts.isoformat()}:{side}:{price:.6f}:{size:.6f}".encode()).hexdigest()
                        cur.execute(q_insert, (trade_id, mid, trader, side, price, size, notional, ts, day))
                        total += 1

            cur.execute(q_build_user_market_daily, (start_day, end_day))
            cur.execute(q_build_cohorts, (start_day, end_day))

            conn.commit()

            return {
                "ok": True,
                "seeded_trades": total,
                "markets": len(market_ids),
                "traders": traders_n,
                "window": {"start_day": str(start_day), "end_day": str(end_day)},
            }

# -----------------------------
# Incidents
# -----------------------------
class IncidentCreate(BaseModel):
    day: str
    status: str = "OPEN"
    note: str
    created_by: str = "operator"


@app.get("/ops/markets/{market_id}/incidents")
def market_incidents(market_id: str, days: int = 14):
    days = max(1, min(int(days), 180))
    q = """
SELECT
  id,
  market_id,
  day,
  status,
  note,
  created_by,
  created_at
FROM market_incidents
WHERE market_id = %s
ORDER BY created_at DESC, id DESC
LIMIT %s;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, days))
            return rows_as_dicts(cur)


@app.post("/ops/markets/{market_id}/incidents")
def create_incident(
    market_id: str,
    payload: IncidentCreate,
    operator: AuthUser = Depends(require_operator),
):
    created_by = operator.email

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "market_not_found",
                "message": "Market not found",
                "details": {"market_id": market_id},
            },
        )

    status = (payload.status or "OPEN").strip().upper()
    if status not in {"OPEN", "MONITOR", "RESOLVED"}:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_status",
                "message": "Invalid status",
                "details": {"allowed": ["OPEN", "MONITOR", "RESOLVED"]},
            },
        )

    q_incident = """
INSERT INTO market_incidents (market_id, day, status, note, created_by)
VALUES (%s, %s::date, %s, %s, %s)
RETURNING id, market_id, day, status, note, created_by, created_at;
"""

    q_event_created = """
INSERT INTO market_incident_events (
  incident_id,
  market_id,
  day,
  event_type,
  from_status,
  to_status,
  note,
  created_by,
  created_at
)
VALUES (%s, %s, %s::date, %s, %s, %s, %s, %s, %s);
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q_incident, (market_id, payload.day, status, payload.note, created_by))
            row = cur.fetchone()
            cols = [c.name for c in cur.description]
            incident = dict(zip(cols, row))

            cur.execute(
                q_event_created,
                (
                    incident["id"],
                    incident["market_id"],
                    incident["day"],
                    "CREATED",
                    None,
                    incident["status"],
                    (incident.get("note") or "").strip() or None,
                    created_by,
                    incident["created_at"],
                ),
            )

            conn.commit()
            return incident

# -----------------------------
# Incident Events (append only)
# -----------------------------
class IncidentStatusUpdate(BaseModel):
    status: str
    note: str | None = None
    created_by: str | None = None


@app.get("/ops/markets/{market_id}/incident_events")
def market_incident_events(market_id: str, days: int = 30):
    days = max(1, min(int(days), 500))
    q = """
SELECT
  id,
  incident_id,
  market_id,
  day,
  event_type,
  from_status,
  to_status,
  note,
  created_by,
  created_at
FROM market_incident_events
WHERE market_id = %s
ORDER BY created_at DESC, id DESC
LIMIT %s;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, days))
            return rows_as_dicts(cur)


@app.post("/ops/incidents/{incident_id}/status")
def update_incident_status(
    incident_id: int,
    payload: IncidentStatusUpdate,
    operator: AuthUser = Depends(require_operator),
):
    created_by = operator.email

    desired = (payload.status or "").strip().upper()
    if desired not in {"OPEN", "MONITOR", "RESOLVED"}:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_status",
                "message": "Invalid status",
                "details": {"allowed": ["OPEN", "MONITOR", "RESOLVED"]},
            },
        )

    q_get = """
SELECT id, market_id, day, status, note, created_by, created_at
FROM market_incidents
WHERE id = %s;
"""
    q_update = """
UPDATE market_incidents
SET status = %s
WHERE id = %s
RETURNING id, market_id, day, status, note, created_by, created_at;
"""
    q_event = """
INSERT INTO market_incident_events (
  incident_id,
  market_id,
  day,
  event_type,
  from_status,
  to_status,
  note,
  created_by
)
VALUES (%s, %s, %s::date, %s, %s, %s, %s, %s)
RETURNING id;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q_get, (incident_id,))
            existing = cur.fetchone()
            if not existing:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "code": "incident_not_found",
                        "message": "Incident not found",
                        "details": {"id": incident_id},
                    },
                )

            cols = [c.name for c in cur.description]
            row = dict(zip(cols, existing))

            from_status = (row.get("status") or "").strip().upper() or "OPEN"
            if from_status == desired:
                return row

            cur.execute(q_update, (desired, incident_id))
            updated = cur.fetchone()
            cols2 = [c.name for c in cur.description]
            updated_row = dict(zip(cols2, updated))

            cur.execute(
                q_event,
                (
                    incident_id,
                    updated_row["market_id"],
                    updated_row["day"],
                    "STATUS_CHANGE",
                    from_status,
                    desired,
                    (payload.note or "").strip() or None,
                    created_by,
                ),
            )

            conn.commit()
            return updated_row

# -----------------------------
# Intervention effectiveness
# Collapsed by (market_id, applied_day, action_code)
# before = day before applied_day
# after  = applied_day
# Adds: action_count, delta_score, roi_score
# -----------------------------
@app.get("/ops/markets/{market_id}/interventions/effectiveness")
def interventions_effectiveness(market_id: str, days: int = 60):
    days = max(1, min(int(days), 180))

    q = """
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(COALESCE(applied_at::date, day)) FROM market_interventions WHERE market_id = %s),
    (SELECT MAX(day) FROM market_incidents WHERE market_id = %s),
    (SELECT MAX(day) FROM market_metrics_daily WHERE market_id = %s),
    (SELECT MAX(day) FROM marts.market_day WHERE market_id = %s)
  ) AS latest_day
),
itv_raw AS (
  SELECT
    id,
    market_id,
    incident_id,
    day,
    action_code,
    title,
    status,
    params,
    created_by,
    created_at,
    applied_at,
    COALESCE(applied_at::date, day) AS applied_day
  FROM market_interventions
  WHERE market_id = %s
    AND status = 'APPLIED'
    AND (
      (SELECT latest_day FROM latest) IS NULL
      OR COALESCE(applied_at::date, day) >= (SELECT latest_day FROM latest) - %s
    )
),
itv_grp AS (
  SELECT
    market_id,
    applied_day,
    action_code,

    COUNT(*)::int AS action_count,

    MIN(created_at) AS first_created_at,
    MAX(created_at) AS last_created_at,

    (ARRAY_AGG(id ORDER BY created_at DESC))[1] AS id,
    (ARRAY_AGG(incident_id ORDER BY created_at DESC))[1] AS incident_id,
    (ARRAY_AGG(day ORDER BY created_at DESC))[1] AS day,
    (ARRAY_AGG(title ORDER BY created_at DESC))[1] AS title,
    (ARRAY_AGG(status ORDER BY created_at DESC))[1] AS status,
    (ARRAY_AGG(params ORDER BY created_at DESC))[1] AS params,
    (ARRAY_AGG(created_by ORDER BY created_at DESC))[1] AS created_by,
    (ARRAY_AGG(created_at ORDER BY created_at DESC))[1] AS created_at,
    (ARRAY_AGG(applied_at ORDER BY created_at DESC))[1] AS applied_at
  FROM itv_raw
  GROUP BY market_id, applied_day, action_code
),
base AS (
  SELECT
    g.*,
    (g.applied_day - INTERVAL '1 day')::date AS before_day,
    g.applied_day AS after_day
  FROM itv_grp g
),
joined AS (
  SELECT
    b.*,

    mb.trades AS b_trades,
    mb.volume AS b_volume,
    mb.risk_score AS b_risk,
    mb.health_score AS b_health,
    mb.spread_median AS b_spread,
    mb.unique_traders AS b_unique,
    mb.concentration_hhi AS b_hhi,
    mb.depth_2pct_median AS b_depth,

    ma.trades AS a_trades,
    ma.volume AS a_volume,
    ma.risk_score AS a_risk,
    ma.health_score AS a_health,
    ma.spread_median AS a_spread,
    ma.unique_traders AS a_unique,
    ma.concentration_hhi AS a_hhi,
    ma.depth_2pct_median AS a_depth

  FROM base b
  LEFT JOIN market_metrics_daily mb
    ON mb.market_id = b.market_id AND mb.day = b.before_day
  LEFT JOIN market_metrics_daily ma
    ON ma.market_id = b.market_id AND ma.day = b.after_day
),
scored AS (
  SELECT
    j.*,

    (j.a_trades - j.b_trades) AS d_trades,
    (j.a_volume - j.b_volume) AS d_volume,
    (j.a_risk - j.b_risk) AS d_risk,
    (j.a_health - j.b_health) AS d_health,
    (j.a_spread - j.b_spread) AS d_spread,
    (j.a_unique - j.b_unique) AS d_unique,
    (j.a_hhi - j.b_hhi) AS d_hhi,
    (j.a_depth - j.b_depth) AS d_depth,

    (
      COALESCE((j.b_risk - j.a_risk), 0) * 1.0 +
      COALESCE((j.a_health - j.b_health), 0) * 1.0 +
      COALESCE((j.b_spread - j.a_spread), 0) * 100.0 +
      COALESCE((j.a_depth - j.b_depth) / 100.0, 0) * 1.0
    ) AS delta_score,

    GREATEST(
      COALESCE((j.params->>'budget')::numeric, 1000 * j.action_count),
      1
    ) AS cost_proxy

  FROM joined j
)
SELECT
  scored.id,
  scored.market_id,
  scored.incident_id,
  scored.day,
  scored.action_code,
  scored.title,
  scored.status,
  scored.params,
  scored.created_by,
  scored.created_at,
  scored.applied_at,

  scored.applied_day,
  scored.action_count,
  scored.first_created_at,
  scored.last_created_at,

  scored.before_day,
  scored.after_day,

  jsonb_build_object(
    'trades', scored.b_trades,
    'volume', scored.b_volume,
    'risk_score', scored.b_risk,
    'health_score', scored.b_health,
    'spread_median', scored.b_spread,
    'unique_traders', scored.b_unique,
    'concentration_hhi', scored.b_hhi,
    'depth_2pct_median', scored.b_depth
  ) AS before,

  jsonb_build_object(
    'trades', scored.a_trades,
    'volume', scored.a_volume,
    'risk_score', scored.a_risk,
    'health_score', scored.a_health,
    'spread_median', scored.a_spread,
    'unique_traders', scored.a_unique,
    'concentration_hhi', scored.a_hhi,
    'depth_2pct_median', scored.a_depth
  ) AS after,

  jsonb_build_object(
    'trades', scored.d_trades,
    'volume', scored.d_volume,
    'risk_score', scored.d_risk,
    'health_score', scored.d_health,
    'spread_median', scored.d_spread,
    'unique_traders', scored.d_unique,
    'concentration_hhi', scored.d_hhi,
    'depth_2pct_median', scored.d_depth
  ) AS delta,

  scored.delta_score,

  CASE
    WHEN scored.cost_proxy IS NULL OR scored.cost_proxy = 0 THEN NULL
    ELSE (scored.delta_score / (scored.cost_proxy / 1000.0))
  END AS roi_score

FROM scored
ORDER BY scored.applied_day DESC, scored.last_created_at DESC;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, market_id, market_id, market_id, days))
            return rows_as_dicts(cur)


# -----------------------------
# Interventions (raw + collapsed for UI)
# -----------------------------
class InterventionCreate(BaseModel):
    day: Optional[str] = None
    incident_id: Optional[int] = None
    action_code: str
    title: str
    status: str = "PLANNED"
    params: dict = {}
    created_by: str = "operator"


@app.get("/ops/markets/{market_id}/interventions")
def list_interventions(market_id: str, days: int = 30):
    days = max(1, min(int(days), 180))
    q = """
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(day) FROM market_interventions WHERE market_id = %s),
    (SELECT MAX(day) FROM market_incidents WHERE market_id = %s),
    (SELECT MAX(day) FROM marts.market_day WHERE market_id = %s)
  ) AS latest_day
)
SELECT
  id,
  market_id,
  incident_id,
  day,
  action_code,
  title,
  status,
  params,
  created_by,
  created_at,
  applied_at
FROM market_interventions
WHERE market_id = %s
  AND (
    (SELECT latest_day FROM latest) IS NULL
    OR day >= (SELECT latest_day FROM latest) - %s
  )
ORDER BY day DESC, created_at DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, market_id, market_id, days))
            return rows_as_dicts(cur)


def list_interventions_collapsed(market_id: str, days: int = 30):
    days = max(1, min(int(days), 180))
    q = """
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(day) FROM market_interventions WHERE market_id = %s),
    (SELECT MAX(day) FROM market_incidents WHERE market_id = %s),
    (SELECT MAX(day) FROM marts.market_day WHERE market_id = %s)
  ) AS latest_day
),
base AS (
  SELECT *
  FROM market_interventions
  WHERE market_id = %s
    AND (
      (SELECT latest_day FROM latest) IS NULL
      OR day >= (SELECT latest_day FROM latest) - %s
    )
)
SELECT
  market_id,
  day,
  action_code,
  COUNT(*)::int AS action_count,
  MIN(created_at) AS first_created_at,
  MAX(created_at) AS last_created_at,

  (ARRAY_AGG(id ORDER BY created_at DESC))[1] AS id,
  (ARRAY_AGG(incident_id ORDER BY created_at DESC))[1] AS incident_id,
  (ARRAY_AGG(title ORDER BY created_at DESC))[1] AS title,
  (ARRAY_AGG(status ORDER BY created_at DESC))[1] AS status,
  (ARRAY_AGG(params ORDER BY created_at DESC))[1] AS params,
  (ARRAY_AGG(created_by ORDER BY created_at DESC))[1] AS created_by,
  (ARRAY_AGG(created_at ORDER BY created_at DESC))[1] AS created_at,
  (ARRAY_AGG(applied_at ORDER BY created_at DESC))[1] AS applied_at
FROM base
GROUP BY market_id, day, action_code
ORDER BY day DESC, last_created_at DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, market_id, market_id, days))
            return rows_as_dicts(cur)


@app.get("/ops/markets/{market_id}/interventions/collapsed")
def interventions_collapsed(market_id: str, days: int = 30):
    return list_interventions_collapsed(market_id, days=days)


# -----------------------------
# Params normalization (prevents band_bps vs spread_bps mismatches)
# -----------------------------
def normalize_params(action_code: str, params: dict | None) -> dict:
    params = params or {}

    if action_code == "LIQUIDITY_BOOST":
        # backwards compat: old UI used band_bps
        if "spread_bps" not in params and "band_bps" in params:
            params["spread_bps"] = params.pop("band_bps")

        # hard defaults so apply/revert always has the full set
        params.setdefault("spread_bps", 10)
        params.setdefault("depth_delta", 500)
        params.setdefault("health_delta", 3)
        params.setdefault("risk_delta", -2)

        # optional but useful for ROI
        if "budget" in params:
            try:
                params["budget"] = float(params["budget"])
            except Exception:
                params["budget"] = 1000.0

    return params


@app.post("/ops/markets/{market_id}/interventions")
def create_intervention(
    market_id: str,
    payload: InterventionCreate,
    operator: AuthUser = Depends(require_operator),
):
    created_by = operator.email

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    status = (payload.status or "PLANNED").strip().upper()
    if status not in {"PLANNED", "APPLIED", "REVERTED", "CANCELLED"}:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_status",
                "message": "Invalid status",
                "details": {"allowed": ["PLANNED", "APPLIED", "REVERTED", "CANCELLED"]},
            },
        )

    norm_params = normalize_params((payload.action_code or "").strip().upper(), payload.params)

    q = """
INSERT INTO market_interventions
  (market_id, incident_id, day, action_code, title, status, params, created_by, applied_at)
VALUES
  (%s, %s, COALESCE(%s::date, CURRENT_DATE), %s, %s, %s, %s::jsonb, %s,
   CASE WHEN %s = 'APPLIED' THEN now() ELSE NULL END)
RETURNING
  id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                (
                    market_id,
                    payload.incident_id,
                    payload.day,
                    (payload.action_code or "").strip().upper(),
                    (payload.title or "").strip(),
                    status,
                    Json(norm_params),
                    created_by,
                    status,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))
        
@app.post("/ops/interventions/{intervention_id}/apply")
def apply_intervention(intervention_id: int, operator: AuthUser = Depends(require_operator)):
    created_by = operator.email
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at
                FROM market_interventions
                WHERE id = %s
                """,
                (intervention_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail={"code": "intervention_not_found", "message": "Intervention not found", "details": {"id": intervention_id}},
                )

            cols = [c.name for c in cur.description]
            itv = dict(zip(cols, row))

            if itv.get("status") == "APPLIED":
                return itv

            market_id = itv["market_id"]
            day = itv["day"]
            action_code = itv["action_code"]
            params = normalize_params(action_code, _parse_params(itv.get("params")))

            cur.execute(
                """
                UPDATE market_interventions
                SET status = 'APPLIED',
                    applied_at = COALESCE(applied_at, now()),
                    created_by = COALESCE(created_by, %s)
                WHERE id = %s
                RETURNING id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at
                """,
                (created_by, intervention_id),
            )
            updated_row = cur.fetchone()
            updated_cols = [c.name for c in cur.description]
            updated = dict(zip(updated_cols, updated_row))

            _ensure_metrics_row(cur, market_id, day)
            _apply_action(cur, action_code, market_id, day, params, direction=+1)

            conn.commit()
            return updated

@app.post("/ops/admin/ingest/polymarket/markets")
def admin_ingest_polymarket_markets(
    limit: int = 200,
    offset: int = 0,
    operator: AuthUser = Depends(require_operator),
):
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    return ingest_polymarket_markets(limit=limit, offset=offset)

from datetime import date as _date
from fastapi import Query
from datetime import datetime, timezone

def _parse_ts(v: str | None) -> datetime | None:
    if not v:
        return None
    s = v.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

@app.post("/ops/admin/ingest/polymarket/bbo_ws_one")
def admin_ingest_polymarket_bbo_ws_one(
    market_id: str = Query(...),
    max_events: int = Query(300, ge=10, le=2000),
    user=Depends(require_operator),
):
    return ingest_polymarket_bbo_ws_for_market(
        market_id=market_id,
        max_events=max_events,
    )

@app.post("/ops/admin/compute/trader_behavior_daily")
def admin_compute_trader_behavior_daily(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=500, ge=1, le=5000),
    market_id: Optional[str] = Query(default=None),
):
    return compute_trader_behavior_daily(
        day=day,
        limit_markets=limit_markets,
        market_id=market_id,
    )


@app.post("/ops/admin/compute/trader_role_daily")
def admin_compute_trader_role_daily(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=500, ge=1, le=5000),
    market_id: Optional[str] = Query(default=None),
):
    return compute_trader_role_daily(
        day=day,
        limit_markets=limit_markets,
        market_id=market_id,
    )


@app.post("/ops/admin/compute/microstructure")
def admin_compute_microstructure(
    day: Optional[_date] = None,
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(500, ge=1, le=5000),
):
    return compute_microstructure_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)

@app.post("/ops/admin/compute/traders_daily")
def admin_compute_traders_daily(
    day: Optional[date] = Query(default=None),
    window_hours: int = Query(default=24, ge=1, le=168),
):
    return compute_trader_daily_stats(day=day, window_hours=window_hours)


@app.post("/ops/admin/compute/trader_labels_daily")
def admin_compute_trader_labels_daily(
    day: Optional[date] = Query(default=None),
    whale_volume_threshold: float = Query(default=1000.0, ge=0),
    farmer_markets_threshold: int = Query(default=10, ge=1),
):
    return compute_trader_labels_daily(
        day=day,
        whale_volume_threshold=whale_volume_threshold,
        farmer_markets_threshold=farmer_markets_threshold,
    )

@app.post("/ops/admin/compute/market_risk_radar")
def admin_compute_market_risk_radar_daily(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=500, ge=1, le=5000),
    operator: AuthUser = Depends(require_operator),
):
    return compute_market_risk_radar_daily(
        day=day,
        limit_markets=limit_markets,
    )


@app.post("/ops/admin/compute/market_integrity")
def admin_compute_market_integrity_daily(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=500, ge=1, le=5000),
    operator: AuthUser = Depends(require_operator),
):
    return compute_market_integrity_daily(
        day=day,
        limit_markets=limit_markets,
    )

@app.post("/ops/admin/compute/market_manipulation")
def admin_compute_market_manipulation(
    day: Optional[date] = None,
    limit_markets: int = 500,
):
    return compute_market_manipulation_daily(day=day, limit_markets=limit_markets)

@app.post("/ops/admin/compute/market_launch_intelligence")
def admin_compute_market_launch_intelligence(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=500, ge=1, le=5000),
):
    return compute_market_launch_intelligence_daily(
        day=day,
        limit_markets=limit_markets,
    )

@app.post("/ops/admin/compute/market_social_intelligence")
def admin_compute_market_social_intelligence(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=500, ge=1, le=5000),
):
    return compute_market_social_intelligence_daily(
        day=day,
        limit_markets=limit_markets,
    )

@app.post("/ops/admin/compute/market_regime_v2")
def admin_compute_market_regime_v2(
    day: Optional[date] = Query(default=None),
    limit_markets: int = Query(default=5000, ge=1, le=20000),
    user: AuthUser = Depends(require_operator),
):
    try:
        result = compute_market_regime_daily_v2(
            day=day,
            limit_markets=limit_markets,
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "internal_error",
                    "message": "Internal server error",
                    "details": {
                        "type": type(e).__name__,
                        "message": str(e),
                    },
                }
            },
        )


@app.post("/ops/admin/ingest/polymarket/metrics_daily")
def admin_ingest_polymarket_metrics_daily(
    limit: int = Query(200, ge=1, le=500),
    user=Depends(require_operator),
):
    return ingest_polymarket_metrics_daily(limit=limit)

@app.post("/ops/admin/ingest/polymarket/trades_rest")
def admin_ingest_polymarket_trades_rest(
    lookback_hours: int = Query(72, ge=1, le=720),
    use_cursor: bool = Query(True),
    operator: AuthUser = Depends(require_operator),
):
    return ingest_polymarket_trades_rest_job(
        lookback_hours=lookback_hours,
        use_cursor=use_cursor,
    )

@app.post("/ops/admin/ingest/polymarket/trades_rest_one")
def admin_ingest_polymarket_trades_rest_one(
    market_id: str,
    lookback_hours: int = Query(240, ge=1, le=720),
    operator: AuthUser = Depends(require_operator),
):
    return ingest_polymarket_trades_rest_for_market_job(
        market_id=market_id,
        lookback_hours=lookback_hours,
    )

@app.post("/ops/admin/ingest/polymarket/trades_ws")
def admin_ingest_polymarket_trades_ws(
    limit_markets: int = Query(50, ge=1, le=200),
    max_events: int = Query(300, ge=10, le=2000),
    user=Depends(require_operator),
):
    return ingest_polymarket_trades_ws(
        limit_markets=limit_markets,
        max_events=max_events,
    )

@app.post("/ops/admin/ingest/polymarket/trades_rest_one")
def admin_ingest_polymarket_trades_rest_one(
    market_id: str,
    lookback_hours: int = Query(240, ge=1, le=720),
    operator: AuthUser = Depends(require_operator),
):
    return ingest_polymarket_trades_rest_for_market(
        market_id=market_id,
        lookback_hours=lookback_hours,
    )

@app.post("/ops/interventions/{intervention_id}/revert")
def revert_intervention(intervention_id: int, operator: AuthUser = Depends(require_operator)):
    created_by = operator.email
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, market_id, day, action_code, status, params
                FROM market_interventions
                WHERE id = %s
                """,
                (intervention_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail={"code": "intervention_not_found", "message": "Intervention not found", "details": {"id": intervention_id}},
                )

            cols = [c.name for c in cur.description]
            itv = dict(zip(cols, row))

            if itv.get("status") != "APPLIED":
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "intervention_not_applied",
                        "message": "Intervention must be APPLIED to revert",
                        "details": {"status": itv.get("status")},
                    },
                )

            market_id = itv["market_id"]
            day = itv["day"]
            action_code = itv["action_code"]
            params = normalize_params(action_code, _parse_params(itv.get("params")))

            _ensure_metrics_row(cur, market_id, day)
            _apply_action(cur, action_code, market_id, day, params, direction=-1)

            cur.execute(
                """
                UPDATE market_interventions
                SET status = 'REVERTED'
                WHERE id = %s
                RETURNING id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at
                """,
                (intervention_id,),
            )
            updated_row = cur.fetchone()
            updated_cols = [c.name for c in cur.description]
            updated = dict(zip(updated_cols, updated_row))

            conn.commit()
            return updated


@app.post("/ops/interventions/{intervention_id}/cancel")
def cancel_intervention(intervention_id: int, operator: AuthUser = Depends(require_operator)):
    created_by = operator.email
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, status
                FROM market_interventions
                WHERE id = %s
                """,
                (intervention_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail={"code": "intervention_not_found", "message": "Intervention not found", "details": {"id": intervention_id}},
                )

            cols = [c.name for c in cur.description]
            itv = dict(zip(cols, row))
            status = (itv.get("status") or "").upper()

            if status == "CANCELLED":
                cur.execute(
                    """
                    SELECT id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at
                    FROM market_interventions
                    WHERE id = %s
                    """,
                    (intervention_id,),
                )
                r2 = cur.fetchone()
                c2 = [c.name for c in cur.description]
                return dict(zip(c2, r2))

            if status == "APPLIED":
                raise HTTPException(
                    status_code=400,
                    detail={"code": "intervention_already_applied", "message": "Intervention is APPLIED. Use /revert instead of /cancel", "details": {"id": intervention_id, "status": status}},
                )

            if status in {"REVERTED"}:
                raise HTTPException(
                    status_code=400,
                    detail={"code": "intervention_not_cancellable", "message": "Intervention cannot be cancelled in its current state", "details": {"id": intervention_id, "status": status}},
                )

            if status != "PLANNED":
                raise HTTPException(
                    status_code=400,
                    detail={"code": "invalid_status_transition", "message": "Only PLANNED interventions can be cancelled", "details": {"id": intervention_id, "status": status, "allowed_from": ["PLANNED"]}},
                )

            cur.execute(
                """
                UPDATE market_interventions
                SET status = 'CANCELLED'
                WHERE id = %s
                RETURNING id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at
                """,
                (intervention_id,),
            )
            updated_row = cur.fetchone()
            updated_cols = [c.name for c in cur.description]
            updated = dict(zip(updated_cols, updated_row))

            conn.commit()
            return updated

# -----------------------------
# Intervention cumulative impact (last N days)
# -----------------------------
@app.get("/ops/markets/{market_id}/interventions/cumulative")
def interventions_cumulative(market_id: str, days: int = 30):
    days = max(1, min(int(days), 180))

    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM marts.market_day
  WHERE market_id = %s
),
itv AS (
  SELECT
    id,
    market_id,
    action_code,
    status,
    created_at,
    applied_at,
    COALESCE(applied_at::date, day) AS applied_day
  FROM market_interventions
  WHERE market_id = %s
    AND status = 'APPLIED'
    AND COALESCE(applied_at::date, day) >= (SELECT latest_day FROM latest) - %s
),
base AS (
  SELECT
    itv.*,
    (itv.applied_day - INTERVAL '1 day')::date AS before_day,
    itv.applied_day AS after_day
  FROM itv
),
joined AS (
  SELECT
    base.id,
    base.action_code,
    mb.risk_score AS b_risk,
    ma.risk_score AS a_risk,
    mb.health_score AS b_health,
    ma.health_score AS a_health,
    mb.spread_median AS b_spread,
    ma.spread_median AS a_spread,
    mb.depth_2pct_median AS b_depth,
    ma.depth_2pct_median AS a_depth
  FROM base
  LEFT JOIN marts.market_day mb
    ON mb.market_id = base.market_id AND mb.day = base.before_day
  LEFT JOIN marts.market_day ma
    ON ma.market_id = base.market_id AND ma.day = base.after_day
)
SELECT
  COUNT(*)::int AS count_total,
  COUNT(*) FILTER (WHERE b_risk IS NOT NULL AND a_risk IS NOT NULL)::int AS count_effective,

  COALESCE(SUM(a_risk - b_risk), 0) AS risk_score,
  COALESCE(SUM(a_health - b_health), 0) AS health_score,
  COALESCE(SUM(a_spread - b_spread), 0) AS spread_median,
  COALESCE(SUM(a_depth - b_depth), 0) AS depth_2pct_median
FROM joined;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            row = cur.fetchone()
            cols = [c.name for c in cur.description]
            out = dict(zip(cols, row))

            def _to_float_local(x):
                try:
                    return float(x)
                except Exception:
                    return 0.0

            return {
                "days": days,
                "count_total": int(out.get("count_total") or 0),
                "count_effective": int(out.get("count_effective") or 0),
                "risk_score": _to_float_local(out.get("risk_score")),
                "health_score": _to_float_local(out.get("health_score")),
                "spread_median": _to_float_local(out.get("spread_median")),
                "depth_2pct_median": _to_float_local(out.get("depth_2pct_median")),
            }
        
# -----------------------------
# Manual overrides
# -----------------------------
class ManualOverrideCreate(BaseModel):
    day: str
    risk_score_override: Optional[int] = None
    health_score_override: Optional[int] = None
    note: Optional[str] = None
    created_by: str = "operator"


@app.get("/ops/markets/{market_id}/overrides")
def list_overrides(market_id: str, days: int = 30):
    days = max(1, min(int(days), 180))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM marts.market_day
  WHERE market_id = %s
)
SELECT
  market_id,
  day,
  risk_score_override,
  health_score_override,
  note,
  created_by,
  created_at
FROM market_manual_overrides
WHERE market_id = %s
  AND day >= (SELECT latest_day FROM latest) - %s
ORDER BY day DESC, created_at DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)


@app.post("/ops/markets/{market_id}/overrides")
def upsert_override(market_id: str, payload: ManualOverrideCreate, operator: AuthUser = Depends(require_operator)):
    created_by = operator.email
    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    q = """
INSERT INTO market_manual_overrides
  (market_id, day, risk_score_override, health_score_override, note, created_by)
VALUES
  (%s, %s::date, %s, %s, COALESCE(%s, ''), %s)
ON CONFLICT (market_id, day)
DO UPDATE SET
  risk_score_override = EXCLUDED.risk_score_override,
  health_score_override = EXCLUDED.health_score_override,
  note = EXCLUDED.note,
  created_by = EXCLUDED.created_by,
  created_at = now()
RETURNING
  market_id, day, risk_score_override, health_score_override, note, created_by, created_at;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                (market_id, payload.day, payload.risk_score_override, payload.health_score_override, payload.note, created_by),
            )
            row = cur.fetchone()
            conn.commit()
            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))