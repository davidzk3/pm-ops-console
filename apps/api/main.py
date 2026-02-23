from fastapi import FastAPI, HTTPException, Depends, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg.types.json import Json

import os
import time
import json
import psycopg
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from .settings import CORS_ORIGINS
from .auth import require_write_key

app = FastAPI()

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
# DB config
# -----------------------------
def get_db_dsn() -> str:
    return os.getenv("DATABASE_URL", "postgresql://pmops:pmops@localhost:5432/pmops")

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

@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    print(f"[unhandled] {type(exc).__name__}: {exc}")
    return error_response(
        code="internal_error",
        message="Internal server error",
        status_code=500,
        details={},
    )

# -----------------------------
# DB helpers
# -----------------------------
def rows_as_dicts(cur):
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def market_exists(market_id: str) -> bool:
    q = "SELECT 1 FROM markets WHERE market_id = %s LIMIT 1;"
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            return cur.fetchone() is not None

def _ensure_metrics_row(cur, market_id: str, day):
    """
    Ensure market_metrics_daily has (market_id, day). If missing, clone the latest day.
    Assumes market_metrics_daily has a UNIQUE constraint on (market_id, day).
    """
    cur.execute(
        """
        INSERT INTO market_metrics_daily (
            market_id, day, volume, trades, unique_traders,
            spread_median, depth_2pct_median, concentration_hhi,
            health_score, risk_score
        )
        SELECT
            market_id,
            %s AS day,
            volume, trades, unique_traders,
            spread_median, depth_2pct_median, concentration_hhi,
            health_score, risk_score
        FROM market_metrics_daily
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

        # spread improves when it goes DOWN. We store spread_median as decimal.
        spread_delta = -(spread_bps / 10000.0)

        cur.execute(
            """
            UPDATE market_metrics_daily
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
    md.risk_score
  FROM market_metrics_daily md
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
    COALESCE(mo.risk_score_override, l.risk_score) AS risk_score,
    (mo.id IS NOT NULL) AS has_manual_override
  FROM markets m
  JOIN latest_md l
    ON l.market_id = m.market_id
  LEFT JOIN market_manual_overrides mo
    ON mo.market_id = l.market_id AND mo.day = l.day
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
  COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'flag_code', f.flag_code,
        'severity', f.severity,
        'details', f.details
      )
    ) FILTER (WHERE f.flag_code IS NOT NULL),
    '[]'::jsonb
  ) AS flags
FROM base b
LEFT JOIN market_flags_daily f
  ON f.market_id = b.market_id AND f.day = b.day
GROUP BY
  b.market_id, b.protocol, b.chain, b.title, b.category,
  b.day, b.volume, b.trades, b.unique_traders,
  b.spread_median, b.depth_2pct_median, b.concentration_hhi,
  b.health_score, b.risk_score, b.has_manual_override
ORDER BY b.risk_score DESC NULLS LAST;
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
  FROM market_metrics_daily
  WHERE market_id = %s
),
w AS (
  SELECT *
  FROM trades
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
)
SELECT
  trader_id,
  COUNT(DISTINCT day)::int AS days_active,
  COUNT(*)::int AS trades,
  COALESCE(SUM(notional), 0)::float AS notional_total,
  COALESCE(SUM(CASE WHEN UPPER(side)='BUY' THEN notional ELSE 0 END), 0)::float AS notional_buy,
  COALESCE(SUM(CASE WHEN UPPER(side)='SELL' THEN notional ELSE 0 END), 0)::float AS notional_sell,
  (COALESCE(SUM(notional), 0) / NULLIF(COUNT(*), 0))::float AS avg_trade_size,
  MIN(ts)::text AS first_ts,
  MAX(ts)::text AS last_ts
FROM w
GROUP BY trader_id
ORDER BY notional_total DESC
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
def market_trader_cohorts_summary(market_id: str, days: int = 30):
    days = max(1, min(int(days), 365))

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
),
w AS (
  SELECT
    ucd.market_id,
    ucd.day,
    ucd.trader_id,
    ucd.cohort
  FROM user_cohorts_daily ucd
  WHERE ucd.market_id = %s
    AND ucd.day >= (SELECT latest_day FROM latest) - %s
),
agg AS (
  SELECT
    w.cohort,
    COUNT(DISTINCT w.trader_id)::int AS traders
  FROM w
  GROUP BY w.cohort
),
um AS (
  SELECT
    market_id,
    trader_id,
    SUM(trades_count)::int AS trades,
    COALESCE(SUM(volume_notional), 0)::float AS notional_total,
    (COALESCE(SUM(volume_notional), 0) / NULLIF(SUM(trades_count), 0))::float AS avg_trade_size
  FROM user_market_daily
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
  GROUP BY market_id, trader_id
),
joined AS (
  SELECT
    w.cohort,
    COALESCE(SUM(um.trades), 0)::int AS trades,
    COALESCE(SUM(um.notional_total), 0)::float AS notional_total,
    (COALESCE(SUM(um.notional_total), 0) / NULLIF(COALESCE(SUM(um.trades), 0), 0))::float AS avg_trade_size
  FROM (SELECT DISTINCT cohort, trader_id FROM w) w
  LEFT JOIN um
    ON um.trader_id = w.trader_id
  GROUP BY w.cohort
),
days_covered AS (
  SELECT COUNT(DISTINCT day)::int AS days_covered
  FROM w
)
SELECT
  a.cohort,
  a.traders,
  j.trades,
  j.notional_total,
  j.avg_trade_size,
  (SELECT days_covered FROM days_covered) AS days_covered
FROM agg a
LEFT JOIN joined j
  ON j.cohort = a.cohort
ORDER BY j.notional_total DESC NULLS LAST, a.traders DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days, market_id, days))
            return rows_as_dicts(cur)

class TraderIntelligenceRow(BaseModel):
    trader_id: str
    days_active: int
    trades: int
    notional_total: float
    avg_trade_size: float
    buy_ratio: float
    cohort: str
    role_tag: str
    flags: Dict[str, Any] = {}

@app.get("/ops/markets/{market_id}/traders/intelligence", response_model=List[TraderIntelligenceRow])
def market_trader_intelligence(market_id: str, days: int = 30, top_n: int = 50):
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
  FROM market_metrics_daily
  WHERE market_id = %s
),
um AS (
  SELECT
    trader_id,
    COUNT(DISTINCT day)::int AS days_active,
    SUM(trades_count)::int AS trades,
    COALESCE(SUM(volume_notional), 0)::float AS notional_total,
    (COALESCE(SUM(volume_notional), 0) / NULLIF(SUM(trades_count), 0))::float AS avg_trade_size,
    COALESCE(SUM(buy_count), 0)::int AS buy_count,
    COALESCE(SUM(sell_count), 0)::int AS sell_count
  FROM user_market_daily
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
  GROUP BY trader_id
),
last_cohort AS (
  SELECT DISTINCT ON (trader_id)
    trader_id,
    cohort
  FROM user_cohorts_daily
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
  ORDER BY trader_id, day DESC
),
base AS (
  SELECT
    um.*,
    COALESCE(lc.cohort, 'UNKNOWN') AS cohort,
    CASE
      WHEN (um.buy_count + um.sell_count) > 0
        THEN (um.buy_count::float / (um.buy_count + um.sell_count))
      ELSE 0.5
    END AS buy_ratio
  FROM um
  LEFT JOIN last_cohort lc
    ON lc.trader_id = um.trader_id
),
ranked AS (
  SELECT *
  FROM base
  ORDER BY notional_total DESC
  LIMIT %s
)
SELECT
  trader_id,
  days_active,
  trades,
  notional_total,
  avg_trade_size,
  buy_ratio,
  cohort
FROM ranked;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days, market_id, days, top_n))
            rows = rows_as_dicts(cur)

    out: List[dict] = []
    for r in rows:
        trades = int(r.get("trades") or 0)
        days_active = int(r.get("days_active") or 0)
        avg_size = float(r.get("avg_trade_size") or 0.0)
        buy_ratio = float(r.get("buy_ratio") or 0.5)
        cohort = (r.get("cohort") or "UNKNOWN").upper()

        imbalance = abs(buy_ratio - 0.5)
        high_freq = trades >= 200
        balanced_flow = imbalance <= 0.12
        very_small = avg_size <= 2.0
        very_large = avg_size >= 25.0

        role_tag = "RETAIL"
        flags: Dict[str, Any] = {
            "balanced_flow": balanced_flow,
            "imbalance": round(imbalance, 4),
        }

        if cohort == "FARMER" or (high_freq and very_small):
            role_tag = "INCENTIVE_FARMER"
            flags["reason"] = "high trade count with small average size"
        elif cohort == "WHALE" or very_large:
            role_tag = "WHALE"
            flags["reason"] = "large average size or whale cohort"
        elif days_active >= 10 and trades >= 80 and balanced_flow:
            role_tag = "MAKER_LIKE"
            flags["reason"] = "sustained activity with balanced buy sell flow"
        elif trades >= 40 and imbalance >= 0.25:
            role_tag = "DIRECTIONAL"
            flags["reason"] = "persistent buy sell imbalance"
        elif trades >= 60:
            role_tag = "ACTIVE_RETAIL"
            flags["reason"] = "high activity without maker like balance"
        else:
            role_tag = "RETAIL"

        out.append(
            {
                "trader_id": r.get("trader_id"),
                "days_active": days_active,
                "trades": trades,
                "notional_total": float(r.get("notional_total") or 0.0),
                "avg_trade_size": avg_size,
                "buy_ratio": buy_ratio,
                "cohort": cohort,
                "role_tag": role_tag,
                "flags": flags,
            }
        )

    return out

# -----------------------------
# Market Quality & Cohort Impact (Institutional View)
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
        SELECT
            c.cohort,
            SUM(umd.volume_notional) AS notional_total,
            SUM(umd.trades_count) AS trades
        FROM user_market_daily umd
        JOIN user_cohorts_daily c
          ON c.market_id = umd.market_id
         AND c.trader_id = umd.trader_id
         AND c.day = umd.day
        WHERE umd.market_id = %s
          AND umd.day BETWEEN %s AND %s
        GROUP BY c.cohort
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

def _compute_cohort_risk_flags(
    recent_cohort_share: List[Dict[str, float]],
    cohort_share_delta: List[Dict[str, float]] | None = None,
) -> List[str]:
    flags: List[str] = []

    recent_map = {c["cohort"].upper(): c for c in (recent_cohort_share or [])}
    delta_map = {c["cohort"].upper(): c for c in (cohort_share_delta or [])}

    whale_notional = _to_float(recent_map.get("WHALE", {}).get("notional_share"))
    casual_trade = _to_float(recent_map.get("CASUAL", {}).get("trade_share"))
    active_trade = _to_float(recent_map.get("ACTIVE", {}).get("trade_share"))

    active_notional_delta = _to_float(delta_map.get("ACTIVE", {}).get("notional_share_delta"))
    active_trade_delta = _to_float(delta_map.get("ACTIVE", {}).get("trade_share_delta"))

    if whale_notional >= 0.35:
        flags.append("WHALE_DOMINANCE_RISK")

    if casual_trade >= 0.85:
        flags.append("LOW_CONVICTION_FLOW")

    if active_trade < 0.12 and whale_notional > 0.25:
        flags.append("THIN_MIDDLE_LAYER")

    if active_notional_delta <= -0.03 or active_trade_delta <= -0.015:
        flags.append("MIDDLE_LAYER_EROSION")

    if whale_notional < 0.10 and casual_trade > 0.75:
        flags.append("NO_DEEP_LIQUIDITY_LAYER")

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
    """
    FIXED:
      - The entire function body was accidentally indented under the 404 block before.
        That caused the handler to return None for valid markets -> ResponseValidationError.
      - This version always returns a dict that matches MarketImpactResponse OR raises HTTPException.
    """
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
            # -----------------------------------
            # Anchor to latest metrics day
            # -----------------------------------
            cur.execute(
                """
                SELECT MAX(day)
                FROM market_metrics_daily
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

            # -----------------------------------
            # Recent window metrics
            # -----------------------------------
            cur.execute(
                """
                SELECT
                    AVG(spread_median),
                    AVG(depth_2pct_median),
                    AVG(concentration_hhi),
                    AVG(unique_traders),
                    AVG(health_score)
                FROM market_metrics_daily
                WHERE market_id = %s
                  AND day BETWEEN %s AND %s
                """,
                (market_id, recent_start, max_day),
            )
            recent = cur.fetchone() or (None, None, None, None, None)

            # -----------------------------------
            # Prior window metrics
            # -----------------------------------
            cur.execute(
                """
                SELECT
                    AVG(spread_median),
                    AVG(depth_2pct_median),
                    AVG(concentration_hhi),
                    AVG(unique_traders),
                    AVG(health_score)
                FROM market_metrics_daily
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

            # -----------------------------------
            # Cohort share (recent + prior) and delta
            # -----------------------------------
            recent_cohort_share = _compute_cohort_share(cur, market_id, recent_start, max_day)
            prior_cohort_share = _compute_cohort_share(cur, market_id, prior_start, prior_end)
            cohort_share_delta = _compute_cohort_share_delta(recent_cohort_share, prior_cohort_share)

            # -----------------------------------
            # Institutional Diagnosis
            # -----------------------------------
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

# -----------------------------
# Single market (latest view)
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
    md.risk_score
  FROM market_metrics_daily md
  WHERE md.market_id = %s
  ORDER BY md.day DESC
  LIMIT 1
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
    COALESCE(mo.risk_score_override, l.risk_score) AS risk_score,
    (mo.id IS NOT NULL) AS has_manual_override
  FROM markets m
  JOIN latest_md l
    ON l.market_id = m.market_id
  LEFT JOIN market_manual_overrides mo
    ON mo.market_id = l.market_id AND mo.day = l.day
  WHERE m.market_id = %s
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
  COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'flag_code', f.flag_code,
        'severity', f.severity,
        'details', f.details
      )
    ) FILTER (WHERE f.flag_code IS NOT NULL),
    '[]'::jsonb
  ) AS flags
FROM base b
LEFT JOIN market_flags_daily f
  ON f.market_id = b.market_id AND f.day = b.day
GROUP BY
  b.market_id, b.protocol, b.chain, b.title, b.category,
  b.day, b.volume, b.trades, b.unique_traders,
  b.spread_median, b.depth_2pct_median, b.concentration_hhi,
  b.health_score, b.risk_score, b.has_manual_override
LIMIT 1;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id))
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT 1 FROM markets WHERE market_id = %s LIMIT 1;", (market_id,))
                exists = cur.fetchone()
                if exists:
                    raise HTTPException(
                        status_code=404,
                        detail={"code": "no_metrics", "message": "Market has no metrics yet", "details": {"market_id": market_id}},
                    )
                raise HTTPException(
                    status_code=404,
                    detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
                )

            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))

# -----------------------------
# Incident effectiveness
# Option B: before = day before incident day
# after = incident day + after_days, capped at latest_day
# -----------------------------
@app.get("/ops/markets/{market_id}/incidents/effectiveness")
def incident_effectiveness(market_id: str, days: int = 30, after_days: int = 3):
    days = max(1, min(int(days), 90))
    after_days = max(0, min(int(after_days), 30))

    q = """
WITH latest AS (
  SELECT market_id, MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
  GROUP BY market_id
),
inc AS (
  SELECT id, market_id, day, status, note, created_by, created_at
  FROM market_incidents
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
),
inc2 AS (
  SELECT
    inc.*,
    l.latest_day,
    (inc.day - INTERVAL '1 day')::date AS before_day,
    LEAST(
      l.latest_day,
      (inc.day + (%s * INTERVAL '1 day'))::date
    ) AS after_day
  FROM inc
  JOIN latest l ON l.market_id = inc.market_id
)
SELECT
  inc2.id,
  inc2.market_id,
  inc2.day,
  inc2.status,
  inc2.note,
  inc2.created_by,
  inc2.created_at,
  inc2.before_day,
  inc2.after_day,

  jsonb_build_object(
    'trades', b.trades,
    'volume', b.volume,
    'risk_score', b.risk_score,
    'health_score', b.health_score,
    'spread_median', b.spread_median,
    'unique_traders', b.unique_traders,
    'concentration_hhi', b.concentration_hhi,
    'depth_2pct_median', b.depth_2pct_median
  ) AS before,

  jsonb_build_object(
    'trades', a.trades,
    'volume', a.volume,
    'risk_score', a.risk_score,
    'health_score', a.health_score,
    'spread_median', a.spread_median,
    'unique_traders', a.unique_traders,
    'concentration_hhi', a.concentration_hhi,
    'depth_2pct_median', a.depth_2pct_median
  ) AS after,

  jsonb_build_object(
    'trades', (a.trades - b.trades),
    'volume', (a.volume - b.volume),
    'risk_score', (a.risk_score - b.risk_score),
    'health_score', (a.health_score - b.health_score),
    'spread_median', (a.spread_median - b.spread_median),
    'unique_traders', (a.unique_traders - b.unique_traders),
    'concentration_hhi', (a.concentration_hhi - b.concentration_hhi),
    'depth_2pct_median', (a.depth_2pct_median - b.depth_2pct_median)
  ) AS delta

FROM inc2
LEFT JOIN market_metrics_daily b
  ON b.market_id = inc2.market_id AND b.day = inc2.before_day
LEFT JOIN market_metrics_daily a
  ON a.market_id = inc2.market_id AND a.day = inc2.after_day
ORDER BY inc2.day DESC, inc2.created_at DESC;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days, after_days))
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
    """
    One payload for the market page.
    Guarded: if one sub query fails, snapshot still returns with errors[] populated.
    """

    timeline_days = max(1, min(int(timeline_days), 60))
    lookback_days = max(1, min(int(lookback_days), 180))
    impact_days = max(4, min(int(impact_days), 60))

    # Effectiveness windows (UI contract)
    INCIDENT_EFFECT_DAYS = 30
    INCIDENT_AFTER_DAYS = 3

    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
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

    # IMPORTANT: snapshot must use after_days for incident effectiveness
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

    # -----------------------------
    # UI meta for delta heat display
    # Frontend uses this to color cells consistently.
    # -----------------------------
    interventions_effectiveness_ui = {
        "heat": {
            # good when value goes up
            "good_up": ["health_score", "depth_2pct_median", "unique_traders", "volume", "trades"],
            # good when value goes down
            "good_down": ["risk_score", "spread_median", "concentration_hhi"],
            # suggested step sizes for intensity bucketing
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
            # suggested decimals for display
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

    return {
        "market": market or {},
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
            "summary": traders_summary or [],
            "cohorts_summary": cohorts_summary or [],
            "intelligence": trader_intelligence or [],
        },
        "impact": impact or {},
        "errors": errors,
    }

# -----------------------------
# Timeline (anchored to latest day)
# -----------------------------
@app.get("/ops/markets/{market_id}/timeline")
def market_timeline(market_id: str, days: int = 14):
    days = max(1, min(int(days), 60))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
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
FROM market_metrics_daily
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

class SeedTradesRequest(BaseModel):
    days: int = 30
    markets: int = 5
    traders: int = 80
    trades_per_day_min: int = 200
    trades_per_day_max: int = 600
    seed: int = 42

@app.post("/dev/seed_trades")
def dev_seed_trades(payload: SeedTradesRequest, operator: str = Depends(require_write_key)):
    days = max(1, min(int(payload.days), 90))
    markets_n = max(1, min(int(payload.markets), 25))
    traders_n = max(5, min(int(payload.traders), 500))
    tmin = max(10, min(int(payload.trades_per_day_min), 5000))
    tmax = max(tmin, min(int(payload.trades_per_day_max), 10000))

    rng = random.Random(int(payload.seed))

    q_markets = """
SELECT DISTINCT market_id
FROM market_metrics_daily
ORDER BY market_id
LIMIT %s;
"""

    q_latest = """
SELECT MAX(day) AS latest_day
FROM market_metrics_daily;
"""

    q_insert = """
INSERT INTO trades (market_id, day, ts, trader_id, side, price, size, notional, source)
VALUES (%s, %s::date, %s::timestamptz, %s, %s, %s, %s, %s, 'seed');
"""

    q_clear_window = """
DELETE FROM trades
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
FROM trades
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
                raise HTTPException(status_code=400, detail={"code": "no_market_days", "message": "market_metrics_daily has no rows"})

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

                        cur.execute(q_insert, (mid, day, ts, trader, side, price, size, notional))
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
    days = max(1, min(int(days), 60))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
)
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
  AND day >= (SELECT latest_day FROM latest) - %s
ORDER BY day DESC, created_at DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)

@app.post("/ops/markets/{market_id}/incidents")
def create_incident(market_id: str, payload: IncidentCreate, operator: str = Depends(require_write_key)):
    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    status = (payload.status or "OPEN").strip().upper()
    if status not in {"OPEN", "MONITOR", "RESOLVED"}:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_status", "message": "Invalid status", "details": {"allowed": ["OPEN", "MONITOR", "RESOLVED"]}},
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
            cur.execute(q_incident, (market_id, payload.day, status, payload.note, operator))
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
                    operator,
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
    days = max(1, min(int(days), 120))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
)
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
  AND day >= (SELECT latest_day FROM latest) - %s
ORDER BY created_at DESC, id DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)

@app.post("/ops/incidents/{incident_id}/status")
def update_incident_status(
    incident_id: int,
    payload: IncidentStatusUpdate,
    operator: str = Depends(require_write_key),
):
    desired = (payload.status or "").strip().upper()
    if desired not in {"OPEN", "MONITOR", "RESOLVED"}:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_status", "message": "Invalid status", "details": {"allowed": ["OPEN", "MONITOR", "RESOLVED"]}},
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
                    detail={"code": "incident_not_found", "message": "Incident not found", "details": {"id": incident_id}},
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
                    operator,
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
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
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
    AND COALESCE(applied_at::date, day) >= (SELECT latest_day FROM latest) - %s
),
itv_grp AS (
  SELECT
    market_id,
    applied_day,
    action_code,

    COUNT(*)::int AS action_count,

    MIN(created_at) AS first_created_at,
    MAX(created_at) AS last_created_at,

    -- keep representative fields from latest row
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

    -- deltas
    (j.a_trades - j.b_trades) AS d_trades,
    (j.a_volume - j.b_volume) AS d_volume,
    (j.a_risk - j.b_risk) AS d_risk,
    (j.a_health - j.b_health) AS d_health,
    (j.a_spread - j.b_spread) AS d_spread,
    (j.a_unique - j.b_unique) AS d_unique,
    (j.a_hhi - j.b_hhi) AS d_hhi,
    (j.a_depth - j.b_depth) AS d_depth,

    -- Delta score: positive is "good"
    -- Risk down good, Health up good, Spread down good, Depth up good
    (
      COALESCE((j.b_risk - j.a_risk), 0) * 1.0 +
      COALESCE((j.a_health - j.b_health), 0) * 1.0 +
      COALESCE((j.b_spread - j.a_spread), 0) * 100.0 +
      COALESCE((j.a_depth - j.b_depth) / 100.0, 0) * 1.0
    ) AS delta_score,

    -- Cost proxy from params:
    -- 1) budget if present
    -- 2) otherwise action_count as tiny cost proxy (prevents div by zero)
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

  -- ROI score: delta_score per 1000 budget (or cost proxy)
  CASE
    WHEN scored.cost_proxy IS NULL OR scored.cost_proxy = 0 THEN NULL
    ELSE (scored.delta_score / (scored.cost_proxy / 1000.0))
  END AS roi_score

FROM scored
ORDER BY scored.applied_day DESC, scored.last_created_at DESC;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
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
    """
    Raw audit trail.
    """
    days = max(1, min(int(days), 180))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
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
  AND day >= (SELECT latest_day FROM latest) - %s
ORDER BY day DESC, created_at DESC;
"""
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)


def list_interventions_collapsed(market_id: str, days: int = 30):
    """
    UI friendly list.
    Collapses by (market_id, day, action_code).
    Adds: action_count, first_created_at, last_created_at.
    """
    days = max(1, min(int(days), 180))
    q = """
WITH latest AS (
  SELECT MAX(day) AS latest_day
  FROM market_metrics_daily
  WHERE market_id = %s
),
base AS (
  SELECT *
  FROM market_interventions
  WHERE market_id = %s
    AND day >= (SELECT latest_day FROM latest) - %s
)
SELECT
  market_id,
  day,
  action_code,
  COUNT(*)::int AS action_count,
  MIN(created_at) AS first_created_at,
  MAX(created_at) AS last_created_at,

  -- representative fields from latest row
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
            cur.execute(q, (market_id, market_id, days))
            return rows_as_dicts(cur)


@app.get("/ops/markets/{market_id}/interventions/collapsed")
def interventions_collapsed(market_id: str, days: int = 30):
    return list_interventions_collapsed(market_id, days=days)


@app.post("/ops/markets/{market_id}/interventions")
def create_intervention(market_id: str, payload: InterventionCreate, operator: str = Depends(require_write_key)):
    if not market_exists(market_id):
        raise HTTPException(
            status_code=404,
            detail={"code": "market_not_found", "message": "Market not found", "details": {"market_id": market_id}},
        )

    status = (payload.status or "PLANNED").strip().upper()
    if status not in {"PLANNED", "APPLIED", "REVERTED", "CANCELLED"}:
        raise HTTPException(
            status_code=400,
            detail={"code": "invalid_status", "message": "Invalid status", "details": {"allowed": ["PLANNED", "APPLIED", "REVERTED", "CANCELLED"]}},
        )

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
                    payload.action_code,
                    payload.title,
                    status,
                    Json(payload.params),
                    operator,
                    status,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))


@app.post("/ops/interventions/{intervention_id}/apply")
def apply_intervention(intervention_id: int, operator: str = Depends(require_write_key)):
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
            params = _parse_params(itv.get("params"))

            cur.execute(
                """
                UPDATE market_interventions
                SET status = 'APPLIED',
                    applied_at = COALESCE(applied_at, now()),
                    created_by = COALESCE(created_by, %s)
                WHERE id = %s
                RETURNING id, market_id, incident_id, day, action_code, title, status, params, created_by, created_at, applied_at
                """,
                (operator, intervention_id),
            )
            updated_row = cur.fetchone()
            updated_cols = [c.name for c in cur.description]
            updated = dict(zip(updated_cols, updated_row))

            _ensure_metrics_row(cur, market_id, day)
            _apply_action(cur, action_code, market_id, day, params, direction=+1)

            conn.commit()
            return updated


@app.post("/ops/interventions/{intervention_id}/revert")
def revert_intervention(intervention_id: int, operator: str = Depends(require_write_key)):
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
            params = _parse_params(itv.get("params"))

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
def cancel_intervention(intervention_id: int, operator: str = Depends(require_write_key)):
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
  FROM market_metrics_daily
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
  LEFT JOIN market_metrics_daily mb
    ON mb.market_id = base.market_id AND mb.day = base.before_day
  LEFT JOIN market_metrics_daily ma
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
  FROM market_metrics_daily
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
def upsert_override(market_id: str, payload: ManualOverrideCreate, operator: str = Depends(require_write_key)):
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
                (market_id, payload.day, payload.risk_score_override, payload.health_score_override, payload.note, operator),
            )
            row = cur.fetchone()
            conn.commit()
            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))
