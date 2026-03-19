from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional
from apps.api.ops.pipeline import run_ops_pipeline

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel

import psycopg
from psycopg.rows import dict_row

from apps.api.db import get_db_dsn

from apps.api.ops.universe import compute_market_universe_daily
from apps.api.ops.microstructure import compute_microstructure_daily
from apps.api.services.microstructure_features import compute_microstructure_features_daily

from apps.api.ops.resolution import (
    compute_market_resolution_raw_daily,
    compute_market_resolution_features_daily,
    compute_market_resolution_scores_daily,
)

BUILD_ID = "microstructure_py_build_2026_03_05_B"

router = APIRouter()


# -------------------------
# Models
# -------------------------

class LatestDayResponse(BaseModel):
    day: Optional[str]
    rows: int


class MicroTopRow(BaseModel):
    market_id: str
    title: Optional[str] = None
    url: Optional[str] = None

    day: str
    window_hours: int

    volume: float = 0.0
    trades: int = 0
    unique_traders: int = 0

    identity_coverage: Optional[float] = None
    identity_blind: bool = False

    top1_trader_share: Optional[float] = None
    top5_trader_share: Optional[float] = None
    hhi: Optional[float] = None

    price_volatility: Optional[float] = None

    bbo_ticks: int = 0
    avg_spread: Optional[float] = None

    suspicious_burst_flag: bool = False
    burst_score: Optional[float] = None

    structural_score: Optional[float] = None

    structural_percentile: Optional[float] = None
    structural_rank: Optional[int] = None

class MarketSearchRow(BaseModel):
    market_id: str
    title: Optional[str] = None
    url: Optional[str] = None
    status: Optional[str] = None
    closed: Optional[bool] = None
    end_date: Optional[str] = None
    closed_time: Optional[str] = None
    resolved_at: Optional[str] = None
    latest_micro_day: Optional[str] = None
    has_resolution_features: bool = False

class FeaturesTopRow(BaseModel):
    market_id: str
    title: Optional[str] = None
    url: Optional[str] = None

    day: str
    window_hours: int

    market_quality_score: float
    liquidity_health_score: float
    concentration_risk_score: float

    trading_activity_score: Optional[float] = None
    spread_quality_score: Optional[float] = None
    volatility_risk_score: Optional[float] = None
    burst_risk_score: Optional[float] = None

    low_activity_flag: Optional[bool] = None
    high_concentration_flag: Optional[bool] = None
    wide_spread_flag: Optional[bool] = None
    high_volatility_flag: Optional[bool] = None
    burst_flag: Optional[bool] = None

    quality_flags: Optional[List[str]] = None
    liquidity_flags: Optional[List[str]] = None
    concentration_flags: Optional[List[str]] = None


class MicroSummaryResponse(BaseModel):
    day: str
    window_hours: int
    protocol: Optional[str] = None

    universe_rows: int
    active_universe_rows: int
    open_rows: int
    non_open_rows: int

    micro_rows: int
    features_rows: int

    active_missing_micro: int
    micro_missing_features: int

    latest_universe_day: Optional[str] = None
    latest_micro_day: Optional[str] = None
    latest_features_day: Optional[str] = None

class MarketRegimeRow(BaseModel):
    market_id: str
    day: str
    title: Optional[str] = None
    url: Optional[str] = None
    regime: Optional[str] = None
    regime_reason: Optional[str] = None

    market_quality_score: Optional[float] = None
    liquidity_health_score: Optional[float] = None
    concentration_risk_score: Optional[float] = None
    whale_volume_share: Optional[float] = None

    trades: Optional[int] = None
    unique_traders: Optional[int] = None
    trader_count: Optional[int] = None

class MarketRegimeV2Row(BaseModel):
    market_id: str
    day: str
    title: Optional[str] = None
    url: Optional[str] = None
    regime: Optional[str] = None
    regime_reason: Optional[str] = None

    market_quality_score: Optional[float] = None
    liquidity_health_score: Optional[float] = None
    concentration_risk_score: Optional[float] = None
    whale_volume_share: Optional[float] = None

    trades: Optional[int] = None
    unique_traders: Optional[int] = None
    trader_count: Optional[int] = None

class TraderRoleRow(BaseModel):
    market_id: str
    title: Optional[str] = None
    trader_id: str
    day: str

    role: str
    confidence: float

    trades: Optional[int] = None
    buy_trades: Optional[int] = None
    sell_trades: Optional[int] = None

    volume: Optional[float] = None
    avg_trade_size: Optional[float] = None
    buy_ratio: Optional[float] = None
    market_volume_share: Optional[float] = None
    active_minutes: Optional[int] = None

    is_large_participant: bool = False
    is_one_sided: bool = False
    is_high_frequency: bool = False

    supporting_flags: Optional[List[str]] = None

class TraderTopRow(BaseModel):
    market_id: str
    title: Optional[str] = None
    trader_id: str
    day: str

    trades: int
    buy_trades: int
    sell_trades: int

    volume: Optional[float] = None
    avg_trade_size: Optional[float] = None
    buy_ratio: Optional[float] = None
    market_volume_share: Optional[float] = None
    active_minutes: Optional[int] = None

    is_large_participant: bool = False
    is_one_sided: bool = False
    is_high_frequency: bool = False

class MarketRiskRadarRow(BaseModel):
    market_id: str
    day: str
    title: Optional[str] = None
    url: Optional[str] = None

    risk_score: Optional[float] = None
    risk_tier: Optional[str] = None
    primary_risk_reason: Optional[str] = None
    dominant_role: Optional[str] = None
    needs_operator_review: bool = False

    regime: Optional[str] = None
    regime_reason: Optional[str] = None

    market_quality_score: Optional[float] = None
    liquidity_health_score: Optional[float] = None
    concentration_risk_score: Optional[float] = None
    whale_volume_share: Optional[float] = None

    trades: Optional[int] = None
    unique_traders: Optional[int] = None
    trader_count: Optional[int] = None

    whale_count: int = 0
    speculator_count: int = 0
    organic_count: int = 0
    high_frequency_count: int = 0
    possible_farmer_count: int = 0

    whale_role_share: Optional[float] = None
    speculator_role_share: Optional[float] = None
    neutral_role_share: Optional[float] = None

    risk_labels: Optional[List[str]] = None


class PipelineStatusResponse(BaseModel):
    build_id: str

    latest_universe_day: Optional[str] = None
    latest_microstructure_day: Optional[str] = None
    latest_features_day: Optional[str] = None
    latest_trader_behavior_day: Optional[str] = None
    latest_market_regime_day: Optional[str] = None
    latest_resolution_features_day: Optional[str] = None

    universe_rows_latest: int = 0
    microstructure_rows_latest: int = 0
    features_rows_latest: int = 0
    trader_behavior_rows_latest: int = 0
    market_regime_rows_latest: int = 0
    resolution_features_rows_latest: int = 0

    latest_trade_ts: Optional[str] = None
    latest_bbo_ts: Optional[str] = None

    health: str

class WatchlistRequest(BaseModel):
    market_ids: List[str]
    day: Optional[date] = None
    window_hours: int = 24


class WatchlistRow(BaseModel):
    market_id: str
    day: Optional[str] = None
    window_hours: int

    title: Optional[str] = None
    url: Optional[str] = None

    status: Optional[str] = None
    is_active_24h: Optional[bool] = None

    structural_score: Optional[float] = None
    market_quality_score: Optional[float] = None

    liquidity_flags: Optional[List[str]] = None
    quality_flags: Optional[List[str]] = None
    concentration_flags: Optional[List[str]] = None

    low_activity_flag: Optional[bool] = None
    wide_spread_flag: Optional[bool] = None
    high_concentration_flag: Optional[bool] = None
    high_volatility_flag: Optional[bool] = None
    burst_flag: Optional[bool] = None

    end_date: Optional[str] = None
    closed_time: Optional[str] = None
    resolved_at: Optional[str] = None
    close_lag_seconds_clean: Optional[float] = None
    close_lag_is_negative: Optional[bool] = None

    # NEW: diagnostics-lite for the watchlist row (does not break existing shape)
    data_state: Optional[str] = None
    reason: Optional[str] = None


# -------------------------
# Helpers
# -------------------------

def _get_conn():
    return psycopg.connect(get_db_dsn())


def _safe_sort(sort: str, allowed: List[str], default: str) -> str:
    return sort if sort in allowed else default


def _safe_order(order: str) -> str:
    return "asc" if order == "asc" else "desc"


def _parse_pg_array_text(s: str) -> List[str]:
    s = s.strip()
    if len(s) >= 2 and s[0] == "{" and s[-1] == "}":
        inner = s[1:-1].strip()
        if inner == "":
            return []
        return [p.strip().strip('"') for p in inner.split(",") if p.strip() != ""]
    return [s]


def _to_str_list(v) -> Optional[List[str]]:
    if v is None:
        return None
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            if x is None:
                continue
            if isinstance(x, str) and x.strip().startswith("{") and x.strip().endswith("}"):
                out.extend(_parse_pg_array_text(x))
            else:
                out.append(str(x))
        return out or None
    if isinstance(v, tuple):
        return _to_str_list(list(v))
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        if s.startswith("{") and s.endswith("}"):
            parsed = _parse_pg_array_text(s)
            return parsed or None
        return [s]
    return [str(v)]


def _num_to_str(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return format(x, "f")
    return str(x)


def _is_non_open_status(status: Optional[str]) -> bool:
    if status is None:
        return False
    s = status.strip().lower()
    return s not in ("open", "")

def _to_str_list(value) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(x) for x in value]
    return None

# -------------------------
# Watchlist (UPDATED)
# -------------------------

@router.post("/ops/microstructure/watchlist", response_model=List[WatchlistRow])
def watchlist(payload: WatchlistRequest = Body(...)) -> List[WatchlistRow]:
    """
    Watchlist returns a per-market row even if:
      - the market is closed and not present in universe for the requested day
      - microstructure is absent due to active-only gating
    """
    market_ids = [m.strip() for m in (payload.market_ids or []) if m and m.strip()]
    if not market_ids:
        return []

    window_hours = payload.window_hours
    req_day = payload.day

    # Default to latest micro day if day not provided
    if req_day is None:
        sql_latest = """
          select max(day)
          from public.market_microstructure_daily
          where window_hours = %(window_hours)s::int;
        """
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_latest, {"window_hours": window_hours})
                d = cur.fetchone()
        req_day = d[0] if d and d[0] else None

    if req_day is None:
        # no pipeline days yet
        out: List[WatchlistRow] = []
        for mid in market_ids:
            out.append(
                WatchlistRow(
                    market_id=mid,
                    day=None,
                    window_hours=window_hours,
                    title=None,
                    url=None,
                    status=None,
                    is_active_24h=None,
                    structural_score=None,
                    market_quality_score=None,
                    liquidity_flags=None,
                    quality_flags=None,
                    concentration_flags=None,
                    low_activity_flag=None,
                    wide_spread_flag=None,
                    high_concentration_flag=None,
                    high_volatility_flag=None,
                    burst_flag=None,
                    end_date=None,
                    closed_time=None,
                    resolved_at=None,
                    close_lag_seconds_clean=None,
                    close_lag_is_negative=None,
                    data_state="no_pipeline_days",
                    reason="no microstructure days exist yet for this window_hours",
                )
            )
        return out

    sql = """
      with ids as (
        select unnest(%(market_ids)s::text[]) as market_id
      ),
      u as (
        select *
        from public.market_universe_daily
        where day = %(day)s::date
          and window_hours = %(window_hours)s::int
          and market_id = any(%(market_ids)s::text[])
      ),
      m as (
        select *
        from public.market_microstructure_daily
        where day = %(day)s::date
          and window_hours = %(window_hours)s::int
          and market_id = any(%(market_ids)s::text[])
      ),
      f as (
        select *
        from public.market_microstructure_features_daily
        where day = %(day)s::date
          and window_hours = %(window_hours)s::int
          and market_id = any(%(market_ids)s::text[])
      ),
      r as (
        select *
        from marts.market_resolution_features
        where market_id = any(%(market_ids)s::text[])
      ),
      pm as (
        select market_id, title, url
        from public.markets
        where market_id = any(%(market_ids)s::text[])
      ),
      cm as (
        select market_id, title
        from core.markets
        where market_id = any(%(market_ids)s::text[])
      )
      select
        i.market_id,

        -- Always return the requested day even if micro row is absent
        %(day)s::date::text as day,
        %(window_hours)s::int as window_hours,

        coalesce(pm.title, cm.title) as title,
        pm.url as url,

        coalesce(u.status, 'closed') as status,

        case
          when u.is_active_24h is not null then u.is_active_24h
          when u.status = 'open' then null
          when u.status is null then null
          else false
        end as is_active_24h,

        m.structural_score::double precision as structural_score,
        f.market_quality_score::double precision as market_quality_score,

        f.liquidity_flags,
        f.quality_flags,
        f.concentration_flags,

        f.low_activity_flag,
        f.wide_spread_flag,
        f.high_concentration_flag,
        f.high_volatility_flag,
        f.burst_flag,

        r.end_date::text as end_date,
        r.closed_time::text as closed_time,
        r.resolved_at::text as resolved_at,
        r.close_lag_seconds_clean,
        r.close_lag_is_negative,

        (u.market_id is not null) as has_universe,
        (m.market_id is not null) as has_micro,
        (f.market_id is not null) as has_features

      from ids i
      left join u on u.market_id = i.market_id
      left join m on m.market_id = i.market_id
      left join f on f.market_id = i.market_id
      left join r on r.market_id = i.market_id
      left join pm on pm.market_id = i.market_id
      left join cm on cm.market_id = i.market_id
      order by i.market_id;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "market_ids": market_ids,
                    "day": req_day,
                    "window_hours": window_hours,
                },
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    out: List[WatchlistRow] = []
    for row in rows:
        d = dict(zip(cols, row))

        status = d.get("status")
        has_universe = bool(d.get("has_universe"))
        has_micro = bool(d.get("has_micro"))
        has_features = bool(d.get("has_features"))

        # data_state + reason (simple, stable)
        data_state: str
        reason: str

        if has_micro and has_features:
            data_state = "ok"
            reason = "microstructure and features present"
        elif has_micro and not has_features:
            data_state = "missing_features"
            reason = "microstructure present but features missing for requested day"
        elif (not has_micro) and _is_non_open_status(status):
            data_state = "no_micro_non_open"
            reason = "market is non-open so microstructure may be absent under active-only gating"
        elif (not has_micro) and (status == "open" or status is None):
            if has_universe:
                data_state = "missing_micro"
                reason = "market in universe but no microstructure row for requested day"
            else:
                data_state = "no_universe_no_micro"
                reason = "market not in universe and no microstructure row for requested day"
        else:
            data_state = "unknown"
            reason = "unclassified state"

        out.append(
            WatchlistRow(
                market_id=d.get("market_id"),
                day=d.get("day"),
                window_hours=d.get("window_hours"),

                title=d.get("title"),
                url=d.get("url"),

                status=status,
                is_active_24h=d.get("is_active_24h"),

                structural_score=d.get("structural_score"),
                market_quality_score=d.get("market_quality_score"),

                liquidity_flags=_to_str_list(d.get("liquidity_flags")),
                quality_flags=_to_str_list(d.get("quality_flags")),
                concentration_flags=_to_str_list(d.get("concentration_flags")),

                low_activity_flag=d.get("low_activity_flag"),
                wide_spread_flag=d.get("wide_spread_flag"),
                high_concentration_flag=d.get("high_concentration_flag"),
                high_volatility_flag=d.get("high_volatility_flag"),
                burst_flag=d.get("burst_flag"),

                end_date=d.get("end_date"),
                closed_time=d.get("closed_time"),
                resolved_at=d.get("resolved_at"),
                close_lag_seconds_clean=d.get("close_lag_seconds_clean"),
                close_lag_is_negative=d.get("close_lag_is_negative"),

                data_state=data_state,
                reason=reason,
            )
        )

    return out


# -------------------------
# Endpoints (unchanged)
# -------------------------

@router.get("/ops/microstructure/latest_day", response_model=LatestDayResponse)
def latest_day(window_hours: int = Query(24, ge=1, le=168)) -> LatestDayResponse:
    sql = """
      select m.day::text as day, count(*)::int as rows
      from market_microstructure_daily m
      where m.window_hours = %(window_hours)s::int
      group by m.day
      order by m.day desc
      limit 1;
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"window_hours": window_hours})
            row = cur.fetchone()

    if not row:
        return LatestDayResponse(day=None, rows=0)
    return LatestDayResponse(day=row[0], rows=row[1])


@router.get("/ops/microstructure/summary", response_model=MicroSummaryResponse)
def microstructure_summary(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    protocol: Optional[str] = Query("polymarket"),
) -> MicroSummaryResponse:
    sql = """
      with u as (
        select *
        from public.market_universe_daily
        where day = %(day)s::date
          and window_hours = %(window_hours)s::int
          and (%(protocol)s::text is null or protocol = %(protocol)s::text)
      ),
      m as (
        select market_id
        from public.market_microstructure_daily
        where day = %(day)s::date
          and window_hours = %(window_hours)s::int
      ),
      f as (
        select market_id
        from public.market_microstructure_features_daily
        where day = %(day)s::date
          and window_hours = %(window_hours)s::int
      )
      select
        count(*)::int as universe_rows,
        sum(case when u.is_active_24h then 1 else 0 end)::int as active_universe_rows,
        sum(case when u.status = 'open' then 1 else 0 end)::int as open_rows,
        sum(case when u.status is not null and u.status <> 'open' then 1 else 0 end)::int as non_open_rows,

        sum(case when m.market_id is not null then 1 else 0 end)::int as micro_rows,
        sum(case when f.market_id is not null then 1 else 0 end)::int as features_rows,

        sum(case when u.is_active_24h and m.market_id is null then 1 else 0 end)::int as active_missing_micro,
        sum(case when m.market_id is not null and f.market_id is null then 1 else 0 end)::int as micro_missing_features
      from u
      left join m on m.market_id = u.market_id
      left join f on f.market_id = u.market_id;
    """

    sql_latest_universe = """
      select max(day)::text
      from public.market_universe_daily
      where window_hours = %(window_hours)s::int
        and (%(protocol)s::text is null or protocol = %(protocol)s::text);
    """
    sql_latest_micro = """
      select max(day)::text
      from public.market_microstructure_daily
      where window_hours = %(window_hours)s::int;
    """
    sql_latest_features = """
      select max(day)::text
      from public.market_microstructure_features_daily
      where window_hours = %(window_hours)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "window_hours": window_hours, "protocol": protocol})
            row = cur.fetchone()

            cur.execute(sql_latest_universe, {"window_hours": window_hours, "protocol": protocol})
            latest_universe_day = (cur.fetchone() or [None])[0]

            cur.execute(sql_latest_micro, {"window_hours": window_hours})
            latest_micro_day = (cur.fetchone() or [None])[0]

            cur.execute(sql_latest_features, {"window_hours": window_hours})
            latest_features_day = (cur.fetchone() or [None])[0]

    if not row:
        return MicroSummaryResponse(
            day=str(day),
            window_hours=window_hours,
            protocol=protocol,
            universe_rows=0,
            active_universe_rows=0,
            open_rows=0,
            non_open_rows=0,
            micro_rows=0,
            features_rows=0,
            active_missing_micro=0,
            micro_missing_features=0,
            latest_universe_day=latest_universe_day,
            latest_micro_day=latest_micro_day,
            latest_features_day=latest_features_day,
        )

    return MicroSummaryResponse(
        day=str(day),
        window_hours=window_hours,
        protocol=protocol,
        universe_rows=row[0],
        active_universe_rows=row[1],
        open_rows=row[2],
        non_open_rows=row[3],
        micro_rows=row[4],
        features_rows=row[5],
        active_missing_micro=row[6],
        micro_missing_features=row[7],
        latest_universe_day=latest_universe_day,
        latest_micro_day=latest_micro_day,
        latest_features_day=latest_features_day,
    )

@router.get("/ops/markets/search", response_model=List[MarketSearchRow])
def market_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    protocol: str = Query("polymarket"),
):
    sql = """
      with latest_micro as (
        select
          market_id,
          max(day)::text as latest_micro_day
        from public.market_microstructure_daily
        group by market_id
      )
      select
        m.market_id,
        m.title,
        m.url,
        m.status,
        m.closed,
        m.end_date::text as end_date,
        m.closed_time::text as closed_time,
        m.resolved_at::text as resolved_at,
        lm.latest_micro_day,
        case when r.market_id is not null then true else false end as has_resolution_features
      from public.markets m
      left join latest_micro lm
        on lm.market_id = m.market_id
      left join marts.market_resolution_features r
        on r.market_id = m.market_id
      where m.protocol = %(protocol)s
        and m.title ilike ('%%' || %(q)s || '%%')
      order by
        coalesce(lm.latest_micro_day, '0001-01-01') desc,
        m.updated_at desc nulls last,
        m.market_id asc
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"q": q, "limit": limit, "protocol": protocol})
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    out: List[MarketSearchRow] = []
    for row in rows:
        d = dict(zip(cols, row))
        out.append(
            MarketSearchRow(
                market_id=d.get("market_id"),
                title=d.get("title"),
                url=d.get("url"),
                status=d.get("status"),
                closed=d.get("closed"),
                end_date=d.get("end_date"),
                closed_time=d.get("closed_time"),
                resolved_at=d.get("resolved_at"),
                latest_micro_day=d.get("latest_micro_day"),
                has_resolution_features=bool(d.get("has_resolution_features")),
            )
        )
    return out

@router.get("/ops/microstructure/top", response_model=List[MicroTopRow])
def top(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(20, ge=1, le=500),
    sort: str = Query("structural_score"),
    order: Literal["asc", "desc"] = Query("desc"),
):
    allowed = [
        "structural_score",
        "trades",
        "volume",
        "avg_spread",
        "price_volatility",
        "bbo_ticks",
        "burst_score",
        "identity_coverage",
        "hhi",
        "unique_traders",
        "top1_trader_share",
        "top5_trader_share",
        "structural_percentile",
        "structural_rank",
    ]
    sort_col = _safe_sort(sort, allowed, "structural_score")
    order_dir = _safe_order(order)

    sql = f"""
      with base as (
        select
          m.market_id,
          mk.title,
          mk.url,

          m.day::text as day,
          m.window_hours,

          coalesce(m.volume,0)::double precision as volume,
          coalesce(m.trades,0)::int as trades,
          coalesce(m.unique_traders,0)::int as unique_traders,

          m.identity_coverage::double precision as identity_coverage,
          coalesce(m.identity_blind,false)::boolean as identity_blind,

          m.top1_trader_share::double precision as top1_trader_share,
          m.top5_trader_share::double precision as top5_trader_share,
          m.hhi::double precision as hhi,

          m.price_volatility::double precision as price_volatility,

          coalesce(m.bbo_ticks,0)::int as bbo_ticks,
          m.avg_spread::double precision as avg_spread,

          coalesce(m.suspicious_burst_flag,false)::boolean as suspicious_burst_flag,
          m.burst_score::double precision as burst_score,

          m.structural_score::double precision as structural_score,

          percent_rank() over (order by m.structural_score desc nulls last) as structural_percentile,
          dense_rank() over (order by m.structural_score desc nulls last)::int as structural_rank

        from market_microstructure_daily m
        left join markets mk on mk.market_id = m.market_id
        where m.day = %(day)s::date
          and m.window_hours = %(window_hours)s::int
      )
      select *
      from base
      order by {sort_col} {order_dir} nulls last
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "window_hours": window_hours, "limit": limit})
            rows = cur.fetchall()

    out: List[MicroTopRow] = []
    for r in rows:
        out.append(
            MicroTopRow(
                market_id=r[0],
                title=r[1],
                url=r[2],
                day=r[3],
                window_hours=r[4],
                volume=r[5],
                trades=r[6],
                unique_traders=r[7],
                identity_coverage=r[8],
                identity_blind=r[9],
                top1_trader_share=r[10],
                top5_trader_share=r[11],
                hhi=r[12],
                price_volatility=r[13],
                bbo_ticks=r[14],
                avg_spread=r[15],
                suspicious_burst_flag=r[16],
                burst_score=r[17],
                structural_score=r[18],
                structural_percentile=r[19],
                structural_rank=r[20],
            )
        )
    return out


@router.post("/ops/universe/daily")
def universe_daily(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(500, ge=1, le=10000),
):
    return compute_market_universe_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)


@router.post("/ops/microstructure/compute")
def compute(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(500, ge=1, le=10000),
) -> Dict[str, Any]:
    return compute_microstructure_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)


@router.post("/ops/microstructure/features/compute")
def compute_features(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(5000, ge=1, le=10000),
) -> Dict[str, Any]:
    return compute_microstructure_features_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)


@router.get("/ops/microstructure/features/top", response_model=List[FeaturesTopRow])
def features_top(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit: int = Query(20, ge=1, le=500),
    sort: str = Query("market_quality_score"),
    order: Literal["asc", "desc"] = Query("desc"),
):
    allowed = [
        "market_quality_score",
        "liquidity_health_score",
        "concentration_risk_score",
        "trading_activity_score",
        "spread_quality_score",
        "volatility_risk_score",
        "burst_risk_score",
    ]
    sort_col = _safe_sort(sort, allowed, "market_quality_score")
    order_dir = _safe_order(order)

    sql = f"""
      select
        f.market_id,
        mk.title,
        mk.url,

        f.day::text,
        f.window_hours,

        f.market_quality_score::double precision,
        f.liquidity_health_score::double precision,
        f.concentration_risk_score::double precision,

        f.trading_activity_score::double precision,
        f.spread_quality_score::double precision,
        f.volatility_risk_score::double precision,
        f.burst_risk_score::double precision,

        f.low_activity_flag,
        f.high_concentration_flag,
        f.wide_spread_flag,
        f.high_volatility_flag,
        f.burst_flag,

        f.quality_flags,
        f.liquidity_flags,
        f.concentration_flags
      from market_microstructure_features_daily f
      left join markets mk on mk.market_id = f.market_id
      where f.day = %(day)s::date
        and f.window_hours = %(window_hours)s::int
      order by {sort_col} {order_dir} nulls last
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "window_hours": window_hours, "limit": limit})
            rows = cur.fetchall()

    out: List[FeaturesTopRow] = []
    for r in rows:
        out.append(
            FeaturesTopRow(
                market_id=r[0],
                title=r[1],
                url=r[2],
                day=r[3],
                window_hours=r[4],
                market_quality_score=r[5],
                liquidity_health_score=r[6],
                concentration_risk_score=r[7],
                trading_activity_score=r[8],
                spread_quality_score=r[9],
                volatility_risk_score=r[10],
                burst_risk_score=r[11],
                low_activity_flag=r[12],
                high_concentration_flag=r[13],
                wide_spread_flag=r[14],
                high_volatility_flag=r[15],
                burst_flag=r[16],
                quality_flags=_to_str_list(r[17]),
                liquidity_flags=_to_str_list(r[18]),
                concentration_flags=_to_str_list(r[19]),
            )
        )
    return out


@router.post("/ops/resolution/raw/compute")
def resolution_raw_compute(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(500, ge=1, le=10000),
) -> Dict[str, Any]:
    return compute_market_resolution_raw_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)


@router.post("/ops/resolution/features/compute")
def resolution_features_compute(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(5000, ge=1, le=20000),
) -> Dict[str, Any]:
    return compute_market_resolution_features_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)


@router.post("/ops/resolution/daily")
def resolution_daily(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(5000, ge=1, le=20000),
) -> Dict[str, Any]:
    raw_res = compute_market_resolution_raw_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)
    feat_res = compute_market_resolution_features_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)
    risk_res = compute_market_resolution_scores_daily(day=day, window_hours=window_hours, limit_markets=limit_markets)

    return {
        "status": "ok",
        "raw": raw_res,
        "features": feat_res,
        "risk": risk_res,
    }


@router.post("/ops/pipeline/daily")
def run_daily_pipeline(
    day: date = Query(...),
    window_hours: int = Query(24, ge=1, le=168),
    limit_markets: int = Query(500, ge=1, le=10000),
    feature_limit_markets: int = Query(5000, ge=1, le=20000),
) -> Dict[str, Any]:
    universe = compute_market_universe_daily(
        day=day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )

    micro = compute_microstructure_daily(
        day=day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )

    features = compute_microstructure_features_daily(
        day=day,
        window_hours=window_hours,
        limit_markets=feature_limit_markets,
    )

    resolution_raw = compute_market_resolution_raw_daily(
        day=day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )

    resolution_features = compute_market_resolution_features_daily(
        day=day,
        window_hours=window_hours,
        limit_markets=feature_limit_markets,
    )

    resolution_risk = compute_market_resolution_scores_daily(
        day=day,
        window_hours=window_hours,
        limit_markets=feature_limit_markets,
    )

    return {
        "status": "ok",
        "universe": universe,
        "microstructure": micro,
        "features": features,
        "resolution": {
            "raw": resolution_raw,
            "features": resolution_features,
            "risk": resolution_risk,
        },
    }

@router.post("/ops/pipeline/run")
def run_pipeline(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
):
    return run_ops_pipeline(
        day=day,
        window_hours=window_hours,
        limit_markets=limit_markets,
    )

@router.get("/ops/pipeline/status", response_model=PipelineStatusResponse)
def pipeline_status():
    sql = """
    with
    latest_universe as (
        select max(day)::text as day
        from public.market_universe_daily
    ),
    latest_micro as (
        select max(day)::text as day
        from public.market_microstructure_daily
    ),
    latest_features as (
        select max(day)::text as day
        from public.market_microstructure_features_daily
    ),
    latest_trader_behavior as (
        select max(day)::text as day
        from public.trader_behavior_daily
    ),
    latest_trader_role as (
        select max(day)::text as day
        from public.trader_role_daily
    ),
    latest_market_regime as (
        select max(day)::text as day
        from public.market_regime_daily
    ),
    latest_resolution_features as (
        select max(day)::text as day
        from public.market_resolution_features_daily
    ),

    universe_rows as (
        select count(*)::int as n
        from public.market_universe_daily
        where day = (select max(day) from public.market_universe_daily)
    ),
    micro_rows as (
        select count(*)::int as n
        from public.market_microstructure_daily
        where day = (select max(day) from public.market_microstructure_daily)
    ),
    feature_rows as (
        select count(*)::int as n
        from public.market_microstructure_features_daily
        where day = (select max(day) from public.market_microstructure_features_daily)
    ),
    trader_behavior_rows as (
        select count(*)::int as n
        from public.trader_behavior_daily
        where day = (select max(day) from public.trader_behavior_daily)
    ),
    trader_role_rows as (
        select count(*)::int as n
        from public.trader_role_daily
        where day = (select max(day) from public.trader_role_daily)
    ),
    market_regime_rows as (
        select count(*)::int as n
        from public.market_regime_daily
        where day = (select max(day) from public.market_regime_daily)
    ),
    resolution_feature_rows as (
        select count(*)::int as n
        from public.market_resolution_features_daily
        where day = (select max(day) from public.market_resolution_features_daily)
    ),

    latest_trade as (
        select max(ts)::text as ts
        from public.trades
    ),
    latest_bbo as (
        select max(ts)::text as ts
        from public.market_bbo_ticks
    )

    select
        %(build_id)s::text as build_id,

        (select day from latest_universe) as latest_universe_day,
        (select day from latest_micro) as latest_microstructure_day,
        (select day from latest_features) as latest_features_day,
        (select day from latest_trader_behavior) as latest_trader_behavior_day,
        (select day from latest_market_regime) as latest_market_regime_day,
        (select day from latest_resolution_features) as latest_resolution_features_day,

        coalesce((select n from universe_rows), 0) as universe_rows_latest,
        coalesce((select n from micro_rows), 0) as microstructure_rows_latest,
        coalesce((select n from feature_rows), 0) as features_rows_latest,
        coalesce((select n from trader_behavior_rows), 0) as trader_behavior_rows_latest,
        coalesce((select n from market_regime_rows), 0) as market_regime_rows_latest,
        coalesce((select n from resolution_feature_rows), 0) as resolution_features_rows_latest,

        (select ts from latest_trade) as latest_trade_ts,
        (select ts from latest_bbo) as latest_bbo_ts,

        case
            when
                (select day from latest_universe) is not null
                and (select day from latest_micro) is not null
                and (select day from latest_features) is not null
                and (select day from latest_trader_behavior) is not null
                and (select day from latest_trader_role) is not null
                and (select day from latest_market_regime) is not null
            then 'ok'
            else 'degraded'
        end as health;
    """

    build_id = "pipeline_status_v2_2026_03_08"

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"build_id": build_id})
            r = cur.fetchone()

    if r is None:
        return PipelineStatusResponse(
            build_id=build_id,
            latest_universe_day=None,
            latest_microstructure_day=None,
            latest_features_day=None,
            latest_trader_behavior_day=None,
            latest_market_regime_day=None,
            latest_resolution_features_day=None,
            universe_rows_latest=0,
            microstructure_rows_latest=0,
            features_rows_latest=0,
            trader_behavior_rows_latest=0,
            market_regime_rows_latest=0,
            resolution_features_rows_latest=0,
            latest_trade_ts=None,
            latest_bbo_ts=None,
            health="degraded",
        )

    return PipelineStatusResponse(
        build_id=r[0],
        latest_universe_day=r[1],
        latest_microstructure_day=r[2],
        latest_features_day=r[3],
        latest_trader_behavior_day=r[4],
        latest_market_regime_day=r[5],
        latest_resolution_features_day=r[6],
        universe_rows_latest=r[7],
        microstructure_rows_latest=r[8],
        features_rows_latest=r[9],
        trader_behavior_rows_latest=r[10],
        market_regime_rows_latest=r[11],
        resolution_features_rows_latest=r[12],
        latest_trade_ts=r[13],
        latest_bbo_ts=r[14],
        health=r[15],
    )

@router.get("/ops/markets/regimes", response_model=List[MarketRegimeRow])
def market_regimes(
    day: date = Query(...),
    limit: int = Query(50, ge=1, le=500),
    regime: Optional[str] = Query(None),
):
    sql = """
      select
        r.market_id,
        r.day::text,
        mk.title,
        mk.url,
        r.regime,
        r.regime_reason,
        r.market_quality_score::double precision,
        r.liquidity_health_score::double precision,
        r.concentration_risk_score::double precision,
        r.whale_volume_share::double precision,
        r.trades::int,
        r.unique_traders::int,
        r.trader_count::int
      from public.market_regime_daily r
      left join public.markets mk
        on mk.market_id = r.market_id
      where r.day = %(day)s::date
        and (%(regime)s::text is null or r.regime = %(regime)s::text)
      order by
        case
          when r.regime = 'healthy' then 1
          when r.regime = 'whale_dominated' then 2
          when r.regime = 'farming_dominated' then 3
          when r.regime = 'liquidity_collapse' then 4
          when r.regime = 'mixed' then 5
          else 6
        end,
        r.market_quality_score desc nulls last
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "limit": limit, "regime": regime})
            rows = cur.fetchall()

    out: List[MarketRegimeRow] = []
    for r in rows:
        out.append(
            MarketRegimeRow(
                market_id=r[0],
                day=r[1],
                title=r[2],
                url=r[3],
                regime=r[4],
                regime_reason=r[5],
                market_quality_score=r[6],
                liquidity_health_score=r[7],
                concentration_risk_score=r[8],
                whale_volume_share=r[9],
                trades=r[10],
                unique_traders=r[11],
                trader_count=r[12],
            )
        )
    return out

@router.get("/ops/markets/regimes/v2", response_model=List[MarketRegimeV2Row])
def market_regimes_v2(
    day: date = Query(...),
    limit: int = Query(50, ge=1, le=500),
    regime: Optional[str] = Query(None),
):
    sql = """
      select
        r.market_id,
        r.day::text,
        mk.title,
        mk.url,
        r.regime,
        r.regime_reason,
        r.market_quality_score::double precision,
        r.liquidity_health_score::double precision,
        r.concentration_risk_score::double precision,
        r.whale_volume_share::double precision,
        r.trades::int,
        r.unique_traders::int,
        r.trader_count::int
      from public.market_regime_daily_v2 r
      left join public.markets mk
        on mk.market_id = r.market_id
      where r.day = %(day)s::date
        and (%(regime)s::text is null or r.regime = %(regime)s::text)
      order by
        case
          when r.regime = 'organic_market' then 1
          when r.regime = 'whale_dominated' then 2
          when r.regime = 'farming_dominated' then 3
          when r.regime = 'liquidity_collapse' then 4
          when r.regime = 'thin_market' then 5
          when r.regime = 'inactive' then 6
          when r.regime = 'mixed' then 7
          else 8
        end,
        r.market_quality_score desc nulls last,
        r.trades desc nulls last
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "limit": limit, "regime": regime})
            rows = cur.fetchall()

    out: List[MarketRegimeV2Row] = []
    for r in rows:
        out.append(
            MarketRegimeV2Row(
                market_id=r[0],
                day=r[1],
                title=r[2],
                url=r[3],
                regime=r[4],
                regime_reason=r[5],
                market_quality_score=r[6],
                liquidity_health_score=r[7],
                concentration_risk_score=r[8],
                whale_volume_share=r[9],
                trades=r[10],
                unique_traders=r[11],
                trader_count=r[12],
            )
        )
    return out

@router.get("/ops/markets/risk-radar", response_model=List[MarketRiskRadarRow])
def market_risk_radar(
    day: date = Query(...),
    limit: int = Query(50, ge=1, le=500),
    risk_tier: Optional[str] = Query(None),
    review_only: bool = Query(False),
):
    sql = """
      select
        r.market_id,
        r.day::text,
        mk.title,
        mk.url,

        r.risk_score::double precision,
        r.risk_tier,
        r.primary_risk_reason,
        r.dominant_role,
        coalesce(r.needs_operator_review, false)::boolean,

        r.regime,
        r.regime_reason,

        r.market_quality_score::double precision,
        r.liquidity_health_score::double precision,
        r.concentration_risk_score::double precision,
        r.whale_volume_share::double precision,

        r.trades::int,
        r.unique_traders::int,
        r.trader_count::int,

        coalesce(r.whale_count, 0)::int,
        coalesce(r.speculator_count, 0)::int,
        coalesce(r.organic_count, 0)::int,
        coalesce(r.high_frequency_count, 0)::int,
        coalesce(r.possible_farmer_count, 0)::int,

        r.whale_role_share::double precision,
        r.speculator_role_share::double precision,
        r.neutral_role_share::double precision,

        r.risk_labels
      from public.market_risk_radar_daily r
      left join public.markets mk
        on mk.market_id = r.market_id
      where r.day = %(day)s::date
        and (%(risk_tier)s::text is null or r.risk_tier = %(risk_tier)s::text)
        and (%(review_only)s::boolean = false or coalesce(r.needs_operator_review, false) = true)
      order by
        case
          when r.risk_tier = 'critical' then 1
          when r.risk_tier = 'high' then 2
          when r.risk_tier = 'medium' then 3
          when r.risk_tier = 'low' then 4
          else 5
        end,
        r.risk_score desc nulls last,
        r.market_quality_score asc nulls last
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "limit": limit,
                    "risk_tier": risk_tier,
                    "review_only": review_only,
                },
            )
            rows = cur.fetchall()

    out: List[MarketRiskRadarRow] = []
    for r in rows:
        out.append(
            MarketRiskRadarRow(
                market_id=r[0],
                day=r[1],
                title=r[2],
                url=r[3],
                risk_score=r[4],
                risk_tier=r[5],
                primary_risk_reason=r[6],
                dominant_role=r[7],
                needs_operator_review=r[8],
                regime=r[9],
                regime_reason=r[10],
                market_quality_score=r[11],
                liquidity_health_score=r[12],
                concentration_risk_score=r[13],
                whale_volume_share=r[14],
                trades=r[15],
                unique_traders=r[16],
                trader_count=r[17],
                whale_count=r[18],
                speculator_count=r[19],
                organic_count=r[20],
                high_frequency_count=r[21],
                possible_farmer_count=r[22],
                whale_role_share=r[23],
                speculator_role_share=r[24],
                neutral_role_share=r[25],
                risk_labels=_to_str_list(r[26]),
            )
        )
    return out

@router.get("/ops/markets/manipulation")
def markets_manipulation(
    day: Optional[date] = None,
    risk_tier: Optional[str] = None,
    review_only: bool = False,
    limit: int = 20,
):
    sql = """
    SELECT
        mm.market_id,
        mm.day,
        m.title,
        m.url,
        mm.manipulation_score,
        mm.risk_tier,
        mm.primary_signal,
        mm.signal_labels,
        mm.needs_operator_review,
        mm.trades,
        mm.unique_traders,
        mm.buy_volume_share,
        mm.sell_volume_share,
        mm.largest_trader_share,
        mm.top2_trader_share,
        mm.avg_trade_size,
        mm.median_trade_size
    FROM public.market_manipulation_daily mm
    LEFT JOIN public.markets m
        ON m.market_id = mm.market_id
    WHERE (%(day)s::date IS NULL OR mm.day = %(day)s::date)
      AND (%(risk_tier)s::text IS NULL OR mm.risk_tier = %(risk_tier)s::text)
      AND (%(review_only)s = false OR mm.needs_operator_review = true)
    ORDER BY mm.manipulation_score DESC NULLS LAST
    LIMIT %(limit)s
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "risk_tier": risk_tier,
                    "review_only": review_only,
                    "limit": limit,
                },
            )
            return cur.fetchall()

@router.get("/ops/markets/integrity")
def markets_integrity(
    day: Optional[date] = Query(None),
    review_only: bool = Query(False),
    band: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=200),
):
    sql = """
    select
        i.market_id,
        i.day,
        i.title,
        i.url,
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
        i.needs_operator_review
    from public.market_integrity_score_daily i
    where (%(day)s::date is null or i.day = %(day)s::date)
      and (%(review_only)s = false or i.needs_operator_review = true)
      and (%(band)s::text is null or i.integrity_band = %(band)s::text)
    order by
        i.needs_operator_review desc,
        i.integrity_score asc,
        i.market_id asc
    limit %(limit)s;
    """

    with _get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "review_only": review_only,
                    "band": band,
                    "limit": limit,
                },
            )
            rows = cur.fetchall()

    return rows

@router.get("/ops/traders/top", response_model=List[TraderTopRow])
def traders_top(
    day: date = Query(...),
    limit: int = Query(50, ge=1, le=500),
    market_id: Optional[str] = Query(None),
    sort: str = Query("volume"),
):
    allowed = {
        "volume": "t.volume desc nulls last",
        "market_volume_share": "t.market_volume_share desc nulls last",
        "trades": "t.trades desc nulls last",
        "active_minutes": "t.active_minutes desc nulls last",
    }
    order_sql = allowed.get(sort, allowed["volume"])

    sql = f"""
      select
        t.market_id,
        mk.title,
        t.trader_id,
        t.day::text,
        t.trades::int,
        t.buy_trades::int,
        t.sell_trades::int,
        t.volume::double precision,
        t.avg_trade_size::double precision,
        t.buy_ratio::double precision,
        t.market_volume_share::double precision,
        t.active_minutes::int,
        coalesce(t.is_large_participant,false)::boolean,
        coalesce(t.is_one_sided,false)::boolean,
        coalesce(t.is_high_frequency,false)::boolean
      from public.trader_behavior_daily t
      left join public.markets mk
        on mk.market_id = t.market_id
      where t.day = %(day)s::date
        and (%(market_id)s::text is null or t.market_id = %(market_id)s::text)
      order by {order_sql}
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "limit": limit, "market_id": market_id})
            rows = cur.fetchall()

    out: List[TraderTopRow] = []
    for r in rows:
        out.append(
            TraderTopRow(
                market_id=r[0],
                title=r[1],
                trader_id=r[2],
                day=r[3],
                trades=r[4],
                buy_trades=r[5],
                sell_trades=r[6],
                volume=r[7],
                avg_trade_size=r[8],
                buy_ratio=r[9],
                market_volume_share=r[10],
                active_minutes=r[11],
                is_large_participant=r[12],
                is_one_sided=r[13],
                is_high_frequency=r[14],
            )
        )
    return out

@router.get("/ops/traders/roles", response_model=List[TraderRoleRow])
def trader_roles(
    day: date = Query(...),
    limit: int = Query(50, ge=1, le=500),
    market_id: Optional[str] = Query(None),
    role: Optional[str] = Query(None),
    sort: str = Query("confidence"),
):
    allowed = {
        "confidence": "t.confidence desc nulls last",
        "volume": "t.volume desc nulls last",
        "market_volume_share": "t.market_volume_share desc nulls last",
        "trades": "t.trades desc nulls last",
    }
    order_sql = allowed.get(sort, allowed["confidence"])

    sql = f"""
      select
        t.market_id,
        mk.title,
        t.trader_id,
        t.day::text,
        t.role,
        t.confidence::double precision,
        t.trades::int,
        t.buy_trades::int,
        t.sell_trades::int,
        t.volume::double precision,
        t.avg_trade_size::double precision,
        t.buy_ratio::double precision,
        t.market_volume_share::double precision,
        t.active_minutes::int,
        coalesce(t.is_large_participant,false)::boolean,
        coalesce(t.is_one_sided,false)::boolean,
        coalesce(t.is_high_frequency,false)::boolean,
        t.supporting_flags
      from public.trader_role_daily t
      left join public.markets mk
        on mk.market_id = t.market_id
      where t.day = %(day)s::date
        and (%(market_id)s::text is null or t.market_id = %(market_id)s::text)
        and (%(role)s::text is null or t.role = %(role)s::text)
      order by {order_sql}
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "limit": limit,
                    "market_id": market_id,
                    "role": role,
                },
            )
            rows = cur.fetchall()

    out: List[TraderRoleRow] = []
    for r in rows:
        out.append(
            TraderRoleRow(
                market_id=r[0],
                title=r[1],
                trader_id=r[2],
                day=r[3],
                role=r[4],
                confidence=r[5],
                trades=r[6],
                buy_trades=r[7],
                sell_trades=r[8],
                volume=r[9],
                avg_trade_size=r[10],
                buy_ratio=r[11],
                market_volume_share=r[12],
                active_minutes=r[13],
                is_large_participant=r[14],
                is_one_sided=r[15],
                is_high_frequency=r[16],
                supporting_flags=_to_str_list(r[17]),
            )
        )
    return out

@router.get("/ops/traders/roles", response_model=List[TraderRoleRow])
def trader_roles(
    day: date = Query(...),
    limit: int = Query(50, ge=1, le=500),
    market_id: Optional[str] = Query(None),
    role: Optional[str] = Query(None),
    sort: str = Query("confidence"),
):
    allowed = {
        "confidence": "t.confidence desc nulls last",
        "volume": "t.volume desc nulls last",
        "market_volume_share": "t.market_volume_share desc nulls last",
        "trades": "t.trades desc nulls last",
    }
    order_sql = allowed.get(sort, allowed["confidence"])

    sql = f"""
      select
        t.market_id,
        mk.title,
        t.trader_id,
        t.day::text,
        t.role,
        t.confidence::double precision,
        t.trades::int,
        t.buy_trades::int,
        t.sell_trades::int,
        t.volume::double precision,
        t.avg_trade_size::double precision,
        t.buy_ratio::double precision,
        t.market_volume_share::double precision,
        t.active_minutes::int,
        coalesce(t.is_large_participant,false)::boolean,
        coalesce(t.is_one_sided,false)::boolean,
        coalesce(t.is_high_frequency,false)::boolean,
        t.supporting_flags
      from public.trader_role_daily t
      left join public.markets mk
        on mk.market_id = t.market_id
      where t.day = %(day)s::date
        and (%(market_id)s::text is null or t.market_id = %(market_id)s::text)
        and (%(role)s::text is null or t.role = %(role)s::text)
      order by {order_sql}
      limit %(limit)s::int;
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "limit": limit,
                    "market_id": market_id,
                    "role": role,
                },
            )
            rows = cur.fetchall()

    out: List[TraderRoleRow] = []
    for r in rows:
        out.append(
            TraderRoleRow(
                market_id=r[0],
                title=r[1],
                trader_id=r[2],
                day=r[3],
                role=r[4],
                confidence=r[5],
                trades=r[6],
                buy_trades=r[7],
                sell_trades=r[8],
                volume=r[9],
                avg_trade_size=r[10],
                buy_ratio=r[11],
                market_volume_share=r[12],
                active_minutes=r[13],
                is_large_participant=r[14],
                is_one_sided=r[15],
                is_high_frequency=r[16],
                supporting_flags=_to_str_list(r[17]),
            )
        )
    return out

# -------------------------
# Market Detail Endpoint (unchanged from your working version)
# -------------------------

@router.get("/ops/microstructure/market/{market_id}")
def market_detail(
    market_id: str,
    lookback_days: int = Query(14, ge=1, le=90),
    window_hours: int = Query(24, ge=1, le=168),
    as_of_day: Optional[date] = Query(None),
    include_diagnostics: bool = Query(True),
):
    diagnostics: Dict[str, Any] = {}

    with _get_conn() as conn:
        with conn.cursor() as cur:
            diagnostics["request"] = {
                "market_id": market_id,
                "window_hours": window_hours,
                "as_of_day": str(as_of_day) if as_of_day else None,
                "lookback_days": lookback_days,
            }

            sql_meta_resolution = """
                with pm as (
                  select market_id, title, url
                  from public.markets
                  where market_id = %(market_id)s
                  limit 1
                ),
                cm as (
                  select market_id, title, null::text as url
                  from core.markets
                  where market_id = %(market_id)s
                  limit 1
                )
                select
                  coalesce(pm.market_id, cm.market_id) as market_id,
                  coalesce(pm.title, cm.title) as title,
                  pm.url as url,

                  r.end_date,
                  r.closed_time,
                  r.resolved_at,
                  r.close_lag_seconds,
                  r.resolve_lag_seconds,
                  r.pre_close_trade_count_24h,
                  r.pre_close_unique_traders_24h,
                  r.pre_close_notional_24h,
                  r.last_trade_ts,
                  r.last_trade_price,
                  r.final_hour_bbo_ticks,
                  r.final_hour_spread_median,
                  r.last_bbo_ts,
                  r.last_best_bid,
                  r.last_best_ask,
                  r.last_spread,
                  r.close_lag_seconds_clean,
                  r.close_lag_is_negative
                from (select 1) x
                left join pm on true
                left join cm on true
                left join marts.market_resolution_features r
                  on r.market_id = coalesce(pm.market_id, cm.market_id);
            """
            cur.execute(sql_meta_resolution, {"market_id": market_id})
            meta_res = cur.fetchone()
            meta_cols = [d[0] for d in cur.description]
            meta_row = dict(zip(meta_cols, meta_res)) if meta_res else {}

            meta = None
            meta_source = None
            if meta_row and meta_row.get("market_id"):
                meta = {"title": meta_row.get("title"), "url": meta_row.get("url")}
                if meta_row.get("url") is not None:
                    meta_source = "public.markets"
                else:
                    meta_source = "core.markets"

            diagnostics["meta_found"] = bool(meta_row and meta_row.get("market_id"))
            diagnostics["meta_source"] = meta_source

            resolution = None
            if meta_row and meta_row.get("market_id") and (
                meta_row.get("end_date") is not None
                or meta_row.get("closed_time") is not None
                or meta_row.get("resolved_at") is not None
            ):
                resolution = {
                    "end_date": meta_row.get("end_date"),
                    "closed_time": meta_row.get("closed_time"),
                    "resolved_at": meta_row.get("resolved_at"),
                    "close_lag_seconds": meta_row.get("close_lag_seconds"),
                    "resolve_lag_seconds": meta_row.get("resolve_lag_seconds"),
                    "close_lag_seconds_clean": meta_row.get("close_lag_seconds_clean"),
                    "close_lag_is_negative": meta_row.get("close_lag_is_negative"),
                    "pre_close_trade_count_24h": meta_row.get("pre_close_trade_count_24h"),
                    "pre_close_unique_traders_24h": meta_row.get("pre_close_unique_traders_24h"),
                    "pre_close_notional_24h": _num_to_str(meta_row.get("pre_close_notional_24h")),
                    "last_trade_ts": meta_row.get("last_trade_ts"),
                    "last_trade_price": _num_to_str(meta_row.get("last_trade_price")),
                    "final_hour_bbo_ticks": meta_row.get("final_hour_bbo_ticks"),
                    "final_hour_spread_median": _num_to_str(meta_row.get("final_hour_spread_median")),
                    "last_bbo_ts": meta_row.get("last_bbo_ts"),
                    "last_best_bid": _num_to_str(meta_row.get("last_best_bid")),
                    "last_best_ask": _num_to_str(meta_row.get("last_best_ask")),
                    "last_spread": _num_to_str(meta_row.get("last_spread")),
                }

            sql_current = """
                select
                    m.*,
                    mk.title as title,
                    mk.url as url,

                    f.engine_version,
                    f.liquidity_health_score,
                    f.trading_activity_score,
                    f.spread_quality_score,
                    f.volatility_risk_score,
                    f.burst_risk_score,
                    f.concentration_risk_score,
                    f.market_quality_score,

                    f.low_activity_flag,
                    f.high_concentration_flag,
                    f.wide_spread_flag,
                    f.high_volatility_flag,
                    f.burst_flag,

                    f.quality_flags,
                    f.liquidity_flags,
                    f.concentration_flags,

                    f.activity_score,
                    f.spread_score,
                    f.depth_score

                from public.market_microstructure_daily m
                left join public.markets mk on mk.market_id = m.market_id
                left join public.market_microstructure_features_daily f
                  on f.market_id = m.market_id
                 and f.day = m.day
                 and f.window_hours = m.window_hours

                where m.market_id = %(market_id)s
                  and m.window_hours = %(window_hours)s
                  and (%(as_of_day)s::date is null or m.day = %(as_of_day)s::date)
                order by m.day desc
                limit 1;
            """
            cur.execute(
                sql_current,
                {"market_id": market_id, "window_hours": window_hours, "as_of_day": as_of_day},
            )
            current = cur.fetchone()

            sql_latest_days = """
                select
                  (select max(day)::text from public.market_microstructure_daily where window_hours = %(window_hours)s)::text as latest_micro_day,
                  (select max(day)::text from public.market_universe_daily where window_hours = %(window_hours)s)::text as latest_universe_day;
            """
            cur.execute(sql_latest_days, {"window_hours": window_hours})
            latest_days = cur.fetchone()
            latest_micro_day = latest_days[0] if latest_days else None
            latest_universe_day = latest_days[1] if latest_days else None

            diagnostics["latest_micro_day"] = latest_micro_day
            diagnostics["latest_universe_day"] = latest_universe_day

            if as_of_day is not None:
                diagnostic_day = str(as_of_day)
            else:
                diagnostic_day = latest_micro_day or latest_universe_day
            diagnostics["diagnostic_day"] = diagnostic_day

            universe_row = None
            if diagnostic_day is not None:
                sql_universe_row = """
                    select
                      u.day::text as day,
                      u.market_id,
                      u.protocol,
                      u.status,
                      u.has_trades_24h,
                      u.has_bbo_24h,
                      u.is_active_24h,
                      u.last_trade_ts,
                      u.last_bbo_ts,
                      u.window_hours
                    from public.market_universe_daily u
                    where u.day = %(day)s::date
                      and u.window_hours = %(window_hours)s::int
                      and u.market_id = %(market_id)s
                    limit 1;
                """
                cur.execute(
                    sql_universe_row,
                    {"day": diagnostic_day, "window_hours": window_hours, "market_id": market_id},
                )
                uni = cur.fetchone()
                if uni:
                    uni_cols = [d[0] for d in cur.description]
                    universe_row = dict(zip(uni_cols, uni))

            diagnostics["universe_row_found"] = universe_row is not None
            diagnostics["universe"] = universe_row

            micro_found = current is not None
            diagnostics["microstructure_row_found"] = micro_found

            features_found = False
            if micro_found:
                colnames = [desc[0] for desc in cur.description]
                current_row_tmp = dict(zip(colnames, current))
                features_found = current_row_tmp.get("market_quality_score") is not None
            diagnostics["features_row_found"] = features_found

            if not current:
                diagnostics["reason"] = "no_data_for_market"
                diagnostics["suggested_actions"] = []

                resp = {
                    "build_id": BUILD_ID,
                    "market_id": market_id,
                    "as_of_day": None,
                    "meta": meta,
                    "resolution": resolution,
                    "current": None,
                    "history": [],
                }
                if include_diagnostics:
                    resp["diagnostics"] = diagnostics
                return resp

            colnames = [desc[0] for desc in cur.description]
            current_row = dict(zip(colnames, current))

            out_meta = {
                "title": current_row.get("title") or (meta_row.get("title") if meta_row else None),
                "url": current_row.get("url") or (meta_row.get("url") if meta_row else None),
            }

            sql_history = """
                with scored as (
                    select
                        m.day,
                        m.market_id,
                        m.window_hours,
                        m.structural_score,

                        1.0 - percent_rank() over (
                            partition by m.day
                            order by m.structural_score asc nulls last
                        ) as structural_percentile,

                        dense_rank() over (
                            partition by m.day
                            order by m.structural_score desc nulls last
                        ) as structural_rank

                    from public.market_microstructure_daily m
                    where m.window_hours = %(window_hours)s
                ),
                hist as (
                    select *
                    from scored
                    where market_id = %(market_id)s
                    order by day desc
                    limit %(lookback_days)s
                )
                select
                    day,
                    structural_score,
                    structural_percentile,
                    structural_rank
                from hist
                order by day asc;
            """
            cur.execute(
                sql_history,
                {"market_id": market_id, "window_hours": window_hours, "lookback_days": lookback_days},
            )
            history_rows = cur.fetchall()
            history_cols = [desc[0] for d in cur.description]
            history = [dict(zip(history_cols, row)) for row in history_rows]

    resp = {
        "build_id": BUILD_ID,
        "market_id": market_id,
        "as_of_day": current_row.get("day"),
        "meta": out_meta,
        "resolution": resolution,
        "current": {
            "microstructure": {
                "volume": current_row.get("volume"),
                "trades": current_row.get("trades"),
                "unique_traders": current_row.get("unique_traders"),
                "top1_trader_share": current_row.get("top1_trader_share"),
                "top5_trader_share": current_row.get("top5_trader_share"),
                "hhi": current_row.get("hhi"),
                "price_volatility": current_row.get("price_volatility"),
                "avg_spread": current_row.get("avg_spread"),
                "bbo_ticks": current_row.get("bbo_ticks"),
                "burst_score": current_row.get("burst_score"),
                "identity_coverage": current_row.get("identity_coverage"),
                "identity_blind": current_row.get("identity_blind"),
                "structural_score": current_row.get("structural_score"),
            },
            "features": {
                "market_quality_score": current_row.get("market_quality_score"),
                "liquidity_health_score": current_row.get("liquidity_health_score"),
                "trading_activity_score": current_row.get("trading_activity_score"),
                "spread_quality_score": current_row.get("spread_quality_score"),
                "volatility_risk_score": current_row.get("volatility_risk_score"),
                "burst_risk_score": current_row.get("burst_risk_score"),
                "concentration_risk_score": current_row.get("concentration_risk_score"),
                "activity_score": current_row.get("activity_score"),
                "spread_score": current_row.get("spread_score"),
                "depth_score": current_row.get("depth_score"),
            },
            "flags": {
                "low_activity_flag": current_row.get("low_activity_flag"),
                "high_concentration_flag": current_row.get("high_concentration_flag"),
                "wide_spread_flag": current_row.get("wide_spread_flag"),
                "high_volatility_flag": current_row.get("high_volatility_flag"),
                "burst_flag": current_row.get("burst_flag"),
                "quality_flags": _to_str_list(current_row.get("quality_flags")),
                "liquidity_flags": _to_str_list(current_row.get("liquidity_flags")),
                "concentration_flags": _to_str_list(current_row.get("concentration_flags")),
            },
        },
        "history": history,
    }
    if include_diagnostics:
        resp["diagnostics"] = diagnostics
    return resp