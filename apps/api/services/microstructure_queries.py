from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg

from apps.api.db import get_db_dsn

# Only allow sorting by these known fields from market_microstructure_daily
ALLOWED_SORT_COLUMNS = {
    "structural_score": "m.structural_score",
    "trades": "m.trades",
    "volume": "m.volume",
    "avg_spread": "m.avg_spread",
    "price_volatility": "m.price_volatility",
    "bbo_ticks": "m.bbo_ticks",
    "burst_score": "m.burst_score",
    "identity_coverage": "m.identity_coverage",
    "hhi": "m.hhi",
}


def latest_day(protocol: str = "polymarket") -> Dict[str, Any]:
    """
    Returns latest day present in market_microstructure_daily and row count for that day.
    """
    sql = """
    with d as (
      select max(m.day) as day
      from market_microstructure_daily m
      join markets mk on mk.market_id = m.market_id
      where mk.protocol = %(protocol)s
    )
    select
      d.day as day,
      coalesce((
        select count(*)
        from market_microstructure_daily m
        where m.day = d.day
      ), 0) as rows
    from d;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"protocol": protocol})
            row = cur.fetchone()

    if not row or row[0] is None:
        return {"day": None, "rows": 0}

    return {"day": row[0].isoformat(), "rows": int(row[1])}


def top_microstructure(
    day: date,
    limit: int = 20,
    sort: str = "structural_score",
    order: str = "desc",
    protocol: str = "polymarket",
    window_hours: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Returns the 'top' markets for a given day from market_microstructure_daily,
    joined to markets table to provide title and url.

    window_hours optional: if provided, filters to that window_hours.
    """
    sort_expr = ALLOWED_SORT_COLUMNS.get(sort, "m.structural_score")
    order_sql = "asc" if order == "asc" else "desc"
    limit = max(1, min(int(limit), 5000))

    wh_filter = ""
    params: Dict[str, Any] = {"day": day, "limit": limit, "protocol": protocol}
    if window_hours is not None:
        wh_filter = "and m.window_hours = %(window_hours)s"
        params["window_hours"] = int(window_hours)

    sql = f"""
    with ranked as (
      select
        m.*,
        dense_rank() over (order by {sort_expr} {order_sql} nulls last) as structural_rank
      from market_microstructure_daily m
      join markets mk on mk.market_id = m.market_id
      where m.day = %(day)s::date
        and mk.protocol = %(protocol)s
        {wh_filter}
    )
    select
      r.market_id,
      mk.title,
      mk.url,
      r.day,
      r.window_hours,

      r.volume,
      r.trades,
      r.unique_traders,

      r.identity_coverage,
      r.identity_blind,

      r.top1_trader_share,
      r.top5_trader_share,
      r.hhi,

      r.price_volatility,

      r.bbo_ticks,
      r.avg_spread,

      r.suspicious_burst_flag,
      r.burst_score,

      r.structural_score,
      r.structural_rank
    from ranked r
    join markets mk on mk.market_id = r.market_id
    order by {sort_expr} {order_sql} nulls last
    limit %(limit)s;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(zip(cols, r))
        if d.get("day") is not None:
            d["day"] = d["day"].isoformat()
        out.append(d)
    return out