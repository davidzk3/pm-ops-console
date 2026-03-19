from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def compute_market_universe_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
    protocol: str = "polymarket",
) -> Dict[str, Any]:
    """
    Universe B layer:
    Write the set of markets that were ACTIVE in the lookback window.

    Active means: has trades OR has bbo ticks in the window.

    Stores into market_universe_daily with full columns:
      day, market_id, protocol, status,
      has_trades_24h, has_bbo_24h, is_active_24h,
      last_trade_ts, last_bbo_ts,
      window_hours
    """
    if day is None:
        day = _utc_today()

    end_ts = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
    start_ts = end_ts - timedelta(hours=window_hours)

    sql = """
    with trades_agg as (
      select
        t.market_id,
        true as has_trades_24h,
        max(t.ts) as last_trade_ts
      from trades t
      join markets m on m.market_id = t.market_id
      where m.protocol = %(protocol)s
        and t.ts >= %(start_ts)s
        and t.ts < %(end_ts)s
      group by 1
    ),
    bbo_agg as (
      select
        b.market_id,
        true as has_bbo_24h,
        max(b.ts) as last_bbo_ts
      from market_bbo_ticks b
      join markets m on m.market_id = b.market_id
      where m.protocol = %(protocol)s
        and b.ts >= %(start_ts)s
        and b.ts < %(end_ts)s
      group by 1
    ),
    active_union as (
      select market_id from trades_agg
      union
      select market_id from bbo_agg
    ),
    limited as (
      select au.market_id
      from active_union au
      order by au.market_id
      limit %(limit_markets)s
    ),
    enriched as (
      select
        %(day)s::date as day,
        l.market_id,
        m.protocol,
        m.status,
        coalesce(t.has_trades_24h, false) as has_trades_24h,
        coalesce(b.has_bbo_24h, false) as has_bbo_24h,
        (coalesce(t.has_trades_24h, false) or coalesce(b.has_bbo_24h, false)) as is_active_24h,
        t.last_trade_ts,
        b.last_bbo_ts,
        %(window_hours)s::int as window_hours
      from limited l
      join markets m on m.market_id = l.market_id
      left join trades_agg t on t.market_id = l.market_id
      left join bbo_agg b on b.market_id = l.market_id
      where m.protocol = %(protocol)s
    )
    insert into market_universe_daily (
      day,
      market_id,
      protocol,
      status,
      has_trades_24h,
      has_bbo_24h,
      is_active_24h,
      last_trade_ts,
      last_bbo_ts,
      window_hours
    )
    select
      day,
      market_id,
      protocol,
      status,
      has_trades_24h,
      has_bbo_24h,
      is_active_24h,
      last_trade_ts,
      last_bbo_ts,
      window_hours
    from enriched
    on conflict (market_id, day, window_hours) do update set
      protocol = excluded.protocol,
      status = excluded.status,
      has_trades_24h = excluded.has_trades_24h,
      has_bbo_24h = excluded.has_bbo_24h,
      is_active_24h = excluded.is_active_24h,
      last_trade_ts = excluded.last_trade_ts,
      last_bbo_ts = excluded.last_bbo_ts;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "window_hours": window_hours,
                    "limit_markets": limit_markets,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "protocol": protocol,
                },
            )
            rows_written = cur.rowcount if cur.rowcount is not None else 0
            conn.commit()

    return {
        "day": str(day),
        "window_hours": window_hours,
        "limit_markets": limit_markets,
        "rows_written": rows_written,
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "protocol": protocol,
        "status": "ok",
    }