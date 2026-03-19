from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


def _day_bounds_utc(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def compute_trader_daily_stats(
    day: Optional[date] = None,
    window_hours: int = 24,
) -> Dict[str, Any]:
    """
    Aggregates trades into trader_daily_stats for a given day.

    We define the window as:
      start_ts = day 00:00 UTC
      end_ts = start_ts + window_hours

    If day is None, uses today UTC.
    """

    if day is None:
        day = datetime.now(timezone.utc).date()

    start_ts, end_ts_24h = _day_bounds_utc(day)
    end_ts = start_ts + timedelta(hours=int(window_hours))

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into trader_daily_stats (
                  day,
                  trader_id,
                  trades,
                  volume,
                  buy_trades,
                  sell_trades,
                  buy_volume,
                  sell_volume,
                  markets_traded,
                  first_seen_at,
                  last_seen_at
                )
                select
                  %s::date as day,
                  coalesce(nullif(trader_id, ''), 'unknown') as trader_id,
                  count(*)::int as trades,
                  coalesce(sum(notional), 0)::numeric as volume,
                  sum(case when side = 'BUY' then 1 else 0 end)::int as buy_trades,
                  sum(case when side = 'SELL' then 1 else 0 end)::int as sell_trades,
                  coalesce(sum(case when side = 'BUY' then notional else 0 end), 0)::numeric as buy_volume,
                  coalesce(sum(case when side = 'SELL' then notional else 0 end), 0)::numeric as sell_volume,
                  count(distinct market_id)::int as markets_traded,
                  min(ts) as first_seen_at,
                  max(ts) as last_seen_at
                from trades
                where ts >= %s
                  and ts < %s
                group by 2
                on conflict (day, trader_id)
                do update set
                  trades = excluded.trades,
                  volume = excluded.volume,
                  buy_trades = excluded.buy_trades,
                  sell_trades = excluded.sell_trades,
                  buy_volume = excluded.buy_volume,
                  sell_volume = excluded.sell_volume,
                  markets_traded = excluded.markets_traded,
                  first_seen_at = excluded.first_seen_at,
                  last_seen_at = excluded.last_seen_at;
                """,
                (day, start_ts, end_ts),
            )

            # Count how many traders were written for that day
            cur.execute(
                "select count(*) from trader_daily_stats where day = %s;",
                (day,),
            )
            traders_written = int(cur.fetchone()[0])

            conn.commit()

    return {
        "day": str(day),
        "window_hours": int(window_hours),
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "traders_written": traders_written,
    }


def compute_trader_labels_daily(
    day: Optional[date] = None,
    whale_volume_threshold: float = 1000.0,
    farmer_markets_threshold: int = 10,
) -> Dict[str, Any]:
    """
    Creates simple cohort labels using trader_daily_stats.
    This is a first pass. We will refine later.

    Labels:
      whale: volume >= whale_volume_threshold
      farmer: markets_traded >= farmer_markets_threshold
      new_trader: first_seen_at within the day window
    """

    if day is None:
        day = datetime.now(timezone.utc).date()

    start_ts, _ = _day_bounds_utc(day)

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            # Clear existing labels for that day so reruns are clean
            cur.execute("delete from trader_labels_daily where day = %s;", (day,))

            # whale
            cur.execute(
                """
                insert into trader_labels_daily (day, trader_id, label, score, details)
                select
                  day,
                  trader_id,
                  'whale' as label,
                  volume as score,
                  jsonb_build_object('volume', volume)
                from trader_daily_stats
                where day = %s
                  and volume >= %s::numeric;
                """,
                (day, whale_volume_threshold),
            )

            # farmer
            cur.execute(
                """
                insert into trader_labels_daily (day, trader_id, label, score, details)
                select
                  day,
                  trader_id,
                  'farmer' as label,
                  markets_traded as score,
                  jsonb_build_object('markets_traded', markets_traded)
                from trader_daily_stats
                where day = %s
                  and markets_traded >= %s;
                """,
                (day, farmer_markets_threshold),
            )

            # new_trader
            cur.execute(
                """
                insert into trader_labels_daily (day, trader_id, label, score, details)
                select
                  day,
                  trader_id,
                  'new_trader' as label,
                  1 as score,
                  jsonb_build_object('first_seen_at', first_seen_at)
                from trader_daily_stats
                where day = %s
                  and first_seen_at >= %s;
                """,
                (day, start_ts),
            )

            # Count labels written
            cur.execute("select count(*) from trader_labels_daily where day = %s;", (day,))
            labels_written = int(cur.fetchone()[0])

            conn.commit()

    return {
        "day": str(day),
        "labels_written": labels_written,
        "whale_volume_threshold": whale_volume_threshold,
        "farmer_markets_threshold": farmer_markets_threshold,
    }