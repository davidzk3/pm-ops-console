from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import psycopg

from apps.api.db import get_db_dsn


def get_microstructure_ranked(
    day: Optional[date] = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str = "structural_score",
    order_dir: str = "desc",
) -> Dict[str, Any]:
    if day is None:
        day = _utc_today()

    allowed_order = {
        "structural_score",
        "volume",
        "trades",
        "unique_traders",
        "bbo_ticks",
        "avg_spread",
        "price_volatility",
        "top1_trader_share",
        "top5_trader_share",
        "hhi",
        "buy_sell_imbalance",
    }
    if order_by not in allowed_order:
        order_by = "structural_score"

    order_dir = order_dir.lower().strip()
    if order_dir not in ("asc", "desc"):
        order_dir = "desc"

    sql = f"""
    select
      ms.day,
      ms.market_id,
      m.title,
      m.url,
      ms.window_hours,
      ms.volume,
      ms.trades,
      ms.unique_traders,
      ms.avg_trade_notional,
      ms.median_trade_notional,
      ms.buy_trades,
      ms.sell_trades,
      ms.buy_sell_imbalance,
      ms.top1_trader_share,
      ms.top5_trader_share,
      ms.hhi,
      ms.price_volatility,
      ms.bbo_ticks,
      ms.avg_spread,
      ms.spread_volatility,
      ms.structural_score
    from market_microstructure_daily ms
    join markets m on m.market_id = ms.market_id
    where ms.day = %(day)s
    order by {order_by} {order_dir} nulls last
    limit %(limit)s
    offset %(offset)s;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"day": day, "limit": limit, "offset": offset})
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    return {"day": str(day), "rows": rows, "limit": limit, "offset": offset, "order_by": order_by, "order_dir": order_dir}