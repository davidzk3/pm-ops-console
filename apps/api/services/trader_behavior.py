from datetime import date
from typing import Optional, Dict, Any

import psycopg
from apps.api.db import get_db_dsn


ENGINE_VERSION = "trader_behavior_v1_2026_03_06_B"


def compute_trader_behavior_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    sql = """
with params as (
    select
        coalesce(%(day)s::date, current_date) as day,
        %(market_id)s::text as market_id
),

eligible_markets as (
    select p.market_id
    from params p
    where p.market_id is not null

    union all

    select x.market_id
    from (
        select m.market_id
        from public.markets m
        join params p on true
        where p.market_id is null
          and m.protocol = 'polymarket'
        order by m.market_id
        limit %(limit_markets)s
    ) x
),

trades_day as (
    select
        t.market_id,
        t.trader_id,
        t.day,
        t.side,
        t.price,
        t.size,
        t.notional,
        t.ts,
        t.source
    from public.trades t
    join eligible_markets e
      on e.market_id = t.market_id
    join params p
      on t.day = p.day
),

market_volume as (
    select
        td.market_id,
        sum(td.notional) as total_volume
    from trades_day td
    group by td.market_id
),

agg as (
    select
        td.market_id,
        td.trader_id,
        td.day,

        count(*)::int as trades,
        sum(case when td.side = 'BUY' then 1 else 0 end)::int as buy_trades,
        sum(case when td.side = 'SELL' then 1 else 0 end)::int as sell_trades,

        sum(td.notional) as volume,
        avg(td.notional) as avg_trade_size,

        min(td.ts) as first_trade_ts,
        max(td.ts) as last_trade_ts,

        floor(extract(epoch from (max(td.ts) - min(td.ts))) / 60.0)::int as active_minutes

    from trades_day td
    group by td.market_id, td.trader_id, td.day
),

final as (
    select
        a.market_id,
        a.trader_id,
        a.day,

        a.trades,
        a.buy_trades,
        a.sell_trades,

        a.volume,
        a.avg_trade_size,

        a.first_trade_ts,
        a.last_trade_ts,

        case
            when a.trades > 0
            then a.buy_trades::double precision / a.trades::double precision
            else 0.5
        end as buy_ratio,

        case
            when mv.total_volume > 0
            then a.volume::double precision / mv.total_volume::double precision
            else 0
        end as market_volume_share,

        coalesce(a.active_minutes, 0) as active_minutes,

        (
            case
                when mv.total_volume > 0
                then a.volume::double precision / mv.total_volume::double precision
                else 0
            end
        ) > 0.10 as is_large_participant,

        abs(
            (
                case
                    when a.trades > 0
                    then a.buy_trades::double precision / a.trades::double precision
                    else 0.5
                end
            ) - 0.5
        ) > 0.35 as is_one_sided,

        a.trades >= 50 as is_high_frequency

    from agg a
    join market_volume mv
      on mv.market_id = a.market_id
),

upserted as (
    insert into public.trader_behavior_daily (
        market_id,
        trader_id,
        day,
        trades,
        buy_trades,
        sell_trades,
        volume,
        avg_trade_size,
        first_trade_ts,
        last_trade_ts,
        buy_ratio,
        market_volume_share,
        active_minutes,
        is_large_participant,
        is_one_sided,
        is_high_frequency
    )
    select
        f.market_id,
        f.trader_id,
        f.day,
        f.trades,
        f.buy_trades,
        f.sell_trades,
        f.volume,
        f.avg_trade_size,
        f.first_trade_ts,
        f.last_trade_ts,
        f.buy_ratio,
        f.market_volume_share,
        f.active_minutes,
        f.is_large_participant,
        f.is_one_sided,
        f.is_high_frequency
    from final f
    on conflict (market_id, trader_id, day)
    do update set
        trades = excluded.trades,
        buy_trades = excluded.buy_trades,
        sell_trades = excluded.sell_trades,
        volume = excluded.volume,
        avg_trade_size = excluded.avg_trade_size,
        first_trade_ts = excluded.first_trade_ts,
        last_trade_ts = excluded.last_trade_ts,
        buy_ratio = excluded.buy_ratio,
        market_volume_share = excluded.market_volume_share,
        active_minutes = excluded.active_minutes,
        is_large_participant = excluded.is_large_participant,
        is_one_sided = excluded.is_one_sided,
        is_high_frequency = excluded.is_high_frequency
    returning 1
)

select count(*)::int as rows_written
from upserted;
"""

    effective_day = day or date.today()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "day": day,
                    "limit_markets": limit_markets,
                    "market_id": market_id,
                },
            )
            row = cur.fetchone()
            rows_written = row[0] if row else 0
            conn.commit()

    return {
        "engine_version": ENGINE_VERSION,
        "day": str(effective_day),
        "limit_markets": limit_markets,
        "market_id": market_id,
        "rows_written": rows_written,
        "status": "ok",
    }