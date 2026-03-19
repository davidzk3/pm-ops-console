from datetime import date
from typing import Optional, Dict, Any

import psycopg
from apps.api.db import get_db_dsn


ENGINE_VERSION = "trader_role_v1_2026_03_08"


def compute_trader_role_daily(
    day: Optional[date] = None,
    limit_markets: int = 500,
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    sql_create = """
    create table if not exists public.trader_role_daily (
        market_id text not null,
        trader_id text not null,
        day date not null,

        role text not null,
        confidence double precision not null default 0,

        trades int,
        buy_trades int,
        sell_trades int,

        volume numeric,
        avg_trade_size numeric,
        buy_ratio double precision,
        market_volume_share double precision,
        active_minutes int,

        is_large_participant boolean,
        is_one_sided boolean,
        is_high_frequency boolean,

        supporting_flags text[],
        created_at timestamptz not null default now(),

        primary key (market_id, trader_id, day)
    );

    create index if not exists idx_trader_role_day
        on public.trader_role_daily(day);

    create index if not exists idx_trader_role_market_day
        on public.trader_role_daily(market_id, day);

    create index if not exists idx_trader_role_role_day
        on public.trader_role_daily(role, day);
    """

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

tb as (
    select
        t.market_id,
        t.trader_id,
        t.day,
        t.trades,
        t.buy_trades,
        t.sell_trades,
        t.volume,
        t.avg_trade_size,
        t.buy_ratio,
        t.market_volume_share,
        t.active_minutes,
        t.is_large_participant,
        t.is_one_sided,
        t.is_high_frequency
    from public.trader_behavior_daily t
    join eligible_markets e
      on e.market_id = t.market_id
    join params p
      on t.day = p.day
),

market_ctx as (
    select
        mm.market_id,
        mm.day,
        coalesce(mm.trades, 0)::int as market_trades,
        coalesce(mm.unique_traders, 0)::int as market_unique_traders,
        mm.top1_trader_share,
        mm.top5_trader_share,
        mm.hhi,
        mm.avg_spread,
        mm.price_volatility,
        mm.burst_score,
        mm.volume as market_volume,

        mf.market_quality_score,
        mf.liquidity_health_score,
        mf.concentration_risk_score,
        coalesce(mf.low_activity_flag, false) as low_activity_flag,
        coalesce(mf.high_concentration_flag, false) as high_concentration_flag,
        coalesce(mf.wide_spread_flag, false) as wide_spread_flag,
        coalesce(mf.high_volatility_flag, false) as high_volatility_flag,
        coalesce(mf.burst_flag, false) as burst_flag
    from public.market_microstructure_daily mm
    join eligible_markets e
      on e.market_id = mm.market_id
    join params p
      on mm.day = p.day
    left join public.market_microstructure_features_daily mf
      on mf.market_id = mm.market_id
     and mf.day = mm.day
     and mf.window_hours = mm.window_hours
),

classified as (
    select
        tb.market_id,
        tb.trader_id,
        tb.day,

        tb.trades,
        tb.buy_trades,
        tb.sell_trades,

        tb.volume,
        tb.avg_trade_size,
        tb.buy_ratio,
        tb.market_volume_share,
        tb.active_minutes,

        tb.is_large_participant,
        tb.is_one_sided,
        tb.is_high_frequency,

        case
            when tb.is_large_participant
                 and coalesce(tb.market_volume_share, 0) >= 0.50
                then 'whale'

            when tb.is_high_frequency
                 and tb.is_one_sided
                 and coalesce(tb.market_volume_share, 0) < 0.10
                then 'possible_farmer'

            when tb.is_high_frequency
                 and coalesce(tb.market_volume_share, 0) >= 0.10
                then 'high_frequency_trader'

            when tb.trades >= 3
                 and coalesce(tb.active_minutes, 0) >= 60
                 and coalesce(tb.market_volume_share, 0) between 0.01 and 0.20
                 and coalesce(tb.buy_ratio, 0.5) between 0.35 and 0.65
                then 'maker_like'

            when tb.is_one_sided
                 and coalesce(tb.market_volume_share, 0) >= 0.10
                then 'one_sided_speculator'

            else 'organic_participant'
        end as role,

        case
            when tb.is_large_participant
                 and coalesce(tb.market_volume_share, 0) >= 0.50
                then least(1.0, 0.70 + coalesce(tb.market_volume_share, 0) * 0.30)

            when tb.is_high_frequency
                 and tb.is_one_sided
                 and coalesce(tb.market_volume_share, 0) < 0.10
                then 0.80

            when tb.is_high_frequency
                 and coalesce(tb.market_volume_share, 0) >= 0.10
                then 0.78

            when tb.trades >= 3
                 and coalesce(tb.active_minutes, 0) >= 60
                 and coalesce(tb.market_volume_share, 0) between 0.01 and 0.20
                 and coalesce(tb.buy_ratio, 0.5) between 0.35 and 0.65
                then 0.72

            when tb.is_one_sided
                 and coalesce(tb.market_volume_share, 0) >= 0.10
                then 0.76

            else 0.60
        end as confidence,

        array_remove(array[
            case when tb.is_large_participant then 'large_participant' end,
            case when tb.is_one_sided then 'one_sided' end,
            case when tb.is_high_frequency then 'high_frequency' end,
            case when coalesce(tb.market_volume_share, 0) >= 0.50 then 'dominant_volume_share' end,
            case when coalesce(tb.buy_ratio, 0.5) in (0, 1) then 'fully_directional' end,
            case when coalesce(tb.active_minutes, 0) >= 60 then 'persistent_activity' end
        ]::text[], null) as supporting_flags

    from tb
    left join market_ctx mc
      on mc.market_id = tb.market_id
     and mc.day = tb.day
),

upserted as (
    insert into public.trader_role_daily (
        market_id,
        trader_id,
        day,
        role,
        confidence,
        trades,
        buy_trades,
        sell_trades,
        volume,
        avg_trade_size,
        buy_ratio,
        market_volume_share,
        active_minutes,
        is_large_participant,
        is_one_sided,
        is_high_frequency,
        supporting_flags
    )
    select
        c.market_id,
        c.trader_id,
        c.day,
        c.role,
        c.confidence,
        c.trades,
        c.buy_trades,
        c.sell_trades,
        c.volume,
        c.avg_trade_size,
        c.buy_ratio,
        c.market_volume_share,
        c.active_minutes,
        c.is_large_participant,
        c.is_one_sided,
        c.is_high_frequency,
        c.supporting_flags
    from classified c
    on conflict (market_id, trader_id, day)
    do update set
        role = excluded.role,
        confidence = excluded.confidence,
        trades = excluded.trades,
        buy_trades = excluded.buy_trades,
        sell_trades = excluded.sell_trades,
        volume = excluded.volume,
        avg_trade_size = excluded.avg_trade_size,
        buy_ratio = excluded.buy_ratio,
        market_volume_share = excluded.market_volume_share,
        active_minutes = excluded.active_minutes,
        is_large_participant = excluded.is_large_participant,
        is_one_sided = excluded.is_one_sided,
        is_high_frequency = excluded.is_high_frequency,
        supporting_flags = excluded.supporting_flags
    returning 1
)

select count(*)::int as rows_written
from upserted;
"""

    effective_day = day or date.today()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_create)
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