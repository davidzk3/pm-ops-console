from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn


ENGINE_VERSION = "microstructure_v2_sql_gated_2026_03_02_active_universe_2026_03_03"


def compute_microstructure_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    if day is None:
        day = date.today()

    end_ts = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
    start_ts = end_ts - timedelta(hours=window_hours)

    sql = """
with active_trade_markets as (
  select distinct t.market_id
  from trades t
  where t.ts >= %(start_ts)s
    and t.ts < %(end_ts)s
),
active_bbo_markets as (
  select distinct b.market_id
  from market_bbo_ticks b
  where b.ts >= %(start_ts)s
    and b.ts < %(end_ts)s
),
active_markets as (
  select market_id from active_trade_markets
  union
  select market_id from active_bbo_markets
),
eligible_markets as (
  select m.market_id
  from markets m
  join active_markets a on a.market_id = m.market_id
  where m.protocol = 'polymarket'
    and coalesce(m.external_id, '') <> ''
  order by m.market_id
  limit %(limit_markets)s
),

trades_window as (
  select
    t.market_id,
    t.trader_id,
    t.side,
    t.ts,
    t.notional::numeric as notional,
    t.price::numeric as price
  from trades t
  join eligible_markets e on e.market_id = t.market_id
  where t.ts >= %(start_ts)s
    and t.ts < %(end_ts)s
),

trade_aggs as (
  select
    market_id,
    coalesce(sum(notional),0) as volume,
    count(*)::int as trades,
    count(distinct trader_id)::int as unique_traders
  from trades_window
  group by 1
),

trader_shares as (
  select market_id, trader_id, sum(notional) as trader_volume
  from trades_window
  group by 1,2
),

total_volume as (
  select market_id, sum(trader_volume) as total_volume
  from trader_shares
  group by 1
),

ranked_shares as (
  select
    s.market_id,
    s.trader_volume / nullif(tv.total_volume,0) as share,
    row_number() over (partition by s.market_id order by s.trader_volume desc nulls last) as rn
  from trader_shares s
  join total_volume tv on tv.market_id = s.market_id
),

concentration as (
  select
    market_id,
    max(share) as top1_trader_share,
    sum(share) filter (where rn <= 5) as top5_trader_share,
    sum(power(share,2)) as hhi
  from ranked_shares
  group by 1
),

price_series as (
  select
    market_id,
    ts,
    price,
    lag(price) over (partition by market_id order by ts) as prev_price
  from trades_window
),

price_moves as (
  select
    market_id,
    abs(price - prev_price) as abs_move
  from price_series
  where prev_price is not null
),

price_vol as (
  select
    market_id,
    coalesce(avg(abs_move),0) as price_volatility
  from price_moves
  group by 1
),

bbo_window as (
  select
    b.market_id,
    b.spread::numeric as spread
  from market_bbo_ticks b
  join eligible_markets e on e.market_id = b.market_id
  where b.ts >= %(start_ts)s
    and b.ts < %(end_ts)s
),

bbo_aggs as (
  select
    market_id,
    count(*)::int as bbo_ticks,
    coalesce(avg(spread),0) as avg_spread
  from bbo_window
  group by 1
),

burst as (
  select
    market_id,
    case when coalesce(trades,0) >= 100 and coalesce(unique_traders,0) <= 2 then true else false end as suspicious_burst_flag,
    case
      when coalesce(trades,0) >= 200 and coalesce(unique_traders,0) <= 2 then 2.0
      when coalesce(trades,0) >= 100 and coalesce(unique_traders,0) <= 2 then 1.0
      else 0.0
    end as burst_score
  from trade_aggs
),

identity_flags as (
  select
    e.market_id,
    0.0::double precision as identity_coverage,
    false as identity_blind
  from eligible_markets e
),

final as (
  select
    e.market_id,
    %(day)s::date as day,
    %(window_hours)s::int as window_hours,

    coalesce(t.volume,0)::numeric(30,10) as volume,
    coalesce(t.trades,0)::int as trades,
    coalesce(t.unique_traders,0)::int as unique_traders,

    c.top1_trader_share::numeric(18,8) as top1_trader_share,
    c.top5_trader_share::numeric(18,8) as top5_trader_share,
    c.hhi::numeric(18,8) as hhi,

    pv.price_volatility::numeric(18,8) as price_volatility,
    coalesce(b.bbo_ticks,0)::int as bbo_ticks,
    coalesce(b.avg_spread,0)::numeric(18,8) as avg_spread,

    coalesce(br.suspicious_burst_flag,false) as suspicious_burst_flag,
    coalesce(br.burst_score,0.0) as burst_score,

    idf.identity_coverage,
    idf.identity_blind,

    greatest(
      0.0,
      least(
        1.0,
        (
          (
              0.25 * greatest(0.0, least(1.0,
                ln(1 + coalesce(t.trades,0)::double precision) / ln(1 + 300.0)
              ))
            + 0.25 * greatest(0.0, least(1.0,
                ln(1 + coalesce(t.unique_traders,0)::double precision) / ln(1 + 60.0)
              ))
            + 0.10 * greatest(0.0, least(1.0,
                ln(1 + coalesce(t.volume,0)::double precision) / ln(1 + 250000.0)
              ))

            + 0.20 * (
                1.0 - greatest(
                  0.0,
                  least(
                    1.0,
                    (coalesce(b.avg_spread,0)::double precision - 0.0005) / (0.02 - 0.0005)
                  )
                )
              )
            + 0.10 * greatest(0.0, least(1.0,
                ln(1 + coalesce(b.bbo_ticks,0)::double precision) / ln(1 + 500.0)
              ))

            + 0.05 * (1.0 - greatest(0.0, least(1.0, coalesce(c.hhi,1.0)::double precision)))
            + 0.05 * (1.0 - greatest(0.0, least(1.0, coalesce(c.top1_trader_share,1.0)::double precision)))
          )

          * (
              case
                when coalesce(t.trades,0) < 5 and coalesce(t.unique_traders,0) < 2 and coalesce(b.bbo_ticks,0) = 0 then 0.01
                when coalesce(t.trades,0) < 5 and coalesce(t.unique_traders,0) < 2 and coalesce(b.bbo_ticks,0) > 0 then 0.50
                else 1.0
              end
            )

          * (case when coalesce(c.top1_trader_share,1.0)::double precision >= 0.95
                   or coalesce(c.hhi,1.0)::double precision >= 0.90
                 then 0.02 else 1.0 end)

          * (1.0 - greatest(0.0, least(1.0, coalesce(br.burst_score,0.0)::double precision / 3.0)))

          * (case when coalesce(idf.identity_blind,false) then 0.25 else 1.0 end)
        )
      )
    )::numeric(18,8) as structural_score

  from eligible_markets e
  left join trade_aggs t on t.market_id = e.market_id
  left join concentration c on c.market_id = e.market_id
  left join price_vol pv on pv.market_id = e.market_id
  left join bbo_aggs b on b.market_id = e.market_id
  left join burst br on br.market_id = e.market_id
  left join identity_flags idf on idf.market_id = e.market_id
)

insert into market_microstructure_daily (
  market_id,
  day,
  window_hours,
  volume,
  trades,
  unique_traders,
  top1_trader_share,
  top5_trader_share,
  hhi,
  price_volatility,
  bbo_ticks,
  avg_spread,
  suspicious_burst_flag,
  burst_score,
  identity_coverage,
  identity_blind,
  structural_score
)
select
  market_id,
  day,
  window_hours,
  volume,
  trades,
  unique_traders,
  top1_trader_share,
  top5_trader_share,
  hhi,
  price_volatility,
  bbo_ticks,
  avg_spread,
  suspicious_burst_flag,
  burst_score,
  identity_coverage,
  identity_blind,
  structural_score
from final
on conflict (market_id, day) do update set
  window_hours = excluded.window_hours,
  volume = excluded.volume,
  trades = excluded.trades,
  unique_traders = excluded.unique_traders,
  top1_trader_share = excluded.top1_trader_share,
  top5_trader_share = excluded.top5_trader_share,
  hhi = excluded.hhi,
  price_volatility = excluded.price_volatility,
  bbo_ticks = excluded.bbo_ticks,
  avg_spread = excluded.avg_spread,
  suspicious_burst_flag = excluded.suspicious_burst_flag,
  burst_score = excluded.burst_score,
  identity_coverage = excluded.identity_coverage,
  identity_blind = excluded.identity_blind,
  structural_score = excluded.structural_score,
  created_at = now()
returning market_id;
    """
    bridge_sql = """
insert into marts.market_day (
  market_id,
  day,
  volume,
  trades,
  unique_traders,
  spread_median,
  depth_2pct_median,
  concentration_hhi,
  health_score,
  risk_score,
  flags
)
select
  market_id,
  day,
  volume,
  trades,
  unique_traders,
  avg_spread as spread_median,
  null::numeric as depth_2pct_median,
  hhi as concentration_hhi,
  round((structural_score * 100.0)::numeric, 4) as health_score,
  round(((1.0 - structural_score) * 100.0)::numeric, 4) as risk_score,
  to_jsonb(array_remove(array[
  case when suspicious_burst_flag then 'SUSPICIOUS_BURST'::text end,
  case when identity_blind then 'IDENTITY_BLIND'::text end,
  case when coalesce(hhi, 0) >= 0.90 then 'WHALE_DOMINANCE'::text end,
  case when coalesce(avg_spread, 0) >= 0.02 then 'SPREAD_BLOWOUT'::text end
], null::text))
from market_microstructure_daily
where day = %(day)s::date
  and window_hours = %(window_hours)s::int
on conflict (market_id, day) do update set
  volume = excluded.volume,
  trades = excluded.trades,
  unique_traders = excluded.unique_traders,
  spread_median = excluded.spread_median,
  depth_2pct_median = excluded.depth_2pct_median,
  concentration_hhi = excluded.concentration_hhi,
  health_score = excluded.health_score,
  risk_score = excluded.risk_score,
  flags = excluded.flags;
    """

    params = {
        "day": day,
        "window_hours": window_hours,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "limit_markets": limit_markets,
    }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            written = cur.rowcount

            cur.execute(bridge_sql, params)

        conn.commit()


    return {
        "engine_version": ENGINE_VERSION,
        "day": day.isoformat(),
        "window_hours": window_hours,
        "markets_written": int(written),
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "status": "ok",
    }