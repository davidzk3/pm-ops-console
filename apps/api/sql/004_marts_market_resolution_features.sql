create schema if not exists marts;

create table if not exists marts.market_resolution_features (
  market_id text primary key,

  end_date timestamptz,
  closed_time timestamptz,
  resolved_at timestamptz,

  close_lag_seconds double precision,
  resolve_lag_seconds double precision,

  pre_close_trade_count_24h bigint not null default 0,
  pre_close_unique_traders_24h bigint not null default 0,
  pre_close_notional_24h numeric(38,8) not null default 0,

  last_trade_ts timestamptz,
  last_trade_price numeric(18,8),

  final_hour_bbo_ticks bigint not null default 0,
  final_hour_spread_median numeric(18,8),

  last_bbo_ts timestamptz,
  last_best_bid numeric(18,8),
  last_best_ask numeric(18,8),
  last_spread numeric(18,8),

  computed_at timestamptz not null default now()
);

create index if not exists idx_mrf_closed_time on marts.market_resolution_features (closed_time);
create index if not exists idx_mrf_resolved_at on marts.market_resolution_features (resolved_at);

with base as (
  select
    m.market_id,
    m.end_date,
    m.closed_time,
    m.resolved_at,
    coalesce(m.closed_time, m.end_date, m.resolved_at) as close_ts
  from public.markets m
  where m.protocol = 'polymarket'
    and coalesce(m.closed, false) = true
),
trade_24h as (
  select
    b.market_id,
    count(*) as trade_count_24h,
    count(distinct t.trader_id) as unique_traders_24h,
    coalesce(sum(t.notional), 0) as notional_24h
  from base b
  join public.trades t
    on t.market_id = b.market_id
   and t.ts >= (b.close_ts - interval '24 hours')
   and t.ts <  b.close_ts
  group by 1
),
last_trade as (
  select distinct on (t.market_id)
    t.market_id,
    t.ts as last_trade_ts,
    t.price as last_trade_price
  from public.trades t
  join base b on b.market_id = t.market_id
  where t.ts < b.close_ts
  order by t.market_id, t.ts desc
),
bbo_1h as (
  select
    b.market_id,
    count(*) as bbo_ticks_1h,
    percentile_cont(0.5) within group (order by mb.spread) as spread_median_1h
  from base b
  join public.market_bbo_ticks mb
    on mb.market_id = b.market_id
   and mb.ts >= (b.close_ts - interval '1 hour')
   and mb.ts <  b.close_ts
  where mb.spread is not null
  group by 1
),
last_bbo as (
  select distinct on (mb.market_id)
    mb.market_id,
    mb.ts as last_bbo_ts,
    mb.best_bid as last_best_bid,
    mb.best_ask as last_best_ask,
    mb.spread as last_spread
  from public.market_bbo_ticks mb
  join base b on b.market_id = mb.market_id
  where mb.ts < b.close_ts
  order by mb.market_id, mb.ts desc
)
insert into marts.market_resolution_features (
  market_id, end_date, closed_time, resolved_at,
  close_lag_seconds, resolve_lag_seconds,
  pre_close_trade_count_24h, pre_close_unique_traders_24h, pre_close_notional_24h,
  last_trade_ts, last_trade_price,
  final_hour_bbo_ticks, final_hour_spread_median,
  last_bbo_ts, last_best_bid, last_best_ask, last_spread,
  computed_at
)
select
  b.market_id,
  b.end_date,
  b.closed_time,
  b.resolved_at,

  case
    when b.end_date is not null and b.closed_time is not null
      then extract(epoch from (b.closed_time - b.end_date))
    else null
  end as close_lag_seconds,

  case
    when b.closed_time is not null and b.resolved_at is not null
      then extract(epoch from (b.resolved_at - b.closed_time))
    else null
  end as resolve_lag_seconds,

  coalesce(t24.trade_count_24h, 0),
  coalesce(t24.unique_traders_24h, 0),
  coalesce(t24.notional_24h, 0),

  lt.last_trade_ts,
  lt.last_trade_price,

  coalesce(b1.bbo_ticks_1h, 0),
  b1.spread_median_1h,

  lb.last_bbo_ts,
  lb.last_best_bid,
  lb.last_best_ask,
  lb.last_spread,

  now()
from base b
left join trade_24h t24 on t24.market_id = b.market_id
left join last_trade lt on lt.market_id = b.market_id
left join bbo_1h b1 on b1.market_id = b.market_id
left join last_bbo lb on lb.market_id = b.market_id
on conflict (market_id) do update set
  end_date = excluded.end_date,
  closed_time = excluded.closed_time,
  resolved_at = excluded.resolved_at,
  close_lag_seconds = excluded.close_lag_seconds,
  resolve_lag_seconds = excluded.resolve_lag_seconds,
  pre_close_trade_count_24h = excluded.pre_close_trade_count_24h,
  pre_close_unique_traders_24h = excluded.pre_close_unique_traders_24h,
  pre_close_notional_24h = excluded.pre_close_notional_24h,
  last_trade_ts = excluded.last_trade_ts,
  last_trade_price = excluded.last_trade_price,
  final_hour_bbo_ticks = excluded.final_hour_bbo_ticks,
  final_hour_spread_median = excluded.final_hour_spread_median,
  last_bbo_ts = excluded.last_bbo_ts,
  last_best_bid = excluded.last_best_bid,
  last_best_ask = excluded.last_best_ask,
  last_spread = excluded.last_spread,
  computed_at = now();