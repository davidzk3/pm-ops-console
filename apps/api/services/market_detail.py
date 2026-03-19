from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from apps.api.db import get_db_dsn


def _merge_flag_arrays(row: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for k in ("quality_flags", "liquidity_flags", "concentration_flags"):
        v = row.get(k)
        if isinstance(v, list):
            out.extend([str(x) for x in v if x])
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def get_market_detail(
    market_id: str,
    lookback_days: int = 14,
    day: Optional[date] = None,
) -> Dict[str, Any]:
    if lookback_days < 1:
        lookback_days = 1
    if lookback_days > 90:
        lookback_days = 90

    dsn = get_db_dsn()

    sql_current = """
    with latest as (
      select *
      from market_microstructure_daily
      where market_id = %(market_id)s
        and (%(day)s::date is null or day = %(day)s::date)
      order by day desc
      limit 1
    )
    select
      m.*,
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
      f.depth_score,
      f.updated_at as features_updated_at
    from latest m
    left join market_microstructure_features_daily f
      on f.market_id = m.market_id
     and f.day = m.day
     and f.window_hours = m.window_hours
    """

    sql_history = """
    with hist as (
      select
        m.day,
        m.window_hours,
        m.structural_score,
        m.trades,
        m.volume,
        m.unique_traders,
        m.top1_trader_share,
        m.top5_trader_share,
        m.hhi,
        m.price_volatility,
        m.avg_spread,
        m.suspicious_burst_flag,
        m.burst_score,
        percent_rank() over (
          partition by m.day
          order by m.structural_score
        ) as structural_percentile,
        rank() over (
          partition by m.day
          order by m.structural_score desc
        ) as structural_rank
      from market_microstructure_daily m
      where m.market_id = %(market_id)s
      order by m.day desc
      limit %(lookback_days)s
    )
    select
      h.day,
      h.window_hours,
      h.structural_score,
      h.structural_rank,
      h.structural_percentile,
      h.trades,
      h.volume,
      h.unique_traders,
      h.top1_trader_share,
      h.top5_trader_share,
      h.hhi,
      h.price_volatility,
      h.avg_spread,
      h.suspicious_burst_flag,
      h.burst_score,

      f.market_quality_score,
      f.liquidity_health_score,
      f.trading_activity_score,
      f.spread_quality_score,
      f.volatility_risk_score,
      f.burst_risk_score,
      f.concentration_risk_score,

      f.low_activity_flag,
      f.high_concentration_flag,
      f.wide_spread_flag,
      f.high_volatility_flag,
      f.burst_flag,

      f.quality_flags,
      f.liquidity_flags,
      f.concentration_flags
    from hist h
    left join market_microstructure_features_daily f
      on f.market_id = %(market_id)s
     and f.day = h.day
     and f.window_hours = h.window_hours
    order by h.day asc
    """

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql_current,
                {"market_id": market_id, "day": day.isoformat() if day else None},
            )
            current = cur.fetchone()
            if not current:
                return {
                    "market_id": market_id,
                    "as_of_day": None,
                    "current_microstructure": {},
                    "current_features": {},
                    "structural_components": {},
                    "feature_components": {},
                    "flags": {},
                    "history": [],
                    "risk_summary": {},
                }

            as_of_day = current["day"]

            current_micro = {
                "day": current.get("day"),
                "window_hours": current.get("window_hours"),
                "volume": current.get("volume"),
                "trades": current.get("trades"),
                "unique_traders": current.get("unique_traders"),
                "top1_trader_share": current.get("top1_trader_share"),
                "top5_trader_share": current.get("top5_trader_share"),
                "hhi": current.get("hhi"),
                "price_volatility": current.get("price_volatility"),
                "avg_spread": current.get("avg_spread"),
                "bbo_ticks": current.get("bbo_ticks"),
                "suspicious_burst_flag": current.get("suspicious_burst_flag"),
                "burst_score": current.get("burst_score"),
                "identity_coverage": current.get("identity_coverage"),
                "identity_blind": current.get("identity_blind"),
                "structural_score": current.get("structural_score"),
                "created_at": current.get("created_at"),
            }

            current_feat = {
                "engine_version": current.get("engine_version"),
                "liquidity_health_score": current.get("liquidity_health_score"),
                "trading_activity_score": current.get("trading_activity_score"),
                "spread_quality_score": current.get("spread_quality_score"),
                "volatility_risk_score": current.get("volatility_risk_score"),
                "burst_risk_score": current.get("burst_risk_score"),
                "concentration_risk_score": current.get("concentration_risk_score"),
                "market_quality_score": current.get("market_quality_score"),
                "activity_score": current.get("activity_score"),
                "spread_score": current.get("spread_score"),
                "depth_score": current.get("depth_score"),
                "features_updated_at": current.get("features_updated_at"),
            }

            structural_components = {
                "activity_proxy": current.get("trades"),
                "liquidity_proxy": current.get("avg_spread"),
                "concentration_proxy": current.get("hhi"),
                "burst_proxy": current.get("burst_score"),
                "identity_blind": current.get("identity_blind"),
            }

            feature_components = {
                "liquidity_health_score": current.get("liquidity_health_score"),
                "trading_activity_score": current.get("trading_activity_score"),
                "spread_quality_score": current.get("spread_quality_score"),
                "volatility_risk_score": current.get("volatility_risk_score"),
                "burst_risk_score": current.get("burst_risk_score"),
                "concentration_risk_score": current.get("concentration_risk_score"),
                "market_quality_score": current.get("market_quality_score"),
            }

            flags = {
                "low_activity_flag": bool(current.get("low_activity_flag")) if current.get("low_activity_flag") is not None else None,
                "high_concentration_flag": bool(current.get("high_concentration_flag")) if current.get("high_concentration_flag") is not None else None,
                "wide_spread_flag": bool(current.get("wide_spread_flag")) if current.get("wide_spread_flag") is not None else None,
                "high_volatility_flag": bool(current.get("high_volatility_flag")) if current.get("high_volatility_flag") is not None else None,
                "burst_flag": bool(current.get("burst_flag")) if current.get("burst_flag") is not None else None,
                "quality_flags": current.get("quality_flags") or [],
                "liquidity_flags": current.get("liquidity_flags") or [],
                "concentration_flags": current.get("concentration_flags") or [],
                "all_flags": _merge_flag_arrays(current),
            }

            cur.execute(sql_history, {"market_id": market_id, "lookback_days": lookback_days})
            history = cur.fetchall()

            risk_summary = {
                "market_quality_score": current.get("market_quality_score"),
                "concentration_risk_score": current.get("concentration_risk_score"),
                "volatility_risk_score": current.get("volatility_risk_score"),
                "burst_risk_score": current.get("burst_risk_score"),
                "active_flags_count": len(flags["all_flags"]),
            }

            return {
                "market_id": market_id,
                "as_of_day": as_of_day,
                "current_microstructure": current_micro,
                "current_features": current_feat,
                "structural_components": structural_components,
                "feature_components": feature_components,
                "flags": flags,
                "history": history,
                "risk_summary": risk_summary,
            }