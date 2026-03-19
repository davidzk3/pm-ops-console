from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional, Set

import psycopg

from apps.api.db import get_db_dsn


ENGINE_VERSION_RAW = "resolution_raw_v1_introspected_2026_03_03"
ENGINE_VERSION_FEATURES = "resolution_features_v1_introspected_2026_03_03"
ENGINE_VERSION_SCORES = "resolution_scores_v1_introspected_2026_03_03"


def _get_table_columns(cur: psycopg.Cursor[Any], table_name: str, schema: str = "public") -> Set[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %(schema)s
          and table_name = %(table)s
        """,
        {"schema": schema, "table": table_name},
    )
    return {r[0] for r in cur.fetchall()}


def compute_market_resolution_raw_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    """
    Raw resolution snapshot (Sprint 5) -> writes into existing market_resolution_raw_daily.

    IMPORTANT:
    - DB table already exists in your schema with PK (market_id, day, window_hours).
    - This function must NOT assume engine_version exists on the raw table.
    - We introspect destination columns and only write what exists.
    """
    if day is None:
        day = date.today()

    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            markets_cols = _get_table_columns(cur, "markets")

            cur.execute(
                """
                create table if not exists market_resolution_raw_daily (
                  market_id text not null,
                  day date not null,
                  window_hours integer not null,

                  protocol text not null,
                  title text null,
                  url text null,

                  resolved_at timestamptz null,
                  resolution_status text null,
                  outcome text null,
                  rules_text text null,
                  source_text text null,

                  raw jsonb not null default '{}'::jsonb,
                  created_at timestamptz not null default now(),

                  external_id text null,
                  market_status text null,
                  resolution text null,

                  primary key (market_id, day, window_hours)
                )
                """
            )

            dest_cols = _get_table_columns(cur, "market_resolution_raw_daily")

            # IMPORTANT: raw layer should NOT invent resolved_at via closed_time.
            # Keep resolved_at strictly from API (markets.resolved_at) if it exists, else NULL.
            resolved_at_expr = "m.resolved_at" if "resolved_at" in markets_cols else "null::timestamptz"

            status_expr = "m.status" if "status" in markets_cols else "null::text"
            resolution_expr = "m.resolution" if "resolution" in markets_cols else "null::text"

            insert_cols = ["market_id", "day", "window_hours"]
            select_exprs = [
                "m.market_id",
                "%(day)s::date as day",
                "%(window_hours)s::int as window_hours",
            ]

            def add_col(col: str, expr: str):
                if col in dest_cols:
                    insert_cols.append(col)
                    select_exprs.append(expr)

            add_col("protocol", "m.protocol")
            add_col("external_id", "m.external_id" if "external_id" in markets_cols else "null::text")
            add_col("title", "m.title" if "title" in markets_cols else "null::text")
            add_col("url", "m.url" if "url" in markets_cols else "null::text")
            add_col("market_status", f"{status_expr} as market_status")
            add_col("resolution", f"{resolution_expr} as resolution")
            add_col("resolved_at", f"{resolved_at_expr} as resolved_at")
            add_col("outcome", "m.outcome" if "outcome" in markets_cols else "null::text")

            if "raw" in dest_cols:
                select_exprs.append(
                    """
                    jsonb_build_object(
                      'market_id', m.market_id,
                      'protocol', m.protocol,
                      'external_id', coalesce(m.external_id,''),
                      'status', coalesce(m.status,'')
                    )::jsonb as raw
                    """
                    if {"external_id", "status"} <= markets_cols
                    else "'{}'::jsonb as raw"
                )
                insert_cols.append("raw")

            sql = f"""
            with eligible_markets as (
              select market_id
              from markets
              where protocol = 'polymarket'
                and coalesce(external_id, '') <> ''
              limit %(limit_markets)s
            )
            insert into market_resolution_raw_daily (
              {", ".join(insert_cols)}
            )
            select
              {", ".join(select_exprs)}
            from markets m
            join eligible_markets em on em.market_id = m.market_id
            on conflict (market_id, day, window_hours) do update
              set
                {", ".join([f"{c} = excluded.{c}" for c in insert_cols if c not in ("market_id", "day", "window_hours")])}
                {"," if any(c not in ("market_id", "day", "window_hours") for c in insert_cols) else ""}
                created_at = now()
            """

            params = {
                "day": day,
                "window_hours": window_hours,
                "limit_markets": limit_markets,
            }

            cur.execute(sql, params)
            conn.commit()

            return {
                "engine_version": ENGINE_VERSION_RAW,
                "day": str(day),
                "window_hours": window_hours,
                "limit_markets": limit_markets,
                "status": "ok",
            }


def compute_market_resolution_features_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    """
    Feature layer from raw (Sprint 5).
    Writes to existing market_resolution_features_daily schema in DB.

    Best practice:
    - Compute a "best available" resolved timestamp:
        resolved_at_final = coalesce(
            markets.resolved_at,
            case when markets.closed and outcome present then markets.closed_time end
          )
    - Compute is_resolved using resolved_at_final OR outcome present.
    """
    if day is None:
        day = date.today()

    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            dest_cols = _get_table_columns(cur, "market_resolution_features_daily")
            raw_cols = _get_table_columns(cur, "market_resolution_raw_daily")
            markets_cols = _get_table_columns(cur, "markets")

            required = {"market_id", "day", "window_hours", "engine_version", "is_resolved"}
            missing_required = [c for c in required if c not in dest_cols]
            if missing_required:
                raise RuntimeError(f"market_resolution_features_daily missing required columns: {missing_required}")

            insert_cols: list[str] = []
            select_exprs: list[str] = []

            def add(col: str, expr: str):
                if col in dest_cols:
                    insert_cols.append(col)
                    select_exprs.append(expr)

            # keys/version
            add("market_id", "r.market_id")
            add("day", "r.day")
            add("window_hours", "r.window_hours")
            add("engine_version", "%(engine_version)s::text")

            # ------- BEST AVAILABLE RESOLUTION TIMESTAMP (guarded fallback) -------
            # Use markets.resolved_at if present; else use closed_time only if:
            #   closed=true AND outcome present (non-empty) AND closed_time not null.
            m_resolved_at = "m.resolved_at" if "resolved_at" in markets_cols else "null::timestamptz"
            m_closed = "m.closed" if "closed" in markets_cols else "false"
            m_closed_time = "m.closed_time" if "closed_time" in markets_cols else "null::timestamptz"
            m_outcome = "m.outcome" if "outcome" in markets_cols else "null::text"

            resolved_at_final_expr = f"""
            coalesce(
              {m_resolved_at},
              case
                when {m_closed} is true
                 and {m_closed_time} is not null
                 and coalesce(nullif(btrim({m_outcome}),''), '') <> ''
                then {m_closed_time}
                else null::timestamptz
              end
            )
            """.strip()

            # optional column if you created it
            add("resolved_at_final", resolved_at_final_expr)

            # is_resolved NOT NULL (robust)
            is_resolved_expr = f"""
            (
              ({resolved_at_final_expr}) is not null
              or coalesce(nullif(btrim({m_outcome}),''), '') <> ''
            )::boolean
            """.strip()
            add("is_resolved", is_resolved_expr)

            # invalid booleans (keep both columns in sync if both exist)
            status_expr = "coalesce(r.market_status,'')" if "market_status" in raw_cols else "''"
            outcome_expr_raw = "lower(coalesce(r.outcome,''))" if "outcome" in raw_cols else "''"
            outcome_expr_m = f"lower(coalesce({m_outcome},''))"

            invalid_expr = f"""(
                position('invalid' in lower({status_expr})) > 0
                or position('cancel' in lower({status_expr})) > 0
                or position('refund' in lower({status_expr})) > 0
                or position('invalid' in {outcome_expr_raw}) > 0
                or position('invalid' in {outcome_expr_m}) > 0
            )::boolean"""

            if "is_invalid" in dest_cols:
                add("is_invalid", invalid_expr)
            if "invalid_flag" in dest_cols:
                add("invalid_flag", invalid_expr)

            # has_resolution_text NOT NULL
            if "has_resolution_text" in dest_cols:
                if "resolution" in raw_cols:
                    add(
                        "has_resolution_text",
                        "(r.resolution is not null and length(btrim(r.resolution)) > 0)::boolean",
                    )
                else:
                    add("has_resolution_text", "false::boolean")

            if "rules_length" in dest_cols:
                if "rules_text" in raw_cols:
                    add("rules_length", "length(coalesce(r.rules_text, ''))::int")
                else:
                    add("rules_length", "0::int")

            # timestamps (populate if present)
            if "created_at" in dest_cols:
                add("created_at", "now()")
            if "inserted_at" in dest_cols:
                add("inserted_at", "now()")

            # conflict key
            conflict_cols: list[str] = []
            for c in ("market_id", "day", "window_hours", "engine_version"):
                if c in dest_cols:
                    conflict_cols.append(c)

            if conflict_cols != ["market_id", "day", "window_hours", "engine_version"]:
                conflict_cols = ["market_id", "day", "window_hours"]
                if not all(c in dest_cols for c in conflict_cols):
                    raise RuntimeError("No usable conflict key found for market_resolution_features_daily")

            updatable_cols = [c for c in insert_cols if c not in conflict_cols]

            sql = f"""
            insert into market_resolution_features_daily (
              {", ".join(insert_cols)}
            )
            select
              {", ".join(select_exprs)}
            from market_resolution_raw_daily r
            join markets m on m.market_id = r.market_id
            where r.day = %(day)s::date
              and r.window_hours = %(window_hours)s::int
            on conflict ({", ".join(conflict_cols)}) do update
              set
                {", ".join([f"{c} = excluded.{c}" for c in updatable_cols])}
            """

            params = {
                "day": day,
                "window_hours": window_hours,
                "engine_version": ENGINE_VERSION_FEATURES,
                "limit_markets": limit_markets,
            }

            cur.execute(sql, params)
            conn.commit()

            return {
                "engine_version": ENGINE_VERSION_FEATURES,
                "day": str(day),
                "window_hours": window_hours,
                "limit_markets": limit_markets,
                "status": "ok",
            }


def compute_market_resolution_scores_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    """
    Writes to existing market_resolution_scores_daily schema in DB.
    PK: (protocol, market_id, day, window_hours)
    """
    if day is None:
        day = date.today()

    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            dest_cols = _get_table_columns(cur, "market_resolution_scores_daily")
            feat_cols = _get_table_columns(cur, "market_resolution_features_daily")

            required_dest = {"protocol", "market_id", "day", "window_hours", "resolution_risk_score", "reasons", "inserted_at"}
            missing = [c for c in required_dest if c not in dest_cols]
            if missing:
                raise RuntimeError(f"market_resolution_scores_daily missing columns: {missing}")

            required_feat = {"market_id", "day", "window_hours", "engine_version", "is_resolved"}
            missing_feat = [c for c in required_feat if c not in feat_cols]
            if missing_feat:
                raise RuntimeError(f"market_resolution_features_daily missing columns needed for scoring: {missing_feat}")

            has_invalid_flag = "invalid_flag" in feat_cols
            has_is_invalid = "is_invalid" in feat_cols

            invalid_ref = "f.is_invalid" if has_is_invalid else ("f.invalid_flag" if has_invalid_flag else "false")

            sql = f"""
            insert into market_resolution_scores_daily (
              protocol,
              market_id,
              day,
              window_hours,
              resolution_risk_score,
              reasons,
              inserted_at
            )
            select
              'polymarket'::text as protocol,
              f.market_id,
              f.day,
              f.window_hours,
              (
                0.0
                + case when {invalid_ref} then 1.0 else 0.0 end
                + case when not f.is_resolved then 0.3 else 0.0 end
              ) as resolution_risk_score,
              (
                select coalesce(jsonb_agg(x) filter (where x is not null), '[]'::jsonb)
                from (
                  values
                    (case when {invalid_ref} then 'INVALID_MARKET'::text else null::text end),
                    (case when not f.is_resolved then 'UNRESOLVED'::text else null::text end)
                ) v(x)
              ) as reasons,
              now() as inserted_at
            from market_resolution_features_daily f
            where f.day = %(day)s::date
              and f.window_hours = %(window_hours)s::int
              and f.engine_version = %(features_engine_version)s
            on conflict (protocol, market_id, day, window_hours) do update
              set
                resolution_risk_score = excluded.resolution_risk_score,
                reasons = excluded.reasons,
                inserted_at = now()
            """

            params = {
                "day": day,
                "window_hours": window_hours,
                "features_engine_version": ENGINE_VERSION_FEATURES,
            }

            cur.execute(sql, params)
            conn.commit()

            return {
                "engine_version": ENGINE_VERSION_SCORES,
                "day": str(day),
                "window_hours": window_hours,
                "limit_markets": limit_markets,
                "status": "ok",
            }