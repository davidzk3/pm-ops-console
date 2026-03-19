from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, Tuple
from .polymarket_trades_rest import ingest_polymarket_trades_rest
from .polymarket_trades_rest import ingest_polymarket_trades_rest_for_market

import hashlib
import json

import psycopg
from psycopg.types.json import Json

from .providers.gamma import fetch_markets, fetch_market_detail
from ..db import get_db_dsn
from .providers.clob_ws import stream_market_events_sync


# -------------------------
# Time helpers
# -------------------------
def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _utc_day_bounds(day: date) -> tuple[datetime, datetime]:
    """
    Returns [start_ts, end_ts) for a UTC day.
    """
    start_ts = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end_ts = start_ts + timedelta(days=1)
    return start_ts, end_ts


def _hash_payload(payload: Any) -> str:
    s = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


def _run_start(cur, source: str, run_type: str, meta: Optional[Dict[str, Any]] = None) -> int:
    cur.execute(
        """
        INSERT INTO raw_source_runs (source, run_type, status, meta)
        VALUES (%s, %s, 'RUNNING', %s::jsonb)
        RETURNING id;
        """,
        (source, run_type, Json(meta or {})),
    )
    return int(cur.fetchone()[0])


def _run_finish(
    cur,
    run_id: int,
    status: str,
    meta: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
):
    cur.execute(
        """
        UPDATE raw_source_runs
        SET finished_at = now(),
            status = %s,
            meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
            error = %s
        WHERE id = %s;
        """,
        (status, Json(meta or {}), error, run_id),
    )


def _ensure_market_id(cur, protocol: str, external_id: str) -> str:
    """
    Creates a stable internal market_id if not present.
    Format: m_<short-hash>
    """
    cur.execute(
        """
        SELECT market_id
        FROM market_id_map
        WHERE protocol = %s AND external_id = %s;
        """,
        (protocol, external_id),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    short = hashlib.sha1(f"{protocol}:{external_id}".encode("utf-8")).hexdigest()[:10]
    market_id = f"m_{short}"

    cur.execute(
        """
        INSERT INTO market_id_map (market_id, protocol, external_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (protocol, external_id) DO NOTHING;
        """,
        (market_id, protocol, external_id),
    )

    return market_id


def _latest_raw_market_payload(cur, source: str, external_id: str) -> Optional[dict]:
    """
    Pull the latest raw market payload we have for a given external market id.
    Used to extract token or asset ids needed for WS subscription.
    """
    cur.execute(
        """
        SELECT payload
        FROM raw_markets
        WHERE source = %s AND external_market_id = %s
        ORDER BY fetched_at DESC
        LIMIT 1;
        """,
        (source, external_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _parse_json_list_field(v: Any) -> list[str]:
    """
    Safely parse fields that may be:
      a real list
      a JSON-encoded list string
      a comma separated string
      a single value
    """
    if v is None:
        return []

    if isinstance(v, list):
        return [str(x) for x in v if x is not None]

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []

        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x is not None]
        except Exception:
            pass

        if "," in s:
            return [p.strip().strip('"') for p in s.split(",") if p.strip()]

        return [s]

    return []

def _derive_winner_from_detail(detail: dict, min_prob: float = 0.99) -> Optional[str]:
    outcomes_obj = _parse_jsonb(detail.get("outcomes"))
    prices_obj = _parse_jsonb(detail.get("outcomePrices") or detail.get("outcome_prices"))

    if not isinstance(outcomes_obj, list) or not outcomes_obj:
        return None
    if not isinstance(prices_obj, list) or len(prices_obj) != len(outcomes_obj):
        return None

    best_i = None
    best_p = None

    for i, p in enumerate(prices_obj):
        try:
            fp = float(p)
        except Exception:
            continue
        if best_p is None or fp > best_p:
            best_p = fp
            best_i = i

    if best_i is None or best_p is None:
        return None

    if best_p < float(min_prob):
        return None

    w = outcomes_obj[best_i]
    if w is None:
        return None
    w = str(w).strip()
    return w if w else None

def _extract_asset_ids(payload: dict) -> list[str]:
    """
    Extract token or asset ids for WS subscription.

    Gamma payload commonly stores clobTokenIds as:
      "clobTokenIds": "[\"id1\",\"id2\"]"
    """
    asset_ids: list[str] = []

    asset_ids.extend(
        _parse_json_list_field(
            payload.get("clobTokenIds") or payload.get("clob_token_ids")
        )
    )

    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        for t in tokens:
            if isinstance(t, dict):
                tid = (
                    t.get("tokenId")
                    or t.get("id")
                    or t.get("assetId")
                    or t.get("clobTokenId")
                )
                if tid is not None:
                    asset_ids.append(str(tid))

    outcome_tokens = payload.get("outcomeTokens")
    if isinstance(outcome_tokens, list):
        for o in outcome_tokens:
            if isinstance(o, dict):
                tid = (
                    o.get("tokenId")
                    or o.get("id")
                    or o.get("assetId")
                    or o.get("clobTokenId")
                )
                if tid is not None:
                    asset_ids.append(str(tid))

    seen = set()
    out: list[str] = []
    for a in asset_ids:
        if a and a not in seen:
            seen.add(a)
            out.append(a)

    return out

def _safe_ts(ev: dict) -> datetime:
    ts = (
        ev.get("timestamp")
        or ev.get("ts")
        or ev.get("time")
        or ev.get("created_at")
        or ev.get("createdAt")
    )

    parsed = _parse_ts(ts) if ts else None
    if parsed is not None:
        return parsed

    return datetime.now(timezone.utc)

def _parse_jsonb(v: Any) -> Optional[Any]:
    """
    Parse a value that might be:
      - None
      - already a list/dict
      - a stringified JSON (common from Gamma fields)
    Returns a python object suitable for json.dumps(...) -> jsonb insert/update.
    """
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def _best_from_book(ev: dict) -> tuple[str, str]:
    best_bid = "0"
    best_ask = "0"

    bids = ev.get("bids")
    if isinstance(bids, list) and bids:
        try:
            prices = []
            for x in bids:
                if isinstance(x, dict) and x.get("price") is not None:
                    prices.append(float(x.get("price")))
            if prices:
                best_bid = str(max(prices))
        except Exception:
            best_bid = "0"

    asks = ev.get("asks")
    if isinstance(asks, list) and asks:
        try:
            prices = []
            for x in asks:
                if isinstance(x, dict) and x.get("price") is not None:
                    prices.append(float(x.get("price")))
            if prices:
                best_ask = str(min(prices))
        except Exception:
            best_ask = "0"

    return best_bid, best_ask


def _parse_ts(ts_any: Any) -> datetime:
    """
    Parse WS timestamp into aware datetime in UTC.
    Accepts epoch seconds, epoch ms, or ISO strings.
    """
    if ts_any is None:
        return datetime.now(timezone.utc)

    if isinstance(ts_any, (int, float)):
        v = float(ts_any)
        if v > 10_000_000_000:
            v = v / 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)

    if isinstance(ts_any, str):
        s = ts_any.strip()
        if not s:
            return datetime.now(timezone.utc)
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    return datetime.now(timezone.utc)


def _coerce_decimal_str(v: Any, default: str = "0") -> str:
    """
    Keep as string to avoid float precision.
    Psycopg will coerce string to numeric for numeric columns.
    """
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        s = v.strip()
        return s if s else default
    return default


def _map_ws_side(side_any: Any) -> str:
    """
    trades.side check requires BUY or SELL.
    """
    if not side_any:
        return "BUY"
    s = str(side_any).strip().upper()
    if s in ("BUY", "BID"):
        return "BUY"
    if s in ("SELL", "ASK"):
        return "SELL"
    return "BUY"


def _event_source_id(source: str, ev: dict) -> str:
    """
    Stable-ish id for raw.trade_events, even when WS does not provide an id.
    """
    for k in ("id", "event_id", "eventId", "hash", "tx", "tx_hash", "txHash"):
        v = ev.get(k)
        if v is not None and str(v).strip():
            return f"{source}:{str(v).strip()}"
    return f"{source}:{_hash_payload(ev)[:24]}"


# ==========================================================
# Step 2: B layer — Active Universe (daily, windowed)
# ==========================================================
def _ensure_universe_table(cur) -> None:
    """
    Stores the active market universe used for downstream snapshot computations.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_universe_daily (
            day date NOT NULL,
            window_hours int NOT NULL,
            market_id text NOT NULL,
            source_flags jsonb NOT NULL DEFAULT '{}'::jsonb,
            computed_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (day, window_hours, market_id)
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_universe_daily_day_window
        ON market_universe_daily (day, window_hours);
        """
    )


def compute_market_universe_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    if day is None:
        day = _utc_today()

    start_day, end_day = _utc_day_bounds(day)
    end_ts = end_day
    start_ts = end_ts - timedelta(hours=int(window_hours))

    source = "internal"
    run_type = "universe_daily"

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            _ensure_universe_table(cur)
            run_id = _run_start(
                cur,
                source,
                run_type,
                meta={
                    "day": str(day),
                    "window_hours": int(window_hours),
                    "limit_markets": int(limit_markets),
                    "start_ts": start_ts.isoformat(),
                    "end_ts": end_ts.isoformat(),
                },
            )

            try:
                cur.execute(
                    """
                    WITH t AS (
                        SELECT market_id, 1::int AS has_trades, 0::int AS has_bbo
                        FROM trades
                        WHERE ts >= %(start_ts)s
                          AND ts <  %(end_ts)s
                        GROUP BY market_id
                    ),
                    b AS (
                        SELECT market_id, 0::int AS has_trades, 1::int AS has_bbo
                        FROM market_bbo_ticks
                        WHERE ts >= %(start_ts)s
                          AND ts <  %(end_ts)s
                        GROUP BY market_id
                    ),
                    u AS (
                        SELECT market_id,
                               MAX(has_trades) AS has_trades,
                               MAX(has_bbo)    AS has_bbo
                        FROM (
                            SELECT * FROM t
                            UNION ALL
                            SELECT * FROM b
                        ) x
                        GROUP BY market_id
                    )
                    SELECT market_id, has_trades, has_bbo
                    FROM u
                    ORDER BY (has_trades + has_bbo) DESC, market_id
                    LIMIT %(limit_markets)s;
                    """,
                    {
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "limit_markets": int(limit_markets),
                    },
                )
                rows = cur.fetchall()

                cur.execute(
                    """
                    DELETE FROM market_universe_daily
                    WHERE day = %(day)s AND window_hours = %(window_hours)s;
                    """,
                    {"day": day, "window_hours": int(window_hours)},
                )

                inserted = 0
                trades_flagged = 0
                bbo_flagged = 0

                for market_id, has_trades, has_bbo in rows:
                    flags = {"has_trades": bool(has_trades), "has_bbo": bool(has_bbo)}
                    if flags["has_trades"]:
                        trades_flagged += 1
                    if flags["has_bbo"]:
                        bbo_flagged += 1

                    cur.execute(
                        """
                        INSERT INTO market_universe_daily (day, window_hours, market_id, source_flags)
                        VALUES (%s, %s, %s, %s::jsonb)
                        ON CONFLICT (day, window_hours, market_id)
                        DO UPDATE SET
                            source_flags = EXCLUDED.source_flags,
                            computed_at = now();
                        """,
                        (day, int(window_hours), str(market_id), Json(flags)),
                    )
                    inserted += 1

                _run_finish(
                    cur,
                    run_id,
                    "OK",
                    meta={
                        "universe_markets": inserted,
                        "has_trades_count": trades_flagged,
                        "has_bbo_count": bbo_flagged,
                    },
                )
                conn.commit()

                return {
                    "run_id": run_id,
                    "day": str(day),
                    "window_hours": int(window_hours),
                    "start_ts": start_ts.isoformat(),
                    "end_ts": end_ts.isoformat(),
                    "universe_markets": inserted,
                    "has_trades_count": trades_flagged,
                    "has_bbo_count": bbo_flagged,
                }

            except Exception as e:
                conn.rollback()
                _run_finish(cur, run_id, "FAILED", error=str(e))
                conn.commit()
                raise


def universe_market_ids(
    cur,
    day: date,
    window_hours: int,
    limit_markets: int = 500,
) -> list[str]:
    cur.execute(
        """
        SELECT market_id
        FROM market_universe_daily
        WHERE day = %s AND window_hours = %s
        ORDER BY market_id
        LIMIT %s;
        """,
        (day, int(window_hours), int(limit_markets)),
    )
    return [r[0] for r in cur.fetchall()]


# ==========================================================
# Existing ingestion
# ==========================================================
def ingest_polymarket_trades_rest_job(
    lookback_hours: int = 72,
    use_cursor: bool = True,
) -> Dict[str, Any]:
    return ingest_polymarket_trades_rest(
        lookback_hours=lookback_hours,
        use_cursor=use_cursor,
    )

def ingest_polymarket_markets(limit: int = 200, offset: int = 0) -> Dict[str, Any]:
    """
    Ingest Polymarket markets from Gamma.

    Behavior:
    - Upserts markets from Gamma list endpoint
    - If closed but missing outcome, fetch detail endpoint and backfill
    """

    dsn = get_db_dsn()

    payload = fetch_markets(limit=limit, offset=offset)

    markets = (
        payload
        if isinstance(payload, list)
        else payload.get("markets", [])
        if isinstance(payload, dict)
        else []
    )

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:

            _ensure_universe_table(cur)

            cur.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'public'
                and table_name = 'markets'
                """
            )

            markets_cols = {r[0] for r in cur.fetchall()}

            has_outcomes_col = "outcomes" in markets_cols
            has_outcome_prices_col = "outcome_prices" in markets_cols
            has_outcome_col = "outcome" in markets_cols

            run_id = _run_start(
                cur,
                source="polymarket_gamma",
                run_type="markets",
                meta={"limit": limit, "offset": offset},
            )

            raw_rows = 0
            markets_upserted = 0
            skipped_missing_title = 0

            for m in markets:

                raw_rows += 1

                external_id = str(m.get("id") or m.get("marketId") or "").strip()
                if not external_id:
                    continue

                market_id = _ensure_market_id(
                    cur,
                    protocol="polymarket",
                    external_id=external_id,
                )

                title = m.get("question") or m.get("title")
                if isinstance(title, str):
                    title = title.strip()
                if not title:
                    skipped_missing_title += 1
                    continue

                url = m.get("url")

                closed = bool(m.get("closed")) if "closed" in m else False
                closed_time = _parse_ts(m.get("closedTime")) if m.get("closedTime") else None
                resolved_at = _parse_ts(m.get("resolvedAt")) if m.get("resolvedAt") else None
                end_date = _parse_ts(m.get("endDate")) if m.get("endDate") else None

                outcome = m.get("outcome")
                if isinstance(outcome, str) and outcome.strip() == "":
                    outcome = None

                # -------------------------
                # MINIMAL PATCH FOR SCHEMA
                # -------------------------
                # markets.chain is NOT NULL -> must provide it
                # markets.status is NOT NULL (default open) -> keep consistent w/ closed flag
                chain = "polygon"
                status = "closed" if closed else "open"

                raw_payload = m

                cur.execute(
                    """
                    insert into markets (
                        market_id,
                        protocol,
                        chain,
                        status,
                        external_id,
                        title,
                        url,
                        closed,
                        closed_time,
                        resolved_at,
                        end_date,
                        outcome,
                        raw
                    )
                    values (
                        %(market_id)s,
                        %(protocol)s,
                        %(chain)s,
                        %(status)s,
                        %(external_id)s,
                        %(title)s,
                        %(url)s,
                        %(closed)s,
                        %(closed_time)s,
                        %(resolved_at)s,
                        %(end_date)s,
                        %(outcome)s,
                        %(raw)s::jsonb
                    )
                    on conflict (market_id) do update
                    set
                        protocol = excluded.protocol,
                        chain = excluded.chain,
                        status = excluded.status,
                        external_id = excluded.external_id,
                        title = excluded.title,
                        url = excluded.url,
                        closed = excluded.closed,
                        closed_time = excluded.closed_time,
                        resolved_at = excluded.resolved_at,
                        end_date = excluded.end_date,
                        outcome = excluded.outcome,
                        raw = excluded.raw,
                        updated_at = now()
                    """,
                    {
                        "market_id": market_id,
                        "protocol": "polymarket",
                        "chain": chain,
                        "status": status,
                        "external_id": external_id,
                        "title": title,
                        "url": url,
                        "closed": closed,
                        "closed_time": closed_time,
                        "resolved_at": resolved_at,
                        "end_date": end_date,
                        "outcome": outcome,
                        "raw": json.dumps(raw_payload),
                    },
                )


                cur.execute(
                    """
                    insert into core.markets (
                        market_id,
                        protocol,
                        chain,
                        title,
                        category,
                        is_active
                    )
                    values (
                        %(market_id)s,
                        %(protocol)s,
                        %(chain)s,
                        %(title)s,
                        %(category)s,
                        true
                    )
                    on conflict (market_id) do update
                    set
                        protocol = excluded.protocol,
                        chain = excluded.chain,
                        title = excluded.title,
                        category = excluded.category,
                        is_active = true,
                        updated_at = now()
                    """,
                    {
                        "market_id": market_id,
                        "protocol": "polymarket",
                        "chain": chain,
                        "title": title,
                        "category": m.get("category"),
                    },
                )
                
                
                
                markets_upserted += 1

                # define these before using them in needs_detail
                outcomes_obj = None
                outcome_prices_obj = None

                needs_detail = closed and (
                    outcome is None
                )

                if needs_detail and (has_outcomes_col or has_outcome_prices_col or has_outcome_col):
                    try:
                        detail = fetch_market_detail(external_id)

                        outcomes_obj = _parse_jsonb(detail.get("outcomes"))
                        outcome_prices_obj = _parse_jsonb(
                            detail.get("outcomePrices") or detail.get("outcome_prices")
                        )

                        winner = (
                            detail.get("winningOutcome")
                            or detail.get("outcome")
                            or _derive_winner_from_detail(detail)
                        )

                        set_parts = []
                        params = []

                        if has_outcomes_col:
                            set_parts.append("outcomes = COALESCE(%s::jsonb, outcomes)")
                            params.append(json.dumps(outcomes_obj) if outcomes_obj is not None else None)

                        if has_outcome_prices_col:
                            set_parts.append("outcome_prices = COALESCE(%s::jsonb, outcome_prices)")
                            params.append(json.dumps(outcome_prices_obj) if outcome_prices_obj is not None else None)

                        if has_outcome_col:
                            set_parts.append("outcome = COALESCE(%s, outcome)")
                            params.append(winner)

                        if set_parts:
                            params.append(external_id)
                            cur.execute(
                                f"""
                                update markets
                                set {", ".join(set_parts)}
                                where protocol = 'polymarket'
                                  and external_id = %s
                                """,
                                tuple(params),
                            )

                    except Exception:
                        pass

            _run_finish(
                cur,
                run_id=run_id,
                status="OK",
                meta={
                    "raw_rows": raw_rows,
                    "markets_upserted": markets_upserted,
                    "skipped_missing_title": skipped_missing_title,
                },
            )

            conn.commit()

            return {
                "run_id": run_id,
                "raw_rows": raw_rows,
                "markets_upserted": markets_upserted,
                "skipped_missing_title": skipped_missing_title,
            }

def ingest_polymarket_metrics_daily(limit: int = 200) -> Dict[str, Any]:
    """
    Fetch detail for markets already in DB and write one row per market for today.
    """
    source = "gamma"
    run_type = "metrics_daily"
    today = _utc_today()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            run_id = _run_start(cur, source, run_type, meta={"limit": limit})

            try:
                cur.execute(
                    """
                    SELECT market_id, external_id
                    FROM markets
                    WHERE protocol = 'polymarket'
                      AND COALESCE(external_id, '') <> ''
                    LIMIT %s;
                    """,
                    (limit,),
                )
                markets = cur.fetchall()

                updated = 0

                for market_id, external_id in markets:
                    try:
                        detail = fetch_market_detail(external_id)
                    except Exception:
                        continue

                    volume = detail.get("volume") or detail.get("volumeUsd") or detail.get("volumeUSD") or 0
                    trades = detail.get("trades") or detail.get("tradeCount") or 0
                    unique_traders = detail.get("uniqueTraders") or detail.get("unique_traders") or 0

                    cur.execute(
                        """
                        INSERT INTO market_metrics_daily (
                            market_id,
                            day,
                            volume,
                            trades,
                            unique_traders
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (market_id, day)
                        DO UPDATE SET
                            volume = EXCLUDED.volume,
                            trades = EXCLUDED.trades,
                            unique_traders = EXCLUDED.unique_traders;
                        """,
                        (market_id, today, volume, trades, unique_traders),
                    )
                    updated += 1

                _run_finish(cur, run_id, "OK", meta={"markets_processed": updated})
                conn.commit()

                return {"run_id": run_id, "markets_processed": updated}

            except Exception as e:
                conn.rollback()
                _run_finish(cur, run_id, "FAILED", error=str(e))
                conn.commit()
                raise


def ingest_polymarket_bbo_ws(
    limit_markets: int = 10,
    max_events: int = 500,
    source: str = "clob_ws",
) -> Dict[str, Any]:
    """
    Subscribe to Polymarket CLOB WS and capture order book / BBO-style updates.
    """
    run_type = "bbo_ws"
    today = _utc_today()

    TRADE_EVENT_TYPES = {"trade", "last_trade", "fill", "match", "execution"}

    def _iter_ws_items(events_obj: Any):
        for msg in events_obj or []:
            obj = msg
            if isinstance(obj, (bytes, bytearray)):
                try:
                    obj = obj.decode("utf-8", errors="ignore")
                except Exception:
                    continue

            if isinstance(obj, str):
                s = obj.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue

            if isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        yield item
                continue

            if isinstance(obj, dict):
                yield obj
                continue

    def _infer_event_type(ev: dict) -> str:
        event_type = ev.get("event_type") or ev.get("type") or ev.get("eventType")
        et = str(event_type or "").strip().lower()

        if et:
            return et

        if ("bids" in ev or "asks" in ev) and ("asset_id" in ev or "assetId" in ev):
            return "book"

        if ("best_bid" in ev or "bestBid" in ev or "best_ask" in ev or "bestAsk" in ev) and (
            "asset_id" in ev or "assetId" in ev
        ):
            return "best_bid_ask"

        if "price_changes" in ev or "priceChanges" in ev:
            return "price_change"

        return ""

    def _safe_ts(ev: dict) -> datetime:
        now_utc = datetime.now(timezone.utc)
        raw_ts = ev.get("timestamp") or ev.get("ts") or ev.get("time") or ev.get("created_at")
        ts = _parse_ts(raw_ts)

        if ts > (now_utc + timedelta(minutes=2)) or ts < (now_utc - timedelta(days=2)):
            return now_utc
        return ts

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            run_id = _run_start(
                cur,
                source,
                run_type,
                meta={"limit_markets": limit_markets, "max_events": max_events},
            )

            try:
                cur.execute(
                    """
                    SELECT market_id, external_id
                    FROM markets
                    WHERE protocol = 'polymarket'
                      AND COALESCE(external_id, '') <> ''
                    ORDER BY market_id
                    LIMIT %s;
                    """,
                    (limit_markets,),
                )
                markets = cur.fetchall()

                if not markets:
                    conn.rollback()
                    _run_finish(cur, run_id, "FAILED", error="No markets found to subscribe")
                    conn.commit()
                    return {"run_id": run_id, "status": "FAILED", "reason": "no_markets"}

                asset_to_market: dict[str, str] = {}
                for mid, external_id in markets:
                    payload = _latest_raw_market_payload(cur, "gamma", external_id)
                    ids = _extract_asset_ids(payload or {})

                    if not ids:
                        try:
                            detail = fetch_market_detail(external_id)
                            ids = _extract_asset_ids(detail or {})
                        except Exception:
                            ids = []

                    for aid in ids:
                        aid_s = str(aid)
                        if aid_s and aid_s not in asset_to_market:
                            asset_to_market[aid_s] = mid

                asset_ids = list(asset_to_market.keys())
                if not asset_ids:
                    conn.rollback()
                    _run_finish(cur, run_id, "FAILED", error="No asset_ids found in gamma payloads or detail")
                    conn.commit()
                    return {"run_id": run_id, "status": "FAILED", "reason": "no_asset_ids"}

                events = stream_market_events_sync(asset_ids=asset_ids, max_events=max_events)

                ws_messages = 0
                bbo_ticks_inserted = 0
                trades_inserted = 0
                ignored = 0

                from collections import Counter
                seen = Counter()

                for ev in _iter_ws_items(events):
                    ws_messages += 1
                    et = _infer_event_type(ev)
                    seen[et] += 1

                    try:
                        payload_hash = _hash_payload(ev)
                        raw_external = _event_source_id(source, ev)
                        cur.execute(
                            """
                            INSERT INTO raw_markets (source, external_market_id, payload, payload_hash)
                            VALUES (%s, %s, %s::jsonb, %s);
                            """,
                            (source, raw_external, Json(ev), payload_hash),
                        )
                    except Exception:
                        pass

                    if et == "price_change":
                        ts = _safe_ts(ev)
                        changes = ev.get("price_changes") or ev.get("priceChanges") or []
                        if not isinstance(changes, list):
                            ignored += 1
                            continue

                        for ch in changes:
                            if not isinstance(ch, dict):
                                continue

                            asset_id = (
                                ch.get("asset_id")
                                or ch.get("assetId")
                                or ch.get("token_id")
                                or ch.get("tokenId")
                            )
                            if asset_id is None:
                                continue
                            asset_id = str(asset_id)

                            market_id = asset_to_market.get(asset_id)
                            if not market_id:
                                continue

                            best_bid_s = _coerce_decimal_str(
                                ch.get("best_bid") or ch.get("bestBid") or ch.get("bid"),
                                default="0",
                            )
                            best_ask_s = _coerce_decimal_str(
                                ch.get("best_ask") or ch.get("bestAsk") or ch.get("ask"),
                                default="0",
                            )

                            if best_bid_s == "0" and best_ask_s == "0":
                                continue

                            spread_s = "0"
                            try:
                                if best_bid_s != "0" and best_ask_s != "0":
                                    spread_s = str(max(float(best_ask_s) - float(best_bid_s), 0.0))
                            except Exception:
                                spread_s = "0"

                            cur.execute(
                                """
                                INSERT INTO market_bbo_ticks (
                                    market_id, asset_id, ts, day, best_bid, best_ask, spread, source
                                )
                                VALUES (%s, %s, %s, %s, %s::numeric, %s::numeric, %s::numeric, %s)
                                ON CONFLICT (market_id, asset_id, ts) DO NOTHING
                                """,
                                (market_id, asset_id, ts, ts.date(), best_bid_s, best_ask_s, spread_s, source),
                            )
                            bbo_ticks_inserted += 1

                        continue

                    if et in ("best_bid_ask", "bbo"):
                        ts = _safe_ts(ev)
                        asset_id = ev.get("asset_id") or ev.get("assetId")
                        if asset_id is None:
                            ignored += 1
                            continue
                        asset_id = str(asset_id)

                        market_id = asset_to_market.get(asset_id)
                        if not market_id:
                            ignored += 1
                            continue

                        best_bid_s = _coerce_decimal_str(
                            ev.get("best_bid") or ev.get("bestBid") or ev.get("bid"),
                            default="0",
                        )
                        best_ask_s = _coerce_decimal_str(
                            ev.get("best_ask") or ev.get("bestAsk") or ev.get("ask"),
                            default="0",
                        )
                        spread_s = _coerce_decimal_str(ev.get("spread"), default="0")

                        cur.execute(
                            """
                            INSERT INTO market_bbo_ticks (
                                market_id, asset_id, ts, day, best_bid, best_ask, spread, source
                            )
                            VALUES (%s, %s, %s, %s, %s::numeric, %s::numeric, %s::numeric, %s)
                            ON CONFLICT (market_id, asset_id, ts) DO NOTHING
                            """,
                            (market_id, asset_id, ts, ts.date(), best_bid_s, best_ask_s, spread_s, source),
                        )
                        bbo_ticks_inserted += 1
                        continue

                    if et == "book":
                        ts = _safe_ts(ev)
                        asset_id = (
                            ev.get("asset_id")
                            or ev.get("assetId")
                            or ev.get("token_id")
                            or ev.get("tokenId")
                        )
                        if asset_id is None:
                            ignored += 1
                            continue
                        asset_id = str(asset_id)

                        market_id = asset_to_market.get(asset_id)
                        if not market_id:
                            ignored += 1
                            continue

                        best_bid_s, best_ask_s = _best_from_book(ev)

                        spread_s = "0"
                        try:
                            if best_bid_s != "0" and best_ask_s != "0":
                                spread_s = str(max(float(best_ask_s) - float(best_bid_s), 0.0))
                        except Exception:
                            spread_s = "0"

                        cur.execute(
                            """
                            INSERT INTO market_bbo_ticks (
                                market_id, asset_id, ts, day, best_bid, best_ask, spread, source
                            )
                            VALUES (%s, %s, %s, %s, %s::numeric, %s::numeric, %s::numeric, %s)
                            ON CONFLICT (market_id, asset_id, ts) DO NOTHING
                            """,
                            (market_id, asset_id, ts, ts.date(), best_bid_s, best_ask_s, spread_s, source),
                        )
                        bbo_ticks_inserted += 1
                        continue

                    if et in TRADE_EVENT_TYPES:
                        asset_id = (
                            ev.get("asset_id")
                            or ev.get("assetId")
                            or ev.get("token_id")
                            or ev.get("tokenId")
                        )
                        if asset_id is None:
                            ignored += 1
                            continue
                        asset_id = str(asset_id)

                        market_id = asset_to_market.get(asset_id)
                        if not market_id:
                            ignored += 1
                            continue

                        ts = _safe_ts(ev)
                        price_s = _coerce_decimal_str(ev.get("price") or ev.get("p"), default="0")
                        size_s = _coerce_decimal_str(
                            ev.get("size") or ev.get("quantity") or ev.get("qty") or ev.get("q"),
                            default="0",
                        )

                        if price_s == "0" or size_s == "0":
                            ignored += 1
                            continue

                        side = _map_ws_side(ev.get("side") or ev.get("taker_side") or ev.get("takerSide"))
                        trader_id = str(ev.get("trader") or ev.get("trader_id") or ev.get("wallet") or "unknown")

                        cur.execute(
                            """
                            INSERT INTO trades (
                                market_id, day, ts, trader_id, side, price, size, notional, source
                            )
                            VALUES (
                                %s, %s, %s, %s, %s,
                                %s::numeric, %s::numeric,
                                (%s::numeric * %s::numeric),
                                %s
                            )
                            ON CONFLICT DO NOTHING;
                            """,
                            (market_id, ts.date(), ts, trader_id, side, price_s, size_s, price_s, size_s, source),
                        )
                        trades_inserted += 1
                        continue

                    ignored += 1

                meta = {
                    "day": str(today),
                    "limit_markets": limit_markets,
                    "max_events": max_events,
                    "asset_ids": len(asset_ids),
                    "markets_considered": len(markets),
                    "ws_messages": ws_messages,
                    "bbo_ticks_inserted": bbo_ticks_inserted,
                    "trades_inserted": trades_inserted,
                    "ignored_events": ignored,
                    "ws_event_types": dict(seen),
                    "raw_events": ws_messages,
                    "bbo_inserted": bbo_ticks_inserted,
                }

                _run_finish(cur, run_id, "OK", meta=meta)
                conn.commit()

                return {
                    "run_id": run_id,
                    "ws_messages": ws_messages,
                    "bbo_ticks_inserted": bbo_ticks_inserted,
                    "trades_inserted": trades_inserted,
                    "ignored_events": ignored,
                    "asset_ids": len(asset_ids),
                    "markets_considered": len(markets),
                    "ws_event_types": dict(seen),
                }

            except Exception as e:
                conn.rollback()
                _run_finish(cur, run_id, "FAILED", error=str(e))
                conn.commit()
                raise


def ingest_polymarket_trades_ws(
    limit_markets: int = 10,
    max_events: int = 500,
    source: str = "clob_ws",
) -> Dict[str, Any]:
    """
    Backward-compatible wrapper.

    The ws market subscription feed you are using is BOOK and BBO, not a real trade tape.
    So this delegates to ingest_polymarket_bbo_ws.

    If you want trader cohort, you must add a real trades ingestion source separately.
    """
    return ingest_polymarket_bbo_ws(
        limit_markets=limit_markets,
        max_events=max_events,
        source=source,
    )

def ingest_polymarket_trades_rest_for_market_job(
    market_id: str,
    lookback_hours: int = 240,
) -> Dict[str, Any]:
    return ingest_polymarket_trades_rest_for_market(
        market_id=market_id,
        lookback_hours=lookback_hours,
    )

def ingest_polymarket_bbo_ws_for_market(
    market_id: str,
    max_events: int = 300,
    source: str = "clob_ws",
) -> Dict[str, Any]:
    run_type = "bbo_ws_single"

    def _iter_ws_items(events_obj: Any):
        for msg in events_obj or []:
            obj = msg
            if isinstance(obj, (bytes, bytearray)):
                try:
                    obj = obj.decode("utf-8", errors="ignore")
                except Exception:
                    continue

            if isinstance(obj, str):
                s = obj.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue

            if isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        yield item
                continue

            if isinstance(obj, dict):
                yield obj
                continue

    def _infer_event_type(ev: dict) -> str:
        event_type = ev.get("event_type") or ev.get("type") or ev.get("eventType")
        et = str(event_type or "").strip().lower()

        if et:
            return et

        if ("bids" in ev or "asks" in ev) and ("asset_id" in ev or "assetId" in ev):
            return "book"

        if ("best_bid" in ev or "bestBid" in ev or "best_ask" in ev or "bestAsk" in ev) and (
            "asset_id" in ev or "assetId" in ev
        ):
            return "best_bid_ask"

        if "price_changes" in ev or "priceChanges" in ev:
            return "price_change"

        return "unknown"

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            run_id = _run_start(
                cur,
                source=source,
                run_type=run_type,
                meta={"market_id": market_id, "max_events": max_events},
            )

            cur.execute(
                """
                SELECT external_id, raw
                FROM markets
                WHERE market_id = %s
                  AND protocol = 'polymarket'
                """,
                (market_id,),
            )
            row = cur.fetchone()

            if not row:
                conn.rollback()
                _run_finish(cur, run_id, "FAILED", error="market_not_found")
                conn.commit()
                return {"run_id": run_id, "status": "FAILED", "reason": "market_not_found"}

            external_id, raw_payload = row

            payload = raw_payload or {}
            ids = _extract_asset_ids(payload)

            if not ids and external_id:
                try:
                    detail = fetch_market_detail(external_id)
                    ids = _extract_asset_ids(detail or {})
                except Exception:
                    ids = []

            if not ids:
                conn.rollback()
                _run_finish(cur, run_id, "FAILED", error="no_asset_ids")
                conn.commit()
                return {
                    "run_id": run_id,
                    "status": "FAILED",
                    "reason": "no_asset_ids",
                    "market_id": market_id,
                }

            asset_to_market = {str(aid): market_id for aid in ids}
            asset_ids = list(asset_to_market.keys())

            events = stream_market_events_sync(asset_ids=asset_ids, max_events=max_events)

            ws_messages = 0
            bbo_ticks_inserted = 0
            ignored = 0

            from collections import Counter
            seen = Counter()

            for ev in _iter_ws_items(events):
                ws_messages += 1
                et = _infer_event_type(ev)
                seen[et] += 1

                try:
                    payload_hash = _hash_payload(ev)
                    raw_external = _event_source_id(source, ev)
                    cur.execute(
                        """
                        INSERT INTO raw_markets (source, external_market_id, payload, payload_hash)
                        VALUES (%s, %s, %s::jsonb, %s);
                        """,
                        (source, raw_external, Json(ev), payload_hash),
                    )
                except Exception:
                    pass

                if et == "price_change":
                    ts = _safe_ts(ev)
                    changes = ev.get("price_changes") or ev.get("priceChanges") or []
                    if not isinstance(changes, list):
                        ignored += 1
                        continue

                    for ch in changes:
                        if not isinstance(ch, dict):
                            continue

                        asset_id = (
                            ch.get("asset_id")
                            or ch.get("assetId")
                            or ch.get("token_id")
                            or ch.get("tokenId")
                        )
                        if asset_id is None:
                            continue

                        asset_id = str(asset_id)
                        mapped_market_id = asset_to_market.get(asset_id)
                        if not mapped_market_id:
                            continue

                        best_bid_s = _coerce_decimal_str(
                            ch.get("best_bid") or ch.get("bestBid") or ch.get("bid"),
                            default="0",
                        )
                        best_ask_s = _coerce_decimal_str(
                            ch.get("best_ask") or ch.get("bestAsk") or ch.get("ask"),
                            default="0",
                        )

                        if best_bid_s == "0" and best_ask_s == "0":
                            continue

                        spread_s = "0"
                        try:
                            if best_bid_s != "0" and best_ask_s != "0":
                                spread_s = str(max(float(best_ask_s) - float(best_bid_s), 0.0))
                        except Exception:
                            spread_s = "0"

                        cur.execute(
                            """
                            INSERT INTO market_bbo_ticks (
                                market_id, asset_id, ts, day, best_bid, best_ask, spread, source
                            )
                            VALUES (%s, %s, %s, %s, %s::numeric, %s::numeric, %s::numeric, %s)
                            ON CONFLICT (market_id, asset_id, ts) DO NOTHING
                            """,
                            (
                                mapped_market_id,
                                asset_id,
                                ts,
                                ts.date(),
                                best_bid_s,
                                best_ask_s,
                                spread_s,
                                source,
                            ),
                        )
                        bbo_ticks_inserted += 1

            _run_finish(
                cur,
                run_id,
                "OK",
                meta={
                    "market_id": market_id,
                    "asset_ids": len(asset_ids),
                    "ws_messages": ws_messages,
                    "bbo_ticks_inserted": bbo_ticks_inserted,
                    "ignored_events": ignored,
                    "ws_event_types": dict(seen),
                },
            )
            conn.commit()

            return {
                "run_id": run_id,
                "market_id": market_id,
                "asset_ids": len(asset_ids),
                "ws_messages": ws_messages,
                "bbo_ticks_inserted": bbo_ticks_inserted,
                "ignored_events": ignored,
                "ws_event_types": dict(seen),
            }

# ==========================================================
# Live runner
# ==========================================================
import time
import traceback


def run_polymarket_live(
    markets_refresh_every_sec: int = 600,
    trades_rest_refresh_every_sec: int = 180,
    trades_rest_lookback_hours: int = 240,
    markets_limit: int = 200,
    markets_offset: int = 0,
    ws_limit_markets: int = 50,
    ws_batch_events: int = 2000,
    sleep_after_batch_sec: float = 0.5,
) -> None:
    """
    Runs forever:
      1 refresh markets periodically from Gamma
      2 refresh REST trade catchup periodically from Polymarket Data API
      3 continuously pull WS batches and write market_bbo_ticks

    Rationale:
      - WS path keeps BBO fresh
      - REST path keeps trades fresh, since WS trade coverage has been stale
      - markets refresh improves mapping coverage for fast/new markets
    """
    last_markets_refresh = 0.0
    last_trades_rest_refresh = 0.0

    while True:
        try:
            now = time.time()

            if now - last_markets_refresh >= float(markets_refresh_every_sec):
                res = ingest_polymarket_markets(limit=markets_limit, offset=markets_offset)
                print("markets_refresh", res)
                last_markets_refresh = now

            if now - last_trades_rest_refresh >= float(trades_rest_refresh_every_sec):
                res = ingest_polymarket_trades_rest_job(
                    lookback_hours=trades_rest_lookback_hours,
                )
                print("trades_rest_refresh", res)
                last_trades_rest_refresh = now

            res = ingest_polymarket_bbo_ws(
                limit_markets=ws_limit_markets,
                max_events=ws_batch_events,
                source="clob_ws",
            )
            print("ws_batch", res)

            time.sleep(float(sleep_after_batch_sec))

        except KeyboardInterrupt:
            raise
        except Exception:
            print("live_runner_error restarting in 5s")
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    run_polymarket_live()