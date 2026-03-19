from __future__ import annotations

import hashlib
import json
import os
import random
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv("apps/api/.env")
load_dotenv()

import psycopg

from apps.api.db import get_db_dsn

BUILD_ID = "polymarket_trades_rest_ingest_2026_03_06_B"


# -------------------------
# Config
# -------------------------

DEFAULT_BASE_URL = os.getenv("POLYMARKET_DATA_API_BASE_URL", "https://data-api.polymarket.com")
DEFAULT_SOURCE = "data_api_rest"

DEFAULT_PAGE_LIMIT = int(os.getenv("POLYMARKET_TRADES_PAGE_LIMIT", "200"))
DEFAULT_MAX_PAGES = int(os.getenv("POLYMARKET_TRADES_MAX_PAGES", "20"))
DEFAULT_SLEEP_SECONDS = float(os.getenv("POLYMARKET_TRADES_SLEEP_SECONDS", "0.25"))

DEFAULT_MAX_RETRIES = int(os.getenv("POLYMARKET_TRADES_MAX_RETRIES", "6"))
DEFAULT_BACKOFF_BASE = float(os.getenv("POLYMARKET_TRADES_BACKOFF_BASE", "0.7"))

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}


# -------------------------
# DB helpers
# -------------------------

def _get_conn():
    return psycopg.connect(get_db_dsn())


def _ensure_cursor_table(conn) -> None:
    sql = """
    create table if not exists public.ingest_cursors (
      source text not null,
      cursor_key text not null,
      cursor_value text,
      updated_at timestamptz not null default now(),
      primary key (source, cursor_key)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)

def _load_polymarket_market_maps(conn) -> Dict[str, Dict[str, str]]:
    """
    Build in memory lookup maps for fast trade -> market_id resolution.
    """
    sql = """
    select
      market_id,
      raw ->> 'slug' as slug,
      raw ->> 'conditionId' as condition_id,
      raw ->> 'clobTokenIds' as clob_token_ids_json
    from public.markets
    where protocol = 'polymarket';
    """

    slug_map: Dict[str, str] = {}
    condition_map: Dict[str, str] = {}
    asset_map: Dict[str, str] = {}

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    for market_id, slug, condition_id, clob_token_ids_json in rows:
        if slug:
            slug_map[str(slug)] = market_id

        if condition_id:
            condition_map[str(condition_id).lower()] = market_id

        if clob_token_ids_json:
            try:
                token_ids = json.loads(clob_token_ids_json)
                if isinstance(token_ids, list):
                    for token_id in token_ids:
                        if token_id is not None:
                            asset_map[str(token_id)] = market_id
            except Exception:
                pass

    return {
        "slug_map": slug_map,
        "condition_map": condition_map,
        "asset_map": asset_map,
    }

def _get_cursor(conn, source: str, cursor_key: str) -> Optional[str]:
    sql = """
    select cursor_value
    from public.ingest_cursors
    where source = %(source)s and cursor_key = %(cursor_key)s
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"source": source, "cursor_key": cursor_key})
        row = cur.fetchone()
    return row[0] if row else None


def _set_cursor(conn, source: str, cursor_key: str, cursor_value: str) -> None:
    sql = """
    insert into public.ingest_cursors (source, cursor_key, cursor_value, updated_at)
    values (%(source)s, %(cursor_key)s, %(cursor_value)s, now())
    on conflict (source, cursor_key)
    do update set cursor_value = excluded.cursor_value, updated_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {"source": source, "cursor_key": cursor_key, "cursor_value": cursor_value},
        )


def _lookup_market_id_by_slug(conn, slug: Optional[str]) -> Optional[str]:
    if not slug:
        return None

    sql = """
    select market_id
    from public.markets
    where protocol = 'polymarket'
      and (
        raw ->> 'slug' = %(slug)s
        or url ilike %(url_like)s
      )
    order by updated_at desc nulls last
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "slug": slug,
                "url_like": f"%{slug}%",
            },
        )
        row = cur.fetchone()
    return row[0] if row else None

def _get_polymarket_condition_id(conn, market_id: str) -> Optional[str]:
    sql = """
    select raw ->> 'conditionId' as condition_id
    from public.markets
    where market_id = %s
      and protocol = 'polymarket'
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (market_id,))
        row = cur.fetchone()

    if not row:
        return None

    condition_id = row[0]
    if not condition_id:
        return None

    condition_id = str(condition_id).strip()
    return condition_id or None

# -------------------------
# HTTP helpers
# -------------------------

def _http_get_json(url: str, params: Dict[str, Any], timeout_s: float = 25.0) -> Any:
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    full = f"{url}?{q}" if q else url

    req = urllib.request.Request(full, headers=DEFAULT_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
        return json.loads(raw.decode("utf-8"))


def _retry_get_json(url: str, params: Dict[str, Any]) -> Any:
    last_err: Optional[Exception] = None

    for i in range(DEFAULT_MAX_RETRIES):
        try:
            return _http_get_json(url, params=params)
        except Exception as e:
            last_err = e
            if i == DEFAULT_MAX_RETRIES - 1:
                break

            sleep_s = min(8.0, DEFAULT_BACKOFF_BASE * (2 ** i))
            sleep_s = sleep_s * (0.85 + 0.3 * random.random())
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed GET after retries: {url} params={params} err={last_err}")


# -------------------------
# Trade model normalization
# -------------------------

@dataclass
class TradeRow:
    market_id: str
    day: date
    ts: datetime
    trader_id: str
    side: str
    price: float
    size: float
    notional: float
    source: str


def _parse_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None

    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=timezone.utc)
        return x.astimezone(timezone.utc)

    if isinstance(x, (int, float)):
        try:
            return datetime.fromtimestamp(float(x), tz=timezone.utc)
        except Exception:
            return None

    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None

        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

        try:
            return datetime.fromtimestamp(float(s), tz=timezone.utc)
        except Exception:
            return None

    return None


def _to_float(x: Any, default: float = 0.0) -> float:
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        return default


def _normalize_side(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().upper()
    if s in ("BUY", "B"):
        return "BUY"
    if s in ("SELL", "S"):
        return "SELL"
    return None


def _stable_trade_id(
    source: str,
    market_id: str,
    ts: datetime,
    trader_id: str,
    side: str,
    price: float,
    size: float,
    tx_hash: Optional[str] = None,
) -> str:
    base = (
        f"{source}|{market_id}|{ts.isoformat()}|{trader_id}|"
        f"{side}|{price:.10f}|{size:.10f}|{tx_hash or ''}"
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def _resolve_market_id_from_trade(raw: Dict[str, Any], maps: Dict[str, Dict[str, str]]) -> Optional[str]:
    condition_id = raw.get("conditionId")
    if condition_id:
        market_id = maps["condition_map"].get(str(condition_id).lower())
        if market_id:
            return market_id

    slug = raw.get("slug")
    if slug:
        market_id = maps["slug_map"].get(str(slug))
        if market_id:
            return market_id

    asset = raw.get("asset")
    if asset:
        market_id = maps["asset_map"].get(str(asset))
        if market_id:
            return market_id

    return None

def _normalize_trade(source: str, raw: Dict[str, Any], market_id: str) -> Optional[TradeRow]:
    ts = _parse_dt(raw.get("timestamp") or raw.get("time") or raw.get("createdAt") or raw.get("ts"))
    if not ts:
        return None

    price = _to_float(raw.get("price"))
    size = _to_float(raw.get("size") or raw.get("amount") or raw.get("quantity"))
    if price <= 0 or size <= 0:
        return None

    notional = _to_float(raw.get("notional"), default=price * size)

    side = _normalize_side(raw.get("side"))
    if side is None:
        return None

    trader_id = (
        raw.get("proxyWallet")
        or raw.get("trader")
        or raw.get("traderId")
        or raw.get("trader_id")
        or raw.get("user")
        or raw.get("owner")
        or raw.get("transactionHash")
    )
    if trader_id is None:
        return None
    trader_id = str(trader_id)

    return TradeRow(
        market_id=market_id,
        day=ts.date(),
        ts=ts,
        trader_id=trader_id,
        side=side,
        price=price,
        size=size,
        notional=notional,
        source=source,
    )


# -------------------------
# Fetch trades
# -------------------------

def _build_trades_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/trades"


def fetch_trades_page(
    base_url: str,
    limit: int,
    offset: int,
    market: Optional[str] = None,
) -> List[Dict[str, Any]]:
    
    """
    Do not send 'since' to this endpoint.
    Page with limit + offset only.

    Important:
    The data API can return HTTP 400 for large offsets.
    Treat that as end-of-pagination, not a hard failure.
    """
    url = _build_trades_url(base_url)
    params: Dict[str, Any] = {
        "limit": min(int(limit), 500),
        "offset": int(offset),
    }

    if market:
        params["market"] = market


    try:
        data = _retry_get_json(url, params=params)
    except RuntimeError as e:
        msg = str(e)
        if "HTTP Error 400" in msg:
            return []
        raise

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            return data["data"]
        if isinstance(data.get("trades"), list):
            return data["trades"]

    return []


# -------------------------
# Insert
# -------------------------

def _ensure_traders(conn, rows: List[TradeRow]) -> int:
    if not rows:
        return 0

    trader_ids = sorted({str(r.trader_id) for r in rows if r.trader_id})
    if not trader_ids:
        return 0

    sql = """
    insert into core.traders (trader_id)
    values (%s)
    on conflict do nothing;
    """

    with conn.cursor() as cur:
        cur.executemany(sql, [(tid,) for tid in trader_ids])

    return len(trader_ids)

def _insert_trades(conn, rows: List[TradeRow]) -> int:
    if not rows:
        return 0

    _ensure_traders(conn, rows)

    sql = """
    insert into core.trades (
      trade_id,
      market_id,
      trader_id,
      side,
      price,
      size,
      notional,
      ts,
      day,
      source
    )
    values (
      %(trade_id)s,
      %(market_id)s,
      %(trader_id)s,
      %(side)s,
      %(price)s,
      %(size)s,
      %(notional)s,
      %(ts)s,
      %(day)s,
      %(source)s
    )
    on conflict do nothing;
    """

    payload = [
        {
            "trade_id": _stable_trade_id(
                source=r.source,
                market_id=r.market_id,
                ts=r.ts,
                trader_id=r.trader_id,
                side=r.side,
                price=r.price,
                size=r.size,
                tx_hash=None,
            ),
            "market_id": r.market_id,
            "trader_id": r.trader_id,
            "side": r.side,
            "price": r.price,
            "size": r.size,
            "notional": r.notional,
            "ts": r.ts,
            "day": r.day,
            "source": r.source,
        }
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.executemany(sql, payload)

    return len(rows)

# -------------------------
# Public entrypoint
# -------------------------

def ingest_polymarket_trades_rest(
    lookback_hours: int = 240,
    use_cursor: bool = True,
    base_url: str = DEFAULT_BASE_URL,
    source: str = DEFAULT_SOURCE,
    page_limit: int = DEFAULT_PAGE_LIMIT,
    max_pages: int = DEFAULT_MAX_PAGES,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
    unmatched_examples: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if unmatched_examples is None:
        unmatched_examples = []

    started_at = datetime.now(timezone.utc)

    with _get_conn() as conn:
        _ensure_cursor_table(conn)

        market_maps = _load_polymarket_market_maps(conn)
        cursor_key = "polymarket_trades_since_ts"
        cursor_val = _get_cursor(conn, source=source, cursor_key=cursor_key) if use_cursor else None

        if cursor_val:
            since_ts = _parse_dt(cursor_val)
        else:
            since_ts = datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))

        if since_ts is None:
            since_ts = datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))

        total_raw = 0
        total_norm = 0
        total_insert_attempts = 0
        total_skipped_no_market_mapping = 0

        max_seen_ts: Optional[datetime] = since_ts

        for page in range(max_pages):
            offset = page * page_limit

            raw_trades = fetch_trades_page(
                base_url=base_url,
                limit=page_limit,
                offset=offset,
            )

            if not raw_trades:
                break

            total_raw += len(raw_trades)

            norm_rows: List[TradeRow] = []
            page_oldest_ts: Optional[datetime] = None

            for raw in raw_trades:
                if not isinstance(raw, dict):
                    continue

                ts = _parse_dt(raw.get("timestamp"))
                if ts is None:
                    continue

                if page_oldest_ts is None or ts < page_oldest_ts:
                    page_oldest_ts = ts

                if ts < since_ts:
                    continue

                market_id = _resolve_market_id_from_trade(raw, market_maps)
                if not market_id:
                    total_skipped_no_market_mapping += 1
                    if len(unmatched_examples) < 25:
                        unmatched_examples.append(
                            {
                                "slug": raw.get("slug"),
                                "conditionId": raw.get("conditionId"),
                                "asset": raw.get("asset"),
                                "title": raw.get("title"),
                                "timestamp": raw.get("timestamp"),
                            }
                        )
                    continue

                row = _normalize_trade(source=source, raw=raw, market_id=market_id)
                if row is None:
                    continue

                norm_rows.append(row)

                if max_seen_ts is None or row.ts > max_seen_ts:
                    max_seen_ts = row.ts

            total_norm += len(norm_rows)
            total_insert_attempts += _insert_trades(conn, norm_rows)

            conn.commit()

            if page_oldest_ts is not None and page_oldest_ts < since_ts:
                break

            if len(raw_trades) < page_limit:
                break

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        if max_seen_ts is not None and max_seen_ts > since_ts:
            _set_cursor(
                conn,
                source=source,
                cursor_key=cursor_key,
                cursor_value=max_seen_ts.isoformat(),
            )
            conn.commit()

    finished_at = datetime.now(timezone.utc)

    return {
        "status": "ok",
        "build_id": BUILD_ID,
        "source": source,
        "unmatched_examples": unmatched_examples,
        "base_url": base_url,
        "lookback_hours": lookback_hours,
        "since_ts": since_ts.isoformat(),
        "max_seen_ts": max_seen_ts.isoformat() if max_seen_ts else None,
        "raw_rows": total_raw,
        "normalized_rows": total_norm,
        "insert_attempts": total_insert_attempts,
        "skipped_no_market_mapping": total_skipped_no_market_mapping,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_seconds": (finished_at - started_at).total_seconds(),
    }

def ingest_polymarket_trades_rest_for_market(
    market_id: str,
    lookback_hours: int = 240,
    base_url: str = DEFAULT_BASE_URL,
    source: str = DEFAULT_SOURCE,
    page_limit: int = 500,
    max_pages: int = 3,
) -> Dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    since_ts = datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))

    total_raw = 0
    total_norm = 0
    total_insert_attempts = 0
    skipped_before_since = 0
    skipped_bad_rows = 0

    max_seen_ts: Optional[datetime] = None

    with _get_conn() as conn:
        condition_id = _get_polymarket_condition_id(conn, market_id)
        if not condition_id:
            finished_at = datetime.now(timezone.utc)
            return {
                "status": "failed",
                "reason": "missing_condition_id",
                "market_id": market_id,
                "lookback_hours": lookback_hours,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "elapsed_seconds": (finished_at - started_at).total_seconds(),
            }

        for page in range(max_pages):
            offset = page * page_limit

            raw_trades = fetch_trades_page(
                base_url=base_url,
                limit=page_limit,
                offset=offset,
                market=condition_id,
            )

            if not raw_trades:
                break

            total_raw += len(raw_trades)
            norm_rows: List[TradeRow] = []

            page_oldest_ts: Optional[datetime] = None

            for raw in raw_trades:
                if not isinstance(raw, dict):
                    skipped_bad_rows += 1
                    continue

                ts = _parse_dt(
                    raw.get("timestamp")
                    or raw.get("time")
                    or raw.get("createdAt")
                    or raw.get("ts")
                )
                if ts is None:
                    skipped_bad_rows += 1
                    continue

                if page_oldest_ts is None or ts < page_oldest_ts:
                    page_oldest_ts = ts

                if ts < since_ts:
                    skipped_before_since += 1
                    continue

                row = _normalize_trade(source=source, raw=raw, market_id=market_id)
                if row is None:
                    skipped_bad_rows += 1
                    continue

                norm_rows.append(row)

                if max_seen_ts is None or row.ts > max_seen_ts:
                    max_seen_ts = row.ts

            total_norm += len(norm_rows)
            total_insert_attempts += _insert_trades(conn, norm_rows)
            conn.commit()

            if page_oldest_ts is not None and page_oldest_ts < since_ts:
                break

            if len(raw_trades) < page_limit:
                break

    finished_at = datetime.now(timezone.utc)

    return {
        "status": "ok",
        "build_id": BUILD_ID,
        "source": source,
        "market_id": market_id,
        "condition_id": condition_id,
        "base_url": base_url,
        "lookback_hours": lookback_hours,
        "since_ts": since_ts.isoformat(),
        "max_seen_ts": max_seen_ts.isoformat() if max_seen_ts else None,
        "raw_rows": total_raw,
        "normalized_rows": total_norm,
        "insert_attempts": total_insert_attempts,
        "skipped_before_since": skipped_before_since,
        "skipped_bad_rows": skipped_bad_rows,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_seconds": (finished_at - started_at).total_seconds(),
    }