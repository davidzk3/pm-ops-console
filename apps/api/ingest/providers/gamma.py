from __future__ import annotations

from typing import Any, Dict, List, Optional
import os
import requests

GAMMA_BASE = os.getenv("GAMMA_BASE", "https://gamma-api.polymarket.com")


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{GAMMA_BASE}{path}"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_active_events(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Best practice: use the events endpoint for market discovery.
    Docs recommend:
      /events?active=true&closed=false&limit=...&offset=...
    """
    data = _get(
        "/events",
        params={
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
        },
    )
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "events" in data and isinstance(data["events"], list):
        return data["events"]
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    raise RuntimeError(f"Unexpected events response shape: {type(data)}")


def fetch_markets(limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Return a flat list of markets.

    Primary strategy (recommended): fetch active events and extract event["markets"].
    Fallback: hit /markets endpoints.
    """
    # ---- Primary: events -> markets
    try:
        events = fetch_active_events(limit=min(limit, 100), offset=offset)
        markets: List[Dict[str, Any]] = []
        for ev in events:
            mlist = ev.get("markets")
            if isinstance(mlist, list):
                for m in mlist:
                    if isinstance(m, dict):
                        # enrich with event context if useful later
                        m.setdefault("_event_slug", ev.get("slug"))
                        m.setdefault("_event_id", ev.get("id"))
                        markets.append(m)
        if markets:
            # If events returns fewer than you asked, it's fine for now.
            # We'll page in the runner later when needed.
            return markets
    except Exception:
        pass

    # ---- Fallbacks: direct markets endpoints
    candidates = [
        ("/markets", {"limit": limit, "offset": offset}),
        ("/markets/active", {"limit": limit, "offset": offset}),
        ("/markets", None),
    ]

    last_err = None
    for path, params in candidates:
        try:
            data = _get(path, params=params)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "markets" in data and isinstance(data["markets"], list):
                return data["markets"]
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                return data["data"]
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Gamma markets fetch failed: {last_err}")


def fetch_market_detail(external_id: str) -> Dict[str, Any]:
    """
    Try a few common detail endpoints.
    """
    candidates = [
        f"/markets/{external_id}",
        f"/market/{external_id}",
    ]
    last_err = None
    for path in candidates:
        try:
            data = _get(path)
            if isinstance(data, dict):
                return data
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Gamma market detail fetch failed for {external_id}: {last_err}")