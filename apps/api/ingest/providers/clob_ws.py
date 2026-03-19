from __future__ import annotations

import asyncio
import json
import os
import random
import time
from typing import Any, AsyncGenerator, Dict, List

import websockets


DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _ws_url() -> str:
    return os.getenv("POLYMARKET_WS_URL") or DEFAULT_WS_URL


def _build_subscribe_payload(asset_ids: List[str]) -> Dict[str, Any]:
    """
    Confirmed working payload from ws_test.py:

        {"type":"market","assets_ids":[...]}

    This produces immediate book / BBO messages.
    """
    return {
        "type": "market",
        "assets_ids": asset_ids,
    }


async def stream_market_events(
    asset_ids: List[str],
    *,
    max_events: int = 3000,
    max_seconds: int = 30,
    recv_timeout: float = 10.0,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Connect to Polymarket CLOB WS and yield decoded JSON events.

    Notes:
      - This feed primarily emits orderbook snapshots (bids/asks arrays)
      - It does NOT include trader wallet addresses
      - We exit after max_events OR max_seconds
    """

    if not asset_ids:
        return

    asset_ids = [str(x) for x in asset_ids]

    deadline = time.time() + max_seconds
    emitted = 0
    attempt = 0

    while time.time() < deadline and emitted < max_events:
        attempt += 1
        url = _ws_url()

        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_queue=256,
            ) as ws:

                # Send confirmed working subscribe payload
                payload = _build_subscribe_payload(asset_ids)
                await ws.send(json.dumps(payload))

                # Immediately expect traffic
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"Connected to {url} but received no data after subscribe."
                    )

                # Yield first message
                try:
                    msg = json.loads(raw)
                    yield msg
                    emitted += 1
                except Exception:
                    pass

                # Main receive loop
                while time.time() < deadline and emitted < max_events:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    yield msg
                    emitted += 1

        except websockets.exceptions.InvalidStatus as e:
            raise RuntimeError(
                f"WebSocket handshake failed. WS_URL={url}. Server rejected connection: {e}."
            ) from e

        except Exception:
            # Controlled reconnect with small backoff
            if time.time() >= deadline:
                break
            backoff = min(5.0, 0.5 * attempt) + random.random()
            await asyncio.sleep(backoff)

    return


def stream_market_events_sync(
    asset_ids: List[str],
    *,
    max_events: int = 3000,
    max_seconds: int = 30,
    recv_timeout: float = 10.0,
) -> List[Dict[str, Any]]:
    """
    Sync wrapper to collect events for ingestion runner.
    """

    out: List[Dict[str, Any]] = []

    async def _run() -> None:
        async for ev in stream_market_events(
            asset_ids,
            max_events=max_events,
            max_seconds=max_seconds,
            recv_timeout=recv_timeout,
        ):
            out.append(ev)

    asyncio.run(_run())
    return out