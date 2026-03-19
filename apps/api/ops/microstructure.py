from __future__ import annotations

"""
DEPRECATED MODULE.

This module previously contained a full copy of the microstructure engine, which caused
drift and schema mismatches (t.amount, t.taker, etc).

Single source of truth is now:
  apps/api/services/microstructure.py

We keep this file as a compatibility shim for any older imports.
"""

from typing import Any, Dict, Optional
from datetime import date

from apps.api.services.microstructure import compute_microstructure_daily as _compute


def compute_microstructure_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
) -> Dict[str, Any]:
    return _compute(day=day, window_hours=window_hours, limit_markets=limit_markets)