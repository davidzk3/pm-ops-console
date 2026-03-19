from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class RunResult:
    run_id: int
    source: str
    run_type: str
    status: str
    meta: Dict[str, Any]


@dataclass
class CanonicalMarket:
    protocol: str
    chain: str
    external_id: str
    title: str
    category: Optional[str] = None
    status: str = "ACTIVE"
    close_time: Optional[str] = None  # ISO string


@dataclass
class CanonicalMarketDay:
    market_external_id: str
    day: str  # YYYY-MM-DD
    volume: Optional[float] = None
    trades: Optional[int] = None
    unique_traders: Optional[int] = None
    spread_median: Optional[float] = None
    depth_2pct_median: Optional[float] = None
    concentration_hhi: Optional[float] = None