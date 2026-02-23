import os
from typing import List


def _split_csv(v: str) -> List[str]:
    return [x.strip() for x in v.split(",") if x.strip()]


# -----------------------------
# Database
# -----------------------------
# Default should match your local pmops DB
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pmops:pmops@localhost:5432/pmops",
)


# -----------------------------
# CORS
# -----------------------------
# You can override via CORS_ORIGINS="http://localhost:3000,http://127.0.0.1:3000"
# Otherwise we fall back to safe local defaults.
_cors_env = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS = _split_csv(_cors_env) if _cors_env.strip() else [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]


# -----------------------------
# Demo write key (protect POST endpoints)
# -----------------------------
# In Next.js you send this as header: x-demo-key: <value>
# You already have NEXT_PUBLIC_DEMO_WRITE_KEY=devkey in web/.env.local
# So set DEMO_WRITE_KEY=devkey in api/.env as well (or your shell env).
DEMO_WRITE_KEY = os.getenv("DEMO_WRITE_KEY", "devkey")


# -----------------------------
# Environment
# -----------------------------
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")  # local | production