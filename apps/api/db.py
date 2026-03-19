from __future__ import annotations

import os

def get_db_dsn() -> str:
    # Load .env if present (Windows + reload subprocesses often miss env)
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    dsn = os.getenv("DATABASE_URL") or os.getenv("DB_DSN")
    if not dsn:
        raise RuntimeError("Missing DATABASE_URL (or DB_DSN) environment variable")
    return dsn