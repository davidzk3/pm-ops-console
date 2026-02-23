from fastapi import Header, HTTPException
from .settings import DEMO_WRITE_KEY

def require_write_key(x_demo_key: str | None = Header(default=None)) -> str:
    """
    Dependency used by write endpoints.
    - Checks the demo write key (x-demo-key header)
    - Returns an operator string that the API can use as created_by
    """

    # If DEMO_WRITE_KEY is empty/None, allow writes (local dev mode)
    if not DEMO_WRITE_KEY:
        return "operator"

    if x_demo_key != DEMO_WRITE_KEY:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "invalid_write_key",
                "message": "Missing or invalid demo write key",
                "details": {},
            },
        )

    return "operator"
