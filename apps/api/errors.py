from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError


def error_response(code: str, message: str, status_code: int = 400, details=None):
    payload = {
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        }
    }
    return JSONResponse(status_code=status_code, content=payload)


async def http_exception_handler(request: Request, exc):
    # Handles FastAPI HTTPException in a consistent format
    detail = exc.detail
    if isinstance(detail, dict):
        message = detail.get("message", "Request failed")
        code = detail.get("code", "http_error")
        details = detail.get("details", {})
    else:
        message = str(detail)
        code = "http_error"
        details = {}

    return error_response(code=code, message=message, status_code=exc.status_code, details=details)


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # Handles Pydantic/body validation errors consistently
    return error_response(
        code="validation_error",
        message="Invalid request payload",
        status_code=422,
        details={"errors": exc.errors()},
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    # Catch-all (don’t leak secrets). Still prints type for local debugging.
    print(f"[unhandled] {type(exc).__name__}")
    return error_response(
        code="internal_error",
        message="Internal server error",
        status_code=500,
        details={},
    )
