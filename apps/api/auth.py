import os
import time
from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_SECRET = os.getenv("JWT_SECRET", "dev_secret_change_me")
JWT_ALG = os.getenv("JWT_ALG", "HS256")
JWT_EXPIRES_MINUTES = int(os.getenv("JWT_EXPIRES_MINUTES", "720"))

# This is the key change: it makes Swagger/OpenAPI expose an "Authorize" button.
bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class AuthUser:
    user_id: str
    email: str
    role: str


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def create_access_token(user_id: str, email: str, role: str) -> str:
    now = int(time.time())
    exp = now + (JWT_EXPIRES_MINUTES * 60)
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "iat": now,
        "exp": exp,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def decode_access_token(token: str) -> AuthUser:
    # DEV shortcut: accept a fixed token without JWT decoding/expiry checks
    dev_token = os.getenv("DEV_BEARER_TOKEN", "")
    if dev_token and token == dev_token:
        return AuthUser(user_id="dev", email="dev@local", role="operator")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return AuthUser(
            user_id=str(payload.get("sub")),
            email=str(payload.get("email")),
            role=str(payload.get("role")),
        )
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def require_auth(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> AuthUser:
    if not creds or not creds.credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return decode_access_token(creds.credentials)


def require_operator(user: AuthUser = Depends(require_auth)) -> AuthUser:
    if user.role != "operator":
        raise HTTPException(status_code=403, detail="Insufficient role")
    return user