"""
app/core/security.py

JWT creation, decoding, and password hashing.
Moved from: backend/services/auth.py

CRITICAL: bcrypt must stay pinned to 4.0.1 in requirements.txt.
          passlib is incompatible with bcrypt 4.1+. Do NOT upgrade.

Usage:
    from app.core.security import (
        hash_password,
        verify_password,
        create_access_token,
        decode_token,
        get_current_user,
    )
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import get_db


# ---------------------------------------------------------------------------
# Password hashing
# bcrypt pinned to 4.0.1 — see requirements.txt
# ---------------------------------------------------------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    """Hash a plaintext password using bcrypt."""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plaintext password against a stored bcrypt hash."""
    return pwd_context.verify(plain_password, hashed_password)


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a signed JWT access token.

    Args:
        data: Payload to encode. Must include "sub" (email).
        expires_delta: Override the default expiry window.

    Returns:
        Signed JWT string.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> Optional[str]:
    """
    Decode a JWT and return the subject (email).

    Returns None instead of raising on invalid/expired tokens —
    callers decide how to handle a missing identity.
    """
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        return payload.get("sub")  # "sub" holds the user's email
    except JWTError:
        return None


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    """
    FastAPI dependency — resolves and returns the authenticated User.

    Raises HTTP 401 if the token is missing, invalid, or the user no longer exists.

    Usage in routes:
        @router.get("/protected")
        def protected(current_user: User = Depends(get_current_user)):
            ...
    """
    # Import here to avoid circular imports at module load time
    from app.models.user import User

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    email = decode_token(token)
    if email is None:
        raise credentials_exception

    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise credentials_exception

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive",
        )

    return user


def get_current_admin_user(
    current_user=Depends(get_current_user),
):
    """
    FastAPI dependency — like get_current_user but also enforces admin role.

    Raises HTTP 403 if the authenticated user is not an admin.

    Usage in routes:
        @router.delete("/admin/user/{id}")
        def delete_user(admin: User = Depends(get_current_admin_user)):
            ...
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user
