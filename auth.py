"""
auth.py — JWT authentication via HTTP-only cookies.

Passwords are hashed with bcrypt. Tokens are signed with HS256.
The secret key is auto-generated on first run and persisted to secret.key.
"""

import os
import datetime
from typing import Optional

from fastapi import Depends, Request, HTTPException, status
from jose import jwt, JWTError
import bcrypt
from sqlalchemy.orm import Session

from database import SessionLocal, User, UserSettings

# ── Secret key management ─────────────────────────────────────────────────────
_SECRET_KEY_FILE = "secret.key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 72

def _load_or_create_secret():
    if os.path.exists(_SECRET_KEY_FILE):
        with open(_SECRET_KEY_FILE) as f:
            return f.read().strip()
    key = os.urandom(32).hex()
    with open(_SECRET_KEY_FILE, "w") as f:
        f.write(key)
    return key

SECRET_KEY = _load_or_create_secret()

# ── Password hashing ─────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()

def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


# ── JWT tokens ────────────────────────────────────────────────────────────────
def create_access_token(user_id: int, is_admin: bool) -> str:
    expire = datetime.datetime.utcnow() + datetime.timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    return jwt.encode(
        {"sub": str(user_id), "admin": is_admin, "exp": expire},
        SECRET_KEY, algorithm=ALGORITHM,
    )


def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


# ── FastAPI dependencies ──────────────────────────────────────────────────────
def _get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(request: Request, db: Session = Depends(_get_db)) -> Optional[User]:
    """Return the logged-in User or None (does not raise)."""
    token = request.cookies.get("access_token")
    if not token:
        return None
    payload = decode_token(token)
    if not payload:
        return None
    user = db.query(User).filter(User.id == int(payload["sub"])).first()
    if user and not user.is_active:
        return None
    return user


def require_user(request: Request, db: Session = Depends(_get_db)) -> User:
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Login required")
    return user


def require_admin(request: Request, db: Session = Depends(_get_db)) -> User:
    user = require_user(request, db)
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Admin access required")
    return user


# ── Seed default admin ────────────────────────────────────────────────────────
def seed_admin_if_needed():
    """Create a default admin account if no users exist."""
    db = SessionLocal()
    try:
        if db.query(User).count() == 0:
            admin = User(
                username="admin",
                email="admin@localhost",
                hashed_password=hash_password("admin"),
                is_admin=True,
            )
            db.add(admin)
            db.flush()
            db.add(UserSettings(user_id=admin.id))
            db.commit()
            print("[auth] Created default admin — username: admin / password: admin")
            print("[auth] *** Change this password immediately! ***")
    finally:
        db.close()
