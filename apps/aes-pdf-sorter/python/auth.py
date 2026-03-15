import os
from datetime import datetime, timedelta
from typing import Optional

from jose import JWTError, jwt
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET_KEY = os.environ.get("SECRET_KEY", "a_very_secret_key_for_dev_purpose")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 48 * 60  # 48時間

FIXED_USERNAME = "admin"
FIXED_HASHED_PASSWORD = "$2b$12$wo/n/MChAv.kUtd6dQSvTe3DTuFyD6TpMiD/Mym0YaHNdsCP3RGOS"

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

if __name__ == "__main__":
    import secrets
    print(f"Generated SECRET_KEY if needed: {secrets.token_hex(32)}")
