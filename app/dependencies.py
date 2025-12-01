from fastapi import Depends, Header, HTTPException, status

from app.settings import get_settings


async def verify_token(authorization: str | None = Header(default=None)) -> None:
    settings = get_settings()
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Authorization header")

    token = authorization.replace("Bearer", "").strip()
    if token != settings.api_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API token")


def get_settings_dependency():
    return get_settings()
