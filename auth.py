import os
from fastapi import Header, HTTPException, status

FASTAPI_API_KEY = os.getenv("FASTAPI_API_KEY")


def verify_api_key(authorization: str = Header(None)):
    if not FASTAPI_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="FASTAPI_API_KEY is not configured on the server"
        )

    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header"
        )

    expected = f"Bearer {FASTAPI_API_KEY}"

    if authorization != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )

    return True
