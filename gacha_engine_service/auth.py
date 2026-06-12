"""Gateway authentication helpers."""

from __future__ import annotations

import secrets
from uuid import UUID

from fastapi import Header, HTTPException, status

from .config import Settings


INTERNAL_TOKEN_HEADER = "X-Internal-Token"
USER_ID_HEADER = "X-User-Id"


def authenticate_gateway_request(
    *,
    settings: Settings,
    internal_token: str | None,
    user_id: str | None,
) -> UUID:
    """Validate gateway-injected headers and return the trusted user id."""

    expected_token = settings.internal_token
    if not expected_token or not internal_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "unauthorized", "message": "request is not authorized"},
        )

    if not secrets.compare_digest(internal_token, expected_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "unauthorized", "message": "request is not authorized"},
        )

    try:
        return UUID(str(user_id))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "invalid_user_id", "message": "X-User-Id must be a UUID"},
        ) from exc


def internal_token_header(
    value: str | None = Header(default=None, alias=INTERNAL_TOKEN_HEADER),
) -> str | None:
    return value


def user_id_header(value: str | None = Header(default=None, alias=USER_ID_HEADER)) -> str | None:
    return value

