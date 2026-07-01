"""Async client for asset-service spend and compensation calls."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx


class AssetServiceError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class AssetServiceUnavailable(AssetServiceError):
    def __init__(self, message: str = "asset service is unavailable") -> None:
        super().__init__(503, "asset_unavailable", message)


class AssetClient:
    def __init__(
        self,
        *,
        base_url: str,
        internal_token: str,
        timeout_seconds: float,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._internal_token = internal_token
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def close(self) -> None:
        await self._client.aclose()

    async def ping(self) -> None:
        try:
            response = await self._client.get(f"{self._base_url}/health")
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise AssetServiceUnavailable() from exc

    async def spend(
        self,
        *,
        user_id: UUID,
        amount_minor: int,
        idempotency_key: str,
        reason: str,
        metadata: dict[str, Any],
        request_id: str = "",
    ) -> None:
        await self._post_entry(
            path="/v1/me/spends",
            user_id=user_id,
            amount_minor=amount_minor,
            idempotency_key=idempotency_key,
            reason=reason,
            metadata=metadata,
            request_id=request_id,
        )

    async def credit(
        self,
        *,
        user_id: UUID,
        amount_minor: int,
        idempotency_key: str,
        reason: str,
        metadata: dict[str, Any],
        request_id: str = "",
    ) -> None:
        await self._post_entry(
            path="/v1/me/credits",
            user_id=user_id,
            amount_minor=amount_minor,
            idempotency_key=idempotency_key,
            reason=reason,
            metadata=metadata,
            request_id=request_id,
        )

    async def _post_entry(
        self,
        *,
        path: str,
        user_id: UUID,
        amount_minor: int,
        idempotency_key: str,
        reason: str,
        metadata: dict[str, Any],
        request_id: str,
    ) -> None:
        headers = {
            "X-Internal-Token": self._internal_token,
            "X-User-Id": str(user_id),
            "Idempotency-Key": idempotency_key,
        }
        if request_id:
            headers["X-Request-Id"] = request_id

        try:
            response = await self._client.post(
                f"{self._base_url}{path}",
                headers=headers,
                json={
                    "amount_minor": amount_minor,
                    "reason": reason,
                    "metadata": metadata,
                },
            )
        except httpx.HTTPError as exc:
            raise AssetServiceUnavailable() from exc

        if response.is_success:
            return

        code = "asset_request_failed"
        message = "asset request failed"
        try:
            body = response.json()
            error = body.get("error") if isinstance(body, dict) else None
            if isinstance(error, dict):
                code = str(error.get("code") or code)
                message = str(error.get("message") or message)
        except ValueError:
            pass

        raise AssetServiceError(response.status_code, code, message)
