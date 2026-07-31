"""Async client for durable Backpack reward receipt checks."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx


class BackpackReceiptError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class BackpackReceiptUnavailable(BackpackReceiptError):
    def __init__(self, message: str = "backpack receipt is unavailable") -> None:
        super().__init__(503, "backpack_unavailable", message)


class BackpackReceiptClient:
    def __init__(
        self,
        *,
        base_url: str,
        internal_token: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._internal_token = internal_token
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def has_pull_event(self, *, user_id: UUID, event_id: UUID) -> bool:
        try:
            response = await self._client.get(
                f"{self._base_url}/v1/me/pull-events/{event_id}",
                headers={
                    "X-Internal-Token": self._internal_token,
                    "X-User-Id": str(user_id),
                    "X-Request-Id": str(event_id),
                },
            )
        except httpx.HTTPError as exc:
            raise BackpackReceiptUnavailable() from exc

        if response.status_code == 404:
            code, message = _response_error(response)
            if code == "pull_event_not_found":
                return False
            raise BackpackReceiptError(response.status_code, code, message)

        if not response.is_success:
            code, message = _response_error(response)
            raise BackpackReceiptError(response.status_code, code, message)

        try:
            body = response.json()
            event = body.get("event") if isinstance(body, dict) else None
            received_event_id = event.get("event_id") if isinstance(event, dict) else None
        except ValueError as exc:
            raise BackpackReceiptError(
                502,
                "backpack_receipt_invalid",
                "backpack receipt response is invalid",
            ) from exc

        if received_event_id != str(event_id):
            raise BackpackReceiptError(
                502,
                "backpack_receipt_invalid",
                "backpack receipt does not match the pull event",
            )
        return True


def _response_error(response: httpx.Response) -> tuple[str, str]:
    code = "backpack_request_failed"
    message = "backpack request failed"
    try:
        body: Any = response.json()
        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, dict):
            code = str(error.get("code") or code)
            message = str(error.get("message") or message)
    except ValueError:
        pass
    return code, message
