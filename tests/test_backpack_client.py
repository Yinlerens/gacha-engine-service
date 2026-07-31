from __future__ import annotations

import unittest
from uuid import UUID

import httpx

from gacha_engine_service.backpack_client import (
    BackpackReceiptClient,
    BackpackReceiptError,
)


USER_ID = UUID("ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e")
EVENT_ID = UUID("f7db8d82-41d2-4b43-9678-22ed0d07ffba")


class BackpackReceiptClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_pull_event_is_a_durable_reward_receipt(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(
                request.url.path,
                f"/v1/me/pull-events/{EVENT_ID}",
            )
            self.assertEqual(request.headers["X-Internal-Token"], "test-token")
            self.assertEqual(request.headers["X-User-Id"], str(USER_ID))
            self.assertEqual(request.headers["X-Request-Id"], str(EVENT_ID))
            return httpx.Response(
                200,
                json={"event": {"event_id": str(EVENT_ID)}, "records": []},
            )

        client = self._client(handler)
        try:
            applied = await client.has_pull_event(user_id=USER_ID, event_id=EVENT_ID)
        finally:
            await client.close()

        self.assertTrue(applied)

    async def test_missing_pull_event_is_not_treated_as_an_error(self) -> None:
        client = self._client(
            lambda _: httpx.Response(
                404,
                json={
                    "error": {
                        "code": "pull_event_not_found",
                        "message": "pull event was not found",
                    }
                },
            )
        )
        try:
            applied = await client.has_pull_event(user_id=USER_ID, event_id=EVENT_ID)
        finally:
            await client.close()

        self.assertFalse(applied)

    async def test_backpack_failure_is_distinct_from_a_missing_receipt(self) -> None:
        client = self._client(
            lambda _: httpx.Response(
                503,
                json={
                    "error": {
                        "code": "database_unavailable",
                        "message": "database is unavailable",
                    }
                },
            )
        )
        try:
            with self.assertRaises(BackpackReceiptError) as raised:
                await client.has_pull_event(user_id=USER_ID, event_id=EVENT_ID)
        finally:
            await client.close()

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.code, "database_unavailable")

    @staticmethod
    def _client(handler: object) -> BackpackReceiptClient:
        return BackpackReceiptClient(
            base_url="http://backpack-service",
            internal_token="test-token",
            timeout_seconds=1,
            transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
        )


if __name__ == "__main__":
    unittest.main()
