from __future__ import annotations

import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from gacha_engine_service.asset_client import AssetServiceError
from gacha_engine_service.catalog_config import CatalogSnapshot
from gacha_engine_service.config import Settings
from gacha_engine_service.main import create_app
from gacha_engine_service.schemas import PitySnapshot

from .fakes import FakeAssetClient, FakeEventPublisher, FakePityStateStore


USER_ID = "ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e"
HEADERS = {
    "X-Internal-Token": "test-token",
    "X-User-Id": USER_ID,
    "Idempotency-Key": "pull-test-1",
}


def make_client(
    *,
    state_store: FakePityStateStore | None = None,
    event_publisher: FakeEventPublisher | None = None,
    asset_client: FakeAssetClient | None = None,
    catalog_repository: object | None = None,
) -> tuple[TestClient, FakePityStateStore, FakeEventPublisher, FakeAssetClient]:
    state_store = state_store or FakePityStateStore()
    event_publisher = event_publisher or FakeEventPublisher()
    asset_client = asset_client or FakeAssetClient()
    app = create_app(
        settings=Settings(internal_token="test-token"),
        state_store=state_store,
        event_publisher=event_publisher,
        asset_client=asset_client,
        catalog_repository=catalog_repository,
    )
    return TestClient(app), state_store, event_publisher, asset_client


class EmptyCatalogRepository:
    async def load_snapshot(self) -> CatalogSnapshot:
        return CatalogSnapshot(
            source="test",
            loaded_at=datetime.now(timezone.utc),
            items=(),
            banners=(),
            banner_configs_by_id={},
        )

    async def close(self) -> None:
        return None


class ApiTests(unittest.TestCase):
    def test_health_returns_ok(self) -> None:
        client, _, _, _ = make_client()

        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_ready_returns_ready_when_dependencies_ping(self) -> None:
        client, _, _, _ = make_client()

        response = client.get("/ready")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ready", "checks": {}})

    def test_ready_reports_dependency_failures(self) -> None:
        client, _, _, _ = make_client(
            state_store=FakePityStateStore(ping_error=True),
            event_publisher=FakeEventPublisher(ping_error=True),
        )

        response = client.get("/ready")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["status"], "not_ready")
        self.assertEqual(response.json()["checks"], {"redis": "unavailable", "kafka": "unavailable"})

    def test_ready_reports_missing_internal_token(self) -> None:
        app = create_app(
            settings=Settings(internal_token=""),
            state_store=FakePityStateStore(),
            event_publisher=FakeEventPublisher(),
            asset_client=FakeAssetClient(),
        )
        client = TestClient(app)

        response = client.get("/ready")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["checks"], {"internal_token": "missing"})

    def test_empty_catalog_returns_empty_lists_and_missing_banner_404s(self) -> None:
        client, _, _, _ = make_client(catalog_repository=EmptyCatalogRepository())

        ready_response = client.get("/ready")
        banners_response = client.get("/v1/banners")
        items_response = client.get("/v1/items")
        pity_response = client.get("/v1/me/pity?banner_id=missing", headers=HEADERS)
        pull_response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "missing", "count": 1},
            headers=HEADERS,
        )

        self.assertEqual(ready_response.status_code, 200)
        self.assertEqual(ready_response.json(), {"status": "ready", "checks": {}})
        self.assertEqual(banners_response.status_code, 200)
        self.assertEqual(banners_response.json(), {"items": []})
        self.assertEqual(items_response.status_code, 200)
        self.assertEqual(items_response.json(), {"items": []})
        self.assertEqual(pity_response.status_code, 404)
        self.assertEqual(pity_response.json()["error"]["code"], "banner_not_found")
        self.assertEqual(pull_response.status_code, 404)
        self.assertEqual(pull_response.json()["error"]["code"], "banner_not_found")

    def test_pull_requires_gateway_token(self) -> None:
        client, _, _, _ = make_client()

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1},
            headers={"X-User-Id": USER_ID},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "unauthorized")

    def test_pull_rejects_invalid_user_id(self) -> None:
        client, _, _, _ = make_client()

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1},
            headers={"X-Internal-Token": "test-token", "X-User-Id": "not-a-uuid"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_user_id")

    def test_pull_requires_idempotency_key(self) -> None:
        client, _, _, _ = make_client()

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1},
            headers={
                "X-Internal-Token": "test-token",
                "X-User-Id": USER_ID,
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "missing_idempotency_key")

    def test_pull_rejects_unknown_banner(self) -> None:
        client, _, _, _ = make_client()

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "missing", "count": 1},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "banner_not_found")

    def test_pull_rejects_invalid_count(self) -> None:
        client, _, _, _ = make_client()

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 2},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 422)

    def test_pull_updates_snapshot_and_publishes_event(self) -> None:
        client, state_store, event_publisher, asset_client = make_client()

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10, "seed": "api-seed"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["seed"], "api-seed")
        self.assertEqual(payload["previous_pity"]["version"], 0)
        self.assertEqual(payload["next_pity"]["version"], 1)
        self.assertEqual(payload["state_version"], 1)
        self.assertEqual(len(payload["records"]), 10)
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(event_publisher.events), 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.spends[0]["amount_minor"], 1600)
        self.assertEqual(asset_client.spends[0]["reason"], "gacha_pull")
        self.assertTrue(str(asset_client.spends[0]["idempotency_key"]).startswith("gacha-pull:"))

        event = event_publisher.events[0]
        self.assertEqual(event.user_id, USER_ID)
        self.assertEqual(event.banner_id, "limited-character-001")
        self.assertEqual(event.seed, "api-seed")
        self.assertEqual(event.state_version, 1)
        self.assertEqual(event.records, event_publisher.events[0].records)

    def test_pull_reuses_succeeded_idempotency_key(self) -> None:
        client, state_store, event_publisher, asset_client = make_client()

        first = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "same-key"},
            headers=HEADERS,
        )
        second = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "same-key"},
            headers=HEADERS,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json(), first.json())
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(len(event_publisher.events), 1)

    def test_pull_rejects_idempotency_key_reuse_with_different_request(self) -> None:
        client, state_store, event_publisher, asset_client = make_client()

        first = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "same-key"},
            headers=HEADERS,
        )
        second = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10, "seed": "same-key"},
            headers=HEADERS,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 409)
        self.assertEqual(second.json()["error"]["code"], "idempotency_conflict")
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(len(event_publisher.events), 1)

    def test_pull_returns_conflict_when_assets_are_insufficient(self) -> None:
        client, state_store, event_publisher, asset_client = make_client(
            asset_client=FakeAssetClient(
                spend_error=AssetServiceError(409, "insufficient_funds", "balance cannot go below zero")
            )
        )

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "no-balance"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "insufficient_assets")
        self.assertEqual(state_store.snapshot.version, 0)
        self.assertEqual(len(asset_client.spends), 0)
        self.assertEqual(len(asset_client.credits), 0)
        self.assertEqual(len(event_publisher.events), 0)

    def test_pull_returns_conflict_when_redis_version_changes(self) -> None:
        client, _, _, asset_client = make_client(state_store=FakePityStateStore(conflict=True))

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "conflict"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "pity_version_conflict")
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(len(asset_client.credits), 1)
        self.assertEqual(asset_client.credits[0]["reason"], "gacha_pull_refund")

    def test_pull_returns_unavailable_when_redis_fails(self) -> None:
        client, _, _, asset_client = make_client(state_store=FakePityStateStore(unavailable=True))

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "redis-fail"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "redis_unavailable")
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(len(asset_client.credits), 1)

    def test_pull_returns_unavailable_when_kafka_publish_fails(self) -> None:
        client, state_store, _, asset_client = make_client(event_publisher=FakeEventPublisher(publish_error=True))

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "kafka-fail"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "kafka_unavailable")
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(len(asset_client.credits), 0)

    def test_pull_replays_pending_event_for_same_idempotency_key(self) -> None:
        event_publisher = FakeEventPublisher(publish_error=True)
        client, state_store, event_publisher, asset_client = make_client(event_publisher=event_publisher)

        first = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "kafka-retry"},
            headers=HEADERS,
        )
        event_publisher.publish_error = False
        second = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "kafka-retry"},
            headers=HEADERS,
        )

        self.assertEqual(first.status_code, 503)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(len(event_publisher.events), 1)

    def test_get_pity_returns_initial_snapshot(self) -> None:
        client, _, _, _ = make_client(
            state_store=FakePityStateStore(snapshot=PitySnapshot(version=0)),
        )

        response = client.get(
            "/v1/me/pity?banner_id=limited-character-001",
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["version"], 0)
        self.assertEqual(response.json()["since_five"], 0)


if __name__ == "__main__":
    unittest.main()
