from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi.testclient import TestClient

from gacha_engine_service.asset_client import AssetServiceError
from gacha_engine_service.catalog_config import (
    CatalogSnapshot,
    ScheduledBannerConfig,
    static_catalog_snapshot,
)
from gacha_engine_service.config import Settings
from gacha_engine_service.main import (
    AppServices,
    create_app,
    execute_claimed_pull,
    recover_expired_processing_pulls_once,
    recover_pending_pull_events_once,
    recover_pending_pull_refunds_once,
)
from gacha_engine_service.postgres_state import PostgresGachaStateStore
from gacha_engine_service.pull_operations import PullOperation, PullRecoveryContext
from gacha_engine_service.schemas import PitySnapshot

from .fakes import FakeAssetClient, FakeEventPublisher, FakePityStateStore


USER_ID = "ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e"
REQUEST_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
HEADERS = {
    "X-Internal-Token": "test-token",
    "X-User-Id": USER_ID,
    "Idempotency-Key": "pull-test-1",
    "X-Request-Id": REQUEST_ID,
    "X-Request-Accepted-At": datetime.now(timezone.utc).isoformat(),
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


def seed_pending_refund(state_store: FakePityStateStore) -> None:
    banner_config = static_catalog_snapshot().banner_configs_by_id[
        "limited-character-001"
    ]
    context = PullRecoveryContext.from_banner_config(
        banner_config=banner_config,
        count=1,
        seed="pending-refund",
        event_id="44444444-4444-4444-8444-444444444444",
        amount_minor=160,
        request_id=REQUEST_ID,
    )
    key = (UUID(USER_ID), HEADERS["Idempotency-Key"])
    operation_key = "55555555-5555-4555-8555-555555555555"
    state_store.operations[key] = PullOperation(
        status="refund_pending",
        request_hash="f" * 64,
        error_code="pull_compensation_required",
        error_message="refund is pending",
        recovery_context=context,
    )
    state_store.operation_ids[key] = operation_key
    state_store.operation_keys[operation_key] = key


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


class FixedCatalogRepository:
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        self.snapshot = snapshot

    async def load_snapshot(self) -> CatalogSnapshot:
        return self.snapshot

    async def close(self) -> None:
        return None


def grouped_catalog_snapshot() -> CatalogSnapshot:
    snapshot = static_catalog_snapshot()
    limited_config = snapshot.banner_configs_by_id["limited-character-001"]
    standard_config = snapshot.banner_configs_by_id["standard-001"]
    next_limited_banner = limited_config.banner.model_copy(
        update={
            "id": "limited-character-002",
            "name": "归潮观测·续期",
        }
    )
    shared_group_id = "limited-character-shared"
    first_limited = replace(
        limited_config,
        banner_version_id="11111111-1111-4111-8111-111111111111",
        version=1,
        pity_group_id=shared_group_id,
    )
    next_limited = replace(
        limited_config,
        banner=next_limited_banner,
        banner_version_id="22222222-2222-4222-8222-222222222222",
        version=2,
        pity_group_id=shared_group_id,
    )
    isolated_standard = replace(
        standard_config,
        pity_group_id="standard-isolated",
    )
    return CatalogSnapshot(
        source="test",
        loaded_at=datetime.now(timezone.utc),
        items=snapshot.items,
        banners=(
            first_limited.banner,
            next_limited.banner,
            isolated_standard.banner,
        ),
        banner_configs_by_id={
            first_limited.banner.id: first_limited,
            next_limited.banner.id: next_limited,
            isolated_standard.banner.id: isolated_standard,
        },
    )


class ApiTests(unittest.TestCase):
    def test_health_returns_ok(self) -> None:
        client, _, _, _ = make_client()

        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertIn("x-request-id", response.headers)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_pull_requires_trusted_gateway_accepted_at(self) -> None:
        client, _, _, _ = make_client()
        headers = {key: value for key, value in HEADERS.items() if key != "X-Request-Accepted-At"}

        response = client.post(
            "/v1/me/pulls",
            headers=headers,
            json={"banner_id": "limited-character-001", "count": 1},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "missing_request_accepted_at")

    def test_pull_uses_gateway_acceptance_time_at_activity_cutoff(self) -> None:
        base_snapshot = static_catalog_snapshot()
        banner_config = base_snapshot.banner_configs_by_id["limited-character-001"]
        cutoff = datetime.now(timezone.utc)
        scheduled = ScheduledBannerConfig(
            config=banner_config,
            effective_from=cutoff - timedelta(days=1),
            effective_to=cutoff,
        )
        snapshot = CatalogSnapshot(
            source="test",
            loaded_at=cutoff + timedelta(seconds=10),
            items=base_snapshot.items,
            banners=(),
            banner_configs_by_id={},
            banner_schedules_by_id={banner_config.banner.id: (scheduled,)},
        )
        client, state_store, _, asset_client = make_client(
            catalog_repository=FixedCatalogRepository(snapshot)
        )
        before_cutoff = cutoff - timedelta(microseconds=1)
        headers = {
            **HEADERS,
            "X-Request-Accepted-At": before_cutoff.isoformat(),
            "Idempotency-Key": "cutoff-before",
        }

        accepted = client.post(
            "/v1/me/pulls",
            headers=headers,
            json={"banner_id": banner_config.banner.id, "count": 1},
        )

        self.assertEqual(accepted.status_code, 200, accepted.text)
        operation = state_store.operations[(UUID(USER_ID), "cutoff-before")]
        self.assertEqual(operation.recovery_context.accepted_at, before_cutoff)
        self.assertEqual(len(asset_client.spends), 1)

        rejected = client.post(
            "/v1/me/pulls",
            headers={
                **HEADERS,
                "X-Request-Accepted-At": cutoff.isoformat(),
                "Idempotency-Key": "cutoff-at",
            },
            json={"banner_id": banner_config.banner.id, "count": 1},
        )

        self.assertEqual(rejected.status_code, 404)
        self.assertEqual(rejected.json()["error"]["code"], "banner_not_found")
        self.assertEqual(len(asset_client.spends), 1)

    def test_completed_pull_replays_after_activity_end(self) -> None:
        base_snapshot = static_catalog_snapshot()
        banner_config = base_snapshot.banner_configs_by_id["limited-character-001"]
        cutoff = datetime.now(timezone.utc)
        snapshot = CatalogSnapshot(
            source="test",
            loaded_at=cutoff,
            items=base_snapshot.items,
            banners=(),
            banner_configs_by_id={},
            banner_schedules_by_id={
                banner_config.banner.id: (
                    ScheduledBannerConfig(
                        config=banner_config,
                        effective_from=cutoff - timedelta(days=1),
                        effective_to=cutoff,
                    ),
                )
            },
        )
        client, _, _, asset_client = make_client(
            catalog_repository=FixedCatalogRepository(snapshot)
        )
        idempotency_key = "cutoff-replay"
        request_body = {"banner_id": banner_config.banner.id, "count": 1}

        first = client.post(
            "/v1/me/pulls",
            headers={
                **HEADERS,
                "X-Request-Accepted-At": (cutoff - timedelta(microseconds=1)).isoformat(),
                "Idempotency-Key": idempotency_key,
            },
            json=request_body,
        )
        replay = client.post(
            "/v1/me/pulls",
            headers={
                **HEADERS,
                "X-Request-Accepted-At": (cutoff + timedelta(seconds=1)).isoformat(),
                "Idempotency-Key": idempotency_key,
            },
            json=request_body,
        )

        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(replay.json(), first.json())
        self.assertEqual(len(asset_client.spends), 1)

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
        self.assertEqual(
            response.json()["checks"],
            {"state_database": "unavailable", "kafka": "unavailable"},
        )

    def test_default_state_store_is_postgres_authoritative(self) -> None:
        app = create_app(
            settings=Settings(
                internal_token="test-token",
                gacha_state_database_url="postgresql://state",
            ),
            event_publisher=FakeEventPublisher(),
            asset_client=FakeAssetClient(),
        )

        self.assertIsInstance(app.state.services.state_store, PostgresGachaStateStore)

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

    def test_get_pull_operation_requires_gateway_authentication(self) -> None:
        client, _, _, _ = make_client()

        response = client.get(
            "/v1/me/pulls/operation",
            headers={
                "X-User-Id": USER_ID,
                "Idempotency-Key": HEADERS["Idempotency-Key"],
            },
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "unauthorized")

    def test_get_pull_operation_returns_only_safe_current_user_state(self) -> None:
        client, state_store, _, _ = make_client()
        pull_response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "query-state"},
            headers=HEADERS,
        )

        response = client.get("/v1/me/pulls/operation", headers=HEADERS)

        self.assertEqual(pull_response.status_code, 200)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "succeeded",
                "response": pull_response.json(),
                "error": None,
            },
        )
        self.assertEqual(set(response.json()), {"status", "response", "error"})
        serialized = response.text
        self.assertNotIn("request_hash", serialized)
        self.assertNotIn(USER_ID, serialized)
        self.assertIn((UUID(USER_ID), HEADERS["Idempotency-Key"]), state_store.operations)

    def test_get_pull_operation_does_not_cross_user_boundaries(self) -> None:
        client, _, _, _ = make_client()
        created = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1},
            headers=HEADERS,
        )
        other_user_headers = {
            **HEADERS,
            "X-User-Id": "b6cc17d1-4e6e-4691-9ed7-d5893ab2dd2d",
        }

        response = client.get(
            f"/v1/me/pulls/operation?user_id={USER_ID}",
            headers=other_user_headers,
        )

        self.assertEqual(created.status_code, 200)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "pull_operation_not_found")

    def test_get_pull_operation_projects_processing_event_pending_and_failed_states(self) -> None:
        client, state_store, _, _ = make_client()
        created = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1},
            headers=HEADERS,
        )
        self.assertEqual(created.status_code, 200)
        operation_key = (UUID(USER_ID), HEADERS["Idempotency-Key"])
        completed_operation = state_store.operations[operation_key]

        cases = [
            (
                PullOperation(status="processing", request_hash="private-processing-hash"),
                {"status": "processing", "response": None, "error": None},
            ),
            (
                completed_operation.model_copy(update={"status": "event_pending"}),
                {"status": "event_pending", "response": created.json(), "error": None},
            ),
            (
                PullOperation(
                    status="refund_pending",
                    request_hash="private-refund-hash",
                    error_code="pity_version_conflict",
                    error_message="refund is pending",
                ),
                {
                    "status": "refund_pending",
                    "response": None,
                    "error": {"code": "pity_version_conflict", "message": "refund is pending"},
                },
            ),
            (
                PullOperation(
                    status="failed",
                    request_hash="private-failed-hash",
                    error_code="insufficient_assets",
                    error_message="balance is insufficient",
                ),
                {
                    "status": "failed",
                    "response": None,
                    "error": {"code": "insufficient_assets", "message": "balance is insufficient"},
                },
            ),
        ]

        for operation, expected in cases:
            with self.subTest(status=operation.status):
                state_store.operations[operation_key] = operation
                response = client.get("/v1/me/pulls/operation", headers=HEADERS)

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)
                self.assertEqual(set(response.json()), {"status", "response", "error"})
                self.assertNotIn("request_hash", response.text)

    def test_get_pull_operation_returns_not_found_without_starting_a_pull(self) -> None:
        client, state_store, event_publisher, asset_client = make_client()

        response = client.get("/v1/me/pulls/operation", headers=HEADERS)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "pull_operation_not_found")
        self.assertEqual(state_store.snapshot.version, 0)
        self.assertEqual(asset_client.spends, [])
        self.assertEqual(event_publisher.events, [])

    def test_get_pull_operation_reports_state_database_failure_as_unknown(self) -> None:
        client, _, _, _ = make_client(state_store=FakePityStateStore(unavailable=True))

        response = client.get("/v1/me/pulls/operation", headers=HEADERS)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "state_store_unavailable")

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
        self.assertEqual(payload["pity_group_id"], "limited-character-001")
        self.assertEqual(payload["previous_pity"]["version"], 0)
        self.assertEqual(payload["next_pity"]["version"], 1)
        self.assertEqual(payload["state_version"], 1)
        self.assertEqual(len(payload["records"]), 10)
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(event_publisher.events), 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.spends[0]["amount_minor"], 1600)
        self.assertEqual(asset_client.spends[0]["reason"], "gacha_pull")
        self.assertEqual(asset_client.spends[0]["request_id"], REQUEST_ID)
        self.assertEqual(response.headers["x-request-id"], REQUEST_ID)
        self.assertTrue(str(asset_client.spends[0]["idempotency_key"]).startswith("gacha-pull:"))

        event = event_publisher.events[0]
        self.assertEqual(event.user_id, USER_ID)
        self.assertEqual(event.banner_id, "limited-character-001")
        self.assertEqual(event.pity_group_id, "limited-character-001")
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

    def test_timed_out_client_can_recover_completed_pull_without_a_second_charge(self) -> None:
        client, state_store, event_publisher, asset_client = make_client()

        completed_but_unseen = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10},
            headers=HEADERS,
        )
        operation = client.get("/v1/me/pulls/operation", headers=HEADERS)
        recovered = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10},
            headers=HEADERS,
        )

        self.assertEqual(completed_but_unseen.status_code, 200)
        self.assertEqual(operation.status_code, 200)
        self.assertEqual(operation.json()["status"], "succeeded")
        self.assertEqual(operation.json()["response"], completed_but_unseen.json())
        self.assertEqual(recovered.json(), completed_but_unseen.json())
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.spends[0]["amount_minor"], 1_600)
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

    def test_persistent_pity_contention_is_deferred_without_refund(self) -> None:
        client, _, _, asset_client = make_client(state_store=FakePityStateStore(conflict=True))

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "conflict"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "pull_recovery_pending")
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])

    def test_two_concurrent_pulls_recalculate_from_the_committed_pity(self) -> None:
        state_store = FakePityStateStore(synchronized_snapshot_readers=2)
        event_publisher = FakeEventPublisher()
        asset_client = FakeAssetClient()
        services = AppServices(state_store, event_publisher, object(), asset_client)
        banner_config = static_catalog_snapshot().banner_configs_by_id[
            "limited-character-001"
        ]

        async def execute_two_pulls() -> list[object]:
            executions: list[object] = []
            for index in range(2):
                context = PullRecoveryContext.from_banner_config(
                    banner_config=banner_config,
                    count=10,
                    seed=f"concurrent-{index}",
                    event_id=f"66666666-6666-4666-8666-66666666666{index}",
                    amount_minor=1600,
                    request_id=REQUEST_ID,
                )
                request_hash = str(index + 1) * 64
                claim = await state_store.begin_pull_operation(
                    user_id=UUID(USER_ID),
                    idempotency_key=f"concurrent-pull-{index}",
                    request_hash=request_hash,
                    processing_lease_seconds=30,
                    recovery_context=context,
                )
                assert claim.processing_token is not None
                executions.append(
                    execute_claimed_pull(
                        services=services,
                        user_id=UUID(USER_ID),
                        operation_key=claim.operation_key,
                        request_hash=request_hash,
                        processing_token=claim.processing_token,
                        context=context,
                    )
                )
            return list(await asyncio.gather(*executions))

        responses = asyncio.run(execute_two_pulls())

        self.assertEqual(
            sorted(response.previous_pity.version for response in responses),
            [0, 1],
        )
        self.assertEqual(
            sorted(response.next_pity.version for response in responses),
            [1, 2],
        )
        self.assertEqual(state_store.snapshot.version, 2)
        self.assertEqual(len(asset_client.spends), 2)
        self.assertEqual(asset_client.credits, [])
        self.assertEqual(len(event_publisher.events), 2)

    def test_pity_group_is_shared_across_banner_versions_and_isolated_from_other_groups(self) -> None:
        catalog = grouped_catalog_snapshot()
        client, state_store, event_publisher, _ = make_client(
            catalog_repository=FixedCatalogRepository(catalog),
        )

        first = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "group-v1"},
            headers=HEADERS | {"Idempotency-Key": "group-v1"},
        )
        next_version = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-002", "count": 1, "seed": "group-v2"},
            headers=HEADERS | {"Idempotency-Key": "group-v2"},
        )
        isolated = client.post(
            "/v1/me/pulls",
            json={"banner_id": "standard-001", "count": 1, "seed": "isolated"},
            headers=HEADERS | {"Idempotency-Key": "group-isolated"},
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(next_version.status_code, 200)
        self.assertEqual(isolated.status_code, 200)
        self.assertEqual(first.json()["previous_pity"]["version"], 0)
        self.assertEqual(next_version.json()["previous_pity"]["version"], 1)
        self.assertEqual(isolated.json()["previous_pity"]["version"], 0)
        self.assertEqual(
            state_store.snapshot_writes,
            [
                "limited-character-shared",
                "limited-character-shared",
                "standard-isolated",
            ],
        )
        self.assertEqual(
            [event.pity_group_id for event in event_publisher.events],
            [
                "limited-character-shared",
                "limited-character-shared",
                "standard-isolated",
            ],
        )

    def test_refund_is_persisted_before_asset_credit(self) -> None:
        state_store = FakePityStateStore()
        seed_pending_refund(state_store)
        status_during_credit: list[str] = []
        operation_key = (UUID(USER_ID), HEADERS["Idempotency-Key"])
        asset_client = FakeAssetClient(
            before_credit=lambda: status_during_credit.append(
                state_store.operations[operation_key].status
            )
        )
        recovered_count = asyncio.run(
            recover_pending_pull_refunds_once(
                AppServices(state_store, FakeEventPublisher(), object(), asset_client),
                limit=10,
                lock_ttl_seconds=30,
            )
        )

        self.assertEqual(recovered_count, 1)
        self.assertEqual(status_during_credit, ["refund_pending"])

    def test_pending_refund_recovers_without_another_client_request(self) -> None:
        state_store = FakePityStateStore()
        seed_pending_refund(state_store)
        asset_client = FakeAssetClient(
            credit_error=AssetServiceError(
                503,
                "asset_unavailable",
                "asset service timed out",
            )
        )
        services = AppServices(state_store, FakeEventPublisher(), object(), asset_client)

        deferred_recovery = asyncio.run(
            recover_pending_pull_refunds_once(
                services,
                limit=10,
                lock_ttl_seconds=30,
            )
        )
        asset_client.credit_error = None
        completed_recovery = asyncio.run(
            recover_pending_pull_refunds_once(
                services,
                limit=10,
                lock_ttl_seconds=30,
            )
        )
        second_recovery = asyncio.run(
            recover_pending_pull_refunds_once(
                services,
                limit=10,
                lock_ttl_seconds=30,
            )
        )

        self.assertEqual(deferred_recovery, 0)
        self.assertEqual(completed_recovery, 1)
        self.assertEqual(second_recovery, 0)
        self.assertEqual(len(asset_client.credits), 1)
        operation = state_store.operations[(UUID(USER_ID), HEADERS["Idempotency-Key"])]
        self.assertEqual(operation.status, "failed")
        self.assertIsNotNone(operation.recovery_context)

    def test_two_refund_workers_only_complete_one_refund(self) -> None:
        state_store = FakePityStateStore()
        seed_pending_refund(state_store)
        asset_client = FakeAssetClient()
        services = AppServices(state_store, FakeEventPublisher(), object(), asset_client)

        async def recover_concurrently() -> list[int]:
            return list(
                await asyncio.gather(
                    recover_pending_pull_refunds_once(
                        services,
                        limit=10,
                        lock_ttl_seconds=30,
                    ),
                    recover_pending_pull_refunds_once(
                        services,
                        limit=10,
                        lock_ttl_seconds=30,
                    ),
                )
            )

        recovered_counts = asyncio.run(recover_concurrently())

        self.assertEqual(sum(recovered_counts), 1)
        self.assertEqual(len(asset_client.credits), 1)

    def test_uncertain_state_commit_never_triggers_a_refund(self) -> None:
        state_store = FakePityStateStore(commit_unavailable=True)
        client, state_store, _, asset_client = make_client(state_store=state_store)

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10, "seed": "unknown-commit"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "state_store_unavailable")
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])
        operation = state_store.operations[(UUID(USER_ID), HEADERS["Idempotency-Key"])]
        self.assertEqual(operation.status, "processing")

    def test_lost_commit_ack_reconciles_the_committed_result(self) -> None:
        state_store = FakePityStateStore(commit_ack_lost=True)
        client, state_store, event_publisher, asset_client = make_client(
            state_store=state_store,
        )

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10, "seed": "lost-ack"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])
        self.assertEqual(len(event_publisher.events), 1)
        operation = state_store.operations[(UUID(USER_ID), HEADERS["Idempotency-Key"])]
        self.assertEqual(operation.status, "succeeded")

    def test_expired_processing_pull_resumes_without_a_second_charge(self) -> None:
        state_store = FakePityStateStore(commit_unavailable=True)
        client, state_store, event_publisher, asset_client = make_client(
            state_store=state_store,
        )

        first = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10, "seed": "resume"},
            headers=HEADERS,
        )
        state_store.commit_unavailable = False
        state_store.expire_processing_lease(
            user_id=UUID(USER_ID),
            idempotency_key=HEADERS["Idempotency-Key"],
        )
        second = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 10, "seed": "resume"},
            headers=HEADERS,
        )

        self.assertEqual(first.status_code, 503)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])
        self.assertEqual(len(event_publisher.events), 1)

    def test_expired_processing_pull_recovers_without_another_client_request(self) -> None:
        state_store = FakePityStateStore(commit_unavailable=True)
        client, state_store, event_publisher, asset_client = make_client(
            state_store=state_store,
        )

        first = client.post(
            "/v1/me/pulls",
            json={
                "banner_id": "limited-character-001",
                "count": 10,
                "seed": "unattended-recovery",
            },
            headers=HEADERS,
        )
        state_store.commit_unavailable = False
        state_store.expire_processing_lease(
            user_id=UUID(USER_ID),
            idempotency_key=HEADERS["Idempotency-Key"],
        )

        recovered_count = asyncio.run(
            recover_expired_processing_pulls_once(
                AppServices(state_store, event_publisher, object(), asset_client),
                limit=10,
                processing_lease_seconds=30,
            )
        )

        self.assertEqual(first.status_code, 503)
        self.assertEqual(recovered_count, 1)
        self.assertEqual(state_store.snapshot.version, 1)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])
        self.assertEqual(len(event_publisher.events), 1)
        operation = state_store.operations[(UUID(USER_ID), HEADERS["Idempotency-Key"])]
        self.assertEqual(operation.status, "succeeded")
        self.assertIsNotNone(operation.response)
        self.assertIsNotNone(operation.recovery_context)
        self.assertIsNotNone(operation.recovery_context.accepted_at)

    def test_transient_asset_failure_keeps_pull_available_for_safe_resume(self) -> None:
        state_store = FakePityStateStore()
        asset_client = FakeAssetClient(
            spend_error=AssetServiceError(503, "asset_unavailable", "asset service timed out")
        )
        client, state_store, event_publisher, asset_client = make_client(
            state_store=state_store,
            asset_client=asset_client,
        )

        first = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "asset-resume"},
            headers=HEADERS,
        )
        asset_client.spend_error = None
        state_store.expire_processing_lease(
            user_id=UUID(USER_ID),
            idempotency_key=HEADERS["Idempotency-Key"],
        )
        second = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "asset-resume"},
            headers=HEADERS,
        )

        self.assertEqual(first.status_code, 503)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])
        self.assertEqual(len(event_publisher.events), 1)

    def test_stale_processing_owner_cannot_refund_or_commit(self) -> None:
        state_store = FakePityStateStore(ownership_lost_on_commit=True)
        client, state_store, event_publisher, asset_client = make_client(
            state_store=state_store,
        )

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "stale-owner"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "pull_in_progress")
        self.assertEqual(state_store.snapshot.version, 0)
        self.assertEqual(len(asset_client.spends), 1)
        self.assertEqual(asset_client.credits, [])
        self.assertEqual(event_publisher.events, [])

    def test_pull_fails_closed_when_state_database_is_unavailable(self) -> None:
        client, _, _, asset_client = make_client(state_store=FakePityStateStore(unavailable=True))

        response = client.post(
            "/v1/me/pulls",
            json={"banner_id": "limited-character-001", "count": 1, "seed": "redis-fail"},
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "state_store_unavailable")
        self.assertEqual(len(asset_client.spends), 0)
        self.assertEqual(len(asset_client.credits), 0)

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

    def test_pending_event_recovery_publishes_and_marks_operation_succeeded(self) -> None:
        event_publisher = FakeEventPublisher(publish_error=True)
        client, state_store, event_publisher, asset_client = make_client(
            event_publisher=event_publisher,
        )

        response = client.post(
            "/v1/me/pulls",
            json={
                "banner_id": "limited-character-001",
                "count": 1,
                "seed": "backend-recovery",
            },
            headers=HEADERS,
        )
        event_publisher.publish_error = False

        recovered_count = asyncio.run(
            recover_pending_pull_events_once(
                AppServices(state_store, event_publisher, object(), asset_client),
                limit=10,
                lock_ttl_seconds=30,
            )
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(recovered_count, 1)
        self.assertEqual(len(event_publisher.events), 1)
        operation = state_store.operations[(UUID(USER_ID), HEADERS["Idempotency-Key"])]
        self.assertEqual(operation.status, "succeeded")
        self.assertIsNotNone(operation.response)
        self.assertIsNotNone(operation.event)

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

    def test_get_pity_resolves_the_banner_to_its_pity_group(self) -> None:
        catalog = grouped_catalog_snapshot()
        client, state_store, _, _ = make_client(
            catalog_repository=FixedCatalogRepository(catalog),
        )

        response = client.get(
            "/v1/me/pity?banner_id=limited-character-002",
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(state_store.snapshot_reads, ["limited-character-shared"])


if __name__ == "__main__":
    unittest.main()
