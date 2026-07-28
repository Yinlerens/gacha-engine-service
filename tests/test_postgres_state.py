from __future__ import annotations

import unittest
from pathlib import Path
from uuid import UUID

from gacha_engine_service.catalog_config import static_catalog_snapshot
from gacha_engine_service.postgres_state import PostgresGachaStateStore
from gacha_engine_service.pull_operations import (
    PullOperation,
    PullOperationClaim,
    PullRecoveryContext,
)
from gacha_engine_service.schemas import PityState
from gacha_engine_service.state_store import PullOperationOwnershipLost


USER_ID = UUID("ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e")
OPERATION_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_HASH = "a" * 64
PROCESSING_TOKEN = UUID("22222222-2222-4222-8222-222222222222")
EVENT_ID = "33333333-3333-4333-8333-333333333333"


class FakeTransaction:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeTransaction:
        self.entered = True
        return self

    async def __aexit__(self, *_: object) -> None:
        self.exited = True


class FakeConnection:
    def __init__(
        self,
        *,
        fetchrow_results: list[dict[str, object] | None] | None = None,
        execute_results: list[str] | None = None,
    ) -> None:
        self.fetchrow_results = list(fetchrow_results or [])
        self.execute_results = list(execute_results or [])
        self.fetchrow_calls: list[tuple[object, ...]] = []
        self.execute_calls: list[tuple[object, ...]] = []
        self.transaction_contexts: list[FakeTransaction] = []

    async def fetchrow(self, *args: object, **_: object) -> dict[str, object] | None:
        self.fetchrow_calls.append(args)
        return self.fetchrow_results.pop(0)

    async def execute(self, *args: object, **_: object) -> str:
        self.execute_calls.append(args)
        return self.execute_results.pop(0) if self.execute_results else "UPDATE 1"

    def transaction(self) -> FakeTransaction:
        context = FakeTransaction()
        self.transaction_contexts.append(context)
        return context


class FakeAcquireContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, *_: object) -> None:
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquireContext:
        return FakeAcquireContext(self.connection)


def make_store(connection: FakeConnection) -> PostgresGachaStateStore:
    store = PostgresGachaStateStore(
        database_url="postgresql://example/gacha",
        pool_size=2,
        query_timeout_seconds=3,
    )
    store._pool = FakePool(connection)
    return store


def operation_row(
    *,
    status: str = "processing",
    processing_token: UUID | None = PROCESSING_TOKEN,
    recovery_context: PullRecoveryContext | None = None,
) -> dict[str, object]:
    return {
        "id": OPERATION_ID,
        "user_id": USER_ID,
        "status": status,
        "request_hash": REQUEST_HASH,
        "response": None,
        "event": None,
        "error_code": None,
        "error_message": None,
        "processing_token": processing_token,
        "recovery_context": (
            recovery_context.model_dump(mode="json")
            if recovery_context is not None
            else None
        ),
    }


def recovery_context() -> PullRecoveryContext:
    banner_config = static_catalog_snapshot().banner_configs_by_id[
        "limited-character-001"
    ]
    return PullRecoveryContext.from_banner_config(
        banner_config=banner_config,
        count=10,
        seed="recoverable-seed",
        event_id=EVENT_ID,
        amount_minor=1600,
        request_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )


class PostgresStateStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_begin_pull_operation_creates_a_durable_fenced_lease(self) -> None:
        context = recovery_context()
        connection = FakeConnection(
            fetchrow_results=[
                operation_row(recovery_context=context),
            ]
        )
        store = make_store(connection)

        claim = await store.begin_pull_operation(
            user_id=USER_ID,
            idempotency_key="pull-key",
            request_hash=REQUEST_HASH,
            processing_lease_seconds=30,
            recovery_context=context,
        )

        self.assertEqual(
            claim,
            PullOperationClaim(
                operation_key=str(OPERATION_ID),
                operation=PullOperation(
                    status="processing",
                    request_hash=REQUEST_HASH,
                    recovery_context=context,
                ),
                processing_token=claim.processing_token,
            ),
        )
        self.assertIsNotNone(claim.processing_token)
        insert_call = connection.fetchrow_calls[0]
        self.assertIn("gacha_runtime.pull_operations", str(insert_call[0]))
        self.assertIn("processing_lease_until", str(insert_call[0]))
        self.assertIn("recovery_context", str(insert_call[0]))
        self.assertNotIn("pull-key", insert_call)

    async def test_expired_processing_operation_is_claimed_with_fencing_token(self) -> None:
        context = recovery_context()
        connection = FakeConnection(
            fetchrow_results=[operation_row(recovery_context=context)]
        )
        store = make_store(connection)

        claim = await store.claim_expired_processing_pull_operation(
            operation_key=str(OPERATION_ID),
            processing_lease_seconds=30,
        )

        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.operation_key, str(OPERATION_ID))
        self.assertEqual(claim.user_id, USER_ID)
        self.assertEqual(claim.operation.recovery_context, context)
        self.assertIsNotNone(claim.processing_token)
        claim_sql = str(connection.fetchrow_calls[0][0])
        self.assertIn("processing_lease_until < now()", claim_sql)
        self.assertIn("recovery_context is not null", claim_sql)

    async def test_begin_pull_operation_reclaims_an_expired_processing_lease(self) -> None:
        connection = FakeConnection(fetchrow_results=[None, operation_row()])
        store = make_store(connection)

        claim = await store.begin_pull_operation(
            user_id=USER_ID,
            idempotency_key="pull-key",
            request_hash=REQUEST_HASH,
            processing_lease_seconds=30,
        )

        self.assertIsNotNone(claim.processing_token)
        self.assertEqual(claim.operation.status, "processing")
        self.assertIn("processing_lease_until < now()", str(connection.fetchrow_calls[1][0]))

    async def test_begin_pull_operation_does_not_take_an_active_lease(self) -> None:
        connection = FakeConnection(fetchrow_results=[None, None, operation_row()])
        store = make_store(connection)

        claim = await store.begin_pull_operation(
            user_id=USER_ID,
            idempotency_key="pull-key",
            request_hash=REQUEST_HASH,
            processing_lease_seconds=30,
        )

        self.assertIsNone(claim.processing_token)
        self.assertEqual(claim.operation.status, "processing")

    async def test_commit_updates_pity_and_operation_in_one_transaction(self) -> None:
        connection = FakeConnection(
            fetchrow_results=[
                operation_row(),
                {
                    "since_five": 1,
                    "since_four": 1,
                    "guaranteed_featured_five": False,
                    "version": 1,
                },
            ]
        )
        store = make_store(connection)
        pending = PullOperation(status="event_pending", request_hash=REQUEST_HASH)

        snapshot = await store.compare_and_set_with_pull_operation(
            user_id=USER_ID,
            banner_id="limited-character-001",
            idempotency_key="pull-key",
            request_hash=REQUEST_HASH,
            expected_version=0,
            next_pity=PityState(since_five=1, since_four=1),
            operation=pending,
            processing_token=PROCESSING_TOKEN,
        )

        self.assertEqual(snapshot.version, 1)
        self.assertEqual(len(connection.transaction_contexts), 1)
        self.assertTrue(connection.transaction_contexts[0].entered)
        self.assertTrue(connection.transaction_contexts[0].exited)
        executed_sql = "\n".join(str(call[0]) for call in connection.execute_calls)
        self.assertIn("gacha_runtime.pity_snapshots", executed_sql)
        self.assertIn("gacha_runtime.pull_operations", executed_sql)

    async def test_stale_processing_owner_is_fenced_before_pity_commit(self) -> None:
        connection = FakeConnection(fetchrow_results=[operation_row()])
        store = make_store(connection)

        with self.assertRaises(PullOperationOwnershipLost):
            await store.compare_and_set_with_pull_operation(
                user_id=USER_ID,
                banner_id="limited-character-001",
                idempotency_key="pull-key",
                request_hash=REQUEST_HASH,
                expected_version=0,
                next_pity=PityState(since_five=1, since_four=1),
                operation=PullOperation(status="event_pending", request_hash=REQUEST_HASH),
                processing_token=UUID("33333333-3333-4333-8333-333333333333"),
            )

        self.assertEqual(connection.execute_calls, [])

    def test_migration_keeps_idempotency_tombstones_without_ttl(self) -> None:
        migration = (
            Path(__file__).parents[1]
            / "migrations"
            / "000001_gacha_runtime_state.up.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("create table if not exists gacha_runtime.pull_operations", migration.lower())
        self.assertIn("unique (user_id, idempotency_key_hash)", migration.lower())
        self.assertIn("create table if not exists gacha_runtime.pity_snapshots", migration.lower())
        self.assertNotIn("expires_at", migration.lower())

    def test_follow_up_migration_adds_processing_lease_and_fencing_token(self) -> None:
        migration = (
            Path(__file__).parents[1]
            / "migrations"
            / "000002_pull_processing_lease.up.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("processing_token uuid", migration.lower())
        self.assertIn("processing_lease_until timestamptz", migration.lower())
        self.assertIn("where status = 'processing'", migration.lower())

    def test_recovery_context_migration_is_expand_only_and_indexed(self) -> None:
        migration = (
            Path(__file__).parents[1]
            / "migrations"
            / "000003_pull_unattended_recovery.up.sql"
        ).read_text(encoding="utf-8")

        normalized = migration.lower()
        self.assertIn("add column if not exists recovery_context jsonb", normalized)
        self.assertIn("recovery_context is not null", normalized)
        self.assertIn("create index concurrently", normalized)
        self.assertNotIn("recovery_context jsonb not null", normalized)


if __name__ == "__main__":
    unittest.main()
