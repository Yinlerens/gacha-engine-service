from __future__ import annotations

import unittest
from datetime import datetime, timezone
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
REQUEST_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
CREATED_AT = datetime(2026, 8, 3, 1, 2, 3, tzinfo=timezone.utc)
UPDATED_AT = datetime(2026, 8, 3, 1, 2, 4, tzinfo=timezone.utc)


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
        fetch_results: list[list[dict[str, object]]] | None = None,
        execute_results: list[str] | None = None,
    ) -> None:
        self.fetchrow_results = list(fetchrow_results or [])
        self.fetch_results = list(fetch_results or [])
        self.execute_results = list(execute_results or [])
        self.fetchrow_calls: list[tuple[object, ...]] = []
        self.fetch_calls: list[tuple[object, ...]] = []
        self.execute_calls: list[tuple[object, ...]] = []
        self.transaction_contexts: list[FakeTransaction] = []

    async def fetchrow(self, *args: object, **_: object) -> dict[str, object] | None:
        self.fetchrow_calls.append(args)
        return self.fetchrow_results.pop(0)

    async def fetch(self, *args: object, **_: object) -> list[dict[str, object]]:
        self.fetch_calls.append(args)
        return self.fetch_results.pop(0) if self.fetch_results else []

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
        "request_id": REQUEST_ID,
        "created_at": CREATED_AT,
        "updated_at": UPDATED_AT,
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
    def test_recovery_context_round_trip_freezes_the_pity_group(self) -> None:
        context = recovery_context()

        restored = PullRecoveryContext.model_validate_json(context.model_dump_json())
        legacy_payload = context.model_dump(mode="json")
        legacy_payload.pop("pity_group_id", None)
        restored_legacy = PullRecoveryContext.model_validate(legacy_payload)

        self.assertEqual(context.pity_group_id, "limited-character-001")
        self.assertEqual(restored.pity_group_id, context.pity_group_id)
        self.assertEqual(restored_legacy.pity_group_id, context.banner.id)
        self.assertEqual(
            restored.to_banner_config().pity_group_id,
            context.pity_group_id,
        )

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

    async def test_list_pull_operations_is_scoped_to_user_and_newest_first(self) -> None:
        connection = FakeConnection(fetch_results=[[operation_row()]])
        store = make_store(connection)

        records = await store.list_pull_operations(user_id=USER_ID, limit=20)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].operation_key, str(OPERATION_ID))
        self.assertEqual(records[0].user_id, USER_ID)
        self.assertEqual(records[0].request_id, REQUEST_ID)
        self.assertEqual(records[0].created_at, CREATED_AT)
        self.assertEqual(records[0].updated_at, UPDATED_AT)
        query = str(connection.fetch_calls[0][0]).lower()
        self.assertIn("where user_id = $1", query)
        self.assertIn("order by created_at desc, id desc", query)
        self.assertEqual(connection.fetch_calls[0][1:], (USER_ID, 20))

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
        context = recovery_context()
        connection = FakeConnection(
            fetchrow_results=[None, operation_row(recovery_context=context)]
        )
        store = make_store(connection)

        claim = await store.begin_pull_operation(
            user_id=USER_ID,
            idempotency_key="pull-key",
            request_hash=REQUEST_HASH,
            processing_lease_seconds=30,
            recovery_context=context,
        )

        self.assertIsNotNone(claim.processing_token)
        self.assertEqual(claim.operation.status, "processing")
        claim_call = connection.fetchrow_calls[1]
        self.assertIn("processing_lease_until < now()", str(claim_call[0]))
        self.assertIn("coalesce(recovery_context", str(claim_call[0]).lower())
        self.assertEqual(claim_call[-1], context.model_dump_json())

    async def test_recovery_lock_claims_only_the_expected_status_once(self) -> None:
        connection = FakeConnection(
            fetchrow_results=[
                operation_row(status="refund_pending"),
                None,
            ]
        )
        store = make_store(connection)

        first = await store.claim_pull_operation_recovery(
            operation_key=str(OPERATION_ID),
            expected_status="refund_pending",
            lock_ttl_seconds=30,
        )
        second = await store.claim_pull_operation_recovery(
            operation_key=str(OPERATION_ID),
            expected_status="refund_pending",
            lock_ttl_seconds=30,
        )

        self.assertTrue(first)
        self.assertFalse(second)
        claim_call = connection.fetchrow_calls[0]
        self.assertIn("status = $3", str(claim_call[0]).lower())
        self.assertIn(
            "status in ('event_pending', 'event_published', 'refund_pending')",
            str(claim_call[0]).lower(),
        )
        self.assertEqual(claim_call[3], "refund_pending")

    async def test_delivery_recovery_scan_includes_published_events(self) -> None:
        connection = FakeConnection()
        store = make_store(connection)

        records = await store.iter_event_pending_pull_operations(limit=25)

        self.assertEqual(records, [])
        scan_call = connection.fetch_calls[0]
        self.assertIn(
            "status in ('event_pending', 'event_published')",
            str(scan_call[0]).lower(),
        )
        self.assertEqual(scan_call[1], 25)

    async def test_recovery_lock_release_does_not_touch_terminal_audit_rows(self) -> None:
        connection = FakeConnection()
        store = make_store(connection)

        await store.release_pull_operation_recovery(operation_key=str(OPERATION_ID))

        release_call = connection.execute_calls[0]
        self.assertIn(
            "status in ('event_pending', 'event_published', 'refund_pending')",
            str(release_call[0]).lower(),
        )

    async def test_begin_pull_operation_does_not_take_an_active_lease(self) -> None:
        context = recovery_context()
        connection = FakeConnection(
            fetchrow_results=[None, None, operation_row(recovery_context=context)]
        )
        store = make_store(connection)

        claim = await store.begin_pull_operation(
            user_id=USER_ID,
            idempotency_key="pull-key",
            request_hash=REQUEST_HASH,
            processing_lease_seconds=30,
            recovery_context=context,
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
            operation_key=str(OPERATION_ID),
            user_id=USER_ID,
            pity_group_id="limited-character-001",
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

    async def test_pity_queries_use_the_explicit_group_key(self) -> None:
        connection = FakeConnection(fetchrow_results=[None])
        store = make_store(connection)

        snapshot = await store.get_snapshot(USER_ID, "limited-character-shared")

        self.assertEqual(snapshot.version, 0)
        select_call = connection.fetchrow_calls[0]
        self.assertIn("pity_group_id = $2", str(select_call[0]).lower())
        self.assertEqual(select_call[2], "limited-character-shared")

    async def test_pull_audit_lookup_uses_user_and_event_id(self) -> None:
        connection = FakeConnection(fetchrow_results=[None])
        store = make_store(connection)

        operation = await store.get_pull_operation_by_event_id(
            user_id=USER_ID,
            event_id=UUID(EVENT_ID),
        )

        self.assertIsNone(operation)
        select_call = connection.fetchrow_calls[0]
        self.assertIn("response ->> 'event_id' = $2", str(select_call[0]).lower())
        self.assertEqual(select_call[1:], (USER_ID, EVENT_ID))

    async def test_stale_processing_owner_is_fenced_before_pity_commit(self) -> None:
        connection = FakeConnection(fetchrow_results=[operation_row()])
        store = make_store(connection)

        with self.assertRaises(PullOperationOwnershipLost):
            await store.compare_and_set_with_pull_operation(
                operation_key=str(OPERATION_ID),
                user_id=USER_ID,
                pity_group_id="limited-character-001",
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
        self.assertIn("status = 'refund_pending'", normalized)
        self.assertNotIn("recovery_context jsonb not null", normalized)

    def test_player_support_migrations_separate_schema_and_request_id_backfill(self) -> None:
        migrations = Path(__file__).parents[1] / "migrations"
        schema = (migrations / "000009_player_support_lookup.up.sql").read_text(
            encoding="utf-8"
        ).lower()
        backfill = (
            migrations / "000010_backfill_player_support_request_ids.up.sql"
        ).read_text(encoding="utf-8").lower()

        self.assertIn("add column if not exists request_id uuid", schema)
        self.assertIn("(user_id, created_at desc, id desc)", schema)
        self.assertIn("create index concurrently", schema)
        self.assertNotIn("update gacha_runtime.pull_operations", schema)
        self.assertIn("update gacha_runtime.pull_operations", backfill)
        self.assertIn("recovery_context ->> 'request_id'", backfill)
        self.assertNotIn("alter table", backfill)

    def test_pity_group_migrations_preserve_legacy_rows_before_enforcing_uniqueness(self) -> None:
        migrations = Path(__file__).parents[1] / "migrations"
        expand = (migrations / "000004_pity_groups_expand.up.sql").read_text(
            encoding="utf-8"
        ).lower()
        backfill = (migrations / "000005_backfill_pity_groups.up.sql").read_text(
            encoding="utf-8"
        ).lower()
        enforce = (migrations / "000006_pity_groups_enforce.up.sql").read_text(
            encoding="utf-8"
        ).lower()

        self.assertIn("add column if not exists pity_group_id text", expand)
        self.assertIn("new.pity_group_id := new.banner_id", expand)
        self.assertIn("set pity_group_id = banner_id", backfill)
        self.assertIn("where pity_group_id is null", backfill)
        self.assertIn("create unique index concurrently", enforce)
        self.assertIn("(user_id, pity_group_id)", enforce)
        self.assertIn("validate constraint pity_snapshots_pity_group_id_length", enforce)
        for version in ("000004", "000005", "000006"):
            self.assertTrue(any(migrations.glob(f"{version}_*.down.sql")))

    def test_pull_audit_migration_protects_completed_evidence(self) -> None:
        migrations = Path(__file__).parents[1] / "migrations"
        audit = (migrations / "000007_pull_audit_integrity.up.sql").read_text(
            encoding="utf-8"
        ).lower()

        self.assertIn("create unique index concurrently", audit)
        self.assertIn("response ->> 'event_id'", audit)
        self.assertIn("before update or delete", audit)
        self.assertIn("old.status = 'succeeded'", audit)
        self.assertIn("new.status not in ('event_pending', 'succeeded')", audit)
        self.assertIn("pull audit evidence is immutable", audit)
        self.assertTrue((migrations / "000007_pull_audit_integrity.down.sql").exists())

    def test_reward_delivery_migration_adds_a_confirmable_published_state(self) -> None:
        migrations = Path(__file__).parents[1] / "migrations"
        delivery = (
            migrations / "000008_reward_delivery_confirmation.up.sql"
        ).read_text(encoding="utf-8").lower()

        self.assertIn("'event_published'", delivery)
        self.assertIn("status in ('event_pending', 'event_published')", delivery)
        self.assertIn("old.status in ('event_pending', 'event_published')", delivery)
        self.assertIn("new.status not in (old.status, 'event_published', 'succeeded')", delivery)
        self.assertTrue(
            (migrations / "000008_reward_delivery_confirmation.down.sql").exists()
        )


if __name__ == "__main__":
    unittest.main()
