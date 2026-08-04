"""Postgres-backed authoritative pull operation and pity state."""

from __future__ import annotations

import hashlib
import json
from typing import Any
import uuid
from uuid import UUID

from .engine import create_initial_pity
from .pull_operations import (
    PullOperation,
    PullOperationClaim,
    PullOperationRecord,
    PullOperationRecoveryClaim,
    PullRecoveryContext,
)
from .schemas import PitySnapshot, PityState
from .state_store import (
    GachaStateStoreError,
    PityVersionConflict,
    PullOperationOwnershipLost,
)


class PostgresGachaStateStore:
    """Persist idempotency results and pity snapshots without expiry."""

    def __init__(
        self,
        *,
        database_url: str,
        pool_size: int,
        query_timeout_seconds: int,
    ) -> None:
        self._database_url = database_url.strip()
        self._pool_size = max(1, pool_size)
        self._query_timeout_seconds = max(1, query_timeout_seconds)
        self._pool: Any | None = None

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> None:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                await connection.execute(
                    "select 1 from gacha_runtime.pull_operations limit 0"
                )
                await connection.execute(
                    "select 1 from gacha_runtime.pity_snapshots limit 0"
                )
        except GachaStateStoreError:
            raise
        except Exception as exc:
            raise GachaStateStoreError("gacha state database is unavailable") from exc

    async def get_snapshot(self, user_id: UUID, pity_group_id: str) -> PitySnapshot:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    SELECT_PITY_SQL,
                    user_id,
                    pity_group_id,
                )
        except Exception as exc:
            raise GachaStateStoreError("read pity state from postgres failed") from exc

        if row is None:
            return PitySnapshot(**create_initial_pity().model_dump(), version=0)
        return _snapshot_from_row(row)

    async def compare_and_set(
        self,
        *,
        user_id: UUID,
        pity_group_id: str,
        expected_version: int,
        next_pity: PityState,
    ) -> PitySnapshot:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await _ensure_initial_pity(connection, user_id, pity_group_id)
                    row = await _update_pity(
                        connection,
                        user_id=user_id,
                        pity_group_id=pity_group_id,
                        expected_version=expected_version,
                        next_pity=next_pity,
                    )
                    if row is None:
                        current_version = await _current_pity_version(
                            connection,
                            user_id,
                            pity_group_id,
                        )
                        raise PityVersionConflict(current_version=current_version)
                    return _snapshot_from_row(row)
        except (GachaStateStoreError, PityVersionConflict):
            raise
        except Exception as exc:
            raise GachaStateStoreError("commit pity state to postgres failed") from exc

    async def begin_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        request_hash: str,
        processing_lease_seconds: int,
        recovery_context: PullRecoveryContext,
    ) -> PullOperationClaim:
        pool = await self._ensure_pool()
        key_hash = _idempotency_key_hash(idempotency_key)
        processing_token = uuid.uuid4()
        lease_seconds = max(1, processing_lease_seconds)
        try:
            async with pool.acquire() as connection:
                created = await connection.fetchrow(
                    INSERT_PULL_OPERATION_SQL,
                    uuid.uuid4(),
                    user_id,
                    key_hash,
                    request_hash,
                    processing_token,
                    lease_seconds,
                    _model_json(recovery_context),
                    recovery_context.request_id or None,
                )
                if created is not None:
                    return PullOperationClaim(
                        operation_key=str(created["id"]),
                        operation=_operation_from_row(created),
                        processing_token=processing_token,
                    )

                claimed = await connection.fetchrow(
                    CLAIM_PROCESSING_OPERATION_SQL,
                    user_id,
                    key_hash,
                    request_hash,
                    processing_token,
                    lease_seconds,
                    _model_json(recovery_context),
                )
                if claimed is not None:
                    return PullOperationClaim(
                        operation_key=str(claimed["id"]),
                        operation=_operation_from_row(claimed),
                        processing_token=processing_token,
                    )

                existing = await _get_pull_operation(connection, user_id, key_hash)
                if existing is None:
                    raise GachaStateStoreError("pull operation disappeared during claim")
                operation_key, operation = existing
                return PullOperationClaim(
                    operation_key=operation_key,
                    operation=operation,
                )
        except GachaStateStoreError:
            raise
        except Exception as exc:
            raise GachaStateStoreError("begin pull operation in postgres failed") from exc

    async def get_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
    ) -> PullOperation | None:
        pool = await self._ensure_pool()
        key_hash = _idempotency_key_hash(idempotency_key)
        try:
            async with pool.acquire() as connection:
                existing = await _get_pull_operation(connection, user_id, key_hash)
                return existing[1] if existing is not None else None
        except Exception as exc:
            raise GachaStateStoreError("read pull operation from postgres failed") from exc

    async def list_pull_operations(
        self,
        *,
        user_id: UUID,
        limit: int,
    ) -> list[PullOperationRecord]:
        pool = await self._ensure_pool()
        normalized_limit = max(1, min(100, limit))
        try:
            async with pool.acquire() as connection:
                rows = await connection.fetch(
                    SELECT_PULL_OPERATIONS_BY_USER_SQL,
                    user_id,
                    normalized_limit,
                )
        except Exception as exc:
            raise GachaStateStoreError("list pull operations from postgres failed") from exc

        return [_operation_record_from_row(row) for row in rows]

    async def get_pull_operation_record(
        self,
        *,
        user_id: UUID,
        operation_id: UUID,
    ) -> PullOperationRecord | None:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    SELECT_PULL_OPERATION_RECORD_SQL,
                    user_id,
                    operation_id,
                )
        except Exception as exc:
            raise GachaStateStoreError("read pull operation replay from postgres failed") from exc

        return _operation_record_from_row(row) if row is not None else None

    async def get_pull_operation_by_key(self, *, operation_key: str) -> PullOperation | None:
        operation_id = _operation_id(operation_key)
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    SELECT_PULL_OPERATION_BY_ID_SQL,
                    operation_id,
                )
                return _operation_from_row(row) if row is not None else None
        except Exception as exc:
            raise GachaStateStoreError("read pull operation by key failed") from exc

    async def get_pull_operation_by_event_id(
        self,
        *,
        user_id: UUID,
        event_id: UUID,
    ) -> PullOperation | None:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    SELECT_PULL_OPERATION_BY_EVENT_ID_SQL,
                    user_id,
                    str(event_id),
                )
                return _operation_from_row(row) if row is not None else None
        except Exception as exc:
            raise GachaStateStoreError("read pull operation by event id failed") from exc

    async def compare_and_set_with_pull_operation(
        self,
        *,
        operation_key: str,
        user_id: UUID,
        pity_group_id: str,
        request_hash: str,
        expected_version: int,
        next_pity: PityState,
        operation: PullOperation,
        processing_token: UUID,
    ) -> PitySnapshot:
        pool = await self._ensure_pool()
        operation_id = _operation_id(operation_key)
        try:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    current_operation = await connection.fetchrow(
                        SELECT_PULL_OPERATION_BY_ID_FOR_UPDATE_SQL,
                        operation_id,
                        user_id,
                    )
                    if current_operation is None:
                        raise GachaStateStoreError("pull operation was not found during commit")
                    if str(current_operation["request_hash"]) != request_hash:
                        raise GachaStateStoreError("pull operation request hash changed during commit")
                    if str(current_operation["status"]) != "processing":
                        raise PullOperationOwnershipLost()
                    if current_operation["processing_token"] != processing_token:
                        raise PullOperationOwnershipLost()

                    await _ensure_initial_pity(connection, user_id, pity_group_id)
                    row = await _update_pity(
                        connection,
                        user_id=user_id,
                        pity_group_id=pity_group_id,
                        expected_version=expected_version,
                        next_pity=next_pity,
                    )
                    if row is None:
                        current_version = await _current_pity_version(
                            connection,
                            user_id,
                            pity_group_id,
                        )
                        raise PityVersionConflict(current_version=current_version)

                    update_result = await connection.execute(
                        UPDATE_PULL_OPERATION_SQL,
                        current_operation["id"],
                        operation.status,
                        _model_json(operation.response),
                        _model_json(operation.event),
                        operation.error_code,
                        operation.error_message,
                        _model_json(operation.recovery_context),
                    )
                    if update_result != "UPDATE 1":
                        raise GachaStateStoreError("pull operation could not be committed")
                    return _snapshot_from_row(row)
        except (GachaStateStoreError, PityVersionConflict, PullOperationOwnershipLost):
            raise
        except Exception as exc:
            raise GachaStateStoreError("commit pull operation to postgres failed") from exc

    async def transition_pull_operation_from_processing(
        self,
        *,
        operation_key: str,
        user_id: UUID,
        request_hash: str,
        processing_token: UUID,
        operation: PullOperation,
    ) -> None:
        pool = await self._ensure_pool()
        operation_id = _operation_id(operation_key)
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    TRANSITION_PROCESSING_OPERATION_SQL,
                    operation_id,
                    user_id,
                    request_hash,
                    processing_token,
                    operation.status,
                    _model_json(operation.response),
                    _model_json(operation.event),
                    operation.error_code,
                    operation.error_message,
                    _model_json(operation.recovery_context),
                )
                if row is None:
                    raise PullOperationOwnershipLost()
        except PullOperationOwnershipLost:
            raise
        except Exception as exc:
            raise GachaStateStoreError("transition pull operation in postgres failed") from exc

    async def save_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        operation: PullOperation,
    ) -> None:
        pool = await self._ensure_pool()
        key_hash = _idempotency_key_hash(idempotency_key)
        try:
            async with pool.acquire() as connection:
                result = await connection.execute(
                    SAVE_PULL_OPERATION_SQL,
                    user_id,
                    key_hash,
                    operation.request_hash,
                    operation.status,
                    _model_json(operation.response),
                    _model_json(operation.event),
                    operation.error_code,
                    operation.error_message,
                    _model_json(operation.recovery_context),
                )
                if result != "UPDATE 1":
                    raise GachaStateStoreError("pull operation was not found during save")
        except GachaStateStoreError:
            raise
        except Exception as exc:
            raise GachaStateStoreError("save pull operation to postgres failed") from exc

    async def iter_event_pending_pull_operations(self, *, limit: int) -> list[PullOperationRecord]:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                rows = await connection.fetch(SELECT_PENDING_OPERATIONS_SQL, max(1, limit))
        except Exception as exc:
            raise GachaStateStoreError("list pending pull operations from postgres failed") from exc

        return [
            PullOperationRecord(
                operation_key=str(row["id"]),
                user_id=row["user_id"],
                operation=_operation_from_row(row),
            )
            for row in rows
        ]

    async def iter_refund_pending_pull_operations(self, *, limit: int) -> list[PullOperationRecord]:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                rows = await connection.fetch(
                    SELECT_REFUND_PENDING_OPERATIONS_SQL,
                    max(1, limit),
                )
        except Exception as exc:
            raise GachaStateStoreError("list pending pull refunds from postgres failed") from exc

        return [
            PullOperationRecord(
                operation_key=str(row["id"]),
                user_id=row["user_id"],
                operation=_operation_from_row(row),
            )
            for row in rows
        ]

    async def iter_expired_processing_pull_operations(
        self,
        *,
        limit: int,
    ) -> list[PullOperationRecord]:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                rows = await connection.fetch(
                    SELECT_EXPIRED_PROCESSING_OPERATIONS_SQL,
                    max(1, limit),
                )
        except Exception as exc:
            raise GachaStateStoreError("list expired processing pulls failed") from exc

        return [
            PullOperationRecord(
                operation_key=str(row["id"]),
                user_id=row["user_id"],
                operation=_operation_from_row(row),
            )
            for row in rows
        ]

    async def claim_expired_processing_pull_operation(
        self,
        *,
        operation_key: str,
        processing_lease_seconds: int,
    ) -> PullOperationRecoveryClaim | None:
        operation_id = _operation_id(operation_key)
        processing_token = uuid.uuid4()
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    CLAIM_EXPIRED_PROCESSING_OPERATION_SQL,
                    operation_id,
                    processing_token,
                    max(1, processing_lease_seconds),
                )
        except Exception as exc:
            raise GachaStateStoreError("claim expired processing pull failed") from exc

        if row is None:
            return None
        return PullOperationRecoveryClaim(
            operation_key=str(row["id"]),
            user_id=row["user_id"],
            operation=_operation_from_row(row),
            processing_token=processing_token,
        )

    async def claim_pull_operation_recovery(
        self,
        *,
        operation_key: str,
        expected_status: str,
        lock_ttl_seconds: int,
    ) -> bool:
        operation_id = _operation_id(operation_key)
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                row = await connection.fetchrow(
                    CLAIM_PENDING_OPERATION_SQL,
                    operation_id,
                    max(1, lock_ttl_seconds),
                    expected_status,
                )
                return row is not None
        except Exception as exc:
            raise GachaStateStoreError("claim pending pull operation failed") from exc

    async def release_pull_operation_recovery(self, *, operation_key: str) -> None:
        operation_id = _operation_id(operation_key)
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                await connection.execute(RELEASE_PENDING_OPERATION_SQL, operation_id)
        except Exception as exc:
            raise GachaStateStoreError("release pending pull operation failed") from exc

    async def save_pull_operation_by_key(
        self,
        *,
        operation_key: str,
        operation: PullOperation,
    ) -> None:
        operation_id = _operation_id(operation_key)
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                result = await connection.execute(
                    UPDATE_PULL_OPERATION_SQL,
                    operation_id,
                    operation.status,
                    _model_json(operation.response),
                    _model_json(operation.event),
                    operation.error_code,
                    operation.error_message,
                    _model_json(operation.recovery_context),
                )
                if result != "UPDATE 1":
                    raise GachaStateStoreError("pending pull operation was not found")
        except GachaStateStoreError:
            raise
        except Exception as exc:
            raise GachaStateStoreError("save pending pull operation failed") from exc

    async def _ensure_pool(self) -> Any:
        if self._pool is not None:
            return self._pool
        if not self._database_url:
            raise GachaStateStoreError("GACHA_STATE_DATABASE_URL is required")

        try:
            import asyncpg

            self._pool = await asyncpg.create_pool(
                dsn=self._database_url,
                min_size=1,
                max_size=self._pool_size,
                command_timeout=self._query_timeout_seconds,
                statement_cache_size=0,
                server_settings={"application_name": "gacha-engine-state"},
            )
        except Exception as exc:
            raise GachaStateStoreError("connect to gacha state database failed") from exc
        return self._pool


async def _get_pull_operation(
    connection: Any,
    user_id: UUID,
    key_hash: str,
) -> tuple[str, PullOperation] | None:
    row = await connection.fetchrow(SELECT_PULL_OPERATION_SQL, user_id, key_hash)
    if row is None:
        return None
    return str(row["id"]), _operation_from_row(row)


async def _ensure_initial_pity(connection: Any, user_id: UUID, pity_group_id: str) -> None:
    await connection.execute(INSERT_INITIAL_PITY_SQL, user_id, pity_group_id)


async def _update_pity(
    connection: Any,
    *,
    user_id: UUID,
    pity_group_id: str,
    expected_version: int,
    next_pity: PityState,
) -> Any | None:
    return await connection.fetchrow(
        UPDATE_PITY_SQL,
        user_id,
        pity_group_id,
        expected_version,
        next_pity.since_five,
        next_pity.since_four,
        next_pity.guaranteed_featured_five,
    )


async def _current_pity_version(connection: Any, user_id: UUID, pity_group_id: str) -> int:
    row = await connection.fetchrow(SELECT_PITY_VERSION_SQL, user_id, pity_group_id)
    return int(row["version"]) if row is not None else -1


def _snapshot_from_row(row: Any) -> PitySnapshot:
    return PitySnapshot(
        since_five=int(row["since_five"]),
        since_four=int(row["since_four"]),
        guaranteed_featured_five=bool(row["guaranteed_featured_five"]),
        version=int(row["version"]),
    )


def _operation_from_row(row: Any) -> PullOperation:
    raw_recovery_context = _json_value(row["recovery_context"])
    return PullOperation(
        status=str(row["status"]),
        request_hash=str(row["request_hash"]),
        response=_json_value(row["response"]),
        event=_json_value(row["event"]),
        error_code=row["error_code"],
        error_message=row["error_message"],
        recovery_context=(
            PullRecoveryContext.model_validate(raw_recovery_context)
            if raw_recovery_context is not None
            else None
        ),
    )


def _operation_record_from_row(row: Any) -> PullOperationRecord:
    request_id = row["request_id"]
    return PullOperationRecord(
        operation_key=str(row["id"]),
        user_id=UUID(str(row["user_id"])),
        operation=_operation_from_row(row),
        request_id=UUID(str(request_id)) if request_id is not None else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _model_json(value: Any | None) -> str | None:
    return value.model_dump_json() if value is not None else None


def _idempotency_key_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _operation_id(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise GachaStateStoreError("pull operation key is invalid") from exc


SELECT_PITY_SQL = """
select since_five, since_four, guaranteed_featured_five, version
from gacha_runtime.pity_snapshots
where user_id = $1 and pity_group_id = $2
"""

INSERT_INITIAL_PITY_SQL = """
insert into gacha_runtime.pity_snapshots (
  user_id, banner_id, pity_group_id,
  since_five, since_four, guaranteed_featured_five, version
)
values ($1, $2, $2, 0, 0, false, 0)
on conflict (user_id, pity_group_id) do nothing
"""

UPDATE_PITY_SQL = """
update gacha_runtime.pity_snapshots
set since_five = $4,
    since_four = $5,
    guaranteed_featured_five = $6,
    version = version + 1,
    updated_at = now()
where user_id = $1 and pity_group_id = $2 and version = $3
returning since_five, since_four, guaranteed_featured_five, version
"""

SELECT_PITY_VERSION_SQL = """
select version
from gacha_runtime.pity_snapshots
where user_id = $1 and pity_group_id = $2
"""

INSERT_PULL_OPERATION_SQL = """
insert into gacha_runtime.pull_operations (
  id, user_id, idempotency_key_hash, request_hash, status,
  processing_token, processing_lease_until, recovery_context, request_id
)
values (
  $1, $2, $3, $4, 'processing', $5,
  now() + make_interval(secs => $6), $7::jsonb, $8
)
on conflict (user_id, idempotency_key_hash) do nothing
returning id, user_id, status, request_hash, response, event, error_code,
          error_message, processing_token, recovery_context
"""

CLAIM_PROCESSING_OPERATION_SQL = """
update gacha_runtime.pull_operations
set processing_token = $4,
    processing_lease_until = now() + make_interval(secs => $5),
    recovery_context = coalesce(recovery_context, $6::jsonb),
    updated_at = now()
where user_id = $1
  and idempotency_key_hash = $2
  and request_hash = $3
  and status = 'processing'
  and (processing_lease_until is null or processing_lease_until < now())
returning id, user_id, status, request_hash, response, event, error_code,
          error_message, processing_token, recovery_context
"""

SELECT_PULL_OPERATION_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where user_id = $1 and idempotency_key_hash = $2
"""

SELECT_PULL_OPERATIONS_BY_USER_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context, request_id,
       created_at, updated_at
from gacha_runtime.pull_operations
where user_id = $1
order by created_at desc, id desc
limit $2
"""

SELECT_PULL_OPERATION_RECORD_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context, request_id,
       created_at, updated_at
from gacha_runtime.pull_operations
where user_id = $1 and id = $2
"""

SELECT_PULL_OPERATION_BY_ID_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where id = $1
"""

SELECT_PULL_OPERATION_BY_EVENT_ID_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where user_id = $1
  and response ->> 'event_id' = $2
limit 1
"""

SELECT_PULL_OPERATION_BY_ID_FOR_UPDATE_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where id = $1 and user_id = $2
for update
"""

UPDATE_PULL_OPERATION_SQL = """
update gacha_runtime.pull_operations
set status = $2,
    response = $3::jsonb,
    event = $4::jsonb,
    error_code = $5,
    error_message = $6,
    recovery_context = $7::jsonb,
    recovery_locked_until = null,
    processing_token = null,
    processing_lease_until = null,
    updated_at = now()
where id = $1
"""

TRANSITION_PROCESSING_OPERATION_SQL = """
update gacha_runtime.pull_operations
set status = $5,
    response = $6::jsonb,
    event = $7::jsonb,
    error_code = $8,
    error_message = $9,
    recovery_context = $10::jsonb,
    processing_token = null,
    processing_lease_until = null,
    updated_at = now()
where id = $1
  and user_id = $2
  and request_hash = $3
  and status = 'processing'
  and processing_token = $4
returning id
"""

SAVE_PULL_OPERATION_SQL = """
update gacha_runtime.pull_operations
set status = $4,
    response = $5::jsonb,
    event = $6::jsonb,
    error_code = $7,
    error_message = $8,
    recovery_context = $9::jsonb,
    recovery_locked_until = null,
    processing_token = null,
    processing_lease_until = null,
    updated_at = now()
where user_id = $1
  and idempotency_key_hash = $2
  and request_hash = $3
"""

SELECT_PENDING_OPERATIONS_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where status in ('event_pending', 'event_published')
order by updated_at, id
limit $1
"""

SELECT_REFUND_PENDING_OPERATIONS_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where status = 'refund_pending'
  and recovery_context is not null
order by updated_at, id
limit $1
"""

SELECT_EXPIRED_PROCESSING_OPERATIONS_SQL = """
select id, user_id, status, request_hash, response, event, error_code,
       error_message, processing_token, recovery_context
from gacha_runtime.pull_operations
where status = 'processing'
  and recovery_context is not null
  and (processing_lease_until is null or processing_lease_until < now())
order by processing_lease_until nulls first, id
limit $1
"""

CLAIM_EXPIRED_PROCESSING_OPERATION_SQL = """
update gacha_runtime.pull_operations
set processing_token = $2,
    processing_lease_until = now() + make_interval(secs => $3),
    updated_at = now()
where id = $1
  and status = 'processing'
  and recovery_context is not null
  and (processing_lease_until is null or processing_lease_until < now())
returning id, user_id, status, request_hash, response, event, error_code,
          error_message, processing_token, recovery_context
"""

CLAIM_PENDING_OPERATION_SQL = """
update gacha_runtime.pull_operations
set recovery_locked_until = now() + make_interval(secs => $2),
    updated_at = now()
where id = $1
  and status = $3
  and status in ('event_pending', 'event_published', 'refund_pending')
  and (recovery_locked_until is null or recovery_locked_until < now())
returning id
"""

RELEASE_PENDING_OPERATION_SQL = """
update gacha_runtime.pull_operations
set recovery_locked_until = null,
    updated_at = now()
where id = $1
  and status in ('event_pending', 'event_published', 'refund_pending')
"""
