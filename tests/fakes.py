from __future__ import annotations

import asyncio
from collections.abc import Callable
import uuid
from uuid import UUID

from gacha_engine_service.asset_client import AssetServiceError
from gacha_engine_service.engine import create_initial_pity
from gacha_engine_service.kafka_events import EventPublishError
from gacha_engine_service.pull_operations import (
    PullOperation,
    PullOperationClaim,
    PullOperationRecord,
    PullOperationRecoveryClaim,
    PullRecoveryContext,
)
from gacha_engine_service.state_store import (
    GachaStateStoreError,
    PityVersionConflict,
    PullOperationOwnershipLost,
)
from gacha_engine_service.schemas import PitySnapshot, PityState, PullCompletedEvent


class FakePityStateStore:
    def __init__(
        self,
        *,
        snapshot: PitySnapshot | None = None,
        ping_error: bool = False,
        conflict: bool = False,
        unavailable: bool = False,
        commit_unavailable: bool = False,
        commit_ack_lost: bool = False,
        ownership_lost_on_commit: bool = False,
        synchronized_snapshot_readers: int = 0,
    ) -> None:
        self.initial_snapshot = snapshot or PitySnapshot(
            **create_initial_pity().model_dump(),
            version=0,
        )
        self.snapshot = self.initial_snapshot
        self.snapshots: dict[str, PitySnapshot] = {}
        self.snapshot_reads: list[str] = []
        self.snapshot_writes: list[str] = []
        self.ping_error = ping_error
        self.conflict = conflict
        self.unavailable = unavailable
        self.commit_unavailable = commit_unavailable
        self.commit_ack_lost = commit_ack_lost
        self.ownership_lost_on_commit = ownership_lost_on_commit
        self.snapshot_read_barrier = (
            asyncio.Barrier(synchronized_snapshot_readers)
            if synchronized_snapshot_readers >= 2
            else None
        )
        self.synchronized_snapshot_reads_remaining = synchronized_snapshot_readers
        self.closed = False
        self.operations: dict[tuple[UUID, str], PullOperation] = {}
        self.operation_ids: dict[tuple[UUID, str], str] = {}
        self.operation_keys: dict[str, tuple[UUID, str]] = {}
        self.processing_tokens: dict[tuple[UUID, str], UUID] = {}
        self.claimable_operations: set[tuple[UUID, str]] = set()
        self.recovery_locks: set[str] = set()

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error:
            raise GachaStateStoreError("state database is unavailable")

    async def get_snapshot(self, user_id: UUID, pity_group_id: str) -> PitySnapshot:
        if self.unavailable:
            raise GachaStateStoreError("state database is unavailable")
        self.snapshot_reads.append(pity_group_id)
        snapshot = self.snapshots.get(pity_group_id, self.initial_snapshot)
        if (
            self.snapshot_read_barrier is not None
            and self.synchronized_snapshot_reads_remaining > 0
        ):
            self.synchronized_snapshot_reads_remaining -= 1
            await self.snapshot_read_barrier.wait()
        return snapshot

    async def compare_and_set(
        self,
        *,
        user_id: UUID,
        pity_group_id: str,
        expected_version: int,
        next_pity: PityState,
    ) -> PitySnapshot:
        if self.unavailable:
            raise GachaStateStoreError("state database is unavailable")
        current_snapshot = self.snapshots.get(pity_group_id, self.initial_snapshot)
        if self.conflict or current_snapshot.version != expected_version:
            raise PityVersionConflict(current_version=current_snapshot.version)

        self.snapshot = PitySnapshot(**next_pity.model_dump(), version=expected_version + 1)
        self.snapshots[pity_group_id] = self.snapshot
        self.snapshot_writes.append(pity_group_id)
        return self.snapshot

    async def begin_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        request_hash: str,
        processing_lease_seconds: int,
        recovery_context: PullRecoveryContext,
    ) -> PullOperationClaim:
        if self.unavailable:
            raise GachaStateStoreError("state database is unavailable")
        key = (user_id, idempotency_key)
        if key in self.operations:
            operation = self.operations[key]
            if (
                operation.status == "processing"
                and operation.request_hash == request_hash
                and key in self.claimable_operations
            ):
                token = uuid.uuid4()
                self.processing_tokens[key] = token
                self.claimable_operations.discard(key)
                return PullOperationClaim(
                    operation_key=self.operation_ids[key],
                    operation=operation,
                    processing_token=token,
                )
            return PullOperationClaim(
                operation_key=self.operation_ids[key],
                operation=operation,
            )

        operation = PullOperation(
            status="processing",
            request_hash=request_hash,
            recovery_context=recovery_context,
        )
        operation_key = str(uuid.uuid4())
        token = uuid.uuid4()
        self.operations[key] = operation
        self.operation_ids[key] = operation_key
        self.operation_keys[operation_key] = key
        self.processing_tokens[key] = token
        return PullOperationClaim(
            operation_key=operation_key,
            operation=operation,
            processing_token=token,
        )

    def expire_processing_lease(self, *, user_id: UUID, idempotency_key: str) -> None:
        self.claimable_operations.add((user_id, idempotency_key))

    async def get_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
    ) -> PullOperation | None:
        if self.unavailable:
            raise GachaStateStoreError("state database is unavailable")
        return self.operations.get((user_id, idempotency_key))

    async def get_pull_operation_by_key(self, *, operation_key: str) -> PullOperation | None:
        key = self.operation_keys.get(operation_key)
        return self.operations.get(key) if key is not None else None

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
        key = self.operation_keys[operation_key]
        if key[0] != user_id:
            raise PullOperationOwnershipLost()
        if self.ownership_lost_on_commit or self.processing_tokens.get(key) != processing_token:
            raise PullOperationOwnershipLost()
        if self.commit_unavailable:
            raise GachaStateStoreError("state database is unavailable during commit")
        next_snapshot = await self.compare_and_set(
            user_id=user_id,
            pity_group_id=pity_group_id,
            expected_version=expected_version,
            next_pity=next_pity,
        )
        self.operations[key] = operation
        self.processing_tokens.pop(key, None)
        if self.commit_ack_lost:
            raise GachaStateStoreError("state database commit acknowledgement was lost")
        return next_snapshot

    async def transition_pull_operation_from_processing(
        self,
        *,
        operation_key: str,
        user_id: UUID,
        request_hash: str,
        processing_token: UUID,
        operation: PullOperation,
    ) -> None:
        key = self.operation_keys[operation_key]
        if key[0] != user_id:
            raise PullOperationOwnershipLost()
        current = self.operations.get(key)
        if (
            current is None
            or current.status != "processing"
            or current.request_hash != request_hash
            or self.processing_tokens.get(key) != processing_token
        ):
            raise PullOperationOwnershipLost()
        self.operations[key] = operation
        self.processing_tokens.pop(key, None)

    async def save_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        operation: PullOperation,
    ) -> None:
        self.operations[(user_id, idempotency_key)] = operation

    async def iter_event_pending_pull_operations(self, *, limit: int) -> list[PullOperationRecord]:
        records: list[PullOperationRecord] = []
        for (user_id, idempotency_key), operation in self.operations.items():
            if operation.status != "event_pending":
                continue
            records.append(
                PullOperationRecord(
                    operation_key=self.operation_ids[(user_id, idempotency_key)],
                    user_id=user_id,
                    operation=operation,
                )
            )
            if len(records) >= limit:
                break
        return records

    async def iter_refund_pending_pull_operations(self, *, limit: int) -> list[PullOperationRecord]:
        records: list[PullOperationRecord] = []
        for (user_id, idempotency_key), operation in self.operations.items():
            if operation.status != "refund_pending" or operation.recovery_context is None:
                continue
            records.append(
                PullOperationRecord(
                    operation_key=self.operation_ids[(user_id, idempotency_key)],
                    user_id=user_id,
                    operation=operation,
                )
            )
            if len(records) >= limit:
                break
        return records

    async def iter_expired_processing_pull_operations(
        self,
        *,
        limit: int,
    ) -> list[PullOperationRecord]:
        records: list[PullOperationRecord] = []
        for key in self.claimable_operations:
            operation = self.operations[key]
            if operation.status != "processing" or operation.recovery_context is None:
                continue
            records.append(
                PullOperationRecord(
                    operation_key=self.operation_ids[key],
                    user_id=key[0],
                    operation=operation,
                )
            )
            if len(records) >= limit:
                break
        return records

    async def claim_expired_processing_pull_operation(
        self,
        *,
        operation_key: str,
        processing_lease_seconds: int,
    ) -> PullOperationRecoveryClaim | None:
        key = self.operation_keys.get(operation_key)
        if key is None or key not in self.claimable_operations:
            return None
        operation = self.operations[key]
        if operation.status != "processing" or operation.recovery_context is None:
            return None
        token = uuid.uuid4()
        self.processing_tokens[key] = token
        self.claimable_operations.discard(key)
        return PullOperationRecoveryClaim(
            operation_key=operation_key,
            user_id=key[0],
            operation=operation,
            processing_token=token,
        )

    async def claim_pull_operation_recovery(
        self,
        *,
        operation_key: str,
        expected_status: str,
        lock_ttl_seconds: int,
    ) -> bool:
        key = self.operation_keys.get(operation_key)
        if key is None or operation_key in self.recovery_locks:
            return False
        if self.operations[key].status != expected_status:
            return False
        self.recovery_locks.add(operation_key)
        return True

    async def release_pull_operation_recovery(self, *, operation_key: str) -> None:
        self.recovery_locks.discard(operation_key)

    async def save_pull_operation_by_key(
        self,
        *,
        operation_key: str,
        operation: PullOperation,
    ) -> None:
        self.operations[self.operation_keys[operation_key]] = operation


class FakeEventPublisher:
    def __init__(self, *, ping_error: bool = False, publish_error: bool = False) -> None:
        self.ping_error = ping_error
        self.publish_error = publish_error
        self.events: list[PullCompletedEvent] = []
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error:
            raise EventPublishError("kafka is unavailable")

    async def publish_pull_completed(self, event: PullCompletedEvent) -> None:
        if self.publish_error:
            raise EventPublishError("failed to publish pull event")
        self.events.append(event)


class FakeAssetClient:
    def __init__(
        self,
        *,
        ping_error: bool = False,
        spend_error: AssetServiceError | None = None,
        credit_error: AssetServiceError | None = None,
        before_credit: Callable[[], None] | None = None,
    ) -> None:
        self.ping_error = ping_error
        self.spend_error = spend_error
        self.credit_error = credit_error
        self.before_credit = before_credit
        self.spends: list[dict[str, object]] = []
        self.credits: list[dict[str, object]] = []
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error:
            raise AssetServiceError(503, "asset_unavailable", "asset service is unavailable")

    async def spend(
        self,
        *,
        user_id: UUID,
        amount_minor: int,
        idempotency_key: str,
        reason: str,
        metadata: dict[str, object],
        request_id: str = "",
    ) -> None:
        if self.spend_error is not None:
            raise self.spend_error
        if any(entry["idempotency_key"] == idempotency_key for entry in self.spends):
            return
        self.spends.append(
            {
                "user_id": user_id,
                "amount_minor": amount_minor,
                "idempotency_key": idempotency_key,
                "reason": reason,
                "metadata": metadata,
                "request_id": request_id,
            }
        )

    async def credit(
        self,
        *,
        user_id: UUID,
        amount_minor: int,
        idempotency_key: str,
        reason: str,
        metadata: dict[str, object],
        request_id: str = "",
    ) -> None:
        if self.before_credit is not None:
            self.before_credit()
        if self.credit_error is not None:
            raise self.credit_error
        if any(entry["idempotency_key"] == idempotency_key for entry in self.credits):
            return
        self.credits.append(
            {
                "user_id": user_id,
                "amount_minor": amount_minor,
                "idempotency_key": idempotency_key,
                "reason": reason,
                "metadata": metadata,
                "request_id": request_id,
            }
        )
