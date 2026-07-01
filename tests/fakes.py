from __future__ import annotations

from uuid import UUID

from gacha_engine_service.asset_client import AssetServiceError
from gacha_engine_service.engine import create_initial_pity
from gacha_engine_service.kafka_events import EventPublishError
from gacha_engine_service.pull_operations import PullOperation, PullOperationRecord
from gacha_engine_service.redis_state import (
    PityStateStoreError,
    PityVersionConflict,
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
    ) -> None:
        self.snapshot = snapshot or PitySnapshot(**create_initial_pity().model_dump(), version=0)
        self.ping_error = ping_error
        self.conflict = conflict
        self.unavailable = unavailable
        self.closed = False
        self.operations: dict[tuple[UUID, str], PullOperation] = {}

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error:
            raise PityStateStoreError("redis is unavailable")

    async def get_snapshot(self, user_id: UUID, banner_id: str) -> PitySnapshot:
        if self.unavailable:
            raise PityStateStoreError("redis is unavailable")
        return self.snapshot

    async def compare_and_set(
        self,
        *,
        user_id: UUID,
        banner_id: str,
        expected_version: int,
        next_pity: PityState,
    ) -> PitySnapshot:
        if self.unavailable:
            raise PityStateStoreError("redis is unavailable")
        if self.conflict or self.snapshot.version != expected_version:
            raise PityVersionConflict(current_version=self.snapshot.version)

        self.snapshot = PitySnapshot(**next_pity.model_dump(), version=expected_version + 1)
        return self.snapshot

    async def begin_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        request_hash: str,
    ) -> PullOperation | None:
        key = (user_id, idempotency_key)
        if key in self.operations:
            return self.operations[key]

        self.operations[key] = PullOperation(status="processing", request_hash=request_hash)
        return None

    async def get_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
    ) -> PullOperation | None:
        return self.operations.get((user_id, idempotency_key))

    async def compare_and_set_with_pull_operation(
        self,
        *,
        user_id: UUID,
        banner_id: str,
        idempotency_key: str,
        request_hash: str,
        expected_version: int,
        next_pity: PityState,
        operation: PullOperation,
    ) -> PitySnapshot:
        next_snapshot = await self.compare_and_set(
            user_id=user_id,
            banner_id=banner_id,
            expected_version=expected_version,
            next_pity=next_pity,
        )
        self.operations[(user_id, idempotency_key)] = operation
        return next_snapshot

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
                    operation_key=f"{user_id}:{idempotency_key}",
                    operation=operation,
                )
            )
            if len(records) >= limit:
                break
        return records

    async def claim_pull_operation_recovery(
        self,
        *,
        operation_key: str,
        lock_ttl_seconds: int,
    ) -> bool:
        return True

    async def release_pull_operation_recovery(self, *, operation_key: str) -> None:
        return None

    async def save_pull_operation_by_key(
        self,
        *,
        operation_key: str,
        operation: PullOperation,
    ) -> None:
        user_id, idempotency_key = operation_key.split(":", 1)
        self.operations[(UUID(user_id), idempotency_key)] = operation


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
    ) -> None:
        self.ping_error = ping_error
        self.spend_error = spend_error
        self.credit_error = credit_error
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
        if self.credit_error is not None:
            raise self.credit_error
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
