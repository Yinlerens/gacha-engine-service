from __future__ import annotations

from uuid import UUID

from gacha_engine_service.engine import create_initial_pity
from gacha_engine_service.kafka_events import EventPublishError
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

