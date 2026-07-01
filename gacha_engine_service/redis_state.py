"""Redis-backed pity snapshot store."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .engine import create_initial_pity
from .pull_operations import PullOperation, PullOperationRecord
from .schemas import PitySnapshot, PityState


COMPARE_AND_SET_LUA = """
local current = redis.call("GET", KEYS[1])
local expected_version = tonumber(ARGV[1])

if current then
  local decoded = cjson.decode(current)
  local current_version = tonumber(decoded["version"] or 0)

  if current_version ~= expected_version then
    return {0, current_version}
  end
else
  if expected_version ~= 0 then
    return {0, -1}
  end
end

local next_snapshot = cjson.decode(ARGV[2])
local next_version = expected_version + 1
next_snapshot["version"] = next_version
redis.call("SET", KEYS[1], cjson.encode(next_snapshot))
return {1, next_version}
"""

COMPARE_AND_SET_WITH_OPERATION_LUA = """
local current = redis.call("GET", KEYS[1])
local expected_version = tonumber(ARGV[1])

if current then
  local decoded = cjson.decode(current)
  local current_version = tonumber(decoded["version"] or 0)

  if current_version ~= expected_version then
    return {0, current_version}
  end
else
  if expected_version ~= 0 then
    return {0, -1}
  end
end

local operation = redis.call("GET", KEYS[2])
if not operation then
  return {2, -1}
end

local decoded_operation = cjson.decode(operation)
if decoded_operation["request_hash"] ~= ARGV[3] then
  return {3, -1}
end

local next_snapshot = cjson.decode(ARGV[2])
local next_version = expected_version + 1
next_snapshot["version"] = next_version
redis.call("SET", KEYS[1], cjson.encode(next_snapshot))
redis.call("SET", KEYS[2], ARGV[4], "EX", tonumber(ARGV[5]))
return {1, next_version}
"""


class PityStateStoreError(Exception):
    """Raised when Redis cannot serve pity state."""


class PityVersionConflict(Exception):
    """Raised when another request updated the same pity snapshot first."""

    def __init__(self, current_version: int) -> None:
        super().__init__("pity state version conflict")
        self.current_version = current_version


class RedisPityStateStore:
    """Read and atomically update pity snapshots in Redis."""

    def __init__(self, redis_url: str, key_prefix: str, pull_operation_ttl_seconds: int) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._key_prefix = key_prefix.strip(":")
        self._pull_operation_ttl_seconds = pull_operation_ttl_seconds

    async def close(self) -> None:
        await self._redis.aclose()

    async def ping(self) -> None:
        try:
            await self._redis.ping()
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

    async def get_snapshot(self, user_id: UUID, banner_id: str) -> PitySnapshot:
        key = self._key(user_id, banner_id)
        try:
            raw = await self._redis.get(key)
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

        if raw is None:
            return PitySnapshot(**create_initial_pity().model_dump(), version=0)

        try:
            return PitySnapshot.model_validate_json(raw)
        except ValueError as exc:
            raise PityStateStoreError("pity snapshot is invalid") from exc

    async def compare_and_set(
        self,
        *,
        user_id: UUID,
        banner_id: str,
        expected_version: int,
        next_pity: PityState,
    ) -> PitySnapshot:
        key = self._key(user_id, banner_id)
        next_payload = json.dumps(
            {
                **next_pity.model_dump(mode="json"),
                "version": expected_version + 1,
            },
            separators=(",", ":"),
        )

        try:
            result = await self._redis.eval(
                COMPARE_AND_SET_LUA,
                1,
                key,
                str(expected_version),
                next_payload,
            )
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

        updated, current_version = int(result[0]), int(result[1])
        if updated != 1:
            raise PityVersionConflict(current_version=current_version)

        return PitySnapshot(**next_pity.model_dump(), version=current_version)

    async def begin_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        request_hash: str,
    ) -> PullOperation | None:
        key = self._pull_operation_key(user_id, idempotency_key)
        operation = PullOperation(status="processing", request_hash=request_hash)

        try:
            created = await self._redis.set(
                key,
                operation.model_dump_json(),
                ex=self._processing_operation_ttl_seconds(),
                nx=True,
            )
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

        if created:
            return None

        return await self.get_pull_operation(user_id=user_id, idempotency_key=idempotency_key)

    async def get_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
    ) -> PullOperation | None:
        key = self._pull_operation_key(user_id, idempotency_key)
        try:
            raw = await self._redis.get(key)
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

        if raw is None:
            return None

        try:
            return PullOperation.model_validate_json(raw)
        except ValueError as exc:
            raise PityStateStoreError("pull operation is invalid") from exc

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
        pity_key = self._key(user_id, banner_id)
        operation_key = self._pull_operation_key(user_id, idempotency_key)
        next_payload = json.dumps(
            {
                **next_pity.model_dump(mode="json"),
                "version": expected_version + 1,
            },
            separators=(",", ":"),
        )

        try:
            result = await self._redis.eval(
                COMPARE_AND_SET_WITH_OPERATION_LUA,
                2,
                pity_key,
                operation_key,
                str(expected_version),
                next_payload,
                request_hash,
                operation.model_dump_json(),
                str(self._pull_operation_ttl_seconds),
            )
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

        updated, current_version = int(result[0]), int(result[1])
        if updated == 0:
            raise PityVersionConflict(current_version=current_version)
        if updated != 1:
            raise PityStateStoreError("pull operation could not be committed")

        return PitySnapshot(**next_pity.model_dump(), version=current_version)

    async def save_pull_operation(
        self,
        *,
        user_id: UUID,
        idempotency_key: str,
        operation: PullOperation,
    ) -> None:
        key = self._pull_operation_key(user_id, idempotency_key)
        try:
            await self._redis.set(
                key,
                operation.model_dump_json(),
                ex=self._pull_operation_ttl_seconds,
            )
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

    async def iter_event_pending_pull_operations(self, *, limit: int) -> list[PullOperationRecord]:
        if limit < 1:
            return []

        records: list[PullOperationRecord] = []
        cursor: int | str = 0
        pattern = f"{self._key_prefix}:pull-operation:*"

        try:
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor,
                    match=pattern,
                    count=max(limit, 100),
                )
                if keys:
                    values = await self._redis.mget(keys)
                    for key, raw in zip(keys, values, strict=False):
                        if raw is None:
                            continue
                        try:
                            operation = PullOperation.model_validate_json(raw)
                        except ValueError:
                            continue
                        if (
                            operation.status == "event_pending"
                            and operation.response is not None
                            and operation.event is not None
                        ):
                            records.append(
                                PullOperationRecord(
                                    operation_key=str(key),
                                    operation=operation,
                                )
                            )
                            if len(records) >= limit:
                                return records

                if cursor == 0 or cursor == "0":
                    return records
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

    async def claim_pull_operation_recovery(
        self,
        *,
        operation_key: str,
        lock_ttl_seconds: int,
    ) -> bool:
        try:
            created = await self._redis.set(
                self._pull_operation_recovery_lock_key(operation_key),
                "1",
                ex=max(1, lock_ttl_seconds),
                nx=True,
            )
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc
        return bool(created)

    async def release_pull_operation_recovery(self, *, operation_key: str) -> None:
        try:
            await self._redis.delete(self._pull_operation_recovery_lock_key(operation_key))
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

    async def save_pull_operation_by_key(
        self,
        *,
        operation_key: str,
        operation: PullOperation,
    ) -> None:
        if not operation_key.startswith(f"{self._key_prefix}:pull-operation:"):
            raise PityStateStoreError("pull operation key is invalid")

        try:
            await self._redis.set(
                operation_key,
                operation.model_dump_json(),
                ex=self._pull_operation_ttl_seconds,
            )
        except RedisError as exc:
            raise PityStateStoreError("redis is unavailable") from exc

    def _key(self, user_id: UUID, banner_id: str) -> str:
        return f"{self._key_prefix}:{user_id}:{banner_id}"

    def _pull_operation_key(self, user_id: UUID, idempotency_key: str) -> str:
        digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        return f"{self._key_prefix}:pull-operation:{user_id}:{digest}"

    def _pull_operation_recovery_lock_key(self, operation_key: str) -> str:
        return f"{operation_key}:recovery-lock"

    def _processing_operation_ttl_seconds(self) -> int:
        return min(self._pull_operation_ttl_seconds, 900)
