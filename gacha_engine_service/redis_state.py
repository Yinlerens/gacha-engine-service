"""Redis-backed pity snapshot store."""

from __future__ import annotations

import json
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .engine import create_initial_pity
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


class PityStateStoreError(Exception):
    """Raised when Redis cannot serve pity state."""


class PityVersionConflict(Exception):
    """Raised when another request updated the same pity snapshot first."""

    def __init__(self, current_version: int) -> None:
        super().__init__("pity state version conflict")
        self.current_version = current_version


class RedisPityStateStore:
    """Read and atomically update pity snapshots in Redis."""

    def __init__(self, redis_url: str, key_prefix: str) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._key_prefix = key_prefix.strip(":")

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

    def _key(self, user_id: UUID, banner_id: str) -> str:
        return f"{self._key_prefix}:{user_id}:{banner_id}"

