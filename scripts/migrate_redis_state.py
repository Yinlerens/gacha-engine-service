"""One-time migration of legacy Redis gacha state into Postgres."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import os
import re
from typing import Literal
import uuid
from uuid import UUID

from gacha_engine_service.pull_operations import PullOperation
from gacha_engine_service.schemas import PitySnapshot


LegacyKey = tuple[Literal["pity", "operation"], UUID, str]
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass
class MigrationStats:
    pity_snapshots: int = 0
    pull_operations: int = 0
    skipped: int = 0


def parse_legacy_key(prefix: str, key: str) -> LegacyKey | None:
    normalized_prefix = prefix.strip(":")
    marker = f"{normalized_prefix}:"
    if not key.startswith(marker):
        return None

    suffix = key[len(marker) :]
    operation_marker = "pull-operation:"
    if suffix.startswith(operation_marker):
        parts = suffix[len(operation_marker) :].split(":")
        if len(parts) != 2 or not SHA256_PATTERN.fullmatch(parts[1]):
            return None
        try:
            return "operation", UUID(parts[0]), parts[1]
        except ValueError:
            return None

    parts = suffix.split(":", 1)
    if len(parts) != 2 or not parts[1] or len(parts[1]) > 100:
        return None
    try:
        return "pity", UUID(parts[0]), parts[1]
    except ValueError:
        return None


async def migrate_legacy_state(
    *,
    redis_url: str,
    database_url: str,
    key_prefix: str,
) -> MigrationStats:
    if not redis_url.strip():
        raise ValueError("REDIS_URL is required")
    if not database_url.strip():
        raise ValueError("GACHA_STATE_DATABASE_URL is required")

    import asyncpg
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url, decode_responses=True)
    pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=2,
        command_timeout=10,
        statement_cache_size=0,
        server_settings={"application_name": "gacha-redis-state-migration"},
    )
    stats = MigrationStats()

    try:
        async with pool.acquire() as connection:
            await connection.execute(
                "select 1 from gacha_runtime.pull_operations limit 0"
            )
            await connection.execute(
                "select 1 from gacha_runtime.pity_snapshots limit 0"
            )

        async for key in redis.scan_iter(match=f"{key_prefix.strip(':')}:*", count=200):
            parsed = parse_legacy_key(key_prefix, str(key))
            if parsed is None:
                stats.skipped += 1
                continue

            raw = await redis.get(key)
            if raw is None:
                stats.skipped += 1
                continue

            record_type, user_id, identifier = parsed
            try:
                async with pool.acquire() as connection:
                    if record_type == "pity":
                        snapshot = PitySnapshot.model_validate_json(raw)
                        await _import_pity_snapshot(
                            connection,
                            user_id=user_id,
                            banner_id=identifier,
                            snapshot=snapshot,
                        )
                        stats.pity_snapshots += 1
                    else:
                        operation = PullOperation.model_validate_json(raw)
                        await _import_pull_operation(
                            connection,
                            user_id=user_id,
                            idempotency_key_hash=identifier,
                            operation=operation,
                        )
                        stats.pull_operations += 1
            except ValueError:
                stats.skipped += 1
    finally:
        await redis.aclose()
        await pool.close()

    return stats


async def _import_pity_snapshot(
    connection: object,
    *,
    user_id: UUID,
    banner_id: str,
    snapshot: PitySnapshot,
) -> None:
    await connection.execute(
        IMPORT_PITY_SQL,
        user_id,
        banner_id,
        snapshot.since_five,
        snapshot.since_four,
        snapshot.guaranteed_featured_five,
        snapshot.version,
    )


async def _import_pull_operation(
    connection: object,
    *,
    user_id: UUID,
    idempotency_key_hash: str,
    operation: PullOperation,
) -> None:
    await connection.execute(
        IMPORT_OPERATION_SQL,
        uuid.uuid4(),
        user_id,
        idempotency_key_hash,
        operation.request_hash,
        operation.status,
        operation.response.model_dump_json() if operation.response is not None else None,
        operation.event.model_dump_json() if operation.event is not None else None,
        operation.error_code,
        operation.error_message,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate legacy Redis gacha state to authoritative Postgres tables."
    )
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""))
    parser.add_argument(
        "--database-url",
        default=(
            os.getenv("GACHA_STATE_DATABASE_URL", "")
            or os.getenv("GACHA_CONFIG_DATABASE_URL", "")
        ),
    )
    parser.add_argument(
        "--key-prefix",
        default=os.getenv("REDIS_KEY_PREFIX", "gacha:pity"),
    )
    return parser


async def _main() -> None:
    args = build_parser().parse_args()
    stats = await migrate_legacy_state(
        redis_url=args.redis_url,
        database_url=args.database_url,
        key_prefix=args.key_prefix,
    )
    print(
        "migration complete: "
        f"pity_snapshots={stats.pity_snapshots} "
        f"pull_operations={stats.pull_operations} "
        f"skipped={stats.skipped}"
    )


IMPORT_PITY_SQL = """
insert into gacha_runtime.pity_snapshots (
  user_id, banner_id, since_five, since_four, guaranteed_featured_five, version
)
values ($1, $2, $3, $4, $5, $6)
on conflict (user_id, banner_id) do update
set since_five = excluded.since_five,
    since_four = excluded.since_four,
    guaranteed_featured_five = excluded.guaranteed_featured_five,
    version = excluded.version,
    updated_at = now()
where gacha_runtime.pity_snapshots.version < excluded.version
"""

IMPORT_OPERATION_SQL = """
insert into gacha_runtime.pull_operations (
  id, user_id, idempotency_key_hash, request_hash, status,
  response, event, error_code, error_message
)
values ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)
on conflict (user_id, idempotency_key_hash) do nothing
"""


if __name__ == "__main__":
    asyncio.run(_main())
