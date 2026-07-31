"""Catalog repositories for static and Supabase/Postgres-backed gacha config."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any, Protocol

from .catalog_config import (
    BannerConfig,
    CatalogSnapshot,
    FeaturedRule,
    PityRule,
    RarityRate,
    ScheduledBannerConfig,
    static_catalog_snapshot,
)
from .schemas import Banner, BannerTheme, GachaItem


class CatalogLoadError(Exception):
    """Raised when the runtime catalog cannot be loaded."""


class CatalogRepository(Protocol):
    async def load_snapshot(self) -> CatalogSnapshot:
        """Load the current catalog snapshot."""

    async def load_release_snapshot(self, release_id: str) -> CatalogSnapshot:
        """Load one immutable historical release."""

    async def close(self) -> None:
        """Close repository resources."""


class StaticCatalogRepository:
    async def load_snapshot(self) -> CatalogSnapshot:
        return static_catalog_snapshot()

    async def load_release_snapshot(self, release_id: str) -> CatalogSnapshot:
        raise CatalogLoadError(f"static catalog has no release {release_id}")

    async def close(self) -> None:
        return None


class CachedCatalogProvider:
    """Small TTL cache around a catalog repository."""

    def __init__(self, repository: CatalogRepository, *, ttl_seconds: int) -> None:
        self._repository = repository
        self._ttl_seconds = max(0, ttl_seconds)
        self._snapshot: CatalogSnapshot | None = None
        self._expires_at = datetime.min.replace(tzinfo=timezone.utc)

    async def get_snapshot(self) -> CatalogSnapshot:
        now = datetime.now(timezone.utc)
        if self._snapshot is not None and now < self._expires_at:
            return self._snapshot

        snapshot = await self._repository.load_snapshot()
        self._snapshot = snapshot
        self._expires_at = now + timedelta(seconds=self._ttl_seconds)
        return snapshot

    async def ping(self) -> None:
        await self.get_snapshot()

    async def get_release_snapshot(self, release_id: str) -> CatalogSnapshot:
        if self._snapshot is not None and self._snapshot.release_id == release_id:
            return self._snapshot
        return await self._repository.load_release_snapshot(release_id)

    async def close(self) -> None:
        await self._repository.close()


class PostgresCatalogRepository:
    def __init__(
        self,
        *,
        database_url: str,
        project_id: str,
        environment_id: str,
        pool_size: int,
        query_timeout_seconds: int,
    ) -> None:
        self._database_url = database_url
        self._project_id = project_id
        self._environment_id = environment_id
        self._pool_size = max(1, pool_size)
        self._query_timeout_seconds = max(1, query_timeout_seconds)
        self._pool: Any | None = None

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def load_snapshot(self) -> CatalogSnapshot:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                release_row = await connection.fetchrow(
                    CURRENT_RELEASE_SQL,
                    self._project_id,
                    self._environment_id,
                    timeout=self._query_timeout_seconds,
                )
        except CatalogLoadError:
            raise
        except Exception as exc:
            raise CatalogLoadError("load gacha catalog from postgres failed") from exc

        return _build_snapshot_from_release(
            release_row,
            expected_project_id=self._project_id,
            expected_environment_id=self._environment_id,
        )

    async def load_release_snapshot(self, release_id: str) -> CatalogSnapshot:
        pool = await self._ensure_pool()
        try:
            async with pool.acquire() as connection:
                release_row = await connection.fetchrow(
                    RELEASE_BY_ID_SQL,
                    self._project_id,
                    self._environment_id,
                    release_id,
                    timeout=self._query_timeout_seconds,
                )
        except CatalogLoadError:
            raise
        except Exception as exc:
            raise CatalogLoadError("load historical gacha release failed") from exc

        if release_row is None:
            raise CatalogLoadError(f"gacha release {release_id} was not found")
        return _build_snapshot_from_release(
            release_row,
            expected_project_id=self._project_id,
            expected_environment_id=self._environment_id,
        )

    async def _ensure_pool(self) -> Any:
        if self._pool is not None:
            return self._pool

        try:
            import asyncpg
        except ImportError as exc:
            raise CatalogLoadError("asyncpg is required for postgres-backed catalog loading") from exc

        try:
            self._pool = await asyncpg.create_pool(
                dsn=self._database_url,
                min_size=1,
                max_size=self._pool_size,
                command_timeout=self._query_timeout_seconds,
                statement_cache_size=0,
                server_settings={"application_name": "gacha-engine-service"},
            )
        except Exception as exc:
            raise CatalogLoadError("connect to gacha config database failed") from exc

        return self._pool


def _build_snapshot_from_release(
    release_row: Any | None,
    *,
    expected_project_id: str,
    expected_environment_id: str,
    now: datetime | None = None,
) -> CatalogSnapshot:
    if release_row is None:
        return _build_snapshot(
            item_rows=[],
            banner_rows=[],
            banner_item_rows=[],
            rarity_rows=[],
            featured_rows=[],
            pity_rows=[],
        )

    if not bool(release_row["checksum_valid"]):
        raise CatalogLoadError("current gacha release checksum is invalid")

    payload = _json_object(release_row["snapshot"])
    if payload.get("schema_version") != 1:
        raise CatalogLoadError("unsupported gacha release snapshot schema")
    if str(payload.get("project_id", "")).lower() != expected_project_id.lower():
        raise CatalogLoadError("gacha release project does not match configured project")
    if str(payload.get("environment_id", "")).lower() != expected_environment_id.lower():
        raise CatalogLoadError("gacha release environment does not match configured environment")

    release_id = str(release_row["release_id"])
    release_number_value = _optional_row_value(release_row, "release_number")
    release_number = (
        int(release_number_value)
        if release_number_value is not None
        else int(payload["release_number"])
        if payload.get("release_number") is not None
        else None
    )
    snapshot_sha256_value = _optional_row_value(release_row, "snapshot_sha256")
    snapshot_sha256 = (
        str(snapshot_sha256_value) if snapshot_sha256_value is not None else None
    )
    embedded_release_id = payload.get("release_id")
    if embedded_release_id is not None and str(embedded_release_id) != release_id:
        raise CatalogLoadError("gacha release identity does not match its snapshot")

    item_rows = _release_array(payload, "items")
    banners_by_id = {
        str(row["id"]): row
        for row in _release_array(payload, "banners")
    }
    effective_at = now or datetime.now(timezone.utc)
    version_ids: set[str] = set()
    banner_rows: list[dict[str, Any]] = []

    for version in _release_array(payload, "banner_versions"):
        version_id = str(version["id"])
        banner_id = str(version["banner_id"])
        banner = banners_by_id.get(banner_id)
        if banner is None:
            raise CatalogLoadError(f"release version {version_id} references missing banner {banner_id}")

        version_ids.add(version_id)
        banner_rows.append(
            {
                "banner_version_id": version_id,
                "banner_id": banner_id,
                "pity_group_id": banner.get("pity_group_id") or banner_id,
                "rule_set_id": version.get("rule_set_id"),
                "version": version["version"],
                "name": banner["name"],
                "short_name": banner["short_name"],
                "banner_type": banner["banner_type"],
                "description": banner["description"],
                "theme": banner["theme"],
                "effective_from": version.get("effective_from"),
                "effective_to": version.get("effective_to"),
            }
        )

    def version_rows(key: str) -> list[dict[str, Any]]:
        return [
            row
            for row in _release_array(payload, key)
            if str(row["banner_version_id"]) in version_ids
        ]

    return _build_snapshot(
        item_rows=item_rows,
        banner_rows=banner_rows,
        banner_item_rows=version_rows("banner_items"),
        rarity_rows=version_rows("rarity_rates"),
        featured_rows=version_rows("featured_rules"),
        pity_rows=version_rows("pity_rules"),
        rule_set_rarity_rows=_release_array(payload, "rule_set_rarity_rates"),
        rule_set_featured_rows=_release_array(payload, "rule_set_featured_rules"),
        rule_set_pity_rows=_release_array(payload, "rule_set_pity_rules"),
        effective_at=effective_at,
        release_id=release_id,
        release_number=release_number,
        snapshot_sha256=snapshot_sha256,
    )


def _release_array(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise CatalogLoadError(f"release snapshot field {key} must be an array of objects")
    return value


def _version_is_effective(version: dict[str, Any], now: datetime) -> bool:
    effective_from = _release_timestamp(version.get("effective_from"), "effective_from")
    raw_effective_to = version.get("effective_to")
    effective_to = (
        _release_timestamp(raw_effective_to, "effective_to")
        if raw_effective_to is not None
        else None
    )
    return effective_from <= now and (effective_to is None or now < effective_to)


def _release_timestamp(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CatalogLoadError(f"release {field_name} is not a valid timestamp") from exc
    else:
        raise CatalogLoadError(f"release {field_name} is not a valid timestamp")

    if parsed.tzinfo is None:
        raise CatalogLoadError(f"release {field_name} must include a timezone")
    return parsed


def _build_snapshot(
    *,
    item_rows: list[Any],
    banner_rows: list[Any],
    banner_item_rows: list[Any],
    rarity_rows: list[Any],
    featured_rows: list[Any],
    pity_rows: list[Any],
    rule_set_rarity_rows: list[Any] | None = None,
    rule_set_featured_rows: list[Any] | None = None,
    rule_set_pity_rows: list[Any] | None = None,
    effective_at: datetime | None = None,
    release_id: str | None = None,
    release_number: int | None = None,
    snapshot_sha256: str | None = None,
) -> CatalogSnapshot:
    snapshot_time = effective_at or datetime.now(timezone.utc)
    items = tuple(_item_from_row(row) for row in item_rows)
    item_by_id = {item.id: item for item in items}

    banner_items_by_version: dict[str, list[Any]] = {}
    for row in banner_item_rows:
        banner_items_by_version.setdefault(str(row["banner_version_id"]), []).append(row)

    rarity_by_version: dict[str, dict[int, RarityRate]] = {}
    for row in rarity_rows:
        version_id = str(row["banner_version_id"])
        rarity_by_version.setdefault(version_id, {})[int(row["rarity"])] = RarityRate(
            rarity=int(row["rarity"]),
            base_rate_ppm=int(row["base_rate_ppm"]),
            roll_order=int(row["roll_order"]),
        )

    featured_by_version: dict[str, dict[int, FeaturedRule]] = {}
    for row in featured_rows:
        version_id = str(row["banner_version_id"])
        featured_by_version.setdefault(version_id, {})[int(row["rarity"])] = FeaturedRule(
            rarity=int(row["rarity"]),
            featured_group=str(row["featured_group"]),
            featured_rate_ppm=int(row["featured_rate_ppm"]),
            guarantee_after_miss=bool(row["guarantee_after_miss"]),
            miss_sets_guarantee=bool(row["miss_sets_guarantee"]),
            guarantee_state_key=row["guarantee_state_key"],
        )

    pity_by_version: dict[str, dict[int, PityRule]] = {}
    for row in pity_rows:
        version_id = str(row["banner_version_id"])
        pity_by_version.setdefault(version_id, {})[int(row["rarity"])] = PityRule(
            rarity=int(row["rarity"]),
            counter_key=str(row["counter_key"]),
            hard_pity=int(row["hard_pity"]),
            soft_pity_start=row["soft_pity_start"],
            soft_pity_increment_ppm=int(row["soft_pity_increment_ppm"]),
            resets_lower_rarity=bool(row["resets_lower_rarity"]),
        )

    rarity_by_rule_set = _rarity_rates_by_owner(rule_set_rarity_rows or [], "rule_set_id")
    featured_by_rule_set = _featured_rules_by_owner(rule_set_featured_rows or [], "rule_set_id")
    pity_by_rule_set = _pity_rules_by_owner(rule_set_pity_rows or [], "rule_set_id")

    banners: list[Banner] = []
    configs: dict[str, BannerConfig] = {}
    schedules: dict[str, list[ScheduledBannerConfig]] = {}
    for row in banner_rows:
        version_id = str(row["banner_version_id"])
        raw_rule_set_id = _optional_row_value(row, "rule_set_id")
        rule_set_id = str(raw_rule_set_id) if raw_rule_set_id is not None else None
        banner_items = banner_items_by_version.get(version_id, [])
        if not banner_items:
            raise CatalogLoadError(f"banner version {version_id} has no items")

        item_pool: list[str] = []
        featured_by_rarity: dict[int, list[str]] = {}
        for item_row in banner_items:
            item_id = str(item_row["item_id"])
            item = item_by_id.get(item_id)
            if item is None:
                raise CatalogLoadError(f"banner version {version_id} references disabled item {item_id}")

            item_pool.append(item_id)
            featured_group = item_row["featured_group"]
            if featured_group:
                featured_by_rarity.setdefault(item.rarity, []).append(item_id)

        featured_five_ids = featured_by_rarity.get(5, [])
        featured_four_ids = featured_by_rarity.get(4, [])
        banner = Banner(
            id=str(row["banner_id"]),
            name=str(row["name"]),
            short_name=str(row["short_name"]),
            type=str(row["banner_type"]),
            description=str(row["description"]),
            featured_five_id=featured_five_ids[0] if featured_five_ids else None,
            featured_four_ids=featured_four_ids,
            item_pool=item_pool,
            theme=BannerTheme(**_json_object(row["theme"])),
        )

        rates = (
            rarity_by_rule_set.get(rule_set_id, {})
            if rule_set_id is not None and rule_set_id in rarity_by_rule_set
            else rarity_by_version.get(version_id, {})
        )
        featured_rules = (
            featured_by_rule_set.get(rule_set_id, {})
            if rule_set_id is not None and rule_set_id in featured_by_rule_set
            else featured_by_version.get(version_id, {})
        )
        pity_rules = (
            pity_by_rule_set.get(rule_set_id, {})
            if rule_set_id is not None and rule_set_id in pity_by_rule_set
            else pity_by_version.get(version_id, {})
        )
        _validate_runtime_banner_config(
            banner=banner,
            banner_version_id=version_id,
            rates=rates,
            pity_rules=pity_rules,
        )

        config = BannerConfig(
            banner=banner,
            pity_group_id=_pity_group_id_from_row(row, banner.id),
            banner_version_id=version_id,
            version=int(row["version"]),
            all_items=items,
            item_by_id=item_by_id,
            rarity_rates=rates,
            featured_rules=featured_rules,
            pity_rules=pity_rules,
            featured_item_ids_by_rarity={
                rarity: tuple(ids) for rarity, ids in featured_by_rarity.items()
            },
        )
        schedule = ScheduledBannerConfig(
            config=config,
            effective_from=_optional_release_timestamp(row, "effective_from"),
            effective_to=_optional_release_timestamp(row, "effective_to"),
        )
        schedules.setdefault(banner.id, []).append(schedule)
        if schedule.is_effective_at(snapshot_time):
            if banner.id in configs:
                raise CatalogLoadError(
                    f"release contains overlapping active versions for banner {banner.id}"
                )
            banners.append(banner)
            configs[banner.id] = config

    ordered_schedules = {
        banner_id: tuple(
            sorted(
                banner_schedules,
                key=lambda entry: entry.effective_from
                or datetime.min.replace(tzinfo=timezone.utc),
            )
        )
        for banner_id, banner_schedules in schedules.items()
    }

    return CatalogSnapshot(
        source="postgres",
        loaded_at=snapshot_time,
        items=items,
        banners=tuple(banners),
        banner_configs_by_id=configs,
        banner_schedules_by_id=ordered_schedules,
        release_id=release_id,
        release_number=release_number,
        snapshot_sha256=snapshot_sha256,
    )


def _optional_release_timestamp(row: Any, field_name: str) -> datetime | None:
    value = _optional_row_value(row, field_name)
    if value is None:
        return None
    return _release_timestamp(value, field_name)


def _validate_runtime_banner_config(
    *,
    banner: Banner,
    banner_version_id: str,
    rates: dict[int, RarityRate],
    pity_rules: dict[int, PityRule],
) -> None:
    for rarity in (5, 4):
        if rarity not in rates:
            raise CatalogLoadError(f"banner version {banner_version_id} missing {rarity}-star rate")
        if rarity not in pity_rules:
            raise CatalogLoadError(f"banner version {banner_version_id} missing {rarity}-star pity rule")

    if not banner.item_pool:
        raise CatalogLoadError(f"banner version {banner_version_id} item_pool is empty")


def _rarity_rates_by_owner(rows: list[Any], owner_key: str) -> dict[str, dict[int, RarityRate]]:
    rates_by_owner: dict[str, dict[int, RarityRate]] = {}
    for row in rows:
        owner_id = str(row[owner_key])
        rates_by_owner.setdefault(owner_id, {})[int(row["rarity"])] = RarityRate(
            rarity=int(row["rarity"]),
            base_rate_ppm=int(row["base_rate_ppm"]),
            roll_order=int(row["roll_order"]),
        )
    return rates_by_owner


def _featured_rules_by_owner(rows: list[Any], owner_key: str) -> dict[str, dict[int, FeaturedRule]]:
    rules_by_owner: dict[str, dict[int, FeaturedRule]] = {}
    for row in rows:
        owner_id = str(row[owner_key])
        rules_by_owner.setdefault(owner_id, {})[int(row["rarity"])] = FeaturedRule(
            rarity=int(row["rarity"]),
            featured_group=str(row["featured_group"]),
            featured_rate_ppm=int(row["featured_rate_ppm"]),
            guarantee_after_miss=bool(row["guarantee_after_miss"]),
            miss_sets_guarantee=bool(row["miss_sets_guarantee"]),
            guarantee_state_key=row["guarantee_state_key"],
        )
    return rules_by_owner


def _pity_rules_by_owner(rows: list[Any], owner_key: str) -> dict[str, dict[int, PityRule]]:
    rules_by_owner: dict[str, dict[int, PityRule]] = {}
    for row in rows:
        owner_id = str(row[owner_key])
        rules_by_owner.setdefault(owner_id, {})[int(row["rarity"])] = PityRule(
            rarity=int(row["rarity"]),
            counter_key=str(row["counter_key"]),
            hard_pity=int(row["hard_pity"]),
            soft_pity_start=row["soft_pity_start"],
            soft_pity_increment_ppm=int(row["soft_pity_increment_ppm"]),
            resets_lower_rarity=bool(row["resets_lower_rarity"]),
        )
    return rules_by_owner


def _item_from_row(row: Any) -> GachaItem:
    return GachaItem(
        id=str(row["id"]),
        name=str(row["name"]),
        subtitle=str(row["subtitle"]),
        rarity=int(row["rarity"]),
        type=str(row["item_type"]),
        element=str(row["element"]),
        role=str(row["role"]),
        faction=str(row["faction"]),
        accent=str(row["accent"]),
        quote=str(row["quote"]),
    )


def _optional_row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)

    try:
        return row[key]
    except KeyError:
        return None


def _pity_group_id_from_row(row: Any, banner_id: str) -> str:
    raw_value = _optional_row_value(row, "pity_group_id")
    pity_group_id = str(raw_value) if raw_value is not None else banner_id
    if not pity_group_id or pity_group_id != pity_group_id.strip() or len(pity_group_id) > 100:
        raise CatalogLoadError(f"banner {banner_id} has an invalid pity group id")
    return pity_group_id


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return decoded
    raise CatalogLoadError("expected JSON object from catalog query")


CURRENT_RELEASE_SQL = """
select
  release.id::text as release_id,
  release.release_number,
  release.snapshot_sha256,
  release.snapshot,
  release.snapshot_sha256 = encode(
    extensions.digest(release.snapshot::text, 'sha256'),
    'hex'
  ) as checksum_valid
from gacha.environment_release_heads head
join gacha.releases release
  on release.id = head.release_id
 and release.project_id = head.project_id
  and release.environment_id = head.environment_id
where head.project_id = $1::uuid
  and head.environment_id = $2::uuid
"""


RELEASE_BY_ID_SQL = """
select
  release.id::text as release_id,
  release.release_number,
  release.snapshot_sha256,
  release.snapshot,
  release.snapshot_sha256 = encode(
    extensions.digest(release.snapshot::text, 'sha256'),
    'hex'
  ) as checksum_valid
from gacha.releases release
where release.project_id = $1::uuid
  and release.environment_id = $2::uuid
  and release.id = $3::uuid
"""
