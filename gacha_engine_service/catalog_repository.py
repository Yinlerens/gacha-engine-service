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
    static_catalog_snapshot,
)
from .schemas import Banner, BannerTheme, GachaItem


class CatalogLoadError(Exception):
    """Raised when the runtime catalog cannot be loaded."""


class CatalogRepository(Protocol):
    async def load_snapshot(self) -> CatalogSnapshot:
        """Load the current catalog snapshot."""

    async def close(self) -> None:
        """Close repository resources."""


class StaticCatalogRepository:
    async def load_snapshot(self) -> CatalogSnapshot:
        return static_catalog_snapshot()

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

    async def close(self) -> None:
        await self._repository.close()


class PostgresCatalogRepository:
    def __init__(
        self,
        *,
        database_url: str,
        pool_size: int,
        query_timeout_seconds: int,
    ) -> None:
        self._database_url = database_url
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
                items_rows = await connection.fetch(ITEMS_SQL, timeout=self._query_timeout_seconds)
                banner_rows = await connection.fetch(BANNERS_SQL, timeout=self._query_timeout_seconds)

                version_ids = [str(row["banner_version_id"]) for row in banner_rows]
                banner_item_rows = await connection.fetch(
                    BANNER_ITEMS_SQL,
                    version_ids,
                    timeout=self._query_timeout_seconds,
                )
                rule_set_ids = sorted(
                    {
                        str(row["rule_set_id"])
                        for row in banner_rows
                        if row["rule_set_id"] is not None
                    }
                )
                rarity_rows = await connection.fetch(
                    RARITY_RATES_SQL,
                    version_ids,
                    timeout=self._query_timeout_seconds,
                )
                featured_rows = await connection.fetch(
                    FEATURED_RULES_SQL,
                    version_ids,
                    timeout=self._query_timeout_seconds,
                )
                rule_set_rarity_rows = await connection.fetch(
                    RULE_SET_RARITY_RATES_SQL,
                    rule_set_ids,
                    timeout=self._query_timeout_seconds,
                )
                rule_set_featured_rows = await connection.fetch(
                    RULE_SET_FEATURED_RULES_SQL,
                    rule_set_ids,
                    timeout=self._query_timeout_seconds,
                )
                rule_set_pity_rows = await connection.fetch(
                    RULE_SET_PITY_RULES_SQL,
                    rule_set_ids,
                    timeout=self._query_timeout_seconds,
                )
                pity_rows = await connection.fetch(
                    PITY_RULES_SQL,
                    version_ids,
                    timeout=self._query_timeout_seconds,
                )
        except CatalogLoadError:
            raise
        except Exception as exc:
            raise CatalogLoadError("load gacha catalog from postgres failed") from exc

        return _build_snapshot(
            item_rows=items_rows,
            banner_rows=banner_rows,
            banner_item_rows=banner_item_rows,
            rarity_rows=rarity_rows,
            featured_rows=featured_rows,
            pity_rows=pity_rows,
            rule_set_rarity_rows=rule_set_rarity_rows,
            rule_set_featured_rows=rule_set_featured_rows,
            rule_set_pity_rows=rule_set_pity_rows,
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
) -> CatalogSnapshot:
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

        banners.append(banner)
        configs[banner.id] = BannerConfig(
            banner=banner,
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

    return CatalogSnapshot(
        source="postgres",
        loaded_at=datetime.now(timezone.utc),
        items=items,
        banners=tuple(banners),
        banner_configs_by_id=configs,
    )


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


ITEMS_SQL = """
select id, name, subtitle, rarity, item_type, element, role, faction, accent, quote
from gacha.items
where is_enabled
order by rarity desc, item_type, id
"""

BANNERS_SQL = """
select
  bv.id::text as banner_version_id,
  bv.banner_id,
  bv.rule_set_id,
  bv.version,
  b.name,
  b.short_name,
  b.banner_type,
  b.description,
  b.theme
from gacha.banner_versions bv
join gacha.banners b on b.id = bv.banner_id
where b.is_enabled
  and bv.status = 'published'
  and now() >= bv.effective_from
  and (bv.effective_to is null or now() < bv.effective_to)
order by b.sort_order, b.id
"""

BANNER_ITEMS_SQL = """
select banner_version_id::text as banner_version_id, item_id, pool_group, featured_group, weight, sort_order
from gacha.banner_items
where banner_version_id::text = any($1::text[])
order by banner_version_id, sort_order, item_id
"""

RARITY_RATES_SQL = """
select banner_version_id::text as banner_version_id, rarity, base_rate_ppm, roll_order
from gacha.rarity_rates
where banner_version_id::text = any($1::text[])
order by banner_version_id, roll_order
"""

FEATURED_RULES_SQL = """
select
  banner_version_id::text as banner_version_id,
  rarity,
  featured_group,
  featured_rate_ppm,
  guarantee_after_miss,
  miss_sets_guarantee,
  guarantee_state_key
from gacha.featured_rules
where banner_version_id::text = any($1::text[])
"""

PITY_RULES_SQL = """
select
  banner_version_id::text as banner_version_id,
  rarity,
  counter_key,
  hard_pity,
  soft_pity_start,
  soft_pity_increment_ppm,
  resets_lower_rarity
from gacha.pity_rules
where banner_version_id::text = any($1::text[])
"""

RULE_SET_RARITY_RATES_SQL = """
select rule_set_id, rarity, base_rate_ppm, roll_order
from gacha.rule_set_rarity_rates
where rule_set_id = any($1::text[])
order by rule_set_id, roll_order
"""

RULE_SET_FEATURED_RULES_SQL = """
select
  rule_set_id,
  rarity,
  featured_group,
  featured_rate_ppm,
  guarantee_after_miss,
  miss_sets_guarantee,
  guarantee_state_key
from gacha.rule_set_featured_rules
where rule_set_id = any($1::text[])
"""

RULE_SET_PITY_RULES_SQL = """
select
  rule_set_id,
  rarity,
  counter_key,
  hard_pity,
  soft_pity_start,
  soft_pity_increment_ppm,
  resets_lower_rarity
from gacha.rule_set_pity_rules
where rule_set_id = any($1::text[])
"""
