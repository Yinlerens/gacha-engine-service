"""Runtime gacha catalog and rule models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from .catalog import BANNERS, GACHA_ITEMS
from .schemas import Banner, GachaItem


DEFAULT_FIVE_STAR_BASE_RATE_PPM = 6000
DEFAULT_FOUR_STAR_BASE_RATE_PPM = 51000
DEFAULT_FIVE_STAR_HARD_PITY = 80
DEFAULT_FOUR_STAR_HARD_PITY = 10
DEFAULT_SOFT_PITY_START = 66
DEFAULT_SOFT_PITY_INCREMENT_PPM = 60000


@dataclass(frozen=True)
class RarityRate:
    rarity: int
    base_rate_ppm: int
    roll_order: int

    @property
    def base_rate(self) -> float:
        return self.base_rate_ppm / 1_000_000


@dataclass(frozen=True)
class FeaturedRule:
    rarity: int
    featured_group: str
    featured_rate_ppm: int
    guarantee_after_miss: bool
    miss_sets_guarantee: bool
    guarantee_state_key: str | None = None

    @property
    def featured_rate(self) -> float:
        return self.featured_rate_ppm / 1_000_000


@dataclass(frozen=True)
class PityRule:
    rarity: int
    counter_key: str
    hard_pity: int
    soft_pity_start: int | None = None
    soft_pity_increment_ppm: int = 0
    resets_lower_rarity: bool = False

    @property
    def soft_pity_increment(self) -> float:
        return self.soft_pity_increment_ppm / 1_000_000


@dataclass(frozen=True)
class BannerConfig:
    banner: Banner
    banner_version_id: str | None
    version: int
    all_items: tuple[GachaItem, ...]
    item_by_id: Mapping[str, GachaItem]
    rarity_rates: Mapping[int, RarityRate]
    featured_rules: Mapping[int, FeaturedRule]
    pity_rules: Mapping[int, PityRule]
    featured_item_ids_by_rarity: Mapping[int, tuple[str, ...]]

    def pool_for_rarity(
        self,
        rarity: int,
        *,
        exclude_ids: set[str] | None = None,
    ) -> list[GachaItem]:
        exclude_ids = exclude_ids or set()
        pool = [
            self.item_by_id[item_id]
            for item_id in self.banner.item_pool
            if item_id in self.item_by_id
            and self.item_by_id[item_id].rarity == rarity
            and item_id not in exclude_ids
        ]
        if pool:
            return pool

        return [
            item
            for item in self.all_items
            if item.rarity == rarity and item.id not in exclude_ids
        ]


@dataclass(frozen=True)
class CatalogSnapshot:
    source: str
    loaded_at: datetime
    items: tuple[GachaItem, ...]
    banners: tuple[Banner, ...]
    banner_configs_by_id: Mapping[str, BannerConfig]


def static_catalog_snapshot() -> CatalogSnapshot:
    items = tuple(GACHA_ITEMS)
    item_by_id = {item.id: item for item in items}
    configs = {
        banner.id: _static_banner_config(banner=banner, items=items, item_by_id=item_by_id)
        for banner in BANNERS
    }
    return CatalogSnapshot(
        source="static",
        loaded_at=datetime.now(timezone.utc),
        items=items,
        banners=tuple(BANNERS),
        banner_configs_by_id=configs,
    )


def _static_banner_config(
    *,
    banner: Banner,
    items: tuple[GachaItem, ...],
    item_by_id: Mapping[str, GachaItem],
) -> BannerConfig:
    featured_by_rarity: dict[int, tuple[str, ...]] = {}
    if banner.featured_five_id:
        featured_by_rarity[5] = (banner.featured_five_id,)
    if banner.featured_four_ids:
        featured_by_rarity[4] = tuple(banner.featured_four_ids)

    featured_rules: dict[int, FeaturedRule] = {}
    if banner.featured_five_id:
        featured_rules[5] = FeaturedRule(
            rarity=5,
            featured_group="five_up",
            featured_rate_ppm=500000,
            guarantee_after_miss=True,
            miss_sets_guarantee=True,
            guarantee_state_key="guaranteed_featured_five",
        )
    if banner.featured_four_ids:
        featured_rules[4] = FeaturedRule(
            rarity=4,
            featured_group="four_up",
            featured_rate_ppm=600000,
            guarantee_after_miss=False,
            miss_sets_guarantee=False,
        )

    return BannerConfig(
        banner=banner,
        banner_version_id=None,
        version=1,
        all_items=items,
        item_by_id=item_by_id,
        rarity_rates={
            5: RarityRate(5, DEFAULT_FIVE_STAR_BASE_RATE_PPM, 1),
            4: RarityRate(4, DEFAULT_FOUR_STAR_BASE_RATE_PPM, 2),
            3: RarityRate(3, 1_000_000 - DEFAULT_FIVE_STAR_BASE_RATE_PPM - DEFAULT_FOUR_STAR_BASE_RATE_PPM, 3),
        },
        featured_rules=featured_rules,
        pity_rules={
            5: PityRule(
                rarity=5,
                counter_key="five_star",
                hard_pity=DEFAULT_FIVE_STAR_HARD_PITY,
                soft_pity_start=DEFAULT_SOFT_PITY_START,
                soft_pity_increment_ppm=DEFAULT_SOFT_PITY_INCREMENT_PPM,
                resets_lower_rarity=True,
            ),
            4: PityRule(
                rarity=4,
                counter_key="four_star",
                hard_pity=DEFAULT_FOUR_STAR_HARD_PITY,
            ),
        },
        featured_item_ids_by_rarity=featured_by_rarity,
    )
