"""Durable pull operation state and processing claims."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from .catalog_config import BannerConfig, FeaturedRule, PityRule, RarityRate
from .schemas import Banner, GachaItem, PullCompletedEvent, PullResponse


PullOperationStatus = Literal[
    "processing",
    "event_pending",
    "succeeded",
    "refund_pending",
    "failed",
]


class RecoveryRarityRate(BaseModel):
    rarity: int
    base_rate_ppm: int
    roll_order: int


class RecoveryFeaturedRule(BaseModel):
    rarity: int
    featured_group: str
    featured_rate_ppm: int
    guarantee_after_miss: bool
    miss_sets_guarantee: bool
    guarantee_state_key: str | None = None


class RecoveryPityRule(BaseModel):
    rarity: int
    counter_key: str
    hard_pity: int
    soft_pity_start: int | None = None
    soft_pity_increment_ppm: int = 0
    resets_lower_rarity: bool = False


class PullRecoveryContext(BaseModel):
    """Frozen inputs required to finish a charged pull without the client."""

    banner: Banner
    banner_version_id: str | None = None
    banner_version: int
    items: tuple[GachaItem, ...]
    rarity_rates: tuple[RecoveryRarityRate, ...]
    featured_rules: tuple[RecoveryFeaturedRule, ...]
    pity_rules: tuple[RecoveryPityRule, ...]
    featured_item_ids_by_rarity: dict[int, tuple[str, ...]]
    count: Literal[1, 10]
    seed: str = Field(min_length=1, max_length=200)
    event_id: UUID
    amount_minor: int = Field(gt=0)
    request_id: str = ""

    @classmethod
    def from_banner_config(
        cls,
        *,
        banner_config: BannerConfig,
        count: Literal[1, 10],
        seed: str,
        event_id: str,
        amount_minor: int,
        request_id: str,
    ) -> PullRecoveryContext:
        return cls(
            banner=banner_config.banner,
            banner_version_id=banner_config.banner_version_id,
            banner_version=banner_config.version,
            items=banner_config.all_items,
            rarity_rates=tuple(
                RecoveryRarityRate(
                    rarity=rate.rarity,
                    base_rate_ppm=rate.base_rate_ppm,
                    roll_order=rate.roll_order,
                )
                for _, rate in sorted(banner_config.rarity_rates.items())
            ),
            featured_rules=tuple(
                RecoveryFeaturedRule(
                    rarity=rule.rarity,
                    featured_group=rule.featured_group,
                    featured_rate_ppm=rule.featured_rate_ppm,
                    guarantee_after_miss=rule.guarantee_after_miss,
                    miss_sets_guarantee=rule.miss_sets_guarantee,
                    guarantee_state_key=rule.guarantee_state_key,
                )
                for _, rule in sorted(banner_config.featured_rules.items())
            ),
            pity_rules=tuple(
                RecoveryPityRule(
                    rarity=rule.rarity,
                    counter_key=rule.counter_key,
                    hard_pity=rule.hard_pity,
                    soft_pity_start=rule.soft_pity_start,
                    soft_pity_increment_ppm=rule.soft_pity_increment_ppm,
                    resets_lower_rarity=rule.resets_lower_rarity,
                )
                for _, rule in sorted(banner_config.pity_rules.items())
            ),
            featured_item_ids_by_rarity={
                int(rarity): tuple(item_ids)
                for rarity, item_ids in banner_config.featured_item_ids_by_rarity.items()
            },
            count=count,
            seed=seed,
            event_id=event_id,
            amount_minor=amount_minor,
            request_id=request_id,
        )

    def to_banner_config(self) -> BannerConfig:
        items = tuple(self.items)
        return BannerConfig(
            banner=self.banner,
            banner_version_id=self.banner_version_id,
            version=self.banner_version,
            all_items=items,
            item_by_id={item.id: item for item in items},
            rarity_rates={
                rate.rarity: RarityRate(
                    rarity=rate.rarity,
                    base_rate_ppm=rate.base_rate_ppm,
                    roll_order=rate.roll_order,
                )
                for rate in self.rarity_rates
            },
            featured_rules={
                rule.rarity: FeaturedRule(
                    rarity=rule.rarity,
                    featured_group=rule.featured_group,
                    featured_rate_ppm=rule.featured_rate_ppm,
                    guarantee_after_miss=rule.guarantee_after_miss,
                    miss_sets_guarantee=rule.miss_sets_guarantee,
                    guarantee_state_key=rule.guarantee_state_key,
                )
                for rule in self.featured_rules
            },
            pity_rules={
                rule.rarity: PityRule(
                    rarity=rule.rarity,
                    counter_key=rule.counter_key,
                    hard_pity=rule.hard_pity,
                    soft_pity_start=rule.soft_pity_start,
                    soft_pity_increment_ppm=rule.soft_pity_increment_ppm,
                    resets_lower_rarity=rule.resets_lower_rarity,
                )
                for rule in self.pity_rules
            },
            featured_item_ids_by_rarity={
                rarity: tuple(item_ids)
                for rarity, item_ids in self.featured_item_ids_by_rarity.items()
            },
        )


class PullOperation(BaseModel):
    status: PullOperationStatus
    request_hash: str
    response: PullResponse | None = None
    event: PullCompletedEvent | None = None
    error_code: str | None = None
    error_message: str | None = None
    recovery_context: PullRecoveryContext | None = None


@dataclass(frozen=True)
class PullOperationClaim:
    operation_key: str
    operation: PullOperation
    processing_token: UUID | None = None

    @property
    def acquired(self) -> bool:
        return self.processing_token is not None


@dataclass(frozen=True)
class PullOperationRecord:
    operation_key: str
    user_id: UUID
    operation: PullOperation


@dataclass(frozen=True)
class PullOperationRecoveryClaim:
    operation_key: str
    user_id: UUID
    operation: PullOperation
    processing_token: UUID
