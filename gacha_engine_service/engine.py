"""Pure gacha calculation logic."""

from __future__ import annotations

import random
import uuid

from .catalog_config import BannerConfig
from .schemas import GachaItem, GachaRarity, PityState, PullRecord


PULL_RECORD_NAMESPACE = uuid.UUID("526b8ca6-8e43-4c6e-a1a6-1373927f45f4")


def create_initial_pity() -> PityState:
    """Return the default pity state for a banner."""

    return PityState()


def perform_pulls(
    *,
    banner_config: BannerConfig,
    count: int,
    pity: PityState,
    seed: str,
) -> tuple[list[PullRecord], PityState]:
    """Run one pull batch and return records plus the next pity state."""

    rng = random.Random(seed)
    banner = banner_config.banner
    current_pity = pity.model_copy(deep=True)
    records: list[PullRecord] = []

    for index in range(count):
        pity_at_five = current_pity.since_five + 1
        pity_at_four = current_pity.since_four + 1
        rarity = roll_rarity(banner_config, current_pity, rng)
        item = roll_item(banner_config, rarity, current_pity, rng)
        is_featured = is_featured_item(banner_config, item)

        if rarity == 5:
            current_pity.since_five = 0
            if banner_config.pity_rules[5].resets_lower_rarity:
                current_pity.since_four = 0
            else:
                current_pity.since_four += 1
            current_pity.guaranteed_featured_five = _next_featured_guarantee(
                banner_config,
                rarity=5,
                is_featured=is_featured,
            )
        elif rarity == 4:
            current_pity.since_five += 1
            current_pity.since_four = 0
        else:
            current_pity.since_five += 1
            current_pity.since_four += 1

        records.append(
            PullRecord(
                id=_record_id(seed, index, banner.id, item.id, pity_at_five, pity_at_four),
                index=index,
                item_id=item.id,
                item_name=item.name,
                item_type=item.type,
                rarity=rarity,
                banner_id=banner.id,
                banner_name=banner.name,
                pity_at_five=pity_at_five,
                pity_at_four=pity_at_four,
                is_featured=is_featured,
            )
        )

    return records, current_pity


def roll_rarity(
    banner_config: BannerConfig,
    pity: PityState,
    rng: random.Random,
) -> GachaRarity:
    """Roll item rarity using base rates and pity counters."""

    pity_at_five = pity.since_five + 1
    pity_at_four = pity.since_four + 1
    five_rule = banner_config.pity_rules[5]
    four_rule = banner_config.pity_rules[4]

    if pity_at_five >= five_rule.hard_pity:
        return 5

    if rng.random() < get_rarity_rate(banner_config, rarity=5, pity_count=pity_at_five):
        return 5

    if pity_at_four >= four_rule.hard_pity:
        return 4

    if rng.random() < get_rarity_rate(banner_config, rarity=4, pity_count=pity_at_four):
        return 4

    return 3


def get_rarity_rate(banner_config: BannerConfig, *, rarity: int, pity_count: int) -> float:
    """Return the configured probability for the current pity count."""

    base_rate = banner_config.rarity_rates[rarity].base_rate
    rule = banner_config.pity_rules.get(rarity)
    if rule is None or rule.soft_pity_start is None or pity_count < rule.soft_pity_start:
        return base_rate

    soft_step = pity_count - rule.soft_pity_start + 1
    return min(1.0, base_rate + soft_step * rule.soft_pity_increment)


def roll_item(banner_config: BannerConfig, rarity: GachaRarity, pity: PityState, rng: random.Random) -> GachaItem:
    """Roll an item from the selected banner for a known rarity."""

    featured_item_ids = banner_config.featured_item_ids_by_rarity.get(rarity, ())
    featured_rule = banner_config.featured_rules.get(rarity)
    if featured_item_ids and featured_rule is not None:
        if _has_featured_guarantee(pity, featured_rule) or rng.random() < featured_rule.featured_rate:
            return _pick_random(
                [banner_config.item_by_id[item_id] for item_id in featured_item_ids],
                rng,
            )

        if featured_rule.miss_sets_guarantee:
            off_rate_pool = banner_config.pool_for_rarity(rarity, exclude_ids=set(featured_item_ids))
            return _pick_random(off_rate_pool, rng)

    return _pick_random(banner_config.pool_for_rarity(rarity), rng)


def is_featured_item(banner_config: BannerConfig, item: GachaItem) -> bool:
    return item.id in banner_config.featured_item_ids_by_rarity.get(item.rarity, ())


def _has_featured_guarantee(pity: PityState, featured_rule: object) -> bool:
    state_key = getattr(featured_rule, "guarantee_state_key", None)
    return bool(state_key and getattr(pity, state_key, False))


def _next_featured_guarantee(
    banner_config: BannerConfig,
    *,
    rarity: int,
    is_featured: bool,
) -> bool:
    featured_rule = banner_config.featured_rules.get(rarity)
    return bool(featured_rule and featured_rule.miss_sets_guarantee and not is_featured)


def _pick_random(items: list[GachaItem], rng: random.Random) -> GachaItem:
    if not items:
        raise ValueError("cannot pick from an empty gacha pool")
    return items[rng.randrange(len(items))]


def _record_id(
    seed: str,
    index: int,
    banner_id: str,
    item_id: str,
    pity_at_five: int,
    pity_at_four: int,
) -> str:
    value = f"{seed}:{index}:{banner_id}:{item_id}:{pity_at_five}:{pity_at_four}"
    return str(uuid.uuid5(PULL_RECORD_NAMESPACE, value))
