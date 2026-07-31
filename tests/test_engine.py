from __future__ import annotations

import unittest

from gacha_engine_service.catalog_config import static_catalog_snapshot
from gacha_engine_service.engine import (
    RNG_ALGORITHM_VERSION,
    create_initial_pity,
    perform_pulls,
)
from gacha_engine_service.schemas import PityState


CATALOG = static_catalog_snapshot()
BANNER_CONFIG_BY_ID = CATALOG.banner_configs_by_id


class EngineTests(unittest.TestCase):
    def test_rng_v1_matches_its_golden_vector(self) -> None:
        banner_config = BANNER_CONFIG_BY_ID["limited-character-001"]

        records, next_pity = perform_pulls(
            banner_config=banner_config,
            count=10,
            pity=PityState(
                since_five=72,
                since_four=8,
                guaranteed_featured_five=True,
            ),
            seed="rng-v1-golden",
        )

        self.assertEqual(RNG_ALGORITHM_VERSION, "wuwa-gacha-rng-v1")
        self.assertEqual(
            [record.item_id for record in records],
            [
                "char-luoxian",
                "weapon-tide",
                "weapon-tide",
                "weapon-tide",
                "weapon-cinder",
                "weapon-tide",
                "weapon-tide",
                "weapon-cinder",
                "weapon-tide",
                "weapon-tide",
            ],
        )
        self.assertEqual(
            next_pity,
            PityState(
                since_five=9,
                since_four=9,
                guaranteed_featured_five=False,
            ),
        )

    def test_seed_makes_results_reproducible(self) -> None:
        banner_config = BANNER_CONFIG_BY_ID["limited-character-001"]
        pity = create_initial_pity()

        first_records, first_pity = perform_pulls(
            banner_config=banner_config,
            count=10,
            pity=pity,
            seed="stable-seed",
        )
        second_records, second_pity = perform_pulls(
            banner_config=banner_config,
            count=10,
            pity=create_initial_pity(),
            seed="stable-seed",
        )

        self.assertEqual(first_records, second_records)
        self.assertEqual(first_pity, second_pity)

    def test_perform_pulls_does_not_mutate_input_pity(self) -> None:
        banner_config = BANNER_CONFIG_BY_ID["limited-character-001"]
        pity = PityState(since_five=3, since_four=4)

        perform_pulls(
            banner_config=banner_config,
            count=1,
            pity=pity,
            seed="copy-check",
        )

        self.assertEqual(pity, PityState(since_five=3, since_four=4))

    def test_five_star_hard_pity_triggers_at_80(self) -> None:
        banner_config = BANNER_CONFIG_BY_ID["standard-001"]
        records, next_pity = perform_pulls(
            banner_config=banner_config,
            count=1,
            pity=PityState(since_five=79, since_four=0),
            seed="anything",
        )

        self.assertEqual(records[0].rarity, 5)
        self.assertEqual(next_pity.since_five, 0)
        self.assertEqual(next_pity.since_four, 0)

    def test_four_star_hard_pity_triggers_at_10(self) -> None:
        banner_config = BANNER_CONFIG_BY_ID["standard-001"]
        records, next_pity = perform_pulls(
            banner_config=banner_config,
            count=1,
            pity=PityState(since_five=0, since_four=9),
            seed="four-star-hard-pity",
        )

        self.assertEqual(records[0].rarity, 4)
        self.assertEqual(next_pity.since_four, 0)

    def test_guaranteed_featured_five_returns_featured_character(self) -> None:
        banner_config = BANNER_CONFIG_BY_ID["limited-character-001"]
        records, next_pity = perform_pulls(
            banner_config=banner_config,
            count=1,
            pity=PityState(
                since_five=79,
                since_four=0,
                guaranteed_featured_five=True,
            ),
            seed="guaranteed-featured",
        )

        self.assertEqual(records[0].rarity, 5)
        self.assertEqual(records[0].item_id, banner_config.banner.featured_five_id)
        self.assertFalse(next_pity.guaranteed_featured_five)


if __name__ == "__main__":
    unittest.main()
