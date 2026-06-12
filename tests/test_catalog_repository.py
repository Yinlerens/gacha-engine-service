from __future__ import annotations

import unittest

from gacha_engine_service.catalog_repository import _build_snapshot


class CatalogRepositoryTests(unittest.TestCase):
    def test_build_snapshot_projects_published_banner_config(self) -> None:
        snapshot = _build_snapshot(
            item_rows=[
                {
                    "id": "char-up",
                    "name": "UP",
                    "subtitle": "",
                    "rarity": 5,
                    "item_type": "character",
                    "element": "",
                    "role": "",
                    "faction": "",
                    "accent": "#fff",
                    "quote": "",
                },
                {
                    "id": "weapon-base",
                    "name": "Base",
                    "subtitle": "",
                    "rarity": 4,
                    "item_type": "weapon",
                    "element": "",
                    "role": "",
                    "faction": "",
                    "accent": "#aaa",
                    "quote": "",
                },
            ],
            banner_rows=[
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "banner_id": "banner-1",
                    "version": 7,
                    "name": "Dynamic",
                    "short_name": "Dyn",
                    "banner_type": "limited-character",
                    "description": "Loaded from postgres",
                    "theme": {"primary": "#fff", "secondary": "#000", "glow": "rgba(0,0,0,0.2)"},
                }
            ],
            banner_item_rows=[
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "item_id": "char-up",
                    "featured_group": "five_up",
                    "sort_order": 1,
                },
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "item_id": "weapon-base",
                    "featured_group": None,
                    "sort_order": 2,
                },
            ],
            rarity_rows=[
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "rarity": 5,
                    "base_rate_ppm": 1000000,
                    "roll_order": 1,
                },
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "rarity": 4,
                    "base_rate_ppm": 0,
                    "roll_order": 2,
                },
            ],
            featured_rows=[
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "rarity": 5,
                    "featured_group": "five_up",
                    "featured_rate_ppm": 1000000,
                    "guarantee_after_miss": True,
                    "miss_sets_guarantee": True,
                    "guarantee_state_key": "guaranteed_featured_five",
                }
            ],
            pity_rows=[
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "rarity": 5,
                    "counter_key": "five_star",
                    "hard_pity": 80,
                    "soft_pity_start": None,
                    "soft_pity_increment_ppm": 0,
                    "resets_lower_rarity": True,
                },
                {
                    "banner_version_id": "11111111-1111-4111-8111-111111111111",
                    "rarity": 4,
                    "counter_key": "four_star",
                    "hard_pity": 10,
                    "soft_pity_start": None,
                    "soft_pity_increment_ppm": 0,
                    "resets_lower_rarity": False,
                },
            ],
        )

        banner_config = snapshot.banner_configs_by_id["banner-1"]

        self.assertEqual(snapshot.source, "postgres")
        self.assertEqual(banner_config.version, 7)
        self.assertEqual(banner_config.banner.featured_five_id, "char-up")
        self.assertEqual(banner_config.rarity_rates[5].base_rate, 1.0)


if __name__ == "__main__":
    unittest.main()
