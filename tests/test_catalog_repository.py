from __future__ import annotations

import unittest

from gacha_engine_service.catalog_repository import PostgresCatalogRepository, _build_snapshot


class FakeAcquireContext:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeConnection:
    def __init__(self, fetch_results: list[list[dict[str, object]]]) -> None:
        self._fetch_results = fetch_results

    async def fetch(self, *_: object, **__: object) -> list[dict[str, object]]:
        return self._fetch_results.pop(0)


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    def acquire(self) -> FakeAcquireContext:
        return FakeAcquireContext(self._connection)


class CatalogRepositoryTests(unittest.TestCase):
    def test_build_snapshot_allows_empty_catalog(self) -> None:
        snapshot = _build_snapshot(
            item_rows=[],
            banner_rows=[],
            banner_item_rows=[],
            rarity_rows=[],
            featured_rows=[],
            pity_rows=[],
        )

        self.assertEqual(snapshot.items, ())
        self.assertEqual(snapshot.banners, ())
        self.assertEqual(snapshot.banner_configs_by_id, {})

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

    def test_build_snapshot_uses_rule_set_rates_and_pity_when_version_rules_are_empty(self) -> None:
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
                    "banner_version_id": "22222222-2222-4222-8222-222222222222",
                    "banner_id": "banner-with-rule-set",
                    "rule_set_id": "default-limited-character",
                    "version": 1,
                    "name": "Rule Set Banner",
                    "short_name": "RuleSet",
                    "banner_type": "limited-character",
                    "description": "Loaded from shared rule set",
                    "theme": {"primary": "#fff", "secondary": "#000", "glow": "rgba(0,0,0,0.2)"},
                }
            ],
            banner_item_rows=[
                {
                    "banner_version_id": "22222222-2222-4222-8222-222222222222",
                    "item_id": "char-up",
                    "featured_group": "five_up",
                    "sort_order": 1,
                },
                {
                    "banner_version_id": "22222222-2222-4222-8222-222222222222",
                    "item_id": "weapon-base",
                    "featured_group": None,
                    "sort_order": 2,
                },
            ],
            rarity_rows=[],
            featured_rows=[],
            pity_rows=[],
            rule_set_rarity_rows=[
                {
                    "rule_set_id": "default-limited-character",
                    "rarity": 5,
                    "base_rate_ppm": 6000,
                    "roll_order": 1,
                },
                {
                    "rule_set_id": "default-limited-character",
                    "rarity": 4,
                    "base_rate_ppm": 51000,
                    "roll_order": 2,
                },
                {
                    "rule_set_id": "default-limited-character",
                    "rarity": 3,
                    "base_rate_ppm": 943000,
                    "roll_order": 3,
                },
            ],
            rule_set_featured_rows=[
                {
                    "rule_set_id": "default-limited-character",
                    "rarity": 5,
                    "featured_group": "five_up",
                    "featured_rate_ppm": 500000,
                    "guarantee_after_miss": True,
                    "miss_sets_guarantee": True,
                    "guarantee_state_key": "guaranteed_featured_five",
                }
            ],
            rule_set_pity_rows=[
                {
                    "rule_set_id": "default-limited-character",
                    "rarity": 5,
                    "counter_key": "five_star",
                    "hard_pity": 80,
                    "soft_pity_start": 66,
                    "soft_pity_increment_ppm": 60000,
                    "resets_lower_rarity": True,
                },
                {
                    "rule_set_id": "default-limited-character",
                    "rarity": 4,
                    "counter_key": "four_star",
                    "hard_pity": 10,
                    "soft_pity_start": None,
                    "soft_pity_increment_ppm": 0,
                    "resets_lower_rarity": False,
                },
            ],
        )

        banner_config = snapshot.banner_configs_by_id["banner-with-rule-set"]

        self.assertEqual(banner_config.rarity_rates[5].base_rate_ppm, 6000)
        self.assertEqual(banner_config.rarity_rates[4].base_rate_ppm, 51000)
        self.assertEqual(banner_config.pity_rules[5].hard_pity, 80)
        self.assertEqual(banner_config.pity_rules[5].soft_pity_start, 66)
        self.assertEqual(banner_config.featured_rules[5].featured_rate_ppm, 500000)


class PostgresCatalogRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_snapshot_allows_empty_current_catalog(self) -> None:
        connection = FakeConnection(
            [
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
            ]
        )
        repository = PostgresCatalogRepository(
            database_url="postgres://example",
            pool_size=1,
            query_timeout_seconds=1,
        )
        repository._pool = FakePool(connection)

        snapshot = await repository.load_snapshot()

        self.assertEqual(snapshot.banners, ())
        self.assertEqual(snapshot.banner_configs_by_id, {})


if __name__ == "__main__":
    unittest.main()
