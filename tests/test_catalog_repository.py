from __future__ import annotations

import unittest
from datetime import datetime, timezone

from gacha_engine_service.catalog_repository import (
    CatalogLoadError,
    PostgresCatalogRepository,
    _build_snapshot,
    _build_snapshot_from_release,
)


class FakeAcquireContext:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeConnection:
    def __init__(
        self,
        fetch_results: list[list[dict[str, object]]] | None = None,
        *,
        fetchrow_result: dict[str, object] | None = None,
    ) -> None:
        self._fetch_results = fetch_results or []
        self._fetchrow_result = fetchrow_result
        self.fetchrow_calls: list[tuple[object, ...]] = []

    async def fetch(self, *_: object, **__: object) -> list[dict[str, object]]:
        return self._fetch_results.pop(0)

    async def fetchrow(self, *args: object, **__: object) -> dict[str, object] | None:
        self.fetchrow_calls.append(args)
        return self._fetchrow_result


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    def acquire(self) -> FakeAcquireContext:
        return FakeAcquireContext(self._connection)


class CatalogRepositoryTests(unittest.TestCase):
    def test_build_snapshot_from_release_selects_only_effective_version(self) -> None:
        project_id = "b2000000-b2b2-4b2b-8b2b-b2b2b2b2b2b2"
        environment_id = "c3000000-c3c3-4c3c-8c3c-c3c3c3c3c3c3"
        active_version_id = "11111111-1111-4111-8111-111111111111"
        future_version_id = "22222222-2222-4222-8222-222222222222"
        release_row = {
            "release_id": "d4000000-d4d4-4d4d-8d4d-d4d4d4d4d4d4",
            "checksum_valid": True,
            "snapshot": {
                "schema_version": 1,
                "project_id": project_id,
                "environment_id": environment_id,
                "items": [
                    {
                        "id": "five",
                        "name": "Five",
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
                        "id": "four",
                        "name": "Four",
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
                "banners": [
                    {
                        "id": "banner",
                        "pity_group_id": "limited-character-shared",
                        "name": "Banner",
                        "short_name": "Banner",
                        "banner_type": "standard",
                        "description": "",
                        "theme": {
                            "primary": "#fff",
                            "secondary": "#000",
                            "glow": "rgba(0,0,0,0.2)",
                        },
                    }
                ],
                "banner_versions": [
                    {
                        "id": active_version_id,
                        "banner_id": "banner",
                        "rule_set_id": None,
                        "version": 1,
                        "status": "published",
                        "effective_from": "2026-07-01T00:00:00+00:00",
                        "effective_to": "2026-08-01T00:00:00+00:00",
                    },
                    {
                        "id": future_version_id,
                        "banner_id": "banner",
                        "rule_set_id": None,
                        "version": 2,
                        "status": "published",
                        "effective_from": "2026-09-01T00:00:00+00:00",
                        "effective_to": None,
                    },
                ],
                "banner_items": [
                    {
                        "banner_version_id": active_version_id,
                        "item_id": "five",
                        "featured_group": None,
                        "sort_order": 1,
                    },
                    {
                        "banner_version_id": active_version_id,
                        "item_id": "four",
                        "featured_group": None,
                        "sort_order": 2,
                    },
                    {
                        "banner_version_id": future_version_id,
                        "item_id": "five",
                        "featured_group": None,
                        "sort_order": 1,
                    },
                    {
                        "banner_version_id": future_version_id,
                        "item_id": "four",
                        "featured_group": None,
                        "sort_order": 2,
                    },
                ],
                "rarity_rates": [
                    {
                        "banner_version_id": active_version_id,
                        "rarity": 5,
                        "base_rate_ppm": 10000,
                        "roll_order": 1,
                    },
                    {
                        "banner_version_id": active_version_id,
                        "rarity": 4,
                        "base_rate_ppm": 90000,
                        "roll_order": 2,
                    },
                    {
                        "banner_version_id": future_version_id,
                        "rarity": 5,
                        "base_rate_ppm": 10000,
                        "roll_order": 1,
                    },
                    {
                        "banner_version_id": future_version_id,
                        "rarity": 4,
                        "base_rate_ppm": 90000,
                        "roll_order": 2,
                    },
                ],
                "featured_rules": [],
                "pity_rules": [
                    {
                        "banner_version_id": active_version_id,
                        "rarity": 5,
                        "counter_key": "five_star",
                        "hard_pity": 80,
                        "soft_pity_start": None,
                        "soft_pity_increment_ppm": 0,
                        "resets_lower_rarity": True,
                    },
                    {
                        "banner_version_id": active_version_id,
                        "rarity": 4,
                        "counter_key": "four_star",
                        "hard_pity": 10,
                        "soft_pity_start": None,
                        "soft_pity_increment_ppm": 0,
                        "resets_lower_rarity": False,
                    },
                    {
                        "banner_version_id": future_version_id,
                        "rarity": 5,
                        "counter_key": "five_star",
                        "hard_pity": 80,
                        "soft_pity_start": None,
                        "soft_pity_increment_ppm": 0,
                        "resets_lower_rarity": True,
                    },
                    {
                        "banner_version_id": future_version_id,
                        "rarity": 4,
                        "counter_key": "four_star",
                        "hard_pity": 10,
                        "soft_pity_start": None,
                        "soft_pity_increment_ppm": 0,
                        "resets_lower_rarity": False,
                    },
                ],
                "rule_sets": [],
                "rule_set_rarity_rates": [],
                "rule_set_featured_rules": [],
                "rule_set_pity_rules": [],
            },
        }

        snapshot = _build_snapshot_from_release(
            release_row,
            expected_project_id=project_id,
            expected_environment_id=environment_id,
            now=datetime(2026, 7, 22, tzinfo=timezone.utc),
        )

        self.assertEqual([banner.id for banner in snapshot.banners], ["banner"])
        self.assertEqual(snapshot.banner_configs_by_id["banner"].version, 1)
        self.assertEqual(
            snapshot.banner_configs_by_id["banner"].banner_version_id,
            active_version_id,
        )
        self.assertEqual(
            snapshot.banner_configs_by_id["banner"].pity_group_id,
            "limited-character-shared",
        )
        self.assertEqual(
            snapshot.banner_config_at(
                "banner",
                datetime(2026, 7, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
            ).version,
            1,
        )
        self.assertIsNone(
            snapshot.banner_config_at(
                "banner",
                datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        self.assertEqual(
            snapshot.banner_config_at(
                "banner",
                datetime(2026, 9, 1, tzinfo=timezone.utc),
            ).version,
            2,
        )

    def test_build_snapshot_from_release_rejects_invalid_checksum(self) -> None:
        with self.assertRaisesRegex(CatalogLoadError, "checksum"):
            _build_snapshot_from_release(
                {"checksum_valid": False, "snapshot": {}},
                expected_project_id="project",
                expected_environment_id="environment",
            )

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
        self.assertEqual(banner_config.pity_group_id, "banner-1")
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
    async def test_load_snapshot_reads_current_release_for_explicit_context(self) -> None:
        project_id = "b2000000-b2b2-4b2b-8b2b-b2b2b2b2b2b2"
        environment_id = "c3000000-c3c3-4c3c-8c3c-c3c3c3c3c3c3"
        connection = FakeConnection(
            fetchrow_result={
                "release_id": "d4000000-d4d4-4d4d-8d4d-d4d4d4d4d4d4",
                "checksum_valid": True,
                "snapshot": {
                    "schema_version": 1,
                    "project_id": project_id,
                    "environment_id": environment_id,
                    "items": [],
                    "banners": [],
                    "banner_versions": [],
                    "banner_items": [],
                    "rarity_rates": [],
                    "featured_rules": [],
                    "pity_rules": [],
                    "rule_sets": [],
                    "rule_set_rarity_rates": [],
                    "rule_set_featured_rules": [],
                    "rule_set_pity_rules": [],
                },
            }
        )
        repository = PostgresCatalogRepository(
            database_url="postgres://example",
            project_id=project_id,
            environment_id=environment_id,
            pool_size=1,
            query_timeout_seconds=1,
        )
        repository._pool = FakePool(connection)

        snapshot = await repository.load_snapshot()

        self.assertEqual(snapshot.items, ())
        self.assertEqual(snapshot.banner_configs_by_id, {})
        self.assertEqual(len(connection.fetchrow_calls), 1)
        self.assertEqual(connection.fetchrow_calls[0][1:], (project_id, environment_id))

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
            project_id="b2000000-b2b2-4b2b-8b2b-b2b2b2b2b2b2",
            environment_id="c3000000-c3c3-4c3c-8c3c-c3c3c3c3c3c3",
            pool_size=1,
            query_timeout_seconds=1,
        )
        repository._pool = FakePool(connection)

        snapshot = await repository.load_snapshot()

        self.assertEqual(snapshot.banners, ())
        self.assertEqual(snapshot.banner_configs_by_id, {})


if __name__ == "__main__":
    unittest.main()
