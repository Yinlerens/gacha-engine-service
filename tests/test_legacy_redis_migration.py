from __future__ import annotations

import unittest
from uuid import UUID

from scripts.migrate_redis_state import parse_legacy_key


USER_ID = UUID("ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e")


class LegacyRedisMigrationTests(unittest.TestCase):
    def test_parses_pity_snapshot_key(self) -> None:
        self.assertEqual(
            parse_legacy_key(
                "gacha:pity",
                f"gacha:pity:{USER_ID}:limited-character-001",
            ),
            ("pity", USER_ID, "limited-character-001"),
        )

    def test_parses_hashed_pull_operation_key(self) -> None:
        digest = "a" * 64

        self.assertEqual(
            parse_legacy_key(
                "gacha:pity",
                f"gacha:pity:pull-operation:{USER_ID}:{digest}",
            ),
            ("operation", USER_ID, digest),
        )

    def test_ignores_recovery_locks_and_unknown_keys(self) -> None:
        digest = "a" * 64

        self.assertIsNone(
            parse_legacy_key(
                "gacha:pity",
                f"gacha:pity:pull-operation:{USER_ID}:{digest}:recovery-lock",
            )
        )
        self.assertIsNone(parse_legacy_key("gacha:pity", "other:key"))


if __name__ == "__main__":
    unittest.main()
