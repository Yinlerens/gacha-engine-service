from __future__ import annotations

import unittest

from gacha_engine_service.redis_state import COMPARE_AND_SET_LUA, PityVersionConflict
from gacha_engine_service.schemas import PityState

from .fakes import FakePityStateStore


class RedisStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_fake_store_compare_and_set_updates_version(self) -> None:
        store = FakePityStateStore()

        next_snapshot = await store.compare_and_set(
            user_id="ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e",
            banner_id="limited-character-001",
            expected_version=0,
            next_pity=PityState(since_five=1, since_four=1),
        )

        self.assertEqual(next_snapshot.version, 1)
        self.assertEqual(next_snapshot.since_five, 1)

    async def test_fake_store_detects_version_conflict(self) -> None:
        store = FakePityStateStore()

        with self.assertRaises(PityVersionConflict):
            await store.compare_and_set(
                user_id="ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e",
                banner_id="limited-character-001",
                expected_version=2,
                next_pity=PityState(since_five=1, since_four=1),
            )

    def test_lua_script_uses_version_compare_and_set(self) -> None:
        self.assertIn("current_version ~= expected_version", COMPARE_AND_SET_LUA)
        self.assertIn('redis.call("SET"', COMPARE_AND_SET_LUA)


if __name__ == "__main__":
    unittest.main()

