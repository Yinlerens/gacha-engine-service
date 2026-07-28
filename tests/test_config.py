from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from gacha_engine_service.config import Settings


class SettingsTests(unittest.TestCase):
    def test_state_database_defaults_to_the_config_database(self) -> None:
        with patch.dict(
            os.environ,
            {"GACHA_CONFIG_DATABASE_URL": "postgresql://config-and-state"},
            clear=True,
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.gacha_state_database_url, "postgresql://config-and-state")

    def test_explicit_state_database_can_be_isolated(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GACHA_CONFIG_DATABASE_URL": "postgresql://config",
                "GACHA_STATE_DATABASE_URL": "postgresql://state",
            },
            clear=True,
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.gacha_state_database_url, "postgresql://state")

    def test_from_env_loads_explicit_gacha_release_context(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GACHA_PROJECT_ID": "b2000000-b2b2-4b2b-8b2b-b2b2b2b2b2b2",
                "GACHA_ENVIRONMENT_ID": "c3000000-c3c3-4c3c-8c3c-c3c3c3c3c3c3",
            },
            clear=True,
        ):
            settings = Settings.from_env()

        self.assertEqual(
            settings.gacha_project_id,
            "b2000000-b2b2-4b2b-8b2b-b2b2b2b2b2b2",
        )
        self.assertEqual(
            settings.gacha_environment_id,
            "c3000000-c3c3-4c3c-8c3c-c3c3c3c3c3c3",
        )


if __name__ == "__main__":
    unittest.main()
