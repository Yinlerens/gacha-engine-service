from __future__ import annotations

import os
from pathlib import Path
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

    def test_from_env_records_the_deployed_engine_build(self) -> None:
        with patch.dict(
            os.environ,
            {"GACHA_ENGINE_BUILD_SHA": "a" * 40},
            clear=True,
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.engine_build_sha, "a" * 40)

    def test_from_env_loads_backpack_receipt_dependency(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BACKPACK_SERVICE_URL": "http://backpack.internal",
                "BACKPACK_INTERNAL_TOKEN": "backpack-token",
                "BACKPACK_REQUEST_TIMEOUT_SECONDS": "7",
            },
            clear=True,
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.backpack_service_url, "http://backpack.internal")
        self.assertEqual(settings.backpack_internal_token, "backpack-token")
        self.assertEqual(settings.backpack_request_timeout_seconds, 7)

    def test_container_build_embeds_the_source_revision_without_ci_loop(self) -> None:
        root = Path(__file__).parents[1]
        dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
        workflow = (
            root / ".github" / "workflows" / "container-image.yaml"
        ).read_text(encoding="utf-8")

        self.assertIn("ARG GACHA_ENGINE_BUILD_SHA", dockerfile)
        self.assertIn("GACHA_ENGINE_BUILD_SHA=${{ github.sha }}", workflow)
        self.assertIn("chore: update image digest [skip ci]", workflow)


if __name__ == "__main__":
    unittest.main()
