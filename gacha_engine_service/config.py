"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from dataclasses import dataclass
import os

from . import __version__


@dataclass(frozen=True)
class Settings:
    """Application settings.

    Postgres stores authoritative pull and pity state. Kafka carries committed
    pull-completed events to downstream services.
    """

    app_name: str = "Gacha Engine Service"
    app_version: str = __version__
    host: str = "127.0.0.1"
    port: int = 8080
    internal_token: str = ""
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "gacha.pull_completed.v1"
    kafka_client_id: str = "gacha-engine-service"
    gacha_config_database_url: str = ""
    gacha_state_database_url: str = ""
    gacha_project_id: str = ""
    gacha_environment_id: str = ""
    gacha_config_cache_ttl_seconds: int = 30
    gacha_config_query_timeout_seconds: int = 5
    gacha_config_pool_size: int = 2
    gacha_state_query_timeout_seconds: int = 5
    gacha_state_pool_size: int = 8
    asset_service_url: str = "http://asset-service.asset-service.svc.cluster.local"
    asset_internal_token: str = ""
    asset_request_timeout_seconds: int = 5
    pending_event_recovery_enabled: bool = True
    pending_event_recovery_interval_seconds: int = 5
    pending_event_recovery_batch_size: int = 100
    pending_event_recovery_lock_ttl_seconds: int = 30

    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings from process environment variables."""

        config_database_url = os.getenv("GACHA_CONFIG_DATABASE_URL", "").strip()
        state_database_url = (
            os.getenv("GACHA_STATE_DATABASE_URL", "").strip() or config_database_url
        )

        return cls(
            host=os.getenv("HOST", cls.host).strip() or cls.host,
            port=_int_env("PORT", cls.port),
            internal_token=os.getenv("INTERNAL_TOKEN", "").strip(),
            kafka_bootstrap_servers=(
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers).strip()
                or cls.kafka_bootstrap_servers
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic).strip() or cls.kafka_topic,
            kafka_client_id=(
                os.getenv("KAFKA_CLIENT_ID", cls.kafka_client_id).strip()
                or cls.kafka_client_id
            ),
            gacha_config_database_url=config_database_url,
            gacha_state_database_url=state_database_url,
            gacha_project_id=os.getenv("GACHA_PROJECT_ID", "").strip(),
            gacha_environment_id=os.getenv("GACHA_ENVIRONMENT_ID", "").strip(),
            gacha_config_cache_ttl_seconds=_non_negative_int_env(
                "GACHA_CONFIG_CACHE_TTL_SECONDS",
                cls.gacha_config_cache_ttl_seconds,
            ),
            gacha_config_query_timeout_seconds=_int_env(
                "GACHA_CONFIG_QUERY_TIMEOUT_SECONDS",
                cls.gacha_config_query_timeout_seconds,
            ),
            gacha_config_pool_size=_int_env("GACHA_CONFIG_POOL_SIZE", cls.gacha_config_pool_size),
            gacha_state_query_timeout_seconds=_int_env(
                "GACHA_STATE_QUERY_TIMEOUT_SECONDS",
                cls.gacha_state_query_timeout_seconds,
            ),
            gacha_state_pool_size=_int_env(
                "GACHA_STATE_POOL_SIZE",
                cls.gacha_state_pool_size,
            ),
            asset_service_url=(
                os.getenv("ASSET_SERVICE_URL", cls.asset_service_url).strip()
                or cls.asset_service_url
            ),
            asset_internal_token=os.getenv("ASSET_INTERNAL_TOKEN", "").strip(),
            asset_request_timeout_seconds=_int_env(
                "ASSET_REQUEST_TIMEOUT_SECONDS",
                cls.asset_request_timeout_seconds,
            ),
            pending_event_recovery_enabled=_bool_env(
                "PENDING_EVENT_RECOVERY_ENABLED",
                cls.pending_event_recovery_enabled,
            ),
            pending_event_recovery_interval_seconds=_int_env(
                "PENDING_EVENT_RECOVERY_INTERVAL_SECONDS",
                cls.pending_event_recovery_interval_seconds,
            ),
            pending_event_recovery_batch_size=_int_env(
                "PENDING_EVENT_RECOVERY_BATCH_SIZE",
                cls.pending_event_recovery_batch_size,
            ),
            pending_event_recovery_lock_ttl_seconds=_int_env(
                "PENDING_EVENT_RECOVERY_LOCK_TTL_SECONDS",
                cls.pending_event_recovery_lock_ttl_seconds,
            ),
        )


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default

    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc

    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _non_negative_int_env(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default

    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc

    if parsed < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return parsed


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default

    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"{name} must be a boolean")
