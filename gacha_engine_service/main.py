"""FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
import secrets
import uuid
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from .auth import authenticate_gateway_request, internal_token_header, user_id_header
from .catalog_repository import (
    CachedCatalogProvider,
    CatalogLoadError,
    CatalogRepository,
    PostgresCatalogRepository,
    StaticCatalogRepository,
)
from .config import Settings
from .engine import perform_pulls
from .kafka_events import EventPublishError, KafkaEventPublisher
from .redis_state import PityStateStoreError, PityVersionConflict, RedisPityStateStore
from .schemas import (
    ErrorResponse,
    HealthResponse,
    PitySnapshot,
    PullCompletedEvent,
    PullRequest,
    PullResponse,
    ReadyResponse,
)


class AppServices:
    """Container for external service adapters."""

    def __init__(self, state_store: object, event_publisher: object, catalog_provider: object) -> None:
        self.state_store = state_store
        self.event_publisher = event_publisher
        self.catalog_provider = catalog_provider


def create_app(
    *,
    settings: Settings | None = None,
    state_store: object | None = None,
    event_publisher: object | None = None,
    catalog_repository: CatalogRepository | None = None,
) -> FastAPI:
    """Create a FastAPI app with injectable external adapters."""

    settings = settings or Settings.from_env()
    owns_state_store = state_store is None
    owns_event_publisher = event_publisher is None

    if state_store is None:
        state_store = RedisPityStateStore(
            redis_url=settings.redis_url,
            key_prefix=settings.redis_key_prefix,
        )

    if event_publisher is None:
        event_publisher = KafkaEventPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            client_id=settings.kafka_client_id,
        )

    catalog_provider = CachedCatalogProvider(
        catalog_repository or create_catalog_repository(settings),
        ttl_seconds=settings.gacha_config_cache_ttl_seconds,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await catalog_provider.close()
            if owns_event_publisher and hasattr(event_publisher, "close"):
                await event_publisher.close()
            if owns_state_store and hasattr(state_store, "close"):
                await state_store.close()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="无数据库抽卡引擎：Redis 保存保底快照，Kafka 发布抽卡完成事件。",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.services = AppServices(state_store, event_publisher, catalog_provider)

    register_exception_handlers(app)
    register_routes(app)
    return app


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
        if isinstance(exc.detail, dict) and "code" in exc.detail:
            return write_error(exc.status_code, exc.detail["code"], exc.detail["message"])

        return write_error(exc.status_code, "request_error", str(exc.detail))


def register_routes(app: FastAPI) -> None:
    @app.get("/health", response_model=HealthResponse, tags=["health"])
    async def health_check() -> HealthResponse:
        return HealthResponse()

    @app.get("/ready", response_model=ReadyResponse, tags=["health"])
    async def readiness_check(request: Request) -> JSONResponse | ReadyResponse:
        settings: Settings = request.app.state.settings
        services: AppServices = request.app.state.services
        checks: dict[str, str] = {}

        if not settings.internal_token:
            checks["internal_token"] = "missing"

        try:
            await services.state_store.ping()
        except Exception:
            checks["redis"] = "unavailable"

        try:
            await services.event_publisher.ping()
        except Exception:
            checks["kafka"] = "unavailable"

        try:
            await services.catalog_provider.ping()
        except Exception:
            checks["gacha_config"] = "unavailable"

        if checks:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=ReadyResponse(status="not_ready", checks=checks).model_dump(mode="json"),
            )

        return ReadyResponse(status="ready")

    @app.get("/v1/banners", tags=["catalog"])
    async def list_banners(request: Request) -> dict[str, object]:
        snapshot = await current_catalog_snapshot(request)
        return {"items": [banner.model_dump(mode="json") for banner in snapshot.banners]}

    @app.get("/v1/items", tags=["catalog"])
    async def list_items(request: Request) -> dict[str, object]:
        snapshot = await current_catalog_snapshot(request)
        return {"items": [item.model_dump(mode="json") for item in snapshot.items]}

    @app.get("/v1/me/pity", response_model=PitySnapshot, tags=["gacha"])
    async def get_pity(
        request: Request,
        banner_id: str,
        user_id: UUID = Depends(current_user_id),
    ) -> PitySnapshot:
        snapshot = await current_catalog_snapshot(request)
        if banner_id not in snapshot.banner_configs_by_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "banner_not_found", "message": "banner was not found"},
            )

        services: AppServices = request.app.state.services
        try:
            return await services.state_store.get_snapshot(user_id, banner_id)
        except PityStateStoreError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "redis_unavailable", "message": "pity state is unavailable"},
            ) from exc

    @app.post("/v1/me/pulls", response_model=PullResponse, tags=["gacha"])
    async def create_pull(
        pull_request: PullRequest,
        request: Request,
        user_id: UUID = Depends(current_user_id),
    ) -> PullResponse:
        snapshot = await current_catalog_snapshot(request)
        banner_config = snapshot.banner_configs_by_id.get(pull_request.banner_id)
        if banner_config is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "banner_not_found", "message": "banner was not found"},
            )
        banner = banner_config.banner

        services: AppServices = request.app.state.services
        seed = pull_request.seed or secrets.token_urlsafe(24)

        try:
            previous_pity = await services.state_store.get_snapshot(user_id, banner.id)
            records, next_pity_state = perform_pulls(
                banner_config=banner_config,
                count=pull_request.count,
                pity=previous_pity.without_version(),
                seed=seed,
            )
            next_pity = await services.state_store.compare_and_set(
                user_id=user_id,
                banner_id=banner.id,
                expected_version=previous_pity.version,
                next_pity=next_pity_state,
            )
        except PityVersionConflict as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "pity_version_conflict",
                    "message": f"pity state was updated at version {exc.current_version}",
                },
            ) from exc
        except PityStateStoreError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "redis_unavailable", "message": "pity state is unavailable"},
            ) from exc

        event_id = str(uuid.uuid4())
        response = PullResponse(
            event_id=event_id,
            banner_version_id=banner_config.banner_version_id,
            seed=seed,
            records=records,
            previous_pity=previous_pity,
            next_pity=next_pity,
            state_version=next_pity.version,
        )
        event = PullCompletedEvent(
            event_id=event_id,
            user_id=str(user_id),
            banner_id=banner.id,
            banner_version_id=banner_config.banner_version_id,
            seed=seed,
            records=records,
            previous_pity=previous_pity,
            next_pity=next_pity,
            state_version=next_pity.version,
        )

        try:
            await services.event_publisher.publish_pull_completed(event)
        except EventPublishError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "kafka_unavailable",
                    "message": "pull event could not be published",
                },
            ) from exc

        return response


def create_catalog_repository(settings: Settings) -> CatalogRepository:
    if not settings.gacha_config_database_url:
        return StaticCatalogRepository()

    return PostgresCatalogRepository(
        database_url=settings.gacha_config_database_url,
        pool_size=settings.gacha_config_pool_size,
        query_timeout_seconds=settings.gacha_config_query_timeout_seconds,
    )


async def current_catalog_snapshot(request: Request):
    services: AppServices = request.app.state.services
    try:
        return await services.catalog_provider.get_snapshot()
    except CatalogLoadError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "gacha_config_unavailable", "message": "gacha config is unavailable"},
        ) from exc


def current_user_id(
    request: Request,
    internal_token: str | None = Depends(internal_token_header),
    user_id: str | None = Depends(user_id_header),
) -> UUID:
    settings: Settings = request.app.state.settings
    return authenticate_gateway_request(
        settings=settings,
        internal_token=internal_token,
        user_id=user_id,
    )


def write_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(error={"code": code, "message": message}).model_dump(mode="json"),
    )


app = create_app()
