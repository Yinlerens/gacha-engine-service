"""FastAPI application entry point."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import logging
import time
import uuid
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from .asset_client import AssetClient, AssetServiceError
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
from .pull_operations import PullOperation
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


LOGGER = logging.getLogger(__name__)
ASTRITE_PER_PULL = 160
IDEMPOTENCY_HEADER = "Idempotency-Key"
REQUEST_ID_HEADER = "X-Request-Id"
MAX_IDEMPOTENCY_KEY_LENGTH = 128
PULL_EVENT_NAMESPACE = uuid.UUID("d19f2db2-5d10-4dbf-95ee-2680ab82d908")


class AppServices:
    """Container for external service adapters."""

    def __init__(
        self,
        state_store: object,
        event_publisher: object,
        catalog_provider: object,
        asset_client: object,
    ) -> None:
        self.state_store = state_store
        self.event_publisher = event_publisher
        self.catalog_provider = catalog_provider
        self.asset_client = asset_client


def create_app(
    *,
    settings: Settings | None = None,
    state_store: object | None = None,
    event_publisher: object | None = None,
    catalog_repository: CatalogRepository | None = None,
    asset_client: object | None = None,
) -> FastAPI:
    """Create a FastAPI app with injectable external adapters."""

    settings = settings or Settings.from_env()
    owns_state_store = state_store is None
    owns_event_publisher = event_publisher is None
    owns_asset_client = asset_client is None

    if state_store is None:
        state_store = RedisPityStateStore(
            redis_url=settings.redis_url,
            key_prefix=settings.redis_key_prefix,
            pull_operation_ttl_seconds=settings.pull_operation_ttl_seconds,
        )

    if event_publisher is None:
        event_publisher = KafkaEventPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            client_id=settings.kafka_client_id,
        )

    if asset_client is None:
        asset_client = AssetClient(
            base_url=settings.asset_service_url,
            internal_token=settings.asset_internal_token or settings.internal_token,
            timeout_seconds=settings.asset_request_timeout_seconds,
        )

    catalog_provider = CachedCatalogProvider(
        catalog_repository or create_catalog_repository(settings),
        ttl_seconds=settings.gacha_config_cache_ttl_seconds,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        recovery_task: asyncio.Task[None] | None = None
        if settings.pending_event_recovery_enabled:
            recovery_task = asyncio.create_task(
                run_pending_event_recovery_worker(
                    services=AppServices(
                        state_store,
                        event_publisher,
                        catalog_provider,
                        asset_client,
                    ),
                    interval_seconds=settings.pending_event_recovery_interval_seconds,
                    batch_size=settings.pending_event_recovery_batch_size,
                    lock_ttl_seconds=settings.pending_event_recovery_lock_ttl_seconds,
                ),
                name="pending-event-recovery",
            )
        try:
            yield
        finally:
            if recovery_task is not None:
                recovery_task.cancel()
                try:
                    await recovery_task
                except asyncio.CancelledError:
                    pass
            await catalog_provider.close()
            if owns_asset_client and hasattr(asset_client, "close"):
                await asset_client.close()
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
    app.state.services = AppServices(state_store, event_publisher, catalog_provider, asset_client)

    register_access_log(app)
    register_exception_handlers(app)
    register_routes(app)
    return app


def register_access_log(app: FastAPI) -> None:
    @app.middleware("http")
    async def access_log_middleware(request: Request, call_next):
        started = time.monotonic()
        request_id = request_id_from_header(request.headers.get(REQUEST_ID_HEADER))
        request.state.request_id = request_id

        try:
            response = await call_next(request)
        except Exception:
            duration_ms = int((time.monotonic() - started) * 1000)
            LOGGER.exception(
                "http request failed request_id=%s method=%s path=%s duration_ms=%s client_ip=%s",
                request_id,
                request.method,
                request.url.path,
                duration_ms,
                request.client.host if request.client else "",
            )
            raise

        response.headers[REQUEST_ID_HEADER] = request_id
        duration_ms = int((time.monotonic() - started) * 1000)
        LOGGER.info(
            "http request request_id=%s method=%s path=%s status=%s duration_ms=%s client_ip=%s",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            request.client.host if request.client else "",
        )
        return response


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
            await services.asset_client.ping()
        except Exception:
            checks["asset_service"] = "unavailable"

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
        idempotency_key: str = Depends(idempotency_key_header),
    ) -> PullResponse:
        request_id = request_id_from_state(request)
        snapshot = await current_catalog_snapshot(request)
        banner_config = snapshot.banner_configs_by_id.get(pull_request.banner_id)
        if banner_config is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "banner_not_found", "message": "banner was not found"},
            )
        banner = banner_config.banner

        services: AppServices = request.app.state.services
        seed = pull_request.seed or deterministic_pull_seed(user_id, idempotency_key)
        request_hash = pull_request_hash(
            banner_id=banner.id,
            count=pull_request.count,
            seed=seed,
        )
        cost_minor = pull_request.count * ASTRITE_PER_PULL
        event_id = deterministic_pull_event_id(user_id, idempotency_key)

        try:
            existing_operation = await services.state_store.begin_pull_operation(
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
        except PityStateStoreError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "redis_unavailable", "message": "pull idempotency is unavailable"},
            ) from exc

        if existing_operation is not None:
            return await handle_existing_pull_operation(
                operation=existing_operation,
                services=services,
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                event_id=event_id,
                amount_minor=cost_minor,
                request_id=request_id,
                metadata=pull_asset_metadata(
                    event_id=event_id,
                    banner_id=banner.id,
                    banner_version_id=banner_config.banner_version_id,
                    count=pull_request.count,
                ),
            )

        metadata = pull_asset_metadata(
            event_id=event_id,
            banner_id=banner.id,
            banner_version_id=banner_config.banner_version_id,
            count=pull_request.count,
        )

        try:
            await services.asset_client.spend(
                user_id=user_id,
                amount_minor=cost_minor,
                idempotency_key=spend_idempotency_key(event_id),
                reason="gacha_pull",
                metadata=metadata,
                request_id=request_id,
            )
        except AssetServiceError as exc:
            LOGGER.warning(
                "asset spend failed request_id=%s status=%s code=%s",
                request_id,
                exc.status_code,
                exc.code,
            )
            await mark_pull_operation_failed(
                services=services,
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                code=asset_error_code(exc),
                message=asset_error_message(exc),
            )
            raise asset_http_exception(exc) from exc

        try:
            previous_pity = await services.state_store.get_snapshot(user_id, banner.id)
            records, next_pity_state = perform_pulls(
                banner_config=banner_config,
                count=pull_request.count,
                pity=previous_pity.without_version(),
                seed=seed,
            )
            next_pity = PitySnapshot(
                **next_pity_state.model_dump(),
                version=previous_pity.version + 1,
            )
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
            pending_operation = PullOperation(
                status="event_pending",
                request_hash=request_hash,
                response=response,
                event=event,
            )
            await services.state_store.compare_and_set_with_pull_operation(
                user_id=user_id,
                banner_id=banner.id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                expected_version=previous_pity.version,
                next_pity=next_pity_state,
                operation=pending_operation,
            )
        except PityVersionConflict as exc:
            await refund_spend(
                services=services,
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                event_id=event_id,
                amount_minor=cost_minor,
                metadata=metadata,
                request_id=request_id,
                code="pity_version_conflict",
                message=f"pity state was updated at version {exc.current_version}",
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "pity_version_conflict",
                    "message": f"pity state was updated at version {exc.current_version}",
                },
            ) from exc
        except PityStateStoreError as exc:
            await refund_spend(
                services=services,
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                event_id=event_id,
                amount_minor=cost_minor,
                metadata=metadata,
                request_id=request_id,
                code="redis_unavailable",
                message="pity state is unavailable",
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "redis_unavailable", "message": "pity state is unavailable"},
            ) from exc

        try:
            await publish_pull_completed_with_retry(services.event_publisher, event)
        except EventPublishError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "kafka_unavailable",
                    "message": "pull event could not be published",
                },
            ) from exc

        await save_pull_operation_best_effort(
            services=services,
            user_id=user_id,
            idempotency_key=idempotency_key,
            operation=PullOperation(
                status="succeeded",
                request_hash=request_hash,
                response=response,
                event=event,
            ),
        )

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


def idempotency_key_header(
    value: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
) -> str:
    idempotency_key = (value or "").strip()
    if not idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "missing_idempotency_key", "message": "Idempotency-Key header is required"},
        )
    if len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "invalid_idempotency_key",
                "message": "Idempotency-Key must be 128 characters or fewer",
            },
        )
    return idempotency_key


async def handle_existing_pull_operation(
    *,
    operation: PullOperation,
    services: AppServices,
    user_id: UUID,
    idempotency_key: str,
    request_hash: str,
    event_id: str,
    amount_minor: int,
    request_id: str,
    metadata: dict[str, object],
) -> PullResponse:
    if operation.request_hash != request_hash:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "idempotency_conflict",
                "message": "Idempotency-Key was already used with a different request",
            },
        )

    if operation.status == "succeeded" and operation.response is not None:
        return operation.response

    if operation.status == "event_pending" and operation.response is not None and operation.event is not None:
        try:
            await publish_pull_completed_with_retry(services.event_publisher, operation.event)
        except EventPublishError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"code": "kafka_unavailable", "message": "pull event could not be published"},
            ) from exc

        await save_pull_operation_best_effort(
            services=services,
            user_id=user_id,
            idempotency_key=idempotency_key,
            operation=PullOperation(
                status="succeeded",
                request_hash=request_hash,
                response=operation.response,
                event=operation.event,
            ),
        )
        return operation.response

    if operation.status == "refund_pending":
        await refund_spend(
            services=services,
            user_id=user_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            event_id=event_id,
            amount_minor=amount_minor,
            request_id=request_id,
            metadata=metadata,
            code=operation.error_code or "pull_refunded",
            message=operation.error_message or "pull was refunded",
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "pull_refunded",
                "message": "previous pull attempt was refunded; retry with a new Idempotency-Key",
            },
        )

    if operation.status == "failed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": operation.error_code or "pull_failed",
                "message": operation.error_message or "previous pull attempt failed; retry with a new Idempotency-Key",
            },
        )

    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={"code": "pull_in_progress", "message": "pull is already in progress"},
    )


async def refund_spend(
    *,
    services: AppServices,
    user_id: UUID,
    idempotency_key: str,
    request_hash: str,
    event_id: str,
    amount_minor: int,
    metadata: dict[str, object],
    request_id: str,
    code: str,
    message: str,
) -> None:
    try:
        await services.asset_client.credit(
            user_id=user_id,
            amount_minor=amount_minor,
            idempotency_key=refund_idempotency_key(event_id),
            reason="gacha_pull_refund",
            metadata={**metadata, "refund_reason": code},
            request_id=request_id,
        )
    except AssetServiceError as exc:
        LOGGER.warning(
            "asset refund failed request_id=%s status=%s code=%s",
            request_id,
            exc.status_code,
            exc.code,
        )
        await save_pull_operation_best_effort(
            services=services,
            user_id=user_id,
            idempotency_key=idempotency_key,
            operation=PullOperation(
                status="refund_pending",
                request_hash=request_hash,
                error_code=code,
                error_message=message,
            ),
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "asset_refund_unavailable",
                "message": "asset refund could not be completed",
            },
        ) from exc

    await mark_pull_operation_failed(
        services=services,
        user_id=user_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        code=code,
        message=message,
    )


async def mark_pull_operation_failed(
    *,
    services: AppServices,
    user_id: UUID,
    idempotency_key: str,
    request_hash: str,
    code: str,
    message: str,
) -> None:
    await save_pull_operation_best_effort(
        services=services,
        user_id=user_id,
        idempotency_key=idempotency_key,
        operation=PullOperation(
            status="failed",
            request_hash=request_hash,
            error_code=code,
            error_message=message,
        ),
    )


async def save_pull_operation_best_effort(
    *,
    services: AppServices,
    user_id: UUID,
    idempotency_key: str,
    operation: PullOperation,
) -> None:
    try:
        await services.state_store.save_pull_operation(
            user_id=user_id,
            idempotency_key=idempotency_key,
            operation=operation,
        )
    except PityStateStoreError:
        LOGGER.warning("failed to save pull operation status", exc_info=True)


async def run_pending_event_recovery_worker(
    *,
    services: AppServices,
    interval_seconds: int,
    batch_size: int,
    lock_ttl_seconds: int,
) -> None:
    while True:
        await asyncio.sleep(max(1, interval_seconds))
        try:
            recovered_count = await recover_pending_pull_events_once(
                services,
                limit=batch_size,
                lock_ttl_seconds=lock_ttl_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("pending pull event recovery worker failed")
            continue

        if recovered_count:
            LOGGER.info("recovered pending pull events count=%s", recovered_count)


async def recover_pending_pull_events_once(
    services: AppServices,
    *,
    limit: int,
    lock_ttl_seconds: int,
) -> int:
    try:
        pending_operations = await services.state_store.iter_event_pending_pull_operations(
            limit=limit,
        )
    except PityStateStoreError:
        LOGGER.warning("failed to scan pending pull operations", exc_info=True)
        return 0

    recovered_count = 0
    for record in pending_operations:
        operation = record.operation
        if (
            operation.status != "event_pending"
            or operation.response is None
            or operation.event is None
        ):
            continue

        event_id = operation.event.event_id
        try:
            claimed = await services.state_store.claim_pull_operation_recovery(
                operation_key=record.operation_key,
                lock_ttl_seconds=lock_ttl_seconds,
            )
        except PityStateStoreError:
            LOGGER.warning(
                "failed to claim pending pull event recovery event_id=%s",
                event_id,
                exc_info=True,
            )
            continue

        if not claimed:
            continue

        try:
            await publish_pull_completed_with_retry(services.event_publisher, operation.event)
            await services.state_store.save_pull_operation_by_key(
                operation_key=record.operation_key,
                operation=PullOperation(
                    status="succeeded",
                    request_hash=operation.request_hash,
                    response=operation.response,
                    event=operation.event,
                ),
            )
            recovered_count += 1
        except EventPublishError:
            LOGGER.warning(
                "failed to recover pending pull event event_id=%s",
                event_id,
                exc_info=True,
            )
        except PityStateStoreError:
            LOGGER.warning(
                "failed to mark recovered pull operation succeeded event_id=%s",
                event_id,
                exc_info=True,
            )
        except Exception:
            LOGGER.exception("unexpected pending pull event recovery failure event_id=%s", event_id)
        finally:
            try:
                await services.state_store.release_pull_operation_recovery(
                    operation_key=record.operation_key,
                )
            except PityStateStoreError:
                LOGGER.warning(
                    "failed to release pending pull event recovery lock event_id=%s",
                    event_id,
                    exc_info=True,
                )

    return recovered_count


async def publish_pull_completed_with_retry(event_publisher: object, event: PullCompletedEvent) -> None:
    last_error: EventPublishError | None = None
    for attempt in range(3):
        try:
            await event_publisher.publish_pull_completed(event)
            return
        except EventPublishError as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(0.2 * (2**attempt))

    if last_error is not None:
        raise last_error


def asset_http_exception(exc: AssetServiceError) -> HTTPException:
    if exc.code == "insufficient_funds":
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "insufficient_assets", "message": "星声不足，请先充值。"},
        )
    if exc.code == "idempotency_conflict":
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "idempotency_conflict",
                "message": "Idempotency-Key was already used with a different request",
            },
        )

    if exc.status_code >= 500:
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "asset_unavailable", "message": "asset service is unavailable"},
        )

    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={"code": asset_error_code(exc), "message": asset_error_message(exc)},
    )


def asset_error_code(exc: AssetServiceError) -> str:
    if exc.code == "insufficient_funds":
        return "insufficient_assets"
    return exc.code or "asset_request_failed"


def asset_error_message(exc: AssetServiceError) -> str:
    if exc.code == "insufficient_funds":
        return "星声不足，请先充值。"
    return exc.message or "asset request failed"


def pull_asset_metadata(
    *,
    event_id: str,
    banner_id: str,
    banner_version_id: str | None,
    count: int,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "source": "gacha-engine-service",
        "event_id": event_id,
        "banner_id": banner_id,
        "count": count,
        "cost_per_pull_minor": ASTRITE_PER_PULL,
    }
    if banner_version_id is not None:
        metadata["banner_version_id"] = banner_version_id
    return metadata


def pull_request_hash(*, banner_id: str, count: int, seed: str) -> str:
    payload = json.dumps(
        {
            "banner_id": banner_id,
            "count": count,
            "seed": seed,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def deterministic_pull_event_id(user_id: UUID, idempotency_key: str) -> str:
    return str(uuid.uuid5(PULL_EVENT_NAMESPACE, f"event:{user_id}:{idempotency_key}"))


def deterministic_pull_seed(user_id: UUID, idempotency_key: str) -> str:
    return str(uuid.uuid5(PULL_EVENT_NAMESPACE, f"seed:{user_id}:{idempotency_key}"))


def request_id_from_header(value: str | None) -> str:
    if value:
        try:
            return str(uuid.UUID(value.strip()))
        except ValueError:
            pass
    return str(uuid.uuid4())


def request_id_from_state(request: Request) -> str:
    value = getattr(request.state, "request_id", "")
    return str(value) if value else ""


def spend_idempotency_key(event_id: str) -> str:
    return f"gacha-pull:{event_id}"


def refund_idempotency_key(event_id: str) -> str:
    return f"gacha-refund:{event_id}"


def write_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(error={"code": code, "message": message}).model_dump(mode="json"),
    )


app = create_app()
