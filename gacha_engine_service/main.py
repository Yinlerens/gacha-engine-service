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
from .postgres_state import PostgresGachaStateStore
from .pull_operations import PullOperation, PullRecoveryContext
from .schemas import (
    ApiError,
    ErrorResponse,
    HealthResponse,
    PitySnapshot,
    PullCompletedEvent,
    PullOperationStateResponse,
    PullRequest,
    PullResponse,
    ReadyResponse,
)
from .state_store import (
    GachaStateStoreError,
    PityVersionConflict,
    PullOperationOwnershipLost,
)


LOGGER = logging.getLogger(__name__)
ASTRITE_PER_PULL = 160
IDEMPOTENCY_HEADER = "Idempotency-Key"
REQUEST_ID_HEADER = "X-Request-Id"
MAX_IDEMPOTENCY_KEY_LENGTH = 128
PULL_PROCESSING_LEASE_SECONDS = 30
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
        state_store = PostgresGachaStateStore(
            database_url=settings.gacha_state_database_url,
            pool_size=settings.gacha_state_pool_size,
            query_timeout_seconds=settings.gacha_state_query_timeout_seconds,
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
                run_pull_recovery_worker(
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
                name="pull-recovery",
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
        description="Postgres 持久化抽卡与保底状态，Kafka 发布已提交的抽卡事件。",
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
            checks["state_database"] = "unavailable"

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
        except GachaStateStoreError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "state_store_unavailable",
                    "message": "pity state is unavailable",
                },
            ) from exc

    @app.get(
        "/v1/me/pulls/operation",
        response_model=PullOperationStateResponse,
        tags=["gacha"],
    )
    async def get_pull_operation(
        request: Request,
        user_id: UUID = Depends(current_user_id),
        idempotency_key: str = Depends(idempotency_key_header),
    ) -> PullOperationStateResponse:
        services: AppServices = request.app.state.services
        try:
            operation = await services.state_store.get_pull_operation(
                user_id=user_id,
                idempotency_key=idempotency_key,
            )
        except GachaStateStoreError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "state_store_unavailable",
                    "message": "pull operation state is unavailable",
                },
            ) from exc

        if operation is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "code": "pull_operation_not_found",
                    "message": "pull operation was not found",
                },
            )

        operation_error = None
        if operation.error_code or operation.error_message:
            operation_error = ApiError(
                code=operation.error_code or "pull_failed",
                message=operation.error_message or "pull operation failed",
            )

        return PullOperationStateResponse(
            status=operation.status,
            response=(
                operation.response
                if operation.status in {"event_pending", "succeeded"}
                else None
            ),
            error=operation_error,
        )

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
        recovery_context = PullRecoveryContext.from_banner_config(
            banner_config=banner_config,
            count=pull_request.count,
            seed=seed,
            event_id=event_id,
            amount_minor=cost_minor,
            request_id=request_id,
        )

        try:
            operation_claim = await services.state_store.begin_pull_operation(
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                processing_lease_seconds=PULL_PROCESSING_LEASE_SECONDS,
                recovery_context=recovery_context,
            )
        except GachaStateStoreError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "state_store_unavailable",
                    "message": "pull idempotency is unavailable",
                },
            ) from exc

        if not operation_claim.acquired:
            existing_context = (
                operation_claim.operation.recovery_context or recovery_context
            )
            return await handle_existing_pull_operation(
                operation=operation_claim.operation,
                services=services,
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                event_id=str(existing_context.event_id),
                amount_minor=existing_context.amount_minor,
                request_id=request_id,
                metadata=pull_asset_metadata(
                    event_id=str(existing_context.event_id),
                    banner_id=existing_context.banner.id,
                    banner_version_id=existing_context.banner_version_id,
                    count=existing_context.count,
                ),
            )

        processing_token = operation_claim.processing_token
        if processing_token is None:
            raise RuntimeError("acquired pull operation is missing its processing token")
        claimed_context = operation_claim.operation.recovery_context or recovery_context
        return await execute_claimed_pull(
            services=services,
            user_id=user_id,
            operation_key=operation_claim.operation_key,
            request_hash=request_hash,
            processing_token=processing_token,
            context=claimed_context,
        )


async def execute_claimed_pull(
    *,
    services: AppServices,
    user_id: UUID,
    operation_key: str,
    request_hash: str,
    processing_token: UUID,
    context: PullRecoveryContext,
) -> PullResponse:
    banner_config = context.to_banner_config()
    banner = banner_config.banner
    event_id = str(context.event_id)
    metadata = pull_asset_metadata(
        event_id=event_id,
        banner_id=banner.id,
        banner_version_id=context.banner_version_id,
        count=context.count,
    )

    try:
        await services.asset_client.spend(
            user_id=user_id,
            amount_minor=context.amount_minor,
            idempotency_key=spend_idempotency_key(event_id),
            reason="gacha_pull",
            metadata=metadata,
            request_id=context.request_id,
        )
    except AssetServiceError as exc:
        LOGGER.warning(
            "asset spend failed request_id=%s status=%s code=%s",
            context.request_id,
            exc.status_code,
            exc.code,
        )
        if exc.status_code >= 500:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "asset_unavailable",
                    "message": "asset spend outcome is being reconciled",
                },
            ) from exc

        try:
            await services.state_store.transition_pull_operation_from_processing(
                operation_key=operation_key,
                user_id=user_id,
                request_hash=request_hash,
                processing_token=processing_token,
                operation=PullOperation(
                    status="failed",
                    request_hash=request_hash,
                    error_code=asset_error_code(exc),
                    error_message=asset_error_message(exc),
                ),
            )
        except PullOperationOwnershipLost as ownership_error:
            raise pull_in_progress_http_exception() from ownership_error
        except GachaStateStoreError as state_error:
            raise state_store_http_exception(
                "pull failure could not be persisted"
            ) from state_error
        raise asset_http_exception(exc) from exc

    try:
        previous_pity = await services.state_store.get_snapshot(user_id, banner.id)
        records, next_pity_state = perform_pulls(
            banner_config=banner_config,
            count=context.count,
            pity=previous_pity.without_version(),
            seed=context.seed,
        )
        next_pity = PitySnapshot(
            **next_pity_state.model_dump(),
            version=previous_pity.version + 1,
        )
        response = PullResponse(
            event_id=event_id,
            banner_version_id=context.banner_version_id,
            seed=context.seed,
            records=records,
            previous_pity=previous_pity,
            next_pity=next_pity,
            state_version=next_pity.version,
        )
        event = PullCompletedEvent(
            event_id=event_id,
            user_id=str(user_id),
            banner_id=banner.id,
            banner_version_id=context.banner_version_id,
            seed=context.seed,
            records=records,
            previous_pity=previous_pity,
            next_pity=next_pity,
            state_version=next_pity.version,
        )
        await services.state_store.compare_and_set_with_pull_operation(
            operation_key=operation_key,
            user_id=user_id,
            banner_id=banner.id,
            request_hash=request_hash,
            expected_version=previous_pity.version,
            next_pity=next_pity_state,
            operation=PullOperation(
                status="event_pending",
                request_hash=request_hash,
                response=response,
                event=event,
            ),
            processing_token=processing_token,
        )
    except PityVersionConflict as exc:
        message = f"pity state was updated at version {exc.current_version}"
        try:
            await services.state_store.transition_pull_operation_from_processing(
                operation_key=operation_key,
                user_id=user_id,
                request_hash=request_hash,
                processing_token=processing_token,
                operation=PullOperation(
                    status="refund_pending",
                    request_hash=request_hash,
                    response=response,
                    event=event,
                    error_code="pity_version_conflict",
                    error_message=message,
                    recovery_context=context,
                ),
            )
        except PullOperationOwnershipLost as ownership_error:
            raise pull_in_progress_http_exception() from ownership_error
        except GachaStateStoreError as state_error:
            raise state_store_http_exception(
                "pull refund could not be scheduled"
            ) from state_error

        await refund_claimed_spend(
            services=services,
            user_id=user_id,
            operation_key=operation_key,
            request_hash=request_hash,
            context=context,
            metadata=metadata,
            code="pity_version_conflict",
            message=message,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "pity_version_conflict", "message": message},
        ) from exc
    except PullOperationOwnershipLost as exc:
        raise pull_in_progress_http_exception() from exc
    except GachaStateStoreError as exc:
        try:
            reconciled_operation = await services.state_store.get_pull_operation_by_key(
                operation_key=operation_key,
            )
        except GachaStateStoreError:
            reconciled_operation = None

        if (
            reconciled_operation is not None
            and reconciled_operation.status != "processing"
        ):
            return await handle_claimed_pull_operation(
                operation=reconciled_operation,
                services=services,
                user_id=user_id,
                operation_key=operation_key,
                request_hash=request_hash,
                context=context,
                metadata=metadata,
            )

        raise state_store_http_exception("pull outcome is being reconciled") from exc

    return await publish_and_complete_claimed_pull(
        services=services,
        operation_key=operation_key,
        request_hash=request_hash,
        response=response,
        event=event,
    )


async def handle_claimed_pull_operation(
    *,
    operation: PullOperation,
    services: AppServices,
    user_id: UUID,
    operation_key: str,
    request_hash: str,
    context: PullRecoveryContext,
    metadata: dict[str, object],
) -> PullResponse:
    if operation.request_hash != request_hash:
        raise state_store_http_exception("pull operation request changed")
    if operation.status == "succeeded" and operation.response is not None:
        return operation.response
    if operation.status == "event_pending" and operation.response is not None and operation.event is not None:
        return await publish_and_complete_claimed_pull(
            services=services,
            operation_key=operation_key,
            request_hash=request_hash,
            response=operation.response,
            event=operation.event,
        )
    if operation.status == "refund_pending":
        await refund_claimed_spend(
            services=services,
            user_id=user_id,
            operation_key=operation_key,
            request_hash=request_hash,
            context=context,
            metadata=metadata,
            code=operation.error_code or "pull_refunded",
            message=operation.error_message or "pull was refunded",
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "pull_refunded",
                "message": "previous pull attempt was refunded",
            },
        )
    if operation.status == "failed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": operation.error_code or "pull_failed",
                "message": operation.error_message or "previous pull attempt failed",
            },
        )
    raise state_store_http_exception("pull outcome is being reconciled")


async def publish_and_complete_claimed_pull(
    *,
    services: AppServices,
    operation_key: str,
    request_hash: str,
    response: PullResponse,
    event: PullCompletedEvent,
) -> PullResponse:
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

    try:
        await services.state_store.save_pull_operation_by_key(
            operation_key=operation_key,
            operation=PullOperation(
                status="succeeded",
                request_hash=request_hash,
                response=response,
                event=event,
            ),
        )
    except GachaStateStoreError:
        LOGGER.warning("failed to save pull operation status", exc_info=True)
    return response


async def refund_claimed_spend(
    *,
    services: AppServices,
    user_id: UUID,
    operation_key: str,
    request_hash: str,
    context: PullRecoveryContext,
    metadata: dict[str, object],
    code: str,
    message: str,
) -> None:
    try:
        await services.asset_client.credit(
            user_id=user_id,
            amount_minor=context.amount_minor,
            idempotency_key=refund_idempotency_key(str(context.event_id)),
            reason="gacha_pull_refund",
            metadata={**metadata, "refund_reason": code},
            request_id=context.request_id,
        )
    except AssetServiceError as exc:
        LOGGER.warning(
            "asset refund failed request_id=%s status=%s code=%s",
            context.request_id,
            exc.status_code,
            exc.code,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "asset_refund_unavailable",
                "message": "asset refund could not be completed",
            },
        ) from exc

    await services.state_store.save_pull_operation_by_key(
        operation_key=operation_key,
        operation=PullOperation(
            status="failed",
            request_hash=request_hash,
            error_code=code,
            error_message=message,
        ),
    )


def create_catalog_repository(settings: Settings) -> CatalogRepository:
    if not settings.gacha_config_database_url:
        return StaticCatalogRepository()

    if not settings.gacha_project_id or not settings.gacha_environment_id:
        raise ValueError(
            "GACHA_PROJECT_ID and GACHA_ENVIRONMENT_ID are required when "
            "GACHA_CONFIG_DATABASE_URL is configured"
        )

    return PostgresCatalogRepository(
        database_url=settings.gacha_config_database_url,
        project_id=settings.gacha_project_id,
        environment_id=settings.gacha_environment_id,
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
    except GachaStateStoreError:
        LOGGER.warning("failed to save pull operation status", exc_info=True)


async def recover_expired_processing_pulls_once(
    services: AppServices,
    *,
    limit: int,
    processing_lease_seconds: int,
) -> int:
    try:
        records = await services.state_store.iter_expired_processing_pull_operations(
            limit=limit,
        )
    except GachaStateStoreError:
        LOGGER.warning("failed to scan expired processing pulls", exc_info=True)
        return 0

    recovered_count = 0
    for record in records:
        if (
            record.operation.status != "processing"
            or record.operation.recovery_context is None
        ):
            continue

        try:
            claim = await services.state_store.claim_expired_processing_pull_operation(
                operation_key=record.operation_key,
                processing_lease_seconds=processing_lease_seconds,
            )
        except GachaStateStoreError:
            LOGGER.warning(
                "failed to claim expired processing pull operation_key=%s",
                record.operation_key,
                exc_info=True,
            )
            continue

        if claim is None or claim.operation.recovery_context is None:
            continue

        event_id = str(claim.operation.recovery_context.event_id)
        try:
            await execute_claimed_pull(
                services=services,
                user_id=claim.user_id,
                operation_key=claim.operation_key,
                request_hash=claim.operation.request_hash,
                processing_token=claim.processing_token,
                context=claim.operation.recovery_context,
            )
            recovered_count += 1
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            LOGGER.warning(
                "expired processing pull recovery deferred event_id=%s status=%s code=%s",
                event_id,
                exc.status_code,
                detail.get("code", "recovery_failed"),
            )
        except Exception:
            LOGGER.exception(
                "unexpected expired processing pull recovery failure event_id=%s",
                event_id,
            )

    return recovered_count


async def run_pull_recovery_worker(
    *,
    services: AppServices,
    interval_seconds: int,
    batch_size: int,
    lock_ttl_seconds: int,
) -> None:
    while True:
        await asyncio.sleep(max(1, interval_seconds))
        try:
            recovered_processing_count = await recover_expired_processing_pulls_once(
                services,
                limit=batch_size,
                processing_lease_seconds=PULL_PROCESSING_LEASE_SECONDS,
            )
            recovered_event_count = await recover_pending_pull_events_once(
                services,
                limit=batch_size,
                lock_ttl_seconds=lock_ttl_seconds,
            )
            recovered_refund_count = await recover_pending_pull_refunds_once(
                services,
                limit=batch_size,
                lock_ttl_seconds=lock_ttl_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("pull recovery worker failed")
            continue

        if recovered_processing_count:
            LOGGER.info(
                "recovered expired processing pulls count=%s",
                recovered_processing_count,
            )
        if recovered_event_count:
            LOGGER.info("recovered pending pull events count=%s", recovered_event_count)
        if recovered_refund_count:
            LOGGER.info("recovered pending pull refunds count=%s", recovered_refund_count)


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
    except GachaStateStoreError:
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
                expected_status="event_pending",
                lock_ttl_seconds=lock_ttl_seconds,
            )
        except GachaStateStoreError:
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
        except GachaStateStoreError:
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
            except GachaStateStoreError:
                LOGGER.warning(
                    "failed to release pending pull event recovery lock event_id=%s",
                    event_id,
                    exc_info=True,
                )

    return recovered_count


async def recover_pending_pull_refunds_once(
    services: AppServices,
    *,
    limit: int,
    lock_ttl_seconds: int,
) -> int:
    try:
        pending_operations = await services.state_store.iter_refund_pending_pull_operations(
            limit=limit,
        )
    except GachaStateStoreError:
        LOGGER.warning("failed to scan pending pull refunds", exc_info=True)
        return 0

    recovered_count = 0
    for record in pending_operations:
        operation = record.operation
        context = operation.recovery_context
        if operation.status != "refund_pending" or context is None:
            continue

        event_id = str(context.event_id)
        try:
            claimed = await services.state_store.claim_pull_operation_recovery(
                operation_key=record.operation_key,
                expected_status="refund_pending",
                lock_ttl_seconds=lock_ttl_seconds,
            )
        except GachaStateStoreError:
            LOGGER.warning(
                "failed to claim pending pull refund event_id=%s",
                event_id,
                exc_info=True,
            )
            continue

        if not claimed:
            continue

        metadata = pull_asset_metadata(
            event_id=event_id,
            banner_id=context.banner.id,
            banner_version_id=context.banner_version_id,
            count=context.count,
        )
        try:
            await refund_claimed_spend(
                services=services,
                user_id=record.user_id,
                operation_key=record.operation_key,
                request_hash=operation.request_hash,
                context=context,
                metadata=metadata,
                code=operation.error_code or "pull_refunded",
                message=operation.error_message or "pull was refunded",
            )
            recovered_count += 1
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            LOGGER.warning(
                "pending pull refund recovery deferred event_id=%s status=%s code=%s",
                event_id,
                exc.status_code,
                detail.get("code", "refund_recovery_failed"),
            )
        except GachaStateStoreError:
            LOGGER.warning(
                "failed to mark pending pull refund complete event_id=%s",
                event_id,
                exc_info=True,
            )
        except Exception:
            LOGGER.exception(
                "unexpected pending pull refund recovery failure event_id=%s",
                event_id,
            )
        finally:
            try:
                await services.state_store.release_pull_operation_recovery(
                    operation_key=record.operation_key,
                )
            except GachaStateStoreError:
                LOGGER.warning(
                    "failed to release pending pull refund recovery lock event_id=%s",
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


def state_store_http_exception(message: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={"code": "state_store_unavailable", "message": message},
    )


def pull_in_progress_http_exception() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "code": "pull_in_progress",
            "message": "pull is already being processed by another worker",
        },
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
