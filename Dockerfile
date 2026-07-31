ARG GACHA_ENGINE_BUILD_SHA=development

FROM python:3.13-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.11 /uv /uvx /bin/

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project --no-cache

FROM python:3.13-slim

ARG GACHA_ENGINE_BUILD_SHA

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    GACHA_ENGINE_BUILD_SHA="$GACHA_ENGINE_BUILD_SHA"

WORKDIR /app

RUN addgroup --system --gid 10001 app \
    && adduser --system --uid 10001 --ingroup app app

COPY --from=builder /app/.venv /app/.venv
COPY gacha_engine_service ./gacha_engine_service

USER 10001:10001

EXPOSE 8080

CMD ["uvicorn", "gacha_engine_service.main:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
