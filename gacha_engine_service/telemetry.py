"""OpenTelemetry bootstrap and trace-aware JSON logging."""

from __future__ import annotations

import atexit
from datetime import datetime, timezone
import json
import logging
import os
from typing import Any

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)


_CONFIGURED = False


def configure_telemetry(default_service_name: str) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    set_global_textmap(
        CompositePropagator(
            [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
        )
    )

    if _export_enabled():
        service_name = os.getenv("OTEL_SERVICE_NAME", "").strip() or default_service_name
        provider = TracerProvider(
            resource=Resource.create({"service.name": service_name}),
        )
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
        atexit.register(provider.shutdown)

    HTTPXClientInstrumentor().instrument()
    AsyncPGInstrumentor().instrument()


def instrument_fastapi(app: FastAPI) -> None:
    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls=".*/health,.*/ready",
        exclude_spans=["receive", "send"],
    )


def application_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not any(getattr(handler, "_gacha_trace_json", False) for handler in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(TraceJSONFormatter())
        handler._gacha_trace_json = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    return logger


class TraceJSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "logger": record.name,
        }

        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            payload["trace_id"] = trace.format_trace_id(span_context.trace_id)
            payload["span_id"] = trace.format_span_id(span_context.span_id)

        for field in (
            "request_id",
            "method",
            "path",
            "status",
            "duration_ms",
            "client_ip",
        ):
            value = getattr(record, field, None)
            if value not in (None, ""):
                payload[field] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _export_enabled() -> bool:
    if os.getenv("OTEL_SDK_DISABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False
    return bool(
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
        or os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
    )
