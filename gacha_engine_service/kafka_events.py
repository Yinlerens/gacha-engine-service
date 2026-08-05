"""Kafka event publisher for completed gacha pulls."""

from __future__ import annotations

import json

from aiokafka import AIOKafkaProducer
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind

from .schemas import PullCompletedEvent


TRACER = trace.get_tracer(__name__)


class EventPublishError(Exception):
    """Raised when a Kafka event cannot be published."""


class KafkaEventPublisher:
    """Lazy Kafka producer wrapper."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        client_id: str,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def ping(self) -> None:
        await self._ensure_started()

    async def publish_pull_completed(self, event: PullCompletedEvent) -> None:
        producer = await self._ensure_started()
        payload = json.dumps(
            event.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        with TRACER.start_as_current_span(
            f"{self._topic} publish",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": self._topic,
                "messaging.operation.name": "publish",
                "messaging.message.id": event.event_id,
            },
        ):
            carrier: dict[str, str] = {}
            propagate.inject(carrier)
            headers = [(key, value.encode("utf-8")) for key, value in carrier.items()]

            try:
                await producer.send_and_wait(
                    self._topic,
                    payload,
                    key=event.user_id.encode("utf-8"),
                    headers=headers,
                )
            except Exception as exc:  # aiokafka exposes several transport exceptions.
                raise EventPublishError("failed to publish pull event") from exc

    async def _ensure_started(self) -> AIOKafkaProducer:
        if self._producer is not None:
            return self._producer

        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            value_serializer=None,
            key_serializer=None,
        )

        try:
            await producer.start()
        except Exception as exc:
            raise EventPublishError("kafka is unavailable") from exc

        self._producer = producer
        return producer
