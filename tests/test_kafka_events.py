from __future__ import annotations

import unittest

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from gacha_engine_service.kafka_events import KafkaEventPublisher
from gacha_engine_service.schemas import PullCompletedEvent


class FakeProducer:
    def __init__(self) -> None:
        self.headers: list[tuple[str, bytes]] = []

    async def send_and_wait(self, *args: object, **kwargs: object) -> None:
        self.headers = list(kwargs.get("headers", []))  # type: ignore[arg-type]

    async def stop(self) -> None:
        return None


class KafkaEventPublisherTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        trace.set_tracer_provider(TracerProvider())

    async def test_publish_injects_w3c_trace_context(self) -> None:
        producer = FakeProducer()
        publisher = KafkaEventPublisher(
            bootstrap_servers="kafka:9092",
            topic="gacha.pull_completed.v1",
            client_id="test",
        )
        publisher._producer = producer  # type: ignore[assignment]

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("test pull") as parent:
            await publisher.publish_pull_completed(sample_event())
            expected_trace_id = trace.format_trace_id(
                parent.get_span_context().trace_id
            )

        headers = {key: value.decode("utf-8") for key, value in producer.headers}
        self.assertIn("traceparent", headers)
        self.assertIn(expected_trace_id, headers["traceparent"])


def sample_event() -> PullCompletedEvent:
    return PullCompletedEvent.model_validate(
        {
            "event_id": "f7db8d82-41d2-4b43-9678-22ed0d07ffba",
            "user_id": "ae6b9d2e-9bb0-42c7-950f-c38ab6d7195e",
            "banner_id": "limited-character-001",
            "seed": "stable-seed",
            "records": [
                {
                    "id": "37f1f86c-2d5b-4f69-b4cd-33e090065f95",
                    "index": 0,
                    "item_id": "char-luoxian",
                    "item_name": "Luoxian",
                    "item_type": "character",
                    "rarity": 5,
                    "banner_id": "limited-character-001",
                    "banner_name": "Limited",
                    "pity_at_five": 80,
                    "pity_at_four": 1,
                    "is_featured": True,
                }
            ],
            "previous_pity": {
                "since_five": 79,
                "since_four": 0,
                "guaranteed_featured_five": True,
                "version": 0,
            },
            "next_pity": {
                "since_five": 0,
                "since_four": 1,
                "guaranteed_featured_five": False,
                "version": 1,
            },
            "state_version": 1,
        }
    )


if __name__ == "__main__":
    unittest.main()
