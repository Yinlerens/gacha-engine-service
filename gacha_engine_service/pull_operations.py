"""Durable pull operation state and processing claims."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from .schemas import PullCompletedEvent, PullResponse


PullOperationStatus = Literal[
    "processing",
    "event_pending",
    "succeeded",
    "refund_pending",
    "failed",
]


class PullOperation(BaseModel):
    status: PullOperationStatus
    request_hash: str
    response: PullResponse | None = None
    event: PullCompletedEvent | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class PullOperationClaim:
    operation: PullOperation
    processing_token: UUID | None = None

    @property
    def acquired(self) -> bool:
        return self.processing_token is not None


@dataclass(frozen=True)
class PullOperationRecord:
    operation_key: str
    operation: PullOperation
