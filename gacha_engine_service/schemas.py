"""Pydantic schemas for the public HTTP API and Kafka event payloads."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


GachaRarity = Literal[3, 4, 5]
GachaItemType = Literal["character", "weapon"]
BannerType = Literal["limited-character", "standard"]


class ApiError(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ApiError


class HealthResponse(BaseModel):
    status: str = "ok"


class ReadyResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, str] = Field(default_factory=dict)


class GachaItem(BaseModel):
    id: str
    name: str
    subtitle: str
    rarity: GachaRarity
    type: GachaItemType
    element: str
    role: str
    faction: str
    accent: str
    quote: str


class BannerTheme(BaseModel):
    primary: str
    secondary: str
    glow: str


class Banner(BaseModel):
    id: str
    name: str
    short_name: str
    type: BannerType
    description: str
    featured_five_id: str | None = None
    featured_four_ids: list[str] = Field(default_factory=list)
    item_pool: list[str]
    theme: BannerTheme


class PityState(BaseModel):
    since_five: int = Field(default=0, ge=0)
    since_four: int = Field(default=0, ge=0)
    guaranteed_featured_five: bool = False


class PitySnapshot(PityState):
    version: int = Field(default=0, ge=0)

    def without_version(self) -> PityState:
        return PityState(
            since_five=self.since_five,
            since_four=self.since_four,
            guaranteed_featured_five=self.guaranteed_featured_five,
        )


class PullRequest(BaseModel):
    banner_id: str = Field(..., min_length=1, max_length=100)
    count: Literal[1, 10]
    seed: str | None = Field(default=None, min_length=1, max_length=200)


class PullRecord(BaseModel):
    id: str
    index: int
    item_id: str
    item_name: str
    item_type: GachaItemType
    rarity: GachaRarity
    banner_id: str
    banner_name: str
    pity_at_five: int
    pity_at_four: int
    is_featured: bool


class PullAuditMetadata(BaseModel):
    schema_version: Literal[1] = 1
    release_id: str | None = None
    release_number: int | None = Field(default=None, ge=1)
    release_snapshot_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    banner_version_id: str | None = None
    banner_config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rng_algorithm_version: str
    engine_version: str
    engine_build_sha: str


class PullResponse(BaseModel):
    event_id: str
    accepted_at: datetime | None = None
    pity_group_id: str | None = None
    banner_version_id: str | None = None
    seed: str
    records: list[PullRecord]
    previous_pity: PitySnapshot
    next_pity: PitySnapshot
    state_version: int
    audit: PullAuditMetadata | None = None


class PullAuditVerificationResponse(BaseModel):
    event_id: str
    status: Literal["verified", "mismatch", "unverifiable"]
    audit: PullAuditMetadata | None = None
    checks: dict[str, bool] = Field(default_factory=dict)
    mismatches: list[str] = Field(default_factory=list)
    configuration: dict[str, Any] | None = None
    recorded_records: list[PullRecord] = Field(default_factory=list)
    replayed_records: list[PullRecord] | None = None
    recorded_next_pity: PitySnapshot | None = None
    replayed_next_pity: PitySnapshot | None = None


class PullOperationStateResponse(BaseModel):
    status: Literal[
        "processing",
        "event_pending",
        "event_published",
        "succeeded",
        "refund_pending",
        "failed",
    ]
    response: PullResponse | None = None
    error: ApiError | None = None


class PullOperationSummary(BaseModel):
    operation_id: str
    event_id: str | None = None
    request_id: str | None = None
    banner_id: str | None = None
    banner_version_id: str | None = None
    pity_group_id: str | None = None
    count: int | None = None
    status: Literal[
        "processing",
        "event_pending",
        "event_published",
        "succeeded",
        "refund_pending",
        "failed",
    ]
    error: ApiError | None = None
    next_pity: PitySnapshot | None = None
    created_at: datetime
    updated_at: datetime


class PullOperationListResponse(BaseModel):
    items: list[PullOperationSummary] = Field(default_factory=list)


class PullCompletedEvent(BaseModel):
    event_id: str
    event_type: Literal["gacha.pull_completed.v1"] = "gacha.pull_completed.v1"
    accepted_at: datetime | None = None
    user_id: str
    banner_id: str
    pity_group_id: str | None = None
    banner_version_id: str | None = None
    seed: str
    records: list[PullRecord]
    previous_pity: PitySnapshot
    next_pity: PitySnapshot
    state_version: int
    audit: PullAuditMetadata | None = None
