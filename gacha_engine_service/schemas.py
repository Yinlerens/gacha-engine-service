"""Pydantic schemas for the public HTTP API and Kafka event payloads."""

from __future__ import annotations

from typing import Literal

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


class PullResponse(BaseModel):
    event_id: str
    banner_version_id: str | None = None
    seed: str
    records: list[PullRecord]
    previous_pity: PitySnapshot
    next_pity: PitySnapshot
    state_version: int


class PullCompletedEvent(BaseModel):
    event_id: str
    event_type: Literal["gacha.pull_completed.v1"] = "gacha.pull_completed.v1"
    user_id: str
    banner_id: str
    banner_version_id: str | None = None
    seed: str
    records: list[PullRecord]
    previous_pity: PitySnapshot
    next_pity: PitySnapshot
    state_version: int
