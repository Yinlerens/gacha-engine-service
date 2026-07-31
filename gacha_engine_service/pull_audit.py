"""Immutable pull evidence and deterministic verification."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .catalog_config import BannerConfig, CatalogSnapshot
from .engine import RNG_ALGORITHM_VERSION, perform_pulls
from .schemas import (
    PitySnapshot,
    PullAuditMetadata,
    PullAuditVerificationResponse,
    PullResponse,
)


def banner_config_evidence(config: BannerConfig) -> dict[str, Any]:
    """Return the canonical configuration that can affect a pull result."""

    effective_items = _effective_items(config)
    return {
        "schema_version": 1,
        "banner": config.banner.model_dump(mode="json"),
        "pity_group_id": config.pity_group_id,
        "banner_version_id": config.banner_version_id,
        "banner_version": config.version,
        "items": [item.model_dump(mode="json") for item in effective_items],
        "rarity_rates": [
            {
                "rarity": rate.rarity,
                "base_rate_ppm": rate.base_rate_ppm,
                "roll_order": rate.roll_order,
            }
            for _, rate in sorted(config.rarity_rates.items())
        ],
        "featured_rules": [
            {
                "rarity": rule.rarity,
                "featured_group": rule.featured_group,
                "featured_rate_ppm": rule.featured_rate_ppm,
                "guarantee_after_miss": rule.guarantee_after_miss,
                "miss_sets_guarantee": rule.miss_sets_guarantee,
                "guarantee_state_key": rule.guarantee_state_key,
            }
            for _, rule in sorted(config.featured_rules.items())
        ],
        "pity_rules": [
            {
                "rarity": rule.rarity,
                "counter_key": rule.counter_key,
                "hard_pity": rule.hard_pity,
                "soft_pity_start": rule.soft_pity_start,
                "soft_pity_increment_ppm": rule.soft_pity_increment_ppm,
                "resets_lower_rarity": rule.resets_lower_rarity,
            }
            for _, rule in sorted(config.pity_rules.items())
        ],
        "featured_item_ids_by_rarity": {
            str(rarity): list(item_ids)
            for rarity, item_ids in sorted(config.featured_item_ids_by_rarity.items())
        },
    }


def _effective_items(config: BannerConfig) -> list[Any]:
    selected_ids = set(config.banner.item_pool)
    explicit_rarities = {
        item.rarity
        for item_id in config.banner.item_pool
        if (item := config.item_by_id.get(item_id)) is not None
    }
    for item in config.all_items:
        if item.rarity not in explicit_rarities:
            selected_ids.add(item.id)
    return [item for item in config.all_items if item.id in selected_ids]


def banner_config_sha256(config: BannerConfig) -> str:
    payload = json.dumps(
        banner_config_evidence(config),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_pull_audit_metadata(
    *,
    snapshot: CatalogSnapshot,
    config: BannerConfig,
    engine_version: str,
    engine_build_sha: str,
) -> PullAuditMetadata:
    if snapshot.source == "postgres" and (
        snapshot.release_id is None
        or snapshot.release_number is None
        or snapshot.snapshot_sha256 is None
    ):
        raise ValueError("postgres catalog snapshot is missing immutable release evidence")

    return PullAuditMetadata(
        release_id=snapshot.release_id,
        release_number=snapshot.release_number,
        release_snapshot_sha256=snapshot.snapshot_sha256,
        banner_version_id=config.banner_version_id,
        banner_config_sha256=banner_config_sha256(config),
        rng_algorithm_version=RNG_ALGORITHM_VERSION,
        engine_version=engine_version,
        engine_build_sha=engine_build_sha,
    )


def verify_pull_response(
    response: PullResponse,
    snapshot: CatalogSnapshot,
) -> PullAuditVerificationResponse:
    audit = response.audit
    if audit is None or response.accepted_at is None or not response.records:
        return PullAuditVerificationResponse(
            event_id=response.event_id,
            status="unverifiable",
            audit=audit,
            mismatches=["legacy pull record lacks deterministic audit evidence"],
            recorded_records=response.records,
            recorded_next_pity=response.next_pity,
        )

    immutable_release_identity = all(
        value is not None
        for value in (
            audit.release_id,
            audit.release_number,
            audit.release_snapshot_sha256,
            snapshot.release_id,
            snapshot.release_number,
            snapshot.snapshot_sha256,
        )
    )
    if not immutable_release_identity:
        return PullAuditVerificationResponse(
            event_id=response.event_id,
            status="unverifiable",
            audit=audit,
            checks={"immutable_release_identity": False},
            mismatches=["pull record lacks an immutable catalog release identity"],
            recorded_records=response.records,
            recorded_next_pity=response.next_pity,
        )

    banner_id = response.records[0].banner_id
    config = snapshot.banner_config_at(banner_id, response.accepted_at)
    if config is None:
        return PullAuditVerificationResponse(
            event_id=response.event_id,
            status="mismatch",
            audit=audit,
            checks={"banner_configuration": False},
            mismatches=["recorded banner configuration is absent from the release"],
            recorded_records=response.records,
            recorded_next_pity=response.next_pity,
        )

    configuration = banner_config_evidence(config)
    checks = {
        "release_id": snapshot.release_id == audit.release_id,
        "release_number": snapshot.release_number == audit.release_number,
        "release_snapshot_sha256": (
            snapshot.snapshot_sha256 == audit.release_snapshot_sha256
        ),
        "banner_version_id": config.banner_version_id == audit.banner_version_id,
        "banner_config_sha256": (
            banner_config_sha256(config) == audit.banner_config_sha256
        ),
        "rng_algorithm_version": audit.rng_algorithm_version == RNG_ALGORITHM_VERSION,
    }

    replayed_records = None
    replayed_next_pity = None
    if checks["rng_algorithm_version"]:
        replayed_records, replayed_state = perform_pulls(
            banner_config=config,
            count=len(response.records),
            pity=response.previous_pity.without_version(),
            seed=response.seed,
        )
        replayed_next_pity = PitySnapshot(
            **replayed_state.model_dump(),
            version=response.previous_pity.version + 1,
        )
        checks["result_records"] = replayed_records == response.records
        checks["next_pity"] = replayed_next_pity == response.next_pity
        checks["state_version"] = response.state_version == replayed_next_pity.version

    mismatches = [name for name, passed in checks.items() if not passed]
    verification_status = (
        "unverifiable"
        if not checks["rng_algorithm_version"]
        else "verified"
        if not mismatches
        else "mismatch"
    )
    return PullAuditVerificationResponse(
        event_id=response.event_id,
        status=verification_status,
        audit=audit,
        checks=checks,
        mismatches=mismatches,
        configuration=configuration,
        recorded_records=response.records,
        replayed_records=replayed_records,
        recorded_next_pity=response.next_pity,
        replayed_next_pity=replayed_next_pity,
    )
