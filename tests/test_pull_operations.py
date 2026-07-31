from __future__ import annotations

from dataclasses import replace

from gacha_engine_service.catalog_config import static_catalog_snapshot
from gacha_engine_service.engine import create_initial_pity, perform_pulls
from gacha_engine_service.pull_audit import banner_config_evidence
from gacha_engine_service.pull_operations import PullRecoveryContext


def test_recovery_context_round_trip_reproduces_the_same_pull() -> None:
    banner_config = static_catalog_snapshot().banner_configs_by_id[
        "limited-character-001"
    ]
    context = PullRecoveryContext.from_banner_config(
        banner_config=banner_config,
        count=10,
        seed="frozen-recovery-seed",
        event_id="33333333-3333-4333-8333-333333333333",
        amount_minor=1600,
        request_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )
    restored = PullRecoveryContext.model_validate_json(context.model_dump_json())
    pity = create_initial_pity()

    expected_records, expected_pity = perform_pulls(
        banner_config=context.to_banner_config(),
        count=context.count,
        pity=pity,
        seed=context.seed,
    )
    actual_records, actual_pity = perform_pulls(
        banner_config=restored.to_banner_config(),
        count=restored.count,
        pity=pity,
        seed=restored.seed,
    )

    assert restored == context
    assert actual_records == expected_records
    assert actual_pity == expected_pity


def test_audit_evidence_excludes_items_that_cannot_affect_the_banner() -> None:
    banner_config = static_catalog_snapshot().banner_configs_by_id[
        "limited-character-001"
    ]
    unrelated_item = banner_config.all_items[0].model_copy(
        update={"id": "future-unrelated-item", "name": "Future Unrelated Item"}
    )
    expanded_config = replace(
        banner_config,
        all_items=(*banner_config.all_items, unrelated_item),
        item_by_id={**banner_config.item_by_id, unrelated_item.id: unrelated_item},
    )

    evidence = banner_config_evidence(expanded_config)

    assert unrelated_item.id not in {item["id"] for item in evidence["items"]}
