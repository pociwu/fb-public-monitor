from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from fb_monitor.capture_v2 import (
    AccessState,
    ArtifactKind,
    ArtifactRecord,
    AuthScope,
    BatchStatus,
    CaptureIntent,
    ContractStatus,
    CoverageStatus,
    CoverageStream,
    CoverageSurface,
    DuplicateReason,
    EvidenceClass,
    EvidenceSignal,
    EvidenceSource,
    ObservationPurpose,
    ObservationWindow,
    ProbeSource,
    RetentionAction,
    StrongPrivateObservation,
    artifact_retention_decision,
    available_budget_usd,
    budget_decision,
    canonical_input_json,
    choose_probe_source,
    classify_access_evidence,
    deterministic_observation_window,
    duplicate_page_circuit_breaker,
    evidence_cap_evictions,
    next_access_state,
    normalized_input,
    planned_probe_source,
    request_hash,
    validate_batch_transition,
    validate_contract_for_paid_run,
    validate_contract_transition,
    validate_coverage_status,
    validate_coverage_transition,
)


NOW = datetime(2026, 8, 16, 4, 0, tzinfo=UTC)


def test_state_enums_use_persistent_lowercase_values():
    assert CoverageStatus.COMPLETE.value == "complete"
    assert AccessState.AUTHENTICATED_VISIBLE.value == "authenticated_visible"
    assert BatchStatus.NEEDS_RECONCILE.value == "needs_reconcile"
    assert ContractStatus.PASSED.value == "passed"


def test_complete_coverage_requires_terminal_evidence_and_limited_states_require_reason():
    assert (
        validate_coverage_status(CoverageStatus.COMPLETE, terminal_evidence="summary:end")
        is CoverageStatus.COMPLETE
    )
    with pytest.raises(ValueError, match="terminal evidence"):
        validate_coverage_status(CoverageStatus.COMPLETE)
    with pytest.raises(ValueError, match="requires a reason"):
        validate_coverage_status(CoverageStatus.SOURCE_LIMITED)
    with pytest.raises(ValueError, match="only be attached"):
        validate_coverage_status(CoverageStatus.IN_PROGRESS, terminal_evidence="end")
    assert (
        validate_coverage_status(CoverageStatus.BUDGET_PAUSED, reason="cycle exhausted")
        is CoverageStatus.BUDGET_PAUSED
    )


def test_coverage_transition_cannot_reset_a_completed_stream():
    assert (
        validate_coverage_transition(
            CoverageStatus.IN_PROGRESS,
            CoverageStatus.COMPLETE,
            terminal_evidence="source terminal",
        )
        is CoverageStatus.COMPLETE
    )
    with pytest.raises(ValueError, match="invalid coverage transition"):
        validate_coverage_transition(
            CoverageStatus.COMPLETE,
            CoverageStatus.IN_PROGRESS,
        )
    assert (
        validate_coverage_transition(
            CoverageStatus.SOURCE_LIMITED,
            CoverageStatus.IN_PROGRESS,
        )
        is CoverageStatus.IN_PROGRESS
    )


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (BatchStatus.PREPARED, BatchStatus.LAUNCHING),
        (BatchStatus.LAUNCHING, BatchStatus.RUN_STARTED),
        (BatchStatus.LAUNCHING, BatchStatus.NEEDS_RECONCILE),
        (BatchStatus.NEEDS_RECONCILE, BatchStatus.RAW_SAVED),
        (BatchStatus.RAW_SAVED, BatchStatus.IMPORT_FAILED),
        (BatchStatus.IMPORT_FAILED, BatchStatus.IMPORTED),
        (BatchStatus.IMPORTED, BatchStatus.COMMITTED),
        (BatchStatus.COMMITTED, BatchStatus.COMMITTED),
    ],
)
def test_valid_batch_transitions(current, target):
    assert validate_batch_transition(current, target) is target


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (BatchStatus.PREPARED, BatchStatus.RUN_STARTED),
        (BatchStatus.LAUNCHING, BatchStatus.COMMITTED),
        (BatchStatus.RAW_SAVED, BatchStatus.COMMITTED),
        (BatchStatus.COMMITTED, BatchStatus.RAW_SAVED),
        (BatchStatus.FAILED, BatchStatus.LAUNCHING),
    ],
)
def test_invalid_batch_transitions(current, target):
    with pytest.raises(ValueError, match="invalid batch transition"):
        validate_batch_transition(current, target)


def test_contract_transition_and_paid_run_gate_are_fail_closed():
    assert validate_contract_transition("pending", "passed") is ContractStatus.PASSED
    assert validate_contract_transition("failed", "pending") is ContractStatus.PENDING
    with pytest.raises(ValueError, match="invalid contract transition"):
        validate_contract_transition("failed", "passed")

    accepted = validate_contract_for_paid_run(
        status="passed",
        fingerprint="actor@build:schema",
        expected_fingerprint="actor@build:schema",
        now=NOW,
        expires_at=NOW + timedelta(seconds=1),
    )
    assert accepted.allowed is True
    assert accepted.reason == "contract_passed"

    assert not validate_contract_for_paid_run(
        status="pending",
        fingerprint="same",
        expected_fingerprint="same",
        now=NOW,
    ).allowed
    assert validate_contract_for_paid_run(
        status="passed",
        fingerprint="old",
        expected_fingerprint="new",
        now=NOW,
    ).reason == "contract_fingerprint_mismatch"
    assert validate_contract_for_paid_run(
        status="passed",
        fingerprint="same",
        expected_fingerprint="same",
        now=NOW,
        expires_at=NOW,
    ).reason == "contract_expired"


def test_contract_gate_and_windows_reject_naive_datetimes():
    with pytest.raises(ValueError, match="timezone-aware"):
        validate_contract_for_paid_run(
            status="passed",
            fingerprint="same",
            expected_fingerprint="same",
            now=datetime(2026, 1, 1),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        deterministic_observation_window(datetime(2026, 1, 1), timedelta(hours=2))


def test_deterministic_observation_window_is_utc_and_half_open():
    taipei = timezone(timedelta(hours=8))
    window = deterministic_observation_window(
        datetime(2026, 8, 16, 11, 59, 59, 999999, tzinfo=taipei),
        timedelta(hours=2),
    )
    assert window.start_at == datetime(2026, 8, 16, 2, 0, tzinfo=UTC)
    assert window.end_at == datetime(2026, 8, 16, 4, 0, tzinfo=UTC)
    assert window.key == "2026-08-16T02:00:00.000000Z/2026-08-16T04:00:00.000000Z"

    boundary = deterministic_observation_window(window.end_at, timedelta(hours=2))
    assert boundary.start_at == window.end_at
    assert boundary.end_at == datetime(2026, 8, 16, 6, 0, tzinfo=UTC)


def test_observation_window_validates_duration_and_order():
    with pytest.raises(ValueError, match="positive"):
        deterministic_observation_window(NOW, timedelta(0))
    with pytest.raises(ValueError, match="end must be after"):
        ObservationWindow(NOW, NOW)


def test_canonical_json_normalizes_order_unicode_tuples_and_numbers():
    decomposed = "e\u0301"
    first = {
        "z": (1.0, -0.0, decomposed),
        "a": {"two": True, "one": None},
    }
    second = {
        "a": {"one": None, "two": True},
        "z": [1, 0, "\u00e9"],
    }
    expected = '{"a":{"one":null,"two":true},"z":[1,0,"\u00e9"]}'
    assert canonical_input_json(first) == expected
    assert canonical_input_json(second) == expected
    assert normalized_input(first) == normalized_input(second)


@pytest.mark.parametrize(
    "payload",
    [
        {"bad": float("nan")},
        {"bad": float("inf")},
        {1: "non-string key"},
        {"bad": {1, 2}},
    ],
)
def test_canonical_json_rejects_non_json_values(payload):
    with pytest.raises((TypeError, ValueError)):
        canonical_input_json(payload)


def test_canonical_json_rejects_keys_that_collide_after_unicode_normalization():
    with pytest.raises(ValueError, match="duplicate normalized"):
        canonical_input_json({"\u00e9": 1, "e\u0301": 2})


def _hash_kwargs():
    return {
        "capture_intent": CaptureIntent.INITIAL_PUBLIC_CAPTURE,
        "window": deterministic_observation_window(NOW, timedelta(hours=2)),
        "profile_id": 3,
        "epoch_id": 9,
        "stream": CoverageStream.POSTS,
        "surface": CoverageSurface.TIMELINE_POSTS,
        "contract_fingerprint": "actor/build/schema/input-v1",
        "actor_input": {"maxPosts": 50, "startUrls": [{"url": "https://facebook.com/3"}]},
    }


def test_request_hash_is_order_and_timezone_independent():
    kwargs = _hash_kwargs()
    first = request_hash(**kwargs)
    kwargs["actor_input"] = {
        "startUrls": [{"url": "https://facebook.com/3"}],
        "maxPosts": 50.0,
    }
    kwargs["window"] = ObservationWindow(
        datetime(2026, 8, 16, 12, 0, tzinfo=timezone(timedelta(hours=8))),
        datetime(2026, 8, 16, 14, 0, tzinfo=timezone(timedelta(hours=8))),
    )
    assert request_hash(**kwargs) == first
    assert len(first) == 64


@pytest.mark.parametrize(
    ("field", "new_value"),
    [
        ("capture_intent", CaptureIntent.RECOVERY_CAPTURE),
        ("profile_id", 4),
        ("epoch_id", 10),
        ("stream", CoverageStream.MEDIA),
        ("surface", CoverageSurface.POST_ALBUMS),
        ("contract_fingerprint", "new-fingerprint"),
        ("actor_input", {"maxPosts": 49}),
    ],
)
def test_request_hash_covers_every_paid_intent_boundary(field, new_value):
    original = _hash_kwargs()
    changed = _hash_kwargs()
    changed[field] = new_value
    assert request_hash(**changed) != request_hash(**original)


def test_request_hash_changes_with_observation_window():
    original = _hash_kwargs()
    changed = _hash_kwargs()
    changed["window"] = deterministic_observation_window(NOW + timedelta(hours=2), timedelta(hours=2))
    assert request_hash(**changed) != request_hash(**original)


def test_request_hash_requires_all_identity_boundaries():
    kwargs = _hash_kwargs()
    kwargs["contract_fingerprint"] = " "
    with pytest.raises(ValueError, match="required"):
        request_hash(**kwargs)


def test_duplicate_page_breaker_trips_on_same_cursor_or_identity_set():
    same_cursor = duplicate_page_circuit_breaker(
        previous_cursor="cursor-a",
        previous_identities=["post:1", "post:2"],
        current_cursor="cursor-a",
        current_identities=["post:3"],
    )
    assert same_cursor.tripped
    assert same_cursor.reasons == (DuplicateReason.SAME_CURSOR,)

    same_items = duplicate_page_circuit_breaker(
        previous_cursor="cursor-a",
        previous_identities=["post:2", "post:1", "post:1"],
        current_cursor="cursor-b",
        current_identities=["post:1", "post:2"],
    )
    assert same_items.tripped
    assert same_items.reasons == (DuplicateReason.SAME_IDENTITIES,)


def test_duplicate_page_breaker_does_not_treat_empty_terminal_pages_as_a_cycle():
    decision = duplicate_page_circuit_breaker(
        previous_cursor=None,
        previous_identities=[],
        current_cursor=None,
        current_identities=[],
    )
    assert decision.tripped is False
    assert decision.reasons == ()

    progressing = duplicate_page_circuit_breaker(
        previous_cursor="cursor-a",
        previous_identities=["post:1"],
        current_cursor="cursor-b",
        current_identities=["post:2"],
    )
    assert progressing.tripped is False


@pytest.mark.parametrize(
    "signal",
    [
        EvidenceSignal.EMPTY,
        EvidenceSignal.NO_ITEMS,
        EvidenceSignal.TIMEOUT,
        EvidenceSignal.LOGIN_WALL,
        EvidenceSignal.PARSE_ERROR,
        EvidenceSignal.HTTP_ERROR,
    ],
)
def test_empty_and_error_access_observations_are_indeterminate(signal):
    assert classify_access_evidence(
        source=EvidenceSource.SERPAPI,
        auth_scope=AuthScope.ANONYMOUS,
        signal=signal,
        identity_matches=True,
    ) is EvidenceClass.INDETERMINATE


def test_identity_mismatch_invalidates_even_apparently_public_content():
    assert classify_access_evidence(
        source=EvidenceSource.BROWSER,
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.PUBLIC_CONTENT,
        purpose=ObservationPurpose.VERIFICATION,
        identity_matches=False,
    ) is EvidenceClass.INVALID_IDENTITY


def test_general_api_probe_can_only_be_suspected_public():
    for source in (EvidenceSource.SERPAPI, EvidenceSource.APIFY, EvidenceSource.BRIGHT_DATA):
        assert classify_access_evidence(
            source=source,
            auth_scope=AuthScope.ANONYMOUS,
            signal=EvidenceSignal.EXPLICIT_PUBLIC,
            purpose=ObservationPurpose.GENERAL_PROBE,
            identity_matches=True,
            contract_explicit_access=True,
        ) is EvidenceClass.SUSPECTED_PUBLIC


def test_anonymous_browser_or_contract_verification_can_be_strong_public():
    assert classify_access_evidence(
        source=EvidenceSource.BROWSER,
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.PUBLIC_CONTENT,
        purpose=ObservationPurpose.VERIFICATION,
        identity_matches=True,
    ) is EvidenceClass.STRONG_PUBLIC
    assert classify_access_evidence(
        source=EvidenceSource.APIFY,
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.EXPLICIT_PUBLIC,
        purpose=ObservationPurpose.VERIFICATION,
        identity_matches=True,
        contract_explicit_access=True,
    ) is EvidenceClass.STRONG_PUBLIC


def test_authenticated_browser_never_proves_public_access():
    assert classify_access_evidence(
        source=EvidenceSource.BROWSER,
        auth_scope=AuthScope.AUTHENTICATED,
        signal=EvidenceSignal.PUBLIC_CONTENT,
        purpose=ObservationPurpose.VERIFICATION,
        identity_matches=True,
        contract_explicit_access=True,
    ) is EvidenceClass.AUTHENTICATED_VISIBLE


def test_only_anonymous_browser_marker_or_explicit_contract_is_strong_private():
    assert classify_access_evidence(
        source=EvidenceSource.BROWSER,
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.EXPLICIT_PRIVATE,
        identity_matches=True,
    ) is EvidenceClass.STRONG_PRIVATE
    assert classify_access_evidence(
        source=EvidenceSource.APIFY,
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.EXPLICIT_PRIVATE,
        identity_matches=True,
        contract_explicit_access=True,
    ) is EvidenceClass.STRONG_PRIVATE
    assert classify_access_evidence(
        source=EvidenceSource.SERPAPI,
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.EXPLICIT_PRIVATE,
        identity_matches=True,
    ) is EvidenceClass.INDETERMINATE
    assert classify_access_evidence(
        source=EvidenceSource.BROWSER,
        auth_scope=AuthScope.AUTHENTICATED,
        signal=EvidenceSignal.EXPLICIT_PRIVATE,
        identity_matches=True,
    ) is EvidenceClass.INDETERMINATE


def test_access_state_reducer_preserves_state_on_indeterminate_and_confirms_strong_public():
    assert next_access_state(
        AccessState.CONFIRMED_PRIVATE,
        EvidenceClass.INDETERMINATE,
        observed_at=NOW,
        source=EvidenceSource.SERPAPI,
    ) is AccessState.CONFIRMED_PRIVATE
    assert next_access_state(
        AccessState.CONFIRMED_PRIVATE,
        EvidenceClass.STRONG_PUBLIC,
        observed_at=NOW,
        source=EvidenceSource.BROWSER,
    ) is AccessState.CONFIRMED_PUBLIC
    assert next_access_state(
        AccessState.UNKNOWN,
        EvidenceClass.AUTHENTICATED_VISIBLE,
        observed_at=NOW,
        source=EvidenceSource.BROWSER,
    ) is AccessState.AUTHENTICATED_VISIBLE


@pytest.mark.parametrize(
    ("minutes", "expected"),
    [
        (29, AccessState.SUSPECTED_PRIVATE),
        (30, AccessState.CONFIRMED_PRIVATE),
        (60, AccessState.CONFIRMED_PRIVATE),
        (61, AccessState.SUSPECTED_PRIVATE),
    ],
)
def test_private_requires_second_strong_observation_in_30_to_60_minute_window(minutes, expected):
    previous = StrongPrivateObservation(NOW, EvidenceSource.BROWSER)
    result = next_access_state(
        AccessState.SUSPECTED_PRIVATE,
        EvidenceClass.STRONG_PRIVATE,
        observed_at=NOW + timedelta(minutes=minutes),
        source=EvidenceSource.APIFY,
        previous_strong_private=previous,
    )
    assert result is expected


def test_first_strong_private_observation_only_suspects_private():
    assert next_access_state(
        AccessState.CONFIRMED_PUBLIC,
        EvidenceClass.STRONG_PRIVATE,
        observed_at=NOW,
        source=EvidenceSource.BROWSER,
    ) is AccessState.SUSPECTED_PRIVATE


def test_probe_schedule_has_exactly_25_serpapi_and_11_apify_slots_per_cycle():
    cycle = [planned_probe_source(slot) for slot in range(36)]
    assert cycle.count(ProbeSource.SERPAPI) == 25
    assert cycle.count(ProbeSource.APIFY) == 11
    assert [planned_probe_source(slot + 36) for slot in range(36)] == cycle
    assert max(
        len(group)
        for group in "".join("A" if item is ProbeSource.APIFY else "S" for item in cycle).split("A")
    ) <= 3


def test_probe_scheduler_rejects_non_integer_slots():
    with pytest.raises(TypeError, match="integer"):
        planned_probe_source(1.5)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="integer"):
        planned_probe_source(True)  # type: ignore[arg-type]


def _apify_slot() -> int:
    return next(slot for slot in range(36) if planned_probe_source(slot) is ProbeSource.APIFY)


def _serpapi_slot() -> int:
    return next(slot for slot in range(36) if planned_probe_source(slot) is ProbeSource.SERPAPI)


def test_apify_frozen_probe_falls_back_to_serpapi_then_bright_data():
    slot = _apify_slot()
    serp = choose_probe_source(
        slot,
        apify_frozen=True,
        serpapi_available=True,
        bright_data_available=True,
    )
    assert serp.planned is ProbeSource.APIFY
    assert serp.selected is ProbeSource.SERPAPI
    assert serp.reason == "apify_frozen"

    bright = choose_probe_source(
        slot,
        apify_frozen=True,
        serpapi_available=False,
        bright_data_available=True,
    )
    assert bright.selected is ProbeSource.BRIGHT_DATA

    degraded = choose_probe_source(
        slot,
        apify_frozen=True,
        serpapi_available=False,
        bright_data_available=False,
    )
    assert degraded.selected is ProbeSource.DEGRADED
    assert degraded.reason == "detection_degraded_apify_frozen"


def test_probe_fallback_never_adds_an_extra_apify_call_to_a_serpapi_slot():
    decision = choose_probe_source(
        _serpapi_slot(),
        apify_frozen=False,
        serpapi_available=False,
        bright_data_available=False,
        apify_available=True,
    )
    assert decision.planned is ProbeSource.SERPAPI
    assert decision.selected is ProbeSource.DEGRADED


def test_unfrozen_apify_slot_uses_apify_and_provider_outage_uses_fallback():
    slot = _apify_slot()
    assert choose_probe_source(
        slot,
        apify_frozen=False,
        serpapi_available=True,
        bright_data_available=True,
    ).selected is ProbeSource.APIFY
    assert choose_probe_source(
        slot,
        apify_frozen=False,
        apify_available=False,
        serpapi_available=False,
        bright_data_available=True,
    ).selected is ProbeSource.BRIGHT_DATA


def test_available_budget_subtracts_official_usage_reserves_and_unsettled_charge_exactly():
    assert available_budget_usd(
        monthly_limit="5.00",
        official_used="1.234",
        outstanding_reserve="2.50",
        unsettled_max_charge="0.125",
    ) == Decimal("1.141")
    assert available_budget_usd(
        monthly_limit=5,
        official_used=6,
        outstanding_reserve=1,
        unsettled_max_charge=1,
    ) == Decimal("0")


def test_budget_decision_never_allows_zero_or_more_than_available():
    kwargs = {
        "monthly_limit": 5,
        "official_used": 2,
        "outstanding_reserve": 2,
        "unsettled_max_charge": Decimal("0.5"),
    }
    allowed = budget_decision("0.5", **kwargs)
    assert allowed.allowed
    assert allowed.available_usd == Decimal("0.5")
    assert not budget_decision("0.5001", **kwargs).allowed
    assert not budget_decision(0, **kwargs).allowed


@pytest.mark.parametrize("field", ["monthly_limit", "official_used", "outstanding_reserve", "unsettled_max_charge"])
def test_budget_rejects_negative_or_non_finite_amounts(field):
    kwargs = {
        "monthly_limit": 5,
        "official_used": 1,
        "outstanding_reserve": 1,
        "unsettled_max_charge": 1,
    }
    kwargs[field] = -1
    with pytest.raises(ValueError, match="non-negative"):
        available_budget_usd(**kwargs)


def _artifact(kind, *, created_at=None, **kwargs):
    return ArtifactRecord(
        artifact_id=kwargs.pop("artifact_id", "artifact"),
        kind=kind,
        created_at=created_at or NOW,
        **kwargs,
    )


def test_successful_raw_artifact_retains_until_90_days_after_epoch_completion():
    completed = NOW - timedelta(days=90)
    before = _artifact(
        ArtifactKind.APIFY_RAW_SUCCESS,
        created_at=NOW - timedelta(days=100),
        epoch_completed_at=completed + timedelta(microseconds=1),
    )
    at_boundary = _artifact(
        ArtifactKind.APIFY_RAW_SUCCESS,
        created_at=NOW - timedelta(days=100),
        epoch_completed_at=completed,
    )
    incomplete = _artifact(
        ArtifactKind.APIFY_RAW_SUCCESS,
        created_at=NOW - timedelta(days=365),
        epoch_completed_at=None,
    )
    assert artifact_retention_decision(before, now=NOW).action is RetentionAction.RETAIN
    assert artifact_retention_decision(at_boundary, now=NOW).action is RetentionAction.DELETE
    assert artifact_retention_decision(incomplete, now=NOW).reason == "capture_not_complete"


def test_unresolved_raw_is_retained_until_reconciliation_closes():
    unresolved = _artifact(ArtifactKind.APIFY_RAW_UNRESOLVED, resolved=False)
    resolved = _artifact(ArtifactKind.APIFY_RAW_UNRESOLVED, resolved=True)
    assert artifact_retention_decision(unresolved, now=NOW).action is RetentionAction.RETAIN
    assert artifact_retention_decision(resolved, now=NOW).action is RetentionAction.DELETE


def test_breaker_evidence_uses_exact_180_day_boundary():
    just_inside = _artifact(
        ArtifactKind.BREAKER_EVIDENCE,
        created_at=NOW - timedelta(days=180) + timedelta(microseconds=1),
    )
    boundary = _artifact(
        ArtifactKind.BREAKER_EVIDENCE,
        created_at=NOW - timedelta(days=180),
    )
    assert artifact_retention_decision(just_inside, now=NOW).action is RetentionAction.RETAIN
    assert artifact_retention_decision(boundary, now=NOW).action is RetentionAction.DELETE


def test_routine_screenshots_keep_latest_ten_and_temp_files_delete_only_after_24_hours():
    assert artifact_retention_decision(
        _artifact(ArtifactKind.ROUTINE_SCREENSHOT, routine_rank=9), now=NOW
    ).action is RetentionAction.RETAIN
    assert artifact_retention_decision(
        _artifact(ArtifactKind.ROUTINE_SCREENSHOT, routine_rank=10), now=NOW
    ).action is RetentionAction.DELETE
    assert artifact_retention_decision(
        _artifact(ArtifactKind.TEMP_PARTIAL, created_at=NOW - timedelta(hours=24)), now=NOW
    ).action is RetentionAction.RETAIN
    assert artifact_retention_decision(
        _artifact(
            ArtifactKind.TEMP_PARTIAL,
            created_at=NOW - timedelta(hours=24, microseconds=1),
        ),
        now=NOW,
    ).action is RetentionAction.DELETE


@pytest.mark.parametrize("kind", [ArtifactKind.BATCH_METADATA, ArtifactKind.PERMANENT_CONTENT])
def test_permanent_artifacts_are_always_retained(kind):
    artifact = _artifact(kind, created_at=NOW - timedelta(days=1000))
    assert artifact_retention_decision(artifact, now=NOW).action is RetentionAction.RETAIN


def test_evidence_cap_evicts_expired_then_oldest_closed_until_within_cap():
    mib = 1024 * 1024
    records = [
        _artifact(
            ArtifactKind.BREAKER_EVIDENCE,
            artifact_id="expired",
            created_at=NOW - timedelta(days=181),
            size_bytes=3 * mib,
        ),
        _artifact(
            ArtifactKind.BREAKER_EVIDENCE,
            artifact_id="old-closed",
            created_at=NOW - timedelta(days=3),
            size_bytes=4 * mib,
        ),
        _artifact(
            ArtifactKind.BREAKER_EVIDENCE,
            artifact_id="new-closed",
            created_at=NOW - timedelta(days=2),
            size_bytes=4 * mib,
        ),
        _artifact(
            ArtifactKind.BREAKER_EVIDENCE,
            artifact_id="open",
            created_at=NOW - timedelta(days=4),
            size_bytes=4 * mib,
            closed=False,
        ),
    ]
    decision = evidence_cap_evictions(records, now=NOW, cap_bytes=10 * mib)
    assert decision.delete_ids == ("expired", "old-closed")
    assert decision.remaining_bytes == 8 * mib
    assert decision.cap_satisfied


def test_evidence_at_exact_cap_is_retained_and_open_overflow_is_reported():
    mib = 1024 * 1024
    exact = _artifact(
        ArtifactKind.BREAKER_EVIDENCE,
        artifact_id="exact",
        size_bytes=500 * mib,
        closed=False,
    )
    decision = evidence_cap_evictions([exact], now=NOW)
    assert decision.delete_ids == ()
    assert decision.cap_satisfied

    overflow = _artifact(
        ArtifactKind.BREAKER_EVIDENCE,
        artifact_id="overflow",
        size_bytes=1,
        closed=False,
    )
    blocked = evidence_cap_evictions([exact, overflow], now=NOW)
    assert blocked.delete_ids == ()
    assert blocked.remaining_bytes == 500 * mib + 1
    assert blocked.cap_satisfied is False


def test_artifact_records_validate_paths_inputs_independently_of_storage():
    with pytest.raises(ValueError, match="timezone-aware"):
        ArtifactRecord(
            artifact_id="bad-time",
            kind=ArtifactKind.BREAKER_EVIDENCE,
            created_at=datetime(2026, 1, 1),
        )
    with pytest.raises(ValueError, match="non-negative"):
        _artifact(ArtifactKind.BREAKER_EVIDENCE, size_bytes=-1)
    with pytest.raises(ValueError, match="earlier"):
        artifact_retention_decision(
            _artifact(ArtifactKind.BREAKER_EVIDENCE, created_at=NOW + timedelta(seconds=1)),
            now=NOW,
        )
