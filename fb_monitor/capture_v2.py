from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Iterable, Mapping, Sequence


class CoverageStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    SOURCE_LIMITED = "source_limited"
    BUDGET_PAUSED = "budget_paused"
    MANUAL_PAUSED = "manual_paused"
    FAILED = "failed"


class AccessState(StrEnum):
    UNKNOWN = "unknown"
    AUTHENTICATED_VISIBLE = "authenticated_visible"
    SUSPECTED_PUBLIC = "suspected_public"
    CONFIRMED_PUBLIC = "confirmed_public"
    SUSPECTED_PRIVATE = "suspected_private"
    CONFIRMED_PRIVATE = "confirmed_private"


class BatchStatus(StrEnum):
    PREPARED = "prepared"
    LAUNCHING = "launching"
    RUN_STARTED = "run_started"
    NEEDS_RECONCILE = "needs_reconcile"
    RAW_SAVED = "raw_saved"
    IMPORT_FAILED = "import_failed"
    IMPORTED = "imported"
    COMMITTED = "committed"
    FAILED = "failed"


class ContractStatus(StrEnum):
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    EXPIRED = "expired"
    DISABLED = "disabled"


class CoverageStream(StrEnum):
    POSTS = "posts"
    MEDIA = "media"
    COMMENTS = "comments"


class CoverageSurface(StrEnum):
    TIMELINE_POSTS = "timeline_posts"
    POST_COMMENTS = "post_comments"
    REELS = "reels"
    VIDEOS = "videos"
    POST_ALBUMS = "post_albums"
    PUBLIC_PHOTO_PAGES = "public_photo_pages"
    AVATAR_HISTORY = "avatar_history"
    COVER_HISTORY = "cover_history"


class CaptureIntent(StrEnum):
    INITIAL_PUBLIC_CAPTURE = "initial_public_capture"
    RECOVERY_CAPTURE = "recovery_capture"
    INCREMENTAL_POLL = "incremental_poll"
    MONTHLY_AUDIT = "monthly_audit"
    CONTRACT_TEST = "contract_test"
    MANUAL_CONTINUE = "manual_continue"
    ACCESS_PROBE = "access_probe"


class AuthScope(StrEnum):
    ANONYMOUS = "anonymous"
    AUTHENTICATED = "authenticated"


class EvidenceSource(StrEnum):
    SERPAPI = "serpapi"
    APIFY = "apify"
    BRIGHT_DATA = "bright_data"
    BROWSER = "browser"


class EvidenceSignal(StrEnum):
    PUBLIC_CONTENT = "public_content"
    EXPLICIT_PUBLIC = "explicit_public"
    EXPLICIT_PRIVATE = "explicit_private"
    EMPTY = "empty"
    NO_ITEMS = "no_items"
    TIMEOUT = "timeout"
    LOGIN_WALL = "login_wall"
    PARSE_ERROR = "parse_error"
    HTTP_ERROR = "http_error"


class ObservationPurpose(StrEnum):
    GENERAL_PROBE = "general_probe"
    VERIFICATION = "verification"


class EvidenceClass(StrEnum):
    INVALID_IDENTITY = "invalid_identity"
    INDETERMINATE = "indeterminate"
    AUTHENTICATED_VISIBLE = "authenticated_visible"
    SUSPECTED_PUBLIC = "suspected_public"
    STRONG_PUBLIC = "strong_public"
    STRONG_PRIVATE = "strong_private"


class ProbeSource(StrEnum):
    SERPAPI = "serpapi"
    APIFY = "apify"
    BRIGHT_DATA = "bright_data"
    DEGRADED = "degraded"


class DuplicateReason(StrEnum):
    SAME_CURSOR = "same_cursor"
    SAME_IDENTITIES = "same_identities"


class ArtifactKind(StrEnum):
    APIFY_RAW_SUCCESS = "apify_raw_success"
    APIFY_RAW_UNRESOLVED = "apify_raw_unresolved"
    BATCH_METADATA = "batch_metadata"
    BREAKER_EVIDENCE = "breaker_evidence"
    ROUTINE_SCREENSHOT = "routine_screenshot"
    TEMP_PARTIAL = "temp_partial"
    PERMANENT_CONTENT = "permanent_content"


class RetentionAction(StrEnum):
    RETAIN = "retain"
    DELETE = "delete"


_BATCH_TRANSITIONS: dict[BatchStatus, frozenset[BatchStatus]] = {
    BatchStatus.PREPARED: frozenset({BatchStatus.LAUNCHING, BatchStatus.FAILED}),
    BatchStatus.LAUNCHING: frozenset(
        {BatchStatus.RUN_STARTED, BatchStatus.NEEDS_RECONCILE, BatchStatus.FAILED}
    ),
    BatchStatus.RUN_STARTED: frozenset(
        {BatchStatus.RAW_SAVED, BatchStatus.NEEDS_RECONCILE, BatchStatus.FAILED}
    ),
    BatchStatus.NEEDS_RECONCILE: frozenset(
        {BatchStatus.RUN_STARTED, BatchStatus.RAW_SAVED, BatchStatus.FAILED}
    ),
    BatchStatus.RAW_SAVED: frozenset({BatchStatus.IMPORTED, BatchStatus.IMPORT_FAILED}),
    BatchStatus.IMPORT_FAILED: frozenset({BatchStatus.IMPORTED, BatchStatus.FAILED}),
    BatchStatus.IMPORTED: frozenset({BatchStatus.COMMITTED, BatchStatus.IMPORT_FAILED}),
    BatchStatus.COMMITTED: frozenset(),
    BatchStatus.FAILED: frozenset(),
}


_CONTRACT_TRANSITIONS: dict[ContractStatus, frozenset[ContractStatus]] = {
    ContractStatus.PENDING: frozenset(
        {ContractStatus.PASSED, ContractStatus.FAILED, ContractStatus.DISABLED}
    ),
    ContractStatus.PASSED: frozenset(
        {ContractStatus.EXPIRED, ContractStatus.FAILED, ContractStatus.DISABLED}
    ),
    ContractStatus.FAILED: frozenset({ContractStatus.PENDING, ContractStatus.DISABLED}),
    ContractStatus.EXPIRED: frozenset({ContractStatus.PENDING, ContractStatus.DISABLED}),
    ContractStatus.DISABLED: frozenset({ContractStatus.PENDING}),
}


_COVERAGE_TRANSITIONS: dict[CoverageStatus, frozenset[CoverageStatus]] = {
    CoverageStatus.PENDING: frozenset(
        {
            CoverageStatus.IN_PROGRESS,
            CoverageStatus.SOURCE_LIMITED,
            CoverageStatus.BUDGET_PAUSED,
            CoverageStatus.MANUAL_PAUSED,
            CoverageStatus.FAILED,
        }
    ),
    CoverageStatus.IN_PROGRESS: frozenset(
        {
            CoverageStatus.COMPLETE,
            CoverageStatus.SOURCE_LIMITED,
            CoverageStatus.BUDGET_PAUSED,
            CoverageStatus.MANUAL_PAUSED,
            CoverageStatus.FAILED,
        }
    ),
    CoverageStatus.SOURCE_LIMITED: frozenset(
        {CoverageStatus.IN_PROGRESS, CoverageStatus.COMPLETE, CoverageStatus.FAILED}
    ),
    CoverageStatus.BUDGET_PAUSED: frozenset(
        {CoverageStatus.IN_PROGRESS, CoverageStatus.MANUAL_PAUSED, CoverageStatus.FAILED}
    ),
    CoverageStatus.MANUAL_PAUSED: frozenset(
        {CoverageStatus.IN_PROGRESS, CoverageStatus.BUDGET_PAUSED, CoverageStatus.FAILED}
    ),
    CoverageStatus.FAILED: frozenset({CoverageStatus.IN_PROGRESS}),
    CoverageStatus.COMPLETE: frozenset(),
}


def _as_enum(value: Any, enum_type: type[StrEnum], field: str) -> StrEnum:
    try:
        return value if isinstance(value, enum_type) else enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} has an unsupported value: {value!r}") from exc


def validate_coverage_status(
    status: CoverageStatus | str,
    *,
    terminal_evidence: Any = None,
    reason: str | None = None,
) -> CoverageStatus:
    normalized = _as_enum(status, CoverageStatus, "coverage status")
    assert isinstance(normalized, CoverageStatus)
    if isinstance(terminal_evidence, str):
        has_evidence = bool(terminal_evidence.strip())
    elif isinstance(terminal_evidence, (Mapping, Sequence)):
        has_evidence = bool(terminal_evidence)
    else:
        has_evidence = terminal_evidence is not None
    explanation = str(reason or "").strip()
    if normalized is CoverageStatus.COMPLETE and not has_evidence:
        raise ValueError("complete coverage requires terminal evidence")
    if normalized is not CoverageStatus.COMPLETE and has_evidence:
        raise ValueError("terminal evidence may only be attached to complete coverage")
    if normalized in {
        CoverageStatus.SOURCE_LIMITED,
        CoverageStatus.BUDGET_PAUSED,
        CoverageStatus.MANUAL_PAUSED,
        CoverageStatus.FAILED,
    } and not explanation:
        raise ValueError(f"{normalized.value} coverage requires a reason")
    return normalized


def validate_coverage_transition(
    current: CoverageStatus | str,
    target: CoverageStatus | str,
    *,
    terminal_evidence: Any = None,
    reason: str | None = None,
) -> CoverageStatus:
    before = _as_enum(current, CoverageStatus, "current coverage status")
    after = validate_coverage_status(
        target,
        terminal_evidence=terminal_evidence,
        reason=reason,
    )
    assert isinstance(before, CoverageStatus)
    if before != after and after not in _COVERAGE_TRANSITIONS[before]:
        raise ValueError(f"invalid coverage transition: {before.value} -> {after.value}")
    return after


def validate_batch_transition(
    current: BatchStatus | str,
    target: BatchStatus | str,
) -> BatchStatus:
    before = _as_enum(current, BatchStatus, "current batch status")
    after = _as_enum(target, BatchStatus, "target batch status")
    assert isinstance(before, BatchStatus)
    assert isinstance(after, BatchStatus)
    if before != after and after not in _BATCH_TRANSITIONS[before]:
        raise ValueError(f"invalid batch transition: {before.value} -> {after.value}")
    return after


def validate_contract_transition(
    current: ContractStatus | str,
    target: ContractStatus | str,
) -> ContractStatus:
    before = _as_enum(current, ContractStatus, "current contract status")
    after = _as_enum(target, ContractStatus, "target contract status")
    assert isinstance(before, ContractStatus)
    assert isinstance(after, ContractStatus)
    if before != after and after not in _CONTRACT_TRANSITIONS[before]:
        raise ValueError(f"invalid contract transition: {before.value} -> {after.value}")
    return after


@dataclass(frozen=True, slots=True)
class ContractDecision:
    allowed: bool
    reason: str


def validate_contract_for_paid_run(
    *,
    status: ContractStatus | str,
    fingerprint: str,
    expected_fingerprint: str,
    now: datetime,
    expires_at: datetime | None = None,
) -> ContractDecision:
    normalized = _as_enum(status, ContractStatus, "contract status")
    assert isinstance(normalized, ContractStatus)
    _require_aware(now, "now")
    if normalized is not ContractStatus.PASSED:
        return ContractDecision(False, f"contract_{normalized.value}")
    if not fingerprint.strip() or fingerprint.strip() != expected_fingerprint.strip():
        return ContractDecision(False, "contract_fingerprint_mismatch")
    if expires_at is not None:
        _require_aware(expires_at, "expires_at")
        if now.astimezone(UTC) >= expires_at.astimezone(UTC):
            return ContractDecision(False, "contract_expired")
    return ContractDecision(True, "contract_passed")


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")


def _utc_text(value: datetime) -> str:
    _require_aware(value, "datetime")
    text = value.astimezone(UTC).isoformat(timespec="microseconds")
    return text[:-6] + "Z" if text.endswith("+00:00") else text


@dataclass(frozen=True, slots=True)
class ObservationWindow:
    start_at: datetime
    end_at: datetime

    def __post_init__(self) -> None:
        _require_aware(self.start_at, "start_at")
        _require_aware(self.end_at, "end_at")
        if self.end_at.astimezone(UTC) <= self.start_at.astimezone(UTC):
            raise ValueError("observation window end must be after start")

    @property
    def key(self) -> str:
        return f"{_utc_text(self.start_at)}/{_utc_text(self.end_at)}"

    def as_json(self) -> dict[str, str]:
        return {"start_at": _utc_text(self.start_at), "end_at": _utc_text(self.end_at)}


def deterministic_observation_window(
    observed_at: datetime,
    duration: timedelta,
    *,
    anchor: datetime = datetime(1970, 1, 1, tzinfo=UTC),
) -> ObservationWindow:
    _require_aware(observed_at, "observed_at")
    _require_aware(anchor, "anchor")
    duration_us = (
        duration.days * 86_400_000_000
        + duration.seconds * 1_000_000
        + duration.microseconds
    )
    if duration_us <= 0:
        raise ValueError("duration must be positive")
    observed_utc = observed_at.astimezone(UTC)
    anchor_utc = anchor.astimezone(UTC)
    delta = observed_utc - anchor_utc
    delta_us = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    slot = delta_us // duration_us
    start = anchor_utc + timedelta(microseconds=slot * duration_us)
    return ObservationWindow(start, start + duration)


def _normalize_json(value: Any, *, path: str = "$") -> Any:
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite number at {path}")
        if value == 0:
            return 0
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for raw_key, child in value.items():
            if not isinstance(raw_key, str):
                raise TypeError(f"JSON object key at {path} must be a string")
            key = unicodedata.normalize("NFC", raw_key)
            if key in normalized:
                raise ValueError(f"duplicate normalized JSON key at {path}: {key!r}")
            normalized[key] = _normalize_json(child, path=f"{path}.{key}")
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json(child, path=f"{path}[{index}]") for index, child in enumerate(value)]
    raise TypeError(f"unsupported JSON value at {path}: {type(value).__name__}")


def normalized_input(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _normalize_json(value)
    assert isinstance(normalized, dict)
    return normalized


def canonical_input_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        normalized_input(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def request_hash(
    *,
    capture_intent: CaptureIntent | str,
    window: ObservationWindow,
    profile_id: int | str,
    epoch_id: int | str,
    stream: CoverageStream | str,
    surface: CoverageSurface | str,
    contract_fingerprint: str,
    actor_input: Mapping[str, Any],
) -> str:
    intent = _as_enum(capture_intent, CaptureIntent, "capture intent")
    stream_value = _as_enum(stream, CoverageStream, "coverage stream")
    surface_value = _as_enum(surface, CoverageSurface, "coverage surface")
    profile = str(profile_id).strip()
    epoch = str(epoch_id).strip()
    fingerprint = str(contract_fingerprint).strip()
    if not profile or not epoch or not fingerprint:
        raise ValueError("profile_id, epoch_id, and contract_fingerprint are required")
    payload = {
        "capture_intent": intent.value,
        "contract_fingerprint": fingerprint,
        "epoch_id": epoch,
        "input": normalized_input(actor_input),
        "observation_window": window.as_json(),
        "profile_id": profile,
        "stream": stream_value.value,
        "surface": surface_value.value,
    }
    encoded = canonical_input_json(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _identity_set(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                unicodedata.normalize("NFC", str(value)).strip()
                for value in values
                if str(value).strip()
            }
        )
    )


@dataclass(frozen=True, slots=True)
class DuplicatePageDecision:
    tripped: bool
    reasons: tuple[DuplicateReason, ...]
    current_identity_hash: str


def duplicate_page_circuit_breaker(
    *,
    previous_cursor: str | None,
    previous_identities: Iterable[str],
    current_cursor: str | None,
    current_identities: Iterable[str],
) -> DuplicatePageDecision:
    before = _identity_set(previous_identities)
    current = _identity_set(current_identities)
    old_cursor = str(previous_cursor or "").strip()
    new_cursor = str(current_cursor or "").strip()
    reasons: list[DuplicateReason] = []
    if old_cursor and new_cursor and old_cursor == new_cursor:
        reasons.append(DuplicateReason.SAME_CURSOR)
    if before and current and before == current:
        reasons.append(DuplicateReason.SAME_IDENTITIES)
    identity_hash = hashlib.sha256(
        canonical_input_json({"identities": list(current)}).encode("utf-8")
    ).hexdigest()
    return DuplicatePageDecision(bool(reasons), tuple(reasons), identity_hash)


def classify_access_evidence(
    *,
    source: EvidenceSource | str,
    auth_scope: AuthScope | str,
    signal: EvidenceSignal | str,
    purpose: ObservationPurpose | str = ObservationPurpose.GENERAL_PROBE,
    identity_matches: bool,
    contract_explicit_access: bool = False,
) -> EvidenceClass:
    evidence_source = _as_enum(source, EvidenceSource, "evidence source")
    scope = _as_enum(auth_scope, AuthScope, "auth scope")
    observed = _as_enum(signal, EvidenceSignal, "evidence signal")
    observation_purpose = _as_enum(purpose, ObservationPurpose, "observation purpose")
    assert isinstance(evidence_source, EvidenceSource)
    assert isinstance(scope, AuthScope)
    assert isinstance(observed, EvidenceSignal)
    assert isinstance(observation_purpose, ObservationPurpose)
    if not identity_matches:
        return EvidenceClass.INVALID_IDENTITY
    if observed in {
        EvidenceSignal.EMPTY,
        EvidenceSignal.NO_ITEMS,
        EvidenceSignal.TIMEOUT,
        EvidenceSignal.LOGIN_WALL,
        EvidenceSignal.PARSE_ERROR,
        EvidenceSignal.HTTP_ERROR,
    }:
        return EvidenceClass.INDETERMINATE
    if observed is EvidenceSignal.EXPLICIT_PRIVATE:
        if scope is AuthScope.ANONYMOUS and (
            evidence_source is EvidenceSource.BROWSER or contract_explicit_access
        ):
            return EvidenceClass.STRONG_PRIVATE
        return EvidenceClass.INDETERMINATE
    if scope is AuthScope.AUTHENTICATED:
        return EvidenceClass.AUTHENTICATED_VISIBLE
    if observation_purpose is ObservationPurpose.GENERAL_PROBE:
        return EvidenceClass.SUSPECTED_PUBLIC
    if evidence_source is EvidenceSource.BROWSER or contract_explicit_access:
        return EvidenceClass.STRONG_PUBLIC
    return EvidenceClass.SUSPECTED_PUBLIC


@dataclass(frozen=True, slots=True)
class StrongPrivateObservation:
    observed_at: datetime
    source: EvidenceSource

    def __post_init__(self) -> None:
        _require_aware(self.observed_at, "observed_at")


def next_access_state(
    current: AccessState | str,
    evidence: EvidenceClass | str,
    *,
    observed_at: datetime,
    source: EvidenceSource | str,
    previous_strong_private: StrongPrivateObservation | None = None,
    private_confirmation_min: timedelta = timedelta(minutes=30),
    private_confirmation_max: timedelta = timedelta(minutes=60),
) -> AccessState:
    state = _as_enum(current, AccessState, "access state")
    classification = _as_enum(evidence, EvidenceClass, "evidence class")
    evidence_source = _as_enum(source, EvidenceSource, "evidence source")
    assert isinstance(state, AccessState)
    assert isinstance(classification, EvidenceClass)
    assert isinstance(evidence_source, EvidenceSource)
    _require_aware(observed_at, "observed_at")
    if private_confirmation_min < timedelta(0) or private_confirmation_max < private_confirmation_min:
        raise ValueError("invalid private confirmation window")
    if classification in {EvidenceClass.INVALID_IDENTITY, EvidenceClass.INDETERMINATE}:
        return state
    if classification is EvidenceClass.STRONG_PUBLIC:
        return AccessState.CONFIRMED_PUBLIC
    if classification is EvidenceClass.SUSPECTED_PUBLIC:
        if state is AccessState.CONFIRMED_PUBLIC:
            return state
        return AccessState.SUSPECTED_PUBLIC
    if classification is EvidenceClass.AUTHENTICATED_VISIBLE:
        if state in {AccessState.UNKNOWN, AccessState.AUTHENTICATED_VISIBLE}:
            return AccessState.AUTHENTICATED_VISIBLE
        return state
    if classification is EvidenceClass.STRONG_PRIVATE:
        if state is AccessState.CONFIRMED_PRIVATE:
            return state
        if previous_strong_private is not None:
            elapsed = observed_at.astimezone(UTC) - previous_strong_private.observed_at.astimezone(UTC)
            if private_confirmation_min <= elapsed <= private_confirmation_max:
                return AccessState.CONFIRMED_PRIVATE
        return AccessState.SUSPECTED_PRIVATE
    raise AssertionError(f"unhandled evidence class {classification}")


def planned_probe_source(slot_index: int) -> ProbeSource:
    if not isinstance(slot_index, int) or isinstance(slot_index, bool):
        raise TypeError("slot_index must be an integer")
    slot = slot_index % 36
    apify_before = (slot * 11) // 36
    apify_after = ((slot + 1) * 11) // 36
    return ProbeSource.APIFY if apify_after > apify_before else ProbeSource.SERPAPI


@dataclass(frozen=True, slots=True)
class ProbeDecision:
    planned: ProbeSource
    selected: ProbeSource
    reason: str

    @property
    def degraded(self) -> bool:
        return self.selected is ProbeSource.DEGRADED


def choose_probe_source(
    slot_index: int,
    *,
    apify_frozen: bool,
    serpapi_available: bool,
    bright_data_available: bool,
    apify_available: bool = True,
) -> ProbeDecision:
    planned = planned_probe_source(slot_index)
    if planned is ProbeSource.SERPAPI:
        if serpapi_available:
            return ProbeDecision(planned, ProbeSource.SERPAPI, "scheduled")
        if bright_data_available:
            return ProbeDecision(planned, ProbeSource.BRIGHT_DATA, "serpapi_unavailable")
        return ProbeDecision(planned, ProbeSource.DEGRADED, "detection_degraded_serpapi_unavailable")
    if not apify_frozen and apify_available:
        return ProbeDecision(planned, ProbeSource.APIFY, "scheduled")
    if serpapi_available:
        reason = "apify_frozen" if apify_frozen else "apify_unavailable"
        return ProbeDecision(planned, ProbeSource.SERPAPI, reason)
    if bright_data_available:
        reason = "apify_frozen" if apify_frozen else "apify_unavailable"
        return ProbeDecision(planned, ProbeSource.BRIGHT_DATA, reason)
    degraded_reason = (
        "detection_degraded_apify_frozen"
        if apify_frozen
        else "detection_degraded_apify_unavailable"
    )
    return ProbeDecision(planned, ProbeSource.DEGRADED, degraded_reason)


def _money(value: Decimal | int | float | str, field: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite non-negative amount") from exc
    if not result.is_finite() or result < 0:
        raise ValueError(f"{field} must be a finite non-negative amount")
    return result


def available_budget_usd(
    *,
    monthly_limit: Decimal | int | float | str,
    official_used: Decimal | int | float | str,
    outstanding_reserve: Decimal | int | float | str = 0,
    unsettled_max_charge: Decimal | int | float | str = 0,
) -> Decimal:
    limit = _money(monthly_limit, "monthly_limit")
    used = _money(official_used, "official_used")
    reserved = _money(outstanding_reserve, "outstanding_reserve")
    unsettled = _money(unsettled_max_charge, "unsettled_max_charge")
    return max(Decimal("0"), limit - used - reserved - unsettled)


@dataclass(frozen=True, slots=True)
class BudgetDecision:
    allowed: bool
    available_usd: Decimal
    requested_usd: Decimal
    reason: str


def budget_decision(
    requested_usd: Decimal | int | float | str,
    *,
    monthly_limit: Decimal | int | float | str,
    official_used: Decimal | int | float | str,
    outstanding_reserve: Decimal | int | float | str = 0,
    unsettled_max_charge: Decimal | int | float | str = 0,
) -> BudgetDecision:
    requested = _money(requested_usd, "requested_usd")
    available = available_budget_usd(
        monthly_limit=monthly_limit,
        official_used=official_used,
        outstanding_reserve=outstanding_reserve,
        unsettled_max_charge=unsettled_max_charge,
    )
    allowed = requested > 0 and requested <= available
    reason = "budget_available" if allowed else "budget_paused"
    return BudgetDecision(allowed, available, requested, reason)


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    artifact_id: str
    kind: ArtifactKind
    created_at: datetime
    size_bytes: int = 0
    epoch_completed_at: datetime | None = None
    committed: bool = True
    resolved: bool = True
    closed: bool = True
    routine_rank: int | None = None

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")
        if self.epoch_completed_at is not None:
            _require_aware(self.epoch_completed_at, "epoch_completed_at")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        if self.routine_rank is not None and self.routine_rank < 0:
            raise ValueError("routine_rank must be non-negative")


@dataclass(frozen=True, slots=True)
class RetentionDecision:
    action: RetentionAction
    reason: str


def artifact_retention_decision(
    artifact: ArtifactRecord,
    *,
    now: datetime,
) -> RetentionDecision:
    _require_aware(now, "now")
    age = now.astimezone(UTC) - artifact.created_at.astimezone(UTC)
    if age < timedelta(0):
        raise ValueError("now must not be earlier than artifact creation")
    if artifact.kind in {ArtifactKind.PERMANENT_CONTENT, ArtifactKind.BATCH_METADATA}:
        return RetentionDecision(RetentionAction.RETAIN, "permanent")
    if artifact.kind is ArtifactKind.APIFY_RAW_SUCCESS:
        if not artifact.committed or artifact.epoch_completed_at is None:
            return RetentionDecision(RetentionAction.RETAIN, "capture_not_complete")
        completed_age = now.astimezone(UTC) - artifact.epoch_completed_at.astimezone(UTC)
        if completed_age < timedelta(0):
            raise ValueError("now must not be earlier than epoch completion")
        if completed_age < timedelta(days=90):
            return RetentionDecision(RetentionAction.RETAIN, "raw_90_day_window")
        return RetentionDecision(RetentionAction.DELETE, "raw_retention_expired")
    if artifact.kind is ArtifactKind.APIFY_RAW_UNRESOLVED:
        if not artifact.resolved:
            return RetentionDecision(RetentionAction.RETAIN, "unresolved_or_disputed")
        return RetentionDecision(RetentionAction.DELETE, "reconciliation_closed")
    if artifact.kind is ArtifactKind.BREAKER_EVIDENCE:
        if age < timedelta(days=180):
            return RetentionDecision(RetentionAction.RETAIN, "evidence_180_day_window")
        return RetentionDecision(RetentionAction.DELETE, "evidence_retention_expired")
    if artifact.kind is ArtifactKind.ROUTINE_SCREENSHOT:
        if artifact.routine_rank is None or artifact.routine_rank < 10:
            return RetentionDecision(RetentionAction.RETAIN, "latest_ten_per_profile")
        return RetentionDecision(RetentionAction.DELETE, "routine_screenshot_superseded")
    if artifact.kind is ArtifactKind.TEMP_PARTIAL:
        if age <= timedelta(hours=24):
            return RetentionDecision(RetentionAction.RETAIN, "temporary_24_hour_window")
        return RetentionDecision(RetentionAction.DELETE, "temporary_file_stale")
    raise AssertionError(f"unhandled artifact kind {artifact.kind}")


@dataclass(frozen=True, slots=True)
class EvidenceCapDecision:
    delete_ids: tuple[str, ...]
    remaining_bytes: int
    cap_satisfied: bool


def evidence_cap_evictions(
    artifacts: Sequence[ArtifactRecord],
    *,
    now: datetime,
    cap_bytes: int = 500 * 1024 * 1024,
) -> EvidenceCapDecision:
    _require_aware(now, "now")
    if cap_bytes < 0:
        raise ValueError("cap_bytes must be non-negative")
    evidence = [item for item in artifacts if item.kind is ArtifactKind.BREAKER_EVIDENCE]
    expired_ids = {
        item.artifact_id
        for item in evidence
        if artifact_retention_decision(item, now=now).action is RetentionAction.DELETE
    }
    remaining = sum(item.size_bytes for item in evidence if item.artifact_id not in expired_ids)
    delete_ids = list(
        item.artifact_id
        for item in sorted(evidence, key=lambda value: (value.created_at, value.artifact_id))
        if item.artifact_id in expired_ids
    )
    if remaining > cap_bytes:
        candidates = sorted(
            (
                item
                for item in evidence
                if item.artifact_id not in expired_ids and item.closed
            ),
            key=lambda value: (value.created_at, value.artifact_id),
        )
        for item in candidates:
            if remaining <= cap_bytes:
                break
            delete_ids.append(item.artifact_id)
            remaining -= item.size_bytes
    return EvidenceCapDecision(tuple(delete_ids), remaining, remaining <= cap_bytes)


__all__ = [
    "AccessState",
    "ArtifactKind",
    "ArtifactRecord",
    "AuthScope",
    "BatchStatus",
    "BudgetDecision",
    "CaptureIntent",
    "ContractDecision",
    "ContractStatus",
    "CoverageStatus",
    "CoverageStream",
    "CoverageSurface",
    "DuplicatePageDecision",
    "DuplicateReason",
    "EvidenceCapDecision",
    "EvidenceClass",
    "EvidenceSignal",
    "EvidenceSource",
    "ObservationPurpose",
    "ObservationWindow",
    "ProbeDecision",
    "ProbeSource",
    "RetentionAction",
    "RetentionDecision",
    "StrongPrivateObservation",
    "artifact_retention_decision",
    "available_budget_usd",
    "budget_decision",
    "canonical_input_json",
    "choose_probe_source",
    "classify_access_evidence",
    "deterministic_observation_window",
    "duplicate_page_circuit_breaker",
    "evidence_cap_evictions",
    "next_access_state",
    "normalized_input",
    "planned_probe_source",
    "request_hash",
    "validate_batch_transition",
    "validate_contract_for_paid_run",
    "validate_contract_transition",
    "validate_coverage_status",
    "validate_coverage_transition",
]
