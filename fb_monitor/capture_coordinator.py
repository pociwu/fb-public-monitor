from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .capture_v2 import CoverageStatus, CoverageStream, CoverageSurface
from .db import Database
from .media import extract_media, media_representation_key
from .normalize import normalize_url


_EXPECTED_MEDIA_KEYS = {
    "attachmentcount",
    "imagecount",
    "imagescount",
    "mediacount",
    "photoscount",
    "photocount",
    "totalattachments",
    "totalimages",
    "totalmedia",
    "totalphotos",
}
_COMPLETE_BOOLEAN_KEYS = {
    "allmediaexpanded",
    "allphotosexpanded",
    "albumcomplete",
    "mediacomplete",
    "mediaexpanded",
    "photoscomplete",
    "photosexpanded",
}
_HAS_MORE_KEYS = {"hasmoremedia", "hasmorephotos", "hasnextmedia", "hasnextphoto"}
_CURSOR_KEYS = {
    "gallerycursor",
    "mediacursor",
    "nextgallerycursor",
    "nextmediacursor",
    "nextphotocursor",
    "photocursor",
}
_STATUS_KEYS = {
    "albumcoveragestatus",
    "albumstatus",
    "mediacoveragestatus",
    "mediastatus",
    "photocoveragestatus",
    "photostatus",
}
_COMPLETE_STATUSES = {
    "complete",
    "complete_album_exhausted",
    "complete_media_exhausted",
    "exhausted",
    "no_media",
    "no_photos",
}
_CAPPED_STATUSES = {
    "capped",
    "limit_reached",
    "partial",
    "partial_actor_limit",
    "source_limited",
    "target_reached",
}
_FAILED_STATUSES = {"error", "failed", "failure", "timeout"}
_COMMENT_BRANCH_KEYS = {"comments", "replies", "topcomments"}


def _key(value: object) -> str:
    return str(value).casefold().replace("_", "").replace("-", "")


def _walk(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            normalized_key = _key(raw_key)
            # Comment attachments and their counters belong to the independent
            # per-post comments stream.  Letting nested comment metadata leak
            # into this walk can make a post album appear capped/incomplete.
            if normalized_key in _COMMENT_BRANCH_KEYS:
                continue
            yield normalized_key, child
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


@dataclass(frozen=True, slots=True)
class MediaCoverageDecision:
    status: CoverageStatus
    seen_media_ids: tuple[str, ...]
    expected_count: int | None
    terminal_evidence: dict[str, Any]
    reason: str | None


@dataclass(frozen=True, slots=True)
class CommentCheckpoint:
    coverage_stream_id: int
    post_entity_id: int
    post_external_id: str
    post_url: str


@dataclass(frozen=True, slots=True)
class EpochResolution:
    ready: bool
    status: str
    reason: str


def evaluate_post_media(
    item: dict[str, Any],
    *,
    ready_source_urls: Iterable[str] = (),
    contract_verified: bool = False,
) -> MediaCoverageDecision:
    """Classify one post's Actor media without inferring a false terminal.

    Actor attachments are already downloaded by ``Ingester`` before this is
    called.  A finite list by itself is not proof that an album ended: the
    provider must declare a count/terminal marker and every listed attachment
    must have a ready local copy.
    """

    media_ids = tuple(
        dict.fromkeys(
            normalized
            for ref in extract_media(item, "post")
            if (normalized := normalize_url(ref.url))
        )
    )
    # Readiness is representation-specific.  A ready 24px thumbnail for one
    # CDN object must not make a newly requested 720px representation look
    # successful merely because both URLs collapse to the same object key.
    ready = {media_representation_key(str(value)) for value in ready_source_urls if value}
    missing_downloads = tuple(
        ref.url
        for ref in extract_media(item, "post")
        if media_representation_key(ref.url) not in ready
    )
    expected_values: list[int] = []
    explicit_complete: list[dict[str, Any]] = []
    has_more = False
    cursor = ""
    capped = ""
    failed = ""

    for name, value in _walk(item):
        if name in _EXPECTED_MEDIA_KEYS and (number := _nonnegative_int(value)) is not None:
            expected_values.append(number)
        elif name in _COMPLETE_BOOLEAN_KEYS and value is True:
            explicit_complete.append({"field": name, "value": True})
        elif name in _HAS_MORE_KEYS:
            if value is False:
                explicit_complete.append({"field": name, "value": False})
            elif value is True:
                has_more = True
        elif name in _CURSOR_KEYS and value not in (None, "", False):
            cursor = str(value)
        elif name in _STATUS_KEYS and value not in (None, ""):
            status = str(value).strip().casefold()
            if status in _COMPLETE_STATUSES:
                explicit_complete.append({"field": name, "value": status})
            elif status in _CAPPED_STATUSES or status.startswith("partial"):
                capped = status
            elif status in _FAILED_STATUSES:
                failed = status

    expected_count = max(expected_values) if expected_values else None
    if failed:
        return MediaCoverageDecision(
            CoverageStatus.FAILED,
            media_ids,
            expected_count,
            {},
            f"Actor media 狀態失敗：{failed}",
        )
    if capped:
        return MediaCoverageDecision(
            CoverageStatus.SOURCE_LIMITED,
            media_ids,
            expected_count,
            {},
            f"Actor media 結果受限：{capped}",
        )
    if has_more or cursor:
        detail = f"，cursor={cursor}" if cursor else ""
        return MediaCoverageDecision(
            CoverageStatus.SOURCE_LIMITED,
            media_ids,
            expected_count,
            {},
            f"Actor media 尚有下一頁但此來源未接續{detail}",
        )
    if expected_count is not None and len(media_ids) != expected_count:
        return MediaCoverageDecision(
            CoverageStatus.SOURCE_LIMITED,
            media_ids,
            expected_count,
            {},
            f"Actor 宣告 {expected_count} 個媒體，但本批列出 {len(media_ids)} 個",
        )
    if missing_downloads:
        return MediaCoverageDecision(
            CoverageStatus.SOURCE_LIMITED,
            media_ids,
            expected_count,
            {},
            f"有 {len(missing_downloads)} 個 Actor 附件尚未成功保存",
        )

    if expected_count is not None and len(media_ids) == expected_count:
        if not contract_verified:
            return MediaCoverageDecision(
                CoverageStatus.SOURCE_LIMITED,
                media_ids,
                expected_count,
                {},
                "媒體完整性尚未通過獨立跨頁 contract；附件已保存但不得宣告 complete",
            )
        evidence = {
            "kind": "declared_count_reached",
            "expected_count": expected_count,
            "seen_count": len(media_ids),
        }
        if explicit_complete:
            evidence["markers"] = explicit_complete
        return MediaCoverageDecision(
            CoverageStatus.COMPLETE,
            media_ids,
            expected_count,
            evidence,
            None,
        )
    if explicit_complete:
        if not contract_verified:
            return MediaCoverageDecision(
                CoverageStatus.SOURCE_LIMITED,
                media_ids,
                expected_count,
                {},
                "媒體完整性尚未通過獨立跨頁 contract；附件已保存但不得宣告 complete",
            )
        return MediaCoverageDecision(
            CoverageStatus.COMPLETE,
            media_ids,
            expected_count,
            {
                "kind": "actor_explicit_terminal",
                "seen_count": len(media_ids),
                "markers": explicit_complete,
            },
            None,
        )
    return MediaCoverageDecision(
        CoverageStatus.SOURCE_LIMITED,
        media_ids,
        expected_count,
        {},
        "Actor 已列出附件，但未提供可驗證的 media terminal／declared count",
    )


def reconcile_post_media_checkpoint(
    db: Database,
    *,
    epoch_id: int,
    profile_id: int,
    post_entity_id: int,
    item: dict[str, Any],
    provider: str,
    contract_id: int | None,
    batch_id: int | None = None,
    contract_verified: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], MediaCoverageDecision]:
    linked = db.rows(
        """SELECT m.source_url,m.status FROM entities e
        JOIN entity_media em ON em.entity_id=e.id AND em.version_id=e.current_version_id
        JOIN media m ON m.id=em.media_id
        WHERE e.id=? AND e.profile_id=?""",
        (post_entity_id, profile_id),
    )
    aliases = db.rows(
        """SELECT ma.source_url,m.status FROM entity_media em
        JOIN media m ON m.id=em.media_id
        JOIN media_aliases ma ON ma.media_id=m.id
        WHERE em.entity_id=? AND m.status='ready'
          AND ma.source_url IS NOT NULL""",
        (post_entity_id,),
    )
    decision = evaluate_post_media(
        item,
        ready_source_urls=(
            str(row["source_url"])
            for row in [*linked, *aliases]
            if str(row.get("status") or "") == "ready"
        ),
        contract_verified=contract_verified,
    )
    coverage = db.upsert_coverage_stream(
        epoch_id,
        stream=CoverageStream.MEDIA.value,
        surface=CoverageSurface.POST_ALBUMS.value,
        scope_type="post",
        scope_id=str(post_entity_id),
        provider=provider,
        contract_id=contract_id,
    )
    checkpoint = db.upsert_post_media_coverage(
        epoch_id,
        post_entity_id=post_entity_id,
        surface=CoverageSurface.POST_ALBUMS.value,
    )
    provider_checkpoint = {
        "batch_id": batch_id,
        "provider": provider,
        "linked_media_count": len(linked),
        "linked_representation_count": len(aliases),
        "contract_verified": contract_verified,
    }
    if str(coverage["status"]) == CoverageStatus.PENDING.value and decision.status is CoverageStatus.COMPLETE:
        db.update_coverage_stream(int(coverage["id"]), status=CoverageStatus.IN_PROGRESS.value)
    if str(checkpoint["status"]) == CoverageStatus.PENDING.value and decision.status is CoverageStatus.COMPLETE:
        db.update_post_media_coverage(int(checkpoint["id"]), status=CoverageStatus.IN_PROGRESS.value)

    db.update_coverage_stream(
        int(coverage["id"]),
        status=decision.status.value,
        provider_checkpoint_json=provider_checkpoint,
        terminal_evidence_json=decision.terminal_evidence,
        seen_count=len(decision.seen_media_ids),
        limited_reason=decision.reason,
    )
    db.update_post_media_coverage(
        int(checkpoint["id"]),
        status=decision.status.value,
        expected_count=decision.expected_count,
        seen_count=len(decision.seen_media_ids),
        seen_media_ids_json=list(decision.seen_media_ids),
        provider_checkpoint_json=provider_checkpoint,
        terminal_evidence_json=decision.terminal_evidence,
        last_error=decision.reason,
    )
    return (
        db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],)) or coverage,
        db.row("SELECT * FROM post_media_coverage WHERE id=?", (checkpoint["id"],))
        or checkpoint,
        decision,
    )


def seed_comment_checkpoints(
    db: Database,
    *,
    epoch_id: int,
    profile_id: int,
    provider: str,
    contract_id: int | None,
) -> list[CommentCheckpoint]:
    posts = db.row(
        """SELECT status FROM coverage_streams
        WHERE epoch_id=? AND stream='posts' AND surface='timeline_posts'
        AND scope_type='profile' AND scope_id=''""",
        (epoch_id,),
    )
    if not posts or str(posts["status"]) != CoverageStatus.COMPLETE.value:
        return []
    rows = db.rows(
        """SELECT DISTINCT e.id,e.external_id,e.source_url
        FROM post_media_coverage pm
        JOIN entities e ON e.id=pm.post_entity_id
        WHERE pm.epoch_id=? AND e.profile_id=? AND e.kind='post'
        ORDER BY e.id""",
        (epoch_id, profile_id),
    )
    checkpoints: list[CommentCheckpoint] = []
    for row in rows:
        stream = db.upsert_coverage_stream(
            epoch_id,
            stream=CoverageStream.COMMENTS.value,
            surface=CoverageSurface.POST_COMMENTS.value,
            scope_type="post",
            scope_id=str(row["id"]),
            provider=provider,
            contract_id=contract_id,
        )
        checkpoints.append(
            CommentCheckpoint(
                coverage_stream_id=int(stream["id"]),
                post_entity_id=int(row["id"]),
                post_external_id=str(row["external_id"]),
                post_url=str(row.get("source_url") or ""),
            )
        )
    return checkpoints


def resolve_epoch(coverage_rows: Iterable[dict[str, Any]]) -> EpochResolution:
    rows = list(coverage_rows)
    if not rows:
        return EpochResolution(False, "running", "尚未建立 coverage stream")
    statuses = {str(row.get("status") or "") for row in rows}
    if CoverageStatus.FAILED.value in statuses:
        return EpochResolution(False, "failed", "至少一個必要 stream 失敗")
    if CoverageStatus.BUDGET_PAUSED.value in statuses:
        return EpochResolution(False, "budget_paused", "至少一個必要 stream 等待額度")
    if CoverageStatus.MANUAL_PAUSED.value in statuses:
        return EpochResolution(False, "manual_paused", "至少一個必要 stream 等待人工處理")
    if statuses & {CoverageStatus.PENDING.value, CoverageStatus.IN_PROGRESS.value}:
        return EpochResolution(False, "running", "必要 stream 尚未全部結案")
    if CoverageStatus.SOURCE_LIMITED.value in statuses:
        return EpochResolution(False, "source_limited", "至少一個必要 stream 為來源受限")
    if statuses == {CoverageStatus.COMPLETE.value}:
        return EpochResolution(True, "complete", "所有必要 stream 均有 terminal evidence")
    return EpochResolution(False, "running", "coverage 狀態尚未可結案")
