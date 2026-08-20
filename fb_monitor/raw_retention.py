from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .capture_v2 import (
    ArtifactKind,
    ArtifactRecord,
    RetentionAction,
    artifact_retention_decision,
)
from .db import Database


@dataclass(frozen=True, slots=True)
class RawCleanupResult:
    checked: int = 0
    deleted: int = 0
    deleted_bytes: int = 0
    temporary_deleted: int = 0
    errors: int = 0


def _aware(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _inside(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve(strict=False).relative_to(root.resolve(strict=False))
        candidate.absolute().relative_to(root.absolute())
    except ValueError:
        return False
    return True


def cleanup_capture_raw(
    db: Database,
    data_dir: Path,
    *,
    now: datetime | None = None,
) -> RawCleanupResult:
    """Remove only resolved Capture V2 raw artifacts whose epoch is complete.

    Ambiguous, failed, running, or incomplete batches are deliberately absent
    from the deletion query.  Each file is first renamed inside the evidence
    root, then the database reference is cleared in one transaction.  A DB
    failure restores the original path so crash evidence is never silently
    discarded.
    """

    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    root = Path(data_dir) / "capture-v2" / "raw"
    root.mkdir(parents=True, exist_ok=True)
    try:
        root.chmod(0o700)
    except OSError:
        pass

    checked = deleted = deleted_bytes = temporary_deleted = errors = 0
    rows = db.rows(
        """SELECT b.id,b.raw_path,b.raw_saved_at,b.created_at,e.completed_at,
        'paid_source_batches' AS source_table
        FROM paid_source_batches b
        JOIN capture_epochs e ON e.id=b.epoch_id
        WHERE b.status='committed' AND b.raw_path IS NOT NULL
          AND e.status='complete' AND e.completed_at IS NOT NULL
        UNION ALL
        SELECT p.id,p.raw_path,p.raw_saved_at,p.created_at,p.committed_at AS completed_at,
        'paid_access_probe_batches' AS source_table
        FROM paid_access_probe_batches p
        WHERE p.status='committed' AND p.raw_path IS NOT NULL
          AND p.committed_at IS NOT NULL
        ORDER BY source_table,id"""
    )
    for row in rows:
        checked += 1
        try:
            path = Path(str(row["raw_path"]))
            created_at = _aware(row.get("raw_saved_at") or row["created_at"])
            completed_at = _aware(row["completed_at"])
            decision = artifact_retention_decision(
                ArtifactRecord(
                    artifact_id=str(row["id"]),
                    kind=ArtifactKind.APIFY_RAW_SUCCESS,
                    created_at=created_at,
                    epoch_completed_at=completed_at,
                    committed=True,
                ),
                now=now,
            )
            if decision.action is not RetentionAction.DELETE:
                continue
            if not _inside(root, path):
                errors += 1
                continue
            if not path.is_file():
                errors += 1
                continue
            size = path.stat().st_size
            staged = path.with_name(f".{path.name}.cleanup-{row['id']}")
            path.replace(staged)
            try:
                table = str(row.get("source_table") or "")
                if table not in {"paid_source_batches", "paid_access_probe_batches"}:
                    raise RuntimeError("unknown Capture V2 raw metadata table")
                with db.connect() as conn:
                    cursor = conn.execute(
                        f"UPDATE {table} SET raw_path=NULL,updated_at=? WHERE id=? AND raw_path=?",
                        (now.astimezone(UTC).isoformat(), row["id"], str(path)),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError("raw metadata changed during cleanup")
            except Exception:
                staged.replace(path)
                raise
            staged.unlink()
            deleted += 1
            deleted_bytes += size
        except (OSError, RuntimeError, TypeError, ValueError):
            errors += 1

    cutoff = now.astimezone(UTC) - timedelta(hours=24)
    for temporary in root.rglob("*.tmp"):
        try:
            if not _inside(root, temporary):
                errors += 1
                continue
            modified = datetime.fromtimestamp(temporary.stat().st_mtime, UTC)
            if modified <= cutoff:
                temporary.unlink()
                temporary_deleted += 1
        except OSError:
            errors += 1

    return RawCleanupResult(
        checked=checked,
        deleted=deleted,
        deleted_bytes=deleted_bytes,
        temporary_deleted=temporary_deleted,
        errors=errors,
    )


__all__ = ["RawCleanupResult", "cleanup_capture_raw"]
