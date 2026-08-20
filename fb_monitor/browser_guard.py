from __future__ import annotations

import hashlib
import io
import json
import os
import random
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps, UnidentifiedImageError

from .db import Database


TAIPEI = timezone(timedelta(hours=8), name="Asia/Taipei")
EVIDENCE_MAX_DIMENSION = 1600
EVIDENCE_TARGET_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class BrowserDecision:
    allowed: bool
    reason: str
    retry_at: datetime | None
    daily_batches: int
    half_open: bool = False


@dataclass(frozen=True, slots=True)
class BrowserChallenge:
    blocked_until: datetime
    repeat_count: int
    evidence: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class EvidenceCleanup:
    removed_records: int
    removed_files: int
    removed_bytes: int
    retained_bytes: int
    errors: tuple[str, ...] = ()


def _as_utc(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC)


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class BrowserGuard:
    """Atomically coordinate every use of one persistent Facebook browser.

    ``acquire`` reserves the batch before Chromium starts.  Queue priority or
    a manual button therefore cannot bypass the same global breaker, spacing,
    and daily allowance used by scheduled canary work.
    """

    def __init__(
        self,
        db: Database,
        evidence_root: Path,
        *,
        browser_identity: str = "default",
        daily_batch_limit: int = 8,
        global_spacing_minutes: tuple[float, float] = (2, 5),
        profile_spacing_minutes: tuple[float, float] = (30, 60),
        challenge_hours: int = 24,
        repeated_challenge_hours: int = 72,
        repeat_window_hours: int = 72,
        half_open_lease_minutes: int = 15,
        evidence_retention_days: int = 180,
        evidence_max_bytes: int = 500 * 1024 * 1024,
        rng: random.Random | random.SystemRandom | None = None,
    ) -> None:
        if not browser_identity.strip():
            raise ValueError("browser_identity must not be empty")
        if daily_batch_limit < 1:
            raise ValueError("daily_batch_limit must be greater than zero")
        for label, bounds in {
            "global_spacing_minutes": global_spacing_minutes,
            "profile_spacing_minutes": profile_spacing_minutes,
        }.items():
            if len(bounds) != 2 or bounds[0] < 0 or bounds[1] < bounds[0]:
                raise ValueError(f"invalid {label}: {bounds!r}")
        if evidence_retention_days < 1 or evidence_max_bytes < 1:
            raise ValueError("evidence retention and size limits must be positive")
        self.db = db
        self.evidence_root = Path(evidence_root).resolve()
        self.browser_identity = browser_identity.strip()
        self.daily_batch_limit = daily_batch_limit
        self.global_spacing_minutes = global_spacing_minutes
        self.profile_spacing_minutes = profile_spacing_minutes
        self.challenge_duration = timedelta(hours=challenge_hours)
        self.repeated_challenge_duration = timedelta(hours=repeated_challenge_hours)
        self.repeat_window = timedelta(hours=repeat_window_hours)
        self.half_open_lease = timedelta(minutes=half_open_lease_minutes)
        self.evidence_retention = timedelta(days=evidence_retention_days)
        self.evidence_max_bytes = evidence_max_bytes
        self.rng = rng or random.SystemRandom()

    @staticmethod
    def _ensure_limit_row(
        connection: Any,
        browser_identity: str,
        scope_type: str,
        scope_id: str,
        now_text: str,
    ) -> dict[str, Any]:
        connection.execute(
            """INSERT OR IGNORE INTO browser_limits(
              browser_identity,scope_type,scope_id,updated_at
            ) VALUES(?,?,?,?)""",
            (browser_identity, scope_type, scope_id, now_text),
        )
        row = connection.execute(
            """SELECT * FROM browser_limits
            WHERE browser_identity=? AND scope_type=? AND scope_id=?""",
            (browser_identity, scope_type, scope_id),
        ).fetchone()
        if row is None:
            raise RuntimeError("browser limit row could not be created")
        return dict(row)

    @staticmethod
    def _breaker_block(
        row: dict[str, Any], now: datetime, half_open_lease: timedelta
    ) -> tuple[str, datetime | None, bool]:
        state = str(row.get("breaker_state") or "closed")
        blocked_until = _parse_time(row.get("blocked_until"))
        if state == "open" and blocked_until and blocked_until > now:
            return "breaker_open", blocked_until, False
        if state == "half_open":
            claimed_at = _parse_time(row.get("half_open_claimed_at"))
            retry_at = claimed_at + half_open_lease if claimed_at else now
            if retry_at > now:
                return "half_open_claimed", retry_at, False
            return "", None, True
        if state == "open":
            return "", None, True
        return "", None, False

    def _spacing(self, bounds: tuple[float, float]) -> timedelta:
        return timedelta(minutes=float(self.rng.uniform(bounds[0], bounds[1])))

    def acquire(self, profile_id: int, now: datetime | None = None) -> BrowserDecision:
        current = _as_utc(now)
        now_text = current.isoformat()
        profile_scope = str(int(profile_id))
        local_date = current.astimezone(TAIPEI).date().isoformat()
        next_local_date = current.astimezone(TAIPEI).date() + timedelta(days=1)
        daily_retry = datetime.combine(next_local_date, datetime_time.min, TAIPEI).astimezone(UTC)

        with self.db.connect() as connection:
            # Serializing the read/check/reserve sequence is what prevents a
            # manual request and scheduler tick from both launching Chromium.
            connection.execute("BEGIN IMMEDIATE")
            profile = connection.execute(
                "SELECT id FROM profiles WHERE id=? AND enabled=1", (profile_id,)
            ).fetchone()
            if profile is None:
                raise ValueError("找不到啟用中的監控帳號")
            global_row = self._ensure_limit_row(
                connection, self.browser_identity, "global", "", now_text
            )
            profile_row = self._ensure_limit_row(
                connection, self.browser_identity, "profile", profile_scope, now_text
            )

            global_reason, global_retry, global_half_open = self._breaker_block(
                global_row, current, self.half_open_lease
            )
            if global_reason:
                return BrowserDecision(False, global_reason, global_retry, int(global_row["daily_batches"]))
            profile_reason, profile_retry, profile_half_open = self._breaker_block(
                profile_row, current, self.half_open_lease
            )
            if profile_reason:
                return BrowserDecision(False, profile_reason, profile_retry, int(global_row["daily_batches"]))

            daily_batches = (
                int(global_row.get("daily_batches") or 0)
                if global_row.get("daily_date") == local_date
                else 0
            )
            if daily_batches >= self.daily_batch_limit:
                return BrowserDecision(False, "daily_limit", daily_retry, daily_batches)

            global_next = _parse_time(global_row.get("next_allowed_at"))
            if global_next and global_next > current:
                return BrowserDecision(False, "global_cooldown", global_next, daily_batches)
            profile_next = _parse_time(profile_row.get("next_allowed_at"))
            if profile_next and profile_next > current:
                return BrowserDecision(False, "profile_cooldown", profile_next, daily_batches)

            global_allowed_at = current + self._spacing(self.global_spacing_minutes)
            profile_allowed_at = current + self._spacing(self.profile_spacing_minutes)
            half_open = global_half_open or profile_half_open
            connection.execute(
                """UPDATE browser_limits
                SET daily_date=?,daily_batches=?,next_allowed_at=?,
                    breaker_state=?,half_open_claimed_at=?,updated_at=?
                WHERE browser_identity=? AND scope_type='global' AND scope_id=''""",
                (
                    local_date,
                    daily_batches + 1,
                    global_allowed_at.isoformat(),
                    "half_open" if global_half_open else str(global_row.get("breaker_state") or "closed"),
                    now_text if global_half_open else global_row.get("half_open_claimed_at"),
                    now_text,
                    self.browser_identity,
                ),
            )
            connection.execute(
                """UPDATE browser_limits
                SET next_allowed_at=?,breaker_state=?,half_open_claimed_at=?,updated_at=?
                WHERE browser_identity=? AND scope_type='profile' AND scope_id=?""",
                (
                    profile_allowed_at.isoformat(),
                    "half_open" if profile_half_open else str(profile_row.get("breaker_state") or "closed"),
                    now_text if profile_half_open else profile_row.get("half_open_claimed_at"),
                    now_text,
                    self.browser_identity,
                    profile_scope,
                ),
            )
            return BrowserDecision(
                True,
                "allowed_half_open" if half_open else "allowed",
                profile_allowed_at,
                daily_batches + 1,
                half_open,
            )

    def record_success(self, profile_id: int, now: datetime | None = None) -> None:
        current = _as_utc(now)
        now_text = current.isoformat()
        with self.db.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for scope_type, scope_id in (("global", ""), ("profile", str(int(profile_id)))):
                row = self._ensure_limit_row(
                    connection, self.browser_identity, scope_type, scope_id, now_text
                )
                blocked_until = _parse_time(row.get("blocked_until"))
                if row.get("breaker_state") == "half_open" or (
                    row.get("breaker_state") == "open"
                    and (blocked_until is None or blocked_until <= current)
                ):
                    connection.execute(
                        """UPDATE browser_limits
                        SET breaker_state='closed',breaker_reason=NULL,blocked_until=NULL,
                            half_open_claimed_at=NULL,updated_at=?
                        WHERE browser_identity=? AND scope_type=? AND scope_id=?""",
                        (now_text, self.browser_identity, scope_type, scope_id),
                    )
            # Evidence is retained for audit, but ceases to be an active
            # breaker record once this browser identity passes its half-open
            # recovery batch.  Cleanup may evict only these closed records.
            connection.execute(
                """UPDATE browser_evidence SET status='closed',closed_at=?
                WHERE browser_identity=? AND status='open'""",
                (now_text, self.browser_identity),
            )

    @staticmethod
    def _encode_evidence(image: Image.Image) -> tuple[bytes, int, int]:
        current = image.copy()
        if max(current.size) > EVIDENCE_MAX_DIMENSION:
            current.thumbnail(
                (EVIDENCE_MAX_DIMENSION, EVIDENCE_MAX_DIMENSION), Image.Resampling.LANCZOS
            )
        while True:
            output = io.BytesIO()
            current.save(output, format="WEBP", lossless=True, method=6)
            encoded = output.getvalue()
            if len(encoded) <= EVIDENCE_TARGET_BYTES or max(current.size) <= 320:
                return encoded, current.width, current.height
            scale = max(
                0.5,
                min(0.9, (EVIDENCE_TARGET_BYTES / len(encoded)) ** 0.5 * 0.94),
            )
            next_size = (
                max(1, round(current.width * scale)),
                max(1, round(current.height * scale)),
            )
            current = current.resize(next_size, Image.Resampling.LANCZOS)

    def record_challenge(
        self,
        profile_id: int,
        now: datetime | None = None,
        *,
        screenshot: bytes | None = None,
        access_observation_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> BrowserChallenge:
        current = _as_utc(now)
        now_text = current.isoformat()
        with self.db.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            global_row = self._ensure_limit_row(
                connection, self.browser_identity, "global", "", now_text
            )
            repeat_started = _parse_time(global_row.get("repeat_window_started_at"))
            if repeat_started is None or current - repeat_started > self.repeat_window:
                repeat_started = current
                repeat_count = 1
                duration = self.challenge_duration
            else:
                repeat_count = int(global_row.get("repeat_count") or 0) + 1
                duration = self.repeated_challenge_duration
            candidate_until = current + duration
            existing_until = _parse_time(global_row.get("blocked_until"))
            blocked_until = max(candidate_until, existing_until) if existing_until else candidate_until
            reason = "repeated_challenge" if repeat_count > 1 else "challenge"
            for scope_type, scope_id in (("global", ""), ("profile", str(int(profile_id)))):
                self._ensure_limit_row(
                    connection, self.browser_identity, scope_type, scope_id, now_text
                )
                connection.execute(
                    """UPDATE browser_limits
                    SET breaker_state='open',breaker_reason=?,blocked_until=?,
                        half_open_claimed_at=NULL,repeat_window_started_at=?,repeat_count=?,updated_at=?
                    WHERE browser_identity=? AND scope_type=? AND scope_id=?""",
                    (
                        reason,
                        blocked_until.isoformat(),
                        repeat_started.isoformat(),
                        repeat_count,
                        now_text,
                        self.browser_identity,
                        scope_type,
                        scope_id,
                    ),
                )

        evidence = None
        if screenshot is not None:
            evidence = self.store_evidence(
                profile_id,
                "challenge",
                screenshot,
                captured_at=current,
                access_observation_id=access_observation_id,
                metadata={
                    **(metadata or {}),
                    "breaker_reason": reason,
                    "blocked_until": blocked_until.isoformat(),
                    "repeat_count": repeat_count,
                },
            )
        return BrowserChallenge(blocked_until, repeat_count, evidence)

    def store_evidence(
        self,
        profile_id: int,
        event_type: str,
        image_bytes: bytes,
        *,
        captured_at: datetime | None = None,
        access_observation_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not event_type.strip():
            raise ValueError("event_type must not be empty")
        current = _as_utc(captured_at)
        try:
            with Image.open(io.BytesIO(image_bytes)) as opened:
                image = ImageOps.exif_transpose(opened)
                image.load()
                if image.mode not in {"RGB", "RGBA"}:
                    image = image.convert("RGBA" if "transparency" in image.info else "RGB")
                webp, width, height = self._encode_evidence(image)
        except (UnidentifiedImageError, OSError, ValueError) as exc:
            raise ValueError("browser evidence is not a valid image") from exc
        if len(webp) > self.evidence_max_bytes:
            raise ValueError("browser evidence exceeds the configured storage cap")

        digest = hashlib.sha256(webp).hexdigest()
        evidence_key = hashlib.sha256(
            json.dumps(
                {
                    "browser_identity": self.browser_identity,
                    "profile_id": int(profile_id),
                    "event_type": event_type.strip(),
                    "captured_at": current.isoformat(),
                    "sha256": digest,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        target = self.evidence_root / digest[:2] / f"{digest}.webp"
        target.parent.mkdir(parents=True, exist_ok=True)
        created_file = False
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=target.parent,
                prefix=f".{digest}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                handle.write(webp)
                handle.flush()
                os.fsync(handle.fileno())
                temporary = Path(handle.name)
            if target.exists():
                if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                    raise RuntimeError(f"immutable evidence hash mismatch: {target}")
            else:
                # The fully flushed temporary file becomes visible in one
                # atomic rename; readers never observe a partial WebP.
                os.replace(temporary, target)
                temporary = None
                created_file = True
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

        expires_at = current + self.evidence_retention
        try:
            row, _ = self.db.record_browser_evidence(
                evidence_key=evidence_key,
                event_type=event_type.strip(),
                path=str(target),
                sha256=digest,
                captured_at=current.isoformat(),
                expires_at=expires_at.isoformat(),
                browser_identity=self.browser_identity,
                profile_id=int(profile_id),
                access_observation_id=access_observation_id,
                size_bytes=len(webp),
                width=width,
                height=height,
                metadata=metadata,
            )
        except Exception:
            if created_file and not self.db.row(
                "SELECT id FROM browser_evidence WHERE path=? LIMIT 1", (str(target),)
            ):
                target.unlink(missing_ok=True)
            raise
        self._cleanup_evidence(current, protected_evidence_key=evidence_key)
        return row

    def _safe_evidence_path(self, value: str) -> Path | None:
        try:
            path = Path(value).resolve()
            path.relative_to(self.evidence_root)
            return path
        except (OSError, ValueError):
            return None

    def _delete_evidence_row(self, row: dict[str, Any]) -> tuple[bool, int, str | None]:
        path = self._safe_evidence_path(str(row.get("path") or ""))
        if path is None:
            error = "evidence path is outside the configured root"
            self.db.execute(
                "UPDATE browser_evidence SET cleanup_error=? WHERE id=?", (error, int(row["id"]))
            )
            return False, 0, error
        shared = self.db.row(
            "SELECT id FROM browser_evidence WHERE path=? AND id<>? LIMIT 1",
            (str(row["path"]), int(row["id"])),
        )
        removed_size = 0
        try:
            if shared is None and path.is_file():
                removed_size = path.stat().st_size
                path.unlink()
            self.db.execute("DELETE FROM browser_evidence WHERE id=?", (int(row["id"]),))
            return True, removed_size, None
        except OSError as exc:
            error = str(exc)
            self.db.execute(
                "UPDATE browser_evidence SET cleanup_error=? WHERE id=?", (error, int(row["id"]))
            )
            return False, 0, error

    def cleanup_evidence(self, now: datetime | None = None) -> EvidenceCleanup:
        return self._cleanup_evidence(_as_utc(now))

    def _cleanup_evidence(
        self,
        current: datetime,
        *,
        protected_evidence_key: str | None = None,
    ) -> EvidenceCleanup:
        removed_records = 0
        removed_files = 0
        removed_bytes = 0
        errors: list[str] = []
        rows = self.db.rows("SELECT * FROM browser_evidence ORDER BY captured_at,id")
        protected_path = ""
        if protected_evidence_key:
            protected = next(
                (row for row in rows if row.get("evidence_key") == protected_evidence_key), None
            )
            protected_path = str(protected.get("path") or "") if protected else ""

        for row in rows:
            if str(row.get("status") or "open") != "closed":
                continue
            expires_at = _parse_time(row.get("expires_at"))
            if expires_at is None or expires_at > current:
                continue
            ok, size, error = self._delete_evidence_row(row)
            if ok:
                removed_records += 1
                if size:
                    removed_files += 1
                    removed_bytes += size
            elif error:
                errors.append(error)

        self.evidence_root.mkdir(parents=True, exist_ok=True)
        referenced = {
            str(path.resolve())
            for row in self.db.rows("SELECT path FROM browser_evidence")
            if (path := self._safe_evidence_path(str(row.get("path") or ""))) is not None
        }
        files = [path for path in self.evidence_root.rglob("*.webp") if path.is_file()]
        retention_cutoff = current.timestamp() - self.evidence_retention.total_seconds()
        for path in sorted(files, key=lambda item: (item.stat().st_mtime, str(item))):
            resolved = str(path.resolve())
            if resolved in referenced or resolved == str(Path(protected_path).resolve()):
                continue
            if path.stat().st_mtime <= retention_cutoff:
                size = path.stat().st_size
                path.unlink()
                removed_files += 1
                removed_bytes += size

        def actual_files() -> list[Path]:
            return [path for path in self.evidence_root.rglob("*.webp") if path.is_file()]

        files = actual_files()
        retained_bytes = sum(path.stat().st_size for path in files)
        if retained_bytes > self.evidence_max_bytes:
            rows = self.db.rows(
                "SELECT * FROM browser_evidence WHERE status='closed' ORDER BY captured_at,id"
            )
            for row in rows:
                if retained_bytes <= self.evidence_max_bytes:
                    break
                if row.get("evidence_key") == protected_evidence_key or str(row.get("path")) == protected_path:
                    continue
                ok, size, error = self._delete_evidence_row(row)
                if ok:
                    removed_records += 1
                    if size:
                        removed_files += 1
                        removed_bytes += size
                        retained_bytes = max(0, retained_bytes - size)
                elif error:
                    errors.append(error)

        # Orphaned files have no retention row; discard oldest ones last, but
        # never the just-written immutable evidence.
        if retained_bytes > self.evidence_max_bytes:
            referenced = {
                str(path.resolve())
                for row in self.db.rows("SELECT path FROM browser_evidence")
                if (path := self._safe_evidence_path(str(row.get("path") or ""))) is not None
            }
            for path in sorted(actual_files(), key=lambda item: (item.stat().st_mtime, str(item))):
                if retained_bytes <= self.evidence_max_bytes:
                    break
                resolved = str(path.resolve())
                if resolved in referenced or resolved == str(Path(protected_path).resolve()):
                    continue
                size = path.stat().st_size
                path.unlink()
                removed_files += 1
                removed_bytes += size
                retained_bytes = max(0, retained_bytes - size)

        return EvidenceCleanup(
            removed_records,
            removed_files,
            removed_bytes,
            retained_bytes,
            tuple(dict.fromkeys(errors)),
        )
