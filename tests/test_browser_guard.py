from __future__ import annotations

import io
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from PIL import Image

from fb_monitor.browser_guard import BrowserGuard
from fb_monitor.db import Database, utcnow


class FixedRandom:
    def uniform(self, lower: float, upper: float) -> float:
        return lower


def add_profile(db: Database, profile_id: int = 1) -> None:
    now = utcnow()
    db.execute(
        """INSERT INTO profiles(id,name,url,created_at,updated_at)
        VALUES(?,?,?,?,?)""",
        (profile_id, f"profile-{profile_id}", f"https://facebook.com/{profile_id}", now, now),
    )


def image_bytes(color: tuple[int, int, int], size: tuple[int, int] = (48, 32)) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", size, color).save(output, format="PNG")
    return output.getvalue()


def make_guard(tmp_path: Path, **kwargs) -> tuple[Database, BrowserGuard]:
    db = Database(tmp_path / "monitor.sqlite3")
    add_profile(db, 1)
    add_profile(db, 2)
    return db, BrowserGuard(db, tmp_path / "evidence", rng=FixedRandom(), **kwargs)


def test_acquire_atomically_reserves_global_and_profile_cooldowns(tmp_path: Path):
    db, guard = make_guard(tmp_path)
    now = datetime(2026, 8, 16, tzinfo=UTC)

    first = guard.acquire(1, now)
    assert first.allowed is True
    assert first.reason == "allowed"
    assert first.daily_batches == 1
    assert first.retry_at == now + timedelta(minutes=30)

    same_global_window = guard.acquire(2, now + timedelta(minutes=1))
    assert same_global_window.allowed is False
    assert same_global_window.reason == "global_cooldown"
    different_profile = guard.acquire(2, now + timedelta(minutes=2))
    assert different_profile.allowed is True
    same_profile = guard.acquire(1, now + timedelta(minutes=31))
    assert same_profile.allowed is True

    global_limit = db.get_browser_limit()
    assert global_limit["daily_date"] == "2026-08-16"
    assert global_limit["daily_batches"] == 3


def test_concurrent_manual_and_scheduler_acquire_only_one_batch(tmp_path: Path):
    db, guard = make_guard(tmp_path)
    now = datetime(2026, 8, 16, tzinfo=UTC)

    with ThreadPoolExecutor(max_workers=2) as executor:
        decisions = list(executor.map(lambda _: guard.acquire(1, now), range(2)))

    assert sum(decision.allowed for decision in decisions) == 1
    assert db.get_browser_limit()["daily_batches"] == 1


def test_daily_limit_resets_at_taipei_midnight(tmp_path: Path):
    _, guard = make_guard(
        tmp_path,
        daily_batch_limit=2,
        global_spacing_minutes=(0, 0),
        profile_spacing_minutes=(0, 0),
    )
    before_midnight = datetime(2026, 8, 16, 15, 59, tzinfo=UTC)

    assert guard.acquire(1, before_midnight).allowed
    assert guard.acquire(2, before_midnight).allowed
    limited = guard.acquire(1, before_midnight)
    assert limited.reason == "daily_limit"
    assert limited.retry_at == datetime(2026, 8, 16, 16, 0, tzinfo=UTC)
    reset = guard.acquire(1, datetime(2026, 8, 16, 16, 0, tzinfo=UTC))
    assert reset.allowed
    assert reset.daily_batches == 1


def test_challenge_opens_24_hours_repeat_opens_72_and_half_open_is_single(tmp_path: Path):
    _, guard = make_guard(
        tmp_path,
        global_spacing_minutes=(0, 0),
        profile_spacing_minutes=(0, 0),
    )
    now = datetime(2026, 8, 16, tzinfo=UTC)

    first = guard.record_challenge(1, now)
    assert first.repeat_count == 1
    assert first.blocked_until == now + timedelta(hours=24)
    denied = guard.acquire(2, now + timedelta(hours=23))
    assert denied.reason == "breaker_open"

    repeated = guard.record_challenge(2, now + timedelta(hours=24))
    assert repeated.repeat_count == 2
    assert repeated.blocked_until == now + timedelta(hours=96)
    half_open = guard.acquire(1, repeated.blocked_until)
    assert half_open.allowed and half_open.half_open
    second_claim = guard.acquire(2, repeated.blocked_until)
    assert second_claim.reason == "half_open_claimed"
    guard.record_success(1, repeated.blocked_until + timedelta(minutes=1))
    allowed = guard.acquire(2, repeated.blocked_until + timedelta(minutes=2))
    assert allowed.allowed


def test_evidence_is_lossless_webp_immutable_and_idempotent(tmp_path: Path):
    db, guard = make_guard(tmp_path)
    captured = datetime(2026, 8, 16, tzinfo=UTC)
    png = image_bytes((12, 34, 56))

    first = guard.store_evidence(1, "challenge", png, captured_at=captured)
    target = Path(first["path"])
    original = target.read_bytes()
    with Image.open(target) as stored:
        assert stored.format == "WEBP"
        assert stored.convert("RGB").getpixel((0, 0)) == (12, 34, 56)
    again = guard.store_evidence(1, "challenge", png, captured_at=captured)

    assert again["id"] == first["id"]
    assert target.read_bytes() == original
    assert db.row("SELECT COUNT(*) count FROM browser_evidence")["count"] == 1


def test_evidence_is_resized_bounded_and_atomically_published(tmp_path: Path):
    _, guard = make_guard(tmp_path)
    noisy = Image.effect_noise((2200, 1700), 100).convert("RGB")
    output = io.BytesIO()
    noisy.save(output, format="PNG")

    row = guard.store_evidence(1, "challenge", output.getvalue())
    target = Path(row["path"])

    assert max(int(row["width"]), int(row["height"])) <= 1600
    assert int(row["size_bytes"]) <= 1024 * 1024
    assert not list(target.parent.glob("*.tmp"))
    with Image.open(target) as stored:
        assert stored.format == "WEBP"
        assert stored.size == (int(row["width"]), int(row["height"]))


def test_record_challenge_saves_evidence_for_180_days(tmp_path: Path):
    _, guard = make_guard(tmp_path)
    now = datetime(2026, 8, 16, tzinfo=UTC)

    result = guard.record_challenge(1, now, screenshot=image_bytes((200, 10, 10)))

    assert result.evidence is not None
    assert Path(result.evidence["path"]).is_file()
    assert result.evidence["expires_at"] == (now + timedelta(days=180)).isoformat()


def test_cleanup_removes_expired_and_oldest_until_actual_root_is_under_cap(tmp_path: Path):
    db, guard = make_guard(tmp_path, evidence_max_bytes=10_000)
    now = datetime(2026, 8, 16, tzinfo=UTC)
    expired = guard.store_evidence(
        1, "challenge", image_bytes((1, 2, 3)), captured_at=now - timedelta(days=181)
    )
    first = guard.store_evidence(1, "challenge", image_bytes((4, 5, 6)), captured_at=now)
    second = guard.store_evidence(
        2, "challenge", image_bytes((7, 8, 9)), captured_at=now + timedelta(seconds=1)
    )
    db.execute(
        "UPDATE browser_evidence SET status='closed',closed_at=?",
        ((now + timedelta(seconds=2)).isoformat(),),
    )
    cap = Path(second["path"]).stat().st_size
    guard.evidence_max_bytes = cap

    result = guard.cleanup_evidence(now + timedelta(seconds=2))

    assert not Path(expired["path"]).exists()
    assert not Path(first["path"]).exists()
    assert Path(second["path"]).exists()
    assert result.retained_bytes <= cap
    assert db.row("SELECT COUNT(*) count FROM browser_evidence")["count"] == 1


def test_cleanup_never_unlinks_a_path_outside_evidence_root(tmp_path: Path):
    db, guard = make_guard(tmp_path)
    outside = tmp_path / "outside.webp"
    outside.write_bytes(b"do not delete")
    db.record_browser_evidence(
        evidence_key="outside",
        event_type="challenge",
        path=str(outside),
        sha256="sha",
        captured_at="2025-01-01T00:00:00+00:00",
        expires_at="2025-01-02T00:00:00+00:00",
        profile_id=1,
    )
    db.execute(
        "UPDATE browser_evidence SET status='closed',closed_at=? WHERE evidence_key='outside'",
        (datetime(2025, 1, 2, tzinfo=UTC).isoformat(),),
    )

    result = guard.cleanup_evidence(datetime(2026, 8, 16, tzinfo=UTC))

    assert outside.read_bytes() == b"do not delete"
    assert result.errors == ("evidence path is outside the configured root",)
    assert db.row("SELECT cleanup_error FROM browser_evidence WHERE evidence_key='outside'")[
        "cleanup_error"
    ]


def test_cleanup_never_evicts_open_breaker_evidence_and_success_closes_it(tmp_path: Path):
    db, guard = make_guard(tmp_path, evidence_max_bytes=100)
    now = datetime(2026, 8, 16, tzinfo=UTC)
    challenge = guard.record_challenge(
        1,
        now - timedelta(days=181),
        screenshot=image_bytes((30, 40, 50), (200, 150)),
    )
    assert challenge.evidence is not None
    target = Path(challenge.evidence["path"])

    guard.cleanup_evidence(now)

    assert target.exists()
    assert db.row("SELECT status FROM browser_evidence WHERE id=?", (challenge.evidence["id"],))[
        "status"
    ] == "open"

    guard.record_success(1, now)
    assert db.row("SELECT status FROM browser_evidence WHERE id=?", (challenge.evidence["id"],))[
        "status"
    ] == "closed"
    guard.cleanup_evidence(now)
    assert not target.exists()
