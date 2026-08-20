from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import json

import pytest
from PIL import Image

from fb_monitor.browser_guard import BrowserDecision
from fb_monitor.config import load_settings
from fb_monitor.facebook_browser import (
    FacebookBrowserChallengeRequired,
    FacebookBrowserLoginRequired,
)
from fb_monitor.service import MonitorService


def make_service(tmp_path: Path, monkeypatch) -> MonitorService:
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: FB-100
    url: https://facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
browser_guard:
  daily_batches: 3
  account_min_minutes: 7
  account_max_minutes: 9
  cross_account_min_minutes: 2
  cross_account_max_minutes: 4
  breaker_hours: 12
  breaker_repeat_hours: 36
evidence:
  retention_days: 90
  cap_mib: 12
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    monkeypatch.setenv("FACEBOOK_BROWSER_DATA_DIR", str(tmp_path / "browser-data"))
    return MonitorService(load_settings(config))


def allowed() -> BrowserDecision:
    return BrowserDecision(True, "allowed", None, 1)


def denied(retry_at: datetime) -> BrowserDecision:
    return BrowserDecision(False, "profile_cooldown", retry_at, 1)


def test_service_builds_shared_configured_browser_guard(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)

    assert service.facebook_anonymous_browser.require_login is False
    assert service.browser_guard.browser_identity == "global"
    assert service.anonymous_browser_guard is service.browser_guard
    assert service.browser_guard.daily_batch_limit == 3
    assert service.browser_guard.profile_spacing_minutes == (7.0, 9.0)
    assert service.browser_guard.global_spacing_minutes == (2.0, 4.0)
    assert service.browser_guard.challenge_duration == timedelta(hours=12)
    assert service.browser_guard.repeated_challenge_duration == timedelta(hours=36)
    assert service.browser_guard.evidence_retention == timedelta(days=90)
    assert service.browser_guard.evidence_max_bytes == 12 * 1024 * 1024


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("job_type", "guard_attribute", "browser_attribute"),
    [
        ("verify_public_v2", "anonymous_browser_guard", "facebook_anonymous_browser"),
        ("browser_visit", "browser_guard", "facebook_browser"),
    ],
)
async def test_manual_and_verify_guard_denial_defers_same_job(
    tmp_path: Path,
    monkeypatch,
    job_type: str,
    guard_attribute: str,
    browser_attribute: str,
):
    service = make_service(tmp_path, monkeypatch)
    retry_at = datetime.now(UTC) + timedelta(minutes=17)
    guard = getattr(service, guard_attribute)
    monkeypatch.setattr(guard, "acquire", lambda profile_id: denied(retry_at))
    browser_called = False

    async def unexpected_profile(*args, **kwargs):
        nonlocal browser_called
        browser_called = True
        raise AssertionError("guard denial must not launch Chromium")

    monkeypatch.setattr(getattr(service, browser_attribute), "profile", unexpected_profile)
    job_id = service._enqueue(1, job_type, -10, datetime.now(UTC), {"manual": True})

    await service._run_next_job()

    job = service.db.row("SELECT * FROM jobs WHERE id=?", (job_id,))
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["started_at"] is None
    assert job["finished_at"] is None
    assert datetime.fromisoformat(job["available_at"]) == retry_at
    assert "profile_cooldown" in job["error"]
    assert browser_called is False
    assert service.db.row(
        "SELECT COUNT(*) count FROM events WHERE event_type='browser_guard_deferred'"
    )["count"] == 1


@pytest.mark.asyncio
async def test_automatic_canary_and_fallback_skip_when_guard_denies(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    retry_at = datetime.now(UTC) + timedelta(minutes=10)
    monkeypatch.setattr(service.browser_guard, "acquire", lambda profile_id: denied(retry_at))
    calls = 0

    async def unexpected(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("automatic fallback must not bypass BrowserGuard")

    monkeypatch.setattr(service.facebook_browser, "canary_posts", unexpected)
    monkeypatch.setattr(service.facebook_browser, "profile", unexpected)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    assert await service._try_browser_canary(profile, 0, "test") == []
    assert await service._try_browser_fallback(profile, "serp", "bright") is False
    assert calls == 0
    assert service.db.row(
        "SELECT COUNT(*) count FROM events WHERE event_type='browser_guard_deferred'"
    )["count"] == 2
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 0


@pytest.mark.asyncio
async def test_logged_fallback_records_breaker_and_existing_screenshot(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service.browser_guard, "acquire", lambda profile_id: allowed())
    screenshot = service.facebook_browser.screenshot_path("1")
    screenshot.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (40, 30), (10, 20, 30)).save(screenshot, format="PNG")
    captured: dict[str, object] = {}

    def record_challenge(profile_id, now=None, **kwargs):
        captured.update({"profile_id": profile_id, **kwargs})

    monkeypatch.setattr(service.browser_guard, "record_challenge", record_challenge)

    async def failed_profile(*args, **kwargs):
        raise FacebookBrowserChallengeRequired("challenge")

    monkeypatch.setattr(service.facebook_browser, "profile", failed_profile)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    assert await service._try_browser_fallback(profile, "serp", "bright") is False
    assert captured["profile_id"] == 1
    assert captured["screenshot"] == screenshot.read_bytes()


@pytest.mark.asyncio
async def test_logged_login_wall_does_not_open_shared_breaker(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service.browser_guard, "acquire", lambda profile_id: allowed())

    def unexpected_challenge(*args, **kwargs):
        raise AssertionError("an expired login must not open the shared OCI/IP breaker")

    monkeypatch.setattr(service.browser_guard, "record_challenge", unexpected_challenge)

    async def login_wall(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login required")

    monkeypatch.setattr(service.facebook_browser, "profile", login_wall)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    assert await service._try_browser_fallback(profile, "serp", "bright") is False


@pytest.mark.asyncio
async def test_anonymous_login_wall_is_indeterminate_not_a_breaker(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service.anonymous_browser_guard, "acquire", lambda profile_id: allowed())
    successes: list[int] = []
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "record_success",
        lambda profile_id: successes.append(profile_id),
    )

    def unexpected_challenge(*args, **kwargs):
        raise AssertionError("an anonymous login wall must not open the logged-in breaker")

    monkeypatch.setattr(
        service.anonymous_browser_guard, "record_challenge", unexpected_challenge
    )

    async def login_wall(*args, **kwargs):
        raise FacebookBrowserLoginRequired("anonymous login wall")

    monkeypatch.setattr(service.facebook_anonymous_browser, "profile", login_wall)

    await service.verify_public_v2(1)

    assert successes == []
    observation = service.db.row(
        "SELECT verdict,evidence_summary_json FROM access_observations WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    assert json.loads(observation["evidence_summary_json"])["signal"] == "login_wall"
    assert observation["verdict"] == "unknown"


@pytest.mark.asyncio
async def test_anonymous_challenge_opens_shared_breaker_with_evidence(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service.anonymous_browser_guard, "acquire", lambda profile_id: allowed())
    diagnostic_key = "anonymous-verify-1"
    screenshot = service.facebook_anonymous_browser.screenshot_path(diagnostic_key)
    screenshot.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (40, 30), (30, 20, 10)).save(screenshot, format="PNG")
    anonymous_challenges: list[dict[str, object]] = []

    def record_challenge(profile_id, now=None, **kwargs):
        anonymous_challenges.append({"profile_id": profile_id, **kwargs})

    monkeypatch.setattr(
        service.anonymous_browser_guard, "record_challenge", record_challenge
    )

    async def challenge(*args, **kwargs):
        raise FacebookBrowserChallengeRequired("checkpoint")

    monkeypatch.setattr(service.facebook_anonymous_browser, "profile", challenge)

    await service.verify_public_v2(1)

    assert len(anonymous_challenges) == 1
    assert anonymous_challenges[0]["profile_id"] == 1
    assert anonymous_challenges[0]["screenshot"] == screenshot.read_bytes()
    assert service.anonymous_browser_guard is service.browser_guard
    observation = service.db.row(
        "SELECT evidence_summary_json FROM access_observations WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    assert json.loads(observation["evidence_summary_json"])["signal"] == "http_error"


def test_service_evidence_cleanup_sets_daily_gate(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    calls = 0

    def cleanup():
        nonlocal calls
        calls += 1

    monkeypatch.setattr(service.browser_guard, "cleanup_evidence", cleanup)

    service._cleanup_browser_evidence()

    assert calls == 1
    assert service._browser_evidence_cleanup_date in {
        datetime.now(UTC).date().isoformat(),
        (datetime.now(UTC) + timedelta(hours=8)).date().isoformat(),
    }
