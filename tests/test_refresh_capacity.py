from datetime import UTC, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from fb_monitor.browser_guard import BrowserGuard
from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService
from fb_monitor.web import create_app


def settings_for(tmp_path, monkeypatch, guard=""):
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: recent\n    url: https://facebook.com/100\n"
        "  - name: stale\n    url: https://facebook.com/200\n"
        "storage:\n  data_dir: data\nschedule:\n  spacing_min_minutes: 0\n  spacing_max_minutes: 0\n" + guard,
        encoding="utf-8",
    )
    return load_settings(config)


def test_capacity_defaults_and_legacy_override(tmp_path, monkeypatch):
    settings = settings_for(tmp_path, monkeypatch)
    assert settings.browser_daily_batches == 32
    assert settings.browser_profile_daily_batches == 8
    legacy = settings_for(tmp_path, monkeypatch, "browser_guard:\n  daily_batches: 6\n")
    assert legacy.browser_daily_batches == 6
    explicit = settings_for(tmp_path, monkeypatch,
        "browser_guard:\n  daily_batches: 8\n  global_daily_batches: 32\n  profile_daily_batches: 8\n")
    assert explicit.browser_daily_batches == 32
    assert explicit.browser_profile_daily_batches == 8


def test_global_and_profile_allowances_persist_and_reset(tmp_path, monkeypatch):
    service = MonitorService(settings_for(tmp_path, monkeypatch))
    kwargs = dict(daily_batch_limit=3, profile_daily_batch_limit=2,
                  global_spacing_minutes=(0, 0), profile_spacing_minutes=(0, 0))
    guard = BrowserGuard(service.db, tmp_path / "evidence", **kwargs)
    now = datetime(2026, 9, 8, 15, 0, tzinfo=UTC)
    assert guard.acquire(1, now).allowed
    assert guard.acquire(1, now).allowed
    restarted = BrowserGuard(service.db, tmp_path / "evidence", **kwargs)
    assert restarted.acquire(1, now).reason == "profile_daily_limit"
    assert restarted.acquire(2, now).allowed
    denied = restarted.acquire(2, now)
    assert denied.reason == "daily_limit"
    assert denied.retry_at == datetime(2026, 9, 8, 16, tzinfo=UTC)
    assert restarted.acquire(1, denied.retry_at).allowed


def test_refresh_priority_prefers_stale_but_rotates_after_attempt(tmp_path, monkeypatch):
    service = MonitorService(settings_for(tmp_path, monkeypatch))
    now = datetime.now(UTC)
    service.db.execute("UPDATE profiles SET last_success_at=? WHERE id=1", ((now-timedelta(days=1)).isoformat(),))
    service.db.execute("UPDATE profiles SET last_success_at=? WHERE id=2", ((now-timedelta(days=10)).isoformat(),))
    recent = service._enqueue(1, "profile_browser_fallback", 5, now)
    stale = service._enqueue(2, "visit", 10, now)
    assert service._select_next_job(now.isoformat())["id"] == stale
    # A failed oldest account must not consume every slot before others try.
    assert service.browser_guard.acquire(2, now).allowed
    assert service._select_next_job(now.isoformat())["id"] == recent
    urgent = service._enqueue(1, "detect_public_v2", -400, now)
    assert service._select_next_job(now.isoformat())["id"] == urgent


def test_concurrent_requests_cannot_exceed_profile_limit(tmp_path, monkeypatch):
    service = MonitorService(settings_for(tmp_path, monkeypatch))
    guard = BrowserGuard(service.db, tmp_path / "evidence", browser_identity="global",
                         daily_batch_limit=32, profile_daily_batch_limit=2,
                         global_spacing_minutes=(0, 0), profile_spacing_minutes=(0, 0))
    now = datetime(2026, 9, 8, tzinfo=UTC)
    with ThreadPoolExecutor(max_workers=8) as pool:
        decisions = list(pool.map(lambda _: guard.acquire(1, now), range(8)))
    assert sum(result.allowed for result in decisions) == 2
    rows = service.db.rows("SELECT daily_batches FROM browser_limits WHERE browser_identity='global'")
    assert all(row["daily_batches"] == 2 for row in rows)


@pytest.mark.asyncio
async def test_pending_fallback_suppresses_repeated_paid_profile_lookup(tmp_path, monkeypatch):
    service = MonitorService(settings_for(tmp_path, monkeypatch))
    now = datetime.now(UTC)
    fallback = service._enqueue(1, "profile_browser_fallback", 5, now + timedelta(days=1))

    async def unexpected_lookup(*args, **kwargs):
        pytest.fail("Already-pending fallback must suppress another profile lookup")

    monkeypatch.setattr(service, "_refresh_serpapi_profile", unexpected_lookup)
    await service.visit_profile(1)
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (fallback,))["status"] == "pending"
    assert service.db.row("SELECT next_visit_at FROM profiles WHERE id=1")["next_visit_at"]


def test_dashboard_queue_breakdown_and_filters(tmp_path, monkeypatch):
    app = create_app(settings_for(tmp_path, monkeypatch))
    db = app.state.db
    past, future = "2026-01-01T00:00:00+00:00", "2099-01-01T00:00:00+00:00"
    for name, status, available, error in [
        ("running-job", "running", past, None),
        ("ready-job", "pending", past, None),
        ("limited-job", "pending", future, "daily_limit"),
        ("scheduled-job", "pending", future, None),
    ]:
        db.execute("INSERT INTO jobs(job_type,status,available_at,created_at,error,priority) VALUES(?,?,?,?,?,10)",
                   (name, status, available, past, error))
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        for state in ("running", "ready", "limited", "scheduled"):
            assert f'data-queue-state="{state}"' in response.text
            filtered = client.get(f"/jobs?status={state}")
            assert filtered.status_code == 200
            assert f"{state}-job" in filtered.text
            assert all(f"{other}-job" not in filtered.text for other in ("running", "ready", "limited", "scheduled") if other != state)


def test_legacy_fallback_is_limited_and_dashboard_reads_correct_identity(tmp_path, monkeypatch):
    from fb_monitor.job_queue import queue_counts
    from fb_monitor.browser_guard import TAIPEI

    app = create_app(settings_for(tmp_path, monkeypatch))
    db = app.state.db
    day = datetime.now(TAIPEI).date().isoformat()
    db.execute("INSERT INTO browser_limits(browser_identity,scope_type,scope_id,daily_date,daily_batches,updated_at) VALUES('global','global','',?,7,?)", (day, datetime.now(UTC).isoformat()))
    db.execute("INSERT INTO jobs(job_type,status,available_at,created_at,priority) VALUES('profile_browser_fallback','pending','2099-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00',5)")
    with db.connect() as conn:
        counts = queue_counts(conn, datetime.now(UTC).isoformat())
    assert counts["limited"] == 1
    assert counts["scheduled"] == 0
    assert counts["count"] == sum(counts[state] for state in ("running", "ready", "limited", "scheduled"))
    with TestClient(app) as client:
        assert "瀏覽器今日 7 / 32 批" in client.get("/").text
    assert db.row("SELECT COUNT(*) n FROM browser_limits WHERE browser_identity='default'")["n"] == 0
