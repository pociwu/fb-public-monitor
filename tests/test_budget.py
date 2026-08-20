from pathlib import Path

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage
from fb_monitor.config import load_settings
from fb_monitor.service import ApifyFrozen, BudgetExceeded, MonitorService


def test_apify_budget_is_available_to_posts_after_profile_moves_to_serpapi(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: one
    url: https://facebook.com/one
  - name: two
    url: https://facebook.com/two
storage:
  data_dir: data
budget:
  monthly_usd: 5
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    assert service._available_for("profile") == 5
    assert service._available_for("posts") == 5


@pytest.mark.asyncio
async def test_actor_checks_official_usage_and_stops_when_exhausted(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\nbudget:\n  monthly_usd: 5\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    actor_called = False

    async def exhausted():
        return MonthlyUsage(5.0, "2026-07-09T00:00:00+00:00", "2026-08-08T23:59:59+00:00")

    async def actor_call(*args, **kwargs):
        nonlocal actor_called
        actor_called = True
        return ActorResult([], None, "run")

    service.apify.monthly_usage = exhausted
    service.apify.call = actor_call
    with pytest.raises(BudgetExceeded) as caught:
        await service._actor("profile", "actor", {})

    assert actor_called is False
    assert caught.value.resume_at.isoformat() == "2026-08-09T00:04:59+00:00"
    assert service.db.apify_usage_snapshot()["used_usd"] == 5.0
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0


@pytest.mark.asyncio
async def test_actor_fails_closed_when_official_usage_cannot_be_checked(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\nbudget:\n  monthly_usd: 5\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    actor_called = False

    async def unavailable():
        raise RuntimeError("temporarily unavailable")

    async def actor_call(*args, **kwargs):
        nonlocal actor_called
        actor_called = True
        return ActorResult([], None, "run")

    service.apify.monthly_usage = unavailable
    service.apify.call = actor_call
    with pytest.raises(BudgetExceeded, match="官方用量查詢失敗") as caught:
        await service._actor("profile", "actor", {})

    assert actor_called is False
    assert caught.value.resume_at is not None
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0


@pytest.mark.asyncio
async def test_frozen_profile_never_checks_usage_or_calls_apify(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute("UPDATE profiles SET apify_frozen=1 WHERE id=1")
    usage_checked = False
    actor_called = False

    async def usage():
        nonlocal usage_checked
        usage_checked = True
        raise AssertionError("must not check paid service usage")

    async def actor_call(*args, **kwargs):
        nonlocal actor_called
        actor_called = True
        raise AssertionError("must not call Apify")

    service.apify.monthly_usage = usage
    service.apify.call = actor_call

    with pytest.raises(ApifyFrozen):
        await service._actor("posts", "actor", {}, profile_id=1)

    assert usage_checked is False
    assert actor_called is False
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0


@pytest.mark.asyncio
async def test_actor_rechecks_unified_freeze_after_official_usage_await(
    tmp_path: Path, monkeypatch
):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\n"
        "storage:\n  data_dir: data\nbudget:\n  monthly_usd: 5\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    actor_called = False

    async def usage():
        service.db.set_profile_source_control(
            1, "apify", frozen=True, reason="freeze during official usage await"
        )
        return MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    async def actor_call(*args, **kwargs):
        nonlocal actor_called
        actor_called = True
        raise AssertionError("frozen profile must not call Apify")

    service.apify.monthly_usage = usage
    service.apify.call = actor_call

    with pytest.raises(ApifyFrozen):
        await service._actor("posts", "actor", {}, profile_id=1)

    assert actor_called is False
    assert service.db.profile_source_frozen(1, "apify") is True
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0
