from pathlib import Path

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage
from fb_monitor.config import load_settings
from fb_monitor.facebook_browser import FacebookBrowserLoginRequired
from fb_monitor.serpapi import SerpApiAccount, SerpApiError, SerpApiProfileResult
from fb_monitor.service import MonitorService, actor_summary_error


def test_failed_actor_summary_is_not_treated_as_empty_success():
    summary = {"health": "failed", "profiles": [{"status": "failed", "error": {"code": "rate_limited", "message": "Facebook rate-limited the timeline query."}}]}
    assert actor_summary_error(summary) == "Facebook rate-limited the timeline query."


async def allow_official_usage(service: MonitorService) -> None:
    async def fake_monthly_usage():
        return MonthlyUsage(1.0, "2026-07-09T00:00:00+00:00", "2026-08-08T23:59:59+00:00")

    service.apify.monthly_usage = fake_monthly_usage

    async def fake_serpapi_profile(url):
        return SerpApiProfileResult(
            {"id": "pfbid0internal", "name": "Watched", "url": url, "profile_intro_text": "Public profile"},
            SerpApiAccount("Free Plan", 250, 250, 0, "2026-08-31", 0, 50),
        )

    service.serpapi.profile = fake_serpapi_profile


@pytest.mark.asyncio
async def test_initial_visit_ingests_profile_post_and_comments(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
budget:
  monthly_usd: 5
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    await allow_official_usage(service)

    async def fake_call(actor_id, payload, max_charge_usd=None):
        if "pages-scraper" in actor_id:
            return ActorResult([{"id": "100", "name": "Watched", "url": "https://facebook.com/100"}], None, "profile")
        if actor_id == "unseenuser/fb-profile":
            return ActorResult([{"name": "Watched", "posts": [{"id": "p1", "url": "https://facebook.com/100/posts/p1", "text": "hello"}]}], None, "posts")
        return ActorResult([{"commentId": "c1", "facebookUrl": "https://facebook.com/100/posts/p1", "text": "reply"}], None, "comments")

    service.apify.call = fake_call
    await service.visit_profile(1)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert profile["public_state"] == "public"
    assert profile["fb_id"] == "100"
    counts = {row["kind"]: row["count"] for row in service.db.rows("SELECT kind,COUNT(*) count FROM entities GROUP BY kind")}
    assert counts == {"comment": 1, "post": 1, "profile": 1}
    # Initial baseline is summarized later rather than generating one notification per item.
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 0


@pytest.mark.asyncio
async def test_posts_retry_formats_and_profile_fallback(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
budget:
  monthly_usd: 5
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
actors:
  posts: spbotdel/facebook-profile-posts-all-photos-scraper
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    await allow_official_usage(service)
    calls = []

    async def fake_call(actor_id, payload, max_charge_usd=None):
        if "pages-scraper" in actor_id:
            return ActorResult([{"id": "100", "title": "Watched", "url": "https://facebook.com/100", "posts": [{"postId": "fallback", "text": "embedded"}]}], None, "profile")
        calls.append(payload["profileUrls"][0])
        if payload["profileUrls"][0] == "100":
            return ActorResult([{"postId": "p1", "text": "retry worked"}], None, "posts")
        return ActorResult([], None, "posts-zero")

    service.apify.call = fake_call
    await service.visit_profile(1)
    assert calls[:2] == ["https://facebook.com/100", "100"]
    assert service.db.row("SELECT external_id FROM entities WHERE kind='post'")["external_id"] == "p1"
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs WHERE category='posts'")["count"] == 2


@pytest.mark.asyncio
async def test_serpapi_profile_does_not_replace_empty_apify_posts_with_profile_data(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
budget:
  monthly_usd: 5
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    await allow_official_usage(service)

    async def fake_call(actor_id, payload, max_charge_usd=None):
        if "pages-scraper" in actor_id:
            return ActorResult([{"id": "100", "title": "Watched", "posts": [{"postId": "fallback", "text": "embedded"}]}], None, "profile")
        return ActorResult([], None, "zero")

    service.apify.call = fake_call
    await service.visit_profile(1)
    assert service.db.row("SELECT external_id FROM entities WHERE kind='post'") is None
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs WHERE category='posts'")["count"] == 2


def test_unseenuser_wrapper_is_flattened_to_posts():
    wrapped = ActorResult(
        [{"name": "Sang Daw", "posts": [{"id": "p1", "url": "https://facebook.com/post/1", "image": "https://cdn.example/1.jpg"}]}],
        None,
        "run",
    )
    result = MonitorService._unwrap_embedded_posts(wrapped, 200)
    assert [item["id"] for item in result.items] == ["p1"]
    assert result.items[0]["ingest_source"] == "posts_actor_embedded"
    assert result.summary["profiles"][0]["coverageStatus"] == "complete"


def test_health_summary_uses_resolved_display_name():
    lines = MonitorService._health_profile_lines([
        {"name": "吳佳欣", "public_state": "public", "last_success_at": "2026-08-01T00:00:00+00:00"},
        {"name": "FB-100027675104517", "public_state": "unknown", "last_success_at": None},
    ])
    assert lines == [
        "吳佳欣: public · 最近成功 2026/8/1 08:00",
        "姓名待確認: unknown · 最近成功 -",
    ]


@pytest.mark.asyncio
async def test_brightdata_fallback_updates_profile_after_serpapi_failure(tmp_path: Path, monkeypatch):
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
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("BRIGHTDATA_API_TOKEN", "secret")
    service = MonitorService(load_settings(config))

    async def failed_serpapi(url):
        raise SerpApiError("Facebook Profile hasn't returned any results for this query.")

    calls = []

    async def brightdata_profile(url):
        calls.append(url)
        return {
            "id": "100", "name": "Alice", "url": url,
            "profile_picture": "https://cdn.example/avatar.jpg",
            "profile_data_source": "Bright Data",
        }

    service.serpapi.profile = failed_serpapi
    service.brightdata.profile = brightdata_profile
    await service.visit_profile(1)

    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert calls == ["https://facebook.com/100"]
    assert profile["public_state"] == "public"
    assert profile["fb_id"] == "100"
    assert profile["serp_last_checked_at"] is not None
    assert '"profile_data_source": "Bright Data"' in profile["profile_details_json"]
    assert service.db.row("SELECT COUNT(*) count FROM events WHERE event_type='brightdata_fallback'")["count"] == 1


@pytest.mark.asyncio
async def test_logged_in_browser_is_final_profile_fallback(tmp_path: Path, monkeypatch):
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
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    monkeypatch.delenv("BRIGHTDATA_API_TOKEN", raising=False)
    service = MonitorService(load_settings(config))

    async def failed_serpapi(url):
        raise SerpApiError("no results")

    browser_calls = []

    async def browser_profile(url):
        browser_calls.append(url)
        return {"id": "100", "name": "Alice", "url": url, "profile_data_source": "Facebook 直接瀏覽器"}

    service.serpapi.profile = failed_serpapi
    service.facebook_browser.profile = browser_profile
    await service.visit_profile(1)

    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert browser_calls == ["https://facebook.com/100"]
    assert profile["public_state"] == "public"
    assert '"profile_data_source": "Facebook 直接瀏覽器"' in profile["profile_details_json"]
    assert service.db.row("SELECT COUNT(*) count FROM events WHERE event_type='facebook_browser_fallback'")["count"] == 1


@pytest.mark.asyncio
async def test_browser_login_wall_does_not_mark_profile_private(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: FB-100
    url: https://facebook.com/100
storage:
  data_dir: data
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    monkeypatch.delenv("BRIGHTDATA_API_TOKEN", raising=False)
    service = MonitorService(load_settings(config))

    async def failed_serpapi(url):
        raise SerpApiError("no results")

    async def login_required(url):
        raise FacebookBrowserLoginRequired("login required")

    service.serpapi.profile = failed_serpapi
    service.facebook_browser.profile = login_required
    await service.visit_profile(1)

    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert profile["public_state"] == "unknown"
    assert profile["serp_last_checked_at"] is None
    assert service.db.row("SELECT COUNT(*) count FROM events WHERE event_type='facebook_browser_login_required'")["count"] == 1


def test_browser_heading_repair_resets_bad_name_and_queues_refresh(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: FB-100
    url: https://facebook.com/100
storage:
  data_dir: data
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute(
        "UPDATE profiles SET display_name='(4) Facebook',serp_last_checked_at='2026-08-03T04:00:00+00:00',profile_details_json=? WHERE id=1",
        ('{"profile_data_source":"Facebook 直接瀏覽器"}',),
    )

    service._seed_browser_name_repair()

    profile = service.db.row("SELECT display_name,serp_last_checked_at FROM profiles WHERE id=1")
    assert profile == {"display_name": None, "serp_last_checked_at": None}
    assert service.db.row("SELECT COUNT(*) count FROM jobs WHERE profile_id=1 AND job_type='visit' AND priority=-50")["count"] == 1


@pytest.mark.asyncio
async def test_manual_visit_bypasses_global_spacing(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
schedule:
  spacing_min_minutes: 30
  spacing_max_minutes: 30
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute("DELETE FROM jobs")
    now = __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat()
    service.db.execute(
        """INSERT INTO jobs(profile_id,job_type,priority,status,payload_json,available_at,attempts,created_at,started_at,finished_at)
        VALUES(1,'visit',10,'done','{}',?,1,?,?,?)""",
        (now, now, now, now),
    )
    queued, _ = service.db.queue_manual_visit(1)
    assert queued is True
    called = []

    async def fake_visit(profile_id):
        called.append(profile_id)

    service.visit_profile = fake_visit
    await service._run_next_job()
    assert called == [1]
    assert service.db.row("SELECT status FROM jobs WHERE priority=-100")["status"] == "done"
