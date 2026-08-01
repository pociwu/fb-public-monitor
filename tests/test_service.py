from pathlib import Path

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage
from fb_monitor.config import load_settings
from fb_monitor.serpapi import SerpApiAccount, SerpApiProfileResult
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
        "吳佳欣: public · 最近成功 2026-08-01T00:00:00+00:00",
        "姓名待確認: unknown · 最近成功 -",
    ]
