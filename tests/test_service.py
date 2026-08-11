import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage
from fb_monitor.config import load_settings
from fb_monitor.facebook_browser import FacebookBrowserError, FacebookBrowserLoginRequired
from fb_monitor.serpapi import SerpApiAccount, SerpApiError, SerpApiProfileResult
from fb_monitor.service import BudgetExceeded, MonitorService, actor_summary_error


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
async def test_initial_visit_finishes_posts_before_backfill_comments(tmp_path: Path, monkeypatch):
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
    post_limits: list[int] = []

    async def fake_call(actor_id, payload, max_charge_usd=None):
        if "pages-scraper" in actor_id:
            return ActorResult([{"id": "100", "name": "Watched", "url": "https://facebook.com/100"}], None, "profile")
        if actor_id == "unseenuser/fb-profile":
            post_limits.append(payload["maxPosts"])
            return ActorResult([{"name": "Watched", "posts": [{"id": "p1", "url": "https://facebook.com/100/posts/p1", "text": "hello"}]}], None, "posts")
        return ActorResult([{"commentId": "c1", "facebookUrl": "https://facebook.com/100/posts/p1", "text": "reply"}], None, "comments")

    service.apify.call = fake_call
    await service.visit_profile(1)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert profile["public_state"] == "public"
    assert profile["fb_id"] == "100"
    assert post_limits == [20]
    counts = {row["kind"]: row["count"] for row in service.db.rows("SELECT kind,COUNT(*) count FROM entities GROUP BY kind")}
    assert counts == {"post": 1, "profile": 1}
    # Initial post baseline is silent; the completion summary is emitted only
    # after the queued comment phase finishes.
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 0
    queued = service.db.row("SELECT payload_json FROM jobs WHERE profile_id=1 AND job_type='backfill_comments' ORDER BY id DESC LIMIT 1")
    await service.backfill_comments(1, json.loads(queued["payload_json"]))
    counts = {row["kind"]: row["count"] for row in service.db.rows("SELECT kind,COUNT(*) count FROM entities GROUP BY kind")}
    assert counts == {"comment": 1, "post": 1, "profile": 1}


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
    # unseenuser charges per returned event. Numeric accounts therefore use
    # only the canonical profile.php input instead of paying for URL aliases.
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs WHERE category='posts'")["count"] == 1


@pytest.mark.asyncio
async def test_browser_canary_supplements_partial_posts_without_reconciling_them(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
browser_canary:
  enabled: true
  cooldown_hours: 72
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    service = MonitorService(load_settings(config))
    await service.ingester.ingest(
        1,
        "post",
        {"postId": "old", "source_url": "https://facebook.com/100/posts/old", "text": "old"},
        notify=False,
    )
    service.db.execute(
        "UPDATE profiles SET public_state='public',serp_last_checked_at=?,backfill_done=1,last_full_audit_at=? WHERE id=1",
        (datetime.now(UTC).isoformat(), datetime.now(UTC).isoformat()),
    )

    async def partial_posts(profile, maximum, cursor=None):
        return ActorResult([{"postId": "api-post", "text": "from api"}], None, "posts")

    async def canary_posts(url, diagnostic_key=None):
        return [{
            "source_post_id": "browser-post",
            "source_url": "https://facebook.com/100/posts/browser-post",
            "text": "from browser",
            "ingest_source": "facebook_browser_canary",
        }]

    reconciled: list[set[str]] = []
    service._fetch_posts = partial_posts
    service.facebook_browser.cached_canary_posts = lambda url: None
    service.facebook_browser.canary_posts = canary_posts
    service.ingester.reconcile = lambda profile_id, kind, seen, *args, **kwargs: reconciled.append(set(seen))

    await service.visit_profile(1)

    assert {row["external_id"] for row in service.db.rows("SELECT external_id FROM entities WHERE kind='post'")} == {
        "old", "api-post", "browser-post",
    }
    assert reconciled == []
    assert service.db.row("SELECT browser_canary_last_attempt_at FROM profiles WHERE id=1")["browser_canary_last_attempt_at"]
    assert service.db.row("SELECT COUNT(*) count FROM events WHERE event_type='browser_canary'")["count"] == 1


@pytest.mark.asyncio
async def test_browser_canary_matches_post_permalink_alias_to_existing_entity(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    await service.ingester.ingest(
        1,
        "post",
        {"postId": "pfbid123", "source_url": "https://facebook.com/100/posts/pfbid123", "text": "same"},
        notify=False,
    )

    await service._ingest_browser_canary_posts(
        1,
        [{
            "source_url": "https://facebook.com/permalink.php?story_fbid=pfbid123&id=100",
            "text": "same",
            "ingest_source": "facebook_browser_canary",
        }],
        notify=False,
    )

    assert service.db.row("SELECT COUNT(*) count FROM entities WHERE profile_id=1 AND kind='post'")["count"] == 1


@pytest.mark.asyncio
async def test_manual_browser_visit_updates_profile_and_canary_posts(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    service = MonitorService(load_settings(config))

    async def fake_profile(url, diagnostic_key=None):
        return {"name": "Browser Name", "profile_data_source": "Facebook 直接瀏覽器"}

    async def fake_posts(url, diagnostic_key=None):
        return [{"source_post_id": "p1", "source_url": "https://facebook.com/100/posts/p1", "text": "post"}]

    service.facebook_browser.profile = fake_profile
    service.facebook_browser.canary_posts = fake_posts
    await service.browser_visit_profile(1)

    assert service.db.row("SELECT last_success_at FROM profiles WHERE id=1")["last_success_at"]
    assert service.db.row("SELECT COUNT(*) count FROM entities WHERE profile_id=1 AND kind='post'")["count"] == 1
    assert service.db.row("SELECT COUNT(*) count FROM events WHERE event_type='browser_manual_visit'")["count"] == 1


@pytest.mark.asyncio
async def test_regular_post_probe_skips_full_batch_when_latest_post_is_unchanged(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    item = {"postId": "p1", "source_url": "https://facebook.com/100/posts/p1", "text": "same"}
    await service.ingester.ingest(1, "post", item, notify=False)
    calls: list[int] = []
    diagnostic_id = service.db.start_actor_run(1, "posts", "actor", "probe", {})

    async def fake_fetch(profile, maximum, cursor=None):
        calls.append(maximum)
        return ActorResult([item], {"profiles": [{"status": "succeeded"}]}, "probe", diagnostic_id=diagnostic_id)

    service._fetch_posts = fake_fetch
    result = await service._fetch_regular_posts({"id": 1, "backfill_done": 1}, initial=False)

    assert calls == [1]
    assert result.items == []
    assert result.summary["source"] == "unchanged_probe"
    assert service.db.row("SELECT duplicate_result_count FROM actor_runs WHERE id=?", (diagnostic_id,))["duplicate_result_count"] == 1


@pytest.mark.asyncio
async def test_incomplete_backfill_skips_regular_post_probe(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    item = {"postId": "p1", "source_url": "https://facebook.com/100/posts/p1", "text": "same"}
    await service.ingester.ingest(1, "post", item, notify=False)
    calls: list[int] = []

    async def fake_fetch(profile, maximum, cursor=None):
        calls.append(maximum)
        return ActorResult([item], {"profiles": [{"status": "succeeded"}]}, "probe")

    service._fetch_posts = fake_fetch
    result = await service._fetch_regular_posts({"id": 1, "backfill_done": 0}, initial=False)

    assert calls == []
    assert result.summary["source"] == "backfill_pending"


@pytest.mark.asyncio
async def test_visit_with_incomplete_backfill_queues_backfill_without_regular_probe(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    await service.ingester.ingest(
        1,
        "post",
        {"postId": "p1", "source_url": "https://facebook.com/100/posts/p1", "text": "latest"},
        notify=False,
    )
    service.db.execute(
        "UPDATE profiles SET public_state='public',backfill_done=0,serp_last_checked_at=? WHERE id=1",
        (datetime.now(UTC).isoformat(),),
    )
    service.db.execute("DELETE FROM jobs")
    calls = 0

    async def fake_regular_posts(profile, initial):
        nonlocal calls
        calls += 1
        return ActorResult([], {"source": "unchanged_probe"}, "probe")

    service._fetch_regular_posts = fake_regular_posts
    await service.visit_profile(1)

    assert calls == 0
    assert service.db.row(
        "SELECT COUNT(*) count FROM jobs WHERE profile_id=1 AND job_type='backfill' AND status='pending'"
    )["count"] == 1


def test_latest_only_completed_profile_is_requeued_for_backfill(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute(
        """INSERT INTO entities(
        profile_id,kind,external_id,source_url,current_hash,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','p1','https://facebook.com/100/posts/p1','hash',1,?,?)""",
        (datetime.now(UTC).isoformat(), datetime.now(UTC).isoformat()),
    )
    service.db.execute(
        "UPDATE profiles SET public_state='public',backfill_done=1,last_full_audit_at=? WHERE id=1",
        (datetime.now(UTC).isoformat(),),
    )
    service.db.execute("DELETE FROM jobs")

    service._seed_latest_only_backfill_repair()

    profile = service.db.row("SELECT backfill_done,backfill_cursor,last_full_audit_at FROM profiles WHERE id=1")
    assert profile == {"backfill_done": 0, "backfill_cursor": None, "last_full_audit_at": None}
    assert service.db.row(
        "SELECT COUNT(*) count FROM jobs WHERE profile_id=1 AND job_type='backfill' AND status='pending'"
    )["count"] == 1


@pytest.mark.asyncio
async def test_cursorless_partial_backfill_stops_repeating_posts_then_queues_comments(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
schedule:
  backfill_posts: 2
  spacing_min_minutes: 0
  spacing_max_minutes: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")

    async def fake_fetch(profile, maximum, cursor=None):
        return ActorResult(
            [
                {"postId": "p1", "source_url": "https://facebook.com/100/posts/p1", "text": "one"},
                {"postId": "p2", "source_url": "https://facebook.com/100/posts/p2", "text": "two"},
            ],
            {"profiles": [{"status": "succeeded", "coverageStatus": "partial_actor_limit", "pointer": {"nextCursor": None}}]},
            "run",
        )

    service._fetch_posts = fake_fetch
    await service.backfill_profile(1)

    queued = service.db.row("SELECT job_type,payload_json FROM jobs WHERE profile_id=1 AND status='pending' ORDER BY id DESC LIMIT 1")
    assert queued["job_type"] == "backfill_comments"
    assert json.loads(queued["payload_json"]) == {"offset": 0, "limited": True}
    assert service.db.row("SELECT COUNT(*) count FROM jobs WHERE profile_id=1 AND job_type='backfill' AND status='pending'")["count"] == 0


@pytest.mark.asyncio
async def test_backfill_comments_reads_saved_posts_in_chunks_and_finishes(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
schedule:
  backfill_posts: 2
  spacing_min_minutes: 0
  spacing_max_minutes: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute("UPDATE profiles SET public_state='public',backfill_done=0 WHERE id=1")
    for number in range(3):
        await service.ingester.ingest(
            1,
            "post",
            {
                "postId": f"p{number}",
                "source_url": f"https://facebook.com/100/posts/p{number}",
                "text": str(number),
                "timestamp": f"2026-08-0{number + 1}T00:00:00+00:00",
            },
            notify=False,
        )
    batches: list[list[str]] = []

    async def fake_comments(profile_id, post_urls, notify):
        batches.append(post_urls)

    service._fetch_comments = fake_comments
    await service.backfill_comments(1, {"offset": 0, "limited": True})
    queued = service.db.row("SELECT payload_json FROM jobs WHERE profile_id=1 AND job_type='backfill_comments' ORDER BY id DESC LIMIT 1")
    next_payload = json.loads(queued["payload_json"])
    assert next_payload == {"offset": 2, "limited": True}
    assert service.db.row("SELECT backfill_done FROM profiles WHERE id=1")["backfill_done"] == 0

    await service.backfill_comments(1, next_payload)

    assert [len(batch) for batch in batches] == [2, 1]
    assert service.db.row("SELECT backfill_done FROM profiles WHERE id=1")["backfill_done"] == 1
    assert service.db.row("SELECT event_type FROM events WHERE profile_id=1 AND event_type='backfill_limited'")["event_type"] == "backfill_limited"


@pytest.mark.asyncio
async def test_full_fetch_exception_probes_before_fetching_batch(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
schedule:
  always_full_fetch_urls:
    - https://facebook.com/100
  always_full_fetch_max_posts: 50
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    item = {"postId": "p1", "source_url": "https://facebook.com/100/posts/p1", "text": "same"}
    await service.ingester.ingest(1, "post", item, notify=False)
    calls: list[int] = []

    async def fake_fetch(profile, maximum, cursor=None):
        calls.append(maximum)
        return ActorResult([item], {"profiles": [{"status": "succeeded"}]}, "probe")

    service._fetch_posts = fake_fetch
    result = await service._fetch_regular_posts({"id": 1, "backfill_done": 1, "url": "https://facebook.com/100"}, initial=False)

    assert calls == [1]
    assert result.summary["source"] == "unchanged_probe"


@pytest.mark.asyncio
async def test_unseenuser_numeric_profile_uses_one_canonical_url(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    calls: list[dict] = []

    async def fake_actor(category, actor_id, payload, profile_id=None, input_variant="default"):
        calls.append(payload)
        return ActorResult([], {"profiles": [{"status": "succeeded"}]}, "run")

    service._actor = fake_actor
    await service._fetch_posts({"id": 1, "url": "https://facebook.com/100", "fb_id": "100"}, 1)

    assert len(calls) == 1
    assert calls[0]["startUrls"] == ["https://www.facebook.com/profile.php?id=100"]


def test_browser_canary_respects_persistent_cooldown(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\nbrowser_canary:\n  cooldown_hours: 72\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    service = MonitorService(load_settings(config))
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert service._browser_canary_due(profile) is True

    service.db.execute(
        "UPDATE profiles SET browser_canary_last_attempt_at=? WHERE id=1",
        ((datetime.now(UTC) - timedelta(hours=71)).isoformat(),),
    )
    assert service._browser_canary_due(service.db.row("SELECT * FROM profiles WHERE id=1")) is False

    service.db.execute(
        "UPDATE profiles SET browser_canary_last_attempt_at=? WHERE id=1",
        ((datetime.now(UTC) - timedelta(hours=73)).isoformat(),),
    )
    assert service._browser_canary_due(service.db.row("SELECT * FROM profiles WHERE id=1")) is True


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


def test_unseenuser_wrapper_preserves_raw_billable_result_count():
    wrapped = ActorResult(
        [{"name": "Profile", "posts": []}],
        None,
        "run",
        raw_result_count=7,
    )

    result = MonitorService._unwrap_embedded_posts(wrapped, 20)

    assert result.raw_result_count == 7
    assert result.items == []


@pytest.mark.asyncio
async def test_paid_unparsed_posts_pause_future_apify_calls(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))

    async def fake_actor(category, actor_id, payload, profile_id=None, input_variant="default"):
        return ActorResult(
            [{"name": "Profile wrapper", "posts": []}],
            None,
            "run",
            charged_usd=0.005,
            diagnostic_id=service.db.start_actor_run(profile_id, category, actor_id, input_variant, payload),
            raw_result_count=1,
        )

    service._actor = fake_actor
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    with pytest.raises(BudgetExceeded, match="安全暫停"):
        await service._fetch_posts(profile, 10)

    paused = service.db.row("SELECT apify_posts_blocked_until,apify_posts_unparsed_streak FROM profiles WHERE id=1")
    assert paused["apify_posts_blocked_until"]
    assert paused["apify_posts_unparsed_streak"] == 1
    run = service.db.row("SELECT * FROM actor_runs ORDER BY id DESC LIMIT 1")
    assert run["raw_result_count"] == 1
    assert run["parsed_result_count"] == 0
    assert run["status"] == "unparsed_paid_result"


@pytest.mark.asyncio
async def test_apify_post_ingest_records_new_and_permalink_duplicate_counts(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    await service.ingester.ingest(
        1,
        "post",
        {"postId": "pfbid123", "source_url": "https://facebook.com/100/posts/pfbid123", "text": "same"},
        notify=False,
    )
    diagnostic_id = service.db.start_actor_run(1, "posts", "actor", "test", {})

    await service._ingest_apify_posts(
        1,
        [
            {"postId": "alias-id", "source_url": "https://facebook.com/permalink.php?story_fbid=pfbid123&id=100", "text": "same"},
            {"postId": "pfbid456", "source_url": "https://facebook.com/100/posts/pfbid456", "text": "new"},
        ],
        notify=False,
        diagnostic_id=diagnostic_id,
    )

    run = service.db.row("SELECT * FROM actor_runs WHERE id=?", (diagnostic_id,))
    assert run["new_result_count"] == 1
    assert run["updated_result_count"] == 0
    assert run["duplicate_result_count"] == 1
    assert service.db.row("SELECT COUNT(*) count FROM entities WHERE profile_id=1 AND kind='post'")["count"] == 2


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
async def test_browser_profile_name_cannot_overwrite_existing_canonical_name(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: FB-100000950467959\n    url: https://facebook.com/100000950467959\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute("UPDATE profiles SET display_name='Ya Ling Shen' WHERE id=1")

    await service._store_profile_details(
        service.db.row("SELECT * FROM profiles WHERE id=1"),
        {
            "id": "100000950467959",
            "name": "慈濟@新竹",
            "url": "https://www.facebook.com/100000950467959",
            "profile_data_source": "Facebook 直接瀏覽器",
        },
    )

    assert service.db.row("SELECT display_name FROM profiles WHERE id=1")["display_name"] == "Ya Ling Shen"


@pytest.mark.asyncio
async def test_browser_profile_can_restore_known_historical_name_and_reject_bad_heading(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: FB-100000950467959\n    url: https://facebook.com/100000950467959\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    trusted_raw = service.settings.data_dir / "trusted-profile.json"
    trusted_raw.parent.mkdir(parents=True, exist_ok=True)
    trusted_raw.write_text(
        '{"name":"Ya Ling Shen","profile_data_source":"SerpApi"}',
        encoding="utf-8",
    )
    entity_id = service.db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'profile','100000950467959','old',1,'2026-08-01','2026-08-09')"""
    )
    service.db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "known", '{"authorName":"Ya Ling Shen"}', str(trusted_raw), "2026-08-01", "created"),
    )
    service.db.execute(
        "UPDATE profiles SET display_name='慈濟@新竹',profile_details_json=? WHERE id=1",
        ('{"name":"慈濟@新竹","profile_data_source":"Facebook 直接瀏覽器"}',),
    )

    await service._store_profile_details(
        service.db.row("SELECT * FROM profiles WHERE id=1"),
        {
            "id": "100000950467959",
            "name": "慈濟@新竹",
            "url": "https://www.facebook.com/100000950467959",
            "profile_data_source": "Facebook 直接瀏覽器",
        },
    )

    repaired = service.db.row("SELECT display_name,profile_details_json FROM profiles WHERE id=1")
    assert repaired["display_name"] == "Ya Ling Shen"
    assert "慈濟@新竹" in repaired["profile_details_json"]
    assert "rejected_profile_names" in repaired["profile_details_json"]

    await service._store_profile_details(
        service.db.row("SELECT * FROM profiles WHERE id=1"),
        {
            "id": "100000950467959",
            "name": "Ya Ling Shen",
            "url": "https://www.facebook.com/100000950467959",
            "profile_data_source": "Facebook 直接瀏覽器",
        },
    )
    assert "慈濟@新竹" in service.db.row("SELECT profile_details_json FROM profiles WHERE id=1")["profile_details_json"]


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

    async def browser_profile(url, diagnostic_key=None):
        browser_calls.append(url)
        assert diagnostic_key == "1"
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

    async def login_required(url, diagnostic_key=None):
        raise FacebookBrowserLoginRequired("login required")

    service.serpapi.profile = failed_serpapi
    service.facebook_browser.profile = login_required
    await service.visit_profile(1)

    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert profile["public_state"] == "unknown"
    assert profile["serp_last_checked_at"] is None
    assert service.db.row("SELECT COUNT(*) count FROM events WHERE event_type='facebook_browser_login_required'")["count"] == 1
    event = service.db.row("SELECT payload_json FROM events WHERE event_type='facebook_browser_login_required'")
    payload = json.loads(event["payload_json"])
    assert "監控網址：https://facebook.com/100" in payload["text"]
    assert payload["source_url"] == "https://facebook.com/100"


@pytest.mark.asyncio
async def test_all_profile_fallback_failure_includes_monitored_url(tmp_path: Path, monkeypatch):
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
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    service = MonitorService(load_settings(config))
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    async def failed_browser(url, diagnostic_key=None):
        raise FacebookBrowserError("no profile data")

    service.facebook_browser.profile = failed_browser
    assert await service._try_browser_fallback(profile, "serp failed", "bright failed") is False

    event = service.db.row("SELECT payload_json FROM events WHERE event_type='profile_fallback_error'")
    payload = json.loads(event["payload_json"])
    assert "監控網址：https://facebook.com/100" in payload["text"]
    assert payload["source_url"] == "https://facebook.com/100"


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


def test_historical_name_repair_restores_latest_known_name(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: FB-100\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    service.db.execute("UPDATE profiles SET display_name='FB-100' WHERE id=1")
    entity_id = service.db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'profile','100','current',1,'2026-08-01','2026-08-03')"""
    )
    service.db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "known", '{"authorName":"吳佳欣"}', "known.json", "2026-08-02", "created"),
    )

    service._seed_historical_name_repair()

    assert service.db.row("SELECT display_name FROM profiles WHERE id=1")["display_name"] == "吳佳欣"


def test_duplicate_profile_preview_cleanup_deletes_database_file_and_thumbnail(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    entity_id = service.db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'profile','100','current',1,'2026-08-01','2026-08-03')"""
    )
    version_id = service.db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "current", '{"authorName":"Alice"}', "current.json", "2026-08-03", "created"),
    )
    service.db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))
    stored = []
    for sha, source_url, role in (
        ("a" * 64, "https://scontent.example.fbcdn.net/v/cover.jpg?quality=high", "cover_photo"),
        ("b" * 64, "https://scontent.example.fbcdn.net/v/cover.jpg?quality=blurred", "image"),
        ("c" * 64, "https://scontent.example.fbcdn.net/v/photo.jpg", "image"),
    ):
        path = service.settings.data_dir / "media" / f"{sha}.jpg"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(sha.encode())
        media_id = service.db.execute(
            """INSERT INTO media(sha256,source_url,mime_type,path,status,first_seen_at)
            VALUES(?,?,?,?,?,?)""",
            (sha, source_url, "image/jpeg", str(path), "ready", "2026-08-03"),
        )
        service.db.execute(
            "INSERT INTO entity_media(entity_id,version_id,media_id,role,position) VALUES(?,?,?,?,?)",
            (entity_id, version_id, media_id, role, len(stored)),
        )
        stored.append((media_id, path, sha))
    thumbnail = service.settings.data_dir / "cache" / "thumbnails" / f"{stored[1][2]}-640.webp"
    thumbnail.parent.mkdir(parents=True, exist_ok=True)
    thumbnail.write_bytes(b"preview")

    counts = service._purge_duplicate_profile_previews()

    assert counts == {"links_removed": 1, "files_removed": 1}
    assert service.db.row("SELECT id FROM media WHERE id=?", (stored[1][0],)) is None
    assert not stored[1][1].exists()
    assert not thumbnail.exists()
    assert service.db.row("SELECT id FROM media WHERE id=?", (stored[0][0],)) is not None
    assert service.db.row("SELECT id FROM media WHERE id=?", (stored[2][0],)) is not None


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
