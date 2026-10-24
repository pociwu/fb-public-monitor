import json

import pytest

from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService
from fb_monitor.scrapecreators import RESERVED_PROFILE_ID
from fb_monitor.serpapi import SerpApiAccount, SerpApiProfileResult, SerpApiNoResults
from fb_monitor.scrapecreators import ScrapeCreatorsError


def service_fixture(tmp_path, monkeypatch, verdict="confirmed_public"):
    config = tmp_path / "config.yaml"
    config.write_text(f"profiles:\n  - name: target\n    url: https://www.facebook.com/{RESERVED_PROFILE_ID}\nstorage:\n  data_dir: data\n  low_disk_gb: 0\n", encoding="utf-8")
    monkeypatch.setenv("SCRAPECREATORS_ENABLED", "1")
    service = MonitorService(load_settings(config))
    service.db.execute("UPDATE scrapecreators_budget SET remaining=100,observed_remaining=100,blocked_reason=NULL WHERE id=1")
    service.db.record_access_observation(1, source="anonymous_browser", auth_scope="anonymous",
        verdict=verdict, target_fb_id=RESERVED_PROFILE_ID, observed_fb_id=RESERVED_PROFILE_ID, identity_match=True)
    return service


@pytest.mark.asyncio
async def test_slug_probe_requires_numeric_target_verification(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, "unknown")
    service.settings.serpapi_key = "test"
    async def lookup(profile):
        return SerpApiProfileResult(
            {"id": "wu.jia.xin.577667", "url": "https://www.facebook.com/wu.jia.xin.577667", "private": True},
            SerpApiAccount("Free", 250, 10, 240, None, 0, 50))
    monkeypatch.setattr(service, "_lookup_serpapi_profile", lookup)
    await service.detect_public_v2(1)
    assert service.db.row("SELECT * FROM jobs WHERE job_type='verify_public_v2' AND status='pending'")
    latest = service.db.row("SELECT * FROM access_observations ORDER BY id DESC LIMIT 1")
    assert latest["identity_match"] == 0
    assert not service.db.row("SELECT * FROM jobs WHERE job_type='scrapecreators_posts'")


@pytest.mark.asyncio
async def test_public_confirmation_queues_reserved_fallback_without_contract(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, "unknown")
    service.scrapecreators.enabled = True
    service.scrapecreators.api_key = "test"
    service.scrapecreators.free_only_confirmed = True
    async def anonymous(url, diagnostic_key=None):
        return {"id": RESERVED_PROFILE_ID, "url": url, "private": False,
            "public_content_proof": {"kind": "target_permalink_article",
                "permalink": f"https://www.facebook.com/permalink.php?story_fbid=pfbid0PublicPost&id={RESERVED_PROFILE_ID}",
                "post_identity": "pfbid0PublicPost", "target_identity": RESERVED_PROFILE_ID, "article_index": 0}}
    monkeypatch.setattr(service.facebook_anonymous_browser, "profile", anonymous)
    await service.verify_public_v2(1)
    await service.verify_public_v2(1)
    assert service.db.row("SELECT COUNT(*) n FROM jobs WHERE job_type='scrapecreators_posts' AND status='pending'")["n"] == 1


def enable_queue(service):
    service.scrapecreators.enabled = True
    service.scrapecreators.api_key = "test"
    service.scrapecreators.free_only_confirmed = True
    service.settings.spacing_min_minutes = 0
    service.settings.spacing_max_minutes = 0


@pytest.mark.asyncio
async def test_worker_commits_page_and_persists_delayed_successor(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    enable_queue(service)
    calls = []
    async def posts(url, cursor):
        calls.append(cursor)
        return {"posts": [{"id": "p1", "author": {"id": RESERVED_PROFILE_ID}}], "cursor": "next"}
    monkeypatch.setattr(service.scrapecreators, "posts", posts)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    job = service._queue_scrapecreators_posts(profile)
    await service._run_next_job()
    assert calls == [None]
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (job,))["status"] == "done"
    successor = service.db.row("SELECT * FROM jobs WHERE job_type='scrapecreators_posts' AND status='pending'")
    assert successor and successor["available_at"] > successor["created_at"]
    assert service._queue_scrapecreators_posts(profile) is None
    # Public access can be revoked while a page is waiting. No next API call.
    service.db.record_access_observation(1, source="anonymous_browser", auth_scope="anonymous",
        verdict="confirmed_private", target_fb_id=RESERVED_PROFILE_ID, observed_fb_id=RESERVED_PROFILE_ID,
        identity_match=True, observed_at="2999-01-01T00:00:00+00:00")
    service.db.execute("UPDATE jobs SET available_at='2000-01-01T00:00:00+00:00' WHERE id=?", (successor["id"],))
    await service._run_next_job()
    assert calls == [None]
    assert not service.db.row("SELECT * FROM jobs WHERE job_type='scrapecreators_posts' AND status='pending'")


def test_queue_stops_at_trial_limit_but_allows_saved_import(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    enable_queue(service)
    service.db.execute("INSERT INTO provider_trial_usage(provider,used) VALUES ('scrapecreators',5)")
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    assert service._queue_scrapecreators_posts(profile) is None
    service.db.execute("INSERT INTO scrapecreators_captures(profile_id,status,pending_json,updated) VALUES (1,'in_progress','{}',0)")
    assert service._queue_scrapecreators_posts(profile)


@pytest.mark.asyncio
async def test_provider_error_blocks_new_jobs_and_visits(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    enable_queue(service)
    calls = []
    async def posts(*args):
        calls.append(args)
        raise ScrapeCreatorsError("outcome unknown")
    monkeypatch.setattr(service.scrapecreators, "posts", posts)
    await service._capture_scrapecreators_posts(1)
    await service._capture_scrapecreators_posts(1)
    assert len(calls) == 1
    assert service._queue_scrapecreators_posts(service.db.row("SELECT * FROM profiles WHERE id=1")) is None


@pytest.mark.asyncio
async def test_no_results_uses_fallback_then_cools_down_without_empty_telegram(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, "unknown")
    calls = []
    async def refresh(profile):
        calls.append("serpapi")
        raise SerpApiNoResults("no results", SerpApiAccount("Free", 250, 10, 240, None, 0, 50),
            [{"query": RESERVED_PROFILE_ID, "status": "empty"}])
    async def fallback(profile, reason):
        calls.append("fallback")
        service.db.execute("UPDATE profiles SET last_error='browser cooldown; queued' WHERE id=1")
        return False
    monkeypatch.setattr(service, "_refresh_serpapi_profile", refresh)
    monkeypatch.setattr(service, "_try_brightdata_fallback", fallback)
    await service.visit_profile(1)
    await service.visit_profile(1)
    assert calls == ["serpapi", "fallback"]
    assert service.db.row("SELECT COUNT(*) n FROM outbox")["n"] == 0
    assert service.db.serpapi_usage_snapshot()["searches_left"] == 9


@pytest.mark.asyncio
@pytest.mark.parametrize("verdict", ["suspected_public", "confirmed_private", "unknown"])
async def test_not_confirmed_public_never_calls(tmp_path, monkeypatch, verdict):
    service = service_fixture(tmp_path, monkeypatch, verdict)
    async def forbidden(*args):
        pytest.fail("must not spend before public")
    service.scrapecreators.posts = forbidden
    assert await service._capture_scrapecreators_posts(1) is None


@pytest.mark.asyncio
async def test_import_failure_replays_raw_before_advancing_cursor(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    calls = []
    async def posts(*args):
        calls.append(args)
        return {"posts": [{"id": "p1", "author": {"id": RESERVED_PROFILE_ID}, "text": "hello"}], "cursor": "next"}
    service.scrapecreators.posts = posts
    original = service._ingest_capture_v2_posts
    async def fail(*args, **kwargs):
        raise RuntimeError("disk unavailable")
    service._ingest_capture_v2_posts = fail
    with pytest.raises(RuntimeError):
        await service._capture_scrapecreators_posts(1)
    checkpoint = service.db.row("SELECT * FROM scrapecreators_captures")
    assert checkpoint["cursor"] is None
    assert checkpoint["pending_json"]
    service._ingest_capture_v2_posts = original
    result = await service._capture_scrapecreators_posts(1)
    assert result["new"] == 1
    assert len(calls) == 1
    assert service.db.row("SELECT cursor FROM scrapecreators_captures")["cursor"] == "next"


@pytest.mark.asyncio
async def test_repeated_post_dedupes_and_stops_without_notifications(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    async def posts(*args):
        return {"posts": [{"id": "p1", "author": {"id": RESERVED_PROFILE_ID}, "text": "hello"}], "cursor": "next"}
    service.scrapecreators.posts = posts
    assert (await service._capture_scrapecreators_posts(1))["new"] == 1
    assert (await service._capture_scrapecreators_posts(1))["duplicate"] == 1
    assert service.db.row("SELECT COUNT(*) n FROM entities WHERE kind='post'")["n"] == 1
    assert service.db.row("SELECT status FROM scrapecreators_captures")["status"] == "source_limited"


@pytest.mark.asyncio
async def test_wrong_author_does_not_import(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    async def posts(*args):
        return {"posts": [{"id": "p1", "author": {"id": "other"}}], "cursor": "next"}
    service.scrapecreators.posts = posts
    assert await service._capture_scrapecreators_posts(1) is None
    assert service.db.row("SELECT COUNT(*) n FROM entities")["n"] == 0


@pytest.mark.asyncio
async def test_photo_download_uses_existing_media_pipeline(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    calls = []
    async def download(url):
        calls.append(url)
        return {"status": "ready", "sha256": "same-bytes", "path": str(tmp_path / "photo.jpg"),
                "mime_type": "image/jpeg", "size_bytes": 123, "source_url": url}
    monkeypatch.setattr(service.ingester.media, "download", download)
    async def posts(*args):
        return {"posts": [{"id": "p1", "author": {"id": RESERVED_PROFILE_ID},
            "image": "https://cdn.example/photo.jpg", "creation_time": 1792768655}], "cursor": None}
    service.scrapecreators.posts = posts
    await service._capture_scrapecreators_posts(1)
    assert calls == ["https://cdn.example/photo.jpg"]
    assert service.db.row("SELECT status FROM media")["status"] == "ready"
    assert service.db.row("SELECT published_at FROM entities")["published_at"] == "1792768655"
    assert service.db.row("SELECT COUNT(*) n FROM outbox")["n"] == 0


@pytest.mark.asyncio
async def test_private_observation_revokes_previous_public_permission(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    service.db.record_access_observation(1, source="anonymous_browser", auth_scope="anonymous",
        verdict="confirmed_private", target_fb_id=RESERVED_PROFILE_ID,
        observed_fb_id=RESERVED_PROFILE_ID, identity_match=True, observed_at="2999-01-01T00:00:00+00:00")
    async def forbidden(*args):
        pytest.fail("private must revoke permission")
    service.scrapecreators.posts = forbidden
    assert await service._capture_scrapecreators_posts(1) is None


@pytest.mark.asyncio
async def test_regular_visit_dispatches_fallback_when_primary_has_no_contract(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    service.settings.capture_v2_enabled = True
    service.settings.apify_v1_backfill_enabled = False
    service.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")
    monkeypatch.setattr(service, "_serpapi_profile_due", lambda p: False)
    monkeypatch.setattr(service, "_ensure_capture_v2_epoch", lambda *args, **kwargs: None)
    monkeypatch.setattr(service, "_preferred_posts_v2_contract", lambda: None)
    called = []
    def capture(profile):
        called.append(profile["id"])
    monkeypatch.setattr(service, "_queue_scrapecreators_posts", capture)
    await service.visit_profile(1)
    assert called == [1]


@pytest.mark.asyncio
async def test_wrong_numeric_identity_does_not_trigger_verification_or_capture(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, "unknown")
    service.settings.serpapi_key = "test"
    async def lookup(profile):
        return SerpApiProfileResult({"id": "999999", "private": False},
            SerpApiAccount("Free", 250, 10, 240, None, 0, 50))
    monkeypatch.setattr(service, "_lookup_serpapi_profile", lookup)
    await service.detect_public_v2(1)
    assert not service.db.row("SELECT * FROM jobs WHERE job_type IN ('verify_public_v2','scrapecreators_posts')")


def test_request_url_never_proves_slug_identity():
    assert MonitorService._capture_v2_observed_id({"id": "alias", "url": f"https://www.facebook.com/{RESERVED_PROFILE_ID}",
        "_url_from_request": True}) == "alias"


def test_interrupted_reserved_job_recovers_without_stealing_live_lease(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch)
    enable_queue(service)
    job = service._queue_scrapecreators_posts(service.db.row("SELECT * FROM profiles WHERE id=1"))
    service.db.execute("UPDATE jobs SET status='running',lease_owner=NULL,leased_at=NULL WHERE id=?", (job,))
    service._recover_stale_capture_v2_jobs()
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (job,))["status"] == "pending"
