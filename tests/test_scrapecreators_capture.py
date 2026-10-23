import json

import pytest

from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService
from fb_monitor.scrapecreators import RESERVED_PROFILE_ID


def service_fixture(tmp_path, monkeypatch, verdict="confirmed_public"):
    config = tmp_path / "config.yaml"
    config.write_text(f"profiles:\n  - name: target\n    url: https://www.facebook.com/{RESERVED_PROFILE_ID}\nstorage:\n  data_dir: data\n  low_disk_gb: 0\n", encoding="utf-8")
    monkeypatch.setenv("SCRAPECREATORS_ENABLED", "1")
    service = MonitorService(load_settings(config))
    service.db.record_access_observation(1, source="anonymous_browser", auth_scope="anonymous",
        verdict=verdict, target_fb_id=RESERVED_PROFILE_ID, observed_fb_id=RESERVED_PROFILE_ID, identity_match=True)
    return service


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
    async def capture(profile_id):
        called.append(profile_id)
    monkeypatch.setattr(service, "_capture_scrapecreators_posts", capture)
    await service.visit_profile(1)
    assert called == [1]
