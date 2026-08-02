from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from fb_monitor.config import load_settings
from fb_monitor.db import Database
from fb_monitor.ingest import Ingester
from fb_monitor.media import MediaStore
from fb_monitor.timeutil import display_time, telegram_time
from fb_monitor.web import create_app
from fb_monitor.apify import ActorResult
from fb_monitor.service import MonitorService


def _entity(db: Database) -> int:
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/p','x','x')")
    return db.execute("""INSERT INTO entities(profile_id,kind,external_id,present,first_seen_at,last_seen_at)
        VALUES(1,'post','post-1',1,'x','x')""")


def test_content_events_are_coalesced_and_media_is_globally_reserved(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    entity_id = _entity(db)
    first = db.add_event("first", "post_created", {"title": "post", "text": "one"}, 1, entity_id, coalesce=True)
    second = db.add_event("second", "post_updated", {"title": "post", "text": "two"}, 1, entity_id, coalesce=True)
    assert first and second
    assert db.row("SELECT COUNT(*) count FROM notification_groups")["count"] == 1
    assert db.row("SELECT COUNT(*) count FROM outbox WHERE kind='summary'")["count"] == 1

    db.execute("INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at) VALUES(?,'media','{}','x','x')", (first,))
    db.bind_media_notification(first, "a" * 64)
    media = db.row("SELECT group_id,media_sha256 FROM outbox WHERE kind='media'")
    assert media["group_id"]
    assert media["media_sha256"] == "a" * 64


@pytest.mark.asyncio
async def test_comment_fallback_fingerprint_avoids_actor_id_duplicates(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/p','x','x')")
    ingester = Ingester(db, tmp_path, MediaStore(db, tmp_path, 0, 30))
    await ingester.ingest(1, "comment", {"commentId": "actor-a", "authorName": "A", "text": "same", "timestamp": "2026-07-21T06:00:10+00:00"}, notify=False, parent_external_id="post-1")
    await ingester.ingest(1, "comment", {"commentId": "actor-b", "authorName": "A", "text": "same", "timestamp": "2026-07-21T06:00:55+00:00"}, notify=False, parent_external_id="post-1")
    assert db.row("SELECT COUNT(*) count FROM entities WHERE kind='comment'")["count"] == 1


def test_time_display_and_dashboard_cancel(tmp_path: Path, monkeypatch):
    assert display_time(1760534325) == "2025-10-15 21:18"
    assert telegram_time("2026-08-02T03:35:59+00:00") == "2026/8/2 11:35"
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    outbox_id = app.state.db.execute("INSERT INTO outbox(kind,payload_json,next_attempt_at,created_at) VALUES('text','{}','x','x')")
    with TestClient(app) as client:
        assert client.post(f"/outbox/{outbox_id}/cancel").status_code == 200
    assert app.state.db.row("SELECT status FROM outbox WHERE id=?", (outbox_id,))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_first_comment_fetch_for_an_old_post_is_a_silent_baseline(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("""profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n  low_disk_gb: 0\nbudget:\n  monthly_usd: 5\n""", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    post_url = "https://facebook.com/100/posts/old"

    async def old_comments(*args, **kwargs):
        return ActorResult([{"commentId": "old-1", "facebookUrl": post_url, "authorName": "A", "text": "historic", "timestamp": "2021-12-12T01:21:00+00:00"}], None, "old")

    service._actor = old_comments
    await service._fetch_comments(1, [post_url], notify=True)
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 0
    assert service.db.row("SELECT 1 FROM comment_baselines WHERE profile_id=1 AND parent_external_id=?", (post_url,))

    async def new_comments(*args, **kwargs):
        return ActorResult([{"commentId": "old-1", "facebookUrl": post_url, "authorName": "A", "text": "historic", "timestamp": "2021-12-12T01:21:00+00:00"}, {"commentId": "new-1", "facebookUrl": post_url, "authorName": "B", "text": "new", "timestamp": "2026-07-23T01:21:00+00:00"}], None, "new")

    service._actor = new_comments
    await service._fetch_comments(1, [post_url], notify=True)
    assert service.db.row("SELECT COUNT(*) count FROM outbox WHERE kind='summary'")["count"] == 1
