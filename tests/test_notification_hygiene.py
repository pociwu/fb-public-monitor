from pathlib import Path

import pytest

from fb_monitor.config import load_settings
from fb_monitor.db import Database
from fb_monitor.ingest import Ingester
from fb_monitor.media import MediaStore
from fb_monitor.service import MonitorService


@pytest.mark.asyncio
async def test_baseline_silently_accepts_first_post_upgrade_change(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/p','x','x')")
    entity_id = db.execute("""INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'post','p1','legacy-hash',1,'x','x')""")
    db.establish_notification_baseline()
    ingester = Ingester(db, tmp_path, MediaStore(db, tmp_path, 0, 30))
    await ingester.ingest(1, "post", {"postId": "p1", "text": "historic post", "publishTime": "2021-01-01T00:00:00+00:00"}, notify=True)
    assert db.row("SELECT notification_hash FROM entities WHERE id=?", (entity_id,))["notification_hash"]
    assert db.row("SELECT COUNT(*) count FROM outbox WHERE status='pending'")["count"] == 0


def test_duplicate_entities_are_merged_into_the_oldest_record(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    db = service.db
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/p','x','x')")
    first = db.execute("""INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'post','original','same',1,'x','x')""")
    second = db.execute("""INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'post','duplicate','same',1,'x','x')""")
    db.execute("INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type) VALUES(?,'v1','{}','a','x','created')", (first,))
    db.execute("INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type) VALUES(?,'v2','{}','b','x','created')", (second,))
    counts = service._dedupe_database()
    assert counts["posts_merged"] == 1
    assert db.row("SELECT COUNT(*) count FROM entities WHERE kind='post'")["count"] == 1
    assert db.row("SELECT COUNT(*) count FROM versions")["count"] == 2
