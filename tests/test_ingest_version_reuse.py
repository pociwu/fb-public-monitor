import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from fb_monitor.db import Database
from fb_monitor.ingest import Ingester
from fb_monitor.media import MediaStore


def make_ingester(tmp_path):
    db = Database(tmp_path / "monitor.sqlite3")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')")
    return db, Ingester(db, tmp_path, MediaStore(db, tmp_path, 0, 30))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["profile", "post", "comment", "photo"])
async def test_historical_content_reuses_version_without_repeat_notification(tmp_path, kind):
    db, ingester = make_ingester(tmp_path)
    a = {"id": "100", "name": "p", "text": "A"}
    b = {**a, "text": "B"}
    entity_id, _, _ = await ingester.ingest(1, kind, a)
    original = db.row("SELECT * FROM versions WHERE entity_id=?", (entity_id,))
    await ingester.ingest(1, kind, b)
    files_before = sorted(path for path in ingester.root.rglob("*") if path.is_file())
    outbox_before = db.rows("SELECT * FROM outbox ORDER BY id")
    events_before = db.rows("SELECT * FROM events ORDER BY id")
    db.execute("UPDATE entities SET present=0,missing_successes=2,last_seen_at='old' WHERE id=?", (entity_id,))

    for _ in range(3):
        same_id, _, changed = await ingester.ingest(1, kind, a)
        assert same_id == entity_id
        assert changed is False

    current = db.row("SELECT * FROM entities WHERE id=?", (entity_id,))
    assert current["current_version_id"] == original["id"]
    assert current["current_hash"] == original["content_hash"]
    assert current["present"] == 1
    assert current["missing_successes"] == 0
    assert current["last_seen_at"] != "old"
    assert db.row("SELECT COUNT(*) n FROM versions")["n"] == 2
    assert db.row("SELECT * FROM versions WHERE id=?", (original["id"],)) == original
    assert sorted(path for path in ingester.root.rglob("*") if path.is_file()) == files_before
    assert db.rows("SELECT * FROM outbox ORDER BY id") == outbox_before
    assert db.rows("SELECT * FROM events ORDER BY id") == events_before

    _, _, changed = await ingester.ingest(1, kind, {**a, "text": "C"})
    assert changed is True
    assert db.row("SELECT COUNT(*) n FROM versions")["n"] == 3
    assert db.row("SELECT COUNT(*) n FROM events")["n"] == len(events_before) + 1
    assert db.row("SELECT notification_group_id FROM events ORDER BY id DESC LIMIT 1")["notification_group_id"] is not None


@pytest.mark.asyncio
async def test_reused_version_repairs_media_on_original_version_without_notification(tmp_path, monkeypatch):
    db, ingester = make_ingester(tmp_path)
    ready = False
    downloads = []

    async def fake_download(url):
        downloads.append(url)
        if not ready:
            return {"status": "pending", "source_url": url, "error": "temporary"}
        return {
            "status": "ready", "source_url": url, "sha256": "repaired-photo",
            "path": str(tmp_path / "photo.jpg"), "mime_type": "image/jpeg", "size_bytes": 123,
        }

    monkeypatch.setattr(ingester.media, "download", fake_download)
    a = {"id": "100", "text": "A", "image": "https://scontent-a.xx.fbcdn.net/photo.jpg"}
    entity_id, _, _ = await ingester.ingest(1, "post", a)
    original_id = db.row("SELECT current_version_id FROM entities")["current_version_id"]
    await ingester.ingest(1, "post", {"id": "100", "text": "B"})
    outbox_before = db.rows("SELECT * FROM outbox ORDER BY id")
    ready = True

    _, _, changed = await ingester.ingest(1, "post", a)
    assert changed is False
    repaired = db.rows("""SELECT em.version_id FROM entity_media em
        JOIN media m ON m.id=em.media_id WHERE em.entity_id=? AND m.status='ready'""", (entity_id,))
    assert repaired == [{"version_id": original_id}]
    assert db.rows("SELECT * FROM outbox ORDER BY id") == outbox_before
    await ingester.ingest(1, "post", a)
    assert len(downloads) == 2


@pytest.mark.parametrize("existing", [False, True])
def test_simultaneous_identical_ingest_is_atomic(tmp_path, monkeypatch, existing):
    db, ingester = make_ingester(tmp_path)
    if existing:
        asyncio.run(ingester.ingest(1, "post", {"id": "100", "text": "A"}))
    start = threading.Barrier(2)
    reads = threading.Barrier(2)
    original_row = db.row

    def synchronize_old_unlocked_read(sql, params=()):
        result = original_row(sql, params)
        if sql.startswith("SELECT * FROM entities WHERE profile_id=? AND kind=? AND external_id=?"):
            # Reproduce two callers seeing the same stale entity snapshot.
            # The fixed transaction must no longer use this unlocked seam.
            reads.wait(timeout=10)
        return result

    monkeypatch.setattr(db, "row", synchronize_old_unlocked_read)

    def ingest_same(_):
        start.wait(timeout=10)
        return asyncio.run(ingester.ingest(1, "post", {"id": "100", "text": "B"}))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(ingest_same, range(2)))
    assert sum(result[2] for result in results) == 1
    assert len({result[0] for result in results}) == 1
    assert db.row("SELECT COUNT(*) n FROM entities")["n"] == 1
    assert db.row("SELECT COUNT(*) n FROM versions")["n"] == (2 if existing else 1)
    current = db.row("SELECT v.normalized_json FROM versions v JOIN entities e ON e.current_version_id=v.id")
    assert json.loads(current["normalized_json"])["text"] == "B"
    assert db.rows("PRAGMA foreign_key_check") == []
