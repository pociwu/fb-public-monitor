from pathlib import Path

import pytest
from PIL import Image

from fb_monitor.db import Database
from fb_monitor.ingest import Ingester
from fb_monitor.media import MediaStore


@pytest.mark.asyncio
async def test_ingest_versions_and_deduplicates(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/p','x','x')")
    ingester = Ingester(db, tmp_path, MediaStore(db, tmp_path, 0, 30))
    _, _, changed = await ingester.ingest(1, "post", {"postId": "1", "text": "hello"}, notify=False)
    assert changed
    _, _, changed = await ingester.ingest(1, "post", {"postId": "1", "text": "hello", "likesCount": 10}, notify=False)
    assert not changed
    _, _, changed = await ingester.ingest(1, "post", {"postId": "1", "text": "edited"}, notify=False)
    assert changed
    assert db.row("SELECT COUNT(*) count FROM versions")["count"] == 2


@pytest.mark.asyncio
async def test_unchanged_profile_upgrades_low_resolution_avatar(tmp_path: Path, monkeypatch):
    db = Database(tmp_path / "db.sqlite")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')")
    store = MediaStore(db, tmp_path, 0, 30)
    low_path = tmp_path / "low.jpg"
    high_path = tmp_path / "high.jpg"
    Image.new("RGB", (24, 24), "gray").save(low_path)
    Image.new("RGB", (720, 720), "navy").save(high_path)

    async def fake_download(url):
        high = "s720x720" in url
        path = high_path if high else low_path
        return {
            "status": "ready",
            "sha256": "high" if high else "low",
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent-nrt1-2.xx.fbcdn.net/v/t39.30808-1/avatar.jpg"
    await ingester.ingest(1, "profile", {"id": "100", "name": "p", "profile_picture": f"{base}?ctp=s24x24"}, notify=False)
    _, _, changed = await ingester.ingest(
        1,
        "profile",
        {"id": "100", "name": "p", "profile_picture": f"{base}?ctp=s720x720"},
        notify=False,
    )

    assert not changed
    assert db.row("SELECT COUNT(*) count FROM versions")["count"] == 1
    selected = db.row(
        """SELECT m.path FROM media m JOIN entity_media em ON em.media_id=m.id
        JOIN entities e ON e.id=em.entity_id WHERE e.profile_id=1 AND em.role='profile_picture'
        ORDER BY em.version_id DESC,m.id DESC LIMIT 1"""
    )
    assert selected["path"] == str(high_path)
