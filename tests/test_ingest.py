from pathlib import Path

import pytest

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

