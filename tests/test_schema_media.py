import json
from pathlib import Path

import pytest

from fb_monitor.db import Database
from fb_monitor.ingest import Ingester, embedded_posts, profile_display_name
from fb_monitor.media import MediaStore, extract_media


def test_schema_aware_profile_and_embedded_posts():
    item = {
        "title": "Alice Example",
        "facebookUrl": "https://www.facebook.com/100",
        "personalProfile": {
            "name": "Alice Example",
            "profilePicLarge": "https://cdn.example/avatar-large.jpg",
            "profilePicSmall": "https://cdn.example/avatar-small.jpg",
            "coverPhotoUrl": "https://cdn.example/cover.jpg",
        },
        "posts": [{"postId": "p1", "attachments": [{"media": {"playableUrl": "https://cdn.example/video.mp4"}}]}],
    }
    refs = extract_media(item, "profile")
    assert {(ref.url, ref.role) for ref in refs} == {
        ("https://cdn.example/avatar-large.jpg", "profile_picture"),
        ("https://cdn.example/avatar-small.jpg", "profile_picture"),
        ("https://cdn.example/cover.jpg", "cover_photo"),
    }
    assert all(ref.json_path.startswith("$") for ref in refs)
    assert profile_display_name(item) == "Alice Example"
    assert embedded_posts(item)[0]["postId"] == "p1"


@pytest.mark.asyncio
async def test_telegram_event_is_readable_and_media_is_file(tmp_path: Path, monkeypatch):
    db = Database(tmp_path / "db.sqlite")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('設定別名','https://facebook.com/100','x','x')")
    store = MediaStore(db, tmp_path, 0, 30)
    media_file = tmp_path / "avatar.jpg"
    media_file.write_bytes(b"jpg")

    async def fake_download(url):
        return {"status": "ready", "sha256": "a" * 64, "path": str(media_file), "mime_type": "image/jpeg", "size_bytes": 3, "source_url": url}

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    await ingester.ingest(1, "profile", {"id": "100", "title": "公開名稱", "coverPhotoUrl": "https://cdn.example/cover.jpg"}, notify=True)
    event = db.row("SELECT payload_json FROM events ORDER BY id DESC")
    payload = json.loads(event["payload_json"])
    assert payload["title"] == "【個人檔案新增】公開名稱"
    assert "cdn.example" not in payload["text"]
    media_outbox = db.row("SELECT payload_json FROM outbox WHERE kind='media'")
    assert json.loads(media_outbox["payload_json"])["path"] == str(media_file)
