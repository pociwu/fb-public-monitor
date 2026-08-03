import json
from pathlib import Path

import pytest

from fb_monitor.db import Database, utcnow
from fb_monitor.telegram import TelegramSender


@pytest.mark.asyncio
async def test_pending_photos_are_sent_as_ten_item_albums(tmp_path: Path, monkeypatch):
    db = Database(tmp_path / "monitor.sqlite3")
    now = utcnow()
    event_id = db.execute(
        "INSERT INTO events(event_key,event_type,payload_json,created_at) VALUES('album','test','{}',?)",
        (now,),
    )
    group_id = db.execute(
        "INSERT INTO notification_groups(payload_json,status,created_at,ready_at,sent_at) VALUES('{}','sent',?,?,?)",
        (now, now, now),
    )
    for index in range(12):
        path = tmp_path / f"photo-{index}.jpg"
        path.write_bytes(b"image")
        payload = json.dumps(
            {"path": str(path), "mime_type": "image/jpeg", "caption": f"照片 {index + 1}"},
            ensure_ascii=False,
        )
        db.execute(
            "INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at,group_id) VALUES(?,'media',?,?,?,?)",
            (event_id, payload, now, now, group_id),
        )

    sender = TelegramSender(db, "token", "chat", interval_seconds=0)
    album_sizes: list[int] = []

    async def send_group(rows):
        album_sizes.append(len(rows))

    async def unexpected_single(payload):
        raise AssertionError("multiple photos should use sendMediaGroup")

    monkeypatch.setattr(sender, "_send_media_group", send_group)
    monkeypatch.setattr(sender, "_send_media", unexpected_single)

    assert await sender.drain_once() is True
    assert await sender.drain_once() is True

    assert album_sizes == [10, 2]
    assert db.row("SELECT COUNT(*) count FROM outbox WHERE status='sent'")["count"] == 12
    assert db.row("SELECT COUNT(*) count FROM outbox WHERE status='pending'")["count"] == 0
