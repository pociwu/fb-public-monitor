import json
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from fb_monitor.config import load_settings
from fb_monitor.db import Database
from fb_monitor.service import MonitorService
from fb_monitor.storage import collect_storage_snapshot, daily_storage_message, storage_delta
from fb_monitor.web import create_app


def test_storage_snapshot_classifies_files_and_calculates_delta(tmp_path: Path, monkeypatch):
    data = tmp_path / "data"
    browser = tmp_path / "browser"
    (data / "media").mkdir(parents=True)
    (data / "profiles").mkdir()
    (data / "cache").mkdir()
    browser.mkdir()
    image = data / "media" / "photo.jpg"
    video = data / "media" / "clip.mp4"
    attachment = data / "media" / "file.bin"
    image.write_bytes(b"i" * 101)
    video.write_bytes(b"v" * 202)
    attachment.write_bytes(b"a" * 303)
    (data / "profiles" / "post.json").write_bytes(b"p" * 404)
    (data / "cache" / "thumb.webp").write_bytes(b"c" * 505)
    (browser / "Cookies").write_bytes(b"b" * 606)
    db = Database(data / "monitor.sqlite3")
    for index, (path, mime) in enumerate(((image, "image/jpeg"), (video, "video/mp4"), (attachment, "application/octet-stream")), 1):
        db.execute(
            "INSERT INTO media(sha256,source_url,mime_type,size_bytes,path,status,first_seen_at) VALUES(?,?,?,?,?,'ready','x')",
            (str(index) * 64, f"https://cdn/{index}", mime, path.stat().st_size, str(path)),
        )
    monkeypatch.setattr("fb_monitor.storage.shutil.disk_usage", lambda path: SimpleNamespace(total=20_000, used=10_000, free=10_000))

    current = collect_storage_snapshot(db, data, browser, "2026-08-04")
    previous = dict(current, snapshot_date="2026-08-03", image_bytes=1)

    assert current["image_bytes"] == 101
    assert current["video_bytes"] == 202
    assert current["attachment_bytes"] == 303
    assert current["content_bytes"] == 404
    assert current["cache_bytes"] == 505
    assert current["browser_bytes"] == 606
    assert current["database_bytes"] > 0
    assert storage_delta(current, previous)["project_used_bytes"] == 100
    message = daily_storage_message(current, previous)["text"]
    assert "fb-public-monitor 專案總用量" in message
    assert "每日增加量：+100 B" in message
    assert "Docker" not in message


def test_daily_storage_notification_is_queued_only_once(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_DATA_DIR", str(tmp_path / "browser"))
    service = MonitorService(load_settings(config))

    assert service.record_daily_storage_snapshot("2026-08-04") is True
    assert service.record_daily_storage_snapshot("2026-08-04") is False
    event = service.db.row("SELECT * FROM events WHERE event_type='storage_daily'")
    payload = json.loads(event["payload_json"])
    assert payload["title"] == "【硬碟每日用量】2026-08-04"
    assert "建立基準" in payload["text"]
    assert service.db.row("SELECT COUNT(*) count FROM outbox WHERE status='pending'")["count"] == 1


def test_dashboard_links_to_storage_detail_page(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_DATA_DIR", str(tmp_path / "browser"))
    app = create_app(load_settings(config))

    with TestClient(app) as client:
        detail = client.get("/storage")
        dashboard = client.get("/")

    assert detail.status_code == 200
    assert "硬碟用量" in detail.text
    assert "目前分類" in detail.text
    assert "最近 30 天" in detail.text
    assert "專案總用量" in detail.text
    assert "其他（含 Docker" not in detail.text
    assert 'href="/storage"' in dashboard.text
    assert "每日增加" in dashboard.text
