import os
from pathlib import Path

from fastapi.testclient import TestClient
from PIL import Image

from fb_monitor.config import load_settings
from fb_monitor.web import create_app


def test_dashboard_is_read_only(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
        assert client.get("/healthz").json() == {"ok": True}
        assert client.get("/diagnostics").status_code == 200
        assert client.get("/diagnostics?profile_id=").status_code == 200
        assert client.post("/profiles").status_code in {404, 405}


def test_profile_cards_render_media_and_thumbnail_cache(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    image_path = app.state.settings.data_dir / "media" / "sample.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (1200, 800), (40, 120, 200)).save(image_path)
    now = "2026-07-19T12:00:00+00:00"
    entity_id = db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,source_url,published_at,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'post','post-1','https://facebook.com/100/posts/1',?,'hash',1,?,?)""",
        (now, now, now),
    )
    version_id = db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,markdown_path,seen_at,change_type)
        VALUES(?,'hash',?,'raw.json','post.md',?,'created')""",
        (entity_id, '{"id":"post-1","text":"這是可讀的貼文內容","publishTime":"2026-07-19"}', now),
    )
    db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))
    media_id = db.execute(
        """INSERT INTO media(sha256,source_url,mime_type,size_bytes,path,status,first_seen_at)
        VALUES('image-sha','https://cdn.example/image.jpg','image/jpeg',100,?,'ready',?)""",
        (str(image_path), now),
    )
    db.execute("INSERT INTO entity_media(entity_id,version_id,media_id,role,discovery_path,position) VALUES(?,?,?,?,?,0)", (entity_id, version_id, media_id, "image", "$.image"))

    with TestClient(app) as client:
        response = client.get("/profiles/1?kind=post")
        assert response.status_code == 200
        assert "這是可讀的貼文內容" in response.text
        assert f'/media/{media_id}/thumbnail' in response.text
        assert "查看詳細與版本" in response.text
        thumbnail = client.get(f"/media/{media_id}/thumbnail")
        assert thumbnail.status_code == 200
        assert thumbnail.headers["content-type"].startswith("image/webp")
        assert client.get("/profiles/1?kind=post&media_filter=image").status_code == 200


def test_dashboard_profile_order_can_be_dragged_and_persisted(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: first
    url: https://facebook.com/first
  - name: second
    url: https://facebook.com/second
storage:
  data_dir: data
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))

    with TestClient(app) as client:
        initial = client.get("/")
        assert initial.status_code == 200
        assert 'data-profile-cards' in initial.text
        assert initial.text.index("first") < initial.text.index("second")

        reordered = client.post("/profiles/reorder", json={"profile_ids": [2, 1]})
        assert reordered.status_code == 200
        assert reordered.json() == {"ok": True}

        refreshed = client.get("/")
        assert refreshed.text.index("second") < refreshed.text.index("first")
        assert client.post("/profiles/reorder", json={"profile_ids": [1]}).status_code == 400


def test_dashboard_can_queue_single_or_all_profile_visits(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: first
    url: https://facebook.com/first
  - name: second
    url: https://facebook.com/second
storage:
  data_dir: data
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    db.execute("DELETE FROM jobs")

    with TestClient(app) as client:
        dashboard = client.get("/")
        assert "2 / 16" in dashboard.text
        assert "立即拜訪（全部）" in dashboard.text

        single = client.post("/profiles/1/scan", follow_redirects=False)
        assert single.status_code == 303
        assert db.row("SELECT COUNT(*) count FROM jobs WHERE job_type='visit' AND status='pending'")["count"] == 1

        all_profiles = client.post("/profiles/scan-all", follow_redirects=False)
        assert all_profiles.status_code == 303
        assert db.row("SELECT COUNT(*) count FROM jobs WHERE job_type='visit' AND status='pending'")["count"] == 2
