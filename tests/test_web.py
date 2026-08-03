import os
from pathlib import Path

from fastapi.testclient import TestClient
from PIL import Image

from fb_monitor.config import load_settings
from fb_monitor.serpapi import SerpApiAccount
from fb_monitor.web import create_app


def test_dashboard_health_and_diagnostics_routes(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert 'href="/jobs?status=active"' in dashboard.text
        jobs = client.get("/jobs?status=active")
        assert jobs.status_code == 200
        assert "工作紀錄" in jobs.text
        assert "進行中" in jobs.text
        assert client.get("/healthz").json() == {"ok": True}
        assert client.get("/diagnostics").status_code == 200
        assert client.get("/diagnostics?profile_id=").status_code == 200
        invalid_add = client.post("/profiles")
        assert invalid_add.status_code == 200
        assert "請輸入 Facebook 個人檔案網址" in invalid_add.text


def test_dashboard_shows_official_usage_reset_countdown(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\nbudget:\n  monthly_usd: 5\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    app.state.db.save_apify_usage(4.25, "2026-07-09T00:00:00+00:00", "2026-08-08T23:59:59+00:00")
    app.state.db.save_serpapi_usage(SerpApiAccount("Free Plan", 250, 40, 210, "2026-08-31", 2, 50))

    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "$4.25 / $5.00" in dashboard.text
        assert "重置倒數" in dashboard.text
        assert 'data-cycle-end="2026-08-08T23:59:59+00:00"' in dashboard.text
        assert "40 / 250" in dashboard.text
        assert "Free Plan" in dashboard.text


def test_dashboard_card_shows_serpapi_profile_details(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles:\n  - name: FB-100\n    url: https://www.facebook.com/100\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    details = '{"name":"吳佳欣","id":"pfbid0example","url":"https://www.facebook.com/wu.jia.xin","profile_intro_text":"公開簡介","followers":"1.2K","current_city":"Taipei","works":[{"title":"Engineer"}],"educations":[{"title":"Example University"}]}'
    app.state.db.execute(
        "UPDATE profiles SET display_name='吳佳欣',fb_id='pfbid0example',public_state='public',profile_details_json=?,last_success_at='2026-08-01T00:10:00+00:00',next_visit_at='2026-08-01T00:10:59+00:00' WHERE id=1",
        (details,),
    )

    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        for expected in ("吳佳欣", "公開簡介", "1.2K", "Taipei", "Engineer", "Example University"):
            assert expected in dashboard.text
        assert "Facebook ID：100" in dashboard.text
        assert "https://www.facebook.com/100" in dashboard.text
        assert "https://www.facebook.com/wu.jia.xin" in dashboard.text
        assert "監控網址：" in dashboard.text
        assert "Facebook 網址：" in dashboard.text
        assert "pfbid0example" not in dashboard.text
        assert "2026-08-01 08:10" in dashboard.text
        assert '<div class="profile-card">' in dashboard.text
        assert "data-copy-profile" in dashboard.text


def test_profile_card_links_to_latest_browser_screenshot(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://facebook.com/100
storage:
  data_dir: data
""",
        encoding="utf-8",
    )
    browser_data = tmp_path / "browser-data"
    screenshot = browser_data / "screenshots" / "profile-1.png"
    screenshot.parent.mkdir(parents=True)
    Image.new("RGB", (320, 200), (20, 80, 140)).save(screenshot)
    monkeypatch.setenv("FACEBOOK_BROWSER_DATA_DIR", str(browser_data))
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))

    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert '瀏覽器擷取畫面' in dashboard.text
        assert 'href="/profiles/1/browser-screenshot"' in dashboard.text

        profile = client.get("/profiles/1")
        assert profile.status_code == 200
        assert '瀏覽器擷取畫面' in profile.text

        capture = client.get("/profiles/1/browser-screenshot")
        assert capture.status_code == 200
        assert capture.headers["content-type"] == "image/png"
        assert capture.headers["cache-control"] == "no-store"
        assert client.get("/profiles/999/browser-screenshot").status_code == 404


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
        manual_job = db.row("SELECT * FROM jobs WHERE profile_id=1 AND job_type='visit' AND status='pending'")
        assert manual_job["priority"] == -100
        assert '"manual":true' in manual_job["payload_json"]
        assert db.row("SELECT last_manual_visit_at FROM profiles WHERE id=1")["last_manual_visit_at"] is not None

        cooling_dashboard = client.get("/")
        assert 'data-manual-cooldown=' in cooling_dashboard.text
        assert "冷卻中" in cooling_dashboard.text
        repeated = client.post("/profiles/1/scan", follow_redirects=False)
        assert repeated.status_code == 303
        assert "error=" in repeated.headers["location"]
        assert db.row("SELECT COUNT(*) count FROM jobs WHERE profile_id=1 AND job_type='visit' AND status='pending'")["count"] == 1

        all_profiles = client.post("/profiles/scan-all", follow_redirects=False)
        assert all_profiles.status_code == 303
        assert db.row("SELECT COUNT(*) count FROM jobs WHERE job_type='visit' AND status='pending'")["count"] == 2
        assert db.row("SELECT priority FROM jobs WHERE profile_id=1 AND status='pending'")["priority"] == -100


def test_dashboard_can_add_and_remove_validated_profile_urls(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: first
    url: https://www.facebook.com/first
storage:
  data_dir: data
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db

    with TestClient(app) as client:
        invalid = client.post("/profiles", data={"url": "https://example.com/not-facebook"})
        assert invalid.status_code == 200
        assert "網址必須是" in invalid.text

        added = client.post("/profiles", data={"url": "https://m.facebook.com/profile.php?id=24680"})
        assert added.status_code == 200
        assert "已新增並排程驗證" in added.text
        profile = db.row("SELECT * FROM profiles WHERE url='https://www.facebook.com/profile.php?id=24680'")
        assert profile and profile["enabled"] == 1
        assert db.row("SELECT COUNT(*) count FROM jobs WHERE profile_id=? AND job_type='visit' AND status='pending'", (profile["id"],))["count"] == 1

        removed = client.post(f"/profiles/{profile['id']}/remove")
        assert removed.status_code == 200
        assert "已停止監控並保留歷史資料" in removed.text
        assert db.row("SELECT enabled FROM profiles WHERE id=?", (profile["id"],))["enabled"] == 0
        assert db.row("SELECT COUNT(*) count FROM jobs WHERE profile_id=? AND status='pending'", (profile["id"],))["count"] == 0
