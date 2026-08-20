import json
import os
from pathlib import Path

from fastapi.testclient import TestClient
from PIL import Image

from fb_monitor.config import load_settings
from fb_monitor.serpapi import SerpApiAccount
from fb_monitor.web import create_app


def _capture_v2_test_app(tmp_path: Path, monkeypatch, *, enabled: bool = True):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: special
    url: https://www.facebook.com/100027675104517
  - name: regular
    url: https://www.facebook.com/200
storage:
  data_dir: data
actors:
  posts_v2_primary: example/posts-primary
  posts_v2_fallback: example/posts-fallback
  posts_input:
    startUrls: "{urls}"
capture_v2:
  enabled: true
  v1_backfill_enabled: false
  special_profile_id: "100027675104517"
  contract_test_budget_usd: 0.20
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("CAPTURE_V2_ENABLED", "1" if enabled else "0")
    monkeypatch.setenv("APIFY_V1_BACKFILL_ENABLED", "0")
    return create_app(load_settings(config))


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


def test_diagnostics_shows_per_profile_apify_outcomes_and_raw_samples(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    db.save_apify_usage(1.25, "2026-08-09T00:00:00+00:00", "2026-09-08T23:59:59+00:00")
    run_id = db.start_actor_run(1, "posts", "unseenuser/fb-profile", "profile_php", {"token": "secret", "maxPosts": 1})
    db.finish_actor_run(
        run_id,
        status="succeeded",
        run_id="actor-run-1",
        result_count=1,
        raw_result_count=1,
        parsed_result_count=1,
        charged_usd=0.005,
        samples=[{"postId": "post-1", "text": "sample"}],
    )
    db.update_actor_ingest_counts(run_id, new=0, updated=0, duplicate=1)

    with TestClient(app) as client:
        dashboard = client.get("/")
        diagnostics = client.get("/diagnostics?profile_id=1")

    assert 'href="/diagnostics?profile_id=1"' in dashboard.text
    assert diagnostics.status_code == 200
    assert "本帳期各帳號 Apify 貼文用量" in diagnostics.text
    assert "原始 1／解析 1／新增 0／更新 0／重複 1" in diagnostics.text
    assert "原始回傳樣本（最多 5 筆）" in diagnostics.text
    assert "post-1" in diagnostics.text
    assert "secret" not in diagnostics.text


def test_page_footer_shows_deployed_version_and_taipei_update_time(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\nstorage:\n  data_dir: data\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("APP_VERSION", "331c1cd")
    monkeypatch.setenv("APP_UPDATED_AT", "2026-08-03T04:05:00+00:00")
    app = create_app(load_settings(config))

    with TestClient(app) as client:
        dashboard = client.get("/")

    assert dashboard.status_code == 200
    assert "版本：331c1cd" in dashboard.text
    assert "更新時間：2026-08-03 12:05" in dashboard.text


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


def test_dashboard_recovers_and_shows_recorded_profile_names(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: FB-100\n    url: https://www.facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    db.execute(
        "UPDATE profiles SET profile_details_json=? WHERE id=1",
        ('{"rejected_profile_names":["慈濟@新竹"]}',),
    )
    entity_id = db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'profile','100','new',1,'2026-08-01','2026-08-03')"""
    )
    db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "old", '{"authorName":"吳佳欣"}', "old.json", "2026-08-01", "created"),
    )
    db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "older", '{"authorName":"吳小姐"}', "older.json", "2026-07-01", "created"),
    )
    db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "rejected", '{"authorName":"慈濟@新竹"}', "rejected.json", "2026-08-02", "created"),
    )

    with TestClient(app) as client:
        dashboard = client.get("/")

    assert dashboard.status_code == 200
    assert "吳佳欣" in dashboard.text
    assert "曾用名稱" in dashboard.text
    assert "吳小姐" in dashboard.text
    assert "慈濟@新竹" not in dashboard.text


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


def test_dashboard_hides_cover_preview_from_public_photos(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    entity_id = db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'profile','100','current',1,'2026-08-01','2026-08-03')"""
    )
    version_id = db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "current", '{"authorName":"吳佳欣"}', "current.json", "2026-08-03", "created"),
    )
    db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))
    media = []
    for sha, source_url, role in (
        ("cover", "https://scontent.example.fbcdn.net/v/cover.jpg?quality=high", "cover_photo"),
        ("blur", "https://scontent.example.fbcdn.net/v/cover.jpg?quality=blurred", "image"),
        ("photo", "https://scontent.example.fbcdn.net/v/photo.jpg", "image"),
    ):
        media_id = db.execute(
            """INSERT INTO media(sha256,source_url,mime_type,path,status,first_seen_at)
            VALUES(?,?,?,'sample.jpg','ready','2026-08-03')""",
            (sha, source_url, "image/jpeg"),
        )
        db.execute(
            "INSERT INTO entity_media(entity_id,version_id,media_id,role,position) VALUES(?,?,?,?,?)",
            (entity_id, version_id, media_id, role, len(media)),
        )
        media.append(media_id)

    with TestClient(app) as client:
        dashboard = client.get("/")

    assert dashboard.status_code == 200
    assert f'/media/{media[1]}/thumbnail' not in dashboard.text
    assert f'/media/{media[2]}/thumbnail' in dashboard.text


def test_dashboard_deduplicates_profile_photos_with_same_perceptual_hash(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    entity_id = db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at)
        VALUES(1,'profile','100','current',1,'2026-08-01','2026-08-03')"""
    )
    version_id = db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,?,?,?,?,?)""",
        (entity_id, "current", '{"authorName":"Mina Lin"}', "current.json", "2026-08-03", "created"),
    )
    db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))
    media = []
    for sha, source_url, size in (
        ("small", "https://scontent.example.fbcdn.net/v/photo-small.jpg", 12000),
        ("large", "https://scontent.example.fbcdn.net/v/photo-large.jpg", 48000),
    ):
        media_id = db.execute(
            """INSERT INTO media(sha256,source_url,mime_type,size_bytes,path,perceptual_hash,status,first_seen_at)
            VALUES(?,?,?,?,?,'0123456789abcdef','ready','2026-08-03')""",
            (sha, source_url, "image/jpeg", size, f"{sha}.jpg"),
        )
        db.execute(
            "INSERT INTO entity_media(entity_id,version_id,media_id,role,position) VALUES(?,?,?,?,?)",
            (entity_id, version_id, media_id, "image", len(media)),
        )
        media.append(media_id)

    with TestClient(app) as client:
        dashboard = client.get("/")

    assert dashboard.status_code == 200
    rendered = [media_id for media_id in media if f'/media/{media_id}/thumbnail' in dashboard.text]
    assert rendered == [media[1]]


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


def test_dashboard_can_queue_immediate_browser_visit(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    app = create_app(load_settings(config))
    db = app.state.db
    db.execute("DELETE FROM jobs")

    with TestClient(app) as client:
        dashboard = client.get("/")
        assert "立即瀏覽器拜訪" in dashboard.text
        queued = client.post("/profiles/1/browser-scan", follow_redirects=False)
        repeated = client.post("/profiles/1/browser-scan", follow_redirects=False)

    assert queued.status_code == 303
    assert repeated.status_code == 303
    assert "error=" in repeated.headers["location"]
    job = db.row("SELECT * FROM jobs WHERE profile_id=1 AND job_type='browser_visit' AND status='pending'")
    assert job and job["priority"] == -110 and '"manual":true' in job["payload_json"]


def test_dashboard_can_freeze_and_unfreeze_apify_per_profile(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles:\n  - name: watched\n    url: https://facebook.com/100\nstorage:\n  data_dir: data\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db

    with TestClient(app) as client:
        dashboard = client.get("/")
        frozen = client.post("/profiles/1/apify-freeze", follow_redirects=False)
        frozen_dashboard = client.get("/")
        unfrozen = client.post("/profiles/1/apify-freeze", follow_redirects=False)

    assert "凍結 Apify" in dashboard.text
    assert frozen.status_code == 303
    assert "解除 Apify 凍結" in frozen_dashboard.text
    assert "Apify 已凍結" in frozen_dashboard.text
    assert unfrozen.status_code == 303
    assert db.row("SELECT apify_frozen FROM profiles WHERE id=1")["apify_frozen"] == 0


def test_dashboard_can_refresh_profile_name(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: FB-100000950467959
    url: https://www.facebook.com/100000950467959
storage:
  data_dir: data
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    app = create_app(load_settings(config))
    db = app.state.db
    db.execute(
        "UPDATE profiles SET display_name='錯誤名稱',serp_last_checked_at='2026-08-09T00:00:00+00:00' WHERE id=1"
    )
    db.execute("DELETE FROM jobs")

    with TestClient(app) as client:
        response = client.post("/profiles/1/refresh-name", follow_redirects=False)

    assert response.status_code == 303
    profile = db.row("SELECT display_name,serp_last_checked_at FROM profiles WHERE id=1")
    assert profile["display_name"] is None
    assert profile["serp_last_checked_at"] is None
    job = db.row("SELECT * FROM jobs WHERE profile_id=1 AND job_type='visit' AND status='pending'")
    assert job and '"manual":true' in job["payload_json"]


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


def test_capture_v2_status_dashboard_and_profile_show_auditable_progress(tmp_path: Path, monkeypatch):
    app = _capture_v2_test_app(tmp_path, monkeypatch)
    db = app.state.db
    actor_id = app.state.settings.actors.posts_v2_primary
    fingerprint = app.state.service._posts_v2_fingerprint(actor_id)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id=actor_id,
        purpose="posts_backfill",
        schema_fingerprint=fingerprint,
        status="passed",
        evidence={"cursor": True},
    )
    epoch, _ = db.get_or_create_capture_epoch(
        1,
        "public_transition",
        status="ready",
        priority=-300,
        scope={"all_public_history": True},
        reserved_budget_usd=4,
    )
    stream = db.upsert_coverage_stream(
        int(epoch["id"]),
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=int(contract["id"]),
    )
    db.update_coverage_stream(
        int(stream["id"]),
        status="in_progress",
        input_cursor="cursor-in",
        output_cursor="cursor-out",
        seen_count=8,
        new_count=3,
        duplicate_count=5,
    )
    db.update_coverage_stream(
        int(stream["id"]),
        status="complete",
        terminal_evidence_json={"cursor_exhausted": True, "last_cursor": "cursor-out"},
    )
    batch, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=int(epoch["id"]),
        coverage_stream_id=int(stream["id"]),
        contract_id=int(contract["id"]),
        provider="apify",
        actor_id=actor_id,
        intent="manual_continue",
        observation_window="page-1",
        normalized_input={"url": "https://www.facebook.com/100027675104517"},
        input_cursor="cursor-in",
    )
    batch = db.transition_paid_source_batch(int(batch["id"]), "launching", run_id="run-1")
    batch = db.transition_paid_source_batch(int(batch["id"]), "run_started", dataset_id="dataset-1")
    batch = db.transition_paid_source_batch(
        int(batch["id"]),
        "raw_saved",
        raw_path="raw/capture-v2/batch-1.json.gz",
        raw_sha256="a" * 64,
        charged_usd=0.0123,
        raw_result_count=8,
        output_cursor="cursor-out",
    )
    batch = db.transition_paid_source_batch(
        int(batch["id"]),
        "imported",
        parsed_result_count=8,
        new_result_count=3,
        duplicate_result_count=5,
    )
    db.transition_paid_source_batch(int(batch["id"]), "committed")
    failed, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=int(epoch["id"]),
        coverage_stream_id=int(stream["id"]),
        contract_id=int(contract["id"]),
        provider="apify",
        actor_id=actor_id,
        intent="manual_continue",
        observation_window="page-2",
        normalized_input={"cursor": "cursor-out"},
        input_cursor="cursor-out",
    )
    db.transition_paid_source_batch(int(failed["id"]), "failed", error="fixture actor failure")

    with TestClient(app) as client:
        dashboard = client.get("/")
        status = client.get("/capture-v2?profile_id=1")
        profile = client.get("/profiles/1")

    assert dashboard.status_code == 200
    assert 'href="/capture-v2"' in dashboard.text
    assert status.status_code == 200
    for expected in (
        "Capture V2 狀態",
        "V1 付費回溯",
        "安全停用",
        actor_id,
        fingerprint,
        "cursor-in",
        "cursor-out",
        "終點證據",
        "cursor_exhausted",
        "新增 3",
        "重複 5",
        "$0.0123",
        "committed",
        "raw/capture-v2/batch-1.json.gz",
        "fixture actor failure",
    ):
        assert expected in status.text
    assert profile.status_code == 200
    assert "Capture V2 回溯" in profile.text
    assert "posts / timeline_posts" in profile.text
    assert "cursor-out" in profile.text


def test_capture_v2_contract_test_only_queues_one_job_and_obeys_freeze(tmp_path: Path, monkeypatch):
    app = _capture_v2_test_app(tmp_path, monkeypatch)
    db = app.state.db
    db.execute("DELETE FROM jobs")
    primary = app.state.settings.actors.posts_v2_primary
    fallback = app.state.settings.actors.posts_v2_fallback
    db.record_access_observation(
        1,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id="100027675104517",
        observed_fb_id="100027675104517",
        identity_match=True,
    )

    with TestClient(app) as client:
        page = client.get("/capture-v2?profile_id=1")
        granted = client.post("/capture-v2/contract-grants", follow_redirects=False)
        grant = db.contract_test_grant_ledger()
        first = client.post(
            "/profiles/1/capture-v2/contract-test",
            data={"actor_id": primary, "fixture_ack": "1"},
            follow_redirects=False,
        )
        repeated = client.post(
            "/profiles/1/capture-v2/contract-test",
            data={"actor_id": primary, "fixture_ack": "1"},
            follow_redirects=False,
        )
        db.set_profile_source_control(2, "apify", frozen=True, reason="test")
        frozen = client.post(
            "/profiles/2/capture-v2/contract-test",
            data={"actor_id": fallback, "fixture_ack": "1"},
            follow_redirects=False,
        )

    assert page.status_code == 200
    assert "全域共用上限 $0.20" in page.text
    assert granted.status_code == 303 and "notice=" in granted.headers["location"]
    assert grant and grant["status"] == "active"
    assert first.status_code == repeated.status_code == frozen.status_code == 303
    assert "notice=" in first.headers["location"]
    assert "error=" in frozen.headers["location"]
    jobs = db.rows("SELECT * FROM jobs WHERE job_type='contract_test_posts_v2'")
    assert len(jobs) == 1
    assert jobs[0]["profile_id"] == 1
    payload = json.loads(jobs[0]["payload_json"])
    assert payload["actor_id"] == primary
    assert payload["max_budget_usd"] == 0.20
    assert payload["contract_grant_id"] == grant["id"]
    assert payload["contract_test_id"].startswith(f"grant:{grant['id']}:")
    assert jobs[0]["dedupe_key"].startswith(f"contract-grant:{grant['id']}:{primary}:")
    assert db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0


def test_capture_v2_paid_mutations_reject_cross_site_browser_forms(tmp_path: Path, monkeypatch):
    app = _capture_v2_test_app(tmp_path, monkeypatch)
    db = app.state.db
    db.execute("DELETE FROM jobs")
    db.record_access_observation(
        1,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id="100027675104517",
        observed_fb_id="100027675104517",
        identity_match=True,
    )
    primary = app.state.settings.actors.posts_v2_primary

    with TestClient(app) as client:
        malicious_grant = client.post(
            "/capture-v2/contract-grants",
            headers={"Origin": "https://attacker.invalid"},
        )
        same_origin_grant = client.post(
            "/capture-v2/contract-grants",
            headers={"Origin": "http://testserver:80"},
            follow_redirects=False,
        )
        malicious_test = client.post(
            "/profiles/1/capture-v2/contract-test",
            data={"actor_id": primary, "fixture_ack": "1"},
            headers={"Referer": "https://attacker.invalid/form"},
        )
        malicious_continue = client.post(
            "/profiles/1/capture-v2/continue",
            headers={"Origin": "null"},
        )
        malicious_regular_scan = client.post(
            "/profiles/1/scan",
            headers={"Origin": "https://attacker.invalid"},
        )
        headerless_cli_scan = client.post(
            "/profiles/1/scan",
            follow_redirects=False,
        )

    assert malicious_grant.status_code == 403
    assert same_origin_grant.status_code == 303
    assert malicious_test.status_code == malicious_continue.status_code == 403
    assert malicious_regular_scan.status_code == 403
    assert headerless_cli_scan.status_code == 303
    assert db.row("SELECT COUNT(*) count FROM jobs")["count"] == 1
    assert db.row("SELECT COUNT(*) count FROM contract_test_grants")["count"] == 1


def test_capture_v2_contract_fixture_rejects_newer_strong_private_evidence(
    tmp_path: Path, monkeypatch
):
    app = _capture_v2_test_app(tmp_path, monkeypatch)
    db = app.state.db
    db.execute("DELETE FROM jobs")
    db.record_access_observation(
        1,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id="100027675104517",
        observed_fb_id="100027675104517",
        identity_match=True,
        observed_at="2026-08-19T00:00:00+00:00",
    )
    db.record_access_observation(
        1,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_private",
        target_fb_id="100027675104517",
        observed_fb_id="100027675104517",
        identity_match=True,
        observed_at="2026-08-20T00:00:00+00:00",
    )
    primary = app.state.settings.actors.posts_v2_primary

    with TestClient(app) as client:
        page = client.get("/capture-v2?profile_id=1")
        client.post("/capture-v2/contract-grants", follow_redirects=False)
        queued = client.post(
            "/profiles/1/capture-v2/contract-test",
            data={"actor_id": primary, "fixture_ack": "1"},
            follow_redirects=False,
        )

    assert page.status_code == 200
    assert "最新強存取證據為 confirmed_private" in page.text
    assert queued.status_code == 303 and "error=" in queued.headers["location"]
    assert db.row("SELECT COUNT(*) count FROM jobs")["count"] == 0


def test_capture_v2_continue_requires_exact_passed_contract_and_is_idempotent(tmp_path: Path, monkeypatch):
    app = _capture_v2_test_app(tmp_path, monkeypatch)
    db = app.state.db
    db.execute("DELETE FROM jobs")
    actor_id = app.state.settings.actors.posts_v2_primary
    with TestClient(app) as client:
        missing = client.post("/profiles/1/capture-v2/continue", follow_redirects=False)
        db.upsert_actor_contract(
            provider="apify",
            actor_id=actor_id,
            purpose="posts_backfill",
            schema_fingerprint="stale-fingerprint",
            status="passed",
        )
        stale = client.post("/profiles/1/capture-v2/continue", follow_redirects=False)
        fingerprint = app.state.service._posts_v2_fingerprint(actor_id)
        contract = db.upsert_actor_contract(
            provider="apify",
            actor_id=actor_id,
            purpose="posts_backfill",
            schema_fingerprint=fingerprint,
            status="passed",
        )
        unverified = client.post(
            "/profiles/1/capture-v2/continue", follow_redirects=False
        )
        assert "error=" in unverified.headers["location"]
        assert db.row(
            "SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1"
        )["count"] == 0
        db.record_access_observation(
            1,
            source="anonymous_browser",
            auth_scope="anonymous",
            verdict="confirmed_public",
            target_fb_id="100",
            observed_fb_id="100",
            identity_match=True,
        )
        first = client.post("/profiles/1/capture-v2/continue", follow_redirects=False)
        repeated = client.post("/profiles/1/capture-v2/continue", follow_redirects=False)
        db.set_profile_source_control(2, "apify", frozen=True, reason="test")
        frozen = client.post("/profiles/2/capture-v2/continue", follow_redirects=False)

    assert "error=" in missing.headers["location"]
    assert "error=" in stale.headers["location"]
    assert first.status_code == repeated.status_code == frozen.status_code == 303
    assert "notice=" in first.headers["location"]
    assert "error=" in frozen.headers["location"]
    assert db.row("SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1 AND is_active=1")["count"] == 1
    stream = db.row("SELECT * FROM coverage_streams WHERE contract_id=?", (contract["id"],))
    assert stream and stream["stream"] == "posts" and stream["surface"] == "timeline_posts"
    jobs = db.rows("SELECT * FROM jobs WHERE job_type='capture_posts_v2'")
    assert len(jobs) == 1
    assert json.loads(jobs[0]["payload_json"]) == {
        "epoch_id": stream["epoch_id"],
        "coverage_stream_id": stream["id"],
        "surface": "timeline_posts",
    }
    assert jobs[0]["epoch_id"] == stream["epoch_id"]
    assert db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0


def test_capture_v2_actions_fail_closed_when_feature_is_disabled(tmp_path: Path, monkeypatch):
    app = _capture_v2_test_app(tmp_path, monkeypatch, enabled=False)
    db = app.state.db
    db.execute("DELETE FROM jobs")
    with TestClient(app) as client:
        page = client.get("/capture-v2")
        contract = client.post(
            "/profiles/1/capture-v2/contract-test", follow_redirects=False
        )
        capture = client.post("/profiles/1/capture-v2/continue", follow_redirects=False)

    assert page.status_code == 200
    assert "未啟用" in page.text
    assert "安全停用" in page.text
    assert contract.status_code == capture.status_code == 303
    assert "error=" in contract.headers["location"]
    assert "error=" in capture.headers["location"]
    assert db.row("SELECT COUNT(*) count FROM jobs")["count"] == 0
