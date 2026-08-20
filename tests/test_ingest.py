import json
import sqlite3
from pathlib import Path

import pytest
from PIL import Image

from fb_monitor.capture_coordinator import evaluate_post_media
from fb_monitor.capture_v2 import CoverageStatus
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
    low_path = store.root / "low.jpg"
    high_path = store.root / "high.jpg"
    Image.new("RGB", (24, 24), "gray").save(low_path)
    Image.new("RGB", (720, 720), "gray").save(high_path)

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
    assert db.row("SELECT COUNT(*) count FROM media")["count"] == 1
    assert not low_path.exists()
    assert high_path.exists()


@pytest.mark.asyncio
async def test_unchanged_post_still_upgrades_new_media_representation(
    tmp_path: Path,
    monkeypatch,
):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    low_path = store.root / "post-low.png"
    high_path = store.root / "post-high.png"
    Image.new("RGB", (32, 32), "navy").save(low_path)
    Image.new("RGB", (900, 900), "navy").save(high_path)

    async def fake_download(url):
        high = "quality=high" in url
        path = high_path if high else low_path
        return {
            "status": "ready",
            "sha256": "post-high" if high else "post-low",
            "perceptual_hash": "ffffffffffffffff",
            "path": str(path),
            "mime_type": "image/png",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent.example.fbcdn.net/v/unchanged-post.png"
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "same", "image": f"{base}?quality=low"},
        notify=False,
    )
    _, _, changed = await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "same", "image": f"{base}?quality=high"},
        notify=False,
    )

    assert changed is False
    assert db.row("SELECT COUNT(*) count FROM versions")["count"] == 1
    assert db.row("SELECT COUNT(*) count FROM media")["count"] == 1
    winner = db.row("SELECT sha256,path,source_url FROM media")
    assert winner == {
        "sha256": "post-high",
        "path": str(high_path),
        "source_url": f"{base}?quality=high",
    }
    assert not low_path.exists()


@pytest.mark.asyncio
async def test_failed_high_representation_does_not_reuse_low_readiness(
    tmp_path: Path,
    monkeypatch,
):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    low_path = store.root / "coverage-low.png"
    Image.new("RGB", (32, 32), "navy").save(low_path)

    async def fake_download(url):
        if "quality=high" in url:
            return {"status": "pending", "source_url": url, "error": "failed high"}
        return {
            "status": "ready",
            "sha256": "coverage-low",
            "perceptual_hash": "ffffffffffffffff",
            "path": str(low_path),
            "mime_type": "image/png",
            "size_bytes": low_path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent.example.fbcdn.net/v/coverage.png"
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "same", "image": f"{base}?quality=low"},
        notify=False,
    )
    high_item = {
        "postId": "p1",
        "text": "same",
        "image": f"{base}?quality=high",
        "mediaCount": 1,
    }
    _, _, changed = await ingester.ingest(1, "post", high_item, notify=False)

    ready_urls = [
        row["source_url"]
        for row in db.rows("SELECT source_url FROM media WHERE status='ready'")
    ]
    decision = evaluate_post_media(
        high_item,
        ready_source_urls=ready_urls,
        contract_verified=True,
    )
    assert changed is False
    assert decision.status is CoverageStatus.SOURCE_LIMITED
    assert "尚未成功保存" in str(decision.reason)
    assert db.row("SELECT COUNT(*) count FROM media WHERE status='pending'")["count"] == 1
    assert low_path.exists()


@pytest.mark.asyncio
async def test_different_images_with_colliding_average_hash_are_not_merged(tmp_path: Path, monkeypatch):
    db = Database(tmp_path / "db.sqlite")
    db.execute("INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')")
    store = MediaStore(db, tmp_path, 0, 30)
    left_path = store.root / "left.png"
    right_path = store.root / "right.png"
    Image.new("RGB", (400, 400), "black").save(left_path)
    Image.new("RGB", (800, 800), "white").save(right_path)

    async def fake_download(url):
        left = "left" in url
        path = left_path if left else right_path
        return {
            "status": "ready",
            "sha256": "left" if left else "right",
            # Uniform black and white images collide under the legacy aHash.
            "perceptual_hash": "ffffffffffffffff",
            "path": str(path),
            "mime_type": "image/png",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    await ingester.ingest(
        1,
        "post",
        {
            "postId": "p1",
            "images": [
                {"url": "https://cdn-a.example/left.png"},
                {"url": "https://cdn-b.example/right.png"},
            ],
        },
        notify=True,
    )

    assert db.row("SELECT COUNT(*) count FROM media")["count"] == 2
    assert left_path.exists()
    assert right_path.exists()
    pending = db.rows(
        "SELECT media_sha256,payload_json FROM outbox WHERE kind='media' AND status='pending' ORDER BY id"
    )
    assert [row["media_sha256"] for row in pending] == ["left", "right"]
    assert [json.loads(row["payload_json"])["path"] for row in pending] == [
        str(left_path),
        str(right_path),
    ]


@pytest.mark.asyncio
async def test_equivalent_media_across_versions_never_downgrades_and_rewires_every_reference(
    tmp_path: Path,
    monkeypatch,
):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    files = {
        "low": store.root / "low.png",
        "high": store.root / "high.png",
        "low-again": store.root / "low-again.png",
    }
    Image.new("RGB", (24, 24), "navy").save(files["low"])
    Image.new("RGB", (720, 720), "navy").save(files["high"])
    Image.new("RGB", (48, 48), "navy").save(files["low-again"])

    async def fake_download(url):
        quality = url.rsplit("=", 1)[-1]
        path = files[quality]
        return {
            "status": "ready",
            "sha256": quality,
            "perceptual_hash": "ffffffffffffffff",
            "path": str(path),
            "mime_type": "image/png",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent-nrt1-2.xx.fbcdn.net/v/t39.30808-1/same-object.png"
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v1", "image": f"{base}?quality=low"},
        notify=True,
    )
    entity = db.row("SELECT id FROM entities WHERE external_id='p1'")
    low = db.row("SELECT id FROM media WHERE sha256='low'")
    db.execute(
        """INSERT INTO media_aliases(
        profile_id,entity_id,media_id,canonical_media_id,provider,alias_type,alias_value,
        source_url,width,height,mime_type,sha256,first_seen_at,last_seen_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            1,
            entity["id"],
            low["id"],
            "asset-1",
            "browser",
            "source_url",
            "low-alias",
            f"{base}?quality=low",
            24,
            24,
            "image/png",
            "low",
            "x",
            "x",
        ),
    )

    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v2", "image": f"{base}?quality=high"},
        notify=True,
    )
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v3", "image": f"{base}?quality=low-again"},
        notify=True,
    )

    winner = db.row("SELECT * FROM media")
    assert winner["sha256"] == "high"
    assert winner["path"] == str(files["high"])
    assert db.row("SELECT COUNT(*) count FROM media")["count"] == 1
    assert db.row("SELECT COUNT(DISTINCT media_id) count FROM entity_media")["count"] == 1
    alias = db.row("SELECT media_id,width,height,mime_type,sha256 FROM media_aliases")
    assert alias == {
        "media_id": winner["id"],
        "width": 720,
        "height": 720,
        "mime_type": "image/png",
        "sha256": "high",
    }
    pending = db.rows(
        "SELECT media_sha256,payload_json FROM outbox WHERE kind='media' AND status='pending'"
    )
    assert pending
    assert {row["media_sha256"] for row in pending} == {"high"}
    assert {json.loads(row["payload_json"])["path"] for row in pending} == {
        str(files["high"])
    }
    assert not files["low"].exists()
    assert files["high"].exists()
    assert not files["low-again"].exists()


@pytest.mark.asyncio
async def test_failed_high_resolution_download_keeps_existing_low_file(tmp_path: Path, monkeypatch):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    low_path = store.root / "low.png"
    Image.new("RGB", (40, 40), "navy").save(low_path)

    async def fake_download(url):
        if "quality=high" in url:
            return {"status": "pending", "source_url": url, "error": "download failed"}
        return {
            "status": "ready",
            "sha256": "low",
            "perceptual_hash": "ffffffffffffffff",
            "path": str(low_path),
            "mime_type": "image/png",
            "size_bytes": low_path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent-nrt1-2.xx.fbcdn.net/v/t39.30808-1/same-object.png"
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v1", "image": f"{base}?quality=low"},
        notify=False,
    )
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v2", "image": f"{base}?quality=high"},
        notify=False,
    )

    ready = db.row("SELECT * FROM media WHERE status='ready'")
    assert ready["sha256"] == "low"
    assert ready["path"] == str(low_path)
    assert low_path.exists()


@pytest.mark.asyncio
async def test_same_event_resolution_upgrade_keeps_one_correct_pending_outbox(
    tmp_path: Path,
    monkeypatch,
):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    low_path = store.root / "same-event-low.png"
    high_path = store.root / "same-event-high.png"
    Image.new("RGB", (30, 30), "navy").save(low_path)
    Image.new("RGB", (600, 600), "navy").save(high_path)

    async def fake_download(url):
        high = "quality=high" in url
        path = high_path if high else low_path
        return {
            "status": "ready",
            "sha256": "same-event-high" if high else "same-event-low",
            "perceptual_hash": "ffffffffffffffff",
            "path": str(path),
            "mime_type": "image/png",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent.example.fbcdn.net/v/same-event.png"
    await ingester.ingest(
        1,
        "post",
        {
            "postId": "p1",
            "images": [
                {"url": f"{base}?quality=low"},
                {"url": f"{base}?quality=high"},
            ],
        },
        notify=True,
    )

    assert db.row("SELECT COUNT(*) count FROM media")["count"] == 1
    pending = db.rows(
        "SELECT media_sha256,payload_json FROM outbox WHERE kind='media' AND status='pending'"
    )
    assert len(pending) == 1
    assert pending[0]["media_sha256"] == "same-event-high"
    assert json.loads(pending[0]["payload_json"])["path"] == str(high_path)
    assert not low_path.exists()
    assert high_path.exists()


@pytest.mark.asyncio
async def test_phash_and_rgb_confirmation_merge_same_pixels_from_different_urls(
    tmp_path: Path,
    monkeypatch,
):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    low_path = store.root / "visual-low.png"
    high_path = store.root / "visual-high.png"
    Image.new("RGB", (60, 40), (30, 80, 140)).save(low_path)
    Image.new("RGB", (900, 600), (30, 80, 140)).save(high_path)

    async def fake_download(url):
        high = "high.example" in url
        path = high_path if high else low_path
        return {
            "status": "ready",
            "sha256": "visual-high" if high else "visual-low",
            "perceptual_hash": "ffffffffffffffff",
            "path": str(path),
            "mime_type": "image/png",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    await ingester.ingest(
        1,
        "post",
        {
            "postId": "p1",
            "images": [
                {"url": "https://low.example/photo.png"},
                {"url": "https://high.example/photo.png"},
            ],
        },
        notify=False,
    )

    winner = db.row("SELECT sha256,path FROM media")
    assert winner == {"sha256": "visual-high", "path": str(high_path)}
    assert not low_path.exists()
    assert high_path.exists()


@pytest.mark.asyncio
async def test_equal_quality_representations_keep_oldest_winner_stably(tmp_path: Path, monkeypatch):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    old_path = store.root / "equal-old.png"
    new_path = store.root / "equal-new.png"
    Image.new("RGB", (300, 300), "navy").save(old_path)
    Image.new("RGB", (300, 300), "navy").save(new_path)

    async def fake_download(url):
        new = "version=new" in url
        path = new_path if new else old_path
        return {
            "status": "ready",
            "sha256": "equal-new" if new else "equal-old",
            "perceptual_hash": "ffffffffffffffff",
            "path": str(path),
            "mime_type": "image/png",
            # Deliberately equal so the deterministic media-id tiebreaker is used.
            "size_bytes": 1000,
            "source_url": url,
        }

    monkeypatch.setattr(store, "download", fake_download)
    ingester = Ingester(db, tmp_path, store)
    base = "https://scontent.example.fbcdn.net/v/equal.png"
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v1", "image": f"{base}?version=old"},
        notify=False,
    )
    await ingester.ingest(
        1,
        "post",
        {"postId": "p1", "text": "v2", "image": f"{base}?version=new"},
        notify=False,
    )

    winner = db.row("SELECT sha256,path FROM media")
    assert winner == {"sha256": "equal-old", "path": str(old_path)}
    assert old_path.exists()
    assert not new_path.exists()


def test_media_replacement_is_atomic_when_any_loser_cannot_be_deleted(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    db.execute(
        "INSERT INTO profiles(name,url,created_at,updated_at) VALUES('p','https://facebook.com/100','x','x')"
    )
    store = MediaStore(db, tmp_path, 0, 30)
    ingester = Ingester(db, tmp_path, store)
    entity_id = db.execute(
        """INSERT INTO entities(
        profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','p1','h',1,'x','x')"""
    )
    version_id = db.execute(
        """INSERT INTO versions(
        entity_id,content_hash,normalized_json,raw_path,seen_at,change_type
        ) VALUES(?,'h','{}','raw','x','created')""",
        (entity_id,),
    )
    db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))

    for index, size in enumerate((100, 200, 300), start=1):
        path = store.root / f"image-{index}.png"
        Image.new("RGB", (size, size), "navy").save(path)
        media_id = db.execute(
            """INSERT INTO media(
            sha256,source_url,mime_type,size_bytes,path,perceptual_hash,status,first_seen_at
            ) VALUES(?,?,?,?,?,?,'ready','x')""",
            (
                f"sha-{index}",
                f"https://scontent.example.fbcdn.net/v/same.png?version={index}",
                "image/png",
                path.stat().st_size,
                str(path),
                "ffffffffffffffff",
            ),
        )
        db.execute(
            "INSERT INTO entity_media(entity_id,version_id,media_id,role,position) VALUES(?,?,?,'image',?)",
            (entity_id, version_id, media_id, index),
        )

    db.execute(
        """CREATE TRIGGER reject_second_loser BEFORE DELETE ON media
        WHEN OLD.sha256='sha-2' BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END"""
    )

    with pytest.raises(sqlite3.IntegrityError, match="injected delete failure"):
        ingester._keep_highest_resolution(entity_id, version_id, "image", 3)

    assert db.row("SELECT COUNT(*) count FROM media")["count"] == 3
    assert db.row("SELECT COUNT(*) count FROM entity_media")["count"] == 3
    assert all((store.root / f"image-{index}.png").exists() for index in range(1, 4))
