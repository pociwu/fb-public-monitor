from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fb_monitor.browser_guard import BrowserDecision
from fb_monitor.config import load_settings
from fb_monitor.facebook_browser import FacebookBrowserError
from fb_monitor.service import MonitorService


def make_service(tmp_path: Path, monkeypatch) -> MonitorService:
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: watched
    url: https://www.facebook.com/100
storage:
  data_dir: data
  low_disk_gb: 0
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_ENABLED", "1")
    monkeypatch.setenv("FACEBOOK_BROWSER_DATA_DIR", str(tmp_path / "browser"))
    return MonitorService(load_settings(config))


def completed_photo_result(items: list[dict], *, declared_total: int | None = None):
    urls = [str(item["url"]) for item in items]
    total = len(items) if declared_total is None else declared_total
    checkpoint = {
        "completed": True,
        "terminal_reason": "grid_inventory_processed",
        "declared_total": total,
        "discovered_urls": urls,
        "processed_urls": urls,
        "discovered_count": total,
        "processed_count": total,
        "pending_count": 0,
        "grid_complete": True,
        "grid_empty_confirmed": total == 0,
        "collected_items": items,
        "resume_url": "",
    }
    return {
        "items": items,
        "batch_items": items,
        "progress": checkpoint,
        "completed": True,
        "terminal_reason": "grid_inventory_processed",
        "stalled_reason": "",
    }


def test_queue_public_photo_capture_is_profile_scoped_and_idempotent(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)

    created, capture = service.queue_public_photo_capture(1)
    created_again, same_capture = service.queue_public_photo_capture(1)

    assert created is True
    assert created_again is False
    assert capture["generation"] == 1
    assert capture["status"] == "pending"
    assert same_capture["id"] == capture["id"]
    assert service.db.row(
        "SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1 AND is_active=1"
    )["count"] == 0
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )
    assert job["epoch_id"] is None
    assert json.loads(job["payload_json"])["photo_capture_id"] == capture["id"]


def test_completed_public_photo_capture_starts_a_new_generation(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, first = service.queue_public_photo_capture(1)
    service.db.execute(
        "UPDATE jobs SET status='done' WHERE job_type='capture_profile_photos'"
    )
    service.db.execute(
        "UPDATE profile_photo_captures SET status='complete' WHERE id=?",
        (first["id"],),
    )

    created, second = service.queue_public_photo_capture(1)

    assert created is True
    assert second["id"] != first["id"]
    assert second["generation"] == 2
    assert service.db.row(
        "SELECT COUNT(*) count FROM profile_photo_captures WHERE profile_id=1"
    )["count"] == 2


@pytest.mark.parametrize("terminal_status", ["source_limited", "failed"])
def test_incomplete_terminal_photo_capture_resumes_same_generation(
    tmp_path: Path, monkeypatch, terminal_status: str
):
    service = make_service(tmp_path, monkeypatch)
    _, first = service.queue_public_photo_capture(1)
    checkpoint = {"resume_url": "https://www.facebook.com/photo.php?fbid=10"}
    service.db.execute(
        "UPDATE jobs SET status=? WHERE job_type='capture_profile_photos'",
        (terminal_status,),
    )
    service.db.execute(
        """UPDATE profile_photo_captures SET status=?,checkpoint_json=?
        WHERE id=?""",
        (terminal_status, json.dumps(checkpoint), first["id"]),
    )

    created, resumed = service.queue_public_photo_capture(1)

    assert created is True
    assert resumed["id"] == first["id"]
    assert resumed["generation"] == 1
    assert json.loads(resumed["checkpoint_json"]) == checkpoint
    assert service.db.row(
        "SELECT COUNT(*) count FROM profile_photo_captures WHERE profile_id=1"
    )["count"] == 1


@pytest.mark.asyncio
async def test_public_photo_job_obeys_shared_browser_guard(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.queue_public_photo_capture(1)
    retry_at = datetime.now(UTC) + timedelta(minutes=19)
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(False, "profile_cooldown", retry_at, 1),
    )
    called = False

    async def unexpected(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("BrowserGuard 拒絕時不得啟動 Chromium")

    monkeypatch.setattr(
        service.facebook_anonymous_browser, "public_profile_photos", unexpected
    )

    await service._run_next_job()

    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos' ORDER BY id LIMIT 1"
    )
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["started_at"] is None
    assert datetime.fromisoformat(job["available_at"]) == retry_at
    assert called is False
    assert service.db.row(
        """SELECT COUNT(*) count FROM jobs WHERE profile_id=1
        AND job_type='capture_profile_photos' AND status IN ('pending','running')"""
    )["count"] == 1


@pytest.mark.asyncio
async def test_public_photo_capture_saves_photos_silently_and_completes_with_evidence(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    job = service.db.row(
        "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
    )
    payload = json.loads(job["payload_json"])
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    successes: list[int] = []
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "record_success",
        lambda profile_id: successes.append(profile_id),
    )

    async def public_profile_photos(profile_url, progress, diagnostic_key):
        return {
            "items": [
                {
                    "id": "photo-1",
                    "url": "https://www.facebook.com/photo.php?fbid=10001",
                    "image": "https://scontent.example.fbcdn.net/v/one.jpg?oh=rotating",
                },
                {
                    "id": "photo-2",
                    "url": "https://www.facebook.com/photo.php?fbid=10002",
                    "image": "https://scontent.example.fbcdn.net/v/two.jpg?oh=rotating",
                },
            ],
            "progress": {
                "completed": True,
                "terminal_reason": "declared_last_position",
                "declared_total": 2,
                "discovered_urls": [
                    "https://www.facebook.com/photo.php?fbid=10001",
                    "https://www.facebook.com/photo.php?fbid=10002",
                ],
                "processed_urls": [
                    "https://www.facebook.com/photo.php?fbid=10001",
                    "https://www.facebook.com/photo.php?fbid=10002",
                ],
                "grid_complete": True,
                "resume_url": "",
            },
            "completed": True,
            "terminal_reason": "declared_last_position",
            "stalled_reason": "",
        }

    async def download(url):
        digest = hashlib.sha256(url.encode()).hexdigest()
        path = tmp_path / f"{digest}.jpg"
        path.write_bytes(url.encode())
        return {
            "status": "ready",
            "sha256": digest,
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": 100,
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_anonymous_browser,
        "public_profile_photos",
        public_profile_photos,
    )
    monkeypatch.setattr(service.media, "download", download)
    reconciled: list[tuple[int, str, set[str], int | None, bool]] = []

    def reconcile(profile_id, kind, seen, limit, notify):
        # The generation terminal marker is the replay/idempotency gate and
        # must be durable before missing counters can advance.
        state = service.db.row(
            "SELECT status FROM profile_photo_captures WHERE id=?", (capture["id"],)
        )
        assert state["status"] == "complete"
        reconciled.append((profile_id, kind, seen, limit, notify))

    monkeypatch.setattr(
        service.ingester,
        "reconcile",
        reconcile,
    )

    await service.capture_profile_photos(1, payload)

    refreshed = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    evidence = json.loads(refreshed["terminal_evidence_json"])
    assert refreshed["status"] == "complete"
    assert refreshed["seen_count"] == 2
    assert evidence["auth_scope"] == "anonymous"
    assert evidence["terminal_reason"] == "declared_last_position"
    assert successes == [1]
    assert reconciled == [(1, "photo", {"photo-1", "photo-2"}, None, False)]
    assert service.db.row(
        "SELECT COUNT(*) count FROM entities WHERE profile_id=1 AND kind='photo'"
    )["count"] == 2
    # Historical files are silent; only one completion summary is queued.
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 1
    assert service.db.row(
        "SELECT COUNT(*) count FROM outbox WHERE kind='media'"
    )["count"] == 0


@pytest.mark.asyncio
async def test_public_photo_capture_continues_from_checkpoint_without_duplicate_job(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    monkeypatch.setattr(
        service.anonymous_browser_guard, "record_success", lambda profile_id: None
    )

    async def public_profile_photos(profile_url, progress, diagnostic_key):
        return {
            "items": [],
            "progress": {
                "completed": False,
                "resume_url": "https://www.facebook.com/photo.php?fbid=10020",
                "collected_items": [],
            },
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
        }

    monkeypatch.setattr(
        service.facebook_anonymous_browser,
        "public_profile_photos",
        public_profile_photos,
    )

    await service.capture_profile_photos(1, payload)

    refreshed = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    assert refreshed["status"] == "in_progress"
    successor = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' AND dedupe_key LIKE '%:1'"""
    )
    assert successor is not None
    assert json.loads(successor["payload_json"])["iteration"] == 1


@pytest.mark.asyncio
async def test_completed_inventory_refreshes_only_pending_media_on_retry(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    monkeypatch.setattr(
        service.anonymous_browser_guard, "record_success", lambda profile_id: None
    )
    browser_calls = 0
    browser_progress: list[dict] = []

    async def public_profile_photos(profile_url, progress, diagnostic_key):
        nonlocal browser_calls
        browser_calls += 1
        browser_progress.append(dict(progress))
        item = {
            "id": "10001",
            "url": "https://www.facebook.com/photo.php?fbid=10001",
            "image": "https://scontent.example.fbcdn.net/v/retry.jpg?oh=rotating",
        }
        checkpoint = {
            "schema_version": 3,
            "completed": True,
            "grid_complete": True,
            "grid_empty_confirmed": False,
            "discovered_urls": [item["url"]],
            "processed_urls": [item["url"]],
            "discovered_count": 1,
            "processed_count": 1,
            "pending_count": 0,
            "collected_items": [item],
            "terminal_reason": "grid_inventory_processed",
            "resume_url": "",
        }
        return {
            "items": [item],
            "batch_items": [item],
            "progress": checkpoint,
            "completed": True,
            "terminal_reason": "grid_inventory_processed",
            "stalled_reason": "",
        }

    download_calls = 0

    async def download(url):
        nonlocal download_calls
        download_calls += 1
        if download_calls == 1:
            return {
                "status": "pending",
                "source_url": url,
                "error": "temporary CDN failure",
                "retry_until": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }
        path = tmp_path / "retried.jpg"
        path.write_bytes(b"retried-photo")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(url.encode()).hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": 100,
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_anonymous_browser,
        "public_profile_photos",
        public_profile_photos,
    )
    monkeypatch.setattr(service.media, "download", download)

    await service.capture_profile_photos(1, payload)

    pending = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    checkpoint = json.loads(pending["checkpoint_json"])
    assert pending["status"] == "in_progress"
    assert checkpoint["pending_media_external_ids"] == ["10001"]
    successor = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' AND dedupe_key LIKE '%:1'"""
    )

    await service.capture_profile_photos(
        1, json.loads(successor["payload_json"])
    )

    complete = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    assert complete["status"] == "complete"
    assert browser_calls == 2
    assert "refresh_media_external_ids" not in browser_progress[0]
    assert browser_progress[1]["refresh_media_external_ids"] == ["10001"]
    assert download_calls == 2
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 1


@pytest.mark.asyncio
async def test_ready_media_row_with_missing_file_cannot_complete_capture(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    item = {
        "id": "missing-file",
        "url": "https://www.facebook.com/photo.php?fbid=missing-file",
        "image": "https://scontent.example.fbcdn.net/missing.jpg",
    }

    async def public_profile_photos(*args, **kwargs):
        return completed_photo_result([item])

    async def download(url):
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"not-on-disk").hexdigest(),
            "path": str(tmp_path / "does-not-exist.jpg"),
            "mime_type": "image/jpeg",
            "size_bytes": 11,
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_anonymous_browser,
        "public_profile_photos",
        public_profile_photos,
    )
    monkeypatch.setattr(service.media, "download", download)

    await service.capture_profile_photos(1, payload)

    pending = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    checkpoint = json.loads(pending["checkpoint_json"])
    assert pending["status"] == "in_progress"
    assert checkpoint["pending_media_external_ids"] == ["missing-file"]
    assert service.db.row("SELECT status FROM media")["status"] == "pending"


@pytest.mark.asyncio
async def test_unresolved_permalink_stops_without_empty_media_retry_loop(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    item = {
        "id": "10001",
        "url": "https://www.facebook.com/photo.php?fbid=10001",
        "image": "https://scontent.example.fbcdn.net/one.jpg",
    }

    async def public_profile_photos(*args, **kwargs):
        result = completed_photo_result([item], declared_total=2)
        result["progress"]["discovered_urls"].append(
            "https://www.facebook.com/photo.php?fbid=unresolved"
        )
        result["progress"]["processed_urls"].append(
            "https://www.facebook.com/photo.php?fbid=unresolved"
        )
        return result

    async def download(url):
        path = tmp_path / "one.jpg"
        path.write_bytes(b"one")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"one").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": 3,
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_anonymous_browser,
        "public_profile_photos",
        public_profile_photos,
    )
    monkeypatch.setattr(service.media, "download", download)

    await service._run_next_job()

    stopped = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )
    assert stopped["status"] == "source_limited"
    assert "尚未解析" in stopped["limited_reason"]
    assert job["status"] == "source_limited"
    assert service.db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )["count"] == 0


@pytest.mark.asyncio
async def test_later_generation_notifies_only_new_unique_photo_bytes(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    monkeypatch.setattr(
        service.anonymous_browser_guard, "record_success", lambda profile_id: None
    )
    current_items: list[dict] = []

    async def public_profile_photos(*args, **kwargs):
        return completed_photo_result(current_items)

    payload_bytes = {
        "https://scontent.example.fbcdn.net/base.jpg": b"same-bytes",
        "https://scontent.example.fbcdn.net/duplicate-name.jpg": b"same-bytes",
        "https://scontent.example.fbcdn.net/new.jpg": b"new-bytes",
    }

    async def download(url):
        body = payload_bytes[url]
        digest = hashlib.sha256(body).hexdigest()
        path = tmp_path / f"{digest}.jpg"
        path.write_bytes(body)
        return {
            "status": "ready",
            "sha256": digest,
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": len(body),
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_anonymous_browser,
        "public_profile_photos",
        public_profile_photos,
    )
    monkeypatch.setattr(service.media, "download", download)
    notified: list[int] = []
    monkeypatch.setattr(
        service.ingester,
        "notify_persisted",
        lambda entity_id: notified.append(entity_id) or True,
    )
    monkeypatch.setattr(service.ingester, "reconcile", lambda *args, **kwargs: None)

    current_items = [
        {
            "id": "base",
            "url": "https://www.facebook.com/photo.php?fbid=base",
            "image": "https://scontent.example.fbcdn.net/base.jpg",
        }
    ]
    _, first = service.queue_public_photo_capture(1)
    first_payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    await service.capture_profile_photos(1, first_payload)
    service.db.execute(
        "UPDATE jobs SET status='done' WHERE job_type='capture_profile_photos'"
    )
    assert service.db.row(
        "SELECT status FROM profile_photo_captures WHERE id=?", (first["id"],)
    )["status"] == "complete"
    assert notified == []

    current_items = [
        {
            "id": "same-file-new-id",
            "url": "https://www.facebook.com/photo.php?fbid=same-file-new-id",
            "image": "https://scontent.example.fbcdn.net/duplicate-name.jpg",
        },
        {
            "id": "unique",
            "url": "https://www.facebook.com/photo.php?fbid=unique",
            "image": "https://scontent.example.fbcdn.net/new.jpg",
        },
    ]
    _, second = service.queue_public_photo_capture(1)
    second_payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs
            WHERE job_type='capture_profile_photos' AND status='pending'
            ORDER BY id DESC LIMIT 1"""
        )["payload_json"]
    )
    await service.capture_profile_photos(1, second_payload)

    completed = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (second["id"],)
    )
    assert completed["status"] == "complete"
    assert completed["new_count"] == 1
    assert completed["duplicate_count"] == 1
    assert len(notified) == 1
    notified_entity = service.db.row(
        "SELECT external_id FROM entities WHERE id=?", (notified[0],)
    )
    assert notified_entity["external_id"] == "unique"


@pytest.mark.asyncio
async def test_photo_browser_failure_stays_in_photo_ledger_not_profile_health(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    monkeypatch.setattr(
        service.anonymous_browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def failed(*args, **kwargs):
        raise FacebookBrowserError("public photo DOM changed")

    monkeypatch.setattr(
        service.facebook_anonymous_browser, "public_profile_photos", failed
    )

    await service._run_next_job()

    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )
    failed_capture = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    assert job["status"] == "failed"
    assert failed_capture["status"] == "failed"
    assert "DOM changed" in failed_capture["limited_reason"]
    assert profile["consecutive_failures"] == 0


def test_daily_entity_hash_dedupe_does_not_merge_photo_inventory(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    now = datetime.now(UTC).isoformat()
    with service.db.connect() as conn:
        for external_id in ("photo-a", "photo-b"):
            conn.execute(
                """INSERT INTO entities(
                profile_id,kind,external_id,current_hash,present,
                first_seen_at,last_seen_at
                ) VALUES(1,'photo',?,'same-projection',1,?,?)""",
                (external_id, now, now),
            )

    counts = service._dedupe_database()

    assert counts["entities_merged"] == 0
    assert service.db.row(
        "SELECT COUNT(*) count FROM entities WHERE profile_id=1 AND kind='photo'"
    )["count"] == 2
