from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fb_monitor.apify import (
    ActorResult,
    ActorRunTerminalError,
    MonthlyUsage,
    StartedActor,
)
from fb_monitor.browser_guard import BrowserDecision
from fb_monitor.config import load_settings
from fb_monitor.facebook_browser import FacebookBrowserError, FacebookBrowserLoginRequired
from fb_monitor.service import DurableActorRunDeferred, MonitorService, PRICES


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


@pytest.mark.asyncio
async def test_local_actor_drain_bypasses_global_spacing_for_same_photo_capture(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.spacing_min_minutes = 30
    service.settings.spacing_max_minutes = 30
    service.db.execute("DELETE FROM jobs")
    now = datetime.now(UTC)
    capture_id = service.db.execute(
        """INSERT INTO profile_photo_captures(
          profile_id,generation,status,created_at,updated_at
        ) VALUES(1,1,'in_progress',?,?)""",
        (now.isoformat(), now.isoformat()),
    )
    batch, _ = service.db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id=service.settings.actors.profile_photos,
        normalized_input={"urls": ["https://www.facebook.com/100"]},
        max_charge_usd=0.1,
    )
    service.db.execute(
        "UPDATE paid_photo_batches SET status='imported' WHERE id=?",
        (batch["id"],),
    )
    previous_id = service._enqueue(
        1,
        "capture_profile_photos",
        -120,
        now,
        {"photo_capture_id": capture_id, "iteration": 0, "source_policy": "auto"},
    )
    service.db.execute(
        """UPDATE jobs SET status='done',started_at=?,finished_at=? WHERE id=?""",
        (now.isoformat(), now.isoformat(), previous_id),
    )
    current_id = service._enqueue(
        1,
        "capture_profile_photos",
        -120,
        now,
        {
            "photo_capture_id": capture_id,
            "iteration": 1,
            "source_policy": "auto",
            "local_actor_drain": True,
            "local_actor_batch_id": batch["id"],
        },
    )
    calls: list[tuple[int, dict]] = []

    async def capture(profile_id: int, payload: dict):
        calls.append((profile_id, payload))

    monkeypatch.setattr(service, "capture_profile_photos", capture)

    await service._run_next_job()

    assert calls == [
        (
            1,
            {
                "photo_capture_id": capture_id,
                "iteration": 1,
                "source_policy": "auto",
                "local_actor_drain": True,
                "local_actor_batch_id": batch["id"],
            },
        )
    ]
    current = service.db.row("SELECT * FROM jobs WHERE id=?", (current_id,))
    assert current["status"] == "done"
    assert current["attempts"] == 1


@pytest.mark.asyncio
async def test_forged_local_actor_drain_payload_cannot_bypass_global_spacing(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.spacing_min_minutes = 30
    service.settings.spacing_max_minutes = 30
    service.db.execute("DELETE FROM jobs")
    now = datetime.now(UTC)
    capture_id = service.db.execute(
        """INSERT INTO profile_photo_captures(
          profile_id,generation,status,created_at,updated_at
        ) VALUES(1,1,'in_progress',?,?)""",
        (now.isoformat(), now.isoformat()),
    )
    previous_id = service._enqueue(
        1,
        "capture_profile_photos",
        -120,
        now,
        {"photo_capture_id": capture_id, "iteration": 0, "source_policy": "auto"},
    )
    service.db.execute(
        """UPDATE jobs SET status='done',started_at=?,finished_at=? WHERE id=?""",
        (now.isoformat(), now.isoformat(), previous_id),
    )
    current_id = service._enqueue(
        1,
        "capture_profile_photos",
        -120,
        now,
        {
            "photo_capture_id": capture_id,
            "iteration": 1,
            "source_policy": "auto",
            "local_actor_drain": True,
            "local_actor_batch_id": 999999,
        },
    )
    calls: list[tuple[int, dict]] = []

    async def capture(profile_id: int, payload: dict):
        calls.append((profile_id, payload))

    monkeypatch.setattr(service, "capture_profile_photos", capture)

    await service._run_next_job()

    assert calls == []
    current = service.db.row("SELECT * FROM jobs WHERE id=?", (current_id,))
    assert current["status"] == "pending"
    assert current["attempts"] == 0
    assert datetime.fromisoformat(current["available_at"]) >= now + timedelta(minutes=30)


@pytest.mark.asyncio
async def test_photo_actor_provider_cursor_successor_is_not_marked_as_local_drain(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1, source_policy="apify")
    first_job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )
    payload = json.loads(first_job["payload_json"])

    async def provider_page(*args, **kwargs):
        return {
            "items": [],
            "batch_items": [],
            "progress": {
                "source_policy": "apify",
                "source": "apify_actor",
                "access_scope": "actor_visible",
                "actor_next_cursor": "page-2",
                "actor_provider_resumable": True,
                "completed": False,
                "grid_complete": False,
            },
            "completed": False,
            "resumable": True,
            "terminal_reason": "",
            "stalled_reason": "",
            "actor_batch_id": 0,
            "actor_batch_ready_to_commit": True,
            "actor_provider_resumable": True,
        }

    monkeypatch.setattr(service, "_capture_profile_photos_actor", provider_page)

    await service.capture_profile_photos(1, payload)

    successor = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' AND id<>? ORDER BY id DESC LIMIT 1""",
        (first_job["id"],),
    )
    assert successor is not None
    successor_payload = json.loads(successor["payload_json"])
    assert successor_payload["photo_capture_id"] == capture["id"]
    assert successor_payload["source_policy"] == "apify"
    assert successor_payload["local_actor_drain"] is False
    assert successor_payload["local_actor_batch_id"] is None
    now = datetime.now(UTC)
    service.settings.spacing_min_minutes = 30
    service.settings.spacing_max_minutes = 30
    service.db.execute(
        """UPDATE jobs SET status='done',started_at=?,finished_at=? WHERE id=?""",
        (now.isoformat(), now.isoformat(), first_job["id"]),
    )
    service.db.execute(
        "UPDATE jobs SET available_at='2000-01-01T00:00:00+00:00' WHERE id=?",
        (successor["id"],),
    )
    calls: list[dict] = []

    async def capture_must_wait(profile_id: int, next_payload: dict):
        calls.append(next_payload)

    monkeypatch.setattr(service, "capture_profile_photos", capture_must_wait)

    await service._run_next_job()

    delayed = service.db.row("SELECT * FROM jobs WHERE id=?", (successor["id"],))
    assert calls == []
    assert delayed["status"] == "pending"
    assert delayed["attempts"] == 0
    assert datetime.fromisoformat(delayed["available_at"]) >= now + timedelta(minutes=30)


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_batch_status", ["failed", "needs_reconcile"])
async def test_terminal_or_reconcile_actor_batch_does_not_block_auto_browser_source(
    tmp_path: Path, monkeypatch, actor_batch_status: str
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1, source_policy="auto")
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    batch, _ = service.db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=int(capture["id"]),
        actor_id=service.settings.actors.profile_photos,
        normalized_input={"urls": ["https://www.facebook.com/100"]},
        max_charge_usd=0.1,
    )
    if actor_batch_status == "failed":
        service.db.transition_paid_photo_batch(batch["id"], "failed")
    else:
        service.db.transition_paid_photo_batch(batch["id"], "launching")
        service.db.transition_paid_photo_batch(batch["id"], "needs_reconcile")

    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    browser_calls: list[str] = []

    async def browser(profile_url, progress, diagnostic_key):
        browser_calls.append(profile_url)
        return completed_photo_result([])

    async def actor_must_not_resume(*args, **kwargs):
        raise AssertionError("terminal/reconcile Actor ledger must not block browser")

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", browser)
    monkeypatch.setattr(service, "_capture_profile_photos_actor", actor_must_not_resume)

    assert await service.capture_profile_photos(1, payload) is None

    assert browser_calls == ["https://www.facebook.com/100"]
    assert service.db.row(
        "SELECT status FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )["status"] == "complete"
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )["status"] == actor_batch_status


@pytest.mark.asyncio
async def test_account_login_failure_uses_crash_safe_photo_actor_fallback(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", login_required
    )
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(0.0, "2026-09-01T00:00:00+00:00", "2026-10-01T00:00:00+00:00")
        ),
    )
    starts: list[tuple[str, dict, float]] = []

    async def start(actor_id, actor_payload, max_charge):
        starts.append((actor_id, actor_payload, max_charge))
        return StartedActor("run-photo-1", "dataset-1", "store-1")

    image_url = "https://scontent-tpe1-1.xx.fbcdn.net/v/photo-1.jpg?oh=one"

    async def finish(started):
        assert started.run_id == "run-photo-1"
        return ActorResult(
            items=[{"photos": [image_url], "totalPhotos": 1}],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=1,
        )

    async def download(url):
        path = tmp_path / "actor-photo.jpg"
        path.write_bytes(b"actor-photo")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"actor-photo").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    await service.capture_profile_photos(1, payload)

    refreshed = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    checkpoint = json.loads(refreshed["checkpoint_json"])
    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?", (capture["id"],)
    )
    assert refreshed["status"] == "source_limited"
    assert checkpoint["source"] == "apify_actor"
    assert checkpoint["access_scope"] == "actor_visible"
    assert checkpoint["actor_inventory_completed"] is True
    assert "登入帳號可見清冊" in refreshed["limited_reason"]
    assert batch["status"] == "committed"
    assert batch["run_id"] == "run-photo-1"
    assert len(starts) == 1
    entity = service.db.row(
        """SELECT * FROM entities
        WHERE profile_id=1 AND kind='photo'"""
    )
    assert entity["source_scope"] == "actor_visible"
    assert entity["source_collector"] == "apify_profile_photo_actor"
    media = service.db.row(
        """SELECT m.* FROM media m
        JOIN entity_media em ON em.media_id=m.id
        WHERE em.entity_id=? AND m.status='ready'""",
        (entity["id"],),
    )
    assert Path(media["path"]).read_bytes() == b"actor-photo"
    assert media["sha256"] == hashlib.sha256(b"actor-photo").hexdigest()
    assert service.db.row("SELECT COUNT(*) count FROM outbox")["count"] == 0


@pytest.mark.asyncio
async def test_photo_actor_input_with_login_cookie_is_rejected_before_paid_batch(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    service.settings.actors.profile_photos_input = {
        "urls": "{urls}",
        "headers": {"Cookie": "c_user=123; xs=secret"},
    }
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", login_required
    )

    outcome = await service.capture_profile_photos(1, payload)

    assert outcome == "source_limited"
    assert "禁止包含登入認證欄位" in service.db.row(
        "SELECT limited_reason FROM profile_photo_captures WHERE id=?",
        (capture["id"],),
    )["limited_reason"]
    assert service.db.row("SELECT COUNT(*) count FROM paid_photo_batches")["count"] == 0
    assert service.db.row("SELECT COUNT(*) count FROM actor_runs")["count"] == 0


@pytest.mark.asyncio
async def test_unfinished_paid_photo_run_resumes_before_browser(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    batch, _ = service.db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=int(capture["id"]),
        actor_id=service.settings.actors.profile_photos,
        normalized_input={"urls": ["https://www.facebook.com/100"]},
        max_charge_usd=0.1,
    )
    service.db.transition_paid_photo_batch(batch["id"], "launching")
    service.db.transition_paid_photo_batch(
        batch["id"],
        "run_started",
        expected_status="launching",
        run_id="existing-run",
        dataset_id="existing-dataset",
        key_value_store_id="existing-store",
    )
    service.settings.photo_actor_fallback_enabled = False
    service.settings.actors.profile_photos = "changed/actor"
    service.db.set_profile_source_control(
        1, "apify", frozen=True, reason="operator freeze"
    )

    async def browser_must_not_run(*args, **kwargs):
        raise AssertionError("browser must not run before paid batch recovery")

    async def start_must_not_run(*args, **kwargs):
        raise AssertionError("existing paid run must not be purchased again")

    finished: list[str] = []

    async def finish(started):
        finished.append(started.run_id)
        return ActorResult(
            items=[{"photos": ["https://scontent.example.fbcdn.net/v/recovered.jpg"]}],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=1,
        )

    async def download(url):
        path = tmp_path / "recovered.jpg"
        path.write_bytes(b"recovered")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"recovered").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", browser_must_not_run
    )
    monkeypatch.setattr(service.apify, "start", start_must_not_run)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    outcome = await service.capture_profile_photos(1, payload)

    assert outcome == "source_limited"
    assert finished == ["existing-run"]
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_manual_retry_after_settled_actor_batch_starts_fresh_first_page(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    service.settings.actors.profile_photos_input = {
        "urls": "{urls}",
        "cursor": "{cursor}",
    }
    _, capture = service.queue_public_photo_capture(1)
    browser_item = {
        "id": "browser-kept",
        "url": "https://www.facebook.com/photo.php?fbid=70001&id=100",
        "image": "https://scontent.example.fbcdn.net/v/browser-kept.jpg",
    }
    checkpoint = {
        "access_scope": "mixed",
        "collected_items": [browser_item],
        "discovered_urls": [browser_item["url"]],
        "processed_urls": [browser_item["url"]],
        "discovered_count": 1,
        "processed_count": 1,
        "grid_complete": False,
        "actor_retry_nonce": 3,
        "actor_next_cursor": "stale-page-2",
        "actor_seen_cursors": ["stale-page-1"],
        "actor_collected_items": [{"id": "stale-actor-item"}],
        "actor_processed_item_ids": ["stale-actor-item"],
    }
    service.db.execute(
        "UPDATE jobs SET status='source_limited' WHERE job_type='capture_profile_photos'"
    )
    service.db.execute(
        """UPDATE profile_photo_captures SET status='source_limited',checkpoint_json=?
        WHERE id=?""",
        (json.dumps(checkpoint), capture["id"]),
    )
    old_batch, _ = service.db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=int(capture["id"]),
        actor_id=service.settings.actors.profile_photos,
        normalized_input={"urls": ["https://www.facebook.com/100"]},
        max_charge_usd=0.1,
        request_hash="settled-old-photo-run",
    )
    service.db.execute(
        """UPDATE paid_photo_batches SET status='committed',run_id='old-run'
        WHERE id=?""",
        (old_batch["id"],),
    )

    created, resumed = service.queue_public_photo_capture(1)

    assert created is True
    assert resumed["id"] == capture["id"]
    retry_checkpoint = json.loads(resumed["checkpoint_json"])
    assert retry_checkpoint["actor_retry_nonce"] == 4
    assert retry_checkpoint["collected_items"] == [browser_item]
    assert retry_checkpoint["discovered_urls"] == [browser_item["url"]]
    assert "actor_next_cursor" not in retry_checkpoint
    assert "actor_processed_item_ids" not in retry_checkpoint
    payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs
            WHERE job_type='capture_profile_photos' AND status='pending'"""
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", login_required
    )
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[dict] = []

    async def start(actor_id, actor_payload, max_charge):
        starts.append(actor_payload)
        return StartedActor("fresh-run", "fresh-dataset", "fresh-store")

    async def finish(started):
        return ActorResult(
            items=[
                {
                    "photos": [
                        "https://scontent.example.fbcdn.net/v/fresh-photo.jpg"
                    ]
                }
            ],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=1,
        )

    async def download(url):
        path = tmp_path / "fresh-photo.jpg"
        path.write_bytes(b"fresh-photo")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"fresh-photo").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    outcome = await service.capture_profile_photos(1, payload)

    assert outcome == "source_limited"
    assert len(starts) == 1
    assert starts[0]["cursor"] == ""
    batches = service.db.rows(
        "SELECT status,run_id FROM paid_photo_batches WHERE photo_capture_id=? ORDER BY id",
        (capture["id"],),
    )
    assert [(row["status"], row["run_id"]) for row in batches] == [
        ("committed", "old-run"),
        ("committed", "fresh-run"),
    ]
    final_checkpoint = json.loads(
        service.db.row(
            "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
            (capture["id"],),
        )["checkpoint_json"]
    )
    assert final_checkpoint["collected_items"] == [browser_item]


@pytest.mark.asyncio
async def test_actor_collision_cannot_replace_browser_photo_version_or_provenance(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    browser_image = "https://scontent.example.fbcdn.net/v/browser-90001.jpg"
    actor_image = "https://scontent.example.fbcdn.net/v/actor-90001.jpg"
    browser_item = {
        "id": "90001",
        "url": "https://www.facebook.com/photo.php?fbid=90001&id=100",
        "image": browser_image,
        "caption": "browser caption",
    }

    async def stalled_browser(*args, **kwargs):
        progress = {
            "access_scope": "account_visible",
            "viewer_scope_hash": "viewer-browser",
            "collected_items": [browser_item],
            "discovered_urls": [browser_item["url"]],
            "processed_urls": [browser_item["url"]],
            "discovered_count": 1,
            "processed_count": 1,
            "grid_complete": False,
            "completed": False,
        }
        return {
            "items": [browser_item],
            "batch_items": [browser_item],
            "progress": progress,
            "completed": False,
            "resumable": False,
            "stalled_reason": "browser surface stopped",
        }

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", stalled_browser
    )
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    monkeypatch.setattr(
        service.apify,
        "start",
        lambda *args, **kwargs: _async_value(
            StartedActor("collision-run", "collision-dataset", "collision-store")
        ),
    )

    async def finish(started):
        return ActorResult(
            items=[
                {
                    "photoId": "90001",
                    "permalinkUrl": browser_item["url"],
                    "imageUrl": actor_image,
                    "caption": "actor caption",
                }
            ],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=1,
        )

    downloaded: list[str] = []

    async def download(url):
        downloaded.append(url)
        path = tmp_path / f"download-{len(downloaded)}.jpg"
        body = url.encode()
        path.write_bytes(body)
        return {
            "status": "ready",
            "sha256": hashlib.sha256(body).hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    outcome = await service.capture_profile_photos(1, payload)

    assert outcome == "source_limited"
    entity = service.db.row(
        """SELECT * FROM entities
        WHERE profile_id=1 AND kind='photo' AND external_id='90001'"""
    )
    assert entity["source_scope"] == "account_visible"
    assert entity["source_collector"] == "authenticated_facebook_photo_viewer"
    assert entity["source_viewer_scope_hash"] == "viewer-browser"
    assert entity["source_url"] == browser_item["url"]
    assert service.db.row(
        "SELECT COUNT(*) count FROM versions WHERE entity_id=?", (entity["id"],)
    )["count"] == 1
    version = service.db.row(
        "SELECT raw_path FROM versions WHERE id=?", (entity["current_version_id"],)
    )
    raw = json.loads(Path(version["raw_path"]).read_text(encoding="utf-8"))
    assert raw["caption"] == "browser caption"
    assert raw["image"]["url"] == browser_image
    assert downloaded == [browser_image]


@pytest.mark.asyncio
async def test_large_actor_raw_is_imported_twenty_at_a_time_without_repurchase(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    first_payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", login_required
    )
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[str] = []
    finishes: list[str] = []

    async def start(actor_id, actor_payload, max_charge):
        starts.append(actor_id)
        return StartedActor("large-run", "large-dataset", "large-store")

    actor_photos = [
        {
            "photoId": str(80000 + index),
            "permalinkUrl": (
                f"https://www.facebook.com/photo.php?fbid={80000 + index}&id=100"
            ),
            "imageUrl": (
                f"https://scontent.example.fbcdn.net/v/large-{index}.jpg"
            ),
        }
        for index in range(55)
    ]

    async def finish(started):
        finishes.append(started.run_id)
        return ActorResult(
            items=actor_photos,
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.05,
            raw_result_count=55,
        )

    downloads: list[str] = []

    async def download(url):
        downloads.append(url)
        path = tmp_path / f"large-{len(downloads)}.jpg"
        body = url.encode()
        path.write_bytes(body)
        return {
            "status": "ready",
            "sha256": hashlib.sha256(body).hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    assert await service.capture_profile_photos(1, first_payload) is None
    first_checkpoint = json.loads(
        service.db.row(
            "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
            (capture["id"],),
        )["checkpoint_json"]
    )
    assert len(first_checkpoint["actor_processed_item_ids"]) == 20
    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?", (capture["id"],)
    )
    assert batch["status"] == "imported"

    second_payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs
            WHERE dedupe_key=? AND status='pending'""",
            (f"capture-account-photos:{capture['id']}:1",),
        )["payload_json"]
    )
    assert await service.capture_profile_photos(1, second_payload) is None
    second_checkpoint = json.loads(
        service.db.row(
            "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
            (capture["id"],),
        )["checkpoint_json"]
    )
    assert len(second_checkpoint["actor_processed_item_ids"]) == 40

    third_payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs
            WHERE dedupe_key=? AND status='pending'""",
            (f"capture-account-photos:{capture['id']}:2",),
        )["payload_json"]
    )
    assert await service.capture_profile_photos(1, third_payload) == "source_limited"

    assert len(starts) == 1
    assert finishes == ["large-run"]
    assert len(downloads) == 55
    assert service.db.row(
        "SELECT COUNT(*) count FROM entities WHERE profile_id=1 AND kind='photo'"
    )["count"] == 55
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )["status"] == "committed"


async def _async_value(value):
    return value


def install_successful_photo_actor(
    service: MonitorService,
    tmp_path: Path,
    monkeypatch,
    *,
    photo_count: int = 1,
) -> list[tuple[str, dict, float]]:
    """Install one real paid-Actor-shaped response for source-policy tests."""

    service.apify.token = "test-token"
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[tuple[str, dict, float]] = []

    async def start(actor_id, actor_payload, max_charge):
        starts.append((actor_id, actor_payload, max_charge))
        return StartedActor("policy-run", "policy-dataset", "policy-store")

    actor_items = [
        {
            "photoId": str(97000 + index),
            "permalinkUrl": (
                f"https://www.facebook.com/photo.php?fbid={97000 + index}&id=100"
            ),
            "imageUrl": (
                f"https://scontent.example.fbcdn.net/v/policy-photo-{index}.jpg"
            ),
        }
        for index in range(photo_count)
    ]

    async def finish(started):
        return ActorResult(
            items=actor_items,
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=photo_count,
        )

    async def download(url):
        body = url.encode()
        path = tmp_path / f"policy-{hashlib.sha256(body).hexdigest()[:12]}.jpg"
        path.write_bytes(body)
        return {
            "status": "ready",
            "sha256": hashlib.sha256(body).hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": len(body),
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)
    return starts


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
    assert json.loads(resumed["checkpoint_json"]) == {
        **checkpoint,
        "source_policy": "auto",
    }
    assert service.db.row(
        "SELECT COUNT(*) count FROM profile_photo_captures WHERE profile_id=1"
    )["count"] == 1


@pytest.mark.asyncio
async def test_apify_photo_source_policy_bypasses_browser_guard_and_chromium(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    starts = install_successful_photo_actor(service, tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1, source_policy="apify")
    job = service.db.row(
        "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
    )
    payload = json.loads(job["payload_json"])

    def guard_must_not_run(profile_id):
        raise AssertionError("Apify-only policy must bypass BrowserGuard")

    async def browser_must_not_run(*args, **kwargs):
        raise AssertionError("Apify-only policy must bypass Chromium")

    monkeypatch.setattr(service.browser_guard, "acquire", guard_must_not_run)
    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", browser_must_not_run
    )

    outcome = await service.capture_profile_photos(1, payload)

    assert payload["source_policy"] == "apify"
    assert outcome == "source_limited"
    assert len(starts) == 1
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_final_freeze_after_photo_claim_returns_batch_to_prepared_without_start(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1, source_policy="apify")
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    original_claim = service.db.claim_paid_photo_batch_launch

    def claim_then_freeze(*args, **kwargs):
        result = original_claim(*args, **kwargs)
        service.db.execute("UPDATE profiles SET apify_frozen=1 WHERE id=1")
        return result

    starts = 0

    async def start(*args, **kwargs):
        nonlocal starts
        starts += 1
        raise AssertionError("final freeze must win before Actor start")

    monkeypatch.setattr(service.db, "claim_paid_photo_batch_launch", claim_then_freeze)
    monkeypatch.setattr(service.apify, "start", start)

    assert await service.capture_profile_photos(1, payload) == "source_limited"

    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?", (capture["id"],)
    )
    assert batch["status"] == "prepared"
    assert batch["launched_at"] is None
    assert batch["actor_run_id"] is None
    assert batch["run_id"] is None
    assert starts == 0
    assert service.db.apify_settled_charge_floor(
        "2000-01-01T00:00:00+00:00", posts_result_price_usd=PRICES["posts"]
    ) == 0


@pytest.mark.asyncio
async def test_auto_photo_daily_limit_runs_actor_then_schedules_account_retry(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.photo_actor_fallback_on_browser_guard_long_deferral = True
    starts = install_successful_photo_actor(service, tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1, source_policy="auto")
    retry_at = datetime.now(UTC) + timedelta(hours=8)
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(False, "daily_limit", retry_at, 8),
    )

    async def browser_must_not_run(*args, **kwargs):
        raise AssertionError("BrowserGuard denial must not start Chromium")

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", browser_must_not_run
    )

    await service._run_next_job()

    assert len(starts) == 1
    refreshed = service.db.row(
        "SELECT status,next_job_at FROM profile_photo_captures WHERE id=?",
        (capture["id"],),
    )
    assert refreshed["status"] == "in_progress"
    successor = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' ORDER BY id DESC LIMIT 1"""
    )
    assert successor is not None
    successor_payload = json.loads(successor["payload_json"])
    assert successor_payload["photo_capture_id"] == capture["id"]
    assert successor_payload["source_policy"] == "account"
    assert datetime.fromisoformat(successor["available_at"]) == retry_at
    assert datetime.fromisoformat(refreshed["next_job_at"]) == retry_at
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_auto_photo_daily_limit_drains_paid_raw_before_account_retry(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.photo_actor_fallback_on_browser_guard_long_deferral = True
    starts = install_successful_photo_actor(
        service, tmp_path, monkeypatch, photo_count=21
    )
    _, capture = service.queue_public_photo_capture(1, source_policy="auto")
    retry_at = datetime.now(UTC) + timedelta(hours=8)
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(False, "daily_limit", retry_at, 8),
    )

    await service._run_next_job()

    actor_continuation = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' ORDER BY id DESC LIMIT 1"""
    )
    actor_payload = json.loads(actor_continuation["payload_json"])
    assert actor_payload["source_policy"] == "auto"
    assert datetime.fromisoformat(actor_continuation["available_at"]) < retry_at

    await service.capture_profile_photos(1, actor_payload)

    account_continuation = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' ORDER BY id DESC LIMIT 1"""
    )
    account_payload = json.loads(account_continuation["payload_json"])
    assert account_payload["source_policy"] == "account"
    assert datetime.fromisoformat(account_continuation["available_at"]) == retry_at
    assert len(starts) == 1
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_apify_only_photo_raw_slices_use_verified_local_drain_and_skip_spacing(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.spacing_min_minutes = 30
    service.settings.spacing_max_minutes = 30
    starts = install_successful_photo_actor(
        service, tmp_path, monkeypatch, photo_count=21
    )
    _, capture = service.queue_public_photo_capture(1, source_policy="apify")
    first_job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )

    await service._run_next_job()

    successor = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' ORDER BY id DESC LIMIT 1"""
    )
    successor_payload = json.loads(successor["payload_json"])
    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )
    assert batch["status"] == "imported"
    assert successor_payload["local_actor_drain"] is True
    assert successor_payload["local_actor_batch_id"] == batch["id"]
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (first_job["id"],))[
        "status"
    ] == "done"
    service.db.execute(
        "UPDATE jobs SET available_at='2000-01-01T00:00:00+00:00' WHERE id=?",
        (successor["id"],),
    )

    await service._run_next_job()

    drained = service.db.row("SELECT * FROM jobs WHERE id=?", (successor["id"],))
    assert drained["attempts"] == 1
    assert drained["status"] == "source_limited"
    assert len(starts) == 1
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_public_photo_job_obeys_shared_browser_guard(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.photo_actor_fallback_on_browser_guard_long_deferral = True
    starts = install_successful_photo_actor(service, tmp_path, monkeypatch)
    service.queue_public_photo_capture(1, source_policy="auto")
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
        service.facebook_browser, "account_profile_photos", unexpected
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
    assert starts == []
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
        service.facebook_browser,
        "account_profile_photos",
        public_profile_photos,
    )
    monkeypatch.setattr(service.media, "download", download)
    reconciled: list[tuple[int, str, set[str], int | None, bool]] = []

    def reconcile(profile_id, kind, seen, limit, notify, **kwargs):
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
    assert evidence["auth_scope"] == "account_visible"
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
async def test_viewer_scope_change_resets_old_inventory_count(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    service.db.execute(
        "UPDATE profile_photo_captures SET seen_count=50 WHERE id=?",
        (capture["id"],),
    )
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    monkeypatch.setattr(service.browser_guard, "record_success", lambda profile_id: None)

    item = {
        "id": "new-viewer-photo",
        "url": "https://www.facebook.com/photo.php?fbid=20001",
        "image": "https://scontent.example.fbcdn.net/v/new-viewer.jpg",
    }

    async def account_profile_photos(*args, **kwargs):
        result = completed_photo_result([item])
        result["progress"].update(
            {
                "access_scope": "account_visible",
                "source": "logged_in_browser",
                "viewer_scope_hash": "new-viewer-hash",
                "viewer_scope_changed": True,
                "previous_viewer_scope_hash": "old-viewer-hash",
            }
        )
        return result

    async def download(url):
        path = tmp_path / "new-viewer.jpg"
        path.write_bytes(b"new-viewer-photo")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"new-viewer-photo").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", account_profile_photos
    )
    monkeypatch.setattr(service.media, "download", download)
    reconciled = []
    monkeypatch.setattr(
        service.ingester,
        "reconcile",
        lambda *args, **kwargs: reconciled.append((args, kwargs)),
    )

    await service.capture_profile_photos(1, payload)

    refreshed = service.db.row(
        "SELECT status,seen_count,terminal_evidence_json "
        "FROM profile_photo_captures WHERE id=?",
        (capture["id"],),
    )
    evidence = json.loads(refreshed["terminal_evidence_json"])
    assert refreshed["status"] == "complete"
    assert refreshed["seen_count"] == 1
    assert evidence["viewer_scope_changed"] is True
    assert evidence["viewer_scope_hash"] == "new-viewer-hash"
    entity = service.db.row(
        """SELECT source_viewer_scope_hash FROM entities
        WHERE profile_id=1 AND kind='photo' AND external_id='new-viewer-photo'"""
    )
    assert entity["source_viewer_scope_hash"] == "new-viewer-hash"
    assert reconciled == [
        (
            (1, "photo", {"new-viewer-photo"}, None),
            {
                "notify": False,
                "source_scope": "account_visible",
                "source_viewer_scope_hash": "new-viewer-hash",
            },
        )
    ]


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
        service.facebook_browser,
        "account_profile_photos",
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
        service.facebook_browser,
        "account_profile_photos",
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
        service.facebook_browser,
        "account_profile_photos",
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
        service.facebook_browser,
        "account_profile_photos",
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
        service.facebook_browser,
        "account_profile_photos",
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
        service.facebook_browser, "account_profile_photos", failed
    )

    await service._run_next_job()

    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )
    failed_capture = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    assert job["status"] == "source_limited"
    assert failed_capture["status"] == "source_limited"
    assert "APIFY_TOKEN" in failed_capture["limited_reason"]
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


@pytest.mark.asyncio
async def test_actor_seen_count_does_not_raise_account_completion_threshold(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    service.db.execute(
        "UPDATE jobs SET status='source_limited' WHERE job_type='capture_profile_photos'"
    )
    checkpoint = {
        "account_seen_count": 0,
        "actor_seen_count": 100,
        "actor_discovered_count": 100,
        "completed": False,
    }
    service.db.execute(
        """UPDATE profile_photo_captures SET status='source_limited',seen_count=100,
        checkpoint_json=? WHERE id=?""",
        (json.dumps(checkpoint), capture["id"]),
    )
    old_batch, _ = service.db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=int(capture["id"]),
        actor_id=service.settings.actors.profile_photos,
        normalized_input={"urls": ["https://www.facebook.com/100"]},
        max_charge_usd=0.1,
        request_hash="old-actor-100",
    )
    service.db.execute(
        "UPDATE paid_photo_batches SET status='committed' WHERE id=?",
        (old_batch["id"],),
    )
    now = datetime.now(UTC).isoformat()
    service.db.execute(
        """INSERT INTO entities(profile_id,kind,external_id,source_scope,
        source_collector,present,first_seen_at,last_seen_at)
        VALUES(1,'photo','actor-only','actor_visible','apify_profile_photo_actor',1,?,?)""",
        (now, now),
    )

    _, resumed = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs WHERE status='pending'
            AND job_type='capture_profile_photos' ORDER BY id DESC LIMIT 1"""
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    account_items = [
        {
            "id": f"account-{index}",
            "url": f"https://www.facebook.com/photo.php?fbid={91000 + index}&id=100",
            "image": f"https://scontent.example.fbcdn.net/v/account-{index}.jpg",
        }
        for index in range(50)
    ]

    async def account_photos(profile_url, progress, diagnostic_key):
        result = completed_photo_result(account_items)
        result["progress"] = {
            **progress,
            **result["progress"],
            "access_scope": "account_visible",
            "collector": "authenticated_facebook_photo_viewer",
        }
        return result

    async def download(url):
        path = tmp_path / f"account-{hashlib.sha256(url.encode()).hexdigest()[:10]}.jpg"
        body = url.encode()
        path.write_bytes(body)
        return {
            "status": "ready",
            "sha256": hashlib.sha256(body).hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", account_photos)
    monkeypatch.setattr(service.media, "download", download)

    await service.capture_profile_photos(1, payload)
    for _ in range(4):
        current = service.db.row(
            "SELECT status FROM profile_photo_captures WHERE id=?",
            (resumed["id"],),
        )
        if current["status"] == "complete":
            break
        continuation = service.db.row(
            """SELECT payload_json FROM jobs WHERE status='pending'
            AND job_type='capture_profile_photos' ORDER BY id DESC LIMIT 1"""
        )
        await service.capture_profile_photos(
            1, json.loads(continuation["payload_json"])
        )

    completed = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (resumed["id"],)
    )
    completed_checkpoint = json.loads(completed["checkpoint_json"])
    assert completed["status"] == "complete"
    assert completed["seen_count"] == 50
    assert completed_checkpoint["account_seen_count"] == 50
    assert completed_checkpoint["actor_seen_count"] == 100
    assert service.db.row(
        "SELECT present FROM entities WHERE external_id='actor-only'"
    )["present"] == 1


@pytest.mark.asyncio
async def test_replay_after_post_ingest_crash_restores_one_photo_notification(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, prior = service.queue_public_photo_capture(1)
    service.db.execute(
        "UPDATE jobs SET status='done' WHERE job_type='capture_profile_photos'"
    )
    service.db.execute(
        """UPDATE profile_photo_captures SET status='complete',
        terminal_evidence_json='{}' WHERE id=?""",
        (prior["id"],),
    )
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs WHERE status='pending'
            AND job_type='capture_profile_photos' ORDER BY id DESC LIMIT 1"""
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    item = {
        "id": "crash-photo",
        "url": "https://www.facebook.com/photo.php?fbid=99111&id=100",
        "image": "https://scontent.example.fbcdn.net/v/crash-photo.jpg",
    }

    async def account_photos(*args, **kwargs):
        return completed_photo_result([item])

    async def download(url):
        path = tmp_path / "crash-photo.jpg"
        path.write_bytes(b"crash-photo")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"crash-photo").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", account_photos)
    monkeypatch.setattr(service.media, "download", download)
    original_ingest = service.ingester.ingest
    crashed = False

    async def crash_after_ingest(*args, **kwargs):
        nonlocal crashed
        result = await original_ingest(*args, **kwargs)
        if not crashed:
            crashed = True
            raise RuntimeError("simulated crash after durable ingest")
        return result

    monkeypatch.setattr(service.ingester, "ingest", crash_after_ingest)
    with pytest.raises(RuntimeError, match="simulated crash"):
        await service.capture_profile_photos(1, payload)
    monkeypatch.setattr(service.ingester, "ingest", original_ingest)

    await service.capture_profile_photos(1, payload)
    await service.capture_profile_photos(1, payload)

    assert service.db.row(
        """SELECT COUNT(DISTINCT ev.id) count FROM events ev JOIN outbox o ON o.event_id=ev.id
        WHERE ev.event_type LIKE 'photo_%'"""
    )["count"] == 1
    assert service.db.row(
        "SELECT status FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )["status"] == "complete"


@pytest.mark.asyncio
async def test_imported_actor_raw_recovers_when_disabled_frozen_and_token_missing(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    batch, _ = service.db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=int(capture["id"]),
        actor_id="original/photo-actor",
        normalized_input={"urls": ["https://www.facebook.com/100"]},
        max_charge_usd=0.1,
        request_hash="recover-saved-photo-raw",
    )
    batch = service.db.transition_paid_photo_batch(batch["id"], "launching")
    batch = service.db.transition_paid_photo_batch(
        batch["id"], "run_started", run_id="saved-run"
    )
    actor_result = ActorResult(
        items=[{"photos": ["https://scontent.example.fbcdn.net/v/saved.jpg"]}],
        summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
        run_id="saved-run",
        charged_usd=0.01,
        raw_result_count=1,
    )
    raw_path, raw_sha = service._save_photo_actor_raw(batch, actor_result)
    batch = service.db.transition_paid_photo_batch(
        batch["id"],
        "raw_saved",
        raw_path=str(raw_path),
        raw_sha256=raw_sha,
        raw_result_count=1,
    )
    service.db.transition_paid_photo_batch(
        batch["id"], "imported", parsed_result_count=1
    )
    service.settings.photo_actor_fallback_enabled = False
    service.settings.actors.profile_photos = "changed/photo-actor"
    service.apify.token = ""
    service.db.set_profile_source_control(1, "apify", frozen=True, reason="frozen")

    async def must_not_run(*args, **kwargs):
        raise AssertionError("saved raw recovery must not call browser/provider")

    async def download(url):
        path = tmp_path / "saved.jpg"
        path.write_bytes(b"saved")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"saved").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", must_not_run)
    monkeypatch.setattr(service.apify, "start", must_not_run)
    monkeypatch.setattr(service.apify, "finish", must_not_run)
    monkeypatch.setattr(service.media, "download", download)

    assert await service.capture_profile_photos(1, payload) == "source_limited"
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_browser_partial_is_persisted_when_actor_fallback_is_disabled(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.photo_actor_fallback_enabled = False
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    item = {
        "id": "partial-browser",
        "url": "https://www.facebook.com/photo.php?fbid=77111&id=100",
        "image": "https://scontent.example.fbcdn.net/v/partial-browser.jpg",
    }

    async def partial(*args, **kwargs):
        return {
            "items": [item],
            "batch_items": [item],
            "progress": {
                "access_scope": "account_visible",
                "collected_items": [item],
                "discovered_urls": [item["url"]],
                "processed_urls": [item["url"]],
                "discovered_count": 1,
                "processed_count": 1,
                "grid_complete": False,
            },
            "completed": False,
            "resumable": False,
            "stalled_reason": "photos_of blocked",
        }

    async def download(url):
        path = tmp_path / "partial-browser.jpg"
        path.write_bytes(b"partial")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"partial").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", partial)
    monkeypatch.setattr(service.media, "download", download)
    original_actor_fallback = service._capture_profile_photos_actor
    durable_handoff_sizes: list[int] = []

    async def verify_durable_handoff(profile, current_capture, progress):
        saved = json.loads(
            service.db.row(
                "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
                (capture["id"],),
            )["checkpoint_json"]
        )
        durable_handoff_sizes.append(
            len(saved.get("fallback_browser_batch_items") or [])
        )
        return await original_actor_fallback(profile, current_capture, progress)

    monkeypatch.setattr(
        service, "_capture_profile_photos_actor", verify_durable_handoff
    )

    assert await service.capture_profile_photos(1, payload) == "source_limited"
    assert durable_handoff_sizes == [1]
    entity = service.db.row(
        "SELECT * FROM entities WHERE profile_id=1 AND external_id='partial-browser'"
    )
    assert entity["source_scope"] == "account_visible"
    assert service.db.row(
        "SELECT status FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )["status"] == "source_limited"


@pytest.mark.asyncio
async def test_actor_raw_retries_failed_download_before_marking_item_processed(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    first_payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", login_required)
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[str] = []
    finishes: list[str] = []

    async def start(actor_id, actor_payload, max_charge):
        starts.append(actor_id)
        return StartedActor("retry-media-run", "retry-media-data", "retry-media-store")

    actor_photos = [
        {
            "photoId": str(88000 + index),
            "permalinkUrl": f"https://www.facebook.com/photo.php?fbid={88000 + index}&id=100",
            "imageUrl": f"https://scontent.example.fbcdn.net/v/transient-{index}.jpg",
        }
        for index in range(25)
    ]

    async def finish(started):
        finishes.append(started.run_id)
        return ActorResult(
            items=actor_photos,
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.02,
            raw_result_count=25,
        )

    attempts: dict[str, int] = {}

    async def download(url):
        attempts[url] = attempts.get(url, 0) + 1
        if "transient-0.jpg" in url and attempts[url] == 1:
            return {"status": "pending", "source_url": url, "error": "temporary CDN"}
        path = tmp_path / f"retry-{hashlib.sha256(url.encode()).hexdigest()[:12]}.jpg"
        body = url.encode()
        path.write_bytes(body)
        return {
            "status": "ready",
            "sha256": hashlib.sha256(body).hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    assert await service.capture_profile_photos(1, first_payload) is None
    first_checkpoint = json.loads(
        service.db.row(
            "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
            (capture["id"],),
        )["checkpoint_json"]
    )
    assert len(first_checkpoint["actor_processed_item_ids"]) == 19
    assert sum(attempts.values()) == 20
    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?", (capture["id"],)
    )
    assert batch["status"] == "imported"

    continuation = service.db.row(
        """SELECT payload_json FROM jobs WHERE status='pending'
        AND dedupe_key=?""",
        (f"capture-account-photos:{capture['id']}:1",),
    )
    assert await service.capture_profile_photos(
        1, json.loads(continuation["payload_json"])
    ) == "source_limited"

    final_checkpoint = json.loads(
        service.db.row(
            "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
            (capture["id"],),
        )["checkpoint_json"]
    )
    assert len(final_checkpoint["actor_processed_item_ids"]) == 25
    assert attempts[
        "https://scontent.example.fbcdn.net/v/transient-0.jpg"
    ] == 2
    assert starts == [service.settings.actors.profile_photos]
    assert finishes == ["retry-media-run"]
    assert service.db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )["status"] == "committed"


@pytest.mark.asyncio
async def test_actor_media_retry_deadline_exhausts_raw_and_commits_without_another_drain(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1, source_policy="auto")
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", login_required)
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[str] = []
    finishes: list[str] = []

    async def start(actor_id, actor_payload, max_charge):
        starts.append(actor_id)
        return StartedActor("expired-media-run", "expired-media-data", "expired-media-store")

    async def finish(started):
        finishes.append(started.run_id)
        return ActorResult(
            items=[
                {
                    "photoId": "88000",
                    "permalinkUrl": "https://www.facebook.com/photo.php?fbid=88000&id=100",
                    "imageUrl": "https://scontent.example.fbcdn.net/v/expired-media.jpg",
                }
            ],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.02,
            raw_result_count=1,
        )

    async def download_never_succeeds(url):
        return {"status": "pending", "source_url": url, "error": "permanent CDN failure"}

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download_never_succeeds)

    await service._run_next_job()

    capture_row = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    checkpoint = json.loads(capture_row["checkpoint_json"])
    assert capture_row["status"] == "in_progress"
    assert checkpoint["actor_pending_media_external_ids"] == ["88000"]
    checkpoint["actor_media_retry_until"] = (
        datetime.now(UTC) - timedelta(seconds=1)
    ).isoformat()
    service.db.execute(
        "UPDATE profile_photo_captures SET checkpoint_json=? WHERE id=?",
        (json.dumps(checkpoint, ensure_ascii=False, sort_keys=True), capture["id"]),
    )
    continuation = service.db.row(
        """SELECT * FROM jobs WHERE job_type='capture_profile_photos'
        AND status='pending' ORDER BY id DESC LIMIT 1"""
    )
    continuation_payload = json.loads(continuation["payload_json"])
    assert continuation_payload["photo_capture_id"] == capture["id"]
    service.db.execute(
        "UPDATE jobs SET available_at=? WHERE id=?",
        (datetime.now(UTC).isoformat(), continuation["id"]),
    )

    await service._run_next_job()

    final_capture = service.db.row(
        "SELECT * FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )
    final_checkpoint = json.loads(final_capture["checkpoint_json"])
    assert final_capture["status"] == "source_limited"
    assert final_checkpoint["actor_exhausted_media_external_ids"] == ["88000"]
    assert "actor_pending_media_external_ids" not in final_checkpoint
    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?", (capture["id"],)
    )
    assert batch["status"] == "committed"
    assert service.db.row(
        """SELECT COUNT(*) AS count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )["count"] == 0
    assert starts == [service.settings.actors.profile_photos]
    assert finishes == ["expired-media-run"]


@pytest.mark.asyncio
async def test_terminal_actor_failure_is_settled_and_manual_retry_starts_new_run(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    first_payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", login_required)
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[str] = []

    async def start(actor_id, actor_payload, max_charge):
        run_id = f"terminal-run-{len(starts) + 1}"
        starts.append(run_id)
        return StartedActor(run_id, f"data-{run_id}", f"store-{run_id}")

    async def finish(started):
        if started.run_id == "terminal-run-1":
            raise ActorRunTerminalError(
                started.run_id, "FAILED", "known provider failure", 0.03
            )
        return ActorResult(
            items=[{"photos": ["https://scontent.example.fbcdn.net/v/retry-ok.jpg"]}],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=1,
        )

    async def download(url):
        path = tmp_path / "retry-ok.jpg"
        path.write_bytes(b"retry-ok")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"retry-ok").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    assert await service.capture_profile_photos(1, first_payload) == "source_limited"
    failed_batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?", (capture["id"],)
    )
    assert failed_batch["status"] == "failed"
    assert failed_batch["charged_usd"] == pytest.approx(0.03)
    assert service.db.row(
        "SELECT estimated_usd FROM usage WHERE category='photos'"
    )["estimated_usd"] >= 0.03

    service.db.execute(
        "UPDATE jobs SET status='source_limited' WHERE job_type='capture_profile_photos'"
    )
    _, resumed = service.queue_public_photo_capture(1)
    retry_checkpoint = json.loads(resumed["checkpoint_json"])
    assert retry_checkpoint["actor_retry_nonce"] == 1
    retry_payload = json.loads(
        service.db.row(
            """SELECT payload_json FROM jobs WHERE status='pending'
            AND job_type='capture_profile_photos' ORDER BY id DESC LIMIT 1"""
        )["payload_json"]
    )
    assert await service.capture_profile_photos(1, retry_payload) == "source_limited"
    assert starts == ["terminal-run-1", "terminal-run-2"]


@pytest.mark.asyncio
async def test_unknown_actor_finish_timeout_defers_same_run_without_reconcile(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    payload = json.loads(
        service.db.row(
            "SELECT payload_json FROM jobs WHERE job_type='capture_profile_photos'"
        )["payload_json"]
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", login_required)
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    monkeypatch.setattr(
        service.apify,
        "start",
        lambda *args, **kwargs: _async_value(
            StartedActor("timeout-run", "timeout-data", "timeout-store")
        ),
    )

    async def finish_timeout(started):
        raise TimeoutError("provider status query timed out")

    monkeypatch.setattr(service.apify, "finish", finish_timeout)

    with pytest.raises(DurableActorRunDeferred, match="same run|同一 run"):
        await service.capture_profile_photos(1, payload)
    batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )
    assert batch["status"] == "run_started"
    assert batch["run_id"] == "timeout-run"
    assert "timed out" in batch["error"]


@pytest.mark.asyncio
async def test_scheduler_retries_photo_finish_timeout_on_same_run_without_new_start(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1, source_policy="auto")
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_profile_photos'"
    )
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )

    async def login_required(*args, **kwargs):
        raise FacebookBrowserLoginRequired("login expired")

    monkeypatch.setattr(service.facebook_browser, "account_profile_photos", login_required)
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    starts: list[str] = []
    finishes: list[str] = []

    async def start(*args, **kwargs):
        starts.append("start")
        return StartedActor("durable-photo-run", "photo-dataset", "photo-store")

    async def finish(started):
        finishes.append(started.run_id)
        if len(finishes) == 1:
            raise TimeoutError("provider status query timed out")
        return ActorResult(
            items=[
                {
                    "photoId": "99001",
                    "permalinkUrl": (
                        "https://www.facebook.com/photo.php?fbid=99001&id=100"
                    ),
                    "imageUrl": "https://scontent.example.fbcdn.net/v/durable.jpg",
                }
            ],
            summary={"coverageStatus": "COMPLETE", "pagination": {"hasMore": False}},
            run_id=started.run_id,
            charged_usd=0.01,
            raw_result_count=1,
        )

    async def download(url):
        path = tmp_path / "durable.jpg"
        path.write_bytes(b"durable-photo")
        return {
            "status": "ready",
            "sha256": hashlib.sha256(b"durable-photo").hexdigest(),
            "path": str(path),
            "mime_type": "image/jpeg",
            "size_bytes": path.stat().st_size,
            "source_url": url,
        }

    monkeypatch.setattr(service.apify, "start", start)
    monkeypatch.setattr(service.apify, "finish", finish)
    monkeypatch.setattr(service.media, "download", download)

    await service._run_next_job()

    deferred_job = service.db.row("SELECT * FROM jobs WHERE id=?", (job["id"],))
    deferred_batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )
    assert deferred_job["status"] == "pending"
    assert deferred_job["attempts"] == 1
    assert deferred_job["started_at"] is None
    assert deferred_batch["status"] == "run_started"
    assert deferred_batch["run_id"] == "durable-photo-run"
    service.db.execute(
        "UPDATE jobs SET available_at='2000-01-01T00:00:00+00:00' WHERE id=?",
        (job["id"],),
    )

    await service._run_next_job()

    completed_job = service.db.row("SELECT * FROM jobs WHERE id=?", (job["id"],))
    completed_batch = service.db.row(
        "SELECT * FROM paid_photo_batches WHERE photo_capture_id=?",
        (capture["id"],),
    )
    assert starts == ["start"]
    assert finishes == ["durable-photo-run", "durable-photo-run"]
    assert completed_job["status"] == "source_limited"
    assert completed_job["attempts"] == 2
    assert completed_batch["status"] == "committed"


@pytest.mark.asyncio
async def test_browser_handoff_uses_whole_twenty_item_allowance_before_actor_raw(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.apify.token = "test-token"
    _, capture = service.queue_public_photo_capture(1)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    browser_items = [
        {
            "id": f"browser-{index}",
            "url": f"https://www.facebook.com/photo.php?fbid={93000 + index}&id=100",
            "image": f"https://scontent.example.fbcdn.net/v/browser-{index}.jpg",
        }
        for index in range(20)
    ]
    progress = {
        "access_scope": "account_visible",
        "collected_items": browser_items,
        "fallback_browser_batch_items": browser_items,
    }
    monkeypatch.setattr(
        service.apify,
        "monthly_usage",
        lambda: _async_value(
            MonthlyUsage(
                0.0,
                "2026-09-01T00:00:00+00:00",
                "2026-10-01T00:00:00+00:00",
            )
        ),
    )
    monkeypatch.setattr(
        service.apify,
        "start",
        lambda *args, **kwargs: _async_value(
            StartedActor("allowance-run", "allowance-data", "allowance-store")
        ),
    )
    actor_items = [
        {
            "photoId": str(94000 + index),
            "permalinkUrl": f"https://www.facebook.com/photo.php?fbid={94000 + index}&id=100",
            "imageUrl": f"https://scontent.example.fbcdn.net/v/actor-{index}.jpg",
        }
        for index in range(55)
    ]
    monkeypatch.setattr(
        service.apify,
        "finish",
        lambda started: _async_value(
            ActorResult(
                items=actor_items,
                summary={
                    "coverageStatus": "COMPLETE",
                    "pagination": {"hasMore": False},
                },
                run_id=started.run_id,
                charged_usd=0.05,
                raw_result_count=55,
            )
        ),
    )

    result = await service._capture_profile_photos_actor(
        profile, capture, progress
    )

    assert len(result["batch_items"]) == 20
    assert all(
        item["_capture_access_scope"] == "account_visible"
        for item in result["batch_items"]
    )
    assert result["actor_batch_candidate_ids"] == []
    assert len(result["actor_page_item_ids"]) == 55
    assert result["resumable"] is True
    assert result["actor_batch_ready_to_commit"] is False
