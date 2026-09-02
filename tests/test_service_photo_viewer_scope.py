from __future__ import annotations

import json
from pathlib import Path

import pytest

from fb_monitor.browser_guard import BrowserDecision
from fb_monitor.config import load_settings
from fb_monitor.db import utcnow
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


def seed_account_photo(
    service: MonitorService, external_id: str, viewer_scope_hash: str
) -> int:
    now = utcnow()
    return service.db.execute(
        """INSERT INTO entities(
          profile_id,kind,external_id,source_url,present,missing_successes,
          source_scope,source_collector,source_viewer_scope_hash,
          first_seen_at,last_seen_at
        ) VALUES(1,'photo',?,?,1,0,'account_visible',
          'authenticated_facebook_photo_viewer',?,?,?)""",
        (
            external_id,
            f"https://www.facebook.com/photo.php?fbid={external_id}&id=100",
            viewer_scope_hash,
            now,
            now,
        ),
    )


async def complete_empty_inventory(
    service: MonitorService,
    monkeypatch,
    *,
    viewer_scope_hash: str,
    viewer_scope_changed: bool,
) -> None:
    _, capture = service.queue_public_photo_capture(1)
    job = service.db.row(
        """SELECT id,payload_json FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'
        ORDER BY id DESC LIMIT 1"""
    )
    payload = json.loads(job["payload_json"])
    monkeypatch.setattr(
        service.browser_guard,
        "acquire",
        lambda profile_id: BrowserDecision(True, "allowed", None, 1),
    )
    monkeypatch.setattr(service.browser_guard, "record_success", lambda profile_id: None)

    async def account_profile_photos(*args, **kwargs):
        progress = {
            "completed": True,
            "terminal_reason": "grid_inventory_processed",
            "declared_total": 0,
            "discovered_urls": [],
            "processed_urls": [],
            "discovered_count": 0,
            "processed_count": 0,
            "pending_count": 0,
            "grid_complete": True,
            "grid_empty_confirmed": True,
            "collected_items": [],
            "resume_url": "",
            "access_scope": "account_visible",
            "source": "logged_in_browser",
            "viewer_scope_hash": viewer_scope_hash,
            "viewer_scope_changed": viewer_scope_changed,
        }
        return {
            "items": [],
            "batch_items": [],
            "progress": progress,
            "completed": True,
            "terminal_reason": "grid_inventory_processed",
            "stalled_reason": "",
        }

    monkeypatch.setattr(
        service.facebook_browser, "account_profile_photos", account_profile_photos
    )
    assert await service.capture_profile_photos(1, payload) is None
    assert service.db.row(
        "SELECT status FROM profile_photo_captures WHERE id=?", (capture["id"],)
    )["status"] == "complete"
    service.db.execute("UPDATE jobs SET status='done' WHERE id=?", (job["id"],))


@pytest.mark.asyncio
async def test_changed_viewer_inventory_never_marks_other_viewer_photo_missing(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    entity_id = seed_account_photo(service, "70001", "viewer-a")

    await complete_empty_inventory(
        service,
        monkeypatch,
        viewer_scope_hash="viewer-b",
        viewer_scope_changed=True,
    )
    await complete_empty_inventory(
        service,
        monkeypatch,
        viewer_scope_hash="viewer-b",
        viewer_scope_changed=False,
    )

    entity = service.db.row(
        "SELECT present,missing_successes FROM entities WHERE id=?", (entity_id,)
    )
    assert entity == {"present": 1, "missing_successes": 0}


@pytest.mark.asyncio
async def test_same_viewer_two_complete_inventories_remove_missing_photo(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    entity_id = seed_account_photo(service, "70002", "viewer-a")

    await complete_empty_inventory(
        service,
        monkeypatch,
        viewer_scope_hash="viewer-a",
        viewer_scope_changed=False,
    )
    await complete_empty_inventory(
        service,
        monkeypatch,
        viewer_scope_hash="viewer-a",
        viewer_scope_changed=False,
    )

    entity = service.db.row(
        "SELECT present,missing_successes FROM entities WHERE id=?", (entity_id,)
    )
    assert entity == {"present": 0, "missing_successes": 2}
