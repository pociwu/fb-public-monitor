from __future__ import annotations

import json
from pathlib import Path

import pytest

from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService


def _service(tmp_path: Path, monkeypatch) -> MonitorService:
    data_dir = (tmp_path / "data").as_posix()
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {data_dir}
  low_disk_gb: 0
capture_v2:
  enabled: true
actors:
  comments: test/comments
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CAPTURE_V2_ENABLED", "1")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    return MonitorService(load_settings(config))


def _scope(service: MonitorService) -> tuple[dict, dict, dict]:
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    epoch, _ = service.db.get_or_create_capture_epoch(1, "test", status="ready")
    posts = service.db.upsert_coverage_stream(
        int(epoch["id"]), stream="posts", surface="timeline_posts"
    )
    return profile, epoch, posts


def _post_with_ready_media(service: MonitorService) -> tuple[int, dict]:
    entity_id = service.db.execute(
        """INSERT INTO entities(
          profile_id,kind,external_id,source_url,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','p1','https://facebook.com/100/posts/p1',1,'now','now')"""
    )
    version_id = service.db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?,'hash','{}','raw.json','now','created')""",
        (entity_id,),
    )
    service.db.execute(
        "UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id)
    )
    media_id = service.db.execute(
        """INSERT INTO media(sha256,source_url,status,first_seen_at,last_attempt_at)
        VALUES('sha','https://cdn.example/a.jpg','ready','now','now')"""
    )
    service.db.execute(
        """INSERT INTO entity_media(entity_id,version_id,media_id,role,position)
        VALUES(?,?,?,'image',0)""",
        (entity_id, version_id, media_id),
    )
    return entity_id, {
        "postId": "p1",
        "postUrl": "https://facebook.com/100/posts/p1",
        "images": [{"url": "https://cdn.example/a.jpg"}],
        "photosCount": 1,
    }


@pytest.mark.asyncio
async def test_terminal_posts_seed_one_comment_job_and_missing_contract_is_source_limited(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    profile, epoch, posts = _scope(service)
    entity_id, item = _post_with_ready_media(service)
    service._reconcile_capture_v2_batch_media(
        profile=profile,
        epoch=epoch,
        batch={"id": 9, "provider": "apify", "contract_id": None},
        raw={"items": [item]},
    )

    media = service.db.row(
        """SELECT * FROM coverage_streams WHERE epoch_id=? AND stream='media'
        AND scope_type='post' AND scope_id=?""",
        (epoch["id"], str(entity_id)),
    )
    assert media["status"] == "source_limited"
    assert "獨立跨頁 contract" in media["limited_reason"]
    assert service.db.row(
        "SELECT status FROM post_media_coverage WHERE epoch_id=? AND post_entity_id=?",
        (epoch["id"], entity_id),
    )["status"] == "source_limited"

    service._seed_capture_v2_comments_after_posts(profile=profile, epoch=epoch)
    assert service.db.row(
        "SELECT COUNT(*) total FROM coverage_streams WHERE stream='comments'"
    )["total"] == 0

    service.db.update_coverage_stream(int(posts["id"]), status="in_progress")
    service.db.update_coverage_stream(
        int(posts["id"]),
        status="complete",
        terminal_evidence_json={"kind": "feed_exhausted"},
    )
    service._seed_capture_v2_comments_after_posts(profile=profile, epoch=epoch)
    service._seed_capture_v2_comments_after_posts(profile=profile, epoch=epoch)
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_comments_v2' ORDER BY id LIMIT 1"
    )
    assert job is not None
    assert service.db.row(
        "SELECT COUNT(*) total FROM jobs WHERE job_type='capture_comments_v2'"
    )["total"] == 1

    async def must_not_launch(*args, **kwargs):
        raise AssertionError("comments without an exact executor must not launch a paid Actor")

    service.apify.start = must_not_launch
    await service.capture_comments_v2(1, json.loads(job["payload_json"]))

    assert service.db.row(
        "SELECT status FROM coverage_streams WHERE stream='comments'"
    )["status"] == "source_limited"
    assert service.db.row(
        "SELECT status,is_active FROM capture_epochs WHERE id=?", (epoch["id"],)
    ) == {"status": "source_limited", "is_active": 0}


@pytest.mark.asyncio
async def test_comment_job_cannot_mutate_another_post_checkpoint(tmp_path: Path, monkeypatch):
    service = _service(tmp_path, monkeypatch)
    profile, epoch, posts = _scope(service)
    entity_id, item = _post_with_ready_media(service)
    service._reconcile_capture_v2_batch_media(
        profile=profile,
        epoch=epoch,
        batch={"id": 9, "provider": "apify", "contract_id": None},
        raw={"items": [item]},
    )
    service.db.update_coverage_stream(int(posts["id"]), status="in_progress")
    service.db.update_coverage_stream(
        int(posts["id"]),
        status="complete",
        terminal_evidence_json={"kind": "feed_exhausted"},
    )
    service._seed_capture_v2_comments_after_posts(profile=profile, epoch=epoch)
    job = service.db.row("SELECT * FROM jobs WHERE job_type='capture_comments_v2'")
    payload = json.loads(job["payload_json"])
    payload["post_entity_id"] = entity_id + 999

    with pytest.raises(ValueError, match="不屬於此帳號"):
        await service.capture_comments_v2(1, payload)

    assert service.db.row(
        "SELECT status FROM coverage_streams WHERE stream='comments'"
    )["status"] == "pending"


def test_failed_required_media_stream_finishes_epoch_failed(tmp_path: Path, monkeypatch):
    service = _service(tmp_path, monkeypatch)
    _, epoch, posts = _scope(service)
    service.db.update_coverage_stream(int(posts["id"]), status="in_progress")
    service.db.update_coverage_stream(
        int(posts["id"]),
        status="complete",
        terminal_evidence_json={"kind": "feed_exhausted"},
    )
    media = service.db.upsert_coverage_stream(
        int(epoch["id"]),
        stream="media",
        surface="post_albums",
        scope_type="post",
        scope_id="99",
    )
    service.db.update_coverage_stream(
        int(media["id"]), status="failed", limited_reason="download failed"
    )

    service._refresh_capture_v2_epoch(int(epoch["id"]))

    assert service.db.row(
        "SELECT status,is_active,terminal_reason FROM capture_epochs WHERE id=?",
        (epoch["id"],),
    ) == {
        "status": "failed",
        "is_active": 0,
        "terminal_reason": "至少一個必要 stream 失敗",
    }


def test_zero_post_terminal_epoch_completes_without_comment_jobs(tmp_path: Path, monkeypatch):
    service = _service(tmp_path, monkeypatch)
    profile, epoch, posts = _scope(service)
    service.db.update_coverage_stream(int(posts["id"]), status="in_progress")
    service.db.update_coverage_stream(
        int(posts["id"]),
        status="complete",
        terminal_evidence_json={"kind": "empty_feed_exhausted"},
    )

    service._seed_capture_v2_comments_after_posts(profile=profile, epoch=epoch)
    service._refresh_capture_v2_epoch(int(epoch["id"]))

    assert service.db.row(
        "SELECT COUNT(*) total FROM jobs WHERE job_type='capture_comments_v2'"
    )["total"] == 0
    assert service.db.row(
        "SELECT status,is_active FROM capture_epochs WHERE id=?", (epoch["id"],)
    ) == {"status": "complete", "is_active": 0}
