from __future__ import annotations

import json
from pathlib import Path

from fb_monitor.capture_coordinator import (
    evaluate_post_media,
    reconcile_post_media_checkpoint,
    resolve_epoch,
    seed_comment_checkpoints,
)
from fb_monitor.capture_v2 import CoverageStatus
from fb_monitor.db import Database


def _scope(tmp_path: Path) -> tuple[Database, dict, int]:
    db = Database(tmp_path / "coordinator.sqlite3")
    db.execute(
        """INSERT INTO profiles(id,name,url,created_at,updated_at)
        VALUES(1,'FB-1','https://facebook.com/1','now','now')"""
    )
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    db.upsert_coverage_stream(
        int(epoch["id"]), stream="posts", surface="timeline_posts"
    )
    entity_id = db.execute(
        """INSERT INTO entities(
          profile_id,kind,external_id,source_url,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','p1','https://facebook.com/1/posts/p1',1,'now','now')"""
    )
    return db, epoch, entity_id


def test_media_requires_explicit_terminal_and_all_downloads_ready():
    item = {
        "postId": "p1",
        "images": [
            {"url": "https://scontent.example.fbcdn.net/a.jpg?_nc_x=1"},
            {"url": "https://scontent.example.fbcdn.net/b.jpg?_nc_x=2"},
        ],
    }
    limited = evaluate_post_media(
        item,
        ready_source_urls=[entry["url"] for entry in item["images"]],
    )
    assert limited.status is CoverageStatus.SOURCE_LIMITED
    assert "terminal" in str(limited.reason)

    item["photosCount"] = 2
    complete = evaluate_post_media(
        item,
        ready_source_urls=[entry["url"] for entry in item["images"]],
        contract_verified=True,
    )
    assert complete.status is CoverageStatus.COMPLETE
    assert complete.terminal_evidence["kind"] == "declared_count_reached"

    missing = evaluate_post_media(
        item,
        ready_source_urls=[item["images"][0]["url"]],
        contract_verified=True,
    )
    assert missing.status is CoverageStatus.SOURCE_LIMITED
    assert "尚未成功保存" in str(missing.reason)

    inconsistent = evaluate_post_media(
        {**item, "photosCount": 1, "allPhotosExpanded": True},
        ready_source_urls=[entry["url"] for entry in item["images"]],
        contract_verified=True,
    )
    assert inconsistent.status is CoverageStatus.SOURCE_LIMITED
    assert "宣告 1 個媒體" in str(inconsistent.reason)


def test_media_capped_or_cursor_never_completes_even_with_declared_count():
    base = {
        "images": [{"url": "https://cdn.example/1.jpg"}],
        "mediaCount": 1,
    }
    capped = evaluate_post_media(
        {**base, "mediaCoverageStatus": "partial_actor_limit"},
        ready_source_urls=["https://cdn.example/1.jpg"],
        contract_verified=True,
    )
    assert capped.status is CoverageStatus.SOURCE_LIMITED

    cursor = evaluate_post_media(
        {**base, "nextMediaCursor": "page-2"},
        ready_source_urls=["https://cdn.example/1.jpg"],
        contract_verified=True,
    )
    assert cursor.status is CoverageStatus.SOURCE_LIMITED
    assert "cursor=page-2" in str(cursor.reason)


def test_nested_comment_media_metadata_does_not_change_post_album_coverage():
    decision = evaluate_post_media(
        {
            "postId": "p1",
            "mediaCount": 0,
            "comments": [
                {
                    "commentId": "c1",
                    "mediaCount": 12,
                    "nextMediaCursor": "comment-media-page-2",
                }
            ],
        },
        contract_verified=True,
    )

    assert decision.status is CoverageStatus.COMPLETE
    assert decision.expected_count == 0
    assert decision.terminal_evidence["kind"] == "declared_count_reached"


def test_reconcile_creates_one_per_post_media_stream_and_checkpoint(tmp_path: Path):
    db, epoch, entity_id = _scope(tmp_path)
    version_id = db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?, 'hash', '{}', 'raw.json', 'now', 'created')""",
        (entity_id,),
    )
    db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))
    media_id = db.execute(
        """INSERT INTO media(
          sha256,source_url,status,first_seen_at,last_attempt_at
        ) VALUES('sha','https://cdn.example/a.jpg','ready','now','now')"""
    )
    db.execute(
        """INSERT INTO entity_media(entity_id,version_id,media_id,role,position)
        VALUES(?,?,?,'image',0)""",
        (entity_id, version_id, media_id),
    )
    item = {
        "postId": "p1",
        "images": [{"url": "https://cdn.example/a.jpg"}],
        "allPhotosExpanded": True,
    }

    coverage, checkpoint, decision = reconcile_post_media_checkpoint(
        db,
        epoch_id=int(epoch["id"]),
        profile_id=1,
        post_entity_id=entity_id,
        item=item,
        provider="apify",
        contract_id=None,
        batch_id=9,
        contract_verified=True,
    )
    second = reconcile_post_media_checkpoint(
        db,
        epoch_id=int(epoch["id"]),
        profile_id=1,
        post_entity_id=entity_id,
        item=item,
        provider="apify",
        contract_id=None,
        batch_id=9,
        contract_verified=True,
    )

    assert decision.status is CoverageStatus.COMPLETE
    assert coverage["status"] == "complete"
    assert checkpoint["status"] == "complete"
    assert json.loads(checkpoint["seen_media_ids_json"]) == ["https://cdn.example/a.jpg"]
    assert second[0]["id"] == coverage["id"]
    assert db.row("SELECT COUNT(*) total FROM post_media_coverage")["total"] == 1
    assert db.row(
        "SELECT COUNT(*) total FROM coverage_streams WHERE stream='media'"
    )["total"] == 1


def test_production_post_contract_cannot_complete_album_without_independent_media_contract(
    tmp_path: Path,
):
    db, epoch, entity_id = _scope(tmp_path)
    version_id = db.execute(
        """INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,seen_at,change_type)
        VALUES(?, 'hash', '{}', 'raw.json', 'now', 'created')""",
        (entity_id,),
    )
    db.execute("UPDATE entities SET current_version_id=? WHERE id=?", (version_id, entity_id))
    media_id = db.execute(
        """INSERT INTO media(sha256,source_url,status,first_seen_at,last_attempt_at)
        VALUES('sha','https://cdn.example/a.jpg','ready','now','now')"""
    )
    db.execute(
        """INSERT INTO entity_media(entity_id,version_id,media_id,role,position)
        VALUES(?,?,?,'image',0)""",
        (entity_id, version_id, media_id),
    )

    coverage, checkpoint, decision = reconcile_post_media_checkpoint(
        db,
        epoch_id=int(epoch["id"]),
        profile_id=1,
        post_entity_id=entity_id,
        item={
            "postId": "p1",
            "images": [{"url": "https://cdn.example/a.jpg"}],
            "allPhotosExpanded": True,
        },
        provider="apify-posts-contract",
        contract_id=None,
    )

    assert decision.status is CoverageStatus.SOURCE_LIMITED
    assert coverage["status"] == "source_limited"
    assert checkpoint["status"] == "source_limited"
    assert "獨立跨頁 contract" in str(decision.reason)


def test_comments_are_seeded_only_after_posts_terminal(tmp_path: Path):
    db, epoch, entity_id = _scope(tmp_path)
    db.upsert_post_media_coverage(
        int(epoch["id"]), post_entity_id=entity_id, surface="post_albums"
    )

    assert seed_comment_checkpoints(
        db, epoch_id=int(epoch["id"]), profile_id=1, provider="apify", contract_id=None
    ) == []

    posts = db.row(
        "SELECT id FROM coverage_streams WHERE epoch_id=? AND stream='posts'",
        (epoch["id"],),
    )
    db.update_coverage_stream(int(posts["id"]), status="in_progress")
    db.update_coverage_stream(
        int(posts["id"]),
        status="complete",
        terminal_evidence_json={"kind": "feed_exhausted"},
    )
    first = seed_comment_checkpoints(
        db, epoch_id=int(epoch["id"]), profile_id=1, provider="apify", contract_id=None
    )
    second = seed_comment_checkpoints(
        db, epoch_id=int(epoch["id"]), profile_id=1, provider="apify", contract_id=None
    )

    assert len(first) == 1
    assert first == second
    assert first[0].post_entity_id == entity_id
    assert first[0].post_url.endswith("/posts/p1")
    assert db.row(
        "SELECT COUNT(*) total FROM coverage_streams WHERE stream='comments'"
    )["total"] == 1


def test_epoch_only_completes_when_every_required_stream_is_complete():
    assert resolve_epoch([{"status": "complete"}, {"status": "in_progress"}]).status == "running"
    assert resolve_epoch([{"status": "complete"}, {"status": "failed"}]).status == "failed"
    limited = resolve_epoch([{"status": "complete"}, {"status": "source_limited"}])
    assert limited.ready is False
    assert limited.status == "source_limited"
    complete = resolve_epoch([{"status": "complete"}, {"status": "complete"}])
    assert complete.ready is True
    assert complete.status == "complete"
