from __future__ import annotations

import hashlib
import json
from pathlib import Path

from PIL import Image

from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_daily_media_dedupe_keeps_high_quality_and_rebinds_all_references(
    tmp_path: Path,
    monkeypatch,
):
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
    service = MonitorService(load_settings(config))
    entity_id = service.db.execute(
        """INSERT INTO entities(
        profile_id,kind,external_id,current_hash,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','p1','hash',1,'now','now')"""
    )
    version_id = service.db.execute(
        """INSERT INTO versions(
        entity_id,content_hash,normalized_json,raw_path,seen_at,change_type
        ) VALUES(?,'hash','{}','raw.json','now','created')""",
        (entity_id,),
    )
    service.db.execute(
        "UPDATE entities SET current_version_id=? WHERE id=?",
        (version_id, entity_id),
    )

    low_path = service.media.root / "legacy-low.png"
    high_path = service.media.root / "legacy-high.png"
    collision_path = service.media.root / "near-collision.png"
    Image.new("RGB", (40, 40), "navy").save(low_path)
    Image.new("RGB", (800, 800), "navy").save(high_path)
    # Uniform images share the old average hash.  RGB validation must still
    # reject this unrelated representation even on the same normalized path.
    Image.new("RGB", (800, 800), "white").save(collision_path)
    base = "https://scontent.example.fbcdn.net/v/same-object.png"

    media_ids = []
    for label, path in (
        ("low", low_path),
        ("high", high_path),
        ("collision", collision_path),
    ):
        media_id = service.db.execute(
            """INSERT INTO media(
            sha256,source_url,mime_type,size_bytes,path,perceptual_hash,status,
            first_seen_at,last_attempt_at
            ) VALUES(?,?,?,?,?,?,'ready','now','now')""",
            (
                f"legacy-{label}",
                f"{base}?representation={label}",
                "image/png",
                path.stat().st_size,
                str(path),
                "ffffffffffffffff",
            ),
        )
        media_ids.append(media_id)
        service.db.execute(
            """INSERT INTO entity_media(entity_id,version_id,media_id,role,position)
            VALUES(?,?,?,'image',?)""",
            (entity_id, version_id, media_id, len(media_ids) - 1),
        )

    low_id, high_id, collision_id = media_ids
    service.db.upsert_media_alias(
        1,
        canonical_media_id="same-object",
        provider="legacy",
        alias_type="source_url",
        alias_value="low-alias",
        entity_id=entity_id,
        media_id=low_id,
        source_url=f"{base}?representation=low",
        width=40,
        height=40,
        mime_type="image/png",
        sha256="legacy-low",
    )
    event_id = service.db.add_event(
        "media-maintenance-event",
        "post_created",
        {"title": "test"},
        1,
        entity_id,
        notify=False,
    )
    service.db.execute(
        """INSERT INTO outbox(
        event_id,kind,payload_json,next_attempt_at,created_at,media_sha256
        ) VALUES(?,'media',?,'now','now','legacy-low')""",
        (
            event_id,
            json.dumps(
                {"path": str(low_path), "mime_type": "image/png", "sha256": "legacy-low"}
            ),
        ),
    )

    counts = service._dedupe_existing_media()

    assert counts == {"checked": 3, "merged": 1, "orphaned": 0, "errors": 0}
    assert service.db.row("SELECT COUNT(*) count FROM media")["count"] == 2
    assert service.db.row("SELECT id FROM media WHERE id=?", (low_id,)) is None
    winner = service.db.row("SELECT * FROM media WHERE id=?", (high_id,))
    collision = service.db.row("SELECT * FROM media WHERE id=?", (collision_id,))
    assert winner["sha256"] == _sha256(high_path)
    assert collision["sha256"] == _sha256(collision_path)
    assert low_path.exists() is False
    assert high_path.exists()
    assert collision_path.exists()
    assert service.db.row(
        "SELECT COUNT(*) count FROM entity_media WHERE media_id=?", (high_id,)
    )["count"] == 1
    assert service.db.row(
        "SELECT COUNT(*) count FROM entity_media WHERE media_id=?", (collision_id,)
    )["count"] == 1
    alias = service.db.row(
        "SELECT media_id,width,height,sha256 FROM media_aliases WHERE alias_value='low-alias'"
    )
    assert alias == {
        "media_id": high_id,
        "width": 800,
        "height": 800,
        "sha256": _sha256(high_path),
    }
    outbox = service.db.row(
        "SELECT media_sha256,payload_json FROM outbox WHERE kind='media'"
    )
    assert outbox["media_sha256"] == _sha256(high_path)
    payload = json.loads(outbox["payload_json"])
    assert payload["path"] == str(high_path)
    assert payload["sha256"] == _sha256(high_path)
