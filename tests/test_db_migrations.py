import sqlite3
from pathlib import Path

from fb_monitor.db import Database


def test_legacy_entity_media_gets_position_column_and_stable_values(tmp_path: Path):
    path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE entity_media (
          entity_id INTEGER NOT NULL,
          version_id INTEGER NOT NULL,
          media_id INTEGER NOT NULL,
          role TEXT,
          discovery_path TEXT,
          PRIMARY KEY(entity_id,version_id,media_id)
        );
        INSERT INTO entity_media VALUES(1,10,100,'image','$.images[0]');
        INSERT INTO entity_media VALUES(1,10,101,'image','$.images[1]');
        INSERT INTO entity_media VALUES(2,20,200,'image','$.image');
        """
    )
    connection.commit()
    connection.close()

    db = Database(path)

    assert db.has_column("entity_media", "position")
    rows = db.rows("SELECT entity_id,version_id,media_id,position FROM entity_media ORDER BY rowid")
    assert [row["position"] for row in rows] == [0, 1, 0]

    # Forward migrations are safe to repeat on every process start.
    db.ensure_schema()
    assert [row["position"] for row in db.rows("SELECT position FROM entity_media ORDER BY rowid")] == [0, 1, 0]


def test_legacy_outbox_is_upgraded_before_new_index_is_created(tmp_path: Path):
    path = tmp_path / "legacy-outbox.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("""CREATE TABLE outbox (
      id INTEGER PRIMARY KEY, event_id INTEGER, kind TEXT NOT NULL DEFAULT 'text', payload_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL,
      last_error TEXT, created_at TEXT NOT NULL, sent_at TEXT, UNIQUE(event_id, kind, payload_json)
    )""")
    connection.commit()
    connection.close()

    db = Database(path)
    columns = {row["name"] for row in db.rows("PRAGMA table_info(outbox)")}
    assert {"group_id", "media_sha256", "cancelled_at"} <= columns
    assert db.row("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_outbox_media_sha'")
