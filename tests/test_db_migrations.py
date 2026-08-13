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


def test_legacy_profiles_get_stable_sort_order(tmp_path: Path):
    path = tmp_path / "legacy-profiles.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("""CREATE TABLE profiles (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")
    connection.execute("INSERT INTO profiles(id,name,url,created_at,updated_at) VALUES(4,'four','https://facebook.com/4','x','x')")
    connection.execute("INSERT INTO profiles(id,name,url,created_at,updated_at) VALUES(9,'nine','https://facebook.com/9','x','x')")
    connection.commit()
    connection.close()

    db = Database(path)

    assert db.has_column("profiles", "sort_order")
    assert db.has_column("profiles", "profile_details_json")
    assert db.has_column("profiles", "serp_last_checked_at")
    assert [row["sort_order"] for row in db.rows("SELECT sort_order FROM profiles ORDER BY id")] == [4, 9]


def test_schema_reopens_unverifiable_browser_backfill_completion(tmp_path: Path):
    db = Database(tmp_path / "browser-cursor.sqlite3")
    db.execute(
        """INSERT INTO profiles(name,url,browser_post_cursor,browser_post_backfill_done,created_at,updated_at)
           VALUES('watched','https://facebook.com/1',NULL,1,'x','x')"""
    )

    db.ensure_schema()

    profile = db.row(
        "SELECT browser_post_cursor,browser_post_backfill_done FROM profiles WHERE url='https://facebook.com/1'"
    )
    assert profile == {"browser_post_cursor": None, "browser_post_backfill_done": 0}
