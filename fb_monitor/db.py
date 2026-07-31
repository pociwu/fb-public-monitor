from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS profiles (
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1,
  fb_id TEXT, display_name TEXT, public_state TEXT NOT NULL DEFAULT 'unknown', missing_successes INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TEXT, last_success_at TEXT, next_visit_at TEXT, last_full_audit_at TEXT,
  backfill_cursor TEXT, backfill_done INTEGER NOT NULL DEFAULT 0, audit_cursor TEXT, audit_token TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0, sort_order INTEGER,
  last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS entities (
  id INTEGER PRIMARY KEY, profile_id INTEGER NOT NULL REFERENCES profiles(id), kind TEXT NOT NULL,
  external_id TEXT NOT NULL, parent_external_id TEXT, dedupe_key TEXT, source_url TEXT, published_at TEXT,
  current_hash TEXT, current_version_id INTEGER, present INTEGER NOT NULL DEFAULT 1,
  missing_successes INTEGER NOT NULL DEFAULT 0, notification_hash TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  UNIQUE(profile_id, kind, external_id)
);
CREATE TABLE IF NOT EXISTS versions (
  id INTEGER PRIMARY KEY, entity_id INTEGER NOT NULL REFERENCES entities(id), content_hash TEXT NOT NULL,
  normalized_json TEXT NOT NULL, raw_path TEXT NOT NULL, markdown_path TEXT,
  seen_at TEXT NOT NULL, change_type TEXT NOT NULL, UNIQUE(entity_id, content_hash)
);
CREATE TABLE IF NOT EXISTS media (
  id INTEGER PRIMARY KEY, sha256 TEXT UNIQUE, source_url TEXT NOT NULL, mime_type TEXT, size_bytes INTEGER,
  path TEXT, perceptual_hash TEXT, status TEXT NOT NULL DEFAULT 'pending', first_seen_at TEXT NOT NULL, last_attempt_at TEXT,
  retry_until TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS entity_media (
  entity_id INTEGER NOT NULL REFERENCES entities(id), version_id INTEGER NOT NULL REFERENCES versions(id),
  media_id INTEGER NOT NULL REFERENCES media(id), role TEXT, discovery_path TEXT, position INTEGER,
  PRIMARY KEY(entity_id, version_id, media_id)
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY, event_key TEXT NOT NULL UNIQUE, profile_id INTEGER REFERENCES profiles(id),
  entity_id INTEGER REFERENCES entities(id), event_type TEXT NOT NULL, payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL, notified_at TEXT, notification_group_id INTEGER
);
CREATE TABLE IF NOT EXISTS notification_groups (
  id INTEGER PRIMARY KEY, profile_id INTEGER REFERENCES profiles(id), entity_id INTEGER REFERENCES entities(id),
  payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
  ready_at TEXT NOT NULL, sent_at TEXT, cancelled_at TEXT
);
CREATE TABLE IF NOT EXISTS comment_baselines (
  profile_id INTEGER NOT NULL REFERENCES profiles(id), parent_external_id TEXT NOT NULL,
  established_at TEXT NOT NULL, PRIMARY KEY(profile_id,parent_external_id)
);
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY, event_id INTEGER REFERENCES events(id), kind TEXT NOT NULL DEFAULT 'text',
  payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL, sent_at TEXT,
  group_id INTEGER REFERENCES notification_groups(id), media_sha256 TEXT, media_perceptual_hash TEXT, cancelled_at TEXT,
  UNIQUE(event_id, kind, payload_json)
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY, profile_id INTEGER REFERENCES profiles(id), job_type TEXT NOT NULL,
  priority INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', payload_json TEXT NOT NULL DEFAULT '{}',
  available_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, error TEXT, created_at TEXT NOT NULL,
  started_at TEXT, finished_at TEXT
);
CREATE TABLE IF NOT EXISTS usage (
  month TEXT NOT NULL, category TEXT NOT NULL, estimated_usd REAL NOT NULL DEFAULT 0,
  results INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, PRIMARY KEY(month, category)
);
CREATE TABLE IF NOT EXISTS audit_seen (
  profile_id INTEGER NOT NULL REFERENCES profiles(id), audit_token TEXT NOT NULL,
  kind TEXT NOT NULL, external_id TEXT NOT NULL,
  PRIMARY KEY(profile_id,audit_token,kind,external_id)
);
CREATE TABLE IF NOT EXISTS actor_runs (
  id INTEGER PRIMARY KEY, profile_id INTEGER REFERENCES profiles(id), category TEXT NOT NULL,
  actor_id TEXT NOT NULL, run_id TEXT, input_variant TEXT, input_json TEXT NOT NULL,
  status TEXT NOT NULL, result_count INTEGER NOT NULL DEFAULT 0, charged_usd REAL NOT NULL DEFAULT 0,
  summary_json TEXT, samples_json TEXT, error TEXT, started_at TEXT NOT NULL, finished_at TEXT
);
CREATE TABLE IF NOT EXISTS schema_migrations (
  name TEXT PRIMARY KEY, applied_at TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS maintenance_runs (
  id INTEGER PRIMARY KEY, task_name TEXT NOT NULL, status TEXT NOT NULL,
  summary_json TEXT NOT NULL DEFAULT '{}', error TEXT, started_at TEXT NOT NULL, finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_entities_profile_kind ON entities(profile_id, kind, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(status, available_at, priority);
CREATE INDEX IF NOT EXISTS idx_outbox_queue ON outbox(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_notification_groups_pending ON notification_groups(entity_id,status,ready_at);
CREATE INDEX IF NOT EXISTS idx_actor_runs_profile ON actor_runs(profile_id, id DESC);
"""


def utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _hash_distance(left: str, right: str) -> int:
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except ValueError:
        return 999


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._local = threading.local()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        """Create the schema and apply idempotent forward migrations.

        Keep this public so entry points can explicitly repair a database copied
        from an older release before serving queries.
        """
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            # Lightweight forward migrations for existing installations.
            columns = {row[1] for row in conn.execute("PRAGMA table_info(profiles)")}
            for name in ("audit_cursor", "audit_token", "display_name"):
                if name not in columns:
                    conn.execute(f"ALTER TABLE profiles ADD COLUMN {name} TEXT")
            if "sort_order" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN sort_order INTEGER")
            conn.execute("UPDATE profiles SET sort_order=id WHERE sort_order IS NULL")
            entity_columns = {row[1] for row in conn.execute("PRAGMA table_info(entities)")}
            if "dedupe_key" not in entity_columns:
                conn.execute("ALTER TABLE entities ADD COLUMN dedupe_key TEXT")
            if "notification_hash" not in entity_columns:
                conn.execute("ALTER TABLE entities ADD COLUMN notification_hash TEXT")
            media_columns = {row[1] for row in conn.execute("PRAGMA table_info(entity_media)")}
            if "discovery_path" not in media_columns:
                conn.execute("ALTER TABLE entity_media ADD COLUMN discovery_path TEXT")
            if "position" not in media_columns:
                conn.execute("ALTER TABLE entity_media ADD COLUMN position INTEGER")
            event_columns = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
            if "notification_group_id" not in event_columns:
                conn.execute("ALTER TABLE events ADD COLUMN notification_group_id INTEGER")
            outbox_columns = {row[1] for row in conn.execute("PRAGMA table_info(outbox)")}
            media_table_columns = {row[1] for row in conn.execute("PRAGMA table_info(media)")}
            if "perceptual_hash" not in media_table_columns:
                conn.execute("ALTER TABLE media ADD COLUMN perceptual_hash TEXT")
            for name, definition in (("group_id", "INTEGER"), ("media_sha256", "TEXT"), ("media_perceptual_hash", "TEXT"), ("cancelled_at", "TEXT")):
                if name not in outbox_columns:
                    conn.execute(f"ALTER TABLE outbox ADD COLUMN {name} {definition}")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_media_sha ON outbox(media_sha256,status)")
            conn.execute(
                """UPDATE entity_media AS target
                SET position=(
                    SELECT COUNT(*)-1 FROM entity_media AS prior
                    WHERE prior.entity_id=target.entity_id
                      AND prior.version_id=target.version_id
                      AND prior.rowid<=target.rowid
                )
                WHERE target.position IS NULL"""
            )

    def has_column(self, table: str, column: str) -> bool:
        if table not in {"profiles", "entities", "versions", "media", "entity_media", "events", "outbox", "jobs", "usage", "audit_seen", "actor_runs", "schema_migrations"}:
            return False
        with self.connect() as conn:
            return column in {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def sync_profiles(self, profiles: list[Any]) -> None:
        now = utcnow()
        configured = {p.url.rstrip("/") for p in profiles}
        with self.connect() as conn:
            for p in profiles:
                url = p.url.rstrip("/")
                conn.execute(
                    """INSERT INTO profiles(name,url,enabled,created_at,updated_at) VALUES(?,?,?,?,?)
                    ON CONFLICT(url) DO UPDATE SET name=excluded.name, enabled=excluded.enabled, updated_at=excluded.updated_at""",
                    (p.name, url, int(p.enabled), now, now),
                )
            rows = conn.execute("SELECT id,url FROM profiles").fetchall()
            for row in rows:
                if row["url"] not in configured:
                    conn.execute("UPDATE profiles SET enabled=0,updated_at=? WHERE id=?", (now, row["id"]))
            conn.execute("UPDATE profiles SET sort_order=id WHERE sort_order IS NULL")

    def reorder_profiles(self, profile_ids: list[int]) -> None:
        with self.connect() as conn:
            current = [int(row[0]) for row in conn.execute("SELECT id FROM profiles")]
            if len(profile_ids) != len(current) or len(set(profile_ids)) != len(profile_ids) or set(profile_ids) != set(current):
                raise ValueError("profile order must contain every profile exactly once")
            conn.executemany(
                "UPDATE profiles SET sort_order=? WHERE id=?",
                ((position, profile_id) for position, profile_id in enumerate(profile_ids)),
            )

    def queue_profile_visits(self, profile_ids: list[int]) -> int:
        now = utcnow()
        queued = 0
        with self.connect() as conn:
            for profile_id in dict.fromkeys(profile_ids):
                pending = conn.execute(
                    "SELECT id FROM jobs WHERE profile_id=? AND job_type='visit' AND status='pending' ORDER BY id LIMIT 1",
                    (profile_id,),
                ).fetchone()
                if pending:
                    conn.execute("UPDATE jobs SET priority=0,available_at=? WHERE id=?", (now, pending["id"]))
                else:
                    conn.execute(
                        "INSERT INTO jobs(profile_id,job_type,priority,payload_json,available_at,created_at) VALUES(?,'visit',0,'{}',?,?)",
                        (profile_id, now, now),
                    )
                queued += 1
        return queued

    def rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def row(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.connect() as conn:
            value = conn.execute(sql, params).fetchone()
            return dict(value) if value else None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        with self.connect() as conn:
            cur = conn.execute(sql, params)
            return int(cur.lastrowid or 0)

    def add_event(self, event_key: str, event_type: str, payload: dict[str, Any], profile_id: int | None = None, entity_id: int | None = None, notify: bool = True, coalesce: bool = False, coalesce_minutes: int = 15) -> int | None:
        now = utcnow()
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO events(event_key,profile_id,entity_id,event_type,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                (event_key, profile_id, entity_id, event_type, json.dumps(payload, ensure_ascii=False), now),
            )
            if not cur.rowcount:
                return None
            event_id = int(cur.lastrowid)
            if notify and coalesce and entity_id is not None:
                ready_at = (datetime.now(UTC) + timedelta(minutes=coalesce_minutes)).isoformat()
                group = conn.execute("SELECT * FROM notification_groups WHERE entity_id=? AND status='pending' AND ready_at>? ORDER BY id DESC LIMIT 1", (entity_id, now)).fetchone()
                if group:
                    group_id = int(group["id"])
                    grouped = json.loads(group["payload_json"])
                    grouped.setdefault("items", []).append(payload)
                    conn.execute("UPDATE notification_groups SET payload_json=? WHERE id=?", (json.dumps(grouped, ensure_ascii=False), group_id))
                else:
                    grouped = {"items": [payload]}
                    cur_group = conn.execute("INSERT INTO notification_groups(profile_id,entity_id,payload_json,created_at,ready_at) VALUES(?,?,?,?,?)", (profile_id, entity_id, json.dumps(grouped, ensure_ascii=False), now, ready_at))
                    group_id = int(cur_group.lastrowid)
                    conn.execute("INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at,group_id) VALUES(?,?,?,?,?,?)", (event_id, "summary", json.dumps({"group_id": group_id}, ensure_ascii=False), ready_at, now, group_id))
                conn.execute("UPDATE events SET notification_group_id=? WHERE id=?", (group_id, event_id))
            elif notify:
                conn.execute(
                    "INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at) VALUES(?,?,?,?,?)",
                    (event_id, "text", json.dumps(payload, ensure_ascii=False), now, now),
                )
            return event_id

    def queue_group_media(self, event_id: int, sha256: str, payload: dict[str, Any]) -> bool:
        """Queue one actual file globally; URL/name changes cannot bypass this."""
        now = utcnow()
        with self.connect() as conn:
            event = conn.execute("SELECT notification_group_id FROM events WHERE id=?", (event_id,)).fetchone()
            if not event or not event["notification_group_id"]:
                return False
            exists = conn.execute("SELECT id FROM outbox WHERE kind='media' AND media_sha256=? LIMIT 1", (sha256,)).fetchone()
            if exists:
                return False
            group = conn.execute("SELECT ready_at FROM notification_groups WHERE id=?", (event["notification_group_id"],)).fetchone()
            conn.execute("INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at,group_id,media_sha256) VALUES(?,?,?,?,?,?,?)", (event_id, "media", json.dumps(payload, ensure_ascii=False), group["ready_at"], now, event["notification_group_id"], sha256))
            return True

    def bind_media_notification(self, event_id: int, sha256: str, perceptual_hash: str | None = None) -> None:
        """Upgrade media rows produced by older ingestion code into a group."""
        with self.connect() as conn:
            event = conn.execute("SELECT notification_group_id FROM events WHERE id=?", (event_id,)).fetchone()
            if not event or not event["notification_group_id"]:
                return
            group_id = int(event["notification_group_id"])
            group = conn.execute("SELECT ready_at FROM notification_groups WHERE id=?", (group_id,)).fetchone()
            conn.execute("UPDATE outbox SET group_id=?,media_sha256=?,media_perceptual_hash=?,next_attempt_at=? WHERE event_id=? AND kind='media' AND group_id IS NULL", (group_id, sha256, perceptual_hash, group["ready_at"], event_id))
            rows = conn.execute("SELECT id,media_sha256,media_perceptual_hash FROM outbox WHERE kind='media' AND status IN ('pending','sent','failed','cancelled') ORDER BY id").fetchall()
            first_id: int | None = None
            for candidate in rows:
                same_file = candidate["media_sha256"] == sha256
                same_image = bool(perceptual_hash and candidate["media_perceptual_hash"] and _hash_distance(perceptual_hash, candidate["media_perceptual_hash"]) <= 6)
                if not same_file and not same_image:
                    continue
                if first_id is None:
                    first_id = int(candidate["id"])
                    continue
                duplicate = candidate
                conn.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='duplicate media content' WHERE id=? AND status='pending'", (utcnow(), duplicate["id"]))

    def establish_notification_baseline(self) -> dict[str, int]:
        """Cancel legacy backlog and silently accept the first post-upgrade scan."""
        now = utcnow()
        with self.connect() as conn:
            pending = conn.execute("SELECT COUNT(*) FROM outbox WHERE status='pending'").fetchone()[0]
            entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            conn.execute("UPDATE entities SET notification_hash=NULL")
            conn.execute("UPDATE notification_groups SET status='cancelled',cancelled_at=? WHERE status='pending'", (now,))
            conn.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='notification baseline reset' WHERE status='pending'", (now,))
            return {"entities": int(entities), "cancelled_outbox": int(pending)}

    def start_maintenance_run(self, task_name: str) -> int:
        return self.execute("INSERT INTO maintenance_runs(task_name,status,started_at) VALUES(?,'running',?)", (task_name, utcnow()))

    def finish_maintenance_run(self, run_id: int, summary: dict[str, Any], error: str | None = None) -> None:
        self.execute("UPDATE maintenance_runs SET status=?,summary_json=?,error=?,finished_at=? WHERE id=?", ("failed" if error else "done", json.dumps(summary, ensure_ascii=False), error, utcnow(), run_id))

    def usage_total(self, month: str) -> float:
        row = self.row("SELECT COALESCE(SUM(estimated_usd),0) total FROM usage WHERE month=?", (month,))
        return float(row["total"] if row else 0)

    def add_usage(self, month: str, category: str, results: int, estimated_usd: float) -> None:
        now = utcnow()
        self.execute(
            """INSERT INTO usage(month,category,estimated_usd,results,updated_at) VALUES(?,?,?,?,?)
            ON CONFLICT(month,category) DO UPDATE SET estimated_usd=estimated_usd+excluded.estimated_usd,
            results=results+excluded.results,updated_at=excluded.updated_at""",
            (month, category, estimated_usd, results, now),
        )

    def migration_applied(self, name: str) -> bool:
        return self.row("SELECT name FROM schema_migrations WHERE name=?", (name,)) is not None

    def mark_migration(self, name: str, details: dict[str, Any] | None = None) -> None:
        self.execute(
            "INSERT OR REPLACE INTO schema_migrations(name,applied_at,details_json) VALUES(?,?,?)",
            (name, utcnow(), json.dumps(details or {}, ensure_ascii=False)),
        )

    def start_actor_run(self, profile_id: int | None, category: str, actor_id: str, input_variant: str, payload: dict[str, Any]) -> int:
        def redact(value: Any) -> Any:
            if isinstance(value, dict):
                return {key: ("***" if any(secret in key.lower() for secret in ("token", "cookie", "password", "secret")) else redact(item)) for key, item in value.items()}
            if isinstance(value, list):
                return [redact(item) for item in value]
            return value

        return self.execute(
            """INSERT INTO actor_runs(profile_id,category,actor_id,input_variant,input_json,status,started_at)
            VALUES(?,?,?,?,?,'running',?)""",
            (profile_id, category, actor_id, input_variant, json.dumps(redact(payload), ensure_ascii=False), utcnow()),
        )

    def finish_actor_run(
        self,
        diagnostic_id: int,
        *,
        status: str,
        run_id: str = "",
        result_count: int = 0,
        charged_usd: float = 0,
        summary: dict[str, Any] | None = None,
        error: str | None = None,
        samples: list[dict[str, Any]] | None = None,
    ) -> None:
        self.execute(
            """UPDATE actor_runs SET status=?,run_id=?,result_count=?,charged_usd=?,summary_json=?,
            samples_json=?,error=?,finished_at=? WHERE id=?""",
            (
                status,
                run_id,
                result_count,
                charged_usd,
                json.dumps(summary, ensure_ascii=False) if summary is not None else None,
                json.dumps((samples or [])[:20], ensure_ascii=False) if samples is not None else None,
                error[:4000] if error else None,
                utcnow(),
                diagnostic_id,
            ),
        )
