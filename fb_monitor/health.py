"""Read-only evidence for stale profile refreshes; never launches providers."""
from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from .timeutil import parse_time
from .job_queue import queue_counts


def collect_health(
    connection: sqlite3.Connection, *, stale_hours: float = 56,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)

    def rows(query: str, params: tuple = ()) -> list[dict[str, Any]]:
        cursor = connection.execute(query, params)
        keys = [column[0] for column in cursor.description]
        return [dict(zip(keys, row)) for row in cursor.fetchall()]

    profiles = rows("""SELECT id,COALESCE(NULLIF(display_name,''),name) name,public_state,
        last_success_at,last_attempt_at,next_visit_at,serp_last_checked_at,
        consecutive_failures,last_error FROM profiles WHERE enabled=1 ORDER BY id""")
    for profile in profiles:
        profile_id = profile["id"]
        last_success = parse_time(profile["last_success_at"])
        age = max(0, (now - last_success).total_seconds() / 3600) if last_success else None
        profile["age_hours"] = round(age, 1) if age is not None else None
        profile["stale"] = age is None or age > stale_hours
        profile["recent_jobs"] = rows("""SELECT id,job_type,status,attempts,available_at,
            started_at,finished_at,error FROM jobs WHERE profile_id=? ORDER BY id DESC LIMIT 5""", (profile_id,))
        profile["pending_refresh"] = next(iter(rows("""SELECT id,job_type,status,available_at,error
            FROM jobs WHERE profile_id=? AND job_type IN ('visit','profile_browser_fallback','browser_visit')
            AND status IN ('pending','running') ORDER BY priority,id LIMIT 1""", (profile_id,))), None)
        profile["serpapi_latest"] = next(iter(rows("""SELECT status,error,attempted_at
            FROM serpapi_profile_attempts WHERE profile_id=? ORDER BY id DESC LIMIT 1""", (profile_id,))), None)
        profile["latest_observation"] = next(iter(rows("""SELECT source,auth_scope,verdict,observed_at
            FROM access_observations WHERE profile_id=? ORDER BY id DESC LIMIT 1""", (profile_id,))), None)
        profile["last_content_at"] = next(iter(rows("""SELECT MAX(v.seen_at) at FROM versions v
            JOIN entities e ON e.id=v.entity_id WHERE e.profile_id=? AND e.kind IN ('post','comment','photo')""", (profile_id,))), {}).get("at")
        events = rows("""SELECT event_type,created_at,payload_json FROM events WHERE profile_id=?
            AND event_type IN ('serpapi_error','serpapi_empty','profile_fallback_error','browser_guard_deferred')
            ORDER BY id DESC LIMIT 3""", (profile_id,))
        for event in events:
            try:
                payload = json.loads(event.pop("payload_json"))
                event["reason"] = str(payload.get("text") or "")[:800]
            except (ValueError, TypeError, AttributeError):
                event["reason"] = "無法解析事件摘要"
        profile["recent_failure_evidence"] = events

    return {
        "observed_at": now.isoformat(), "stale_after_hours": stale_hours,
        "profiles": profiles,
        "queue": queue_counts(connection, now.isoformat()),
        "jobs_by_status": rows("SELECT job_type,status,COUNT(*) count FROM jobs WHERE status IN ('pending','running') GROUP BY job_type,status"),
        "browser_limits": rows("""SELECT browser_identity,scope_type,scope_id,breaker_state,
            breaker_reason,blocked_until,next_allowed_at,daily_date,daily_batches FROM browser_limits"""),
        "serpapi_usage": next(iter(rows("SELECT * FROM serpapi_usage_snapshot")), None),
        "apify_usage": next(iter(rows("SELECT * FROM apify_usage_snapshot")), None),
    }
