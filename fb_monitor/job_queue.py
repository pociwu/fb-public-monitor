"""Read-only queue presentation; eligibility still requires runtime guards."""
from __future__ import annotations

import sqlite3


# One bound parameter: the snapshot time. Shared by counts and list filters
# so a dashboard number and its link describe exactly the same population.
QUEUE_STATE_SQL = """CASE
    WHEN j.status='pending' THEN CASE
        WHEN julianday(j.available_at)<=julianday(?) THEN 'ready'
        WHEN TRIM(COALESCE(j.error,''))<>'' OR j.job_type='profile_browser_fallback' THEN 'limited'
        ELSE 'scheduled' END
    ELSE j.status END"""


def queue_counts(connection: sqlite3.Connection, now: str) -> dict[str, int]:
    counts = {"running": 0, "ready": 0, "limited": 0, "scheduled": 0}
    rows = connection.execute(
        f"""SELECT {QUEUE_STATE_SQL} queue_state,COUNT(*) n FROM jobs j
        WHERE j.status IN ('pending','running') GROUP BY queue_state""", (now,),
    )
    for state, count in rows:
        counts[state] = count
    counts["count"] = sum(counts.values())
    return counts
