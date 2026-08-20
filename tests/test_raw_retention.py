import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fb_monitor.config import ProfileConfig
from fb_monitor.db import Database
from fb_monitor.raw_retention import cleanup_capture_raw


NOW = datetime(2026, 8, 20, 0, 0, tzinfo=UTC)


def _scope(db: Database, root: Path, *, status: str, completed_at: str | None, suffix: str):
    db.sync_profiles([ProfileConfig(name="FB-100", url="https://facebook.com/100")])
    epoch, _ = db.get_or_create_capture_epoch(1, f"test-{suffix}", status="running")
    if completed_at:
        db.execute(
            "UPDATE capture_epochs SET status=?,is_active=0,completed_at=? WHERE id=?",
            (status, completed_at, epoch["id"]),
        )
    else:
        db.execute(
            "UPDATE capture_epochs SET status=?,is_active=0 WHERE id=?",
            (status, epoch["id"]),
        )
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts"
    )
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id=f"test/actor-{suffix}",
        purpose="posts_backfill",
        schema_fingerprint=f"schema-{suffix}",
        input_mapping_hash=f"mapping-{suffix}",
        status="passed",
    )
    batch, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window=f"window-{suffix}",
        normalized_input={"cursor": suffix},
        request_hash=(suffix * 64)[:64],
    )
    raw = root / "capture-v2" / "raw" / suffix[:2] / f"{suffix}.json.gz"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(b"raw-evidence")
    db.execute(
        """UPDATE paid_source_batches SET status='committed',raw_path=?,raw_saved_at=?,
        committed_at=? WHERE id=?""",
        (
            str(raw),
            (NOW - timedelta(days=100)).isoformat(),
            (NOW - timedelta(days=100)).isoformat(),
            batch["id"],
        ),
    )
    return batch, raw


def _probe_raw(db: Database, root: Path, *, status: str, suffix: str):
    db.sync_profiles([ProfileConfig(name="FB-100", url="https://facebook.com/100")])
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id=f"test/probe-{suffix}",
        purpose="access_probe",
        schema_fingerprint=f"probe-schema-{suffix}",
        input_mapping_hash=f"probe-mapping-{suffix}",
        status="passed",
    )
    batch, _ = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window=f"probe-window-{suffix}",
        normalized_input={"profile": "100"},
        max_charge_usd=0.01,
        request_hash=(f"probe-{suffix}" * 64)[:64],
    )
    raw = root / "capture-v2" / "raw" / suffix[:2] / f"probe-{suffix}.json.gz"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(b"probe-raw-evidence")
    db.execute(
        """UPDATE paid_access_probe_batches
        SET status=?,raw_path=?,raw_saved_at=?,committed_at=?,updated_at=?
        WHERE id=?""",
        (
            status,
            str(raw),
            (NOW - timedelta(days=100)).isoformat(),
            (NOW - timedelta(days=91)).isoformat() if status == "committed" else None,
            (NOW - timedelta(days=91)).isoformat(),
            batch["id"],
        ),
    )
    return batch, raw


def test_cleanup_deletes_only_committed_raw_for_completed_old_epoch(tmp_path: Path):
    db = Database(tmp_path / "monitor.sqlite3")
    batch, raw = _scope(
        db,
        tmp_path,
        status="complete",
        completed_at=(NOW - timedelta(days=91)).isoformat(),
        suffix="a1",
    )

    result = cleanup_capture_raw(db, tmp_path, now=NOW)

    assert result.deleted == 1
    assert result.deleted_bytes == len(b"raw-evidence")
    assert not raw.exists()
    saved = db.row(
        "SELECT raw_path,raw_sha256 FROM paid_source_batches WHERE id=?", (batch["id"],)
    )
    assert saved["raw_path"] is None


def test_cleanup_retains_incomplete_and_recent_completed_raw(tmp_path: Path):
    db = Database(tmp_path / "monitor.sqlite3")
    _, incomplete = _scope(db, tmp_path, status="manual_paused", completed_at=None, suffix="b2")
    _, recent = _scope(
        db,
        tmp_path,
        status="complete",
        completed_at=(NOW - timedelta(days=89)).isoformat(),
        suffix="c3",
    )

    result = cleanup_capture_raw(db, tmp_path, now=NOW)

    assert result.deleted == 0
    assert incomplete.exists()
    assert recent.exists()


def test_cleanup_rejects_path_outside_raw_root(tmp_path: Path):
    db = Database(tmp_path / "monitor.sqlite3")
    batch, raw = _scope(
        db,
        tmp_path,
        status="complete",
        completed_at=(NOW - timedelta(days=91)).isoformat(),
        suffix="d4",
    )
    outside = tmp_path / "outside.json.gz"
    raw.replace(outside)
    db.execute("UPDATE paid_source_batches SET raw_path=? WHERE id=?", (str(outside), batch["id"]))

    result = cleanup_capture_raw(db, tmp_path, now=NOW)

    assert result.errors == 1
    assert outside.exists()


def test_cleanup_removes_stale_temporary_files(tmp_path: Path):
    db = Database(tmp_path / "monitor.sqlite3")
    root = tmp_path / "capture-v2" / "raw"
    root.mkdir(parents=True)
    old = root / "old.json.gz.tmp"
    old.write_bytes(b"partial")
    timestamp = (NOW - timedelta(hours=25)).timestamp()
    os.utime(old, (timestamp, timestamp))

    result = cleanup_capture_raw(db, tmp_path, now=NOW)

    assert result.temporary_deleted == 1
    assert not old.exists()


def test_cleanup_applies_success_retention_to_committed_access_probe_raw(tmp_path: Path):
    db = Database(tmp_path / "monitor.sqlite3")
    batch, raw = _probe_raw(db, tmp_path, status="committed", suffix="e5")

    result = cleanup_capture_raw(db, tmp_path, now=NOW)

    assert result.deleted == 1
    assert not raw.exists()
    saved = db.row(
        "SELECT raw_path FROM paid_access_probe_batches WHERE id=?", (batch["id"],)
    )
    assert saved["raw_path"] is None


def test_cleanup_retains_unresolved_access_probe_raw(tmp_path: Path):
    db = Database(tmp_path / "monitor.sqlite3")
    batch, raw = _probe_raw(db, tmp_path, status="needs_reconcile", suffix="f6")

    result = cleanup_capture_raw(db, tmp_path, now=NOW)

    assert result.deleted == 0
    assert raw.exists()
    saved = db.row(
        "SELECT raw_path FROM paid_access_probe_batches WHERE id=?", (batch["id"],)
    )
    assert saved["raw_path"] == str(raw)
