import json
import sys
from pathlib import Path

import pytest

from fb_monitor.cli import main
from fb_monitor.config import load_settings
from fb_monitor.db import Database


def _write_cli_config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {(tmp_path / 'data').as_posix()}
""",
        encoding="utf-8",
    )
    return config


def _prepare_source_reconcile_fixture(db: Database, window: str) -> dict:
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )
    batch, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=None,
        provider="apify",
        actor_id="example/posts",
        intent="initial_public_capture",
        observation_window=window,
        normalized_input={"maxPostsPerProfile": 10},
    )
    db.transition_paid_source_batch(batch["id"], "launching")
    return db.transition_paid_source_batch(batch["id"], "needs_reconcile")


def _prepare_contract_reconcile_fixture(db: Database, *, expired: bool) -> tuple[dict, int]:
    grant = db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    job_id, _, allocation = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/posts",
        schema_fingerprint="fp",
        fixture_ack=True,
    )
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="fp",
        status="pending",
        evidence={"test_generation": allocation["test_generation"]},
    )
    run, _ = db.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"page": 1}
    )
    db.execute(
        "UPDATE contract_runs SET status='needs_reconcile' WHERE id=?", (run["id"],)
    )
    db.execute("UPDATE jobs SET status='failed' WHERE id=?", (job_id,))
    if expired:
        db.execute(
            """UPDATE contract_test_grants SET status='expired',
            expires_at='2000-01-01T00:00:00+00:00' WHERE id=?""",
            (grant["id"],),
        )
    return run, job_id


def test_status_uses_capture_v2_schema_columns(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {(tmp_path / 'data').as_posix()}
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["fb-monitor", "--config", str(config), "status"],
    )

    main()

    document = json.loads(capsys.readouterr().out)
    assert document["capture_v2"]["contracts"] == []
    assert document["capture_v2"]["epochs"] == []
    assert document["capture_v2"]["coverage"] == []
    assert document["capture_v2"]["recent_paid_batches"] == []
    assert document["capture_v2"]["recent_paid_access_probes"] == []
    assert document["capture_v2"]["recent_paid_photo_batches"] == []


def test_reconcile_access_probe_cli_attaches_existing_run(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {(tmp_path / 'data').as_posix()}
""",
        encoding="utf-8",
    )
    settings = load_settings(config)
    db = Database(settings.db_path)
    db.sync_profiles(settings.profiles)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="test/posts-v2",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )
    batch, _ = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window="window-1",
        normalized_input={"maxPostsPerProfile": 1},
        max_charge_usd=0.01,
    )
    db.transition_paid_access_probe_batch(batch["id"], "launching")
    db.transition_paid_access_probe_batch(batch["id"], "needs_reconcile")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fb-monitor",
            "--config",
            str(config),
            "reconcile-access-probe",
            str(batch["id"]),
            "--run-id",
            "existing-run",
            "--dataset-id",
            "existing-dataset",
        ],
    )

    main()

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "run_started"
    assert output["run_id"] == "existing-run"
    stored = db.row("SELECT * FROM paid_access_probe_batches WHERE id=?", (batch["id"],))
    assert stored["status"] == "run_started"
    assert stored["dataset_id"] == "existing-dataset"


def test_reconcile_photo_batch_cli_attaches_existing_run_and_requeues(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {(tmp_path / 'data').as_posix()}
""",
        encoding="utf-8",
    )
    settings = load_settings(config)
    db = Database(settings.db_path)
    db.sync_profiles(settings.profiles)
    capture_id = db.execute(
        """INSERT INTO profile_photo_captures(
          profile_id,generation,status,created_at,updated_at
        ) VALUES(1,1,'source_limited','now','now')"""
    )
    batch, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="test/photos",
        normalized_input={"urls": ["https://facebook.com/100"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(batch["id"], "launching")
    db.transition_paid_photo_batch(batch["id"], "needs_reconcile")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fb-monitor",
            "--config",
            str(config),
            "reconcile-photo-batch",
            str(batch["id"]),
            "--run-id",
            "existing-photo-run",
            "--dataset-id",
            "existing-photo-dataset",
        ],
    )

    main()

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "run_started"
    stored = db.row("SELECT * FROM paid_photo_batches WHERE id=?", (batch["id"],))
    assert stored["run_id"] == "existing-photo-run"
    assert stored["dataset_id"] == "existing-photo-dataset"
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )["count"] == 1


def test_reconcile_photo_batch_cli_abandons_import_failed_and_requeues(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {(tmp_path / 'data').as_posix()}
""",
        encoding="utf-8",
    )
    settings = load_settings(config)
    db = Database(settings.db_path)
    db.sync_profiles(settings.profiles)
    browser_item = {
        "id": "browser-kept",
        "url": "https://www.facebook.com/photo.php?fbid=70001&id=100",
        "image": "https://scontent.example.fbcdn.net/v/browser-kept.jpg",
    }
    checkpoint = {
        "source_policy": "apify",
        "capture_iteration": 7,
        "collected_items": [browser_item],
        "actor_retry_nonce": 4,
        "actor_next_cursor": "stale-page-9",
        "actor_seen_cursors": ["stale-page-8"],
        "actor_collected_items": [{"id": "stale-actor-photo"}],
        "actor_processed_item_ids": ["stale-actor-photo"],
        "actor_run_id": "stale-run",
        "actor_batch_id": 999,
        "actor_evidence": {"coverage": "old-schema"},
        "actor_inventory_completed": False,
        "actor_declared_total": 999,
        "actor_discovered_urls": ["https://facebook.com/old-photo"],
        "actor_discovered_count": 1,
        "actor_pending_media_external_ids": ["stale-actor-photo"],
        "actor_exhausted_media_external_ids": ["stale-exhausted-photo"],
        "actor_media_retry_attempt": 5,
        "actor_media_retry_until": "2026-10-01T00:00:00+00:00",
    }
    capture_id = db.execute(
        """INSERT INTO profile_photo_captures(
          profile_id,generation,status,checkpoint_json,created_at,updated_at
        ) VALUES(1,1,'source_limited',?,'now','now')""",
        (json.dumps(checkpoint),),
    )
    batch, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="old/photos",
        normalized_input={"urls": ["https://facebook.com/100"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(batch["id"], "launching")
    db.transition_paid_photo_batch(
        batch["id"], "run_started", run_id="old-run"
    )
    db.transition_paid_photo_batch(
        batch["id"], "raw_saved", raw_path="/data/old-photo.json.gz"
    )
    db.transition_paid_photo_batch(
        batch["id"], "import_failed", error="old schema"
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fb-monitor",
            "--config",
            str(config),
            "reconcile-photo-batch",
            str(batch["id"]),
            "--abandon-import-failed",
        ],
    )

    main()

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "failed"
    assert "安全重試" in output["message"]
    stored = db.row("SELECT * FROM paid_photo_batches WHERE id=?", (batch["id"],))
    assert stored["status"] == "failed"
    capture = db.row(
        "SELECT status,checkpoint_json FROM profile_photo_captures WHERE id=?",
        (capture_id,),
    )
    assert capture["status"] == "in_progress"
    repaired_checkpoint = json.loads(capture["checkpoint_json"])
    assert repaired_checkpoint["actor_retry_nonce"] == 5
    assert repaired_checkpoint["collected_items"] == [browser_item]
    assert repaired_checkpoint["capture_iteration"] == 7
    cleared_actor_keys = {
        "actor_next_cursor",
        "actor_seen_cursors",
        "actor_collected_items",
        "actor_processed_item_ids",
        "actor_run_id",
        "actor_batch_id",
        "actor_evidence",
        "actor_inventory_completed",
        "actor_declared_total",
        "actor_discovered_urls",
        "actor_discovered_count",
        "actor_pending_media_external_ids",
        "actor_exhausted_media_external_ids",
        "actor_media_retry_attempt",
        "actor_media_retry_until",
    }
    assert cleared_actor_keys.isdisjoint(repaired_checkpoint)
    queued = db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )
    assert queued["count"] == 1
    retry_job = db.row(
        """SELECT payload_json FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'
        ORDER BY id DESC LIMIT 1"""
    )
    retry_payload = json.loads(retry_job["payload_json"])
    assert retry_payload["iteration"] == 7
    assert retry_payload.get("local_actor_drain") is not True

    # The DB method owns the whole mutation. Replaying the same CLI command
    # must fail closed without incrementing the nonce or creating another job.
    with pytest.raises(SystemExit, match="expected import_failed"):
        main()
    replay_capture = db.row(
        "SELECT checkpoint_json FROM profile_photo_captures WHERE id=?",
        (capture_id,),
    )
    assert json.loads(replay_capture["checkpoint_json"])["actor_retry_nonce"] == 5
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )["count"] == 1


@pytest.mark.parametrize("resolution", ["attach", "confirm_not_launched"])
def test_reconcile_source_batch_cli_handles_both_resolutions(
    tmp_path: Path, monkeypatch, capsys, resolution: str
) -> None:
    config = _write_cli_config(tmp_path)
    settings = load_settings(config)
    db = Database(settings.db_path)
    db.sync_profiles(settings.profiles)
    batch = _prepare_source_reconcile_fixture(db, f"window-{resolution}")
    argv = [
        "fb-monitor",
        "--config",
        str(config),
        "reconcile-source-batch",
        str(batch["id"]),
    ]
    if resolution == "attach":
        argv.extend(
            [
                "--run-id",
                "existing-source-run",
                "--dataset-id",
                "existing-source-dataset",
                "--key-value-store-id",
                "existing-source-store",
            ]
        )
    else:
        argv.append("--confirm-not-launched")
    monkeypatch.setattr(sys, "argv", argv)

    main()

    output = json.loads(capsys.readouterr().out)
    original = db.row(
        "SELECT * FROM paid_source_batches WHERE id=?", (batch["id"],)
    )
    queued = db.row(
        """SELECT * FROM jobs WHERE job_type='capture_posts_v2'
        AND batch_id=? AND status='pending' ORDER BY id DESC LIMIT 1""",
        (output["resume_batch_id"],),
    )
    assert queued is not None
    if resolution == "attach":
        assert output["status"] == "run_started"
        assert output["resume_batch_id"] == batch["id"]
        assert original["run_id"] == "existing-source-run"
        assert original["dataset_id"] == "existing-source-dataset"
        assert original["key_value_store_id"] == "existing-source-store"
    else:
        assert output["status"] == "failed"
        assert output["resume_batch_id"] != batch["id"]
        replacement = db.row(
            "SELECT * FROM paid_source_batches WHERE id=?",
            (output["resume_batch_id"],),
        )
        assert original["status"] == "failed"
        assert replacement["status"] == "prepared"
        assert replacement["request_hash"] != batch["request_hash"]


@pytest.mark.parametrize("resolution", ["attach", "confirm_not_launched"])
def test_reconcile_contract_run_cli_handles_expired_grant_without_rebuy(
    tmp_path: Path, monkeypatch, capsys, resolution: str
) -> None:
    config = _write_cli_config(tmp_path)
    settings = load_settings(config)
    db = Database(settings.db_path)
    db.sync_profiles(settings.profiles)
    run, job_id = _prepare_contract_reconcile_fixture(db, expired=True)
    argv = [
        "fb-monitor",
        "--config",
        str(config),
        "reconcile-contract-run",
        str(run["id"]),
    ]
    if resolution == "attach":
        argv.extend(
            [
                "--run-id",
                "existing-contract-run",
                "--dataset-id",
                "existing-contract-dataset",
            ]
        )
    else:
        argv.append("--confirm-not-launched")
    monkeypatch.setattr(sys, "argv", argv)

    main()

    output = json.loads(capsys.readouterr().out)
    stored = db.row("SELECT * FROM contract_runs WHERE id=?", (run["id"],))
    job = db.row("SELECT * FROM jobs WHERE id=?", (job_id,))
    if resolution == "attach":
        assert output["status"] == "run_started"
        assert output["run_id"] == "existing-contract-run"
        assert output["job_requeued"] is True
        assert stored["dataset_id"] == "existing-contract-dataset"
        assert job["status"] == "pending"
    else:
        assert output["status"] == "failed"
        assert output["replacement_run_row_id"] is None
        assert output["job_requeued"] is False
        assert stored["status"] == "failed"
        assert job["status"] == "failed"
