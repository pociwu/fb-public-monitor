import json
import sys
from pathlib import Path

from fb_monitor.cli import main
from fb_monitor.config import load_settings
from fb_monitor.db import Database


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
