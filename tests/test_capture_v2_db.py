import json
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from fb_monitor.db import (
    CAPTURE_V2_SCHEMA_MIGRATION,
    CONTRACT_TEST_GRANT_MIGRATION,
    Database,
    ProviderRunOwnershipConflict,
    canonical_request_hash,
)


def add_profile(db: Database, profile_id: int = 1, *, frozen: int = 0) -> None:
    db.execute(
        """INSERT INTO profiles(id,name,url,apify_frozen,created_at,updated_at)
        VALUES(?,?,?,?,'now','now')""",
        (profile_id, f"FB-{profile_id}", f"https://facebook.com/{profile_id}", frozen),
    )


def add_photo_capture(db: Database, profile_id: int = 1, generation: int = 1) -> int:
    return db.execute(
        """INSERT INTO profile_photo_captures(
          profile_id,generation,status,created_at,updated_at
        ) VALUES(?,?,'pending','now','now')""",
        (profile_id, generation),
    )


def test_paid_photo_batch_is_idempotent_and_reserves_global_budget(tmp_path: Path):
    db = Database(tmp_path / "paid-photo.sqlite3")
    add_profile(db)
    capture_id = add_photo_capture(db)

    first, created = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/profile-photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.25,
    )
    replay, created_again = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/profile-photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.25,
    )

    assert created is True
    assert created_again is False
    assert replay["id"] == first["id"]
    claimed, won = db.claim_paid_photo_batch_launch(
        first["id"],
        global_capacity_usd=0.20,
        minimum_charge_usd=0.0029,
        posts_result_price_usd=0.005,
    )
    assert won is True
    assert claimed["status"] == "launching"
    assert claimed["max_charge_usd"] == pytest.approx(0.20)
    reservations = db.paid_budget_reservations(posts_result_price_usd=0.005)
    assert reservations["photo_unsettled_usd"] == pytest.approx(0.20)
    assert reservations["total_unsettled_usd"] >= 0.20


def test_paid_photo_batch_claim_rejects_insufficient_atomic_budget(tmp_path: Path):
    db = Database(tmp_path / "paid-photo-budget.sqlite3")
    add_profile(db)
    capture_id = add_photo_capture(db)
    batch, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/profile-photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.25,
    )

    denied, claimed = db.claim_paid_photo_batch_launch(
        batch["id"],
        global_capacity_usd=0.001,
        minimum_charge_usd=0.0029,
        posts_result_price_usd=0.005,
    )

    assert claimed is False
    assert denied["status"] == "prepared"
    assert denied["max_charge_usd"] == pytest.approx(0.001)


def test_paid_photo_batch_reconcile_attaches_run_or_closes_unlaunched(tmp_path: Path):
    db = Database(tmp_path / "paid-photo-reconcile.sqlite3")
    add_profile(db)
    first_capture = add_photo_capture(db)
    first, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=first_capture,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(first["id"], "launching")
    db.transition_paid_photo_batch(first["id"], "needs_reconcile")

    attached = db.reconcile_paid_photo_batch(
        first["id"], run_id="existing-run", dataset_id="existing-dataset"
    )

    assert attached["status"] == "run_started"
    assert attached["run_id"] == "existing-run"
    assert attached["dataset_id"] == "existing-dataset"
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'
          AND dedupe_key=?""",
        (f"capture-account-photos:{first_capture}:reconcile:{first['id']}",),
    )["count"] == 1

    second_capture = add_photo_capture(db, generation=2)
    second, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=second_capture,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"], "page": 2},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(second["id"], "launching")
    db.transition_paid_photo_batch(second["id"], "needs_reconcile")

    closed = db.reconcile_paid_photo_batch(
        second["id"], confirm_not_launched=True
    )

    assert closed["status"] == "failed"
    assert "not launched" in closed["error"]
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )["count"] == 2

    third_capture = add_photo_capture(db, generation=3)
    third, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=third_capture,
        actor_id="example/photos-v2",
        normalized_input={"urls": ["https://facebook.com/1"], "schema": 2},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(third["id"], "launching")
    db.transition_paid_photo_batch(
        third["id"], "run_started", run_id="bad-schema-run"
    )
    db.transition_paid_photo_batch(
        third["id"],
        "raw_saved",
        raw_path="/data/photo-capture/raw/bad.json.gz",
    )
    db.transition_paid_photo_batch(
        third["id"], "import_failed", error="schema mismatch"
    )

    abandoned = db.reconcile_paid_photo_batch(
        third["id"], abandon_import_failed=True
    )

    assert abandoned["status"] == "failed"
    assert "abandoned import_failed" in abandoned["error"]


def test_photo_reconcile_batch_checkpoint_and_resume_job_are_atomic(tmp_path: Path):
    db = Database(tmp_path / "paid-photo-reconcile-atomic.sqlite3")
    add_profile(db)
    capture_id = add_photo_capture(db)
    checkpoint = {"actor_retry_nonce": 4, "capture_iteration": 2}
    db.execute(
        "UPDATE profile_photo_captures SET checkpoint_json=? WHERE id=?",
        (json.dumps(checkpoint), capture_id),
    )
    batch, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(batch["id"], "launching")
    db.transition_paid_photo_batch(batch["id"], "needs_reconcile")
    db.execute(
        """CREATE TRIGGER reject_photo_reconcile_job
        BEFORE INSERT ON jobs
        WHEN NEW.dedupe_key LIKE 'capture-account-photos:%:reconcile:%'
        BEGIN SELECT RAISE(ABORT, 'simulated resume job failure'); END"""
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated resume job failure"):
        db.reconcile_paid_photo_batch(batch["id"], confirm_not_launched=True)

    stored = db.row(
        "SELECT status FROM paid_photo_batches WHERE id=?", (batch["id"],)
    )
    capture = db.row(
        "SELECT status,checkpoint_json FROM profile_photo_captures WHERE id=?",
        (capture_id,),
    )
    assert stored["status"] == "needs_reconcile"
    assert capture["status"] == "pending"
    assert json.loads(capture["checkpoint_json"])["actor_retry_nonce"] == 4
    assert db.row("SELECT COUNT(*) count FROM jobs")["count"] == 0

    db.execute("DROP TRIGGER reject_photo_reconcile_job")
    reconciled = db.reconcile_paid_photo_batch(
        batch["id"], confirm_not_launched=True
    )

    assert reconciled["status"] == "failed"
    capture = db.row(
        "SELECT status,checkpoint_json FROM profile_photo_captures WHERE id=?",
        (capture_id,),
    )
    assert capture["status"] == "in_progress"
    assert json.loads(capture["checkpoint_json"])["actor_retry_nonce"] == 5
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_profile_photos' AND status='pending'"""
    )["count"] == 1


def test_paid_source_batch_reconcile_attaches_run_or_rotates_unlaunched_identity(
    tmp_path: Path,
):
    db = Database(tmp_path / "paid-source-reconcile.sqlite3")
    add_profile(db)
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )

    def prepare(window: str) -> dict:
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

    first = prepare("window-1")
    attached = db.reconcile_paid_source_batch(
        first["id"],
        run_id="existing-source-run",
        dataset_id="existing-source-dataset",
        key_value_store_id="existing-source-store",
    )

    assert attached["status"] == "run_started"
    assert attached["run_id"] == "existing-source-run"
    assert attached["dataset_id"] == "existing-source-dataset"
    assert attached["key_value_store_id"] == "existing-source-store"
    assert attached["resume_batch_id"] == first["id"]
    assert attached["replacement_batch_id"] is None
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_posts_v2' AND status='pending'
          AND batch_id=?""",
        (first["id"],),
    )["count"] == 1

    second = prepare("window-2")
    closed = db.reconcile_paid_source_batch(
        second["id"], confirm_not_launched=True
    )
    replacement = db.row(
        "SELECT * FROM paid_source_batches WHERE id=?",
        (closed["replacement_batch_id"],),
    )

    assert closed["status"] == "failed"
    assert "not launched" in closed["error"]
    assert closed["resume_batch_id"] == replacement["id"]
    assert replacement["status"] == "prepared"
    assert replacement["request_hash"] != second["request_hash"]
    assert replacement["normalized_input_json"] == second["normalized_input_json"]
    assert replacement["epoch_id"] == second["epoch_id"]
    assert db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type='capture_posts_v2' AND status='pending'"""
    )["count"] == 2


def test_provider_run_cannot_be_reconciled_to_two_source_batches(tmp_path: Path):
    db = Database(tmp_path / "provider-run-same-ledger.sqlite3")
    add_profile(db)
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )

    def ambiguous(window: str) -> dict:
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

    first = ambiguous("window-one")
    second = ambiguous("window-two")
    db.reconcile_paid_source_batch(
        first["id"], run_id="shared-provider-run", dataset_id="shared-dataset"
    )

    with pytest.raises(RuntimeError, match="already belongs to paid_source_batch"):
        db.reconcile_paid_source_batch(
            second["id"], run_id="shared-provider-run", dataset_id="shared-dataset"
        )

    assert db.row(
        "SELECT status,run_id FROM paid_source_batches WHERE id=?", (second["id"],)
    ) == {"status": "needs_reconcile", "run_id": None}
    owner = db.row(
        "SELECT owner_type,owner_id FROM provider_run_registry WHERE provider=? AND run_id=?",
        ("apify", "shared-provider-run"),
    )
    assert owner == {"owner_type": "paid_source_batch", "owner_id": first["id"]}


def test_provider_run_cannot_cross_paid_ledger_owners(tmp_path: Path):
    db = Database(tmp_path / "provider-run-cross-ledger.sqlite3")
    add_profile(db)
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )
    source, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=None,
        provider="apify",
        actor_id="example/posts",
        intent="initial_public_capture",
        observation_window="source-window",
        normalized_input={"maxPostsPerProfile": 10},
    )
    db.transition_paid_source_batch(source["id"], "launching")
    db.transition_paid_source_batch(
        source["id"],
        "run_started",
        run_id="cross-ledger-run",
        dataset_id="cross-ledger-dataset",
    )

    capture_id = add_photo_capture(db)
    photo, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(photo["id"], "launching")
    db.transition_paid_photo_batch(photo["id"], "needs_reconcile")

    with pytest.raises(RuntimeError, match="already belongs to paid_source_batch"):
        db.reconcile_paid_photo_batch(
            photo["id"],
            run_id="cross-ledger-run",
            dataset_id="cross-ledger-dataset",
        )

    assert db.row(
        "SELECT status,run_id FROM paid_photo_batches WHERE id=?", (photo["id"],)
    ) == {"status": "needs_reconcile", "run_id": None}


@pytest.mark.parametrize("kind", ["source", "photo", "access", "contract"])
def test_started_provider_run_conflict_quarantines_every_paid_owner(
    tmp_path: Path, kind: str
):
    db = Database(tmp_path / f"provider-run-conflict-{kind}.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
    )
    db.execute(
        """INSERT INTO provider_run_registry(
          provider,run_id,owner_type,owner_id,actor_id,request_fingerprint,
          metadata_json,created_at,updated_at
        ) VALUES('apify','already-owned-run','paid_source_batch',999,
                 'other/actor','other-request','{}','now','now')"""
    )
    diagnostic_id = 0

    if kind == "source":
        epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
        coverage = db.upsert_coverage_stream(
            epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
        )
        record, _ = db.prepare_paid_source_batch(
            profile_id=1,
            epoch_id=epoch["id"],
            coverage_stream_id=coverage["id"],
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            intent="initial_public_capture",
            observation_window="conflict-window",
            normalized_input={"maxPostsPerProfile": 1},
        )
        db.transition_paid_source_batch(record["id"], "launching")
        attach = lambda: db.transition_paid_source_batch(
            record["id"], "run_started", run_id="already-owned-run"
        )
        reconcile = db.reconcile_paid_source_batch
        table = "paid_source_batches"
        owner_type = "paid_source_batch"
    elif kind == "photo":
        capture_id = add_photo_capture(db)
        record, _ = db.prepare_paid_photo_batch(
            profile_id=1,
            photo_capture_id=capture_id,
            actor_id="example/photos",
            normalized_input={"urls": ["https://facebook.com/1"]},
            max_charge_usd=0.1,
        )
        diagnostic_id = db.start_actor_run(
            1, "profile_photos", "example/photos", "test", {}
        )
        db.transition_paid_photo_batch(
            record["id"], "launching", actor_run_id=diagnostic_id
        )
        attach = lambda: db.transition_paid_photo_batch(
            record["id"], "run_started", run_id="already-owned-run"
        )
        reconcile = db.reconcile_paid_photo_batch
        table = "paid_photo_batches"
        owner_type = "paid_photo_batch"
    elif kind == "access":
        record, _ = db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window="conflict-window",
            normalized_input={"maxPostsPerProfile": 1},
            max_charge_usd=0.01,
        )
        diagnostic_id = db.start_actor_run(
            1, "access_probe_v2", contract["actor_id"], "test", {}
        )
        db.transition_paid_access_probe_batch(
            record["id"], "launching", actor_run_id=diagnostic_id
        )
        attach = lambda: db.transition_paid_access_probe_batch(
            record["id"], "run_started", run_id="already-owned-run"
        )
        reconcile = db.reconcile_paid_access_probe_batch
        table = "paid_access_probe_batches"
        owner_type = "paid_access_probe_batch"
    else:
        record, _ = db.record_contract_run(
            contract["id"],
            test_case="page_1",
            normalized_input={"maxPostsPerProfile": 1},
        )
        db.execute(
            "UPDATE contract_runs SET status='launching',lease_owner='worker' WHERE id=?",
            (record["id"],),
        )
        attach = lambda: db.attach_contract_run_provider_identity(
            record["id"],
            run_id="already-owned-run",
            dataset_id="dataset",
            lease_owner="worker",
        )
        reconcile = db.reconcile_contract_run
        table = "contract_runs"
        owner_type = "contract_run"

    with pytest.raises(ProviderRunOwnershipConflict) as caught:
        attach()
    db.record_provider_run_ownership_conflict(
        caught.value,
        actor_id=(
            str(record.get("actor_id") or "")
            if kind != "contract"
            else str(contract["actor_id"])
        ),
        request_fingerprint=str(record["request_hash"]),
        dataset_id="dataset",
    )

    assert db.row(
        f"SELECT status,run_id FROM {table} WHERE id=?", (record["id"],)
    ) == {"status": "needs_reconcile", "run_id": None}
    conflict = db.row(
        """SELECT attempted_owner_type,attempted_owner_id,run_id
        FROM provider_run_conflicts WHERE attempted_owner_type=? AND attempted_owner_id=?""",
        (owner_type, record["id"]),
    )
    assert conflict == {
        "attempted_owner_type": owner_type,
        "attempted_owner_id": record["id"],
        "run_id": "already-owned-run",
    }
    if diagnostic_id:
        assert db.row(
            "SELECT status,run_id FROM actor_runs WHERE id=?", (diagnostic_id,)
        ) == {"status": "needs_reconcile", "run_id": "already-owned-run"}
    with pytest.raises(RuntimeError, match="provider"):
        reconcile(record["id"], confirm_not_launched=True)


def test_provider_run_registry_backfill_is_idempotent(tmp_path: Path):
    path = tmp_path / "provider-run-backfill.sqlite3"
    db = Database(path)
    add_profile(db)
    capture_id = add_photo_capture(db)
    photo, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(photo["id"], "launching")
    db.transition_paid_photo_batch(
        photo["id"], "run_started", run_id="persisted-photo-run"
    )
    # Simulate a paid row written before this registry existed.
    db.execute(
        "DELETE FROM provider_run_registry WHERE owner_type=? AND owner_id=?",
        ("paid_photo_batch", photo["id"]),
    )

    reopened = Database(path)
    owner = reopened.row(
        "SELECT owner_type,owner_id FROM provider_run_registry WHERE provider=? AND run_id=?",
        ("apify", "persisted-photo-run"),
    )
    assert owner == {"owner_type": "paid_photo_batch", "owner_id": photo["id"]}
    assert Database(path).row("SELECT COUNT(*) count FROM provider_run_registry")[
        "count"
    ] == 1


def test_paid_transition_cannot_clear_registered_run_identity(tmp_path: Path):
    db = Database(tmp_path / "provider-run-cannot-clear.sqlite3")
    add_profile(db)
    capture_id = add_photo_capture(db)
    photo, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.1,
    )
    db.transition_paid_photo_batch(photo["id"], "launching")
    db.transition_paid_photo_batch(
        photo["id"], "run_started", run_id="durable-photo-run"
    )

    with pytest.raises(RuntimeError, match="cannot be cleared or replaced"):
        db.transition_paid_photo_batch(photo["id"], "raw_saved", run_id=None)

    stored = db.row(
        "SELECT status,run_id FROM paid_photo_batches WHERE id=?", (photo["id"],)
    )
    assert stored == {"status": "run_started", "run_id": "durable-photo-run"}


@pytest.mark.parametrize("kind", ["source", "photo", "access", "contract"])
def test_reconcile_attach_preserves_existing_provider_identity(
    tmp_path: Path, kind: str
):
    db = Database(tmp_path / f"reconcile-provider-identity-{kind}.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )
    has_store = kind != "contract"

    if kind == "source":
        epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
        coverage = db.upsert_coverage_stream(
            epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
        )
        record, _ = db.prepare_paid_source_batch(
            profile_id=1,
            epoch_id=epoch["id"],
            coverage_stream_id=coverage["id"],
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            intent="initial_public_capture",
            observation_window="identity-window",
            normalized_input={"maxPostsPerProfile": 1},
        )
        table = "paid_source_batches"
        reconcile = db.reconcile_paid_source_batch
    elif kind == "photo":
        capture_id = add_photo_capture(db)
        record, _ = db.prepare_paid_photo_batch(
            profile_id=1,
            photo_capture_id=capture_id,
            actor_id="example/photos",
            normalized_input={"urls": ["https://facebook.com/1"]},
            max_charge_usd=0.1,
        )
        table = "paid_photo_batches"
        reconcile = db.reconcile_paid_photo_batch
    elif kind == "access":
        record, _ = db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window="identity-window",
            normalized_input={"maxPostsPerProfile": 1},
            max_charge_usd=0.01,
        )
        table = "paid_access_probe_batches"
        reconcile = db.reconcile_paid_access_probe_batch
    else:
        record, _ = db.record_contract_run(
            contract["id"],
            test_case="page_1",
            normalized_input={"maxPostsPerProfile": 1},
        )
        table = "contract_runs"
        reconcile = db.reconcile_contract_run

    store_assignment = ",key_value_store_id='store-existing'" if has_store else ""
    db.execute(
        f"""UPDATE {table} SET status='needs_reconcile',
        run_id='run-existing',dataset_id='dataset-existing'{store_assignment}
        WHERE id=?""",
        (record["id"],),
    )

    with pytest.raises(RuntimeError, match="run_id"):
        reconcile(record["id"], run_id="run-different")
    with pytest.raises(RuntimeError, match="dataset_id"):
        reconcile(
            record["id"],
            run_id="run-existing",
            dataset_id="dataset-different",
        )
    if has_store:
        with pytest.raises(RuntimeError, match="key_value_store_id"):
            reconcile(
                record["id"],
                run_id="run-existing",
                dataset_id="dataset-existing",
                key_value_store_id="store-different",
            )

    attached = reconcile(record["id"], run_id="run-existing")

    assert attached["status"] == "run_started"
    assert attached["run_id"] == "run-existing"
    assert attached["dataset_id"] == "dataset-existing"
    if has_store:
        assert attached["key_value_store_id"] == "store-existing"


@pytest.mark.parametrize("kind", ["source", "photo", "access", "contract"])
@pytest.mark.parametrize("evidence", ["run", "charge", "registry"])
def test_reconcile_confirm_not_launched_rejects_any_provider_evidence(
    tmp_path: Path, kind: str, evidence: str
):
    db = Database(tmp_path / f"reconcile-confirm-evidence-{kind}-{evidence}.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
    )

    if kind == "source":
        epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
        coverage = db.upsert_coverage_stream(
            epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
        )
        record, _ = db.prepare_paid_source_batch(
            profile_id=1,
            epoch_id=epoch["id"],
            coverage_stream_id=coverage["id"],
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            intent="initial_public_capture",
            observation_window="evidence-window",
            normalized_input={"maxPostsPerProfile": 1},
        )
        table = "paid_source_batches"
        reconcile = db.reconcile_paid_source_batch
    elif kind == "photo":
        capture_id = add_photo_capture(db)
        record, _ = db.prepare_paid_photo_batch(
            profile_id=1,
            photo_capture_id=capture_id,
            actor_id="example/photos",
            normalized_input={"urls": ["https://facebook.com/1"]},
            max_charge_usd=0.1,
        )
        table = "paid_photo_batches"
        reconcile = db.reconcile_paid_photo_batch
    elif kind == "access":
        record, _ = db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window="evidence-window",
            normalized_input={"maxPostsPerProfile": 1},
            max_charge_usd=0.01,
        )
        table = "paid_access_probe_batches"
        reconcile = db.reconcile_paid_access_probe_batch
    else:
        record, _ = db.record_contract_run(
            contract["id"],
            test_case="page_1",
            normalized_input={"maxPostsPerProfile": 1},
        )
        table = "contract_runs"
        reconcile = db.reconcile_contract_run

    db.execute(
        f"UPDATE {table} SET status='needs_reconcile' WHERE id=?", (record["id"],)
    )
    if evidence == "registry":
        owner_type = {
            "source": "paid_source_batch",
            "photo": "paid_photo_batch",
            "access": "paid_access_probe_batch",
            "contract": "contract_run",
        }[kind]
        actor_id = (
            str(record.get("actor_id") or "")
            if kind != "contract"
            else str(contract["actor_id"])
        )
        db.execute(
            """INSERT INTO provider_run_registry(
              provider,run_id,owner_type,owner_id,actor_id,request_fingerprint,
              metadata_json,created_at,updated_at
            ) VALUES('apify','registry-only-run',?,?,?,?, '{}','now','now')""",
            (owner_type, record["id"], actor_id, record["request_hash"]),
        )
    elif kind in {"photo", "access"}:
        diagnostic_id = db.start_actor_run(
            1, kind, "example/actor", "primary", {"test": True}
        )
        if evidence == "run":
            db.execute(
                "UPDATE actor_runs SET run_id='linked-provider-run' WHERE id=?",
                (diagnostic_id,),
            )
        else:
            db.execute(
                "UPDATE actor_runs SET charged_usd=0.01 WHERE id=?", (diagnostic_id,)
            )
        db.execute(
            f"UPDATE {table} SET actor_run_id=? WHERE id=?",
            (diagnostic_id, record["id"]),
        )
    elif evidence == "run":
        db.execute(
            f"UPDATE {table} SET run_id='persisted-provider-run' WHERE id=?",
            (record["id"],),
        )
    else:
        db.execute(
            f"UPDATE {table} SET charged_usd=0.01 WHERE id=?", (record["id"],)
        )

    with pytest.raises(RuntimeError, match="provider"):
        reconcile(record["id"], confirm_not_launched=True)

    stored = db.row(f"SELECT status FROM {table} WHERE id=?", (record["id"],))
    assert stored["status"] == "needs_reconcile"


def test_contract_reconcile_resumes_expired_grant_but_cannot_start_new_run(
    tmp_path: Path,
):
    db = Database(tmp_path / "contract-reconcile-expired.sqlite3")
    add_profile(db)
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
    existing, _ = db.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"page": 1}
    )
    pending, _ = db.record_contract_run(
        contract["id"], test_case="page_2", normalized_input={"page": 2}
    )
    db.execute(
        "UPDATE contract_runs SET status='needs_reconcile' WHERE id=?",
        (existing["id"],),
    )
    db.execute(
        "UPDATE jobs SET status='failed',error='worker crashed' WHERE id=?",
        (job_id,),
    )
    db.execute(
        """UPDATE contract_test_grants
        SET status='expired',expires_at='2000-01-01T00:00:00+00:00' WHERE id=?""",
        (grant["id"],),
    )

    resumed = db.reconcile_contract_run(
        existing["id"],
        run_id="existing-contract-run",
        dataset_id="existing-contract-dataset",
    )

    assert resumed["status"] == "run_started"
    assert resumed["run_id"] == "existing-contract-run"
    assert resumed["dataset_id"] == "existing-contract-dataset"
    assert resumed["job_requeued"] is True
    assert db.row("SELECT status FROM jobs WHERE id=?", (job_id,))["status"] == "pending"

    denied, claimed = db.claim_contract_run_launch(
        pending["id"],
        lease_owner="worker",
        claimed_at="2026-09-04T00:00:00+00:00",
        monthly_limit_usd=5.0,
        official_used_usd=0.0,
        outstanding_reserve_usd=0.0,
        posts_result_price_usd=0.005,
    )
    assert claimed is False
    assert denied["status"] == "pending"
    assert denied["claim_denied_reason"] == "contract_grant_not_active"


def test_contract_reconcile_confirm_not_launched_does_not_requeue_expired_grant(
    tmp_path: Path,
):
    db = Database(tmp_path / "contract-reconcile-no-launch.sqlite3")
    add_profile(db)
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
    db.execute(
        """UPDATE contract_test_grants
        SET status='expired',expires_at='2000-01-01T00:00:00+00:00' WHERE id=?""",
        (grant["id"],),
    )

    closed = db.reconcile_contract_run(run["id"], confirm_not_launched=True)

    assert closed["status"] == "failed"
    assert "not launched" in closed["error"]
    assert closed["replacement_run_row_id"] is None
    assert closed["job_requeued"] is False
    assert db.row("SELECT status FROM jobs WHERE id=?", (job_id,))["status"] == "failed"


def test_contract_confirm_not_launched_replacement_can_claim_full_allocation(
    tmp_path: Path,
):
    db = Database(tmp_path / "contract-replacement-claim.sqlite3")
    add_profile(db)
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
    runs: dict[str, dict] = {}
    for test_case in ("page_1", "page_2", "page_2_replay", "known_boundary"):
        run, _ = db.record_contract_run(
            contract["id"],
            test_case=test_case,
            normalized_input={"test_case": test_case},
        )
        runs[test_case] = run
    original = runs["page_1"]
    db.execute(
        "UPDATE contract_runs SET status='needs_reconcile' WHERE id=?",
        (original["id"],),
    )
    db.execute("UPDATE jobs SET status='failed' WHERE id=?", (job_id,))

    reconciled = db.reconcile_contract_run(
        original["id"], confirm_not_launched=True
    )
    replacement_id = int(reconciled["replacement_run_row_id"])
    old = db.row("SELECT * FROM contract_runs WHERE id=?", (original["id"],))
    replacement = db.row(
        "SELECT * FROM contract_runs WHERE id=?", (replacement_id,)
    )
    assert old["status"] == "failed"
    assert old["run_id"] is None
    assert old["charged_usd"] == 0
    assert replacement["authorized_max_usd"] == pytest.approx(0.06)
    db.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))

    claimed_row, claimed = db.claim_contract_run_launch(
        replacement_id,
        lease_owner="worker",
        monthly_limit_usd=5.0,
        official_used_usd=0.0,
        outstanding_reserve_usd=0.0,
        posts_result_price_usd=0.005,
    )

    assert claimed is True
    assert claimed_row["status"] == "launching"
    # 0.06 + 0.06 + 0.06 + 0.02 remains exactly the authorized $0.20;
    # the archived no-launch row must not add another $0.06.
    active_authorized = db.row(
        """SELECT COALESCE(SUM(authorized_max_usd),0) total
        FROM contract_runs WHERE grant_allocation_id=?
        AND NOT(status='failed' AND COALESCE(run_id,'')=''
                AND COALESCE(charged_usd,0)<=0)""",
        (allocation["id"],),
    )
    assert active_authorized["total"] == pytest.approx(0.20)


def _race_two(call_left, call_right):
    barrier = threading.Barrier(2)

    def run(call):
        barrier.wait(timeout=5)
        return call()

    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(run, call_left)
        right = pool.submit(run, call_right)
        return left.result(timeout=10), right.result(timeout=10)


def test_job_claim_is_atomic_across_database_connections(tmp_path: Path):
    path = tmp_path / "job-claim.sqlite3"
    left = Database(path)
    add_profile(left)
    job_id = left.execute(
        """INSERT INTO jobs(profile_id,job_type,priority,available_at,created_at)
        VALUES(1,'detect_public_v2',0,'2026-08-20T00:00:00+00:00','now')"""
    )
    right = Database(path)

    results = _race_two(
        lambda: left.claim_pending_job(
            job_id, lease_owner="left", claimed_at="2026-08-20T01:00:00+00:00"
        ),
        lambda: right.claim_pending_job(
            job_id, lease_owner="right", claimed_at="2026-08-20T01:00:00+00:00"
        ),
    )

    assert sum(result is not None for result in results) == 1
    claimed = left.row("SELECT status,attempts,lease_owner FROM jobs WHERE id=?", (job_id,))
    assert claimed["status"] == "running"
    assert claimed["attempts"] == 1
    assert claimed["lease_owner"] in {"left", "right"}


def test_contract_launch_claim_is_atomic_across_database_connections(tmp_path: Path):
    path = tmp_path / "contract-claim.sqlite3"
    left = Database(path)
    add_profile(left)
    contract = left.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        status="pending",
    )
    run, _ = left.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"maximum": 10}
    )
    right = Database(path)

    results = _race_two(
        lambda: left.claim_contract_run_launch(run["id"], lease_owner="left"),
        lambda: right.claim_contract_run_launch(run["id"], lease_owner="right"),
    )

    assert sum(claimed for _, claimed in results) == 1
    row = left.row("SELECT status,lease_owner FROM contract_runs WHERE id=?", (run["id"],))
    assert row["status"] == "launching"
    assert row["lease_owner"] in {"left", "right"}


def test_contract_launch_claim_atomically_counts_all_paid_ledgers_and_reserve(
    tmp_path: Path,
):
    db = Database(tmp_path / "contract-budget-claim.sqlite3")
    add_profile(db)
    grant = db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    job_id, _, allocation = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/posts",
        schema_fingerprint="fp",
        fixture_ack=True,
    )
    db.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="fp",
        status="pending",
        evidence={"test_generation": allocation["test_generation"]},
    )
    run, _ = db.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"maximum": 10}
    )
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )
    source, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id="example/posts",
        intent="initial_public_capture",
        observation_window="source-window",
        normalized_input={"maxPostsPerProfile": 10},
    )
    db.transition_paid_source_batch(source["id"], "launching")
    probe, _ = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id="example/posts",
        observation_window="probe-window",
        normalized_input={"maxPostsPerProfile": 1},
        max_charge_usd=0.01,
    )
    db.transition_paid_access_probe_batch(probe["id"], "launching")

    # $5 - $0.20 official - $4.55 protected = $0.25, while the durable
    # ledgers reserve $0.20 contract + $0.05 source + $0.01 access.
    denied, claimed = db.claim_contract_run_launch(
        run["id"],
        lease_owner="worker",
        monthly_limit_usd=5,
        official_used_usd=0.20,
        outstanding_reserve_usd=4.55,
        posts_result_price_usd=0.005,
    )
    assert claimed is False
    assert denied["status"] == "pending"
    assert denied["claim_denied_reason"] == "monthly_budget_capacity"

    # With another $0.10 of official capacity, the exact same atomic ledger
    # fits and the single launch lease can be acquired.
    accepted, claimed = db.claim_contract_run_launch(
        run["id"],
        lease_owner="worker",
        monthly_limit_usd=5,
        official_used_usd=0.10,
        outstanding_reserve_usd=4.55,
        posts_result_price_usd=0.005,
    )
    assert claimed is True
    assert accepted["status"] == "launching"


def test_contract_launch_claim_rejects_round_authorization_oversubscription(
    tmp_path: Path,
):
    db = Database(tmp_path / "contract-oversubscribed.sqlite3")
    add_profile(db)
    grant = db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    job_id, _, allocation = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/posts",
        schema_fingerprint="fp",
        fixture_ack=True,
    )
    db.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="fp",
        status="pending",
        evidence={"test_generation": allocation["test_generation"]},
    )
    runs = []
    for index in range(4):
        run, _ = db.record_contract_run(
            contract["id"],
            test_case="page_1",
            normalized_input={"maximum": 10, "variant": index},
        )
        runs.append(run)

    denied, claimed = db.claim_contract_run_launch(
        runs[0]["id"],
        lease_owner="worker",
        monthly_limit_usd=5,
        official_used_usd=0,
        outstanding_reserve_usd=0,
        posts_result_price_usd=0.005,
    )

    assert claimed is False
    assert denied["claim_denied_reason"] == "contract_allocation_oversubscribed"
    assert db.row("SELECT status FROM contract_runs WHERE id=?", (runs[0]["id"],))[
        "status"
    ] == "pending"


def test_paid_source_launch_claim_is_atomic_across_database_connections(tmp_path: Path):
    path = tmp_path / "paid-source-claim.sqlite3"
    left = Database(path)
    add_profile(left)
    epoch, _ = left.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = left.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )
    batch, _ = left.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=None,
        provider="apify",
        actor_id="example/posts",
        intent="initial_public_capture",
        observation_window="window-1",
        normalized_input={"maximum": 50},
    )
    right = Database(path)

    results = _race_two(
        lambda: left.claim_paid_source_batch_launch(
            batch["id"], lease_owner="left", posts_result_price_usd=0.01
        ),
        lambda: right.claim_paid_source_batch_launch(
            batch["id"], lease_owner="right", posts_result_price_usd=0.01
        ),
    )

    assert sum(claimed for _, claimed in results) == 1
    row = left.row(
        "SELECT status,run_id,lease_owner FROM paid_source_batches WHERE id=?", (batch["id"],)
    )
    assert row["status"] == "launching"
    assert row["run_id"] is None
    assert row["lease_owner"] in {"left", "right"}


def test_paid_source_launch_persists_ceiling_across_later_price_decrease(
    tmp_path: Path,
):
    db = Database(tmp_path / "paid-source-persistent-ceiling.sqlite3")
    add_profile(db)
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )

    def prepare(window: str) -> dict:
        batch, _ = db.prepare_paid_source_batch(
            profile_id=1,
            epoch_id=epoch["id"],
            coverage_stream_id=coverage["id"],
            contract_id=None,
            provider="apify",
            actor_id="example/posts",
            intent="initial_public_capture",
            observation_window=window,
            normalized_input={"maxPostsPerProfile": 20},
        )
        return batch

    first = prepare("price-window-1")
    first_row, first_claimed = db.claim_paid_source_batch_launch(
        first["id"],
        lease_owner="worker-1",
        budget_capacity_usd=0.11,
        posts_result_price_usd=0.005,
    )
    assert first_claimed is True
    assert first_row["status"] == "launching"
    assert first_row["max_charge_usd"] == pytest.approx(0.10)

    second = prepare("price-window-2")
    second_row, second_claimed = db.claim_paid_source_batch_launch(
        second["id"],
        lease_owner="worker-2",
        budget_capacity_usd=0.11,
        posts_result_price_usd=0.001,
    )

    # The active first batch reserved $0.10 when it crossed the launch
    # boundary. Recomputing it at today's lower $0.001/result price would
    # incorrectly reserve only $0.02 and allow this second purchase.
    assert second_claimed is False
    assert second_row["status"] == "prepared"
    reservations = db.paid_budget_reservations(posts_result_price_usd=0.001)
    assert reservations["source_unsettled_usd"] == pytest.approx(0.10)


def test_capture_v2_migration_seeds_legacy_controls_and_names_once(tmp_path: Path):
    path = tmp_path / "legacy-v2.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE profiles (
          id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE,
          enabled INTEGER NOT NULL DEFAULT 1, display_name TEXT, apify_frozen INTEGER NOT NULL DEFAULT 0,
          profile_details_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )"""
    )
    connection.execute(
        """INSERT INTO profiles(
          id,name,url,display_name,apify_frozen,profile_details_json,created_at,updated_at
        ) VALUES(1,'FB-1','https://facebook.com/1','Trusted Name',1,?,'old','old')""",
        (json.dumps({"rejected_profile_names": ["Wrong Name"]}),),
    )
    connection.commit()
    connection.close()

    db = Database(path)

    assert db.migration_applied(CAPTURE_V2_SCHEMA_MIGRATION)
    assert db.profile_source_frozen(1, "apify") is True
    names = db.rows(
        """SELECT candidate_name,status,is_current FROM profile_name_candidates
        WHERE profile_id=1 ORDER BY candidate_name"""
    )
    assert names == [
        {"candidate_name": "Trusted Name", "status": "accepted", "is_current": 1},
        {"candidate_name": "Wrong Name", "status": "rejected", "is_current": 0},
    ]
    assert db.row("SELECT display_name,apify_frozen FROM profiles WHERE id=1") == {
        "display_name": "Trusted Name",
        "apify_frozen": 1,
    }
    assert db.row("SELECT COUNT(*) AS total FROM jobs")["total"] == 0

    db.ensure_schema()

    assert db.row("SELECT COUNT(*) AS total FROM profile_source_controls")["total"] == 1
    assert db.row("SELECT COUNT(*) AS total FROM profile_name_candidates")["total"] == 2

    # V1 and V2 freeze representations remain synchronized during migration.
    db.execute("UPDATE profiles SET apify_frozen=0 WHERE id=1")
    assert db.profile_source_frozen(1, "apify") is False
    db.set_profile_source_control(1, "apify", frozen=True, reason="manual")
    assert db.row("SELECT apify_frozen FROM profiles WHERE id=1")["apify_frozen"] == 1


def test_contract_test_grant_migration_is_additive_for_existing_v2_database(tmp_path: Path):
    path = tmp_path / "legacy-contract-runs.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE contract_runs (
          id INTEGER PRIMARY KEY, contract_id INTEGER NOT NULL, request_hash TEXT NOT NULL UNIQUE,
          test_case TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', run_id TEXT,
          dataset_id TEXT, input_json TEXT NOT NULL DEFAULT '{}', expected_json TEXT NOT NULL DEFAULT '{}',
          result_json TEXT NOT NULL DEFAULT '{}', result_count INTEGER NOT NULL DEFAULT 0,
          charged_usd REAL NOT NULL DEFAULT 0, error TEXT, started_at TEXT NOT NULL, finished_at TEXT
        )"""
    )
    connection.execute(
        """CREATE TABLE jobs (
          id INTEGER PRIMARY KEY, profile_id INTEGER, job_type TEXT NOT NULL,
          priority INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          payload_json TEXT NOT NULL DEFAULT '{}', available_at TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, error TEXT, created_at TEXT NOT NULL,
          started_at TEXT, finished_at TEXT
        )"""
    )
    connection.execute(
        """INSERT INTO jobs(job_type,priority,status,available_at,created_at)
        VALUES('contract_test_posts_v2',-250,'pending','old','old')"""
    )
    connection.execute(
        """INSERT INTO jobs(job_type,priority,status,available_at,created_at)
        VALUES('contract_test_posts_v2',-250,'running','old','old')"""
    )
    connection.commit()
    connection.close()

    db = Database(path)

    assert db.migration_applied(CONTRACT_TEST_GRANT_MIGRATION)
    assert db.has_column("contract_runs", "grant_allocation_id")
    assert db.has_column("contract_runs", "authorized_max_usd")
    assert db.row(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='contract_test_grants'"
    )
    assert db.row("SELECT status FROM jobs WHERE id=1")["status"] == "cancelled"
    assert db.row("SELECT status FROM jobs WHERE id=2")["status"] == "needs_reconcile"
    db.ensure_schema()
    assert db.row(
        "SELECT COUNT(*) total FROM schema_migrations WHERE name=?",
        (CONTRACT_TEST_GRANT_MIGRATION,),
    )["total"] == 1


def test_contract_test_grant_is_global_and_fallback_only_uses_round_remainder(tmp_path: Path):
    db = Database(tmp_path / "contract-grant.sqlite3")
    add_profile(db, 1)
    add_profile(db, 2)
    with pytest.raises(ValueError, match="cannot exceed"):
        db.create_contract_test_grant(max_usd=0.21, authorized_by="test")
    grant = db.create_contract_test_grant(
        max_usd=0.20, valid_hours=24, authorized_by="test"
    )
    with pytest.raises(ValueError, match="at least 25"):
        db.queue_contract_test_job(
            grant_id=grant["id"],
            profile_id=1,
            actor_id="example/primary",
            schema_fingerprint="primary-fingerprint",
        )
    primary_job, created, primary = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/primary",
        schema_fingerprint="primary-fingerprint",
        fixture_ack=True,
    )
    same_job, created_again, _ = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/primary",
        schema_fingerprint="primary-fingerprint",
        fixture_ack=True,
    )
    assert created is True and created_again is False and same_job == primary_job
    with pytest.raises(ValueError, match="pending or running"):
        db.close_contract_test_grant(grant["id"])
    with pytest.raises(ValueError, match="pending or running"):
        db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    with pytest.raises(ValueError, match="pending or running"):
        db.queue_contract_test_job(
            grant_id=grant["id"],
            profile_id=2,
            actor_id="example/fallback",
            schema_fingerprint="fallback-fingerprint",
            fixture_ack=True,
        )

    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/primary",
        purpose="posts_backfill",
        schema_fingerprint="primary-fingerprint",
        status="pending",
        evidence={"test_generation": primary["test_generation"]},
    )
    run, _ = db.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"maximum": 10}
    )
    assert run["grant_allocation_id"] == primary["id"]
    assert run["authorized_max_usd"] == pytest.approx(0.06)
    db.execute(
        "UPDATE contract_runs SET status='succeeded',charged_usd=0.02 WHERE id=?", (run["id"],)
    )
    db.execute("UPDATE jobs SET status='failed',finished_at='now' WHERE id=?", (primary_job,))

    ledger = db.contract_test_grant_ledger(grant["id"])
    assert ledger["status"] == "active"
    assert ledger["spent_usd"] == pytest.approx(0.02)
    assert ledger["reserved_usd"] == pytest.approx(0)
    assert ledger["remaining_usd"] == pytest.approx(0.18)

    fallback_job, fallback_created, fallback = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=2,
        actor_id="example/fallback",
        schema_fingerprint="fallback-fingerprint",
        fixture_ack=True,
    )
    assert fallback_created is True and fallback_job != primary_job
    assert fallback["authorized_usd"] == pytest.approx(0.18)
    payload = json.loads(db.row("SELECT payload_json FROM jobs WHERE id=?", (fallback_job,))["payload_json"])
    assert payload["max_budget_usd"] == pytest.approx(0.18)
    assert payload["contract_grant_id"] == grant["id"]
    assert payload["fixture_ack"] is True
    assert payload["fixture_expected_min_public_posts"] == 25


def test_contract_test_grant_keeps_ambiguous_charge_reserved(tmp_path: Path):
    db = Database(tmp_path / "contract-grant-ambiguous.sqlite3")
    add_profile(db)
    grant = db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    job_id, _, allocation = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/primary",
        schema_fingerprint="fp",
        fixture_ack=True,
    )
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/primary",
        purpose="posts_backfill",
        schema_fingerprint="fp",
        status="pending",
        evidence={"test_generation": allocation["test_generation"]},
    )
    run, _ = db.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"maximum": 10}
    )
    db.execute(
        "UPDATE contract_runs SET status='needs_reconcile',charged_usd=0.01 WHERE id=?",
        (run["id"],),
    )
    db.execute("UPDATE jobs SET status='failed',finished_at='now' WHERE id=?", (job_id,))

    ledger = db.contract_test_grant_ledger(grant["id"])
    assert ledger["spent_usd"] == pytest.approx(0.01)
    assert ledger["reserved_usd"] == pytest.approx(0.05)
    assert ledger["remaining_usd"] == pytest.approx(0.14)
    with pytest.raises(ValueError, match="ambiguous"):
        db.queue_contract_test_job(
            grant_id=grant["id"],
            profile_id=1,
            actor_id="example/fallback",
            schema_fingerprint="fallback-fp",
            fixture_ack=True,
        )
    with pytest.raises(ValueError, match="ambiguous"):
        db.close_contract_test_grant(grant["id"])
    with pytest.raises(ValueError, match="ambiguous"):
        db.create_contract_test_grant(max_usd=0.20, authorized_by="test")


def test_contract_test_grant_counts_known_charge_even_when_run_failed(tmp_path: Path):
    db = Database(tmp_path / "contract-grant-failed-charge.sqlite3")
    add_profile(db)
    grant = db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    job_id, _, allocation = db.queue_contract_test_job(
        grant_id=grant["id"],
        profile_id=1,
        actor_id="example/primary",
        schema_fingerprint="fp",
        fixture_ack=True,
    )
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/primary",
        purpose="posts_backfill",
        schema_fingerprint="fp",
        status="pending",
        evidence={"test_generation": allocation["test_generation"]},
    )
    run, _ = db.record_contract_run(
        contract["id"], test_case="page_1", normalized_input={"maximum": 10}
    )
    db.execute(
        "UPDATE contract_runs SET status='failed',charged_usd=0.02 WHERE id=?",
        (run["id"],),
    )
    db.execute("UPDATE jobs SET status='failed',finished_at='now' WHERE id=?", (job_id,))

    ledger = db.contract_test_grant_ledger(grant["id"])

    assert ledger["spent_usd"] == pytest.approx(0.02)
    assert ledger["reserved_usd"] == pytest.approx(0)
    assert ledger["remaining_usd"] == pytest.approx(0.18)


def test_capture_epoch_and_coverage_uniqueness_are_database_enforced(tmp_path: Path):
    db = Database(tmp_path / "capture.sqlite3")
    add_profile(db)

    first, created = db.get_or_create_capture_epoch(
        1, "public_transition", priority=0, scope={"surfaces": ["timeline_posts"]}
    )
    same, created_again = db.get_or_create_capture_epoch(1, "recovery")
    assert created is True
    assert created_again is False
    assert same["id"] == first["id"]

    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            """INSERT INTO capture_epochs(
              profile_id,trigger_reason,status,is_active,created_at,updated_at
            ) VALUES(1,'duplicate','ready',1,'now','now')"""
        )

    coverage = db.upsert_coverage_stream(
        first["id"], stream="posts", surface="timeline_posts", provider="primary"
    )
    same_coverage = db.upsert_coverage_stream(
        first["id"], stream="posts", surface="timeline_posts", provider="updated-provider"
    )
    assert same_coverage["id"] == coverage["id"]
    assert same_coverage["provider"] == "updated-provider"
    assert db.row("SELECT COUNT(*) AS total FROM coverage_streams")["total"] == 1

    job_id, job_created = db.queue_unique_job(
        profile_id=1,
        job_type="capture_v2_continue",
        priority=0,
        dedupe_key=f"epoch:{first['id']}:posts:timeline",
        payload={"coverage_stream_id": coverage["id"]},
        epoch_id=first["id"],
    )
    same_job_id, same_job_created = db.queue_unique_job(
        profile_id=1,
        job_type="capture_v2_continue",
        priority=0,
        dedupe_key=f"epoch:{first['id']}:posts:timeline",
        epoch_id=first["id"],
    )
    assert job_created is True
    assert same_job_created is False
    assert same_job_id == job_id
    assert db.has_column("jobs", "dedupe_key")
    assert db.has_column("jobs", "epoch_id")
    assert db.has_column("jobs", "batch_id")

    db.update_coverage_stream(
        coverage["id"],
        status="in_progress",
        output_cursor="page-2",
        provider_checkpoint_json={"next": "page-2"},
        gaps_json=["reels"],
    )
    updated = db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert updated["status"] == "in_progress"
    assert json.loads(updated["provider_checkpoint_json"]) == {"next": "page-2"}

    db.finish_capture_epoch(first["id"], status="complete", terminal_reason="all surfaces terminal")
    second, second_created = db.get_or_create_capture_epoch(1, "monthly_repair")
    assert second_created is True
    assert second["id"] != first["id"]


def test_coverage_updates_require_legal_transition_evidence_and_reasons(tmp_path: Path):
    db = Database(tmp_path / "coverage-transitions.sqlite3")
    add_profile(db)
    epoch, _ = db.get_or_create_capture_epoch(1, "public_transition", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts"
    )

    with pytest.raises(ValueError, match="complete coverage requires terminal evidence"):
        db.update_coverage_stream(coverage["id"], status="complete")
    with pytest.raises(ValueError, match="source_limited coverage requires a reason"):
        db.update_coverage_stream(coverage["id"], status="source_limited")
    assert db.row("SELECT status FROM coverage_streams WHERE id=?", (coverage["id"],))["status"] == "pending"

    db.update_coverage_stream(coverage["id"], status="in_progress", output_cursor="cursor-1")
    db.update_coverage_stream(
        coverage["id"],
        status="complete",
        output_cursor=None,
        terminal_evidence_json={"cursor_exhausted": True, "last_cursor": "cursor-1"},
    )
    complete = db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert complete["status"] == "complete"
    assert json.loads(complete["terminal_evidence_json"])["cursor_exhausted"] is True

    limited = db.upsert_coverage_stream(
        epoch["id"], stream="media", surface="public_photo_pages"
    )
    db.update_coverage_stream(
        limited["id"], status="source_limited", limited_reason="provider has no album cursor"
    )
    assert db.row("SELECT status,limited_reason FROM coverage_streams WHERE id=?", (limited["id"],)) == {
        "status": "source_limited",
        "limited_reason": "provider has no album cursor",
    }


def test_paid_batch_is_idempotent_and_retains_replay_state(tmp_path: Path):
    db = Database(tmp_path / "paid.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="spbotdel/facebook-profile-posts-all-photos-scraper",
        purpose="posts_backfill",
        build_id="build-1",
        schema_fingerprint="schema-1",
        input_mapping_hash="mapping-1",
        status="passed",
        evidence={"cursor_replay": True},
    )
    assert db.valid_actor_contract(
        provider="apify",
        actor_id=contract["actor_id"],
        purpose="posts_backfill",
    )["id"] == contract["id"]

    epoch, _ = db.get_or_create_capture_epoch(1, "public_transition", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"],
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=contract["id"],
    )
    request_identity = {
        "intent": "initial_capture",
        "window": "epoch-1",
        "profile_id": 1,
        "cursor": "",
        "input": {"maxPosts": 50},
    }
    expected_hash = canonical_request_hash(request_identity)
    first, created = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_capture",
        observation_window="epoch-1",
        normalized_input={"maxPosts": 50},
        request_identity=request_identity,
    )
    replay, created_again = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_capture",
        observation_window="epoch-1",
        normalized_input={"maxPosts": 50},
        request_identity=request_identity,
    )
    assert first["request_hash"] == expected_hash
    assert replay["id"] == first["id"]
    assert created is True
    assert created_again is False

    db.transition_paid_source_batch(
        first["id"],
        "launching",
        expected_status="prepared",
    )
    db.transition_paid_source_batch(
        first["id"],
        "run_started",
        expected_status="launching",
        run_id="run-1",
        dataset_id="dataset-1",
    )
    raw = db.transition_paid_source_batch(
        first["id"],
        "raw_saved",
        expected_status="run_started",
        raw_path="raw/batch.json.gz",
        raw_sha256="abc",
        charged_usd=0.05,
        raw_result_count=50,
    )
    assert raw["raw_saved_at"]
    db.transition_paid_source_batch(first["id"], "import_failed", error="temporary db failure")
    db.transition_paid_source_batch(
        first["id"],
        "imported",
        parsed_result_count=50,
        new_result_count=48,
        duplicate_result_count=2,
    )
    committed = db.transition_paid_source_batch(
        first["id"], "committed", output_cursor="cursor-2", identity_set_hash="identity-hash"
    )
    assert committed["status"] == "committed"
    assert committed["run_id"] == "run-1"
    assert committed["raw_path"] == "raw/batch.json.gz"
    assert committed["output_cursor"] == "cursor-2"


def test_paid_access_probe_batch_is_unique_and_has_crash_safe_milestones(tmp_path: Path):
    db = Database(tmp_path / "paid-access-probe.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )
    identity = {
        "intent": "access_probe",
        "window": "2026-08-20T00:00:00Z/2026-08-20T02:00:00Z",
        "profile_id": 1,
        "input": {"maxPostsPerProfile": 1, "knownPostIds": []},
    }
    first, created = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window=identity["window"],
        normalized_input=identity["input"],
        max_charge_usd=0.01,
        request_identity=identity,
    )
    replay, created_again = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window=identity["window"],
        normalized_input=identity["input"],
        max_charge_usd=0.01,
        request_identity=identity,
    )

    assert created is True
    assert created_again is False
    assert replay["id"] == first["id"]
    assert replay["request_hash"] == canonical_request_hash(identity)
    with pytest.raises(ValueError, match="different paid access probe"):
        db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window=identity["window"],
            normalized_input={"maxPostsPerProfile": 2, "knownPostIds": []},
            max_charge_usd=0.01,
            request_identity={**identity, "input": {"maxPostsPerProfile": 2}},
        )
    assert db.row("SELECT COUNT(*) total FROM paid_access_probe_batches")["total"] == 1
    launching = db.transition_paid_access_probe_batch(
        first["id"], "launching", expected_status="prepared"
    )
    assert launching["launched_at"]
    started = db.transition_paid_access_probe_batch(
        first["id"],
        "run_started",
        expected_status="launching",
        run_id="run-1",
        dataset_id="dataset-1",
        key_value_store_id="store-1",
    )
    assert started["run_id"] == "run-1"
    raw = db.transition_paid_access_probe_batch(
        first["id"],
        "raw_saved",
        expected_status="run_started",
        raw_path="capture-v2/raw/probe.json.gz",
        raw_sha256="sha",
        charged_usd=0.005,
        raw_result_count=1,
    )
    assert raw["raw_saved_at"]
    imported = db.transition_paid_access_probe_batch(
        first["id"], "imported", expected_status="raw_saved", parsed_result_count=1
    )
    assert imported["imported_at"]
    committed = db.transition_paid_access_probe_batch(
        first["id"], "committed", expected_status="imported"
    )
    assert committed["committed_at"]
    assert db.has_column("paid_access_probe_batches", "request_hash")


def test_prepared_access_probe_charge_can_only_be_clamped_down(tmp_path: Path):
    db = Database(tmp_path / "probe-clamp.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
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

    lowered = db.clamp_paid_access_probe_max_charge(batch["id"], 0.006)
    unchanged = db.clamp_paid_access_probe_max_charge(batch["id"], 0.02)

    assert lowered["max_charge_usd"] == pytest.approx(0.006)
    assert unchanged["max_charge_usd"] == pytest.approx(0.006)


def test_access_probe_reconcile_attaches_existing_run_or_closes_without_rebuy(
    tmp_path: Path,
):
    db = Database(tmp_path / "probe-reconcile.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )

    def ambiguous(window: str) -> dict:
        batch, _ = db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window=window,
            normalized_input={"maxPostsPerProfile": 1},
            max_charge_usd=0.01,
        )
        batch = db.transition_paid_access_probe_batch(batch["id"], "launching")
        return db.transition_paid_access_probe_batch(batch["id"], "needs_reconcile")

    attached = db.reconcile_paid_access_probe_batch(
        ambiguous("window-run")["id"],
        run_id="run-existing",
        dataset_id="dataset-existing",
    )
    closed = db.reconcile_paid_access_probe_batch(
        ambiguous("window-none")["id"],
        confirm_not_launched=True,
    )

    assert attached["status"] == "run_started"
    assert attached["run_id"] == "run-existing"
    assert attached["dataset_id"] == "dataset-existing"
    assert closed["status"] == "failed"
    assert "not launched" in closed["error"]
    with pytest.raises(RuntimeError, match="expected needs_reconcile"):
        db.reconcile_paid_access_probe_batch(
            attached["id"], run_id="run-second"
        )


def test_paid_budget_reservations_unify_source_and_access_probe_ledgers(tmp_path: Path):
    db = Database(tmp_path / "paid-reservations.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"],
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=contract["id"],
    )
    source, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window="source-window",
        normalized_input={"maxPostsPerProfile": 10},
    )
    db.transition_paid_source_batch(
        source["id"], "launching", charged_usd=0.005
    )
    probe, _ = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window="probe-window",
        normalized_input={"maxPostsPerProfile": 1},
        max_charge_usd=0.01,
    )
    db.transition_paid_access_probe_batch(probe["id"], "launching")

    reservations = db.paid_budget_reservations(posts_result_price_usd=0.005)

    assert reservations["source_unsettled_usd"] == pytest.approx(0.045)
    assert reservations["access_probe_unsettled_usd"] == pytest.approx(0.01)
    assert reservations["total_unsettled_usd"] == pytest.approx(0.055)


def test_legacy_actor_running_and_reconcile_ceilings_share_paid_reservations(
    tmp_path: Path,
):
    db = Database(tmp_path / "legacy-actor-reservations.sqlite3")
    add_profile(db)
    diagnostic_id, denial = db.claim_legacy_actor_run_launch(
        1,
        "posts",
        "legacy/posts",
        "default",
        {"startUrls": ["https://facebook.com/1"]},
        max_charge_usd=0.03,
        cycle_start_at="2026-09-01T00:00:00+00:00",
        monthly_limit_usd=5.0,
        provider_used_usd=0.0,
        baseline_local_settled_usd=0.0,
        posts_result_price_usd=0.005,
    )

    assert denial is None
    assert diagnostic_id is not None
    running = db.paid_budget_reservations(posts_result_price_usd=0.005)
    assert running["legacy_actor_unsettled_usd"] == pytest.approx(0.03)
    assert running["total_unsettled_usd"] == pytest.approx(0.03)

    db.finish_actor_run(
        diagnostic_id,
        status="needs_reconcile",
        charged_usd=0.01,
        error="provider accepted the request but response was lost",
    )
    ambiguous = db.paid_budget_reservations(posts_result_price_usd=0.005)
    assert ambiguous["legacy_actor_unsettled_usd"] == pytest.approx(0.02)
    assert ambiguous["total_unsettled_usd"] == pytest.approx(0.02)


def test_apify_settled_charge_floor_is_cycle_scoped_and_uses_terminal_ceilings(
    tmp_path: Path,
):
    db = Database(tmp_path / "settled-charge-floor.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"],
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=contract["id"],
    )

    def source(window: str, maximum: int) -> dict:
        batch, _ = db.prepare_paid_source_batch(
            profile_id=1,
            epoch_id=epoch["id"],
            coverage_stream_id=coverage["id"],
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            intent="initial_public_capture",
            observation_window=window,
            normalized_input={"maxPostsPerProfile": maximum},
        )
        return batch

    cycle_start = "2026-09-01T00:00:00+00:00"
    current = "2026-09-02T00:00:00+00:00"
    previous = "2026-08-31T23:59:59+00:00"

    terminal_zero = source("terminal-zero", 20)
    failed_without_run = source("failed-without-run", 50)
    old_terminal = source("old-terminal", 50)
    prepared = source("still-prepared", 50)
    active_with_known_charge = source("active-known-charge", 50)

    db.execute(
        """UPDATE paid_source_batches
        SET status='launching',launched_at=?,charged_usd=0.02,updated_at=? WHERE id=?""",
        (current, current, active_with_known_charge["id"]),
    )
    db.execute(
        """UPDATE paid_source_batches
        SET status='failed',run_id=NULL,charged_usd=0,launched_at=?,updated_at=? WHERE id=?""",
        (current, current, failed_without_run["id"]),
    )
    db.execute(
        """UPDATE paid_source_batches
        SET status='committed',run_id='old-run',charged_usd=0,
            launched_at=?,committed_at=?,updated_at=? WHERE id=?""",
        (previous, previous, previous, old_terminal["id"]),
    )

    # Merely preparing a request does not reserve a settled charge. An active
    # launch contributes only a provider-reported charge; its remaining ceiling
    # is represented by paid_budget_reservations instead.
    assert db.apify_settled_charge_floor(
        cycle_start, posts_result_price_usd=0.005
    ) == pytest.approx(0.02)

    db.execute(
        """UPDATE paid_source_batches
        SET status='committed',run_id='source-run',charged_usd=0,
            launched_at=?,committed_at=?,updated_at=? WHERE id=?""",
        (current, current, current, terminal_zero["id"]),
    )

    capture_id = add_photo_capture(db)
    photo, _ = db.prepare_paid_photo_batch(
        profile_id=1,
        photo_capture_id=capture_id,
        actor_id="example/photos",
        normalized_input={"urls": ["https://facebook.com/1"]},
        max_charge_usd=0.04,
    )
    db.execute(
        """UPDATE paid_photo_batches
        SET status='failed',run_id='photo-run',charged_usd=0,
            launched_at=?,updated_at=? WHERE id=?""",
        (current, current, photo["id"]),
    )

    probe, _ = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window="probe-terminal-zero",
        normalized_input={"maxPostsPerProfile": 1},
        max_charge_usd=0.03,
    )
    db.execute(
        """UPDATE paid_access_probe_batches
        SET status='failed',run_id='probe-run',charged_usd=0,
            launched_at=?,updated_at=? WHERE id=?""",
        (current, current, probe["id"]),
    )

    contract_run, _ = db.record_contract_run(
        contract["id"],
        test_case="terminal-zero",
        normalized_input={"maxPostsPerProfile": 1},
    )
    db.execute(
        """UPDATE contract_runs
        SET status='failed',run_id='contract-run',charged_usd=0,
            authorized_max_usd=0.06,started_at=?,finished_at=? WHERE id=?""",
        (current, current, contract_run["id"]),
    )

    # Current-cycle terminal rows with an unknown/zero provider charge retain
    # their authorized ceiling. Previous-cycle terminal, current prepared, and
    # a failed request proven not to have launched are excluded.
    assert db.apify_settled_charge_floor(
        cycle_start, posts_result_price_usd=0.005
    ) == pytest.approx(0.25)
    assert db.row(
        "SELECT status FROM paid_source_batches WHERE id=?", (prepared["id"],)
    )["status"] == "prepared"


def test_access_probe_launch_claim_atomically_accounts_for_other_probe(tmp_path: Path):
    db = Database(tmp_path / "probe-atomic-budget.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )

    def prepare(window: str) -> dict:
        batch, _ = db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window=window,
            normalized_input={"maxPostsPerProfile": 1},
            max_charge_usd=0.01,
        )
        return batch

    first, second = prepare("window-1"), prepare("window-2")
    first_claim, first_won = db.claim_paid_access_probe_launch(
        first["id"],
        global_capacity_usd=0.014,
        detection_capacity_usd=0.014,
        posts_result_price_usd=0.005,
    )
    second_claim, second_won = db.claim_paid_access_probe_launch(
        second["id"],
        global_capacity_usd=0.014,
        detection_capacity_usd=0.014,
        posts_result_price_usd=0.005,
    )

    assert first_won is True
    assert first_claim["status"] == "launching"
    assert second_won is False
    assert second_claim["status"] == "prepared"
    assert second_claim["max_charge_usd"] == pytest.approx(0.01)

    recovered, recovered_won = db.claim_paid_access_probe_launch(
        second["id"],
        global_capacity_usd=0.02,
        detection_capacity_usd=0.02,
        posts_result_price_usd=0.005,
    )
    assert recovered_won is True
    assert recovered["status"] == "launching"
    assert recovered["max_charge_usd"] == pytest.approx(0.01)


def test_access_probe_cycle_detection_budget_counts_each_reservation_once(
    tmp_path: Path,
):
    db = Database(tmp_path / "probe-cycle-detection-budget.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )

    def prepare(window: str) -> dict:
        batch, _ = db.prepare_paid_access_probe_batch(
            profile_id=1,
            contract_id=contract["id"],
            provider="apify",
            actor_id=contract["actor_id"],
            observation_window=window,
            normalized_input={"maxPostsPerProfile": 1},
            max_charge_usd=0.01,
        )
        return batch

    first, second, third = (
        prepare("cycle-window-1"),
        prepare("cycle-window-2"),
        prepare("cycle-window-3"),
    )
    claim_args = {
        "global_capacity_usd": 5.0,
        "detection_capacity_usd": 0.02,
        "posts_result_price_usd": 0.005,
        "cycle_start_at": "2000-01-01T00:00:00+00:00",
        "monthly_limit_usd": 5.0,
        "provider_used_usd": 0.0,
        "baseline_local_settled_usd": 0.0,
        "outstanding_reserve_usd": 0.0,
        "detection_limit_usd": 0.02,
        "detection_historical_used_usd": 0.0,
    }

    first_row, first_won = db.claim_paid_access_probe_launch(
        first["id"], **claim_args
    )
    second_row, second_won = db.claim_paid_access_probe_launch(
        second["id"], **claim_args
    )
    third_row, third_won = db.claim_paid_access_probe_launch(
        third["id"], **claim_args
    )

    assert first_won is True
    assert first_row["max_charge_usd"] == pytest.approx(0.01)
    # The second request exactly fills $0.02. Its predecessor is counted by
    # the cycle-aware ledger, so it must not be subtracted a second time.
    assert second_won is True
    assert second_row["max_charge_usd"] == pytest.approx(0.01)
    assert third_won is False
    assert third_row["status"] == "prepared"
    assert third_row["max_charge_usd"] == pytest.approx(0.01)

    recovered_args = {
        **claim_args,
        "detection_capacity_usd": 0.03,
        "detection_limit_usd": 0.03,
    }
    recovered, recovered_won = db.claim_paid_access_probe_launch(
        third["id"], **recovered_args
    )
    assert recovered_won is True
    assert recovered["status"] == "launching"
    assert recovered["max_charge_usd"] == pytest.approx(0.01)


def test_source_launch_claim_atomically_accounts_for_access_probe_ledger(tmp_path: Path):
    db = Database(tmp_path / "source-cross-ledger-budget.sqlite3")
    add_profile(db)
    contract = db.upsert_actor_contract(
        provider="apify",
        actor_id="example/posts",
        purpose="posts_backfill",
        schema_fingerprint="schema-1",
        status="passed",
        evidence={"test": True},
    )
    epoch, _ = db.get_or_create_capture_epoch(1, "test", status="ready")
    coverage = db.upsert_coverage_stream(
        epoch["id"], stream="posts", surface="timeline_posts", provider="apify"
    )
    source, _ = db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window="source-window",
        normalized_input={"maxPostsPerProfile": 1},
    )
    probe, _ = db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window="probe-window",
        normalized_input={"maxPostsPerProfile": 1},
        max_charge_usd=0.01,
    )
    _, probe_claimed = db.claim_paid_access_probe_launch(
        probe["id"],
        global_capacity_usd=0.014,
        detection_capacity_usd=0.014,
        posts_result_price_usd=0.005,
    )

    source_row, source_claimed = db.claim_paid_source_batch_launch(
        source["id"],
        lease_owner="worker",
        budget_capacity_usd=0.014,
        posts_result_price_usd=0.005,
    )

    assert probe_claimed is True
    assert source_claimed is False
    assert source_row["status"] == "prepared"


def test_observations_aliases_browser_and_name_helpers_are_idempotent(tmp_path: Path):
    db = Database(tmp_path / "helpers.sqlite3")
    add_profile(db)
    observed_at = "2026-08-16T00:00:00+00:00"
    observation = db.record_access_observation(
        1,
        source="anonymous_chromium",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id="1",
        observed_fb_id="1",
        identity_match=True,
        evidence_hash="page-hash",
        observed_at=observed_at,
    )
    duplicate = db.record_access_observation(
        1,
        source="anonymous_chromium",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id="1",
        observed_fb_id="1",
        identity_match=True,
        evidence_hash="page-hash",
        observed_at=observed_at,
    )
    assert duplicate["id"] == observation["id"]

    name = db.upsert_profile_name_candidate(
        1,
        "Correct Name",
        source="anonymous_summary",
        auth_scope="anonymous",
        trust_level=90,
        status="accepted",
        is_current=True,
        access_observation_id=observation["id"],
    )
    repeated = db.upsert_profile_name_candidate(
        1,
        "Correct   Name",
        source="anonymous_summary",
        auth_scope="anonymous",
        trust_level=90,
        status="accepted",
        is_current=True,
    )
    assert repeated["id"] == name["id"]
    assert repeated["observation_count"] == 2

    post = db.upsert_post_alias(
        1,
        canonical_post_id="post:123",
        provider="browser",
        alias_type="facebook_post_id",
        alias_value="123",
        source_url="https://facebook.com/1/posts/123",
    )
    assert db.upsert_post_alias(
        1,
        canonical_post_id="post:123",
        provider="apify",
        alias_type="facebook_post_id",
        alias_value="123",
    )["id"] == post["id"]
    media = db.upsert_media_alias(
        1,
        canonical_media_id="photo:456",
        provider="browser",
        alias_type="facebook_media_id",
        alias_value="456",
        width=200,
        height=200,
    )
    upgraded = db.upsert_media_alias(
        1,
        canonical_media_id="photo:456",
        provider="apify",
        alias_type="facebook_media_id",
        alias_value="456",
        width=1080,
        height=1080,
    )
    assert upgraded["id"] == media["id"]
    assert (upgraded["width"], upgraded["height"]) == (1080, 1080)

    limit = db.update_browser_limit(
        breaker_state="open", breaker_reason="checkpoint", blocked_until="2026-08-17T00:00:00+00:00"
    )
    assert limit["breaker_state"] == "open"
    evidence, evidence_created = db.record_browser_evidence(
        evidence_key="checkpoint:1",
        event_type="checkpoint",
        path="evidence/checkpoint.webp",
        sha256="sha",
        captured_at=observed_at,
        expires_at="2027-02-12T00:00:00+00:00",
        profile_id=1,
        access_observation_id=observation["id"],
        size_bytes=1234,
    )
    same_evidence, created_again = db.record_browser_evidence(
        evidence_key="checkpoint:1",
        event_type="checkpoint",
        path="evidence/checkpoint.webp",
        sha256="sha",
        captured_at=observed_at,
        expires_at="2027-02-12T00:00:00+00:00",
        profile_id=1,
    )
    assert evidence_created is True
    assert created_again is False
    assert same_evidence["id"] == evidence["id"]
