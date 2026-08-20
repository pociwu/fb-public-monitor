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
    canonical_request_hash,
)


def add_profile(db: Database, profile_id: int = 1, *, frozen: int = 0) -> None:
    db.execute(
        """INSERT INTO profiles(id,name,url,apify_frozen,created_at,updated_at)
        VALUES(?,?,?,?,'now','now')""",
        (profile_id, f"FB-{profile_id}", f"https://facebook.com/{profile_id}", frozen),
    )


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
        lambda: left.claim_paid_source_batch_launch(batch["id"], lease_owner="left"),
        lambda: right.claim_paid_source_batch_launch(batch["id"], lease_owner="right"),
    )

    assert sum(claimed for _, claimed in results) == 1
    row = left.row(
        "SELECT status,run_id,lease_owner FROM paid_source_batches WHERE id=?", (batch["id"],)
    )
    assert row["status"] == "launching"
    assert row["run_id"] is None
    assert row["lease_owner"] in {"left", "right"}


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
    assert second_claim["max_charge_usd"] == pytest.approx(0.004)


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
