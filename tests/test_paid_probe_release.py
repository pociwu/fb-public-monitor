import hashlib
from pathlib import Path

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage, StartedActor
from fb_monitor.capture_v2 import canonical_input_json
from fb_monitor.config import load_settings
from fb_monitor.service import BudgetExceeded, MonitorService


def make_service(tmp_path: Path, monkeypatch) -> MonitorService:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {(tmp_path / 'data').as_posix()}
  low_disk_gb: 0
budget:
  monthly_usd: 5
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
capture_v2:
  enabled: true
  special_profile_id: "100"
  special_capture_reserve_usd: 4
actors:
  posts_v2_primary: test/posts-v2
  posts_v2_fallback: test/posts-v2-fallback
  posts_input:
    profileUrls: "{{profile_url}}"
    resultsLimit: "{{max_posts}}"
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CAPTURE_V2_ENABLED", "1")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    monkeypatch.setenv("FACEBOOK_BROWSER_DATA_DIR", str(tmp_path / "browser-data"))
    return MonitorService(load_settings(config))


def pass_exact_contract(service: MonitorService) -> dict:
    mapping_hash = hashlib.sha256(
        canonical_input_json(
            service._posts_v2_contract_mapping(service.settings.actors.posts_v2_primary)
        ).encode("utf-8")
    ).hexdigest()
    return service.db.upsert_actor_contract(
        provider="apify",
        actor_id=service.settings.actors.posts_v2_primary,
        purpose="posts_backfill",
        schema_fingerprint=service._posts_v2_fingerprint(),
        input_mapping_hash=mapping_hash,
        status="passed",
        evidence={"test": True},
    )


def allow_budget(service: MonitorService, *, used: float) -> None:
    async def monthly_usage() -> MonthlyUsage:
        return MonthlyUsage(
            used,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    service.apify.monthly_usage = monthly_usage


@pytest.mark.asyncio
async def test_access_probe_keeps_special_capture_reserve_before_public_transition(
    tmp_path: Path, monkeypatch
) -> None:
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service, used=0.996)
    launches = 0

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("the $4 full-history reserve must remain unavailable")

    service.apify.start = start

    with pytest.raises(BudgetExceeded, match="保留額不足"):
        await service._capture_v2_apify_probe(
            service.db.row("SELECT * FROM profiles WHERE id=1")
        )

    assert launches == 0
    assert service.db.row("SELECT COUNT(*) count FROM paid_access_probe_batches")[
        "count"
    ] == 0


@pytest.mark.asyncio
async def test_existing_prepared_probe_is_clamped_to_current_budget_before_launch(
    tmp_path: Path, monkeypatch
) -> None:
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service, used=0.1)
    original_claim = service.db.claim_paid_access_probe_launch

    def stop_before_launch(batch_id, **kwargs):
        raise SystemExit("simulated exit with a prepared reservation")

    service.db.claim_paid_access_probe_launch = stop_before_launch
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    with pytest.raises(SystemExit):
        await service._capture_v2_apify_probe(profile)

    prepared = service.db.row("SELECT * FROM paid_access_probe_batches")
    assert prepared["status"] == "prepared"
    assert prepared["max_charge_usd"] == pytest.approx(0.01)

    service.db.claim_paid_access_probe_launch = original_claim
    allow_budget(service, used=0.994)
    launched_caps: list[float] = []

    async def start(actor_id, payload, max_charge_usd=None):
        launched_caps.append(float(max_charge_usd))
        return StartedActor("run-clamped", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [{"postId": "p1", "postUrl": "https://facebook.com/100/posts/p1"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
            charged_usd=0.005,
        )

    service.apify.start = start
    service.apify.finish = finish
    await service._capture_v2_apify_probe(profile)

    assert launched_caps == [pytest.approx(0.006)]
    assert service.db.row("SELECT max_charge_usd FROM paid_access_probe_batches")[
        "max_charge_usd"
    ] == pytest.approx(0.006)


@pytest.mark.asyncio
async def test_reconciled_probe_finishes_existing_run_without_repurchase(
    tmp_path: Path, monkeypatch
) -> None:
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    allow_budget(service, used=0.1)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    payload = service._capture_v2_posts_payload(
        profile,
        actor_id=contract["actor_id"],
        maximum=1,
        cursor=None,
        known_post_ids=[],
    )
    batch, _ = service.db.prepare_paid_access_probe_batch(
        profile_id=1,
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        observation_window="manual-reconcile-window",
        normalized_input=payload,
        max_charge_usd=0.01,
    )
    service.db.transition_paid_access_probe_batch(batch["id"], "launching")
    service.db.transition_paid_access_probe_batch(batch["id"], "needs_reconcile")
    service.db.reconcile_paid_access_probe_batch(
        batch["id"], run_id="already-paid", dataset_id="dataset"
    )
    starts = 0

    async def start(*args, **kwargs):
        nonlocal starts
        starts += 1
        raise AssertionError("reconciliation must never purchase a second run")

    async def finish(started):
        assert started.run_id == "already-paid"
        return ActorResult(
            [{"postId": "p2", "postUrl": "https://facebook.com/100/posts/p2"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
            charged_usd=0.005,
        )

    service.apify.start = start
    service.apify.finish = finish
    item = await service._capture_v2_apify_probe(profile)

    assert item["postId"] == "p2"
    assert starts == 0
    assert service.db.row("SELECT status FROM paid_access_probe_batches")[
        "status"
    ] == "committed"


def test_raw_save_fsyncs_file_and_parent_directory(tmp_path: Path, monkeypatch) -> None:
    service = make_service(tmp_path, monkeypatch)
    synced: list[int] = []
    closed: list[int] = []
    directory_fd = 987654

    monkeypatch.setattr("fb_monitor.service.os.fsync", lambda fd: synced.append(fd))
    monkeypatch.setattr(
        "fb_monitor.service.os.open", lambda path, flags: directory_fd
    )
    monkeypatch.setattr("fb_monitor.service.os.close", lambda fd: closed.append(fd))
    batch = {
        "request_hash": "a" * 64,
        "actor_id": "test/posts-v2",
        "created_at": "2026-08-20T00:00:00+00:00",
    }

    path, digest = service._save_capture_v2_raw(
        batch,
        ActorResult([], {"profiles": []}, "run-fsync", charged_usd=0),
    )

    assert path.is_file()
    assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert len(synced) == 2
    assert synced[-1] == directory_fd
    assert closed == [directory_fd]
