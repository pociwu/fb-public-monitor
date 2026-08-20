import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage, StartedActor
from fb_monitor.capture_v2 import canonical_input_json
from fb_monitor.config import load_settings
from fb_monitor.normalize import normalize_url
from fb_monitor.serpapi import SerpApiAccount, SerpApiProfileResult
from fb_monitor.service import ApifyFrozen, BudgetExceeded, MonitorService


def make_service(tmp_path: Path, monkeypatch, *, special_profile_id: str = "100") -> MonitorService:
    tmp_path.mkdir(parents=True, exist_ok=True)
    data_dir = (tmp_path / "data").as_posix()
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {data_dir}
  low_disk_gb: 0
budget:
  monthly_usd: 5
schedule:
  spacing_min_minutes: 0
  spacing_max_minutes: 0
capture_v2:
  enabled: true
  special_profile_id: "{special_profile_id}"
  special_capture_reserve_usd: 4
  contract_test_budget_usd: 0.20
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


def capture_scope(service: MonitorService) -> tuple[dict, dict, dict]:
    service.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")
    confirm_public_access(service)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    epoch = service._ensure_capture_v2_epoch(profile, "test_public_transition")
    coverage = service.db.row(
        """SELECT * FROM coverage_streams WHERE epoch_id=?
        AND stream='posts' AND surface='timeline_posts'""",
        (epoch["id"],),
    )
    return profile, epoch, coverage


def allow_budget(service: MonitorService, *, used: float = 0.1) -> None:
    async def monthly_usage():
        return MonthlyUsage(
            used,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    service.apify.monthly_usage = monthly_usage


def authorize_contract_test(
    service: MonitorService, *, actor_id: str | None = None
) -> dict:
    actor_id = actor_id or service.settings.actors.posts_v2_primary
    grant = service.db.create_contract_test_grant(max_usd=0.20, authorized_by="test")
    job_id, created, _ = service.db.queue_contract_test_job(
        grant_id=int(grant["id"]),
        profile_id=1,
        actor_id=actor_id,
        schema_fingerprint=service._posts_v2_fingerprint(actor_id),
        fixture_ack=True,
    )
    assert created is True
    service.db.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))
    job = service.db.row("SELECT payload_json FROM jobs WHERE id=?", (job_id,))
    return json.loads(job["payload_json"])


@pytest.mark.asyncio
async def test_capture_v2_post_identity_merges_stable_id_across_url_aliases(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)

    share = {
        "postId": "178",
        "postUrl": "https://www.facebook.com/share/p/rotating-token/?mibextid=test",
        "text": "same post",
    }
    permalink = {
        "storyFbid": "178",
        "postUrl": "https://www.facebook.com/permalink.php?story_fbid=178&id=100",
        "text": "same post",
    }
    without_url = {
        "sourcePostId": "178",
        "text": "same post",
    }
    url_alias_only = {
        "postUrl": "https://m.facebook.com/share/p/rotating-token/?ref=bookmarks",
        "text": "same post",
    }

    first = await service._ingest_capture_v2_posts(1, [share], notify=False)
    alias_only = await service._ingest_capture_v2_posts(1, [url_alias_only], notify=False)
    second = await service._ingest_capture_v2_posts(1, [permalink], notify=False)
    third = await service._ingest_capture_v2_posts(1, [without_url], notify=False)

    entities = service.db.rows(
        "SELECT id,external_id FROM entities WHERE profile_id=1 AND kind='post'"
    )
    aliases = service.db.rows(
        "SELECT alias_type,alias_value,canonical_post_id,entity_id "
        "FROM post_aliases WHERE profile_id=1 ORDER BY alias_type,alias_value"
    )

    assert first == {
        "identities": ["178"],
        "seen": 1,
        "new": 1,
        "updated": 0,
        "duplicate": 0,
    }
    assert alias_only["new"] == 0
    assert second["new"] == 0
    assert third["new"] == 0
    assert entities == [{"id": entities[0]["id"], "external_id": "178"}]
    assert {row["entity_id"] for row in aliases} == {entities[0]["id"]}
    assert {
        (row["alias_type"], row["alias_value"])
        for row in aliases
    } >= {
        ("facebook_post_id", "178"),
        ("facebook_post_id", "rotating-token"),
        ("source_url", "https://www.facebook.com/share/p/rotating-token"),
        (
            "source_url",
            normalize_url(permalink["postUrl"]),
        ),
    }
    assert all(not row["canonical_post_id"].startswith("post:") for row in aliases)


def test_priority_queue_releases_due_ordinary_job_after_four_priority_jobs(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    now = datetime.now(UTC)
    for index in range(4):
        job_id = service._enqueue(1, "detect_public_v2", -400, now)
        service.db.execute(
            "UPDATE jobs SET status='done',started_at=?,finished_at=? WHERE id=?",
            (
                (now - timedelta(minutes=4 - index)).isoformat(),
                (now - timedelta(minutes=4 - index)).isoformat(),
                job_id,
            ),
        )
    service._enqueue(1, "detect_public_v2", -400, now)
    ordinary_id = service._enqueue(1, "visit", 10, now)

    selected = service._select_next_job((now + timedelta(seconds=1)).isoformat())

    assert selected["id"] == ordinary_id
    assert selected["priority"] == 10


@pytest.mark.asyncio
async def test_two_services_atomically_claim_one_pending_job(tmp_path: Path, monkeypatch):
    left = make_service(tmp_path, monkeypatch)
    right = make_service(tmp_path, monkeypatch)
    job_id = left._enqueue(1, "detect_public_v2", 0, datetime.now(UTC))
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[int] = []

    async def detect(profile_id: int):
        calls.append(profile_id)
        entered.set()
        await release.wait()

    left.detect_public_v2 = detect
    right.detect_public_v2 = detect

    winner = asyncio.create_task(left._run_next_job())
    await asyncio.wait_for(entered.wait(), timeout=5)
    await right._run_next_job()
    release.set()
    await asyncio.wait_for(winner, timeout=5)

    assert calls == [1]
    job = left.db.row(
        "SELECT status,attempts,lease_owner FROM jobs WHERE id=?", (job_id,)
    )
    assert job["status"] == "done"
    assert job["attempts"] == 1
    assert job["lease_owner"] == left.worker_id


@pytest.mark.asyncio
async def test_two_services_launch_one_contract_run_at_most_once(tmp_path: Path, monkeypatch):
    left = make_service(tmp_path, monkeypatch)
    right = make_service(tmp_path, monkeypatch)
    allow_budget(left)
    allow_budget(right)
    authorization = authorize_contract_test(left)
    contract = left.db.upsert_actor_contract(
        provider="apify",
        actor_id=left.settings.actors.posts_v2_primary,
        purpose="posts_backfill",
        schema_fingerprint=left._posts_v2_fingerprint(),
        status="pending",
        evidence={"test_generation": authorization["contract_test_id"]},
    )
    profile = left.db.row("SELECT * FROM profiles WHERE id=1")
    actor_payload = {
        "profileUrls": [profile["url"]],
        "maxPostsPerProfile": 10,
        "omitPinnedPosts": True,
        "expandAllPhotos": True,
    }
    entered = asyncio.Event()
    release = asyncio.Event()
    starts = 0

    async def start(actor_id, payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        entered.set()
        await release.wait()
        return StartedActor("single-run", "single-dataset", "store")

    async def finish(started):
        return ActorResult(
            [],
            {
                "profiles": [
                    {
                        "status": "succeeded",
                        "profileId": "100",
                        "coverageStatus": "complete",
                        "hasNextPage": False,
                    }
                ]
            },
            started.run_id,
            charged_usd=0.01,
        )

    for service in (left, right):
        service.apify.start = start
        service.apify.finish = finish

    winner = asyncio.create_task(
        left._run_capture_v2_contract_case(
            contract=contract,
            profile=profile,
            test_generation=authorization["contract_test_id"],
            test_case="page_1",
            payload=actor_payload,
            max_charge_usd=0.05,
            grant_allocation_id=authorization["contract_allocation_id"],
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    with pytest.raises(RuntimeError, match="worker"):
        await right._run_capture_v2_contract_case(
            contract=contract,
            profile=profile,
            test_generation=authorization["contract_test_id"],
            test_case="page_1",
            payload=actor_payload,
            max_charge_usd=0.05,
            grant_allocation_id=authorization["contract_allocation_id"],
        )
    release.set()
    result = await asyncio.wait_for(winner, timeout=5)

    assert result.run_id == "single-run"
    assert starts == 1
    run = left.db.row("SELECT status,run_id FROM contract_runs")
    assert run == {"status": "succeeded", "run_id": "single-run"}


@pytest.mark.asyncio
async def test_two_services_launch_one_paid_source_batch_at_most_once(
    tmp_path: Path, monkeypatch
):
    left = make_service(tmp_path, monkeypatch)
    right = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(left)
    left.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")
    confirm_public_access(left)
    epoch, _ = left.db.get_or_create_capture_epoch(
        1,
        "race",
        status="ready",
        scope={"capture_intent": "initial_public_capture", "all_public_history": True},
    )
    coverage = left.db.upsert_coverage_stream(
        epoch["id"],
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=contract["id"],
    )
    payload = {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    budget_gate = asyncio.Event()
    arrivals = 0
    starts = 0

    async def available():
        nonlocal arrivals
        arrivals += 1
        if arrivals == 2:
            budget_gate.set()
        await budget_gate.wait()
        return 4.9, MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("one-paid-run", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [
                {
                    "postId": "post-1",
                    "postUrl": "https://facebook.com/100/posts/post-1",
                    "publishedAt": "2026-08-20T00:00:00+00:00",
                }
            ],
            {
                "profiles": [
                    {
                        "status": "succeeded",
                        "profileId": "100",
                        "coverageStatus": "complete",
                        "hasNextPage": False,
                    }
                ]
            },
            started.run_id,
            charged_usd=0.01,
        )

    for service in (left, right):
        service._official_available = available
        service.apify.start = start
        service.apify.finish = finish

    await asyncio.gather(
        left.capture_posts_v2(1, payload),
        right.capture_posts_v2(1, payload),
    )

    assert starts == 1
    batch = left.db.row("SELECT status,run_id FROM paid_source_batches")
    assert batch == {"status": "committed", "run_id": "one-paid-run"}


def test_capture_raw_cleanup_runs_through_daily_service_hook(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    calls = []

    def cleanup(db, data_dir):
        calls.append((db, data_dir))
        return SimpleNamespace(errors=0)

    monkeypatch.setattr("fb_monitor.service.cleanup_capture_raw", cleanup)

    service._cleanup_capture_raw()

    assert calls == [(service.db, service.settings.data_dir)]
    assert service._capture_raw_cleanup_date == datetime.now(UTC).date().isoformat()


def confirm_public_access(service: MonitorService, profile_id: int = 1) -> dict:
    profile = service.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
    target = service._capture_v2_target_id(profile)
    return service.db.record_access_observation(
        profile_id,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id=target,
        observed_fb_id=target,
        identity_match=True,
        evidence_summary={"classification": "strong_public"},
        observation_key=f"confirmed-public:{profile_id}",
    )


@pytest.mark.asyncio
async def test_detection_only_suspects_public_and_enqueues_anonymous_verification(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setenv("SERPAPI_KEY", "test")
    service.settings.serpapi_key = "test"

    async def serp_profile(url):
        return SerpApiProfileResult(
            {"id": "100", "url": url, "name": "Watched"},
            SerpApiAccount("Free", 250, 249, 1, "2026-09-01", 1, 50),
        )

    service.serpapi.profile = serp_profile

    await service.detect_public_v2(1)

    observation = service.db.row(
        "SELECT * FROM access_observations WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    assert observation["verdict"] == "suspected_public"
    assert service.db.row("SELECT public_state FROM profiles WHERE id=1")["public_state"] == "unknown"
    assert service.db.row(
        "SELECT status FROM jobs WHERE profile_id=1 AND job_type='verify_public_v2'"
    )["status"] == "pending"


@pytest.mark.asyncio
async def test_only_cookie_free_browser_can_confirm_public_and_epoch_is_unique(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    anonymous_calls = 0

    async def anonymous_profile(url, diagnostic_key=None):
        nonlocal anonymous_calls
        anonymous_calls += 1
        return {
            "id": "100",
            "url": url,
            "name": "Watched",
            "private": False,
            "public_content_proof": {
                "kind": "target_permalink_article",
                "permalink": (
                    "https://www.facebook.com/permalink.php?"
                    "story_fbid=pfbid0PublicPost&id=100"
                ),
                "post_identity": "pfbid0PublicPost",
                "target_identity": "100",
                "article_index": 0,
            },
        }

    async def logged_profile(*args, **kwargs):
        raise AssertionError("logged-in browser must not confirm public access")

    service.facebook_anonymous_browser.profile = anonymous_profile
    service.facebook_browser.profile = logged_profile

    await service.verify_public_v2(1)
    await service.verify_public_v2(1)

    assert anonymous_calls == 2
    assert service.db.row("SELECT public_state FROM profiles WHERE id=1")["public_state"] == "public"
    assert service.db.row(
        "SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1 AND is_active=1"
    )["count"] == 1

    coverage = service.db.row(
        "SELECT * FROM coverage_streams WHERE stream='posts' AND surface='timeline_posts'"
    )
    service.db.update_coverage_stream(coverage["id"], status="in_progress")
    service.db.update_coverage_stream(
        coverage["id"], status="complete", terminal_evidence_json={"source": "test"}
    )
    service.db.execute("DELETE FROM jobs")
    service._enqueue_due_special_detection()
    assert service.db.row("SELECT COUNT(*) count FROM jobs")["count"] == 0
    assert service.db.row(
        "SELECT verdict FROM access_observations WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )["verdict"] == "confirmed_public"


@pytest.mark.asyncio
async def test_anonymous_name_only_profile_does_not_confirm_public_or_create_epoch(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)

    async def anonymous_name_only(url, diagnostic_key=None):
        return {
            "id": "100",
            "url": url,
            "name": "Watched",
            "private": False,
            "profile_picture": "https://scontent.example.fbcdn.net/avatar.jpg",
            "followers": "34",
        }

    service.facebook_anonymous_browser.profile = anonymous_name_only

    await service.verify_public_v2(1)

    observation = service.db.row(
        "SELECT verdict,evidence_summary_json FROM access_observations "
        "WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    evidence = json.loads(observation["evidence_summary_json"])
    assert observation["verdict"] == "unknown"
    assert evidence["classification"] == "indeterminate"
    assert evidence["signal"] == "parse_error"
    assert evidence["error"] == "anonymous page lacks target public-content proof"
    assert service.db.row("SELECT public_state FROM profiles WHERE id=1")["public_state"] == "unknown"
    assert service.db.row(
        "SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1"
    )["count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_identity", ["", "999"])
async def test_anonymous_private_marker_requires_observed_target_identity(
    tmp_path: Path, monkeypatch, observed_identity: str
):
    service = make_service(tmp_path, monkeypatch)

    async def anonymous_private(url, diagnostic_key=None):
        item = {
            "id": "100",  # display fallback copied from the request target
            "url": url,
            "name": "Restricted page",
            "private": True,
        }
        if observed_identity:
            item["observed_profile_identity"] = observed_identity
        return item

    service.facebook_anonymous_browser.profile = anonymous_private

    await service.verify_public_v2(1)
    await service.verify_public_v2(1)

    observation = service.db.row(
        "SELECT verdict,evidence_summary_json FROM access_observations "
        "WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    assert observation["verdict"] == "unknown"
    assert json.loads(observation["evidence_summary_json"])["classification"] == "invalid_identity"
    assert service.db.row("SELECT public_state FROM profiles WHERE id=1")["public_state"] == "unknown"


@pytest.mark.asyncio
async def test_anonymous_private_marker_matching_observed_identity_is_strong(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)

    async def anonymous_private(url, diagnostic_key=None):
        return {
            "id": "100",
            "observed_profile_identity": "100",
            "observed_profile_url": "https://www.facebook.com/100",
            "url": url,
            "name": "Watched",
            "private": True,
        }

    service.facebook_anonymous_browser.profile = anonymous_private

    await service.verify_public_v2(1)

    observation = service.db.row(
        "SELECT verdict,evidence_summary_json FROM access_observations "
        "WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    assert observation["verdict"] == "suspected_private"
    assert json.loads(observation["evidence_summary_json"])["classification"] == "strong_private"


@pytest.mark.asyncio
async def test_anonymous_public_permalink_still_requires_matching_profile_identity(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)

    async def wrong_identity(url, diagnostic_key=None):
        return {
            "id": "999",
            "url": "https://www.facebook.com/999",
            "name": "Wrong profile",
            "private": False,
            "public_content_proof": {
                "kind": "target_permalink_article",
                "permalink": (
                    "https://www.facebook.com/permalink.php?"
                    "story_fbid=pfbid0PublicPost&id=100"
                ),
                "post_identity": "pfbid0PublicPost",
                "target_identity": "100",
                "article_index": 0,
            },
        }

    service.facebook_anonymous_browser.profile = wrong_identity

    await service.verify_public_v2(1)

    evidence = service.db.row(
        "SELECT verdict,evidence_summary_json FROM access_observations "
        "WHERE profile_id=1 ORDER BY id DESC LIMIT 1"
    )
    assert evidence["verdict"] == "unknown"
    assert json.loads(evidence["evidence_summary_json"])["classification"] == "invalid_identity"
    assert service.db.row(
        "SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1"
    )["count"] == 0


@pytest.mark.asyncio
async def test_capture_posts_persists_raw_before_import_and_commits_explicit_terminal(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)
    starts: list[dict] = []
    transitions: list[str] = []
    original_claim = service.db.claim_paid_source_batch_launch
    original_transition = service.db.transition_paid_source_batch
    original_ingest = service.ingester.ingest

    def claim(batch_id, *args, **kwargs):
        row, claimed = original_claim(batch_id, *args, **kwargs)
        if claimed:
            transitions.append("launching")
        return row, claimed

    def transition(batch_id, status, **kwargs):
        transitions.append(status)
        return original_transition(batch_id, status, **kwargs)

    async def start(actor_id, payload, max_charge_usd=None):
        starts.append(payload)
        return StartedActor("run-1", "dataset-1", "store-1")

    async def finish(started):
        return ActorResult(
            [
                {"postId": "p1", "postUrl": "https://facebook.com/100/posts/p1", "text": "one"},
                {"postId": "p2", "postUrl": "https://facebook.com/100/posts/p2", "text": "two"},
            ],
            {"profiles": [{"profileId": "100", "status": "succeeded", "coverageStatus": "complete", "hasNextPage": False}]},
            started.run_id,
            charged_usd=0.01,
        )

    async def checked_ingest(*args, **kwargs):
        batch = service.db.row("SELECT * FROM paid_source_batches ORDER BY id DESC LIMIT 1")
        assert batch["status"] == "raw_saved"
        assert Path(batch["raw_path"]).is_file()
        return await original_ingest(*args, **kwargs)

    service.db.claim_paid_source_batch_launch = claim
    service.db.transition_paid_source_batch = transition
    service.apify.start = start
    service.apify.finish = finish
    service.ingester.ingest = checked_ingest

    await service.capture_posts_v2(
        1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    )

    assert transitions == ["launching", "run_started", "raw_saved", "imported", "committed"]
    assert starts[0]["profileUrls"] == ["https://www.facebook.com/100"]
    assert starts[0]["maxPostsPerProfile"] == 50
    assert starts[0]["expandAllPhotos"] is True
    assert starts[0]["omitPinnedPosts"] is True
    assert starts[0]["knownPostIds"] == []
    assert "startUrls" not in starts[0]
    assert "maxPosts" not in starts[0]
    assert "startCursor" not in starts[0]
    batch = service.db.row("SELECT * FROM paid_source_batches ORDER BY id DESC LIMIT 1")
    assert batch["status"] == "committed"
    assert batch["request_hash"]
    assert Path(batch["raw_path"]).is_file()
    complete = service.db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert complete["status"] == "complete"
    assert json.loads(complete["terminal_evidence_json"])["source"] == "SUMMARY"
    assert service.db.row("SELECT COUNT(*) count FROM entities WHERE kind='post'")["count"] == 2


@pytest.mark.asyncio
async def test_upgrade_recovery_crosses_existing_recent_posts_without_known_boundary(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    service.db.execute(
        """INSERT INTO entities(
        profile_id,kind,external_id,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','already-known',1,?,?)""",
        ("2026-08-01T00:00:00+00:00", "2026-08-01T00:00:00+00:00"),
    )
    service.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")
    confirm_public_access(service)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    epoch = service._ensure_capture_v2_epoch(profile, "upgrade_recovery")
    coverage = service.db.row(
        """SELECT * FROM coverage_streams WHERE epoch_id=?
        AND stream='posts' AND surface='timeline_posts'""",
        (epoch["id"],),
    )
    allow_budget(service)
    launched: list[dict] = []

    async def start(actor_id, actor_payload, max_charge_usd=None):
        launched.append(actor_payload)
        return StartedActor("run-recovery", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [],
            {"profiles": [{"profileId": "100", "coverageStatus": "no_public_posts"}]},
            started.run_id,
        )

    service.apify.start = start
    service.apify.finish = finish

    await service.capture_posts_v2(
        1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    )

    assert service._capture_v2_known_post_ids(1) == ["already-known"]
    assert launched[0]["knownPostIds"] == []
    batch = service.db.row("SELECT * FROM paid_source_batches ORDER BY id DESC LIMIT 1")
    assert batch["intent"] == "recovery_capture"


@pytest.mark.asyncio
async def test_access_probe_never_sends_known_post_ids(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    service.db.execute(
        """INSERT INTO entities(
        profile_id,kind,external_id,present,first_seen_at,last_seen_at
        ) VALUES(1,'post','newest-known',1,?,?)""",
        ("2026-08-01T00:00:00+00:00", "2026-08-01T00:00:00+00:00"),
    )
    allow_budget(service)
    launched: list[dict] = []

    async def start(actor_id, actor_payload, max_charge_usd=None):
        launched.append(actor_payload)
        return StartedActor("run-probe", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [{"postId": "newest-known", "postUrl": "https://facebook.com/100/posts/newest-known"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
        )

    service.apify.start = start
    service.apify.finish = finish

    await service._capture_v2_apify_probe(service.db.row("SELECT * FROM profiles WHERE id=1"))

    assert launched[0]["knownPostIds"] == []
    assert launched[0]["maxPostsPerProfile"] == 1


@pytest.mark.asyncio
async def test_access_probe_replays_committed_raw_in_same_window_without_repurchase(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service)
    starts = 0
    finishes = 0

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("run-probe-replay", "dataset", "store")

    async def finish(started):
        nonlocal finishes
        finishes += 1
        return ActorResult(
            [{"postId": "probe-1", "postUrl": "https://facebook.com/100/posts/probe-1"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
            charged_usd=0.005,
        )

    service.apify.start = start
    service.apify.finish = finish
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    first = await service._capture_v2_apify_probe(profile)
    replay = await service._capture_v2_apify_probe(profile)

    assert first == replay
    assert starts == 1
    assert finishes == 1
    batch = service.db.row("SELECT * FROM paid_access_probe_batches")
    assert batch["status"] == "committed"
    assert Path(batch["raw_path"]).is_file()
    assert service.db.row("SELECT COUNT(*) count FROM paid_access_probe_batches")["count"] == 1


@pytest.mark.asyncio
async def test_access_probe_rejects_wrong_profile_before_committing_signal(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service)

    async def start(actor_id, actor_payload, max_charge_usd=None):
        return StartedActor("run-probe-wrong-profile", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [
                {
                    "id": "post-not-profile-id",
                    "postId": "probe-wrong",
                    "postUrl": "https://facebook.com/999/posts/probe-wrong",
                    "authorId": "999",
                }
            ],
            {
                "profiles": [
                    {
                        "status": "succeeded",
                        "profileId": "999",
                        "profileUrl": "https://www.facebook.com/999",
                    }
                ]
            },
            started.run_id,
            charged_usd=0.005,
        )

    service.apify.start = start
    service.apify.finish = finish
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    with pytest.raises(RuntimeError, match="目標帳號身分不符"):
        await service._capture_v2_apify_probe(profile)

    batch = service.db.row("SELECT * FROM paid_access_probe_batches")
    assert batch["status"] == "import_failed"
    assert Path(batch["raw_path"]).is_file()


@pytest.mark.asyncio
async def test_access_probe_lost_start_response_becomes_reconcile_without_repurchase(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service)
    starts = 0
    original_transition = service.db.transition_paid_access_probe_batch
    crash_once = True

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("run-response-lost", "dataset", "store")

    def transition(batch_id, status, **kwargs):
        nonlocal crash_once
        if status == "run_started" and crash_once:
            crash_once = False
            raise SystemExit("simulated process exit after paid start")
        return original_transition(batch_id, status, **kwargs)

    service.apify.start = start
    service.db.transition_paid_access_probe_batch = transition
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    with pytest.raises(SystemExit):
        await service._capture_v2_apify_probe(profile)
    assert service.db.row("SELECT status FROM paid_access_probe_batches")["status"] == "launching"

    service.db.transition_paid_access_probe_batch = original_transition
    with pytest.raises(RuntimeError, match="需要對帳"):
        await service._capture_v2_apify_probe(profile)

    assert starts == 1
    assert service.db.row("SELECT status FROM paid_access_probe_batches")["status"] == "needs_reconcile"


@pytest.mark.asyncio
async def test_access_probe_resumes_persisted_run_id_after_process_exit(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service)
    starts = 0
    finishes = 0
    crash_once = True

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("run-persisted", "dataset", "store")

    async def finish(started):
        nonlocal finishes, crash_once
        finishes += 1
        if crash_once:
            crash_once = False
            raise SystemExit("simulated process exit while polling")
        return ActorResult(
            [{"postId": "probe-2", "postUrl": "https://facebook.com/100/posts/probe-2"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
            charged_usd=0.005,
        )

    service.apify.start = start
    service.apify.finish = finish
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    with pytest.raises(SystemExit):
        await service._capture_v2_apify_probe(profile)
    started_batch = service.db.row("SELECT * FROM paid_access_probe_batches")
    assert started_batch["status"] == "run_started"
    assert started_batch["run_id"] == "run-persisted"

    # Freeze and contract expiry stop new Apify launches, but must not discard
    # a run that was already paid for and has a durable run id.
    service.db.set_profile_source_control(1, "apify", frozen=True, reason="operator")
    service.db.execute("UPDATE actor_contracts SET status='expired'")

    item = await service._capture_v2_apify_probe(profile)

    assert item["postId"] == "probe-2"
    assert starts == 1
    assert finishes == 2
    assert service.db.row("SELECT status FROM paid_access_probe_batches")["status"] == "committed"


@pytest.mark.asyncio
async def test_access_probe_replays_raw_saved_before_import_without_external_call(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service)
    starts = 0
    finishes = 0
    original_transition = service.db.transition_paid_access_probe_batch
    crash_once = True

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("run-raw-replay", "dataset", "store")

    async def finish(started):
        nonlocal finishes
        finishes += 1
        return ActorResult(
            [{"postId": "probe-3", "postUrl": "https://facebook.com/100/posts/probe-3"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
            charged_usd=0.005,
        )

    def transition(batch_id, status, **kwargs):
        nonlocal crash_once
        if status == "imported" and crash_once:
            crash_once = False
            raise SystemExit("simulated process exit before import marker")
        return original_transition(batch_id, status, **kwargs)

    service.apify.start = start
    service.apify.finish = finish
    service.db.transition_paid_access_probe_batch = transition
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    with pytest.raises(SystemExit):
        await service._capture_v2_apify_probe(profile)
    raw_saved = service.db.row("SELECT * FROM paid_access_probe_batches")
    assert raw_saved["status"] == "raw_saved"
    assert Path(raw_saved["raw_path"]).is_file()

    service.db.transition_paid_access_probe_batch = original_transition
    item = await service._capture_v2_apify_probe(profile)

    assert item["postId"] == "probe-3"
    assert starts == 1
    assert finishes == 1
    assert service.db.row("SELECT status FROM paid_access_probe_batches")["status"] == "committed"


@pytest.mark.asyncio
async def test_access_probe_recovers_atomic_raw_written_before_raw_saved_marker(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    allow_budget(service)
    starts = 0
    finishes = 0
    original_transition = service.db.transition_paid_access_probe_batch
    crash_once = True

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("run-raw-marker", "dataset", "store")

    async def finish(started):
        nonlocal finishes
        finishes += 1
        return ActorResult(
            [{"postId": "probe-4", "postUrl": "https://facebook.com/100/posts/probe-4"}],
            {"profiles": [{"status": "succeeded", "profileId": "100"}]},
            started.run_id,
            charged_usd=0.005,
        )

    def transition(batch_id, status, **kwargs):
        nonlocal crash_once
        if status == "raw_saved" and crash_once:
            crash_once = False
            raise SystemExit("simulated process exit after atomic raw rename")
        return original_transition(batch_id, status, **kwargs)

    service.apify.start = start
    service.apify.finish = finish
    service.db.transition_paid_access_probe_batch = transition
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")

    with pytest.raises(SystemExit):
        await service._capture_v2_apify_probe(profile)
    run_started = service.db.row("SELECT * FROM paid_access_probe_batches")
    assert run_started["status"] == "run_started"
    raw_path = service._capture_v2_raw_path(run_started["request_hash"])
    assert raw_path.is_file()

    async def no_remote_finish(started):
        raise AssertionError("durable local raw must be replayed before polling Apify")

    service.db.transition_paid_access_probe_batch = original_transition
    service.apify.finish = no_remote_finish
    item = await service._capture_v2_apify_probe(profile)

    assert item["postId"] == "probe-4"
    assert starts == 1
    assert finishes == 1
    assert service.db.row("SELECT status FROM paid_access_probe_batches")["status"] == "committed"


@pytest.mark.asyncio
async def test_cursorless_capped_page_is_source_limited_not_complete(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)
    items = [
        {"postId": f"p{index}", "postUrl": f"https://facebook.com/100/posts/p{index}"}
        for index in range(50)
    ]

    async def start(actor_id, payload, max_charge_usd=None):
        return StartedActor("run-capped", "dataset", "store")

    async def finish(started):
        return ActorResult(
            items,
            {"profiles": [{"profileId": "100", "status": "succeeded"}]},
            started.run_id,
        )

    persist_posts = service._ingest_capture_v2_posts

    async def ingest(profile_id, raw_items, *, notify=False):
        await persist_posts(profile_id, raw_items, notify=notify)
        return {
            "identities": [f"p{index}" for index in range(50)],
            "seen": 50,
            "new": 50,
            "updated": 0,
            "duplicate": 0,
        }

    service.apify.start = start
    service.apify.finish = finish
    service._ingest_capture_v2_posts = ingest

    await service.capture_posts_v2(
        1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    )

    limited = service.db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert limited["status"] == "source_limited"
    assert "上限" in limited["limited_reason"]
    assert json.loads(limited["terminal_evidence_json"]) == {}


@pytest.mark.asyncio
async def test_repeated_cursor_and_identity_trips_paid_page_circuit_breaker(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)
    run_number = 0

    async def start(actor_id, payload, max_charge_usd=None):
        nonlocal run_number
        run_number += 1
        if run_number == 2:
            assert payload["startCursor"] == "cursor-1"
        return StartedActor(f"run-{run_number}", f"dataset-{run_number}", "store")

    async def finish(started):
        return ActorResult(
            [
                {"postId": "p1", "postUrl": "https://facebook.com/100/posts/p1"},
                {"postId": "p2", "postUrl": "https://facebook.com/100/posts/p2"},
            ],
            {"profiles": [{"profileId": "100", "status": "succeeded", "pointer": {"nextCursor": "cursor-1"}}]},
            started.run_id,
        )

    persist_posts = service._ingest_capture_v2_posts

    async def ingest(profile_id, raw_items, *, notify=False):
        await persist_posts(profile_id, raw_items, notify=notify)
        return {"identities": ["p1", "p2"], "seen": 2, "new": 0, "updated": 0, "duplicate": 2}

    service.apify.start = start
    service.apify.finish = finish
    service._ingest_capture_v2_posts = ingest

    job_payload = {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    await service.capture_posts_v2(1, job_payload)
    service.db.execute(
        "UPDATE jobs SET status='running' WHERE job_type='capture_posts_v2' AND status='pending'"
    )
    await service.capture_posts_v2(1, job_payload)

    limited = service.db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert limited["status"] == "source_limited"
    assert "same_cursor" in limited["limited_reason"]
    assert "same_identities" in limited["limited_reason"]
    assert run_number == 2


@pytest.mark.asyncio
async def test_ambiguous_launch_is_needs_reconcile_and_is_never_repurchased(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)
    calls = 0

    async def ambiguous_start(actor_id, payload, max_charge_usd=None):
        nonlocal calls
        calls += 1
        raise TimeoutError("response lost after launch")

    service.apify.start = ambiguous_start
    job_payload = {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}

    with pytest.raises(RuntimeError, match="停止自動重買"):
        await service.capture_posts_v2(1, job_payload)
    await service.capture_posts_v2(1, job_payload)

    assert calls == 1
    assert service.db.row("SELECT status FROM paid_source_batches")["status"] == "needs_reconcile"
    assert service.db.row("SELECT status FROM capture_epochs WHERE id=?", (epoch["id"],))["status"] == "needs_reconcile"


@pytest.mark.asyncio
async def test_preexisting_launching_without_run_id_becomes_needs_reconcile_without_start(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    profile, epoch, coverage = capture_scope(service)
    actor_payload = service._capture_v2_posts_payload(
        profile,
        actor_id=service.settings.actors.posts_v2_primary,
        maximum=50,
        cursor=None,
        known_post_ids=[],
    )
    batch, _ = service.db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window="test-window",
        normalized_input=actor_payload,
        request_hash="a" * 64,
    )
    service.db.transition_paid_source_batch(batch["id"], "launching", expected_status="prepared")
    calls = 0

    async def start(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("launching batch must never be relaunched")

    service.apify.start = start

    await service.capture_posts_v2(
        1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    )

    assert calls == 0
    assert service.db.row("SELECT status FROM paid_source_batches WHERE id=?", (batch["id"],))["status"] == "needs_reconcile"


@pytest.mark.asyncio
async def test_committed_batch_replays_raw_when_coverage_checkpoint_was_not_written(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)
    starts = 0

    async def start(actor_id, payload, max_charge_usd=None):
        nonlocal starts
        starts += 1
        return StartedActor("run-crash", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [{"postId": "p1", "postUrl": "https://facebook.com/100/posts/p1"}],
            {"profiles": [{"profileId": "100", "coverageStatus": "complete", "hasNextPage": False}]},
            started.run_id,
        )

    original_commit = service._commit_capture_v2_batch

    def crash_after_batch_commit(**kwargs):
        batch = kwargs["batch"]
        service.db.transition_paid_source_batch(
            batch["id"], "committed", expected_status="imported"
        )
        raise RuntimeError("simulated crash before coverage checkpoint")

    service.apify.start = start
    service.apify.finish = finish
    service._commit_capture_v2_batch = crash_after_batch_commit
    job_payload = {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}

    with pytest.raises(RuntimeError, match="simulated crash"):
        await service.capture_posts_v2(1, job_payload)
    assert service.db.row("SELECT status FROM paid_source_batches")["status"] == "committed"
    assert json.loads(
        service.db.row("SELECT provider_checkpoint_json FROM coverage_streams")["provider_checkpoint_json"]
    ) == {}

    service._commit_capture_v2_batch = original_commit
    await service.capture_posts_v2(1, job_payload)

    assert starts == 1
    assert service.db.row("SELECT status FROM coverage_streams")["status"] == "complete"


@pytest.mark.asyncio
async def test_recovered_primary_batch_reads_max_posts_per_profile(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    profile, epoch, coverage = capture_scope(service)
    actor_payload = service._capture_v2_posts_payload(
        profile,
        actor_id=service.settings.actors.posts_v2_primary,
        maximum=10,
        cursor=None,
        known_post_ids=[],
    )
    batch, _ = service.db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window="test-window",
        normalized_input=actor_payload,
        request_hash="b" * 64,
    )
    batch = service.db.transition_paid_source_batch(batch["id"], "launching")
    batch = service.db.transition_paid_source_batch(
        batch["id"],
        "run_started",
        run_id="existing-run",
        dataset_id="dataset",
        key_value_store_id="store",
    )
    raw_path, raw_hash = service._save_capture_v2_raw(
        batch,
        ActorResult(
            [],
            {"profiles": [{"profileId": "100", "status": "succeeded"}]},
            "existing-run",
        ),
    )
    batch = service.db.transition_paid_source_batch(
        batch["id"], "raw_saved", raw_path=str(raw_path), raw_sha256=raw_hash
    )
    batch = service.db.transition_paid_source_batch(batch["id"], "imported")
    service.db.transition_paid_source_batch(batch["id"], "committed")
    observed_maximum: list[int] = []

    def capture_maximum(**kwargs):
        observed_maximum.append(kwargs["maximum"])

    service._commit_capture_v2_batch = capture_maximum

    await service.capture_posts_v2(
        1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
    )

    assert observed_maximum == [10]


def test_raw_artifact_timestamp_is_deterministic_and_existing_file_is_reused(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    profile, epoch, coverage = capture_scope(service)
    actor_payload = service._capture_v2_posts_payload(
        profile,
        actor_id=contract["actor_id"],
        maximum=50,
        cursor=None,
        known_post_ids=[],
    )
    batch, _ = service.db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window="raw-window",
        normalized_input=actor_payload,
        request_hash="f" * 64,
    )
    first = ActorResult(
        [{"postId": "first", "postUrl": "https://facebook.com/100/posts/first"}],
        {"profiles": [{"profileId": "100", "pointer": {"nextCursor": "cursor"}}]},
        "run-first",
    )
    second = ActorResult(
        [{"postId": "changed", "postUrl": "https://facebook.com/100/posts/changed"}],
        {"profiles": [{"profileId": "100", "coverageStatus": "complete_feed_exhausted"}]},
        "run-second",
    )

    path1, digest1 = service._save_capture_v2_raw(batch, first)
    path2, digest2 = service._save_capture_v2_raw(batch, second)
    saved = service._load_capture_v2_raw(path1)

    assert path1 == path2
    assert digest1 == digest2
    assert saved["saved_at"] == batch["created_at"]
    assert [item["postId"] for item in saved["items"]] == ["first"]
    if os.name != "nt":
        assert path1.stat().st_mode & 0o777 == 0o600
        assert path1.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
async def test_public_confirmation_without_contract_creates_awaiting_epoch_but_no_paid_job(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)

    async def anonymous_profile(url, diagnostic_key=None):
        return {
            "id": "100",
            "url": url,
            "name": "Watched",
            "private": False,
            "public_content_proof": {
                "kind": "target_permalink_article",
                "permalink": (
                    "https://www.facebook.com/permalink.php?"
                    "story_fbid=pfbid0PublicPost&id=100"
                ),
                "post_identity": "pfbid0PublicPost",
                "target_identity": "100",
                "article_index": 0,
            },
        }

    service.facebook_anonymous_browser.profile = anonymous_profile

    await service.verify_public_v2(1)

    epoch = service.db.row("SELECT * FROM capture_epochs WHERE profile_id=1")
    assert epoch["status"] == "awaiting_contract"
    assert service.db.row(
        "SELECT COUNT(*) count FROM jobs WHERE job_type='capture_posts_v2'"
    )["count"] == 0
    assert service.facebook_anonymous_browser.data_dir == service.settings.data_dir / "anonymous-browser-data"


@pytest.mark.asyncio
async def test_deploy_maintenance_flag_stops_scheduler_before_enqueue_or_dequeue(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.deploy_maintenance_flag.parent.mkdir(parents=True, exist_ok=True)
    service.settings.deploy_maintenance_flag.write_text("drain", encoding="utf-8")
    calls: list[str] = []

    class StopAfterOneWait:
        stopped = False

        def is_set(self):
            return self.stopped

        async def wait(self):
            self.stopped = True

    service.stop_event = StopAfterOneWait()
    service._enqueue_due_visits = lambda: calls.append("visits")
    service._enqueue_due_special_detection = lambda: calls.append("detect")

    async def run_next():
        calls.append("dequeue")

    service._run_next_job = run_next

    await service._scheduler_loop()

    assert calls == []


def test_confirmed_public_special_resumes_capture_without_requeueing_detection(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    service.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")
    confirm_public_access(service)

    service._enqueue_due_special_detection()
    service._enqueue_due_special_detection()

    assert service.db.row(
        """SELECT COUNT(*) count FROM jobs
        WHERE job_type IN ('detect_public_v2','verify_public_v2')
        AND status IN ('pending','running')"""
    )["count"] == 0
    assert service.db.row(
        """SELECT COUNT(*) count FROM jobs WHERE job_type='capture_posts_v2'
        AND status IN ('pending','running')"""
    )["count"] == 1
    assert service.db.row(
        "SELECT COUNT(*) count FROM capture_epochs WHERE profile_id=1 AND is_active=1"
    )["count"] == 1

    coverage = service.db.row(
        "SELECT * FROM coverage_streams WHERE stream='posts' AND surface='timeline_posts'"
    )
    service.db.update_coverage_stream(coverage["id"], status="in_progress")
    service.db.update_coverage_stream(
        coverage["id"], status="complete", terminal_evidence_json={"source": "test"}
    )
    service.db.execute("DELETE FROM jobs")
    service._enqueue_due_special_detection()
    assert service.db.row("SELECT COUNT(*) count FROM jobs")["count"] == 0


def test_legacy_public_flag_without_strong_observation_only_schedules_detection(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    service.db.execute("UPDATE profiles SET public_state='public' WHERE id=1")

    service._seed_capture_v2()

    assert service._has_confirmed_public_observation(1) is False
    assert service.db.row("SELECT COUNT(*) count FROM capture_epochs")["count"] == 0
    assert service.db.row(
        """SELECT COUNT(*) count FROM jobs WHERE job_type='detect_public_v2'
        AND status='pending'"""
    )["count"] == 1


def test_newer_strong_private_observation_revokes_older_public_authorization(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    confirm_public_access(service)
    service.db.record_access_observation(
        1,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_private",
        target_fb_id="100",
        observed_fb_id="100",
        identity_match=True,
        evidence_summary={"classification": "strong_private"},
        observation_key="newer-confirmed-private",
    )

    assert service._has_confirmed_public_observation(1) is False


@pytest.mark.asyncio
async def test_paid_capture_fails_closed_without_confirmed_public_observation(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    epoch, _ = service.db.get_or_create_capture_epoch(1, "legacy_public", status="ready")
    coverage = service.db.upsert_coverage_stream(
        epoch["id"],
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=contract["id"],
    )
    launches = 0

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("unverified legacy public state must not launch")

    service.apify.start = start

    with pytest.raises(RuntimeError, match="confirmed_public"):
        await service.capture_posts_v2(
            1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
        )

    assert launches == 0
    assert service.db.row(
        "SELECT status FROM coverage_streams WHERE id=?", (coverage["id"],)
    )["status"] == "manual_paused"


@pytest.mark.asyncio
async def test_restart_requeues_run_started_capture_and_finishes_without_new_launch(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    profile, epoch, coverage = capture_scope(service)
    actor_payload = service._capture_v2_posts_payload(
        profile,
        actor_id=service.settings.actors.posts_v2_primary,
        maximum=50,
        cursor=None,
        known_post_ids=[],
    )
    batch, _ = service.db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="recovery_capture",
        observation_window="restart-window",
        normalized_input=actor_payload,
        request_hash="c" * 64,
    )
    batch = service.db.transition_paid_source_batch(batch["id"], "launching")
    service.db.transition_paid_source_batch(
        batch["id"],
        "run_started",
        run_id="durable-run",
        dataset_id="dataset",
        key_value_store_id="store",
    )
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_posts_v2' ORDER BY id DESC LIMIT 1"
    )
    service.db.execute(
        "UPDATE jobs SET status='running',started_at=? WHERE id=?",
        ("2026-08-16T00:00:00+00:00", job["id"]),
    )
    launches = 0
    finishes = 0

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("durably recorded run must not launch again")

    async def finish(started):
        nonlocal finishes
        finishes += 1
        assert started.run_id == "durable-run"
        return ActorResult(
            [],
            {"profiles": [{"profileId": "100", "coverageStatus": "complete_feed_exhausted"}]},
            started.run_id,
        )

    service.apify.start = start
    service.apify.finish = finish

    recovered = service._recover_stale_capture_v2_jobs()
    await service._run_next_job()

    assert recovered == {"pending": 1, "needs_reconcile": 0}
    assert launches == 0
    assert finishes == 1
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (job["id"],))["status"] == "done"
    assert service.db.row("SELECT status FROM coverage_streams WHERE id=?", (coverage["id"],))["status"] == "complete"


def test_restart_quarantines_ambiguous_capture_and_contract_launches(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    contract = pass_exact_contract(service)
    profile, epoch, coverage = capture_scope(service)
    actor_payload = service._capture_v2_posts_payload(
        profile,
        actor_id=service.settings.actors.posts_v2_primary,
        maximum=50,
        cursor=None,
        known_post_ids=[],
    )
    batch, _ = service.db.prepare_paid_source_batch(
        profile_id=1,
        epoch_id=epoch["id"],
        coverage_stream_id=coverage["id"],
        contract_id=contract["id"],
        provider="apify",
        actor_id=contract["actor_id"],
        intent="initial_public_capture",
        observation_window="ambiguous-window",
        normalized_input=actor_payload,
        request_hash="d" * 64,
    )
    service.db.transition_paid_source_batch(batch["id"], "launching")
    capture_job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_posts_v2' ORDER BY id DESC LIMIT 1"
    )
    service.db.execute("UPDATE jobs SET status='running' WHERE id=?", (capture_job["id"],))

    pending_contract = service.db.upsert_actor_contract(
        provider="apify",
        actor_id=service.settings.actors.posts_v2_fallback,
        purpose="posts_backfill",
        schema_fingerprint=service._posts_v2_fingerprint(
            service.settings.actors.posts_v2_fallback
        ),
        input_mapping_hash="fallback-mapping",
        status="pending",
    )
    contract_run, _ = service.db.record_contract_run(
        pending_contract["id"],
        test_case="page_1",
        normalized_input={"startUrls": [profile["url"]]},
        request_hash="e" * 64,
    )
    service.db.execute(
        "UPDATE contract_runs SET status='launching' WHERE id=?", (contract_run["id"],)
    )
    contract_job_id = service._enqueue(
        1,
        "contract_test_posts_v2",
        -10,
        service._capture_v2_datetime("2026-08-16T00:00:00+00:00"),
        {"actor_id": service.settings.actors.posts_v2_fallback},
    )
    service.db.execute("UPDATE jobs SET status='running' WHERE id=?", (contract_job_id,))

    recovered = service._recover_stale_capture_v2_jobs()

    assert recovered == {"pending": 0, "needs_reconcile": 2}
    assert service.db.row("SELECT status FROM paid_source_batches WHERE id=?", (batch["id"],))["status"] == "needs_reconcile"
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (capture_job["id"],))["status"] == "needs_reconcile"
    assert service.db.row("SELECT status FROM contract_runs WHERE id=?", (contract_run["id"],))["status"] == "needs_reconcile"
    assert service.db.row("SELECT status FROM jobs WHERE id=?", (contract_job_id,))["status"] == "needs_reconcile"


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_v2_enabled", [True, False])
async def test_regular_visit_never_calls_v1_posts_or_comments_when_v1_is_disabled(
    tmp_path: Path, monkeypatch, capture_v2_enabled: bool
):
    service = make_service(tmp_path, monkeypatch)
    service.settings.capture_v2_enabled = capture_v2_enabled
    service.settings.apify_v1_backfill_enabled = False
    service.db.execute(
        """UPDATE profiles SET public_state='public',backfill_done=1,
        serp_last_checked_at='2999-01-01T00:00:00+00:00' WHERE id=1"""
    )
    paid_calls = 0

    async def paid_call(*args, **kwargs):
        nonlocal paid_calls
        paid_calls += 1
        raise AssertionError("V1 posts/comments Actor must not be called")

    service.apify.call = paid_call

    await service.visit_profile(1)

    assert paid_calls == 0
    epochs = service.db.row("SELECT COUNT(*) count FROM capture_epochs")["count"]
    assert epochs == 0
    verify_jobs = service.db.row(
        "SELECT COUNT(*) count FROM jobs WHERE job_type='verify_public_v2' AND status='pending'"
    )["count"]
    assert verify_jobs == (1 if capture_v2_enabled else 0)


@pytest.mark.asyncio
async def test_freeze_and_contract_mismatch_fail_closed_before_paid_launch(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    calls = 0

    async def start(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("paid launch must be blocked")

    service.apify.start = start
    service.db.set_profile_source_control(1, "apify", frozen=True, reason="manual")
    with pytest.raises(ApifyFrozen):
        await service.capture_posts_v2(
            1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
        )
    assert calls == 0

    other = make_service(tmp_path / "mismatch", monkeypatch)
    wrong = other.db.upsert_actor_contract(
        provider="apify",
        actor_id=other.settings.actors.posts_v2_primary,
        purpose="posts_backfill",
        schema_fingerprint="wrong-fingerprint",
        input_mapping_hash="wrong-mapping",
        status="passed",
    )
    epoch2, _ = other.db.get_or_create_capture_epoch(1, "test", status="awaiting_contract")
    confirm_public_access(other)
    coverage2 = other.db.upsert_coverage_stream(
        epoch2["id"],
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=wrong["id"],
    )
    other.apify.start = start
    with pytest.raises(RuntimeError, match="exact fingerprint"):
        await other.capture_posts_v2(
            1, {"epoch_id": epoch2["id"], "coverage_stream_id": coverage2["id"]}
        )
    assert calls == 0


@pytest.mark.asyncio
async def test_capture_rechecks_freeze_after_official_usage_await(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    launches = 0

    async def usage_then_freeze():
        service.db.set_profile_source_control(1, "apify", frozen=True, reason="operator")
        usage = MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )
        return 4.9, usage

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("freeze at the launch boundary must win")

    service._official_available = usage_then_freeze
    service.apify.start = start

    with pytest.raises(ApifyFrozen):
        await service.capture_posts_v2(
            1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
        )

    assert launches == 0
    assert service.db.row("SELECT status FROM capture_epochs WHERE id=?", (epoch["id"],))["status"] == "manual_paused"


@pytest.mark.asyncio
async def test_capture_freeze_after_launch_transition_returns_unlaunched_batch_to_prepared(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)
    launches = 0
    original_claim = service.db.claim_paid_source_batch_launch

    def claim(batch_id, *args, **kwargs):
        row, claimed = original_claim(batch_id, *args, **kwargs)
        if claimed:
            service.db.set_profile_source_control(1, "apify", frozen=True, reason="operator")
        return row, claimed

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("freeze immediately before start must win")

    service.db.claim_paid_source_batch_launch = claim
    service.apify.start = start

    with pytest.raises(ApifyFrozen):
        await service.capture_posts_v2(
            1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
        )

    assert launches == 0
    batch = service.db.row("SELECT status,run_id,launched_at FROM paid_source_batches")
    assert batch == {"status": "prepared", "run_id": None, "launched_at": None}


@pytest.mark.asyncio
async def test_access_probe_rechecks_freeze_after_official_usage_await(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    pass_exact_contract(service)
    launches = 0

    async def usage_then_freeze():
        service.db.set_profile_source_control(1, "apify", frozen=True, reason="operator")
        usage = MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )
        return 4.9, usage

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("freeze at the launch boundary must win")

    service._official_available = usage_then_freeze
    service.apify.start = start

    with pytest.raises(ApifyFrozen):
        await service._capture_v2_apify_probe(
            service.db.row("SELECT * FROM profiles WHERE id=1")
        )

    assert launches == 0


@pytest.mark.asyncio
async def test_contract_test_validates_two_pages_replay_and_known_boundary(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    usage_checks = 0

    async def monthly_usage():
        nonlocal usage_checks
        usage_checks += 1
        return MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    service.apify.monthly_usage = monthly_usage
    payloads: list[dict] = []

    async def start(actor_id, payload, max_charge_usd=None):
        payloads.append(payload)
        return StartedActor(f"contract-{len(payloads)}", f"dataset-{len(payloads)}", "store")

    async def finish(started):
        number = int(started.run_id.rsplit("-", 1)[1])
        case_number = ((number - 1) % 4) + 1
        if case_number == 1:
            ids, cursor = range(0, 10), "cursor-1"
        elif case_number in {2, 3}:
            ids, cursor = range(10, 20), "cursor-2"
        else:
            ids, cursor = [], None
        profile_summary = {
            "profileId": "100",
            "status": "succeeded",
            "pointer": {"nextCursor": cursor},
        }
        if case_number == 4:
            profile_summary["coverageStatus"] = "complete_until_known_post"
        summary = {"profiles": [profile_summary]}
        return ActorResult(
            [
                {
                    "postId": f"p{index}",
                    "postUrl": f"https://facebook.com/100/posts/p{index}",
                    "publishedAt": (
                        datetime(2026, 8, 20, tzinfo=UTC) - timedelta(days=index)
                    ).isoformat(),
                }
                for index in ids
            ],
            summary,
            started.run_id,
            charged_usd=0.01,
        )

    service.apify.start = start
    service.apify.finish = finish

    first_authorization = authorize_contract_test(service)
    await service.contract_test_posts_v2(1, first_authorization)

    assert len(payloads) == 4
    assert payloads[0]["profileUrls"] == ["https://www.facebook.com/100"]
    assert payloads[0]["maxPostsPerProfile"] == 10
    assert payloads[0]["omitPinnedPosts"] is True
    assert payloads[0]["expandAllPhotos"] is True
    assert "startUrls" not in payloads[0]
    assert "maxPosts" not in payloads[0]
    assert payloads[1]["startCursor"] == "cursor-1"
    assert payloads[1]["maxPostsPerProfile"] == 10
    assert payloads[2] == payloads[1]
    assert payloads[3]["knownPostIds"] == [f"p{index}" for index in range(20)]
    assert payloads[3]["maxPostsPerProfile"] == 2
    contract = service._valid_posts_v2_contract()
    assert contract is not None
    assert contract["status"] == "passed"
    assert contract["expires_at"]
    assert service.db.row("SELECT COUNT(*) count FROM contract_runs")["count"] == 4
    assert service.db.row("SELECT COUNT(*) count FROM capture_epochs")["count"] == 0

    # A deliberate retest is a new generation and must really call the Actor;
    # retrying the same generation replays its durable contract results.
    service.db.execute(
        "UPDATE jobs SET status='done',finished_at=? WHERE status='running'",
        (datetime.now(UTC).isoformat(),),
    )
    second_authorization = authorize_contract_test(service)
    await service.contract_test_posts_v2(1, second_authorization)
    assert len(payloads) == 8
    assert service.db.row("SELECT COUNT(*) count FROM contract_runs")["count"] == 8
    await service.contract_test_posts_v2(1, second_authorization)
    assert len(payloads) == 8
    assert service.db.row("SELECT COUNT(*) count FROM contract_runs")["count"] == 8
    assert usage_checks == 8


@pytest.mark.asyncio
async def test_contract_test_fails_closed_when_a_later_case_cannot_refresh_official_usage(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    usage_checks = launches = 0

    async def monthly_usage():
        nonlocal usage_checks
        usage_checks += 1
        if usage_checks == 2:
            raise RuntimeError("official billing unavailable")
        return MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    async def start(actor_id, payload, max_charge_usd=None):
        nonlocal launches
        launches += 1
        return StartedActor("first-case", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [
                {
                    "postId": f"p{index}",
                    "postUrl": f"https://facebook.com/100/posts/p{index}",
                    "publishedAt": (
                        datetime(2026, 8, 20, tzinfo=UTC) - timedelta(days=index)
                    ).isoformat(),
                }
                for index in range(10)
            ],
            {
                "profiles": [
                    {
                        "profileId": "100",
                        "status": "succeeded",
                        "pointer": {"nextCursor": "cursor-1"},
                    }
                ]
            },
            started.run_id,
            charged_usd=0.01,
        )

    service.apify.monthly_usage = monthly_usage
    service.apify.start = start
    service.apify.finish = finish
    authorization = authorize_contract_test(service)

    with pytest.raises(BudgetExceeded, match="官方用量查詢失敗"):
        await service.contract_test_posts_v2(1, authorization)

    assert usage_checks == 2
    assert launches == 1
    rows = service.db.rows("SELECT test_case,status FROM contract_runs ORDER BY id")
    assert rows == [
        {"test_case": "page_1", "status": "succeeded"},
        {"test_case": "page_2", "status": "pending"},
    ]


@pytest.mark.asyncio
async def test_contract_test_applies_special_reserve_at_atomic_launch_boundary(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    launches = 0

    async def monthly_usage():
        return MonthlyUsage(
            0.30,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("protected special-account budget must block launch")

    service.apify.monthly_usage = monthly_usage
    service.apify.start = start
    authorization = authorize_contract_test(service)

    with pytest.raises(BudgetExceeded, match="特別帳號保留額"):
        await service.contract_test_posts_v2(1, authorization)

    assert launches == 0
    run = service.db.row("SELECT status,run_id FROM contract_runs")
    assert run == {"status": "pending", "run_id": None}


@pytest.mark.asyncio
async def test_contract_test_requires_operator_fixture_confirmation(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    allow_budget(service)
    launches = 0

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("fixture confirmation must be checked before launch")

    service.apify.start = start

    with pytest.raises(ValueError, match="25 篇"):
        await service.contract_test_posts_v2(
            1, {"max_budget_usd": 0.20, "contract_test_id": "missing-fixture-ack"}
        )

    assert launches == 0


@pytest.mark.asyncio
async def test_contract_test_service_boundary_rejects_missing_paid_grant(
    tmp_path: Path, monkeypatch
):
    service = make_service(tmp_path, monkeypatch)
    allow_budget(service)
    launches = 0

    async def start(*args, **kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("a direct service call must not bypass the grant ledger")

    service.apify.start = start

    with pytest.raises(BudgetExceeded, match="付費授權"):
        await service.contract_test_posts_v2(
            1,
            {
                "max_budget_usd": 0.20,
                "contract_test_id": "manual-without-grant",
                "fixture_ack": 1,
                "fixture_expected_min_public_posts": 25,
            },
        )

    assert launches == 0


def test_contract_item_guards_parse_time_and_detect_pinned(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)

    assert service._capture_v2_item_is_pinned({"isPinned": True}) is True
    assert service._capture_v2_item_is_pinned({"pinned": "true"}) is True
    assert service._capture_v2_item_is_pinned({"isPinned": False}) is False
    assert service._capture_v2_item_published_at(
        {"publishedAt": "2026-08-20T12:00:00+08:00"}
    ) == datetime(2026, 8, 20, 4, 0, tzinfo=UTC)
    assert service._capture_v2_item_published_at({"timestamp": 1_787_200_000_000}) == datetime.fromtimestamp(
        1_787_200_000, UTC
    )
    assert service._capture_v2_item_published_at({"date": "not-a-date"}) is None


@pytest.mark.asyncio
async def test_contract_rechecks_apify_freeze_before_every_paid_case(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    allow_budget(service)
    launches = 0

    async def start(actor_id, actor_payload, max_charge_usd=None):
        nonlocal launches
        launches += 1
        return StartedActor("freeze-run-1", "dataset", "store")

    async def finish(started):
        service.db.set_profile_source_control(1, "apify", frozen=True, reason="operator")
        return ActorResult(
            [
                {
                    "postId": f"p{index}",
                    "postUrl": f"https://facebook.com/100/posts/p{index}",
                    "publishedAt": (
                        datetime(2026, 8, 20, tzinfo=UTC) - timedelta(days=index)
                    ).isoformat(),
                }
                for index in range(10)
            ],
            {"profiles": [{"profileId": "100", "pointer": {"nextCursor": "cursor-1"}}]},
            started.run_id,
        )

    service.apify.start = start
    service.apify.finish = finish
    authorization = authorize_contract_test(service)

    with pytest.raises(ApifyFrozen):
        await service.contract_test_posts_v2(1, authorization)

    assert launches == 1


def test_summary_terminal_requires_explicit_full_history_evidence(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)

    exhausted = service._capture_v2_summary_state(
        {"profiles": [{"coverageStatus": "complete_feed_exhausted"}]},
        result_count=3,
        maximum=50,
    )
    empty = service._capture_v2_summary_state(
        {"profiles": [{"coverageStatus": "no_public_posts"}]},
        result_count=0,
        maximum=50,
    )
    target_reached = service._capture_v2_summary_state(
        {"profiles": [{"coverageStatus": "complete_target_reached", "hasNextPage": False}]},
        result_count=50,
        maximum=50,
    )

    assert exhausted["terminal"] is True
    assert empty["terminal"] is True
    assert target_reached["terminal"] is False
    assert target_reached["capped"] is True


def test_known_boundary_contract_requires_explicit_summary_marker(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)

    assert service._capture_v2_known_boundary_reached(
        {"profiles": [{"coverageStatus": "complete_until_known_post"}]}
    )
    assert service._capture_v2_known_boundary_reached(
        {"profiles": [{"stopReason": "known_post_boundary"}]}
    )
    assert not service._capture_v2_known_boundary_reached(
        {"profiles": [{"status": "succeeded", "hasNextPage": False}]}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_case", [1, 2, 3, 4])
async def test_contract_test_rejects_wrong_profile_in_every_paid_case(
    tmp_path: Path, monkeypatch, wrong_case: int
):
    service = make_service(tmp_path / str(wrong_case), monkeypatch)
    allow_budget(service)
    launches = 0

    async def start(actor_id, payload, max_charge_usd=None):
        nonlocal launches
        launches += 1
        return StartedActor(f"identity-{launches}", f"dataset-{launches}", "store")

    async def finish(started):
        number = int(started.run_id.rsplit("-", 1)[1])
        if number == 1:
            ids, cursor = range(0, 10), "cursor-1"
        elif number in {2, 3}:
            ids, cursor = range(10, 20), "cursor-2"
        else:
            ids, cursor = [], None
        observed_profile = "999" if number == wrong_case else "100"
        profile_summary = {
            "status": "succeeded",
            "profileId": observed_profile,
            "profileUrl": f"https://www.facebook.com/{observed_profile}",
            "pointer": {"nextCursor": cursor},
        }
        if number == 4:
            profile_summary["coverageStatus"] = "complete_until_known_post"
        return ActorResult(
            [
                {
                    "postId": f"p{index}",
                    "postUrl": f"https://facebook.com/{observed_profile}/posts/p{index}",
                    "authorId": observed_profile,
                    "publishedAt": (
                        datetime(2026, 8, 20, tzinfo=UTC) - timedelta(days=index)
                    ).isoformat(),
                }
                for index in ids
            ],
            {"profiles": [profile_summary]},
            started.run_id,
            charged_usd=0.01,
        )

    service.apify.start = start
    service.apify.finish = finish
    authorization = authorize_contract_test(service)

    with pytest.raises(RuntimeError, match="目標帳號身分不符"):
        await service.contract_test_posts_v2(1, authorization)

    assert launches == wrong_case
    assert service.db.row(
        "SELECT status FROM actor_contracts ORDER BY id DESC LIMIT 1"
    )["status"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["summary", "item_author"])
async def test_production_batch_rejects_wrong_profile_before_import_or_cursor_advance(
    tmp_path: Path, monkeypatch, mismatch: str
):
    service = make_service(tmp_path / mismatch, monkeypatch)
    pass_exact_contract(service)
    _, epoch, coverage = capture_scope(service)
    allow_budget(service)

    async def start(actor_id, payload, max_charge_usd=None):
        return StartedActor(f"wrong-{mismatch}", "dataset", "store")

    async def finish(started):
        summary_profile = "999" if mismatch == "summary" else "100"
        item_author = "999" if mismatch == "item_author" else "100"
        return ActorResult(
            [
                {
                    "postId": "wrong-profile-post",
                    "postUrl": "https://www.facebook.com/100/posts/wrong-profile-post",
                    "authorId": item_author,
                }
            ],
            {
                "profiles": [
                    {
                        "status": "succeeded",
                        "profileId": summary_profile,
                        "profileUrl": f"https://www.facebook.com/{summary_profile}",
                        "pointer": {"nextCursor": "wrong-cursor"},
                    }
                ]
            },
            started.run_id,
            charged_usd=0.01,
        )

    service.apify.start = start
    service.apify.finish = finish

    with pytest.raises(RuntimeError, match="目標帳號身分不符"):
        await service.capture_posts_v2(
            1, {"epoch_id": epoch["id"], "coverage_stream_id": coverage["id"]}
        )

    assert service.db.row("SELECT COUNT(*) count FROM entities WHERE kind='post'")["count"] == 0
    batch = service.db.row("SELECT * FROM paid_source_batches ORDER BY id DESC LIMIT 1")
    assert batch["status"] == "import_failed"
    assert batch["output_cursor"] is None
    checkpoint = service.db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert checkpoint["input_cursor"] is None
    assert checkpoint["output_cursor"] is None


def test_primary_and_fallback_payloads_and_fingerprints_are_isolated(tmp_path: Path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    primary = service._capture_v2_posts_payload(
        profile,
        actor_id=service.settings.actors.posts_v2_primary,
        maximum=10,
        cursor="primary-cursor",
        known_post_ids=["p1"],
    )
    fallback = service._capture_v2_posts_payload(
        profile,
        actor_id=service.settings.actors.posts_v2_fallback,
        maximum=10,
        cursor="fallback-cursor",
        known_post_ids=["p1"],
    )

    assert primary["profileUrls"] == [profile["url"]]
    assert primary["maxPostsPerProfile"] == 10
    assert primary["omitPinnedPosts"] is True
    assert "startUrls" not in primary and "maxPosts" not in primary
    assert fallback["startUrls"] == [profile["url"]]
    assert fallback["maxPosts"] == 10
    assert "profileUrls" not in fallback and "maxPostsPerProfile" not in fallback
    assert service._posts_v2_fingerprint(
        service.settings.actors.posts_v2_primary
    ) != service._posts_v2_fingerprint(service.settings.actors.posts_v2_fallback)
