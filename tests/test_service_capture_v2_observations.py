from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fb_monitor.apify import ActorResult, MonthlyUsage, StartedActor
from fb_monitor.capture_v2 import (
    AuthScope,
    CaptureIntent,
    EvidenceSignal,
    EvidenceSource,
    ObservationPurpose,
    canonical_input_json,
)
from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService


def _service(tmp_path: Path, monkeypatch) -> MonitorService:
    data_dir = (tmp_path / "data").as_posix()
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""profiles:
  - name: FB-100
    url: https://www.facebook.com/100
storage:
  data_dir: {data_dir}
  low_disk_gb: 0
schedule:
  visit_min_hours: 6
  visit_max_hours: 8
  spacing_min_minutes: 0
  spacing_max_minutes: 0
  recent_posts: 20
  full_audit_days: 7
budget:
  monthly_usd: 5
capture_v2:
  enabled: true
  special_profile_id: ""
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
    monkeypatch.setenv("APIFY_V1_BACKFILL_ENABLED", "0")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    return MonitorService(load_settings(config))


def _pass_contract(service: MonitorService) -> dict:
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


def _confirm_public(service: MonitorService) -> None:
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    target = service._capture_v2_target_id(profile)
    service.db.execute(
        "UPDATE profiles SET public_state='public',serp_last_checked_at='2999-01-01T00:00:00+00:00' WHERE id=1"
    )
    service.db.record_access_observation(
        1,
        source="anonymous_browser",
        auth_scope="anonymous",
        verdict="confirmed_public",
        target_fb_id=target,
        observed_fb_id=target,
        identity_match=True,
        evidence_summary={"classification": "strong_public"},
        observation_key="confirmed-public:1",
    )


def _conclude_full_history(service: MonitorService, completed_at: datetime) -> dict:
    epoch, _ = service.db.get_or_create_capture_epoch(
        1,
        "initial_public_capture",
        status="ready",
        scope={
            "all_public_history": True,
            "capture_intent": CaptureIntent.INITIAL_PUBLIC_CAPTURE.value,
        },
    )
    posts = service.db.upsert_coverage_stream(
        int(epoch["id"]), stream="posts", surface="timeline_posts"
    )
    service.db.update_coverage_stream(int(posts["id"]), status="in_progress")
    service.db.update_coverage_stream(
        int(posts["id"]),
        status="complete",
        terminal_evidence_json={"kind": "feed_exhausted"},
    )
    service._refresh_capture_v2_epoch(int(epoch["id"]))
    service.db.execute(
        "UPDATE capture_epochs SET completed_at=?,updated_at=? WHERE id=?",
        (completed_at.isoformat(), completed_at.isoformat(), epoch["id"]),
    )
    return service.db.row("SELECT * FROM capture_epochs WHERE id=?", (epoch["id"],))


def _install_budget(service: MonitorService) -> None:
    async def monthly_usage():
        return MonthlyUsage(
            0.1,
            "2026-08-09T00:00:00+00:00",
            "2026-09-08T23:59:59+00:00",
        )

    service.apify.monthly_usage = monthly_usage


def test_indeterminate_observation_does_not_replace_previous_strong_private_evidence(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    target = service._capture_v2_target_id(profile)
    start = datetime(2026, 8, 20, 0, 0, tzinfo=UTC)

    first, _, first_state = service._record_capture_v2_access(
        profile,
        source=EvidenceSource.BROWSER,
        source_label="anonymous_browser",
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.EXPLICIT_PRIVATE,
        purpose=ObservationPurpose.VERIFICATION,
        observed_id=target,
        identity_match=True,
        evidence={"step": "first-strong"},
        observed_at=start,
    )
    assert first_state.value == "suspected_private"
    assert json.loads(first["evidence_summary_json"])["evidence_source"] == "browser"

    _, _, weak_state = service._record_capture_v2_access(
        profile,
        source=EvidenceSource.BROWSER,
        source_label="anonymous_browser",
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.TIMEOUT,
        purpose=ObservationPurpose.VERIFICATION,
        observed_id=target,
        identity_match=True,
        evidence={"step": "indeterminate"},
        observed_at=start + timedelta(minutes=40),
    )
    assert weak_state.value == "suspected_private"

    _, _, final_state = service._record_capture_v2_access(
        profile,
        source=EvidenceSource.BROWSER,
        source_label="anonymous_browser",
        auth_scope=AuthScope.ANONYMOUS,
        signal=EvidenceSignal.EXPLICIT_PRIVATE,
        purpose=ObservationPurpose.VERIFICATION,
        observed_id=target,
        identity_match=True,
        evidence={"step": "second-strong"},
        observed_at=start + timedelta(minutes=75),
    )
    assert final_state.value == "suspected_private"


def test_full_history_epoch_manifest_marks_unsupported_surfaces_source_limited(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    _pass_contract(service)
    _confirm_public(service)

    epoch = service._ensure_capture_v2_epoch(
        service.db.row("SELECT * FROM profiles WHERE id=1"),
        "initial_public_capture",
        intent=CaptureIntent.INITIAL_PUBLIC_CAPTURE,
    )
    rows = service.db.rows(
        "SELECT id,stream,surface,status,limited_reason FROM coverage_streams WHERE epoch_id=?",
        (epoch["id"],),
    )
    by_surface = {row["surface"]: row for row in rows}
    assert by_surface["timeline_posts"]["status"] == "pending"
    for surface in (
        "reels",
        "videos",
        "public_photo_pages",
        "avatar_history",
        "cover_history",
    ):
        assert by_surface[surface]["status"] == "source_limited"
        assert by_surface[surface]["limited_reason"]

    timeline = by_surface["timeline_posts"]
    service.db.update_coverage_stream(int(timeline["id"]), status="in_progress")
    service.db.update_coverage_stream(
        int(timeline["id"]),
        status="complete",
        terminal_evidence_json={"kind": "no_public_posts"},
    )
    service._refresh_capture_v2_epoch(int(epoch["id"]))
    concluded = service.db.row(
        "SELECT status,is_active,terminal_reason FROM capture_epochs WHERE id=?",
        (epoch["id"],),
    )
    assert concluded["status"] == "source_limited"
    assert concluded["is_active"] == 0
    assert "來源受限" in concluded["terminal_reason"]
    assert int(service._capture_v2_concluded_history(1)["id"]) == int(epoch["id"])


def test_concluded_source_limited_epoch_is_inactivated_but_pending_stream_is_not(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    epoch, _ = service.db.get_or_create_capture_epoch(1, "limited", status="ready")
    posts = service.db.upsert_coverage_stream(
        int(epoch["id"]), stream="posts", surface="timeline_posts"
    )
    media = service.db.upsert_coverage_stream(
        int(epoch["id"]),
        stream="media",
        surface="post_albums",
        scope_type="post",
        scope_id="10",
    )
    service.db.update_coverage_stream(
        int(posts["id"]), status="source_limited", limited_reason="actor cap"
    )

    service._refresh_capture_v2_epoch(int(epoch["id"]))
    active = service.db.row("SELECT status,is_active FROM capture_epochs WHERE id=?", (epoch["id"],))
    assert active == {"status": "running", "is_active": 1}

    service.db.update_coverage_stream(
        int(media["id"]), status="source_limited", limited_reason="album cap"
    )
    service._refresh_capture_v2_epoch(int(epoch["id"]))
    concluded = service.db.row(
        "SELECT status,is_active,completed_at,terminal_reason FROM capture_epochs WHERE id=?",
        (epoch["id"],),
    )
    assert concluded["status"] == "source_limited"
    assert concluded["is_active"] == 0
    assert concluded["completed_at"]
    assert "來源受限" in concluded["terminal_reason"]


def test_source_limited_timeline_resumes_compatible_cursor_in_recovery_epoch(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    contract = _pass_contract(service)
    _confirm_public(service)
    previous, _ = service.db.get_or_create_capture_epoch(
        1,
        "initial_public_capture",
        status="ready",
        scope={
            "all_public_history": True,
            "capture_intent": CaptureIntent.INITIAL_PUBLIC_CAPTURE.value,
        },
    )
    posts = service.db.upsert_coverage_stream(
        int(previous["id"]),
        stream="posts",
        surface="timeline_posts",
        provider="apify",
        contract_id=int(contract["id"]),
    )
    service.db.update_coverage_stream(
        int(posts["id"]),
        status="source_limited",
        output_cursor="cursor-after-page-1",
        limited_reason="temporary provider boundary",
    )
    service._refresh_capture_v2_epoch(int(previous["id"]))

    assert service._capture_v2_concluded_history(1) is None
    assert service._capture_v2_regular_intent(1, datetime.now(UTC)) is CaptureIntent.RECOVERY_CAPTURE

    recovery = service._ensure_capture_v2_epoch(
        service.db.row("SELECT * FROM profiles WHERE id=1"),
        "regular_visit_v2",
    )
    scope = service._capture_v2_epoch_scope(recovery)
    assert scope["capture_intent"] == CaptureIntent.RECOVERY_CAPTURE.value
    checkpoint = service.db.row(
        """SELECT * FROM coverage_streams WHERE epoch_id=?
        AND stream='posts' AND surface='timeline_posts'""",
        (recovery["id"],),
    )
    assert checkpoint["input_cursor"] == "cursor-after-page-1"
    assert checkpoint["output_cursor"] == "cursor-after-page-1"
    provider_state = json.loads(checkpoint["provider_checkpoint_json"])
    assert provider_state["resumed_from_coverage_id"] == posts["id"]


@pytest.mark.asyncio
async def test_regular_v2_poll_sends_recent_twenty_known_ids_without_one_post_probe(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    _pass_contract(service)
    _confirm_public(service)
    observed_at = datetime.now(UTC)
    _conclude_full_history(service, observed_at - timedelta(minutes=5))
    for index in range(25):
        stamp = (observed_at - timedelta(minutes=25 - index)).isoformat()
        service.db.execute(
            """INSERT INTO entities(
              profile_id,kind,external_id,present,published_at,first_seen_at,last_seen_at
            ) VALUES(1,'post',?,1,?,?,?)""",
            (f"p{index}", stamp, stamp, stamp),
        )
    _install_budget(service)
    launches: list[dict] = []

    async def start(actor_id, payload, max_charge_usd=None):
        launches.append(payload)
        return StartedActor("incremental-run", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [],
            {
                "profiles": [
                    {
                        "profileId": "100",
                        "coverageStatus": "complete_until_known_post",
                    }
                ]
            },
            started.run_id,
        )

    service.apify.start = start
    service.apify.finish = finish

    before_visit = datetime.now(UTC)
    await service.visit_profile(1)
    after_visit = datetime.now(UTC)
    scheduled = service.db.row("SELECT next_visit_at FROM profiles WHERE id=1")
    next_visit_at = datetime.fromisoformat(scheduled["next_visit_at"])
    assert before_visit + timedelta(hours=6) <= next_visit_at
    assert next_visit_at <= after_visit + timedelta(hours=8)
    job = service.db.row(
        "SELECT * FROM jobs WHERE job_type='capture_posts_v2' AND status='pending' ORDER BY id DESC LIMIT 1"
    )
    payload = json.loads(job["payload_json"])
    assert payload["intent"] == CaptureIntent.INCREMENTAL_POLL.value

    await service.capture_posts_v2(1, payload)

    assert len(launches) == 1
    assert launches[0]["maxPostsPerProfile"] == 20
    assert launches[0]["knownPostIds"] == [f"p{index}" for index in range(24, 4, -1)]
    assert all(value != 1 for value in (launch["maxPostsPerProfile"] for launch in launches))
    batch = service.db.row("SELECT * FROM paid_source_batches ORDER BY id DESC LIMIT 1")
    assert batch["intent"] == CaptureIntent.INCREMENTAL_POLL.value
    assert service.db.row("SELECT COUNT(*) total FROM jobs WHERE job_type='audit'")["total"] == 0
    epoch = service.db.row("SELECT * FROM capture_epochs WHERE id=?", (payload["epoch_id"],))
    assert epoch["status"] == "complete"
    assert epoch["is_active"] == 0


@pytest.mark.asyncio
async def test_monthly_audit_is_five_posts_with_fixed_month_window_and_no_cursor_successor(
    tmp_path: Path, monkeypatch
):
    service = _service(tmp_path, monkeypatch)
    _pass_contract(service)
    _confirm_public(service)
    observed_at = datetime.now(UTC)
    current_window = service._capture_v2_month_window(observed_at.strftime("%Y-%m"))
    _conclude_full_history(service, current_window.start_at - timedelta(days=1))
    _install_budget(service)
    launches: list[dict] = []

    async def start(actor_id, payload, max_charge_usd=None):
        launches.append(payload)
        return StartedActor("monthly-run", "dataset", "store")

    async def finish(started):
        return ActorResult(
            [
                {
                    "postId": f"monthly-{index}",
                    "postUrl": f"https://facebook.com/100/posts/monthly-{index}",
                    "photosCount": 0,
                }
                for index in range(5)
            ],
            {
                "profiles": [
                    {
                        "profileId": "100",
                        "coverageStatus": "complete_target_reached",
                        "pointer": {"nextCursor": "must-not-follow"},
                    }
                ]
            },
            started.run_id,
        )

    service.apify.start = start
    service.apify.finish = finish
    profile = service.db.row("SELECT * FROM profiles WHERE id=1")
    epoch = service._ensure_capture_v2_epoch(
        profile, "regular_visit_v2", observed_at=observed_at
    )
    coverage = service.db.row(
        "SELECT * FROM coverage_streams WHERE epoch_id=? AND stream='posts'", (epoch["id"],)
    )

    await service.capture_posts_v2(
        1,
        {
            "epoch_id": epoch["id"],
            "coverage_stream_id": coverage["id"],
            "intent": CaptureIntent.MONTHLY_AUDIT.value,
        },
    )

    assert len(launches) == 1
    assert launches[0]["maxPostsPerProfile"] == 5
    assert launches[0]["knownPostIds"] == []
    batch = service.db.row("SELECT * FROM paid_source_batches ORDER BY id DESC LIMIT 1")
    assert batch["intent"] == CaptureIntent.MONTHLY_AUDIT.value
    assert batch["observation_window"] == current_window.key
    posts = service.db.row("SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],))
    assert posts["status"] == "complete"
    evidence = json.loads(posts["terminal_evidence_json"])
    assert evidence["kind"] == "monthly_target_reached"
    assert evidence["target_count"] == 5
    assert service.db.row(
        "SELECT COUNT(*) total FROM jobs WHERE job_type='capture_posts_v2' AND status='pending'"
    )["total"] == 1  # the current test invokes the queued job directly; no successor was added

    for row in service.db.rows(
        "SELECT id FROM coverage_streams WHERE epoch_id=? AND stream='comments'", (epoch["id"],)
    ):
        service.db.update_coverage_stream(
            int(row["id"]), status="source_limited", limited_reason="test comments cap"
        )
    service._refresh_capture_v2_epoch(int(epoch["id"]))
    same_month = service._ensure_capture_v2_epoch(
        profile, "regular_visit_v2", observed_at=observed_at + timedelta(days=1)
    )
    same_scope = service._capture_v2_epoch_scope(same_month)
    assert same_scope["capture_intent"] == CaptureIntent.INCREMENTAL_POLL.value

    same_posts = service.db.row(
        "SELECT id FROM coverage_streams WHERE epoch_id=? AND stream='posts'", (same_month["id"],)
    )
    service.db.update_coverage_stream(
        int(same_posts["id"]), status="source_limited", limited_reason="test poll cap"
    )
    service._refresh_capture_v2_epoch(int(same_month["id"]))
    next_month_at = current_window.end_at + timedelta(days=1)
    next_month = service._ensure_capture_v2_epoch(
        profile, "regular_visit_v2", observed_at=next_month_at
    )
    next_scope = service._capture_v2_epoch_scope(next_month)
    assert next_scope["capture_intent"] == CaptureIntent.MONTHLY_AUDIT.value
    assert next_scope["observation_month"] == next_month_at.strftime("%Y-%m")
