from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .apify import ActorResult, ApifyGateway, MonthlyUsage, StartedActor
from .brightdata import BrightDataError, BrightDataGateway
from .browser_guard import BrowserDecision, BrowserGuard
from .capture_coordinator import (
    reconcile_post_media_checkpoint,
    resolve_epoch,
    seed_comment_checkpoints,
)
from .capture_v2 import (
    AccessState,
    AuthScope,
    CaptureIntent,
    CoverageStatus,
    CoverageStream,
    CoverageSurface,
    EvidenceClass,
    EvidenceSignal,
    EvidenceSource,
    ObservationPurpose,
    ProbeSource,
    StrongPrivateObservation,
    budget_decision,
    canonical_input_json,
    choose_probe_source,
    classify_access_evidence,
    deterministic_observation_window,
    duplicate_page_circuit_breaker,
    next_access_state,
    request_hash as capture_request_hash,
)
from .config import Settings, actor_input, load_settings
from .db import Database, utcnow
from .facebook_browser import (
    FacebookBrowserChallengeRequired,
    FacebookBrowserError,
    FacebookBrowserGateway,
    FacebookBrowserLoginRequired,
    is_facebook_ui_heading,
    public_content_proof_matches_profile,
)
from .ingest import Ingester, external_id, is_placeholder_profile_name, monitored_projection, profile_display_name
from .media import MediaStore, extract_media
from .normalize import content_hash, facebook_post_identity, normalize_url
from .raw_retention import cleanup_capture_raw
from .telegram import TelegramSender
from .serpapi import SerpApiError, SerpApiGateway, SerpApiQuotaExceeded, profile_id_from_url
from .storage import collect_storage_snapshot, daily_storage_message
from .timeutil import telegram_time

log = logging.getLogger(__name__)
PRICES = {"profile": 5.40 / 1000, "posts": 4.99 / 1000, "comments": 1.40 / 1000}
REPAIR_MIGRATION = "schema_media_v2_20260719"
PROFILE_PIC_MIGRATION = "profile_pic_fields_v3_20260719"
NOTIFICATION_HYGIENE_MIGRATION = "notification_hygiene_v5_20260723"
BROWSER_NAME_REPAIR_MIGRATION = "browser_name_heading_v2_20260803"
HISTORICAL_NAME_REPAIR_MIGRATION = "historical_profile_name_v1_20260803"
LATEST_ONLY_BACKFILL_REPAIR_MIGRATION = "latest_only_backfill_v1_20260812"
CAPTURE_V2_CONTRACT_SCHEMA = "posts-summary-cursor-media-target-v2"
WORKER_LEASE_MINUTES = 30


class BudgetExceeded(RuntimeError):
    def __init__(self, message: str, resume_at: datetime | None = None):
        super().__init__(message)
        self.resume_at = resume_at


class ApifyFrozen(RuntimeError):
    """The selected profile has explicitly disabled all Apify work."""


class BrowserGuardDeferred(RuntimeError):
    """A queued browser job must wait for the shared risk-control window."""

    def __init__(self, decision: BrowserDecision):
        super().__init__(f"Chromium 安全閘門延後：{decision.reason}")
        self.decision = decision


def actor_summary_error(summary: dict[str, Any] | None) -> str | None:
    if not isinstance(summary, dict):
        return None
    profiles = summary.get("profiles")
    failures = [entry for entry in profiles if isinstance(entry, dict) and entry.get("status") == "failed"] if isinstance(profiles, list) else []
    failed = str(summary.get("health", "")).lower() == "failed" or (bool(profiles) and len(failures) == len(profiles))
    if not failed:
        return None
    messages = []
    for entry in failures:
        error = entry.get("error")
        if isinstance(error, dict):
            messages.append(str(error.get("message") or error.get("code") or "Actor profile failed"))
        elif error:
            messages.append(str(error))
    return "; ".join(dict.fromkeys(messages)) or "Actor SUMMARY 回報失敗"


class MonitorService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.worker_id = f"{os.getpid()}:{uuid.uuid4().hex}"
        self.db = Database(settings.db_path)
        self.db.sync_profiles(settings.profiles)
        self.apify = ApifyGateway(settings.apify_token)
        self.serpapi = SerpApiGateway(settings.serpapi_key)
        self.brightdata = BrightDataGateway(settings.brightdata_api_token, settings.brightdata_dataset_id)
        self.facebook_browser = FacebookBrowserGateway(
            settings.facebook_browser_enabled,
            settings.facebook_browser_data_dir,
            settings.facebook_browser_timeout_seconds,
            settings.browser_canary_max_posts,
        )
        # A separate empty user-data directory is the only browser allowed to
        # confirm anonymous public visibility.  It never shares cookies with
        # the operator's logged-in fallback profile.
        self.facebook_anonymous_browser = FacebookBrowserGateway(
            settings.facebook_browser_enabled,
            settings.data_dir / "anonymous-browser-data",
            settings.facebook_browser_timeout_seconds,
            0,
            require_login=False,
        )
        account_spacing = (
            settings.browser_account_min_minutes,
            max(settings.browser_account_min_minutes, settings.browser_account_max_minutes),
        )
        cross_account_spacing = (
            settings.browser_cross_account_min_minutes,
            max(settings.browser_cross_account_min_minutes, settings.browser_cross_account_max_minutes),
        )
        evidence_root = settings.data_dir / "browser-evidence"
        guard_options = {
            "daily_batch_limit": settings.browser_daily_batches,
            "global_spacing_minutes": cross_account_spacing,
            "profile_spacing_minutes": account_spacing,
            "challenge_hours": settings.browser_breaker_hours,
            "repeated_challenge_hours": settings.browser_breaker_repeat_hours,
            "evidence_retention_days": settings.evidence_retention_days,
            "evidence_max_bytes": settings.evidence_cap_bytes,
        }
        # Both browser contexts share one OCI host/IP and therefore one
        # rate-limit/challenge boundary.  Login walls remain an observation
        # (they do not call record_challenge), while a real checkpoint,
        # challenge or 429 must stop every Chromium path.
        self.browser_guard = BrowserGuard(
            self.db, evidence_root, browser_identity="global", **guard_options
        )
        self.anonymous_browser_guard = self.browser_guard
        self.media = MediaStore(self.db, settings.data_dir, settings.low_disk_gb, settings.media_retry_days)
        self.ingester = Ingester(self.db, settings.data_dir, self.media)
        self.telegram = TelegramSender(
            self.db, settings.telegram_bot_token, settings.telegram_chat_id,
            interval_seconds=settings.telegram_send_interval_seconds,
            high_water=settings.telegram_high_water,
            max_attempts=settings.telegram_max_attempts,
            retry_hours=settings.telegram_retry_hours,
        )
        self.stop_event = asyncio.Event()
        self._maintenance_lock = asyncio.Lock()
        self._config_mtime = settings.config_path.stat().st_mtime

    @staticmethod
    def _lease_is_active(row: dict[str, Any]) -> bool:
        if not row.get("lease_owner") or not row.get("leased_at"):
            return False
        try:
            leased_at = datetime.fromisoformat(str(row["leased_at"]))
        except ValueError:
            return False
        return leased_at > datetime.now(UTC) - timedelta(minutes=WORKER_LEASE_MINUTES)

    async def start(self) -> None:
        self._cleanup_browser_evidence()
        self._cleanup_capture_raw()
        self._pause_unsafe_v1_jobs()
        self._recover_stale_capture_v2_jobs()
        self._seed_capture_v2()
        self._seed_browser_name_repair()
        self._seed_historical_name_repair()
        self._purge_duplicate_profile_previews()
        self._seed_notification_hygiene()
        self._seed_profile_pic_migration()
        self._seed_upgrade_repair()
        self._seed_latest_only_backfill_repair()
        self._seed_initial_jobs()
        await asyncio.gather(
            self._scheduler_loop(), self._outbox_loop(), self._health_loop(),
            self._media_retry_loop(), self._daily_media_dedupe_loop(),
            self._storage_snapshot_loop(),
            self._browser_evidence_cleanup_loop(),
            self._capture_raw_cleanup_loop(),
        )

    @staticmethod
    def _browser_retry_text(decision: BrowserDecision) -> str:
        return decision.retry_at.isoformat() if decision.retry_at else "稍後"

    def _browser_guard_event(
        self,
        profile: dict[str, Any],
        decision: BrowserDecision,
        *,
        operation: str,
        identity: str,
    ) -> None:
        hour = datetime.now(UTC).strftime("%Y-%m-%dT%H")
        self.db.add_event(
            f"browser-guard:{identity}:{profile['id']}:{operation}:{decision.reason}:{hour}",
            "browser_guard_deferred",
            {
                "title": "Chromium 安全閘門延後本次操作",
                "text": (
                    f"操作：{operation}；原因：{decision.reason}；"
                    f"可重試時間：{self._browser_retry_text(decision)}"
                ),
                "source_url": profile["url"],
            },
            int(profile["id"]),
            notify=False,
        )

    def _acquire_browser(
        self,
        profile: dict[str, Any],
        *,
        anonymous: bool,
        operation: str,
        defer_job: bool,
    ) -> bool:
        guard = self.anonymous_browser_guard if anonymous else self.browser_guard
        decision = guard.acquire(int(profile["id"]))
        if decision.allowed:
            return True
        self._browser_guard_event(
            profile,
            decision,
            operation=operation,
            identity="anonymous" if anonymous else "logged-in",
        )
        if defer_job:
            raise BrowserGuardDeferred(decision)
        return False

    @staticmethod
    def _browser_screenshot_bytes(
        gateway: FacebookBrowserGateway, diagnostic_key: str
    ) -> bytes | None:
        try:
            path = gateway.screenshot_path(diagnostic_key)
            return path.read_bytes() if path.is_file() else None
        except OSError:
            return None

    def _record_browser_challenge(
        self,
        profile: dict[str, Any],
        *,
        anonymous: bool,
        diagnostic_key: str,
        error: Exception,
    ) -> None:
        guard = self.anonymous_browser_guard if anonymous else self.browser_guard
        gateway = self.facebook_anonymous_browser if anonymous else self.facebook_browser
        guard.record_challenge(
            int(profile["id"]),
            screenshot=self._browser_screenshot_bytes(gateway, diagnostic_key),
            metadata={"error": str(error)[:1000], "source_url": str(profile["url"])},
        )

    def _cleanup_browser_evidence(self) -> None:
        try:
            self.browser_guard.cleanup_evidence()
            try:
                local_now = datetime.now(ZoneInfo(self.settings.timezone))
            except Exception:
                # Minimal Windows test hosts may not ship the IANA tzdata
                # package. Cleanup gating remains safe with UTC fallback.
                local_now = datetime.now(UTC)
            self._browser_evidence_cleanup_date = local_now.date().isoformat()
        except Exception:
            log.exception("browser evidence cleanup failed")

    def _cleanup_capture_raw(self) -> None:
        try:
            result = cleanup_capture_raw(self.db, self.settings.data_dir)
            try:
                local_now = datetime.now(ZoneInfo(self.settings.timezone))
            except Exception:
                local_now = datetime.now(UTC)
            self._capture_raw_cleanup_date = local_now.date().isoformat()
            if result.errors:
                log.warning(
                    "Capture V2 raw cleanup completed with %s errors", result.errors
                )
        except Exception:
            log.exception("Capture V2 raw cleanup failed")

    def _seed_browser_name_repair(self) -> None:
        if self.db.migration_applied(BROWSER_NAME_REPAIR_MIGRATION):
            return
        repaired = 0
        for profile in self.db.rows("SELECT id,display_name,profile_details_json FROM profiles WHERE enabled=1"):
            if "Facebook 直接瀏覽器" not in str(profile.get("profile_details_json") or ""):
                continue
            if not is_facebook_ui_heading(profile.get("display_name")):
                continue
            profile_id = int(profile["id"])
            self.db.execute("UPDATE profiles SET display_name=NULL,serp_last_checked_at=NULL WHERE id=?", (profile_id,))
            if not self.db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='visit' AND status IN ('pending','running')", (profile_id,)):
                self._enqueue(profile_id, "visit", -50, datetime.now(UTC))
            repaired += 1
        self.db.mark_migration(BROWSER_NAME_REPAIR_MIGRATION, {"profiles_reset": repaired})

    def _seed_historical_name_repair(self) -> None:
        if self.db.migration_applied(HISTORICAL_NAME_REPAIR_MIGRATION):
            return
        repaired = 0
        for profile in self.db.rows("SELECT id,display_name FROM profiles WHERE enabled=1"):
            if not is_placeholder_profile_name(profile.get("display_name")):
                continue
            versions = self.db.rows(
                """SELECT v.normalized_json FROM versions v
                JOIN entities e ON e.id=v.entity_id
                WHERE e.profile_id=? AND e.kind='profile'
                ORDER BY v.seen_at DESC,v.id DESC""",
                (profile["id"],),
            )
            recovered = ""
            for version in versions:
                try:
                    recovered = str(json.loads(version["normalized_json"]).get("authorName") or "")
                except (TypeError, json.JSONDecodeError):
                    continue
                if not is_placeholder_profile_name(recovered):
                    break
                recovered = ""
            if recovered:
                self.db.execute("UPDATE profiles SET display_name=? WHERE id=?", (recovered, profile["id"]))
                repaired += 1
        self.db.mark_migration(HISTORICAL_NAME_REPAIR_MIGRATION, {"profiles_recovered": repaired})

    def _seed_latest_only_backfill_repair(self) -> None:
        """Restart profiles that an older probe-first flow falsely completed."""
        if not self.settings.apify_v1_backfill_enabled:
            return
        if self.db.migration_applied(LATEST_ONLY_BACKFILL_REPAIR_MIGRATION):
            return
        repaired = 0
        profiles = self.db.rows(
            """SELECT p.id FROM profiles p
            WHERE p.enabled=1 AND p.public_state='public' AND p.backfill_done=1
              AND COALESCE(p.apify_frozen,0)=0
              AND (SELECT COUNT(*) FROM entities e
                   WHERE e.profile_id=p.id AND e.kind='post' AND e.present=1) <= 1"""
        )
        for profile in profiles:
            profile_id = int(profile["id"])
            self.db.execute(
                "UPDATE profiles SET backfill_done=0,backfill_cursor=NULL,last_full_audit_at=NULL WHERE id=?",
                (profile_id,),
            )
            if not self.db.row(
                "SELECT id FROM jobs WHERE profile_id=? AND job_type IN ('backfill','backfill_comments') AND status IN ('pending','running')",
                (profile_id,),
            ):
                self._enqueue(profile_id, "backfill", 30, datetime.now(UTC))
            repaired += 1
        self.db.mark_migration(
            LATEST_ONLY_BACKFILL_REPAIR_MIGRATION,
            {"profiles_requeued": repaired},
        )

    def _pause_unsafe_v1_jobs(self) -> int:
        """Fail closed while the cursorless V1 Actor is disabled.

        The old jobs are retained as evidence.  Marking them paused prevents a
        rebuilt container from buying the same first page again while V2 is
        awaiting a validated Actor contract.
        """
        if self.settings.apify_v1_backfill_enabled:
            return 0
        now = utcnow()
        with self.db.connect() as conn:
            cursor = conn.execute(
                """UPDATE jobs SET status='paused_contract',finished_at=?,
                error='V1 無游標回溯已停用；等待 Capture V2 Actor 契約通過'
                WHERE status='pending' AND job_type IN ('backfill','backfill_comments','audit')""",
                (now,),
            )
            count = int(cursor.rowcount)
        if count:
            self.db.add_event(
                "capture-v2:v1-paused",
                "capture_contract_paused",
                {
                    "title": "舊版付費回溯已安全暫停",
                    "text": f"保留並暫停 {count} 個舊工作；未再呼叫無游標 Actor。",
                },
                notify=False,
            )
        return count

    def _recover_stale_capture_v2_jobs(self) -> dict[str, int]:
        """Recover V2 jobs left ``running`` by a stopped container.

        Read-only probes are safe to replay.  A paid job is replayable only
        when no launch happened, a run id was durably saved, or raw/imported
        evidence already exists.  An ambiguous launch is quarantined as
        ``needs_reconcile`` so startup can never purchase the page again.
        """

        counts = {"pending": 0, "needs_reconcile": 0}
        now = utcnow()
        jobs = self.db.rows(
            """SELECT * FROM jobs WHERE status='running' AND job_type IN (
            'detect_public_v2','verify_public_v2','capture_posts_v2','contract_test_posts_v2'
            ) ORDER BY id"""
        )
        for job in jobs:
            # Another scheduler may be actively processing this row.  Startup
            # recovery is only for legacy/unleased or expired work; stealing
            # a live lease could execute the same paid request twice.
            if self._lease_is_active(job):
                continue
            try:
                payload = json.loads(job.get("payload_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                payload = {}
            reconcile_reason = ""
            if job["job_type"] == "capture_posts_v2":
                coverage_stream_id = int(payload.get("coverage_stream_id") or 0)
                epoch_id = int(payload.get("epoch_id") or 0)
                batch = self.db.row(
                    """SELECT * FROM paid_source_batches
                    WHERE coverage_stream_id=? AND epoch_id=? AND status<>'failed'
                    ORDER BY id DESC LIMIT 1""",
                    (coverage_stream_id, epoch_id),
                )
                if batch:
                    batch_status = str(batch.get("status") or "")
                    if batch_status == "launching":
                        if batch.get("run_id"):
                            batch = self.db.transition_paid_source_batch(
                                int(batch["id"]),
                                "run_started",
                                expected_status="launching",
                            )
                        else:
                            self.db.transition_paid_source_batch(
                                int(batch["id"]),
                                "needs_reconcile",
                                expected_status="launching",
                                error="服務重啟時發現 launch 結果不明；禁止自動重買",
                            )
                            reconcile_reason = "Actor launch 結果不明；需人工 reconcile"
                    elif batch_status == "run_started" and not batch.get("run_id"):
                        self.db.transition_paid_source_batch(
                            int(batch["id"]),
                            "needs_reconcile",
                            expected_status="run_started",
                            error="服務重啟時發現 run_started 缺少 run_id",
                        )
                        reconcile_reason = "run_started 缺少 run_id；需人工 reconcile"
                    elif batch_status == "needs_reconcile":
                        reconcile_reason = str(batch.get("error") or "付費批次需人工 reconcile")
                if reconcile_reason and epoch_id:
                    self.db.execute(
                        "UPDATE capture_epochs SET status='needs_reconcile',updated_at=? WHERE id=?",
                        (now, epoch_id),
                    )
            elif job["job_type"] == "contract_test_posts_v2":
                actor_id = str(
                    payload.get("actor_id") or self.settings.actors.posts_v2_primary
                )
                contract_run = self.db.row(
                    """SELECT cr.* FROM contract_runs cr
                    JOIN actor_contracts ac ON ac.id=cr.contract_id
                    WHERE ac.provider='apify' AND ac.actor_id=?
                      AND ac.purpose='posts_backfill'
                      AND cr.status IN ('launching','run_started','needs_reconcile')
                    ORDER BY cr.id DESC LIMIT 1""",
                    (actor_id,),
                )
                if contract_run:
                    run_status = str(contract_run.get("status") or "")
                    if run_status == "launching" or (
                        run_status == "run_started" and not contract_run.get("run_id")
                    ):
                        self.db.execute(
                            """UPDATE contract_runs SET status='needs_reconcile',error=?,finished_at=?
                            WHERE id=?""",
                            (
                                "服務重啟時發現契約 Actor launch 結果不明；禁止自動重買",
                                now,
                                contract_run["id"],
                            ),
                        )
                        reconcile_reason = "契約 Actor launch 結果不明；需人工 reconcile"
                    elif run_status == "needs_reconcile":
                        reconcile_reason = str(
                            contract_run.get("error") or "契約測試需人工 reconcile"
                        )

            if reconcile_reason:
                self.db.execute(
                    "UPDATE jobs SET status='needs_reconcile',finished_at=?,error=? WHERE id=?",
                    (now, reconcile_reason[:2000], job["id"]),
                )
                counts["needs_reconcile"] += 1
            else:
                self.db.execute(
                    """UPDATE jobs SET status='pending',available_at=?,started_at=NULL,
                    finished_at=NULL,error=NULL,lease_owner=NULL,leased_at=NULL WHERE id=?""",
                    (now, job["id"]),
                )
                counts["pending"] += 1
        return counts

    def _seed_capture_v2(self) -> None:
        if not self.settings.capture_v2_enabled:
            return
        special = self._special_profile()
        if not special:
            return
        # Upgrade recovery is deliberately idempotent.  An old boolean
        # backfill_done is not terminal evidence, so an already-public special
        # account receives a V2 epoch as soon as the feature is enabled.
        if (
            str(special.get("public_state") or "") == "public"
            and self._has_confirmed_public_observation(int(special["id"]))
        ):
            self._resume_or_seed_capture_v2_history(special, "upgrade_recovery")
        elif str(special.get("public_state") or "") == "public":
            # A legacy public_state flag is not access evidence.  Re-probe it
            # anonymously before creating any paid epoch.
            self._enqueue_v2_unique(
                int(special["id"]), "detect_public_v2", -400, {"epoch_id": 0}
            )

    def _special_profile(self) -> dict[str, Any] | None:
        target = self.settings.special_profile_id.strip()
        if not target:
            return None
        return self.db.row(
            """SELECT * FROM profiles WHERE enabled=1 AND
            (fb_id=? OR url LIKE ? OR name=? OR name=?) ORDER BY id LIMIT 1""",
            (target, f"%{target}%", target, f"FB-{target}"),
        )

    def _has_confirmed_public_observation(self, profile_id: int) -> bool:
        """Require the latest strong, identity-bound observation to be public.

        An older public observation cannot authorize a paid run after a newer
        anonymous/contract-qualified observation confirms the same identity is
        private.  Weak hints and indeterminate observations are intentionally
        excluded from this ordering.
        """

        latest = self.db.row(
            """SELECT verdict FROM access_observations
            WHERE profile_id=? AND identity_match=1
              AND (
                auth_scope='anonymous'
                OR source='contract_explicit'
                OR source LIKE 'contract:%'
              )
            ORDER BY observed_at DESC,id DESC LIMIT 1""",
            (profile_id,),
        )
        return bool(latest and str(latest.get("verdict") or "") == "confirmed_public")

    def _posts_v2_fingerprint(self, actor_id: str | None = None) -> str:
        selected = actor_id or self.settings.actors.posts_v2_primary
        payload = {
            "actor_id": selected,
            "schema": (
                f"{CAPTURE_V2_CONTRACT_SCHEMA}:spbotdel-profileUrls-maxPostsPerProfile-v1"
                if selected == self.settings.actors.posts_v2_primary
                else f"{CAPTURE_V2_CONTRACT_SCHEMA}:fallback-startUrls-maxPosts-v1"
            ),
            "mapping": self._posts_v2_contract_mapping(selected),
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def _posts_v2_contract_mapping(self, actor_id: str) -> dict[str, Any]:
        if actor_id == self.settings.actors.posts_v2_primary:
            required = {
                "profileUrls": "list[profile_url]",
                "maxPostsPerProfile": "max_posts<=50",
                "expandAllPhotos": True,
                "omitPinnedPosts": True,
                "startCursor": "cursor_if_present",
                "knownPostIds": "known_ids<=20",
            }
        elif actor_id == self.settings.actors.posts_v2_fallback:
            # Fallback has a separate contract on purpose.  If its store
            # schema changes, only its fingerprint expires; a passed primary
            # contract can never authorize the fallback payload.
            required = {
                "startUrls": "list[profile_url]",
                "maxPosts": "max_posts<=50",
                "expandAllPhotos": True,
                "startCursor": "cursor_if_present",
                "knownPostIds": "known_ids<=20",
            }
        else:
            raise ValueError("Actor 不在 Capture V2 primary/fallback 候選名單")
        return {"configured": self.settings.actors.posts_input, "required": required}

    def _valid_posts_v2_contract(self, actor_id: str | None = None) -> dict[str, Any] | None:
        selected = actor_id or self.settings.actors.posts_v2_primary
        contract = self.db.valid_actor_contract(
            provider="apify", actor_id=selected, purpose="posts_backfill"
        )
        if not contract:
            return None
        # A contract is valid only for the exact code/input fingerprint that
        # was tested.  Configuration drift fails closed.
        if str(contract.get("schema_fingerprint") or "") != self._posts_v2_fingerprint(selected):
            return None
        return contract

    def _preferred_posts_v2_contract(self) -> dict[str, Any] | None:
        for actor_id in dict.fromkeys(
            (
                self.settings.actors.posts_v2_primary,
                self.settings.actors.posts_v2_fallback,
            )
        ):
            if contract := self._valid_posts_v2_contract(actor_id):
                return contract
        return None

    def _ensure_capture_v2_epoch(
        self,
        profile: dict[str, Any],
        trigger_reason: str,
        observation_id: int | None = None,
        *,
        intent: CaptureIntent | str | None = None,
        observed_at: datetime | None = None,
    ) -> dict[str, Any]:
        profile_id = int(profile["id"])
        active = self.db.row(
            "SELECT * FROM capture_epochs WHERE profile_id=? AND is_active=1",
            (profile_id,),
        )
        if active:
            # A fully-concluded source-limited epoch used to remain active
            # forever and consequently blocked the next observation epoch.
            self._refresh_capture_v2_epoch(int(active["id"]))
            active = self.db.row(
                "SELECT * FROM capture_epochs WHERE profile_id=? AND is_active=1",
                (profile_id,),
            )

        when = (observed_at or datetime.now(UTC)).astimezone(UTC)
        if intent is None:
            selected_intent = (
                self._capture_v2_regular_intent(profile_id, when)
                if trigger_reason == "regular_visit_v2"
                else (
                    CaptureIntent.RECOVERY_CAPTURE
                    if "recover" in trigger_reason.casefold()
                    else CaptureIntent.INITIAL_PUBLIC_CAPTURE
                )
            )
        else:
            try:
                selected_intent = intent if isinstance(intent, CaptureIntent) else CaptureIntent(intent)
            except ValueError as exc:
                raise ValueError(f"不支援的 Capture V2 intent：{intent}") from exc

        month_key = when.strftime("%Y-%m")
        all_public_history = selected_intent in {
            CaptureIntent.INITIAL_PUBLIC_CAPTURE,
            CaptureIntent.RECOVERY_CAPTURE,
            CaptureIntent.MANUAL_CONTINUE,
        }
        scope: dict[str, Any] = {
            "all_public_history": all_public_history,
            "capture_intent": selected_intent.value,
            "special": bool(
                str(profile.get("fb_id") or "") == self.settings.special_profile_id
                or self.settings.special_profile_id in str(profile.get("url") or "")
            ),
        }
        if selected_intent is CaptureIntent.INCREMENTAL_POLL:
            scope.update({"known_post_limit": 20, "max_posts": 20})
        elif selected_intent is CaptureIntent.MONTHLY_AUDIT:
            scope.update({"observation_month": month_key, "max_posts": 5})

        contract = self._preferred_posts_v2_contract()
        status = "ready" if contract else "awaiting_contract"
        reserve = (
            self.settings.special_capture_reserve_usd
            if str(profile.get("fb_id") or "") == self.settings.special_profile_id
            or self.settings.special_profile_id in str(profile.get("url") or "")
            else 0.0
        )
        if active:
            epoch = active
        else:
            epoch, _ = self.db.get_or_create_capture_epoch(
                profile_id,
                trigger_reason,
                status=status,
                priority=-300 if reserve else -50,
                scope=scope,
                signal_observation_id=observation_id,
                reserved_budget_usd=reserve,
            )
        try:
            epoch_scope = json.loads(epoch.get("scope_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            epoch_scope = {}
        epoch_intent = str(epoch_scope.get("capture_intent") or selected_intent.value)
        stream = self.db.upsert_coverage_stream(
            int(epoch["id"]),
            stream=CoverageStream.POSTS.value,
            surface=CoverageSurface.TIMELINE_POSTS.value,
            provider="apify" if contract else None,
            contract_id=int(contract["id"]) if contract else None,
            status="pending",
        )
        if (
            contract
            and bool(epoch_scope.get("all_public_history", all_public_history))
            and str(stream.get("status") or "") == CoverageStatus.PENDING.value
            and not (stream.get("input_cursor") or stream.get("output_cursor"))
        ):
            recovery = self._capture_v2_recovery_checkpoint(
                profile_id,
                int(contract["id"]),
            )
            if recovery:
                resume_cursor = str(recovery.get("output_cursor") or "").strip()
                if resume_cursor:
                    self.db.update_coverage_stream(
                        int(stream["id"]),
                        input_cursor=resume_cursor,
                        output_cursor=resume_cursor,
                        provider_checkpoint_json={
                            "resumed_from_coverage_id": int(recovery["id"]),
                            "resumed_from_cursor": resume_cursor,
                        },
                    )
                    stream = self.db.row(
                        "SELECT * FROM coverage_streams WHERE id=?",
                        (stream["id"],),
                    ) or stream
        if bool(epoch_scope.get("all_public_history", all_public_history)):
            # A missing coverage row must never be interpreted as a surface
            # that was completely inventoried. These collectors do not yet
            # have an independently validated cursor/terminal contract, so a
            # full-history epoch records the limitation up front. Future
            # collectors may explicitly resume a row from source_limited.
            unsupported_surfaces = (
                (CoverageStream.POSTS, CoverageSurface.REELS),
                (CoverageStream.POSTS, CoverageSurface.VIDEOS),
                (CoverageStream.MEDIA, CoverageSurface.PUBLIC_PHOTO_PAGES),
                (CoverageStream.MEDIA, CoverageSurface.AVATAR_HISTORY),
                (CoverageStream.MEDIA, CoverageSurface.COVER_HISTORY),
            )
            for required_stream, required_surface in unsupported_surfaces:
                manifest_row = self.db.upsert_coverage_stream(
                    int(epoch["id"]),
                    stream=required_stream.value,
                    surface=required_surface.value,
                    status=CoverageStatus.PENDING.value,
                )
                if str(manifest_row.get("status") or "") == CoverageStatus.PENDING.value:
                    self.db.update_coverage_stream(
                        int(manifest_row["id"]),
                        status=CoverageStatus.SOURCE_LIMITED.value,
                        limited_reason=(
                            f"{required_surface.value} 尚未有通過獨立游標與終點契約的 collector"
                        ),
                    )
        if contract:
            self.db.execute(
                "UPDATE capture_epochs SET status='ready',updated_at=? WHERE id=? AND status='awaiting_contract'",
                (utcnow(), epoch["id"]),
            )
            if str(stream.get("status") or "") in {
                CoverageStatus.PENDING.value,
                CoverageStatus.IN_PROGRESS.value,
            }:
                self._enqueue_v2_unique(
                    profile_id,
                    "capture_posts_v2",
                    -300 if reserve else -50,
                    {
                        "epoch_id": int(epoch["id"]),
                        "coverage_stream_id": int(stream["id"]),
                        "intent": epoch_intent,
                    },
                )
        return self.db.row("SELECT * FROM capture_epochs WHERE id=?", (epoch["id"],)) or epoch

    @staticmethod
    def _capture_v2_epoch_scope(epoch: dict[str, Any] | None) -> dict[str, Any]:
        if not epoch:
            return {}
        try:
            scope = json.loads(epoch.get("scope_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            return {}
        return scope if isinstance(scope, dict) else {}

    def _capture_v2_concluded_history(self, profile_id: int) -> dict[str, Any] | None:
        """Return history only after the timeline itself proved its terminal.

        An epoch may be closed as ``source_limited`` solely because an
        auxiliary surface (Reels, avatar history, etc.) has no collector.  It
        is nevertheless a concluded *post* history when timeline_posts is
        complete.  Conversely, a source-limited timeline must stay in recovery
        mode and must never be treated as an incremental baseline.
        """

        for epoch in self.db.rows(
            """SELECT ce.* FROM capture_epochs ce
            JOIN coverage_streams cs ON cs.epoch_id=ce.id
              AND cs.stream='posts' AND cs.surface='timeline_posts'
              AND cs.scope_type='profile' AND cs.scope_id=''
              AND cs.status='complete'
            WHERE ce.profile_id=? AND ce.is_active=0
              AND ce.status IN ('complete','source_limited')
            ORDER BY COALESCE(ce.completed_at,ce.updated_at) DESC,ce.id DESC""",
            (profile_id,),
        ):
            if self._capture_v2_epoch_scope(epoch).get("all_public_history") is True:
                return epoch
        return None

    def _capture_v2_recovery_checkpoint(
        self,
        profile_id: int,
        contract_id: int | None,
    ) -> dict[str, Any] | None:
        """Return the newest compatible, committed post cursor to resume.

        Provider cursors are contract/fingerprint specific.  Never feed a
        cursor to a different contract and never fall back to buying page one
        when a compatible source-limited checkpoint exists.
        """

        if not contract_id:
            return None
        for row in self.db.rows(
            """SELECT cs.*,ce.scope_json FROM coverage_streams cs
            JOIN capture_epochs ce ON ce.id=cs.epoch_id
            WHERE ce.profile_id=? AND ce.is_active=0
              AND cs.stream='posts' AND cs.surface='timeline_posts'
              AND cs.scope_type='profile' AND cs.scope_id=''
              AND cs.status='source_limited' AND cs.output_cursor IS NOT NULL
              AND cs.output_cursor<>'' AND cs.contract_id=?
            ORDER BY COALESCE(ce.completed_at,ce.updated_at) DESC,cs.id DESC""",
            (profile_id, contract_id),
        ):
            try:
                scope = json.loads(str(row.get("scope_json") or "{}"))
            except (TypeError, json.JSONDecodeError):
                scope = {}
            if isinstance(scope, dict) and scope.get("all_public_history") is True:
                return row
        return None

    @staticmethod
    def _capture_v2_month_window(month_key: str):
        try:
            year_text, month_text = month_key.split("-", 1)
            year, month = int(year_text), int(month_text)
            start = datetime(year, month, 1, tzinfo=UTC)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"無效的 Capture V2 月份：{month_key}") from exc
        end = (
            datetime(year + 1, 1, 1, tzinfo=UTC)
            if month == 12
            else datetime(year, month + 1, 1, tzinfo=UTC)
        )
        return deterministic_observation_window(start, end - start, anchor=start)

    def _capture_v2_monthly_epoch_exists(self, profile_id: int, month_key: str) -> bool:
        for epoch in self.db.rows(
            "SELECT scope_json FROM capture_epochs WHERE profile_id=? ORDER BY id DESC",
            (profile_id,),
        ):
            scope = self._capture_v2_epoch_scope(epoch)
            if (
                scope.get("capture_intent") == CaptureIntent.MONTHLY_AUDIT.value
                and scope.get("observation_month") == month_key
            ):
                return True
        return False

    def _capture_v2_regular_intent(
        self,
        profile_id: int,
        observed_at: datetime,
    ) -> CaptureIntent:
        history = self._capture_v2_concluded_history(profile_id)
        if not history:
            contract = self._preferred_posts_v2_contract()
            if self._capture_v2_recovery_checkpoint(
                profile_id,
                int(contract["id"]) if contract else None,
            ):
                return CaptureIntent.RECOVERY_CAPTURE
            return CaptureIntent.INITIAL_PUBLIC_CAPTURE
        month_key = observed_at.astimezone(UTC).strftime("%Y-%m")
        month_start = self._capture_v2_month_window(month_key).start_at
        completed_at = self._capture_v2_datetime(
            history.get("completed_at") or history.get("updated_at")
        )
        if (
            completed_at < month_start
            and not self._capture_v2_monthly_epoch_exists(profile_id, month_key)
        ):
            return CaptureIntent.MONTHLY_AUDIT
        return CaptureIntent.INCREMENTAL_POLL

    def _resume_or_seed_capture_v2_history(
        self,
        profile: dict[str, Any],
        trigger_reason: str,
    ) -> dict[str, Any] | None:
        """Resume one active full capture without recreating it after conclusion."""

        profile_id = int(profile["id"])
        active = self.db.row(
            "SELECT * FROM capture_epochs WHERE profile_id=? AND is_active=1",
            (profile_id,),
        )
        if active:
            self._refresh_capture_v2_epoch(int(active["id"]))
            active = self.db.row(
                "SELECT * FROM capture_epochs WHERE profile_id=? AND is_active=1",
                (profile_id,),
            )
        if active:
            return self._ensure_capture_v2_epoch(profile, trigger_reason)
        if concluded := self._capture_v2_concluded_history(profile_id):
            return concluded
        return self._ensure_capture_v2_epoch(profile, trigger_reason)

    def _enqueue_v2_unique(
        self,
        profile_id: int,
        job_type: str,
        priority: int,
        payload: dict[str, Any],
        available: datetime | None = None,
    ) -> int | None:
        epoch_id = int(payload.get("epoch_id") or 0)
        for row in self.db.rows(
            "SELECT id,payload_json FROM jobs WHERE profile_id=? AND job_type=? AND status IN ('pending','running')",
            (profile_id, job_type),
        ):
            try:
                current = json.loads(row.get("payload_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                current = {}
            if int(current.get("epoch_id") or 0) == epoch_id:
                return None
        return self._enqueue(profile_id, job_type, priority, available or datetime.now(UTC), payload)

    @staticmethod
    def _capture_v2_datetime(value: object) -> datetime:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

    @staticmethod
    def _capture_v2_observed_id(item: dict[str, Any]) -> str:
        # Access-probe Actors return a post object.  Its generic ``id`` is the
        # post ID, not the monitored profile ID, so profile-specific fields
        # must always win.  A validated probe result adds ``profileId`` from
        # the matching SUMMARY before this helper is called.
        for key in ("profileId", "profile_id", "facebookId", "facebook_id", "userId", "user_id", "id"):
            value = str(item.get(key) or "").strip()
            if value.isdigit():
                return value
        for key in ("url", "profileUrl", "profile_url", "facebookUrl"):
            value = str(item.get(key) or "").strip()
            if not value:
                continue
            try:
                candidate = profile_id_from_url(value)
            except SerpApiError:
                continue
            if candidate:
                return candidate
        return ""

    @staticmethod
    def _capture_v2_target_id(profile: dict[str, Any]) -> str:
        configured = str(profile.get("fb_id") or "").strip()
        if configured:
            return configured
        try:
            return profile_id_from_url(str(profile.get("url") or ""))
        except SerpApiError:
            return ""

    @staticmethod
    def _capture_v2_identity_candidates(
        container: dict[str, Any] | None,
        *,
        item: bool,
    ) -> list[tuple[str, str]]:
        """Return only fields that describe the captured profile/author.

        A post's own ``id``/``url`` must never be treated as target-profile
        evidence.  Actor payloads vary, so accept their common author/profile
        aliases while retaining the field path for a fail-closed diagnostic.
        """
        if not isinstance(container, dict):
            return []
        if item:
            id_keys = (
                "authorId", "author_id", "authorFacebookId", "author_facebook_id",
                "ownerId", "owner_id", "profileId", "profile_id", "userId", "user_id",
            )
            url_keys = (
                "authorUrl", "author_url", "ownerUrl", "owner_url",
                "profileUrl", "profile_url", "userUrl", "user_url",
                "facebookUrl", "facebook_url",
            )
            nested_keys = ("author", "owner", "profile", "user")
        else:
            id_keys = (
                "id", "profileId", "profile_id", "facebookId", "facebook_id",
                "userId", "user_id", "accountId", "account_id", "ownerId", "owner_id",
            )
            url_keys = (
                "url", "profileUrl", "profile_url", "facebookUrl", "facebook_url",
                "inputUrl", "input_url", "requestedUrl", "requested_url",
                "canonicalUrl", "canonical_url",
            )
            nested_keys = ("profile", "target", "input", "account", "user")

        candidates: list[tuple[str, str]] = []

        def append(path: str, value: object) -> None:
            if value in (None, "") or isinstance(value, (bool, dict, list, tuple, set)):
                return
            normalized = str(value).strip()
            if normalized:
                candidates.append((path, normalized))

        for key in (*id_keys, *url_keys):
            append(key, container.get(key))
        for nested_key in nested_keys:
            nested = container.get(nested_key)
            if isinstance(nested, dict):
                for key in (
                    "id", "profileId", "profile_id", "facebookId", "facebook_id",
                    "userId", "user_id", "accountId", "account_id",
                    "url", "profileUrl", "profile_url", "facebookUrl", "facebook_url",
                ):
                    append(f"{nested_key}.{key}", nested.get(key))
            else:
                # Some Actors use ``author`` for a display name.  A display
                # name cannot bind a paid result to the monitored profile;
                # only scalar IDs or URLs are identity evidence here.
                value = str(nested or "").strip()
                if value.isdigit() or "://" in value:
                    append(nested_key, value)
        return list(dict.fromkeys(candidates))

    def _capture_v2_identity_matches_target(
        self,
        profile: dict[str, Any],
        value: str,
    ) -> bool:
        observed = str(value or "").strip()
        if not observed:
            return False
        target_id = self._capture_v2_target_id(profile)
        target_url = str(profile.get("url") or "").strip()
        if observed.isdigit():
            return bool(target_id) and observed == target_id
        if "://" in observed:
            try:
                observed_id = profile_id_from_url(observed)
            except SerpApiError:
                observed_id = ""
            if target_id and observed_id:
                if observed_id.casefold() == target_id.casefold():
                    return True
                if observed_id.isdigit():
                    return False
            return bool(target_url) and normalize_url(observed).casefold() == normalize_url(target_url).casefold()
        if target_id and observed.casefold() == target_id.casefold():
            return True
        try:
            target_url_id = profile_id_from_url(target_url)
        except SerpApiError:
            target_url_id = ""
        return bool(target_url_id) and observed.casefold() == target_url_id.casefold()

    def _capture_v2_summary_profile_entry(
        self,
        profile: dict[str, Any],
        summary: dict[str, Any] | None,
    ) -> dict[str, Any]:
        profiles = summary.get("profiles") if isinstance(summary, dict) else None
        entries = [entry for entry in profiles if isinstance(entry, dict)] if isinstance(profiles, list) else []
        if len(entries) != 1:
            raise RuntimeError(
                "Actor SUMMARY 必須且只能包含一個可驗證的目標帳號"
            )
        entry = entries[0]
        candidates = self._capture_v2_identity_candidates(entry, item=False)
        if not candidates:
            candidates = [
                candidate
                for candidate in self._capture_v2_identity_candidates(summary, item=False)
                if candidate[0] not in {"id", "url"}
            ]
        if not candidates:
            raise RuntimeError("Actor SUMMARY 缺少可驗證的目標帳號身分")
        mismatches = [
            (path, value)
            for path, value in candidates
            if not self._capture_v2_identity_matches_target(profile, value)
        ]
        if mismatches:
            target = self._capture_v2_target_id(profile) or str(profile.get("url") or "")
            observed = ", ".join(f"{path}={value}" for path, value in mismatches[:4])
            raise RuntimeError(
                f"Actor SUMMARY 目標帳號身分不符：expected={target}; observed={observed}"
            )
        return entry

    def _capture_v2_validate_actor_result(
        self,
        profile: dict[str, Any],
        items: list[dict[str, Any]],
        summary: dict[str, Any] | None,
        *,
        maximum: int,
    ) -> dict[str, Any]:
        state = self._capture_v2_summary_state(
            summary,
            result_count=len(items),
            maximum=maximum,
            target_profile=profile,
        )
        for index, item in enumerate(items, start=1):
            candidates = self._capture_v2_identity_candidates(item, item=True)
            mismatches = [
                (path, value)
                for path, value in candidates
                if not self._capture_v2_identity_matches_target(profile, value)
            ]
            if mismatches:
                target = self._capture_v2_target_id(profile) or str(profile.get("url") or "")
                observed = ", ".join(f"{path}={value}" for path, value in mismatches[:4])
                raise RuntimeError(
                    f"Actor 第 {index} 筆貼文目標帳號身分不符："
                    f"expected={target}; observed={observed}"
                )
        return state

    def _capture_v2_validate_access_probe_result(
        self,
        profile: dict[str, Any],
        items: list[dict[str, Any]],
        summary: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Bind one paid access probe to its requested Facebook profile.

        The probe Actor returns posts, so neither a post ``id`` nor its
        permalink can prove which profile was requested.  The exact-contract
        SUMMARY is the authoritative target binding; any author/profile field
        on returned posts must agree with it.  Empty output remains an empty
        (indeterminate) signal unless the SUMMARY explicitly says private.
        """

        self._capture_v2_validate_actor_result(
            profile,
            items,
            summary,
            maximum=1,
        )
        entry = self._capture_v2_summary_profile_entry(profile, summary)
        target_id = self._capture_v2_target_id(profile)

        def explicit_private(container: dict[str, Any] | None) -> bool:
            if not isinstance(container, dict):
                return False
            for key in ("private", "isPrivate", "is_private", "profilePrivate"):
                value = container.get(key)
                if value is True or str(value or "").strip().casefold() in {
                    "true",
                    "private",
                    "locked",
                }:
                    return True
            for key in ("privacyStatus", "privacy_status", "accessStatus", "access_status"):
                if str(container.get(key) or "").strip().casefold() in {
                    "private",
                    "locked",
                }:
                    return True
            return False

        is_private = explicit_private(entry) or explicit_private(summary)
        if not items and not is_private:
            return {}
        observed = dict(items[0]) if items else {}
        if target_id:
            observed["profileId"] = target_id
        if profile.get("url"):
            observed["profileUrl"] = str(profile["url"])
        if is_private:
            observed["private"] = True
        return observed

    def _latest_v2_access_state(self, profile_id: int) -> AccessState:
        row = self.db.row(
            "SELECT verdict FROM access_observations WHERE profile_id=? ORDER BY observed_at DESC,id DESC LIMIT 1",
            (profile_id,),
        )
        try:
            return AccessState(str(row["verdict"])) if row else AccessState.UNKNOWN
        except ValueError:
            return AccessState.UNKNOWN

    def _record_capture_v2_access(
        self,
        profile: dict[str, Any],
        *,
        source: EvidenceSource,
        source_label: str,
        auth_scope: AuthScope,
        signal: EvidenceSignal,
        purpose: ObservationPurpose,
        observed_id: str,
        identity_match: bool,
        evidence: dict[str, Any],
        contract_explicit_access: bool = False,
        observed_at: datetime | None = None,
    ) -> tuple[dict[str, Any], EvidenceClass, AccessState]:
        observed_at = observed_at or datetime.now(UTC)
        classification = classify_access_evidence(
            source=source,
            auth_scope=auth_scope,
            signal=signal,
            purpose=purpose,
            identity_matches=identity_match,
            contract_explicit_access=contract_explicit_access,
        )
        current = self._latest_v2_access_state(int(profile["id"]))
        previous_private: StrongPrivateObservation | None = None
        if classification is EvidenceClass.STRONG_PRIVATE:
            previous: dict[str, Any] | None = None
            previous_source: EvidenceSource | None = None
            for candidate in self.db.rows(
                """SELECT source,observed_at,evidence_summary_json
                FROM access_observations
                WHERE profile_id=? AND auth_scope='anonymous' AND identity_match=1
                ORDER BY observed_at DESC,id DESC LIMIT 50""",
                (profile["id"],),
            ):
                try:
                    summary = json.loads(candidate.get("evidence_summary_json") or "{}")
                except (TypeError, json.JSONDecodeError):
                    continue
                if summary.get("classification") != EvidenceClass.STRONG_PRIVATE.value:
                    continue
                source_value = str(summary.get("evidence_source") or "").strip()
                if not source_value:
                    source_label = str(candidate.get("source") or "").casefold()
                    source_value = next(
                        (
                            value.value
                            for marker, value in (
                                ("browser", EvidenceSource.BROWSER),
                                ("bright", EvidenceSource.BRIGHT_DATA),
                                ("serp", EvidenceSource.SERPAPI),
                                ("apify", EvidenceSource.APIFY),
                            )
                            if marker in source_label
                        ),
                        "",
                    )
                try:
                    previous_source = EvidenceSource(source_value)
                except ValueError:
                    continue
                previous = candidate
                break
            if previous and previous_source:
                try:
                    previous_private = StrongPrivateObservation(
                        self._capture_v2_datetime(previous["observed_at"]),
                        previous_source,
                    )
                except ValueError:
                    previous_private = None
        state = next_access_state(
            current,
            classification,
            observed_at=observed_at,
            source=source,
            previous_strong_private=previous_private,
        )
        target_id = self._capture_v2_target_id(profile)
        evidence_hash = hashlib.sha256(canonical_input_json(evidence).encode("utf-8")).hexdigest()
        window = deterministic_observation_window(
            observed_at,
            timedelta(hours=max(1.0, self.settings.special_detection_hours)),
        )
        observation_key = hashlib.sha256(
            canonical_input_json(
                {
                    "profile_id": int(profile["id"]),
                    "source": source_label,
                    "window": window.key,
                    "signal": signal.value,
                    "evidence_hash": evidence_hash,
                }
            ).encode("utf-8")
        ).hexdigest()
        row = self.db.record_access_observation(
            int(profile["id"]),
            source=source_label,
            auth_scope=auth_scope.value,
            verdict=state.value,
            target_fb_id=target_id,
            observed_fb_id=observed_id or None,
            identity_match=identity_match,
            evidence_summary={
                "classification": classification.value,
                "evidence_source": source.value,
                "signal": signal.value,
                **evidence,
            },
            evidence_hash=evidence_hash,
            observed_at=observed_at.isoformat(),
            observation_key=observation_key,
        )
        return row, classification, state

    def _capture_v2_posts_payload(
        self,
        profile: dict[str, Any],
        *,
        actor_id: str | None = None,
        maximum: int,
        cursor: str | None,
        known_post_ids: list[str],
    ) -> dict[str, Any]:
        selected = actor_id or self.settings.actors.posts_v2_primary
        profile_url = str(profile["url"])
        max_posts = max(1, min(50, int(maximum)))
        known = list(dict.fromkeys(str(value) for value in known_post_ids if str(value)))[:20]
        payload: dict[str, Any] = actor_input(
            self.settings.actors.posts_input,
            profile_url=profile_url,
            max_posts=max_posts,
            cursor=str(cursor or ""),
            known_post_ids=known,
        )
        # These fields are the tested Capture V2 contract.  They deliberately
        # win over a stale template so a configuration typo cannot silently
        # turn a cursor run back into a paid first-page request.
        if selected == self.settings.actors.posts_v2_primary:
            for stale in ("startUrls", "maxPosts"):
                payload.pop(stale, None)
            payload.update(
                {
                    "profileUrls": [profile_url],
                    "maxPostsPerProfile": max_posts,
                    "knownPostIds": known,
                    "expandAllPhotos": True,
                    "omitPinnedPosts": True,
                }
            )
        elif selected == self.settings.actors.posts_v2_fallback:
            for stale in ("profileUrls", "maxPostsPerProfile", "omitPinnedPosts"):
                payload.pop(stale, None)
            payload.update(
                {
                    "startUrls": [profile_url],
                    "maxPosts": max_posts,
                    "knownPostIds": known,
                    "expandAllPhotos": True,
                }
            )
        else:
            raise ValueError("Actor 不在 Capture V2 primary/fallback 候選名單")
        if cursor:
            payload["startCursor"] = str(cursor)
        else:
            payload.pop("startCursor", None)
        return payload

    def _capture_v2_known_post_ids(self, profile_id: int, limit: int = 20) -> list[str]:
        return [
            str(row["external_id"])
            for row in self.db.rows(
                """SELECT external_id FROM entities
                WHERE profile_id=? AND kind='post' AND present=1
                ORDER BY COALESCE(published_at,last_seen_at) DESC,id DESC LIMIT ?""",
                (profile_id, limit),
            )
            if row.get("external_id")
        ]

    async def _capture_v2_apify_probe(self, profile: dict[str, Any]) -> dict[str, Any]:
        profile_id = int(profile["id"])
        observed_at = datetime.now(UTC)
        window = deterministic_observation_window(
            observed_at,
            timedelta(hours=max(1.0, self.settings.special_detection_hours)),
        )

        # A paid run from an older observation window must be recovered before
        # considering a new window.  In particular, a restart after start()
        # must finish the persisted run id rather than buy another probe.
        batch = self.db.row(
            """SELECT * FROM paid_access_probe_batches
            WHERE profile_id=? AND status IN(
              'launching','run_started','needs_reconcile','raw_saved','import_failed','imported'
            ) ORDER BY id DESC LIMIT 1""",
            (profile_id,),
        )
        if batch is None:
            batch = self.db.row(
                "SELECT * FROM paid_access_probe_batches WHERE profile_id=? AND observation_window=?",
                (profile_id, window.key),
            )

        status = str(batch.get("status") or "") if batch else ""
        contract: dict[str, Any] | None = None
        request = ""
        if batch:
            actor_id = str(batch["actor_id"])
            try:
                payload = json.loads(str(batch["normalized_input_json"] or "{}"))
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("公開探測已保留的 canonical input 損毀") from exc
            if not isinstance(payload, dict):
                raise RuntimeError("公開探測已保留的 canonical input 格式錯誤")
        else:
            if self.db.profile_source_frozen(profile_id, "apify"):
                raise ApifyFrozen("此帳號已凍結 Apify；公開探測已改走備援")
            contract = self._preferred_posts_v2_contract()
            if not contract:
                raise RuntimeError("Apify 公開探測缺少通過且 fingerprint 相符的 Actor 契約")
            actor_id = str(contract["actor_id"])
            payload = self._capture_v2_posts_payload(
                profile,
                actor_id=actor_id,
                maximum=1,
                cursor=None,
                known_post_ids=[],
            )
            payload["expandAllPhotos"] = False
            request = capture_request_hash(
                capture_intent=CaptureIntent.ACCESS_PROBE,
                window=window,
                profile_id=profile_id,
                epoch_id="access-probe",
                stream=CoverageStream.POSTS,
                surface=CoverageSurface.TIMELINE_POSTS,
                contract_fingerprint=str(contract["schema_fingerprint"]),
                actor_input=payload,
            )
        if batch and status == "launching":
            diagnostic_id = int(batch.get("actor_run_id") or 0)
            batch = self.db.transition_paid_access_probe_batch(
                int(batch["id"]),
                "needs_reconcile",
                expected_status="launching",
                error="Actor launch 結果不明；禁止自動重買公開探測",
            )
            if diagnostic_id:
                self.db.finish_actor_run(
                    diagnostic_id,
                    status="needs_reconcile",
                    error=str(batch["error"]),
                )
            raise RuntimeError("公開探測 Actor launch 結果不明；需要對帳後才能繼續")
        if batch and status == "needs_reconcile":
            raise RuntimeError("公開探測付費執行待對帳；禁止自動重買")
        if batch and status == "failed":
            raise RuntimeError(str(batch.get("error") or "公開探測批次已失敗"))

        if batch is None or status == "prepared":
            if self.db.profile_source_frozen(profile_id, "apify"):
                raise ApifyFrozen("此帳號已凍結 Apify；公開探測已改走備援")
            current_contract = self._preferred_posts_v2_contract()
            if not current_contract:
                raise RuntimeError("Apify 公開探測缺少通過且 fingerprint 相符的 Actor 契約")
            if batch and (
                int(batch["contract_id"]) != int(current_contract["id"])
                or str(batch["actor_id"]) != str(current_contract["actor_id"])
            ):
                raise RuntimeError("公開探測已保留批次的 Actor/contract 已失效；禁止付費啟動")
            contract = current_contract
            official_remaining, usage = await self._official_available()
            historical = self.db.row(
                """SELECT COALESCE(SUM(ar.charged_usd),0) total
                FROM actor_runs ar
                WHERE ar.category='access_probe_v2' AND ar.started_at>=?
                  AND NOT EXISTS(
                    SELECT 1 FROM paid_access_probe_batches pb WHERE pb.actor_run_id=ar.id
                  )""",
                (usage.cycle_start_at,),
            ) or {"total": 0}
            durable = self.db.row(
                """SELECT COALESCE(SUM(charged_usd),0) charged,
                  COALESCE(SUM(CASE
                    WHEN status IN ('launching','run_started','needs_reconcile')
                    THEN MAX(max_charge_usd-charged_usd,0) ELSE 0 END),0) unsettled
                FROM paid_access_probe_batches WHERE created_at>=?""",
                (usage.cycle_start_at,),
            ) or {"charged": 0, "unsettled": 0}
            detection_charged = (
                float(historical["total"] or 0)
                + float(durable["charged"] or 0)
            )
            detection_capacity = max(
                0.0,
                self.settings.special_detection_budget_usd - detection_charged,
            )
            detection_remaining = max(
                0.0,
                detection_capacity - float(durable["unsettled"] or 0),
            )
            reservations = self.db.paid_budget_reservations(
                posts_result_price_usd=PRICES["posts"],
                excluding_access_probe_batch_id=(int(batch["id"]) if batch else None),
            )
            outstanding_reserve = self._capture_v2_outstanding_reserve(
                spending_profile_id=profile_id,
                purpose="access_probe",
            )
            global_capacity = max(
                0.0,
                self.settings.monthly_budget_usd
                - float(usage.used_usd)
                - outstanding_reserve,
            )
            global_available = max(
                0.0,
                global_capacity
                - float(reservations["total_unsettled_usd"]),
            )
            max_charge = min(
                official_remaining,
                global_available,
                detection_remaining,
                0.01,
            )
            if max_charge < PRICES["posts"]:
                raise BudgetExceeded(
                    "特殊帳號 Apify 公開探測保留額不足",
                    self._usage_cycle_resume(usage),
                )
            if batch is None:
                batch, _ = self.db.prepare_paid_access_probe_batch(
                    profile_id=profile_id,
                    contract_id=int(contract["id"]),
                    provider="apify",
                    actor_id=actor_id,
                    observation_window=window.key,
                    normalized_input=payload,
                    max_charge_usd=max_charge,
                    request_hash=request,
                )
            batch = self.db.clamp_paid_access_probe_max_charge(
                int(batch["id"]), max_charge
            )
            if float(batch["max_charge_usd"] or 0) < PRICES["posts"]:
                raise BudgetExceeded(
                    "特殊帳號 Apify 公開探測批次上限不足一筆結果",
                    self._usage_cycle_resume(usage),
                )
            status = str(batch["status"])

        if status == "committed":
            raw = self._load_capture_v2_raw(batch["raw_path"])
            if (
                str(raw.get("request_hash") or "") != str(batch["request_hash"])
                or str(raw.get("actor_id") or "") != str(batch["actor_id"])
            ):
                raise RuntimeError("公開探測 raw 的 request_hash 或 Actor 不符")
            items = [item for item in raw["items"] if isinstance(item, dict)]
            observed = self._capture_v2_validate_access_probe_result(
                profile,
                items,
                raw.get("summary"),
            )
            diagnostic_id = int(batch.get("actor_run_id") or 0)
            if diagnostic_id:
                self.db.finish_actor_run(
                    diagnostic_id,
                    status="succeeded" if items else "succeeded_zero",
                    run_id=str(raw.get("run_id") or batch.get("run_id") or ""),
                    result_count=len(items),
                    charged_usd=float(
                        raw.get("charged_usd") or batch.get("charged_usd") or 0
                    ),
                    summary=raw.get("summary"),
                    samples=items,
                    raw_result_count=int(raw.get("raw_result_count") or len(items)),
                    parsed_result_count=len(items),
                )
            return observed

        raw: dict[str, Any] | None = None
        if status in {"raw_saved", "import_failed", "imported"}:
            raw = self._load_capture_v2_raw(batch["raw_path"])
        elif status in {"prepared", "run_started"}:
            diagnostic_id = int(batch.get("actor_run_id") or 0)
            if status == "prepared":
                # The official-usage lookup above may yield to the operator.
                # Re-read immediately before crossing the paid launch boundary.
                if self.db.profile_source_frozen(profile_id, "apify"):
                    raise ApifyFrozen("此帳號已凍結 Apify；公開探測未啟動付費 Actor")
                batch, claimed = self.db.claim_paid_access_probe_launch(
                    int(batch["id"]),
                    global_capacity_usd=global_capacity,
                    detection_capacity_usd=detection_capacity,
                    posts_result_price_usd=PRICES["posts"],
                )
                if not claimed:
                    if str(batch.get("status") or "") == "prepared":
                        raise BudgetExceeded(
                            "特殊帳號 Apify 公開探測原子預算不足",
                            self._usage_cycle_resume(usage),
                        )
                    raise RuntimeError("公開探測批次已由另一個 worker 取得；禁止重複啟動")
                if not diagnostic_id:
                    diagnostic_id = self.db.start_actor_run(
                        profile_id, "access_probe_v2", actor_id, "special_probe", payload
                    )
                batch = self.db.transition_paid_access_probe_batch(
                    int(batch["id"]),
                    "launching",
                    expected_status="launching",
                    actor_run_id=diagnostic_id,
                    error=None,
                )
                # No await or other external operation may sit between this
                # final freeze check and start().
                if self.db.profile_source_frozen(profile_id, "apify"):
                    self.db.transition_paid_access_probe_batch(
                        int(batch["id"]),
                        "failed",
                        expected_status="launching",
                        error="凍結於 Actor 啟動邊界生效；未產生付費執行",
                    )
                    self.db.finish_actor_run(
                        diagnostic_id,
                        status="failed",
                        error="凍結於 Actor 啟動邊界生效；未啟動",
                    )
                    raise ApifyFrozen("此帳號已凍結 Apify；公開探測未啟動付費 Actor")
                try:
                    started = await self.apify.start(
                        actor_id, payload, float(batch["max_charge_usd"])
                    )
                except Exception as exc:
                    self.db.transition_paid_access_probe_batch(
                        int(batch["id"]),
                        "needs_reconcile",
                        expected_status="launching",
                        error=str(exc)[:4000],
                    )
                    self.db.finish_actor_run(
                        diagnostic_id, status="needs_reconcile", error=str(exc)
                    )
                    raise RuntimeError("公開探測 Actor launch 結果不明；已停止自動重買") from exc
                batch = self.db.transition_paid_access_probe_batch(
                    int(batch["id"]),
                    "run_started",
                    expected_status="launching",
                    run_id=started.run_id,
                    dataset_id=started.dataset_id,
                    key_value_store_id=started.key_value_store_id,
                    error=None,
                )
            else:
                started = StartedActor(
                    str(batch.get("run_id") or ""),
                    str(batch.get("dataset_id") or ""),
                    str(batch.get("key_value_store_id") or ""),
                )
                if not started.run_id:
                    self.db.transition_paid_access_probe_batch(
                        int(batch["id"]),
                        "needs_reconcile",
                        expected_status="run_started",
                        error="run_started 缺少 run_id",
                    )
                    raise RuntimeError("公開探測 run_started 缺少 run_id；禁止自動重買")
            path = self._capture_v2_raw_path(str(batch["request_hash"]))
            result: ActorResult | None = None
            if path.is_file():
                # A process may exit after the atomic rename but before the
                # raw_saved transaction.  The immutable local artifact is
                # authoritative and can be replayed without even polling the
                # provider again.
                raw = self._load_capture_v2_raw(path)
                if (
                    str(raw.get("request_hash") or "") != str(batch["request_hash"])
                    or str(raw.get("actor_id") or "") != str(batch["actor_id"])
                ):
                    raise RuntimeError("公開探測 raw 的 request_hash 或 Actor 不符")
                raw_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
            else:
                try:
                    result = await self.apify.finish(started)
                    path, raw_sha256 = self._save_capture_v2_raw(batch, result)
                except Exception as exc:
                    self.db.transition_paid_access_probe_batch(
                        int(batch["id"]),
                        "needs_reconcile",
                        expected_status="run_started",
                        error=str(exc)[:4000],
                    )
                    if diagnostic_id:
                        self.db.finish_actor_run(
                            diagnostic_id,
                            status="needs_reconcile",
                            run_id=str(batch.get("run_id") or ""),
                            error=str(exc),
                        )
                    raise
                raw = self._load_capture_v2_raw(path)
            batch = self.db.transition_paid_access_probe_batch(
                int(batch["id"]),
                "raw_saved",
                expected_status="run_started",
                raw_path=str(path),
                raw_sha256=raw_sha256,
                charged_usd=float(
                    result.charged_usd if result else raw.get("charged_usd") or 0
                ),
                raw_result_count=(
                    result.raw_result_count
                    if result and result.raw_result_count is not None
                    else len(result.items) if result else len(raw["items"])
                ),
                error=None,
            )
        else:
            raise RuntimeError(f"公開探測付費批次狀態無法自動處理：{status}")

        assert raw is not None
        if (
            str(raw.get("request_hash") or "") != str(batch["request_hash"])
            or str(raw.get("actor_id") or "") != str(batch["actor_id"])
        ):
            raise RuntimeError("公開探測 raw 的 request_hash 或 Actor 不符")
        summary_error = actor_summary_error(raw.get("summary"))
        if summary_error:
            if str(batch["status"]) in {"raw_saved", "import_failed"}:
                self.db.transition_paid_access_probe_batch(
                    int(batch["id"]),
                    "import_failed",
                    expected_status=str(batch["status"]),
                    error=summary_error,
                )
            raise RuntimeError(summary_error)
        items = [item for item in raw["items"] if isinstance(item, dict)]
        try:
            observed = self._capture_v2_validate_access_probe_result(
                profile,
                items,
                raw.get("summary"),
            )
        except RuntimeError as exc:
            if str(batch["status"]) in {"raw_saved", "import_failed", "imported"}:
                self.db.transition_paid_access_probe_batch(
                    int(batch["id"]),
                    "import_failed",
                    expected_status=str(batch["status"]),
                    parsed_result_count=len(items),
                    error=str(exc)[:4000],
                )
            diagnostic_id = int(batch.get("actor_run_id") or 0)
            if diagnostic_id:
                self.db.finish_actor_run(
                    diagnostic_id,
                    status="failed",
                    run_id=str(raw.get("run_id") or batch.get("run_id") or ""),
                    result_count=len(items),
                    charged_usd=float(raw.get("charged_usd") or batch.get("charged_usd") or 0),
                    summary=raw.get("summary"),
                    samples=items,
                    raw_result_count=int(raw.get("raw_result_count") or len(items)),
                    parsed_result_count=0,
                    error=str(exc),
                )
            raise
        if str(batch["status"]) in {"raw_saved", "import_failed"}:
            batch = self.db.transition_paid_access_probe_batch(
                int(batch["id"]),
                "imported",
                expected_status=str(batch["status"]),
                parsed_result_count=len(items),
                error=None,
            )
        if str(batch["status"]) == "imported":
            batch = self.db.transition_paid_access_probe_batch(
                int(batch["id"]), "committed", expected_status="imported", error=None
            )
        diagnostic_id = int(batch.get("actor_run_id") or 0)
        if diagnostic_id:
            self.db.finish_actor_run(
                diagnostic_id,
                status="succeeded" if items else "succeeded_zero",
                run_id=str(raw.get("run_id") or batch.get("run_id") or ""),
                result_count=len(items),
                charged_usd=float(raw.get("charged_usd") or batch.get("charged_usd") or 0),
                summary=raw.get("summary"),
                samples=items,
                raw_result_count=int(raw.get("raw_result_count") or len(items)),
                parsed_result_count=len(items),
            )
        return observed

    async def detect_public_v2(self, profile_id: int) -> None:
        """Run one scheduled anonymous API probe without changing public_state."""
        profile = self.db.row("SELECT * FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile or (
            str(profile.get("public_state") or "") == "public"
            and self._has_confirmed_public_observation(profile_id)
        ):
            return
        slot = int(
            (self.db.row(
                "SELECT COUNT(*) count FROM access_observations WHERE profile_id=? AND source LIKE 'special_probe:%'",
                (profile_id,),
            ) or {"count": 0})["count"]
        )
        serp_snapshot = self.db.serpapi_usage_snapshot()
        decision = choose_probe_source(
            slot,
            apify_frozen=self.db.profile_source_frozen(profile_id, "apify"),
            serpapi_available=bool(self.settings.serpapi_key)
            and not (serp_snapshot and int(serp_snapshot.get("searches_left") or 0) <= 0),
            bright_data_available=bool(self.settings.brightdata_api_token),
            apify_available=bool(self._preferred_posts_v2_contract()),
        )
        selected = decision.selected
        item: dict[str, Any] = {}
        error = ""
        try:
            if selected is ProbeSource.SERPAPI:
                result = await self.serpapi.profile(str(profile["url"]))
                result.account.searches_left = max(0, result.account.searches_left - 1)
                result.account.this_month_usage += 1
                self.db.save_serpapi_usage(result.account)
                item = dict(result.item)
            elif selected is ProbeSource.APIFY:
                item = await self._capture_v2_apify_probe(profile)
            elif selected is ProbeSource.BRIGHT_DATA:
                item = await self.brightdata.profile(str(profile["url"]))
            else:
                error = decision.reason
        except (SerpApiError, BrightDataError, ApifyFrozen, BudgetExceeded, RuntimeError) as exc:
            error = str(exc)
            if selected is not ProbeSource.BRIGHT_DATA and self.settings.brightdata_api_token:
                try:
                    item = await self.brightdata.profile(str(profile["url"]))
                    selected = ProbeSource.BRIGHT_DATA
                    error = ""
                except BrightDataError as fallback_exc:
                    error = f"{error}; Bright Data: {fallback_exc}"

        target_id = self._capture_v2_target_id(profile)
        observed_id = self._capture_v2_observed_id(item)
        identity_match = bool(target_id and observed_id and target_id == observed_id)
        if item:
            signal = EvidenceSignal.EXPLICIT_PRIVATE if bool(item.get("private") or item.get("is_private")) else EvidenceSignal.PUBLIC_CONTENT
            evidence = {"provider": selected.value, "fields": sorted(item)[:30]}
        else:
            signal = EvidenceSignal.NO_ITEMS if not error else EvidenceSignal.HTTP_ERROR
            # A provider failure says nothing about identity, but identity must
            # be treated as known here so empty/error remains indeterminate
            # instead of being mislabelled as an identity mismatch.
            observed_id = target_id
            identity_match = bool(target_id)
            evidence = {"provider": selected.value, "error": error or "no_items", "reason": decision.reason}
        observation, classification, state = self._record_capture_v2_access(
            profile,
            source={
                ProbeSource.SERPAPI: EvidenceSource.SERPAPI,
                ProbeSource.APIFY: EvidenceSource.APIFY,
                ProbeSource.BRIGHT_DATA: EvidenceSource.BRIGHT_DATA,
            }.get(selected, EvidenceSource.SERPAPI),
            source_label=f"special_probe:{selected.value}",
            auth_scope=AuthScope.ANONYMOUS,
            signal=signal,
            purpose=ObservationPurpose.GENERAL_PROBE,
            observed_id=observed_id,
            identity_match=identity_match,
            evidence=evidence,
        )
        if classification is EvidenceClass.SUSPECTED_PUBLIC and state is AccessState.SUSPECTED_PUBLIC:
            self._enqueue_v2_unique(profile_id, "verify_public_v2", -410, {"epoch_id": 0})
        elif decision.degraded or error:
            self.db.add_event(
                f"capture-v2:detection:{profile_id}:{observation['id']}",
                "capture_detection_degraded",
                {
                    "title": "特殊帳號公開偵測受限",
                    "text": evidence.get("error") or decision.reason,
                    "source_url": profile["url"],
                },
                profile_id,
                notify=False,
            )

    async def verify_public_v2(self, profile_id: int) -> None:
        """Confirm access only with a cookie-free anonymous browser context."""
        profile = self.db.row("SELECT * FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            return
        if self.settings.facebook_browser_enabled:
            self._acquire_browser(
                profile,
                anonymous=True,
                operation="verify_public_v2",
                defer_job=True,
            )
        target_id = self._capture_v2_target_id(profile)
        item: dict[str, Any] = {}
        error = ""
        signal = EvidenceSignal.PARSE_ERROR
        diagnostic_key = f"anonymous-verify-{profile_id}"
        try:
            item = await self.facebook_anonymous_browser.profile(
                str(profile["url"]), diagnostic_key
            )
            self.anonymous_browser_guard.record_success(profile_id)
            if bool(item.get("private") or item.get("is_private")):
                signal = EvidenceSignal.EXPLICIT_PRIVATE
            elif public_content_proof_matches_profile(
                item.get("public_content_proof"), str(profile["url"])
            ):
                signal = EvidenceSignal.PUBLIC_CONTENT
            else:
                # Profile metadata alone (name/avatar/friend count) remains
                # visible in multiple restricted states.  Anonymous public
                # verification therefore fails closed unless the gateway saw
                # an identity-bound, stable target article permalink.
                signal = EvidenceSignal.PARSE_ERROR
                error = "anonymous page lacks target public-content proof"
        except FacebookBrowserChallengeRequired as exc:
            self._record_browser_challenge(
                profile,
                anonymous=True,
                diagnostic_key=diagnostic_key,
                error=exc,
            )
            signal, error = EvidenceSignal.HTTP_ERROR, str(exc)
        except FacebookBrowserLoginRequired as exc:
            # A cookie-free session may legitimately reach a login wall.  It
            # is indeterminate access evidence, not a reason to trip the
            # shared browser breaker.  It also does not prove that a previous
            # challenge has cleared, so an active half-open probe must remain
            # half-open until its lease expires and a conclusive probe runs.
            signal, error = EvidenceSignal.LOGIN_WALL, str(exc)
        except FacebookBrowserError as exc:
            error = str(exc)
            signal = EvidenceSignal.TIMEOUT if "timeout" in error.casefold() or "逾時" in error else EvidenceSignal.PARSE_ERROR

        if signal is EvidenceSignal.EXPLICIT_PRIVATE:
            # The regular item ID may be a display fallback copied from the
            # requested URL.  A privacy transition requires identity observed
            # on the rendered page itself, never a request that self-proves.
            observed_id = str(item.get("observed_profile_identity") or "").strip()
        else:
            observed_id = self._capture_v2_observed_id(item) if item else ""
        identity_match = bool(target_id and observed_id and target_id == observed_id)
        observation, classification, state = self._record_capture_v2_access(
            profile,
            source=EvidenceSource.BROWSER,
            source_label="anonymous_browser",
            auth_scope=AuthScope.ANONYMOUS,
            signal=signal,
            purpose=ObservationPurpose.VERIFICATION,
            observed_id=observed_id,
            identity_match=identity_match,
            evidence={
                "fields": sorted(item)[:30],
                "error": error,
                "public_content_proof": item.get("public_content_proof"),
            },
        )
        previous_state = str(profile.get("public_state") or "unknown")
        display = str(profile.get("display_name") or profile.get("name") or target_id or "Facebook")
        if classification is EvidenceClass.STRONG_PUBLIC and state is AccessState.CONFIRMED_PUBLIC:
            self.db.execute(
                "UPDATE profiles SET public_state='public',last_success_at=?,last_error=NULL WHERE id=?",
                (utcnow(), profile_id),
            )
            if previous_state != "public":
                self.db.add_event(
                    f"capture-v2:profile:{profile_id}:opened:{observation['id']}",
                    "profile_opened",
                    {"title": f"{display} 已公開", "source_url": profile["url"]},
                    profile_id,
                )
            refreshed = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,)) or profile
            self._ensure_capture_v2_epoch(refreshed, "public_transition", int(observation["id"]))
        elif classification is EvidenceClass.STRONG_PRIVATE and state is AccessState.CONFIRMED_PRIVATE:
            self.db.execute("UPDATE profiles SET public_state='private' WHERE id=?", (profile_id,))
            if previous_state != "private":
                self.db.add_event(
                    f"capture-v2:profile:{profile_id}:private:{observation['id']}",
                    "profile_private",
                    {"title": f"{display} 目前為私人帳號", "source_url": profile["url"]},
                    profile_id,
                )

    @staticmethod
    def _capture_v2_normalize_post_id(value: object) -> str:
        if value in (None, "") or isinstance(value, (bool, dict, list, tuple, set)):
            return ""
        normalized = str(value).strip()
        if normalized.casefold().startswith("post:"):
            normalized = normalized[5:].strip()
        return normalized

    @staticmethod
    def _capture_v2_item_source_url(item: dict[str, Any]) -> str:
        return next(
            (
                str(item.get(key))
                for key in ("source_url", "postUrl", "post_url", "url", "facebookUrl")
                if item.get(key)
            ),
            "",
        )

    @classmethod
    def _capture_v2_item_stable_ids(cls, item: dict[str, Any]) -> list[str]:
        values = [
            cls._capture_v2_normalize_post_id(item.get(key))
            for key in (
                "sourcePostId",
                "source_post_id",
                "postId",
                "post_id",
                "storyFbid",
                "story_fbid",
                "facebookPostId",
                "facebook_post_id",
            )
        ]
        return list(dict.fromkeys(value for value in values if value))

    @classmethod
    def _capture_v2_item_identity(cls, item: dict[str, Any]) -> str:
        # Actor URL forms such as /share/p/<token> are aliases, not the
        # canonical post identity.  Prefer an explicit provider post ID so a
        # later permalink.php?story_fbid=<id> page converges on the same row.
        stable_ids = cls._capture_v2_item_stable_ids(item)
        if stable_ids:
            return stable_ids[0]
        source_url = cls._capture_v2_item_source_url(item)
        canonical = facebook_post_identity(source_url)
        if canonical:
            return cls._capture_v2_normalize_post_id(canonical)
        try:
            return cls._capture_v2_normalize_post_id(external_id(item, "post"))
        except (TypeError, ValueError):
            return ""

    @classmethod
    def _capture_v2_item_alias_candidates(
        cls, item: dict[str, Any]
    ) -> list[tuple[str, str]]:
        aliases: list[tuple[str, str]] = []
        stable_ids = cls._capture_v2_item_stable_ids(item)
        source_url = cls._capture_v2_item_source_url(item)
        url_identity = cls._capture_v2_normalize_post_id(
            facebook_post_identity(source_url)
        )
        for post_id in [*stable_ids, url_identity]:
            if not post_id:
                continue
            aliases.extend(
                (
                    ("facebook_post_id", post_id),
                    ("external_id", post_id),
                    # Resolve rows produced before Capture V2 removed its
                    # artificial ``post:`` prefix.  New aliases never use it.
                    ("external_id", f"post:{post_id}"),
                )
            )
        normalized_url = normalize_url(source_url) if source_url else ""
        if normalized_url:
            aliases.append(("source_url", normalized_url))
        return list(dict.fromkeys(aliases))

    def _capture_v2_resolve_post_entity(
        self, profile_id: int, item: dict[str, Any]
    ) -> dict[str, Any] | None:
        aliases = self._capture_v2_item_alias_candidates(item)
        for alias_type, alias_value in aliases:
            alias = self.db.row(
                """SELECT * FROM post_aliases
                WHERE profile_id=? AND alias_type=? AND alias_value=?""",
                (profile_id, alias_type, alias_value),
            )
            if not alias:
                continue
            if alias.get("entity_id") is not None:
                entity = self.db.row(
                    """SELECT * FROM entities
                    WHERE id=? AND profile_id=? AND kind='post'""",
                    (alias["entity_id"], profile_id),
                )
                if entity:
                    return entity
            canonical = self._capture_v2_normalize_post_id(
                alias.get("canonical_post_id")
            )
            for external in (canonical, f"post:{canonical}" if canonical else ""):
                if not external:
                    continue
                entity = self.db.row(
                    """SELECT * FROM entities
                    WHERE profile_id=? AND kind='post' AND external_id=?""",
                    (profile_id, external),
                )
                if entity:
                    return entity

        candidate_ids = [
            value
            for alias_type, value in aliases
            if alias_type == "external_id" and value
        ]
        identity = self._capture_v2_item_identity(item)
        if identity:
            candidate_ids.extend((identity, f"post:{identity}"))
        for external in dict.fromkeys(candidate_ids):
            entity = self.db.row(
                """SELECT * FROM entities
                WHERE profile_id=? AND kind='post' AND external_id=?""",
                (profile_id, external),
            )
            if entity:
                return entity

        source_url = self._capture_v2_item_source_url(item)
        if source_url:
            normalized_url = normalize_url(source_url)
            for entity in self.db.rows(
                """SELECT * FROM entities
                WHERE profile_id=? AND kind='post' AND source_url IS NOT NULL""",
                (profile_id,),
            ):
                if normalize_url(str(entity.get("source_url") or "")) == normalized_url:
                    return entity
        return None

    def _capture_v2_record_post_aliases(
        self,
        profile_id: int,
        item: dict[str, Any],
        *,
        identity: str,
        persisted_id: str,
        entity_id: int,
        provider: str,
    ) -> None:
        source_url = self._capture_v2_item_source_url(item)
        canonical_id = self._capture_v2_normalize_post_id(identity or persisted_id)
        aliases = [
            (alias_type, alias_value)
            for alias_type, alias_value in self._capture_v2_item_alias_candidates(item)
            if not (alias_type == "external_id" and alias_value.startswith("post:"))
        ]
        aliases.append(("external_id", persisted_id))
        for alias_type, alias_value in dict.fromkeys(aliases):
            self.db.upsert_post_alias(
                profile_id,
                canonical_post_id=canonical_id,
                provider=provider,
                alias_type=alias_type,
                alias_value=alias_value,
                entity_id=entity_id,
                normalized_url=normalize_url(source_url) if source_url else None,
                source_url=source_url or None,
            )

    @staticmethod
    def _capture_v2_item_is_pinned(item: dict[str, Any]) -> bool:
        for key in ("isPinned", "is_pinned", "isPinnedPost", "pinned"):
            value = item.get(key)
            if value is True or str(value).strip().casefold() in {"1", "true", "yes"}:
                return True
        return False

    @staticmethod
    def _capture_v2_item_published_at(item: dict[str, Any]) -> datetime | None:
        for key in (
            "timestamp",
            "publishedAt",
            "published_at",
            "publishedTime",
            "postedAt",
            "createdAt",
            "creationTime",
            "date",
            "time",
        ):
            value = item.get(key)
            if value in (None, "") or isinstance(value, bool):
                continue
            if isinstance(value, (int, float)) or str(value).strip().replace(".", "", 1).isdigit():
                try:
                    stamp = float(value)
                    if stamp > 100_000_000_000:
                        stamp /= 1000
                    return datetime.fromtimestamp(stamp, UTC)
                except (OSError, OverflowError, ValueError):
                    continue
            try:
                parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
            except ValueError:
                continue
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
        return None

    def _capture_v2_summary_state(
        self,
        summary: dict[str, Any] | None,
        *,
        result_count: int,
        maximum: int,
        target_profile: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        profiles = summary.get("profiles") if isinstance(summary, dict) else None
        entry = (
            self._capture_v2_summary_profile_entry(target_profile, summary)
            if target_profile is not None
            else next((value for value in profiles if isinstance(value, dict)), None)
            if isinstance(profiles, list)
            else None
        )
        pointer = entry.get("pointer") if isinstance(entry, dict) and isinstance(entry.get("pointer"), dict) else {}
        cursor = ""
        for container in (pointer, entry or {}, summary or {}):
            for key in ("nextCursor", "next_cursor", "cursor"):
                value = container.get(key) if isinstance(container, dict) else None
                if value not in (None, ""):
                    cursor = str(value)
                    break
            if cursor:
                break
        coverage = str(
            (entry or {}).get("coverageStatus")
            or (entry or {}).get("coverage_status")
            or (summary or {}).get("coverageStatus")
            or ""
        ).strip()
        normalized_coverage = coverage.casefold().replace("-", "_").replace(" ", "_")
        terminal_values = {
            "complete", "completed", "terminal", "exhausted", "end", "end_of_results",
            "fully_captured", "complete_feed_exhausted", "no_public_posts",
        }
        terminal = normalized_coverage in terminal_values
        terminal_fields: dict[str, Any] = {}
        for label, container in (("pointer", pointer), ("profile", entry or {}), ("summary", summary or {})):
            for key in ("terminal", "isTerminal", "isComplete", "completed", "endReached", "endOfResults"):
                if key in container and container.get(key) is True:
                    terminal = True
                    terminal_fields[f"{label}.{key}"] = True
            if "hasNextPage" in container and container.get("hasNextPage") is False:
                terminal = True
                terminal_fields[f"{label}.hasNextPage"] = False
        # This means a requested item/limit boundary was reached, not that the
        # profile's public history is exhausted.
        if normalized_coverage == "complete_target_reached":
            terminal = False
            terminal_fields = {}
        capped = (
            normalized_coverage.startswith(("partial", "capped", "limit", "max_"))
            or (result_count >= maximum and not cursor and not terminal)
        )
        evidence = {
            "source": "SUMMARY",
            "coverage_status": coverage,
            "terminal_fields": terminal_fields,
            "result_count": result_count,
        }
        return {
            "summary_present": isinstance(summary, dict),
            "profile_present": isinstance(entry, dict),
            "cursor": cursor or None,
            "coverage": coverage,
            "terminal": bool(terminal),
            "capped": bool(capped),
            "terminal_evidence": evidence if terminal else None,
        }

    @staticmethod
    def _capture_v2_known_boundary_reached(summary: dict[str, Any] | None) -> bool:
        if not isinstance(summary, dict):
            return False
        profiles = summary.get("profiles")
        containers = [summary]
        if isinstance(profiles, list):
            containers.extend(entry for entry in profiles if isinstance(entry, dict))
        accepted = {
            "complete_until_known_post",
            "known_post_boundary",
            "stopped_at_known_post",
        }
        for container in containers:
            for key in ("coverageStatus", "coverage_status", "stopReason", "stop_reason"):
                normalized = str(container.get(key) or "").casefold().replace("-", "_").replace(" ", "_")
                if normalized in accepted:
                    return True
            if container.get("knownPostBoundaryReached") is True:
                return True
        return False

    @staticmethod
    def _capture_v2_contract_result(row: dict[str, Any]) -> ActorResult:
        try:
            value = json.loads(str(row.get("result_json") or "{}"))
        except (TypeError, json.JSONDecodeError):
            value = {}
        return ActorResult(
            items=[item for item in value.get("items", []) if isinstance(item, dict)],
            summary=value.get("summary") if isinstance(value.get("summary"), dict) else None,
            run_id=str(row.get("run_id") or ""),
            charged_usd=float(row.get("charged_usd") or 0),
            raw_result_count=int(row.get("result_count") or 0),
        )

    def _capture_v2_contract_allocation(
        self,
        *,
        profile_id: int,
        actor_id: str,
        schema_fingerprint: str,
        test_generation: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve the operator-authorized allocation for one paid contract test.

        The web route is only a convenience layer.  This guard deliberately
        lives at the service payment boundary so a hand-written job or direct
        method call cannot bypass the global $0.20 grant ledger.
        """

        try:
            grant_id = int(payload.get("contract_grant_id") or 0)
            allocation_id = int(payload.get("contract_allocation_id") or 0)
        except (TypeError, ValueError):
            grant_id = allocation_id = 0
        if grant_id <= 0 or allocation_id <= 0:
            raise BudgetExceeded("Capture V2 契約測試缺少操作人核准的付費授權")
        allocation = self.db.row(
            """SELECT a.*,g.purpose,g.status AS grant_status,g.expires_at,
            j.status AS job_status,j.profile_id AS job_profile_id,j.job_type
            FROM contract_test_allocations a
            JOIN contract_test_grants g ON g.id=a.grant_id
            LEFT JOIN jobs j ON j.id=a.job_id
            WHERE a.id=? AND a.grant_id=?""",
            (allocation_id, grant_id),
        )
        expected = {
            "profile_id": int(profile_id),
            "actor_id": str(actor_id),
            "schema_fingerprint": str(schema_fingerprint),
            "test_generation": str(test_generation),
        }
        if not allocation or any(
            str(allocation.get(key) if key != "profile_id" else int(allocation.get(key) or 0))
            != str(value)
            for key, value in expected.items()
        ):
            raise BudgetExceeded("Capture V2 契約測試付費授權與帳號、Actor 或版本不相符")
        if (
            str(allocation.get("purpose") or "") != "posts_cursor"
            or str(allocation.get("grant_status") or "") not in {"active", "fulfilled"}
            or str(allocation.get("job_status") or "") != "running"
            or int(allocation.get("job_profile_id") or 0) != int(profile_id)
            or str(allocation.get("job_type") or "") != "contract_test_posts_v2"
            or float(allocation.get("authorized_usd") or 0) <= 0
        ):
            raise BudgetExceeded("Capture V2 契約測試付費授權目前不可使用")
        if self._capture_v2_datetime(allocation["expires_at"]) <= datetime.now(UTC):
            raise BudgetExceeded("Capture V2 契約測試付費授權已逾期")
        return allocation

    def _capture_v2_contract_launch_authorized(self, allocation_id: int) -> bool:
        allocation = self.db.row(
            """SELECT g.status AS grant_status,g.expires_at,j.status AS job_status
            FROM contract_test_allocations a
            JOIN contract_test_grants g ON g.id=a.grant_id
            LEFT JOIN jobs j ON j.id=a.job_id
            WHERE a.id=?""",
            (allocation_id,),
        )
        return bool(
            allocation
            and str(allocation.get("grant_status") or "") == "active"
            and str(allocation.get("job_status") or "") == "running"
            and self._capture_v2_datetime(allocation["expires_at"]) > datetime.now(UTC)
        )

    async def _run_capture_v2_contract_case(
        self,
        *,
        contract: dict[str, Any],
        profile: dict[str, Any],
        test_generation: str,
        test_case: str,
        payload: dict[str, Any],
        max_charge_usd: float,
        grant_allocation_id: int,
    ) -> ActorResult:
        created_at = self._capture_v2_datetime(contract["created_at"])
        window = deterministic_observation_window(
            created_at,
            timedelta(days=36500),
            anchor=created_at,
        )
        request = capture_request_hash(
            capture_intent=CaptureIntent.CONTRACT_TEST,
            window=window,
            profile_id=profile["id"],
            epoch_id=f"contract:{contract['id']}:{test_generation}:{test_case}",
            stream=CoverageStream.POSTS,
            surface=CoverageSurface.TIMELINE_POSTS,
            contract_fingerprint=str(contract["schema_fingerprint"]),
            actor_input=payload,
        )
        row, created = self.db.record_contract_run(
            int(contract["id"]),
            test_case=test_case,
            normalized_input=payload,
            expected={
                "summary_profiles": True,
                "target_profile_id": self._capture_v2_target_id(profile),
                "target_profile_url": normalize_url(str(profile.get("url") or "")),
                "item_author_identity_if_present": True,
                "expandAllPhotos": True,
            },
            request_hash=request,
        )
        if (
            int(row.get("grant_allocation_id") or 0) != int(grant_allocation_id)
            or float(row.get("authorized_max_usd") or 0) <= 0
        ):
            raise BudgetExceeded("契約測試批次沒有可驗證的付費授權；禁止啟動 Actor")
        max_charge_usd = min(
            max(0.0, float(max_charge_usd)),
            max(0.0, float(row["authorized_max_usd"])),
        )
        if max_charge_usd <= 0:
            raise BudgetExceeded("契約測試批次核准額度為零；禁止啟動 Actor")
        maximum = max(
            1,
            min(
                50,
                int(payload.get("maxPostsPerProfile") or payload.get("maxPosts") or 1),
            ),
        )
        status = str(row.get("status") or "pending")
        if status == "succeeded":
            cached = self._capture_v2_contract_result(row)
            self._capture_v2_validate_actor_result(
                profile,
                cached.items,
                cached.summary,
                maximum=maximum,
            )
            return cached
        if status in {"launching", "needs_reconcile", "failed"}:
            if status == "launching" and not self._lease_is_active(row):
                self.db.execute(
                    """UPDATE contract_runs SET status='needs_reconcile',error=?,finished_at=?
                    WHERE id=? AND status='launching'
                    AND (leased_at=? OR (leased_at IS NULL AND ? IS NULL))""",
                    (
                        "Actor launch 結果不明；禁止自動重買",
                        utcnow(),
                        row["id"],
                        row.get("leased_at"),
                        row.get("leased_at"),
                    ),
                )
            elif status == "launching":
                raise RuntimeError(f"契約測試 {test_case} 已由另一個 worker 執行")
            raise RuntimeError(f"契約測試 {test_case} 狀態為 {status}；不自動重買")
        if status == "run_started" and row.get("run_id"):
            started = StartedActor(str(row["run_id"]), str(row.get("dataset_id") or ""), "")
        else:
            if self.db.profile_source_frozen(int(profile["id"]), "apify"):
                raise ApifyFrozen("此帳號已凍結 Apify；契約測試未啟動付費 Actor")
            if not self._capture_v2_contract_launch_authorized(grant_allocation_id):
                raise BudgetExceeded("契約測試付費授權已關閉或逾期；禁止啟動 Actor")
            # Each individual case has its own paid launch.  Provider usage is
            # therefore refreshed for every case, not merely once at the
            # beginning of the four-case suite.  No event-loop await is
            # allowed between the checks below and the atomic DB claim.
            _, official_usage = await self._official_available()
            if self.db.profile_source_frozen(int(profile["id"]), "apify"):
                raise ApifyFrozen("此帳號已凍結 Apify；契約測試未啟動付費 Actor")
            if not self._capture_v2_contract_launch_authorized(grant_allocation_id):
                raise BudgetExceeded("契約測試付費授權已關閉或逾期；禁止啟動 Actor")
            outstanding_reserve = self._capture_v2_outstanding_reserve(
                spending_profile_id=int(profile["id"]),
                purpose="contract_test",
            )
            row, claimed = self.db.claim_contract_run_launch(
                int(row["id"]),
                lease_owner=self.worker_id,
                monthly_limit_usd=self.settings.monthly_budget_usd,
                official_used_usd=official_usage.used_usd,
                outstanding_reserve_usd=outstanding_reserve,
                posts_result_price_usd=PRICES["posts"],
            )
            if not claimed:
                if str(row.get("status") or "") == "succeeded":
                    return self._capture_v2_contract_result(row)
                if str(row.get("claim_denied_reason") or "") == "monthly_budget_capacity":
                    raise BudgetExceeded(
                        "Apify 官方用量、進行中付費工作與特別帳號保留額合計已達月上限；"
                        "契約測試未啟動 Actor",
                        self._usage_cycle_resume(official_usage),
                    )
                if str(row.get("claim_denied_reason") or "").startswith("contract_"):
                    raise BudgetExceeded("契約測試付費 grant 已失效或超額；禁止啟動 Actor")
                raise RuntimeError(
                    f"契約測試 {test_case} 已由另一個 worker 取得付費啟動租約"
                )
            if self.db.profile_source_frozen(int(profile["id"]), "apify"):
                self.db.execute(
                    """UPDATE contract_runs SET status='pending',error=NULL,
                    lease_owner=NULL,leased_at=NULL
                    WHERE id=? AND status='launching' AND run_id IS NULL AND lease_owner=?""",
                    (row["id"], self.worker_id),
                )
                raise ApifyFrozen("此帳號已凍結 Apify；契約測試未啟動付費 Actor")
            if not self._capture_v2_contract_launch_authorized(grant_allocation_id):
                self.db.execute(
                    """UPDATE contract_runs SET status='pending',error=NULL,
                    lease_owner=NULL,leased_at=NULL
                    WHERE id=? AND status='launching' AND run_id IS NULL AND lease_owner=?""",
                    (row["id"], self.worker_id),
                )
                raise BudgetExceeded("契約測試付費授權已關閉或逾期；禁止啟動 Actor")
            try:
                started = await self.apify.start(
                    str(contract["actor_id"]), payload, max_charge_usd
                )
            except Exception as exc:
                self.db.execute(
                    """UPDATE contract_runs SET status='needs_reconcile',error=?,finished_at=?
                    WHERE id=? AND status='launching' AND lease_owner=?""",
                    (str(exc)[:4000], utcnow(), row["id"], self.worker_id),
                )
                raise RuntimeError(
                    f"契約測試 {test_case} launch 不明，已停止自動重買"
                ) from exc
            self.db.execute(
                """UPDATE contract_runs SET status='run_started',run_id=?,dataset_id=?,leased_at=?
                WHERE id=? AND status='launching' AND lease_owner=?""",
                (started.run_id, started.dataset_id, utcnow(), row["id"], self.worker_id),
            )
        try:
            result = await self.apify.finish(started)
        except Exception as exc:
            self.db.execute(
                "UPDATE contract_runs SET status='needs_reconcile',error=?,finished_at=? WHERE id=?",
                (str(exc)[:4000], utcnow(), row["id"]),
            )
            raise
        result_json = json.dumps(
            {"items": result.items, "summary": result.summary}, ensure_ascii=False
        )
        try:
            self._capture_v2_validate_actor_result(
                profile,
                result.items,
                result.summary,
                maximum=maximum,
            )
        except RuntimeError as exc:
            self.db.execute(
                """UPDATE contract_runs SET status='failed',result_json=?,result_count=?,charged_usd=?,
                error=?,finished_at=? WHERE id=?""",
                (
                    result_json,
                    len(result.items),
                    result.charged_usd,
                    str(exc)[:4000],
                    utcnow(),
                    row["id"],
                ),
            )
            raise
        self.db.execute(
            """UPDATE contract_runs SET status='succeeded',result_json=?,result_count=?,charged_usd=?,
            error=NULL,finished_at=? WHERE id=?""",
            (
                result_json,
                len(result.items),
                result.charged_usd,
                utcnow(),
                row["id"],
            ),
        )
        return result

    async def contract_test_posts_v2(self, profile_id: int, payload: dict[str, Any]) -> None:
        """Validate cursor/replay/boundary semantics before any production run."""
        profile = self.db.row("SELECT * FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            raise ValueError("Capture V2 契約測試帳號不存在")
        fixture_ack = str(payload.get("fixture_ack") or "").strip().casefold()
        fixture_minimum = int(payload.get("fixture_expected_min_public_posts") or 0)
        if fixture_ack not in {"1", "true", "yes", "on"} or fixture_minimum < 25:
            raise ValueError("契約測試前須確認此帳號至少有 25 篇可見非置頂貼文")
        if self.db.profile_source_frozen(profile_id, "apify"):
            raise ApifyFrozen("此帳號已凍結 Apify；契約測試未啟動付費 Actor")
        actor_id = str(payload.get("actor_id") or self.settings.actors.posts_v2_primary)
        candidates = {
            self.settings.actors.posts_v2_primary,
            self.settings.actors.posts_v2_fallback,
        }
        if actor_id not in candidates:
            raise ValueError("Actor 不在 Capture V2 primary/fallback 候選名單")
        test_generation = str(payload.get("contract_test_id") or "")
        if not test_generation:
            raise BudgetExceeded("Capture V2 契約測試缺少耐久測試世代")
        schema_fingerprint = self._posts_v2_fingerprint(actor_id)
        allocation = self._capture_v2_contract_allocation(
            profile_id=profile_id,
            actor_id=actor_id,
            schema_fingerprint=schema_fingerprint,
            test_generation=test_generation,
            payload=payload,
        )
        approved = min(
            self.settings.actor_contract_test_budget_usd,
            max(0.0, float(payload.get("max_budget_usd") or self.settings.actor_contract_test_budget_usd)),
            max(0.0, float(allocation["authorized_usd"])),
        )
        if approved <= 0:
            raise BudgetExceeded("Capture V2 契約測試額度未核准")
        mapping_hash = hashlib.sha256(
            canonical_input_json(self._posts_v2_contract_mapping(actor_id)).encode("utf-8")
        ).hexdigest()
        contract = self.db.upsert_actor_contract(
            provider="apify",
            actor_id=actor_id,
            purpose="posts_backfill",
            build_id=str(payload.get("build_id") or ""),
            schema_fingerprint=schema_fingerprint,
            input_mapping_hash=mapping_hash,
            status="pending",
            evidence={
                "approved_budget_usd": approved,
                "test_generation": test_generation,
            },
        )
        ceilings = [approved * 0.30, approved * 0.30, approved * 0.30, approved * 0.10]
        results: list[ActorResult] = []
        try:
            first_input = self._capture_v2_posts_payload(
                profile, actor_id=actor_id, maximum=10, cursor=None, known_post_ids=[]
            )
            first = await self._run_capture_v2_contract_case(
                contract=contract,
                profile=profile,
                test_generation=test_generation,
                test_case="page_1",
                payload=first_input,
                max_charge_usd=ceilings[0],
                grant_allocation_id=int(allocation["id"]),
            )
            results.append(first)
            first_state = self._capture_v2_summary_state(first.summary, result_count=len(first.items), maximum=10)
            if not first_state["profile_present"] or not first_state["cursor"] or len(first.items) != 10:
                raise RuntimeError("第一頁契約失敗：需 10 筆且 SUMMARY 含下一頁游標")
            first_ids = [value for item in first.items if (value := self._capture_v2_item_identity(item))]
            first_times = [
                value
                for item in first.items
                if (value := self._capture_v2_item_published_at(item)) is not None
            ]
            if len(set(first_ids)) != 10:
                raise RuntimeError("第一頁契約失敗：十筆結果必須都有唯一標準貼文身分")
            if any(self._capture_v2_item_is_pinned(item) for item in first.items):
                raise RuntimeError("第一頁契約失敗：Actor 未排除置頂貼文")
            if len(first_times) != 10:
                raise RuntimeError("第一頁契約失敗：十筆結果必須都有可解析的發文時間")

            second_input = self._capture_v2_posts_payload(
                profile, actor_id=actor_id, maximum=10, cursor=str(first_state["cursor"]), known_post_ids=[]
            )
            second = await self._run_capture_v2_contract_case(
                contract=contract,
                profile=profile,
                test_generation=test_generation,
                test_case="page_2",
                payload=second_input,
                max_charge_usd=ceilings[1],
                grant_allocation_id=int(allocation["id"]),
            )
            results.append(second)
            second_state = self._capture_v2_summary_state(second.summary, result_count=len(second.items), maximum=10)
            second_ids = [value for item in second.items if (value := self._capture_v2_item_identity(item))]
            second_times = [
                value
                for item in second.items
                if (value := self._capture_v2_item_published_at(item)) is not None
            ]
            if not second_state["profile_present"] or not second_state["cursor"] or len(second.items) != 10:
                raise RuntimeError("第二頁契約失敗：需 10 筆且游標繼續前進")
            if len(set(second_ids)) != 10 or set(first_ids) & set(second_ids):
                raise RuntimeError("契約失敗：第一、二頁貼文身分重疊")
            if any(self._capture_v2_item_is_pinned(item) for item in second.items):
                raise RuntimeError("第二頁契約失敗：Actor 未排除置頂貼文")
            if len(second_times) != 10:
                raise RuntimeError("第二頁契約失敗：十筆結果必須都有可解析的發文時間")
            if min(second_times) >= min(first_times):
                raise RuntimeError("契約失敗：第二頁最舊貼文時間未向歷史方向前進")

            replay = await self._run_capture_v2_contract_case(
                contract=contract,
                profile=profile,
                test_generation=test_generation,
                test_case="page_2_replay",
                payload=second_input,
                max_charge_usd=ceilings[2],
                grant_allocation_id=int(allocation["id"]),
            )
            results.append(replay)
            replay_state = self._capture_v2_summary_state(replay.summary, result_count=len(replay.items), maximum=10)
            replay_ids = [value for item in replay.items if (value := self._capture_v2_item_identity(item))]
            if sorted(replay_ids) != sorted(second_ids) or replay_state["cursor"] != second_state["cursor"]:
                raise RuntimeError("契約失敗：同一第二頁游標無法重現")

            known = list(dict.fromkeys(first_ids + second_ids))[:20]
            boundary_input = self._capture_v2_posts_payload(
                profile, actor_id=actor_id, maximum=2, cursor=None, known_post_ids=known
            )
            boundary = await self._run_capture_v2_contract_case(
                contract=contract,
                profile=profile,
                test_generation=test_generation,
                test_case="known_boundary",
                payload=boundary_input,
                max_charge_usd=ceilings[3],
                grant_allocation_id=int(allocation["id"]),
            )
            results.append(boundary)
            boundary_ids = [value for item in boundary.items if (value := self._capture_v2_item_identity(item))]
            if set(boundary_ids) & set(known):
                raise RuntimeError("契約失敗：knownPostIds 停止邊界仍付費回傳已知貼文")
            if not self._capture_v2_known_boundary_reached(boundary.summary):
                raise RuntimeError(
                    "契約失敗：Actor 未以 SUMMARY 明確證明已停在 knownPostIds 邊界"
                )
            if any(call.charged_usd < 0 for call in results) or sum(call.charged_usd for call in results) > approved + 1e-9:
                raise RuntimeError("契約測試實際費用超過核准上限")
        except Exception as exc:
            self.db.upsert_actor_contract(
                provider="apify",
                actor_id=actor_id,
                purpose="posts_backfill",
                build_id=str(payload.get("build_id") or ""),
                schema_fingerprint=schema_fingerprint,
                input_mapping_hash=mapping_hash,
                status="failed",
                evidence={
                    "error": str(exc),
                    "approved_budget_usd": approved,
                    "test_generation": test_generation,
                },
            )
            raise

        passed = self.db.upsert_actor_contract(
            provider="apify",
            actor_id=actor_id,
            purpose="posts_backfill",
            build_id=str(payload.get("build_id") or ""),
            schema_fingerprint=schema_fingerprint,
            input_mapping_hash=mapping_hash,
            status="passed",
            expires_at=(datetime.now(UTC) + timedelta(days=30)).isoformat(),
            evidence={
                "cases": ["page_1", "page_2", "page_2_replay", "known_boundary"],
                "charged_usd": sum(call.charged_usd for call in results),
                "test_generation": test_generation,
            },
        )
        if str(passed.get("schema_fingerprint") or "") != schema_fingerprint:
            raise RuntimeError("Capture V2 契約 fingerprint 寫入失敗")
        refreshed = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,)) or profile
        special = self._special_profile()
        if (
            special
            and int(special["id"]) == profile_id
            and self._has_confirmed_public_observation(profile_id)
        ):
            self._ensure_capture_v2_epoch(refreshed, "contract_passed")

    def _capture_v2_raw_path(self, request_hash: str) -> Path:
        return (
            self.settings.data_dir
            / "capture-v2"
            / "raw"
            / request_hash[:2]
            / f"{request_hash}.json.gz"
        )

    def _save_capture_v2_raw(
        self,
        batch: dict[str, Any],
        result: ActorResult,
    ) -> tuple[Path, str]:
        """Durably save the provider response before any database import."""
        path = self._capture_v2_raw_path(str(batch["request_hash"]))
        raw_root = self.settings.data_dir / "capture-v2" / "raw"
        path.parent.mkdir(parents=True, exist_ok=True)
        for directory in (raw_root, path.parent):
            try:
                directory.chmod(0o700)
            except OSError:
                pass
        if path.exists():
            existing_document = self._load_capture_v2_raw(path)
            if (
                str(existing_document.get("request_hash") or "")
                != str(batch["request_hash"])
                or str(existing_document.get("actor_id") or "")
                != str(batch["actor_id"])
            ):
                raise RuntimeError("同一 request_hash 已存在來源不符的 Apify raw")
            existing = path.read_bytes()
            try:
                path.chmod(0o600)
            except OSError:
                pass
            return path, hashlib.sha256(existing).hexdigest()
        document = {
            "format": "capture-v2-apify-raw-v1",
            "request_hash": str(batch["request_hash"]),
            "actor_id": str(batch["actor_id"]),
            "run_id": result.run_id,
            "charged_usd": result.charged_usd,
            "raw_result_count": result.raw_result_count,
            "items": result.items,
            "summary": result.summary,
            "saved_at": str(batch.get("created_at") or batch.get("updated_at") or ""),
        }
        encoded = json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        compressed = gzip.compress(encoded, mtime=0)
        digest = hashlib.sha256(compressed).hexdigest()
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("wb") as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            temporary.chmod(0o600)
        except OSError:
            pass
        temporary.replace(path)
        # Persist the rename itself.  Without syncing the directory, an OCI
        # reboot can lose the name even though the file contents were synced.
        directory_fd: int | None = None
        try:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            directory_fd = os.open(path.parent, flags)
            os.fsync(directory_fd)
        except OSError:
            # Windows test hosts cannot open a directory descriptor.  Linux is
            # the production target and executes the durable branch above.
            pass
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
        return path, digest

    @staticmethod
    def _load_capture_v2_raw(path_value: object) -> dict[str, Any]:
        path = Path(str(path_value or ""))
        if not path.is_file():
            raise RuntimeError(f"Capture V2 raw 不存在：{path}")
        try:
            document = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Capture V2 raw 無法讀取：{path}") from exc
        if not isinstance(document, dict) or not isinstance(document.get("items"), list):
            raise RuntimeError("Capture V2 raw 格式錯誤")
        document["items"] = [item for item in document["items"] if isinstance(item, dict)]
        if not isinstance(document.get("summary"), dict):
            document["summary"] = None
        return document

    async def _ingest_capture_v2_posts(
        self,
        profile_id: int,
        items: list[dict[str, Any]],
        *,
        notify: bool,
    ) -> dict[str, Any]:
        identities: list[str] = []
        new_count = 0
        updated_count = 0
        duplicate_count = 0
        for raw_item in items:
            item = dict(raw_item)
            identity = self._capture_v2_item_identity(item)
            existing_entity = self._capture_v2_resolve_post_entity(profile_id, item)
            if identity:
                # Make all Actor URL/ID aliases converge on one entity key.
                item["source_post_id"] = (
                    str(existing_entity["external_id"])
                    if existing_entity
                    else identity
                )
                identities.append(identity)
            ext_id = external_id(item, "post")
            before = existing_entity or self.db.row(
                """SELECT * FROM entities
                WHERE profile_id=? AND kind='post' AND external_id=?""",
                (profile_id, ext_id),
            )
            entity_id, persisted_id, changed = await self.ingester.ingest(
                profile_id, "post", item, notify=notify
            )
            if before is None:
                new_count += 1
            elif changed:
                updated_count += 1
            else:
                duplicate_count += 1
            self._capture_v2_record_post_aliases(
                profile_id,
                item,
                identity=identity,
                persisted_id=persisted_id,
                entity_id=entity_id,
                provider="apify",
            )
        return {
            "identities": list(dict.fromkeys(identities)),
            "seen": len(items),
            "new": new_count,
            "updated": updated_count,
            "duplicate": duplicate_count,
        }

    def _capture_v2_previous_committed_batch(
        self,
        coverage_stream_id: int,
        batch_id: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        previous = self.db.row(
            """SELECT * FROM paid_source_batches
            WHERE coverage_stream_id=? AND status='committed' AND id<>?
            ORDER BY id DESC LIMIT 1""",
            (coverage_stream_id, batch_id),
        )
        if not previous or not previous.get("raw_path"):
            return previous, None
        try:
            return previous, self._load_capture_v2_raw(previous["raw_path"])
        except RuntimeError:
            # The current page is still safe to commit.  Missing old evidence
            # merely prevents the identity half of the circuit breaker.
            return previous, None

    def _enqueue_capture_v2_successor(
        self,
        profile_id: int,
        epoch: dict[str, Any],
        coverage: dict[str, Any],
    ) -> int | None:
        # The current capture job is still running here, so the generic V2
        # uniqueness helper would incorrectly suppress the successor.  Only a
        # pending successor for this epoch is a duplicate.
        for row in self.db.rows(
            "SELECT payload_json FROM jobs WHERE profile_id=? AND job_type='capture_posts_v2' AND status='pending'",
            (profile_id,),
        ):
            try:
                candidate = json.loads(row.get("payload_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                candidate = {}
            if int(candidate.get("epoch_id") or 0) == int(epoch["id"]):
                return None
        scope = self._capture_v2_epoch_scope(epoch)
        successor_payload: dict[str, Any] = {
            "epoch_id": int(epoch["id"]),
            "coverage_stream_id": int(coverage["id"]),
            "surface": CoverageSurface.TIMELINE_POSTS.value,
        }
        if scope.get("capture_intent"):
            successor_payload["intent"] = str(scope["capture_intent"])
        return self._enqueue(
            profile_id,
            "capture_posts_v2",
            int(epoch.get("priority") or -50),
            datetime.now(UTC),
            successor_payload,
        )

    def _reconcile_capture_v2_batch_media(
        self,
        *,
        profile: dict[str, Any],
        epoch: dict[str, Any],
        batch: dict[str, Any],
        raw: dict[str, Any],
    ) -> None:
        """Create one durable media checkpoint for every imported post.

        ``Ingester`` has already persisted/downloaded the Actor attachments at
        this point.  This pass records whether the provider actually proved an
        album terminal; a finite-looking list without terminal metadata is
        deliberately recorded as ``source_limited``.
        """

        profile_id = int(profile["id"])
        for raw_item in raw.get("items", []):
            if not isinstance(raw_item, dict):
                continue
            item = dict(raw_item)
            identity = self._capture_v2_item_identity(item)
            entity = self._capture_v2_resolve_post_entity(profile_id, item)
            if identity and not entity:
                item["source_post_id"] = identity
                entity = self.db.row(
                    """SELECT * FROM entities
                    WHERE profile_id=? AND kind='post' AND external_id=?""",
                    (profile_id, external_id(item, "post")),
                )
            if not entity:
                persisted_id = external_id(item, "post")
                raise RuntimeError(
                    f"Capture V2 post {persisted_id} 已匯入但找不到 entity；media checkpoint 未提交"
                )
            item["source_post_id"] = str(entity["external_id"])
            reconcile_post_media_checkpoint(
                self.db,
                epoch_id=int(epoch["id"]),
                profile_id=profile_id,
                post_entity_id=int(entity["id"]),
                item=item,
                provider=str(batch.get("provider") or "apify"),
                contract_id=(
                    int(batch["contract_id"]) if batch.get("contract_id") is not None else None
                ),
                batch_id=int(batch["id"]),
            )

    def _refresh_capture_v2_epoch(self, epoch_id: int) -> None:
        epoch = self.db.row("SELECT * FROM capture_epochs WHERE id=?", (epoch_id,))
        if not epoch or not bool(epoch.get("is_active")):
            return
        resolution = resolve_epoch(
            self.db.rows(
                "SELECT stream,surface,scope_type,scope_id,status FROM coverage_streams WHERE epoch_id=?",
                (epoch_id,),
            )
        )
        if resolution.ready:
            self.db.finish_capture_epoch(
                epoch_id,
                status="complete",
                terminal_reason=resolution.reason,
            )
            return
        if resolution.status == "failed":
            self.db.finish_capture_epoch(
                epoch_id,
                status="failed",
                terminal_reason=resolution.reason,
            )
            return
        if resolution.status == CoverageStatus.SOURCE_LIMITED.value:
            # ``resolve_epoch`` returns source_limited only after every
            # required stream has concluded (there are no pending/paused
            # streams left).  Close this observation honestly as limited so
            # it cannot monopolize the per-profile active-epoch slot forever.
            now = utcnow()
            self.db.execute(
                """UPDATE capture_epochs SET status='source_limited',is_active=0,
                terminal_reason=?,completed_at=?,updated_at=? WHERE id=? AND is_active=1""",
                (resolution.reason, now, now, epoch_id),
            )
            return
        self.db.execute(
            "UPDATE capture_epochs SET status=?,updated_at=? WHERE id=?",
            (resolution.status, utcnow(), epoch_id),
        )

    def _seed_capture_v2_comments_after_posts(
        self,
        *,
        profile: dict[str, Any],
        epoch: dict[str, Any],
    ) -> None:
        """Create one comments checkpoint/job per post, after posts terminal.

        The comments Actor is intentionally not authorized by the posts
        contract.  Jobs are still made durable now; the handler records an
        explicit source limitation until an exact ``comments_backfill``
        contract is available, rather than spending through the V1 path.
        """

        contract = self.db.valid_actor_contract(
            provider="apify",
            actor_id=self.settings.actors.comments,
            purpose="comments_backfill",
        )
        checkpoints = seed_comment_checkpoints(
            self.db,
            epoch_id=int(epoch["id"]),
            profile_id=int(profile["id"]),
            provider="apify",
            contract_id=int(contract["id"]) if contract else None,
        )
        for checkpoint in checkpoints:
            dedupe_key = f"capture-v2:comments:{epoch['id']}:{checkpoint.post_entity_id}"
            # The active-only unique index intentionally permits later retries,
            # but terminal-post reconciliation can itself be replayed.  Do not
            # turn a committed-batch replay into a second historical comments
            # purchase for a checkpoint that already has a durable job record.
            if self.db.row(
                "SELECT id FROM jobs WHERE dedupe_key=? ORDER BY id LIMIT 1",
                (dedupe_key,),
            ):
                continue
            self.db.queue_unique_job(
                profile_id=int(profile["id"]),
                job_type="capture_comments_v2",
                priority=int(epoch.get("priority") or -50) + 1,
                dedupe_key=dedupe_key,
                payload={
                    "epoch_id": int(epoch["id"]),
                    "coverage_stream_id": checkpoint.coverage_stream_id,
                    "post_entity_id": checkpoint.post_entity_id,
                    "post_external_id": checkpoint.post_external_id,
                    "post_url": checkpoint.post_url,
                },
                epoch_id=int(epoch["id"]),
            )

    async def capture_comments_v2(self, profile_id: int, payload: dict[str, Any]) -> None:
        """Fail closed until a separately tested comments cursor contract exists."""

        epoch_id = int(payload.get("epoch_id") or 0)
        coverage_id = int(payload.get("coverage_stream_id") or 0)
        post_entity_id = int(payload.get("post_entity_id") or 0)
        coverage = self.db.row(
            """SELECT c.* FROM coverage_streams c
            JOIN capture_epochs ce ON ce.id=c.epoch_id AND ce.profile_id=?
            JOIN entities e ON e.id=CAST(c.scope_id AS INTEGER)
              AND e.profile_id=? AND e.kind='post'
            WHERE c.id=? AND c.epoch_id=?
              AND c.stream='comments' AND c.surface='post_comments'
              AND c.scope_type='post' AND c.scope_id=?""",
            (profile_id, profile_id, coverage_id, epoch_id, str(post_entity_id)),
        )
        if not coverage:
            raise ValueError("Capture V2 comments checkpoint 不存在或不屬於此帳號／貼文")
        if str(coverage.get("status") or "") in {
            CoverageStatus.COMPLETE.value,
            CoverageStatus.SOURCE_LIMITED.value,
        }:
            self._refresh_capture_v2_epoch(epoch_id)
            return
        posts = self.db.row(
            """SELECT status FROM coverage_streams WHERE epoch_id=?
            AND stream='posts' AND surface='timeline_posts'
            AND scope_type='profile' AND scope_id=''""",
            (epoch_id,),
        )
        if not posts or str(posts.get("status") or "") != CoverageStatus.COMPLETE.value:
            raise RuntimeError("貼文清冊尚未 terminal；禁止提前抓取留言")

        contract = self.db.valid_actor_contract(
            provider="apify",
            actor_id=self.settings.actors.comments,
            purpose="comments_backfill",
        )
        reason = (
            "留言 Actor 尚未通過獨立 cursor／nested replies／terminal 契約；未啟動付費 Actor"
            if not contract
            else "留言 Actor 契約已登錄，但 V2 durable cursor executor 尚未啟用；未啟動付費 Actor"
        )
        self.db.update_coverage_stream(
            coverage_id,
            provider="apify",
            contract_id=int(contract["id"]) if contract else None,
            status=CoverageStatus.SOURCE_LIMITED.value,
            limited_reason=reason,
            terminal_evidence_json={},
        )
        self._refresh_capture_v2_epoch(epoch_id)

    def _commit_capture_v2_batch(
        self,
        *,
        profile: dict[str, Any],
        epoch: dict[str, Any],
        coverage: dict[str, Any],
        batch: dict[str, Any],
        raw: dict[str, Any],
        maximum: int,
    ) -> None:
        self._capture_v2_validate_actor_result(
            profile,
            raw.get("items", []),
            raw.get("summary"),
            maximum=maximum,
        )
        self._reconcile_capture_v2_batch_media(
            profile=profile,
            epoch=epoch,
            batch=batch,
            raw=raw,
        )
        if str(batch["status"]) == "imported":
            batch = self.db.transition_paid_source_batch(
                int(batch["id"]), "committed", expected_status="imported"
            )
        elif str(batch["status"]) != "committed":
            raise RuntimeError(f"Capture V2 batch 尚不可 commit：{batch['status']}")

        identities = [
            value
            for item in raw.get("items", [])
            if (value := self._capture_v2_item_identity(item))
        ]
        summary_state = self._capture_v2_summary_state(
            raw.get("summary"),
            result_count=len(raw.get("items", [])),
            maximum=maximum,
            target_profile=profile,
        )
        try:
            batch_intent = CaptureIntent(str(batch.get("intent") or ""))
        except ValueError:
            batch_intent = CaptureIntent.INITIAL_PUBLIC_CAPTURE
        bounded_terminal: dict[str, Any] | None = None
        if (
            batch_intent is CaptureIntent.INCREMENTAL_POLL
            and self._capture_v2_known_boundary_reached(raw.get("summary"))
        ):
            bounded_terminal = {
                "source": "SUMMARY",
                "kind": "known_post_boundary_reached",
                "capture_intent": batch_intent.value,
                "coverage_status": summary_state["coverage"],
                "result_count": len(raw.get("items", [])),
                "known_post_limit": 20,
            }
        elif (
            batch_intent is CaptureIntent.MONTHLY_AUDIT
            and str(summary_state["coverage"]).casefold().replace("-", "_").replace(" ", "_")
            == "complete_target_reached"
        ):
            bounded_terminal = {
                "source": "SUMMARY",
                "kind": "monthly_target_reached",
                "capture_intent": batch_intent.value,
                "coverage_status": summary_state["coverage"],
                "result_count": len(raw.get("items", [])),
                "target_count": maximum,
                "observation_window": batch.get("observation_window"),
            }
        if bounded_terminal:
            # These are terminals for the bounded observation, not evidence
            # that the profile's full public history is exhausted.
            summary_state = {
                **summary_state,
                "cursor": None,
                "terminal": True,
                "capped": False,
                "terminal_evidence": bounded_terminal,
            }
        previous, previous_raw = self._capture_v2_previous_committed_batch(
            int(coverage["id"]), int(batch["id"])
        )
        previous_identities = (
            [
                value
                for item in previous_raw.get("items", [])
                if (value := self._capture_v2_item_identity(item))
            ]
            if previous_raw
            else []
        )
        breaker = duplicate_page_circuit_breaker(
            previous_cursor=str((previous or {}).get("output_cursor") or "") or None,
            previous_identities=previous_identities,
            current_cursor=summary_state["cursor"],
            current_identities=identities,
        )
        totals = self.db.row(
            """SELECT COALESCE(SUM(raw_result_count),0) seen,
            COALESCE(SUM(new_result_count),0) new,
            COALESCE(SUM(updated_result_count),0) updated,
            COALESCE(SUM(duplicate_result_count),0) duplicate
            FROM paid_source_batches WHERE coverage_stream_id=? AND status='committed'""",
            (coverage["id"],),
        ) or {"seen": 0, "new": 0, "updated": 0, "duplicate": 0}
        common: dict[str, Any] = {
            "provider": "apify",
            "contract_id": batch.get("contract_id"),
            "input_cursor": batch.get("input_cursor"),
            "output_cursor": summary_state["cursor"],
            "provider_checkpoint_json": {
                "request_hash": batch["request_hash"],
                "run_id": batch.get("run_id"),
                "identity_set_hash": batch.get("identity_set_hash"),
            },
            "seen_count": int(totals["seen"]),
            "new_count": int(totals["new"]),
            "updated_count": int(totals["updated"]),
            "duplicate_count": int(totals["duplicate"]),
        }
        if breaker.tripped:
            reason = "重複頁面斷路：" + ",".join(reason.value for reason in breaker.reasons)
            self.db.update_coverage_stream(
                int(coverage["id"]),
                **common,
                status=CoverageStatus.SOURCE_LIMITED.value,
                limited_reason=reason,
                terminal_evidence_json={},
            )
            self.db.execute(
                "UPDATE capture_epochs SET status='source_limited',updated_at=? WHERE id=?",
                (utcnow(), epoch["id"]),
            )
            self._refresh_capture_v2_epoch(int(epoch["id"]))
            return
        if summary_state["terminal"]:
            self.db.update_coverage_stream(
                int(coverage["id"]),
                **common,
                status=CoverageStatus.COMPLETE.value,
                limited_reason=None,
                terminal_evidence_json=summary_state["terminal_evidence"],
            )
            self._seed_capture_v2_comments_after_posts(profile=profile, epoch=epoch)
            self._refresh_capture_v2_epoch(int(epoch["id"]))
            return
        if summary_state["cursor"]:
            self.db.update_coverage_stream(
                int(coverage["id"]),
                **common,
                status=CoverageStatus.IN_PROGRESS.value,
                limited_reason=None,
                terminal_evidence_json={},
            )
            refreshed = self.db.row(
                "SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],)
            ) or coverage
            self._enqueue_capture_v2_successor(int(profile["id"]), epoch, refreshed)
            self._refresh_capture_v2_epoch(int(epoch["id"]))
            return

        reason = (
            "SUMMARY 顯示結果受上限截斷但未提供游標"
            if summary_state["capped"]
            else "SUMMARY 未提供明確終點或續頁游標"
        )
        self.db.update_coverage_stream(
            int(coverage["id"]),
            **common,
            status=CoverageStatus.SOURCE_LIMITED.value,
            limited_reason=reason,
            terminal_evidence_json={},
        )
        self.db.execute(
            "UPDATE capture_epochs SET status='source_limited',updated_at=? WHERE id=?",
            (utcnow(), epoch["id"]),
        )
        self._refresh_capture_v2_epoch(int(epoch["id"]))

    def _capture_v2_special_history_complete(self) -> bool:
        special = self._special_profile()
        if not special:
            return False
        for row in self.db.rows(
            """SELECT scope_json FROM capture_epochs
            WHERE profile_id=? AND is_active=0 AND status='complete'
            ORDER BY id DESC""",
            (special["id"],),
        ):
            try:
                scope = json.loads(str(row.get("scope_json") or "{}"))
            except (TypeError, json.JSONDecodeError):
                continue
            if scope.get("all_public_history") is True and str(
                scope.get("capture_intent") or ""
            ) in {
                CaptureIntent.INITIAL_PUBLIC_CAPTURE.value,
                CaptureIntent.RECOVERY_CAPTURE.value,
                CaptureIntent.MANUAL_CONTINUE.value,
            }:
                return True
        return False

    def _capture_v2_outstanding_reserve(
        self,
        *,
        spending_profile_id: int,
        purpose: str,
    ) -> float:
        """Return cycle reserves that this paid purpose may not consume."""

        special = self._special_profile()
        special_id = int(special["id"]) if special else None
        reserve = 0.0
        spending_special_capture = (
            purpose == "source_capture"
            and special_id is not None
            and int(spending_profile_id) == special_id
        )
        if not spending_special_capture and not self._capture_v2_special_history_complete():
            reserve += max(0.0, float(self.settings.special_capture_reserve_usd))

        detection_active = not (
            special
            and str(special.get("public_state") or "") == "public"
            and self._has_confirmed_public_observation(int(special["id"]))
        )
        if purpose != "access_probe" and detection_active:
            reserve += max(0.0, float(self.settings.special_detection_budget_usd))
        return reserve

    def _capture_v2_unsettled_max_charge(self, excluding_batch_id: int) -> float:
        reservations = self.db.paid_budget_reservations(
            posts_result_price_usd=PRICES["posts"],
            excluding_source_batch_id=excluding_batch_id,
        )
        return float(reservations["total_unsettled_usd"])

    async def capture_posts_v2(self, profile_id: int, payload: dict[str, Any]) -> None:
        """Run or resume one crash-safe, cursor-bearing paid posts page."""
        profile = self.db.row("SELECT * FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            raise ValueError("Capture V2 帳號不存在")
        epoch_id = int(payload.get("epoch_id") or 0)
        coverage_stream_id = int(payload.get("coverage_stream_id") or 0)
        epoch = self.db.row(
            "SELECT * FROM capture_epochs WHERE id=? AND profile_id=? AND is_active=1",
            (epoch_id, profile_id),
        )
        coverage = self.db.row(
            """SELECT * FROM coverage_streams WHERE id=? AND epoch_id=?
            AND stream='posts' AND surface='timeline_posts'""",
            (coverage_stream_id, epoch_id),
        )
        if not epoch or not coverage:
            raise ValueError("Capture V2 epoch/coverage 不存在或不屬於此帳號")
        requested_surface = str(payload.get("surface") or CoverageSurface.TIMELINE_POSTS.value)
        if requested_surface != CoverageSurface.TIMELINE_POSTS.value:
            raise ValueError("Capture V2 posts 僅接受 timeline_posts surface")
        if str(coverage.get("status")) == CoverageStatus.COMPLETE.value:
            return
        if not self._has_confirmed_public_observation(profile_id):
            if str(coverage.get("status") or "") in {
                CoverageStatus.PENDING.value,
                CoverageStatus.IN_PROGRESS.value,
                CoverageStatus.BUDGET_PAUSED.value,
                CoverageStatus.MANUAL_PAUSED.value,
            }:
                self.db.update_coverage_stream(
                    int(coverage["id"]),
                    status=CoverageStatus.MANUAL_PAUSED.value,
                    limited_reason="缺少匿名且身分一致的 confirmed_public 存取證據",
                )
            self.db.execute(
                "UPDATE capture_epochs SET status='manual_paused',updated_at=? WHERE id=?",
                (utcnow(), epoch["id"]),
            )
            raise RuntimeError("Capture V2 尚未取得匿名 confirmed_public 證據；禁止付費")

        contract: dict[str, Any] | None = None
        actor_id = ""
        if coverage.get("contract_id") is not None:
            selected = self.db.row(
                """SELECT * FROM actor_contracts WHERE id=? AND provider='apify'
                AND purpose='posts_backfill' AND status='passed'""",
                (coverage["contract_id"],),
            )
            if selected:
                actor_id = str(selected.get("actor_id") or "")
                exact = self._valid_posts_v2_contract(actor_id)
                if exact and int(exact["id"]) == int(selected["id"]):
                    contract = exact
        else:
            contract = self._preferred_posts_v2_contract()
            if contract:
                actor_id = str(contract["actor_id"])
                self.db.update_coverage_stream(
                    int(coverage["id"]), contract_id=int(contract["id"]), provider="apify"
                )
                coverage = self.db.row(
                    "SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],)
                ) or coverage
        if not contract:
            self.db.update_coverage_stream(
                int(coverage["id"]),
                status=CoverageStatus.MANUAL_PAUSED.value,
                limited_reason="缺少 exact fingerprint passed contract",
            )
            self.db.execute(
                "UPDATE capture_epochs SET status='awaiting_contract',updated_at=? WHERE id=?",
                (utcnow(), epoch["id"]),
            )
            raise RuntimeError("Capture V2 缺少 exact fingerprint passed contract；禁止付費")

        if str(coverage.get("status")) != CoverageStatus.IN_PROGRESS.value:
            self.db.update_coverage_stream(
                int(coverage["id"]),
                status=CoverageStatus.IN_PROGRESS.value,
                limited_reason=None,
                terminal_evidence_json={},
            )
            coverage = self.db.row(
                "SELECT * FROM coverage_streams WHERE id=?", (coverage["id"],)
            ) or coverage

        maximum = 50
        try:
            checkpoint = json.loads(coverage.get("provider_checkpoint_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            checkpoint = {}
        latest_batch = self.db.row(
            """SELECT * FROM paid_source_batches WHERE coverage_stream_id=? AND status<>'failed'
            ORDER BY id DESC LIMIT 1""",
            (coverage_stream_id,),
        )
        recover_latest = bool(
            latest_batch
            and (
                str(latest_batch["status"]) != "committed"
                or str(checkpoint.get("request_hash") or "")
                != str(latest_batch["request_hash"])
            )
        )
        if recover_latest:
            batch = latest_batch
            if (
                int(batch.get("contract_id") or 0) != int(contract["id"])
                or str(batch.get("actor_id") or "") != actor_id
            ):
                raise RuntimeError("未完成 batch 的 Actor/contract 與目前 exact contract 不符")
            try:
                actor_payload = json.loads(batch.get("normalized_input_json") or "{}")
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("未完成 batch 的 canonical input 損毀") from exc
            if not isinstance(actor_payload, dict):
                raise RuntimeError("未完成 batch 的 canonical input 格式錯誤")
            cursor = str(batch.get("input_cursor") or "") or None
            maximum = max(
                1,
                min(
                    50,
                    int(
                        actor_payload.get("maxPostsPerProfile")
                        or actor_payload.get("maxPosts")
                        or 50
                    ),
                ),
            )
        else:
            cursor = str(coverage.get("output_cursor") or coverage.get("input_cursor") or "") or None
            try:
                epoch_scope = json.loads(epoch.get("scope_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                epoch_scope = {}
            if not isinstance(epoch_scope, dict):
                epoch_scope = {}
            default_intent = (
                CaptureIntent.RECOVERY_CAPTURE
                if "recover" in str(epoch.get("trigger_reason") or "").casefold()
                else CaptureIntent.INITIAL_PUBLIC_CAPTURE
            )
            requested_intent = str(payload.get("intent") or "").strip()
            scoped_intent = str(epoch_scope.get("capture_intent") or "").strip()
            if requested_intent and scoped_intent and requested_intent != scoped_intent:
                raise ValueError("Capture V2 job intent 與 epoch scope 不一致")
            selected_intent = scoped_intent or requested_intent
            if selected_intent:
                try:
                    intent = CaptureIntent(selected_intent)
                except ValueError as exc:
                    raise ValueError(f"不支援的 Capture V2 intent：{selected_intent}") from exc
            else:
                intent = default_intent
            # Full-history epochs must cross already-known recent posts to
            # reach older history.  Supplying knownPostIds here would make an
            # upgraded database stop at its first page forever.  Known IDs
            # are reserved for a separate incremental stream/intent only.
            if epoch_scope.get("all_public_history"):
                intent = (
                    CaptureIntent(str(epoch_scope["capture_intent"]))
                    if epoch_scope.get("capture_intent")
                    else default_intent
                )
            if intent is CaptureIntent.MONTHLY_AUDIT:
                maximum = 5
            elif intent is CaptureIntent.INCREMENTAL_POLL:
                maximum = max(1, min(20, int(epoch_scope.get("max_posts") or 20)))
            known_post_ids = (
                self._capture_v2_known_post_ids(profile_id, limit=20)
                if intent is CaptureIntent.INCREMENTAL_POLL
                else []
            )
            actor_payload = self._capture_v2_posts_payload(
                profile,
                actor_id=actor_id,
                maximum=maximum,
                cursor=cursor,
                known_post_ids=known_post_ids,
            )
            created_at = self._capture_v2_datetime(epoch["created_at"])
            if intent is CaptureIntent.MONTHLY_AUDIT:
                month_key = str(
                    epoch_scope.get("observation_month") or created_at.strftime("%Y-%m")
                )
                window = self._capture_v2_month_window(month_key)
            elif intent is CaptureIntent.INCREMENTAL_POLL:
                window = deterministic_observation_window(
                    created_at,
                    timedelta(hours=max(1.0, self.settings.visit_max_hours)),
                )
            else:
                window = deterministic_observation_window(
                    created_at, timedelta(days=36500), anchor=created_at
                )
            request = capture_request_hash(
                capture_intent=intent,
                window=window,
                profile_id=profile_id,
                epoch_id=epoch_id,
                stream=CoverageStream.POSTS,
                surface=CoverageSurface.TIMELINE_POSTS,
                contract_fingerprint=str(contract["schema_fingerprint"]),
                actor_input=actor_payload,
            )
            batch, _ = self.db.prepare_paid_source_batch(
                profile_id=profile_id,
                epoch_id=epoch_id,
                coverage_stream_id=coverage_stream_id,
                contract_id=int(contract["id"]),
                provider="apify",
                actor_id=actor_id,
                intent=intent.value,
                observation_window=window.key,
                normalized_input=actor_payload,
                input_cursor=cursor,
                request_hash=request,
            )
        status = str(batch["status"])
        if status == "committed":
            raw = self._load_capture_v2_raw(batch["raw_path"])
            self._capture_v2_validate_actor_result(
                profile,
                raw.get("items", []),
                raw.get("summary"),
                maximum=maximum,
            )
            self._commit_capture_v2_batch(
                profile=profile,
                epoch=epoch,
                coverage=coverage,
                batch=batch,
                raw=raw,
                maximum=maximum,
            )
            return
        if status == "needs_reconcile":
            self.db.execute(
                "UPDATE capture_epochs SET status='needs_reconcile',updated_at=? WHERE id=?",
                (utcnow(), epoch["id"]),
            )
            return
        if status == "launching":
            if self._lease_is_active(batch):
                # A concurrent worker owns the only right to call start().
                # It will persist run_started/raw evidence for the shared
                # request; this invocation must not quarantine its live work.
                return
            # start() may have reached Apify before the response was lost.  A
            # second launch could double charge, so this is a reconciliation
            # state even when no run id was persisted.
            batch = self.db.transition_paid_source_batch(
                int(batch["id"]),
                "needs_reconcile",
                expected_status="launching",
                error="Actor launch 結果不明；禁止自動重買",
            )
            self.db.execute(
                "UPDATE capture_epochs SET status='needs_reconcile',updated_at=? WHERE id=?",
                (utcnow(), epoch["id"]),
            )
            return

        raw: dict[str, Any] | None = None
        if status in {"raw_saved", "import_failed", "imported"}:
            raw = self._load_capture_v2_raw(batch["raw_path"])
        elif status in {"prepared", "run_started"}:
            if status == "prepared":
                if self.db.profile_source_frozen(profile_id, "apify"):
                    self.db.update_coverage_stream(
                        coverage_stream_id,
                        status=CoverageStatus.MANUAL_PAUSED.value,
                        limited_reason="此帳號已凍結 Apify",
                    )
                    self.db.execute(
                        "UPDATE capture_epochs SET status='manual_paused',updated_at=? WHERE id=?",
                        (utcnow(), epoch_id),
                    )
                    raise ApifyFrozen("此帳號已凍結 Apify；Capture V2 未啟動付費 Actor")
                _, usage = await self._official_available()
                requested = maximum * PRICES["posts"]
                outstanding_reserve = self._capture_v2_outstanding_reserve(
                    spending_profile_id=profile_id,
                    purpose="source_capture",
                )
                budget_capacity = max(
                    0.0,
                    self.settings.monthly_budget_usd
                    - float(usage.used_usd)
                    - outstanding_reserve,
                )
                budget = budget_decision(
                    requested,
                    monthly_limit=self.settings.monthly_budget_usd,
                    official_used=usage.used_usd,
                    outstanding_reserve=outstanding_reserve,
                    unsettled_max_charge=self._capture_v2_unsettled_max_charge(int(batch["id"])),
                )
                if not budget.allowed:
                    self.db.update_coverage_stream(
                        coverage_stream_id,
                        status=CoverageStatus.BUDGET_PAUSED.value,
                        limited_reason=(
                            f"可用 ${float(budget.available_usd):.4f}，"
                            f"本批需要 ${float(budget.requested_usd):.4f}"
                        ),
                    )
                    self.db.execute(
                        "UPDATE capture_epochs SET status='budget_paused',updated_at=? WHERE id=?",
                        (utcnow(), epoch_id),
                    )
                    raise BudgetExceeded(
                        "Capture V2 官方額度或保留額不足；未啟動付費 Actor",
                        self._usage_cycle_resume(usage),
                    )
                # Budget lookup is an external await.  A freeze performed
                # during it must win over the prepared batch before launch.
                if self.db.profile_source_frozen(profile_id, "apify"):
                    self.db.update_coverage_stream(
                        coverage_stream_id,
                        status=CoverageStatus.MANUAL_PAUSED.value,
                        limited_reason="此帳號已凍結 Apify",
                    )
                    self.db.execute(
                        "UPDATE capture_epochs SET status='manual_paused',updated_at=? WHERE id=?",
                        (utcnow(), epoch_id),
                    )
                    raise ApifyFrozen("此帳號已凍結 Apify；Capture V2 未啟動付費 Actor")
                batch, claimed = self.db.claim_paid_source_batch_launch(
                    int(batch["id"]),
                    lease_owner=self.worker_id,
                    budget_capacity_usd=budget_capacity,
                    posts_result_price_usd=PRICES["posts"],
                )
                if not claimed:
                    if str(batch.get("status") or "") == "prepared":
                        self.db.update_coverage_stream(
                            coverage_stream_id,
                            status=CoverageStatus.BUDGET_PAUSED.value,
                            limited_reason="跨付費 ledger 原子預算不足",
                        )
                        self.db.execute(
                            "UPDATE capture_epochs SET status='budget_paused',updated_at=? WHERE id=?",
                            (utcnow(), epoch_id),
                        )
                        raise BudgetExceeded(
                            "Capture V2 跨付費 ledger 原子預算不足；未啟動 Actor",
                            self._usage_cycle_resume(usage),
                        )
                    # Another service/worker won the durable CAS.  Whether it
                    # is currently launching or has already persisted the run
                    # id, this invocation has no authority to call start().
                    return
                # Keep the final user-controlled freeze check adjacent to the
                # external launch.  If it wins after the durable transition,
                # return the never-launched batch to ``prepared`` so a later
                # explicit unfreeze can resume without a false ambiguous run.
                if self.db.profile_source_frozen(profile_id, "apify"):
                    self.db.execute(
                        """UPDATE paid_source_batches SET status='prepared',launched_at=NULL,
                        error=NULL,updated_at=?,lease_owner=NULL,leased_at=NULL
                        WHERE id=? AND status='launching' AND run_id IS NULL AND lease_owner=?""",
                        (utcnow(), batch["id"], self.worker_id),
                    )
                    self.db.update_coverage_stream(
                        coverage_stream_id,
                        status=CoverageStatus.MANUAL_PAUSED.value,
                        limited_reason="此帳號已凍結 Apify",
                    )
                    self.db.execute(
                        "UPDATE capture_epochs SET status='manual_paused',updated_at=? WHERE id=?",
                        (utcnow(), epoch_id),
                    )
                    raise ApifyFrozen("此帳號已凍結 Apify；Capture V2 未啟動付費 Actor")
                try:
                    started = await self.apify.start(actor_id, actor_payload, requested)
                except Exception as exc:
                    self.db.transition_paid_source_batch(
                        int(batch["id"]),
                        "needs_reconcile",
                        expected_status="launching",
                        error=str(exc)[:4000],
                    )
                    self.db.execute(
                        "UPDATE capture_epochs SET status='needs_reconcile',updated_at=? WHERE id=?",
                        (utcnow(), epoch_id),
                    )
                    raise RuntimeError("Actor launch 結果不明；已停止自動重買") from exc
                batch = self.db.transition_paid_source_batch(
                    int(batch["id"]),
                    "run_started",
                    expected_status="launching",
                    run_id=started.run_id,
                    dataset_id=started.dataset_id,
                    key_value_store_id=started.key_value_store_id,
                    lease_owner=self.worker_id,
                    leased_at=utcnow(),
                    error=None,
                )
            else:
                started = StartedActor(
                    str(batch.get("run_id") or ""),
                    str(batch.get("dataset_id") or ""),
                    str(batch.get("key_value_store_id") or ""),
                )
                if not started.run_id:
                    self.db.transition_paid_source_batch(
                        int(batch["id"]),
                        "needs_reconcile",
                        expected_status="run_started",
                        error="run_started 缺少 run_id",
                    )
                    self.db.execute(
                        "UPDATE capture_epochs SET status='needs_reconcile',updated_at=? WHERE id=?",
                        (utcnow(), epoch_id),
                    )
                    return
            try:
                result = await self.apify.finish(started)
                path, raw_sha256 = self._save_capture_v2_raw(batch, result)
            except Exception as exc:
                self.db.transition_paid_source_batch(
                    int(batch["id"]),
                    "needs_reconcile",
                    expected_status="run_started",
                    error=str(exc)[:4000],
                )
                self.db.execute(
                    "UPDATE capture_epochs SET status='needs_reconcile',updated_at=? WHERE id=?",
                    (utcnow(), epoch_id),
                )
                raise
            batch = self.db.transition_paid_source_batch(
                int(batch["id"]),
                "raw_saved",
                expected_status="run_started",
                raw_path=str(path),
                raw_sha256=raw_sha256,
                charged_usd=result.charged_usd,
                raw_result_count=(
                    result.raw_result_count
                    if result.raw_result_count is not None
                    else len(result.items)
                ),
                output_cursor=None,
                error=None,
            )
            raw = self._load_capture_v2_raw(path)
        else:
            raise RuntimeError(f"Capture V2 batch 狀態不支援自動處理：{status}")

        assert raw is not None
        summary_error = actor_summary_error(raw.get("summary"))
        if summary_error:
            if str(batch["status"]) in {"raw_saved", "import_failed"}:
                self.db.transition_paid_source_batch(
                    int(batch["id"]),
                    "import_failed",
                    expected_status=str(batch["status"]),
                    error=summary_error,
                )
            self.db.update_coverage_stream(
                coverage_stream_id,
                status=CoverageStatus.FAILED.value,
                limited_reason=summary_error,
            )
            raise RuntimeError(summary_error)

        try:
            summary_state = self._capture_v2_validate_actor_result(
                profile,
                raw.get("items", []),
                raw.get("summary"),
                maximum=maximum,
            )
        except RuntimeError as exc:
            if str(batch["status"]) in {"raw_saved", "import_failed", "imported"}:
                batch = self.db.transition_paid_source_batch(
                    int(batch["id"]),
                    "import_failed",
                    expected_status=str(batch["status"]),
                    output_cursor=None,
                    error=str(exc)[:4000],
                )
            self.db.update_coverage_stream(
                coverage_stream_id,
                status=CoverageStatus.FAILED.value,
                limited_reason=str(exc)[:4000],
            )
            raise
        self.db.execute(
            "UPDATE paid_source_batches SET output_cursor=?,updated_at=? WHERE id=?",
            (summary_state["cursor"], utcnow(), batch["id"]),
        )
        batch = self.db.row("SELECT * FROM paid_source_batches WHERE id=?", (batch["id"],)) or batch

        if str(batch["status"]) in {"raw_saved", "import_failed"}:
            try:
                notify = str(batch.get("intent") or "") in {
                    CaptureIntent.INCREMENTAL_POLL.value,
                    CaptureIntent.MONTHLY_AUDIT.value,
                }
                counts = await self._ingest_capture_v2_posts(
                    profile_id,
                    raw["items"],
                    notify=notify,
                )
            except Exception as exc:
                self.db.transition_paid_source_batch(
                    int(batch["id"]),
                    "import_failed",
                    expected_status=str(batch["status"]),
                    error=str(exc)[:4000],
                )
                raise
            identity_hash = duplicate_page_circuit_breaker(
                previous_cursor=None,
                previous_identities=[],
                current_cursor=None,
                current_identities=counts["identities"],
            ).current_identity_hash
            batch = self.db.transition_paid_source_batch(
                int(batch["id"]),
                "imported",
                expected_status=str(batch["status"]),
                parsed_result_count=counts["seen"],
                new_result_count=counts["new"],
                updated_result_count=counts["updated"],
                duplicate_result_count=counts["duplicate"],
                identity_set_hash=identity_hash,
                error=None,
            )
        self.db.execute(
            "UPDATE capture_epochs SET status='running',started_at=COALESCE(started_at,?),updated_at=? WHERE id=?",
            (utcnow(), utcnow(), epoch_id),
        )
        self._commit_capture_v2_batch(
            profile=profile,
            epoch=epoch,
            coverage=coverage,
            batch=batch,
            raw=raw,
            maximum=maximum,
        )

    def _purge_duplicate_profile_previews(self) -> dict[str, int]:
        """Remove downloaded image previews that duplicate an avatar or cover asset."""
        rows = self.db.rows(
            """SELECT e.profile_id,em.entity_id,em.version_id,em.media_id,em.role,
            m.source_url,m.path,m.sha256
            FROM entity_media em
            JOIN entities e ON e.id=em.entity_id
            JOIN media m ON m.id=em.media_id
            WHERE e.kind='profile' AND em.role IN ('profile_picture','cover_photo','image')"""
        )
        protected_assets: dict[int, set[str]] = {}
        for row in rows:
            if row["role"] in {"profile_picture", "cover_photo"}:
                protected_assets.setdefault(int(row["profile_id"]), set()).add(
                    normalize_url(str(row["source_url"]))
                )

        duplicate_links = [
            row for row in rows
            if row["role"] == "image"
            and normalize_url(str(row["source_url"])) in protected_assets.get(int(row["profile_id"]), set())
        ]
        counts = {"links_removed": 0, "files_removed": 0}
        affected_media: dict[int, dict[str, Any]] = {}
        for row in duplicate_links:
            self.db.execute(
                "DELETE FROM entity_media WHERE entity_id=? AND version_id=? AND media_id=? AND role='image'",
                (row["entity_id"], row["version_id"], row["media_id"]),
            )
            counts["links_removed"] += 1
            affected_media[int(row["media_id"])] = row

        media_root = self.media.root.resolve()
        thumbnail_root = (self.settings.data_dir / "cache" / "thumbnails").resolve()
        for media_id, row in affected_media.items():
            if self.db.row("SELECT 1 present FROM entity_media WHERE media_id=? LIMIT 1", (media_id,)):
                continue
            self.db.execute("DELETE FROM media WHERE id=?", (media_id,))
            path = Path(str(row.get("path") or ""))
            try:
                resolved = path.resolve()
                if path.is_file() and (resolved == media_root or media_root in resolved.parents):
                    path.unlink()
                    counts["files_removed"] += 1
            except OSError:
                pass
            sha = str(row.get("sha256") or "")
            if sha and thumbnail_root.is_dir():
                for thumbnail in thumbnail_root.glob(f"{sha}-*.webp"):
                    try:
                        thumbnail.unlink()
                    except OSError:
                        pass
        return counts

    def stop(self) -> None:
        self.stop_event.set()

    def _seed_initial_jobs(self) -> None:
        if self.db.row("SELECT id FROM jobs WHERE status IN ('pending','running') LIMIT 1") or self.db.row("SELECT id FROM profiles WHERE last_attempt_at IS NOT NULL OR next_visit_at IS NOT NULL LIMIT 1"):
            return
        now = datetime.now(UTC)
        profiles = self.db.rows("SELECT * FROM profiles WHERE enabled=1 ORDER BY id")
        for index, profile in enumerate(profiles):
            delay = 0 if index == 0 else random.uniform(self.settings.spacing_min_minutes, self.settings.spacing_max_minutes) * 60 * index
            self._enqueue(int(profile["id"]), "visit", 10, now + timedelta(seconds=delay))

    def _seed_upgrade_repair(self) -> None:
        if self.db.migration_applied(REPAIR_MIGRATION):
            return
        if self.db.row("SELECT id FROM jobs WHERE job_type='migrate_raw' AND status='failed' LIMIT 1"):
            return
        existing = self.db.row("SELECT id FROM versions LIMIT 1") or self.db.row("SELECT id FROM profiles WHERE last_attempt_at IS NOT NULL LIMIT 1")
        if not existing:
            self.db.mark_migration(REPAIR_MIGRATION, {"fresh_install": True})
            return
        if not self.db.row("SELECT id FROM jobs WHERE job_type='migrate_raw' AND status IN ('pending','running')"):
            self._enqueue(None, "migrate_raw", 1, datetime.now(UTC))
        now = datetime.now(UTC)
        elapsed = 0.0
        for index, profile in enumerate(self.db.rows("SELECT id FROM profiles WHERE enabled=1 ORDER BY id")):
            if self.db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='repair_scan' AND status IN ('pending','running','done')", (profile["id"],)):
                continue
            if index:
                elapsed += random.uniform(self.settings.spacing_min_minutes, self.settings.spacing_max_minutes)
            self.db.execute("UPDATE jobs SET status='superseded',finished_at=? WHERE profile_id=? AND job_type='visit' AND status='pending'", (utcnow(), profile["id"]))
            self._enqueue(int(profile["id"]), "repair_scan", 2, now + timedelta(minutes=elapsed))

    def _seed_profile_pic_migration(self) -> None:
        if self.db.migration_applied(PROFILE_PIC_MIGRATION):
            return
        if not self.db.row("SELECT id FROM versions LIMIT 1"):
            self.db.mark_migration(PROFILE_PIC_MIGRATION, {"fresh_install": True})
            return
        if self.db.row("SELECT id FROM jobs WHERE job_type='migrate_profile_pics' AND status IN ('pending','running','failed') LIMIT 1"):
            return
        self._enqueue(None, "migrate_profile_pics", 0, datetime.now(UTC))

    def _seed_notification_hygiene(self) -> None:
        if self.db.migration_applied(NOTIFICATION_HYGIENE_MIGRATION):
            return
        if not self.db.row("SELECT id FROM jobs WHERE job_type='dedupe_database' AND status IN ('pending','running')"):
            self._enqueue(None, "dedupe_database", 0, datetime.now(UTC))

    def _repair_actor_diagnostic_statuses(self) -> int:
        corrected = 0
        for run in self.db.rows("SELECT id,summary_json FROM actor_runs WHERE status IN ('succeeded','succeeded_zero') AND summary_json IS NOT NULL"):
            try:
                error = actor_summary_error(json.loads(run["summary_json"]))
            except (TypeError, json.JSONDecodeError):
                error = None
            if error:
                self.db.execute("UPDATE actor_runs SET status='failed_summary',error=? WHERE id=?", (error[:4000], run["id"]))
                corrected += 1
        return corrected

    def _enqueue(self, profile_id: int | None, job_type: str, priority: int, available: datetime, payload: dict[str, Any] | None = None) -> int:
        return self.db.execute(
            "INSERT INTO jobs(profile_id,job_type,priority,payload_json,available_at,created_at) VALUES(?,?,?,?,?,?)",
            (profile_id, job_type, priority, json.dumps(payload or {}), available.isoformat(), utcnow()),
        )

    async def _scheduler_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                # Deployment creates this flag before asking the current
                # container to drain.  Keep health/outbox loops alive, but do
                # not enqueue or claim another job until the replacement
                # container removes the flag.
                if (self.settings.data_dir / "deploy-maintenance").exists():
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=5)
                    except TimeoutError:
                        pass
                    continue
                self._reload_config_if_changed()
                self._enqueue_due_visits()
                self._enqueue_due_special_detection()
                await self._run_next_job()
            except Exception:
                log.exception("scheduler loop failed")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=15)
            except TimeoutError:
                pass

    def _reload_config_if_changed(self) -> None:
        mtime = self.settings.config_path.stat().st_mtime
        if mtime == self._config_mtime:
            return
        new = load_settings(self.settings.config_path)
        self.db.sync_profiles(new.profiles)
        self.settings = new
        self._config_mtime = mtime
        for index, profile in enumerate(self.db.rows("SELECT * FROM profiles WHERE enabled=1 AND last_attempt_at IS NULL")):
            exists = self.db.row("SELECT id FROM jobs WHERE profile_id=? AND status IN ('pending','running')", (profile["id"],))
            if not exists:
                self._enqueue(int(profile["id"]), "visit", 10, datetime.now(UTC) + timedelta(minutes=index * random.uniform(self.settings.spacing_min_minutes, self.settings.spacing_max_minutes)))

    def _enqueue_due_visits(self) -> None:
        now = utcnow()
        for profile in self.db.rows("SELECT * FROM profiles WHERE enabled=1 AND next_visit_at IS NOT NULL AND next_visit_at<=?", (now,)):
            exists = self.db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='visit' AND status IN ('pending','running')", (profile["id"],))
            if not exists:
                self._enqueue(int(profile["id"]), "visit", 10, datetime.now(UTC))
                self.db.execute("UPDATE profiles SET next_visit_at=NULL WHERE id=?", (profile["id"],))

    def _enqueue_due_special_detection(self) -> None:
        if not self.settings.capture_v2_enabled:
            return
        profile = self._special_profile()
        if not profile:
            return
        # Once anonymous verification has confirmed public access, probes are
        # no longer useful.  Keep (or resume) the single capture epoch instead
        # of enqueueing a high-priority no-op every scheduler tick.
        if (
            str(profile.get("public_state") or "") == "public"
            and self._has_confirmed_public_observation(int(profile["id"]))
        ):
            self._resume_or_seed_capture_v2_history(profile, "confirmed_public_resume")
            return
        existing = self.db.row(
            """SELECT id FROM jobs WHERE profile_id=? AND job_type IN ('detect_public_v2','verify_public_v2')
            AND status IN ('pending','running') LIMIT 1""",
            (profile["id"],),
        )
        if existing:
            return
        latest = self.db.row(
            """SELECT observed_at FROM access_observations WHERE profile_id=?
            AND source LIKE 'special_probe:%' ORDER BY observed_at DESC LIMIT 1""",
            (profile["id"],),
        )
        if latest:
            try:
                observed = datetime.fromisoformat(str(latest["observed_at"]).replace("Z", "+00:00"))
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=UTC)
                if datetime.now(UTC) - observed < timedelta(hours=self.settings.special_detection_hours):
                    return
            except ValueError:
                pass
        self._enqueue_v2_unique(
            int(profile["id"]), "detect_public_v2", -400, {"epoch_id": 0}
        )

    def _select_next_job(self, now: str) -> dict[str, Any] | None:
        """Select a due job without starving ordinary patrol work.

        Capture recovery deliberately uses negative priorities.  Looking at
        the last four started jobs makes the 4:1 fairness rule durable across
        service restarts: after four consecutive priority jobs, one due
        non-negative job is allowed through.  If no ordinary job is due, the
        priority queue continues instead of idling.
        """

        recent = self.db.rows(
            "SELECT priority FROM jobs WHERE started_at IS NOT NULL ORDER BY started_at DESC,id DESC LIMIT 4"
        )
        if len(recent) == 4 and all(int(row["priority"]) < 0 for row in recent):
            ordinary = self.db.row(
                """SELECT * FROM jobs WHERE status='pending' AND available_at<=?
                AND priority>=0 ORDER BY priority,id LIMIT 1""",
                (now,),
            )
            if ordinary:
                return ordinary
        return self.db.row(
            "SELECT * FROM jobs WHERE status='pending' AND available_at<=? ORDER BY priority,id LIMIT 1",
            (now,),
        )

    async def _run_next_job(self) -> None:
        job = self._select_next_job(utcnow())
        if not job:
            return
        if (
            not self.settings.apify_v1_backfill_enabled
            and job["job_type"] in {"backfill", "backfill_comments", "audit"}
        ):
            self.db.execute(
                """UPDATE jobs SET status='paused_contract',finished_at=?,
                error='V1 無游標回溯已停用；等待 Capture V2 Actor 契約通過' WHERE id=?""",
                (utcnow(), job["id"]),
            )
            return
        try:
            job_payload = json.loads(job.get("payload_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            job_payload = {}
        is_manual_visit = (
            isinstance(job_payload, dict)
            and job["job_type"] in {"visit", "browser_visit"}
            and job_payload.get("manual") is True
        )
        last = self.db.row("SELECT started_at FROM jobs WHERE profile_id IS NOT NULL AND started_at IS NOT NULL AND id<>? ORDER BY started_at DESC LIMIT 1", (job["id"],))
        if not is_manual_visit and job["profile_id"] is not None and last and last["started_at"]:
            earliest = datetime.fromisoformat(last["started_at"]) + timedelta(minutes=random.uniform(self.settings.spacing_min_minutes, self.settings.spacing_max_minutes))
            if earliest > datetime.now(UTC):
                self.db.execute(
                    "UPDATE jobs SET available_at=? WHERE id=? AND status='pending'",
                    (earliest.isoformat(), job["id"]),
                )
                return
        job = self.db.claim_pending_job(
            int(job["id"]), lease_owner=self.worker_id, claimed_at=utcnow()
        )
        if job is None:
            # Another scheduler won the status compare-and-swap.
            return
        try:
            if job["job_type"] == "visit":
                await self.visit_profile(int(job["profile_id"]))
            elif job["job_type"] == "browser_visit":
                await self.browser_visit_profile(int(job["profile_id"]))
            elif job["job_type"] == "detect_public_v2":
                await self.detect_public_v2(int(job["profile_id"]))
            elif job["job_type"] == "verify_public_v2":
                await self.verify_public_v2(int(job["profile_id"]))
            elif job["job_type"] == "capture_posts_v2":
                await self.capture_posts_v2(int(job["profile_id"]), job_payload)
            elif job["job_type"] == "capture_comments_v2":
                await self.capture_comments_v2(int(job["profile_id"]), job_payload)
            elif job["job_type"] == "contract_test_posts_v2":
                job_payload.setdefault("contract_test_id", f"job:{job['id']}")
                await self.contract_test_posts_v2(int(job["profile_id"]), job_payload)
            elif job["job_type"] == "backfill":
                await self.backfill_profile(int(job["profile_id"]))
            elif job["job_type"] == "backfill_comments":
                await self.backfill_comments(int(job["profile_id"]), job_payload)
            elif job["job_type"] == "audit":
                await self.audit_profile(int(job["profile_id"]))
            elif job["job_type"] == "migrate_raw":
                counts = await self.ingester.reprocess_existing()
                self.db.mark_migration(REPAIR_MIGRATION, counts)
                self.db.add_event(
                    f"migration:{REPAIR_MIGRATION}",
                    "repair_summary",
                    {"title": "資料修復完成", "text": f"重新解析 {counts['versions']} 個版本；補得 {counts['media']} 個媒體連結；更新 {counts['names']} 筆名稱；錯誤 {counts['errors']} 筆。"},
                )
            elif job["job_type"] == "migrate_profile_pics":
                counts = await self.ingester.reprocess_existing()
                counts["diagnostics"] = self._repair_actor_diagnostic_statuses()
                self.db.mark_migration(PROFILE_PIC_MIGRATION, counts)
                self.db.add_event(
                    f"migration:{PROFILE_PIC_MIGRATION}",
                    "repair_summary",
                    {"title": "大頭照欄位修復完成", "text": f"重新解析 {counts['versions']} 個版本；補得 {counts['media']} 個媒體連結；更正 {counts['diagnostics']} 筆 Actor 診斷狀態；錯誤 {counts['errors']} 筆。未重新呼叫 Apify。"},
                )
            elif job["job_type"] == "repair_scan":
                await self.visit_profile(int(job["profile_id"]))
                profile = self.db.row("SELECT name,display_name,url FROM profiles WHERE id=?", (job["profile_id"],)) or {}
                self.db.add_event(
                    f"repair:{job['profile_id']}:{REPAIR_MIGRATION}",
                    "repair_summary",
                    {"title": f"{profile.get('display_name') or profile.get('name') or 'Facebook'} 修復掃描完成", "text": "已套用多格式貼文重試、內嵌貼文 fallback 與遞迴媒體解析。", "source_url": profile.get("url", "")},
                    int(job["profile_id"]),
                )
            elif job["job_type"] == "dedupe_database":
                run_id = self.db.start_maintenance_run("upgrade_dedupe")
                try:
                    baseline = self.db.establish_notification_baseline()
                    counts = await asyncio.to_thread(self._dedupe_database)
                    counts.update(baseline)
                    self.db.finish_maintenance_run(run_id, counts)
                    self.db.mark_migration(NOTIFICATION_HYGIENE_MIGRATION, counts)
                except Exception as exc:
                    self.db.finish_maintenance_run(run_id, {}, str(exc))
                    raise
            self.db.execute(
                """UPDATE jobs SET status='done',finished_at=?,error=NULL
                WHERE id=? AND status='running' AND lease_owner=?""",
                (utcnow(), job["id"], self.worker_id),
            )
        except BrowserGuardDeferred as exc:
            resume = exc.decision.retry_at or (datetime.now(UTC) + timedelta(minutes=5))
            self.db.execute(
                """UPDATE jobs SET status='pending',available_at=?,started_at=NULL,
                finished_at=NULL,error=?,attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END,
                lease_owner=NULL,leased_at=NULL
                WHERE id=? AND status='running' AND lease_owner=?""",
                (resume.isoformat(), str(exc)[:2000], job["id"], self.worker_id),
            )
        except ApifyFrozen as exc:
            self.db.execute(
                """UPDATE jobs SET status='skipped_apify_frozen',finished_at=?,error=?
                WHERE id=? AND status='running' AND lease_owner=?""",
                (utcnow(), str(exc), job["id"], self.worker_id),
            )
            if job["job_type"] == "visit":
                self._schedule_next(int(job["profile_id"]))
        except BudgetExceeded as exc:
            resume = exc.resume_at or self._next_month()
            self.db.execute(
                """UPDATE jobs SET status='deferred_budget',finished_at=?,error=?
                WHERE id=? AND status='running' AND lease_owner=?""",
                (utcnow(), str(exc), job["id"], self.worker_id),
            )
            if job["job_type"] == "visit":
                self.db.execute("UPDATE profiles SET next_visit_at=? WHERE id=?", (resume.isoformat(), job["profile_id"]))
            else:
                self._enqueue(int(job["profile_id"]), job["job_type"], int(job["priority"]), resume, json.loads(job["payload_json"]))
        except Exception as exc:
            self.db.execute(
                """UPDATE jobs SET status='failed',finished_at=?,error=?
                WHERE id=? AND status='running' AND lease_owner=?""",
                (utcnow(), str(exc)[:2000], job["id"], self.worker_id),
            )
            if job["job_type"] == "backfill_comments":
                self._enqueue(int(job["profile_id"]), "backfill_comments", int(job["priority"]), datetime.now(UTC) + timedelta(minutes=15), job_payload)
            if job["profile_id"]:
                self._record_failure(int(job["profile_id"]), exc)
            else:
                self.db.add_event(
                    f"system-job:{job['id']}:failed",
                    "system_error",
                    {"title": "系統修復工作失敗，已停止", "text": f"工作：{job['job_type']}\n錯誤：{str(exc)[:3000]}"},
                )

    async def browser_visit_profile(self, profile_id: int) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            return
        if not self.settings.facebook_browser_enabled:
            raise FacebookBrowserError("Facebook 直接瀏覽器尚未啟用")
        self._acquire_browser(
            profile,
            anonymous=False,
            operation="browser_visit",
            defer_job=True,
        )
        attempted_at = utcnow()
        self.db.execute(
            "UPDATE profiles SET last_attempt_at=?,browser_canary_last_attempt_at=? WHERE id=?",
            (attempted_at, attempted_at, profile_id),
        )
        diagnostic_key = str(profile_id)
        try:
            item = await self.facebook_browser.profile(str(profile["url"]), diagnostic_key)
            await self._store_profile_details(profile, item)
            browser_backfill_done = bool(profile.get("browser_post_backfill_done"))
            if browser_backfill_done:
                posts = await self.facebook_browser.canary_posts(
                    str(profile["url"]), diagnostic_key
                )
                page = {
                    "posts": posts,
                    "next_cursor": profile.get("browser_post_cursor"),
                    "completed": True,
                }
            else:
                page = await self.facebook_browser.canary_post_page(
                    str(profile["url"]), diagnostic_key, profile.get("browser_post_cursor")
                )
        except FacebookBrowserChallengeRequired as exc:
            self._record_browser_challenge(
                profile,
                anonymous=False,
                diagnostic_key=diagnostic_key,
                error=exc,
            )
            raise
        except FacebookBrowserLoginRequired as exc:
            # A stale/expired login is local session state, not proof that
            # Facebook challenged this OCI/IP.  Keep the shared breaker for
            # checkpoints, challenges and 429 only.
            raise
        self.browser_guard.record_success(profile_id)
        posts = [post for post in page.get("posts") or [] if isinstance(post, dict)]
        ingest_stats = await self._ingest_browser_canary_posts(
            profile_id,
            posts,
            notify=True,
        )
        self.db.execute(
            "UPDATE profiles SET browser_post_cursor=?,browser_post_backfill_done=? WHERE id=?",
            (page.get("next_cursor"), int(bool(page.get("completed"))), profile_id),
        )
        display = item.get("name") or profile.get("display_name") or profile.get("name") or "Facebook"
        self.db.add_event(
            f"browser-manual:{profile_id}:{utcnow()}",
            "browser_manual_visit",
            {
                "title": f"{display} 瀏覽器拜訪完成",
                "text": (
                    "已更新個人資料、擷取畫面；"
                    f"補充 {ingest_stats['enriched']} 篇已由公開來源列舉的貼文，"
                    f"另有 {ingest_stats['skipped_unlisted']} 篇未經獨立公開列舉，未匯入清冊。"
                ),
                "source_url": profile["url"],
            },
            profile_id,
            notify=False,
        )

    def _remaining_budget(self) -> float:
        snapshot = self.db.apify_usage_snapshot()
        if snapshot:
            try:
                now = datetime.now(UTC)
                start_at = datetime.fromisoformat(str(snapshot["cycle_start_at"]).replace("Z", "+00:00"))
                end_at = datetime.fromisoformat(str(snapshot["cycle_end_at"]).replace("Z", "+00:00"))
                if start_at.tzinfo is None:
                    start_at = start_at.replace(tzinfo=UTC)
                if end_at.tzinfo is None:
                    end_at = end_at.replace(tzinfo=UTC)
                if start_at <= now <= end_at:
                    return max(0.0, self.settings.monthly_budget_usd - float(snapshot["used_usd"]))
            except (KeyError, TypeError, ValueError):
                pass
        month = datetime.now(UTC).strftime("%Y-%m")
        return max(0.0, self.settings.monthly_budget_usd - self.db.usage_total(month))

    @staticmethod
    def _next_month() -> datetime:
        now = datetime.now(UTC)
        if now.month == 12:
            return datetime(now.year + 1, 1, 1, 0, 5, tzinfo=UTC)
        return datetime(now.year, now.month + 1, 1, 0, 5, tzinfo=UTC)

    def _available_for(self, category: str) -> float:
        # Personal-profile retrieval is handled by SerpApi, so all Apify budget
        # is available to posts and comments instead of being reserved for the
        # retired Apify profile Actor.
        return self._remaining_budget()

    @staticmethod
    def _usage_cycle_resume(usage: MonthlyUsage) -> datetime:
        end_at = datetime.fromisoformat(usage.cycle_end_at.replace("Z", "+00:00"))
        if end_at.tzinfo is None:
            end_at = end_at.replace(tzinfo=UTC)
        return end_at.astimezone(UTC) + timedelta(minutes=5)

    async def _official_available(self) -> tuple[float, MonthlyUsage]:
        try:
            usage = await self.apify.monthly_usage()
        except Exception as exc:
            day = datetime.now(UTC).date().isoformat()
            self.db.add_event(
                f"budget-check:{day}:failed",
                "budget_check_failed",
                {"title": "Apify 官方用量查詢失敗", "text": f"為避免超支，本次 Actor 不執行。\n錯誤：{str(exc)[:1000]}"},
            )
            raise BudgetExceeded(
                "Apify 官方用量查詢失敗；本次付費工作已取消",
                datetime.now(UTC) + timedelta(minutes=15),
            ) from exc
        self.db.save_apify_usage(usage.used_usd, usage.cycle_start_at, usage.cycle_end_at)
        return max(0.0, self.settings.monthly_budget_usd - usage.used_usd), usage

    async def _actor(self, category: str, actor_id: str, payload: dict[str, Any], profile_id: int | None = None, input_variant: str = "default") -> ActorResult:
        if profile_id is not None and self.db.profile_source_frozen(profile_id, "apify"):
            raise ApifyFrozen("此帳號已凍結 Apify；本次付費工作未執行")
        official_remaining, official_usage = await self._official_available()
        remaining = min(self._available_for(category), official_remaining)
        if remaining < PRICES[category]:
            cycle_key = official_usage.cycle_start_at[:10]
            if official_remaining < PRICES[category]:
                self.db.add_event(f"budget:{cycle_key}:limit", "budget_limit", {"title": "Apify 官方用量已達上限", "text": f"官方用量 ${official_usage.used_usd:.2f} / ${self.settings.monthly_budget_usd:.2f}；付費工作暫停至帳期更新。"})
                raise BudgetExceeded("Apify 官方帳期已無足夠額度產生一筆結果；付費工作延至下一帳期", self._usage_cycle_resume(official_usage))
            else:
                self.db.add_event(f"budget:{cycle_key}:reserved", "budget_reserved", {"title": "Apify 剩餘預算已保留", "text": "剩餘額優先保留給個人檔案與公開狀態檢查；貼文、留言與回溯暫停。"})
                raise BudgetExceeded("Apify 剩餘額度已保留給個人檔案檢查；本次工作不執行")
        # The official usage lookup above is an await point.  Re-read the
        # unified source-control table immediately before recording/calling
        # the paid Actor so a freeze applied during that await wins the race.
        if profile_id is not None and self.db.profile_source_frozen(profile_id, "apify"):
            raise ApifyFrozen("此帳號已凍結 Apify；本次付費工作未執行")
        diagnostic_id = self.db.start_actor_run(profile_id, category, actor_id, input_variant, payload)
        try:
            result = await self.apify.call(actor_id, payload, remaining)
        except Exception as exc:
            self.db.finish_actor_run(diagnostic_id, status="failed", error=str(exc))
            raise
        result.diagnostic_id = diagnostic_id
        raw_result_count = result.raw_result_count if result.raw_result_count is not None else len(result.items)
        result.raw_result_count = raw_result_count
        summary_error = actor_summary_error(result.summary)
        self.db.finish_actor_run(
            diagnostic_id,
            status="failed_summary" if summary_error else ("succeeded" if result.items else "succeeded_zero"),
            run_id=result.run_id,
            result_count=len(result.items),
            charged_usd=result.charged_usd,
            summary=result.summary,
            error=summary_error,
            samples=result.items,
            raw_result_count=raw_result_count,
            parsed_result_count=len(result.items),
        )
        # Store pricing is result-based; usageTotalUsd captures platform/run charges
        # when Apify exposes them. A small conservative run buffer prevents the
        # local ledger from understating synthetic Actor-start events.
        cost = max(raw_result_count * PRICES[category], result.charged_usd) + 0.001
        self.db.add_usage(datetime.now(UTC).strftime("%Y-%m"), category, raw_result_count, cost)
        cycle_key = official_usage.cycle_start_at[:10]
        used = official_usage.used_usd
        if used >= self.settings.monthly_budget_usd * self.settings.budget_warning_ratio:
            self.db.add_event(f"budget:{cycle_key}:warning", "budget_warning", {"title": "Apify 官方用量已使用 80%", "text": f"執行前官方用量 ${used:.2f} / ${self.settings.monthly_budget_usd:.2f}"})
        return result

    async def visit_profile(self, profile_id: int) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile:
            return
        now = utcnow()
        self.db.execute("UPDATE profiles SET last_attempt_at=? WHERE id=?", (now, profile_id))
        if self._serpapi_profile_due(profile):
            try:
                await self._refresh_serpapi_profile(profile)
                profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,)) or profile
            except SerpApiQuotaExceeded as exc:
                if exc.account is not None:
                    self.db.save_serpapi_usage(exc.account)
                day = datetime.now(UTC).date().isoformat()
                self.db.add_event(
                    f"serpapi:{day}:quota",
                    "serpapi_quota",
                    {
                        "title": "SerpApi 免費查詢額度已用完",
                        "text": f"{exc}；若已設定 Bright Data 將改用備援，貼文仍依 Apify 額度執行。\n監控網址：{profile['url']}",
                        "source_url": profile["url"],
                    },
                )
                await self._try_brightdata_fallback(profile, str(exc))
            except SerpApiError as exc:
                hour = datetime.now(UTC).strftime("%Y-%m-%dT%H")
                self.db.add_event(
                    f"serpapi:{profile_id}:{hour}:failed",
                    "serpapi_error",
                    {
                        "title": "SerpApi 個人檔案查詢失敗",
                        "text": f"{str(exc)[:850]}\n監控網址：{profile['url']}",
                        "source_url": profile["url"],
                    },
                    profile_id,
                )
                await self._try_brightdata_fallback(profile, str(exc))
            profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,)) or profile
        if (
            self.settings.capture_v2_enabled
            and profile.get("public_state") == "public"
            and not self._has_confirmed_public_observation(profile_id)
        ):
            # A legacy public flag may have come from SerpApi, Bright Data, or
            # an authenticated browser.  Re-verify anonymously before any V2
            # paid epoch is created.
            self._enqueue_v2_unique(
                profile_id,
                "verify_public_v2",
                -210,
                {"epoch_id": 0},
            )
            self._schedule_next(profile_id)
            return
        if profile.get("public_state") != "public":
            self._schedule_next(profile_id)
            return
        if not self.settings.apify_v1_backfill_enabled:
            # V1's cursorless latest-post probe was still billable even when
            # all legacy backfill jobs were paused.  Once V1 is disabled, a
            # regular visit must never enter the old posts/comments path.
            if self.settings.capture_v2_enabled:
                self._ensure_capture_v2_epoch(
                    profile,
                    "regular_visit_v2",
                    observed_at=self._capture_v2_datetime(now),
                )
            else:
                canary_items = await self._try_browser_canary(
                    profile, 0, "apify_v1_disabled"
                )
                await self._ingest_browser_canary_posts(
                    profile_id, canary_items, notify=True
                )
            self._schedule_next(profile_id)
            return
        initial = not bool(self.db.row("SELECT id FROM entities WHERE profile_id=? AND kind='post' LIMIT 1", (profile_id,)))
        if not initial and not profile.get("backfill_done"):
            # Backfill itself is the source of post coverage. A latest-post
            # probe here both spends another result and can return early before
            # the missing backfill job is repaired.
            active_backfill = self.db.row(
                "SELECT id FROM jobs WHERE profile_id=? AND job_type IN ('backfill','backfill_comments') AND status IN ('pending','running')",
                (profile_id,),
            )
            if not active_backfill:
                self._enqueue(
                    profile_id,
                    "backfill",
                    30,
                    datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes),
                )
            self._schedule_next(profile_id)
            return
        try:
            posts = await self._fetch_regular_posts(profile, initial)
        except (ApifyFrozen, BudgetExceeded) as exc:
            reason = "apify_frozen" if isinstance(exc, ApifyFrozen) else "apify_budget"
            canary_items = await self._try_browser_canary(profile, 0, reason)
            await self._ingest_browser_canary_posts(profile_id, canary_items, notify=not initial)
            self._schedule_next(profile_id)
            return
        if isinstance(posts.summary, dict) and posts.summary.get("source") == "unchanged_probe":
            self._schedule_next(profile_id)
            return
        initial_pointer: str | None = None
        initial_posts_finished = False
        initial_limited = False
        if initial and isinstance(posts.summary, dict) and posts.summary.get("profiles"):
            summary_profile = posts.summary["profiles"][0]
            initial_pointer = (summary_profile.get("pointer") or {}).get("nextCursor")
            coverage = summary_profile.get("coverageStatus") or summary_profile.get("coverage_status") or ""
            initial_posts_finished = not bool(initial_pointer)
            initial_limited = initial_posts_finished and str(coverage).startswith("partial")
        summary_error = actor_summary_error(posts.summary)
        if not posts.items:
            if summary_error and not self.settings.browser_canary_enabled:
                raise RuntimeError(f"貼文 Actor 三種輸入格式均失敗：{summary_error}")
        if initial and posts.items:
            self.db.execute(
                "UPDATE profiles SET backfill_done=0,backfill_cursor=?,last_full_audit_at=NULL WHERE id=?",
                (initial_pointer, profile_id),
            )
            profile["backfill_done"] = 0
        post_urls, _ = await self._ingest_apify_posts(
            profile_id,
            posts.items,
            notify=not initial,
            diagnostic_id=posts.diagnostic_id,
        )
        # A regular patrol is a bounded latest-post sample, not a complete
        # inventory. Never mark older posts removed from this sample; removal
        # reconciliation belongs to the cursor-based full audit below.

        cached_canary = self.facebook_browser.cached_canary_posts(str(profile["url"]))
        canary_items: list[dict[str, Any]] = []
        if cached_canary is not None or summary_error or len(posts.items) < self.settings.recent_posts:
            canary_items = await self._try_browser_canary(
                profile,
                len(posts.items),
                "actor_error" if summary_error else "partial_actor_result",
            )
            if initial and canary_items and not posts.items:
                self.db.execute("UPDATE profiles SET backfill_done=0,backfill_cursor=NULL,last_full_audit_at=NULL WHERE id=?", (profile_id,))
                profile["backfill_done"] = 0
            await self._ingest_browser_canary_posts(profile_id, canary_items, notify=not initial)
        if summary_error and not canary_items:
            raise RuntimeError(f"貼文 Actor 回傳錯誤：{summary_error}")
        if post_urls and not initial and self._remaining_budget() > 0:
            try:
                await self._fetch_comments(profile_id, post_urls, notify=not initial)
            except (ApifyFrozen, BudgetExceeded):
                pass
        active_backfill = self.db.row(
            "SELECT id FROM jobs WHERE profile_id=? AND job_type IN ('backfill','backfill_comments') AND status IN ('pending','running')",
            (profile_id,),
        )
        if not profile["backfill_done"] and not active_backfill:
            available = datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes)
            if initial and initial_posts_finished:
                self._enqueue(
                    profile_id,
                    "backfill_comments",
                    31,
                    available,
                    {"offset": 0, "limited": initial_limited},
                )
            else:
                self._enqueue(profile_id, "backfill", 30, available)
        elif profile["backfill_done"]:
            last_audit = datetime.fromisoformat(profile["last_full_audit_at"]) if profile.get("last_full_audit_at") else None
            if (not last_audit or datetime.now(UTC) - last_audit >= timedelta(days=self.settings.full_audit_days)) and not self.db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='audit' AND status IN ('pending','running')", (profile_id,)):
                self._enqueue(profile_id, "audit", 40, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
        self._schedule_next(profile_id)

    async def _fetch_regular_posts(self, profile: dict[str, Any], initial: bool) -> ActorResult:
        """Probe one latest post before paying for the normal ten-post batch."""
        if not initial and not profile.get("backfill_done"):
            return ActorResult([], {"source": "backfill_pending"}, "")
        always_full = str(profile.get("url") or "").rstrip("/") in {
            str(url).rstrip("/") for url in self.settings.always_full_fetch_urls
        }
        if initial:
            return await self._fetch_posts(profile, self.settings.backfill_posts)
        if always_full:
            # Full-fetch exceptions still use a cheap latest-post probe.  Only
            # fetch the configured batch when the newest post changed; this
            # preserves complete capture for updates without paying for the
            # same 50-result batch on every patrol.
            probe = await self._fetch_posts(profile, 1)
            probe_error = actor_summary_error(probe.summary)
            if not probe_error and self._probe_posts_unchanged(int(profile["id"]), probe.items):
                self.db.update_actor_ingest_counts(
                    probe.diagnostic_id,
                    new=0,
                    updated=0,
                    duplicate=len(probe.items),
                )
                return ActorResult(
                    [],
                    {"source": "unchanged_probe", "profiles": [{"status": "succeeded", "postsReturned": 1}]},
                    probe.run_id,
                    probe.charged_usd,
                    probe.diagnostic_id,
                )
            if probe_error or not probe.items:
                return probe
            return await self._fetch_posts(profile, self.settings.always_full_fetch_max_posts)

        probe = await self._fetch_posts(profile, 1)
        probe_error = actor_summary_error(probe.summary)
        if not probe_error and self._probe_posts_unchanged(int(profile["id"]), probe.items):
            # The single probe result is already known and unchanged. Avoid a
            # second Actor run and, importantly, avoid the comments Actor too.
            self.db.update_actor_ingest_counts(
                probe.diagnostic_id,
                new=0,
                updated=0,
                duplicate=len(probe.items),
            )
            return ActorResult([], {"source": "unchanged_probe", "profiles": [{"status": "succeeded", "postsReturned": 1}]}, probe.run_id, probe.charged_usd, probe.diagnostic_id)
        if probe_error or not probe.items:
            return probe
        return await self._fetch_posts(profile, self.settings.recent_posts)

    def _probe_posts_unchanged(self, profile_id: int, posts: list[dict[str, Any]]) -> bool:
        if len(posts) != 1:
            return False
        item = posts[0]
        identity = facebook_post_identity(str(next((item.get(key) for key in ("source_url", "postUrl", "post_url", "url", "facebookUrl") if item.get(key)), "")))
        ext_id = external_id(item, "post")
        existing = self.db.row("SELECT * FROM entities WHERE profile_id=? AND kind='post' AND external_id=?", (profile_id, ext_id))
        if not existing and identity:
            for row in self.db.rows("SELECT * FROM entities WHERE profile_id=? AND kind='post' AND source_url IS NOT NULL", (profile_id,)):
                if facebook_post_identity(str(row.get("source_url") or "")) == identity:
                    existing = row
                    break
        if not existing:
            return False
        digest = content_hash(monitored_projection(item, "post", extract_media(item, "post")))
        return existing.get("current_hash") == digest

    def _browser_canary_due(self, profile: dict[str, Any]) -> bool:
        if not self.settings.facebook_browser_enabled or not self.settings.browser_canary_enabled:
            return False
        if self.settings.browser_canary_max_posts <= 0:
            return False
        attempted_at = profile.get("browser_canary_last_attempt_at")
        if not attempted_at:
            return True
        try:
            attempted = datetime.fromisoformat(str(attempted_at))
            if attempted.tzinfo is None:
                attempted = attempted.replace(tzinfo=UTC)
        except ValueError:
            return True
        return datetime.now(UTC) - attempted >= timedelta(hours=self.settings.browser_canary_cooldown_hours)

    async def _try_browser_canary(
        self,
        profile: dict[str, Any],
        api_result_count: int,
        reason: str,
    ) -> list[dict[str, Any]]:
        if not self.settings.facebook_browser_enabled or not self.settings.browser_canary_enabled:
            return []
        profile_url = str(profile["url"])
        cached = self.facebook_browser.cached_canary_posts(profile_url)
        reused_page = cached is not None
        if cached is None:
            if not self._browser_canary_due(profile):
                return []
        if not self._acquire_browser(
            profile,
            anonymous=False,
            operation="browser_canary",
            defer_job=False,
        ):
            return []
        if cached is None:
            attempted_at = utcnow()
            self.db.execute(
                "UPDATE profiles SET browser_canary_last_attempt_at=? WHERE id=?",
                (attempted_at, profile["id"]),
            )
            profile["browser_canary_last_attempt_at"] = attempted_at
        try:
            # canary_posts reuses cached profile-page parsing, then opens only
            # the selected permalinks to complete their photo viewers.
            items = await self.facebook_browser.canary_posts(profile_url, str(profile["id"]))
        except FacebookBrowserChallengeRequired as exc:
            self._record_browser_challenge(
                profile,
                anonymous=False,
                diagnostic_key=str(profile["id"]),
                error=exc,
            )
            self.db.add_event(
                f"browser-canary:{profile['id']}:{utcnow()[:13]}:failed",
                "browser_canary_failed",
                {
                    "title": "Chromium 金絲雀補抓失敗",
                    "text": f"原因：{reason}；{str(exc)[:600]}",
                    "source_url": profile_url,
                },
                int(profile["id"]),
                notify=False,
            )
            return []
        except FacebookBrowserLoginRequired as exc:
            self.db.add_event(
                f"browser-canary:{profile['id']}:{utcnow()[:13]}:failed",
                "browser_canary_failed",
                {
                    "title": "Chromium 金絲雀補抓失敗",
                    "text": f"原因：{reason}；{str(exc)[:600]}",
                    "source_url": profile_url,
                },
                int(profile["id"]),
                notify=False,
            )
            return []
        except FacebookBrowserError as exc:
            self.db.add_event(
                f"browser-canary:{profile['id']}:{utcnow()[:13]}:failed",
                "browser_canary_failed",
                {
                    "title": "Chromium 金絲雀補抓失敗",
                    "text": f"原因：{reason}；{str(exc)[:600]}",
                    "source_url": profile_url,
                },
                int(profile["id"]),
                notify=False,
            )
            return []
        self.browser_guard.record_success(int(profile["id"]))
        self.db.add_event(
            f"browser-canary:{profile['id']}:{utcnow()[:13]}:done",
            "browser_canary",
            {
                "title": "Chromium 金絲雀本輪補抓完成",
                "text": (
                    f"Apify {api_result_count} 篇；補抓 {len(items)} 篇、"
                    f"{sum(len(item.get('images') or []) for item in items)} 張照片；"
                    f"{'沿用個人頁解析結果' if reused_page else '重新載入個人頁'}"
                ),
                "source_url": profile_url,
            },
            int(profile["id"]),
            notify=False,
        )
        return items

    async def _ingest_browser_canary_posts(
        self,
        profile_id: int,
        items: list[dict[str, Any]],
        notify: bool,
    ) -> dict[str, int]:
        """Enrich posts already established by a public inventory source.

        The logged-in browser can see content that is not public.  It is
        therefore not an authority for adding a post to the public inventory,
        even when the profile itself is currently classified as public.  A
        browser result may only update an existing canonical post (for example
        with text or higher-resolution attachments).  Unknown permalinks are
        retained as a non-notifying diagnostic until Apify or the anonymous
        browser independently enumerates them.
        """
        existing = self.db.rows(
            "SELECT external_id,source_url FROM entities WHERE profile_id=? AND kind='post'",
            (profile_id,),
        )
        ids_by_external_id = {
            str(row["external_id"]): str(row["external_id"])
            for row in existing
            if row.get("external_id") not in (None, "")
        }
        urls_by_external_id = {
            str(row["external_id"]): str(row["source_url"])
            for row in existing
            if row.get("external_id") not in (None, "") and row.get("source_url")
        }
        ids_by_url = {
            normalize_url(str(row["source_url"])): str(row["external_id"])
            for row in existing
            if row.get("source_url")
        }
        ids_by_identity = {
            identity: str(row["external_id"])
            for row in existing
            if row.get("source_url")
            for identity in [facebook_post_identity(str(row["source_url"]))]
            if identity
        }
        profile = self.db.row(
            "SELECT url FROM profiles WHERE id=?",
            (profile_id,),
        ) or {}
        enriched = 0
        skipped = 0
        for raw_item in items:
            item = dict(raw_item)
            source_url = next(
                (
                    str(item[key])
                    for key in (
                        "source_url",
                        "postUrl",
                        "post_url",
                        "url",
                        "facebookUrl",
                    )
                    if item.get(key)
                ),
                "",
            )
            identity = facebook_post_identity(source_url)
            browser_external_id = next(
                (
                    str(item[key])
                    for key in (
                        "source_post_id",
                        "postId",
                        "post_id",
                        "id",
                        "facebookId",
                    )
                    if item.get(key) not in (None, "")
                ),
                "",
            )
            known_id = (
                ids_by_external_id.get(browser_external_id)
                or (ids_by_identity.get(identity) if identity else None)
                or (
                    ids_by_url.get(normalize_url(source_url))
                    if source_url
                    else None
                )
            )
            if not known_id:
                skipped += 1
                evidence_token = identity or normalize_url(source_url) or browser_external_id
                if not evidence_token:
                    evidence_token = content_hash(item)
                self.db.add_event(
                    f"browser-post-unlisted:{profile_id}:{content_hash(evidence_token)[:24]}",
                    "browser_post_unlisted",
                    {
                        "title": "登入瀏覽器發現未經公開來源列舉的貼文",
                        "text": "未匯入公開貼文清冊；等待 Apify 或匿名瀏覽器獨立列舉後才可補充。",
                        "source_url": source_url or str(profile.get("url") or ""),
                    },
                    profile_id,
                    notify=False,
                )
                continue
            item["source_post_id"] = known_id
            if not source_url and urls_by_external_id.get(known_id):
                item["source_url"] = urls_by_external_id[known_id]
            await self.ingester.ingest(profile_id, "post", item, notify=notify)
            enriched += 1
        return {"enriched": enriched, "skipped_unlisted": skipped}

    async def _ingest_apify_posts(
        self,
        profile_id: int,
        items: list[dict[str, Any]],
        *,
        notify: bool,
        diagnostic_id: int | None,
    ) -> tuple[list[str], set[str]]:
        existing_rows = self.db.rows(
            "SELECT external_id,source_url FROM entities WHERE profile_id=? AND kind='post'",
            (profile_id,),
        )
        ids_by_identity = {
            identity: str(row["external_id"])
            for row in existing_rows
            if row.get("source_url")
            for identity in [facebook_post_identity(str(row["source_url"]))]
            if identity
        }
        new_count = 0
        updated_count = 0
        duplicate_count = 0
        post_urls: list[str] = []
        persisted_ids: set[str] = set()
        for raw_item in items:
            item = dict(raw_item)
            source_url = next(
                (str(item.get(key)) for key in ("source_url", "postUrl", "post_url", "url", "facebookUrl") if item.get(key)),
                "",
            )
            identity = facebook_post_identity(source_url)
            known_alias_id = ids_by_identity.get(identity) if identity else None
            if known_alias_id:
                item["source_post_id"] = known_alias_id
            ext_id = external_id(item, "post")
            existed = self.db.row(
                "SELECT id FROM entities WHERE profile_id=? AND kind='post' AND external_id=?",
                (profile_id, ext_id),
            )
            _, persisted_id, changed = await self.ingester.ingest(profile_id, "post", item, notify=notify)
            persisted_ids.add(persisted_id)
            if not existed:
                new_count += 1
            elif changed:
                updated_count += 1
            else:
                duplicate_count += 1
            if source_url:
                post_urls.append(source_url)
            if identity:
                ids_by_identity[identity] = ext_id
        self.db.update_actor_ingest_counts(
            diagnostic_id,
            new=new_count,
            updated=updated_count,
            duplicate=duplicate_count,
        )
        return post_urls, persisted_ids

    def _serpapi_profile_due(self, profile: dict[str, Any]) -> bool:
        checked_at = profile.get("serp_last_checked_at")
        if not checked_at:
            return True
        try:
            checked = datetime.fromisoformat(str(checked_at))
            if checked.tzinfo is None:
                checked = checked.replace(tzinfo=UTC)
        except ValueError:
            return True
        return datetime.now(UTC) - checked >= timedelta(hours=self.settings.serpapi_profile_refresh_hours)

    async def _refresh_serpapi_profile(self, profile: dict[str, Any]) -> None:
        result = await self.serpapi.profile(str(profile["url"]))
        account = result.account
        # Account API is queried immediately before the successful search, so
        # reflect the just-consumed search in the local dashboard snapshot.
        account.searches_left = max(0, account.searches_left - 1)
        account.this_month_usage += 1
        self.db.save_serpapi_usage(account)
        item = dict(result.item)
        item["profile_data_source"] = "SerpApi"
        await self._store_profile_details(profile, item)
        if account.searches_left in {50, 10, 0}:
            self.db.add_event(
                f"serpapi:{account.renewal_date}:{account.searches_left}",
                "serpapi_usage_warning",
                {"title": f"SerpApi 剩餘 {account.searches_left} 次", "text": f"{account.plan_name}：已用 {account.this_month_usage} / {account.searches_per_month}；重置日 {account.renewal_date or '-'}。"},
            )

    async def _try_brightdata_fallback(self, profile: dict[str, Any], primary_error: str) -> bool:
        if not self.settings.brightdata_api_token:
            return await self._try_browser_fallback(profile, primary_error, "Bright Data API token 未設定")
        try:
            item = await self.brightdata.profile(str(profile["url"]))
            await self._store_profile_details(profile, item)
        except BrightDataError as exc:
            return await self._try_browser_fallback(profile, primary_error, str(exc))
        self.db.add_event(
            f"brightdata:{profile['id']}:{utcnow()[:13]}:success",
            "brightdata_fallback",
            {"title": "Bright Data 備援查詢成功", "text": f"SerpApi 失敗後已由 Bright Data 更新個人檔案。\n原因：{primary_error[:700]}"},
            int(profile["id"]),
        )
        return True

    async def _try_browser_fallback(self, profile: dict[str, Any], primary_error: str, brightdata_error: str) -> bool:
        if not self.settings.facebook_browser_enabled:
            hour = datetime.now(UTC).strftime("%Y-%m-%dT%H")
            self.db.add_event(
                f"profile-fallback:{profile['id']}:{hour}:failed",
                "profile_fallback_error",
                {
                    "title": "個人檔案備援查詢失敗",
                    "text": f"SerpApi：{primary_error[:350]}\nBright Data：{brightdata_error[:350]}\n直接瀏覽器：未啟用\n監控網址：{profile['url']}",
                    "source_url": profile["url"],
                },
                int(profile["id"]),
            )
            return False
        if not self._acquire_browser(
            profile,
            anonymous=False,
            operation="profile_fallback",
            defer_job=False,
        ):
            return False
        try:
            attempted_at = utcnow()
            self.db.execute(
                "UPDATE profiles SET browser_canary_last_attempt_at=? WHERE id=?",
                (attempted_at, profile["id"]),
            )
            profile["browser_canary_last_attempt_at"] = attempted_at
            item = await self.facebook_browser.profile(str(profile["url"]), str(profile["id"]))
            await self._store_profile_details(profile, item)
        except FacebookBrowserChallengeRequired as exc:
            self._record_browser_challenge(
                profile,
                anonymous=False,
                diagnostic_key=str(profile["id"]),
                error=exc,
            )
            day = datetime.now(UTC).date().isoformat()
            self.db.add_event(
                f"facebook-browser:{day}:challenge",
                "facebook_browser_login_required",
                {
                    "title": "Facebook 瀏覽器需要安全驗證",
                    "text": f"{exc}。請在 OCI 啟動 browser-login，透過 Tailscale 互動式完成驗證。\n監控網址：{profile['url']}",
                    "source_url": profile["url"],
                },
            )
            return False
        except FacebookBrowserLoginRequired as exc:
            day = datetime.now(UTC).date().isoformat()
            self.db.add_event(
                f"facebook-browser:{day}:login-required",
                "facebook_browser_login_required",
                {
                    "title": "Facebook 瀏覽器需重新登入",
                    "text": f"{exc}。請在 OCI 啟動 browser-login，透過 Tailscale 完成互動式登入。\n監控網址：{profile['url']}",
                    "source_url": profile["url"],
                },
            )
            return False
        except FacebookBrowserError as exc:
            hour = datetime.now(UTC).strftime("%Y-%m-%dT%H")
            self.db.add_event(
                f"profile-fallback:{profile['id']}:{hour}:failed",
                "profile_fallback_error",
                {
                    "title": "個人檔案所有備援皆失敗",
                    "text": f"SerpApi：{primary_error[:260]}\nBright Data：{brightdata_error[:260]}\n直接瀏覽器：{str(exc)[:260]}\n監控網址：{profile['url']}",
                    "source_url": profile["url"],
                },
                int(profile["id"]),
            )
            return False
        self.browser_guard.record_success(int(profile["id"]))
        self.db.add_event(
            f"facebook-browser:{profile['id']}:{utcnow()[:13]}:success",
            "facebook_browser_fallback",
            {
                "title": "Facebook 直接瀏覽器備援成功",
                "text": "SerpApi 與 Bright Data 失敗後，已由登入中的 Chromium 更新個人檔案。",
            },
            int(profile["id"]),
        )
        return True

    async def _store_profile_details(self, profile: dict[str, Any], item: dict[str, Any]) -> None:
        item = dict(item)
        # Browser DOM headings can be page labels, group names, or stale UI text.
        # Never let that low-trust fallback replace a name already established by
        # a provider or the user's configured identity. It may still fill a blank
        # FB-id placeholder on the first successful browser visit.
        source_label = str(item.get("profile_data_source") or "")
        browser_source = "瀏覽器" in source_label or "browser" in source_label.casefold()
        existing_name = str(profile.get("display_name") or "")
        rejected: set[str] = set()
        if browser_source:
            try:
                previous_details = json.loads(str(profile.get("profile_details_json") or "{}"))
            except (TypeError, json.JSONDecodeError):
                previous_details = {}
            rejected = {
                str(value).strip()
                for value in previous_details.get("rejected_profile_names", [])
                if str(value).strip()
            }
        if browser_source and not is_placeholder_profile_name(existing_name):
            incoming_name = str(item.get("name") or "").strip()
            historical_names: set[str] = set()
            trusted_names: list[str] = []
            for version in self.db.rows(
                """SELECT v.normalized_json,v.raw_path FROM versions v
                JOIN entities e ON e.id=v.entity_id
                WHERE e.profile_id=? AND e.kind='profile'
                ORDER BY v.seen_at DESC,v.id DESC""",
                (profile["id"],),
            ):
                try:
                    known_name = str(json.loads(version["normalized_json"]).get("authorName") or "").strip()
                except (TypeError, json.JSONDecodeError):
                    continue
                if known_name and not is_placeholder_profile_name(known_name):
                    historical_names.add(known_name)
                try:
                    raw_item = json.loads(Path(str(version.get("raw_path") or "")).read_text(encoding="utf-8"))
                except (OSError, TypeError, json.JSONDecodeError):
                    continue
                raw_source = str(raw_item.get("profile_data_source") or "")
                raw_is_browser = "瀏覽器" in raw_source or "browser" in raw_source.casefold()
                trusted_name = profile_display_name(raw_item)
                if raw_is_browser or is_placeholder_profile_name(trusted_name) or trusted_name in trusted_names:
                    continue
                trusted_names.append(trusted_name)
            if incoming_name and incoming_name != existing_name and incoming_name in historical_names:
                rejected.add(existing_name)
            elif (
                "瀏覽器" in str(previous_details.get("profile_data_source") or "")
                and trusted_names
                and existing_name not in trusted_names
            ):
                restored_name = trusted_names[0]
                rejected.add(existing_name)
                if incoming_name and incoming_name != restored_name:
                    rejected.add(incoming_name)
                item["name"] = restored_name
            else:
                item["name"] = existing_name
        if rejected:
            item["rejected_profile_names"] = sorted(rejected)
        previous_state = str(profile.get("public_state") or "unknown")
        configured_id = profile_id_from_url(str(profile["url"]))
        provider_id = str(item.get("id") or "")
        previous_id = str(profile.get("fb_id") or "")
        # SerpApi may return a pfbid token. Keep numeric Facebook IDs from the
        # monitored URL instead; pfbid is not a useful account identifier here.
        fb_id = next((value for value in (configured_id, provider_id, previous_id) if value.isdigit()), "")
        await self.ingester.ingest(int(profile["id"]), "profile", item, notify=previous_state != "unknown")

        lowered_source = source_label.casefold()
        if browser_source:
            evidence_source = EvidenceSource.BROWSER
            auth_scope = AuthScope.AUTHENTICATED
            observation_source = "profile_refresh:authenticated_browser"
        elif "bright" in lowered_source:
            evidence_source = EvidenceSource.BRIGHT_DATA
            auth_scope = AuthScope.ANONYMOUS
            observation_source = "profile_refresh:bright_data"
        elif "serp" in lowered_source:
            evidence_source = EvidenceSource.SERPAPI
            auth_scope = AuthScope.ANONYMOUS
            observation_source = "profile_refresh:serpapi"
        else:
            evidence_source = EvidenceSource.APIFY
            auth_scope = AuthScope.ANONYMOUS
            observation_source = "profile_refresh:apify"
        target_id = self._capture_v2_target_id(profile)
        observed_id = self._capture_v2_observed_id(item)
        identity_match = bool(target_id and observed_id and target_id == observed_id)
        signal = (
            EvidenceSignal.EXPLICIT_PRIVATE
            if bool(item.get("private") or item.get("is_private"))
            else EvidenceSignal.PUBLIC_CONTENT
        )
        _, classification, _ = self._record_capture_v2_access(
            profile,
            source=evidence_source,
            source_label=observation_source,
            auth_scope=auth_scope,
            signal=signal,
            purpose=ObservationPurpose.GENERAL_PROBE,
            observed_id=observed_id,
            identity_match=identity_match,
            evidence={
                "profile_data_source": source_label or "unknown",
                "private_marker": signal is EvidenceSignal.EXPLICIT_PRIVATE,
            },
        )
        # SerpApi has its own refresh cadence.  A Bright Data or logged-browser
        # fallback must not postpone the next genuine SerpApi attempt.
        checked_at = utcnow() if evidence_source is EvidenceSource.SERPAPI else profile.get("serp_last_checked_at")
        self.db.execute(
            """UPDATE profiles SET fb_id=?,profile_details_json=?,serp_last_checked_at=?,
            missing_successes=0,last_success_at=?,consecutive_failures=0,last_error=NULL WHERE id=?""",
            (fb_id, json.dumps(item, ensure_ascii=False), checked_at, utcnow(), profile["id"]),
        )
        # Weak API signals and authenticated visibility never change
        # public_state.  They only request an anonymous, identity-bound
        # verification; verify_public_v2 owns state transitions and alerts.
        if self.settings.capture_v2_enabled and classification in {
            EvidenceClass.SUSPECTED_PUBLIC,
            EvidenceClass.AUTHENTICATED_VISIBLE,
            EvidenceClass.STRONG_PRIVATE,
        }:
            self._enqueue_v2_unique(
                int(profile["id"]),
                "verify_public_v2",
                -210,
                {"epoch_id": 0},
            )

    async def _fetch_posts(self, profile: dict[str, Any], maximum: int, cursor: str | None = None) -> ActorResult:
        blocked_until = profile.get("apify_posts_blocked_until")
        if blocked_until:
            try:
                resume = datetime.fromisoformat(str(blocked_until))
                if resume.tzinfo is None:
                    resume = resume.replace(tzinfo=UTC)
                if resume > datetime.now(UTC):
                    raise BudgetExceeded("Apify 貼文查詢因付費結果無法解析而暫停", resume)
            except ValueError:
                pass
        original = str(profile["url"])
        numeric = str(profile.get("fb_id") or "")
        if not numeric:
            numeric = next((part for part in original.rstrip("/").split("/")[::-1] if part.isdigit()), "")
        if self.settings.actors.posts == "unseenuser/fb-profile" and numeric:
            # This Actor charges each returned result event.  Numeric profile
            # URLs have a canonical profile.php form, so do not pay for both
            # the original URL and its equivalent alias on every probe.
            variants = [("profile_php", f"https://www.facebook.com/profile.php?id={numeric}")]
        else:
            variants = [("original_url", original)]
            if numeric:
                variants.extend((("numeric_id", numeric), ("profile_php", f"https://www.facebook.com/profile.php?id={numeric}")))
        seen_inputs: set[str] = set()
        last = ActorResult([], None, "")
        successful_empty: ActorResult | None = None
        for label, profile_input in variants:
            if profile_input in seen_inputs:
                continue
            seen_inputs.add(profile_input)
            if self.settings.actors.posts == "unseenuser/fb-profile":
                payload = {"startUrls": [profile_input], "includePosts": True, "maxPosts": maximum}
            else:
                payload = {"profileUrls": [profile_input], "maxPostsPerProfile": maximum, "expandAllPhotos": True, "omitPinnedPosts": True}
                if cursor:
                    payload["startCursor"] = cursor
            payload.update(actor_input(self.settings.actors.posts_input, profile_url=profile_input, max_posts=maximum, cursor=cursor or ""))
            last = await self._actor("posts", self.settings.actors.posts, payload, int(profile["id"]), label)
            last = self._unwrap_embedded_posts(last, maximum)
            raw_count = last.raw_result_count if last.raw_result_count is not None else len(last.items)
            if last.diagnostic_id:
                self.db.finish_actor_run(
                    last.diagnostic_id,
                    status="succeeded" if last.items else "succeeded_zero",
                    run_id=last.run_id,
                    result_count=len(last.items),
                    charged_usd=last.charged_usd,
                    summary=last.summary,
                    raw_result_count=raw_count,
                    parsed_result_count=len(last.items),
                )
            if raw_count > 0 and not last.items:
                resume = datetime.now(UTC) + timedelta(hours=24)
                self.db.execute(
                    """UPDATE profiles SET apify_posts_blocked_until=?,
                    apify_posts_unparsed_streak=apify_posts_unparsed_streak+1 WHERE id=?""",
                    (resume.isoformat(), profile["id"]),
                )
                if last.diagnostic_id:
                    self.db.finish_actor_run(
                        last.diagnostic_id,
                        status="unparsed_paid_result",
                        run_id=last.run_id,
                        result_count=0,
                        charged_usd=last.charged_usd,
                        summary=last.summary,
                        error=f"Apify 回傳 {raw_count} 筆計費結果，但未解析出貼文；已暫停 24 小時",
                        parsed_result_count=0,
                    )
                raise BudgetExceeded("Apify 回傳付費結果但無法解析；已安全暫停 24 小時", resume)
            if last.items:
                self.db.execute(
                    "UPDATE profiles SET apify_posts_blocked_until=NULL,apify_posts_unparsed_streak=0 WHERE id=?",
                    (profile["id"],),
                )
            if not last.items:
                if not actor_summary_error(last.summary):
                    successful_empty = successful_empty or last
                continue
            if not any(any(item.get(k) for k in ("source_post_id", "postId", "post_id", "id", "facebookId", "source_url", "postUrl", "url", "facebookUrl")) for item in last.items):
                if last.diagnostic_id:
                    self.db.finish_actor_run(last.diagnostic_id, status="import_failed", run_id=last.run_id, result_count=len(last.items), charged_usd=last.charged_usd, summary=last.summary, error="貼文 Actor schema 找不到穩定 ID 或 URL", samples=last.items)
                continue
            return last
        return successful_empty or last

    @staticmethod
    def _unwrap_embedded_posts(result: ActorResult, maximum: int) -> ActorResult:
        wrappers: list[dict[str, Any]] = []
        direct: list[dict[str, Any]] = []
        for item in result.items:
            if isinstance(item.get("posts"), list) or item.get("type") == "profile" or "_metadata" in item:
                wrappers.append(item)
                for post in item.get("posts") or []:
                    if isinstance(post, dict):
                        normalized = dict(post)
                        normalized.setdefault("ingest_source", "posts_actor_embedded")
                        direct.append(normalized)
            else:
                direct.append(item)
        if not wrappers:
            return result
        errors = [str(item.get("error")) for item in wrappers if item.get("error")]
        if errors and not direct:
            summary = {"health": "failed", "profiles": [{"status": "failed", "error": {"message": message}} for message in errors]}
        else:
            coverage = "partial_actor_limit" if len(direct) >= maximum else "complete"
            summary = {"source": "embedded_posts", "profiles": [{"status": "succeeded", "postsReturned": len(direct), "coverageStatus": coverage, "pointer": {"nextCursor": None}}]}
        return ActorResult(
            direct,
            summary,
            result.run_id,
            result.charged_usd,
            result.diagnostic_id,
            result.raw_result_count if result.raw_result_count is not None else len(result.items),
        )

    async def _fetch_comments(self, profile_id: int, post_urls: list[str], notify: bool) -> None:
        remaining = self._available_for("comments")
        limit = max(1, int(remaining / PRICES["comments"]))
        payload: dict[str, Any] = {"startUrls": [{"url": url} for url in post_urls], "resultsLimit": limit, "includeNestedComments": True, "viewOption": "RANKED_UNFILTERED"}
        payload.update(actor_input(self.settings.actors.comments_input, post_urls=post_urls, results_limit=limit))
        result = await self._actor("comments", self.settings.actors.comments, payload, profile_id, "post_urls")
        if result.items and not any(any(item.get(k) for k in ("commentId", "comment_id", "id", "text")) for item in result.items):
            if result.diagnostic_id:
                self.db.finish_actor_run(result.diagnostic_id, status="import_failed", run_id=result.run_id, result_count=len(result.items), charged_usd=result.charged_usd, summary=result.summary, error="留言 Actor schema 找不到 ID 或文字", samples=result.items)
            raise RuntimeError("留言 Actor schema 異常：找不到留言 ID 或文字")
        by_post: dict[str, set[str]] = {url: set() for url in post_urls}
        baseline_before = {
            url: bool(self.db.row("SELECT 1 FROM comment_baselines WHERE profile_id=? AND parent_external_id=?", (profile_id, url)))
            for url in post_urls
        }
        new_count = 0
        updated_count = 0
        duplicate_count = 0
        for comment in result.items:
            parent_url = str(comment.get("facebookUrl") or comment.get("inputUrl") or comment.get("postUrl") or "")
            if not parent_url and len(post_urls) == 1:
                parent_url = post_urls[0]
            had_baseline = baseline_before.setdefault(parent_url, bool(self.db.row("SELECT 1 FROM comment_baselines WHERE profile_id=? AND parent_external_id=?", (profile_id, parent_url))))
            candidate_id = external_id(comment, "comment")
            existed = self.db.row(
                "SELECT id FROM entities WHERE profile_id=? AND kind='comment' AND external_id=? AND parent_external_id=?",
                (profile_id, candidate_id, parent_url),
            )
            _, ext, changed = await self.ingester.ingest(profile_id, "comment", comment, notify=notify and had_baseline, parent_external_id=parent_url)
            if not existed:
                new_count += 1
            elif changed:
                updated_count += 1
            else:
                duplicate_count += 1
            by_post.setdefault(parent_url, set()).add(ext)
        self.db.update_actor_ingest_counts(
            result.diagnostic_id,
            new=new_count,
            updated=updated_count,
            duplicate=duplicate_count,
        )
        # Absence is only reconciled by a complete, uncapped result set; partial budget runs must not imply deletion.
        if len(result.items) < limit:
            for parent_url, seen in by_post.items():
                self.ingester.reconcile(profile_id, "comment", seen, None, notify=notify and baseline_before.get(parent_url, False), parent_external_id=parent_url)
        # The first successful retrieval is the per-post baseline: preserve all
        # historic replies silently, then only notify later real changes.
        for parent_url in by_post:
            self.db.execute("INSERT OR IGNORE INTO comment_baselines(profile_id,parent_external_id,established_at) VALUES(?,?,?)", (profile_id, parent_url, utcnow()))

    async def backfill_profile(self, profile_id: int) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile or profile["public_state"] != "public":
            return
        if profile.get("apify_frozen"):
            return
        result = await self._fetch_posts(profile, self.settings.backfill_posts, profile.get("backfill_cursor"))
        await self._ingest_apify_posts(
            profile_id,
            result.items,
            notify=False,
            diagnostic_id=result.diagnostic_id,
        )
        if not result.summary or not result.summary.get("profiles"):
            raise RuntimeError("貼文 Actor schema 異常：完整回溯缺少 SUMMARY.profiles")
        summary_profile = result.summary["profiles"][0]
        pointer = (summary_profile.get("pointer") or {}).get("nextCursor")
        coverage = summary_profile.get("coverageStatus") or summary_profile.get("coverage_status") or ""
        limited = not pointer and str(coverage).startswith("partial")
        self.db.execute("UPDATE profiles SET backfill_cursor=?,backfill_done=0 WHERE id=?", (pointer, profile_id))
        if pointer:
            # Finish every cursor-addressable post page before spending quota
            # on historical comments.
            self._enqueue(profile_id, "backfill", 30, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
            return

        # Some result-based Actors cap the embedded posts but expose no cursor.
        # Retrying such a page only buys the same results again. Treat the
        # returned range as the Actor's boundary and continue with comments.
        self._enqueue(
            profile_id,
            "backfill_comments",
            31,
            datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes),
            {"offset": 0, "limited": limited},
        )

    def _backfill_post_urls(self, profile_id: int, offset: int, limit: int) -> list[str]:
        rows = self.db.rows(
            """SELECT source_url FROM entities
            WHERE profile_id=? AND kind='post' AND present=1
              AND source_url IS NOT NULL AND source_url!=''
            ORDER BY COALESCE(published_at, last_seen_at) DESC, id DESC
            LIMIT ? OFFSET ?""",
            (profile_id, limit, offset),
        )
        return [str(row["source_url"]) for row in rows]

    async def backfill_comments(self, profile_id: int, payload: dict[str, Any]) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile or profile["public_state"] != "public":
            return
        if profile.get("apify_frozen"):
            return
        legacy_urls = payload.get("post_urls")
        batch_size = max(1, int(self.settings.backfill_posts))
        offset = max(0, int(payload.get("offset") or 0))
        post_urls = (
            [str(url) for url in legacy_urls or [] if url]
            if legacy_urls is not None
            else self._backfill_post_urls(profile_id, offset, batch_size)
        )
        if post_urls and self._remaining_budget() < PRICES["comments"]:
            raise BudgetExceeded("貼文已完成；留言等待 Apify 額度恢復", self._next_month())
        if post_urls:
            await self._fetch_comments(profile_id, post_urls, notify=False)
        pointer = payload.get("next_cursor")
        if legacy_urls is not None and pointer:
            self._enqueue(profile_id, "backfill", 30, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
            return
        if legacy_urls is None and len(post_urls) == batch_size:
            next_offset = offset + len(post_urls)
            if self._backfill_post_urls(profile_id, next_offset, 1):
                self._enqueue(
                    profile_id,
                    "backfill_comments",
                    31,
                    datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes),
                    {"offset": next_offset, "limited": bool(payload.get("limited"))},
                )
                return
        self.db.execute("UPDATE profiles SET backfill_cursor=NULL,backfill_done=1,last_full_audit_at=? WHERE id=?", (utcnow(), profile_id))
        counts = self.db.row("SELECT SUM(kind='post') posts,SUM(kind='comment') comments FROM entities WHERE profile_id=?", (profile_id,)) or {}
        limited = bool(payload.get("limited"))
        event_type = "backfill_limited" if limited else "backfill_complete"
        title = f"{profile['name']} Actor 可取得範圍回溯完成" if limited else f"{profile['name']} 回溯完成"
        text = f"已保存 {counts.get('posts') or 0} 篇貼文、{counts.get('comments') or 0} 則留言。"
        if limited:
            text += " 貼文 Actor 未提供下一頁游標，已停止重複抓取相同批次。"
        self.db.add_event(
            f"profile:{profile_id}:{event_type}",
            event_type,
            {"title": title, "text": text, "source_url": profile["url"]},
            profile_id,
        )

    async def audit_profile(self, profile_id: int) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile or profile["public_state"] != "public":
            return
        if profile.get("apify_frozen"):
            return
        token = profile.get("audit_token") or datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        cursor = profile.get("audit_cursor")
        result = await self._fetch_posts(profile, self.settings.backfill_posts, cursor)
        post_urls, persisted_ids = await self._ingest_apify_posts(
            profile_id,
            result.items,
            notify=True,
            diagnostic_id=result.diagnostic_id,
        )
        with self.db.connect() as conn:
            for ext in persisted_ids:
                conn.execute("INSERT OR IGNORE INTO audit_seen(profile_id,audit_token,kind,external_id) VALUES(?,?,?,?)", (profile_id, token, "post", ext))
        if post_urls and self._remaining_budget() > 0:
            await self._fetch_comments(profile_id, post_urls, notify=True)
        if not result.summary or not result.summary.get("profiles"):
            raise RuntimeError("貼文 Actor schema 異常：完整核對缺少 SUMMARY.profiles")
        summary_profile = result.summary["profiles"][0]
        pointer = (summary_profile.get("pointer") or {}).get("nextCursor")
        coverage = summary_profile.get("coverageStatus") or summary_profile.get("coverage_status") or ""
        limited = not pointer and str(coverage).startswith("partial")
        if pointer:
            self.db.execute("UPDATE profiles SET audit_cursor=?,audit_token=? WHERE id=?", (pointer, token, profile_id))
            if self._remaining_budget() > 0:
                self._enqueue(profile_id, "audit", 40, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
            return
        if not limited:
            seen = {row["external_id"] for row in self.db.rows("SELECT external_id FROM audit_seen WHERE profile_id=? AND audit_token=? AND kind='post'", (profile_id, token))}
            self.ingester.reconcile(profile_id, "post", seen, None, notify=True)
        self.db.execute("DELETE FROM audit_seen WHERE profile_id=? AND audit_token=?", (profile_id, token))
        self.db.execute("UPDATE profiles SET audit_cursor=NULL,audit_token=NULL,last_full_audit_at=? WHERE id=?", (utcnow(), profile_id))
        if limited:
            self.db.add_event(
                f"profile:{profile_id}:audit_limited:{datetime.now(UTC).date().isoformat()}",
                "audit_limited",
                {
                    "title": f"{profile['name']} 完整核對受 Actor 範圍限制",
                    "text": "貼文 Actor 未提供下一頁游標；本次不判定舊貼文已移除，也不重複抓取相同批次。",
                    "source_url": profile["url"],
                },
                profile_id,
            )

    def _record_profile_missing(self, profile: dict[str, Any]) -> None:
        misses = int(profile["missing_successes"]) + 1
        state = profile["public_state"]
        if state == "unknown":
            state = "unavailable"
        elif state == "public" and misses >= 2:
            state = "unavailable"
            self.db.add_event(f"profile:{profile['id']}:unavailable:{utcnow()}", "profile_unavailable", {"title": f"{profile['name']} 已連續兩輪無法公開取得", "source_url": profile["url"]}, profile["id"])
        self.db.execute("UPDATE profiles SET public_state=?,missing_successes=? WHERE id=?", (state, misses, profile["id"]))

    def _record_failure(self, profile_id: int, exc: Exception) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile:
            return
        failures = int(profile["consecutive_failures"]) + 1
        self.db.execute("UPDATE profiles SET consecutive_failures=?,last_error=? WHERE id=?", (failures, str(exc)[:1000], profile_id))
        if failures in {1, 3}:
            self.db.add_event(
                f"profile:{profile_id}:error:{failures}:{utcnow()[:13]}",
                "system_error",
                {
                    "title": f"{profile['name']} 抓取失敗（連續 {failures} 次）",
                    "text": f"{str(exc)[:850]}\n監控網址：{profile['url']}",
                    "source_url": profile["url"],
                },
                profile_id,
            )
        self._schedule_next(profile_id)

    def _schedule_next(self, profile_id: int) -> None:
        hours = random.uniform(self.settings.visit_min_hours, self.settings.visit_max_hours)
        next_at = datetime.now(UTC) + timedelta(hours=hours)
        self.db.execute("UPDATE profiles SET next_visit_at=? WHERE id=?", (next_at.isoformat(), profile_id))

    async def _outbox_loop(self) -> None:
        while not self.stop_event.is_set():
            sent = await self.telegram.drain_once()
            if not sent:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=10)
                except TimeoutError:
                    pass

    async def _health_loop(self) -> None:
        tz = ZoneInfo(self.settings.timezone)
        while not self.stop_event.is_set():
            local = datetime.now(tz)
            if local.hour == self.settings.health_hour:
                key = f"health:{local.date().isoformat()}"
                profiles = self.db.rows("SELECT COALESCE(display_name,name) name,public_state,last_success_at,next_visit_at,consecutive_failures FROM profiles WHERE enabled=1 ORDER BY id")
                free_gb = __import__("shutil").disk_usage(self.settings.data_dir).free / 1024**3
                official = self.db.apify_usage_snapshot()
                used = float(official["used_usd"]) if official else self.settings.monthly_budget_usd - self._remaining_budget()
                usage_label = "官方" if official else "本地估算"
                lines = self._health_profile_lines(profiles, self.settings.timezone)
                self.db.add_event(key, "health", {"title": "每日 08:00 健康摘要", "text": "\n".join(lines) + f"\n磁碟剩餘：{free_gb:.1f} GB\nApify（{usage_label}）：${used:.2f}/${self.settings.monthly_budget_usd:.2f}"})
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except TimeoutError:
                pass

    @staticmethod
    def _health_profile_lines(profiles: list[dict[str, Any]], timezone: str = "Asia/Taipei") -> list[str]:
        lines = []
        for profile in profiles:
            name = str(profile["name"])
            if name.startswith("FB-") and name[3:].isdigit():
                name = "姓名待確認"
            last_success = telegram_time(profile.get("last_success_at"), timezone)
            lines.append(f"{name}: {profile['public_state']} · 最近成功 {last_success}")
        return lines

    async def _media_retry_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if not self.media.has_space():
                    day = datetime.now(UTC).date().isoformat()
                    self.db.add_event(f"disk:low:{day}", "disk_low", {"title": "媒體下載已暫停", "text": f"磁碟剩餘空間低於 {self.settings.low_disk_gb:g} GB。文字與 metadata 仍會繼續保存。"})
                else:
                    latest_disk = self.db.row("SELECT event_type FROM events WHERE event_type IN ('disk_low','disk_recovered') ORDER BY id DESC LIMIT 1")
                    if latest_disk and latest_disk["event_type"] == "disk_low":
                        self.db.add_event(f"disk:recovered:{datetime.now(UTC).isoformat()}", "disk_recovered", {"title": "媒體下載已恢復", "text": "磁碟空間已高於安全門檻，系統開始補抓待下載媒體。"})
                    self.db.execute("UPDATE media SET status='unavailable',error='超過 30 天補抓期限' WHERE status IN ('pending','paused_low_disk') AND retry_until IS NOT NULL AND retry_until<?", (utcnow(),))
                    row = self.db.row(
                        """SELECT * FROM media WHERE status IN ('pending','paused_low_disk')
                        AND (retry_until IS NULL OR retry_until>=?)
                        AND (last_attempt_at IS NULL OR last_attempt_at<=?) ORDER BY id LIMIT 1""",
                        (utcnow(), (datetime.now(UTC) - timedelta(hours=1)).isoformat()),
                    )
                    if row:
                        result = await self.media.download(row["source_url"])
                        await self._complete_media_retry(row, result)
            except Exception:
                log.exception("media retry failed")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=600)
            except TimeoutError:
                pass

    async def _daily_media_dedupe_loop(self) -> None:
        tz = ZoneInfo(self.settings.timezone)
        while not self.stop_event.is_set():
            local = datetime.now(tz)
            if local.hour == 3:
                key = f"media-dedupe:{local.date().isoformat()}"
                if not self.db.row("SELECT id FROM events WHERE event_key=?", (key,)):
                    async with self._maintenance_lock:
                        run_id = self.db.start_maintenance_run("daily_media_dedupe")
                        try:
                            counts = await asyncio.to_thread(self._dedupe_existing_media)
                            self.db.finish_maintenance_run(run_id, counts)
                        except Exception as exc:
                            self.db.finish_maintenance_run(run_id, {}, str(exc))
                            raise
                    self.db.add_event(key, "media_dedupe", {"title": "每日媒體去重完成", "text": f"重新雜湊 {counts['checked']} 個檔案；合併 {counts['merged']} 筆；移除 {counts['orphaned']} 個無引用檔案；錯誤 {counts['errors']} 筆。"}, notify=False)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except TimeoutError:
                pass

    def capture_storage_snapshot(self, snapshot_date: str) -> dict[str, Any]:
        return collect_storage_snapshot(
            self.db,
            self.settings.data_dir,
            self.settings.facebook_browser_data_dir,
            snapshot_date,
        )

    def record_daily_storage_snapshot(self, snapshot_date: str) -> bool:
        event_key = f"storage-daily:{snapshot_date}"
        if self.db.row("SELECT id FROM events WHERE event_key=?", (event_key,)):
            return False
        current = self.capture_storage_snapshot(snapshot_date)
        previous = self.db.row(
            "SELECT * FROM storage_snapshots WHERE snapshot_date<? ORDER BY snapshot_date DESC LIMIT 1",
            (snapshot_date,),
        )
        return bool(self.db.add_event(event_key, "storage_daily", daily_storage_message(current, previous)))

    async def _storage_snapshot_loop(self) -> None:
        tz = ZoneInfo(self.settings.timezone)
        while not self.stop_event.is_set():
            local = datetime.now(tz)
            snapshot_date = local.date().isoformat()
            if local.hour >= self.settings.health_hour:
                try:
                    await asyncio.to_thread(self.record_daily_storage_snapshot, snapshot_date)
                except Exception:
                    log.exception("daily storage snapshot failed")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except TimeoutError:
                pass

    async def _browser_evidence_cleanup_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                local_date = datetime.now(ZoneInfo(self.settings.timezone)).date().isoformat()
            except Exception:
                local_date = datetime.now(UTC).date().isoformat()
            if getattr(self, "_browser_evidence_cleanup_date", None) != local_date:
                await asyncio.to_thread(self._cleanup_browser_evidence)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except TimeoutError:
                pass

    async def _capture_raw_cleanup_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                local_date = datetime.now(ZoneInfo(self.settings.timezone)).date().isoformat()
            except Exception:
                local_date = datetime.now(UTC).date().isoformat()
            if getattr(self, "_capture_raw_cleanup_date", None) != local_date:
                await asyncio.to_thread(self._cleanup_capture_raw)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60)
            except TimeoutError:
                pass

    def _dedupe_existing_media(self) -> dict[str, int]:
        counts = {"checked": 0, "merged": 0, "orphaned": 0, "errors": 0}
        verified: list[dict[str, Any]] = []
        actual_sha256: dict[int, str] = {}
        for row in self.db.rows("SELECT * FROM media WHERE status='ready' ORDER BY id"):
            path = Path(row.get("path") or "")
            if not path.is_file():
                counts["errors"] += 1
                continue
            try:
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for block in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(block)
                sha = digest.hexdigest()
                counts["checked"] += 1
                perceptual = self.media.perceptual_hash(path, str(row.get("mime_type") or ""))
                actual_sha256[int(row["id"])] = sha
                # Preserve the row that already owns this digest.  Exact-byte
                # duplicates are atomically redirected in the consolidation
                # pass below, avoiding a UNIQUE collision half-way through a
                # maintenance run.
                digest_owner = self.db.row(
                    "SELECT id FROM media WHERE sha256=? AND id<>?",
                    (sha, row["id"]),
                )
                if not digest_owner:
                    old_sha = str(row.get("sha256") or "")
                    with self.db.connect() as conn:
                        conn.execute(
                            "UPDATE media SET sha256=?,perceptual_hash=?,size_bytes=? WHERE id=?",
                            (sha, perceptual, path.stat().st_size, row["id"]),
                        )
                        conn.execute(
                            "UPDATE media_aliases SET sha256=? WHERE media_id=?",
                            (sha, row["id"]),
                        )
                        if old_sha:
                            conn.execute(
                                """UPDATE outbox SET media_sha256=?
                                WHERE kind='media' AND status='pending'
                                  AND media_sha256=?""",
                                (sha, old_sha),
                            )
                    row["sha256"] = sha
                else:
                    self.db.execute(
                        "UPDATE media SET perceptual_hash=?,size_bytes=? WHERE id=?",
                        (perceptual, path.stat().st_size, row["id"]),
                    )
                row["perceptual_hash"] = perceptual
                row["size_bytes"] = path.stat().st_size
                verified.append(row)
            except Exception:
                counts["errors"] += 1
        # Never merge on aHash distance alone.  The shared ingester applies
        # exact-byte or decoded RGB/aspect-ratio proof, selects the highest
        # quality file, and rebinds aliases, entity links and pending outbox
        # rows in one transaction before deleting any file.
        counts["merged"] = self.ingester.consolidate_media_representations(
            verified,
            actual_sha256=actual_sha256,
        )
        for row in self.db.rows(
            """SELECT m.* FROM media m
            LEFT JOIN entity_media em ON em.media_id=m.id
            LEFT JOIN media_aliases ma ON ma.media_id=m.id
            WHERE em.media_id IS NULL AND ma.media_id IS NULL
              AND NOT EXISTS(
                SELECT 1 FROM outbox o
                WHERE o.kind='media' AND o.status='pending'
                  AND o.media_sha256=m.sha256
              )"""
        ):
            try:
                path = Path(row.get("path") or "")
                if path.is_file():
                    path.unlink()
                self.db.execute("DELETE FROM media WHERE id=?", (row["id"],))
                counts["orphaned"] += 1
            except OSError:
                counts["errors"] += 1
        return counts

    def _dedupe_database(self) -> dict[str, int]:
        counts = {"entities_merged": 0, "posts_merged": 0, "comments_merged": 0}
        groups = self.db.rows("""SELECT profile_id,kind,current_hash,GROUP_CONCAT(id) ids
            FROM entities WHERE current_hash IS NOT NULL GROUP BY profile_id,kind,current_hash HAVING COUNT(*) > 1""")
        alias_groups: dict[tuple[int, str], list[int]] = {}
        for row in self.db.rows("SELECT id,profile_id,source_url FROM entities WHERE kind='post' AND source_url IS NOT NULL"):
            identity = facebook_post_identity(str(row.get("source_url") or ""))
            if identity:
                alias_groups.setdefault((int(row["profile_id"]), identity), []).append(int(row["id"]))
        groups = list(groups) + [
            {"profile_id": profile_id, "kind": "post", "current_hash": None, "ids": ",".join(map(str, ids))}
            for (profile_id, _), ids in alias_groups.items()
            if len(ids) > 1
        ]
        for group in groups:
            ids = sorted(
                int(value)
                for value in str(group["ids"]).split(",")
                if self.db.row("SELECT id FROM entities WHERE id=?", (int(value),))
            )
            if len(ids) < 2:
                continue
            canonical = ids[0]
            for duplicate in ids[1:]:
                with self.db.connect() as conn:
                    versions = conn.execute("SELECT * FROM versions WHERE entity_id=? ORDER BY id", (duplicate,)).fetchall()
                    for version in versions:
                        target = conn.execute("SELECT id FROM versions WHERE entity_id=? AND content_hash=?", (canonical, version["content_hash"])).fetchone()
                        target_id = int(target["id"]) if target else int(version["id"])
                        links = conn.execute("SELECT media_id,role,discovery_path,position FROM entity_media WHERE entity_id=? AND version_id=?", (duplicate, version["id"])).fetchall()
                        for link in links:
                            conn.execute("INSERT OR IGNORE INTO entity_media(entity_id,version_id,media_id,role,discovery_path,position) VALUES(?,?,?,?,?,?)", (canonical, target_id, link["media_id"], link["role"], link["discovery_path"], link["position"]))
                        conn.execute("DELETE FROM entity_media WHERE entity_id=? AND version_id=?", (duplicate, version["id"]))
                        if target:
                            conn.execute("DELETE FROM versions WHERE id=?", (version["id"],))
                        else:
                            conn.execute("UPDATE versions SET entity_id=? WHERE id=?", (canonical, version["id"]))
                    conn.execute("UPDATE events SET entity_id=? WHERE entity_id=?", (canonical, duplicate))
                    conn.execute("UPDATE notification_groups SET entity_id=? WHERE entity_id=?", (canonical, duplicate))
                    conn.execute("UPDATE entities SET notification_hash=COALESCE(notification_hash,(SELECT notification_hash FROM entities WHERE id=?)) WHERE id=?", (duplicate, canonical))
                    conn.execute("DELETE FROM entities WHERE id=?", (duplicate,))
                counts["entities_merged"] += 1
                if group["kind"] == "post":
                    counts["posts_merged"] += 1
                elif group["kind"] == "comment":
                    counts["comments_merged"] += 1
        media_counts = self._dedupe_existing_media()
        counts.update(media_counts)
        return counts

    async def _complete_media_retry(self, row: dict[str, Any], result: dict[str, Any]) -> None:
        if result.get("status") != "ready":
            self.db.execute(
                "UPDATE media SET status=?,last_attempt_at=?,error=?,retry_until=COALESCE(retry_until,?) WHERE id=?",
                (result.get("status", "pending"), utcnow(), result.get("error"), result.get("retry_until"), row["id"]),
            )
            return
        existing = self.db.row("SELECT id FROM media WHERE sha256=? AND id<>?", (result["sha256"], row["id"]))
        with self.db.connect() as conn:
            target_id = int(existing["id"]) if existing else int(row["id"])
            if existing:
                links = conn.execute("SELECT entity_id,version_id,role,discovery_path,position FROM entity_media WHERE media_id=?", (row["id"],)).fetchall()
                for link in links:
                    conn.execute("INSERT OR IGNORE INTO entity_media(entity_id,version_id,media_id,role,discovery_path,position) VALUES(?,?,?,?,?,?)", (link[0], link[1], target_id, link[2], link[3], link[4]))
                conn.execute("DELETE FROM entity_media WHERE media_id=?", (row["id"],))
                conn.execute("DELETE FROM media WHERE id=?", (row["id"],))
            else:
                conn.execute(
                    "UPDATE media SET sha256=?,mime_type=?,size_bytes=?,path=?,status='ready',last_attempt_at=?,error=NULL WHERE id=?",
                    (result["sha256"], result.get("mime_type"), result.get("size_bytes"), result.get("path"), utcnow(), row["id"]),
                )
            event = conn.execute(
                """SELECT DISTINCT ev.id FROM events ev JOIN entity_media em ON em.entity_id=ev.entity_id
                WHERE em.media_id=? AND ev.notified_at IS NOT NULL ORDER BY ev.id DESC LIMIT 1""",
                (target_id,),
            ).fetchone()
            if event:
                payload = json.dumps({"path": result["path"], "mime_type": result.get("mime_type"), "caption": "延後補抓的媒體"}, ensure_ascii=False)
                conn.execute("INSERT OR IGNORE INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at) VALUES(?,?,?,?,?)", (event[0], "media", payload, utcnow(), utcnow()))
