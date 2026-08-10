from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .apify import ActorResult, ApifyGateway, MonthlyUsage
from .brightdata import BrightDataError, BrightDataGateway
from .config import Settings, actor_input, load_settings
from .db import Database, utcnow
from .facebook_browser import (
    FacebookBrowserChallengeRequired,
    FacebookBrowserError,
    FacebookBrowserGateway,
    FacebookBrowserLoginRequired,
    is_facebook_ui_heading,
)
from .ingest import Ingester, external_id, is_placeholder_profile_name, monitored_projection
from .media import MediaStore, extract_media
from .normalize import content_hash, facebook_post_identity, normalize_url
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


class BudgetExceeded(RuntimeError):
    def __init__(self, message: str, resume_at: datetime | None = None):
        super().__init__(message)
        self.resume_at = resume_at


class ApifyFrozen(RuntimeError):
    """The selected profile has explicitly disabled all Apify work."""


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

    async def start(self) -> None:
        self._seed_browser_name_repair()
        self._seed_historical_name_repair()
        self._purge_duplicate_profile_previews()
        self._seed_notification_hygiene()
        self._seed_profile_pic_migration()
        self._seed_upgrade_repair()
        self._seed_initial_jobs()
        await asyncio.gather(
            self._scheduler_loop(), self._outbox_loop(), self._health_loop(),
            self._media_retry_loop(), self._daily_media_dedupe_loop(),
            self._storage_snapshot_loop(),
        )

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
                self._reload_config_if_changed()
                self._enqueue_due_visits()
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

    async def _run_next_job(self) -> None:
        job = self.db.row("SELECT * FROM jobs WHERE status='pending' AND available_at<=? ORDER BY priority,id LIMIT 1", (utcnow(),))
        if not job:
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
                self.db.execute("UPDATE jobs SET available_at=? WHERE id=?", (earliest.isoformat(), job["id"]))
                return
        self.db.execute("UPDATE jobs SET status='running',started_at=?,attempts=attempts+1 WHERE id=?", (utcnow(), job["id"]))
        try:
            if job["job_type"] == "visit":
                await self.visit_profile(int(job["profile_id"]))
            elif job["job_type"] == "browser_visit":
                await self.browser_visit_profile(int(job["profile_id"]))
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
            self.db.execute("UPDATE jobs SET status='done',finished_at=?,error=NULL WHERE id=?", (utcnow(), job["id"]))
        except ApifyFrozen as exc:
            self.db.execute("UPDATE jobs SET status='skipped_apify_frozen',finished_at=?,error=? WHERE id=?", (utcnow(), str(exc), job["id"]))
            if job["job_type"] == "visit":
                self._schedule_next(int(job["profile_id"]))
        except BudgetExceeded as exc:
            resume = exc.resume_at or self._next_month()
            self.db.execute("UPDATE jobs SET status='deferred_budget',finished_at=?,error=? WHERE id=?", (utcnow(), str(exc), job["id"]))
            if job["job_type"] == "visit":
                self.db.execute("UPDATE profiles SET next_visit_at=? WHERE id=?", (resume.isoformat(), job["profile_id"]))
            else:
                self._enqueue(int(job["profile_id"]), job["job_type"], int(job["priority"]), resume, json.loads(job["payload_json"]))
        except Exception as exc:
            self.db.execute("UPDATE jobs SET status='failed',finished_at=?,error=? WHERE id=?", (utcnow(), str(exc)[:2000], job["id"]))
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
        attempted_at = utcnow()
        self.db.execute(
            "UPDATE profiles SET last_attempt_at=?,browser_canary_last_attempt_at=? WHERE id=?",
            (attempted_at, attempted_at, profile_id),
        )
        item = await self.facebook_browser.profile(str(profile["url"]), str(profile_id))
        await self._store_profile_details(profile, item)
        posts = await self.facebook_browser.canary_posts(str(profile["url"]), str(profile_id))
        await self._ingest_browser_canary_posts(profile_id, posts, notify=True)
        display = item.get("name") or profile.get("display_name") or profile.get("name") or "Facebook"
        self.db.add_event(
            f"browser-manual:{profile_id}:{utcnow()}",
            "browser_manual_visit",
            {
                "title": f"{display} 瀏覽器拜訪完成",
                "text": f"已更新個人資料、擷取畫面，並處理 {len(posts)} 篇具永久連結的貼文。",
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
        if profile_id is not None:
            profile = self.db.row("SELECT apify_frozen FROM profiles WHERE id=?", (profile_id,))
            if profile and profile.get("apify_frozen"):
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
        if profile.get("public_state") != "public":
            self._schedule_next(profile_id)
            return
        initial = not bool(self.db.row("SELECT id FROM entities WHERE profile_id=? AND kind='post' LIMIT 1", (profile_id,)))
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
        summary_error = actor_summary_error(posts.summary)
        if not posts.items:
            if summary_error and not self.settings.browser_canary_enabled:
                raise RuntimeError(f"貼文 Actor 三種輸入格式均失敗：{summary_error}")
        if initial and posts.items:
            self.db.execute("UPDATE profiles SET backfill_done=0,backfill_cursor=NULL,last_full_audit_at=NULL WHERE id=?", (profile_id,))
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
        if post_urls and self._remaining_budget() > 0:
            try:
                await self._fetch_comments(profile_id, post_urls, notify=not initial)
            except (ApifyFrozen, BudgetExceeded):
                pass
        if not profile["backfill_done"] and not self.db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='backfill' AND status IN ('pending','running')", (profile_id,)):
            self._enqueue(profile_id, "backfill", 30, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
        elif profile["backfill_done"]:
            last_audit = datetime.fromisoformat(profile["last_full_audit_at"]) if profile.get("last_full_audit_at") else None
            if (not last_audit or datetime.now(UTC) - last_audit >= timedelta(days=self.settings.full_audit_days)) and not self.db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='audit' AND status IN ('pending','running')", (profile_id,)):
                self._enqueue(profile_id, "audit", 40, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
        self._schedule_next(profile_id)

    async def _fetch_regular_posts(self, profile: dict[str, Any], initial: bool) -> ActorResult:
        """Probe one latest post before paying for the normal ten-post batch."""
        always_full = str(profile.get("url") or "").rstrip("/") in {
            str(url).rstrip("/") for url in self.settings.always_full_fetch_urls
        }
        if initial or not profile.get("backfill_done"):
            return await self._fetch_posts(profile, self.settings.recent_posts)
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
        except (FacebookBrowserChallengeRequired, FacebookBrowserLoginRequired, FacebookBrowserError) as exc:
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
    ) -> None:
        existing = self.db.rows(
            "SELECT external_id,source_url FROM entities WHERE profile_id=? AND kind='post' AND source_url IS NOT NULL",
            (profile_id,),
        )
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
        for raw_item in items:
            item = dict(raw_item)
            source_url = str(item.get("source_url") or "")
            identity = facebook_post_identity(source_url)
            known_id = (ids_by_identity.get(identity) if identity else None) or (ids_by_url.get(normalize_url(source_url)) if source_url else None)
            if known_id:
                item["source_post_id"] = known_id
            await self.ingester.ingest(profile_id, "post", item, notify=notify)

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
        if browser_source and not is_placeholder_profile_name(existing_name):
            item["name"] = existing_name
        previous_state = str(profile.get("public_state") or "unknown")
        state = "private" if bool(item.get("private") or item.get("is_private")) else "public"
        configured_id = profile_id_from_url(str(profile["url"]))
        provider_id = str(item.get("id") or "")
        previous_id = str(profile.get("fb_id") or "")
        # SerpApi may return a pfbid token. Keep numeric Facebook IDs from the
        # monitored URL instead; pfbid is not a useful account identifier here.
        fb_id = next((value for value in (configured_id, provider_id, previous_id) if value.isdigit()), "")
        await self.ingester.ingest(int(profile["id"]), "profile", item, notify=previous_state != "unknown")
        self.db.execute(
            """UPDATE profiles SET fb_id=?,public_state=?,profile_details_json=?,serp_last_checked_at=?,
            missing_successes=0,last_success_at=?,consecutive_failures=0,last_error=NULL WHERE id=?""",
            (fb_id, state, json.dumps(item, ensure_ascii=False), utcnow(), utcnow(), profile["id"]),
        )
        display = str(item.get("name") or profile.get("display_name") or profile.get("name"))
        if state == "public" and previous_state not in {"unknown", "public"}:
            self.db.add_event(f"profile:{profile['id']}:opened:{utcnow()}", "profile_opened", {"title": f"{display} 已公開", "source_url": profile["url"]}, int(profile["id"]))
        if state == "private" and previous_state != "private":
            self.db.add_event(f"profile:{profile['id']}:private:{utcnow()[:13]}", "profile_private", {"title": f"{display} 目前為私人帳號", "source_url": profile["url"]}, int(profile["id"]))

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
        post_urls, _ = await self._ingest_apify_posts(
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
        if not pointer and str(coverage).startswith("partial"):
            raise BudgetExceeded(f"完整回溯尚未完成：{coverage}")
        done = not bool(pointer)
        self.db.execute("UPDATE profiles SET backfill_cursor=?,backfill_done=0 WHERE id=?", (pointer, profile_id))
        if post_urls:
            self._enqueue(profile_id, "backfill_comments", 31, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes), {"post_urls": post_urls, "next_cursor": pointer})
        elif not done:
            self._enqueue(profile_id, "backfill", 30, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
        elif done:
            counts = self.db.row("SELECT SUM(kind='post') posts,SUM(kind='comment') comments FROM entities WHERE profile_id=?", (profile_id,)) or {}
            self.db.add_event(f"profile:{profile_id}:backfill_complete", "backfill_complete", {"title": f"{profile['name']} 初始完整回溯完成", "text": f"已保存 {counts.get('posts') or 0} 篇貼文、{counts.get('comments') or 0} 則留言。", "source_url": profile["url"]}, profile_id)

    async def backfill_comments(self, profile_id: int, payload: dict[str, Any]) -> None:
        profile = self.db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile or profile["public_state"] != "public":
            return
        if profile.get("apify_frozen"):
            return
        post_urls = [str(url) for url in payload.get("post_urls") or [] if url]
        if post_urls and self._remaining_budget() < PRICES["comments"]:
            raise BudgetExceeded("貼文已完成；留言等待 Apify 額度恢復", self._next_month())
        if post_urls:
            await self._fetch_comments(profile_id, post_urls, notify=False)
        pointer = payload.get("next_cursor")
        if pointer:
            self._enqueue(profile_id, "backfill", 30, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
            return
        self.db.execute("UPDATE profiles SET backfill_cursor=NULL,backfill_done=1,last_full_audit_at=? WHERE id=?", (utcnow(), profile_id))
        counts = self.db.row("SELECT SUM(kind='post') posts,SUM(kind='comment') comments FROM entities WHERE profile_id=?", (profile_id,)) or {}
        self.db.add_event(f"profile:{profile_id}:backfill_complete", "backfill_complete", {"title": f"{profile['name']} 回溯完成", "text": f"已保存 {counts.get('posts') or 0} 篇貼文、{counts.get('comments') or 0} 則留言。", "source_url": profile["url"]}, profile_id)

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
        if not pointer and str(coverage).startswith("partial"):
            raise BudgetExceeded(f"完整核對尚未完成：{coverage}")
        if pointer:
            self.db.execute("UPDATE profiles SET audit_cursor=?,audit_token=? WHERE id=?", (pointer, token, profile_id))
            if self._remaining_budget() > 0:
                self._enqueue(profile_id, "audit", 40, datetime.now(UTC) + timedelta(minutes=self.settings.spacing_max_minutes))
            return
        seen = {row["external_id"] for row in self.db.rows("SELECT external_id FROM audit_seen WHERE profile_id=? AND audit_token=? AND kind='post'", (profile_id, token))}
        self.ingester.reconcile(profile_id, "post", seen, None, notify=True)
        self.db.execute("DELETE FROM audit_seen WHERE profile_id=? AND audit_token=?", (profile_id, token))
        self.db.execute("UPDATE profiles SET audit_cursor=NULL,audit_token=NULL,last_full_audit_at=? WHERE id=?", (utcnow(), profile_id))

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

    def _dedupe_existing_media(self) -> dict[str, int]:
        counts = {"checked": 0, "merged": 0, "orphaned": 0, "errors": 0}
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
                existing = self.db.row("SELECT * FROM media WHERE sha256=? AND id<>?", (sha, row["id"]))
                if not existing and perceptual:
                    for candidate in self.db.rows("SELECT * FROM media WHERE id<>? AND perceptual_hash IS NOT NULL AND status='ready'", (row["id"],)):
                        try:
                            if (int(perceptual, 16) ^ int(candidate["perceptual_hash"], 16)).bit_count() <= 6:
                                existing = candidate
                                break
                        except ValueError:
                            continue
                with self.db.connect() as conn:
                    if existing:
                        links = conn.execute("SELECT entity_id,version_id,role,discovery_path,position FROM entity_media WHERE media_id=?", (row["id"],)).fetchall()
                        for link in links:
                            conn.execute("INSERT OR IGNORE INTO entity_media(entity_id,version_id,media_id,role,discovery_path,position) VALUES(?,?,?,?,?,?)", (link[0], link[1], existing["id"], link[2], link[3], link[4]))
                        conn.execute("DELETE FROM entity_media WHERE media_id=?", (row["id"],))
                        conn.execute("DELETE FROM media WHERE id=?", (row["id"],))
                        counts["merged"] += 1
                    else:
                        conn.execute("UPDATE media SET sha256=?,perceptual_hash=?,size_bytes=? WHERE id=?", (sha, perceptual, path.stat().st_size, row["id"]))
                if existing and path != Path(existing.get("path") or "") and path.is_file():
                    path.unlink()
            except Exception:
                counts["errors"] += 1
        for row in self.db.rows("SELECT m.* FROM media m LEFT JOIN entity_media em ON em.media_id=m.id WHERE em.media_id IS NULL"):
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
