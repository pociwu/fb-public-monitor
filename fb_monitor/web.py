from __future__ import annotations

import asyncio
import json
import os
import threading
from datetime import UTC, datetime, timedelta
from difflib import unified_diff
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from PIL import Image, ImageOps, UnidentifiedImageError

from .config import MAX_PROFILES, Settings, add_profile_to_config, load_settings, remove_profile_from_config
from .db import Database, utcnow
from .ingest import is_placeholder_profile_name
from .normalize import normalize_url
from .service import MonitorService
from .serpapi import profile_id_from_url
from .storage import decorate_snapshot
from .timeutil import display_time, parse_time, timezone_module_fallback

PACKAGE = Path(__file__).parent
templates = Jinja2Templates(directory=str(PACKAGE / "templates"))
MANUAL_VISIT_COOLDOWN_MINUTES = 10


def _web_origin(value: str) -> tuple[str, str, int] | None:
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        return None
    try:
        port = parsed.port or (443 if scheme == "https" else 80)
    except ValueError:
        return None
    return scheme, parsed.hostname.lower(), port


def _require_paid_action_same_origin(request: Request) -> None:
    """Reject browser cross-site POSTs while preserving headerless CLI calls.

    Browsers send Origin and/or Referer for form submissions.  A script or curl
    invocation may send neither, so absence is intentionally allowed; any
    supplied browser provenance must match the effective request origin.
    """
    expected = _web_origin(str(request.base_url))
    for header in ("origin", "referer"):
        value = request.headers.get(header)
        supplied = _web_origin(value) if value else None
        if value and (supplied is None or supplied != expected):
            raise HTTPException(403, "付費操作拒絕跨來源請求")


def _json(value: str | None) -> Any:
    try:
        return json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}


def _similar_perceptual_hash(left: object, right: object, max_distance: int = 6) -> bool:
    if not left or not right:
        return False
    try:
        return (int(str(left), 16) ^ int(str(right), 16)).bit_count() <= max_distance
    except ValueError:
        return False


def _attach_profile_name_history(db: Database, profile: dict[str, Any]) -> None:
    names: list[str] = []
    details = _json(profile.get("profile_details_json"))
    rejected = {
        str(value).strip()
        for value in details.get("rejected_profile_names", [])
        if str(value).strip()
    }
    rows = db.rows(
        """SELECT v.normalized_json FROM versions v
        JOIN entities e ON e.id=v.entity_id
        WHERE e.profile_id=? AND e.kind='profile'
        ORDER BY v.seen_at DESC,v.id DESC""",
        (profile["id"],),
    )
    for row in rows:
        payload = _json(row.get("normalized_json"))
        name = str(payload.get("authorName") or "").strip()
        if is_placeholder_profile_name(name) or name in names or name in rejected:
            continue
        names.append(name)
    current = str(profile.get("display_name") or "")
    if is_placeholder_profile_name(current) and names:
        profile["display_name"] = names[0]
        current = names[0]
    profile["previous_names"] = [name for name in names if name != current]


def _first_value(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    return next((item[key] for key in keys if item.get(key) not in (None, "", [])), None)


def _entity_content(item: dict[str, Any], kind: str) -> dict[str, str]:
    author = item.get("author")
    author_name = ""
    if isinstance(author, dict):
        author_name = str(author.get("name") or author.get("title") or "")
    elif isinstance(author, str):
        author_name = author
    author_name = author_name or str(_first_value(item, ("authorName", "author_name", "profileName", "source_profile_name")) or "")
    text = str(_first_value(item, ("text", "raw_text", "message", "postText", "description", "caption", "bio", "pageIntro")) or "")
    timestamp = str(_first_value(item, ("publishTime", "created_at", "created_time", "timestamp", "date", "time")) or "")
    if kind == "profile":
        nested = item.get("personalProfile") if isinstance(item.get("personalProfile"), dict) else {}
        title = str(_first_value(nested, ("name", "title", "fullName")) or _first_value(item, ("title", "name", "pageName")) or "個人檔案")
        text = str(_first_value(nested, ("bio", "intro", "description")) or text)
    elif kind == "comment":
        title = author_name or "留言"
    else:
        title = author_name or "貼文"
    return {"title": title, "text": text, "timestamp": timestamp}


def _media_kind(row: dict[str, Any]) -> str:
    mime = str(row.get("mime_type") or "").lower()
    role = str(row.get("role") or "").lower()
    if mime.startswith("video/") or role == "video":
        return "video"
    if mime.startswith("image/") or any(word in role for word in ("image", "photo", "picture", "cover")):
        return "image"
    return "file"


def _has_column(db: Database, table: str, column: str) -> bool:
    """Inspect SQLite without depending on a particular Database release."""
    if table not in {"entity_media"}:
        return False
    return column in {str(row["name"]) for row in db.rows(f"PRAGMA table_info({table})")}


def _attach_browser_capture(profile: dict[str, Any], cfg: Settings) -> None:
    path = cfg.facebook_browser_data_dir / "screenshots" / f"profile-{int(profile['id'])}.png"
    profile["browser_capture_available"] = path.is_file()
    profile["browser_capture_display"] = display_time(path.stat().st_mtime, cfg.timezone) if path.is_file() else ""


def _attach_current_media(db: Database, entities: list[dict[str, Any]]) -> None:
    if not entities:
        return
    placeholders = ",".join("?" for _ in entities)
    has_position = _has_column(db, "entity_media", "position")
    position_select = "em.position" if has_position else "NULL AS position"
    position_order = "COALESCE(em.position,999999)," if has_position else ""
    rows = db.rows(
        f"""SELECT em.entity_id,em.role,em.discovery_path,{position_select},m.*
        FROM entity_media em JOIN media m ON m.id=em.media_id JOIN entities e ON e.id=em.entity_id
        WHERE em.entity_id IN ({placeholders}) AND em.version_id=e.current_version_id
        ORDER BY em.entity_id,{position_order}em.discovery_path,m.id""",
        tuple(entity["id"] for entity in entities),
    )
    grouped: dict[int, list[dict[str, Any]]] = {int(entity["id"]): [] for entity in entities}
    seen: dict[int, set[str]] = {int(entity["id"]): set() for entity in entities}
    for row in rows:
        entity_id = int(row["entity_id"])
        identity = str(row.get("sha256") or row.get("source_url") or row["id"])
        if identity in seen[entity_id]:
            continue
        seen[entity_id].add(identity)
        row["kind"] = _media_kind(row)
        grouped[entity_id].append(row)
    for entity in entities:
        entity["media"] = grouped[int(entity["id"])]
        entity["media_count"] = len(entity["media"])
        entity["image_count"] = sum(media["kind"] == "image" for media in entity["media"])
        entity["video_count"] = sum(media["kind"] == "video" for media in entity["media"])


def _capture_v2_summary(db: Database, enabled: bool) -> dict[str, Any]:
    """Return a compact dashboard summary without mutating capture state."""
    counts = db.row(
        """SELECT
        (SELECT COUNT(*) FROM capture_epochs WHERE is_active=1) AS active_epochs,
        (SELECT COUNT(*) FROM coverage_streams) AS coverage_total,
        (SELECT COUNT(*) FROM coverage_streams WHERE status='complete') AS coverage_complete,
        (SELECT COUNT(*) FROM paid_source_batches
         WHERE status NOT IN ('committed','failed')) AS open_batches"""
    ) or {}
    return {
        "enabled": enabled,
        "active_epochs": int(counts.get("active_epochs") or 0),
        "coverage_total": int(counts.get("coverage_total") or 0),
        "coverage_complete": int(counts.get("coverage_complete") or 0),
        "open_batches": int(counts.get("open_batches") or 0),
    }


def _capture_v2_contracts(
    db: Database,
    service: MonitorService,
    cfg: Settings,
) -> list[dict[str, Any]]:
    """Decorate the latest contract evidence for every configured candidate Actor."""
    actor_ids = list(
        dict.fromkeys(
            actor_id.strip()
            for actor_id in (cfg.actors.posts_v2_primary, cfg.actors.posts_v2_fallback)
            if actor_id.strip()
        )
    )
    if not actor_ids:
        return []
    placeholders = ",".join("?" for _ in actor_ids)
    rows = db.rows(
        f"""SELECT ac.*,
        (SELECT cr.status FROM contract_runs cr WHERE cr.contract_id=ac.id
         ORDER BY cr.id DESC LIMIT 1) AS latest_run_status,
        (SELECT cr.error FROM contract_runs cr WHERE cr.contract_id=ac.id
         ORDER BY cr.id DESC LIMIT 1) AS latest_run_error
        FROM actor_contracts ac
        WHERE ac.provider='apify' AND ac.purpose='posts_backfill'
          AND ac.actor_id IN ({placeholders})
        ORDER BY ac.updated_at DESC,ac.id DESC""",
        tuple(actor_ids),
    )
    grouped: dict[str, list[dict[str, Any]]] = {actor_id: [] for actor_id in actor_ids}
    for row in rows:
        grouped.setdefault(str(row["actor_id"]), []).append(row)
    candidates: list[dict[str, Any]] = []
    for index, actor_id in enumerate(actor_ids):
        expected_fingerprint = service._posts_v2_fingerprint(actor_id)
        matching = next(
            (
                row
                for row in grouped.get(actor_id, [])
                if str(row.get("schema_fingerprint") or "") == expected_fingerprint
            ),
            None,
        )
        row = matching or next(iter(grouped.get(actor_id, [])), None)
        valid = service._valid_posts_v2_contract(actor_id)
        candidate = dict(row or {})
        candidate.update(
            {
                "actor_id": actor_id,
                "role": "主要" if index == 0 else "備援",
                "status": str(candidate.get("status") or "pending"),
                "valid": valid is not None,
                "fingerprint": str(candidate.get("schema_fingerprint") or expected_fingerprint),
                "expected_fingerprint": expected_fingerprint,
                "fingerprint_matches": bool(
                    candidate
                    and str(candidate.get("schema_fingerprint") or "") == expected_fingerprint
                ),
                "updated_display": display_time(candidate.get("updated_at"), cfg.timezone),
            }
        )
        candidates.append(candidate)
    return candidates


def _capture_v2_fixture_eligibility(
    db: Database, profile_id: int
) -> tuple[bool, str]:
    """Require the latest identity-bound strong observation to be public.

    A historical public observation is not enough after a newer anonymous or
    contract-qualified strong observation has confirmed the profile private.
    Weak API hints and indeterminate observations do not replace strong
    evidence, so they are intentionally excluded from this ordering.
    """
    latest = db.row(
        """SELECT verdict,source,auth_scope,observed_at
        FROM access_observations
        WHERE profile_id=? AND identity_match=1
          AND (
            auth_scope='anonymous'
            OR source='contract_explicit'
            OR source LIKE 'contract:%'
          )
        ORDER BY observed_at DESC,id DESC LIMIT 1""",
        (profile_id,),
    )
    if not latest:
        return False, "尚無匿名、身分一致的強存取證據"
    if str(latest.get("verdict") or "") != "confirmed_public":
        return False, f"最新強存取證據為 {latest.get('verdict') or '無法判定'}"
    return True, "可作為已確認公開的貼文游標契約 fixture"


def _decorate_capture_rows(rows: list[dict[str, Any]], cfg: Settings) -> None:
    for row in rows:
        for field in ("created_at", "updated_at", "started_at", "completed_at", "next_job_at"):
            row[f"{field}_display"] = display_time(row.get(field), cfg.timezone)
        if "terminal_evidence_json" in row:
            row["terminal_evidence"] = _json(row.get("terminal_evidence_json"))


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or load_settings()
    templates.env.globals["app_version"] = cfg.app_version
    templates.env.globals["app_updated_display"] = display_time(cfg.app_updated_at, cfg.timezone) if cfg.app_updated_at else ""
    service = MonitorService(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(service.start(), name="monitor-service") if cfg.scheduler_enabled else None
        yield
        service.stop()
        if task:
            await task

    app = FastAPI(title="FB Public Monitor", lifespan=lifespan)
    app.state.settings = cfg
    app.state.db = service.db
    app.state.service = service
    app.mount("/static", StaticFiles(directory=str(PACKAGE / "static")), name="static")

    @app.get("/")
    def dashboard(request: Request, notice: str = "", error: str = ""):
        db: Database = request.app.state.db
        profiles = db.rows("""SELECT p.*,
            (SELECT COUNT(*) FROM entities e WHERE e.profile_id=p.id AND e.kind='post') post_count,
            (SELECT COUNT(*) FROM entities e WHERE e.profile_id=p.id AND e.kind='comment') comment_count,
            (SELECT m.id FROM media m JOIN entity_media em ON em.media_id=m.id JOIN entities e ON e.id=em.entity_id
             WHERE e.profile_id=p.id AND e.kind='profile' AND m.status='ready' AND em.role='profile_picture'
             ORDER BY em.version_id DESC,
               CASE WHEN LOWER(COALESCE(em.discovery_path,'')) LIKE '%large%' THEN 0
                    WHEN LOWER(COALESCE(em.discovery_path,'')) LIKE '%medium%' THEN 1 ELSE 2 END,
               m.id DESC LIMIT 1) avatar_media_id,
            (SELECT m.id FROM media m JOIN entity_media em ON em.media_id=m.id JOIN entities e ON e.id=em.entity_id
             WHERE e.profile_id=p.id AND e.kind='profile' AND m.status='ready' AND em.role='cover_photo'
             ORDER BY em.version_id DESC,m.id DESC LIMIT 1) cover_media_id
            FROM profiles p WHERE p.enabled=1 ORDER BY COALESCE(p.sort_order,p.id),p.id""")
        for profile in profiles:
            _attach_browser_capture(profile, cfg)
            _attach_profile_name_history(db, profile)
            profile["last_success_display"] = display_time(profile.get("last_success_at"), cfg.timezone)
            profile["next_visit_display"] = display_time(profile.get("next_visit_at"), cfg.timezone)
            profile["manual_available_at"] = ""
            if profile.get("last_manual_visit_at"):
                try:
                    manual_at = parse_time(profile["last_manual_visit_at"])
                    available_at = manual_at + timedelta(minutes=MANUAL_VISIT_COOLDOWN_MINUTES) if manual_at else None
                    if available_at and available_at > datetime.now(available_at.tzinfo):
                        profile["manual_available_at"] = available_at.isoformat()
                except (TypeError, ValueError):
                    pass
            profile["browser_manual_available_at"] = ""
            browser_job = db.row(
                "SELECT created_at,status FROM jobs WHERE profile_id=? AND job_type='browser_visit' ORDER BY id DESC LIMIT 1",
                (profile["id"],),
            )
            if browser_job:
                try:
                    created_at = parse_time(browser_job.get("created_at"))
                    available_at = created_at + timedelta(minutes=MANUAL_VISIT_COOLDOWN_MINUTES) if created_at else None
                    if browser_job.get("status") in {"pending", "running"} and available_at:
                        available_at = max(available_at, datetime.now(available_at.tzinfo) + timedelta(minutes=1))
                    if available_at and available_at > datetime.now(available_at.tzinfo):
                        profile["browser_manual_available_at"] = available_at.isoformat()
                except (TypeError, ValueError):
                    pass
            configured_id = profile_id_from_url(str(profile["url"]))
            stored_id = str(profile.get("fb_id") or "")
            profile["display_fb_id"] = next((value for value in (configured_id, stored_id) if value.isdigit()), "")
            profile["details"] = _json(profile.get("profile_details_json"))
            details = profile["details"]
            profile["work_labels"] = [str(item.get("title") or item.get("name")) for item in details.get("works", []) if isinstance(item, dict) and (item.get("title") or item.get("name"))]
            profile["education_labels"] = [str(item.get("title") or item.get("name")) for item in details.get("educations", []) if isinstance(item, dict) and (item.get("title") or item.get("name"))]
            for section in details.get("about_details", []):
                if not isinstance(section, dict):
                    continue
                labels = [str(item.get("title") or item.get("name")) for item in section.get("items", []) if isinstance(item, dict) and (item.get("title") or item.get("name"))]
                if section.get("section_type") == "work":
                    profile["work_labels"].extend(labels)
                elif section.get("section_type") in {"college", "secondary_school", "education"}:
                    profile["education_labels"].extend(labels)
            excluded_assets: set[str] = set()
            for media_id in (profile.get("avatar_media_id"), profile.get("cover_media_id")):
                if media_id and (media_row := db.row("SELECT source_url FROM media WHERE id=?", (media_id,))):
                    excluded_assets.add(normalize_url(str(media_row["source_url"])))
            public_photo_rows = db.rows(
                """SELECT DISTINCT m.id,m.source_url,m.perceptual_hash,m.size_bytes FROM media m JOIN entity_media em ON em.media_id=m.id
                JOIN entities e ON e.id=em.entity_id WHERE e.profile_id=? AND e.kind='profile'
                AND em.version_id=e.current_version_id
                AND m.status='ready' AND em.role='image'
                ORDER BY COALESCE(m.size_bytes,0) DESC,m.id DESC LIMIT 64""",
                (profile["id"],),
            )
            public_photo_ids: list[int] = []
            public_assets: set[str] = set()
            public_hashes: list[str] = []
            for row in public_photo_rows:
                asset = normalize_url(str(row["source_url"]))
                perceptual_hash = str(row.get("perceptual_hash") or "")
                if (
                    asset in excluded_assets
                    or asset in public_assets
                    or any(_similar_perceptual_hash(perceptual_hash, known) for known in public_hashes)
                ):
                    continue
                public_photo_ids.append(int(row["id"]))
                if asset:
                    public_assets.add(asset)
                if perceptual_hash:
                    public_hashes.append(perceptual_hash)
                if len(public_photo_ids) == 4:
                    break
            profile["public_photo_ids"] = public_photo_ids
        usage = db.rows("SELECT * FROM usage ORDER BY month DESC,category")
        pending = db.row("SELECT COUNT(*) count FROM jobs WHERE status IN ('pending','running')")
        outbox = db.row("SELECT COUNT(*) count FROM outbox WHERE status='pending'")
        outbox_counts = {row["status"]: row["count"] for row in db.rows("SELECT status,COUNT(*) count FROM outbox GROUP BY status")}
        outbox_rows = db.rows("""SELECT o.*,e.payload_json event_payload,COALESCE(p.display_name,p.name,'系統') profile_name
            FROM outbox o LEFT JOIN events e ON e.id=o.event_id LEFT JOIN profiles p ON p.id=e.profile_id
            WHERE o.status IN ('pending','failed','cancelled') ORDER BY o.id DESC LIMIT 100""")
        for row in outbox_rows:
            payload = _json(row.get("event_payload")) or _json(row.get("payload_json"))
            row["summary"] = str(payload.get("title") or payload.get("text") or row["kind"])
            row["created_display"] = display_time(row.get("created_at"), cfg.timezone)
            row["next_display"] = display_time(row.get("next_attempt_at"), cfg.timezone)
        maintenance_runs = db.rows("SELECT * FROM maintenance_runs ORDER BY id DESC LIMIT 20")
        for row in maintenance_runs:
            row["summary"] = _json(row.get("summary_json"))
            row["started_display"] = display_time(row.get("started_at"), cfg.timezone)
            row["finished_display"] = display_time(row.get("finished_at"), cfg.timezone)
        media = db.row("SELECT COUNT(*) total,SUM(CASE WHEN status='ready' THEN 1 ELSE 0 END) ready FROM media")
        monitored = db.row("SELECT COUNT(*) count FROM profiles WHERE enabled=1")
        storage_rows = db.rows("SELECT * FROM storage_snapshots ORDER BY snapshot_date DESC LIMIT 2")
        storage_latest = decorate_snapshot(storage_rows[0], storage_rows[1] if len(storage_rows) > 1 else None) if storage_rows else None
        official_usage = db.apify_usage_snapshot()
        serpapi_usage = db.serpapi_usage_snapshot()
        capture_v2 = _capture_v2_summary(db, cfg.capture_v2_enabled)
        if official_usage:
            official_usage["cycle_start_display"] = display_time(official_usage.get("cycle_start_at"), cfg.timezone)
            official_usage["cycle_end_display"] = display_time(official_usage.get("cycle_end_at"), cfg.timezone)
            official_usage["fetched_display"] = display_time(official_usage.get("fetched_at"), cfg.timezone)
        return templates.TemplateResponse(request, "dashboard.html", {"profiles": profiles, "usage": usage, "official_usage": official_usage, "serpapi_usage": serpapi_usage, "pending": pending, "outbox": outbox, "outbox_counts": outbox_counts, "outbox_rows": outbox_rows, "maintenance_runs": maintenance_runs, "media": media, "storage_latest": storage_latest, "budget": cfg.monthly_budget_usd, "monitored": monitored, "max_profiles": MAX_PROFILES, "browser_enabled": cfg.facebook_browser_enabled, "capture_v2": capture_v2, "notice": notice, "error": error})

    @app.get("/storage")
    def storage_detail(request: Request):
        db: Database = request.app.state.db
        try:
            timezone = ZoneInfo(cfg.timezone)
        except ZoneInfoNotFoundError:
            timezone = timezone_module_fallback(cfg.timezone)
        today = datetime.now(timezone).date().isoformat()
        if not db.row("SELECT snapshot_date FROM storage_snapshots WHERE snapshot_date=?", (today,)):
            request.app.state.service.capture_storage_snapshot(today)
        rows = db.rows("SELECT * FROM storage_snapshots ORDER BY snapshot_date DESC LIMIT 31")
        history = [
            decorate_snapshot(row, rows[index + 1] if index + 1 < len(rows) else None)
            for index, row in enumerate(rows[:30])
        ]
        return templates.TemplateResponse(
            request,
            "storage.html",
            {"current": history[0] if history else None, "history": history},
        )

    @app.get("/capture-v2")
    def capture_v2_status(
        request: Request,
        profile_id: int | None = Query(None, ge=1),
        notice: str = "",
        error: str = "",
    ):
        db: Database = request.app.state.db
        selected_profile = None
        if profile_id is not None:
            selected_profile = db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
            if not selected_profile:
                raise HTTPException(404)

        contracts = _capture_v2_contracts(db, service, cfg)
        contract_grant = db.contract_test_grant_ledger()
        if contract_grant:
            for field in ("created_at", "expires_at", "closed_at"):
                contract_grant[f"{field}_display"] = display_time(
                    contract_grant.get(field), cfg.timezone
                )
            for allocation in contract_grant.get("allocations", []):
                allocation["created_at_display"] = display_time(
                    allocation.get("created_at"), cfg.timezone
                )
        profiles = db.rows(
            "SELECT id,name,display_name,url,fb_id,enabled,apify_frozen FROM profiles WHERE enabled=1 ORDER BY COALESCE(sort_order,id),id"
        )
        for profile in profiles:
            profile["apify_frozen"] = db.profile_source_frozen(int(profile["id"]), "apify")
            eligible, reason = _capture_v2_fixture_eligibility(
                db, int(profile["id"])
            )
            profile["contract_eligible"] = not profile["apify_frozen"] and eligible
            profile["contract_eligibility_reason"] = (
                "Apify 已凍結" if profile["apify_frozen"] else reason
            )

        special = db.row(
            """SELECT * FROM profiles WHERE enabled=1 AND
            (fb_id=? OR url LIKE ? OR name=? OR name=?) ORDER BY id LIMIT 1""",
            (
                cfg.special_profile_id,
                f"%{cfg.special_profile_id}%",
                cfg.special_profile_id,
                f"FB-{cfg.special_profile_id}",
            ),
        )
        special_epoch = None
        if special:
            special_epoch = db.row(
                """SELECT ce.* FROM capture_epochs ce WHERE ce.profile_id=?
                ORDER BY ce.is_active DESC,ce.id DESC LIMIT 1""",
                (special["id"],),
            )
            if special_epoch:
                _decorate_capture_rows([special_epoch], cfg)

        coverage_where = "WHERE ce.profile_id=?" if profile_id is not None else ""
        coverage_params: tuple[Any, ...] = (profile_id,) if profile_id is not None else ()
        coverage = db.rows(
            f"""SELECT cs.*,ce.profile_id,ce.status AS epoch_status,ce.is_active,
            COALESCE(p.display_name,p.name) AS profile_name,p.url AS profile_url,
            ac.actor_id AS contract_actor_id,ac.schema_fingerprint AS contract_fingerprint
            FROM coverage_streams cs
            JOIN capture_epochs ce ON ce.id=cs.epoch_id
            JOIN profiles p ON p.id=ce.profile_id
            LEFT JOIN actor_contracts ac ON ac.id=cs.contract_id
            {coverage_where}
            ORDER BY ce.is_active DESC,ce.id DESC,cs.id""",
            coverage_params,
        )
        _decorate_capture_rows(coverage, cfg)

        batch_where = "WHERE pb.profile_id=?" if profile_id is not None else ""
        batches = db.rows(
            f"""SELECT pb.*,COALESCE(p.display_name,p.name) AS profile_name,
            cs.stream,cs.surface
            FROM paid_source_batches pb
            JOIN profiles p ON p.id=pb.profile_id
            JOIN coverage_streams cs ON cs.id=pb.coverage_stream_id
            {batch_where}
            ORDER BY pb.id DESC LIMIT 200""",
            coverage_params,
        )
        _decorate_capture_rows(batches, cfg)

        v1_jobs = db.row(
            """SELECT
            SUM(CASE WHEN status IN ('pending','running') THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status='paused_contract' THEN 1 ELSE 0 END) AS paused
            FROM jobs WHERE job_type IN ('backfill','backfill_comments','audit')"""
        ) or {}
        return templates.TemplateResponse(
            request,
            "capture_v2.html",
            {
                "enabled": cfg.capture_v2_enabled,
                "v1_backfill_enabled": cfg.apify_v1_backfill_enabled,
                "v1_jobs": v1_jobs,
                "contracts": contracts,
                "profiles": profiles,
                "selected_profile": selected_profile,
                "special_profile": special,
                "special_epoch": special_epoch,
                "coverage": coverage,
                "batches": batches,
                "contract_budget": cfg.actor_contract_test_budget_usd,
                "contract_grant": contract_grant,
                "notice": notice,
                "error": error,
            },
        )

    @app.post("/capture-v2/contract-grants")
    def create_capture_v2_contract_grant(request: Request):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        redirect = "/capture-v2"
        if not cfg.capture_v2_enabled:
            return RedirectResponse(
                url=f"{redirect}?error={quote('Capture V2 功能尚未啟用')}", status_code=303
            )
        if any(contract.get("valid") for contract in _capture_v2_contracts(db, service, cfg)):
            return RedirectResponse(
                url=f"{redirect}?error={quote('當前貼文游標契約已通過，不需要再新增付費測試輪次')}",
                status_code=303,
            )
        try:
            grant = db.create_contract_test_grant(
                max_usd=cfg.actor_contract_test_budget_usd,
                valid_hours=cfg.actor_contract_test_grant_hours,
                authorized_by="web_same_origin",
                note="operator approved posts cursor contract round",
            )
        except ValueError as exc:
            return RedirectResponse(
                url=f"{redirect}?error={quote(str(exc))}", status_code=303
            )
        return RedirectResponse(
            url=(
                f"{redirect}?notice="
                + quote(
                    f"已核准第 {grant['id']} 輪貼文游標契約測試；"
                    f"全輪共用上限 ${float(grant['max_usd']):.2f}"
                )
            ),
            status_code=303,
        )

    @app.post("/capture-v2/contract-grants/{grant_id}/close")
    def close_capture_v2_contract_grant(request: Request, grant_id: int):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        try:
            db.close_contract_test_grant(grant_id)
        except ValueError as exc:
            return RedirectResponse(
                url=f"/capture-v2?error={quote(str(exc))}", status_code=303
            )
        return RedirectResponse(
            url=f"/capture-v2?notice={quote(f'已結束第 {grant_id} 輪契約測試')}",
            status_code=303,
        )

    @app.post("/profiles/{profile_id}/capture-v2/contract-test")
    async def queue_capture_v2_contract_test(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        redirect = f"/capture-v2?profile_id={profile_id}"
        if not cfg.capture_v2_enabled:
            return RedirectResponse(
                url=f"{redirect}&error={quote('Capture V2 功能尚未啟用')}", status_code=303
            )
        db: Database = request.app.state.db
        profile = db.row(
            "SELECT id,display_name,name FROM profiles WHERE id=? AND enabled=1", (profile_id,)
        )
        if not profile:
            raise HTTPException(404)
        if db.profile_source_frozen(profile_id, "apify"):
            return RedirectResponse(
                url=f"{redirect}&error={quote('此帳號的 Apify 已凍結，未建立契約測試工作')}",
                status_code=303,
            )
        eligible, _ = _capture_v2_fixture_eligibility(db, profile_id)
        if not eligible:
            return RedirectResponse(
                url=f"{redirect}&error={quote('契約 fixture 尚無匿名、身分一致的已公開證據')}",
                status_code=303,
            )
        params = parse_qs((await request.body()).decode("utf-8"))
        if str((params.get("fixture_ack") or [""])[0]) != "1":
            return RedirectResponse(
                url=f"{redirect}&error={quote('請先確認 fixture 有足夠的公開歷史貼文，且本測試只驗證貼文游標')}",
                status_code=303,
            )
        actor_id = str((params.get("actor_id") or [cfg.actors.posts_v2_primary])[0]).strip()
        allowed_actors = {
            actor.strip()
            for actor in (cfg.actors.posts_v2_primary, cfg.actors.posts_v2_fallback)
            if actor.strip()
        }
        if actor_id not in allowed_actors:
            return RedirectResponse(
                url=f"{redirect}&error={quote('Actor 不在 Capture V2 候選名單中')}",
                status_code=303,
            )
        if any(service._valid_posts_v2_contract(candidate) for candidate in allowed_actors):
            return RedirectResponse(
                url=f"{redirect}&error={quote('當前貼文游標契約已通過，未再建立付費測試')}",
                status_code=303,
            )
        grant = db.contract_test_grant_ledger()
        if not grant or str(grant.get("status")) != "active":
            return RedirectResponse(
                url=f"{redirect}&error={quote('尚無有效的全輪 $0.20 契約測試 grant；請先明確核准新輪次')}",
                status_code=303,
            )
        fingerprint = service._posts_v2_fingerprint(actor_id)
        try:
            _, created, allocation = db.queue_contract_test_job(
                grant_id=int(grant["id"]),
                profile_id=profile_id,
                actor_id=actor_id,
                schema_fingerprint=fingerprint,
                fixture_ack=True,
            )
        except ValueError as exc:
            return RedirectResponse(
                url=f"{redirect}&error={quote(str(exc))}", status_code=303
            )
        label = profile.get("display_name") or profile.get("name") or "Facebook"
        message = (
            f"已排入 {label} 的 {actor_id} 貼文游標契約測試"
            f"（本輪剩餘核准 ${float(allocation['authorized_usd']):.4f}）"
            if created
            else f"{label} 的 {actor_id} 契約測試已在佇列中"
        )
        return RedirectResponse(url=f"{redirect}&notice={quote(message)}", status_code=303)

    @app.post("/profiles/{profile_id}/capture-v2/continue")
    def queue_capture_v2_continue(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        redirect = f"/capture-v2?profile_id={profile_id}"
        if not cfg.capture_v2_enabled:
            return RedirectResponse(
                url=f"{redirect}&error={quote('Capture V2 功能尚未啟用')}", status_code=303
            )
        db: Database = request.app.state.db
        profile = db.row("SELECT * FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            raise HTTPException(404)
        if db.profile_source_frozen(profile_id, "apify"):
            return RedirectResponse(
                url=f"{redirect}&error={quote('此帳號的 Apify 已凍結，未建立續抓工作')}",
                status_code=303,
            )
        eligible, _ = _capture_v2_fixture_eligibility(db, profile_id)
        if not eligible:
            return RedirectResponse(
                url=f"{redirect}&error={quote('尚無匿名、身分一致的已公開證據；禁止建立付費回溯工作')}",
                status_code=303,
            )
        actor_ids = list(
            dict.fromkeys(
                actor.strip()
                for actor in (cfg.actors.posts_v2_primary, cfg.actors.posts_v2_fallback)
                if actor.strip()
            )
        )
        contract = None
        for actor_id in actor_ids:
            contract = service._valid_posts_v2_contract(actor_id)
            if contract is not None:
                break
        if not contract:
            return RedirectResponse(
                url=f"{redirect}&error={quote('沒有符合目前程式指紋且已通過的貼文 Actor 契約，請先執行契約測試')}",
                status_code=303,
            )
        actor_id = str(contract["actor_id"])
        special = (
            str(profile.get("fb_id") or "") == cfg.special_profile_id
            or cfg.special_profile_id in str(profile.get("url") or "")
        )
        priority = -300 if special else -100
        epoch, _ = db.get_or_create_capture_epoch(
            profile_id,
            "manual_continue",
            status="ready",
            priority=priority,
            scope={
                "all_public_history": True,
                "capture_intent": "manual_continue",
                "surfaces": ["timeline_posts"],
                "manual": True,
            },
            reserved_budget_usd=cfg.special_capture_reserve_usd if special else 0,
        )
        db.execute(
            "UPDATE capture_epochs SET status='ready',updated_at=? WHERE id=? AND status='awaiting_contract'",
            (utcnow(), epoch["id"]),
        )
        coverage_stream = db.upsert_coverage_stream(
            int(epoch["id"]),
            stream="posts",
            surface="timeline_posts",
            provider="apify",
            contract_id=int(contract["id"]),
            status="pending",
        )
        if (
            str(coverage_stream.get("status") or "") == "pending"
            and not (
                coverage_stream.get("input_cursor")
                or coverage_stream.get("output_cursor")
            )
        ):
            recovery = service._capture_v2_recovery_checkpoint(
                profile_id,
                int(contract["id"]),
            )
            if recovery and str(recovery.get("output_cursor") or "").strip():
                resume_cursor = str(recovery["output_cursor"])
                db.update_coverage_stream(
                    int(coverage_stream["id"]),
                    input_cursor=resume_cursor,
                    output_cursor=resume_cursor,
                    provider_checkpoint_json={
                        "resumed_from_coverage_id": int(recovery["id"]),
                        "resumed_from_cursor": resume_cursor,
                    },
                )
                coverage_stream = db.row(
                    "SELECT * FROM coverage_streams WHERE id=?",
                    (coverage_stream["id"],),
                ) or coverage_stream
        if str(coverage_stream.get("status") or "") == "complete":
            return RedirectResponse(
                url=f"{redirect}&notice={quote('此回溯範圍已有終點證據並標記完成，未重複排付費工作')}",
                status_code=303,
            )
        fingerprint = str(contract.get("schema_fingerprint") or "")
        _, created = db.queue_unique_job(
            profile_id=profile_id,
            job_type="capture_posts_v2",
            priority=priority,
            dedupe_key=(
                f"capture-v2:continue:{profile_id}:{epoch['id']}:{coverage_stream['id']}:{fingerprint}"
            ),
            payload={
                "epoch_id": int(epoch["id"]),
                "coverage_stream_id": int(coverage_stream["id"]),
                "surface": "timeline_posts",
            },
            epoch_id=int(epoch["id"]),
        )
        label = profile.get("display_name") or profile.get("name") or "Facebook"
        message = (
            f"已排入 {label} 的 Capture V2 貼文續抓"
            if created
            else f"{label} 的 Capture V2 貼文續抓已在佇列中"
        )
        return RedirectResponse(url=f"{redirect}&notice={quote(message)}", status_code=303)

    @app.post("/profiles")
    async def add_profile(request: Request):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        params = parse_qs((await request.body()).decode("utf-8"))
        submitted_url = (params.get("url") or [""])[0]
        try:
            added = add_profile_to_config(cfg.config_path, submitted_url)
            refreshed = load_settings(cfg.config_path)
            db.sync_profiles(refreshed.profiles)
            profile = db.row("SELECT id FROM profiles WHERE url=?", (added.url,))
            if not profile:
                raise RuntimeError("新增後找不到監控帳號")
            db.queue_profile_visits([int(profile["id"])])
        except (OSError, RuntimeError, ValueError) as exc:
            return RedirectResponse(url=f"/?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(url=f"/?notice={quote('已新增並排程驗證：' + added.url)}", status_code=303)

    @app.post("/profiles/reorder")
    async def reorder_profiles(request: Request):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        payload = await request.json()
        profile_ids = payload.get("profile_ids") if isinstance(payload, dict) else None
        if not isinstance(profile_ids, list) or not all(isinstance(profile_id, int) and not isinstance(profile_id, bool) for profile_id in profile_ids):
            raise HTTPException(400, "profile_ids 必須是整數陣列")
        try:
            db.reorder_profiles(profile_ids)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"ok": True}

    @app.post("/profiles/scan-all")
    def scan_all_profiles(request: Request):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        profile_ids = [int(row["id"]) for row in db.rows("SELECT id FROM profiles WHERE enabled=1 ORDER BY id")]
        db.queue_profile_visits(profile_ids)
        return RedirectResponse(url="/", status_code=303)

    @app.post("/profiles/{profile_id}/scan")
    def scan_profile(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        profile = db.row("SELECT id FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            raise HTTPException(404)
        queued, available_at = db.queue_manual_visit(profile_id, MANUAL_VISIT_COOLDOWN_MINUTES)
        if not queued:
            message = f"此帳號仍在立即拜訪冷卻時間內，可再次執行：{display_time(available_at.isoformat(), cfg.timezone)}"
            return RedirectResponse(url=f"/?error={quote(message)}", status_code=303)
        return RedirectResponse(url=f"/?notice={quote('已排入立即拜訪，將於目前工作完成後執行')}", status_code=303)

    @app.post("/profiles/{profile_id}/browser-scan")
    def browser_scan_profile(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        if not cfg.facebook_browser_enabled:
            return RedirectResponse(url=f"/?error={quote('Facebook 直接瀏覽器尚未啟用')}", status_code=303)
        db: Database = request.app.state.db
        profile = db.row("SELECT id,display_name,name FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            raise HTTPException(404)
        queued, available_at = db.queue_manual_browser_visit(profile_id, MANUAL_VISIT_COOLDOWN_MINUTES)
        if not queued:
            message = f"瀏覽器拜訪冷卻中，請於 {display_time(available_at.isoformat(), cfg.timezone)} 後再試"
            return RedirectResponse(url=f"/?error={quote(message)}", status_code=303)
        label = profile.get("display_name") or profile.get("name") or "Facebook"
        return RedirectResponse(url=f"/?notice={quote(f'已排入 {label} 立即瀏覽器拜訪')}", status_code=303)

    @app.post("/profiles/{profile_id}/apify-freeze")
    def toggle_profile_apify(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        profile = db.row(
            "SELECT id,display_name,name,apify_frozen FROM profiles WHERE id=? AND enabled=1",
            (profile_id,),
        )
        if not profile:
            raise HTTPException(404)
        frozen = 0 if profile.get("apify_frozen") else 1
        db.execute("UPDATE profiles SET apify_frozen=?,updated_at=? WHERE id=?", (frozen, utcnow(), profile_id))
        label = profile.get("display_name") or profile.get("name") or "Facebook"
        action = "已凍結 Apify；其他功能維持正常" if frozen else "已解除 Apify 凍結"
        return RedirectResponse(url=f"/?notice={quote(f'{label} {action}')}", status_code=303)

    @app.post("/profiles/{profile_id}/refresh-name")
    def refresh_profile_name(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        profile = db.row(
            "SELECT id,display_name,name FROM profiles WHERE id=? AND enabled=1",
            (profile_id,),
        )
        if not profile:
            raise HTTPException(404)
        queued, available_at = db.queue_manual_visit(profile_id, MANUAL_VISIT_COOLDOWN_MINUTES)
        if not queued:
            message = f"名稱重新抓取冷卻中，請於 {display_time(available_at.isoformat(), cfg.timezone)} 後再試"
            return RedirectResponse(url=f"/?error={quote(message)}", status_code=303)
        db.execute(
            "UPDATE profiles SET display_name=NULL,serp_last_checked_at=NULL WHERE id=?",
            (profile_id,),
        )
        label = profile.get("display_name") or profile.get("name") or "Facebook"
        return RedirectResponse(url=f"/?notice={quote(f'已排入 {label} 名稱重新抓取')}", status_code=303)

    @app.post("/profiles/{profile_id}/remove")
    def remove_profile(request: Request, profile_id: int):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        profile = db.row("SELECT id,name,url FROM profiles WHERE id=? AND enabled=1", (profile_id,))
        if not profile:
            raise HTTPException(404)
        try:
            if not remove_profile_from_config(cfg.config_path, str(profile["url"])):
                raise ValueError("此帳號不在 config.yaml 監控名單中")
            refreshed = load_settings(cfg.config_path)
            db.sync_profiles(refreshed.profiles)
            db.execute(
                "UPDATE jobs SET status='cancelled',finished_at=?,error='monitoring removed from dashboard' WHERE profile_id=? AND status='pending'",
                (utcnow(), profile_id),
            )
        except (OSError, ValueError) as exc:
            return RedirectResponse(url=f"/?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(url=f"/?notice={quote('已停止監控並保留歷史資料：' + str(profile['name']))}", status_code=303)

    @app.post("/outbox/{outbox_id}/cancel")
    def cancel_outbox(request: Request, outbox_id: int):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        row = db.row("SELECT * FROM outbox WHERE id=?", (outbox_id,))
        if not row:
            raise HTTPException(404)
        if row.get("group_id"):
            db.execute("UPDATE notification_groups SET status='cancelled',cancelled_at=? WHERE id=? AND status='pending'", (__import__("fb_monitor.db", fromlist=["utcnow"]).utcnow(), row["group_id"]))
            db.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='cancelled from dashboard' WHERE group_id=? AND status='pending'", (__import__("fb_monitor.db", fromlist=["utcnow"]).utcnow(), row["group_id"]))
        else:
            db.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='cancelled from dashboard' WHERE id=? AND status='pending'", (__import__("fb_monitor.db", fromlist=["utcnow"]).utcnow(), outbox_id))
        return RedirectResponse(url="/", status_code=303)

    @app.post("/outbox/clear")
    def clear_outbox(request: Request):
        _require_paid_action_same_origin(request)
        db: Database = request.app.state.db
        now = __import__("fb_monitor.db", fromlist=["utcnow"]).utcnow()
        db.execute("UPDATE notification_groups SET status='cancelled',cancelled_at=? WHERE status='pending'", (now,))
        db.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='cleared from dashboard' WHERE status='pending'", (now,))
        return RedirectResponse(url="/", status_code=303)

    @app.get("/profiles/{profile_id}")
    def profile_page(
        request: Request,
        profile_id: int,
        kind: str = Query("post", pattern="^(post|comment|profile)$"),
        q: str = "",
        media_filter: str = Query("all", pattern="^(all|image|video|none|gone)$"),
        page: int = Query(1, ge=1),
    ):
        db: Database = request.app.state.db
        profile = db.row("SELECT * FROM profiles WHERE id=?", (profile_id,))
        if not profile:
            raise HTTPException(404)
        profile["apify_frozen"] = db.profile_source_frozen(profile_id, "apify")
        _attach_browser_capture(profile, cfg)
        _attach_profile_name_history(db, profile)
        size, offset = 20, (page - 1) * 20
        params: tuple[Any, ...] = (profile_id, kind)
        where = "e.profile_id=? AND e.kind=?"
        if q:
            where += " AND (e.external_id LIKE ? OR e.source_url LIKE ? OR v.normalized_json LIKE ?)"
            like = f"%{q}%"
            params += (like, like, like)
        current_media = "em.entity_id=e.id AND em.version_id=e.current_version_id"
        if media_filter == "image":
            where += f" AND EXISTS (SELECT 1 FROM entity_media em JOIN media m ON m.id=em.media_id WHERE {current_media} AND (m.mime_type LIKE 'image/%' OR LOWER(COALESCE(em.role,'')) LIKE '%image%' OR LOWER(COALESCE(em.role,'')) LIKE '%photo%' OR LOWER(COALESCE(em.role,'')) LIKE '%picture%'))"
        elif media_filter == "video":
            where += f" AND EXISTS (SELECT 1 FROM entity_media em JOIN media m ON m.id=em.media_id WHERE {current_media} AND (m.mime_type LIKE 'video/%' OR LOWER(COALESCE(em.role,''))='video'))"
        elif media_filter == "none":
            where += f" AND NOT EXISTS (SELECT 1 FROM entity_media em WHERE {current_media})"
        elif media_filter == "gone":
            where += " AND e.present=0"
        count = db.row(f"SELECT COUNT(*) count FROM entities e LEFT JOIN versions v ON v.id=e.current_version_id WHERE {where}", params)
        entities = db.rows(
            f"""SELECT e.*,v.change_type,v.markdown_path,v.seen_at,v.normalized_json,
            (SELECT COUNT(*) FROM versions vv WHERE vv.entity_id=e.id) version_count
            FROM entities e LEFT JOIN versions v ON v.id=e.current_version_id
            WHERE {where}""",
            params,
        )
        entities.sort(key=lambda row: parse_time(row.get("published_at")) or parse_time(row.get("last_seen_at")) or datetime.min.replace(tzinfo=__import__("datetime").UTC), reverse=True)
        entities = entities[offset:offset + size]
        for entity in entities:
            entity["content"] = _entity_content(_json(entity.get("normalized_json")), kind)
            entity["content"]["timestamp"] = display_time(entity["content"].get("timestamp"), cfg.timezone)
            entity["published_display"] = display_time(entity.get("published_at"), cfg.timezone)
        _attach_current_media(db, entities)
        capture_epoch = db.row(
            """SELECT * FROM capture_epochs WHERE profile_id=?
            ORDER BY is_active DESC,id DESC LIMIT 1""",
            (profile_id,),
        )
        capture_coverage: list[dict[str, Any]] = []
        if capture_epoch:
            _decorate_capture_rows([capture_epoch], cfg)
            capture_coverage = db.rows(
                "SELECT * FROM coverage_streams WHERE epoch_id=? ORDER BY stream,surface,id",
                (capture_epoch["id"],),
            )
            _decorate_capture_rows(capture_coverage, cfg)
        capture_contract_ready = any(
            service._valid_posts_v2_contract(actor_id) is not None
            for actor_id in dict.fromkeys(
                actor.strip()
                for actor in (cfg.actors.posts_v2_primary, cfg.actors.posts_v2_fallback)
                if actor.strip()
            )
        )
        return templates.TemplateResponse(request, "profile.html", {"profile": profile, "entities": entities, "kind": kind, "q": q, "media_filter": media_filter, "page": page, "pages": max(1, ((count or {"count": 0})["count"] + size - 1) // size), "capture_v2_enabled": cfg.capture_v2_enabled, "capture_epoch": capture_epoch, "capture_coverage": capture_coverage, "capture_contract_ready": capture_contract_ready, "capture_contract_budget": cfg.actor_contract_test_budget_usd})

    @app.get("/profiles/{profile_id}/browser-screenshot")
    def browser_screenshot(request: Request, profile_id: int):
        db: Database = request.app.state.db
        if not db.row("SELECT id FROM profiles WHERE id=?", (profile_id,)):
            raise HTTPException(404)
        path = cfg.facebook_browser_data_dir / "screenshots" / f"profile-{profile_id}.png"
        if not path.is_file():
            raise HTTPException(404, "尚無直接瀏覽器擷取畫面")
        return FileResponse(path, media_type="image/png", headers={"Cache-Control": "no-store"})

    @app.get("/entities/{entity_id}")
    def entity_page(request: Request, entity_id: int):
        db: Database = request.app.state.db
        entity = db.row("SELECT e.*,COALESCE(p.display_name,p.name) profile_name FROM entities e JOIN profiles p ON p.id=e.profile_id WHERE e.id=?", (entity_id,))
        if not entity:
            raise HTTPException(404)
        versions = db.rows("SELECT * FROM versions WHERE entity_id=? ORDER BY seen_at DESC", (entity_id,))
        for index, version in enumerate(versions):
            version["content"] = _json(version["normalized_json"])
            version["diff"] = ""
            if index + 1 < len(versions):
                older = json.dumps(_json(versions[index + 1]["normalized_json"]), ensure_ascii=False, indent=2, sort_keys=True).splitlines()
                newer = json.dumps(version["content"], ensure_ascii=False, indent=2, sort_keys=True).splitlines()
                version["diff"] = "\n".join(unified_diff(older, newer, fromfile="較舊版本", tofile="此版本", lineterm=""))
        has_position = _has_column(db, "entity_media", "position")
        position_select = "em.position" if has_position else "NULL AS position"
        position_order = "COALESCE(em.position,999999)," if has_position else ""
        media = db.rows(f"""SELECT DISTINCT m.*,em.role,em.discovery_path,{position_select} FROM media m JOIN entity_media em ON em.media_id=m.id
            WHERE em.entity_id=? ORDER BY em.version_id DESC,{position_order}m.id""", (entity_id,))
        seen_media: set[str] = set()
        media = [row for row in media if not (str(row.get("sha256") or row["id"]) in seen_media or seen_media.add(str(row.get("sha256") or row["id"])))]
        for row in media:
            row["kind"] = _media_kind(row)
        return templates.TemplateResponse(request, "entity.html", {"entity": entity, "versions": versions, "media": media})

    def checked_file(path_value: str) -> Path:
        path = Path(path_value).resolve()
        root = cfg.data_dir.resolve()
        if path != root and root not in path.parents:
            raise HTTPException(403)
        if not path.is_file():
            raise HTTPException(404)
        return path

    @app.get("/media/{media_id}")
    def media_file(request: Request, media_id: int, download: bool = False):
        row = request.app.state.db.row("SELECT * FROM media WHERE id=? AND status='ready'", (media_id,))
        if not row or not row["path"]:
            raise HTTPException(404)
        path = checked_file(row["path"])
        return FileResponse(path, media_type=row["mime_type"], filename=path.name if download else None)

    @app.get("/media/{media_id}/thumbnail")
    def media_thumbnail(request: Request, media_id: int):
        row = request.app.state.db.row("SELECT * FROM media WHERE id=? AND status='ready' AND mime_type LIKE 'image/%'", (media_id,))
        if not row or not row.get("path"):
            raise HTTPException(404)
        source = checked_file(row["path"])
        cache_dir = cfg.data_dir / "cache" / "thumbnails"
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / f"{row['sha256']}-640.webp"
        if not target.exists():
            temporary = target.with_suffix(f".{os.getpid()}-{threading.get_ident()}.tmp")
            try:
                with Image.open(source) as opened:
                    image = ImageOps.exif_transpose(opened)
                    image.seek(0)
                    image.thumbnail((640, 640), Image.Resampling.LANCZOS)
                    if image.mode not in {"RGB", "RGBA"}:
                        image = image.convert("RGB")
                    image.save(temporary, format="WEBP", quality=82, method=4)
                temporary.replace(target)
            except (UnidentifiedImageError, OSError):
                temporary.unlink(missing_ok=True)
                return FileResponse(source, media_type=row["mime_type"], headers={"Cache-Control": "public, max-age=86400"})
        return FileResponse(target, media_type="image/webp", headers={"Cache-Control": "public, max-age=31536000, immutable"})

    @app.get("/versions/{version_id}/{format}")
    def version_file(request: Request, version_id: int, format: str):
        if format not in {"json", "markdown"}:
            raise HTTPException(404)
        row = request.app.state.db.row("SELECT * FROM versions WHERE id=?", (version_id,))
        if not row:
            raise HTTPException(404)
        path = checked_file(row["raw_path"] if format == "json" else row["markdown_path"])
        return FileResponse(path, filename=path.name)

    @app.get("/jobs")
    def jobs(
        request: Request,
        status: str = Query("active", pattern="^(active|all|pending|running|done|failed|cancelled|deferred_budget)$"),
        page: int = Query(1, ge=1),
    ):
        db: Database = request.app.state.db
        if status == "active":
            where, params = "WHERE j.status IN ('pending','running')", ()
        elif status == "all":
            where, params = "", ()
        else:
            where, params = "WHERE j.status=?", (status,)
        size = 100
        count = db.row(f"SELECT COUNT(*) count FROM jobs j {where}", params) or {"count": 0}
        rows = db.rows(
            f"""SELECT j.*,COALESCE(p.display_name,p.name) profile_name
            FROM jobs j LEFT JOIN profiles p ON p.id=j.profile_id {where}
            ORDER BY CASE j.status WHEN 'running' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END,
            CASE WHEN j.status IN ('running','pending') THEN j.available_at END ASC,j.id DESC
            LIMIT ? OFFSET ?""",
            params + (size, (page - 1) * size),
        )
        type_labels = {
            "visit": "定期拜訪", "browser_visit": "立即瀏覽器拜訪", "backfill": "首次回溯", "audit": "完整核對",
            "repair_scan": "修復掃描", "migrate_raw": "歷史資料轉換",
            "migrate_profile_pics": "大頭照欄位更新", "dedupe_database": "資料庫去重",
            "contract_test_posts_v2": "Capture V2 Actor 契約測試",
            "capture_posts_v2": "Capture V2 貼文續抓",
        }
        status_labels = {"pending": "等待中", "running": "執行中", "done": "完成", "failed": "失敗", "cancelled": "已取消", "deferred_budget": "額度延後"}
        for row in rows:
            row["type_label"] = type_labels.get(str(row["job_type"]), str(row["job_type"]))
            row["status_label"] = status_labels.get(str(row["status"]), str(row["status"]))
            row["payload"] = _json(row.get("payload_json"))
            for field in ("available_at", "created_at", "started_at", "finished_at"):
                row[f"{field}_display"] = display_time(row.get(field), cfg.timezone)
        pages = max(1, (int(count["count"]) + size - 1) // size)
        return templates.TemplateResponse(request, "jobs.html", {"jobs": rows, "status": status, "page": page, "pages": pages, "count": count["count"]})

    @app.get("/diagnostics")
    def diagnostics(request: Request, profile_id: str = "", page: int = Query(1, ge=1)):
        db: Database = request.app.state.db
        where, params = "", ()
        selected_id = int(profile_id) if profile_id.isdigit() else None
        if selected_id is not None:
            where, params = "WHERE ar.profile_id=?", (selected_id,)
        rows = db.rows(
            f"""SELECT ar.*,COALESCE(p.display_name,p.name) profile_name
            FROM actor_runs ar LEFT JOIN profiles p ON p.id=ar.profile_id
            {where} ORDER BY ar.id DESC LIMIT 50 OFFSET ?""",
            params + ((page - 1) * 50,),
        )
        for row in rows:
            row["input"] = _json(row.get("input_json"))
            row["summary"] = _json(row.get("summary_json"))
            row["samples"] = _json(row.get("samples_json")) if row.get("samples_json") else []
            row["unhandled_result_count"] = max(
                0,
                int(row.get("parsed_result_count") or 0)
                - int(row.get("new_result_count") or 0)
                - int(row.get("updated_result_count") or 0)
                - int(row.get("duplicate_result_count") or 0),
            )
        profiles = db.rows("SELECT id,COALESCE(display_name,name) label FROM profiles ORDER BY id")
        usage_snapshot = db.apify_usage_snapshot()
        cycle_start = str(usage_snapshot.get("cycle_start_at")) if usage_snapshot else datetime.now(UTC).strftime("%Y-%m-01T00:00:00+00:00")
        profile_usage = db.rows(
            """SELECT p.id,COALESCE(p.display_name,p.name) profile_name,p.url,
            COUNT(ar.id) run_count,COALESCE(SUM(ar.raw_result_count),0) raw_count,
            COALESCE(SUM(ar.parsed_result_count),0) parsed_count,
            COALESCE(SUM(ar.new_result_count),0) new_count,
            COALESCE(SUM(ar.updated_result_count),0) updated_count,
            COALESCE(SUM(ar.duplicate_result_count),0) duplicate_count,
            MAX(0,COALESCE(SUM(ar.parsed_result_count),0)-COALESCE(SUM(ar.new_result_count),0)
              -COALESCE(SUM(ar.updated_result_count),0)-COALESCE(SUM(ar.duplicate_result_count),0)) unhandled_count,
            COALESCE(SUM(ar.charged_usd),0) charged_usd
            FROM profiles p LEFT JOIN actor_runs ar ON ar.profile_id=p.id
              AND ar.category='posts' AND ar.started_at>=?
            WHERE p.enabled=1 GROUP BY p.id ORDER BY charged_usd DESC,p.id""",
            (cycle_start,),
        )
        return templates.TemplateResponse(request, "diagnostics.html", {"runs": rows, "profiles": profiles, "profile_usage": profile_usage, "cycle_start_display": display_time(cycle_start, cfg.timezone), "profile_id": selected_id, "page": page})

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app
