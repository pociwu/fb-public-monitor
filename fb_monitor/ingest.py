from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .db import Database, utcnow
from .media import MediaRef, MediaStore, extract_media, media_representation_key
from .normalize import content_hash, normalize_text, normalize_url, stable_projection
from .timeutil import display_time, telegram_time


ID_KEYS = {
    "profile": ("profileId", "profile_id", "id", "pageId", "facebookId"),
    "post": ("source_post_id", "postId", "post_id", "id", "facebookId"),
    "comment": ("commentId", "comment_id", "id"),
}
URL_KEYS = {
    "profile": ("url", "profileUrl", "profile_url", "facebookUrl", "pageUrl"),
    "post": ("source_url", "postUrl", "post_url", "url", "facebookUrl"),
    "comment": ("commentUrl", "comment_url", "url"),
}


def first(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    return next((item[key] for key in keys if item.get(key) not in (None, "")), None)


def external_id(item: dict[str, Any], kind: str) -> str:
    value = first(item, ID_KEYS[kind])
    if value:
        return str(value)
    url = str(first(item, URL_KEYS[kind]) or "")
    if url:
        return content_hash(url)[:24]
    return content_hash(item)[:24]


def comment_dedupe_key(item: dict[str, Any], parent_external_id: str | None) -> str:
    author = normalize_text(first(item, ("authorName", "author_name", "name", "profileName")) or "")
    text = normalize_text(first(item, ("text", "raw_text", "message", "commentText")) or "")
    published = display_time(first(item, ("created_at", "date", "timestamp", "publishTime", "time")))
    # Actor timestamps are sometimes seconds and sometimes ISO strings; minute
    # precision intentionally absorbs scrape-format noise without merging replies.
    return content_hash({"parent": parent_external_id or "", "author": author, "text": text, "published": published})


def monitored_projection(item: dict[str, Any], kind: str, refs: list[MediaRef]) -> dict[str, Any]:
    """Only fields whose change is meaningful enough to notify about."""
    author = first(item, ("authorName", "author_name", "name", "profileName", "source_profile_name")) or ""
    text = first(item, ("raw_text", "text", "message", "postText", "description", "caption", "bio", "about", "profile_intro_text")) or ""
    published = display_time(first(item, ("created_at", "date", "timestamp", "publishTime", "time")))
    if kind == "profile":
        author = profile_display_name(item)
    projection = {
        "authorName": normalize_text(author), "text": normalize_text(text), "publishTime": published,
        "attachments": sorted({(ref.role, normalize_url(ref.url)) for ref in refs}),
    }
    if kind == "profile":
        projection["profileDetails"] = stable_projection({
            key: item.get(key) for key in (
                "alternate_name", "verified", "followers", "following", "likes", "profile_type",
                "category", "current_city", "hometown", "relationship", "educations", "works", "about_details",
            ) if item.get(key) not in (None, "", [], {})
        })
    return projection


def safe_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:100]


def markdown_for(item: dict[str, Any], kind: str) -> str:
    title = profile_display_name(item) if kind == "profile" else first(item, ("name", "profileName", "source_profile_name", "postTitle", "authorName"))
    title = title or kind.title()
    text = first(item, ("raw_text", "text", "message", "postText", "description", "profile_intro_text")) or ""
    url = first(item, URL_KEYS[kind]) or ""
    date = first(item, ("created_at", "date", "timestamp", "publishTime")) or ""
    return f"# {normalize_text(title)}\n\n- 類型：{kind}\n- 時間：{date}\n- 來源：{url}\n\n{normalize_text(text)}\n"


def profile_display_name(item: dict[str, Any]) -> str:
    nested = item.get("personalProfile")
    candidates = []
    if isinstance(nested, dict):
        candidates.extend(nested.get(key) for key in ("name", "title", "fullName", "profileName"))
    candidates.extend(item.get(key) for key in ("title", "name", "pageName", "profileName", "source_profile_name"))
    return next(
        (
            normalize_text(value)
            for value in candidates
            if isinstance(value, str) and not is_placeholder_profile_name(value)
        ),
        "",
    )


def is_placeholder_profile_name(value: object) -> bool:
    name = normalize_text(value)
    if not name:
        return True
    return bool(
        re.fullmatch(r"(?:FB|Facebook)[\s_-]*\d+", name, flags=re.IGNORECASE)
        or re.fullmatch(r"\(\d+\)\s*Facebook", name, flags=re.IGNORECASE)
    )


def embedded_posts(item: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[Any] = [item.get("posts")]
    if isinstance(item.get("personalProfile"), dict):
        candidates.append(item["personalProfile"].get("posts"))
    return [post for group in candidates if isinstance(group, list) for post in group if isinstance(post, dict)]


def _event_payload(db: Database, profile_id: int, kind: str, change_type: str, item: dict[str, Any], source_url: str, ext_id: str, media: list[MediaRef]) -> dict[str, Any]:
    profile = db.row("SELECT name,display_name,fb_id FROM profiles WHERE id=?", (profile_id,)) or {}
    display = profile_display_name(item) if kind == "profile" else ""
    display = display or profile.get("display_name") or profile.get("name") or f"Facebook {profile.get('fb_id') or profile_id}"
    labels = {"created": "新增", "updated": "更新", "restored": "恢復"}
    label = labels.get(change_type, change_type)
    text = normalize_text(str(first(item, ("raw_text", "text", "message", "postText", "description", "bio", "about", "profile_intro_text")) or ""))
    published = telegram_time(first(item, ("created_at", "date", "timestamp", "publishTime", "time")))
    roles = {ref.role for ref in media}
    if kind == "profile":
        changes = []
        if display:
            changes.append(f"名稱：{display}")
        if text:
            changes.append(f"簡介：{text}")
        if "profile_picture" in roles:
            changes.append("大頭照已擷取")
        if "cover_photo" in roles:
            changes.append("封面照片已擷取")
        body = "\n".join(changes) or "公開個人檔案資料有變更"
        title = f"【個人檔案{label}】{display}"
    elif kind == "post":
        body = "\n".join(part for part in (f"時間：{published}" if published else "", f"內容：{text}" if text else "（無文字內容）", f"附件：{len(media)} 個" if media else "") if part)
        title = f"【{label}貼文】{display}"
    else:
        author = first(item, ("authorName", "author_name", "name", "profileName")) or "未知作者"
        body = "\n".join(part for part in (f"作者：{normalize_text(str(author))}", f"時間：{published}" if published else "", f"留言：{text}" if text else "（無文字內容）", f"附件：{len(media)} 個" if media else "") if part)
        title = f"【{label}留言】{display}"
    return {"title": title, "kind": kind, "change_type": change_type, "external_id": ext_id, "source_url": source_url, "text": body[:3500]}


class Ingester:
    def __init__(self, db: Database, data_dir: Path, media: MediaStore):
        self.db = db
        self.root = data_dir / "profiles"
        self.media = media

    async def ingest(self, profile_id: int, kind: str, item: dict[str, Any], notify: bool = True, parent_external_id: str | None = None) -> tuple[int, str, bool]:
        now = utcnow()
        ext_id = external_id(item, kind)
        dedupe_key = comment_dedupe_key(item, parent_external_id) if kind == "comment" else None
        source_url = str(first(item, URL_KEYS[kind]) or "")
        published = str(first(item, ("created_at", "date", "timestamp", "publishTime")) or "")
        media_refs = extract_media(item, kind)
        normalized = monitored_projection(item, kind, media_refs)
        digest = content_hash(normalized)
        if kind == "profile":
            display_name = profile_display_name(item)
            if display_name:
                self.db.execute("UPDATE profiles SET display_name=? WHERE id=?", (display_name, profile_id))
        existing = self.db.row("SELECT * FROM entities WHERE profile_id=? AND kind=? AND external_id=?", (profile_id, kind, ext_id))
        if not existing and dedupe_key:
            existing = self.db.row("SELECT * FROM entities WHERE profile_id=? AND kind='comment' AND dedupe_key=?", (profile_id, dedupe_key))
        if existing and existing["current_hash"] == digest:
            await self._refresh_unchanged_media(existing, media_refs)
            self.db.execute("UPDATE entities SET notification_hash=COALESCE(notification_hash,?),last_seen_at=?,present=1,missing_successes=0 WHERE id=?", (digest, now, existing["id"]))
            return int(existing["id"]), ext_id, False
        notify_change = notify and not (existing and existing.get("notification_hash") is None)
        profile_dir = self.root / str(profile_id) / f"{kind}s" / safe_part(ext_id)
        profile_dir.mkdir(parents=True, exist_ok=True)
        raw_path = profile_dir / f"{now.replace(':', '-')}-{digest[:12]}.json"
        md_path = profile_dir / f"{now.replace(':', '-')}-{digest[:12]}.md"
        raw_path.write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(markdown_for(item, kind), encoding="utf-8")
        with self.db.connect() as conn:
            if not existing:
                cur = conn.execute(
                    """INSERT INTO entities(profile_id,kind,external_id,parent_external_id,dedupe_key,source_url,published_at,current_hash,notification_hash,present,first_seen_at,last_seen_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (profile_id, kind, ext_id, parent_external_id, dedupe_key, source_url, published, digest, digest if notify else None, 1, now, now),
                )
                entity_id = int(cur.lastrowid)
                change_type = "created"
            else:
                entity_id = int(existing["id"])
                change_type = "restored" if not existing["present"] else "updated"
            cur = conn.execute(
                "INSERT INTO versions(entity_id,content_hash,normalized_json,raw_path,markdown_path,seen_at,change_type) VALUES(?,?,?,?,?,?,?)",
                (entity_id, digest, json.dumps(normalized, ensure_ascii=False), str(raw_path), str(md_path), now, change_type),
            )
            version_id = int(cur.lastrowid)
            conn.execute(
                "UPDATE entities SET source_url=?,published_at=?,dedupe_key=COALESCE(dedupe_key,?),current_hash=?,notification_hash=?,current_version_id=?,present=1,missing_successes=0,last_seen_at=? WHERE id=?",
                (source_url, published, dedupe_key, digest, digest, version_id, now, entity_id),
            )
        payload = _event_payload(self.db, profile_id, kind, change_type, item, source_url, ext_id, media_refs)
        event_id = self.db.add_event(
            f"{kind}:{profile_id}:{ext_id}:{digest}", f"{kind}_{change_type}", payload,
            profile_id, entity_id, notify_change, coalesce=notify_change, coalesce_minutes=15,
        )
        for position, ref in enumerate(media_refs):
            result = await self.media.download(ref.url)
            self._link_media(
                entity_id,
                version_id,
                result,
                ref.role,
                ref.json_path,
                position,
                event_id if notify_change else None,
                int(existing["current_version_id"]) if existing and existing.get("current_version_id") else None,
            )
        return entity_id, ext_id, True

    async def _refresh_unchanged_media(
        self,
        entity: dict[str, Any],
        refs: list[MediaRef],
    ) -> None:
        """Persist new representations even when entity metadata is unchanged.

        Normalized Facebook CDN URLs intentionally omit rotating query
        parameters, so a 24px and 720px URL do not create fake entity
        versions.  They still must traverse the media pipeline: otherwise the
        first thumbnail permanently masks a later high-resolution candidate.
        """
        version_id = entity.get("current_version_id")
        if not version_id:
            return
        for position, ref in enumerate(refs):
            representation_key = media_representation_key(ref.url)
            exact_ready = self.db.row(
                """SELECT m.id FROM media m
                JOIN entity_media em ON em.media_id=m.id
                LEFT JOIN media_aliases ma
                  ON ma.media_id=m.id AND ma.alias_type='representation_url'
                WHERE em.entity_id=? AND em.version_id=? AND em.role=?
                  AND m.status='ready'
                  AND (m.source_url=? OR ma.alias_value=?)
                LIMIT 1""",
                (
                    entity["id"],
                    version_id,
                    ref.role,
                    ref.url,
                    representation_key,
                ),
            )
            if exact_ready:
                continue
            result = await self.media.download(ref.url)
            self._link_media(
                int(entity["id"]),
                int(version_id),
                result,
                ref.role,
                ref.json_path,
                position,
                None,
                None,
            )

    def _link_media(self, entity_id: int, version_id: int, result: dict[str, Any], role: str, discovery_path: str, position: int, event_id: int | None, previous_version_id: int | None) -> None:
        now = utcnow()
        sha = result.get("sha256")
        source_url = str(result.get("source_url") or "")
        dimensions = self.media.image_dimensions(result.get("path"), result.get("mime_type"))
        width, height = dimensions if dimensions else (None, None)
        with self.db.connect() as conn:
            if sha:
                conn.execute(
                    """INSERT INTO media(sha256,source_url,mime_type,size_bytes,path,perceptual_hash,status,first_seen_at,last_attempt_at,retry_until,error)
                    VALUES(?,?,?,?,?,?,'ready',?,?,NULL,NULL) ON CONFLICT(sha256) DO UPDATE SET
                    source_url=excluded.source_url,mime_type=COALESCE(excluded.mime_type,media.mime_type),
                    size_bytes=COALESCE(excluded.size_bytes,media.size_bytes),status='ready',path=excluded.path,
                    perceptual_hash=COALESCE(excluded.perceptual_hash,media.perceptual_hash),
                    last_attempt_at=excluded.last_attempt_at,retry_until=NULL,error=NULL""",
                    (sha, result["source_url"], result.get("mime_type"), result.get("size_bytes"), result.get("path"), result.get("perceptual_hash"), now, now),
                )
                media_id = conn.execute("SELECT id FROM media WHERE sha256=?", (sha,)).fetchone()[0]
            else:
                synthetic = content_hash(result["source_url"])
                conn.execute(
                    """INSERT INTO media(sha256,source_url,status,first_seen_at,last_attempt_at,retry_until,error)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT(sha256) DO UPDATE SET status=excluded.status,last_attempt_at=excluded.last_attempt_at,error=excluded.error""",
                    (synthetic, result["source_url"], result.get("status", "pending"), now, result.get("last_attempt_at", now), result.get("retry_until"), result.get("error")),
                )
                media_id = conn.execute("SELECT id FROM media WHERE sha256=?", (synthetic,)).fetchone()[0]
            conn.execute(
                """INSERT INTO entity_media(entity_id,version_id,media_id,role,discovery_path,position) VALUES(?,?,?,?,?,?)
                ON CONFLICT(entity_id,version_id,media_id) DO UPDATE SET role=excluded.role,
                discovery_path=excluded.discovery_path,position=COALESCE(entity_media.position,excluded.position)""",
                (entity_id, version_id, media_id, role, discovery_path, position),
            )
            profile_row = conn.execute(
                "SELECT profile_id FROM entities WHERE id=?",
                (entity_id,),
            ).fetchone()
            if profile_row and source_url:
                representation_key = media_representation_key(source_url)
                conn.execute(
                    """INSERT INTO media_aliases(
                    profile_id,entity_id,media_id,canonical_media_id,provider,
                    alias_type,alias_value,source_url,width,height,mime_type,sha256,
                    first_seen_at,last_seen_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(profile_id,alias_type,alias_value) DO UPDATE SET
                      entity_id=excluded.entity_id,media_id=excluded.media_id,
                      canonical_media_id=excluded.canonical_media_id,
                      source_url=excluded.source_url,width=COALESCE(excluded.width,media_aliases.width),
                      height=COALESCE(excluded.height,media_aliases.height),
                      mime_type=COALESCE(excluded.mime_type,media_aliases.mime_type),
                      sha256=COALESCE(excluded.sha256,media_aliases.sha256),
                      last_seen_at=excluded.last_seen_at""",
                    (
                        profile_row["profile_id"],
                        entity_id,
                        media_id,
                        normalize_url(source_url) or str(sha or media_id),
                        "ingest",
                        "representation_url",
                        representation_key,
                        source_url,
                        width,
                        height,
                        result.get("mime_type"),
                        sha,
                        now,
                        now,
                    ),
                )
            previous_shas: set[str] = set()
            if previous_version_id:
                previous_shas = {row[0] for row in conn.execute("SELECT m.sha256 FROM media m JOIN entity_media em ON em.media_id=m.id WHERE em.version_id=?", (previous_version_id,)).fetchall()}
            if event_id and result.get("status") == "ready" and sha not in previous_shas:
                captions = {"profile_picture": "大頭照", "cover_photo": "封面照片", "video": "影片", "image": "照片", "comment_attachment": "留言附件", "attachment": "貼文附件"}
                payload = json.dumps({"path": result["path"], "mime_type": result.get("mime_type"), "caption": captions.get(role, "附件")}, ensure_ascii=False)
                conn.execute(
                    "INSERT OR IGNORE INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at) VALUES(?,?,?,?,?)",
                    (event_id, "media", payload, now, now),
                )

        notification_sha = str(sha or "")
        if result.get("status") == "ready" and sha and str(result.get("mime_type") or "").startswith("image/"):
            winner = self._keep_highest_resolution(entity_id, version_id, role, int(media_id))
            if winner:
                notification_sha = str(winner.get("sha256") or notification_sha)

        if event_id and result.get("status") == "ready" and notification_sha:
            # Storage equivalence has already been confirmed using CDN identity
            # or pHash plus RGB pixels.  Notification de-duplication therefore
            # uses the selected file SHA only; forwarding an unverified aHash
            # would let two different, mostly-solid images cancel each other.
            self.db.bind_media_notification(event_id, notification_sha, None)

    def _keep_highest_resolution(
        self,
        entity_id: int,
        version_id: int,
        role: str,
        linked_media_id: int,
    ) -> dict[str, Any] | None:
        """Collapse verified representations and retain only the best file.

        Matching CDN object paths and aHash matches are only candidates.  The
        shared media verifier also requires matching aspect ratio and RGB
        content, preventing crops, placeholders or collisions from deleting an
        unrelated image.
        """
        rows = self.db.rows(
            """SELECT DISTINCT m.* FROM media m JOIN entity_media em ON em.media_id=m.id
            WHERE em.entity_id=? AND em.role=?
              AND m.status='ready' AND m.mime_type LIKE 'image/%'
            ORDER BY m.id""",
            (entity_id, role),
        )
        if not rows:
            return None

        groups: list[list[dict[str, Any]]] = []
        for row in rows:
            placed = False
            for group in groups:
                representative = group[0]
                if self.media.image_records_equivalent(row, representative):
                    group.append(row)
                    placed = True
                    break
            if not placed:
                groups.append([row])

        selected = next(
            (row for row in rows if int(row["id"]) == linked_media_id),
            None,
        )
        replacements: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for group in groups:
            winner = max(
                group,
                key=lambda item: (
                    *self.media.image_quality(
                        item.get("path"), item.get("mime_type"), item.get("size_bytes")
                    ),
                    -int(item["id"]),
                ),
            )
            # A row marked ready can still contain a truncated or corrupt file.
            # Never remove other representations unless a real image supplies
            # the winning effective pixel count.
            if self.media.image_quality(
                winner.get("path"), winner.get("mime_type"), winner.get("size_bytes")
            )[0] <= 0:
                continue
            if any(int(item["id"]) == linked_media_id for item in group):
                selected = winner
            for loser in group:
                if int(loser["id"]) != int(winner["id"]):
                    replacements.append((loser, winner))
        if replacements:
            self._replace_media_representations(replacements)
        return selected

    def _replace_media_representation(
        self,
        loser: dict[str, Any],
        winner: dict[str, Any],
    ) -> None:
        self._replace_media_representations([(loser, winner)])

    def consolidate_media_representations(
        self,
        rows: list[dict[str, Any]],
        *,
        actual_sha256: dict[int, str] | None = None,
    ) -> int:
        """Safely merge retained media and return the number of removed rows.

        Exact file bytes are conclusive for every MIME type.  Image similarity
        is only accepted through ``MediaStore.image_records_equivalent``.  The
        winner is chosen by decoded pixel quality, and all database references
        are redirected in one transaction by ``_replace_media_representations``.
        """
        actual = actual_sha256 or {}
        groups: list[list[dict[str, Any]]] = []
        for row in rows:
            row_id = int(row["id"])
            row_actual = str(actual.get(row_id) or "")
            placed = False
            for group in groups:
                representative = group[0]
                representative_actual = str(actual.get(int(representative["id"])) or "")
                exact_bytes = bool(row_actual and row_actual == representative_actual)
                if exact_bytes or self.media.image_records_equivalent(row, representative):
                    group.append(row)
                    placed = True
                    break
            if not placed:
                groups.append([row])

        replacements: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for group in groups:
            if len(group) < 2:
                continue
            common_actual = str(actual.get(int(group[0]["id"])) or "")
            exact_group = bool(
                common_actual
                and all(
                    str(actual.get(int(item["id"])) or "") == common_actual
                    for item in group
                )
            )
            winner = max(
                group,
                key=lambda item: (
                    int(exact_group and str(item.get("sha256") or "") == common_actual),
                    *self.media.image_quality(
                        item.get("path"), item.get("mime_type"), item.get("size_bytes")
                    ),
                    -int(item["id"]),
                ),
            )
            for loser in group:
                if int(loser["id"]) != int(winner["id"]):
                    replacements.append((loser, winner))
        if replacements:
            self._replace_media_representations(replacements)
        return len({int(loser["id"]) for loser, _winner in replacements})

    def _replace_media_representations(
        self,
        replacements: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> None:
        """Atomically redirect every DB reference, then remove orphan files."""
        unique: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
        for loser, winner in replacements:
            loser_id = int(loser["id"])
            winner_id = int(winner["id"])
            if loser_id != winner_id:
                unique[loser_id] = (loser, winner)
        if not unique:
            return

        cleanup: list[tuple[dict[str, Any], dict[str, Any]]] = []
        with self.db.connect() as conn:
            for loser, winner in unique.values():
                loser_id = int(loser["id"])
                winner_id = int(winner["id"])
                links = conn.execute(
                    """SELECT entity_id,version_id,role,discovery_path,position
                    FROM entity_media WHERE media_id=?""",
                    (loser_id,),
                ).fetchall()
                for link in links:
                    conn.execute(
                        """INSERT INTO entity_media(
                        entity_id,version_id,media_id,role,discovery_path,position
                        ) VALUES(?,?,?,?,?,?)
                        ON CONFLICT(entity_id,version_id,media_id) DO UPDATE SET
                          role=COALESCE(entity_media.role,excluded.role),
                          discovery_path=COALESCE(entity_media.discovery_path,excluded.discovery_path),
                          position=CASE
                            WHEN entity_media.position IS NULL THEN excluded.position
                            WHEN excluded.position IS NULL THEN entity_media.position
                            ELSE MIN(entity_media.position,excluded.position)
                          END""",
                        (
                            link["entity_id"],
                            link["version_id"],
                            winner_id,
                            link["role"],
                            link["discovery_path"],
                            link["position"],
                        ),
                    )
                conn.execute("DELETE FROM entity_media WHERE media_id=?", (loser_id,))

                dimensions = self.media.image_dimensions(
                    winner.get("path"),
                    winner.get("mime_type"),
                )
                width, height = dimensions if dimensions else (None, None)
                conn.execute(
                    """UPDATE media_aliases SET
                    media_id=?,sha256=?,mime_type=COALESCE(?,mime_type),
                    width=COALESCE(?,width),height=COALESCE(?,height)
                    WHERE media_id=?""",
                    (
                        winner_id,
                        winner.get("sha256"),
                        winner.get("mime_type"),
                        width,
                        height,
                        loser_id,
                    ),
                )

                conn.execute(
                    """UPDATE outbox SET media_sha256=?,media_perceptual_hash=?
                    WHERE media_sha256=? AND status='pending' AND kind='media'""",
                    (
                        winner.get("sha256"),
                        winner.get("perceptual_hash"),
                        loser.get("sha256"),
                    ),
                )
                for outbox in conn.execute(
                    """SELECT id,event_id,payload_json FROM outbox
                    WHERE status='pending' AND kind='media'"""
                ).fetchall():
                    try:
                        payload = json.loads(outbox["payload_json"] or "{}")
                    except (TypeError, json.JSONDecodeError):
                        continue
                    if str(payload.get("path") or "") != str(loser.get("path") or ""):
                        continue
                    payload["path"] = winner.get("path")
                    payload["mime_type"] = winner.get("mime_type") or payload.get("mime_type")
                    if "sha256" in payload:
                        payload["sha256"] = winner.get("sha256")
                    serialized = json.dumps(payload, ensure_ascii=False)
                    duplicate = conn.execute(
                        """SELECT id,status FROM outbox
                        WHERE event_id IS ? AND kind='media' AND payload_json=? AND id<>?
                        ORDER BY id LIMIT 1""",
                        (outbox["event_id"], serialized, outbox["id"]),
                    ).fetchone()
                    if duplicate:
                        # UNIQUE(event_id,kind,payload_json) spans every status.
                        # Once two rows resolve to one winner, retain only the
                        # oldest delivery record so the transaction can publish
                        # one correct pending path without a constraint race.
                        discard_id = (
                            int(outbox["id"])
                            if duplicate["status"] != "pending"
                            else max(int(outbox["id"]), int(duplicate["id"]))
                        )
                        conn.execute("DELETE FROM outbox WHERE id=?", (discard_id,))
                        if discard_id == int(outbox["id"]):
                            continue
                    conn.execute(
                        "UPDATE outbox SET payload_json=? WHERE id=?",
                        (serialized, outbox["id"]),
                    )

                remaining_links = conn.execute(
                    """SELECT
                    EXISTS(SELECT 1 FROM entity_media WHERE media_id=?) OR
                    EXISTS(SELECT 1 FROM media_aliases WHERE media_id=?)""",
                    (loser_id, loser_id),
                ).fetchone()[0]
                if remaining_links:
                    raise RuntimeError(f"低解析度媒體仍被引用：{loser_id}")
                conn.execute("DELETE FROM media WHERE id=?", (loser_id,))
                cleanup.append((loser, winner))

        for loser, winner in cleanup:
            self._remove_unreferenced_media_files(loser, winner)

    def _remove_unreferenced_media_files(
        self,
        loser: dict[str, Any],
        winner: dict[str, Any],
    ) -> None:
        loser_id = int(loser["id"])
        if self.db.row("SELECT id FROM media WHERE id=?", (loser_id,)):
            return
        loser_path = Path(str(loser.get("path") or ""))
        winner_path = Path(str(winner.get("path") or ""))
        inside_media_root = False
        try:
            media_root = self.media.root.resolve()
            resolved = loser_path.resolve()
            inside_media_root = (
                resolved != winner_path.resolve()
                and resolved != media_root
                and media_root in resolved.parents
            )
            if inside_media_root and loser_path.is_file():
                loser_path.unlink()
        except OSError:
            pass
        loser_sha = str(loser.get("sha256") or "")
        thumbnail_root = self.media.root.parent / "cache" / "thumbnails"
        if inside_media_root and loser_sha and thumbnail_root.is_dir():
            resolved_thumbnail_root = thumbnail_root.resolve()
            for thumbnail in thumbnail_root.glob(f"{loser_sha}-*.webp"):
                try:
                    resolved_thumbnail = thumbnail.resolve()
                    if resolved_thumbnail_root in resolved_thumbnail.parents:
                        thumbnail.unlink()
                except OSError:
                    pass

    async def reprocess_existing(self) -> dict[str, int]:
        """Re-read retained raw versions without creating history or item notifications."""
        counts = {"versions": 0, "names": 0, "media": 0, "errors": 0}
        rows = self.db.rows(
            """SELECT v.id version_id,v.raw_path,e.id entity_id,e.kind,e.profile_id
            FROM versions v JOIN entities e ON e.id=v.entity_id ORDER BY v.id"""
        )
        for row in rows:
            try:
                path = Path(row["raw_path"])
                if not path.exists():
                    counts["errors"] += 1
                    continue
                item = json.loads(path.read_text(encoding="utf-8"))
                counts["versions"] += 1
                if row["kind"] == "profile":
                    name = profile_display_name(item)
                    if name:
                        self.db.execute("UPDATE profiles SET display_name=? WHERE id=?", (name, row["profile_id"]))
                        counts["names"] += 1
                for position, ref in enumerate(extract_media(item, row["kind"])):
                    exists = self.db.row(
                        """SELECT em.media_id FROM entity_media em JOIN media m ON m.id=em.media_id
                        WHERE em.entity_id=? AND em.version_id=? AND m.source_url=?""",
                        (row["entity_id"], row["version_id"], ref.url),
                    )
                    if exists:
                        self.db.execute(
                            "UPDATE entity_media SET role=?,discovery_path=?,position=COALESCE(position,?) WHERE entity_id=? AND version_id=? AND media_id=?",
                            (ref.role, ref.json_path, position, row["entity_id"], row["version_id"], exists["media_id"]),
                        )
                        continue
                    result = await self.media.download(ref.url)
                    self._link_media(int(row["entity_id"]), int(row["version_id"]), result, ref.role, ref.json_path, position, None, None)
                    counts["media"] += 1
            except Exception:
                counts["errors"] += 1
        return counts

    def reconcile(self, profile_id: int, kind: str, seen: set[str], limit: int | None, notify: bool, parent_external_id: str | None = None) -> None:
        sql = "SELECT * FROM entities WHERE profile_id=? AND kind=? AND present=1 ORDER BY published_at DESC, id DESC"
        params: tuple[Any, ...] = (profile_id, kind)
        if parent_external_id is not None:
            sql = "SELECT * FROM entities WHERE profile_id=? AND kind=? AND parent_external_id=? AND present=1 ORDER BY published_at DESC, id DESC"
            params = (profile_id, kind, parent_external_id)
        if limit:
            sql += " LIMIT ?"
            params += (limit,)
        for row in self.db.rows(sql, params):
            if row["external_id"] in seen:
                continue
            misses = int(row["missing_successes"]) + 1
            if misses < 2:
                self.db.execute("UPDATE entities SET missing_successes=? WHERE id=?", (misses, row["id"]))
                continue
            self.db.execute("UPDATE entities SET present=0,missing_successes=? WHERE id=?", (misses, row["id"]))
            payload = {"title": f"{kind} removed", "kind": kind, "change_type": "removed", "external_id": row["external_id"], "source_url": row["source_url"]}
            event_id = self.db.add_event(f"{kind}:{profile_id}:{row['external_id']}:removed:{utcnow()}", f"{kind}_removed", payload, profile_id, row["id"], notify)
            if event_id:
                preview = self.db.row("""SELECT m.path,m.mime_type,m.sha256 FROM media m JOIN entity_media em ON em.media_id=m.id
                    WHERE em.entity_id=? AND m.status='ready' AND m.mime_type LIKE 'image/%' ORDER BY em.version_id DESC LIMIT 1""", (row["id"],))
                if preview:
                    self.db.execute(
                        "INSERT OR IGNORE INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at) VALUES(?,?,?,?,?)",
                        (event_id, "media", json.dumps({"path": preview["path"], "mime_type": preview["mime_type"], "caption": "已移除內容的預覽縮圖"}, ensure_ascii=False), utcnow(), utcnow()),
                    )
