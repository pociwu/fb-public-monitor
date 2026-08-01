from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from PIL import Image, UnidentifiedImageError

from .db import Database, utcnow


@dataclass(frozen=True)
class MediaRef:
    url: str
    role: str
    json_path: str


MEDIA_WORDS = ("media", "image", "photo", "picture", "profilepic", "avatar", "cover", "video", "attachment", "playable")
GENERIC_URL_KEYS = {"url", "uri", "source", "src", "link", "downloadurl", "sourceurl"}


def _role_for(path: list[str], kind: str) -> str:
    joined = ".".join(path).lower().replace("_", "")
    if "profilepicture" in joined or "profilepic" in joined or "avatar" in joined:
        return "profile_picture"
    if "coverphoto" in joined or "coverpicture" in joined:
        return "cover_photo"
    if "video" in joined or "playable" in joined:
        return "video"
    if "image" in joined or "photo" in joined or "picture" in joined:
        return "image"
    return "comment_attachment" if kind == "comment" else "attachment"


def extract_media(item: dict[str, Any], kind: str) -> list[MediaRef]:
    """Find media in changing Actor schemas and retain its JSON discovery path."""
    found: dict[str, MediaRef] = {}

    def walk(value: Any, path: list[str], media_context: bool = False) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized_key = str(key).lower().replace("_", "")
                if kind == "profile" and normalized_key in {"posts", "latestposts", "recentposts"}:
                    continue
                if kind == "post" and normalized_key in {"comments", "topcomments", "replies"}:
                    continue
                contextual = media_context or any(word in normalized_key for word in MEDIA_WORDS)
                walk(child, [*path, str(key)], contextual)
            return
        if isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, [*path, str(index)], media_context)
            return
        if not isinstance(value, str) or not value.startswith(("http://", "https://")) or not path:
            return
        host = (urlsplit(value).hostname or "").lower()
        if host == "facebook.com" or host.endswith(".facebook.com"):
            return
        key = path[-1].lower().replace("_", "")
        if any(blocked in key for blocked in ("pageurl", "permalink", "targeturl", "website")):
            return
        key_is_media = any(word in key for word in MEDIA_WORDS)
        if not key_is_media and not (media_context and key in GENERIC_URL_KEYS):
            return
        # Prefer original media over previews/thumbnails when both are present,
        # but retain a preview when it is the only downloadable representation.
        role = _role_for(path, kind)
        ref = MediaRef(value, role, "$.'" + "'.'".join(path) + "'")
        previous = found.get(value)
        if not previous or ("thumbnail" in previous.json_path.lower() and "thumbnail" not in ref.json_path.lower()):
            found[value] = ref

    walk(item, [])
    return list(found.values())


class MediaStore:
    def __init__(self, db: Database, data_dir: Path, low_disk_gb: float, retry_days: int):
        self.db = db
        self.root = data_dir / "media"
        self.root.mkdir(parents=True, exist_ok=True)
        self.low_disk_bytes = int(low_disk_gb * 1024**3)
        self.retry_days = retry_days

    def has_space(self) -> bool:
        return shutil.disk_usage(self.root).free >= self.low_disk_bytes

    @staticmethod
    def perceptual_hash(path: Path, mime: str) -> str | None:
        if not mime.startswith("image/"):
            return None
        try:
            with Image.open(path) as image:
                image = image.convert("L").resize((8, 8))
                average = sum(image.getdata()) / 64
                bits = "".join("1" if value >= average else "0" for value in image.getdata())
                return f"{int(bits, 2):016x}"
        except (OSError, UnidentifiedImageError):
            return None

    async def download(self, url: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        retry_until = (now + timedelta(days=self.retry_days)).isoformat()
        if not self.has_space():
            return {"status": "paused_low_disk", "source_url": url}
        last_error: Exception | None = None
        for attempt in range(3):
            tmp = self.root / f".download-{os.getpid()}-{hashlib.sha1(url.encode()).hexdigest()}.part"
            digest = hashlib.sha256()
            size = 0
            mime = "application/octet-stream"
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
                    async with client.stream("GET", url) as response:
                        response.raise_for_status()
                        mime = response.headers.get("content-type", mime).split(";", 1)[0]
                        if mime in {"text/html", "application/json"}:
                            raise ValueError(f"網址未回傳媒體內容：{mime}")
                        with tmp.open("wb") as fh:
                            async for chunk in response.aiter_bytes():
                                digest.update(chunk)
                                size += len(chunk)
                                fh.write(chunk)
                if size == 0:
                    raise ValueError("下載檔案為空")
                sha = digest.hexdigest()
                ext = mimetypes.guess_extension(mime) or Path(urlsplit(url).path).suffix[:8] or ".bin"
                target = self.root / sha[:2] / f"{sha}{ext}"
                target.parent.mkdir(parents=True, exist_ok=True)
                if not target.exists():
                    tmp.replace(target)
                else:
                    tmp.unlink(missing_ok=True)
                return {"status": "ready", "sha256": sha, "perceptual_hash": self.perceptual_hash(target, mime), "path": str(target), "mime_type": mime, "size_bytes": size, "source_url": url}
            except Exception as exc:
                last_error = exc
                tmp.unlink(missing_ok=True)
                if attempt < 2:
                    await asyncio.sleep(2**attempt)
        return {"status": "pending", "source_url": url, "error": str(last_error), "retry_until": retry_until, "last_attempt_at": utcnow()}
