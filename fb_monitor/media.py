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
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx
from PIL import Image, ImageChops, ImageOps, ImageStat, UnidentifiedImageError

from .db import Database, utcnow
from .normalize import normalize_url


@dataclass(frozen=True)
class MediaRef:
    url: str
    role: str
    json_path: str


MEDIA_WORDS = ("media", "image", "photo", "picture", "profilepic", "avatar", "cover", "video", "attachment", "playable")
GENERIC_URL_KEYS = {"url", "uri", "source", "src", "link", "downloadurl", "sourceurl"}


def media_representation_key(url: str | None) -> str:
    """Identify a downloadable representation without rotating CDN tokens.

    ``normalize_url`` intentionally maps every rendition of one Facebook CDN
    object to the same asset.  Download readiness needs one finer level: a low
    thumbnail cannot satisfy a high-resolution request.  Keep stable rendition
    parameters (for example ``stp``/``ctp``/dimensions), while discarding
    expiry and signature fields.
    """
    value = str(url or "")
    if not value.startswith(("http://", "https://")):
        return value
    parts = urlsplit(value)
    host = (parts.hostname or "").casefold()
    if "fbcdn.net" not in host and not host.startswith("scontent-"):
        return normalize_url(value)
    stable_query = sorted(
        (key, item)
        for key, item in parse_qsl(parts.query, keep_blank_values=True)
        if not key.casefold().startswith("_nc_")
        and key.casefold() not in {"oh", "oe"}
    )
    suffix = urlencode(stable_query)
    return f"facebook-cdn-representation:{parts.path}" + (f"?{suffix}" if suffix else "")


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
    def image_dimensions(path: str | Path | None, mime: str | None) -> tuple[int, int] | None:
        if not path or not str(mime or "").startswith("image/"):
            return None
        try:
            with Image.open(path) as image:
                return ImageOps.exif_transpose(image).size
        except (OSError, UnidentifiedImageError):
            return None

    @classmethod
    def image_quality(
        cls,
        path: str | Path | None,
        mime: str | None,
        size_bytes: int | None = None,
    ) -> tuple[int, int, int]:
        """Return a stable quality ordering led by effective pixel area."""
        dimensions = cls.image_dimensions(path, mime)
        if not dimensions:
            return (0, 0, int(size_bytes or 0))
        width, height = dimensions
        return (width * height, max(width, height), int(size_bytes or 0))

    @staticmethod
    def images_visually_equivalent(
        left: str | Path | None,
        right: str | Path | None,
        *,
        max_mean_difference: float = 8.0,
        max_rms_difference: float = 16.0,
    ) -> bool:
        """Confirm a perceptual-hash candidate before deleting either file.

        Average hashes alone collide frequently (especially for blurred or
        mostly solid thumbnails).  Compare EXIF-corrected RGB pixels at a
        common size and require nearly identical aspect ratios as a second,
        independent check.
        """
        if not left or not right:
            return False
        try:
            with Image.open(left) as left_image, Image.open(right) as right_image:
                left_image = ImageOps.exif_transpose(left_image).convert("RGB")
                right_image = ImageOps.exif_transpose(right_image).convert("RGB")
                left_ratio = left_image.width / max(1, left_image.height)
                right_ratio = right_image.width / max(1, right_image.height)
                if abs(left_ratio - right_ratio) / max(left_ratio, right_ratio, 0.001) > 0.01:
                    return False
                sample_size = (64, 64)
                left_sample = left_image.resize(sample_size, Image.Resampling.LANCZOS)
                right_sample = right_image.resize(sample_size, Image.Resampling.LANCZOS)
                statistics = ImageStat.Stat(ImageChops.difference(left_sample, right_sample))
                mean_difference = sum(statistics.mean) / len(statistics.mean)
                rms_difference = sum(statistics.rms) / len(statistics.rms)
                return (
                    mean_difference <= max_mean_difference
                    and rms_difference <= max_rms_difference
                )
        except (OSError, UnidentifiedImageError, ValueError):
            return False

    @classmethod
    def image_records_equivalent(
        cls,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_hash_distance: int = 2,
    ) -> bool:
        """Return true only for verified representations of one full image.

        CDN object identity and aHash are candidate generators, never deletion
        proof.  Both candidates still have to decode, keep the same aspect
        ratio, and pass the EXIF-corrected RGB comparison.  This deliberately
        rejects crops, blurred placeholders and near-hash collisions.
        """
        if cls.image_quality(
            left.get("path"), left.get("mime_type"), left.get("size_bytes")
        )[0] <= 0 or cls.image_quality(
            right.get("path"), right.get("mime_type"), right.get("size_bytes")
        )[0] <= 0:
            return False

        left_url = normalize_url(str(left.get("source_url") or ""))
        right_url = normalize_url(str(right.get("source_url") or ""))
        same_cdn_object = bool(
            left_url
            and left_url == right_url
            and left_url.startswith("facebook-cdn:")
        )
        close_hash = False
        left_hash = str(left.get("perceptual_hash") or "")
        right_hash = str(right.get("perceptual_hash") or "")
        try:
            close_hash = bool(
                left_hash
                and right_hash
                and (int(left_hash, 16) ^ int(right_hash, 16)).bit_count()
                <= max_hash_distance
            )
        except ValueError:
            close_hash = False
        if not (same_cdn_object or close_hash):
            return False
        return cls.images_visually_equivalent(left.get("path"), right.get("path"))

    @staticmethod
    def perceptual_hash(path: Path, mime: str) -> str | None:
        if not mime.startswith("image/"):
            return None
        try:
            with Image.open(path) as image:
                image = ImageOps.exif_transpose(image).convert("L").resize((8, 8))
                pixels = image.tobytes()
                average = sum(pixels) / len(pixels)
                bits = "".join("1" if value >= average else "0" for value in pixels)
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
