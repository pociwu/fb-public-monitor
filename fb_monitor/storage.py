from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from .db import Database, utcnow


IMAGE_EXTENSIONS = {".avif", ".bmp", ".gif", ".heic", ".jpeg", ".jpg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".webm"}
STORAGE_CATEGORIES = (
    ("image_bytes", "圖片"),
    ("video_bytes", "影片"),
    ("attachment_bytes", "其他附件"),
    ("database_bytes", "資料庫"),
    ("content_bytes", "JSON／Markdown 歷史"),
    ("cache_bytes", "縮圖快取"),
    ("browser_bytes", "Chromium 資料"),
)


def format_bytes(value: int | float | None, signed: bool = False) -> str:
    amount = int(value or 0)
    prefix = ""
    if signed:
        prefix = "+" if amount > 0 else "-" if amount < 0 else ""
    size = abs(amount)
    units = ("B", "KB", "MB", "GB", "TB")
    number = float(size)
    unit = units[0]
    for candidate in units:
        unit = candidate
        if number < 1024 or candidate == units[-1]:
            break
        number /= 1024
    precision = 0 if unit == "B" else 1
    return f"{prefix}{number:.{precision}f} {unit}"


def _files(root: Path):
    if not root.is_dir():
        return
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            yield Path(entry.path), entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue


def directory_size(root: Path) -> int:
    return sum(size for _, size in _files(root) or ())


def _media_sizes(db: Database, media_root: Path) -> tuple[int, int, int]:
    mime_by_path: dict[str, str] = {}
    for row in db.rows("SELECT path,mime_type FROM media WHERE path IS NOT NULL"):
        try:
            mime_by_path[str(Path(row["path"]).resolve())] = str(row.get("mime_type") or "").lower()
        except (OSError, TypeError):
            continue
    images = videos = attachments = 0
    for path, size in _files(media_root) or ():
        try:
            mime = mime_by_path.get(str(path.resolve()), "")
        except OSError:
            mime = ""
        suffix = path.suffix.lower()
        if mime.startswith("image/") or (not mime and suffix in IMAGE_EXTENSIONS):
            images += size
        elif mime.startswith("video/") or (not mime and suffix in VIDEO_EXTENSIONS):
            videos += size
        else:
            attachments += size
    return images, videos, attachments


def collect_storage_snapshot(db: Database, data_dir: Path, browser_dir: Path, snapshot_date: str) -> dict[str, Any]:
    data_dir = data_dir.resolve()
    browser_dir = browser_dir.resolve()
    image_bytes, video_bytes, attachment_bytes = _media_sizes(db, data_dir / "media")
    database_bytes = sum(
        path.stat().st_size
        for path in (db.path, Path(f"{db.path}-wal"), Path(f"{db.path}-shm"))
        if path.is_file()
    )
    content_bytes = directory_size(data_dir / "profiles")
    cache_bytes = directory_size(data_dir / "cache")
    browser_bytes = directory_size(browser_dir)
    usage = shutil.disk_usage(data_dir)
    values: dict[str, Any] = {
        "snapshot_date": snapshot_date,
        "captured_at": utcnow(),
        "image_bytes": image_bytes,
        "video_bytes": video_bytes,
        "attachment_bytes": attachment_bytes,
        "database_bytes": database_bytes,
        "content_bytes": content_bytes,
        "cache_bytes": cache_bytes,
        "browser_bytes": browser_bytes,
        # Retained for schema compatibility with the first storage release.
        # Host/Docker usage is deliberately not attributed to this project.
        "other_bytes": 0,
        "filesystem_used_bytes": usage.used,
        "filesystem_total_bytes": usage.total,
        "filesystem_free_bytes": usage.free,
    }
    columns = ",".join(values)
    placeholders = ",".join("?" for _ in values)
    updates = ",".join(f"{column}=excluded.{column}" for column in values if column != "snapshot_date")
    db.execute(
        f"INSERT INTO storage_snapshots({columns}) VALUES({placeholders}) "
        f"ON CONFLICT(snapshot_date) DO UPDATE SET {updates}",
        tuple(values.values()),
    )
    return values


def storage_delta(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, int] | None:
    if not previous:
        return None
    result = {
        key: int(current.get(key) or 0) - int(previous.get(key) or 0)
        for key, _ in STORAGE_CATEGORIES
    }
    result["project_used_bytes"] = project_used_bytes(current) - project_used_bytes(previous)
    return result


def project_used_bytes(snapshot: dict[str, Any]) -> int:
    return sum(int(snapshot.get(key) or 0) for key, _ in STORAGE_CATEGORIES)


def decorate_snapshot(snapshot: dict[str, Any], previous: dict[str, Any] | None = None) -> dict[str, Any]:
    row = dict(snapshot)
    delta = storage_delta(row, previous)
    project_bytes = project_used_bytes(row)
    row["used_display"] = format_bytes(project_bytes)
    row["total_display"] = format_bytes(row.get("filesystem_total_bytes"))
    row["free_display"] = format_bytes(row.get("filesystem_free_bytes"))
    row["used_percent"] = round(
        100 * project_bytes / max(1, int(row.get("filesystem_total_bytes") or 0)), 1
    )
    row["categories"] = [
        {
            "key": key,
            "label": label,
            "bytes": int(row.get(key) or 0),
            "display": format_bytes(row.get(key)),
            "delta": delta.get(key, 0) if delta else None,
            "delta_display": format_bytes(delta.get(key, 0), signed=True) if delta else "建立基準",
            "percent": round(100 * int(row.get(key) or 0) / max(1, project_bytes), 2),
        }
        for key, label in STORAGE_CATEGORIES
    ]
    row["used_delta"] = delta.get("project_used_bytes") if delta else None
    row["used_delta_display"] = format_bytes(delta.get("project_used_bytes", 0), signed=True) if delta else "建立基準"
    return row


def daily_storage_message(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, str]:
    decorated = decorate_snapshot(current, previous)
    lines = [
        f"fb-public-monitor 專案總用量：{decorated['used_display']}",
        f"主機剩餘空間：{decorated['free_display']}",
        f"每日增加量：{decorated['used_delta_display']}",
        "",
    ]
    lines.extend(f"{item['label']}：{item['display']}（{item['delta_display']}）" for item in decorated["categories"])
    return {"title": f"【硬碟每日用量】{current['snapshot_date']}", "text": "\n".join(lines)}
