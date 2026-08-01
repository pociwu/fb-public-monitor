from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

import yaml

MAX_PROFILES = 16
_CONFIG_WRITE_LOCK = threading.Lock()
_RESERVED_FACEBOOK_PATHS = {
    "events", "groups", "marketplace", "permalink.php", "photo", "photos",
    "posts", "reel", "reels", "share", "story.php", "watch",
}


@dataclass(slots=True)
class ProfileConfig:
    name: str
    url: str
    enabled: bool = True


@dataclass(slots=True)
class ActorConfig:
    profile: str = "apify/facebook-pages-scraper"
    posts: str = "unseenuser/fb-profile"
    comments: str = "apify/facebook-comments-scraper"
    profile_input: dict[str, Any] = field(default_factory=dict)
    posts_input: dict[str, Any] = field(default_factory=dict)
    comments_input: dict[str, Any] = field(default_factory=dict)


def normalize_profile_url(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError("請輸入 Facebook 個人檔案網址")
    parsed = urlsplit(candidate)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not (host == "facebook.com" or host.endswith(".facebook.com")):
        raise ValueError("網址必須是 https://www.facebook.com/ 的個人檔案網址")
    path = parsed.path.rstrip("/")
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        raise ValueError("Facebook 網址缺少個人檔案識別名稱或 ID")
    first = segments[0].lower()
    query = ""
    if first == "profile.php":
        profile_id = (parse_qs(parsed.query).get("id") or [""])[0]
        if not profile_id.isdigit():
            raise ValueError("profile.php 網址必須包含數字 id")
        path = "/profile.php"
        query = f"id={profile_id}"
    elif first == "people":
        if len(segments) != 3 or not segments[-1].isdigit():
            raise ValueError("Facebook people 網址格式不正確")
    elif first in _RESERVED_FACEBOOK_PATHS or len(segments) != 1:
        raise ValueError("請輸入個人檔案首頁網址，不要使用貼文、社團或影音網址")
    return urlunsplit(("https", "www.facebook.com", path, query, ""))


def profile_name_from_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.path.lower() == "/profile.php":
        return f"FB-{(parse_qs(parsed.query).get('id') or ['profile'])[0]}"
    return parsed.path.rstrip("/").split("/")[-1]


def _write_config(path: Path, raw: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        yaml.safe_dump(raw, handle, allow_unicode=True, sort_keys=False)
        handle.flush()
        os.fsync(handle.fileno())


def add_profile_to_config(path: Path, url: str) -> ProfileConfig:
    normalized = normalize_profile_url(url)
    with _CONFIG_WRITE_LOCK:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        profiles = list(raw.get("profiles") or [])
        existing_urls = {normalize_profile_url(str(item.get("url") or "")) for item in profiles}
        if normalized in existing_urls:
            raise ValueError("此 Facebook 網址已在監控名單中")
        if len(profiles) >= MAX_PROFILES:
            raise ValueError(f"監控人數已達上限 {MAX_PROFILES} 人")
        item = {"name": profile_name_from_url(normalized), "url": normalized, "enabled": True}
        profiles.append(item)
        raw["profiles"] = profiles
        _write_config(path, raw)
    return ProfileConfig(**item)


def remove_profile_from_config(path: Path, url: str) -> bool:
    target = normalize_profile_url(url)
    with _CONFIG_WRITE_LOCK:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        profiles = list(raw.get("profiles") or [])
        kept = [item for item in profiles if normalize_profile_url(str(item.get("url") or "")) != target]
        if len(kept) == len(profiles):
            return False
        raw["profiles"] = kept
        _write_config(path, raw)
    return True


@dataclass(slots=True)
class Settings:
    config_path: Path
    data_dir: Path
    timezone: str = "Asia/Taipei"
    visit_min_hours: float = 6
    visit_max_hours: float = 8
    spacing_min_minutes: float = 20
    spacing_max_minutes: float = 30
    recent_posts: int = 10
    backfill_posts: int = 20
    full_audit_days: int = 7
    serpapi_profile_refresh_hours: float = 48
    low_disk_gb: float = 10
    media_retry_days: int = 30
    monthly_budget_usd: float = 5
    budget_warning_ratio: float = 0.8
    health_hour: int = 8
    telegram_coalesce_minutes: int = 15
    telegram_send_interval_seconds: int = 3
    telegram_high_water: int = 50
    telegram_max_attempts: int = 5
    telegram_retry_hours: int = 24
    web_host: str = "127.0.0.1"
    web_port: int = 8080
    actors: ActorConfig = field(default_factory=ActorConfig)
    profiles: list[ProfileConfig] = field(default_factory=list)
    apify_token: str = ""
    serpapi_key: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    scheduler_enabled: bool = True

    @property
    def db_path(self) -> Path:
        return self.data_dir / "monitor.sqlite3"


def _merge(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    result = dict(base)
    if override:
        result.update(override)
    return result


def load_settings(path: str | Path | None = None) -> Settings:
    config_path = Path(path or os.getenv("FB_MONITOR_CONFIG", "config.yaml")).resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    schedule = raw.get("schedule", {})
    storage = raw.get("storage", {})
    budget = raw.get("budget", {})
    web = raw.get("web", {})
    telegram = raw.get("telegram", {})
    actors_raw = raw.get("actors", {})
    data_dir = Path(os.getenv("FB_MONITOR_DATA_DIR", storage.get("data_dir", "data")))
    if not data_dir.is_absolute():
        data_dir = (config_path.parent / data_dir).resolve()
    profiles = [ProfileConfig(**item) for item in raw.get("profiles", [])]
    if len(profiles) > MAX_PROFILES:
        raise ValueError(f"profiles 最多只能設定 {MAX_PROFILES} 個")
    urls = [p.url.rstrip("/") for p in profiles]
    if len(set(urls)) != len(urls):
        raise ValueError("profiles 不可包含重複網址")
    actor_defaults = ActorConfig()
    actors = ActorConfig(
        profile=actors_raw.get("profile", actor_defaults.profile),
        posts=actors_raw.get("posts", actor_defaults.posts),
        comments=actors_raw.get("comments", actor_defaults.comments),
        profile_input=actors_raw.get("profile_input", {}),
        posts_input=actors_raw.get("posts_input", {}),
        comments_input=actors_raw.get("comments_input", {}),
    )
    return Settings(
        config_path=config_path,
        data_dir=data_dir,
        timezone=raw.get("timezone", "Asia/Taipei"),
        visit_min_hours=float(schedule.get("visit_min_hours", 6)),
        visit_max_hours=float(schedule.get("visit_max_hours", 8)),
        spacing_min_minutes=float(schedule.get("spacing_min_minutes", 20)),
        spacing_max_minutes=float(schedule.get("spacing_max_minutes", 30)),
        recent_posts=int(schedule.get("recent_posts", 10)),
        backfill_posts=int(schedule.get("backfill_posts", 20)),
        full_audit_days=int(schedule.get("full_audit_days", 7)),
        serpapi_profile_refresh_hours=float(schedule.get("serpapi_profile_refresh_hours", 48)),
        low_disk_gb=float(storage.get("low_disk_gb", 10)),
        media_retry_days=int(storage.get("media_retry_days", 30)),
        monthly_budget_usd=float(budget.get("monthly_usd", 5)),
        budget_warning_ratio=float(budget.get("warning_ratio", 0.8)),
        health_hour=int(telegram.get("health_hour", 8)),
        telegram_coalesce_minutes=int(telegram.get("coalesce_minutes", 15)),
        telegram_send_interval_seconds=int(telegram.get("send_interval_seconds", 3)),
        telegram_high_water=int(telegram.get("high_water", 50)),
        telegram_max_attempts=int(telegram.get("max_attempts", 5)),
        telegram_retry_hours=int(telegram.get("retry_hours", 24)),
        web_host=str(web.get("host", "127.0.0.1")),
        web_port=int(web.get("port", 8080)),
        actors=actors,
        profiles=profiles,
        apify_token=os.getenv("APIFY_TOKEN", ""),
        serpapi_key=os.getenv("SERPAPI_KEY", ""),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        scheduler_enabled=os.getenv("FB_MONITOR_SCHEDULER", "1") not in {"0", "false", "False"},
    )


def actor_input(template: dict[str, Any], **values: Any) -> dict[str, Any]:
    """Recursively replace {name} placeholders while retaining native list values."""
    def render(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
            key = value[1:-1]
            return values.get(key, value)
        if isinstance(value, str):
            try:
                return value.format(**values)
            except (KeyError, ValueError):
                return value
        if isinstance(value, list):
            return [render(item) for item in value]
        if isinstance(value, dict):
            return {key: render(item) for key, item in value.items()}
        return value
    return render(template)
