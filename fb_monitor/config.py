from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

MAX_PROFILES = 16


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
