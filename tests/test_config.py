from pathlib import Path

import pytest

from fb_monitor.config import (
    actor_input,
    add_profile_to_config,
    load_settings,
    normalize_profile_url,
    remove_profile_from_config,
)


def test_actor_input_keeps_native_values():
    result = actor_input({"profileUrls": "{urls}", "limit": "{limit}"}, urls=["a", "b"], limit=20)
    assert result == {"profileUrls": ["a", "b"], "limit": 20}


def test_browser_canary_defaults_are_conservative(tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\n", encoding="utf-8")

    settings = load_settings(config)

    assert settings.browser_canary_enabled is True
    assert settings.browser_canary_max_posts == 2
    assert settings.browser_canary_cooldown_hours == 72
    assert settings.capture_v2_enabled is False
    assert settings.apify_v1_backfill_enabled is False
    assert settings.recent_posts == 20
    assert settings.full_audit_days == 30
    assert settings.low_disk_gb == 30
    assert settings.browser_album_operations == 20
    assert settings.evidence_cap_bytes == 500 * 1024 * 1024


def test_capture_v2_and_browser_guard_settings(tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles: []
capture_v2:
  enabled: true
  v1_backfill_enabled: false
  special_profile_id: "123"
  contract_test_budget_usd: 0.19
  contract_test_grant_hours: 12
browser_guard:
  account_min_minutes: 40
  account_max_minutes: 70
  album_operations: 99
  batch_seconds: 999
evidence:
  retention_days: 90
  cap_mib: 321
actors:
  posts_v2_primary: example/primary
  posts_v2_fallback: example/fallback
""",
        encoding="utf-8",
    )

    settings = load_settings(config)

    assert settings.capture_v2_enabled is True
    assert settings.apify_v1_backfill_enabled is False
    assert settings.special_profile_id == "123"
    assert settings.actor_contract_test_budget_usd == pytest.approx(0.19)
    assert settings.actor_contract_test_grant_hours == pytest.approx(12)
    assert settings.browser_account_min_minutes == 40
    assert settings.browser_account_max_minutes == 70
    assert settings.browser_album_operations == 20
    assert settings.browser_batch_seconds == 180
    assert settings.evidence_retention_days == 90
    assert settings.evidence_cap_bytes == 321 * 1024 * 1024
    assert settings.actors.posts_v2_primary == "example/primary"
    assert settings.actors.posts_v2_fallback == "example/fallback"


def test_posts_cursor_contract_round_has_hard_twenty_cent_cap(tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles: []\ncapture_v2:\n  contract_test_budget_usd: 9.99\n",
        encoding="utf-8",
    )

    assert load_settings(config).actor_contract_test_budget_usd == pytest.approx(0.20)


@pytest.mark.parametrize(
    ("capture_v2", "v1_backfill"),
    [("1", "1"), ("true", "yes")],
)
def test_config_rejects_capture_v2_and_v1_backfill_enabled_together(
    tmp_path: Path, monkeypatch, capture_v2: str, v1_backfill: str
):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\n", encoding="utf-8")
    monkeypatch.setenv("CAPTURE_V2_ENABLED", capture_v2)
    monkeypatch.setenv("APIFY_V1_BACKFILL_ENABLED", v1_backfill)

    with pytest.raises(ValueError, match="不可同時啟用"):
        load_settings(config)


def test_yaml_rejects_capture_v2_and_v1_backfill_enabled_together(
    tmp_path: Path, monkeypatch
):
    config = tmp_path / "config.yaml"
    config.write_text(
        "profiles: []\ncapture_v2:\n  enabled: true\n  v1_backfill_enabled: true\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("CAPTURE_V2_ENABLED", raising=False)
    monkeypatch.delenv("APIFY_V1_BACKFILL_ENABLED", raising=False)

    with pytest.raises(ValueError, match="不可同時啟用"):
        load_settings(config)


def test_config_rejects_more_than_sixteen(tmp_path: Path):
    config = tmp_path / "config.yaml"
    profiles = "\n".join(f"  - name: p{i}\n    url: https://facebook.com/{i}" for i in range(17))
    config.write_text(f"profiles:\n{profiles}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="最多"):
        load_settings(config)


def test_dashboard_profile_config_mutations_validate_and_preserve_settings(tmp_path: Path):
    config = tmp_path / "config.yaml"
    config.write_text(
        """timezone: Asia/Taipei
profiles: []
schedule:
  recent_posts: 10
""",
        encoding="utf-8",
    )

    added = add_profile_to_config(config, "https://m.facebook.com/profile.php?id=12345&utm_source=test")

    assert added.url == "https://www.facebook.com/profile.php?id=12345"
    assert load_settings(config).recent_posts == 10
    assert load_settings(config).profiles[0].url == added.url
    with pytest.raises(ValueError, match="已在監控"):
        add_profile_to_config(config, "http://facebook.com/profile.php?id=12345")
    with pytest.raises(ValueError, match="個人檔案首頁"):
        normalize_profile_url("https://www.facebook.com/12345/posts/67890")

    assert remove_profile_from_config(config, added.url)
    assert load_settings(config).profiles == []
    assert not remove_profile_from_config(config, added.url)
