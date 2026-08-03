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
    assert settings.browser_canary_max_photos_per_post == 9
    assert settings.browser_canary_cooldown_hours == 72


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
