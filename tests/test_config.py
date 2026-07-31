from pathlib import Path

import pytest

from fb_monitor.config import actor_input, load_settings


def test_actor_input_keeps_native_values():
    result = actor_input({"profileUrls": "{urls}", "limit": "{limit}"}, urls=["a", "b"], limit=20)
    assert result == {"profileUrls": ["a", "b"], "limit": 20}


def test_config_rejects_more_than_sixteen(tmp_path: Path):
    config = tmp_path / "config.yaml"
    profiles = "\n".join(f"  - name: p{i}\n    url: https://facebook.com/{i}" for i in range(17))
    config.write_text(f"profiles:\n{profiles}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="最多"):
        load_settings(config)
