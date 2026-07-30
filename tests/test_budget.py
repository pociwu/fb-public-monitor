from pathlib import Path

from fb_monitor.config import load_settings
from fb_monitor.service import MonitorService


def test_budget_reserves_profile_checks(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text(
        """profiles:
  - name: one
    url: https://facebook.com/one
  - name: two
    url: https://facebook.com/two
storage:
  data_dir: data
budget:
  monthly_usd: 5
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "0")
    service = MonitorService(load_settings(config))
    assert service._available_for("profile") == 5
    assert 0 < service._available_for("posts") < 5
