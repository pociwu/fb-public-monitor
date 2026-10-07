import asyncio
from types import SimpleNamespace

import httpx
import pytest

from fb_monitor.config import load_settings
from fb_monitor.runtime_diagnostics import report_task_exit, task_snapshot
from fb_monitor.web import create_app


@pytest.mark.asyncio
async def test_failure_is_visible_without_exception_secrets(caplog):
    async def fail():
        secret = "secret-token-should-not-appear"
        raise RuntimeError(secret)

    task = asyncio.create_task(fail(), name="worker-test")
    with pytest.raises(RuntimeError):
        await task
    report_task_exit(task)
    snapshot = task_snapshot(task)
    assert snapshot["state"] == "failed"
    assert snapshot["exception_type"] == "RuntimeError"
    assert snapshot["frames"][-1]["function"] == "fail"
    assert "secret-token" not in str(snapshot) + caplog.text


@pytest.mark.asyncio
async def test_waiting_task_shows_await_location():
    event = asyncio.Event()
    async def wait_worker():
        await event.wait()
    task = asyncio.create_task(wait_worker())
    await asyncio.sleep(0)
    snapshot = task_snapshot(task)
    assert snapshot["state"] == "running"
    assert snapshot["frames"][0]["function"] == "wait_worker"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task_snapshot(task)["state"] == "cancelled"


@pytest.mark.asyncio
async def test_runtime_endpoint_is_local_only(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("profiles: []\n", encoding="utf-8")
    monkeypatch.setenv("FB_MONITOR_SCHEDULER", "1")
    stop = asyncio.Event()
    service = SimpleNamespace(db=None, stop_event=stop, start=stop.wait, stop=stop.set)
    monkeypatch.setattr("fb_monitor.web.MonitorService", lambda cfg: service)
    app = create_app(load_settings(config))
    async with app.router.lifespan_context(app):
        for host, status in [("127.0.0.1", 200), ("100.120.200.1", 403)]:
            transport = httpx.ASGITransport(app=app, client=(host, 1234))
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/diagnostics/runtime")
                assert response.status_code == status
                if status == 200:
                    assert response.json()["service"]["state"] == "running"
                    assert response.json()["scheduler_enabled"] is True
