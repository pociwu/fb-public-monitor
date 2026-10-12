"""Read-only task diagnostics; never include locals, payloads or credentials."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

log = logging.getLogger(__name__)

WORKER_NAMES = (
    "_scheduler_loop", "_outbox_loop", "_health_loop", "_media_retry_loop",
    "_daily_media_dedupe_loop", "_storage_snapshot_loop",
    "_browser_evidence_cleanup_loop", "_capture_raw_cleanup_loop",
)


def background_health(service, task: asyncio.Task | None) -> str | None:
    if service.stop_event.is_set():
        return "stopping"
    if task is None or task.done():
        return "background_service_stopped"
    if getattr(service, "startup_phase", None) != "workers_started":
        return "initializing"
    workers = getattr(service, "worker_tasks", [])
    if {worker.get_name() for worker in workers} != set(WORKER_NAMES):
        return "background_workers_missing"
    if any(worker.done() for worker in workers):
        return "background_worker_stopped"
    return None


def task_snapshot(task: asyncio.Task | None) -> dict:
    if task is None:
        return {"state": "not_started"}
    result = {"name": task.get_name(), "state": "running", "frames": []}
    if task.cancelled():
        result["state"] = "cancelled"
        return result
    if task.done():
        error = task.exception()
        result["state"] = "failed" if error else "completed"
        if error:
            result["exception_type"] = type(error).__name__
            tb = error.__traceback__
            while tb and len(result["frames"]) < 30:
                result["frames"].append(_frame_location(tb.tb_frame, tb.tb_lineno))
                tb = tb.tb_next
        return result
    coroutine = task.get_coro()
    for _ in range(30):
        frame = getattr(coroutine, "cr_frame", None) or getattr(coroutine, "gi_frame", None)
        if frame:
            result["frames"].append(_frame_location(frame, frame.f_lineno))
        coroutine = getattr(coroutine, "cr_await", None) or getattr(coroutine, "gi_yieldfrom", None)
        if coroutine is None:
            break
    return result


def _frame_location(frame, lineno):
    return {"file": Path(frame.f_code.co_filename).name, "function": frame.f_code.co_name, "line": lineno}


def report_task_exit(task: asyncio.Task) -> None:
    snapshot = task_snapshot(task)
    if snapshot["state"] == "failed":
        log.error("background task failed: %s", snapshot)

