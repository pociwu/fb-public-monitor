"""Read-only task diagnostics; never include locals, payloads or credentials."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

log = logging.getLogger(__name__)


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
        log.error("[DEBUG-runtime] background task failed: %s", snapshot)

