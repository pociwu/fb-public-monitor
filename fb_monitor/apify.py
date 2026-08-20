from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from apify_client import ApifyClient


@dataclass(slots=True)
class ActorResult:
    items: list[dict[str, Any]]
    summary: dict[str, Any] | None
    run_id: str
    charged_usd: float = 0.0
    diagnostic_id: int | None = None
    raw_result_count: int | None = None


@dataclass(slots=True)
class MonthlyUsage:
    used_usd: float
    cycle_start_at: str
    cycle_end_at: str


@dataclass(slots=True)
class StartedActor:
    """Identifiers persisted before waiting on a potentially billable run."""

    run_id: str
    dataset_id: str
    key_value_store_id: str


class ApifyGateway:
    def __init__(self, token: str):
        self.token = token
        self.client = ApifyClient(token) if token else None

    async def call(self, actor_id: str, payload: dict[str, Any], max_charge_usd: float | None = None) -> ActorResult:
        if not self.client:
            raise RuntimeError("APIFY_TOKEN 尚未設定")
        return await asyncio.to_thread(self._call_sync, actor_id, payload, max_charge_usd)

    async def start(self, actor_id: str, payload: dict[str, Any], max_charge_usd: float | None = None) -> StartedActor:
        """Start once and return durable identifiers before polling.

        Capture V2 writes these identifiers to SQLite immediately.  If the
        process exits afterwards, it can reconcile the same run instead of
        starting and paying for a duplicate request.
        """
        if not self.client:
            raise RuntimeError("APIFY_TOKEN 尚未設定")
        return await asyncio.to_thread(self._start_sync, actor_id, payload, max_charge_usd)

    async def finish(self, started: StartedActor, timeout_seconds: int = 3600) -> ActorResult:
        if not self.client:
            raise RuntimeError("APIFY_TOKEN 尚未設定")
        return await asyncio.to_thread(self._finish_sync, started, timeout_seconds)

    async def monthly_usage(self) -> MonthlyUsage:
        """Read the same current-cycle usage shown on Apify's Billing page."""
        if not self.client:
            raise RuntimeError("APIFY_TOKEN 未設定")
        return await asyncio.to_thread(self._monthly_usage_sync)

    def _monthly_usage_sync(self) -> MonthlyUsage:
        data = self.client.user("me").monthly_usage()  # type: ignore[union-attr]
        if not isinstance(data, dict):
            raise RuntimeError("Apify 官方用量 API 未回傳資料")
        cycle = data.get("usageCycle")
        if not isinstance(cycle, dict):
            raise RuntimeError("Apify 官方用量缺少帳期資料")
        start_at = cycle.get("startAt")
        end_at = cycle.get("endAt")
        used = data.get("totalUsageCreditsUsdAfterVolumeDiscount")
        if start_at is None or end_at is None or used is None:
            raise RuntimeError("Apify 官方用量回傳格式不完整")
        return MonthlyUsage(
            used_usd=float(used),
            cycle_start_at=start_at.isoformat() if hasattr(start_at, "isoformat") else str(start_at),
            cycle_end_at=end_at.isoformat() if hasattr(end_at, "isoformat") else str(end_at),
        )

    def _call_sync(self, actor_id: str, payload: dict[str, Any], max_charge_usd: float | None) -> ActorResult:
        kwargs: dict[str, Any] = {"run_input": payload, "timeout_secs": 3600}
        if max_charge_usd is not None and max_charge_usd > 0:
            kwargs["max_total_charge_usd"] = Decimal(str(round(max_charge_usd, 4)))
        actor = self.client.actor(actor_id)  # type: ignore[union-attr]
        # Never retry without the charge ceiling: the configured monthly hard cap
        # is more important than completing a run against an outdated SDK.
        run = actor.call(**kwargs)
        if not run:
            raise RuntimeError(f"Actor {actor_id} 未回傳 run 資訊")
        items = self.client.dataset(run["defaultDatasetId"]).list_items(clean=True).items  # type: ignore[union-attr]
        summary = None
        store_id = run.get("defaultKeyValueStoreId")
        if store_id:
            record = self.client.key_value_store(store_id).get_record("SUMMARY")  # type: ignore[union-attr]
            if record:
                summary = record.get("value")
        charged = run.get("usageTotalUsd") or run.get("usageUsd") or 0
        result_items = list(items)
        return ActorResult(
            items=result_items,
            summary=summary,
            run_id=str(run.get("id", "")),
            charged_usd=float(charged),
            raw_result_count=len(result_items),
        )

    def _start_sync(self, actor_id: str, payload: dict[str, Any], max_charge_usd: float | None) -> StartedActor:
        kwargs: dict[str, Any] = {"run_input": payload}
        if max_charge_usd is not None and max_charge_usd > 0:
            kwargs["max_total_charge_usd"] = Decimal(str(round(max_charge_usd, 4)))
        run = self.client.actor(actor_id).start(**kwargs)  # type: ignore[union-attr]
        if not run or not run.get("id"):
            raise RuntimeError(f"Actor {actor_id} 未回傳 run 資訊")
        return StartedActor(
            run_id=str(run["id"]),
            dataset_id=str(run.get("defaultDatasetId") or ""),
            key_value_store_id=str(run.get("defaultKeyValueStoreId") or ""),
        )

    def _finish_sync(self, started: StartedActor, timeout_seconds: int) -> ActorResult:
        run_client = self.client.run(started.run_id)  # type: ignore[union-attr]
        run = run_client.wait_for_finish(wait_secs=max(1, int(timeout_seconds)))
        if not isinstance(run, dict):
            raise RuntimeError(f"Actor run {started.run_id} 未回傳完成狀態")
        status = str(run.get("status") or "").upper()
        if status not in {"SUCCEEDED"}:
            message = run.get("statusMessage") or status or "unknown"
            raise RuntimeError(f"Actor run {started.run_id} 未成功：{message}")
        dataset_id = str(run.get("defaultDatasetId") or started.dataset_id)
        store_id = str(run.get("defaultKeyValueStoreId") or started.key_value_store_id)
        items = self.client.dataset(dataset_id).list_items(clean=True).items if dataset_id else []  # type: ignore[union-attr]
        summary = None
        if store_id:
            record = self.client.key_value_store(store_id).get_record("SUMMARY")  # type: ignore[union-attr]
            if record:
                summary = record.get("value")
        result_items = list(items)
        return ActorResult(
            items=result_items,
            summary=summary,
            run_id=started.run_id,
            charged_usd=float(run.get("usageTotalUsd") or run.get("usageUsd") or 0),
            raw_result_count=len(result_items),
        )
