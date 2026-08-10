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


class ApifyGateway:
    def __init__(self, token: str):
        self.token = token
        self.client = ApifyClient(token) if token else None

    async def call(self, actor_id: str, payload: dict[str, Any], max_charge_usd: float | None = None) -> ActorResult:
        if not self.client:
            raise RuntimeError("APIFY_TOKEN 尚未設定")
        return await asyncio.to_thread(self._call_sync, actor_id, payload, max_charge_usd)

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
