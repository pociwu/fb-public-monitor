from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx


class SerpApiError(RuntimeError):
    pass


class SerpApiQuotaExceeded(SerpApiError):
    def __init__(self, message: str, account: Any | None = None):
        super().__init__(message)
        self.account = account


@dataclass(slots=True)
class SerpApiAccount:
    plan_name: str
    searches_per_month: int
    searches_left: int
    this_month_usage: int
    renewal_date: str | None
    this_hour_searches: int
    rate_limit_per_hour: int


@dataclass(slots=True)
class SerpApiProfileResult:
    item: dict[str, Any]
    account: SerpApiAccount


def profile_id_from_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.path.rstrip("/").lower() == "/profile.php":
        profile_id = (parse_qs(parsed.query).get("id") or [""])[0]
    else:
        parts = [part for part in parsed.path.split("/") if part]
        profile_id = parts[-1] if parts else ""
    if not profile_id:
        raise SerpApiError("Facebook 網址缺少可用的個人檔案 ID")
    return profile_id


class SerpApiGateway:
    def __init__(self, api_key: str):
        self.api_key = api_key

    async def account(self) -> SerpApiAccount:
        if not self.api_key:
            raise SerpApiError("SERPAPI_KEY 未設定")
        data = await self._get_json("https://serpapi.com/account.json", {"api_key": self.api_key})
        return SerpApiAccount(
            plan_name=str(data.get("plan_name") or ""),
            searches_per_month=int(data.get("searches_per_month") or 0),
            searches_left=int(data.get("total_searches_left", data.get("plan_searches_left")) or 0),
            this_month_usage=int(data.get("this_month_usage") or 0),
            renewal_date=str(data["plan_renewal_date"]) if data.get("plan_renewal_date") else None,
            this_hour_searches=int(data.get("this_hour_searches") or 0),
            rate_limit_per_hour=int(data.get("account_rate_limit_per_hour") or 0),
        )

    async def profile(self, profile_url: str) -> SerpApiProfileResult:
        account = await self.account()
        if account.searches_left <= 0:
            raise SerpApiQuotaExceeded("SerpApi 本帳期查詢額度已用完", account)
        try:
            data = await self._get_json(
                "https://serpapi.com/search.json",
                {"engine": "facebook_profile", "profile_id": profile_id_from_url(profile_url), "api_key": self.api_key},
            )
        except SerpApiQuotaExceeded as exc:
            exc.account = account
            raise
        item = data.get("profile_results")
        if not isinstance(item, dict):
            raise SerpApiError(str(data.get("error") or "SerpApi Facebook Profile API 未回傳 profile_results"))
        normalized = dict(item)
        if isinstance(normalized.get("photos"), list):
            normalized["photos"] = normalized["photos"][:6]
        normalized.setdefault("url", profile_url)
        return SerpApiProfileResult(normalized, account)

    async def _get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.get(url, params=params)
        except httpx.RequestError as exc:
            raise SerpApiError(f"SerpApi 連線失敗：{exc.__class__.__name__}") from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise SerpApiError(f"SerpApi 回傳非 JSON 資料（HTTP {response.status_code}）") from exc
        if response.status_code == 429:
            raise SerpApiQuotaExceeded(str(data.get("error") or "SerpApi 額度已用完或超過每小時上限"))
        if response.status_code >= 400:
            raise SerpApiError(str(data.get("error") or f"SerpApi HTTP {response.status_code}"))
        if not isinstance(data, dict):
            raise SerpApiError("SerpApi 回傳格式不正確")
        return data
