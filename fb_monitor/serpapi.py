from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx


class SerpApiError(RuntimeError):
    pass


class SerpApiQuotaExceeded(SerpApiError):
    def __init__(self, message: str, account: Any | None = None):
        super().__init__(message)
        self.account = account


class SerpApiNoResults(SerpApiError):
    """The provider accepted the search but did not return a profile."""

    def __init__(self, message: str, account: Any, attempts: list[dict[str, str]]):
        super().__init__(message)
        self.account = account
        self.attempts = attempts


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
    attempts: list[dict[str, str]] = field(default_factory=list)
    searches_used: int = 1


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

    async def profile(
        self,
        profile_url: str,
        *,
        aliases: tuple[str, ...] = (),
        max_attempts: int = 2,
        empty_retry_seconds: float = 30,
    ) -> SerpApiProfileResult:
        """Look up a profile through stable identifiers with one bounded retry.

        A Facebook Profile search is charged per request, so this deliberately
        never fans out without a small, explicit attempt limit.  Aliases are
        historical numeric IDs or profile slugs only; a display name is never
        sent as a profile ID because that could match a different person.
        """
        account = await self.account()
        if account.searches_left <= 0:
            raise SerpApiQuotaExceeded("SerpApi 本帳期查詢額度已用完", account)
        candidates = list(dict.fromkeys(value for value in (profile_id_from_url(profile_url), *aliases) if value))
        limit = max(1, min(int(max_attempts), len(candidates) + 1, int(account.searches_left)))
        attempts: list[dict[str, str]] = []
        for index in range(limit):
            # With one known identifier, retry it once after a short delay.
            candidate = candidates[min(index, len(candidates) - 1)]
            try:
                data = await self._get_json(
                    "https://serpapi.com/search.json",
                    {"engine": "facebook_profile", "profile_id": candidate, "api_key": self.api_key},
                )
            except SerpApiQuotaExceeded as exc:
                exc.account = account
                raise
            item = data.get("profile_results")
            if isinstance(item, dict):
                attempts.append({"query": candidate, "status": "success", "error": ""})
                normalized = dict(item)
                if isinstance(normalized.get("photos"), list):
                    normalized["photos"] = normalized["photos"][:6]
                normalized.setdefault("url", profile_url)
                return SerpApiProfileResult(normalized, account, attempts, len(attempts))

            message = str(data.get("error") or "SerpApi Facebook Profile API 未回傳 profile_results")
            if "hasn't returned any results" not in message.casefold() and "no results" not in message.casefold():
                raise SerpApiError(message)
            attempts.append({"query": candidate, "status": "empty", "error": message})
            if index + 1 < limit and empty_retry_seconds > 0:
                await asyncio.sleep(empty_retry_seconds)

        raise SerpApiNoResults(attempts[-1]["error"], account, attempts)

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
