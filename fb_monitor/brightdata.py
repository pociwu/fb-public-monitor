from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


class BrightDataError(RuntimeError):
    pass


@dataclass(slots=True)
class BrightDataBalance:
    balance: float
    pending_balance: float


class BrightDataGateway:
    API_URL = "https://api.brightdata.com/datasets/v3/scrape"

    def __init__(self, api_token: str, dataset_id: str = "gd_mf0urb782734ik94dz"):
        self.api_token = api_token
        self.dataset_id = dataset_id

    async def balance(self) -> BrightDataBalance:
        if not self.api_token:
            raise BrightDataError("BRIGHTDATA_API_TOKEN 未設定")
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(
                    "https://api.brightdata.com/customer/balance",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                )
        except httpx.RequestError as exc:
            raise BrightDataError(f"Bright Data 餘額查詢連線失敗：{exc.__class__.__name__}") from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise BrightDataError(f"Bright Data 餘額回傳非 JSON 資料（HTTP {response.status_code}）") from exc
        if response.status_code >= 400:
            message = data.get("error") if isinstance(data, dict) else None
            raise BrightDataError(str(message or f"Bright Data 餘額查詢 HTTP {response.status_code}"))
        if not isinstance(data, dict) or not isinstance(data.get("balance"), (int, float)):
            raise BrightDataError("Bright Data 餘額回傳格式不正確")
        return BrightDataBalance(float(data["balance"]), float(data.get("pending_balance") or 0))

    async def profile(self, profile_url: str) -> dict[str, Any]:
        if not self.api_token:
            raise BrightDataError("BRIGHTDATA_API_TOKEN 未設定")
        try:
            async with httpx.AsyncClient(timeout=90) as client:
                response = await client.post(
                    self.API_URL,
                    params={"dataset_id": self.dataset_id, "include_errors": "true"},
                    headers={
                        "Authorization": f"Bearer {self.api_token}",
                        "Content-Type": "application/json",
                    },
                    json={"input": [{"url": profile_url}]},
                )
        except httpx.RequestError as exc:
            raise BrightDataError(f"Bright Data 連線失敗：{exc.__class__.__name__}") from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise BrightDataError(f"Bright Data 回傳非 JSON 資料（HTTP {response.status_code}）") from exc
        if response.status_code >= 400:
            message = data.get("error") if isinstance(data, dict) else None
            raise BrightDataError(str(message or f"Bright Data HTTP {response.status_code}"))

        rows = data if isinstance(data, list) else data.get("data") if isinstance(data, dict) else None
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            raise BrightDataError("Bright Data Facebook Profiles API 未回傳資料")
        row = rows[0]
        error = row.get("error") or row.get("error_message")
        if error:
            raise BrightDataError(str(error))
        if not any(row.get(key) for key in ("id", "name", "url", "profile_photo")):
            raise BrightDataError("Bright Data Facebook Profiles API 回傳空白個人檔案")
        return normalize_brightdata_profile(row, profile_url)


def normalize_brightdata_profile(row: dict[str, Any], profile_url: str) -> dict[str, Any]:
    """Map Bright Data's profile schema to the fields used by the dashboard."""
    item = dict(row)
    aliases = {
        "profile_photo": "profile_picture",
        "profile_pic": "profile_picture",
        "cover_image": "cover_photo",
        "bio": "profile_intro_text",
        "introduction": "profile_intro_text",
        "city": "current_city",
        "location": "current_city",
        "followers_count": "followers",
    }
    for source, target in aliases.items():
        if item.get(source) not in (None, "", [], {}) and not item.get(target):
            item[target] = item[source]

    if not item.get("works") and item.get("work"):
        values = item["work"] if isinstance(item["work"], list) else [item["work"]]
        item["works"] = [value if isinstance(value, dict) else {"title": str(value)} for value in values]

    if not item.get("educations"):
        education: list[dict[str, Any]] = []
        for key in ("college", "high_school"):
            value = item.get(key)
            if not value:
                continue
            values = value if isinstance(value, list) else [value]
            education.extend(entry if isinstance(entry, dict) else {"title": str(entry)} for entry in values)
        if education:
            item["educations"] = education

    if isinstance(item.get("photos"), list):
        item["photos"] = item["photos"][:6]
    item.setdefault("url", profile_url)
    item["profile_data_source"] = "Bright Data"
    return item
