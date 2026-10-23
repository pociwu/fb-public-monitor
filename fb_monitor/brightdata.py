from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from .provider_state import ProviderState


class BrightDataError(RuntimeError):
    pass


class BrightDataPending(BrightDataError):
    """Accepted request; next visit resumes the same snapshot, without relaunch."""


class BrightDataGateway:
    API_URL = "https://api.brightdata.com/datasets/v3/scrape"

    def __init__(self, api_token: str, dataset_id: str = "gd_mf0urb782734ik94dz", *, state_path: Path | str = ":memory:"):
        self.api_token = api_token
        self.dataset_id = dataset_id
        self.state = ProviderState(state_path)

    async def profile(self, profile_url: str) -> dict[str, Any]:
        if not self.api_token:
            raise BrightDataError("BRIGHTDATA_API_TOKEN 未設定")
        key = self.dataset_id + ":" + profile_url.rstrip("/")
        launch, saved = self.state.claim("brightdata", key)
        if not launch:
            if saved["status"] == "done":
                return json.loads(saved["payload"])
            if saved["status"] == "failed":
                raise BrightDataError("Bright Data 前次請求已失敗；需人工核對，不重新提交")
            if saved["remote_id"]:
                return await self._snapshot(key, saved["remote_id"], profile_url)
            raise BrightDataError("Bright Data 提交結果待人工核對；已阻止重複提交")
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
            self.state.save("brightdata", key, "failed")
            raise BrightDataError(f"Bright Data HTTP {response.status_code}；需核對後重試")

        if response.status_code == 202 or (isinstance(data, dict) and data.get("snapshot_id")):
            snapshot = data.get("snapshot_id") if isinstance(data, dict) else None
            if not isinstance(snapshot, str) or not snapshot:
                raise BrightDataError("Bright Data 非同步回應缺少 snapshot_id；需人工核對")
            self.state.save("brightdata", key, "pending", snapshot)
            return await self._snapshot(key, snapshot, profile_url)
        result = self._normalize(data, profile_url)
        self.state.save("brightdata", key, "done", payload=result)
        return result

    async def _snapshot(self, key: str, snapshot: str, profile_url: str) -> dict[str, Any]:
        base = "https://api.brightdata.com/datasets/v3"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.get(f"{base}/progress/{quote(snapshot, safe='')}", headers=headers)
                if response.status_code != 200:
                    raise BrightDataPending(f"Bright Data snapshot 查詢暫不可用（HTTP {response.status_code}）；保留續查")
                progress = response.json()
                status = progress.get("status") if isinstance(progress, dict) else None
                if status == "failed":
                    self.state.save("brightdata", key, "failed", snapshot)
                    raise BrightDataError("Bright Data snapshot 失敗；需人工核對")
                if status != "ready":
                    raise BrightDataPending("Bright Data snapshot 尚在處理；下次續查，不重新提交")
                response = await client.get(f"{base}/snapshot/{quote(snapshot, safe='')}", headers=headers, params={"format": "json"})
                if response.status_code != 200:
                    raise BrightDataPending("Bright Data snapshot 尚未可下載；保留續查")
                result = self._normalize(response.json(), profile_url)
        except (httpx.RequestError, ValueError) as exc:
            raise BrightDataPending("Bright Data snapshot 讀取暫時失敗；保留續查") from exc
        self.state.save("brightdata", key, "done", snapshot, result)
        return result

    @staticmethod
    def _normalize(data: Any, profile_url: str) -> dict[str, Any]:
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
