"""Opt-in, bounded free-credit trial. No purchasing or automatic quota reset."""
from __future__ import annotations

import json
from pathlib import Path

import httpx

from .provider_state import ProviderState
from .serpapi import profile_id_from_url

RESERVED_PROFILE_ID = "100027675104517"


def reserved_public_profile(db, profile: dict) -> bool:
    if not profile.get("enabled"):
        return False
    url_id = profile_id_from_url(str(profile.get("url") or ""))
    identity = str(profile.get("fb_id") or url_id)
    if identity != RESERVED_PROFILE_ID or (url_id.isdigit() and url_id != RESERVED_PROFILE_ID):
        return False
    latest = db.row("""SELECT verdict FROM access_observations
        WHERE profile_id=? AND identity_match=1 AND target_fb_id=? AND observed_fb_id=? AND
        (auth_scope='anonymous' OR source='contract_explicit' OR source LIKE 'contract:%')
        ORDER BY observed_at DESC,id DESC LIMIT 1""", (profile["id"], RESERVED_PROFILE_ID, RESERVED_PROFILE_ID))
    return bool(latest and latest["verdict"] == "confirmed_public")


class ScrapeCreatorsError(RuntimeError):
    pass


class ScrapeCreatorsGateway:
    def __init__(self, api_key: str, *, state_path: Path | str, enabled=False,
                 free_only_confirmed=False, trial_limit=5):
        self.api_key = api_key
        self.enabled = enabled
        self.free_only_confirmed = free_only_confirmed
        self.trial_limit = max(0, min(10, int(trial_limit)))
        self.state = ProviderState(state_path)

    async def _request(self, endpoint: str, params: dict) -> dict:
        if not (self.enabled and self.free_only_confirmed and self.api_key):
            raise ScrapeCreatorsError("ScrapeCreators 試用未啟用、未確認免費點數或未設定 API key")
        key = endpoint + ":" + json.dumps(params, sort_keys=True)
        launch, saved = self.state.claim("scrapecreators", key, ttl=86400)
        if not launch:
            if saved["status"] == "done":
                return json.loads(saved["payload"])
            raise ScrapeCreatorsError("ScrapeCreators 上次請求失敗或結果不明；不自動重試")
        if not self.state.reserve_trial("scrapecreators", self.trial_limit):
            self.state.save("scrapecreators", key, "blocked")
            raise ScrapeCreatorsError("ScrapeCreators 本機免費試用上限已達；不啟用付費")
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.get("https://api.scrapecreators.com" + endpoint,
                                            params=params, headers={"x-api-key": self.api_key})
            if response.status_code != 200:
                raise ScrapeCreatorsError(f"ScrapeCreators HTTP {response.status_code}；不自動重試")
            data = response.json()
            if not isinstance(data, dict) or data.get("success") is not True:
                raise ScrapeCreatorsError("ScrapeCreators 未回傳成功結果")
        except (httpx.RequestError, ValueError) as exc:
            raise ScrapeCreatorsError("ScrapeCreators 請求結果不明；保留試用扣點，不重送") from exc
        self.state.save("scrapecreators", key, "done", payload=data)
        return data

    async def profile(self, url: str) -> dict:
        data = await self._request("/v1/facebook/profile", {"url": url})
        if not data.get("id") or not data.get("name"):
            raise ScrapeCreatorsError("ScrapeCreators 個人檔案缺少身份欄位")
        item = {k: v for k, v in data.items() if k not in {"success", "credits_remaining", "credits_charged"}}
        item["profile_data_source"] = "ScrapeCreators"
        item.setdefault("url", url)
        if item.get("profilePicLarge"):
            item["profile_picture"] = item["profilePicLarge"]
        cover = item.get("coverPhoto")
        if isinstance(cover, dict):
            photo = cover.get("photo")
            image = photo.get("image") if isinstance(photo, dict) else None
            if isinstance(image, dict) and image.get("uri"):
                item["cover_photo"] = image["uri"]
        return item

    async def posts(self, url: str, cursor: str | None = None) -> dict:
        params = {"url": url}
        if cursor:
            params["cursor"] = cursor
        data = await self._request("/v1/facebook/profile/posts", params)
        if not isinstance(data.get("posts"), list) or any(not isinstance(p, dict) for p in data["posts"]):
            raise ScrapeCreatorsError("ScrapeCreators 貼文格式不符；保留原始回應")
        # Cursor is provider-specific. An absent cursor does not prove historical completeness.
        return data
