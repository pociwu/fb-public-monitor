"""Opt-in, bounded free-credit trial. No purchasing or automatic quota reset."""
from __future__ import annotations

import json
import time
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
                 free_only_confirmed=False, trial_limit=5, credit_reserve=10):
        self.api_key = api_key
        self.enabled = enabled
        self.free_only_confirmed = free_only_confirmed
        self.trial_limit = max(0, min(90, int(trial_limit)))
        self.credit_reserve = max(10, int(credit_reserve))
        self.state = ProviderState(state_path)
        self._initialize_budget()

    @staticmethod
    def _credit_number(value):
        return value if type(value) is int and value >= 0 else None

    def _initialize_budget(self):
        """Migrate saved provider receipts, never reset an existing budget."""
        conn = self.state.conn
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("""CREATE TABLE IF NOT EXISTS scrapecreators_budget (
                id INTEGER PRIMARY KEY CHECK(id=1), remaining INTEGER,
                observed_remaining INTEGER, charged_total INTEGER NOT NULL DEFAULT 0,
                blocked_reason TEXT, updated REAL NOT NULL)""")
            if conn.execute("SELECT 1 FROM scrapecreators_budget WHERE id=1").fetchone():
                return
            receipts = conn.execute("SELECT * FROM provider_requests WHERE provider='scrapecreators' ORDER BY updated DESC").fetchall()
            remaining = None
            charged = 0
            ambiguous = False
            for row in receipts:
                try:
                    data = json.loads(row["payload"] or "{}")
                except (ValueError, TypeError):
                    data = {}
                if not isinstance(data, dict):
                    data = {}
                balance = self._credit_number(data.get("credits_remaining"))
                cost = self._credit_number(data.get("credits_charged"))
                if row["status"] != "done" or balance is None or cost is None:
                    ambiguous = True
                if remaining is None and balance is not None:
                    remaining = balance
                charged += cost or 0
            reason = "missing_credit_receipt" if remaining is None else ("legacy_request_uncertain" if ambiguous else None)
            conn.execute("INSERT INTO scrapecreators_budget VALUES (1,?,?,?,?,?)",
                         (remaining, remaining, charged, reason, time.time()))

    def budget_status(self) -> dict:
        budget = dict(self.state.conn.execute("SELECT * FROM scrapecreators_budget WHERE id=1").fetchone())
        usage = self.state.conn.execute("SELECT used FROM provider_trial_usage WHERE provider='scrapecreators'").fetchone()
        used = usage["used"] if usage else 0
        remaining = budget["remaining"]
        budget.update(used=used, limit=self.trial_limit, reserve=self.credit_reserve,
                      can_request=bool(not budget["blocked_reason"] and used < self.trial_limit
                                       and remaining is not None and remaining - 1 >= self.credit_reserve))
        return budget

    def _reserve_request(self, key):
        conn = self.state.conn
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            saved = conn.execute("SELECT * FROM provider_requests WHERE provider='scrapecreators' AND request_key=?", (key,)).fetchone()
            if saved:
                if saved["status"] == "done":
                    return json.loads(saved["payload"])
                raise ScrapeCreatorsError("ScrapeCreators 上次請求失敗或結果不明；不自動重試")
            budget = self.budget_status()
            if budget["used"] >= self.trial_limit:
                raise ScrapeCreatorsError("ScrapeCreators 累計免費呼叫上限已達；不啟用付費")
            if not budget["can_request"]:
                raise ScrapeCreatorsError("ScrapeCreators 餘額不足保留 10 credits、餘額未知或請求結果不明；停止新呼叫")
            conn.execute("INSERT OR IGNORE INTO provider_trial_usage(provider,used) VALUES ('scrapecreators',0)")
            conn.execute("UPDATE provider_trial_usage SET used=used+1 WHERE provider='scrapecreators'")
            # One-credit endpoints only. Hold the global permit until the
            # response is accounted for; crashes fail closed across restarts.
            conn.execute("UPDATE scrapecreators_budget SET remaining=remaining-1,blocked_reason='request_in_flight',updated=? WHERE id=1", (time.time(),))
            conn.execute("INSERT INTO provider_requests(provider,request_key,status,updated) VALUES ('scrapecreators',?,'launching',?)", (key, time.time()))
        return None

    def _settle_request(self, key, data, success):
        balance = self._credit_number(data.get("credits_remaining"))
        cost = self._credit_number(data.get("credits_charged"))
        conn = self.state.conn
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute("SELECT remaining FROM scrapecreators_budget WHERE id=1").fetchone()[0]
            reason = None
            if balance is None or cost is None:
                reason = "missing_credit_receipt"
            elif cost > 1:
                reason = "unexpected_credit_cost"
            if not success:
                reason = reason or "request_failed"
            conservative = current - max(0, (cost or 0) - 1)
            if balance is not None:
                conservative = min(conservative, balance)
            conn.execute("""UPDATE scrapecreators_budget SET remaining=?,observed_remaining=COALESCE(?,observed_remaining),
                charged_total=charged_total+?,blocked_reason=?,updated=? WHERE id=1""",
                (max(0, conservative), balance, cost or 0, reason, time.time()))
            conn.execute("UPDATE provider_requests SET status=?,payload=?,updated=? WHERE provider='scrapecreators' AND request_key=?",
                         ("done" if success else "failed", json.dumps(data), time.time(), key))

    def _mark_unknown(self, key):
        with self.state.conn:
            self.state.conn.execute("UPDATE scrapecreators_budget SET blocked_reason='request_outcome_unknown',updated=? WHERE id=1 AND blocked_reason='request_in_flight'", (time.time(),))
            self.state.conn.execute("UPDATE provider_requests SET status='unknown',updated=? WHERE provider='scrapecreators' AND request_key=? AND status='launching'", (time.time(), key))

    async def _request(self, endpoint: str, params: dict) -> dict:
        if not (self.enabled and self.free_only_confirmed and self.api_key):
            raise ScrapeCreatorsError("ScrapeCreators 試用未啟用、未確認免費點數或未設定 API key")
        key = endpoint + ":" + json.dumps(params, sort_keys=True)
        if endpoint not in {"/v1/facebook/profile", "/v1/facebook/profile/posts"}:
            raise ScrapeCreatorsError("未核准的 ScrapeCreators endpoint")
        saved = self._reserve_request(key)
        if saved is not None:
            return saved
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.get("https://api.scrapecreators.com" + endpoint,
                                            params=params, headers={"x-api-key": self.api_key})
            data = response.json()
            if not isinstance(data, dict):
                raise ScrapeCreatorsError("ScrapeCreators 未回傳成功結果")
            success = response.status_code == 200 and data.get("success") is True
            self._settle_request(key, data, success)
            if not success:
                raise ScrapeCreatorsError(f"ScrapeCreators HTTP {response.status_code} 或來源失敗；不自動重試")
        except (httpx.RequestError, ValueError) as exc:
            self._mark_unknown(key)
            raise ScrapeCreatorsError("ScrapeCreators 請求結果不明；保留試用扣點，不重送") from exc
        except ScrapeCreatorsError:
            self._mark_unknown(key)
            raise
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
