from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from .db import Database, utcnow


class TelegramSender:
    def __init__(self, db: Database, token: str, chat_id: str, *, interval_seconds: int = 3, high_water: int = 50, max_attempts: int = 5, retry_hours: int = 24):
        self.db, self.token, self.chat_id = db, token, chat_id
        self.interval_seconds = interval_seconds
        self.high_water = high_water
        self.max_attempts = max_attempts
        self.retry_hours = retry_hours

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def drain_once(self) -> bool:
        if not self.enabled:
            return False
        last = self.db.row("SELECT sent_at FROM outbox WHERE status='sent' ORDER BY sent_at DESC LIMIT 1")
        if last and last.get("sent_at"):
            try:
                if datetime.now(UTC) - datetime.fromisoformat(last["sent_at"]) < timedelta(seconds=self.interval_seconds):
                    return False
            except ValueError:
                pass
        now = utcnow()
        pending = self.db.row("SELECT COUNT(*) count FROM outbox WHERE status='pending'") or {"count": 0}
        media_allowed = int(pending["count"]) <= self.high_water
        row = self.db.row(
            """SELECT o.*,g.status group_status FROM outbox o LEFT JOIN notification_groups g ON g.id=o.group_id
            WHERE o.status='pending' AND o.next_attempt_at<=?
              AND (o.kind IN ('text','summary') OR (? AND o.kind='media' AND (o.group_id IS NULL OR g.status='sent')))
            ORDER BY CASE WHEN o.kind IN ('text','summary') THEN 0 ELSE 1 END,o.id LIMIT 1""",
            (now, int(media_allowed)),
        )
        if not row:
            return False
        try:
            payload = json.loads(row["payload_json"])
            if row["kind"] == "media":
                await self._send_media(payload)
            elif row["kind"] == "summary" and row.get("group_id"):
                await self._send_text(self._format_group(int(row["group_id"])))
            else:
                await self._send_text(self._format(payload))
            sent_at = utcnow()
            self.db.execute("UPDATE outbox SET status='sent',sent_at=?,last_error=NULL WHERE id=?", (sent_at, row["id"]))
            if row.get("group_id") and row["kind"] == "summary":
                self.db.execute("UPDATE notification_groups SET status='sent',sent_at=? WHERE id=?", (sent_at, row["group_id"]))
            if row.get("event_id"):
                self.db.execute("UPDATE events SET notified_at=? WHERE id=?", (sent_at, row["event_id"]))
        except Exception as exc:
            attempts = int(row["attempts"]) + 1
            expired = datetime.now(UTC) - datetime.fromisoformat(row["created_at"]) >= timedelta(hours=self.retry_hours)
            if attempts >= self.max_attempts or expired:
                self.db.execute("UPDATE outbox SET status='failed',attempts=?,last_error=? WHERE id=?", (attempts, str(exc)[:1000], row["id"]))
                if row.get("group_id") and row["kind"] == "summary":
                    self.db.execute("UPDATE notification_groups SET status='failed' WHERE id=?", (row["group_id"],))
                    self.db.execute("UPDATE outbox SET status='failed',last_error='summary failed' WHERE group_id=? AND kind='media' AND status='pending'", (row["group_id"],))
            else:
                delay = min(3600, 30 * (2 ** min(attempts, 7)))
                self.db.execute("UPDATE outbox SET attempts=?,next_attempt_at=?,last_error=? WHERE id=?", (attempts, (datetime.now(UTC) + timedelta(seconds=delay)).isoformat(), str(exc)[:1000], row["id"]))
        return True

    def _format_group(self, group_id: int) -> str:
        group = self.db.row("SELECT payload_json FROM notification_groups WHERE id=?", (group_id,)) or {}
        items = json.loads(group.get("payload_json") or "{}").get("items", [])
        if not items:
            return "<b>FB Monitor</b>\n\n內容更新"
        title = str(items[-1].get("title") or "FB Monitor")
        lines = []
        for item in items[-8:]:
            text = str(item.get("text") or "內容已更新").replace("\n", " ")
            lines.append(f"• {text[:420]}")
        suffix = f"\n\n共 {len(items)} 項變更已合併" if len(items) > 1 else ""
        source = str(items[-1].get("source_url") or "")
        link = f'\n\n<a href="{self._escape(source)}">查看 Facebook 來源</a>' if source else ""
        return f"<b>{self._escape(title)}</b>\n\n{self._escape(chr(10).join(lines))}{suffix}{link}"[:4096]

    def _format(self, payload: dict[str, Any]) -> str:
        title = payload.get("title", "FB Monitor")
        parts = [f"<b>{self._escape(str(title))}</b>"]
        if payload.get("text"):
            parts.append(self._escape(str(payload["text"])[:3500]))
        if payload.get("source_url"):
            parts.append(f'<a href="{self._escape(str(payload["source_url"]))}">查看 Facebook 來源</a>')
        return "\n\n".join(parts)[:4096]

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    async def _send_text(self, text: str) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(f"https://api.telegram.org/bot{self.token}/sendMessage", json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
            response.raise_for_status()
            if not response.json().get("ok"):
                raise RuntimeError(response.text)

    async def _send_media(self, payload: dict[str, Any]) -> None:
        path = Path(payload["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        mime = str(payload.get("mime_type") or "application/octet-stream")
        method, field = ("sendPhoto", "photo") if mime.startswith("image/") else (("sendVideo", "video") if mime.startswith("video/") else ("sendDocument", "document"))
        async with httpx.AsyncClient(timeout=300) as client:
            with path.open("rb") as file_handle:
                response = await client.post(f"https://api.telegram.org/bot{self.token}/{method}", data={"chat_id": self.chat_id, "caption": str(payload.get("caption", ""))[:1024]}, files={field: (path.name, file_handle, mime)})
        response.raise_for_status()
