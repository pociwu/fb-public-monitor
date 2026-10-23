"""Durable, conservative request reservations; never hold transactions over HTTP."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path


class ProviderState:
    def __init__(self, path: Path | str = ":memory:"):
        self.conn = sqlite3.connect(str(path), timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""CREATE TABLE IF NOT EXISTS provider_requests (
            provider TEXT NOT NULL, request_key TEXT NOT NULL, status TEXT NOT NULL,
            remote_id TEXT, payload TEXT, updated REAL NOT NULL,
            PRIMARY KEY(provider, request_key))""")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS provider_trial_usage (
            provider TEXT PRIMARY KEY, used INTEGER NOT NULL DEFAULT 0)""")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS scrapecreators_captures (
            profile_id INTEGER PRIMARY KEY, cursor TEXT, pending_json TEXT,
            status TEXT NOT NULL DEFAULT 'in_progress', last_ids TEXT, updated REAL NOT NULL)""")
        self.conn.commit()

    def claim(self, provider: str, key: str, ttl: int = 300) -> tuple[bool, dict]:
        with self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            row = self.conn.execute(
                "SELECT * FROM provider_requests WHERE provider=? AND request_key=?", (provider, key)
            ).fetchone()
            if row and not (row["status"] == "done" and time.time() - row["updated"] > ttl):
                return False, dict(row)
            self.conn.execute("""INSERT OR REPLACE INTO provider_requests
                (provider,request_key,status,updated) VALUES (?,?,'launching',?)""", (provider, key, time.time()))
        return True, {}

    def save(self, provider: str, key: str, status: str, remote_id=None, payload=None):
        with self.conn:
            self.conn.execute("""UPDATE provider_requests SET status=?, remote_id=?, payload=?,updated=?
                WHERE provider=? AND request_key=?""",
                (status, remote_id, json.dumps(payload) if payload is not None else None, time.time(), provider, key))

    def reserve_trial(self, provider: str, limit: int) -> bool:
        with self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute("INSERT OR IGNORE INTO provider_trial_usage(provider,used) VALUES (?,0)", (provider,))
            result = self.conn.execute(
                "UPDATE provider_trial_usage SET used=used+1 WHERE provider=? AND used<?", (provider, limit)
            )
            return result.rowcount == 1
