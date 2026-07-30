from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_KEYS = {"fbclid", "__tn__", "__cft__", "ref", "refsrc", "mibextid"}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")).strip()


def normalize_url(value: str | None) -> str:
    if not value:
        return ""
    parts = urlsplit(value)
    # Facebook rotates CDN hosts and signed query parameters for the same media.
    # Keep the immutable object path so a URL refresh is not a content update.
    if "fbcdn.net" in parts.netloc.lower() or parts.netloc.lower().startswith("scontent-"):
        return f"facebook-cdn:{parts.path}"
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() not in TRACKING_KEYS and not k.startswith("_nc_")]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), urlencode(query), ""))


def stable_projection(value: Any) -> Any:
    if isinstance(value, dict):
        ignored = {"scrapedAt", "scraped_at", "crawlTime", "runId", "likesCount", "reactionsCount", "commentsCount", "sharesCount"}
        return {k: stable_projection(v) for k, v in sorted(value.items()) if k not in ignored}
    if isinstance(value, list):
        return [stable_projection(item) for item in value]
    if isinstance(value, str):
        if value.startswith(("http://", "https://")):
            return normalize_url(value)
        return normalize_text(value)
    return value


def content_hash(value: Any) -> str:
    payload = json.dumps(stable_projection(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
