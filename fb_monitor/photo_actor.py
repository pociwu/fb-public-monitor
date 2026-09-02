from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Sequence
from urllib.parse import parse_qs, parse_qsl, urlsplit, urlunsplit


_PHOTO_COLLECTION_KEYS = {
    "photos",
    "photoitems",
    "photoresults",
    "media",
    "mediaitems",
    "results",
    "items",
    "edges",
    "nodes",
    "data",
}
# Generic graph/envelope names such as ``data`` and ``items`` only describe
# transport structure.  They do not prove that a nested image is a photo from
# the target collection.  Only these explicit ancestors provide that meaning.
_PHOTO_SEMANTIC_COLLECTION_KEYS = {
    "photos",
    "photoitems",
    "photoresults",
    "mediaitems",
}
_IMAGE_CONTAINER_KEYS = {
    "image",
    "images",
    "media",
    "mediaimage",
    "mediaimages",
    "picture",
    "pictures",
    "thumbnail",
    "thumbnails",
    "preview",
    "previews",
}
_IMAGE_VALUE_KEYS = {
    "image",
    "imageurl",
    "photo",
    "photourl",
    "picture",
    "pictureurl",
    "src",
    "uri",
    "downloadurl",
    "fullsizeurl",
    "fullresolutionurl",
    "highresurl",
    "largeimageurl",
    "displayurl",
    "originalurl",
}
_PERMALINK_KEYS = {
    "url",
    "permalink",
    "permalinkurl",
    "facebookurl",
    "sourceurl",
    "posturl",
    "photopageurl",
    "viewerurl",
}
_ID_KEYS = ("photoid", "mediaid", "fbid", "id")
_CAPTION_KEYS = ("caption", "description", "text", "message", "title")
_TIMESTAMP_KEYS = (
    "timestamp",
    "createdat",
    "createdtime",
    "takenat",
    "publishtime",
    "publishedat",
    "date",
)
_EVIDENCE_CONTAINER_KEYS = {
    "pagination",
    "paging",
    "pageinfo",
    "pageinformation",
    "cursorinfo",
    "coverage",
    "summary",
    "metadata",
    "meta",
}
_NEXT_CURSOR_KEYS = {
    "nextcursor",
    "nextpagecursor",
    "continuationcursor",
    "continuationtoken",
    "nexttoken",
    "endcursor",
}
_HAS_NEXT_KEYS = {
    "hasnextpage",
    "hasnext",
    "hasmore",
    "moreavailable",
}
_IS_LAST_KEYS = {"islastpage", "lastpage", "atend"}
_COMPLETE_KEYS = {
    "completed",
    "iscomplete",
    "done",
    "exhausted",
    "endreached",
    "terminal",
}
_TOTAL_KEYS = {
    "totalphotos",
    "photocount",
    "totalcount",
    "declaredtotal",
}
_TERMINAL_REASON_KEYS = {
    "terminalreason",
    "completionreason",
    "coveragestatus",
    "status",
}
_COMPLETE_STATUS_VALUES = {
    "complete",
    "completed",
    "exhausted",
    "full",
    "fullycovered",
    "endreached",
}
_INCOMPLETE_STATUS_VALUES = {
    "partial",
    "incomplete",
    "limited",
    "sourcelimited",
    "running",
    "pending",
    "failed",
}
_PROFILE_HEADER_KEYS = {
    "name",
    "profilename",
    "profilepic",
    "profilepicture",
    "profilepictureurl",
    "coverphoto",
    "coverphotourl",
}

# The paid fallback is deliberately isolated from the operator's Facebook
# session. Pagination keys such as ``nextToken`` remain allowed.
_FORBIDDEN_AUTH_KEYS = {
    "authorization",
    "authorizationheader",
    "bearertoken",
    "cookie",
    "cookies",
    "cookieheader",
    "cuser",
    "datr",
    "facebookcookie",
    "facebookcookies",
    "facebookaccesstoken",
    "facebooktoken",
    "fbtoken",
    "fr",
    "login",
    "logincookie",
    "password",
    "passwd",
    "session",
    "sessioncookie",
    "sessionid",
    "token",
    "accesstoken",
    "authtoken",
    "xs",
    "xfbtoken",
    "credential",
    "credentials",
}
_FORBIDDEN_AUTH_QUERY_KEYS = {
    "accesstoken",
    "token",
    "cookie",
    "session",
    "password",
    "cuser",
    "xs",
}
_CURSOR_CONTEXT_KEYS = {
    "cursor",
    "cursorinfo",
    "pageinfo",
    "pagination",
    "paging",
}
_CURSOR_TOKEN_KEYS = {
    "continuationtoken",
    "cursortoken",
    "endcursor",
    "nextpagecursor",
    "nexttoken",
    "pagetoken",
}
_AUTH_VALUE_PATTERNS = (
    re.compile(r"(?:^|[;\s])c_user\s*=", re.IGNORECASE),
    re.compile(r"(?:^|[;\s])xs\s*=", re.IGNORECASE),
    re.compile(r"\bcookie\s*:", re.IGNORECASE),
    re.compile(r"\bauthorization\s*:\s*bearer\b", re.IGNORECASE),
)


@dataclass(slots=True)
class PhotoActorParseResult:
    """Conservative, source-neutral result returned by a photo Actor adapter."""

    items: list[dict[str, Any]]
    completed: bool = False
    next_cursor: str | None = None
    has_next_page: bool | None = None
    declared_total: int | None = None
    terminal_reason: str = ""
    raw_candidate_count: int = 0
    evidence: dict[str, Any] = field(default_factory=dict)


def _key(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


def validate_photo_actor_input(payload: Mapping[str, Any]) -> None:
    """Reject login credentials before persistence or Actor upload.

    Only the offending path is reported, so a credential value cannot leak to
    logs or diagnostics.
    """

    def walk(value: object, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for raw_key, child in value.items():
                key = str(raw_key)
                normalized = _key(key)
                child_path = path + (key,)
                pagination_token = normalized == "token" and any(
                    _key(part) in _CURSOR_CONTEXT_KEYS for part in path
                )
                if (
                    (normalized in _FORBIDDEN_AUTH_KEYS and not pagination_token)
                    or (
                        normalized.endswith("token")
                        and normalized not in _CURSOR_TOKEN_KEYS
                        and not pagination_token
                    )
                    or "password" in normalized
                    or normalized.startswith("cookie")
                    or normalized.endswith("cookie")
                    or normalized.startswith("session")
                    or normalized.endswith("session")
                    or "credential" in normalized
                ):
                    raise ValueError(".".join(child_path))
                walk(child, child_path)
            return
        if isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                walk(child, path + (f"[{index}]",))
            return
        if isinstance(value, str):
            if any(pattern.search(value) for pattern in _AUTH_VALUE_PATTERNS):
                raise ValueError(".".join(path) or "$")
            try:
                parsed = urlsplit(value)
            except ValueError:
                parsed = None
            if parsed is not None and parsed.scheme.casefold() in {"http", "https"}:
                for query_key, _query_value in parse_qsl(
                    parsed.query, keep_blank_values=True
                ):
                    normalized_query = _key(query_key)
                    if (
                        normalized_query in _FORBIDDEN_AUTH_QUERY_KEYS
                        or normalized_query.startswith(("cookie", "session", "password"))
                        or (
                            normalized_query.endswith("token")
                            and normalized_query not in _CURSOR_TOKEN_KEYS
                        )
                    ):
                        raise ValueError(".".join(path) or "$")

    walk(payload, ("$",))


def _nonempty_text(value: object) -> str:
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return ""
    return str(value).strip()


def _bool_value(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    text = _nonempty_text(value).casefold()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _safe_url(value: object) -> str:
    text = _nonempty_text(value)
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except ValueError:
        return ""
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
        return ""
    return urlunsplit(
        (
            parsed.scheme.casefold(),
            parsed.netloc,
            parsed.path,
            parsed.query,
            "",
        )
    )


def is_fbcdn_image_url(value: object) -> bool:
    url = _safe_url(value)
    if not url:
        return False
    host = (urlsplit(url).hostname or "").casefold()
    return host == "fbcdn.net" or host.endswith(".fbcdn.net")


def is_facebook_photo_permalink(value: object) -> bool:
    url = _safe_url(value)
    if not url:
        return False
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold()
    if not (host == "facebook.com" or host.endswith(".facebook.com")):
        return False
    path = parsed.path.casefold().rstrip("/")
    query = parse_qs(parsed.query)
    if query.get("fbid") or query.get("story_fbid"):
        return True
    return bool(
        path == "/photo"
        or path == "/photo.php"
        or "/photos/" in f"{path}/"
        or path.startswith("/photos/")
    )


def _mapping_value(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    normalized = {_key(key): value for key, value in mapping.items()}
    for candidate in keys:
        if candidate in normalized:
            return normalized[candidate]
    return None


def _image_candidates(value: object, *, depth: int = 0) -> Iterator[tuple[int, str]]:
    if depth > 5:
        return
    if isinstance(value, str):
        url = _safe_url(value)
        if is_fbcdn_image_url(url):
            yield (0, url)
        return
    if isinstance(value, Mapping):
        width = _positive_int(_mapping_value(value, ("width",))) or 0
        height = _positive_int(_mapping_value(value, ("height",))) or 0
        area = width * height
        for child in value.values():
            yield from (
                (max(area, child_area), url)
                for child_area, url in _image_candidates(child, depth=depth + 1)
            )
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            yield from _image_candidates(child, depth=depth + 1)


def _extract_image(mapping: Mapping[str, Any]) -> str:
    candidates: list[tuple[int, int, str]] = []
    for order, (raw_key, value) in enumerate(mapping.items()):
        normalized = _key(raw_key)
        if normalized in _IMAGE_VALUE_KEYS or normalized in _IMAGE_CONTAINER_KEYS:
            for area, url in _image_candidates(value):
                candidates.append((area, -order, url))
    # Some Actors emit a flat photo item whose only media field is `url`.
    raw_url = _mapping_value(mapping, ("url",))
    if is_fbcdn_image_url(raw_url):
        candidates.append((0, -len(mapping), _safe_url(raw_url)))
    if not candidates:
        return ""
    return max(candidates)[2]


def _extract_permalink(mapping: Mapping[str, Any]) -> str:
    for raw_key, value in mapping.items():
        if _key(raw_key) not in _PERMALINK_KEYS:
            continue
        if is_facebook_photo_permalink(value):
            return _safe_url(value)
    return ""


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _photo_id_from_permalink(url: str) -> str:
    if not url:
        return ""
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    for key in ("fbid", "story_fbid"):
        value = _nonempty_text((query.get(key) or [""])[0])
        if value:
            return value
    parts = [part for part in parsed.path.split("/") if part]
    if "photos" in [part.casefold() for part in parts]:
        for part in reversed(parts):
            if part.isdigit():
                return part
    return ""


def _photo_owner_from_permalink(url: str) -> str:
    if not url:
        return ""
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    owner = _nonempty_text((query.get("id") or [""])[0])
    if owner:
        return owner
    parts = [part for part in parsed.path.split("/") if part]
    lowered = [part.casefold() for part in parts]
    if "photos" in lowered:
        index = lowered.index("photos")
        if index > 0:
            return parts[index - 1]
    return ""


_PROFILE_ID_KEYS = ("profileid", "facebookid", "userid")
_PROFILE_URL_KEYS = ("profileurl", "facebookurl", "pageurl", "url")
_NON_PROFILE_FACEBOOK_PATHS = {
    "events",
    "groups",
    "marketplace",
    "permalink.php",
    "photo.php",
    "photos",
    "posts",
    "reel",
    "reels",
    "share",
    "stories",
    "story.php",
    "videos",
    "watch",
}


def _profile_identity_from_url(value: object) -> str:
    """Return a Facebook profile identifier, never a content permalink ID."""

    url = _safe_url(value)
    if not url:
        return ""
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold()
    if not (host == "facebook.com" or host.endswith(".facebook.com")):
        return ""
    if is_facebook_photo_permalink(url):
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    lowered = [part.casefold() for part in parts]
    if not parts:
        return ""
    if lowered[0] == "profile.php":
        return _nonempty_text((parse_qs(parsed.query).get("id") or [""])[0])
    if lowered[0] == "people" and len(parts) == 3:
        return parts[-1]
    if lowered[0] in _NON_PROFILE_FACEBOOK_PATHS:
        return ""
    # A second path component denotes content/navigation. Restrict envelope
    # proof to canonical one-segment profile URLs rather than guessing that a
    # post/photo/video path belongs to the requested account.
    if len(parts) != 1:
        return ""
    return parts[0]


def _profile_envelope_identity_matches(
    payload: object,
    target_profile_id: str,
) -> bool:
    """Verify that a dataset row is the requested profile envelope.

    Explicit profile ID fields have priority over profile URL aliases. The
    generic root ``id`` is accepted only for a row that demonstrably wraps a
    collection/profile, because flat photo rows commonly use ``id`` for the
    photo itself.
    """

    if not target_profile_id or not isinstance(payload, Mapping):
        return False
    normalized = {_key(key): value for key, value in payload.items()}
    identities = [
        _nonempty_text(normalized.get(key))
        for key in _PROFILE_ID_KEYS
        if _nonempty_text(normalized.get(key))
    ]
    is_envelope = _is_collection_envelope(payload) or bool(
        set(normalized) & _PROFILE_HEADER_KEYS
    )
    root_id = _nonempty_text(normalized.get("id"))
    if root_id and is_envelope and not root_id.startswith(("http://", "https://")):
        identities.append(root_id)
    target = target_profile_id.casefold()
    if identities:
        return any(identity.casefold() == target for identity in identities)

    url_identities = [
        identity
        for key in _PROFILE_URL_KEYS
        if (identity := _profile_identity_from_url(normalized.get(key)))
    ]
    return any(identity.casefold() == target for identity in url_identities)


def _image_identity(url: str) -> str:
    parsed = urlsplit(url)
    filename = parsed.path.rstrip("/").rsplit("/", 1)[-1].casefold()
    if filename:
        return filename
    return urlunsplit(("https", (parsed.hostname or "").casefold(), parsed.path, "", ""))


def _explicit_photo_id(mapping: Mapping[str, Any]) -> str:
    value = _mapping_value(mapping, _ID_KEYS)
    text = _nonempty_text(value)
    if not text or len(text) > 512 or text.startswith(("http://", "https://")):
        return ""
    return text


def _candidate_from_mapping(mapping: Mapping[str, Any]) -> tuple[dict[str, Any], set[str]] | None:
    image = _extract_image(mapping)
    if not image:
        return None
    permalink = _extract_permalink(mapping)
    explicit_id = _explicit_photo_id(mapping)
    permalink_id = _photo_id_from_permalink(permalink)
    image_id = _image_identity(image)
    photo_id = explicit_id or permalink_id
    if not photo_id:
        photo_id = hashlib.sha256(image_id.encode("utf-8")).hexdigest()[:24]
    caption = _nonempty_text(_mapping_value(mapping, _CAPTION_KEYS))
    timestamp = _nonempty_text(_mapping_value(mapping, _TIMESTAMP_KEYS))
    item = {
        "id": photo_id,
        "url": permalink or image,
        "image": image,
        "caption": caption,
        "timestamp": timestamp,
    }
    aliases = {f"image:{image_id}"}
    if explicit_id:
        aliases.add(f"id:{explicit_id}")
    if permalink_id:
        aliases.add(f"facebook:{permalink_id}")
    if permalink:
        aliases.add(f"permalink:{_photo_id_from_permalink(permalink) or permalink.casefold()}")
    return item, aliases


def _walk(value: object, path: tuple[str, ...] = (), *, depth: int = 0) -> Iterator[tuple[object, tuple[str, ...]]]:
    if depth > 14:
        return
    if isinstance(value, Mapping):
        yield value, path
        for raw_key, child in value.items():
            key = _key(raw_key)
            yield from _walk(child, path + (key,), depth=depth + 1)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _walk(child, path, depth=depth + 1)
    elif isinstance(value, str):
        yield value, path


def _is_collection_envelope(mapping: Mapping[str, Any]) -> bool:
    """Whether a mapping wraps photo rows instead of representing one photo.

    ``media`` is intentionally excluded here because many real photo rows use
    it for image variants.  The other collection keys only count when their
    value is a container, so scalar metadata named ``data`` cannot hide a
    valid photo row.
    """
    for raw_key, value in mapping.items():
        normalized = _key(raw_key)
        if normalized == "media" or normalized not in _PHOTO_COLLECTION_KEYS:
            continue
        if isinstance(value, (Mapping, list, tuple)):
            return True
    return False


def _is_profile_header(mapping: Mapping[str, Any], path: tuple[str, ...]) -> bool:
    direct_keys = {_key(key) for key in mapping}
    if direct_keys & (_PROFILE_HEADER_KEYS - {"name"}):
        return True
    # A generic `data` envelope often contains just the profile name/avatar.
    # Treat an unbound name+image object as profile chrome, while allowing a
    # real photo row that carries an ID, permalink, timestamp, or that lives in
    # an explicitly named photo collection.
    return bool(
        "name" in direct_keys
        and not _explicit_photo_id(mapping)
        and not _extract_permalink(mapping)
        and not _nonempty_text(_mapping_value(mapping, _TIMESTAMP_KEYS))
        and not any(part in {"photos", "photoitems", "photoresults"} for part in path)
    )


def _iter_photo_candidates(
    payloads: Sequence[object],
    *,
    target_profile_id: str = "",
) -> Iterator[tuple[dict[str, Any], set[str]]]:
    for payload in payloads:
        envelope_matches_target = _profile_envelope_identity_matches(
            payload, target_profile_id
        )
        for value, path in _walk(payload):
            if isinstance(value, Mapping):
                # Image-variant dictionaries are consumed by their parent photo
                # object and must not become a second synthetic photo.
                if path and path[-1] in _IMAGE_CONTAINER_KEYS:
                    continue
                # Envelopes can appear below generic `data`/`results` wrappers.
                # Never treat the envelope's avatar or cover as a photo; its
                # child collection will still be visited by `_walk`.
                if _is_collection_envelope(value) or _is_profile_header(value, path):
                    continue
                candidate = _candidate_from_mapping(value)
                if candidate is not None:
                    item, aliases = candidate
                    # Outside a known photo collection, require a durable photo
                    # permalink. This rejects recommendation/avatar media found
                    # elsewhere in a deeply nested profile response.
                    if (
                        path
                        and not any(
                            part in _PHOTO_SEMANTIC_COLLECTION_KEYS
                            for part in path
                        )
                        and not is_facebook_photo_permalink(item.get("url"))
                    ):
                        continue
                    owner = _photo_owner_from_permalink(str(item.get("url") or ""))
                    if target_profile_id:
                        if owner:
                            if owner.casefold() != target_profile_id.casefold():
                                continue
                        elif not envelope_matches_target:
                            continue
                    yield item, aliases
            elif (
                isinstance(value, str)
                and bool(path)
                and path[-1] in _PHOTO_SEMANTIC_COLLECTION_KEYS
                and not any(part in _IMAGE_CONTAINER_KEYS for part in path)
                and is_fbcdn_image_url(value)
            ):
                if target_profile_id and not envelope_matches_target:
                    continue
                image = _safe_url(value)
                image_id = _image_identity(image)
                yield (
                    {
                        "id": hashlib.sha256(image_id.encode("utf-8")).hexdigest()[:24],
                        "url": image,
                        "image": image,
                        "caption": "",
                        "timestamp": "",
                    },
                    {f"image:{image_id}"},
                )


def _merge_photo(existing: dict[str, Any], incoming: dict[str, Any]) -> None:
    if is_fbcdn_image_url(existing.get("url")) and is_facebook_photo_permalink(incoming.get("url")):
        existing["url"] = incoming["url"]
    for key in ("caption", "timestamp"):
        if not existing.get(key) and incoming.get(key):
            existing[key] = incoming[key]


def _evidence_nodes(payloads: Sequence[object], summary: object | None) -> Iterator[tuple[Mapping[str, Any], str]]:
    if isinstance(summary, Mapping):
        yield summary, "summary"
        for value, path in _walk(summary):
            if isinstance(value, Mapping) and path and path[-1] in _EVIDENCE_CONTAINER_KEYS:
                yield value, "summary." + ".".join(path)
    for index, payload in enumerate(payloads):
        if not isinstance(payload, Mapping):
            continue
        if _candidate_from_mapping(payload) is None:
            yield payload, f"items[{index}]"
        else:
            # A flat dataset row may be both one photo and the page envelope.
            # Keep its pagination/count evidence, but deliberately exclude the
            # generic ``status`` field (usually Actor run status, not inventory
            # completion).
            evidence_keys = (
                _NEXT_CURSOR_KEYS
                | _HAS_NEXT_KEYS
                | _IS_LAST_KEYS
                | _COMPLETE_KEYS
                | _TOTAL_KEYS
                | (_TERMINAL_REASON_KEYS - {"status"})
            )
            envelope = {
                key: value
                for key, value in payload.items()
                if _key(key) in evidence_keys
            }
            if envelope:
                yield envelope, f"items[{index}].envelope"
        for value, path in _walk(payload):
            if isinstance(value, Mapping) and path and path[-1] in _EVIDENCE_CONTAINER_KEYS:
                yield value, f"items[{index}]." + ".".join(path)


def _parse_evidence(payloads: Sequence[object], summary: object | None, extracted_count: int) -> dict[str, Any]:
    cursors: list[str] = []
    has_next_signals: list[bool] = []
    complete_signals: list[bool] = []
    totals: list[int] = []
    reasons: list[str] = []
    paths: list[str] = []
    for node, path in _evidence_nodes(payloads, summary):
        normalized = {_key(key): value for key, value in node.items()}
        matched = False
        for key in _NEXT_CURSOR_KEYS:
            cursor = _nonempty_text(normalized.get(key))
            if cursor:
                cursors.append(cursor)
                matched = True
        for key in _HAS_NEXT_KEYS:
            if key in normalized:
                signal = _bool_value(normalized[key])
                if signal is not None:
                    has_next_signals.append(signal)
                    matched = True
        for key in _IS_LAST_KEYS:
            if key in normalized:
                signal = _bool_value(normalized[key])
                if signal is not None:
                    has_next_signals.append(not signal)
                    matched = True
        for key in _COMPLETE_KEYS:
            if key in normalized:
                signal = _bool_value(normalized[key])
                if signal is not None:
                    complete_signals.append(signal)
                    matched = True
        for key in _TOTAL_KEYS:
            total = _positive_int(normalized.get(key))
            if total is not None:
                totals.append(total)
                matched = True
        for key in _TERMINAL_REASON_KEYS:
            reason = _nonempty_text(normalized.get(key))
            if not reason:
                continue
            reasons.append(reason)
            status = _key(reason)
            if status in _COMPLETE_STATUS_VALUES:
                complete_signals.append(True)
            elif status in _INCOMPLETE_STATUS_VALUES:
                complete_signals.append(False)
            matched = True
        if matched:
            paths.append(path)

    next_cursor = cursors[0] if cursors else None
    has_next_page: bool | None = None
    if True in has_next_signals:
        has_next_page = True
    elif False in has_next_signals:
        has_next_page = False
    declared_total = max(totals) if totals else None
    terminal_signal = True in complete_signals or has_next_page is False
    incomplete_signal = (
        next_cursor is not None
        or has_next_page is True
        or False in complete_signals
        or (declared_total is not None and extracted_count < declared_total)
    )
    completed = bool(terminal_signal and not incomplete_signal)
    terminal_reason = reasons[0] if reasons else (
        "explicit_end_of_pagination" if completed else ""
    )
    return {
        "completed": completed,
        "next_cursor": next_cursor,
        "has_next_page": has_next_page,
        "declared_total": declared_total,
        "terminal_reason": terminal_reason,
        "paths": sorted(set(paths)),
        "complete_signals": complete_signals,
        "has_next_signals": has_next_signals,
        "cursor_count": len(cursors),
    }


def parse_photo_actor_output(
    items: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None,
    summary: Mapping[str, Any] | None = None,
    *,
    target_profile_id: str = "",
) -> PhotoActorParseResult:
    """Normalize heterogeneous Actor output without inventing completeness.

    Only downloadable ``*.fbcdn.net`` image URLs are emitted.  A Facebook
    photo permalink is retained when present; otherwise the CDN URL is used as
    the source URL.  One dataset row, a non-empty ``photos`` array, or a count
    equal to the current page length is deliberately *not* terminal evidence.
    """

    if items is None:
        payloads: list[object] = []
    elif isinstance(items, Mapping):
        payloads = [items]
    else:
        payloads = [item for item in items]

    normalized: list[dict[str, Any]] = []
    alias_to_index: dict[str, int] = {}
    raw_candidate_count = 0
    for item, aliases in _iter_photo_candidates(
        payloads, target_profile_id=str(target_profile_id or "").strip()
    ):
        raw_candidate_count += 1
        matches = {alias_to_index[alias] for alias in aliases if alias in alias_to_index}
        if matches:
            index = min(matches)
            # An Actor can first expose the same photo once by permalink and
            # once by a CDN variant, followed by a richer row that connects
            # both identities.  Collapse all previously separate rows instead
            # of merely pointing future aliases at the first one.
            for duplicate_index in sorted(matches - {index}, reverse=True):
                _merge_photo(normalized[index], normalized[duplicate_index])
                normalized.pop(duplicate_index)
                for alias, alias_index in list(alias_to_index.items()):
                    if alias_index == duplicate_index:
                        alias_to_index[alias] = index
                    elif alias_index > duplicate_index:
                        alias_to_index[alias] = alias_index - 1
            _merge_photo(normalized[index], item)
        else:
            index = len(normalized)
            normalized.append(item)
        for alias in aliases:
            alias_to_index[alias] = index

    target = str(target_profile_id or "").strip()
    if target:
        evidence_payloads = [
            payload
            for payload in payloads
            if _profile_envelope_identity_matches(payload, target)
        ]
        evidence_summary = (
            summary
            if _profile_envelope_identity_matches(summary, target)
            or bool(evidence_payloads)
            else None
        )
    else:
        evidence_payloads = payloads
        evidence_summary = summary
    evidence = _parse_evidence(evidence_payloads, evidence_summary, len(normalized))
    return PhotoActorParseResult(
        items=normalized,
        completed=bool(evidence["completed"]),
        next_cursor=evidence["next_cursor"],
        has_next_page=evidence["has_next_page"],
        declared_total=evidence["declared_total"],
        terminal_reason=str(evidence["terminal_reason"] or ""),
        raw_candidate_count=raw_candidate_count,
        evidence=evidence,
    )


__all__ = [
    "PhotoActorParseResult",
    "is_facebook_photo_permalink",
    "is_fbcdn_image_url",
    "parse_photo_actor_output",
    "validate_photo_actor_input",
]
