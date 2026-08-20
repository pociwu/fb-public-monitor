from __future__ import annotations

import json
import random
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError, async_playwright

from .normalize import facebook_post_identity, normalize_url

CAPTURE_VIEWPORT_MULTIPLIER = 3


class FacebookBrowserError(RuntimeError):
    pass


class FacebookBrowserLoginRequired(FacebookBrowserError):
    pass


class FacebookBrowserChallengeRequired(FacebookBrowserLoginRequired):
    pass


def _is_facebook_host(value: str) -> bool:
    host = (urlsplit(value).hostname or "").casefold()
    return host == "facebook.com" or host.endswith(".facebook.com")


def is_facebook_permalink(value: object) -> bool:
    """Whether *value* identifies a stable Facebook post or photo."""
    url = str(value or "")
    if not url or not _is_facebook_host(url):
        return False
    parsed = urlsplit(url)
    path = parsed.path.rstrip("/").casefold()
    query = {key.casefold(): item for key, item in parse_qs(parsed.query).items()}
    return bool(
        facebook_post_identity(url)
        and (
            any(marker in path for marker in ("/posts/", "/photos/", "/videos/", "/reel/", "/share/p/"))
            or path == "/permalink.php"
            or path in {"/photo", "/photo.php"}
            or query.get("story_fbid")
            or query.get("fbid")
        )
    )


def select_facebook_permalink(candidates: object) -> str:
    """Choose an article permalink without mistaking its first attachment.

    Facebook articles frequently place one or more photo attachment anchors
    before the post timestamp.  DOM extraction therefore returns all anchors
    plus small semantic hints and this function ranks them deterministically.
    It also accepts plain strings for compatibility with test fixtures.
    """
    if not isinstance(candidates, list):
        return ""
    ranked: list[tuple[int, int, str]] = []
    for index, raw in enumerate(candidates):
        if isinstance(raw, str):
            item: dict[str, Any] = {"url": raw}
        elif isinstance(raw, dict):
            item = raw
        else:
            continue
        url = str(item.get("url") or "")
        if not is_facebook_permalink(url):
            continue
        parsed = urlsplit(url)
        path = parsed.path.casefold()
        query = {key.casefold(): values for key, values in parse_qs(parsed.query).items()}
        score = 0
        if "/posts/" in path:
            score += 140
        elif path.rstrip("/").endswith("/permalink.php") or query.get("story_fbid"):
            score += 135
        elif any(marker in path for marker in ("/videos/", "/reel/", "/share/p/")):
            score += 125
        elif "/photos/" in path:
            score += 90
        elif path.rstrip("/") in {"/photo", "/photo.php"} and query.get("fbid"):
            score += 65

        text = " ".join(
            str(item.get(key) or "") for key in ("text", "aria_label", "title")
        ).strip()
        if bool(item.get("is_timestamp")):
            score += 80
        elif re.search(
            r"(?:\d+\s*(?:分鐘|小時|天|週|年|m|h|d|w|y)\b|"
            r"\d{4}[/-]\d{1,2}[/-]\d{1,2}|(?:上午|下午)?\d{1,2}:\d{2})",
            text,
            flags=re.IGNORECASE,
        ):
            score += 35
        if bool(item.get("has_image")):
            score -= 45
        # Stable sort by DOM position after score so identical aliases remain
        # predictable across runs.
        ranked.append((score, -index, normalize_url(url)))
    return max(ranked)[2] if ranked else ""


def _facebook_profile_identity(value: object) -> str:
    """Return the account token represented by a Facebook profile URL.

    This intentionally accepts only URL shapes that identify one account.  It
    is used by anonymous-public verification, so an unrecognised shape must
    fail closed instead of treating a generic Facebook page as the target.
    """
    url = str(value or "")
    if not url or not _is_facebook_host(url):
        return ""
    parsed = urlsplit(url)
    query_id = (parse_qs(parsed.query).get("id") or [""])[0].strip()
    if parsed.path.rstrip("/").casefold() == "/profile.php":
        return query_id
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    if not parts:
        return ""
    if parts[0].casefold() == "people" and len(parts) >= 3 and parts[-1].isdigit():
        return parts[-1]
    if parts[0].casefold() in {
        "groups", "pages", "watch", "reel", "share", "photo", "photo.php",
        "permalink.php", "login", "checkpoint", "challenge",
    }:
        return ""
    return parts[0]


def _facebook_permalink_owner(value: object) -> str:
    """Return the account token that owns a stable Facebook permalink."""
    url = str(value or "")
    if not is_facebook_permalink(url):
        return ""
    parsed = urlsplit(url)
    query_id = (parse_qs(parsed.query).get("id") or [""])[0].strip()
    if query_id:
        return query_id
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    folded = [part.casefold() for part in parts]
    if "groups" in folded:
        return ""
    for marker in ("posts", "photos", "videos"):
        if marker in folded:
            index = folded.index(marker)
            if index >= 1:
                return parts[index - 1]
    # /reel/... and /share/p/... identify content but do not encode its owner.
    # They cannot independently prove that the article belongs to the watched
    # account, so verification deliberately rejects them here.
    return ""


def public_content_proof(raw: dict[str, Any], profile_url: str) -> dict[str, Any] | None:
    """Extract identity-bound public-content evidence from a rendered page.

    A visible name, avatar, bio or friend count is profile metadata and remains
    readable in several restricted states.  Only a stable permalink obtained
    from an article and whose owner is the monitored account is accepted as
    proof that anonymous visitors can actually read target public content.
    """
    target = _facebook_profile_identity(profile_url)
    if not target:
        return None
    for index, post in enumerate(raw.get("posts") or []):
        if not isinstance(post, dict):
            continue
        permalink = str(post.get("url") or "")
        owner = _facebook_permalink_owner(permalink)
        if not owner or owner.casefold() != target.casefold():
            continue
        return {
            "kind": "target_permalink_article",
            "permalink": normalize_url(permalink),
            "post_identity": facebook_post_identity(permalink),
            "target_identity": target,
            "article_index": index,
        }
    return None


def public_content_proof_matches_profile(proof: object, profile_url: str) -> bool:
    """Validate an anonymous browser proof without trusting copied metadata."""
    if not isinstance(proof, dict) or proof.get("kind") != "target_permalink_article":
        return False
    target = _facebook_profile_identity(profile_url)
    permalink = str(proof.get("permalink") or "")
    owner = _facebook_permalink_owner(permalink)
    identity = facebook_post_identity(permalink)
    return bool(
        target
        and owner
        and owner.casefold() == target.casefold()
        and identity
        and str(proof.get("target_identity") or "").casefold() == target.casefold()
        and str(proof.get("post_identity") or "") == identity
    )


def _parse_viewer_position(value: object) -> tuple[int | None, int | None]:
    """Extract a trustworthy 1-based photo position and declared total."""
    text = str(value or "")
    patterns = (
        r"(?:第\s*)?(\d+)\s*(?:張|項)?\s*[,，·•-]?\s*(?:/|／|of|共)\s*(\d+)\s*(?:張|項)?",
        r"(?:photo|相片|照片)\s*(\d+)\s*(?:of|/|／|共)\s*(\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        position, total = (int(match.group(1)), int(match.group(2)))
        if 1 <= position <= total <= 100_000:
            return position, total
    return None, None


_FACEBOOK_UI_HEADINGS = {
    "facebook", "notifications", "notification", "通知", "menu", "功能表",
    "search", "搜尋", "chats", "聊天室", "settings & privacy", "設定和隱私權",
}


def _numeric_profile_id(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.path.rstrip("/").lower() == "/profile.php":
        return (parse_qs(parsed.query).get("id") or [""])[0]
    return next((part for part in reversed(parsed.path.split("/")) if part.isdigit()), "")


def _first_labeled_value(lines: list[str], labels: tuple[str, ...]) -> str:
    lowered = tuple(label.casefold() for label in labels)
    for index, line in enumerate(lines):
        folded = line.casefold()
        for label, label_folded in zip(labels, lowered, strict=True):
            if folded == label_folded and index + 1 < len(lines):
                return lines[index + 1]
            if folded.startswith(label_folded):
                value = line[len(label):].lstrip(" ：:·-")
                if value:
                    return value
    return ""


def _clean_name_candidate(value: object) -> str:
    candidate = re.sub(r"\s*[|\-]‎?\s*Facebook\s*$", "", str(value or ""), flags=re.IGNORECASE).strip()
    if (
        candidate.casefold() in _FACEBOOK_UI_HEADINGS
        or re.fullmatch(r"\(\d+\)\s*facebook", candidate, flags=re.IGNORECASE)
        or re.fullmatch(r"[\(（][^\(（\)）]{1,80}[\)）]", candidate)
    ):
        return ""
    return candidate


def is_facebook_ui_heading(value: object) -> bool:
    candidate = re.sub(r"\s*[|\-]‎?\s*Facebook\s*$", "", str(value or ""), flags=re.IGNORECASE).strip()
    return candidate.casefold() in _FACEBOOK_UI_HEADINGS or bool(re.fullmatch(r"\(\d+\)\s*facebook", candidate, flags=re.IGNORECASE))


def _name_from_profile_image(images: list[dict[str, Any]]) -> str:
    patterns = (
        r"^(?:查看\s*)?(.+?)的(?:個人)?大頭貼(?:照片|照)?$",
        r"^(.+?)(?:'s|’s) profile picture$",
    )
    for image in images:
        rendered_width = int(image.get("rendered_width") or 0)
        rendered_height = int(image.get("rendered_height") or 0)
        # Timeline author avatars are normally rendered around 40x40 and may
        # carry an explicit "X's profile picture" alt label.  They identify a
        # post author, not the owner of the profile page.  The profile-header
        # avatar in our desktop viewport is substantially larger.
        if rendered_width and rendered_height and min(rendered_width, rendered_height) < 96:
            continue
        alt = str(image.get("alt") or "").strip()
        for pattern in patterns:
            match = re.match(pattern, alt, flags=re.IGNORECASE)
            if match and (candidate := _clean_name_candidate(match.group(1))):
                return candidate
    return ""


def _name_from_profile_summary(lines: list[str]) -> str:
    count_pattern = re.compile(
        r"^[\d,.]+(?:\s*[KMB萬億])?\s*(?:位\s*)?(?:朋友|追蹤者|friends|followers)(?:\s*[·•・]\s*.*)?$",
        re.IGNORECASE,
    )
    for index, line in enumerate(lines):
        if not count_pattern.fullmatch(line):
            continue
        alias = ""
        for candidate_line in reversed(lines[max(0, index - 3):index]):
            if re.fullmatch(r"[\(（][^\(（\)）]{1,80}[\)）]", candidate_line):
                alias = candidate_line
                continue
            candidate = _clean_name_candidate(candidate_line)
            if candidate and len(candidate) <= 80 and not candidate.startswith(("http://", "https://")):
                return f"{candidate}{alias}" if alias and alias not in candidate else candidate
    return ""


def normalize_browser_profile(raw: dict[str, Any], profile_url: str) -> dict[str, Any]:
    """Map the rendered Facebook page into the dashboard's profile fields."""
    text = str(raw.get("text") or "")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    images = [item for item in raw.get("images", []) if isinstance(item, dict) and item.get("src")]
    headings = raw.get("headings") if isinstance(raw.get("headings"), list) else []
    role_headings = raw.get("role_headings") if isinstance(raw.get("role_headings"), list) else []
    summary_name = _name_from_profile_summary(lines)
    aliased_summary = summary_name if re.search(r"[\(（][^\(（\)）]{1,80}[\)）]$", summary_name) else ""
    image_name = _name_from_profile_image(images)
    name_candidates = [
        # The name adjacent to the friend/follower count and the avatar alt text
        # belong to the profile header.  A generic role=main h1 may instead be a
        # visible post author, so only use it after those profile-specific clues.
        aliased_summary, image_name, summary_name, raw.get("main_heading"),
        *role_headings, *headings,
        raw.get("og_title"), raw.get("heading"), raw.get("title"),
    ]
    name = next((candidate for value in name_candidates if (candidate := _clean_name_candidate(value))), "")

    def image_score(item: dict[str, Any]) -> int:
        width = int(item.get("natural_width") or item.get("rendered_width") or 0)
        height = int(item.get("natural_height") or item.get("rendered_height") or 0)
        return width * height

    def image_asset_key(value: object) -> str:
        parsed = urlsplit(str(value or ""))
        return f"{parsed.netloc.casefold()}{parsed.path}"

    profile_candidates = [
        item for item in images
        if any(word in str(item.get("alt") or "").casefold() for word in ("profile picture", "個人大頭貼", "大頭貼照片", "頭像"))
    ]
    if not profile_candidates:
        profile_candidates = [
            item for item in images
            if 96 <= int(item.get("rendered_width") or 0) <= 420
            and 96 <= int(item.get("rendered_height") or 0) <= 420
            and 0.75 <= int(item.get("rendered_width") or 0) / max(1, int(item.get("rendered_height") or 0)) <= 1.34
            and 0 <= int(item.get("y") or 0) <= 650
        ]
    cover_candidates = [
        item for item in images
        if any(word in str(item.get("alt") or "").casefold() for word in ("cover photo", "封面相片", "封面照片"))
    ]
    profile_picture = str(max(profile_candidates, key=image_score).get("src")) if profile_candidates else str(raw.get("og_image") or "")
    cover_photo = str(max(cover_candidates, key=image_score).get("src")) if cover_candidates else ""

    excluded = {profile_picture, cover_photo}
    excluded_assets = {image_asset_key(src) for src in excluded if src}
    photos = []
    for image in sorted(images, key=image_score, reverse=True):
        src = str(image.get("src") or "")
        asset_key = image_asset_key(src)
        width = int(image.get("natural_width") or image.get("rendered_width") or 0)
        height = int(image.get("natural_height") or image.get("rendered_height") or 0)
        if src in excluded or asset_key in excluded_assets or not src.startswith("http") or width < 180 or height < 180:
            continue
        if "fbcdn.net" not in (urlsplit(src).hostname or ""):
            continue
        photos.append({"url": src})
        excluded.add(src)
        excluded_assets.add(asset_key)
        if len(photos) == 6:
            break

    followers = ""
    follower_match = re.search(r"([\d,.]+(?:\s*[KMB萬億])?)\s*(?:followers|位追蹤者|追蹤者)", text, re.IGNORECASE)
    if follower_match:
        followers = follower_match.group(1).replace(" ", "")

    intro = _first_labeled_value(lines, ("簡介", "Intro", "Bio"))
    city = _first_labeled_value(lines, ("現居", "住在", "Lives in"))
    hometown = _first_labeled_value(lines, ("來自", "From"))
    work = _first_labeled_value(lines, ("任職於", "Works at"))
    education = _first_labeled_value(lines, ("曾就讀", "就讀於", "Studied at", "Went to"))
    canonical = str(raw.get("og_url") or profile_url)
    # Evidence identity must come from the page Facebook actually rendered.
    # The requested URL is useful for display fallback, but cannot prove that
    # a redirect or error page belongs to the monitored account.
    observed_profile_url = str(raw.get("og_url") or raw.get("page_url") or "")
    observed_profile_identity = _numeric_profile_id(observed_profile_url)
    item: dict[str, Any] = {
        "id": _numeric_profile_id(canonical) or _numeric_profile_id(profile_url),
        "name": name,
        "url": canonical,
        "private": bool(raw.get("private")),
        "profile_data_source": "Facebook 直接瀏覽器",
    }
    if observed_profile_url:
        item["observed_profile_url"] = observed_profile_url
    if observed_profile_identity:
        item["observed_profile_identity"] = observed_profile_identity
    optional = {
        "profile_picture": profile_picture,
        "cover_photo": cover_photo,
        "profile_intro_text": intro,
        "current_city": city,
        "hometown": hometown,
        "followers": followers,
        "photos": photos,
    }
    item.update({key: value for key, value in optional.items() if value not in (None, "", [], {})})
    if work:
        item["works"] = [{"title": work}]
    if education:
        item["educations"] = [{"title": education}]
    return item


def normalize_browser_canary_posts(
    raw_posts: object,
    max_posts: int = 2,
    after_cursor: str | None = None,
) -> list[dict[str, Any]]:
    """Normalize only visible posts that expose a stable Facebook permalink."""
    if not isinstance(raw_posts, list) or max_posts <= 0:
        return []
    posts: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    seen_post_ids: set[str] = set()
    cursor = str(after_cursor or "")
    cursor_found = not bool(cursor)
    for raw in raw_posts:
        if not isinstance(raw, dict):
            continue
        source_url = normalize_url(str(raw.get("url") or ""))
        if not is_facebook_permalink(source_url) or source_url in seen_urls:
            continue
        post_id = facebook_post_identity(source_url)
        if post_id and post_id in seen_post_ids:
            continue
        if not cursor_found:
            if post_id == cursor or source_url == cursor:
                cursor_found = True
            continue
        photos: list[dict[str, str]] = []
        seen_assets: set[str] = set()
        for image in raw.get("images") or []:
            if not isinstance(image, dict):
                continue
            src = str(image.get("src") or image.get("url") or "")
            width = int(image.get("natural_width") or image.get("rendered_width") or 0)
            height = int(image.get("natural_height") or image.get("rendered_height") or 0)
            asset = normalize_url(src)
            if (
                not src.startswith("http")
                or "fbcdn.net" not in (urlsplit(src).hostname or "")
                or width < 180
                or height < 180
                or asset in seen_assets
            ):
                continue
            photos.append({"url": src})
            seen_assets.add(asset)
        item: dict[str, Any] = {
            "source_url": source_url,
            "text": str(raw.get("text") or "")[:8000],
            "ingest_source": "facebook_browser_canary",
        }
        if post_id:
            item["source_post_id"] = post_id
        if photos:
            item["images"] = photos
        posts.append(item)
        seen_urls.add(source_url)
        if post_id:
            seen_post_ids.add(post_id)
        if len(posts) >= max_posts:
            break
    return posts


class FacebookBrowserGateway:
    def __init__(
        self,
        enabled: bool,
        data_dir: Path,
        timeout_seconds: int = 60,
        canary_max_posts: int = 2,
        *,
        require_login: bool = True,
    ):
        self.enabled = enabled
        self.data_dir = data_dir
        self.timeout_ms = max(10, timeout_seconds) * 1000
        self.canary_max_posts = max(0, min(2, canary_max_posts))
        self.require_login = bool(require_login)
        # Separate limits keep existing deployments configurable while making
        # the risk-control contract explicit: never perform over twenty Next
        # operations or keep a viewer open longer than three minutes per batch.
        self.album_batch_max_operations = 20
        self.album_batch_max_new_photos = 20
        self.album_batch_max_seconds = 180
        self._canary_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._expanded_canary_cache: set[str] = set()

    @property
    def _album_progress_path(self) -> Path:
        return self.data_dir / "canary-album-progress.json"

    def _load_album_progress(self) -> dict[str, dict[str, Any]]:
        try:
            value = json.loads(self._album_progress_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _save_album_progress(self, progress: dict[str, dict[str, Any]]) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        target = self._album_progress_path
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(progress, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        temporary.replace(target)

    def cached_canary_posts(self, profile_url: str, max_age_seconds: int = 600) -> list[dict[str, Any]] | None:
        cached = self._canary_cache.get(profile_url.rstrip("/"))
        if not cached or time.monotonic() - cached[0] > max_age_seconds:
            return None
        return [dict(post) for post in cached[1]]

    async def canary_posts(self, profile_url: str, diagnostic_key: str | None = None) -> list[dict[str, Any]]:
        cached = self.cached_canary_posts(profile_url)
        cache_key = profile_url.rstrip("/")
        if cached is None:
            try:
                await self.profile(profile_url, diagnostic_key)
            except FacebookBrowserError:
                # The profile name may be unparseable even though its rendered
                # timeline already yielded stable post permalinks.
                if self.cached_canary_posts(profile_url) is None:
                    raise
            cached = self.cached_canary_posts(profile_url) or []
        if not cached or cache_key in self._expanded_canary_cache:
            return cached
        expanded = await self._expand_canary_albums(cached)
        self._canary_cache[cache_key] = (time.monotonic(), expanded)
        if not self._album_progress_pending(expanded):
            self._expanded_canary_cache.add(cache_key)
        return [dict(post) for post in expanded]

    async def canary_post_page(
        self,
        profile_url: str,
        diagnostic_key: str | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Return the next small DOM page after a persistent post identity."""
        if not cursor:
            posts = await self.canary_posts(profile_url, diagnostic_key)
            if len(posts) >= self.canary_max_posts:
                next_cursor = str(posts[-1].get("source_post_id") or posts[-1].get("source_url") or "")
                if self._album_progress_pending(posts):
                    # Do not let the timeline cursor orphan an unfinished
                    # album.  The next browser visit resumes its JSON state and
                    # only then advances to the following timeline page.
                    next_cursor = None
                return {"posts": posts, "next_cursor": next_cursor or None, "completed": False}
            # Facebook often omits permalink anchors from its initial DOM.
            # A short first page must scroll before deciding that no cursor is
            # available, otherwise every later visit repeats the empty cache.
        return await self._scroll_post_page(profile_url, cursor)

    async def _scroll_post_page(self, profile_url: str, cursor: str | None) -> dict[str, Any]:
        """Scroll the timeline to establish or advance a persistent cursor."""
        if not self.enabled:
            raise FacebookBrowserError("Facebook 直接瀏覽器備援未啟用")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        posts: list[dict[str, Any]] = []
        next_cursor: str | None = cursor
        completed = False
        async with async_playwright() as playwright:
            try:
                context = await playwright.chromium.launch_persistent_context(
                    str(self.data_dir), headless=True, locale="zh-TW", timezone_id="Asia/Taipei",
                    viewport={"width": 1365, "height": 900}, args=["--disable-dev-shm-usage"],
                )
            except Exception as exc:
                raise FacebookBrowserError(f"無法啟動 Chromium 貼文續抓：{exc}") from exc
            try:
                self._require_login(await context.cookies("https://www.facebook.com"))
                page = context.pages[0] if context.pages else await context.new_page()
                response = await page.goto(profile_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                if response and response.status >= 400:
                    raise FacebookBrowserError(f"Facebook 貼文時間軸 HTTP {response.status}")
                await self._wait_for_profile_content(page)
                await self._raise_for_access_wall(page)
                collected: list[dict[str, Any]] = []
                seen: set[str] = set()
                stagnant = 0
                cursor_found = not bool(cursor)
                for _ in range(12):
                    raw_posts = await self._timeline_posts(page)
                    before = len(collected)
                    for raw in raw_posts:
                        url = normalize_url(str(raw.get("url") or ""))
                        identity = facebook_post_identity(url)
                        key = identity or url
                        if key and key not in seen:
                            collected.append(raw)
                            seen.add(key)
                        if identity == cursor or url == cursor:
                            cursor_found = True
                    page_posts = normalize_browser_canary_posts(collected, self.canary_max_posts, cursor)
                    if cursor_found and len(page_posts) >= self.canary_max_posts:
                        break
                    stagnant = stagnant + 1 if len(collected) == before else 0
                    if stagnant >= 3:
                        break
                    await page.evaluate("window.scrollBy(0, Math.max(window.innerHeight * 0.85, 700))")
                    await page.wait_for_timeout(round(random.uniform(2200, 4200)))
                    await self._raise_for_access_wall(page)
                posts = normalize_browser_canary_posts(collected, self.canary_max_posts, cursor)
                next_cursor = str(posts[-1].get("source_post_id") or posts[-1].get("source_url") or "") if posts else cursor
                completed = bool(cursor) and cursor_found and len(posts) < self.canary_max_posts and stagnant >= 3
            finally:
                await context.close()
        # The persistent profile directory cannot be opened by two Chromium
        # contexts concurrently. Expand albums only after the timeline context
        # has released its lock.
        expanded = await self._expand_canary_albums(posts) if posts else []
        if self._album_progress_pending(expanded):
            next_cursor = cursor
            completed = False
        return {"posts": expanded, "next_cursor": next_cursor, "completed": completed}

    def _album_progress_pending(self, posts: list[dict[str, Any]]) -> bool:
        progress = self._load_album_progress()
        for post in posts:
            post_url = normalize_url(str(post.get("source_url") or ""))
            state = progress.get(post_url) if post_url else None
            if isinstance(state, dict) and not bool(state.get("completed")):
                return True
        return False

    @staticmethod
    async def _timeline_posts(page: Page) -> list[dict[str, Any]]:
        raw_posts = await page.evaluate(
            r"""() => {
                const readImage = (image) => {
                    const rect = image.getBoundingClientRect();
                    return {src: image.currentSrc || image.src || '', natural_width: image.naturalWidth || 0,
                        natural_height: image.naturalHeight || 0, rendered_width: Math.round(rect.width),
                        rendered_height: Math.round(rect.height)};
                };
                return [...document.querySelectorAll('[role="main"] [role="article"], main [role="article"]')]
                    .map((article) => {
                        const links = [...article.querySelectorAll('a[href]')].map((link) => ({
                            url: link.href || '', text: (link.innerText || '').trim(),
                            aria_label: link.getAttribute('aria-label') || '', title: link.title || '',
                            has_image: Boolean(link.querySelector('img, svg image')),
                            is_timestamp: Boolean(link.querySelector('abbr, time'))
                                || Boolean(link.closest('abbr, time')),
                        }));
                        return {links, text: (article.innerText || '').slice(0, 8000),
                            images: [...article.querySelectorAll('img, svg image')].map(readImage)};
                    });
            }"""
        )
        posts: list[dict[str, Any]] = []
        for raw in raw_posts if isinstance(raw_posts, list) else []:
            if not isinstance(raw, dict):
                continue
            url = select_facebook_permalink(raw.pop("links", []))
            if url:
                raw["url"] = url
                posts.append(raw)
        return posts

    async def _expand_canary_albums(self, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Open each selected permalink and walk its photo viewer until it ends."""
        expanded = [dict(post) for post in posts]
        progress = self._load_album_progress()
        attempted_post = False
        operations_used = 0
        batch_deadline = time.monotonic() + self.album_batch_max_seconds
        async with async_playwright() as playwright:
            try:
                context = await playwright.chromium.launch_persistent_context(
                    str(self.data_dir),
                    headless=True,
                    locale="zh-TW",
                    timezone_id="Asia/Taipei",
                    viewport={"width": 1365, "height": 900},
                    args=["--disable-dev-shm-usage"],
                )
            except Exception as exc:
                raise FacebookBrowserError(f"無法啟動 Chromium 相簿補抓：{exc}") from exc
            try:
                self._require_login(await context.cookies("https://www.facebook.com"))
                page = context.pages[0] if context.pages else await context.new_page()
                for post in expanded:
                    post_url = str(post.get("source_url") or "")
                    progress_key = normalize_url(post_url)
                    state = progress.get(progress_key, {})
                    if bool(state.get("completed")):
                        discovered = [
                            str(url) for url in state.get("collected_photos") or [] if url
                        ]
                        next_state = state
                    else:
                        remaining_operations = max(0, self.album_batch_max_operations - operations_used)
                        if remaining_operations <= 0 or time.monotonic() >= batch_deadline:
                            discovered = [
                                str(url) for url in state.get("collected_photos") or [] if url
                            ]
                            next_state = dict(state)
                            next_state.update({
                                "schema_version": 2,
                                "post_url": progress_key,
                                "collected_photos": discovered,
                                "completed": False,
                                "terminal_reason": "",
                                "stalled_reason": "batch_budget_exhausted",
                                "batch_new_photos": 0,
                                "batch_operations": 0,
                                "updated_at": time.time(),
                            })
                            if progress_key:
                                progress[progress_key] = next_state
                                self._save_album_progress(progress)
                        else:
                            if attempted_post:
                                await self._wait_between_canary_posts(page)
                            attempted_post = True
                            try:
                                discovered, next_state = await self._collect_post_album_photos(
                                    page,
                                    post_url,
                                    state,
                                    operation_limit=remaining_operations,
                                    deadline=batch_deadline,
                                )
                            except (FacebookBrowserLoginRequired, FacebookBrowserChallengeRequired):
                                raise
                            except Exception:
                                discovered = [
                                    str(url) for url in state.get("collected_photos") or [] if url
                                ]
                                next_state = dict(state)
                                next_state.update({
                                    "completed": False,
                                    "stalled_reason": "album_read_error",
                                    "updated_at": time.time(),
                                })
                            operations_used += int(next_state.get("batch_operations") or 0)
                    if progress_key:
                        progress[progress_key] = next_state
                        self._save_album_progress(progress)
                    images = [item for item in post.get("images") or [] if isinstance(item, dict) and item.get("url")]
                    seen = {normalize_url(str(item["url"])) for item in images}
                    for url in discovered:
                        asset = normalize_url(url)
                        if asset and asset not in seen:
                            images.append({"url": url})
                            seen.add(asset)
                    if images:
                        post["images"] = images
            finally:
                await context.close()
        return expanded

    @staticmethod
    async def _wait_between_canary_posts(page: Page) -> None:
        """Pause between separate post visits to avoid bursty browser activity."""
        await page.wait_for_timeout(round(random.uniform(8000, 18000)))

    async def _collect_post_album_photos(
        self,
        page: Page,
        post_url: str,
        progress: dict[str, Any] | None = None,
        *,
        operation_limit: int | None = None,
        deadline: float | None = None,
    ) -> tuple[list[str], dict[str, Any]]:
        state = dict(progress or {})
        if not post_url:
            return [], state
        state["schema_version"] = 2
        state["post_url"] = normalize_url(post_url)
        collected = [str(url) for url in state.get("collected_photos") or [] if url]
        seen = {str(value) for value in state.get("seen_assets") or [] if value}
        seen.update(normalize_url(url) for url in collected)
        seen_media_ids = {str(value) for value in state.get("seen_media_ids") or [] if value}
        new_photos = 0
        operations = 0

        def remember(url: str) -> None:
            nonlocal new_photos
            asset = normalize_url(url)
            if not url or not asset or asset in seen:
                return
            collected.append(url)
            seen.add(asset)
            new_photos += 1

        response = await page.goto(post_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        if response and response.status >= 400:
            raise FacebookBrowserError(f"Facebook 貼文頁面 HTTP {response.status}")
        await page.wait_for_timeout(round(random.uniform(2200, 3800)))
        await self._raise_for_access_wall(page)
        for url in await self._large_facebook_images(
            page,
            "[role='main'] [role='article'] img, main [role='article'] img",
        ):
            remember(url)

        if bool(state.get("completed")):
            return collected, state

        photo_links = await page.locator(
            "[role='main'] [role='article'] a[href*='/photo'], main [role='article'] a[href*='/photo']"
        ).evaluate_all("nodes => nodes.map(node => node.href || '').filter(Boolean)")
        first_photo_url = next(
            (normalize_url(str(url)) for url in photo_links if is_facebook_permalink(url)),
            "",
        )
        resume_url = str(state.get("resume_url") or "")
        if resume_url and (not _is_facebook_host(resume_url) or not is_facebook_permalink(resume_url)):
            resume_url = ""
        viewer_url = normalize_url(resume_url) or first_photo_url
        if not viewer_url:
            state.update({
                "seen_assets": sorted(seen),
                "seen_media_ids": sorted(seen_media_ids),
                "collected_photos": collected,
                "resume_url": "",
                "completed": False,
                "terminal_reason": "",
                "stalled_reason": "photo_link_missing",
                "batch_new_photos": new_photos,
                "batch_operations": operations,
                "updated_at": time.time(),
            })
            return collected, state

        await page.goto(viewer_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        await page.wait_for_timeout(round(random.uniform(1800, 3200)))
        await self._raise_for_access_wall(page)
        first_media_id = str(state.get("first_media_id") or "")
        viewer_seen: set[str] = set()
        successful_transitions = 0
        started_at = time.monotonic()
        operation_limit = self.album_batch_max_operations if operation_limit is None else max(0, operation_limit)
        deadline = started_at + self.album_batch_max_seconds if deadline is None else deadline
        completed = False
        terminal_reason = ""
        stalled_reason = ""
        resume_url = viewer_url
        declared_total = int(state.get("declared_total") or 0)
        # This remains a hard loop-safety circuit breaker.  The configured
        # operation and elapsed-time limits normally stop a batch much sooner.
        for _ in range(500):
            current = await self._largest_viewer_image(page)
            asset = normalize_url(current)
            media_id = await self._current_viewer_media_id(page)
            position, total = await self._viewer_position(page)
            if total:
                declared_total = max(declared_total, total)
            identity = media_id or asset
            if not current or not asset or not identity:
                stalled_reason = "viewer_media_missing"
                break
            if (
                media_id
                and first_media_id
                and media_id == first_media_id
                and media_id in seen_media_ids
                and len(seen_media_ids) >= 2
                and successful_transitions >= 1
            ):
                completed = True
                terminal_reason = "returned_to_first_media"
                break
            if identity in viewer_seen:
                stalled_reason = "viewer_cycle_stalled"
                break
            viewer_seen.add(identity)
            if media_id:
                if not first_media_id:
                    first_media_id = media_id
                seen_media_ids.add(media_id)
            remember(current)
            resume_url = normalize_url(str(getattr(page, "url", "") or "")) or viewer_url
            if position and total and position >= total:
                completed = True
                terminal_reason = "declared_last_position"
                break
            if (
                new_photos >= self.album_batch_max_new_photos
                or operations >= operation_limit
                or time.monotonic() >= deadline
            ):
                break
            previous_identity = identity
            if not await self._click_next_photo(page):
                stalled_reason = "next_control_missing"
                break
            operations += 1
            changed = False
            for _ in range(8):
                await page.wait_for_timeout(round(random.uniform(300, 550)))
                next_media_id = await self._current_viewer_media_id(page)
                next_asset = normalize_url(await self._largest_viewer_image(page))
                next_identity = next_media_id or next_asset
                if next_identity and next_identity != previous_identity:
                    changed = True
                    break
            if not changed:
                stalled_reason = "viewer_did_not_advance"
                break
            successful_transitions += 1
            await self._raise_for_access_wall(page)
            await page.wait_for_timeout(round(random.uniform(3000, 7000)))
        state.update({
            "schema_version": 2,
            "post_url": normalize_url(post_url),
            "seen_assets": sorted(seen),
            "seen_media_ids": sorted(seen_media_ids),
            "collected_photos": collected,
            "first_media_id": first_media_id,
            "resume_url": "" if completed else resume_url,
            "completed": completed,
            "terminal_reason": terminal_reason,
            "stalled_reason": "" if completed else stalled_reason,
            "declared_total": declared_total or None,
            "batch_new_photos": new_photos,
            "batch_operations": operations,
            "total_operations": int(state.get("total_operations") or 0) + operations,
            "updated_at": time.time(),
        })
        return collected, state

    @staticmethod
    async def _current_viewer_media_id(page: Page) -> str:
        page_url = str(getattr(page, "url", "") or "")
        parsed_page_url = urlsplit(page_url)
        page_query = {key.casefold(): value for key, value in parse_qs(parsed_page_url.query).items()}
        page_path = parsed_page_url.path.rstrip("/").casefold()
        page_is_photo = bool(
            page_query.get("fbid")
            or "/photos/" in page_path
            or page_path in {"/photo", "/photo.php"}
        )
        candidates: list[str] = [page_url] if page_is_photo else []
        try:
            candidates.extend(
                await page.locator(
                    "link[rel='canonical'], [role='dialog'] a[href*='fbid='], "
                    "[role='dialog'] a[href*='/photos/']"
                ).evaluate_all(
                    "nodes => nodes.map(node => node.href || '').filter(Boolean)"
                )
            )
        except AttributeError:
            pass
        for url in candidates:
            parsed = urlsplit(str(url))
            query = {key.casefold(): value for key, value in parse_qs(parsed.query).items()}
            path = parsed.path.rstrip("/").casefold()
            # A post permalink may remain in location.href while its photo
            # dialog advances.  It is not a media ID and must not make every
            # viewer frame look identical.
            if not (query.get("fbid") or "/photos/" in path or path in {"/photo", "/photo.php"}):
                continue
            if identity := facebook_post_identity(str(url)):
                return identity
        return ""

    @staticmethod
    async def _viewer_position(page: Page) -> tuple[int | None, int | None]:
        try:
            values = await page.locator(
                "[role='dialog'] [aria-label], [role='dialog'] [title], "
                "[role='dialog'] [role='heading']"
            ).evaluate_all(
                "nodes => nodes.flatMap(node => [node.getAttribute('aria-label') || '', "
                "node.getAttribute('title') || '', node.innerText || '']).filter(Boolean)"
            )
        except AttributeError:
            return None, None
        for value in values if isinstance(values, list) else []:
            position = _parse_viewer_position(value)
            if position != (None, None):
                return position
        return None, None

    @staticmethod
    async def _large_facebook_images(page: Page, selector: str) -> list[str]:
        return await page.evaluate(
            r"""selector => [...document.querySelectorAll(selector)]
                .map((image) => ({
                    src: image.currentSrc || image.src || '',
                    width: image.naturalWidth || image.getBoundingClientRect().width || 0,
                    height: image.naturalHeight || image.getBoundingClientRect().height || 0,
                }))
                .filter((image) => image.src.includes('fbcdn.net') && image.width >= 180 && image.height >= 180)
                .sort((left, right) => (right.width * right.height) - (left.width * left.height))
                .map((image) => image.src)""",
            selector,
        )

    async def _largest_viewer_image(self, page: Page) -> str:
        candidates = await self._large_facebook_images(
            page,
            "[role='dialog'] img, [role='main'] img, main img",
        )
        return candidates[0] if candidates else ""

    @staticmethod
    async def _click_next_photo(page: Page) -> bool:
        controls = page.locator(
            "[aria-label='下一張相片'], [aria-label='Next photo'], "
            "[aria-label='下一張'], [aria-label='Next']"
        )
        for index in range(await controls.count()):
            control = controls.nth(index)
            if await control.is_visible():
                try:
                    await control.click(timeout=5000)
                    return True
                except PlaywrightTimeoutError:
                    continue
        return False

    async def profile(self, profile_url: str, diagnostic_key: str | None = None) -> dict[str, Any]:
        if not self.enabled:
            raise FacebookBrowserError("Facebook 直接瀏覽器備援未啟用")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        async with async_playwright() as playwright:
            try:
                context = await playwright.chromium.launch_persistent_context(
                    str(self.data_dir),
                    headless=True,
                    locale="zh-TW",
                    timezone_id="Asia/Taipei",
                    viewport={"width": 1365, "height": 900},
                    args=["--disable-dev-shm-usage"],
                )
            except Exception as exc:
                raise FacebookBrowserError(f"無法啟動 Chromium：{exc}") from exc
            try:
                if self.require_login:
                    self._require_login(await context.cookies("https://www.facebook.com"))
                page = context.pages[0] if context.pages else await context.new_page()
                try:
                    return await self._read_profile(page, profile_url, diagnostic_key)
                except FacebookBrowserError:
                    raise
                except Exception as exc:
                    await self._save_failure(page, diagnostic_key)
                    raise FacebookBrowserError(f"Facebook 頁面解析失敗：{exc.__class__.__name__}") from exc
            finally:
                await context.close()

    @staticmethod
    def _require_login(cookies: list[dict[str, Any]]) -> None:
        if not any(cookie.get("name") == "c_user" and cookie.get("value") for cookie in cookies):
            raise FacebookBrowserLoginRequired("尚未建立 Facebook 瀏覽器登入狀態")

    @staticmethod
    async def _access_wall_ui_text(page: Page) -> str:
        """Read verification UI without trusting monitored post contents."""
        try:
            value = await page.evaluate(
                r"""() => [...document.querySelectorAll(
                    '[role="dialog"], [role="alertdialog"], [role="alert"], '
                    + 'form[action*="checkpoint"], form[action*="login"], '
                    + 'h1, [role="heading"]'
                )]
                    .filter((node) => !node.closest('[role="article"], article'))
                    .map((node) => (node.innerText || node.textContent || '').trim())
                    .filter(Boolean)
                    .join('\n')
                    .slice(0, 50000)"""
            )
            return value if isinstance(value, str) else ""
        except (AttributeError, PlaywrightTimeoutError):
            return ""

    async def _raise_for_access_wall(self, page: Page, diagnostic_key: str | None = None) -> None:
        """Raise typed errors for login and checkpoint pages in every flow."""
        current_url = str(getattr(page, "url", "") or "").casefold()
        folded = (await self._access_wall_ui_text(page)).casefold()
        challenge = any(part in current_url for part in ("/checkpoint/", "/challenge/", "/recover/")) or any(
            marker in folded
            for marker in (
                "security check",
                "confirm your identity",
                "confirm your account",
                "確認你的身分",
                "確認你的帳號",
                "安全檢查",
            )
        )
        if challenge:
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserChallengeRequired("Facebook 要求安全驗證，需重新進行互動式登入")

        login_inputs = 0
        try:
            login_inputs = await page.locator("input[name='email'], input[type='password']").count()
        except AttributeError:
            pass
        if "/login" in current_url or login_inputs:
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserLoginRequired("Facebook 登入狀態已失效，需重新進行互動式登入")

    async def _wait_for_profile_content(self, page: Page) -> None:
        """Allow Facebook's client-rendered heading and useful images to settle."""
        await page.wait_for_timeout(3000)
        try:
            await page.wait_for_function(
                """() => {
                    const heading = document.querySelector('[role="main"] h1, main h1, h1, [role="heading"][aria-level="1"]');
                    if (!heading || !(heading.innerText || '').trim()) return false;
                    const visibleImages = [...document.querySelectorAll('img, svg image')].filter((image) => {
                        const src = image.currentSrc || image.src || image.href?.baseVal || image.getAttribute('href') || '';
                        const rect = image.getBoundingClientRect();
                        return src.includes('fbcdn.net') && rect.width > 0 && rect.height > 0;
                    });
                    return visibleImages.length === 0 || visibleImages.some(
                        (image) => {
                            const rect = image.getBoundingClientRect();
                            return (image.complete && image.naturalWidth >= 180 && image.naturalHeight >= 180)
                                || (image.tagName.toLowerCase() === 'image' && rect.width >= 96 && rect.height >= 96);
                        }
                    );
                }""",
                timeout=5000,
            )
        except PlaywrightTimeoutError:
            # Continue with best-effort parsing; the overall navigation timeout
            # and the existing login/challenge checks still govern failure.
            pass

    async def _read_profile(self, page: Page, profile_url: str, diagnostic_key: str | None) -> dict[str, Any]:
        try:
            response = await page.goto(profile_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            await self._wait_for_profile_content(page)
        except PlaywrightTimeoutError as exc:
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserError("Facebook 頁面載入逾時") from exc
        await self._save_capture(page, diagnostic_key)
        await self._raise_for_access_wall(page, diagnostic_key)
        if response and response.status >= 400:
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserError(f"Facebook 頁面 HTTP {response.status}")

        raw = await page.evaluate(
            r"""() => {
                const meta = (property) => document.querySelector(`meta[property="${property}"]`)?.content || '';
                const readImage = (image) => {
                    const rect = image.getBoundingClientRect();
                    return {
                        src: image.currentSrc || image.src || image.href?.baseVal || image.getAttribute('href') || '',
                        alt: image.alt || image.getAttribute('aria-label') || image.closest('[aria-label]')?.getAttribute('aria-label') || '',
                        natural_width: image.naturalWidth || 0, natural_height: image.naturalHeight || 0,
                        rendered_width: Math.round(rect.width), rendered_height: Math.round(rect.height),
                        x: Math.round(rect.x), y: Math.round(rect.y),
                    };
                };
                const images = [...document.querySelectorAll('img, svg image')].map(readImage);
                const posts = [...document.querySelectorAll('[role="main"] [role="article"], main [role="article"]')]
                    .map((article) => {
                        const links = [...article.querySelectorAll('a[href]')].map((link) => ({
                            url: link.href || '', text: (link.innerText || '').trim(),
                            aria_label: link.getAttribute('aria-label') || '', title: link.title || '',
                            has_image: Boolean(link.querySelector('img, svg image')),
                            is_timestamp: Boolean(link.querySelector('abbr, time'))
                                || Boolean(link.closest('abbr, time')),
                        }));
                        return {
                            links,
                            text: (article.innerText || '').slice(0, 8000),
                            images: [...article.querySelectorAll('img, svg image')].map(readImage),
                        };
                    })
                    ;
                const text = (document.body?.innerText || '').slice(0, 200000);
                const profileRoot = document.querySelector('[role="main"], main') || document.body;
                const profileCopy = profileRoot?.cloneNode(true);
                profileCopy?.querySelectorAll('[role="article"], article').forEach((node) => node.remove());
                const profileText = (profileCopy?.textContent || '').slice(0, 100000);
                return {
                    title: document.title || '', heading: document.querySelector('h1')?.innerText || '',
                    main_heading: document.querySelector('[role="main"] h1, main h1')?.innerText || '',
                    headings: [...document.querySelectorAll('h1')].map((node) => node.innerText || '').filter(Boolean),
                    role_headings: [...document.querySelectorAll('[role="main"] [role="heading"], [role="heading"][aria-level="1"]')]
                        .map((node) => node.innerText || '').filter(Boolean),
                    og_title: meta('og:title'), og_description: meta('og:description'),
                    og_image: meta('og:image'), og_url: meta('og:url'), text, images, posts,
                    profile_text: profileText,
                    private: /profile is locked|this profile is locked|這份個人檔案已鎖定|已鎖定個人檔案/i.test(profileText),
                };
            }"""
        )
        # Keep the final rendered URL separate from the requested target.
        # Privacy/public evidence may use this observed value, but must never
        # let the request URL self-prove the page identity.
        raw["page_url"] = str(page.url or "")
        for post in raw.get("posts") if isinstance(raw.get("posts"), list) else []:
            if not isinstance(post, dict):
                continue
            post["url"] = select_facebook_permalink(post.pop("links", []))
        raw["posts"] = [post for post in raw.get("posts", []) if isinstance(post, dict) and post.get("url")]
        item = normalize_browser_profile(raw, profile_url)
        if not self.require_login:
            proof = public_content_proof(raw, profile_url)
            if proof:
                item["public_content_proof"] = proof
        canary_posts = normalize_browser_canary_posts(
            raw.get("posts"),
            self.canary_max_posts,
        )
        cache_key = profile_url.rstrip("/")
        self._canary_cache[cache_key] = (time.monotonic(), canary_posts)
        self._expanded_canary_cache.discard(cache_key)
        profile_folded = str(raw.get("profile_text") or "").casefold()
        unavailable = any(marker in profile_folded for marker in ("this content isn't available", "content not found", "這則內容目前無法顯示", "找不到這個頁面"))
        if unavailable or not item.get("name"):
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserError("Facebook 直接瀏覽器未取得可用的個人檔案資料")
        return item

    def screenshot_path(self, diagnostic_key: str) -> Path:
        safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", diagnostic_key)[:80] or "unknown"
        return self.data_dir / "screenshots" / f"profile-{safe_key}.png"

    async def _save_capture(self, page: Page, diagnostic_key: str | None) -> Path | None:
        if not diagnostic_key:
            return None
        target = self.screenshot_path(diagnostic_key)
        original_viewport = page.viewport_size or {"width": 1365, "height": 900}
        expanded_viewport = {
            "width": int(original_viewport["width"]),
            "height": int(original_viewport["height"]) * CAPTURE_VIEWPORT_MULTIPLIER,
        }
        resized = False
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            await page.set_viewport_size(expanded_viewport)
            resized = True
            await page.evaluate("window.scrollTo(0, 0)")
            # Facebook lazily renders timeline content according to the visible
            # viewport. Give the newly visible second and third screens time to
            # settle before taking the diagnostic capture.
            await page.wait_for_timeout(2000)
            await page.screenshot(path=str(target), full_page=False)
            return target
        except Exception:
            return None
        finally:
            if resized:
                try:
                    await page.set_viewport_size(original_viewport)
                except Exception:
                    pass

    async def _save_failure(self, page: Page, diagnostic_key: str | None) -> None:
        try:
            await self._save_capture(page, diagnostic_key)
            await page.screenshot(path=str(self.data_dir / "last-failure.png"), full_page=False)
        except Exception:
            pass
