from __future__ import annotations

import json
import random
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError, async_playwright

from .normalize import facebook_post_identity, normalize_url

CAPTURE_VIEWPORT_MULTIPLIER = 3


class FacebookBrowserError(RuntimeError):
    pass


class FacebookBrowserLoginRequired(FacebookBrowserError):
    pass


class FacebookBrowserChallengeRequired(FacebookBrowserLoginRequired):
    pass


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
    item: dict[str, Any] = {
        "id": _numeric_profile_id(canonical) or _numeric_profile_id(profile_url),
        "name": name,
        "url": canonical,
        "private": bool(raw.get("private")),
        "profile_data_source": "Facebook 直接瀏覽器",
    }
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
) -> list[dict[str, Any]]:
    """Normalize only visible posts that expose a stable Facebook permalink."""
    if not isinstance(raw_posts, list) or max_posts <= 0:
        return []
    posts: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    seen_post_ids: set[str] = set()
    for raw in raw_posts:
        if not isinstance(raw, dict):
            continue
        source_url = normalize_url(str(raw.get("url") or ""))
        parsed = urlsplit(source_url)
        host = (parsed.hostname or "").casefold()
        if host != "facebook.com" and not host.endswith(".facebook.com"):
            continue
        permalink = any(
            marker in parsed.path.casefold()
            for marker in ("/posts/", "/photos/", "/videos/", "/reel/", "/share/p/")
        ) or parsed.path.casefold().endswith("/permalink.php") or bool(parse_qs(parsed.query).get("story_fbid"))
        if not permalink or source_url in seen_urls:
            continue
        post_id = facebook_post_identity(source_url)
        if post_id and post_id in seen_post_ids:
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
    ):
        self.enabled = enabled
        self.data_dir = data_dir
        self.timeout_ms = max(10, timeout_seconds) * 1000
        self.canary_max_posts = max(0, min(2, canary_max_posts))
        self.album_batch_max_new_photos = 30
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
        self._expanded_canary_cache.add(cache_key)
        return [dict(post) for post in expanded]

    async def _expand_canary_albums(self, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Open each selected permalink and walk its photo viewer until it ends."""
        expanded = [dict(post) for post in posts]
        progress = self._load_album_progress()
        attempted_post = False
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
                        discovered = []
                        next_state = state
                    else:
                        if attempted_post:
                            await self._wait_between_canary_posts(page)
                        attempted_post = True
                        try:
                            discovered, next_state = await self._collect_post_album_photos(page, post_url, state)
                        except Exception:
                            discovered = []
                            next_state = state
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
    ) -> tuple[list[str], dict[str, Any]]:
        state = dict(progress or {})
        if not post_url:
            return [], state
        response = await page.goto(post_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        if response and response.status >= 400:
            raise FacebookBrowserError(f"Facebook 貼文頁面 HTTP {response.status}")
        await page.wait_for_timeout(round(random.uniform(2200, 3800)))
        photos = await self._large_facebook_images(page, "[role='main'] [role='article'] img, main [role='article'] img")
        seen = {str(value) for value in state.get("seen_assets") or [] if value}
        seen.update(normalize_url(url) for url in photos)

        if bool(state.get("completed")):
            return photos, state

        photo_links = await page.locator(
            "[role='main'] [role='article'] a[href*='/photo'], main [role='article'] a[href*='/photo']"
        ).evaluate_all("nodes => nodes.map(node => node.href || '').filter(Boolean)")
        first_photo_url = next((str(url) for url in photo_links if "/photo" in str(url)), "")
        resume_url = str(state.get("resume_url") or "")
        resume_host = (urlsplit(resume_url).hostname or "").casefold()
        if resume_host != "facebook.com" and not resume_host.endswith(".facebook.com"):
            resume_url = ""
        first_photo_url = resume_url or first_photo_url
        if not first_photo_url:
            state.update({"seen_assets": sorted(seen), "resume_url": "", "completed": True})
            return photos, state

        await page.goto(first_photo_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
        await page.wait_for_timeout(round(random.uniform(1800, 3200)))
        previous_asset = ""
        viewer_seen: set[str] = set()
        new_photos = 0
        started_at = time.monotonic()
        completed = False
        resume_url = first_photo_url
        # This is a loop-safety circuit breaker, not a configured photo limit.
        # Normal termination is no next button or returning to a seen photo.
        for _ in range(500):
            current = await self._largest_viewer_image(page)
            asset = normalize_url(current)
            if not current or (asset and asset in viewer_seen):
                completed = True
                break
            viewer_seen.add(asset)
            if asset not in seen:
                photos.append(current)
                seen.add(asset)
                new_photos += 1
            previous_asset = asset
            resume_url = page.url
            if (
                new_photos >= self.album_batch_max_new_photos
                or time.monotonic() - started_at >= self.album_batch_max_seconds
            ):
                break
            if not await self._click_next_photo(page):
                completed = True
                break
            changed = False
            for _ in range(8):
                await page.wait_for_timeout(round(random.uniform(300, 550)))
                next_asset = normalize_url(await self._largest_viewer_image(page))
                if next_asset and next_asset != previous_asset:
                    changed = True
                    break
            if not changed:
                completed = True
                break
            await page.wait_for_timeout(round(random.uniform(3000, 7000)))
        state.update({
            "seen_assets": sorted(seen),
            "resume_url": "" if completed else resume_url,
            "completed": completed,
            "updated_at": time.time(),
        })
        return photos, state

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
        current_url = page.url.casefold()
        body = (await page.locator("body").inner_text(timeout=10_000))[:200_000]
        folded = body.casefold()
        if any(part in current_url for part in ("/checkpoint/", "/challenge/", "/recover/")) or any(
            marker in folded for marker in ("security check", "confirm your identity", "確認你的身分", "安全檢查")
        ):
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserChallengeRequired("Facebook 要求安全驗證，需重新進行互動式登入")
        if "/login" in current_url or await page.locator("input[name='email'], input[type='password']").count():
            await self._save_failure(page, diagnostic_key)
            raise FacebookBrowserLoginRequired("Facebook 登入狀態已失效，需重新進行互動式登入")
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
                const permalinkPattern = /\/posts\/|\/photos\/|\/videos\/|\/reel\/|\/share\/p\/|\/permalink\.php|story_fbid=/i;
                const posts = [...document.querySelectorAll('[role="main"] [role="article"], main [role="article"]')]
                    .map((article) => {
                        const links = [...article.querySelectorAll('a[href]')].map((link) => link.href || '');
                        const url = links.find((href) => permalinkPattern.test(href)) || '';
                        return {
                            url,
                            text: (article.innerText || '').slice(0, 8000),
                            images: [...article.querySelectorAll('img, svg image')].map(readImage),
                        };
                    })
                    .filter((post) => post.url);
                const text = (document.body?.innerText || '').slice(0, 200000);
                return {
                    title: document.title || '', heading: document.querySelector('h1')?.innerText || '',
                    main_heading: document.querySelector('[role="main"] h1, main h1')?.innerText || '',
                    headings: [...document.querySelectorAll('h1')].map((node) => node.innerText || '').filter(Boolean),
                    role_headings: [...document.querySelectorAll('[role="main"] [role="heading"], [role="heading"][aria-level="1"]')]
                        .map((node) => node.innerText || '').filter(Boolean),
                    og_title: meta('og:title'), og_description: meta('og:description'),
                    og_image: meta('og:image'), og_url: meta('og:url'), text, images, posts,
                    private: /profile is locked|this profile is locked|這份個人檔案已鎖定|已鎖定個人檔案/i.test(text),
                };
            }"""
        )
        item = normalize_browser_profile(raw, profile_url)
        canary_posts = normalize_browser_canary_posts(
            raw.get("posts"),
            self.canary_max_posts,
        )
        cache_key = profile_url.rstrip("/")
        self._canary_cache[cache_key] = (time.monotonic(), canary_posts)
        self._expanded_canary_cache.discard(cache_key)
        unavailable = any(marker in folded for marker in ("this content isn't available", "content not found", "這則內容目前無法顯示", "找不到這個頁面"))
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
