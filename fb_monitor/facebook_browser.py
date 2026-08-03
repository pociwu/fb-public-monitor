from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError, async_playwright

from .normalize import normalize_url


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
    name_candidates = [
        aliased_summary, raw.get("main_heading"), *role_headings, *headings,
        _name_from_profile_image(images), summary_name,
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
    max_photos_per_post: int = 3,
) -> list[dict[str, Any]]:
    """Normalize only visible posts that expose a stable Facebook permalink."""
    if not isinstance(raw_posts, list) or max_posts <= 0:
        return []
    posts: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
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
        post_id = (parse_qs(parsed.query).get("story_fbid") or [""])[0]
        if not post_id:
            match = re.search(r"/(?:posts|videos|reel)/([^/?#]+)", parsed.path, flags=re.IGNORECASE)
            post_id = match.group(1) if match else ""
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
            if len(photos) >= max_photos_per_post:
                break
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
        canary_max_photos_per_post: int = 3,
    ):
        self.enabled = enabled
        self.data_dir = data_dir
        self.timeout_ms = max(10, timeout_seconds) * 1000
        self.canary_max_posts = max(0, min(2, canary_max_posts))
        self.canary_max_photos_per_post = max(0, min(3, canary_max_photos_per_post))
        self._canary_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

    def cached_canary_posts(self, profile_url: str, max_age_seconds: int = 600) -> list[dict[str, Any]] | None:
        cached = self._canary_cache.get(profile_url.rstrip("/"))
        if not cached or time.monotonic() - cached[0] > max_age_seconds:
            return None
        return [dict(post) for post in cached[1]]

    async def canary_posts(self, profile_url: str, diagnostic_key: str | None = None) -> list[dict[str, Any]]:
        cached = self.cached_canary_posts(profile_url)
        if cached is not None:
            return cached
        await self.profile(profile_url, diagnostic_key)
        return self.cached_canary_posts(profile_url) or []

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
            self.canary_max_photos_per_post,
        )
        self._canary_cache[profile_url.rstrip("/")] = (time.monotonic(), canary_posts)
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
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(target), full_page=False)
            return target
        except Exception:
            return None

    async def _save_failure(self, page: Page, diagnostic_key: str | None) -> None:
        try:
            await self._save_capture(page, diagnostic_key)
            await page.screenshot(path=str(self.data_dir / "last-failure.png"), full_page=False)
        except Exception:
            pass
