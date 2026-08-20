import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from fb_monitor.facebook_browser import (
    FacebookBrowserChallengeRequired,
    FacebookBrowserGateway,
    FacebookBrowserLoginRequired,
    normalize_browser_canary_posts,
    normalize_browser_profile,
    public_content_proof,
    public_content_proof_matches_profile,
    select_facebook_permalink,
)
from fb_monitor.normalize import normalize_url


class EmptyCookieBrowser:
    class Response:
        status = 200

    class Locator:
        def __init__(self, selector: str):
            self.selector = selector

        async def inner_text(self, timeout: int):
            if self.selector == "body":
                return "Anonymous User\n1 位追蹤者"
            return ""

        async def count(self):
            return 0

    class Page:
        def __init__(self):
            self.url = ""
            self.viewport_size = {"width": 1365, "height": 900}
            self.visited: list[str] = []

        async def goto(self, url: str, **kwargs):
            self.url = url
            self.visited.append(url)
            return EmptyCookieBrowser.Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

        async def wait_for_function(self, expression: str, *, timeout: int):
            return None

        async def evaluate(self, expression: str):
            return {
                "title": "Anonymous User | Facebook",
                "heading": "Anonymous User",
                "main_heading": "Anonymous User",
                "headings": ["Anonymous User"],
                "role_headings": ["Anonymous User"],
                "og_title": "Anonymous User | Facebook",
                "og_description": "",
                "og_image": "",
                "og_url": self.url,
                "text": "Anonymous User\n1 位追蹤者",
                "images": [],
                "posts": [],
                "private": False,
            }

        def locator(self, selector: str):
            return EmptyCookieBrowser.Locator(selector)

    class Context:
        def __init__(self):
            self.page = EmptyCookieBrowser.Page()
            self.pages = [self.page]
            self.closed = False
            self.cookie_reads = 0

        async def cookies(self, url: str):
            self.cookie_reads += 1
            return []

        async def new_page(self):
            return self.page

        async def close(self):
            self.closed = True

    class Chromium:
        def __init__(self, context):
            self.context = context

        async def launch_persistent_context(self, *args, **kwargs):
            return self.context

    class Playwright:
        def __init__(self, context):
            self.chromium = EmptyCookieBrowser.Chromium(context)

    class Manager:
        def __init__(self, context):
            self.playwright = EmptyCookieBrowser.Playwright(context)

        async def __aenter__(self):
            return self.playwright

        async def __aexit__(self, exc_type, exc, traceback):
            return False


class PublicPermalinkBrowser(EmptyCookieBrowser):
    class Page(EmptyCookieBrowser.Page):
        async def evaluate(self, expression: str):
            raw = await super().evaluate(expression)
            raw["posts"] = [
                {
                    "links": [
                        {
                            "url": (
                                "https://www.facebook.com/permalink.php?"
                                "story_fbid=pfbid0PublicPost&id=100123"
                            ),
                            "text": "2 小時",
                            "aria_label": "2 小時",
                            "title": "",
                            "has_image": False,
                            "is_timestamp": True,
                        }
                    ],
                    "text": "Anonymous User\n這是一則公開貼文",
                    "images": [],
                }
            ]
            return raw

    class Context(EmptyCookieBrowser.Context):
        def __init__(self):
            self.page = PublicPermalinkBrowser.Page()
            self.pages = [self.page]
            self.closed = False
            self.cookie_reads = 0


@pytest.mark.asyncio
async def test_anonymous_profile_with_empty_cookies_still_reads_and_normalizes_identity(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()
    context = browser.Context()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)

    item = await gateway.profile("https://www.facebook.com/100123")

    assert item["id"] == "100123"
    assert item["observed_profile_identity"] == "100123"
    assert item["name"] == "Anonymous User"
    assert item["private"] is False
    assert "public_content_proof" not in item
    assert context.cookie_reads == 0
    assert context.page.visited == ["https://www.facebook.com/100123"]
    assert context.closed is True


@pytest.mark.asyncio
async def test_anonymous_profile_attaches_identity_bound_public_permalink_proof(
    tmp_path: Path, monkeypatch
):
    browser = PublicPermalinkBrowser()
    context = browser.Context()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)

    item = await gateway.profile("https://www.facebook.com/100123")

    assert item["public_content_proof"] == {
        "kind": "target_permalink_article",
        "permalink": (
            "https://www.facebook.com/permalink.php?"
            "story_fbid=pfbid0PublicPost&id=100123"
        ),
        "post_identity": "pfbid0PublicPost",
        "target_identity": "100123",
        "article_index": 0,
    }
    assert public_content_proof_matches_profile(
        item["public_content_proof"], "https://www.facebook.com/100123"
    ) is True


def test_public_content_proof_rejects_profile_metadata_and_other_accounts_posts():
    name_only = {
        "text": "Anonymous User\n1 位追蹤者\n相片\n關於",
        "headings": ["Anonymous User"],
        "images": [
            {
                "src": "https://scontent.example.fbcdn.net/avatar.jpg",
                "alt": "Anonymous User 的大頭貼照片",
                "rendered_width": 168,
                "rendered_height": 168,
            }
        ],
        "posts": [],
    }
    shared_other_account = {
        **name_only,
        "posts": [
            {
                "url": (
                    "https://www.facebook.com/permalink.php?"
                    "story_fbid=pfbid0OtherPost&id=999999"
                ),
                "text": "Other account shared post",
            }
        ],
    }

    assert public_content_proof(name_only, "https://www.facebook.com/100123") is None
    assert public_content_proof(
        shared_other_account, "https://www.facebook.com/100123"
    ) is None


@pytest.mark.asyncio
async def test_logged_profile_with_empty_cookies_still_requires_login(tmp_path: Path, monkeypatch):
    browser = EmptyCookieBrowser()
    context = browser.Context()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path)

    with pytest.raises(FacebookBrowserLoginRequired, match="尚未建立"):
        await gateway.profile("https://www.facebook.com/100123")

    assert context.cookie_reads == 1
    assert context.page.visited == []
    assert context.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("posts", [[], [{"source_post_id": "p1"}]])
async def test_initial_browser_post_page_scrolls_when_initial_dom_is_short(tmp_path: Path, posts):
    gateway = FacebookBrowserGateway(True, tmp_path, canary_max_posts=2)
    scrolled = []

    async def fake_canary_posts(profile_url, diagnostic_key=None):
        return posts

    async def fake_scroll(profile_url, cursor):
        scrolled.append((profile_url, cursor))
        return {"posts": [], "next_cursor": None, "completed": False}

    gateway.canary_posts = fake_canary_posts
    gateway._scroll_post_page = fake_scroll

    page = await gateway.canary_post_page("https://facebook.com/1")

    assert page["completed"] is False
    assert scrolled == [("https://facebook.com/1", None)]


@pytest.mark.asyncio
async def test_initial_browser_post_page_uses_full_initial_dom_without_extra_scroll(tmp_path: Path):
    gateway = FacebookBrowserGateway(True, tmp_path, canary_max_posts=2)
    posts = [{"source_post_id": "p1"}, {"source_post_id": "p2"}]

    async def fake_canary_posts(profile_url, diagnostic_key=None):
        return posts

    async def unexpected_scroll(profile_url, cursor):
        raise AssertionError("a full initial page should establish the cursor directly")

    gateway.canary_posts = fake_canary_posts
    gateway._scroll_post_page = unexpected_scroll

    page = await gateway.canary_post_page("https://facebook.com/1")

    assert page == {"posts": posts, "next_cursor": "p2", "completed": False}


@pytest.mark.asyncio
async def test_browser_post_cursor_waits_for_serialized_album_progress(tmp_path: Path):
    gateway = FacebookBrowserGateway(True, tmp_path, canary_max_posts=2)
    posts = [
        {"source_post_id": "p1", "source_url": "https://facebook.com/example/posts/p1"},
        {"source_post_id": "p2", "source_url": "https://facebook.com/example/posts/p2"},
    ]

    async def fake_canary_posts(profile_url, diagnostic_key=None):
        return posts

    gateway.canary_posts = fake_canary_posts
    progress_key = normalize_url(posts[0]["source_url"])
    gateway._save_album_progress({
        progress_key: {
            "schema_version": 2,
            "post_url": progress_key,
            "collected_photos": ["https://scontent.example.fbcdn.net/v/p1.jpg"],
            "completed": False,
            "resume_url": "https://www.facebook.com/photo.php?fbid=1",
        }
    })

    pending = await gateway.canary_post_page("https://facebook.com/example")
    assert pending["next_cursor"] is None

    progress = gateway._load_album_progress()
    progress[progress_key]["completed"] = True
    gateway._save_album_progress(progress)
    finished = await gateway.canary_post_page("https://facebook.com/example")
    assert finished["next_cursor"] == "p2"


def test_normalize_browser_profile_extracts_card_fields():
    item = normalize_browser_profile(
        {
            "heading": "王小明",
            "og_title": "王小明 | Facebook",
            "og_url": "https://www.facebook.com/people/example/100123/",
            "og_image": "https://scontent.example.fbcdn.net/fallback.jpg",
            "text": "王小明\n簡介\n今天很好\n現居\n台北市\n任職於\nExample Inc.\n1.2 萬位追蹤者",
            "images": [
                {"src": "https://scontent.example.fbcdn.net/avatar.jpg", "alt": "王小明的大頭貼照片", "natural_width": 720, "natural_height": 720},
                {"src": "https://scontent.example.fbcdn.net/cover.jpg", "alt": "王小明的封面照片", "natural_width": 1600, "natural_height": 600},
                {"src": "https://scontent.example.fbcdn.net/photo.jpg", "alt": "", "natural_width": 1080, "natural_height": 1080},
            ],
        },
        "https://www.facebook.com/100123",
    )
    assert item["id"] == "100123"
    assert item["name"] == "王小明"
    assert item["profile_picture"].endswith("/avatar.jpg")
    assert item["cover_photo"].endswith("/cover.jpg")
    assert item["profile_intro_text"] == "今天很好"
    assert item["current_city"] == "台北市"
    assert item["followers"] == "1.2萬"
    assert item["works"] == [{"title": "Example Inc."}]
    assert item["photos"] == [{"url": "https://scontent.example.fbcdn.net/photo.jpg"}]
    assert item["profile_data_source"] == "Facebook 直接瀏覽器"


def test_normalize_browser_profile_separates_requested_and_observed_identity():
    item = normalize_browser_profile(
        {
            "main_heading": "Wrong page",
            "og_url": "",
            "page_url": "https://www.facebook.com/999",
            "text": "這份個人檔案已鎖定",
            "private": True,
            "images": [],
        },
        "https://www.facebook.com/100",
    )

    assert item["id"] == "100"
    assert item["url"] == "https://www.facebook.com/100"
    assert item["observed_profile_identity"] == "999"
    assert item["observed_profile_url"] == "https://www.facebook.com/999"


def test_normalize_browser_profile_excludes_low_quality_duplicate_of_cover():
    item = normalize_browser_profile(
        {
            "main_heading": "吳佳欣",
            "images": [
                {
                    "src": "https://scontent.example.fbcdn.net/v/photo.jpg?stp=cover-high",
                    "alt": "吳佳欣的封面相片",
                    "natural_width": 1200,
                    "natural_height": 500,
                },
                {
                    "src": "https://scontent.example.fbcdn.net/v/photo.jpg?stp=blurred-preview",
                    "alt": "",
                    "natural_width": 400,
                    "natural_height": 300,
                },
                {
                    "src": "https://scontent.example.fbcdn.net/v/public-photo.jpg",
                    "alt": "",
                    "natural_width": 800,
                    "natural_height": 800,
                },
            ],
        },
        "https://www.facebook.com/100",
    )

    assert item["photos"] == [
        {"url": "https://scontent.example.fbcdn.net/v/public-photo.jpg"}
    ]


def test_normalize_browser_profile_ignores_notification_overlay_heading():
    item = normalize_browser_profile(
        {
            "heading": "通知",
            "main_heading": "林小華",
            "headings": ["通知", "林小華"],
            "og_title": "林小華 | Facebook",
            "title": "通知 | Facebook",
            "text": "通知\n林小華\n2,115 位追蹤者\n來自\n台南市",
            "images": [],
        },
        "https://www.facebook.com/100000063131907",
    )
    assert item["name"] == "林小華"


def test_normalize_browser_profile_ignores_unread_count_facebook_title():
    item = normalize_browser_profile(
        {
            "heading": "",
            "main_heading": "",
            "headings": [],
            "role_headings": ["謝球球"],
            "og_title": "(4) Facebook",
            "title": "(4) Facebook",
            "text": "首頁\n通知\n謝球球\n446 位朋友\n貼文\n關於",
            "images": [],
        },
        "https://www.facebook.com/100000288843407",
    )
    assert item["name"] == "謝球球"

    item_without_role_heading = normalize_browser_profile(
        {
            "role_headings": [],
            "og_title": "(4) Facebook",
            "title": "(4) Facebook",
            "text": "首頁\n通知\n謝球球\n446 位朋友\n貼文\n關於",
            "images": [],
        },
        "https://www.facebook.com/100000288843407",
    )
    assert item_without_role_heading["name"] == "謝球球"


def test_normalize_browser_profile_prefers_avatar_name_over_post_author_heading():
    item = normalize_browser_profile(
        {
            # Facebook can expose the author of a visible timeline post as the
            # first heading inside role=main.  The profile summary and avatar
            # still identify the actual owner of the page.
            "main_heading": "慈濟@新竹",
            "role_headings": ["慈濟@新竹", "Ya Ling Shen"],
            "headings": ["慈濟@新竹"],
            "og_title": "(4) Facebook",
            "title": "(4) Facebook",
            "text": "慈濟@新竹\nYa Ling Shen\n34 位朋友\nThis Is Me\n貼文",
            "images": [
                {
                    "src": "https://scontent.example.fbcdn.net/avatar.jpg",
                    "alt": "Ya Ling Shen 的大頭貼照片",
                    "natural_width": 720,
                    "natural_height": 720,
                }
            ],
        },
        "https://www.facebook.com/100000950467959",
    )

    assert item["name"] == "Ya Ling Shen"


def test_normalize_browser_profile_ignores_small_post_author_avatar_name():
    item = normalize_browser_profile(
        {
            "main_heading": "",
            "heading": "通知",
            "headings": ["通知"],
            "role_headings": [],
            "og_title": "",
            "title": "(6) Facebook",
            "text": "通知\nYa Ling Shen\n34 位朋友\nThis Is Me\n貼文\n慈濟＠新竹",
            "images": [
                {
                    "src": "https://scontent.example.fbcdn.net/profile.jpg",
                    "alt": "Ya Ling Shen",
                    "natural_width": 720,
                    "natural_height": 720,
                    "rendered_width": 168,
                    "rendered_height": 168,
                },
                {
                    "src": "https://scontent.example.fbcdn.net/post-author.jpg",
                    "alt": "慈濟＠新竹的大頭貼照",
                    "natural_width": 40,
                    "natural_height": 40,
                    "rendered_width": 40,
                    "rendered_height": 40,
                },
            ],
        },
        "https://www.facebook.com/100000950467959",
    )

    assert item["name"] == "Ya Ling Shen"


def test_normalize_browser_profile_uses_name_before_combined_follow_summary():
    item = normalize_browser_profile(
        {
            "main_heading": "",
            "headings": [],
            "role_headings": [],
            "og_title": "(4) Facebook",
            "title": "(4) Facebook",
            "text": "吳佳蓉\n503 位追蹤者 · 正在追蹤 183 人\n數位創作者\n全部\n關於\n朋友\n相片",
            "images": [],
        },
        "https://www.facebook.com/100000063131907",
    )
    assert item["name"] == "吳佳蓉"
    assert item["followers"] == "503"


def test_normalize_browser_profile_combines_primary_name_alias_and_svg_avatar():
    item = normalize_browser_profile(
        {
            "main_heading": "",
            "headings": ["（林小黑）"],
            "role_headings": ["（林小黑）"],
            "og_title": "(4) Facebook",
            "title": "(4) Facebook",
            "text": "林純玉\n（林小黑）\n164 位朋友\n全部\n關於\n朋友\n相片",
            "images": [
                {
                    "src": "https://scontent.example.fbcdn.net/v/avatar.jpg",
                    "alt": "",
                    "natural_width": 0,
                    "natural_height": 0,
                    "rendered_width": 300,
                    "rendered_height": 300,
                    "x": 100,
                    "y": 350,
                }
            ],
        },
        "https://www.facebook.com/100000117208012",
    )

    assert item["name"] == "林純玉（林小黑）"
    assert item["profile_picture"] == "https://scontent.example.fbcdn.net/v/avatar.jpg"


def test_normalize_browser_canary_posts_limits_posts_but_keeps_all_photos():
    raw_posts = [
        {
            "url": f"https://www.facebook.com/example/posts/p{i}?fbclid=tracking",
            "text": f"post {i}",
            "images": [
                {
                    "src": f"https://scontent.example.fbcdn.net/v/post-{i}-{photo}.jpg?token=x",
                    "natural_width": 800,
                    "natural_height": 800,
                }
                for photo in range(10)
            ],
        }
        for i in range(3)
    ]
    raw_posts.append({"url": "https://www.facebook.com/example", "text": "not a post"})

    posts = normalize_browser_canary_posts(raw_posts, max_posts=2)

    assert [post["source_post_id"] for post in posts] == ["p0", "p1"]
    assert all(post["ingest_source"] == "facebook_browser_canary" for post in posts)
    assert all(len(post["images"]) == 10 for post in posts)
    assert all("fbclid" not in post["source_url"] for post in posts)


def test_normalize_browser_canary_posts_deduplicates_alias_urls_for_same_post():
    posts = normalize_browser_canary_posts(
        [
            {"url": "https://www.facebook.com/permalink.php?story_fbid=pfbid123&id=100", "text": "same"},
            {"url": "https://www.facebook.com/100/posts/pfbid123", "text": "same"},
        ],
        max_posts=2,
    )

    assert len(posts) == 1


def test_article_permalink_prefers_timestamp_over_first_photo_attachment():
    selected = select_facebook_permalink(
        [
            {
                "url": "https://m.facebook.com/photo/?fbid=attachment-1&set=pcb.9",
                "has_image": True,
            },
            {
                "url": "https://mbasic.facebook.com/example/posts/post-9?ref=bookmarks",
                "text": "2 小時",
                "is_timestamp": True,
            },
            {
                "url": "https://www.facebook.com/photo.php?fbid=attachment-2",
                "has_image": True,
            },
        ]
    )

    assert selected == "https://www.facebook.com/example/posts/post-9"


def test_browser_post_page_continues_after_saved_cursor():
    raw_posts = [
        {"url": f"https://www.facebook.com/example/posts/p{i}", "text": f"post {i}"}
        for i in range(5)
    ]

    page = normalize_browser_canary_posts(raw_posts, max_posts=2, after_cursor="p1")

    assert [post["source_post_id"] for post in page] == ["p2", "p3"]


@pytest.mark.asyncio
async def test_album_walker_keeps_advancing_until_photo_repeats(tmp_path: Path, monkeypatch):
    class Response:
        status = 200

    class Links:
        async def evaluate_all(self, expression: str):
            return ["https://www.facebook.com/photo/?fbid=1"]

    class FakePage:
        def __init__(self):
            self.visited = []
            self.url = ""

        async def goto(self, url: str, **kwargs):
            self.visited.append(url)
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

        def locator(self, selector: str):
            return Links()

    gateway = FacebookBrowserGateway(True, tmp_path)
    page = FakePage()
    viewer = [
        "https://scontent.example.fbcdn.net/v/photo-b.jpg?token=1",
        "https://scontent.example.fbcdn.net/v/photo-c.jpg?token=2",
        "https://scontent.example.fbcdn.net/v/photo-b.jpg?token=3",
    ]
    state = {"index": 0}

    async def visible_images(current_page, selector):
        return ["https://scontent.example.fbcdn.net/v/photo-a.jpg"]

    async def current_viewer_image(current_page):
        return viewer[state["index"]]

    async def click_next(current_page):
        state["index"] = min(state["index"] + 1, len(viewer) - 1)
        media_ids = ["1", "2", "1"]
        current_page.url = f"https://www.facebook.com/photo.php?fbid={media_ids[state['index']]}"
        return True

    monkeypatch.setattr(gateway, "_large_facebook_images", visible_images)
    monkeypatch.setattr(gateway, "_largest_viewer_image", current_viewer_image)
    monkeypatch.setattr(gateway, "_click_next_photo", click_next)

    photos, progress = await gateway._collect_post_album_photos(page, "https://www.facebook.com/example/posts/p1")

    assert page.visited == [
        "https://www.facebook.com/example/posts/p1",
        "https://www.facebook.com/photo.php?fbid=1",
    ]
    assert [normalize_url(url) for url in photos] == [
        "facebook-cdn:/v/photo-a.jpg",
        "facebook-cdn:/v/photo-b.jpg",
        "facebook-cdn:/v/photo-c.jpg",
    ]
    assert progress["completed"] is True
    assert progress["resume_url"] == ""


@pytest.mark.asyncio
async def test_browser_waits_randomly_between_canary_posts(tmp_path: Path, monkeypatch):
    class FakePage:
        def __init__(self):
            self.waits = []

        async def wait_for_timeout(self, milliseconds: int):
            self.waits.append(milliseconds)

    page = FakePage()
    gateway = FacebookBrowserGateway(True, tmp_path)
    monkeypatch.setattr("fb_monitor.facebook_browser.random.uniform", lambda minimum, maximum: 12_345.4)

    await gateway._wait_between_canary_posts(page)

    assert page.waits == [12_345]


@pytest.mark.asyncio
async def test_album_walker_saves_resume_state_when_batch_limit_is_reached(tmp_path: Path, monkeypatch):
    class Response:
        status = 200

    class Links:
        async def evaluate_all(self, expression: str):
            return ["https://www.facebook.com/photo/?fbid=1"]

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

        def locator(self, selector: str):
            return Links()

    gateway = FacebookBrowserGateway(True, tmp_path)
    gateway.album_batch_max_new_photos = 1

    async def no_grid_images(current_page, selector):
        return []

    async def current_viewer_image(current_page):
        return "https://scontent.example.fbcdn.net/v/photo-new.jpg?token=1"

    async def unexpected_click(current_page):
        raise AssertionError("batch should stop before clicking next")

    monkeypatch.setattr(gateway, "_large_facebook_images", no_grid_images)
    monkeypatch.setattr(gateway, "_largest_viewer_image", current_viewer_image)
    monkeypatch.setattr(gateway, "_click_next_photo", unexpected_click)

    photos, progress = await gateway._collect_post_album_photos(FakePage(), "https://www.facebook.com/example/posts/p1")

    assert len(photos) == 1
    assert progress["completed"] is False
    assert progress["resume_url"] == "https://www.facebook.com/photo.php?fbid=1"
    gateway._save_album_progress({"post": progress})
    assert gateway._load_album_progress()["post"]["resume_url"] == progress["resume_url"]


@pytest.mark.asyncio
async def test_album_walker_resumes_65_photos_in_20_20_20_5_batches(tmp_path: Path, monkeypatch):
    total = 65

    class Response:
        status = 200

    class Locator:
        def __init__(self, page, selector: str):
            self.page = page
            self.selector = selector

        async def evaluate_all(self, expression: str):
            if "article" in self.selector and "href*='/photo'" in self.selector:
                return ["https://m.facebook.com/photo/?fbid=1&set=album"]
            if "aria-label" in self.selector or "role='heading'" in self.selector:
                return [f"{self.page.index + 1} / {total}"]
            if "canonical" in self.selector or "fbid=" in self.selector:
                return [self.page.url]
            return []

        async def inner_text(self, timeout: int):
            return ""

        async def count(self):
            return 0

    class FakePage:
        def __init__(self):
            self.url = ""
            self.index = 0

        async def goto(self, url: str, **kwargs):
            self.url = url
            fbid = (parse_qs(urlsplit(url).query).get("fbid") or [""])[0]
            if fbid.isdigit():
                self.index = int(fbid) - 1
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

        def locator(self, selector: str):
            return Locator(self, selector)

    gateway = FacebookBrowserGateway(True, tmp_path)
    page = FakePage()

    async def no_grid_images(current_page, selector):
        return []

    async def current_viewer_image(current_page):
        return f"https://scontent.example.fbcdn.net/v/photo-{current_page.index + 1}.jpg?token=rotating"

    async def click_next(current_page):
        if current_page.index + 1 >= total:
            return False
        current_page.index += 1
        current_page.url = f"https://www.facebook.com/photo.php?fbid={current_page.index + 1}"
        return True

    monkeypatch.setattr(gateway, "_large_facebook_images", no_grid_images)
    monkeypatch.setattr(gateway, "_largest_viewer_image", current_viewer_image)
    monkeypatch.setattr(gateway, "_click_next_photo", click_next)

    state = None
    batch_sizes = []
    cumulative_sizes = []
    for _ in range(4):
        photos, state = await gateway._collect_post_album_photos(
            page,
            "https://www.facebook.com/example/posts/p1",
            state,
        )
        batch_sizes.append(state["batch_new_photos"])
        cumulative_sizes.append(len(photos))
        # The checkpoint is deliberately plain JSON so a later timeline cursor
        # cannot orphan already discovered album media.
        state = json.loads(json.dumps(state))

    assert batch_sizes == [20, 20, 20, 5]
    assert cumulative_sizes == [20, 40, 60, 65]
    assert state["completed"] is True
    assert state["terminal_reason"] == "declared_last_position"
    assert state["resume_url"] == ""
    assert len(state["seen_media_ids"]) == 65
    assert len(state["collected_photos"]) == 65


@pytest.mark.asyncio
async def test_album_walker_does_not_complete_on_single_photo_stalled_cycle(tmp_path: Path, monkeypatch):
    class Response:
        status = 200

    class Locator:
        def __init__(self, page, selector: str):
            self.page = page
            self.selector = selector

        async def evaluate_all(self, expression: str):
            if "article" in self.selector and "href*='/photo'" in self.selector:
                return ["https://www.facebook.com/photo.php?fbid=1"]
            if "canonical" in self.selector or "fbid=" in self.selector:
                return [self.page.url]
            return []

        async def inner_text(self, timeout: int):
            return ""

        async def count(self):
            return 0

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

        def locator(self, selector: str):
            return Locator(self, selector)

    gateway = FacebookBrowserGateway(True, tmp_path)

    async def no_grid_images(current_page, selector):
        return []

    async def current_viewer_image(current_page):
        return "https://scontent.example.fbcdn.net/v/only-photo.jpg?token=1"

    async def click_without_advancing(current_page):
        return True

    monkeypatch.setattr(gateway, "_large_facebook_images", no_grid_images)
    monkeypatch.setattr(gateway, "_largest_viewer_image", current_viewer_image)
    monkeypatch.setattr(gateway, "_click_next_photo", click_without_advancing)

    _, progress = await gateway._collect_post_album_photos(
        FakePage(),
        "https://www.facebook.com/example/posts/p1",
    )

    assert progress["completed"] is False
    assert progress["stalled_reason"] == "viewer_did_not_advance"
    assert progress["resume_url"] == "https://www.facebook.com/photo.php?fbid=1"


@pytest.mark.asyncio
async def test_album_access_wall_raises_typed_challenge_error(tmp_path: Path):
    class Response:
        status = 200

    class Locator:
        def __init__(self, selector: str):
            self.selector = selector

        async def inner_text(self, timeout: int):
            return "Security check — confirm your identity"

        async def count(self):
            return 0

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = "https://www.facebook.com/checkpoint/123"
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

        def locator(self, selector: str):
            return Locator(selector)

    gateway = FacebookBrowserGateway(True, tmp_path)

    with pytest.raises(FacebookBrowserChallengeRequired):
        await gateway._collect_post_album_photos(
            FakePage(),
            "https://www.facebook.com/example/posts/p1",
        )


@pytest.mark.asyncio
async def test_access_wall_does_not_scan_article_body_text(tmp_path: Path):
    class Locator:
        async def inner_text(self, timeout: int):
            return "貼文正文提到安全檢查與 confirm your identity"

        async def count(self):
            return 0

    class Page:
        url = "https://www.facebook.com/100"

        async def evaluate(self, expression: str):
            # The scoped UI query excludes every article, so none of the post
            # wording is returned as verification UI.
            return ""

        def locator(self, selector: str):
            return Locator()

    gateway = FacebookBrowserGateway(True, tmp_path)
    await gateway._raise_for_access_wall(Page())


@pytest.mark.asyncio
async def test_access_wall_accepts_scoped_dialog_or_heading_marker(tmp_path: Path):
    class Locator:
        async def count(self):
            return 0

    class Page:
        url = "https://www.facebook.com/100"

        async def evaluate(self, expression: str):
            return "安全檢查"

        def locator(self, selector: str):
            return Locator()

    gateway = FacebookBrowserGateway(True, tmp_path)
    with pytest.raises(FacebookBrowserChallengeRequired):
        await gateway._raise_for_access_wall(Page())


def test_post_text_lock_wording_does_not_mark_profile_private():
    public_item = normalize_browser_profile(
        {
            "main_heading": "Public account",
            "og_url": "https://www.facebook.com/100",
            "text": "貼文正文：這份個人檔案已鎖定",
            "profile_text": "Public account\n1 位追蹤者",
            "private": False,
            "images": [],
        },
        "https://www.facebook.com/100",
    )
    private_item = normalize_browser_profile(
        {
            "main_heading": "Locked account",
            "og_url": "https://www.facebook.com/100",
            "text": "Locked account",
            "profile_text": "這份個人檔案已鎖定",
            "private": True,
            "images": [],
        },
        "https://www.facebook.com/100",
    )

    assert public_item["private"] is False
    assert private_item["private"] is True


@pytest.mark.asyncio
async def test_browser_capture_is_saved_per_profile(tmp_path: Path):
    class FakePage:
        def __init__(self):
            self.viewport_size = {"width": 1365, "height": 900}
            self.loaded_content_height = 900
            self.captured_content_height = 0

        async def set_viewport_size(self, size: dict):
            self.viewport_size = dict(size)
            self.loaded_content_height = int(size["height"])

        async def wait_for_timeout(self, milliseconds: int):
            pass

        async def evaluate(self, expression: str):
            return 5000

        async def screenshot(self, *, path: str, full_page: bool):
            self.captured_content_height = self.loaded_content_height
            Path(path).write_bytes(b"png")

    gateway = FacebookBrowserGateway(True, tmp_path)
    page = FakePage()
    saved = await gateway._save_capture(page, "profile/1")

    assert saved == tmp_path / "screenshots" / "profile-profile_1.png"
    assert saved.read_bytes() == b"png"
    assert page.captured_content_height == 2700
    assert page.viewport_size == {"width": 1365, "height": 900}


@pytest.mark.asyncio
async def test_browser_waits_for_profile_heading_and_loaded_media(tmp_path: Path):
    class FakePage:
        def __init__(self):
            self.base_wait = 0
            self.condition_timeout = 0
            self.condition = ""

        async def wait_for_timeout(self, milliseconds: int):
            self.base_wait = milliseconds

        async def wait_for_function(self, condition: str, *, timeout: int):
            self.condition = condition
            self.condition_timeout = timeout

    page = FakePage()
    gateway = FacebookBrowserGateway(True, tmp_path)

    await gateway._wait_for_profile_content(page)

    assert page.base_wait == 3000
    assert page.condition_timeout == 5000
    assert "naturalWidth >= 180" in page.condition
