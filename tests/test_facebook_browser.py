import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from fb_monitor.facebook_browser import (
    FacebookBrowserChallengeRequired,
    FacebookBrowserError,
    FacebookBrowserGateway,
    FacebookBrowserLoginRequired,
    normalize_browser_canary_posts,
    normalize_browser_profile,
    public_content_proof,
    public_content_proof_matches_profile,
    select_facebook_permalink,
)
from fb_monitor.normalize import normalize_url


def test_browser_gateway_batch_limit_constructor_defaults_and_overrides(tmp_path: Path):
    default_gateway = FacebookBrowserGateway(True, tmp_path)
    configured_gateway = FacebookBrowserGateway(
        True,
        tmp_path,
        album_batch_max_operations=7,
        album_batch_max_seconds=75,
    )

    assert default_gateway.album_batch_max_operations == 20
    assert default_gateway.album_batch_max_new_photos == 20
    assert default_gateway.profile_photo_grid_batch_max_scrolls == 20
    assert default_gateway.album_batch_max_seconds == 180
    assert configured_gateway.album_batch_max_operations == 7
    assert configured_gateway.album_batch_max_new_photos == 7
    assert configured_gateway.profile_photo_grid_batch_max_scrolls == 7
    assert configured_gateway.album_batch_max_seconds == 75


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
async def test_viewer_media_id_uses_photo_not_album_set_id(tmp_path: Path):
    class EmptyLinks:
        async def evaluate_all(self, expression: str):
            return []

    class FakePage:
        url = "https://www.facebook.com/example/photos/a.111111/987654321/"

        def locator(self, selector: str):
            return EmptyLinks()

    gateway = FacebookBrowserGateway(True, tmp_path)

    assert await gateway._current_viewer_media_id(FakePage()) == "987654321"


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
    assert len(state["collected_items"]) == 65
    assert state["collected_items"][0] == {
        "id": "1",
        "url": "https://www.facebook.com/photo.php?fbid=1",
        "image": "https://scontent.example.fbcdn.net/v/photo-1.jpg?token=rotating",
    }
    assert state["collected_items"][-1]["id"] == "65"


@pytest.mark.asyncio
async def test_public_profile_photos_uses_dedicated_surface_and_returns_checkpoint(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()
    context = browser.Context()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    observed = {}

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        observed.update({
            "source_url": source_url,
            "profile_identity": profile_identity,
            "progress": progress,
            **kwargs,
        })
        checkpoint = {
            "collected_items": [],
            "discovered_urls": [],
            "processed_urls": [],
            "grid_complete": False,
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
            "resume_url": source_url,
        }
        return gateway._public_photo_result(checkpoint, [])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)
    initial_progress = {"resume_url": "https://www.facebook.com/photo.php?fbid=122"}

    result = await gateway.public_profile_photos(
        "https://www.facebook.com/profile.php?id=100123",
        initial_progress,
        "profile-7-photos",
    )

    assert observed["source_url"] == "https://www.facebook.com/100123/photos_by"
    assert observed["profile_identity"] == "100123"
    assert observed["progress"] is initial_progress
    assert observed["diagnostic_key"] == "profile-7-photos"
    assert result == {
        "items": [],
        "batch_items": [],
        "progress": {
            "collected_items": [],
            "discovered_urls": [],
            "processed_urls": [],
            "grid_complete": False,
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
            "resume_url": "https://www.facebook.com/100123/photos_by",
            "discovered_count": 0,
            "processed_count": 0,
            "pending_count": 0,
            "permalink_failure_count": 0,
            "permalink_exhausted_count": 0,
        },
        "discovered_count": 0,
        "processed_count": 0,
        "pending_count": 0,
        "permalink_failure_count": 0,
        "permalink_exhausted_count": 0,
        "grid_complete": False,
        "resumable": True,
        "completed": False,
        "terminal_reason": "",
        "stalled_reason": "",
    }
    assert context.cookie_reads == 1
    assert context.closed is True


@pytest.mark.asyncio
async def test_public_photo_grid_lazy_loads_multiple_albums_then_fetches_20_20_5(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        def __init__(self):
            self.url = ""
            self.grid_scrolls = 0
            self.visited: list[str] = []

        async def goto(self, url: str, **kwargs):
            self.url = url
            self.visited.append(url)
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    gateway.profile_photo_grid_stable_rounds = 2
    page = FakePage()
    all_links = [
        (
            f"https://www.facebook.com/100/photos/a.{1 if index <= 23 else 2}/{index}/"
            if index % 2
            else f"https://www.facebook.com/photo.php?fbid={index}&set=a.album"
        )
        for index in range(1, 46)
    ]

    async def grid_links(current_page, profile_identity):
        assert profile_identity == "100"
        counts = [8, 20, 35, 45]
        count = counts[min(current_page.grid_scrolls, len(counts) - 1)]
        # The target grid can contain an unrelated navigation thumbnail; an
        # encoded foreign owner must never enter the inventory.
        return all_links[:count] + [
            "https://www.facebook.com/999/photos/a.foreign/999001/"
        ]

    async def grid_metrics(current_page):
        if current_page.grid_scrolls < 3:
            height = 2000 + current_page.grid_scrolls * 1000
            return {"scroll_height": height, "viewport_height": 500, "scroll_y": height - 700}
        return {"scroll_height": 5000, "viewport_height": 500, "scroll_y": 4500}

    async def scroll_grid(current_page):
        current_page.grid_scrolls += 1

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    async def viewer_image(current_page):
        media_id = (
            parse_qs(urlsplit(current_page.url).query).get("fbid") or
            [current_page.url.rstrip("/").split("/")[-1]]
        )[0]
        return f"https://scontent.example.fbcdn.net/v/photo-{media_id}.jpg?token=rotating"

    async def viewer_media_id(current_page):
        return (
            parse_qs(urlsplit(current_page.url).query).get("fbid") or
            [current_page.url.rstrip("/").split("/")[-1]]
        )[0]

    monkeypatch.setattr(gateway, "_public_photo_grid_links", grid_links)
    monkeypatch.setattr(gateway, "_public_photo_grid_metrics", grid_metrics)
    monkeypatch.setattr(gateway, "_scroll_public_photo_grid", scroll_grid)
    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)
    monkeypatch.setattr(gateway, "_public_photo_original_image", viewer_image)
    monkeypatch.setattr(gateway, "_current_viewer_media_id", viewer_media_id)

    state = None
    results = []
    for _ in range(3):
        result = await gateway._collect_public_profile_photo_inventory(
            page,
            "https://www.facebook.com/100/photos_by",
            "100",
            state,
        )
        results.append(result)
        state = json.loads(json.dumps(result["progress"]))

    assert [len(result["batch_items"]) for result in results] == [20, 20, 5]
    assert [len(result["items"]) for result in results] == [20, 40, 45]
    assert [result["processed_count"] for result in results] == [20, 40, 45]
    assert [result["pending_count"] for result in results] == [25, 5, 0]
    assert results[0]["grid_complete"] is True
    assert results[0]["completed"] is False
    assert results[0]["stalled_reason"] == ""
    assert results[-1]["completed"] is True
    assert results[-1]["terminal_reason"] == "grid_inventory_processed"
    assert results[-1]["progress"]["declared_total"] == 45
    assert results[-1]["progress"]["discovered_count"] == 45
    assert results[-1]["progress"]["processed_count"] == 45
    assert results[-1]["progress"]["pending_count"] == 0
    assert page.visited.count("https://www.facebook.com/100/photos_by") == 1
    assert "https://www.facebook.com/999/photos/a.foreign/999001" not in state["discovered_urls"]


@pytest.mark.asyncio
async def test_public_photo_refresh_reopens_completed_media_in_bounded_batches_without_grid_scan(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        def __init__(self):
            self.url = ""
            self.visited: list[str] = []

        async def goto(self, url: str, **kwargs):
            self.url = url
            self.visited.append(url)
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    urls = [
        f"https://www.facebook.com/100/photos/a.album/{index}/"
        for index in range(1, 22)
    ]

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    async def current_media_id(current_page):
        return current_page.url.rstrip("/").split("/")[-1]

    async def refreshed_image(current_page):
        media_id = current_page.url.rstrip("/").split("/")[-1]
        return f"https://scontent.example.fbcdn.net/v/photo-{media_id}.jpg?fresh=1"

    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)
    monkeypatch.setattr(gateway, "_current_viewer_media_id", current_media_id)
    monkeypatch.setattr(gateway, "_public_photo_original_image", refreshed_image)

    state = {
        "schema_version": 4,
        "grid_complete": True,
        "completed": True,
        "discovered_urls": urls,
        "processed_urls": urls,
        "collected_items": [
            {
                "id": str(index),
                "url": urls[index - 1],
                "image": f"https://scontent.example.fbcdn.net/v/photo-{index}.jpg?expired=1",
            }
            for index in range(1, 22)
        ],
        "refresh_media_external_ids": [str(index) for index in range(1, 22)],
    }
    first_page = FakePage()
    first = await gateway._collect_public_profile_photo_inventory(
        first_page,
        "https://www.facebook.com/100/photos_by",
        "100",
        state,
    )

    assert len(first_page.visited) == 20
    assert all("/photos_by" not in url for url in first_page.visited)
    assert [item["id"] for item in first["batch_items"]] == [
        str(index) for index in range(1, 21)
    ]
    assert first["items"][0]["image"].endswith("?fresh=1")
    assert first["items"][-1]["image"].endswith("?expired=1")
    assert first["progress"]["refresh_media_external_ids"] == ["21"]
    assert first["processed_count"] == 20
    assert first["pending_count"] == 1
    assert first["completed"] is False

    second_page = FakePage()
    second = await gateway._collect_public_profile_photo_inventory(
        second_page,
        "https://www.facebook.com/100/photos_by",
        "100",
        json.loads(json.dumps(first["progress"])),
    )

    assert second_page.visited == [urls[-1].rstrip("/")]
    assert second["batch_items"][0]["id"] == "21"
    assert second["items"][-1]["image"].endswith("?fresh=1")
    assert second["progress"]["refresh_media_external_ids"] == []
    assert second["completed"] is True
    assert second["terminal_reason"] == "grid_inventory_processed"


@pytest.mark.asyncio
async def test_public_photo_refresh_keeps_old_item_and_failure_budget_until_verified(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    url = "https://www.facebook.com/photo.php?fbid=77"

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    async def current_media_id(current_page):
        return "77"

    async def missing_original(current_page):
        return ""

    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)
    monkeypatch.setattr(gateway, "_current_viewer_media_id", current_media_id)
    monkeypatch.setattr(gateway, "_public_photo_original_image", missing_original)

    result = await gateway._collect_public_profile_photo_inventory(
        FakePage(),
        "https://www.facebook.com/100/photos_by",
        "100",
        {
            "schema_version": 4,
            "grid_complete": True,
            "completed": True,
            "discovered_urls": [url],
            "processed_urls": [url],
            "collected_items": [
                {"id": "77", "url": url, "image": "https://old.fbcdn.net/77.jpg"}
            ],
            "refresh_media_external_ids": ["77"],
        },
    )

    assert result["batch_items"] == []
    assert result["items"][0]["image"] == "https://old.fbcdn.net/77.jpg"
    assert result["progress"]["refresh_media_external_ids"] == ["77"]
    assert result["processed_count"] == 0
    assert result["completed"] is False
    failure = result["progress"]["permalink_failures"]["media:77"]
    assert failure["attempts"] == 1
    assert failure["last_reason"] == "public_photo_original_missing"


@pytest.mark.asyncio
async def test_public_photo_grid_limit_is_resumable_without_stalled_reason(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        url = ""
        grid_scrolls = 0
        viewer_visits = 0

        async def goto(self, url: str, **kwargs):
            self.url = url
            if "/photos_by" not in url:
                self.viewer_visits += 1
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    gateway.profile_photo_grid_batch_max_scrolls = 2
    page = FakePage()

    async def grid_links(current_page, profile_identity):
        return [
            f"https://www.facebook.com/{profile_identity}/photos/a.1/{index}/"
            for index in range(1, current_page.grid_scrolls + 2)
        ]

    async def not_at_bottom(current_page):
        return {"scroll_height": 9000, "viewport_height": 500, "scroll_y": current_page.grid_scrolls * 500}

    async def scroll_grid(current_page):
        current_page.grid_scrolls += 1

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    monkeypatch.setattr(gateway, "_public_photo_grid_links", grid_links)
    monkeypatch.setattr(gateway, "_public_photo_grid_metrics", not_at_bottom)
    monkeypatch.setattr(gateway, "_scroll_public_photo_grid", scroll_grid)
    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)

    result = await gateway._collect_public_profile_photo_inventory(
        page,
        "https://www.facebook.com/100/photos_by",
        "100",
    )

    assert result["completed"] is False
    assert result["grid_complete"] is False
    assert result["stalled_reason"] == ""
    assert result["progress"]["phase"] == "discover_grid"
    assert result["progress"]["resume_url"] == "https://www.facebook.com/100/photos_by"
    assert result["progress"]["grid_scroll_depth"] == 2
    assert result["progress"]["batch_grid_scrolls"] == 2
    assert result["discovered_count"] == 3
    assert result["processed_count"] == 0
    assert page.viewer_visits == 0


@pytest.mark.asyncio
async def test_photo_surface_redirect_to_profile_root_is_not_terminal_evidence(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = "https://www.facebook.com/100"
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)

    async def no_access_wall(*args, **kwargs):
        return None

    monkeypatch.setattr(
        gateway,
        "_raise_for_access_wall",
        no_access_wall,
    )

    with pytest.raises(
        FacebookBrowserError,
        match="facebook_photo_surface_redirected_or_unverified",
    ):
        await gateway._collect_public_profile_photo_inventory(
            FakePage(),
            "https://www.facebook.com/100/photos_of",
            "100",
            enforce_profile_owner=False,
        )


@pytest.mark.asyncio
async def test_public_photo_grid_replays_checkpoint_then_adds_only_bounded_new_scrolls(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        def __init__(self):
            self.url = ""
            self.grid_scrolls = 0
            self.scroll_calls = 0

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    gateway.profile_photo_grid_batch_max_scrolls = 2

    async def grid_links(current_page, profile_identity):
        return [
            f"https://www.facebook.com/{profile_identity}/photos/a.1/{index}/"
            for index in range(1, current_page.grid_scrolls + 2)
        ]

    async def grid_metrics(current_page):
        return {
            "scroll_height": 10_000,
            "viewport_height": 500,
            "scroll_y": current_page.grid_scrolls * 500,
        }

    async def scroll_grid(current_page):
        current_page.grid_scrolls += 1
        current_page.scroll_calls += 1

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    monkeypatch.setattr(gateway, "_public_photo_grid_links", grid_links)
    monkeypatch.setattr(gateway, "_public_photo_grid_metrics", grid_metrics)
    monkeypatch.setattr(gateway, "_scroll_public_photo_grid", scroll_grid)
    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)

    first_page = FakePage()
    first = await gateway._collect_public_profile_photo_inventory(
        first_page, "https://www.facebook.com/100/photos_by", "100"
    )
    second_page = FakePage()  # a continuation always opens a fresh browser page
    second = await gateway._collect_public_profile_photo_inventory(
        second_page,
        "https://www.facebook.com/100/photos_by",
        "100",
        json.loads(json.dumps(first["progress"])),
    )

    assert first["progress"]["grid_scroll_depth"] == 2
    assert first["progress"]["batch_grid_replay_scrolls"] == 0
    assert first["progress"]["batch_grid_new_scrolls"] == 2
    assert second["progress"]["grid_scroll_depth"] == 4
    assert second["progress"]["batch_grid_replay_scrolls"] == 2
    assert second["progress"]["batch_grid_new_scrolls"] == 2
    assert second["progress"]["total_grid_new_scrolls"] == 4
    assert second["progress"]["total_grid_replay_scrolls"] == 2
    assert second_page.scroll_calls == 4
    assert second["discovered_count"] == 5
    assert second["stalled_reason"] == ""


@pytest.mark.asyncio
async def test_public_photo_grid_accepts_checkpointed_numeric_owner_alias(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    gateway.profile_photo_grid_batch_max_scrolls = 0

    async def numeric_owner_link(current_page, profile_identity):
        assert profile_identity == "vanity.name"
        return ["https://www.facebook.com/100123/photos/a.album/777/"]

    async def not_at_bottom(current_page):
        return {"scroll_height": 5000, "viewport_height": 500, "scroll_y": 0}

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    monkeypatch.setattr(gateway, "_public_photo_grid_links", numeric_owner_link)
    monkeypatch.setattr(gateway, "_public_photo_grid_metrics", not_at_bottom)
    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)

    result = await gateway._collect_public_profile_photo_inventory(
        FakePage(),
        "https://www.facebook.com/vanity.name/photos_by",
        "vanity.name",
        {"profile_owner_aliases": ["100123"]},
    )

    assert result["progress"]["profile_owner_aliases"] == ["vanity.name", "100123"]
    assert result["progress"]["discovered_urls"] == [
        "https://www.facebook.com/100123/photos/a.album/777"
    ]
    assert result["stalled_reason"] == ""


@pytest.mark.asyncio
async def test_public_photo_grid_links_excludes_album_only_urls_and_defers_owner_filter(
    tmp_path: Path,
):
    class Locator:
        async def evaluate_all(self, expression: str):
            return [
                "https://www.facebook.com/100123/photos/a.555/",
                "https://www.facebook.com/100123/photos/a.555/777/",
            ]

    class FakePage:
        def locator(self, selector: str):
            return Locator()

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    links = await gateway._public_photo_grid_links(FakePage(), "vanity.name")

    assert links == ["https://www.facebook.com/100123/photos/a.555/777"]


@pytest.mark.asyncio
async def test_public_photo_grid_http_429_raises_typed_challenge(tmp_path: Path):
    class Response:
        status = 429

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)

    with pytest.raises(FacebookBrowserChallengeRequired, match="429"):
        await gateway._collect_public_profile_photo_inventory(
            FakePage(), "https://www.facebook.com/100/photos_by", "100"
        )


@pytest.mark.asyncio
async def test_public_photo_permalink_failures_do_not_block_fresh_urls_and_exhaust_at_three(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        def __init__(self):
            self.url = ""
            self.visited: list[str] = []

        async def goto(self, url: str, **kwargs):
            self.url = url
            self.visited.append(url)
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    gateway.album_batch_max_operations = 2
    gateway.album_batch_max_new_photos = 2
    page = FakePage()
    urls = [f"https://www.facebook.com/photo.php?fbid={index}" for index in range(1, 4)]

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    async def current_media_id(current_page):
        expected = (parse_qs(urlsplit(current_page.url).query).get("fbid") or [""])[0]
        return "999" if expected == "1" else expected

    async def original_image(current_page):
        media_id = (parse_qs(urlsplit(current_page.url).query).get("fbid") or [""])[0]
        return f"https://scontent.example.fbcdn.net/v/photo-{media_id}.jpg"

    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)
    monkeypatch.setattr(gateway, "_current_viewer_media_id", current_media_id)
    monkeypatch.setattr(gateway, "_public_photo_original_image", original_image)

    state = {
        "schema_version": 4,
        "grid_complete": True,
        "discovered_urls": urls,
        "processed_urls": [],
        "collected_items": [],
    }
    results = []
    for _ in range(3):
        result = await gateway._collect_public_profile_photo_inventory(
            page,
            "https://www.facebook.com/100/photos_by",
            "100",
            state,
        )
        results.append(result)
        state = json.loads(json.dumps(result["progress"]))

    assert [result["progress"]["batch_operations"] for result in results] == [2, 2, 1]
    assert [result["processed_count"] for result in results] == [1, 2, 2]
    assert [item["id"] for item in results[-1]["items"]] == ["2", "3"]
    assert page.visited == [urls[0], urls[1], urls[2], urls[0], urls[0]]
    assert results[0]["stalled_reason"] == ""
    assert results[1]["stalled_reason"] == ""
    assert results[-1]["stalled_reason"] == "public_photo_permalink_failures_exhausted"
    assert results[-1]["resumable"] is False
    failure = results[-1]["progress"]["permalink_failures"]["media:1"]
    assert failure["attempts"] == 3
    assert "expected=1,current=999" in failure["last_reason"]


@pytest.mark.asyncio
async def test_public_photo_permalink_http_429_raises_challenge_without_retrying(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 429

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    state = {
        "schema_version": 4,
        "grid_complete": True,
        "discovered_urls": ["https://www.facebook.com/photo.php?fbid=1"],
        "processed_urls": [],
        "collected_items": [],
    }

    with pytest.raises(FacebookBrowserChallengeRequired, match="429"):
        await gateway._collect_public_profile_photo_inventory(
            FakePage(), "https://www.facebook.com/100/photos_by", "100", state
        )


@pytest.mark.asyncio
async def test_public_photo_original_prefers_open_graph_media(tmp_path: Path):
    class Locator:
        async def evaluate_all(self, expression: str):
            return ["https://scontent.example.fbcdn.net/v/current-photo.jpg?token=1"]

    class FakePage:
        def locator(self, selector: str):
            assert "og:image" in selector
            return Locator()

        async def evaluate(self, expression: str, selector: str):
            raise AssertionError("Open Graph media should win before DOM image fallback")

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    image = await gateway._public_photo_original_image(FakePage())

    assert image == "https://scontent.example.fbcdn.net/v/current-photo.jpg?token=1"


@pytest.mark.asyncio
async def test_public_photo_original_rejects_unrelated_generic_main_image(tmp_path: Path):
    class Locator:
        async def evaluate_all(self, expression: str):
            return []

    class FakePage:
        def __init__(self):
            self.selectors: list[str] = []

        def locator(self, selector: str):
            return Locator()

        async def evaluate(self, expression: str, selector: str):
            self.selectors.append(selector)
            if selector == "[role='main'] img, main img":
                return ["https://scontent.example.fbcdn.net/v/unrelated-large-ad.jpg"]
            return []

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    page = FakePage()
    image = await gateway._public_photo_original_image(page)

    assert image == ""
    assert len(page.selectors) == 1
    assert "data-visualcompletion='media-vc-image'" in page.selectors[0]
    assert page.selectors[0] != "[role='main'] img, main img"


@pytest.mark.asyncio
async def test_public_photo_grid_requires_explicit_empty_evidence(
    tmp_path: Path, monkeypatch
):
    class Response:
        status = 200

    class FakePage:
        url = ""

        async def goto(self, url: str, **kwargs):
            self.url = url
            return Response()

        async def wait_for_timeout(self, milliseconds: int):
            return None

    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)
    gateway.profile_photo_grid_stable_rounds = 1

    async def no_links(current_page, profile_identity):
        return []

    async def bottom(current_page):
        return {"scroll_height": 1000, "viewport_height": 500, "scroll_y": 500}

    async def scroll_grid(current_page):
        return None

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    async def not_explicitly_empty(current_page):
        return False

    monkeypatch.setattr(gateway, "_public_photo_grid_links", no_links)
    monkeypatch.setattr(gateway, "_public_photo_grid_metrics", bottom)
    monkeypatch.setattr(gateway, "_scroll_public_photo_grid", scroll_grid)
    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)
    monkeypatch.setattr(gateway, "_public_photo_grid_empty_state", not_explicitly_empty)

    result = await gateway._collect_public_profile_photo_inventory(
        FakePage(), "https://www.facebook.com/100/photos_by", "100"
    )

    assert result["completed"] is False
    assert result["stalled_reason"] == "public_photo_grid_empty_or_unavailable"
    assert result["progress"]["phase"] == "source_limited"


@pytest.mark.asyncio
async def test_public_profile_photos_rejects_authenticated_cookie_before_navigation(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class PollutedContext(browser.Context):
        async def cookies(self, url: str):
            self.cookie_reads += 1
            return [{"name": "c_user", "value": "123"}]

    context = PollutedContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)

    async def unexpected(*args, **kwargs):
        raise AssertionError("cookie pollution must stop before collector navigation")

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", unexpected)

    with pytest.raises(FacebookBrowserLoginRequired, match="cookie"):
        await gateway.public_profile_photos("https://www.facebook.com/100")

    assert context.cookie_reads == 1
    assert context.page.visited == []
    assert context.closed is True


@pytest.mark.asyncio
async def test_account_profile_photos_requires_authenticated_gateway(tmp_path: Path):
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=False)

    with pytest.raises(FacebookBrowserError, match="require_login=True"):
        await gateway.account_profile_photos("https://www.facebook.com/100")


@pytest.mark.asyncio
async def test_account_profile_photos_requires_login_cookie_before_navigation(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()
    context = browser.Context()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)

    async def unexpected(*args, **kwargs):
        raise AssertionError("missing login must stop before collector navigation")

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", unexpected)

    with pytest.raises(FacebookBrowserLoginRequired, match="互動式登入"):
        await gateway.account_profile_photos("https://www.facebook.com/100")

    assert context.cookie_reads == 1
    assert context.page.visited == []
    assert context.closed is True


@pytest.mark.asyncio
async def test_account_profile_photos_uses_logged_in_photos_surface_and_marks_scope(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            self.cookie_reads += 1
            return [{"name": "c_user", "value": "operator-account"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    observed = []

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        observed.append({
            "source_url": source_url,
            "profile_identity": profile_identity,
            "progress": progress,
            **kwargs,
        })
        checkpoint = {
            "collected_items": [],
            "discovered_urls": [],
            "processed_urls": [],
            "grid_complete": False,
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
            "resume_url": source_url,
        }
        return gateway._public_photo_result(checkpoint, [])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)
    initial_progress = {"resume_url": "https://www.facebook.com/photo.php?fbid=122"}

    result = await gateway.account_profile_photos(
        "https://www.facebook.com/profile.php?id=100123",
        initial_progress,
        "profile-7-account-photos",
    )

    assert [entry["source_url"] for entry in observed] == [
        "https://www.facebook.com/100123/photos_by",
        "https://www.facebook.com/100123/photos_of",
    ]
    assert all(entry["profile_identity"] == "100123" for entry in observed)
    assert observed[0]["progress"] == initial_progress
    assert observed[1]["progress"] == {}
    assert all(
        entry["diagnostic_key"] == "profile-7-account-photos"
        for entry in observed
    )
    assert observed[0]["enforce_profile_owner"] is True
    assert observed[1]["enforce_profile_owner"] is False
    assert result["progress"]["access_scope"] == "account_visible"
    assert result["progress"]["collector"] == "authenticated_facebook_photo_viewer"
    expected_scope_hash = hashlib.sha256(b"operator-account").hexdigest()
    assert result["progress"]["viewer_scope_hash"] == expected_scope_hash
    assert result["progress"]["viewer_scope_changed"] is False
    assert all(
        surface["viewer_scope_hash"] == expected_scope_hash
        for surface in result["progress"]["surfaces"].values()
    )
    assert result["access_scope"] == "account_visible"
    assert result["collector"] == "authenticated_facebook_photo_viewer"
    assert result["evidence"]["access_scope"] == "account_visible"
    assert result["evidence"]["collector"] == "authenticated_facebook_photo_viewer"
    assert result["evidence"]["authenticated_cookie_verified"] is True
    assert result["evidence"]["viewer_scope_hash"] == expected_scope_hash
    assert result["evidence"]["all_surfaces_terminal"] is False
    assert set(result["evidence"]["surfaces"]) == {"photos_by", "photos_of"}
    assert context.cookie_reads == 1
    assert context.closed is True


@pytest.mark.asyncio
async def test_account_profile_photos_merges_and_deduplicates_both_terminal_surfaces(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": "100999"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        tagged = source_url.endswith("/photos_of")
        unique_id = "2" if tagged else "1"
        items = [
            {
                "id": "shared",
                "url": "https://www.facebook.com/photo.php?fbid=shared",
                "image": "https://scontent.example.fbcdn.net/shared.jpg",
            },
            {
                "id": unique_id,
                "url": f"https://www.facebook.com/photo.php?fbid={unique_id}",
                "image": f"https://scontent.example.fbcdn.net/{unique_id}.jpg",
            },
        ]
        checkpoint = {
            "collected_items": items,
            "discovered_urls": [item["url"] for item in items],
            "processed_urls": [item["url"] for item in items],
            "grid_complete": True,
            "grid_empty_confirmed": False,
            "completed": True,
            "terminal_reason": "grid_inventory_processed",
            "stalled_reason": "",
            "resume_url": "",
        }
        return gateway._public_photo_result(checkpoint, items)

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)

    result = await gateway.account_profile_photos("https://www.facebook.com/100")

    assert result["completed"] is True
    assert result["grid_complete"] is True
    assert result["terminal_reason"] == "account_photo_surfaces_processed"
    assert result["evidence"]["all_surfaces_terminal"] is True
    assert all(
        evidence["terminal_verified"]
        for evidence in result["evidence"]["surfaces"].values()
    )
    assert [item["id"] for item in result["items"]] == ["shared", "1", "2"]
    assert [item["id"] for item in result["batch_items"]] == ["shared", "1", "2"]
    assert result["discovered_count"] == 3
    assert result["processed_count"] == 3


@pytest.mark.asyncio
async def test_account_profile_photos_shares_permalink_and_new_photo_limits_across_surfaces(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": "100999"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    gateway.album_batch_max_operations = 5
    gateway.album_batch_max_new_photos = 3
    observed_limits: list[tuple[int, int]] = []

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        operation_limit = int(kwargs["batch_operation_limit"])
        new_photo_limit = int(kwargs["batch_new_photo_limit"])
        observed_limits.append((operation_limit, new_photo_limit))
        if source_url.endswith("/photos_by"):
            operation_count = min(4, operation_limit)
            item_count = min(2, new_photo_limit)
            prefix = "by"
        else:
            operation_count = min(1, operation_limit)
            item_count = min(1, new_photo_limit)
            prefix = "of"
        items = [
            {
                "id": f"{prefix}-{index}",
                "url": f"https://www.facebook.com/photo.php?fbid={prefix}-{index}",
                "image": f"https://scontent.example.fbcdn.net/{prefix}-{index}.jpg",
            }
            for index in range(item_count)
        ]
        checkpoint = {
            "collected_items": items,
            "discovered_urls": [item["url"] for item in items],
            "processed_urls": [item["url"] for item in items],
            "grid_complete": False,
            "grid_empty_confirmed": False,
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
            "resume_url": source_url,
            "batch_operations": operation_count,
        }
        return gateway._public_photo_result(checkpoint, items)

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)

    result = await gateway.account_profile_photos("https://www.facebook.com/100")

    assert observed_limits == [(5, 3), (1, 1)]
    assert len(result["batch_items"]) == 3
    assert sum(
        int(surface.get("batch_operations") or 0)
        for surface in result["progress"]["surfaces"].values()
    ) == 5


@pytest.mark.asyncio
async def test_account_profile_photos_dispatches_top_level_refresh_to_completed_surface(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()
    viewer = "100999"

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": viewer}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    by_url = "https://www.facebook.com/photo.php?fbid=11"
    of_url = "https://www.facebook.com/photo.php?fbid=22"

    async def no_access_wall(current_page, diagnostic_key=None):
        return None

    async def current_media_id(current_page):
        return (parse_qs(urlsplit(current_page.url).query).get("fbid") or [""])[0]

    async def refreshed_image(current_page):
        media_id = await current_media_id(current_page)
        return f"https://scontent.example.fbcdn.net/{media_id}.jpg?fresh=1"

    monkeypatch.setattr(gateway, "_raise_for_access_wall", no_access_wall)
    monkeypatch.setattr(gateway, "_current_viewer_media_id", current_media_id)
    monkeypatch.setattr(gateway, "_public_photo_original_image", refreshed_image)

    def completed_surface(media_id: str, viewer_url: str) -> dict:
        return {
            "schema_version": 4,
            "grid_complete": True,
            "grid_empty_confirmed": False,
            "completed": True,
            "terminal_reason": "grid_inventory_processed",
            "discovered_urls": [viewer_url],
            "processed_urls": [viewer_url],
            "collected_items": [{
                "id": media_id,
                "url": viewer_url,
                "image": f"https://scontent.example.fbcdn.net/{media_id}.jpg?expired=1",
            }],
        }

    scope_hash = hashlib.sha256(viewer.encode()).hexdigest()
    result = await gateway.account_profile_photos(
        "https://www.facebook.com/100",
        {
            "access_scope": "account_visible",
            "viewer_scope_hash": scope_hash,
            "surfaces": {
                "photos_by": completed_surface("11", by_url),
                "photos_of": completed_surface("22", of_url),
            },
            "refresh_media_external_ids": ["22"],
        },
    )

    assert context.page.visited == [of_url]
    assert [item["id"] for item in result["batch_items"]] == ["22"]
    assert next(item for item in result["items"] if item["id"] == "22")[
        "image"
    ].endswith("?fresh=1")
    assert result["progress"]["surfaces"]["photos_by"].get(
        "refresh_media_external_ids", []
    ) == []
    assert result["progress"]["surfaces"]["photos_of"][
        "refresh_media_external_ids"
    ] == []
    assert result["progress"].get("refresh_media_external_ids", []) == []


@pytest.mark.asyncio
async def test_account_profile_photos_requires_both_surfaces_to_reach_terminal(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": "100999"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        complete = source_url.endswith("/photos_by")
        checkpoint = {
            "collected_items": [],
            "discovered_urls": [],
            "processed_urls": [],
            "grid_complete": complete,
            "grid_empty_confirmed": complete,
            "completed": complete,
            "terminal_reason": "explicit_empty_grid" if complete else "",
            "stalled_reason": "",
            "resume_url": "" if complete else source_url,
        }
        return gateway._public_photo_result(checkpoint, [])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)

    result = await gateway.account_profile_photos("https://www.facebook.com/100")

    assert result["completed"] is False
    assert result["grid_complete"] is False
    assert result["resumable"] is True
    assert result["progress"]["resume_url"].endswith("/photos_of")
    assert result["evidence"]["surfaces"]["photos_by"]["terminal_verified"] is True
    assert result["evidence"]["surfaces"]["photos_of"]["terminal_verified"] is False


@pytest.mark.asyncio
async def test_account_profile_photos_preserves_successful_surface_when_other_fails(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": "100999"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    photo = {
        "id": "11",
        "url": "https://www.facebook.com/photo.php?fbid=11",
        "image": "https://scontent.example.fbcdn.net/11.jpg",
    }

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        if source_url.endswith("/photos_of"):
            raise FacebookBrowserError(
                "facebook_photo_surface_redirected_or_unverified"
            )
        checkpoint = {
            "collected_items": [photo],
            "discovered_urls": [photo["url"]],
            "processed_urls": [photo["url"]],
            "grid_complete": True,
            "grid_empty_confirmed": False,
            "completed": True,
            "terminal_reason": "grid_inventory_processed",
            "stalled_reason": "",
            "resume_url": "",
            "batch_operations": 1,
        }
        return gateway._public_photo_result(checkpoint, [photo])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)

    result = await gateway.account_profile_photos("https://www.facebook.com/100")

    assert result["completed"] is False
    assert result["resumable"] is False
    assert result["batch_items"] == [photo]
    assert result["items"] == [photo]
    assert result["progress"]["surfaces"]["photos_by"]["completed"] is True
    assert result["progress"]["surfaces"]["photos_of"]["completed"] is False
    assert "photos_of:collector_error:" in result["stalled_reason"]
    assert result["evidence"]["surface_failures"]["photos_of"]["type"] == (
        "collector_error"
    )
    assert result["evidence"]["all_surfaces_terminal"] is False


@pytest.mark.asyncio
async def test_account_profile_photos_resumes_each_surface_from_its_own_checkpoint(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()
    viewer = "100999"

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": viewer}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    observed = {}

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        surface = "photos_of" if source_url.endswith("/photos_of") else "photos_by"
        observed[surface] = dict(progress)
        checkpoint = dict(progress)
        checkpoint.update({
            "collected_items": [],
            "discovered_urls": [],
            "processed_urls": [],
            "grid_complete": False,
            "grid_empty_confirmed": False,
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
            "resume_url": source_url,
        })
        return gateway._public_photo_result(checkpoint, [])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)
    scope_hash = hashlib.sha256(viewer.encode()).hexdigest()
    progress = {
        "access_scope": "account_visible",
        "viewer_scope_hash": scope_hash,
        "surfaces": {
            "photos_by": {"grid_scroll_depth": 3, "resume_url": "by-cursor"},
            "photos_of": {"grid_scroll_depth": 7, "resume_url": "of-cursor"},
        },
    }

    result = await gateway.account_profile_photos(
        "https://www.facebook.com/100", progress
    )

    assert observed["photos_by"]["grid_scroll_depth"] == 3
    assert observed["photos_by"]["resume_url"] == "by-cursor"
    assert observed["photos_of"]["grid_scroll_depth"] == 7
    assert observed["photos_of"]["resume_url"] == "of-cursor"
    assert result["progress"]["surfaces"]["photos_by"]["grid_scroll_depth"] == 3
    assert result["progress"]["surfaces"]["photos_of"]["grid_scroll_depth"] == 7


@pytest.mark.asyncio
async def test_account_profile_photos_keeps_partial_mixed_scope_for_same_viewer(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()
    viewer = "100999"

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": viewer}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    observed: dict[str, dict] = {}

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        surface = "photos_of" if source_url.endswith("/photos_of") else "photos_by"
        observed[surface] = dict(progress)
        checkpoint = dict(progress)
        checkpoint.update({
            "collected_items": list(progress.get("collected_items") or []),
            "discovered_urls": list(progress.get("discovered_urls") or []),
            "processed_urls": list(progress.get("processed_urls") or []),
            "grid_complete": False,
            "grid_empty_confirmed": False,
            "completed": False,
            "terminal_reason": "",
            "stalled_reason": "",
            "resume_url": source_url,
        })
        return gateway._public_photo_result(checkpoint, [])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)
    scope_hash = hashlib.sha256(viewer.encode()).hexdigest()
    partial_url = "https://www.facebook.com/photo.php?fbid=11"
    result = await gateway.account_profile_photos(
        "https://www.facebook.com/100",
        {
            "access_scope": "mixed",
            "viewer_scope_hash": scope_hash,
            "surfaces": {
                "photos_by": {
                    "schema_version": 4,
                    "grid_scroll_depth": 3,
                    "discovered_urls": [partial_url],
                    "processed_urls": [],
                    "collected_items": [],
                    "resume_url": "https://www.facebook.com/100/photos_by",
                },
            },
            "actor_collected_items": [{"id": "actor-supplement"}],
        },
    )

    assert observed["photos_by"]["grid_scroll_depth"] == 3
    assert observed["photos_by"]["discovered_urls"] == [partial_url]
    assert observed["photos_of"] == {}
    assert result["progress"]["viewer_scope_changed"] is False
    assert result["evidence"]["previous_viewer_scope_hash"] == ""


@pytest.mark.asyncio
async def test_account_profile_photos_resets_inventory_when_viewer_scope_changes(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": "new-viewer"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)
    observed_progress = []

    async def collect(page, source_url, profile_identity, progress, **kwargs):
        observed_progress.append(dict(progress))
        checkpoint = {
            "collected_items": [],
            "discovered_urls": [],
            "processed_urls": [],
            "grid_complete": True,
            "grid_empty_confirmed": True,
            "completed": True,
            "terminal_reason": "explicit_empty_grid",
            "stalled_reason": "",
            "resume_url": "",
        }
        return gateway._public_photo_result(checkpoint, [])

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", collect)
    old_hash = hashlib.sha256(b"old-viewer").hexdigest()
    progress = {
        "access_scope": "account_visible",
        "viewer_scope_hash": old_hash,
        "surfaces": {
            "photos_by": {"discovered_urls": ["https://facebook.com/photo.php?fbid=1"]},
            "photos_of": {"discovered_urls": ["https://facebook.com/photo.php?fbid=2"]},
        },
        "discovered_urls": ["https://facebook.com/photo.php?fbid=1"],
        "collected_items": [{"id": "1", "url": "old", "image": "old"}],
    }

    result = await gateway.account_profile_photos(
        "https://www.facebook.com/100", progress
    )

    new_hash = hashlib.sha256(b"new-viewer").hexdigest()
    assert observed_progress == [{}, {}]
    assert result["progress"]["viewer_scope_hash"] == new_hash
    assert result["progress"]["viewer_scope_changed"] is True
    assert result["progress"]["previous_viewer_scope_hash"] == old_hash
    assert result["evidence"]["viewer_scope_changed"] is True


@pytest.mark.asyncio
async def test_account_profile_photos_reports_expired_login_clearly(
    tmp_path: Path, monkeypatch
):
    browser = EmptyCookieBrowser()

    class LoggedInContext(browser.Context):
        async def cookies(self, url: str):
            return [{"name": "c_user", "value": "operator-account"}]

    context = LoggedInContext()
    monkeypatch.setattr(
        "fb_monitor.facebook_browser.async_playwright", lambda: browser.Manager(context)
    )
    gateway = FacebookBrowserGateway(True, tmp_path, require_login=True)

    async def expired(*args, **kwargs):
        raise FacebookBrowserLoginRequired("generic login wall")

    monkeypatch.setattr(gateway, "_collect_public_profile_photo_inventory", expired)

    with pytest.raises(FacebookBrowserLoginRequired, match="登入狀態已失效"):
        await gateway.account_profile_photos("https://www.facebook.com/100")

    assert context.closed is True


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
