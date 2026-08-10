from pathlib import Path

import pytest

from fb_monitor.facebook_browser import FacebookBrowserGateway, normalize_browser_canary_posts, normalize_browser_profile
from fb_monitor.normalize import normalize_url


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
        return True

    monkeypatch.setattr(gateway, "_large_facebook_images", visible_images)
    monkeypatch.setattr(gateway, "_largest_viewer_image", current_viewer_image)
    monkeypatch.setattr(gateway, "_click_next_photo", click_next)

    photos, progress = await gateway._collect_post_album_photos(page, "https://www.facebook.com/example/posts/p1")

    assert page.visited == [
        "https://www.facebook.com/example/posts/p1",
        "https://www.facebook.com/photo/?fbid=1",
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
    assert progress["resume_url"] == "https://www.facebook.com/photo/?fbid=1"
    gateway._save_album_progress({"post": progress})
    assert gateway._load_album_progress()["post"]["resume_url"] == progress["resume_url"]


@pytest.mark.asyncio
async def test_browser_capture_is_saved_per_profile(tmp_path: Path):
    class FakePage:
        viewport_size = {"width": 1365, "height": 900}

        def __init__(self):
            self.clip = None

        async def evaluate(self, expression: str):
            return 5000

        async def screenshot(self, *, path: str, full_page: bool, clip: dict):
            self.clip = clip
            Path(path).write_bytes(b"png")

    gateway = FacebookBrowserGateway(True, tmp_path)
    page = FakePage()
    saved = await gateway._save_capture(page, "profile/1")

    assert saved == tmp_path / "screenshots" / "profile-profile_1.png"
    assert saved.read_bytes() == b"png"
    assert page.clip == {"x": 0, "y": 0, "width": 1365, "height": 2700}


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
