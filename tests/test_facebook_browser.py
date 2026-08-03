from fb_monitor.facebook_browser import normalize_browser_profile


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
