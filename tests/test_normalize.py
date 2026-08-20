from fb_monitor.normalize import content_hash, facebook_post_identity, normalize_text, normalize_url


def test_normalization_ignores_tracking_and_engagement():
    a = {"text": "hello  world", "url": "https://x.test/post?fbclid=1", "likesCount": 1}
    b = {"text": "hello world", "url": "https://x.test/post", "likesCount": 99}
    assert content_hash(a) == content_hash(b)
    assert normalize_url(a["url"]) == "https://x.test/post"
    assert normalize_text("a  b\r\n c") == "a b\nc"


def test_facebook_photo_permalink_aliases_share_canonical_identity():
    aliases = [
        "https://www.facebook.com/photo.php?fbid=12345&set=a.9&fbclid=x",
        "https://m.facebook.com/photo/?fbid=12345&set=a.9",
        "https://mbasic.facebook.com/photo.php?fbid=12345&type=3",
    ]

    assert {facebook_post_identity(url) for url in aliases} == {"12345"}
    assert {normalize_url(url) for url in aliases} == {
        "https://www.facebook.com/photo.php?fbid=12345"
    }


def test_mobile_post_alias_normalizes_to_desktop_host():
    assert normalize_url("https://m.facebook.com/example/posts/p1/?ref=bookmarks") == (
        "https://www.facebook.com/example/posts/p1"
    )
