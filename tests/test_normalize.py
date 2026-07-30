from fb_monitor.normalize import content_hash, normalize_text, normalize_url


def test_normalization_ignores_tracking_and_engagement():
    a = {"text": "hello  world", "url": "https://x.test/post?fbclid=1", "likesCount": 1}
    b = {"text": "hello world", "url": "https://x.test/post", "likesCount": 99}
    assert content_hash(a) == content_hash(b)
    assert normalize_url(a["url"]) == "https://x.test/post"
    assert normalize_text("a  b\r\n c") == "a b\nc"

