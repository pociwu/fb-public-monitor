import httpx
import pytest

from fb_monitor.brightdata import BrightDataError, BrightDataGateway, normalize_brightdata_profile


def test_normalize_profile_maps_dashboard_fields():
    item = normalize_brightdata_profile(
        {
            "id": "123", "name": "Alice", "profile_photo": "https://cdn/avatar.jpg",
            "cover_photo": "https://cdn/cover.jpg", "bio": "Hello", "followers_count": 42,
            "work": "Example Inc.", "college": "Example University", "photos": list(range(10)),
        },
        "https://www.facebook.com/123",
    )
    assert item["profile_picture"] == "https://cdn/avatar.jpg"
    assert item["profile_intro_text"] == "Hello"
    assert item["followers"] == 42
    assert item["works"] == [{"title": "Example Inc."}]
    assert item["educations"] == [{"title": "Example University"}]
    assert len(item["photos"]) == 6
    assert item["profile_data_source"] == "Bright Data"


@pytest.mark.asyncio
async def test_profile_uses_official_dataset_and_bearer_token(monkeypatch):
    seen = {}

    async def fake_post(self, url, **kwargs):
        seen.update(url=url, **kwargs)
        return httpx.Response(200, json=[{"id": "123", "name": "Alice", "profile_photo": "https://cdn/avatar.jpg"}])

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    result = await BrightDataGateway("secret").profile("https://www.facebook.com/123")
    assert result["name"] == "Alice"
    assert seen["params"]["dataset_id"] == "gd_mf0urb782734ik94dz"
    assert seen["headers"]["Authorization"] == "Bearer secret"
    assert seen["json"] == {"input": [{"url": "https://www.facebook.com/123"}]}


@pytest.mark.asyncio
async def test_profile_requires_token():
    with pytest.raises(BrightDataError, match="BRIGHTDATA_API_TOKEN"):
        await BrightDataGateway("").profile("https://www.facebook.com/123")


@pytest.mark.asyncio
async def test_async_snapshot_is_downloaded(monkeypatch):
    async def post(self, url, **kwargs):
        return httpx.Response(202, json={"snapshot_id": "s_test"})

    async def get(self, url, **kwargs):
        if "/progress/" in url:
            return httpx.Response(200, json={"status": "ready"})
        return httpx.Response(200, json=[{"id": "123", "name": "Alice"}])

    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    assert (await BrightDataGateway("secret").profile("https://www.facebook.com/123"))["name"] == "Alice"


@pytest.mark.asyncio
async def test_pending_survives_restart_without_relaunch(tmp_path, monkeypatch):
    from fb_monitor.brightdata import BrightDataPending
    launches = []
    ready = False

    async def post(self, url, **kwargs):
        launches.append(url)
        return httpx.Response(202, json={"snapshot_id": "s_test"})

    async def get(self, url, **kwargs):
        if "/progress/" in url:
            return httpx.Response(200, json={"status": "ready" if ready else "running"})
        return httpx.Response(200, json=[{"id": "123", "name": "Alice"}])

    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    path = tmp_path / "state.sqlite3"
    with pytest.raises(BrightDataPending):
        await BrightDataGateway("secret", state_path=path).profile("https://facebook.com/123")
    ready = True
    result = await BrightDataGateway("secret", state_path=path).profile("https://facebook.com/123")
    assert result["name"] == "Alice"
    assert len(launches) == 1


@pytest.mark.asyncio
async def test_uncertain_launch_never_repeats(tmp_path, monkeypatch):
    calls = []

    async def post(self, url, **kwargs):
        calls.append(url)
        raise httpx.ReadTimeout("timeout")

    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    for _ in range(2):
        with pytest.raises(BrightDataError):
            await BrightDataGateway("secret", state_path=tmp_path / "state.sqlite3").profile("https://facebook.com/123")
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["starting", "running", "failed"])
async def test_snapshot_non_ready_never_downloads_or_relaunches(tmp_path, monkeypatch, status):
    calls = []
    async def post(self, url, **kwargs):
        calls.append("post")
        return httpx.Response(202, json={"snapshot_id": "s_test"})
    async def get(self, url, **kwargs):
        assert "/progress/" in url
        return httpx.Response(200, json={"status": status})
    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    gateway = BrightDataGateway("secret", state_path=tmp_path / "s.db")
    for _ in range(2):
        with pytest.raises(BrightDataError):
            await gateway.profile("https://facebook.com/123")
    assert calls == ["post"]
