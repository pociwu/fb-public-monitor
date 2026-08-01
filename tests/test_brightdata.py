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
