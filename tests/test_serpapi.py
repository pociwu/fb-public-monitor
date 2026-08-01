import pytest

from fb_monitor.serpapi import SerpApiGateway, SerpApiQuotaExceeded, profile_id_from_url


def test_profile_id_from_supported_facebook_urls():
    assert profile_id_from_url("https://www.facebook.com/alice") == "alice"
    assert profile_id_from_url("https://www.facebook.com/profile.php?id=12345") == "12345"
    assert profile_id_from_url("https://www.facebook.com/people/Alice/98765") == "98765"


@pytest.mark.asyncio
async def test_profile_checks_account_and_limits_public_photos(monkeypatch):
    gateway = SerpApiGateway("secret")
    calls = []

    async def fake_get(url, params):
        calls.append((url, params))
        if "account" in url:
            return {
                "plan_name": "Free Plan", "searches_per_month": 250,
                "total_searches_left": 40, "this_month_usage": 210,
                "plan_renewal_date": "2026-08-31", "this_hour_searches": 2,
                "account_rate_limit_per_hour": 50,
            }
        return {"profile_results": {"id": "123", "name": "Alice", "photos": [{"link": f"https://cdn/{index}.jpg"} for index in range(10)]}}

    monkeypatch.setattr(gateway, "_get_json", fake_get)
    result = await gateway.profile("https://www.facebook.com/123")
    assert result.account.searches_left == 40
    assert len(result.item["photos"]) == 6
    assert [call[0] for call in calls] == ["https://serpapi.com/account.json", "https://serpapi.com/search.json"]


@pytest.mark.asyncio
async def test_profile_does_not_search_when_account_has_no_credits(monkeypatch):
    gateway = SerpApiGateway("secret")
    calls = []

    async def fake_get(url, params):
        calls.append(url)
        return {
            "plan_name": "Free Plan", "searches_per_month": 250,
            "total_searches_left": 0, "this_month_usage": 250,
            "plan_renewal_date": "2026-08-31",
        }

    monkeypatch.setattr(gateway, "_get_json", fake_get)
    with pytest.raises(SerpApiQuotaExceeded) as caught:
        await gateway.profile("https://www.facebook.com/123")
    assert caught.value.account.searches_left == 0
    assert calls == ["https://serpapi.com/account.json"]
