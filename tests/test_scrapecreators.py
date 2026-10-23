import httpx
import pytest

from fb_monitor.scrapecreators import ScrapeCreatorsGateway, ScrapeCreatorsError


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,confirmed,key", [(False, True, "x"), (True, False, "x"), (True, True, "")])
async def test_free_trial_guard_never_calls_network(tmp_path, monkeypatch, enabled, confirmed, key):
    async def forbidden(*args, **kwargs):
        pytest.fail("guard must stop network")
    monkeypatch.setattr(httpx.AsyncClient, "get", forbidden)
    gateway = ScrapeCreatorsGateway(key, state_path=tmp_path / "s.db", enabled=enabled, free_only_confirmed=confirmed)
    with pytest.raises(ScrapeCreatorsError):
        await gateway.profile("https://facebook.com/123")


@pytest.mark.asyncio
async def test_trial_limit_and_cache_survive_restart(tmp_path, monkeypatch):
    calls = []
    async def get(self, url, **kwargs):
        calls.append(url)
        return httpx.Response(200, json={"success": True, "id": "123", "name": "Alice", "profilePicLarge": "https://cdn/avatar.jpg"})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    def gateway():
        return ScrapeCreatorsGateway("secret", state_path=tmp_path / "s.db", enabled=True, free_only_confirmed=True, trial_limit=1)
    item = await gateway().profile("https://facebook.com/123")
    assert item["profile_picture"] == "https://cdn/avatar.jpg"
    assert item["profile_data_source"] == "ScrapeCreators"
    assert (await gateway().profile("https://facebook.com/123"))["name"] == "Alice"
    with pytest.raises(ScrapeCreatorsError, match="上限"):
        await gateway().profile("https://facebook.com/456")
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_timeout_consumes_reservation_no_auto_retry(tmp_path, monkeypatch):
    calls = []
    async def get(self, url, **kwargs):
        calls.append(url)
        raise httpx.ReadTimeout("secret must not be logged")
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    gateway = ScrapeCreatorsGateway("secret", state_path=tmp_path / "s.db", enabled=True, free_only_confirmed=True, trial_limit=1)
    for url in ["https://facebook.com/123", "https://facebook.com/123", "https://facebook.com/456"]:
        with pytest.raises(ScrapeCreatorsError):
            await gateway.profile(url)
    assert len(calls) == 1
