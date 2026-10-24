import httpx
import asyncio
import pytest

from fb_monitor.scrapecreators import ScrapeCreatorsGateway, ScrapeCreatorsError
from fb_monitor.provider_state import ProviderState


def seeded_gateway(tmp_path, remaining=100, trial_limit=90):
    path = tmp_path / "s.db"
    state = ProviderState(path)
    launch, _ = state.claim("scrapecreators", "legacy-receipt")
    if launch:
        state.save("scrapecreators", "legacy-receipt", "done",
                   payload={"success": True, "credits_remaining": remaining, "credits_charged": 0})
    state.conn.close()
    return ScrapeCreatorsGateway("secret", state_path=path, enabled=True,
                                free_only_confirmed=True, trial_limit=trial_limit)


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
        return httpx.Response(200, json={"success": True, "id": "123", "name": "Alice", "profilePicLarge": "https://cdn/avatar.jpg", "credits_remaining": 99, "credits_charged": 1})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    def gateway():
        return seeded_gateway(tmp_path, trial_limit=1)
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
    gateway = seeded_gateway(tmp_path, trial_limit=1)
    for url in ["https://facebook.com/123", "https://facebook.com/123", "https://facebook.com/456"]:
        with pytest.raises(ScrapeCreatorsError):
            await gateway.profile(url)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_reserve_ten_stops_at_boundary_and_survives_restart(tmp_path, monkeypatch):
    calls = []
    async def get(self, url, **kwargs):
        calls.append(url)
        return httpx.Response(200, json={"success": True, "posts": [], "credits_remaining": 10, "credits_charged": 1})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    gateway = seeded_gateway(tmp_path, remaining=11)
    await gateway.posts("https://facebook.com/123")
    assert gateway.budget_status()["remaining"] == 10
    with pytest.raises(ScrapeCreatorsError):
        await seeded_gateway(tmp_path).posts("https://facebook.com/123", "next")
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_ninetieth_lifetime_call_is_last(tmp_path, monkeypatch):
    calls = []
    async def get(self, url, **kwargs):
        calls.append(url)
        return httpx.Response(200, json={"success": True, "posts": [], "credits_remaining": 50, "credits_charged": 1})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    gateway = seeded_gateway(tmp_path)
    gateway.state.conn.execute("INSERT INTO provider_trial_usage VALUES ('scrapecreators',89)")
    gateway.state.conn.commit()
    await gateway.posts("https://facebook.com/123")
    with pytest.raises(ScrapeCreatorsError, match="上限"):
        await seeded_gateway(tmp_path).posts("https://facebook.com/123", "next")
    assert len(calls) == 1
    assert gateway.budget_status()["used"] == 90


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", [{}, {"credits_remaining": 90},
    {"credits_remaining": "90", "credits_charged": 1}, {"credits_remaining": 97, "credits_charged": 3}])
async def test_bad_or_changed_billing_stops_new_requests_but_keeps_result(tmp_path, monkeypatch, receipt):
    calls = []
    async def get(self, url, **kwargs):
        calls.append(url)
        return httpx.Response(200, json={"success": True, "posts": [], **receipt})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    gateway = seeded_gateway(tmp_path)
    await gateway.posts("https://facebook.com/123")
    assert gateway.budget_status()["blocked_reason"]
    # Replay raw data even when new purchases are blocked.
    await seeded_gateway(tmp_path).posts("https://facebook.com/123")
    with pytest.raises(ScrapeCreatorsError):
        await gateway.posts("https://facebook.com/123", "next")
    assert len(calls) == 1


def test_migrate_existing_two_calls_keeps_98_balance(tmp_path):
    path = tmp_path / "s.db"
    state = ProviderState(path)
    for i, remaining in enumerate([99, 98]):
        state.claim("scrapecreators", str(i))
        state.save("scrapecreators", str(i), "done", payload={"credits_remaining": remaining, "credits_charged": 1})
        state.reserve_trial("scrapecreators", 5)
    gateway = ScrapeCreatorsGateway("x", state_path=path, enabled=True, free_only_confirmed=True, trial_limit=90)
    budget = gateway.budget_status()
    assert (budget["used"], budget["remaining"], budget["charged_total"], budget["limit"], budget["reserve"]) == (2, 98, 2, 90, 10)
    assert budget["can_request"]


@pytest.mark.asyncio
async def test_missing_receipt_blocks_without_network(tmp_path, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("unknown credit balance must not call API")
    monkeypatch.setattr(httpx.AsyncClient, "get", forbidden)
    gateway = ScrapeCreatorsGateway("x", state_path=tmp_path / "s.db", enabled=True, free_only_confirmed=True, trial_limit=90)
    with pytest.raises(ScrapeCreatorsError):
        await gateway.posts("https://facebook.com/123")


def test_hard_caps_cannot_be_disabled(tmp_path):
    gateway = ScrapeCreatorsGateway("x", state_path=tmp_path / "s.db", trial_limit=999, credit_reserve=0)
    assert gateway.trial_limit == 90
    assert gateway.credit_reserve == 10


@pytest.mark.asyncio
async def test_concurrent_clients_cannot_both_spend_last_credit(tmp_path, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def get(self, url, **kwargs):
        calls.append(url)
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"success": True, "posts": [], "credits_remaining": 10, "credits_charged": 1})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    left = seeded_gateway(tmp_path, remaining=11)
    right = seeded_gateway(tmp_path, remaining=11)
    pending = asyncio.create_task(left.posts("https://facebook.com/123"))
    await entered.wait()
    try:
        with pytest.raises(ScrapeCreatorsError):
            await right.posts("https://facebook.com/123", "next")
    finally:
        release.set()
        await pending
    assert len(calls) == 1
    assert right.budget_status()["used"] == 1


@pytest.mark.asyncio
async def test_larger_upstream_balance_never_refills_local_free_budget(tmp_path, monkeypatch):
    async def get(self, url, **kwargs):
        return httpx.Response(200, json={"success": True, "posts": [], "credits_remaining": 1000, "credits_charged": 1})
    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    gateway = seeded_gateway(tmp_path, remaining=11)
    await gateway.posts("https://facebook.com/123")
    budget = gateway.budget_status()
    assert budget["observed_remaining"] == 1000
    assert budget["remaining"] == 10
    assert not budget["can_request"]
