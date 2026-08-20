from datetime import UTC, datetime

from decimal import Decimal

from fb_monitor.apify import ApifyGateway, StartedActor


class FakeUser:
    def monthly_usage(self):
        return {
            "usageCycle": {
                "startAt": datetime(2026, 7, 9, tzinfo=UTC),
                "endAt": datetime(2026, 8, 8, 23, 59, 59, tzinfo=UTC),
            },
            "totalUsageCreditsUsdAfterVolumeDiscount": 4.99,
        }


class FakeClient:
    def user(self, user_id):
        assert user_id == "me"
        return FakeUser()


def test_monthly_usage_parses_official_billing_response():
    gateway = ApifyGateway("")
    gateway.client = FakeClient()
    usage = gateway._monthly_usage_sync()
    assert usage.used_usd == 4.99
    assert usage.cycle_start_at == "2026-07-09T00:00:00+00:00"
    assert usage.cycle_end_at == "2026-08-08T23:59:59+00:00"


class FakeActor:
    def __init__(self):
        self.kwargs = None

    def start(self, **kwargs):
        self.kwargs = kwargs
        return {
            "id": "run-1",
            "defaultDatasetId": "dataset-1",
            "defaultKeyValueStoreId": "store-1",
        }


class FakeRun:
    def wait_for_finish(self, wait_secs):
        assert wait_secs == 90
        return {
            "id": "run-1",
            "status": "SUCCEEDED",
            "defaultDatasetId": "dataset-1",
            "defaultKeyValueStoreId": "store-1",
            "usageTotalUsd": 0.0123,
        }


class FakeDataset:
    class Result:
        items = [{"postId": "p1"}]

    def list_items(self, clean):
        assert clean is True
        return self.Result()


class FakeStore:
    def get_record(self, key):
        assert key == "SUMMARY"
        return {"value": {"health": "ok"}}


class FakeCaptureClient:
    def __init__(self):
        self.actor_client = FakeActor()

    def actor(self, actor_id):
        assert actor_id == "example/posts"
        return self.actor_client

    def run(self, run_id):
        assert run_id == "run-1"
        return FakeRun()

    def dataset(self, dataset_id):
        assert dataset_id == "dataset-1"
        return FakeDataset()

    def key_value_store(self, store_id):
        assert store_id == "store-1"
        return FakeStore()


def test_capture_gateway_persists_identifiers_before_waiting_and_keeps_charge_cap():
    gateway = ApifyGateway("")
    gateway.client = FakeCaptureClient()

    started = gateway._start_sync("example/posts", {"maxPostsPerProfile": 50}, 0.19)

    assert started == StartedActor("run-1", "dataset-1", "store-1")
    assert gateway.client.actor_client.kwargs == {
        "run_input": {"maxPostsPerProfile": 50},
        "max_total_charge_usd": Decimal("0.19"),
    }

    result = gateway._finish_sync(started, 90)
    assert result.run_id == "run-1"
    assert result.items == [{"postId": "p1"}]
    assert result.summary == {"health": "ok"}
    assert result.charged_usd == 0.0123
