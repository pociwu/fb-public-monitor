from datetime import UTC, datetime

from fb_monitor.apify import ApifyGateway


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
