from datetime import UTC, datetime, timedelta
import pytest
from test_scrapecreators_capture import service_fixture


@pytest.mark.asyncio
async def test_empty_cooldown_stale_profile_queues_browser_without_provider_retry(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, 'unknown')
    service.settings.spacing_min_minutes = service.settings.spacing_max_minutes = 0
    old = (datetime.now(UTC) - timedelta(hours=72)).isoformat()
    service.db.execute("UPDATE profiles SET last_success_at=?,serp_last_checked_at=NULL WHERE id=1", (old,))
    service._record_serpapi_attempts(1, [{'query': 'target', 'status': 'empty'}])
    async def forbidden(*args):
        pytest.fail('cooldown must not retry SerpApi or Bright Data')
    monkeypatch.setattr(service, '_refresh_serpapi_profile', forbidden)
    monkeypatch.setattr(service.brightdata, 'profile', forbidden)
    job = service._enqueue(1, 'visit', 10, datetime.now(UTC))
    await service._run_next_job()
    assert service.db.row("SELECT id FROM jobs WHERE job_type='profile_browser_fallback' AND status='pending'")
    assert service.db.row('SELECT last_error FROM profiles WHERE id=1')['last_error']
    assert service.db.row('SELECT status FROM jobs WHERE id=?', (job,))['status'] == 'deferred_profile'


@pytest.mark.asyncio
async def test_failed_refresh_job_keeps_error(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, 'unknown')
    service.settings.spacing_min_minutes = service.settings.spacing_max_minutes = 0
    async def visit(profile_id):
        service._profile_refresh_failed(profile_id, 'all sources failed')
    monkeypatch.setattr(service, 'visit_profile', visit)
    job = service._enqueue(1, 'visit', 10, datetime.now(UTC))
    await service._run_next_job()
    row = service.db.row('SELECT status,error FROM jobs WHERE id=?', (job,))
    assert row == {'status': 'failed', 'error': 'all sources failed'}


@pytest.mark.asyncio
async def test_public_verification_does_not_clear_profile_failure(tmp_path, monkeypatch):
    service = service_fixture(tmp_path, monkeypatch, 'unknown')
    old = (datetime.now(UTC) - timedelta(hours=72)).isoformat()
    service.db.execute("UPDATE profiles SET last_success_at=?,last_error='profile refresh failed' WHERE id=1", (old,))
    target = service._capture_v2_target_id(service.db.row('SELECT * FROM profiles WHERE id=1'))
    async def anonymous(url, diagnostic_key=None):
        return {'id': target, 'url': url, 'public_content_proof': {
            'kind': 'target_permalink_article', 'permalink': f'https://www.facebook.com/permalink.php?story_fbid=pfbid0PublicPost&id={target}',
            'post_identity': 'pfbid0PublicPost', 'target_identity': target, 'article_index': 0}}
    monkeypatch.setattr(service.facebook_anonymous_browser, 'profile', anonymous)
    await service.verify_public_v2(1)
    row = service.db.row('SELECT public_state,last_success_at,last_error FROM profiles WHERE id=1')
    assert row == {'public_state': 'public', 'last_success_at': old, 'last_error': 'profile refresh failed'}
