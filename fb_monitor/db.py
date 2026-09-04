from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from .apify import provider_charge_ceiling


CAPTURE_V2_SCHEMA_MIGRATION = "capture_v2_additive_schema_v1"
CONTRACT_TEST_GRANT_MIGRATION = "capture_v2_contract_test_grants_v1"
# Release-known unit price used by every pre-V2.1 paid posts launch.  It is
# intentionally immutable: future configuration changes must not shrink an
# in-flight legacy reservation during schema migration.
LEGACY_SOURCE_RESULT_PRICE_USD = 4.99 / 1000

CAPTURE_EPOCH_ACTIVE_STATUSES = frozenset(
    {
        "awaiting_contract",
        "ready",
        "running",
        "budget_paused",
        "manual_paused",
        "source_limited",
        "needs_reconcile",
    }
)
COVERAGE_STATUSES = frozenset(
    {"pending", "in_progress", "complete", "source_limited", "budget_paused", "manual_paused", "failed"}
)
CONTRACT_STATUSES = frozenset({"pending", "passed", "failed", "expired", "disabled"})
CONTRACT_GRANT_STATUSES = frozenset(
    {"active", "fulfilled", "exhausted", "closed", "expired", "revoked"}
)
ACCESS_STATES = frozenset(
    {
        "unknown",
        "authenticated_visible",
        "suspected_public",
        "confirmed_public",
        "suspected_private",
        "confirmed_private",
    }
)
BATCH_STATUSES = frozenset(
    {
        "prepared",
        "launching",
        "run_started",
        "needs_reconcile",
        "raw_saved",
        "import_failed",
        "imported",
        "committed",
        "failed",
    }
)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS profiles (
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL DEFAULT 1,
  fb_id TEXT, display_name TEXT, public_state TEXT NOT NULL DEFAULT 'unknown', missing_successes INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TEXT, last_success_at TEXT, next_visit_at TEXT, last_full_audit_at TEXT,
  backfill_cursor TEXT, backfill_done INTEGER NOT NULL DEFAULT 0, audit_cursor TEXT, audit_token TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0, sort_order INTEGER, last_manual_visit_at TEXT,
  apify_frozen INTEGER NOT NULL DEFAULT 0,
  last_error TEXT, profile_details_json TEXT, serp_last_checked_at TEXT, browser_canary_last_attempt_at TEXT,
  browser_post_cursor TEXT, browser_post_backfill_done INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS entities (
  id INTEGER PRIMARY KEY, profile_id INTEGER NOT NULL REFERENCES profiles(id), kind TEXT NOT NULL,
  external_id TEXT NOT NULL, parent_external_id TEXT, dedupe_key TEXT, source_url TEXT, published_at TEXT,
  current_hash TEXT, current_version_id INTEGER, present INTEGER NOT NULL DEFAULT 1,
  missing_successes INTEGER NOT NULL DEFAULT 0, notification_hash TEXT,
  source_scope TEXT, source_collector TEXT, source_run_id TEXT,
  source_viewer_scope_hash TEXT,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  UNIQUE(profile_id, kind, external_id)
);
CREATE TABLE IF NOT EXISTS versions (
  id INTEGER PRIMARY KEY, entity_id INTEGER NOT NULL REFERENCES entities(id), content_hash TEXT NOT NULL,
  normalized_json TEXT NOT NULL, raw_path TEXT NOT NULL, markdown_path TEXT,
  seen_at TEXT NOT NULL, change_type TEXT NOT NULL, UNIQUE(entity_id, content_hash)
);
CREATE TABLE IF NOT EXISTS media (
  id INTEGER PRIMARY KEY, sha256 TEXT UNIQUE, source_url TEXT NOT NULL, mime_type TEXT, size_bytes INTEGER,
  path TEXT, perceptual_hash TEXT, status TEXT NOT NULL DEFAULT 'pending', first_seen_at TEXT NOT NULL, last_attempt_at TEXT,
  retry_until TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS entity_media (
  entity_id INTEGER NOT NULL REFERENCES entities(id), version_id INTEGER NOT NULL REFERENCES versions(id),
  media_id INTEGER NOT NULL REFERENCES media(id), role TEXT, discovery_path TEXT, position INTEGER,
  PRIMARY KEY(entity_id, version_id, media_id)
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY, event_key TEXT NOT NULL UNIQUE, profile_id INTEGER REFERENCES profiles(id),
  entity_id INTEGER REFERENCES entities(id), event_type TEXT NOT NULL, payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL, notified_at TEXT, notification_group_id INTEGER
);
CREATE TABLE IF NOT EXISTS notification_groups (
  id INTEGER PRIMARY KEY, profile_id INTEGER REFERENCES profiles(id), entity_id INTEGER REFERENCES entities(id),
  payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
  ready_at TEXT NOT NULL, sent_at TEXT, cancelled_at TEXT
);
CREATE TABLE IF NOT EXISTS comment_baselines (
  profile_id INTEGER NOT NULL REFERENCES profiles(id), parent_external_id TEXT NOT NULL,
  established_at TEXT NOT NULL, PRIMARY KEY(profile_id,parent_external_id)
);
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY, event_id INTEGER REFERENCES events(id), kind TEXT NOT NULL DEFAULT 'text',
  payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL, sent_at TEXT,
  group_id INTEGER REFERENCES notification_groups(id), media_sha256 TEXT, media_perceptual_hash TEXT, cancelled_at TEXT,
  UNIQUE(event_id, kind, payload_json)
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY, profile_id INTEGER REFERENCES profiles(id), job_type TEXT NOT NULL,
  priority INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', payload_json TEXT NOT NULL DEFAULT '{}',
  available_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, error TEXT, created_at TEXT NOT NULL,
  started_at TEXT, finished_at TEXT, dedupe_key TEXT, lease_owner TEXT, leased_at TEXT,
  epoch_id INTEGER REFERENCES capture_epochs(id), batch_id INTEGER REFERENCES paid_source_batches(id)
);
CREATE TABLE IF NOT EXISTS usage (
  month TEXT NOT NULL, category TEXT NOT NULL, estimated_usd REAL NOT NULL DEFAULT 0,
  results INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL, PRIMARY KEY(month, category)
);
CREATE TABLE IF NOT EXISTS apify_usage_snapshot (
  id INTEGER PRIMARY KEY CHECK(id=1), used_usd REAL NOT NULL,
  cycle_start_at TEXT NOT NULL, cycle_end_at TEXT NOT NULL, fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS serpapi_usage_snapshot (
  id INTEGER PRIMARY KEY CHECK(id=1), plan_name TEXT NOT NULL,
  searches_per_month INTEGER NOT NULL, searches_left INTEGER NOT NULL,
  this_month_usage INTEGER NOT NULL, renewal_date TEXT,
  this_hour_searches INTEGER NOT NULL, rate_limit_per_hour INTEGER NOT NULL,
  fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS serpapi_profile_attempts (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  query_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('success','empty','error')),
  error TEXT,
  attempted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_serpapi_profile_attempts_profile
  ON serpapi_profile_attempts(profile_id,attempted_at DESC);
CREATE TABLE IF NOT EXISTS audit_seen (
  profile_id INTEGER NOT NULL REFERENCES profiles(id), audit_token TEXT NOT NULL,
  kind TEXT NOT NULL, external_id TEXT NOT NULL,
  PRIMARY KEY(profile_id,audit_token,kind,external_id)
);
CREATE TABLE IF NOT EXISTS actor_runs (
  id INTEGER PRIMARY KEY, profile_id INTEGER REFERENCES profiles(id), category TEXT NOT NULL,
  actor_id TEXT NOT NULL, run_id TEXT, input_variant TEXT, input_json TEXT NOT NULL,
  status TEXT NOT NULL, result_count INTEGER NOT NULL DEFAULT 0, charged_usd REAL NOT NULL DEFAULT 0,
  max_charge_usd REAL NOT NULL DEFAULT 0,
  raw_result_count INTEGER NOT NULL DEFAULT 0, parsed_result_count INTEGER NOT NULL DEFAULT 0,
  new_result_count INTEGER NOT NULL DEFAULT 0, updated_result_count INTEGER NOT NULL DEFAULT 0,
  duplicate_result_count INTEGER NOT NULL DEFAULT 0,
  summary_json TEXT, samples_json TEXT, error TEXT, started_at TEXT NOT NULL, finished_at TEXT
);
CREATE TABLE IF NOT EXISTS schema_migrations (
  name TEXT PRIMARY KEY, applied_at TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS access_observations (
  id INTEGER PRIMARY KEY,
  observation_key TEXT NOT NULL UNIQUE,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  source TEXT NOT NULL,
  auth_scope TEXT NOT NULL,
  verdict TEXT NOT NULL,
  target_fb_id TEXT,
  observed_fb_id TEXT,
  identity_match INTEGER NOT NULL DEFAULT 0 CHECK(identity_match IN (0,1)),
  evidence_hash TEXT,
  evidence_summary_json TEXT NOT NULL DEFAULT '{}',
  raw_evidence_path TEXT,
  observed_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actor_contracts (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  purpose TEXT NOT NULL,
  build_id TEXT NOT NULL DEFAULT '',
  schema_fingerprint TEXT NOT NULL DEFAULT '',
  input_mapping_hash TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending',
  passed_at TEXT,
  expires_at TEXT,
  invalidated_at TEXT,
  evidence_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(provider,actor_id,purpose,build_id,schema_fingerprint,input_mapping_hash)
);
CREATE TABLE IF NOT EXISTS contract_runs (
  id INTEGER PRIMARY KEY,
  contract_id INTEGER NOT NULL REFERENCES actor_contracts(id),
  request_hash TEXT NOT NULL UNIQUE,
  test_case TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  run_id TEXT,
  dataset_id TEXT,
  input_json TEXT NOT NULL DEFAULT '{}',
  expected_json TEXT NOT NULL DEFAULT '{}',
  result_json TEXT NOT NULL DEFAULT '{}',
  result_count INTEGER NOT NULL DEFAULT 0,
  charged_usd REAL NOT NULL DEFAULT 0,
  grant_allocation_id INTEGER REFERENCES contract_test_allocations(id),
  authorized_max_usd REAL NOT NULL DEFAULT 0,
  lease_owner TEXT,
  leased_at TEXT,
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE TABLE IF NOT EXISTS contract_test_grants (
  id INTEGER PRIMARY KEY,
  grant_key TEXT NOT NULL UNIQUE,
  purpose TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  max_usd REAL NOT NULL CHECK(max_usd>0),
  authorized_by TEXT NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  closed_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_contract_test_one_active_grant
  ON contract_test_grants(purpose) WHERE status='active';
CREATE TABLE IF NOT EXISTS contract_test_allocations (
  id INTEGER PRIMARY KEY,
  grant_id INTEGER NOT NULL REFERENCES contract_test_grants(id),
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  actor_id TEXT NOT NULL,
  schema_fingerprint TEXT NOT NULL,
  test_generation TEXT NOT NULL UNIQUE,
  authorized_usd REAL NOT NULL CHECK(authorized_usd>0),
  job_id INTEGER REFERENCES jobs(id),
  created_at TEXT NOT NULL,
  UNIQUE(grant_id,actor_id,schema_fingerprint)
);
CREATE TABLE IF NOT EXISTS capture_epochs (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  trigger_reason TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'awaiting_contract',
  is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  priority INTEGER NOT NULL DEFAULT 100,
  scope_json TEXT NOT NULL DEFAULT '{}',
  signal_observation_id INTEGER REFERENCES access_observations(id),
  reserved_budget_usd REAL NOT NULL DEFAULT 0,
  started_at TEXT,
  completed_at TEXT,
  terminal_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_capture_epochs_one_active_profile
  ON capture_epochs(profile_id) WHERE is_active=1;
CREATE TABLE IF NOT EXISTS coverage_streams (
  id INTEGER PRIMARY KEY,
  epoch_id INTEGER NOT NULL REFERENCES capture_epochs(id),
  stream TEXT NOT NULL,
  surface TEXT NOT NULL,
  scope_type TEXT NOT NULL DEFAULT 'profile',
  scope_id TEXT NOT NULL DEFAULT '',
  provider TEXT,
  contract_id INTEGER REFERENCES actor_contracts(id),
  status TEXT NOT NULL DEFAULT 'pending',
  input_cursor TEXT,
  output_cursor TEXT,
  provider_checkpoint_json TEXT NOT NULL DEFAULT '{}',
  low_watermark TEXT,
  high_watermark TEXT,
  terminal_evidence_json TEXT NOT NULL DEFAULT '{}',
  gaps_json TEXT NOT NULL DEFAULT '[]',
  seen_count INTEGER NOT NULL DEFAULT 0,
  new_count INTEGER NOT NULL DEFAULT 0,
  updated_count INTEGER NOT NULL DEFAULT 0,
  duplicate_count INTEGER NOT NULL DEFAULT 0,
  limited_reason TEXT,
  next_job_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(epoch_id,stream,surface,scope_type,scope_id)
);
CREATE TABLE IF NOT EXISTS profile_photo_captures (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  generation INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'pending',
  checkpoint_json TEXT NOT NULL DEFAULT '{}',
  seen_count INTEGER NOT NULL DEFAULT 0,
  new_count INTEGER NOT NULL DEFAULT 0,
  updated_count INTEGER NOT NULL DEFAULT 0,
  duplicate_count INTEGER NOT NULL DEFAULT 0,
  terminal_evidence_json TEXT NOT NULL DEFAULT '{}',
  limited_reason TEXT,
  next_job_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  UNIQUE(profile_id,generation)
);
CREATE TABLE IF NOT EXISTS paid_photo_batches (
  id INTEGER PRIMARY KEY,
  request_hash TEXT NOT NULL UNIQUE,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  photo_capture_id INTEGER NOT NULL REFERENCES profile_photo_captures(id),
  actor_run_id INTEGER REFERENCES actor_runs(id),
  actor_id TEXT NOT NULL,
  normalized_input_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'prepared',
  max_charge_usd REAL NOT NULL DEFAULT 0,
  run_id TEXT,
  dataset_id TEXT,
  key_value_store_id TEXT,
  raw_path TEXT,
  raw_sha256 TEXT,
  charged_usd REAL NOT NULL DEFAULT 0,
  raw_result_count INTEGER NOT NULL DEFAULT 0,
  parsed_result_count INTEGER NOT NULL DEFAULT 0,
  input_cursor TEXT,
  output_cursor TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  launched_at TEXT,
  raw_saved_at TEXT,
  imported_at TEXT,
  committed_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paid_source_batches (
  id INTEGER PRIMARY KEY,
  request_hash TEXT NOT NULL UNIQUE,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  epoch_id INTEGER NOT NULL REFERENCES capture_epochs(id),
  coverage_stream_id INTEGER NOT NULL REFERENCES coverage_streams(id),
  contract_id INTEGER REFERENCES actor_contracts(id),
  provider TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  intent TEXT NOT NULL,
  observation_window TEXT NOT NULL DEFAULT '',
  normalized_input_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'prepared',
  max_charge_usd REAL NOT NULL DEFAULT 0,
  run_id TEXT,
  dataset_id TEXT,
  key_value_store_id TEXT,
  raw_path TEXT,
  raw_sha256 TEXT,
  charged_usd REAL NOT NULL DEFAULT 0,
  raw_result_count INTEGER NOT NULL DEFAULT 0,
  parsed_result_count INTEGER NOT NULL DEFAULT 0,
  new_result_count INTEGER NOT NULL DEFAULT 0,
  updated_result_count INTEGER NOT NULL DEFAULT 0,
  duplicate_result_count INTEGER NOT NULL DEFAULT 0,
  input_cursor TEXT,
  output_cursor TEXT,
  identity_set_hash TEXT,
  lease_owner TEXT,
  leased_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  launched_at TEXT,
  raw_saved_at TEXT,
  imported_at TEXT,
  committed_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paid_access_probe_batches (
  id INTEGER PRIMARY KEY,
  request_hash TEXT NOT NULL UNIQUE,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  contract_id INTEGER NOT NULL REFERENCES actor_contracts(id),
  provider TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  intent TEXT NOT NULL DEFAULT 'access_probe',
  observation_window TEXT NOT NULL,
  normalized_input_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'prepared',
  max_charge_usd REAL NOT NULL DEFAULT 0,
  actor_run_id INTEGER REFERENCES actor_runs(id),
  run_id TEXT,
  dataset_id TEXT,
  key_value_store_id TEXT,
  raw_path TEXT,
  raw_sha256 TEXT,
  charged_usd REAL NOT NULL DEFAULT 0,
  raw_result_count INTEGER NOT NULL DEFAULT 0,
  parsed_result_count INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at TEXT NOT NULL,
  launched_at TEXT,
  raw_saved_at TEXT,
  imported_at TEXT,
  committed_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_run_registry (
  provider TEXT NOT NULL,
  run_id TEXT NOT NULL,
  owner_type TEXT NOT NULL CHECK(owner_type IN (
    'paid_source_batch','paid_photo_batch','paid_access_probe_batch','contract_run'
  )),
  owner_id INTEGER NOT NULL,
  actor_id TEXT NOT NULL DEFAULT '',
  request_fingerprint TEXT NOT NULL DEFAULT '',
  provider_started_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(provider,run_id),
  UNIQUE(owner_type,owner_id)
);
CREATE TABLE IF NOT EXISTS provider_run_conflicts (
  id INTEGER PRIMARY KEY,
  provider TEXT NOT NULL,
  run_id TEXT NOT NULL,
  attempted_owner_type TEXT NOT NULL,
  attempted_owner_id INTEGER NOT NULL,
  existing_provider TEXT NOT NULL,
  existing_run_id TEXT NOT NULL,
  existing_owner_type TEXT NOT NULL,
  existing_owner_id INTEGER NOT NULL,
  actor_id TEXT NOT NULL DEFAULT '',
  request_fingerprint TEXT NOT NULL DEFAULT '',
  evidence_json TEXT NOT NULL DEFAULT '{}',
  observed_at TEXT NOT NULL,
  UNIQUE(provider,run_id,attempted_owner_type,attempted_owner_id)
);
CREATE TABLE IF NOT EXISTS post_aliases (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  entity_id INTEGER REFERENCES entities(id),
  canonical_post_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  alias_type TEXT NOT NULL,
  alias_value TEXT NOT NULL,
  normalized_url TEXT,
  source_url TEXT,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  UNIQUE(profile_id,alias_type,alias_value)
);
CREATE TABLE IF NOT EXISTS media_aliases (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  entity_id INTEGER REFERENCES entities(id),
  media_id INTEGER REFERENCES media(id),
  canonical_media_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  alias_type TEXT NOT NULL,
  alias_value TEXT NOT NULL,
  source_url TEXT,
  width INTEGER,
  height INTEGER,
  mime_type TEXT,
  sha256 TEXT,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  UNIQUE(profile_id,alias_type,alias_value)
);
CREATE TABLE IF NOT EXISTS post_media_coverage (
  id INTEGER PRIMARY KEY,
  epoch_id INTEGER NOT NULL REFERENCES capture_epochs(id),
  post_entity_id INTEGER NOT NULL REFERENCES entities(id),
  surface TEXT NOT NULL DEFAULT 'post_album',
  status TEXT NOT NULL DEFAULT 'pending',
  input_cursor TEXT,
  output_cursor TEXT,
  resume_url TEXT,
  expected_count INTEGER,
  seen_count INTEGER NOT NULL DEFAULT 0,
  seen_media_ids_json TEXT NOT NULL DEFAULT '[]',
  provider_checkpoint_json TEXT NOT NULL DEFAULT '{}',
  terminal_evidence_json TEXT NOT NULL DEFAULT '{}',
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(epoch_id,post_entity_id,surface)
);
CREATE TABLE IF NOT EXISTS browser_limits (
  id INTEGER PRIMARY KEY,
  browser_identity TEXT NOT NULL DEFAULT 'default',
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL DEFAULT '',
  breaker_state TEXT NOT NULL DEFAULT 'closed',
  breaker_reason TEXT,
  blocked_until TEXT,
  half_open_claimed_at TEXT,
  next_allowed_at TEXT,
  daily_date TEXT,
  daily_batches INTEGER NOT NULL DEFAULT 0,
  window_started_at TEXT,
  window_operations INTEGER NOT NULL DEFAULT 0,
  repeat_window_started_at TEXT,
  repeat_count INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  UNIQUE(browser_identity,scope_type,scope_id)
);
CREATE TABLE IF NOT EXISTS browser_evidence (
  id INTEGER PRIMARY KEY,
  evidence_key TEXT NOT NULL UNIQUE,
  browser_identity TEXT NOT NULL DEFAULT 'default',
  profile_id INTEGER REFERENCES profiles(id),
  access_observation_id INTEGER REFERENCES access_observations(id),
  event_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  path TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  mime_type TEXT NOT NULL DEFAULT 'image/webp',
  size_bytes INTEGER NOT NULL DEFAULT 0,
  width INTEGER,
  height INTEGER,
  captured_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  closed_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  cleanup_error TEXT
);
CREATE TABLE IF NOT EXISTS profile_name_candidates (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  candidate_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  source TEXT NOT NULL,
  auth_scope TEXT NOT NULL DEFAULT 'unknown',
  trust_level INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  is_current INTEGER NOT NULL DEFAULT 0 CHECK(is_current IN (0,1)),
  manual_locked INTEGER NOT NULL DEFAULT 0 CHECK(manual_locked IN (0,1)),
  rejection_reason TEXT,
  access_observation_id INTEGER REFERENCES access_observations(id),
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  observation_count INTEGER NOT NULL DEFAULT 1,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  UNIQUE(profile_id,normalized_name,source)
);
CREATE TABLE IF NOT EXISTS profile_source_controls (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  source TEXT NOT NULL,
  frozen INTEGER NOT NULL DEFAULT 0 CHECK(frozen IN (0,1)),
  reason TEXT,
  frozen_at TEXT,
  unfrozen_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL,
  UNIQUE(profile_id,source)
);
CREATE TABLE IF NOT EXISTS large_media_approvals (
  id INTEGER PRIMARY KEY,
  profile_id INTEGER NOT NULL REFERENCES profiles(id),
  entity_id INTEGER REFERENCES entities(id),
  canonical_media_id TEXT NOT NULL,
  source_url TEXT NOT NULL,
  declared_size_bytes INTEGER,
  thumbnail_url TEXT,
  status TEXT NOT NULL DEFAULT 'awaiting_approval',
  requested_at TEXT NOT NULL,
  decided_at TEXT,
  decision_note TEXT,
  UNIQUE(profile_id,canonical_media_id)
);
CREATE TABLE IF NOT EXISTS maintenance_runs (
  id INTEGER PRIMARY KEY, task_name TEXT NOT NULL, status TEXT NOT NULL,
  summary_json TEXT NOT NULL DEFAULT '{}', error TEXT, started_at TEXT NOT NULL, finished_at TEXT
);
CREATE TABLE IF NOT EXISTS storage_snapshots (
  snapshot_date TEXT PRIMARY KEY, captured_at TEXT NOT NULL,
  image_bytes INTEGER NOT NULL DEFAULT 0, video_bytes INTEGER NOT NULL DEFAULT 0,
  attachment_bytes INTEGER NOT NULL DEFAULT 0, database_bytes INTEGER NOT NULL DEFAULT 0,
  content_bytes INTEGER NOT NULL DEFAULT 0, cache_bytes INTEGER NOT NULL DEFAULT 0,
  browser_bytes INTEGER NOT NULL DEFAULT 0, other_bytes INTEGER NOT NULL DEFAULT 0,
  filesystem_used_bytes INTEGER NOT NULL DEFAULT 0, filesystem_total_bytes INTEGER NOT NULL DEFAULT 0,
  filesystem_free_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_entities_profile_kind ON entities(profile_id, kind, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(status, available_at, priority);
CREATE INDEX IF NOT EXISTS idx_outbox_queue ON outbox(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_notification_groups_pending ON notification_groups(entity_id,status,ready_at);
CREATE INDEX IF NOT EXISTS idx_actor_runs_profile ON actor_runs(profile_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_access_observations_profile ON access_observations(profile_id,observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_contracts_lookup ON actor_contracts(provider,actor_id,purpose,status,expires_at);
CREATE INDEX IF NOT EXISTS idx_contract_allocations_grant ON contract_test_allocations(grant_id,id);
CREATE INDEX IF NOT EXISTS idx_coverage_epoch_status ON coverage_streams(epoch_id,status,stream,surface);
CREATE INDEX IF NOT EXISTS idx_profile_photo_captures_latest
  ON profile_photo_captures(profile_id,generation DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_paid_photo_batches_capture
  ON paid_photo_batches(photo_capture_id,status,updated_at,id);
CREATE INDEX IF NOT EXISTS idx_paid_batches_status ON paid_source_batches(status,updated_at);
CREATE INDEX IF NOT EXISTS idx_paid_batches_epoch ON paid_source_batches(epoch_id,coverage_stream_id,id);
CREATE INDEX IF NOT EXISTS idx_paid_access_probe_profile
  ON paid_access_probe_batches(profile_id,status,updated_at,id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_paid_access_probe_window
  ON paid_access_probe_batches(profile_id,observation_window);
CREATE INDEX IF NOT EXISTS idx_post_aliases_canonical ON post_aliases(profile_id,canonical_post_id);
CREATE INDEX IF NOT EXISTS idx_media_aliases_canonical ON media_aliases(profile_id,canonical_media_id);
CREATE INDEX IF NOT EXISTS idx_browser_evidence_retention ON browser_evidence(expires_at,captured_at,id);
CREATE INDEX IF NOT EXISTS idx_name_candidates_profile ON profile_name_candidates(profile_id,status,trust_level DESC);
CREATE INDEX IF NOT EXISTS idx_source_controls_profile ON profile_source_controls(profile_id,source,frozen);
"""


def utcnow() -> str:
    return datetime.now(UTC).isoformat()


def canonical_request_hash(payload: dict[str, Any]) -> str:
    """Return the stable identity used to prevent duplicate paid requests."""
    # Keep DB-side idempotency identical to the capture engine's NFC and
    # numeric normalization.  The lazy import avoids making db.py a model hub.
    from .capture_v2 import canonical_input_json

    serialized = canonical_input_json(payload)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _provider_evidence_text(value: Any) -> str:
    return str(value or "").strip()


def _provider_evidence_present(*records: dict[str, Any]) -> bool:
    """Whether any durable row proves that a provider launch may exist."""

    for record in records:
        if any(
            _provider_evidence_text(record.get(field))
            for field in ("run_id", "dataset_id", "key_value_store_id")
        ):
            return True
        if float(record.get("charged_usd") or 0) > 0:
            return True
    return False


def _resolve_provider_attach(
    *records: dict[str, Any],
    run_id: str,
    dataset_id: str | None = None,
    key_value_store_id: str | None = None,
) -> tuple[str, str | None, str | None]:
    """Merge operator evidence without overwriting persisted identities."""

    supplied = {
        "run_id": _provider_evidence_text(run_id),
        "dataset_id": _provider_evidence_text(dataset_id),
        "key_value_store_id": _provider_evidence_text(key_value_store_id),
    }
    if not supplied["run_id"]:
        raise ValueError("provider run_id is required")
    resolved: dict[str, str | None] = {}
    for field in ("run_id", "dataset_id", "key_value_store_id"):
        existing = {
            _provider_evidence_text(record.get(field))
            for record in records
            if _provider_evidence_text(record.get(field))
        }
        if len(existing) > 1:
            raise RuntimeError(f"既有 provider {field} 證據彼此衝突；禁止覆寫")
        current = next(iter(existing), "")
        incoming = supplied[field]
        if current and incoming and current != incoming:
            raise RuntimeError(f"既有 provider {field} 與輸入不符；禁止覆寫")
        resolved[field] = current or incoming or None
    return (
        str(resolved["run_id"]),
        resolved["dataset_id"],
        resolved["key_value_store_id"],
    )


class ProviderRunOwnershipConflict(RuntimeError):
    """A provider run is already bound to another durable paid identity."""

    def __init__(
        self,
        *,
        provider: str,
        run_id: str,
        attempted_owner_type: str,
        attempted_owner_id: int,
        existing_provider: str,
        existing_run_id: str,
        existing_owner_type: str,
        existing_owner_id: int,
        reason: str,
    ) -> None:
        self.provider = provider
        self.run_id = run_id
        self.attempted_owner_type = attempted_owner_type
        self.attempted_owner_id = attempted_owner_id
        self.existing_provider = existing_provider
        self.existing_run_id = existing_run_id
        self.existing_owner_type = existing_owner_type
        self.existing_owner_id = existing_owner_id
        super().__init__(reason)


def _claim_provider_run_ownership(
    conn: sqlite3.Connection,
    *,
    provider: str,
    run_id: str,
    owner_type: str,
    owner_id: int,
    actor_id: str = "",
    request_fingerprint: str = "",
    provider_started_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Atomically bind one provider run to exactly one durable paid owner.

    The registry deliberately stores the Actor, request fingerprint and start
    time even though manual reconciliation cannot validate those fields yet.
    A future provider-metadata verifier can therefore extend this boundary
    without changing ownership semantics.
    """

    normalized_provider = str(provider or "").strip().casefold()
    normalized_run_id = str(run_id or "").strip()
    normalized_owner_type = str(owner_type or "").strip()
    normalized_owner_id = int(owner_id)
    allowed_owner_types = {
        "paid_source_batch",
        "paid_photo_batch",
        "paid_access_probe_batch",
        "contract_run",
    }
    if not normalized_provider or not normalized_run_id:
        raise ValueError("provider and run_id are required for ownership")
    if normalized_owner_type not in allowed_owner_types:
        raise ValueError(f"unsupported provider run owner: {normalized_owner_type}")

    run_owner = conn.execute(
        "SELECT * FROM provider_run_registry WHERE provider=? AND run_id=?",
        (normalized_provider, normalized_run_id),
    ).fetchone()
    if run_owner is not None and (
        str(run_owner["owner_type"]) != normalized_owner_type
        or int(run_owner["owner_id"]) != normalized_owner_id
    ):
        raise ProviderRunOwnershipConflict(
            provider=normalized_provider,
            run_id=normalized_run_id,
            attempted_owner_type=normalized_owner_type,
            attempted_owner_id=normalized_owner_id,
            existing_provider=str(run_owner["provider"]),
            existing_run_id=str(run_owner["run_id"]),
            existing_owner_type=str(run_owner["owner_type"]),
            existing_owner_id=int(run_owner["owner_id"]),
            reason=(
                f"provider run {normalized_provider}/{normalized_run_id} already belongs to "
                f"{run_owner['owner_type']} {run_owner['owner_id']}"
            ),
        )

    owner_run = conn.execute(
        "SELECT * FROM provider_run_registry WHERE owner_type=? AND owner_id=?",
        (normalized_owner_type, normalized_owner_id),
    ).fetchone()
    if owner_run is not None and (
        str(owner_run["provider"]) != normalized_provider
        or str(owner_run["run_id"]) != normalized_run_id
    ):
        raise ProviderRunOwnershipConflict(
            provider=normalized_provider,
            run_id=normalized_run_id,
            attempted_owner_type=normalized_owner_type,
            attempted_owner_id=normalized_owner_id,
            existing_provider=str(owner_run["provider"]),
            existing_run_id=str(owner_run["run_id"]),
            existing_owner_type=str(owner_run["owner_type"]),
            existing_owner_id=int(owner_run["owner_id"]),
            reason=(
                f"{normalized_owner_type} {normalized_owner_id} already owns provider run "
                f"{owner_run['provider']}/{owner_run['run_id']}"
            ),
        )

    normalized_actor_id = str(actor_id or "").strip()
    normalized_fingerprint = str(request_fingerprint or "").strip()
    if run_owner is not None:
        existing_actor = str(run_owner["actor_id"] or "").strip()
        existing_fingerprint = str(run_owner["request_fingerprint"] or "").strip()
        if existing_actor and normalized_actor_id and existing_actor != normalized_actor_id:
            raise ProviderRunOwnershipConflict(
                provider=normalized_provider,
                run_id=normalized_run_id,
                attempted_owner_type=normalized_owner_type,
                attempted_owner_id=normalized_owner_id,
                existing_provider=normalized_provider,
                existing_run_id=normalized_run_id,
                existing_owner_type=normalized_owner_type,
                existing_owner_id=normalized_owner_id,
                reason="provider run Actor identity conflicts with its existing owner",
            )
        if (
            existing_fingerprint
            and normalized_fingerprint
            and existing_fingerprint != normalized_fingerprint
        ):
            raise ProviderRunOwnershipConflict(
                provider=normalized_provider,
                run_id=normalized_run_id,
                attempted_owner_type=normalized_owner_type,
                attempted_owner_id=normalized_owner_id,
                existing_provider=normalized_provider,
                existing_run_id=normalized_run_id,
                existing_owner_type=normalized_owner_type,
                existing_owner_id=normalized_owner_id,
                reason="provider run request fingerprint conflicts with its existing owner",
            )

    now = utcnow()
    if run_owner is None:
        conn.execute(
            """INSERT INTO provider_run_registry(
              provider,run_id,owner_type,owner_id,actor_id,request_fingerprint,
              provider_started_at,metadata_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                normalized_provider,
                normalized_run_id,
                normalized_owner_type,
                normalized_owner_id,
                normalized_actor_id,
                normalized_fingerprint,
                provider_started_at,
                json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                now,
                now,
            ),
        )
        return

    conn.execute(
        """UPDATE provider_run_registry
        SET actor_id=CASE WHEN actor_id='' THEN ? ELSE actor_id END,
            request_fingerprint=CASE WHEN request_fingerprint='' THEN ? ELSE request_fingerprint END,
            provider_started_at=COALESCE(provider_started_at,?),updated_at=?
        WHERE provider=? AND run_id=? AND owner_type=? AND owner_id=?""",
        (
            normalized_actor_id,
            normalized_fingerprint,
            provider_started_at,
            now,
            normalized_provider,
            normalized_run_id,
            normalized_owner_type,
            normalized_owner_id,
        ),
    )


def _transition_provider_run_id(
    current_run_id: Any, fields: dict[str, Any]
) -> str:
    """Return an incoming run ID while forbidding identity removal/replacement."""

    if "run_id" not in fields:
        return ""
    current = _provider_evidence_text(current_run_id)
    incoming = _provider_evidence_text(fields.get("run_id"))
    if current and incoming != current:
        raise RuntimeError("persisted provider run_id cannot be cleared or replaced")
    return incoming


def _provider_run_ownership_evidence(
    conn: sqlite3.Connection, owner_type: str, owner_id: int
) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT provider,run_id,actor_id,request_fingerprint,provider_started_at
        FROM provider_run_registry WHERE owner_type=? AND owner_id=?""",
        (owner_type, owner_id),
    ).fetchone()
    if row is not None:
        return dict(row)
    conflict = conn.execute(
        """SELECT provider,run_id,actor_id,request_fingerprint,observed_at AS provider_started_at
        FROM provider_run_conflicts
        WHERE attempted_owner_type=? AND attempted_owner_id=?
        ORDER BY id DESC LIMIT 1""",
        (owner_type, owner_id),
    ).fetchone()
    return dict(conflict) if conflict is not None else None


def _normalized_name(value: str) -> str:
    return " ".join(value.split()).casefold()


def _hash_distance(left: str, right: str) -> int:
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except ValueError:
        return 999


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._local = threading.local()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        """Create the schema and apply idempotent forward migrations.

        Keep this public so entry points can explicitly repair a database copied
        from an older release before serving queries.
        """
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            # Lightweight forward migrations for existing installations.
            columns = {row[1] for row in conn.execute("PRAGMA table_info(profiles)")}
            for name in ("audit_cursor", "audit_token", "display_name", "profile_details_json", "serp_last_checked_at"):
                if name not in columns:
                    conn.execute(f"ALTER TABLE profiles ADD COLUMN {name} TEXT")
            if "browser_canary_last_attempt_at" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN browser_canary_last_attempt_at TEXT")
            if "browser_post_cursor" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN browser_post_cursor TEXT")
            if "browser_post_backfill_done" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN browser_post_backfill_done INTEGER NOT NULL DEFAULT 0")
            # Older browser-cursor builds treated an empty initial DOM page as
            # a completed history.  Completion without a persistent cursor is
            # unverifiable, so safely reopen those profiles for another pass.
            conn.execute(
                "UPDATE profiles SET browser_post_backfill_done=0 "
                "WHERE browser_post_cursor IS NULL AND browser_post_backfill_done=1"
            )
            if "sort_order" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN sort_order INTEGER")
            if "last_manual_visit_at" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN last_manual_visit_at TEXT")
            if "apify_frozen" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN apify_frozen INTEGER NOT NULL DEFAULT 0")
            if "apify_posts_blocked_until" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN apify_posts_blocked_until TEXT")
            if "apify_posts_unparsed_streak" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN apify_posts_unparsed_streak INTEGER NOT NULL DEFAULT 0")
            conn.execute("UPDATE profiles SET sort_order=id WHERE sort_order IS NULL")
            entity_columns = {row[1] for row in conn.execute("PRAGMA table_info(entities)")}
            if "dedupe_key" not in entity_columns:
                conn.execute("ALTER TABLE entities ADD COLUMN dedupe_key TEXT")
            if "notification_hash" not in entity_columns:
                conn.execute("ALTER TABLE entities ADD COLUMN notification_hash TEXT")
            for name in (
                "source_scope",
                "source_collector",
                "source_run_id",
                "source_viewer_scope_hash",
            ):
                if name not in entity_columns:
                    conn.execute(f"ALTER TABLE entities ADD COLUMN {name} TEXT")
            media_columns = {row[1] for row in conn.execute("PRAGMA table_info(entity_media)")}
            if "discovery_path" not in media_columns:
                conn.execute("ALTER TABLE entity_media ADD COLUMN discovery_path TEXT")
            if "position" not in media_columns:
                conn.execute("ALTER TABLE entity_media ADD COLUMN position INTEGER")
            event_columns = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
            if "notification_group_id" not in event_columns:
                conn.execute("ALTER TABLE events ADD COLUMN notification_group_id INTEGER")
            outbox_columns = {row[1] for row in conn.execute("PRAGMA table_info(outbox)")}
            media_table_columns = {row[1] for row in conn.execute("PRAGMA table_info(media)")}
            if "perceptual_hash" not in media_table_columns:
                conn.execute("ALTER TABLE media ADD COLUMN perceptual_hash TEXT")
            for name, definition in (("group_id", "INTEGER"), ("media_sha256", "TEXT"), ("media_perceptual_hash", "TEXT"), ("cancelled_at", "TEXT")):
                if name not in outbox_columns:
                    conn.execute(f"ALTER TABLE outbox ADD COLUMN {name} {definition}")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_media_sha ON outbox(media_sha256,status)")
            actor_run_columns = {row[1] for row in conn.execute("PRAGMA table_info(actor_runs)")}
            if "max_charge_usd" not in actor_run_columns:
                conn.execute(
                    "ALTER TABLE actor_runs "
                    "ADD COLUMN max_charge_usd REAL NOT NULL DEFAULT 0"
                )
            for name in (
                "raw_result_count", "parsed_result_count", "new_result_count",
                "updated_result_count", "duplicate_result_count",
            ):
                if name not in actor_run_columns:
                    conn.execute(f"ALTER TABLE actor_runs ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0")
            job_columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
            for name, definition in (
                ("dedupe_key", "TEXT"),
                ("epoch_id", "INTEGER REFERENCES capture_epochs(id)"),
                ("batch_id", "INTEGER REFERENCES paid_source_batches(id)"),
                ("lease_owner", "TEXT"),
                ("leased_at", "TEXT"),
            ):
                if name not in job_columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
                ON jobs(dedupe_key)
                WHERE dedupe_key IS NOT NULL AND status IN ('pending','running')"""
            )
            conn.execute(
                """UPDATE entity_media AS target
                SET position=(
                    SELECT COUNT(*)-1 FROM entity_media AS prior
                    WHERE prior.entity_id=target.entity_id
                      AND prior.version_id=target.version_id
                      AND prior.rowid<=target.rowid
                )
                WHERE target.position IS NULL"""
            )
            self._ensure_capture_v2_migration(conn)
            paid_source_columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(paid_source_batches)")
            }
            if "max_charge_usd" not in paid_source_columns:
                conn.execute(
                    "ALTER TABLE paid_source_batches "
                    "ADD COLUMN max_charge_usd REAL NOT NULL DEFAULT 0"
                )
            legacy_source_rows = conn.execute(
                """SELECT id,status,charged_usd,normalized_input_json
                FROM paid_source_batches
                WHERE status<>'prepared' AND COALESCE(max_charge_usd,0)<=0"""
            ).fetchall()
            for legacy in legacy_source_rows:
                try:
                    normalized = json.loads(
                        str(legacy["normalized_input_json"] or "{}")
                    )
                except (TypeError, json.JSONDecodeError):
                    normalized = {}
                maximum = max(
                    1,
                    min(
                        50,
                        int(
                            normalized.get("maxPostsPerProfile")
                            or normalized.get("maxPosts")
                            or 50
                        ),
                    ),
                )
                historical_ceiling = max(
                    max(0.0, float(legacy["charged_usd"] or 0)),
                    float(
                        provider_charge_ceiling(
                            maximum * LEGACY_SOURCE_RESULT_PRICE_USD
                        )
                    ),
                )
                conn.execute(
                    "UPDATE paid_source_batches SET max_charge_usd=? WHERE id=?",
                    (historical_ceiling, legacy["id"]),
                )
            self._ensure_contract_test_grant_migration(conn)
            contract_run_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(contract_runs)")
            }
            for name in ("lease_owner", "leased_at"):
                if name not in contract_run_columns:
                    conn.execute(f"ALTER TABLE contract_runs ADD COLUMN {name} TEXT")
            self._ensure_provider_run_registry(conn)
            self._ensure_source_control_triggers(conn)

    def record_provider_run_ownership_conflict(
        self,
        conflict: ProviderRunOwnershipConflict,
        *,
        actor_id: str,
        request_fingerprint: str,
        dataset_id: str | None = None,
        key_value_store_id: str | None = None,
    ) -> dict[str, Any]:
        """Fail closed after a provider start returned an already-owned run.

        This is intentionally a second transaction: the ownership claim and
        owner update rolled back together.  The attempted run is stored only
        as conflict evidence, never in the losing owner's ``run_id`` column.
        """

        table_by_owner = {
            "paid_source_batch": "paid_source_batches",
            "paid_photo_batch": "paid_photo_batches",
            "paid_access_probe_batch": "paid_access_probe_batches",
            "contract_run": "contract_runs",
        }
        table = table_by_owner.get(conflict.attempted_owner_type)
        if table is None:
            raise ValueError(
                f"unsupported provider run owner: {conflict.attempted_owner_type}"
            )
        now = utcnow()
        evidence = {
            "dataset_id": str(dataset_id or ""),
            "key_value_store_id": str(key_value_store_id or ""),
            "existing_provider": conflict.existing_provider,
            "existing_run_id": conflict.existing_run_id,
            "existing_owner_type": conflict.existing_owner_type,
            "existing_owner_id": conflict.existing_owner_id,
            "error": str(conflict),
        }
        error = (
            "provider start succeeded but run ownership conflicted; "
            f"attempted={conflict.provider}/{conflict.run_id}; "
            f"existing={conflict.existing_owner_type}:{conflict.existing_owner_id}"
        )
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                f"SELECT * FROM {table} WHERE id=?",
                (conflict.attempted_owner_id,),
            ).fetchone()
            if current is None:
                raise ValueError(
                    f"unknown {conflict.attempted_owner_type}: "
                    f"{conflict.attempted_owner_id}"
                )
            if _provider_evidence_text(current["run_id"]):
                raise RuntimeError(
                    "losing provider owner unexpectedly persisted a run_id"
                )
            conn.execute(
                """INSERT INTO provider_run_conflicts(
                  provider,run_id,attempted_owner_type,attempted_owner_id,
                  existing_provider,existing_run_id,existing_owner_type,
                  existing_owner_id,actor_id,request_fingerprint,evidence_json,observed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(provider,run_id,attempted_owner_type,attempted_owner_id)
                DO UPDATE SET evidence_json=excluded.evidence_json,
                              observed_at=excluded.observed_at""",
                (
                    conflict.provider,
                    conflict.run_id,
                    conflict.attempted_owner_type,
                    conflict.attempted_owner_id,
                    conflict.existing_provider,
                    conflict.existing_run_id,
                    conflict.existing_owner_type,
                    conflict.existing_owner_id,
                    str(actor_id or ""),
                    str(request_fingerprint or ""),
                    json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            if conflict.attempted_owner_type == "contract_run":
                cursor = conn.execute(
                    """UPDATE contract_runs
                    SET status='needs_reconcile',error=?,finished_at=?
                    WHERE id=? AND status='launching' AND run_id IS NULL""",
                    (error, now, conflict.attempted_owner_id),
                )
            else:
                cursor = conn.execute(
                    f"""UPDATE {table} SET status='needs_reconcile',error=?,updated_at=?
                    WHERE id=? AND status='launching' AND run_id IS NULL""",
                    (error, now, conflict.attempted_owner_id),
                )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"{conflict.attempted_owner_type} was not at the launch boundary"
                )
            if conflict.attempted_owner_type == "paid_source_batch":
                conn.execute(
                    """UPDATE capture_epochs SET status='needs_reconcile',updated_at=?
                    WHERE id=?""",
                    (now, current["epoch_id"]),
                )
            diagnostic_id = int(current["actor_run_id"] or 0) if (
                conflict.attempted_owner_type
                in {"paid_photo_batch", "paid_access_probe_batch"}
            ) else 0
            if diagnostic_id:
                conn.execute(
                    """UPDATE actor_runs
                    SET status='needs_reconcile',run_id=?,error=?,finished_at=?
                    WHERE id=?""",
                    (conflict.run_id, error, now, diagnostic_id),
                )
            stored = conn.execute(
                """SELECT * FROM provider_run_conflicts
                WHERE provider=? AND run_id=? AND attempted_owner_type=?
                  AND attempted_owner_id=?""",
                (
                    conflict.provider,
                    conflict.run_id,
                    conflict.attempted_owner_type,
                    conflict.attempted_owner_id,
                ),
            ).fetchone()
            return dict(stored)

    @staticmethod
    def _ensure_provider_run_registry(conn: sqlite3.Connection) -> None:
        """Backfill ownership for provider runs written by earlier builds.

        Historical collisions fail startup instead of silently selecting one
        owner: both rows may still be resumable, so choosing a winner without
        quarantining the other would leave the ownership invariant false.
        """

        rows = conn.execute(
            """SELECT provider,run_id,'paid_source_batch' AS owner_type,id AS owner_id,
                      actor_id,request_hash AS request_fingerprint,launched_at AS provider_started_at,
                      1 AS owner_order
               FROM paid_source_batches WHERE NULLIF(TRIM(run_id),'') IS NOT NULL
               UNION ALL
               SELECT 'apify',run_id,'paid_photo_batch',id,actor_id,request_hash,launched_at,2
               FROM paid_photo_batches WHERE NULLIF(TRIM(run_id),'') IS NOT NULL
               UNION ALL
               SELECT provider,run_id,'paid_access_probe_batch',id,actor_id,request_hash,launched_at,3
               FROM paid_access_probe_batches WHERE NULLIF(TRIM(run_id),'') IS NOT NULL
               UNION ALL
               SELECT ac.provider,cr.run_id,'contract_run',cr.id,ac.actor_id,
                      cr.request_hash,cr.leased_at,4
               FROM contract_runs cr
               JOIN actor_contracts ac ON ac.id=cr.contract_id
               WHERE NULLIF(TRIM(cr.run_id),'') IS NOT NULL
               ORDER BY owner_order,owner_id"""
        ).fetchall()
        for row in rows:
            _claim_provider_run_ownership(
                conn,
                provider=str(row["provider"]),
                run_id=str(row["run_id"]),
                owner_type=str(row["owner_type"]),
                owner_id=int(row["owner_id"]),
                actor_id=str(row["actor_id"] or ""),
                request_fingerprint=str(row["request_fingerprint"] or ""),
                provider_started_at=row["provider_started_at"],
                metadata={"backfilled": True},
            )

    def _ensure_capture_v2_migration(self, conn: sqlite3.Connection) -> None:
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (CAPTURE_V2_SCHEMA_MIGRATION,),
        ).fetchone()
        if applied:
            return

        now = utcnow()
        conn.execute(
            """INSERT INTO profile_source_controls(
              profile_id,source,frozen,reason,frozen_at,unfrozen_at,updated_at
            )
            SELECT id,'apify',COALESCE(apify_frozen,0),
                   CASE WHEN COALESCE(apify_frozen,0)=1 THEN 'legacy_profile_flag' END,
                   CASE WHEN COALESCE(apify_frozen,0)=1 THEN ? END,
                   CASE WHEN COALESCE(apify_frozen,0)=0 THEN ? END,?
            FROM profiles
            WHERE 1
            ON CONFLICT(profile_id,source) DO NOTHING""",
            (now, now, now),
        )

        for profile in conn.execute(
            "SELECT id,name,display_name,profile_details_json FROM profiles ORDER BY id"
        ).fetchall():
            profile_id = int(profile["id"])
            current_name = str(profile["display_name"] or "").strip()
            if current_name:
                conn.execute(
                    """INSERT INTO profile_name_candidates(
                      profile_id,candidate_name,normalized_name,source,auth_scope,trust_level,
                      status,is_current,first_seen_at,last_seen_at,metadata_json
                    ) VALUES(?,?,?,'legacy_display_name','legacy',70,'accepted',1,?,?,?)
                    ON CONFLICT(profile_id,normalized_name,source) DO NOTHING""",
                    (
                        profile_id,
                        current_name,
                        _normalized_name(current_name),
                        now,
                        now,
                        json.dumps({"seeded_from": "profiles.display_name"}, ensure_ascii=False),
                    ),
                )
            try:
                details = json.loads(str(profile["profile_details_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                details = {}
            rejected = details.get("rejected_profile_names", []) if isinstance(details, dict) else []
            if not isinstance(rejected, list):
                rejected = []
            for value in rejected:
                rejected_name = str(value).strip()
                if not rejected_name:
                    continue
                conn.execute(
                    """INSERT INTO profile_name_candidates(
                      profile_id,candidate_name,normalized_name,source,auth_scope,trust_level,
                      status,is_current,rejection_reason,first_seen_at,last_seen_at,metadata_json
                    ) VALUES(?,?,?,'legacy_rejected','legacy',0,'rejected',0,
                             'legacy rejected_profile_names',?,?,?)
                    ON CONFLICT(profile_id,normalized_name,source) DO NOTHING""",
                    (
                        profile_id,
                        rejected_name,
                        _normalized_name(rejected_name),
                        now,
                        now,
                        json.dumps({"seeded_from": "profile_details_json"}, ensure_ascii=False),
                    ),
                )

        conn.execute(
            "INSERT INTO schema_migrations(name,applied_at,details_json) VALUES(?,?,?)",
            (
                CAPTURE_V2_SCHEMA_MIGRATION,
                now,
                json.dumps({"kind": "additive", "starts_jobs": False}, ensure_ascii=False),
            ),
        )

    def _ensure_contract_test_grant_migration(self, conn: sqlite3.Connection) -> None:
        """Add the operator-authorized, global contract-test spending ledger.

        This remains a separate additive migration because Capture V2 schema v1
        may already be recorded on an OCI database.  Re-running startup is safe.
        """
        applied = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?",
            (CONTRACT_TEST_GRANT_MIGRATION,),
        ).fetchone()
        if applied:
            return
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS contract_test_grants (
              id INTEGER PRIMARY KEY,
              grant_key TEXT NOT NULL UNIQUE,
              purpose TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'active',
              max_usd REAL NOT NULL CHECK(max_usd>0),
              authorized_by TEXT NOT NULL,
              note TEXT,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              closed_at TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_contract_test_one_active_grant
              ON contract_test_grants(purpose) WHERE status='active';
            CREATE TABLE IF NOT EXISTS contract_test_allocations (
              id INTEGER PRIMARY KEY,
              grant_id INTEGER NOT NULL REFERENCES contract_test_grants(id),
              profile_id INTEGER NOT NULL REFERENCES profiles(id),
              actor_id TEXT NOT NULL,
              schema_fingerprint TEXT NOT NULL,
              test_generation TEXT NOT NULL UNIQUE,
              authorized_usd REAL NOT NULL CHECK(authorized_usd>0),
              job_id INTEGER REFERENCES jobs(id),
              created_at TEXT NOT NULL,
              UNIQUE(grant_id,actor_id,schema_fingerprint)
            );
            CREATE INDEX IF NOT EXISTS idx_contract_allocations_grant
              ON contract_test_allocations(grant_id,id);
            """
        )
        run_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(contract_runs)")
        }
        if "grant_allocation_id" not in run_columns:
            conn.execute(
                "ALTER TABLE contract_runs ADD COLUMN grant_allocation_id INTEGER "
                "REFERENCES contract_test_allocations(id)"
            )
        if "authorized_max_usd" not in run_columns:
            conn.execute(
                "ALTER TABLE contract_runs ADD COLUMN authorized_max_usd REAL NOT NULL DEFAULT 0"
            )
        # Jobs queued by pre-ledger builds have no operator grant.  A pending
        # one is safe to cancel; a formerly running one is ambiguous and must
        # be reconciled instead of silently retried after restart.
        now = utcnow()
        conn.execute(
            """UPDATE jobs SET status='cancelled',error=?,finished_at=?
            WHERE job_type='contract_test_posts_v2' AND status='pending'
              AND NOT EXISTS(
                SELECT 1 FROM contract_test_allocations a WHERE a.job_id=jobs.id
              )""",
            ("Capture V2 契約測試需重新明確核准全域 grant", now),
        )
        conn.execute(
            """UPDATE jobs SET status='needs_reconcile',error=?,finished_at=?
            WHERE job_type='contract_test_posts_v2' AND status='running'
              AND NOT EXISTS(
                SELECT 1 FROM contract_test_allocations a WHERE a.job_id=jobs.id
              )""",
            ("Capture V2 舊版契約測試無 grant ledger；需人工 reconcile", now),
        )
        conn.execute(
            "INSERT INTO schema_migrations(name,applied_at,details_json) VALUES(?,?,?)",
            (
                CONTRACT_TEST_GRANT_MIGRATION,
                utcnow(),
                json.dumps(
                    {
                        "tables": ["contract_test_grants", "contract_test_allocations"],
                        "contract_run_columns": [
                            "grant_allocation_id",
                            "authorized_max_usd",
                        ],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )

    @staticmethod
    def _ensure_source_control_triggers(conn: sqlite3.Connection) -> None:
        # V1 code still writes profiles.apify_frozen directly.  Keep the new
        # per-source control synchronized until all callers have migrated.
        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS trg_profiles_apify_control_insert
            AFTER INSERT ON profiles
            BEGIN
              INSERT INTO profile_source_controls(
                profile_id,source,frozen,reason,frozen_at,unfrozen_at,updated_at
              ) VALUES(
                NEW.id,'apify',COALESCE(NEW.apify_frozen,0),
                CASE WHEN COALESCE(NEW.apify_frozen,0)=1 THEN 'legacy_profile_flag' END,
                CASE WHEN COALESCE(NEW.apify_frozen,0)=1 THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') END,
                CASE WHEN COALESCE(NEW.apify_frozen,0)=0 THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') END,
                strftime('%Y-%m-%dT%H:%M:%fZ','now')
              )
              ON CONFLICT(profile_id,source) DO UPDATE SET
                frozen=excluded.frozen,
                reason=excluded.reason,
                frozen_at=CASE WHEN excluded.frozen=1 THEN excluded.updated_at ELSE profile_source_controls.frozen_at END,
                unfrozen_at=CASE WHEN excluded.frozen=0 THEN excluded.updated_at ELSE profile_source_controls.unfrozen_at END,
                updated_at=excluded.updated_at;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_profiles_apify_control_update
            AFTER UPDATE OF apify_frozen ON profiles
            WHEN NOT EXISTS(
              SELECT 1 FROM profile_source_controls
              WHERE profile_id=NEW.id AND source='apify' AND frozen=COALESCE(NEW.apify_frozen,0)
            )
            BEGIN
              INSERT INTO profile_source_controls(
                profile_id,source,frozen,reason,frozen_at,unfrozen_at,updated_at
              ) VALUES(
                NEW.id,'apify',COALESCE(NEW.apify_frozen,0),
                CASE WHEN COALESCE(NEW.apify_frozen,0)=1 THEN 'legacy_profile_flag' END,
                CASE WHEN COALESCE(NEW.apify_frozen,0)=1 THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') END,
                CASE WHEN COALESCE(NEW.apify_frozen,0)=0 THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') END,
                strftime('%Y-%m-%dT%H:%M:%fZ','now')
              )
              ON CONFLICT(profile_id,source) DO UPDATE SET
                frozen=excluded.frozen,
                reason=excluded.reason,
                frozen_at=CASE WHEN excluded.frozen=1 THEN excluded.updated_at ELSE profile_source_controls.frozen_at END,
                unfrozen_at=CASE WHEN excluded.frozen=0 THEN excluded.updated_at ELSE profile_source_controls.unfrozen_at END,
                updated_at=excluded.updated_at;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_apify_control_profiles_update
            AFTER UPDATE OF frozen ON profile_source_controls
            WHEN NEW.source='apify'
              AND EXISTS(
                SELECT 1 FROM profiles
                WHERE id=NEW.profile_id AND COALESCE(apify_frozen,0)<>NEW.frozen
              )
            BEGIN
              UPDATE profiles
              SET apify_frozen=NEW.frozen,
                  updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE id=NEW.profile_id;
            END;
            """
        )

    def has_column(self, table: str, column: str) -> bool:
        if table not in {
            "profiles", "entities", "versions", "media", "entity_media", "events", "outbox", "jobs",
            "usage", "audit_seen", "actor_runs", "schema_migrations", "access_observations",
            "actor_contracts", "contract_runs", "capture_epochs", "coverage_streams", "profile_photo_captures",
            "contract_test_grants", "contract_test_allocations", "paid_photo_batches",
            "paid_source_batches", "paid_access_probe_batches", "post_aliases", "media_aliases", "post_media_coverage",
            "browser_limits", "browser_evidence", "profile_name_candidates", "profile_source_controls",
            "large_media_approvals",
        }:
            return False
        with self.connect() as conn:
            return column in {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def sync_profiles(self, profiles: list[Any]) -> None:
        now = utcnow()
        configured = {p.url.rstrip("/") for p in profiles}
        with self.connect() as conn:
            for p in profiles:
                url = p.url.rstrip("/")
                conn.execute(
                    """INSERT INTO profiles(name,url,enabled,created_at,updated_at) VALUES(?,?,?,?,?)
                    ON CONFLICT(url) DO UPDATE SET name=excluded.name, enabled=excluded.enabled, updated_at=excluded.updated_at""",
                    (p.name, url, int(p.enabled), now, now),
                )
            rows = conn.execute("SELECT id,url FROM profiles").fetchall()
            for row in rows:
                if row["url"] not in configured:
                    conn.execute("UPDATE profiles SET enabled=0,updated_at=? WHERE id=?", (now, row["id"]))
            conn.execute("UPDATE profiles SET sort_order=id WHERE sort_order IS NULL")

    def reorder_profiles(self, profile_ids: list[int]) -> None:
        with self.connect() as conn:
            current = [int(row[0]) for row in conn.execute("SELECT id FROM profiles WHERE enabled=1")]
            if len(profile_ids) != len(current) or len(set(profile_ids)) != len(profile_ids) or set(profile_ids) != set(current):
                raise ValueError("profile order must contain every profile exactly once")
            conn.executemany(
                "UPDATE profiles SET sort_order=? WHERE id=?",
                ((position, profile_id) for position, profile_id in enumerate(profile_ids)),
            )

    def queue_profile_visits(self, profile_ids: list[int]) -> int:
        now = utcnow()
        queued = 0
        with self.connect() as conn:
            for profile_id in dict.fromkeys(profile_ids):
                pending = conn.execute(
                    "SELECT id FROM jobs WHERE profile_id=? AND job_type='visit' AND status='pending' ORDER BY id LIMIT 1",
                    (profile_id,),
                ).fetchone()
                if pending:
                    conn.execute("UPDATE jobs SET priority=MIN(priority,0),available_at=? WHERE id=?", (now, pending["id"]))
                else:
                    conn.execute(
                        "INSERT INTO jobs(profile_id,job_type,priority,payload_json,available_at,created_at) VALUES(?,'visit',0,'{}',?,?)",
                        (profile_id, now, now),
                    )
                queued += 1
        return queued

    def queue_unique_job(
        self,
        *,
        profile_id: int,
        job_type: str,
        priority: int,
        dedupe_key: str,
        payload: dict[str, Any] | None = None,
        available_at: str | None = None,
        epoch_id: int | None = None,
        batch_id: int | None = None,
    ) -> tuple[int, bool]:
        """Queue one active continuation job for a durable capture boundary."""
        dedupe_key = dedupe_key.strip()
        if not dedupe_key:
            raise ValueError("dedupe_key must not be empty")
        now = utcnow()
        available_at = available_at or now
        payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO jobs(
                  profile_id,job_type,priority,status,payload_json,available_at,created_at,
                  dedupe_key,epoch_id,batch_id
                ) VALUES(?,?,?,'pending',?,?,?,?,?,?)""",
                (
                    profile_id,
                    job_type,
                    priority,
                    payload_json,
                    available_at,
                    now,
                    dedupe_key,
                    epoch_id,
                    batch_id,
                ),
            )
            row = conn.execute(
                """SELECT id,profile_id,job_type FROM jobs
                WHERE dedupe_key=? AND status IN ('pending','running') ORDER BY id LIMIT 1""",
                (dedupe_key,),
            ).fetchone()
            if row is None:
                raise RuntimeError("unique job could not be queued")
            if int(row["profile_id"]) != profile_id or str(row["job_type"]) != job_type:
                raise ValueError("dedupe_key already belongs to another active job")
            return int(row["id"]), bool(cursor.rowcount)

    def queue_manual_visit(self, profile_id: int, cooldown_minutes: int = 10) -> tuple[bool, datetime]:
        now = datetime.now(UTC)
        with self.connect() as conn:
            profile = conn.execute("SELECT last_manual_visit_at FROM profiles WHERE id=? AND enabled=1", (profile_id,)).fetchone()
            if not profile:
                raise ValueError("找不到啟用中的監控帳號")
            if profile["last_manual_visit_at"]:
                try:
                    last = datetime.fromisoformat(str(profile["last_manual_visit_at"]))
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=UTC)
                    available = last + timedelta(minutes=cooldown_minutes)
                    if available > now:
                        return False, available
                except ValueError:
                    pass
            now_text = now.isoformat()
            pending = conn.execute(
                "SELECT id FROM jobs WHERE profile_id=? AND job_type='visit' AND status='pending' ORDER BY id LIMIT 1",
                (profile_id,),
            ).fetchone()
            payload = json.dumps({"manual": True}, separators=(",", ":"))
            if pending:
                conn.execute(
                    "UPDATE jobs SET priority=-100,available_at=?,payload_json=? WHERE id=?",
                    (now_text, payload, pending["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO jobs(profile_id,job_type,priority,payload_json,available_at,created_at) VALUES(?,'visit',-100,?,?,?)",
                    (profile_id, payload, now_text, now_text),
                )
            conn.execute("UPDATE profiles SET last_manual_visit_at=?,updated_at=? WHERE id=?", (now_text, now_text, profile_id))
            return True, now + timedelta(minutes=cooldown_minutes)

    def queue_manual_browser_visit(self, profile_id: int, cooldown_minutes: int = 10) -> tuple[bool, datetime]:
        now = datetime.now(UTC)
        with self.connect() as conn:
            profile = conn.execute("SELECT id FROM profiles WHERE id=? AND enabled=1", (profile_id,)).fetchone()
            if not profile:
                raise ValueError("找不到可拜訪的監控帳號")
            latest = conn.execute(
                "SELECT status,created_at FROM jobs WHERE profile_id=? AND job_type='browser_visit' ORDER BY id DESC LIMIT 1",
                (profile_id,),
            ).fetchone()
            if latest:
                try:
                    created = datetime.fromisoformat(str(latest["created_at"]))
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=UTC)
                    available = created + timedelta(minutes=cooldown_minutes)
                    if latest["status"] in {"pending", "running"}:
                        return False, max(available, now + timedelta(minutes=1))
                    if available > now:
                        return False, available
                except ValueError:
                    pass
            now_text = now.isoformat()
            conn.execute(
                "INSERT INTO jobs(profile_id,job_type,priority,payload_json,available_at,created_at) VALUES(?,'browser_visit',-110,'{\"manual\":true}',?,?)",
                (profile_id, now_text, now_text),
            )
            return True, now + timedelta(minutes=cooldown_minutes)

    def rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def row(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.connect() as conn:
            value = conn.execute(sql, params).fetchone()
            return dict(value) if value else None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        with self.connect() as conn:
            cur = conn.execute(sql, params)
            return int(cur.lastrowid or 0)

    def claim_pending_job(
        self,
        job_id: int,
        *,
        lease_owner: str,
        claimed_at: str | None = None,
    ) -> dict[str, Any] | None:
        """Atomically lease one due pending job to exactly one worker.

        Selecting the queue head and later updating it is not sufficient when
        two scheduler processes share the SQLite database.  The status
        predicate is the compare-and-swap gate; ``BEGIN IMMEDIATE`` makes the
        winner and the returned row one durable transaction.
        """

        if not lease_owner:
            raise ValueError("job lease_owner is required")
        claimed_at = claimed_at or utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE jobs SET status='running',started_at=?,attempts=attempts+1,
                lease_owner=?,leased_at=?
                WHERE id=? AND status='pending' AND available_at<=?""",
                (claimed_at, lease_owner, claimed_at, job_id, claimed_at),
            )
            if cursor.rowcount != 1:
                return None
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def record_access_observation(
        self,
        profile_id: int,
        *,
        source: str,
        auth_scope: str,
        verdict: str,
        target_fb_id: str | None = None,
        observed_fb_id: str | None = None,
        identity_match: bool = False,
        evidence_summary: dict[str, Any] | None = None,
        evidence_hash: str | None = None,
        raw_evidence_path: str | None = None,
        observed_at: str | None = None,
        observation_key: str | None = None,
    ) -> dict[str, Any]:
        if verdict not in ACCESS_STATES:
            raise ValueError(f"unsupported access verdict: {verdict}")
        observed_at = observed_at or utcnow()
        summary = evidence_summary or {}
        observation_key = observation_key or canonical_request_hash(
            {
                "profile_id": profile_id,
                "source": source,
                "auth_scope": auth_scope,
                "verdict": verdict,
                "target_fb_id": target_fb_id or "",
                "observed_fb_id": observed_fb_id or "",
                "identity_match": bool(identity_match),
                "evidence_hash": evidence_hash or "",
                "observed_at": observed_at,
            }
        )
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO access_observations(
                  observation_key,profile_id,source,auth_scope,verdict,target_fb_id,observed_fb_id,
                  identity_match,evidence_hash,evidence_summary_json,raw_evidence_path,observed_at,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    observation_key,
                    profile_id,
                    source,
                    auth_scope,
                    verdict,
                    target_fb_id,
                    observed_fb_id,
                    int(identity_match),
                    evidence_hash,
                    json.dumps(summary, ensure_ascii=False, sort_keys=True),
                    raw_evidence_path,
                    observed_at,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM access_observations WHERE observation_key=?", (observation_key,)
            ).fetchone()
            return dict(row)

    def upsert_actor_contract(
        self,
        *,
        provider: str,
        actor_id: str,
        purpose: str,
        build_id: str = "",
        schema_fingerprint: str = "",
        input_mapping_hash: str = "",
        status: str = "pending",
        evidence: dict[str, Any] | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        if status not in CONTRACT_STATUSES:
            raise ValueError(f"unsupported contract status: {status}")
        now = utcnow()
        passed_at = now if status == "passed" else None
        invalidated_at = now if status in {"failed", "expired", "disabled"} else None
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO actor_contracts(
                  provider,actor_id,purpose,build_id,schema_fingerprint,input_mapping_hash,status,
                  passed_at,expires_at,invalidated_at,evidence_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(provider,actor_id,purpose,build_id,schema_fingerprint,input_mapping_hash)
                DO UPDATE SET status=excluded.status,passed_at=excluded.passed_at,
                  expires_at=excluded.expires_at,invalidated_at=excluded.invalidated_at,
                  evidence_json=excluded.evidence_json,updated_at=excluded.updated_at""",
                (
                    provider,
                    actor_id,
                    purpose,
                    build_id,
                    schema_fingerprint,
                    input_mapping_hash,
                    status,
                    passed_at,
                    expires_at,
                    invalidated_at,
                    json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """SELECT * FROM actor_contracts
                WHERE provider=? AND actor_id=? AND purpose=? AND build_id=?
                  AND schema_fingerprint=? AND input_mapping_hash=?""",
                (provider, actor_id, purpose, build_id, schema_fingerprint, input_mapping_hash),
            ).fetchone()
            return dict(row)

    def valid_actor_contract(
        self,
        *,
        provider: str,
        actor_id: str,
        purpose: str,
        at: str | None = None,
    ) -> dict[str, Any] | None:
        at = at or utcnow()
        return self.row(
            """SELECT * FROM actor_contracts
            WHERE provider=? AND actor_id=? AND purpose=? AND status='passed'
              AND invalidated_at IS NULL AND (expires_at IS NULL OR expires_at>?)
            ORDER BY passed_at DESC,id DESC LIMIT 1""",
            (provider, actor_id, purpose, at),
        )

    def invalidate_actor_contract(self, contract_id: int, reason: str) -> dict[str, Any]:
        """Fail one exact Actor fingerprint while retaining its test evidence."""

        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM actor_contracts WHERE id=?", (contract_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown actor contract: {contract_id}")
            try:
                evidence = json.loads(str(current["evidence_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                evidence = {}
            if not isinstance(evidence, dict):
                evidence = {}
            evidence["invalidated_reason"] = str(reason)[:4000]
            evidence["invalidated_at"] = now
            conn.execute(
                """UPDATE actor_contracts SET status='failed',invalidated_at=?,
                evidence_json=?,updated_at=? WHERE id=?""",
                (
                    now,
                    json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                    now,
                    contract_id,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM actor_contracts WHERE id=?", (contract_id,)
            ).fetchone()
            return dict(updated)

    @staticmethod
    def _contract_grant_ledger_conn(
        conn: sqlite3.Connection, grant: sqlite3.Row | dict[str, Any]
    ) -> dict[str, Any]:
        """Calculate actual and conservatively at-risk spend for one grant."""
        grant_id = int(grant["id"])
        allocations = conn.execute(
            """SELECT a.*,j.status AS job_status,j.error AS job_error,
            COALESCE(SUM(CASE WHEN cr.charged_usd>0 THEN cr.charged_usd ELSE 0 END),0)
              AS spent_usd,
            COALESCE(SUM(CASE WHEN cr.status IN ('launching','run_started','needs_reconcile')
              THEN MAX(cr.authorized_max_usd-MAX(cr.charged_usd,0),0) ELSE 0 END),0)
              AS ambiguous_usd
            FROM contract_test_allocations a
            LEFT JOIN jobs j ON j.id=a.job_id
            LEFT JOIN contract_runs cr ON cr.grant_allocation_id=a.id
            WHERE a.grant_id=?
            GROUP BY a.id ORDER BY a.id""",
            (grant_id,),
        ).fetchall()
        spent = 0.0
        reserved = 0.0
        decorated: list[dict[str, Any]] = []
        for row in allocations:
            item = dict(row)
            actual = max(0.0, float(item.get("spent_usd") or 0))
            ambiguous = max(0.0, float(item.get("ambiguous_usd") or 0))
            authorized = max(0.0, float(item.get("authorized_usd") or 0))
            job_status = str(item.get("job_status") or "")
            at_risk = (
                max(0.0, authorized - actual)
                if job_status in {"pending", "running"}
                else ambiguous
            )
            spent += actual
            reserved += at_risk
            item["reserved_usd"] = at_risk
            decorated.append(item)
        maximum = max(0.0, float(grant["max_usd"] or 0))
        return {
            **dict(grant),
            "spent_usd": spent,
            "reserved_usd": reserved,
            "remaining_usd": max(0.0, maximum - spent - reserved),
            "allocations": decorated,
        }

    @classmethod
    def _refresh_contract_test_grants_conn(
        cls, conn: sqlite3.Connection, *, purpose: str = "posts_cursor"
    ) -> None:
        now = utcnow()
        conn.execute(
            """UPDATE contract_test_grants SET status='expired',closed_at=?
            WHERE purpose=? AND status='active' AND expires_at<=?""",
            (now, purpose, now),
        )
        rows = conn.execute(
            "SELECT * FROM contract_test_grants WHERE purpose=? AND status='active' ORDER BY id",
            (purpose,),
        ).fetchall()
        for grant in rows:
            passed = conn.execute(
                """SELECT 1 FROM contract_test_allocations a
                JOIN contract_runs cr ON cr.grant_allocation_id=a.id
                JOIN actor_contracts ac ON ac.id=cr.contract_id
                WHERE a.grant_id=? AND ac.status='passed'
                  AND ac.schema_fingerprint=a.schema_fingerprint
                LIMIT 1""",
                (grant["id"],),
            ).fetchone()
            if passed:
                conn.execute(
                    "UPDATE contract_test_grants SET status='fulfilled',closed_at=? WHERE id=?",
                    (now, grant["id"]),
                )
                continue
            ledger = cls._contract_grant_ledger_conn(conn, grant)
            unsettled = any(
                str(item.get("job_status") or "") in {"pending", "running"}
                or float(item.get("reserved_usd") or 0) > 1e-9
                for item in ledger["allocations"]
            )
            if float(ledger["remaining_usd"]) <= 1e-9 and not unsettled:
                conn.execute(
                    "UPDATE contract_test_grants SET status='exhausted',closed_at=? WHERE id=?",
                    (now, grant["id"]),
                )

    def contract_test_grant_ledger(
        self, grant_id: int | None = None, *, purpose: str = "posts_cursor"
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            self._refresh_contract_test_grants_conn(conn, purpose=purpose)
            if grant_id is None:
                grant = conn.execute(
                    """SELECT * FROM contract_test_grants WHERE purpose=?
                    ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END,id DESC LIMIT 1""",
                    (purpose,),
                ).fetchone()
            else:
                grant = conn.execute(
                    "SELECT * FROM contract_test_grants WHERE id=? AND purpose=?",
                    (grant_id, purpose),
                ).fetchone()
            return self._contract_grant_ledger_conn(conn, grant) if grant else None

    def create_contract_test_grant(
        self,
        *,
        purpose: str = "posts_cursor",
        max_usd: float,
        valid_hours: float = 24,
        authorized_by: str = "operator",
        note: str | None = None,
    ) -> dict[str, Any]:
        maximum = float(max_usd)
        if maximum <= 0:
            raise ValueError("contract test grant must be greater than zero")
        if purpose == "posts_cursor" and maximum > 0.20 + 1e-9:
            raise ValueError("posts cursor contract-test grant cannot exceed $0.20")
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        expires_at = (now_dt + timedelta(hours=max(1.0, float(valid_hours)))).isoformat()
        with self.connect() as conn:
            self._refresh_contract_test_grants_conn(conn, purpose=purpose)
            in_flight = conn.execute(
                """SELECT 1 FROM contract_test_allocations a
                JOIN jobs j ON j.id=a.job_id
                JOIN contract_test_grants g ON g.id=a.grant_id
                WHERE g.purpose=? AND j.status IN ('pending','running') LIMIT 1""",
                (purpose,),
            ).fetchone()
            if in_flight:
                raise ValueError(
                    "contract test is pending or running; wait for it before authorizing a new grant"
                )
            if conn.execute(
                """SELECT 1 FROM contract_runs
                WHERE status IN ('launching','run_started','needs_reconcile') LIMIT 1"""
            ).fetchone():
                raise ValueError("contract test has an ambiguous paid run; reconcile it first")
            active = conn.execute(
                "SELECT * FROM contract_test_grants WHERE purpose=? AND status='active' LIMIT 1",
                (purpose,),
            ).fetchone()
            if active:
                raise ValueError("an active contract test grant already exists")
            grant_key = canonical_request_hash(
                {
                    "purpose": purpose,
                    "authorized_at": now,
                    "authorized_by": authorized_by,
                    "max_usd": maximum,
                }
            )
            cursor = conn.execute(
                """INSERT INTO contract_test_grants(
                  grant_key,purpose,status,max_usd,authorized_by,note,created_at,expires_at
                ) VALUES(?,?,'active',?,?,?,?,?)""",
                (grant_key, purpose, maximum, authorized_by, note, now, expires_at),
            )
            grant = conn.execute(
                "SELECT * FROM contract_test_grants WHERE id=?", (cursor.lastrowid,)
            ).fetchone()
            return self._contract_grant_ledger_conn(conn, grant)

    def close_contract_test_grant(
        self, grant_id: int, *, purpose: str = "posts_cursor"
    ) -> dict[str, Any]:
        now = utcnow()
        with self.connect() as conn:
            grant = conn.execute(
                "SELECT * FROM contract_test_grants WHERE id=? AND purpose=?",
                (grant_id, purpose),
            ).fetchone()
            if not grant:
                raise ValueError("contract test grant does not exist")
            in_flight = conn.execute(
                """SELECT 1 FROM contract_test_allocations a
                JOIN jobs j ON j.id=a.job_id
                WHERE a.grant_id=? AND j.status IN ('pending','running') LIMIT 1""",
                (grant_id,),
            ).fetchone()
            if in_flight:
                raise ValueError(
                    "contract test grant has a pending or running job; wait before closing it"
                )
            ambiguous = conn.execute(
                """SELECT 1 FROM contract_runs cr
                JOIN contract_test_allocations a ON a.id=cr.grant_allocation_id
                WHERE a.grant_id=? AND cr.status IN ('launching','run_started','needs_reconcile')
                LIMIT 1""",
                (grant_id,),
            ).fetchone()
            if ambiguous:
                raise ValueError("contract test grant has an ambiguous paid run; reconcile it first")
            if str(grant["status"]) == "active":
                conn.execute(
                    "UPDATE contract_test_grants SET status='closed',closed_at=? WHERE id=?",
                    (now, grant_id),
                )
            refreshed = conn.execute(
                "SELECT * FROM contract_test_grants WHERE id=?", (grant_id,)
            ).fetchone()
            return self._contract_grant_ledger_conn(conn, refreshed)

    def queue_contract_test_job(
        self,
        *,
        grant_id: int,
        profile_id: int,
        actor_id: str,
        schema_fingerprint: str,
        fixture_ack: bool = False,
        priority: int = -250,
    ) -> tuple[int, bool, dict[str, Any]]:
        """Atomically allocate the grant remainder and queue one paid test."""
        if fixture_ack is not True:
            raise ValueError(
                "operator must confirm the fixture has at least 25 public historical posts"
            )
        now = utcnow()
        with self.connect() as conn:
            self._refresh_contract_test_grants_conn(conn)
            grant = conn.execute(
                """SELECT * FROM contract_test_grants
                WHERE id=? AND purpose='posts_cursor' AND status='active'""",
                (grant_id,),
            ).fetchone()
            if not grant:
                raise ValueError("no active posts cursor contract-test grant")
            existing = conn.execute(
                """SELECT a.*,j.status AS job_status FROM contract_test_allocations a
                LEFT JOIN jobs j ON j.id=a.job_id
                WHERE a.grant_id=? AND a.actor_id=? AND a.schema_fingerprint=?""",
                (grant_id, actor_id, schema_fingerprint),
            ).fetchone()
            if existing:
                if str(existing["job_status"] or "") in {"pending", "running"}:
                    return int(existing["job_id"]), False, dict(existing)
                raise ValueError("this Actor fingerprint was already tested in the current grant")
            in_flight = conn.execute(
                """SELECT a.job_id FROM contract_test_allocations a
                JOIN jobs j ON j.id=a.job_id
                WHERE j.status IN ('pending','running') ORDER BY a.id LIMIT 1"""
            ).fetchone()
            if in_flight:
                raise ValueError("another contract test is already pending or running")
            ambiguous = conn.execute(
                """SELECT 1 FROM contract_runs cr
                JOIN contract_test_allocations a ON a.id=cr.grant_allocation_id
                WHERE a.grant_id=? AND cr.status IN ('launching','run_started','needs_reconcile')
                LIMIT 1""",
                (grant_id,),
            ).fetchone()
            if ambiguous:
                raise ValueError("contract test grant has an ambiguous paid run; reconcile it first")
            ledger = self._contract_grant_ledger_conn(conn, grant)
            remaining = float(ledger["remaining_usd"])
            if remaining <= 1e-9:
                raise ValueError("contract test grant has no remaining budget")
            generation = "grant:" + str(grant_id) + ":" + canonical_request_hash(
                {
                    "grant_id": grant_id,
                    "profile_id": profile_id,
                    "actor_id": actor_id,
                    "schema_fingerprint": schema_fingerprint,
                }
            )[:20]
            allocation_cursor = conn.execute(
                """INSERT INTO contract_test_allocations(
                  grant_id,profile_id,actor_id,schema_fingerprint,test_generation,
                  authorized_usd,created_at
                ) VALUES(?,?,?,?,?,?,?)""",
                (
                    grant_id,
                    profile_id,
                    actor_id,
                    schema_fingerprint,
                    generation,
                    remaining,
                    now,
                ),
            )
            allocation_id = int(allocation_cursor.lastrowid)
            payload = {
                "actor_id": actor_id,
                "max_budget_usd": remaining,
                "contract_test_id": generation,
                "contract_grant_id": grant_id,
                "contract_allocation_id": allocation_id,
                "fixture_ack": True,
                "fixture_expected_min_public_posts": 25,
            }
            job_cursor = conn.execute(
                """INSERT INTO jobs(
                  profile_id,job_type,priority,status,payload_json,available_at,created_at,dedupe_key
                ) VALUES(?,'contract_test_posts_v2',?,'pending',?,?,?,?)""",
                (
                    profile_id,
                    priority,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                    f"contract-grant:{grant_id}:{actor_id}:{schema_fingerprint}",
                ),
            )
            job_id = int(job_cursor.lastrowid)
            conn.execute(
                "UPDATE contract_test_allocations SET job_id=? WHERE id=?",
                (job_id, allocation_id),
            )
            allocation = conn.execute(
                "SELECT * FROM contract_test_allocations WHERE id=?", (allocation_id,)
            ).fetchone()
            return job_id, True, dict(allocation)

    def record_contract_run(
        self,
        contract_id: int,
        *,
        test_case: str,
        normalized_input: dict[str, Any],
        expected: dict[str, Any] | None = None,
        request_hash: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        request_hash = request_hash or canonical_request_hash(
            {"contract_id": contract_id, "test_case": test_case, "input": normalized_input}
        )
        now = utcnow()
        with self.connect() as conn:
            contract = conn.execute(
                "SELECT evidence_json FROM actor_contracts WHERE id=?", (contract_id,)
            ).fetchone()
            try:
                evidence = json.loads(str(contract["evidence_json"] or "{}")) if contract else {}
            except (TypeError, json.JSONDecodeError):
                evidence = {}
            generation = str(evidence.get("test_generation") or "")
            allocation = (
                conn.execute(
                    "SELECT * FROM contract_test_allocations WHERE test_generation=?",
                    (generation,),
                ).fetchone()
                if generation
                else None
            )
            factor = {
                "page_1": 0.30,
                "page_2": 0.30,
                "page_2_replay": 0.30,
                "known_boundary": 0.10,
            }.get(test_case, 1.0)
            allocation_id = int(allocation["id"]) if allocation else None
            authorized_max = (
                float(
                    provider_charge_ceiling(
                        float(allocation["authorized_usd"]) * factor
                    )
                )
                if allocation
                else 0.0
            )
            cursor = conn.execute(
                """INSERT OR IGNORE INTO contract_runs(
                  contract_id,request_hash,test_case,input_json,expected_json,started_at,
                  grant_allocation_id,authorized_max_usd
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (
                    contract_id,
                    request_hash,
                    test_case,
                    json.dumps(normalized_input, ensure_ascii=False, sort_keys=True),
                    json.dumps(expected or {}, ensure_ascii=False, sort_keys=True),
                    now,
                    allocation_id,
                    authorized_max,
                ),
            )
            row = conn.execute("SELECT * FROM contract_runs WHERE request_hash=?", (request_hash,)).fetchone()
            return dict(row), bool(cursor.rowcount)

    def claim_contract_run_launch(
        self,
        run_id: int,
        *,
        lease_owner: str,
        claimed_at: str | None = None,
        monthly_limit_usd: float | None = None,
        official_used_usd: float = 0,
        outstanding_reserve_usd: float = 0,
        posts_result_price_usd: float = 0,
        cycle_start_at: str | None = None,
        baseline_local_settled_usd: float = 0,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically authorize budget and claim one contract-test launch.

        ``monthly_limit_usd`` is optional for the low-level CAS tests and old
        callers.  Production contract tests always supply it after a fresh
        provider usage query.  Under the same SQLite write lock we then count
        source batches, access probes, and the entire still-unsettled contract
        allocation before granting the only right to call ``apify.start``.
        """

        if not lease_owner:
            raise ValueError("contract run lease_owner is required")
        claimed_at = claimed_at or utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM contract_runs WHERE id=?", (run_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown contract run: {run_id}")
            if str(current["status"]) != "pending" or current["run_id"]:
                return dict(current), False
            if monthly_limit_usd is not None:
                allocation = conn.execute(
                    """SELECT a.*,g.status AS grant_status,g.max_usd AS grant_max_usd,
                    g.expires_at,j.status AS job_status
                    FROM contract_test_allocations a
                    JOIN contract_test_grants g ON g.id=a.grant_id
                    LEFT JOIN jobs j ON j.id=a.job_id
                    WHERE a.id=?""",
                    (current["grant_allocation_id"],),
                ).fetchone()
                denial = ""
                if not allocation:
                    denial = "missing_contract_allocation"
                elif (
                    str(allocation["grant_status"]) != "active"
                    or str(allocation["job_status"] or "") != "running"
                    or str(allocation["expires_at"] or "") <= claimed_at
                ):
                    denial = "contract_grant_not_active"
                else:
                    allocated = float(
                        conn.execute(
                            "SELECT COALESCE(SUM(authorized_usd),0) "
                            "FROM contract_test_allocations WHERE grant_id=?",
                            (allocation["grant_id"],),
                        ).fetchone()[0]
                        or 0
                    )
                    authorized_runs = float(
                        conn.execute(
                            """SELECT COALESCE(SUM(authorized_max_usd),0)
                            FROM contract_runs WHERE grant_allocation_id=?
                              AND NOT (
                                status='failed'
                                AND NULLIF(TRIM(run_id),'') IS NULL
                                AND NULLIF(TRIM(dataset_id),'') IS NULL
                                AND COALESCE(charged_usd,0)<=0
                              )""",
                            (allocation["id"],),
                        ).fetchone()[0]
                        or 0
                    )
                    if allocated > float(allocation["grant_max_usd"] or 0) + 1e-9:
                        denial = "contract_grant_oversubscribed"
                    elif authorized_runs > float(allocation["authorized_usd"] or 0) + 1e-9:
                        denial = "contract_allocation_oversubscribed"
                    else:
                        reservations = self._paid_budget_reservations_in_connection(
                            conn,
                            result_price=max(0.0, float(posts_result_price_usd)),
                        )
                        capacity = (
                            self._atomic_paid_capacity_in_connection(
                                conn,
                                cycle_start_at=cycle_start_at,
                                monthly_limit_usd=monthly_limit_usd,
                                provider_used_usd=official_used_usd,
                                baseline_local_settled_usd=(
                                    baseline_local_settled_usd
                                ),
                                outstanding_reserve_usd=(
                                    outstanding_reserve_usd
                                ),
                                posts_result_price_usd=(
                                    posts_result_price_usd
                                ),
                            )
                            if cycle_start_at
                            else max(
                                0.0,
                                float(monthly_limit_usd)
                                - max(0.0, float(official_used_usd))
                                - max(0.0, float(outstanding_reserve_usd)),
                            )
                        )
                        if (
                            float(reservations["total_unsettled_usd"])
                            > capacity + 1e-9
                        ):
                            denial = "monthly_budget_capacity"
                if denial:
                    denied = dict(current)
                    denied["claim_denied_reason"] = denial
                    return denied, False
            cursor = conn.execute(
                """UPDATE contract_runs SET status='launching',lease_owner=?,leased_at=?
                WHERE id=? AND status='pending' AND run_id IS NULL""",
                (lease_owner, claimed_at, run_id),
            )
            row = conn.execute("SELECT * FROM contract_runs WHERE id=?", (run_id,)).fetchone()
            return dict(row), cursor.rowcount == 1

    def attach_contract_run_provider_identity(
        self,
        run_row_id: int,
        *,
        run_id: str,
        dataset_id: str | None,
        lease_owner: str,
        provider_started_at: str | None = None,
    ) -> dict[str, Any]:
        """Persist an accepted contract-test run and its global ownership."""

        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            raise ValueError("provider run_id is required")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                """SELECT cr.*,ac.provider AS contract_provider,
                          ac.actor_id AS contract_actor_id
                FROM contract_runs cr
                JOIN actor_contracts ac ON ac.id=cr.contract_id
                WHERE cr.id=?""",
                (run_row_id,),
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown contract run: {run_row_id}")
            if str(current["status"]) != "launching":
                raise RuntimeError(
                    f"contract run {run_row_id} is {current['status']}, expected launching"
                )
            if str(current["lease_owner"] or "") != str(lease_owner or ""):
                raise RuntimeError("contract run launch lease belongs to another worker")
            _claim_provider_run_ownership(
                conn,
                provider=str(current["contract_provider"] or ""),
                run_id=normalized_run_id,
                owner_type="contract_run",
                owner_id=run_row_id,
                actor_id=str(current["contract_actor_id"] or ""),
                request_fingerprint=str(current["request_hash"] or ""),
                provider_started_at=provider_started_at or now,
                metadata={
                    "dataset_id": str(dataset_id or ""),
                    "attached_by": "provider_start",
                },
            )
            cursor = conn.execute(
                """UPDATE contract_runs
                SET status='run_started',run_id=?,dataset_id=?,leased_at=?
                WHERE id=? AND status='launching' AND lease_owner=?""",
                (
                    normalized_run_id,
                    str(dataset_id or ""),
                    provider_started_at or now,
                    run_row_id,
                    lease_owner,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("contract run lost atomic provider identity attach")
            updated = conn.execute(
                "SELECT * FROM contract_runs WHERE id=?", (run_row_id,)
            ).fetchone()
            return dict(updated)

    def reconcile_contract_run(
        self,
        run_row_id: int,
        *,
        run_id: str | None = None,
        dataset_id: str | None = None,
        confirm_not_launched: bool = False,
    ) -> dict[str, Any]:
        """Resolve an ambiguous paid contract-test launch without rebuying it.

        Attaching a provider run makes the existing row resumable even after
        its grant expired or the profile was frozen.  A positive
        ``confirm_not_launched`` preserves the ambiguous row as failed and,
        while the original grant is still active, recreates the exact request
        identity as a fresh pending row that must pass all launch gates again.
        """

        normalized_run_id = str(run_id or "").strip()
        if bool(normalized_run_id) == bool(confirm_not_launched):
            raise ValueError("provide exactly one of run_id or confirm_not_launched")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                """SELECT cr.*,a.job_id,g.status AS grant_status,g.expires_at,
                          ac.provider AS contract_provider,
                          ac.actor_id AS contract_actor_id
                FROM contract_runs cr
                JOIN actor_contracts ac ON ac.id=cr.contract_id
                LEFT JOIN contract_test_allocations a
                  ON a.id=cr.grant_allocation_id
                LEFT JOIN contract_test_grants g ON g.id=a.grant_id
                WHERE cr.id=?""",
                (run_row_id,),
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown contract run: {run_row_id}")
            if str(current["status"]) != "needs_reconcile":
                raise RuntimeError(
                    f"contract run {run_row_id} is {current['status']}, "
                    "expected needs_reconcile"
                )
            evidence_rows = [dict(current)]
            registry_evidence = _provider_run_ownership_evidence(
                conn, "contract_run", run_row_id
            )
            if registry_evidence:
                evidence_rows.append(registry_evidence)
            if confirm_not_launched and _provider_evidence_present(*evidence_rows):
                raise RuntimeError(
                    "contract run 已有 provider run/dataset/charge 證據；"
                    "不可確認為未啟動"
                )
            resolved_run_id: str | None = None
            resolved_dataset_id: str | None = None
            if normalized_run_id:
                resolved_run_id, resolved_dataset_id, _ = _resolve_provider_attach(
                    *evidence_rows,
                    run_id=normalized_run_id,
                    dataset_id=dataset_id,
                )
                _claim_provider_run_ownership(
                    conn,
                    provider=str(current["contract_provider"] or ""),
                    run_id=resolved_run_id,
                    owner_type="contract_run",
                    owner_id=run_row_id,
                    actor_id=str(current["contract_actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["leased_at"] or now,
                    metadata={
                        "dataset_id": str(resolved_dataset_id or ""),
                        "attached_by": "operator_reconcile",
                    },
                )
            job_id = int(current["job_id"] or 0)
            if job_id:
                job = conn.execute(
                    "SELECT status FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                if job and str(job["status"]) == "running":
                    raise RuntimeError("contract test job is still running; wait before reconciling")

            replacement_id: int | None = None
            if normalized_run_id:
                conn.execute(
                    """UPDATE contract_runs
                    SET status='run_started',run_id=?,dataset_id=?,error=NULL,
                        finished_at=NULL,lease_owner=NULL,leased_at=COALESCE(leased_at,?)
                    WHERE id=? AND status='needs_reconcile'""",
                    (resolved_run_id, resolved_dataset_id, now, run_row_id),
                )
            else:
                original_hash = str(current["request_hash"])
                archived_hash = canonical_request_hash(
                    {
                        "reconciled_contract_run_id": run_row_id,
                        "original_request_hash": original_hash,
                        "confirmed_not_launched_at": now,
                    }
                )
                conn.execute(
                    """UPDATE contract_runs
                    SET status='failed',request_hash=?,run_id=NULL,dataset_id=NULL,
                        error=?,finished_at=?,
                        lease_owner=NULL
                    WHERE id=? AND status='needs_reconcile'""",
                    (
                        archived_hash,
                        "operator confirmed provider run was not launched",
                        now,
                        run_row_id,
                    ),
                )
                grant_active = (
                    str(current["grant_status"] or "") == "active"
                    and str(current["expires_at"] or "") > now
                )
                if grant_active:
                    cursor = conn.execute(
                        """INSERT INTO contract_runs(
                          contract_id,request_hash,test_case,status,input_json,
                          expected_json,result_json,result_count,charged_usd,
                          grant_allocation_id,authorized_max_usd,started_at
                        ) VALUES(?,?,?,'pending',?,?, '{}',0,0,?,?,?)""",
                        (
                            current["contract_id"],
                            original_hash,
                            current["test_case"],
                            current["input_json"],
                            current["expected_json"],
                            current["grant_allocation_id"],
                            current["authorized_max_usd"],
                            now,
                        ),
                    )
                    replacement_id = int(cursor.lastrowid)

            if job_id:
                should_requeue = bool(normalized_run_id or replacement_id)
                if should_requeue:
                    conn.execute(
                        """UPDATE jobs SET status='pending',available_at=?,started_at=NULL,
                        finished_at=NULL,error=NULL,lease_owner=NULL,leased_at=NULL
                        WHERE id=? AND status<>'running'""",
                        (now, job_id),
                    )
            updated = conn.execute(
                "SELECT * FROM contract_runs WHERE id=?", (run_row_id,)
            ).fetchone()
            result = dict(updated)
            result["replacement_run_row_id"] = replacement_id
            result["job_requeued"] = bool(job_id and (normalized_run_id or replacement_id))
            return result

    def get_or_create_capture_epoch(
        self,
        profile_id: int,
        trigger_reason: str,
        *,
        status: str = "awaiting_contract",
        priority: int = 100,
        scope: dict[str, Any] | None = None,
        signal_observation_id: int | None = None,
        reserved_budget_usd: float = 0,
    ) -> tuple[dict[str, Any], bool]:
        if status not in CAPTURE_EPOCH_ACTIVE_STATUSES:
            raise ValueError(f"capture epoch must start active, got: {status}")
        now = utcnow()
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO capture_epochs(
                  profile_id,trigger_reason,status,is_active,priority,scope_json,signal_observation_id,
                  reserved_budget_usd,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    profile_id,
                    trigger_reason,
                    status,
                    1,
                    priority,
                    json.dumps(scope or {}, ensure_ascii=False, sort_keys=True),
                    signal_observation_id,
                    reserved_budget_usd,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM capture_epochs WHERE profile_id=? AND is_active=1", (profile_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("capture epoch could not be created")
            return dict(row), bool(cursor.rowcount)

    def finish_capture_epoch(
        self,
        epoch_id: int,
        *,
        status: str,
        terminal_reason: str | None = None,
    ) -> None:
        if status in CAPTURE_EPOCH_ACTIVE_STATUSES:
            raise ValueError(f"terminal capture epoch status required, got: {status}")
        now = utcnow()
        self.execute(
            """UPDATE capture_epochs SET status=?,is_active=0,terminal_reason=?,completed_at=?,updated_at=?
            WHERE id=?""",
            (status, terminal_reason, now, now, epoch_id),
        )

    def upsert_coverage_stream(
        self,
        epoch_id: int,
        *,
        stream: str,
        surface: str,
        scope_type: str = "profile",
        scope_id: str = "",
        provider: str | None = None,
        contract_id: int | None = None,
        status: str = "pending",
    ) -> dict[str, Any]:
        if status not in COVERAGE_STATUSES:
            raise ValueError(f"unsupported coverage status: {status}")
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO coverage_streams(
                  epoch_id,stream,surface,scope_type,scope_id,provider,contract_id,status,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(epoch_id,stream,surface,scope_type,scope_id) DO UPDATE SET
                  provider=COALESCE(excluded.provider,coverage_streams.provider),
                  contract_id=COALESCE(excluded.contract_id,coverage_streams.contract_id),
                  updated_at=excluded.updated_at""",
                (epoch_id, stream, surface, scope_type, scope_id, provider, contract_id, status, now, now),
            )
            row = conn.execute(
                """SELECT * FROM coverage_streams
                WHERE epoch_id=? AND stream=? AND surface=? AND scope_type=? AND scope_id=?""",
                (epoch_id, stream, surface, scope_type, scope_id),
            ).fetchone()
            return dict(row)

    def update_coverage_stream(self, coverage_stream_id: int, **fields: Any) -> None:
        allowed = {
            "provider", "contract_id", "status", "input_cursor", "output_cursor",
            "provider_checkpoint_json", "low_watermark", "high_watermark", "terminal_evidence_json",
            "gaps_json", "seen_count", "new_count", "updated_count", "duplicate_count",
            "limited_reason", "next_job_at",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported coverage fields: {sorted(unknown)}")
        if "status" in fields and fields["status"] not in COVERAGE_STATUSES:
            raise ValueError(f"unsupported coverage status: {fields['status']}")
        if not fields:
            return
        with self.connect() as conn:
            current = conn.execute(
                "SELECT * FROM coverage_streams WHERE id=?", (coverage_stream_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown coverage stream: {coverage_stream_id}")

            def evidence_value(value: Any) -> Any:
                if not isinstance(value, str):
                    return value
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value

            target_status = str(fields.get("status", current["status"]))
            terminal_evidence = evidence_value(
                fields.get("terminal_evidence_json", current["terminal_evidence_json"])
            )
            reason = fields.get("limited_reason", current["limited_reason"])
            from .capture_v2 import validate_coverage_transition

            validate_coverage_transition(
                str(current["status"]),
                target_status,
                terminal_evidence=terminal_evidence,
                reason=str(reason or ""),
            )
            for name in ("provider_checkpoint_json", "terminal_evidence_json", "gaps_json"):
                if name in fields and not isinstance(fields[name], str):
                    fields[name] = json.dumps(fields[name], ensure_ascii=False, sort_keys=True)
            fields["updated_at"] = utcnow()
            assignments = ",".join(f"{name}=?" for name in fields)
            conn.execute(
                f"UPDATE coverage_streams SET {assignments} WHERE id=?",
                tuple(fields.values()) + (coverage_stream_id,),
            )

    def upsert_post_media_coverage(
        self,
        epoch_id: int,
        *,
        post_entity_id: int,
        surface: str = "post_albums",
        status: str = "pending",
    ) -> dict[str, Any]:
        if status not in COVERAGE_STATUSES:
            raise ValueError(f"unsupported post media coverage status: {status}")
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO post_media_coverage(
                  epoch_id,post_entity_id,surface,status,created_at,updated_at
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(epoch_id,post_entity_id,surface) DO UPDATE SET
                  updated_at=excluded.updated_at""",
                (epoch_id, post_entity_id, surface, status, now, now),
            )
            row = conn.execute(
                """SELECT * FROM post_media_coverage
                WHERE epoch_id=? AND post_entity_id=? AND surface=?""",
                (epoch_id, post_entity_id, surface),
            ).fetchone()
            if row is None:
                raise RuntimeError("post media coverage could not be created")
            return dict(row)

    def update_post_media_coverage(self, checkpoint_id: int, **fields: Any) -> None:
        allowed = {
            "status", "input_cursor", "output_cursor", "resume_url", "expected_count",
            "seen_count", "seen_media_ids_json", "provider_checkpoint_json",
            "terminal_evidence_json", "last_error",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported post media coverage fields: {sorted(unknown)}")
        if not fields:
            return
        with self.connect() as conn:
            current = conn.execute(
                "SELECT * FROM post_media_coverage WHERE id=?", (checkpoint_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown post media coverage: {checkpoint_id}")

            def evidence_value(value: Any) -> Any:
                if not isinstance(value, str):
                    return value
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value

            target_status = str(fields.get("status", current["status"]))
            terminal_evidence = evidence_value(
                fields.get("terminal_evidence_json", current["terminal_evidence_json"])
            )
            reason = fields.get("last_error", current["last_error"])
            from .capture_v2 import validate_coverage_transition

            validate_coverage_transition(
                str(current["status"]),
                target_status,
                terminal_evidence=terminal_evidence,
                reason=str(reason or ""),
            )
            for name in (
                "seen_media_ids_json", "provider_checkpoint_json", "terminal_evidence_json"
            ):
                if name in fields and not isinstance(fields[name], str):
                    fields[name] = json.dumps(fields[name], ensure_ascii=False, sort_keys=True)
            fields["updated_at"] = utcnow()
            assignments = ",".join(f"{name}=?" for name in fields)
            conn.execute(
                f"UPDATE post_media_coverage SET {assignments} WHERE id=?",
                tuple(fields.values()) + (checkpoint_id,),
            )

    def prepare_paid_photo_batch(
        self,
        *,
        profile_id: int,
        photo_capture_id: int,
        actor_id: str,
        normalized_input: dict[str, Any],
        max_charge_usd: float,
        input_cursor: str | None = None,
        request_hash: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Persist one profile-photo Actor request before crossing the paid boundary."""

        input_json = json.dumps(
            normalized_input,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        request_hash = request_hash or canonical_request_hash(
            {
                "profile_id": profile_id,
                "photo_capture_id": photo_capture_id,
                "actor_id": actor_id,
                "input_cursor": input_cursor or "",
                "input": normalized_input,
            }
        )
        now = utcnow()
        desired_max_charge = float(provider_charge_ceiling(max_charge_usd))
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO paid_photo_batches(
                  request_hash,profile_id,photo_capture_id,actor_id,
                  normalized_input_json,max_charge_usd,input_cursor,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    request_hash,
                    profile_id,
                    photo_capture_id,
                    actor_id,
                    input_json,
                    desired_max_charge,
                    input_cursor,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE request_hash=?",
                (request_hash,),
            ).fetchone()
            if row is None:
                raise RuntimeError("paid photo batch could not be prepared")
            if (
                int(row["profile_id"]) != profile_id
                or int(row["photo_capture_id"]) != photo_capture_id
                or str(row["actor_id"]) != actor_id
                or str(row["normalized_input_json"]) != input_json
            ):
                raise ValueError("request_hash already belongs to a different paid photo request")
            if str(row["status"]) == "prepared" and not row["run_id"]:
                conn.execute(
                    """UPDATE paid_photo_batches
                    SET max_charge_usd=MAX(max_charge_usd,?),updated_at=?
                    WHERE id=? AND status='prepared' AND run_id IS NULL""",
                    (max(0.0, float(max_charge_usd)), now, row["id"]),
                )
                row = conn.execute(
                    "SELECT * FROM paid_photo_batches WHERE id=?", (row["id"],)
                ).fetchone()
            return dict(row), bool(cursor.rowcount)

    def transition_paid_photo_batch(
        self,
        batch_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        if status not in BATCH_STATUSES:
            raise ValueError(f"unsupported paid photo batch status: {status}")
        allowed = {
            "actor_run_id",
            "run_id",
            "dataset_id",
            "key_value_store_id",
            "raw_path",
            "raw_sha256",
            "charged_usd",
            "raw_result_count",
            "parsed_result_count",
            "input_cursor",
            "output_cursor",
            "error",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported paid photo batch fields: {sorted(unknown)}")
        now = utcnow()
        milestone = {
            "launching": "launched_at",
            "raw_saved": "raw_saved_at",
            "imported": "imported_at",
            "committed": "committed_at",
        }.get(status)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid photo batch: {batch_id}")
            if expected_status is not None and str(current["status"]) != expected_status:
                raise RuntimeError(
                    f"paid photo batch {batch_id} is {current['status']}, expected {expected_status}"
                )
            from .capture_v2 import validate_batch_transition

            validate_batch_transition(str(current["status"]), status)
            incoming_run_id = _transition_provider_run_id(current["run_id"], fields)
            if status == "run_started" and not (
                incoming_run_id or _provider_evidence_text(current["run_id"])
            ):
                raise RuntimeError("run_started requires a durable provider run_id")
            if incoming_run_id:
                _claim_provider_run_ownership(
                    conn,
                    provider="apify",
                    run_id=incoming_run_id,
                    owner_type="paid_photo_batch",
                    owner_id=batch_id,
                    actor_id=str(current["actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["launched_at"] or now,
                    metadata={
                        "dataset_id": str(fields.get("dataset_id") or ""),
                        "key_value_store_id": str(
                            fields.get("key_value_store_id") or ""
                        ),
                    },
                )
            values = {"status": status, **fields, "updated_at": now}
            if milestone and not current[milestone]:
                values[milestone] = now
            assignments = ",".join(f"{name}=?" for name in values)
            compare_status = str(current["status"])
            cursor = conn.execute(
                f"UPDATE paid_photo_batches SET {assignments} WHERE id=? AND status=?",
                tuple(values.values()) + (batch_id, compare_status),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"paid photo batch {batch_id} lost atomic transition from {compare_status}"
                )
            updated = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(updated)

    def claim_paid_photo_batch_launch(
        self,
        batch_id: int,
        *,
        global_capacity_usd: float,
        minimum_charge_usd: float,
        posts_result_price_usd: float,
        cycle_start_at: str | None = None,
        monthly_limit_usd: float | None = None,
        provider_used_usd: float = 0,
        baseline_local_settled_usd: float = 0,
        outstanding_reserve_usd: float = 0,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically reserve the photo run against every durable Apify ledger."""

        now = utcnow()
        minimum = float(provider_charge_ceiling(minimum_charge_usd))
        posts_result_price = max(0.0, float(posts_result_price_usd))
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid photo batch: {batch_id}")
            if str(current["status"]) != "prepared" or current["run_id"]:
                return dict(current), False
            reservations = self._paid_budget_reservations_in_connection(
                conn,
                result_price=posts_result_price,
                excluding_photo_batch_id=batch_id,
            )
            atomic_capacity = max(0.0, float(global_capacity_usd))
            if cycle_start_at and monthly_limit_usd is not None:
                atomic_capacity = min(
                    atomic_capacity,
                    self._atomic_paid_capacity_in_connection(
                        conn,
                        cycle_start_at=cycle_start_at,
                        monthly_limit_usd=monthly_limit_usd,
                        provider_used_usd=provider_used_usd,
                        baseline_local_settled_usd=baseline_local_settled_usd,
                        outstanding_reserve_usd=outstanding_reserve_usd,
                        posts_result_price_usd=posts_result_price,
                    ),
                )
            available = max(
                0.0,
                atomic_capacity
                - float(reservations["total_unsettled_usd"]),
            )
            clamped = float(
                provider_charge_ceiling(
                    min(max(0.0, float(current["max_charge_usd"])), available)
                )
            )
            if clamped <= 0 or clamped + 1e-12 < minimum:
                conn.execute(
                    "UPDATE paid_photo_batches SET max_charge_usd=?,updated_at=? "
                    "WHERE id=? AND status='prepared'",
                    (clamped, now, batch_id),
                )
                row = conn.execute(
                    "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
                ).fetchone()
                return dict(row), False
            cursor = conn.execute(
                """UPDATE paid_photo_batches
                SET status='launching',max_charge_usd=?,
                    launched_at=COALESCE(launched_at,?),updated_at=?
                WHERE id=? AND status='prepared' AND run_id IS NULL""",
                (clamped, now, now, batch_id),
            )
            row = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(row), cursor.rowcount == 1

    def reconcile_paid_photo_batch(
        self,
        batch_id: int,
        *,
        run_id: str | None = None,
        dataset_id: str | None = None,
        key_value_store_id: str | None = None,
        confirm_not_launched: bool = False,
        abandon_import_failed: bool = False,
    ) -> dict[str, Any]:
        """Resolve or explicitly abandon one blocked paid photo batch."""

        normalized_run_id = str(run_id or "").strip()
        if sum(
            bool(value)
            for value in (
                normalized_run_id,
                confirm_not_launched,
                abandon_import_failed,
            )
        ) != 1:
            raise ValueError(
                "provide exactly one of run_id, confirm_not_launched, "
                "or abandon_import_failed"
            )
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid photo batch: {batch_id}")
            capture = conn.execute(
                "SELECT * FROM profile_photo_captures WHERE id=?",
                (current["photo_capture_id"],),
            ).fetchone()
            if capture is None:
                raise RuntimeError("找不到照片回溯狀態")
            expected_status = (
                "import_failed" if abandon_import_failed else "needs_reconcile"
            )
            if str(current["status"]) != expected_status:
                raise RuntimeError(
                    f"paid photo batch {batch_id} is {current['status']}, "
                    f"expected {expected_status}"
                )
            diagnostic_id = int(current["actor_run_id"] or 0)
            diagnostic = (
                conn.execute(
                    "SELECT run_id,charged_usd FROM actor_runs WHERE id=?",
                    (diagnostic_id,),
                ).fetchone()
                if diagnostic_id
                else None
            )
            evidence_rows = [dict(current)]
            if diagnostic:
                evidence_rows.append(dict(diagnostic))
            registry_evidence = _provider_run_ownership_evidence(
                conn, "paid_photo_batch", batch_id
            )
            if registry_evidence:
                evidence_rows.append(registry_evidence)
            if confirm_not_launched and _provider_evidence_present(*evidence_rows):
                raise RuntimeError(
                    "照片批次已有 provider run/dataset/charge 證據；"
                    "不可確認為未啟動"
                )
            resolved_run_id: str | None = None
            resolved_dataset_id: str | None = None
            resolved_store_id: str | None = None
            if normalized_run_id:
                (
                    resolved_run_id,
                    resolved_dataset_id,
                    resolved_store_id,
                ) = _resolve_provider_attach(
                    *evidence_rows,
                    run_id=normalized_run_id,
                    dataset_id=dataset_id,
                    key_value_store_id=key_value_store_id,
                )
                _claim_provider_run_ownership(
                    conn,
                    provider="apify",
                    run_id=resolved_run_id,
                    owner_type="paid_photo_batch",
                    owner_id=batch_id,
                    actor_id=str(current["actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["launched_at"] or now,
                    metadata={
                        "dataset_id": str(resolved_dataset_id or ""),
                        "key_value_store_id": str(resolved_store_id or ""),
                        "attached_by": "operator_reconcile",
                    },
                )
            from .capture_v2 import validate_batch_transition

            destination = "run_started" if normalized_run_id else "failed"
            validate_batch_transition(str(current["status"]), destination)
            if normalized_run_id:
                conn.execute(
                    """UPDATE paid_photo_batches
                    SET status='run_started',run_id=?,dataset_id=?,key_value_store_id=?,
                        launched_at=COALESCE(launched_at,?),error=NULL,updated_at=?
                    WHERE id=? AND status='needs_reconcile'""",
                    (
                        resolved_run_id,
                        resolved_dataset_id,
                        resolved_store_id,
                        now,
                        now,
                        batch_id,
                    ),
                )
                if diagnostic_id:
                    conn.execute(
                    """UPDATE actor_runs SET status='running',run_id=?,error=NULL,
                        finished_at=NULL WHERE id=?""",
                        (resolved_run_id, diagnostic_id),
                    )
            else:
                reason = (
                    "operator abandoned import_failed raw after Actor/schema switch"
                    if abandon_import_failed
                    else "operator confirmed provider run was not launched"
                )
                if confirm_not_launched:
                    conn.execute(
                        """UPDATE paid_photo_batches
                        SET status='failed',run_id=NULL,dataset_id=NULL,
                            key_value_store_id=NULL,error=?,updated_at=?
                        WHERE id=? AND status=?""",
                        (reason, now, batch_id, expected_status),
                    )
                else:
                    conn.execute(
                        """UPDATE paid_photo_batches
                        SET status='failed',error=?,updated_at=?
                        WHERE id=? AND status=?""",
                        (reason, now, batch_id, expected_status),
                    )
                if diagnostic_id:
                    conn.execute(
                        """UPDATE actor_runs SET status='failed',error=?,finished_at=?
                        WHERE id=?""",
                        (reason, now, diagnostic_id),
                    )
            try:
                checkpoint = json.loads(str(capture["checkpoint_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                checkpoint = {}
            if not isinstance(checkpoint, dict):
                checkpoint = {}
            if not normalized_run_id:
                checkpoint["actor_retry_nonce"] = int(
                    checkpoint.get("actor_retry_nonce") or 0
                ) + 1
            if abandon_import_failed:
                for key in (
                    "actor_next_cursor",
                    "actor_seen_cursors",
                    "actor_batch_id",
                    "actor_run_id",
                    "actor_id",
                    "actor_evidence",
                    "actor_inventory_completed",
                    "actor_declared_total",
                    "actor_discovered_urls",
                    "actor_discovered_count",
                    "actor_collected_items",
                    "actor_processed_item_ids",
                    "actor_fallback_error",
                    "actor_media_retry_attempt",
                    "actor_media_retry_until",
                    "actor_pending_media_external_ids",
                    "actor_exhausted_media_external_ids",
                ):
                    checkpoint.pop(key, None)
                checkpoint["completed"] = False
                checkpoint["terminal_reason"] = ""
                checkpoint["stalled_reason"] = ""
            conn.execute(
                """UPDATE profile_photo_captures SET status='in_progress',
                checkpoint_json=?,limited_reason=NULL,next_job_at=?,completed_at=NULL,
                updated_at=? WHERE id=?""",
                (
                    json.dumps(checkpoint, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                    capture["id"],
                ),
            )
            resume_payload = json.dumps(
                {
                    "photo_capture_id": int(capture["id"]),
                    "iteration": int(checkpoint.get("capture_iteration") or 0),
                    "manual": True,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            conn.execute(
                """INSERT OR IGNORE INTO jobs(
                  profile_id,job_type,priority,status,payload_json,available_at,
                  created_at,dedupe_key
                ) VALUES(?,'capture_profile_photos',-120,'pending',?,?,?,?)""",
                (
                    current["profile_id"],
                    resume_payload,
                    now,
                    now,
                    f"capture-account-photos:{capture['id']}:reconcile:{batch_id}",
                ),
            )
            updated = conn.execute(
                "SELECT * FROM paid_photo_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(updated)

    def prepare_paid_source_batch(
        self,
        *,
        profile_id: int,
        epoch_id: int,
        coverage_stream_id: int,
        contract_id: int | None,
        provider: str,
        actor_id: str,
        intent: str,
        observation_window: str,
        normalized_input: dict[str, Any],
        input_cursor: str | None = None,
        request_hash: str | None = None,
        request_identity: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        input_json = json.dumps(normalized_input, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        request_hash = request_hash or canonical_request_hash(
            request_identity
            or {
                "profile_id": profile_id,
                "epoch_id": epoch_id,
                "stream_id": coverage_stream_id,
                "contract_id": contract_id,
                "provider": provider,
                "actor_id": actor_id,
                "intent": intent,
                "window": observation_window,
                "input_cursor": input_cursor or "",
                "input": normalized_input,
            }
        )
        now = utcnow()
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO paid_source_batches(
                  request_hash,profile_id,epoch_id,coverage_stream_id,contract_id,provider,actor_id,
                  intent,observation_window,normalized_input_json,input_cursor,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    request_hash,
                    profile_id,
                    epoch_id,
                    coverage_stream_id,
                    contract_id,
                    provider,
                    actor_id,
                    intent,
                    observation_window,
                    input_json,
                    input_cursor,
                    now,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM paid_source_batches WHERE request_hash=?", (request_hash,)).fetchone()
            if row is None:
                raise RuntimeError("paid source batch could not be prepared")
            if (
                int(row["profile_id"]) != profile_id
                or int(row["epoch_id"]) != epoch_id
                or str(row["normalized_input_json"]) != input_json
            ):
                raise ValueError("request_hash already belongs to a different paid request")
            return dict(row), bool(cursor.rowcount)

    def transition_paid_source_batch(
        self,
        batch_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        if status not in BATCH_STATUSES:
            raise ValueError(f"unsupported batch status: {status}")
        allowed = {
            "run_id", "dataset_id", "key_value_store_id", "raw_path", "raw_sha256", "charged_usd",
            "raw_result_count", "parsed_result_count", "new_result_count", "updated_result_count",
            "duplicate_result_count", "input_cursor", "output_cursor", "identity_set_hash",
            "lease_owner", "leased_at", "error",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported paid batch fields: {sorted(unknown)}")
        now = utcnow()
        milestone = {
            "launching": "launched_at",
            "raw_saved": "raw_saved_at",
            "imported": "imported_at",
            "committed": "committed_at",
        }.get(status)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute("SELECT * FROM paid_source_batches WHERE id=?", (batch_id,)).fetchone()
            if current is None:
                raise ValueError(f"unknown paid source batch: {batch_id}")
            if expected_status is not None and current["status"] != expected_status:
                raise RuntimeError(
                    f"paid source batch {batch_id} is {current['status']}, expected {expected_status}"
                )
            from .capture_v2 import validate_batch_transition

            validate_batch_transition(str(current["status"]), status)
            incoming_run_id = _transition_provider_run_id(current["run_id"], fields)
            if status == "run_started" and not (
                incoming_run_id or _provider_evidence_text(current["run_id"])
            ):
                raise RuntimeError("run_started requires a durable provider run_id")
            if incoming_run_id:
                _claim_provider_run_ownership(
                    conn,
                    provider=str(current["provider"] or ""),
                    run_id=incoming_run_id,
                    owner_type="paid_source_batch",
                    owner_id=batch_id,
                    actor_id=str(current["actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["launched_at"] or now,
                    metadata={
                        "dataset_id": str(fields.get("dataset_id") or ""),
                        "key_value_store_id": str(
                            fields.get("key_value_store_id") or ""
                        ),
                    },
                )
            values = {"status": status, **fields, "updated_at": now}
            if milestone and not current[milestone]:
                values[milestone] = now
            assignments = ",".join(f"{name}=?" for name in values)
            compare_status = str(current["status"])
            cursor = conn.execute(
                f"UPDATE paid_source_batches SET {assignments} WHERE id=? AND status=?",
                tuple(values.values()) + (batch_id, compare_status),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"paid source batch {batch_id} lost atomic transition from {compare_status}"
                )
            updated = conn.execute("SELECT * FROM paid_source_batches WHERE id=?", (batch_id,)).fetchone()
            return dict(updated)

    def claim_paid_source_batch_launch(
        self,
        batch_id: int,
        *,
        lease_owner: str,
        posts_result_price_usd: float,
        claimed_at: str | None = None,
        budget_capacity_usd: float | None = None,
        cycle_start_at: str | None = None,
        monthly_limit_usd: float | None = None,
        provider_used_usd: float = 0,
        baseline_local_settled_usd: float = 0,
        outstanding_reserve_usd: float = 0,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically claim a prepared batch before the external Actor launch.

        When a capacity is supplied, every paid ledger is read under the same
        SQLite write lock.  A concurrent source batch, access probe, or
        contract-test allocation therefore wins the budget exactly once.
        """

        if not lease_owner:
            raise ValueError("paid batch lease_owner is required")
        claimed_at = claimed_at or utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_source_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid source batch: {batch_id}")
            if str(current["status"]) != "prepared" or current["run_id"]:
                return dict(current), False
            result_price = max(0.0, float(posts_result_price_usd))
            try:
                normalized = json.loads(
                    str(current["normalized_input_json"] or "{}")
                )
            except (TypeError, json.JSONDecodeError):
                normalized = {}
            maximum = max(
                1,
                min(
                    50,
                    int(
                        normalized.get("maxPostsPerProfile")
                        or normalized.get("maxPosts")
                        or 50
                    ),
                ),
            )
            requested = float(provider_charge_ceiling(maximum * result_price))
            if budget_capacity_usd is not None or (
                cycle_start_at and monthly_limit_usd is not None
            ):
                reservations = self._paid_budget_reservations_in_connection(
                    conn,
                    result_price=result_price,
                    excluding_source_batch_id=batch_id,
                )
                capacity = (
                    max(0.0, float(budget_capacity_usd))
                    if budget_capacity_usd is not None
                    else float("inf")
                )
                if cycle_start_at and monthly_limit_usd is not None:
                    capacity = min(
                        capacity,
                        self._atomic_paid_capacity_in_connection(
                            conn,
                            cycle_start_at=cycle_start_at,
                            monthly_limit_usd=monthly_limit_usd,
                            provider_used_usd=provider_used_usd,
                            baseline_local_settled_usd=(
                                baseline_local_settled_usd
                            ),
                            outstanding_reserve_usd=outstanding_reserve_usd,
                            posts_result_price_usd=result_price,
                        ),
                    )
                remaining = max(
                    0.0,
                    capacity
                    - float(reservations["total_unsettled_usd"]),
                )
                if requested <= 0 or requested > remaining + 1e-12:
                    return dict(current), False
            cursor = conn.execute(
                """UPDATE paid_source_batches
                SET status='launching',max_charge_usd=?,lease_owner=?,leased_at=?,
                    launched_at=COALESCE(launched_at,?),updated_at=?
                WHERE id=? AND status='prepared' AND run_id IS NULL""",
                (
                    requested,
                    lease_owner,
                    claimed_at,
                    claimed_at,
                    claimed_at,
                    batch_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM paid_source_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(row), cursor.rowcount == 1

    def reconcile_paid_source_batch(
        self,
        batch_id: int,
        *,
        run_id: str | None = None,
        dataset_id: str | None = None,
        key_value_store_id: str | None = None,
        confirm_not_launched: bool = False,
    ) -> dict[str, Any]:
        """Resolve one ambiguous Capture V2 post batch without a duplicate buy."""

        normalized_run_id = str(run_id or "").strip()
        if bool(normalized_run_id) == bool(confirm_not_launched):
            raise ValueError("provide exactly one of run_id or confirm_not_launched")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_source_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid source batch: {batch_id}")
            if str(current["status"]) != "needs_reconcile":
                raise RuntimeError(
                    f"paid source batch {batch_id} is {current['status']}, "
                    "expected needs_reconcile"
                )
            evidence_rows = [dict(current)]
            registry_evidence = _provider_run_ownership_evidence(
                conn, "paid_source_batch", batch_id
            )
            if registry_evidence:
                evidence_rows.append(registry_evidence)
            if confirm_not_launched and _provider_evidence_present(*evidence_rows):
                raise RuntimeError(
                    "貼文批次已有 provider run/dataset/charge 證據；"
                    "不可確認為未啟動"
                )
            resolved_run_id: str | None = None
            resolved_dataset_id: str | None = None
            resolved_store_id: str | None = None
            if normalized_run_id:
                (
                    resolved_run_id,
                    resolved_dataset_id,
                    resolved_store_id,
                ) = _resolve_provider_attach(
                    *evidence_rows,
                    run_id=normalized_run_id,
                    dataset_id=dataset_id,
                    key_value_store_id=key_value_store_id,
                )
                _claim_provider_run_ownership(
                    conn,
                    provider=str(current["provider"] or ""),
                    run_id=resolved_run_id,
                    owner_type="paid_source_batch",
                    owner_id=batch_id,
                    actor_id=str(current["actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["launched_at"] or now,
                    metadata={
                        "dataset_id": str(resolved_dataset_id or ""),
                        "key_value_store_id": str(resolved_store_id or ""),
                        "attached_by": "operator_reconcile",
                    },
                )
            epoch = conn.execute(
                "SELECT is_active FROM capture_epochs WHERE id=?",
                (current["epoch_id"],),
            ).fetchone()
            if not epoch or not bool(epoch["is_active"]):
                raise RuntimeError("paid source batch belongs to a closed capture epoch")

            replacement_id: int | None = None
            if normalized_run_id:
                conn.execute(
                    """UPDATE paid_source_batches
                    SET status='run_started',run_id=?,dataset_id=?,key_value_store_id=?,
                        launched_at=COALESCE(launched_at,?),error=NULL,
                        lease_owner=NULL,leased_at=COALESCE(leased_at,?),updated_at=?
                    WHERE id=? AND status='needs_reconcile'""",
                    (
                        resolved_run_id,
                        resolved_dataset_id,
                        resolved_store_id,
                        now,
                        now,
                        now,
                        batch_id,
                    ),
                )
            else:
                conn.execute(
                    """UPDATE paid_source_batches
                    SET status='failed',run_id=NULL,dataset_id=NULL,
                        key_value_store_id=NULL,error=?,lease_owner=NULL,
                        updated_at=? WHERE id=? AND status='needs_reconcile'""",
                    (
                        "operator confirmed provider run was not launched",
                        now,
                        batch_id,
                    ),
                )
                replacement_hash = canonical_request_hash(
                    {
                        "reconciled_source_batch_id": batch_id,
                        "original_request_hash": str(current["request_hash"]),
                        "confirmed_not_launched_at": now,
                    }
                )
                cursor = conn.execute(
                    """INSERT INTO paid_source_batches(
                      request_hash,profile_id,epoch_id,coverage_stream_id,contract_id,
                      provider,actor_id,intent,observation_window,
                      normalized_input_json,status,input_cursor,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,'prepared',?,?,?)""",
                    (
                        replacement_hash,
                        current["profile_id"],
                        current["epoch_id"],
                        current["coverage_stream_id"],
                        current["contract_id"],
                        current["provider"],
                        current["actor_id"],
                        current["intent"],
                        current["observation_window"],
                        current["normalized_input_json"],
                        current["input_cursor"],
                        now,
                        now,
                    ),
                )
                replacement_id = int(cursor.lastrowid)

            conn.execute(
                """UPDATE coverage_streams SET status='in_progress',limited_reason=NULL,
                next_job_at=?,updated_at=? WHERE id=?""",
                (now, now, current["coverage_stream_id"]),
            )
            conn.execute(
                """UPDATE capture_epochs SET status='ready',
                terminal_reason=NULL,completed_at=NULL,updated_at=? WHERE id=?""",
                (now, current["epoch_id"]),
            )
            resume_batch_id = replacement_id or batch_id
            resume_payload = json.dumps(
                {
                    "epoch_id": int(current["epoch_id"]),
                    "coverage_stream_id": int(current["coverage_stream_id"]),
                    "surface": "timeline_posts",
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            conn.execute(
                """INSERT OR IGNORE INTO jobs(
                  profile_id,job_type,priority,status,payload_json,available_at,
                  created_at,dedupe_key,epoch_id,batch_id
                ) VALUES(?,'capture_posts_v2',-300,'pending',?,?,?,?,?,?)""",
                (
                    current["profile_id"],
                    resume_payload,
                    now,
                    now,
                    f"capture-v2:reconcile:{batch_id}:{resume_batch_id}",
                    current["epoch_id"],
                    resume_batch_id,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM paid_source_batches WHERE id=?", (batch_id,)
            ).fetchone()
            result = dict(updated)
            result["replacement_batch_id"] = replacement_id
            result["resume_batch_id"] = resume_batch_id
            return result

    def prepare_paid_access_probe_batch(
        self,
        *,
        profile_id: int,
        contract_id: int,
        provider: str,
        actor_id: str,
        observation_window: str,
        normalized_input: dict[str, Any],
        max_charge_usd: float,
        request_hash: str | None = None,
        request_identity: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Reserve one paid access probe identity before any Actor launch.

        Unlike ``paid_source_batches``, an access probe intentionally has no
        capture epoch or coverage stream.  Keeping it in a separate ledger
        prevents a two-hour visibility check from creating or advancing a
        full-history capture epoch while retaining the same crash-safe state
        machine and UNIQUE request gate.
        """
        input_json = json.dumps(
            normalized_input,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        request_hash = request_hash or canonical_request_hash(
            request_identity
            or {
                "profile_id": profile_id,
                "contract_id": contract_id,
                "provider": provider,
                "actor_id": actor_id,
                "intent": "access_probe",
                "window": observation_window,
                "input": normalized_input,
            }
        )
        now = utcnow()
        desired_max_charge = float(provider_charge_ceiling(max_charge_usd))
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO paid_access_probe_batches(
                  request_hash,profile_id,contract_id,provider,actor_id,intent,
                  observation_window,normalized_input_json,max_charge_usd,created_at,updated_at
                ) VALUES(?,?,?,?,?,'access_probe',?,?,?,?,?)""",
                (
                    request_hash,
                    profile_id,
                    contract_id,
                    provider,
                    actor_id,
                    observation_window,
                    input_json,
                    desired_max_charge,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE request_hash=?",
                (request_hash,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """SELECT * FROM paid_access_probe_batches
                    WHERE profile_id=? AND observation_window=?""",
                    (profile_id, observation_window),
                ).fetchone()
            if row is None:
                raise RuntimeError("paid access probe batch could not be prepared")
            if (
                str(row["request_hash"]) != request_hash
                or
                int(row["profile_id"]) != profile_id
                or int(row["contract_id"]) != contract_id
                or str(row["actor_id"]) != actor_id
                or str(row["observation_window"]) != observation_window
                or str(row["normalized_input_json"]) != input_json
            ):
                raise ValueError("request_hash already belongs to a different paid access probe")
            if (
                str(row["status"]) == "prepared"
                and desired_max_charge > float(row["max_charge_usd"] or 0)
            ):
                conn.execute(
                    """UPDATE paid_access_probe_batches
                    SET max_charge_usd=?,updated_at=? WHERE id=? AND status='prepared'""",
                    (desired_max_charge, now, row["id"]),
                )
                row = conn.execute(
                    "SELECT * FROM paid_access_probe_batches WHERE id=?",
                    (row["id"],),
                ).fetchone()
            return dict(row), bool(cursor.rowcount)

    def transition_paid_access_probe_batch(
        self,
        batch_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        if status not in BATCH_STATUSES:
            raise ValueError(f"unsupported access probe batch status: {status}")
        allowed = {
            "actor_run_id",
            "run_id",
            "dataset_id",
            "key_value_store_id",
            "raw_path",
            "raw_sha256",
            "charged_usd",
            "raw_result_count",
            "parsed_result_count",
            "error",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported paid access probe fields: {sorted(unknown)}")
        now = utcnow()
        milestone = {
            "launching": "launched_at",
            "raw_saved": "raw_saved_at",
            "imported": "imported_at",
            "committed": "committed_at",
        }.get(status)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid access probe batch: {batch_id}")
            if expected_status is not None and current["status"] != expected_status:
                raise RuntimeError(
                    f"paid access probe batch {batch_id} is {current['status']}, "
                    f"expected {expected_status}"
                )
            from .capture_v2 import validate_batch_transition

            validate_batch_transition(str(current["status"]), status)
            incoming_run_id = _transition_provider_run_id(current["run_id"], fields)
            if status == "run_started" and not (
                incoming_run_id or _provider_evidence_text(current["run_id"])
            ):
                raise RuntimeError("run_started requires a durable provider run_id")
            if incoming_run_id:
                _claim_provider_run_ownership(
                    conn,
                    provider=str(current["provider"] or ""),
                    run_id=incoming_run_id,
                    owner_type="paid_access_probe_batch",
                    owner_id=batch_id,
                    actor_id=str(current["actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["launched_at"] or now,
                    metadata={
                        "dataset_id": str(fields.get("dataset_id") or ""),
                        "key_value_store_id": str(
                            fields.get("key_value_store_id") or ""
                        ),
                    },
                )
            values = {"status": status, **fields, "updated_at": now}
            if milestone and not current[milestone]:
                values[milestone] = now
            assignments = ",".join(f"{name}=?" for name in values)
            compare_status = str(current["status"])
            cursor = conn.execute(
                f"UPDATE paid_access_probe_batches SET {assignments} "
                "WHERE id=? AND status=?",
                tuple(values.values()) + (batch_id, compare_status),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"paid access probe batch {batch_id} lost atomic transition "
                    f"from {compare_status}"
                )
            updated = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(updated)

    def clamp_paid_access_probe_max_charge(
        self,
        batch_id: int,
        upper_bound_usd: float,
    ) -> dict[str, Any]:
        """Atomically lower a prepared probe's provider-side charge ceiling."""

        upper_bound = max(0.0, float(upper_bound_usd))
        now = utcnow()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown paid access probe batch: {batch_id}")
            if str(row["status"]) != "prepared":
                raise RuntimeError(
                    f"paid access probe batch {batch_id} is {row['status']}, expected prepared"
                )
            clamped = min(max(0.0, float(row["max_charge_usd"])), upper_bound)
            conn.execute(
                "UPDATE paid_access_probe_batches SET max_charge_usd=?,updated_at=? "
                "WHERE id=? AND status='prepared'",
                (clamped, now, batch_id),
            )
            updated = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(updated)

    def reconcile_paid_access_probe_batch(
        self,
        batch_id: int,
        *,
        run_id: str | None = None,
        dataset_id: str | None = None,
        key_value_store_id: str | None = None,
        confirm_not_launched: bool = False,
    ) -> dict[str, Any]:
        """Resolve one ambiguous access probe without ever buying it again.

        An operator may either attach a provider run that is known to have
        started, allowing the normal raw-save/replay path to resume, or attest
        that no run was launched.  The latter closes this request identity as
        failed; it never returns the same batch to ``prepared``.
        """

        normalized_run_id = str(run_id or "").strip()
        if bool(normalized_run_id) == bool(confirm_not_launched):
            raise ValueError("provide exactly one of run_id or confirm_not_launched")
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid access probe batch: {batch_id}")
            if str(current["status"]) != "needs_reconcile":
                raise RuntimeError(
                    f"paid access probe batch {batch_id} is {current['status']}, "
                    "expected needs_reconcile"
                )
            diagnostic_id = int(current["actor_run_id"] or 0)
            diagnostic = (
                conn.execute(
                    "SELECT run_id,charged_usd FROM actor_runs WHERE id=?",
                    (diagnostic_id,),
                ).fetchone()
                if diagnostic_id
                else None
            )
            evidence_rows = [dict(current)]
            if diagnostic:
                evidence_rows.append(dict(diagnostic))
            registry_evidence = _provider_run_ownership_evidence(
                conn, "paid_access_probe_batch", batch_id
            )
            if registry_evidence:
                evidence_rows.append(registry_evidence)
            if confirm_not_launched and _provider_evidence_present(*evidence_rows):
                raise RuntimeError(
                    "公開探測批次已有 provider run/dataset/charge 證據；"
                    "不可確認為未啟動"
                )
            resolved_run_id: str | None = None
            resolved_dataset_id: str | None = None
            resolved_store_id: str | None = None
            if normalized_run_id:
                (
                    resolved_run_id,
                    resolved_dataset_id,
                    resolved_store_id,
                ) = _resolve_provider_attach(
                    *evidence_rows,
                    run_id=normalized_run_id,
                    dataset_id=dataset_id,
                    key_value_store_id=key_value_store_id,
                )
                _claim_provider_run_ownership(
                    conn,
                    provider=str(current["provider"] or ""),
                    run_id=resolved_run_id,
                    owner_type="paid_access_probe_batch",
                    owner_id=batch_id,
                    actor_id=str(current["actor_id"] or ""),
                    request_fingerprint=str(current["request_hash"] or ""),
                    provider_started_at=current["launched_at"] or now,
                    metadata={
                        "dataset_id": str(resolved_dataset_id or ""),
                        "key_value_store_id": str(resolved_store_id or ""),
                        "attached_by": "operator_reconcile",
                    },
                )
            from .capture_v2 import validate_batch_transition

            validate_batch_transition(
                str(current["status"]),
                "run_started" if normalized_run_id else "failed",
            )

            if normalized_run_id:
                conn.execute(
                    """UPDATE paid_access_probe_batches
                    SET status='run_started',run_id=?,dataset_id=?,key_value_store_id=?,
                        launched_at=COALESCE(launched_at,?),error=NULL,updated_at=?
                    WHERE id=? AND status='needs_reconcile'""",
                    (
                        resolved_run_id,
                        resolved_dataset_id,
                        resolved_store_id,
                        now,
                        now,
                        batch_id,
                    ),
                )
                if diagnostic_id:
                    conn.execute(
                    """UPDATE actor_runs SET status='running',run_id=?,error=NULL,
                        finished_at=NULL WHERE id=?""",
                        (resolved_run_id, diagnostic_id),
                    )
            else:
                reason = "operator confirmed provider run was not launched"
                conn.execute(
                    """UPDATE paid_access_probe_batches
                    SET status='failed',run_id=NULL,dataset_id=NULL,
                        key_value_store_id=NULL,error=?,updated_at=?
                    WHERE id=? AND status='needs_reconcile'""",
                    (reason, now, batch_id),
                )
                if diagnostic_id:
                    conn.execute(
                        """UPDATE actor_runs SET status='failed',error=?,finished_at=?
                        WHERE id=?""",
                        (reason, now, diagnostic_id),
                    )
            updated = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(updated)

    @staticmethod
    def _paid_budget_reservations_in_connection(
        conn: sqlite3.Connection,
        *,
        result_price: float,
        excluding_source_batch_id: int | None = None,
        excluding_access_probe_batch_id: int | None = None,
        excluding_photo_batch_id: int | None = None,
    ) -> dict[str, float]:
        source_unsettled = access_unsettled = contract_unsettled = photo_unsettled = 0.0
        legacy_actor_unsettled = 0.0
        source_rows = conn.execute(
            """SELECT id,normalized_input_json,max_charge_usd,charged_usd
            FROM paid_source_batches
            WHERE status IN ('launching','run_started','needs_reconcile')"""
        ).fetchall()
        for row in source_rows:
            if excluding_source_batch_id is not None and int(row["id"]) == int(
                excluding_source_batch_id
            ):
                continue
            try:
                normalized = json.loads(str(row["normalized_input_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                normalized = {}
            maximum = max(
                1,
                min(
                    50,
                    int(
                        normalized.get("maxPostsPerProfile")
                        or normalized.get("maxPosts")
                        or 50
                    ),
                ),
            )
            charged = max(0.0, float(row["charged_usd"] or 0))
            stored_ceiling = max(0.0, float(row["max_charge_usd"] or 0))
            # V2.1 persists the accepted launch ceiling.  A zero belongs to a
            # pre-migration row, where the best available conservative floor
            # is its known charge or a derivation using today's unit price.
            ceiling = (
                stored_ceiling
                if stored_ceiling > 0
                else max(
                    charged,
                    float(provider_charge_ceiling(maximum * result_price)),
                )
            )
            source_unsettled += max(0.0, ceiling - charged)

        access_rows = conn.execute(
            """SELECT id,max_charge_usd,charged_usd
            FROM paid_access_probe_batches
            WHERE status IN ('launching','run_started','needs_reconcile')"""
        ).fetchall()
        for row in access_rows:
            if excluding_access_probe_batch_id is not None and int(row["id"]) == int(
                excluding_access_probe_batch_id
            ):
                continue
            access_unsettled += max(
                0.0,
                float(row["max_charge_usd"] or 0) - float(row["charged_usd"] or 0),
            )

        photo_rows = conn.execute(
            """SELECT id,max_charge_usd,charged_usd
            FROM paid_photo_batches
            WHERE status IN ('launching','run_started','needs_reconcile')"""
        ).fetchall()
        for row in photo_rows:
            if excluding_photo_batch_id is not None and int(row["id"]) == int(
                excluding_photo_batch_id
            ):
                continue
            photo_unsettled += max(
                0.0,
                float(row["max_charge_usd"] or 0) - float(row["charged_usd"] or 0),
            )

        allocations = conn.execute(
            """SELECT a.id,a.authorized_usd,g.status AS grant_status,
            COALESCE(j.status,'') AS job_status,
            COALESCE(SUM(cr.charged_usd),0) AS charged_usd
            FROM contract_test_allocations a
            JOIN contract_test_grants g ON g.id=a.grant_id
            LEFT JOIN jobs j ON j.id=a.job_id
            LEFT JOIN contract_runs cr ON cr.grant_allocation_id=a.id
            GROUP BY a.id"""
        ).fetchall()
        for allocation in allocations:
            ambiguous = conn.execute(
                """SELECT COALESCE(SUM(MAX(authorized_max_usd-charged_usd,0)),0)
                FROM contract_runs WHERE grant_allocation_id=?
                  AND status IN ('launching','run_started','needs_reconcile')""",
                (allocation["id"],),
            ).fetchone()[0]
            active_remaining = 0.0
            if (
                str(allocation["grant_status"]) == "active"
                and str(allocation["job_status"])
                in {"pending", "running", "needs_reconcile"}
            ):
                active_remaining = max(
                    0.0,
                    float(allocation["authorized_usd"] or 0)
                    - float(allocation["charged_usd"] or 0),
                )
            contract_unsettled += max(active_remaining, float(ambiguous or 0))

        # A staged rollback may still expose the old V1 Actor path.  Preserve
        # its accepted provider ceiling in this same global reservation pool so
        # two processes cannot both spend the same lagging official balance.
        for row in conn.execute(
            """SELECT ar.max_charge_usd,ar.charged_usd
            FROM actor_runs ar
            WHERE ar.status IN ('running','needs_reconcile')
              AND COALESCE(ar.max_charge_usd,0)>0
              AND NOT EXISTS(
                SELECT 1 FROM paid_photo_batches ppb WHERE ppb.actor_run_id=ar.id
              )
              AND NOT EXISTS(
                SELECT 1 FROM paid_access_probe_batches pab WHERE pab.actor_run_id=ar.id
              )"""
        ):
            legacy_actor_unsettled += max(
                0.0,
                float(row["max_charge_usd"] or 0)
                - float(row["charged_usd"] or 0),
            )

        return {
            "source_unsettled_usd": source_unsettled,
            "access_probe_unsettled_usd": access_unsettled,
            "photo_unsettled_usd": photo_unsettled,
            "contract_test_unsettled_usd": contract_unsettled,
            "legacy_actor_unsettled_usd": legacy_actor_unsettled,
            "total_unsettled_usd": (
                source_unsettled
                + access_unsettled
                + photo_unsettled
                + contract_unsettled
                + legacy_actor_unsettled
            ),
        }

    def paid_budget_reservations(
        self,
        *,
        posts_result_price_usd: float,
        excluding_source_batch_id: int | None = None,
        excluding_access_probe_batch_id: int | None = None,
        excluding_photo_batch_id: int | None = None,
    ) -> dict[str, float]:
        """Read every durable paid ledger in one SQLite snapshot.

        Official provider usage can lag an accepted run.  These reservations
        therefore cover the still-unsettled maximum of source batches, access
        probes, and operator-authorized contract-test allocations.
        """

        result_price = max(0.0, float(posts_result_price_usd))
        with self.connect() as conn:
            conn.execute("BEGIN")
            return self._paid_budget_reservations_in_connection(
                conn,
                result_price=result_price,
                excluding_source_batch_id=excluding_source_batch_id,
                excluding_access_probe_batch_id=excluding_access_probe_batch_id,
                excluding_photo_batch_id=excluding_photo_batch_id,
            )

    @staticmethod
    def _apify_settled_charge_floor_in_connection(
        conn: sqlite3.Connection,
        cycle_start_at: str,
        *,
        posts_result_price_usd: float,
    ) -> float:
        unsettled = {"launching", "run_started", "needs_reconcile"}

        def settled_or_ceiling(row: sqlite3.Row, ceiling: float) -> float:
            charged = max(0.0, float(row["charged_usd"] or 0))
            if charged > 0 or str(row["status"] or "") in unsettled:
                return charged
            return max(0.0, ceiling)

        floor = 0.0
        result_price = max(0.0, float(posts_result_price_usd))
        for row in conn.execute(
            """SELECT status,charged_usd,max_charge_usd,normalized_input_json
            FROM paid_source_batches
            WHERE COALESCE(launched_at,created_at)>=?
              AND status<>'prepared'
              AND NOT(status='failed' AND NULLIF(TRIM(run_id),'') IS NULL
                      AND NULLIF(TRIM(dataset_id),'') IS NULL
                      AND NULLIF(TRIM(key_value_store_id),'') IS NULL
                      AND COALESCE(charged_usd,0)<=0)""",
            (cycle_start_at,),
        ):
            try:
                normalized = json.loads(str(row["normalized_input_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                normalized = {}
            maximum = max(
                1,
                min(
                    50,
                    int(
                        normalized.get("maxPostsPerProfile")
                        or normalized.get("maxPosts")
                        or 50
                    ),
                ),
            )
            stored_ceiling = max(0.0, float(row["max_charge_usd"] or 0))
            historical_fallback = max(
                max(0.0, float(row["charged_usd"] or 0)),
                float(provider_charge_ceiling(maximum * result_price)),
            )
            floor += settled_or_ceiling(
                row,
                stored_ceiling if stored_ceiling > 0 else historical_fallback,
            )
        for table in ("paid_access_probe_batches", "paid_photo_batches"):
            for row in conn.execute(
                f"""SELECT status,charged_usd,max_charge_usd FROM {table}
                WHERE COALESCE(launched_at,created_at)>=?
                  AND status<>'prepared'
                  AND NOT(status='failed' AND NULLIF(TRIM(run_id),'') IS NULL
                          AND NULLIF(TRIM(dataset_id),'') IS NULL
                          AND NULLIF(TRIM(key_value_store_id),'') IS NULL
                          AND COALESCE(charged_usd,0)<=0)""",
                (cycle_start_at,),
            ):
                floor += settled_or_ceiling(
                    row, max(0.0, float(row["max_charge_usd"] or 0))
                )
        for row in conn.execute(
            """SELECT status,charged_usd,authorized_max_usd FROM contract_runs
            WHERE COALESCE(leased_at,started_at)>=? AND status<>'pending'
              AND NOT(status='failed' AND NULLIF(TRIM(run_id),'') IS NULL
                      AND NULLIF(TRIM(dataset_id),'') IS NULL
                      AND COALESCE(charged_usd,0)<=0)""",
            (cycle_start_at,),
        ):
            floor += settled_or_ceiling(
                row, max(0.0, float(row["authorized_max_usd"] or 0))
            )
        unlinked = conn.execute(
            """SELECT COALESCE(SUM(MAX(ar.charged_usd,0)),0) AS charged
            FROM actor_runs ar WHERE ar.started_at>=?
              AND NOT EXISTS(
                SELECT 1 FROM paid_photo_batches ppb WHERE ppb.actor_run_id=ar.id
              )
              AND NOT EXISTS(
                SELECT 1 FROM paid_access_probe_batches pab WHERE pab.actor_run_id=ar.id
              )""",
            (cycle_start_at,),
        ).fetchone()
        floor += max(0.0, float(unlinked["charged"] or 0))
        return floor

    def apify_settled_charge_floor(
        self, cycle_start_at: str, *, posts_result_price_usd: float
    ) -> float:
        """Return locally known settled Apify charges for one billing cycle.

        Provider usage can lag just-finished runs. Durable V2 ledgers are the
        canonical rows for their runs; legacy/unlinked ``actor_runs`` are added
        separately so the same charge is never counted twice.
        """

        with self.connect() as conn:
            conn.execute("BEGIN")
            floor = self._apify_settled_charge_floor_in_connection(
                conn,
                cycle_start_at,
                posts_result_price_usd=posts_result_price_usd,
            )
        return floor

    @classmethod
    def _atomic_paid_capacity_in_connection(
        cls,
        conn: sqlite3.Connection,
        *,
        cycle_start_at: str,
        monthly_limit_usd: float,
        provider_used_usd: float,
        baseline_local_settled_usd: float,
        outstanding_reserve_usd: float,
        posts_result_price_usd: float,
    ) -> float:
        """Recheck newly settled charges inside the launch claim transaction."""

        current_floor = cls._apify_settled_charge_floor_in_connection(
            conn,
            cycle_start_at,
            posts_result_price_usd=posts_result_price_usd,
        )
        baseline = max(0.0, float(baseline_local_settled_usd))
        effective_used = max(max(0.0, float(provider_used_usd)), baseline) + max(
            0.0, current_floor - baseline
        )
        return max(
            0.0,
            float(monthly_limit_usd)
            - effective_used
            - max(0.0, float(outstanding_reserve_usd)),
        )

    @staticmethod
    def _paid_access_committed_or_reserved_in_connection(
        conn: sqlite3.Connection,
        cycle_start_at: str,
        *,
        excluding_batch_id: int | None = None,
    ) -> float:
        """Count this cycle's access-probe spend and ambiguous ceilings once."""

        total = 0.0
        for row in conn.execute(
            """SELECT id,status,run_id,dataset_id,key_value_store_id,
            charged_usd,max_charge_usd
            FROM paid_access_probe_batches
            WHERE COALESCE(launched_at,created_at)>=? AND status<>'prepared'""",
            (cycle_start_at,),
        ):
            if excluding_batch_id is not None and int(row["id"]) == int(
                excluding_batch_id
            ):
                continue
            status = str(row["status"] or "")
            charged = max(0.0, float(row["charged_usd"] or 0))
            ceiling = max(0.0, float(row["max_charge_usd"] or 0))
            if status in {"launching", "run_started", "needs_reconcile"}:
                total += max(charged, ceiling)
            elif not (
                status == "failed"
                and not _provider_evidence_present(dict(row))
            ):
                total += charged if charged > 0 else ceiling
        return total

    def claim_paid_access_probe_launch(
        self,
        batch_id: int,
        *,
        global_capacity_usd: float,
        detection_capacity_usd: float,
        posts_result_price_usd: float,
        cycle_start_at: str | None = None,
        monthly_limit_usd: float | None = None,
        provider_used_usd: float = 0,
        baseline_local_settled_usd: float = 0,
        outstanding_reserve_usd: float = 0,
        detection_limit_usd: float | None = None,
        detection_historical_used_usd: float = 0,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically clamp and claim one prepared probe across every ledger."""

        result_price = max(0.0, float(posts_result_price_usd))
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"unknown paid access probe batch: {batch_id}")
            if str(current["status"]) != "prepared":
                return dict(current), False
            reservations = self._paid_budget_reservations_in_connection(
                conn,
                result_price=result_price,
                excluding_access_probe_batch_id=batch_id,
            )
            atomic_global_capacity = max(0.0, float(global_capacity_usd))
            if cycle_start_at and monthly_limit_usd is not None:
                atomic_global_capacity = min(
                    atomic_global_capacity,
                    self._atomic_paid_capacity_in_connection(
                        conn,
                        cycle_start_at=cycle_start_at,
                        monthly_limit_usd=monthly_limit_usd,
                        provider_used_usd=provider_used_usd,
                        baseline_local_settled_usd=baseline_local_settled_usd,
                        outstanding_reserve_usd=outstanding_reserve_usd,
                        posts_result_price_usd=result_price,
                    ),
                )
            global_remaining = max(
                0.0,
                atomic_global_capacity
                - float(reservations["total_unsettled_usd"]),
            )
            atomic_detection_capacity = max(
                0.0, float(detection_capacity_usd)
            )
            if cycle_start_at and detection_limit_usd is not None:
                atomic_detection_capacity = min(
                    atomic_detection_capacity,
                    max(
                        0.0,
                        float(detection_limit_usd)
                        - max(0.0, float(detection_historical_used_usd))
                        - self._paid_access_committed_or_reserved_in_connection(
                            conn,
                            cycle_start_at,
                            excluding_batch_id=batch_id,
                        ),
                    ),
                )
            # The cycle-aware helper already counts terminal charges and the
            # full ceiling of every ambiguous access probe.  Subtracting the
            # access reservation again would double-count it and reject a
            # batch that exactly fits the detection sub-budget.
            detection_remaining = (
                atomic_detection_capacity
                if cycle_start_at and detection_limit_usd is not None
                else max(
                    0.0,
                    atomic_detection_capacity
                    - float(reservations["access_probe_unsettled_usd"]),
                )
            )
            clamped = float(
                provider_charge_ceiling(
                    min(
                        max(0.0, float(current["max_charge_usd"] or 0)),
                        global_remaining,
                        detection_remaining,
                    )
                )
            )
            minimum = float(provider_charge_ceiling(result_price))
            if clamped <= 0 or clamped + 1e-12 < minimum:
                row = conn.execute(
                    "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
                ).fetchone()
                return dict(row), False
            cursor = conn.execute(
                """UPDATE paid_access_probe_batches
                SET status='launching',max_charge_usd=?,
                    launched_at=COALESCE(launched_at,?),updated_at=?
                WHERE id=? AND status='prepared' AND run_id IS NULL""",
                (clamped, now, now, batch_id),
            )
            row = conn.execute(
                "SELECT * FROM paid_access_probe_batches WHERE id=?", (batch_id,)
            ).fetchone()
            return dict(row), cursor.rowcount == 1

    def upsert_post_alias(
        self,
        profile_id: int,
        *,
        canonical_post_id: str,
        provider: str,
        alias_type: str,
        alias_value: str,
        entity_id: int | None = None,
        normalized_url: str | None = None,
        source_url: str | None = None,
    ) -> dict[str, Any]:
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO post_aliases(
                  profile_id,entity_id,canonical_post_id,provider,alias_type,alias_value,
                  normalized_url,source_url,first_seen_at,last_seen_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(profile_id,alias_type,alias_value) DO UPDATE SET
                  entity_id=COALESCE(excluded.entity_id,post_aliases.entity_id),
                  canonical_post_id=excluded.canonical_post_id,provider=excluded.provider,
                  normalized_url=COALESCE(excluded.normalized_url,post_aliases.normalized_url),
                  source_url=COALESCE(excluded.source_url,post_aliases.source_url),
                  last_seen_at=excluded.last_seen_at""",
                (
                    profile_id,
                    entity_id,
                    canonical_post_id,
                    provider,
                    alias_type,
                    alias_value,
                    normalized_url,
                    source_url,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM post_aliases WHERE profile_id=? AND alias_type=? AND alias_value=?",
                (profile_id, alias_type, alias_value),
            ).fetchone()
            return dict(row)

    def upsert_media_alias(
        self,
        profile_id: int,
        *,
        canonical_media_id: str,
        provider: str,
        alias_type: str,
        alias_value: str,
        entity_id: int | None = None,
        media_id: int | None = None,
        source_url: str | None = None,
        width: int | None = None,
        height: int | None = None,
        mime_type: str | None = None,
        sha256: str | None = None,
    ) -> dict[str, Any]:
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO media_aliases(
                  profile_id,entity_id,media_id,canonical_media_id,provider,alias_type,alias_value,
                  source_url,width,height,mime_type,sha256,first_seen_at,last_seen_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(profile_id,alias_type,alias_value) DO UPDATE SET
                  entity_id=COALESCE(excluded.entity_id,media_aliases.entity_id),
                  media_id=COALESCE(excluded.media_id,media_aliases.media_id),
                  canonical_media_id=excluded.canonical_media_id,provider=excluded.provider,
                  source_url=COALESCE(excluded.source_url,media_aliases.source_url),
                  width=COALESCE(excluded.width,media_aliases.width),
                  height=COALESCE(excluded.height,media_aliases.height),
                  mime_type=COALESCE(excluded.mime_type,media_aliases.mime_type),
                  sha256=COALESCE(excluded.sha256,media_aliases.sha256),
                  last_seen_at=excluded.last_seen_at""",
                (
                    profile_id,
                    entity_id,
                    media_id,
                    canonical_media_id,
                    provider,
                    alias_type,
                    alias_value,
                    source_url,
                    width,
                    height,
                    mime_type,
                    sha256,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM media_aliases WHERE profile_id=? AND alias_type=? AND alias_value=?",
                (profile_id, alias_type, alias_value),
            ).fetchone()
            return dict(row)

    def get_browser_limit(
        self,
        *,
        browser_identity: str = "default",
        scope_type: str = "global",
        scope_id: str = "",
    ) -> dict[str, Any]:
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO browser_limits(
                  browser_identity,scope_type,scope_id,updated_at
                ) VALUES(?,?,?,?)""",
                (browser_identity, scope_type, scope_id, now),
            )
            row = conn.execute(
                """SELECT * FROM browser_limits
                WHERE browser_identity=? AND scope_type=? AND scope_id=?""",
                (browser_identity, scope_type, scope_id),
            ).fetchone()
            return dict(row)

    def update_browser_limit(
        self,
        *,
        browser_identity: str = "default",
        scope_type: str = "global",
        scope_id: str = "",
        **fields: Any,
    ) -> dict[str, Any]:
        allowed = {
            "breaker_state", "breaker_reason", "blocked_until", "half_open_claimed_at",
            "next_allowed_at", "daily_date", "daily_batches", "window_started_at",
            "window_operations", "repeat_window_started_at", "repeat_count",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported browser limit fields: {sorted(unknown)}")
        self.get_browser_limit(
            browser_identity=browser_identity, scope_type=scope_type, scope_id=scope_id
        )
        if fields:
            fields["updated_at"] = utcnow()
            assignments = ",".join(f"{name}=?" for name in fields)
            self.execute(
                f"""UPDATE browser_limits SET {assignments}
                WHERE browser_identity=? AND scope_type=? AND scope_id=?""",
                tuple(fields.values()) + (browser_identity, scope_type, scope_id),
            )
        return self.get_browser_limit(
            browser_identity=browser_identity, scope_type=scope_type, scope_id=scope_id
        )

    def record_browser_evidence(
        self,
        *,
        evidence_key: str,
        event_type: str,
        path: str,
        sha256: str,
        captured_at: str,
        expires_at: str,
        browser_identity: str = "default",
        profile_id: int | None = None,
        access_observation_id: int | None = None,
        size_bytes: int = 0,
        width: int | None = None,
        height: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO browser_evidence(
                  evidence_key,browser_identity,profile_id,access_observation_id,event_type,path,sha256,
                  size_bytes,width,height,captured_at,expires_at,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    evidence_key,
                    browser_identity,
                    profile_id,
                    access_observation_id,
                    event_type,
                    path,
                    sha256,
                    size_bytes,
                    width,
                    height,
                    captured_at,
                    expires_at,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                ),
            )
            row = conn.execute("SELECT * FROM browser_evidence WHERE evidence_key=?", (evidence_key,)).fetchone()
            return dict(row), bool(cursor.rowcount)

    def upsert_profile_name_candidate(
        self,
        profile_id: int,
        candidate_name: str,
        *,
        source: str,
        auth_scope: str = "unknown",
        trust_level: int = 0,
        status: str = "candidate",
        is_current: bool = False,
        manual_locked: bool = False,
        rejection_reason: str | None = None,
        access_observation_id: int | None = None,
        observed_at: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        candidate_name = " ".join(candidate_name.split())
        if not candidate_name:
            raise ValueError("candidate name must not be empty")
        normalized = _normalized_name(candidate_name)
        observed_at = observed_at or utcnow()
        with self.connect() as conn:
            if is_current:
                conn.execute(
                    "UPDATE profile_name_candidates SET is_current=0 WHERE profile_id=?", (profile_id,)
                )
            conn.execute(
                """INSERT INTO profile_name_candidates(
                  profile_id,candidate_name,normalized_name,source,auth_scope,trust_level,status,
                  is_current,manual_locked,rejection_reason,access_observation_id,first_seen_at,last_seen_at,
                  observation_count,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)
                ON CONFLICT(profile_id,normalized_name,source) DO UPDATE SET
                  candidate_name=excluded.candidate_name,auth_scope=excluded.auth_scope,
                  trust_level=MAX(profile_name_candidates.trust_level,excluded.trust_level),
                  status=excluded.status,is_current=MAX(profile_name_candidates.is_current,excluded.is_current),
                  manual_locked=MAX(profile_name_candidates.manual_locked,excluded.manual_locked),
                  rejection_reason=excluded.rejection_reason,
                  access_observation_id=COALESCE(excluded.access_observation_id,profile_name_candidates.access_observation_id),
                  last_seen_at=excluded.last_seen_at,
                  observation_count=profile_name_candidates.observation_count+1,
                  metadata_json=excluded.metadata_json""",
                (
                    profile_id,
                    candidate_name,
                    normalized,
                    source,
                    auth_scope,
                    trust_level,
                    status,
                    int(is_current),
                    int(manual_locked),
                    rejection_reason,
                    access_observation_id,
                    observed_at,
                    observed_at,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                ),
            )
            row = conn.execute(
                """SELECT * FROM profile_name_candidates
                WHERE profile_id=? AND normalized_name=? AND source=?""",
                (profile_id, normalized, source),
            ).fetchone()
            return dict(row)

    def set_profile_source_control(
        self,
        profile_id: int,
        source: str,
        *,
        frozen: bool,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utcnow()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO profile_source_controls(
                  profile_id,source,frozen,reason,frozen_at,unfrozen_at,metadata_json,updated_at
                ) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(profile_id,source) DO UPDATE SET
                  frozen=excluded.frozen,reason=excluded.reason,
                  frozen_at=CASE WHEN excluded.frozen=1 THEN excluded.updated_at
                                 ELSE profile_source_controls.frozen_at END,
                  unfrozen_at=CASE WHEN excluded.frozen=0 THEN excluded.updated_at
                                   ELSE profile_source_controls.unfrozen_at END,
                  metadata_json=excluded.metadata_json,updated_at=excluded.updated_at""",
                (
                    profile_id,
                    source,
                    int(frozen),
                    reason,
                    now if frozen else None,
                    now if not frozen else None,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            if source == "apify":
                conn.execute(
                    "UPDATE profiles SET apify_frozen=?,updated_at=? WHERE id=?",
                    (int(frozen), now, profile_id),
                )
            row = conn.execute(
                "SELECT * FROM profile_source_controls WHERE profile_id=? AND source=?",
                (profile_id, source),
            ).fetchone()
            return dict(row)

    def profile_source_frozen(self, profile_id: int, source: str) -> bool:
        row = self.row(
            "SELECT frozen FROM profile_source_controls WHERE profile_id=? AND source=?",
            (profile_id, source),
        )
        if row is not None:
            return bool(row["frozen"])
        if source == "apify":
            legacy = self.row("SELECT apify_frozen FROM profiles WHERE id=?", (profile_id,))
            return bool(legacy and legacy["apify_frozen"])
        return False

    def add_event(self, event_key: str, event_type: str, payload: dict[str, Any], profile_id: int | None = None, entity_id: int | None = None, notify: bool = True, coalesce: bool = False, coalesce_minutes: int = 15) -> int | None:
        now = utcnow()
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO events(event_key,profile_id,entity_id,event_type,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                (event_key, profile_id, entity_id, event_type, json.dumps(payload, ensure_ascii=False), now),
            )
            if not cur.rowcount:
                return None
            event_id = int(cur.lastrowid)
            if notify and coalesce and entity_id is not None:
                ready_at = (datetime.now(UTC) + timedelta(minutes=coalesce_minutes)).isoformat()
                group = conn.execute("SELECT * FROM notification_groups WHERE entity_id=? AND status='pending' AND ready_at>? ORDER BY id DESC LIMIT 1", (entity_id, now)).fetchone()
                if group:
                    group_id = int(group["id"])
                    grouped = json.loads(group["payload_json"])
                    grouped.setdefault("items", []).append(payload)
                    conn.execute("UPDATE notification_groups SET payload_json=? WHERE id=?", (json.dumps(grouped, ensure_ascii=False), group_id))
                else:
                    grouped = {"items": [payload]}
                    cur_group = conn.execute("INSERT INTO notification_groups(profile_id,entity_id,payload_json,created_at,ready_at) VALUES(?,?,?,?,?)", (profile_id, entity_id, json.dumps(grouped, ensure_ascii=False), now, ready_at))
                    group_id = int(cur_group.lastrowid)
                    conn.execute("INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at,group_id) VALUES(?,?,?,?,?,?)", (event_id, "summary", json.dumps({"group_id": group_id}, ensure_ascii=False), ready_at, now, group_id))
                conn.execute("UPDATE events SET notification_group_id=? WHERE id=?", (group_id, event_id))
            elif notify:
                conn.execute(
                    "INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at) VALUES(?,?,?,?,?)",
                    (event_id, "text", json.dumps(payload, ensure_ascii=False), now, now),
                )
            return event_id

    def queue_group_media(self, event_id: int, sha256: str, payload: dict[str, Any]) -> bool:
        """Queue one actual file globally; URL/name changes cannot bypass this."""
        now = utcnow()
        with self.connect() as conn:
            event = conn.execute("SELECT notification_group_id FROM events WHERE id=?", (event_id,)).fetchone()
            if not event or not event["notification_group_id"]:
                return False
            exists = conn.execute("SELECT id FROM outbox WHERE kind='media' AND media_sha256=? LIMIT 1", (sha256,)).fetchone()
            if exists:
                return False
            group = conn.execute("SELECT ready_at FROM notification_groups WHERE id=?", (event["notification_group_id"],)).fetchone()
            conn.execute("INSERT INTO outbox(event_id,kind,payload_json,next_attempt_at,created_at,group_id,media_sha256) VALUES(?,?,?,?,?,?,?)", (event_id, "media", json.dumps(payload, ensure_ascii=False), group["ready_at"], now, event["notification_group_id"], sha256))
            return True

    def bind_media_notification(self, event_id: int, sha256: str, perceptual_hash: str | None = None) -> None:
        """Upgrade media rows produced by older ingestion code into a group."""
        with self.connect() as conn:
            event = conn.execute("SELECT notification_group_id FROM events WHERE id=?", (event_id,)).fetchone()
            if not event or not event["notification_group_id"]:
                return
            group_id = int(event["notification_group_id"])
            group = conn.execute("SELECT ready_at FROM notification_groups WHERE id=?", (group_id,)).fetchone()
            conn.execute("UPDATE outbox SET group_id=?,media_sha256=?,media_perceptual_hash=?,next_attempt_at=? WHERE event_id=? AND kind='media' AND group_id IS NULL", (group_id, sha256, perceptual_hash, group["ready_at"], event_id))
            rows = conn.execute("SELECT id,media_sha256,media_perceptual_hash FROM outbox WHERE kind='media' AND status IN ('pending','sent','failed','cancelled') ORDER BY id").fetchall()
            first_id: int | None = None
            for candidate in rows:
                same_file = candidate["media_sha256"] == sha256
                same_image = bool(perceptual_hash and candidate["media_perceptual_hash"] and _hash_distance(perceptual_hash, candidate["media_perceptual_hash"]) <= 6)
                if not same_file and not same_image:
                    continue
                if first_id is None:
                    first_id = int(candidate["id"])
                    continue
                duplicate = candidate
                conn.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='duplicate media content' WHERE id=? AND status='pending'", (utcnow(), duplicate["id"]))

    def establish_notification_baseline(self) -> dict[str, int]:
        """Cancel legacy backlog and silently accept the first post-upgrade scan."""
        now = utcnow()
        with self.connect() as conn:
            pending = conn.execute("SELECT COUNT(*) FROM outbox WHERE status='pending'").fetchone()[0]
            entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            conn.execute("UPDATE entities SET notification_hash=NULL")
            conn.execute("UPDATE notification_groups SET status='cancelled',cancelled_at=? WHERE status='pending'", (now,))
            conn.execute("UPDATE outbox SET status='cancelled',cancelled_at=?,last_error='notification baseline reset' WHERE status='pending'", (now,))
            return {"entities": int(entities), "cancelled_outbox": int(pending)}

    def start_maintenance_run(self, task_name: str) -> int:
        return self.execute("INSERT INTO maintenance_runs(task_name,status,started_at) VALUES(?,'running',?)", (task_name, utcnow()))

    def finish_maintenance_run(self, run_id: int, summary: dict[str, Any], error: str | None = None) -> None:
        self.execute("UPDATE maintenance_runs SET status=?,summary_json=?,error=?,finished_at=? WHERE id=?", ("failed" if error else "done", json.dumps(summary, ensure_ascii=False), error, utcnow(), run_id))

    def usage_total(self, month: str) -> float:
        row = self.row("SELECT COALESCE(SUM(estimated_usd),0) total FROM usage WHERE month=?", (month,))
        return float(row["total"] if row else 0)

    def add_usage(self, month: str, category: str, results: int, estimated_usd: float) -> None:
        now = utcnow()
        self.execute(
            """INSERT INTO usage(month,category,estimated_usd,results,updated_at) VALUES(?,?,?,?,?)
            ON CONFLICT(month,category) DO UPDATE SET estimated_usd=estimated_usd+excluded.estimated_usd,
            results=results+excluded.results,updated_at=excluded.updated_at""",
            (month, category, estimated_usd, results, now),
        )

    def save_apify_usage(self, used_usd: float, cycle_start_at: str, cycle_end_at: str) -> None:
        self.execute(
            """INSERT INTO apify_usage_snapshot(id,used_usd,cycle_start_at,cycle_end_at,fetched_at)
            VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET used_usd=excluded.used_usd,
            cycle_start_at=excluded.cycle_start_at,cycle_end_at=excluded.cycle_end_at,fetched_at=excluded.fetched_at""",
            (used_usd, cycle_start_at, cycle_end_at, utcnow()),
        )

    def apify_usage_snapshot(self) -> dict[str, Any] | None:
        return self.row("SELECT used_usd,cycle_start_at,cycle_end_at,fetched_at FROM apify_usage_snapshot WHERE id=1")

    def save_serpapi_usage(self, account: Any) -> None:
        self.execute(
            """INSERT INTO serpapi_usage_snapshot(
              id,plan_name,searches_per_month,searches_left,this_month_usage,renewal_date,
              this_hour_searches,rate_limit_per_hour,fetched_at
            ) VALUES(1,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
              plan_name=excluded.plan_name,searches_per_month=excluded.searches_per_month,
              searches_left=excluded.searches_left,this_month_usage=excluded.this_month_usage,
              renewal_date=excluded.renewal_date,this_hour_searches=excluded.this_hour_searches,
              rate_limit_per_hour=excluded.rate_limit_per_hour,fetched_at=excluded.fetched_at""",
            (account.plan_name, account.searches_per_month, account.searches_left, account.this_month_usage,
             account.renewal_date, account.this_hour_searches, account.rate_limit_per_hour, utcnow()),
        )

    def serpapi_usage_snapshot(self) -> dict[str, Any] | None:
        return self.row("SELECT * FROM serpapi_usage_snapshot WHERE id=1")

    def record_serpapi_profile_attempts(
        self, profile_id: int, attempts: list[dict[str, Any]]
    ) -> None:
        """Persist each charged profile-search outcome for reliability reports."""
        if not attempts:
            return
        now = utcnow()
        rows = [
            (
                profile_id,
                str(attempt.get("query") or ""),
                str(attempt.get("status") or "error"),
                str(attempt.get("error") or "")[:1000] or None,
                str(attempt.get("attempted_at") or now),
            )
            for attempt in attempts
        ]
        with self.connect() as conn:
            conn.executemany(
                """INSERT INTO serpapi_profile_attempts(
                  profile_id,query_id,status,error,attempted_at
                ) VALUES(?,?,?,?,?)""",
                rows,
            )

    def migration_applied(self, name: str) -> bool:
        return self.row("SELECT name FROM schema_migrations WHERE name=?", (name,)) is not None

    def mark_migration(self, name: str, details: dict[str, Any] | None = None) -> None:
        self.execute(
            "INSERT OR REPLACE INTO schema_migrations(name,applied_at,details_json) VALUES(?,?,?)",
            (name, utcnow(), json.dumps(details or {}, ensure_ascii=False)),
        )

    def claim_legacy_actor_run_launch(
        self,
        profile_id: int | None,
        category: str,
        actor_id: str,
        input_variant: str,
        payload: dict[str, Any],
        *,
        max_charge_usd: float,
        cycle_start_at: str,
        monthly_limit_usd: float,
        provider_used_usd: float,
        baseline_local_settled_usd: float,
        posts_result_price_usd: float,
    ) -> tuple[int | None, str | None]:
        """Atomically reserve the provider ceiling for a legacy V1 call.

        V1 remains disabled by default, but a rollback deployment may still
        enable it.  The old read-then-launch sequence allowed two processes to
        spend the same provider balance while the official usage API lagged.
        This claim shares the V2 ledgers' ``BEGIN IMMEDIATE`` budget snapshot.
        """

        def redact(value: Any) -> Any:
            if isinstance(value, dict):
                return {
                    key: (
                        "***"
                        if any(
                            secret in key.lower()
                            for secret in ("token", "cookie", "password", "secret")
                        )
                        else redact(item)
                    )
                    for key, item in value.items()
                }
            if isinstance(value, list):
                return [redact(item) for item in value]
            return value

        ceiling = float(provider_charge_ceiling(max_charge_usd))
        if ceiling <= 0:
            return None, "zero_provider_ceiling"
        now = utcnow()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if profile_id is not None:
                frozen = conn.execute(
                    """SELECT 1 FROM profiles p
                    LEFT JOIN profile_source_controls psc
                      ON psc.profile_id=p.id AND psc.source='apify'
                    WHERE p.id=? AND (
                      COALESCE(p.apify_frozen,0)=1 OR COALESCE(psc.frozen,0)=1
                    )""",
                    (profile_id,),
                ).fetchone()
                if frozen is not None:
                    return None, "profile_apify_frozen"
            capacity = self._atomic_paid_capacity_in_connection(
                conn,
                cycle_start_at=cycle_start_at,
                monthly_limit_usd=monthly_limit_usd,
                provider_used_usd=provider_used_usd,
                baseline_local_settled_usd=baseline_local_settled_usd,
                outstanding_reserve_usd=0,
                posts_result_price_usd=posts_result_price_usd,
            )
            reservations = self._paid_budget_reservations_in_connection(
                conn,
                result_price=max(0.0, float(posts_result_price_usd)),
            )
            if (
                float(reservations["total_unsettled_usd"]) + ceiling
                > capacity + 1e-9
            ):
                return None, "monthly_budget_capacity"
            cursor = conn.execute(
                """INSERT INTO actor_runs(
                  profile_id,category,actor_id,input_variant,input_json,status,
                  max_charge_usd,started_at
                ) VALUES(?,?,?,?,?,'running',?,?)""",
                (
                    profile_id,
                    category,
                    actor_id,
                    input_variant,
                    json.dumps(redact(payload), ensure_ascii=False),
                    ceiling,
                    now,
                ),
            )
            return int(cursor.lastrowid), None

    def start_actor_run(self, profile_id: int | None, category: str, actor_id: str, input_variant: str, payload: dict[str, Any]) -> int:
        def redact(value: Any) -> Any:
            if isinstance(value, dict):
                return {key: ("***" if any(secret in key.lower() for secret in ("token", "cookie", "password", "secret")) else redact(item)) for key, item in value.items()}
            if isinstance(value, list):
                return [redact(item) for item in value]
            return value

        return self.execute(
            """INSERT INTO actor_runs(profile_id,category,actor_id,input_variant,input_json,status,started_at)
            VALUES(?,?,?,?,?,'running',?)""",
            (profile_id, category, actor_id, input_variant, json.dumps(redact(payload), ensure_ascii=False), utcnow()),
        )

    def finish_actor_run(
        self,
        diagnostic_id: int,
        *,
        status: str,
        run_id: str = "",
        result_count: int = 0,
        charged_usd: float = 0,
        summary: dict[str, Any] | None = None,
        error: str | None = None,
        samples: list[dict[str, Any]] | None = None,
        raw_result_count: int | None = None,
        parsed_result_count: int | None = None,
    ) -> None:
        self.execute(
            """UPDATE actor_runs SET status=?,run_id=?,result_count=?,charged_usd=?,summary_json=?,
            samples_json=COALESCE(?,samples_json),error=?,finished_at=?,
            raw_result_count=COALESCE(?,raw_result_count),
            parsed_result_count=COALESCE(?,parsed_result_count) WHERE id=?""",
            (
                status,
                run_id,
                result_count,
                charged_usd,
                json.dumps(summary, ensure_ascii=False) if summary is not None else None,
                json.dumps((samples or [])[:5], ensure_ascii=False) if samples is not None else None,
                error[:4000] if error else None,
                utcnow(),
                raw_result_count,
                parsed_result_count,
                diagnostic_id,
            ),
        )

    def update_actor_ingest_counts(
        self,
        diagnostic_id: int | None,
        *,
        new: int,
        updated: int,
        duplicate: int,
    ) -> None:
        if diagnostic_id is None:
            return
        self.execute(
            """UPDATE actor_runs SET new_result_count=?,updated_result_count=?,duplicate_result_count=?
            WHERE id=?""",
            (new, updated, duplicate, diagnostic_id),
        )
