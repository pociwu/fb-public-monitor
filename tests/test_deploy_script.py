from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "deploy.sh"


def test_deploy_freezes_old_writer_before_backup_and_build():
    source = SCRIPT.read_text(encoding="utf-8")

    tag = source.index('docker image tag "$CURRENT_IMAGE_ID" "$LAST_GOOD_TAG"')
    pause = source.index('docker pause "$CURRENT_CONTAINER"')
    verify_while_paused = source.index('paused_running_jobs="$(running_job_count)"')
    stop = source.index("docker compose stop --timeout 30 monitor")
    verify_after_stop = source.index('stopped_running_jobs="$(running_job_count)"')
    backup = source.index('backup_output="$("$PYTHON_BIN" scripts/backup_database.py backup')
    build = source.index("docker compose up -d --build")

    assert tag < pause < verify_while_paused < stop < verify_after_stop < backup < build


def test_deploy_failure_unpauses_or_restores_old_image_before_removing_flag():
    source = SCRIPT.read_text(encoding="utf-8")

    cleanup = source[source.index("cleanup() {") : source.index("running_job_count() {")]
    restore = source[source.index("restore_previous_service() {") : source.index("cleanup() {")]
    assert 'docker unpause "$CURRENT_CONTAINER"' in cleanup
    assert "restore_previous_service" in cleanup
    assert cleanup.index("restore_previous_service") < cleanup.index('rm -f -- "$MAINTENANCE_FLAG"')
    assert 'docker image tag "$CURRENT_IMAGE_ID" "$CURRENT_IMAGE_NAME"' in restore
    assert 'export APP_VERSION="$CURRENT_APP_VERSION"' in restore
    assert 'export APP_UPDATED_AT="$CURRENT_APP_UPDATED_AT"' in restore
    assert "docker compose up -d --no-build --force-recreate monitor" in restore


def test_deploy_records_previous_version_metadata_before_rebuild():
    source = SCRIPT.read_text(encoding="utf-8")

    inspect_version = source.index('CURRENT_APP_VERSION="$(docker inspect')
    inspect_updated = source.index('CURRENT_APP_UPDATED_AT="$(docker inspect')
    build = source.index("docker compose up -d --build")

    assert inspect_version < build
    assert inspect_updated < build


def test_deploy_keeps_host_writes_outside_container_owned_data_tree():
    source = SCRIPT.read_text(encoding="utf-8")

    assert (
        'DEPLOY_STATE_DIR="${FB_MONITOR_DEPLOY_STATE_DIR:-$APP_DIR/deploy-state}"'
        in source
    )
    assert 'BACKUP_DIR="${FB_MONITOR_BACKUP_DIR:-$APP_DIR/backups/deploy}"' in source
    assert 'MAINTENANCE_FLAG="$DEPLOY_STATE_DIR/deploy-maintenance"' in source
    assert 'mkdir -p -- "$DATA_DIR" "$DEPLOY_STATE_DIR" "$BACKUP_DIR"' in source
    assert 'chmod 755 -- "$DEPLOY_STATE_DIR"' in source
    assert 'chmod 700 -- "$BACKUP_DIR"' in source
    assert '$DATA_DIR/backups' not in source


def test_compose_mounts_the_host_deploy_state_read_only_for_the_scheduler():
    compose = (SCRIPT.parents[1] / "compose.yaml").read_text(encoding="utf-8")

    assert (
        '"${FB_MONITOR_DEPLOY_STATE_DIR:-./deploy-state}:/deploy-state:ro"'
        in compose
    )
    assert (
        'FB_MONITOR_DEPLOY_MAINTENANCE_FLAG: '
        '"${FB_MONITOR_DEPLOY_MAINTENANCE_FLAG:-/deploy-state/deploy-maintenance}"'
        in compose
    )
