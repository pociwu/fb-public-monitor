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
