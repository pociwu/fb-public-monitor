#!/usr/bin/env bash

set -euo pipefail

APP_DIR="${FB_MONITOR_DIR:-/home/ubuntu/fb-public-monitor}"
PYTHON_BIN="${FB_MONITOR_PYTHON:-python3}"
DATA_DIR="${FB_MONITOR_HOST_DATA_DIR:-$APP_DIR/data}"
DATABASE_PATH="${FB_MONITOR_DB_PATH:-$DATA_DIR/monitor.sqlite3}"
DEPLOY_STATE_DIR="${FB_MONITOR_DEPLOY_STATE_DIR:-$APP_DIR/deploy-state}"
BACKUP_DIR="${FB_MONITOR_BACKUP_DIR:-$APP_DIR/backups/deploy}"
MAINTENANCE_FLAG="$DEPLOY_STATE_DIR/deploy-maintenance"
DRAIN_TIMEOUT_SECONDS="${DEPLOY_DRAIN_TIMEOUT_SECONDS:-1800}"
DRAIN_POLL_SECONDS="${DEPLOY_DRAIN_POLL_SECONDS:-5}"
HEALTH_TIMEOUT_SECONDS="${DEPLOY_HEALTH_TIMEOUT_SECONDS:-240}"
HEALTH_POLL_SECONDS="${DEPLOY_HEALTH_POLL_SECONDS:-5}"

MAINTENANCE_OWNED=0
CURRENT_CONTAINER=""
CURRENT_CONTAINER_PAUSED=0
CURRENT_IMAGE_ID=""
CURRENT_IMAGE_NAME=""
CURRENT_APP_VERSION="development"
CURRENT_APP_UPDATED_AT=""
LAST_GOOD_TAG=""
PREVIOUS_SERVICE_STOPPED=0
DEPLOY_SUCCEEDED=0

restore_previous_service() {
  if [[ "$PREVIOUS_SERVICE_STOPPED" != "1" || "$DEPLOY_SUCCEEDED" == "1" ]]; then
    return
  fi
  if [[ -z "$CURRENT_IMAGE_ID" || -z "$CURRENT_IMAGE_NAME" ]]; then
    printf '部署失敗，且沒有可自動恢復的舊映像；monitor 服務維持停止。\n' >&2
    return
  fi

  printf '部署失敗；正在從 %s 恢復先前 monitor 映像。\n' \
    "${LAST_GOOD_TAG:-$CURRENT_IMAGE_ID}" >&2
  # compose.yaml reads these values from the current shell.  Restore the
  # previous container metadata too, otherwise a healthy last-good image
  # would misleadingly advertise the failed new commit in its footer.
  export APP_VERSION="$CURRENT_APP_VERSION"
  export APP_UPDATED_AT="$CURRENT_APP_UPDATED_AT"
  if docker image tag "$CURRENT_IMAGE_ID" "$CURRENT_IMAGE_NAME" \
      && docker compose up -d --no-build --force-recreate monitor; then
    printf '已重新啟動先前映像：%s\n' "$CURRENT_IMAGE_NAME" >&2
  else
    printf '自動恢復失敗；monitor 服務可能停止。可用映像：%s\n' \
      "${LAST_GOOD_TAG:-$CURRENT_IMAGE_ID}" >&2
  fi
}

cleanup() {
  status=$?
  set +e
  if [[ "$CURRENT_CONTAINER_PAUSED" == "1" && -n "$CURRENT_CONTAINER" ]]; then
    docker unpause "$CURRENT_CONTAINER" >/dev/null 2>&1
    CURRENT_CONTAINER_PAUSED=0
    printf '已解除舊 monitor 容器暫停狀態。\n' >&2
  fi
  restore_previous_service
  if [[ "$MAINTENANCE_OWNED" == "1" ]]; then
    rm -f -- "$MAINTENANCE_FLAG"
    MAINTENANCE_OWNED=0
    printf '\n已解除部署維護模式：%s\n' "$MAINTENANCE_FLAG"
  fi
  set -e
  return "$status"
}

running_job_count() {
  local count
  count="$("$PYTHON_BIN" scripts/backup_database.py running-jobs --source "$DATABASE_PATH")"
  if [[ ! "$count" =~ ^[0-9]+$ ]]; then
    printf '無法判讀執行中工作數量：%s\n' "$count" >&2
    return 1
  fi
  printf '%s' "$count"
}

trap cleanup EXIT
trap 'exit 130' INT TERM HUP

cd "$APP_DIR"

git pull --ff-only
export APP_VERSION
export APP_UPDATED_AT
APP_VERSION="$(git rev-parse --short HEAD)"
APP_UPDATED_AT="$(git show -s --format=%cI HEAD)"

printf '準備部署版本：%s\n更新時間：%s\n' "$APP_VERSION" "$APP_UPDATED_AT"
# The monitor container may own DATA_DIR with a UID that differs from the OCI
# login user.  Host-side deploy artifacts therefore live in dedicated,
# host-owned directories rather than below the application data bind mount.
mkdir -p -- "$DATA_DIR" "$DEPLOY_STATE_DIR" "$BACKUP_DIR"
chmod 755 -- "$DEPLOY_STATE_DIR"
chmod 700 -- "$BACKUP_DIR"
if [[ -e "$MAINTENANCE_FLAG" ]]; then
  printf '偵測到既有維護旗標，為避免重疊部署而中止：%s\n' "$MAINTENANCE_FLAG" >&2
  exit 1
fi
printf 'version=%s\nstarted_at=%s\n' "$APP_VERSION" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$MAINTENANCE_FLAG"
chmod 644 -- "$MAINTENANCE_FLAG"
MAINTENANCE_OWNED=1
printf '已進入部署維護模式：%s\n' "$MAINTENANCE_FLAG"

# Record the exact old image before draining. This is required on the first
# deployment of maintenance-aware code because the still-running old service
# does not yet honor deploy-maintenance.
CURRENT_CONTAINER="$(docker compose ps -q monitor 2>/dev/null || true)"
if [[ -n "$CURRENT_CONTAINER" ]]; then
  CURRENT_IMAGE_ID="$(docker inspect --format '{{.Image}}' "$CURRENT_CONTAINER")"
  CURRENT_IMAGE_NAME="$(docker inspect --format '{{.Config.Image}}' "$CURRENT_CONTAINER")"
  CURRENT_APP_VERSION="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$CURRENT_CONTAINER" \
    | awk -F= '$1 == "APP_VERSION" {print substr($0, index($0, "=") + 1); exit}')"
  CURRENT_APP_UPDATED_AT="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$CURRENT_CONTAINER" \
    | awk -F= '$1 == "APP_UPDATED_AT" {print substr($0, index($0, "=") + 1); exit}')"
  CURRENT_APP_VERSION="${CURRENT_APP_VERSION:-development}"
  running_version="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$CURRENT_CONTAINER" \
    | awk -F= '$1 == "APP_VERSION" {print $2; exit}')"
  if [[ -z "$running_version" || "$running_version" == "development" ]]; then
    running_version="${CURRENT_IMAGE_ID#sha256:}"
    running_version="${running_version:0:12}"
  fi
  safe_running_version="$(printf '%s' "$running_version" | tr -cs 'A-Za-z0-9_.-' '-')"
  last_good_repository="${FB_MONITOR_LAST_GOOD_REPOSITORY:-${COMPOSE_PROJECT_NAME:-$(basename "$APP_DIR")}-monitor}"
  LAST_GOOD_TAG="${last_good_repository}:last-good-${safe_running_version}"
  docker image tag "$CURRENT_IMAGE_ID" "$LAST_GOOD_TAG"
  printf '已保留目前映像：%s\n' "$LAST_GOOD_TAG"
else
  printf '目前沒有執行中的 monitor 容器；略過 last-good 映像標記。\n'
fi

drain_deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
while true; do
  running_jobs="$(running_job_count)"
  if [[ "$running_jobs" != "0" ]]; then
    if [[ -z "$CURRENT_CONTAINER" ]]; then
      printf '資料庫仍有 %s 筆 running 工作，但 monitor 容器未執行；安全中止。\n' \
        "$running_jobs" >&2
      exit 1
    fi
    if (( SECONDS >= drain_deadline )); then
      printf '等待執行中工作歸零逾時（仍有 %s 筆）；安全中止，未重建容器。\n' \
        "$running_jobs" >&2
      exit 1
    fi
    printf '仍有 %s 筆工作執行中；不會中斷可能計費的工作，%s 秒後再檢查。\n' \
      "$running_jobs" "$DRAIN_POLL_SECONDS"
    sleep "$DRAIN_POLL_SECONDS"
    continue
  fi

  if [[ -z "$CURRENT_CONTAINER" ]]; then
    printf '沒有舊 monitor writer，資料庫可以安全備份。\n'
    break
  fi

  # Close the zero-to-backup race for old images that ignore the maintenance
  # flag: freeze every process, re-read the DB, and only stop at a proven zero.
  docker pause "$CURRENT_CONTAINER" >/dev/null
  CURRENT_CONTAINER_PAUSED=1
  paused_running_jobs="$(running_job_count)"
  if [[ "$paused_running_jobs" != "0" ]]; then
    docker unpause "$CURRENT_CONTAINER" >/dev/null
    CURRENT_CONTAINER_PAUSED=0
    printf '暫停後發現 %s 筆工作剛被領取；恢復舊容器並繼續等待。\n' \
      "$paused_running_jobs"
    if (( SECONDS >= drain_deadline )); then
      printf '等待執行中工作歸零逾時；安全中止，未重建容器。\n' >&2
      exit 1
    fi
    sleep "$DRAIN_POLL_SECONDS"
    continue
  fi

  printf '舊 monitor 已暫停且 running 工作為 0；現在停止 writer。\n'
  docker compose stop --timeout 30 monitor
  CURRENT_CONTAINER_PAUSED=0
  PREVIOUS_SERVICE_STOPPED=1
  stopped_running_jobs="$(running_job_count)"
  if [[ "$stopped_running_jobs" != "0" ]]; then
    printf '停止 writer 後資料庫仍有 %s 筆 running 工作；中止部署並恢復舊服務。\n' \
      "$stopped_running_jobs" >&2
    exit 1
  fi
  printf '舊 monitor 已停止；不再有資料庫 writer。\n'
  break
done

if [[ -f "$DATABASE_PATH" ]]; then
  backup_output="$("$PYTHON_BIN" scripts/backup_database.py backup \
    --source "$DATABASE_PATH" --output-dir "$BACKUP_DIR" --keep 5)"
  printf '%s\n' "$backup_output"
else
  printf '尚無資料庫（首次部署）；略過部署前備份：%s\n' "$DATABASE_PATH"
fi

docker compose up -d --build

health_deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
while true; do
  new_container="$(docker compose ps -q monitor 2>/dev/null || true)"
  health_status=""
  container_status=""
  if [[ -n "$new_container" ]]; then
    health_status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$new_container" 2>/dev/null || true)"
    container_status="$(docker inspect --format '{{.State.Status}}' "$new_container" 2>/dev/null || true)"
  fi
  if [[ "$health_status" == "healthy" ]]; then
    printf '新版本健康檢查已通過：%s\n' "$APP_VERSION"
    break
  fi
  if [[ "$container_status" == "exited" || "$container_status" == "dead" ]]; then
    printf '新容器狀態為 %s，部署失敗。\n' "$container_status" >&2
    docker compose logs --tail=100 monitor || true
    exit 1
  fi
  if (( SECONDS >= health_deadline )); then
    printf '等待新版本健康檢查逾時（health=%s, status=%s）。\n' \
      "${health_status:-unknown}" "${container_status:-unknown}" >&2
    docker compose logs --tail=100 monitor || true
    exit 1
  fi
  printf '等待健康檢查（health=%s, status=%s）……\n' \
    "${health_status:-unknown}" "${container_status:-unknown}"
  sleep "$HEALTH_POLL_SECONDS"
done

DEPLOY_SUCCEEDED=1
cleanup
trap - EXIT INT TERM HUP
docker compose ps
printf '\n已部署版本：%s\n更新時間：%s\n備份目錄：%s\n' \
  "$APP_VERSION" "$APP_UPDATED_AT" "$BACKUP_DIR"
