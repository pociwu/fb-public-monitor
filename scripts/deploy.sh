#!/usr/bin/env bash

set -euo pipefail

APP_DIR="${FB_MONITOR_DIR:-/home/ubuntu/fb-public-monitor}"
cd "$APP_DIR"

git pull --ff-only
export APP_VERSION
export APP_UPDATED_AT
APP_VERSION="$(git rev-parse --short HEAD)"
APP_UPDATED_AT="$(git show -s --format=%cI HEAD)"

docker compose up -d --build
docker compose ps
printf '\n已部署版本：%s\n更新時間：%s\n' "$APP_VERSION" "$APP_UPDATED_AT"
