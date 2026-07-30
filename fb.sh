#!/usr/bin/env bash
# OCI maintenance menu for FB Public Monitor.
# Deploy as /home/ubuntu/fb.sh and run: chmod 750 /home/ubuntu/fb.sh && ~/fb.sh

set -u

APP_DIR="${FB_MONITOR_DIR:-/home/ubuntu/fb-public-monitor}"
PAGE_SIZE=20

PROFILES_PY=$(cat <<'PY'
from fb_monitor.config import load_settings
from fb_monitor.db import Database

settings = load_settings()
db = Database(settings.db_path)
for profile in db.rows("SELECT id, name, display_name, url FROM profiles ORDER BY id"):
    print("{id}\t{name}\t{url}".format(
        id=profile["id"],
        name=profile["display_name"] or profile["name"],
        url=profile["url"],
    ))
PY
)

POSTS_PY=$(cat <<'PY'
import json
import sys
from datetime import UTC, datetime, timedelta, timezone

from fb_monitor.config import load_settings
from fb_monitor.db import Database

profile_id = int(sys.argv[1])
page = max(1, int(sys.argv[2]))
page_size = int(sys.argv[3])
offset = (page - 1) * page_size

settings = load_settings()
db = Database(settings.db_path)

# Taiwan has a fixed UTC+8 offset and does not observe daylight saving time.
TAIPEI = timezone(timedelta(hours=8), name="台北時間")

def display_time(value):
    if value is None or value == "":
        return "未知時間"
    try:
        if isinstance(value, (int, float)) or str(value).strip().isdigit():
            timestamp = float(value)
            if timestamp > 10_000_000_000:  # Actors occasionally return milliseconds.
                timestamp /= 1000
            instant = datetime.fromtimestamp(timestamp, tz=UTC)
        else:
            instant = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if instant.tzinfo is None:
                instant = instant.replace(tzinfo=UTC)
        return instant.astimezone(TAIPEI).strftime("%Y-%m-%d %H:%M:%S %Z")
    except (OSError, OverflowError, ValueError):
        return str(value)

total = db.row(
    "SELECT COUNT(*) AS count FROM entities WHERE profile_id=? AND kind='post'",
    (profile_id,),
)["count"]
rows = db.rows(
    """
    SELECT e.published_at, e.source_url, e.present, v.normalized_json
    FROM entities e
    LEFT JOIN versions v ON v.id=e.current_version_id
    WHERE e.profile_id=? AND e.kind='post'
    ORDER BY COALESCE(e.published_at, e.first_seen_at) DESC, e.id DESC
    LIMIT ? OFFSET ?
    """,
    (profile_id, page_size, offset),
)

print("TOTAL\t{}".format(total))
for index, row in enumerate(rows, start=offset + 1):
    content = json.loads(row["normalized_json"] or "{}")
    text = str(content.get("text") or content.get("raw_text") or content.get("message") or content.get("postText") or "（無文字內容）")
    text = " ".join(text.split())
    if len(text) > 180:
        text = text[:177] + "..."
    print("POST\t{}\t{}\t{}\t{}\t{}".format(
        index,
        display_time(row["published_at"]),
        "存在" if row["present"] else "已消失",
        text,
        row["source_url"] or "（無原始連結）",
    ))
PY
)

dc() {
    if [[ ! -d "$APP_DIR" ]]; then
        printf '找不到部署目錄：%s\n' "$APP_DIR" >&2
        return 1
    fi
    (cd "$APP_DIR" && docker compose "$@")
}

pause() {
    read -r -p '按 Enter 回到選單...'
}

select_profile() {
    local -a rows
    local row number id name url

    mapfile -t rows < <(dc exec -T monitor python -c "$PROFILES_PY") || return 1
    if (( ${#rows[@]} == 0 )); then
        printf '尚未設定任何監控帳號。\n' >&2
        return 1
    fi

    printf '\n監控帳號：\n'
    for number in "${!rows[@]}"; do
        IFS=$'\t' read -r id name url <<<"${rows[$number]}"
        printf '  %d. %s\n     %s\n' "$((number + 1))" "$name" "$url"
    done
    read -r -p '輸入帳號號碼（0 取消）：' number
    [[ "$number" =~ ^[0-9]+$ ]] || { printf '請輸入數字。\n' >&2; return 1; }
    (( number == 0 )) && return 1
    (( number >= 1 && number <= ${#rows[@]} )) || { printf '沒有這個帳號。\n' >&2; return 1; }

    row="${rows[$((number - 1))]}"
    IFS=$'\t' read -r SELECTED_ID SELECTED_NAME SELECTED_URL <<<"$row"
}

show_status() {
    printf '\n=== fb-monitor 目前狀態 ===\n'
    dc exec -T monitor fb-monitor status
}

show_execution_log() {
    printf '\n=== 執行紀錄 ===\n'
    dc exec -T monitor python -c '
from fb_monitor.config import load_settings
from fb_monitor.db import Database
s = load_settings(); d = Database(s.db_path)
for p in d.rows("SELECT name, display_name, public_state, last_success_at, next_visit_at, consecutive_failures, last_error FROM profiles ORDER BY id"):
    print("名稱：{}\n公開狀態：{}\n上次成功抓取：{}\n下次排程：{}\n連續失敗：{}{}\n".format(p["display_name"] or p["name"], p["public_state"], p["last_success_at"] or "尚無", p["next_visit_at"] or "尚未排程", p["consecutive_failures"], "\n最後錯誤：" + p["last_error"] if p["last_error"] else ""))
'
}

show_posts() {
    local page=1 output total line kind index published present text url action
    select_profile || return 0
    while :; do
        output=$(dc exec -T monitor python -c "$POSTS_PY" "$SELECTED_ID" "$page" "$PAGE_SIZE") || return 1
        total=0
        printf '\n=== %s：貼文第 %d 頁 ===\n' "$SELECTED_NAME" "$page"
        while IFS=$'\t' read -r kind index published present text url; do
            if [[ "$kind" == "TOTAL" ]]; then
                total="$index"
            elif [[ "$kind" == "POST" ]]; then
                printf '\n[%s] %s · %s\n%s\n%s\n' "$index" "$published" "$present" "$text" "$url"
            fi
        done <<<"$output"
        if (( total == 0 )); then
            printf '尚無已保存的貼文。\n'
            return 0
        fi
        printf '\n共 %d 篇。 [n] 下一頁  [p] 上一頁  [b] 返回：' "$total"
        read -r action
        case "$action" in
            n|N) (( page * PAGE_SIZE < total )) && ((page++)) || printf '已是最後一頁。\n' ;;
            p|P) (( page > 1 )) && ((page--)) || printf '已是第一頁。\n' ;;
            b|B|'') return 0 ;;
            *) printf '請輸入 n、p 或 b。\n' ;;
        esac
    done
}

queue_scan() {
    local confirm
    select_profile || return 0
    printf '\n即將優先排程抓取：%s\n%s\n' "$SELECTED_NAME" "$SELECTED_URL"
    read -r -p '輸入 y 確認：' confirm
    [[ "$confirm" == "y" || "$confirm" == "Y" ]] || { printf '已取消。\n'; return 0; }
    dc exec -T monitor fb-monitor scan "$SELECTED_ID"
}

main() {
    local choice
    command -v docker >/dev/null 2>&1 || { printf '找不到 docker 指令。\n' >&2; exit 1; }
    while :; do
        printf '\n=== FB Public Monitor 維運選單 ===\n'
        printf '1. fb-monitor 目前狀態\n'
        printf '2. 執行紀錄（上次成功抓取時間、下次排程時間）\n'
        printf '3. 查看使用者貼文\n'
        printf '4. 立即執行抓取\n'
        printf '0. 離開\n'
        read -r -p '請選擇：' choice
        case "$choice" in
            1) show_status; pause ;;
            2) show_execution_log; pause ;;
            3) show_posts; pause ;;
            4) queue_scan; pause ;;
            0) printf '已離開。\n'; return 0 ;;
            *) printf '請輸入 0 至 4。\n' ;;
        esac
    done
}

main "$@"
