# FB Public Monitor

在 Ubuntu 以 Docker Compose 長期監控最多 10 個公開 Facebook 個人帳號。服務透過 Apify 擷取個人檔案、貼文、公開留言與附件，保存完整版本歷史，並將變更送至單一 Telegram 群組。

## 已實作行為

- 每個帳號在上次完成後隨機 6–8 小時再次拜訪；全域工作間隔隨機 20–30 分鐘。
- 首次完整回溯可用游標跨月接續；一般拜訪每次檢查最近 10 篇；每 7 天完整核對每批最多 20 篇。
- 僅處理未登入可見資料。公開判定以個人檔案 Actor 能否回傳有效資料為準。
- 內容消失需連續兩次成功核對才確認；Actor 失敗不會改變 Facebook 狀態。
- 所有公開留言及最多三層回覆；留言附件依官方 Actor 回傳內容 best-effort 下載。
- SQLite 保存實體、版本、事件、排程、通知 outbox 與預算估算。JSON、Markdown 和媒體保存在 `/data`。
- 媒體依 SHA-256 去重。磁碟少於 10 GB 時暫停媒體下載；失敗項目保留 30 天補抓狀態。
- 每月 Apify 上限 5 美元，Actor run 也帶入剩餘費用上限；80% 與 100% 時 Telegram 告警。
- Telegram 先傳文字、後補媒體；過大檔案降級為本機路徑提示。通知失敗持久化補發。
- Telegram 使用固定中文摘要並直接上傳真正變更的照片／影片，不傳 CDN 網址或原始 JSON diff。
- 每天 08:00（Asia/Taipei）發健康摘要。
- 唯讀 FastAPI/Jinja2/HTMX Web UI 顯示自動名稱、設定別名、Facebook ID 與大頭照，並提供永久 Actor 診斷頁；只由 Docker 發布至主機 `127.0.0.1:8080`。
- 貼文與留言列表使用實際媒體卡片：圖片縮圖、影片原地播放、最多四格附件、文字摘要、媒體篩選及已消失遮罩；個人檔案分頁使用封面＋大頭照概覽卡。
- 圖片列表採延遲生成的 640px 縮圖，快取位於 `/data/cache/thumbnails/`；原始媒體不變，lightbox 與詳細頁仍可下載原檔。
- 貼文 Actor 依原始網址、數字 ID、`profile.php?id=` 自動重試；仍無結果時才使用 Profile Actor 內嵌貼文 fallback。
- 升級後會重解析既有 raw JSON 並自動錯開執行 `repair_scan`，不重送歷史項目，只發修復摘要。

## Ubuntu 部署

```bash
cp .env.example .env
cp config.example.yaml config.yaml
mkdir -p data
sudo chown -R 1000:1000 data
nano .env
nano config.yaml
docker compose up -d --build
docker compose logs -f monitor
```

`.env` 必須設定：

- `APIFY_TOKEN`：Apify Console → Integrations 中的 API token。
- `TELEGRAM_BOT_TOKEN`：由 BotFather 建立的 bot token。
- `TELEGRAM_CHAT_ID`：單一 Telegram 群組 ID；把 bot 加入群組並授權傳送訊息。

Web UI 在 Ubuntu 本機開啟 `http://127.0.0.1:8080`。從其他電腦查看時使用 SSH tunnel：

```bash
ssh -L 8080:127.0.0.1:8080 ubuntu@your-server
```

然後在本機瀏覽器開啟 `http://127.0.0.1:8080`。

## 管理

編輯 `config.yaml` 可新增、停用或移除網址，服務會自動重新載入；最多 10 個。移除只停止監控，不刪除歷史資料。

```bash
docker compose exec monitor fb-monitor status
docker compose exec monitor fb-monitor scan 1
docker compose exec monitor fb-monitor scan example-account
docker compose exec monitor fb-monitor diagnose
docker compose exec monitor fb-monitor diagnose example-account
```

### OCI 維運選單

將專案內的 `fb.sh` 部署到 OCI 的 `/home/ubuntu/fb.sh` 後，可用互動式選單查看服務狀態、排程時間與已保存貼文，或將指定帳號的抓取工作排至佇列最前端。選單不會等待抓取完成，且仍遵守既有預算與全域間隔。

```bash
install -m 750 ~/fb-public-monitor/fb.sh ~/fb.sh
~/fb.sh
```

`scan` 只將工作排到最前端，仍遵守全域間隔與預算。Web UI 是唯讀，不提供修改、刪除或觸發掃描 API。

### 從 GitHub 更新 OCI

程式碼更新後，在 OCI 執行：

```bash
cd /home/ubuntu/fb-public-monitor
git pull --ff-only
docker compose up -d --build
docker compose ps
```

`.env`、`config.yaml` 與 `/data` 不在 Git 版本控制中，更新程式不會覆寫權杖、監控帳號設定或既有資料。公開倉庫只保存不含真實帳號的 `config.example.yaml`。

## Actor 設定與限制

預設 Actor：

- 個人檔案：`apify/facebook-pages-scraper`
- 貼文與貼文附件：`spbotdel/facebook-profile-posts-all-photos-scraper`
- 留言與回覆：`apify/facebook-comments-scraper`

Actor ID 與額外輸入可在 `config.yaml` 的 `actors` 區塊覆寫。每次 Actor 呼叫、輸入格式、結果數、SUMMARY、錯誤與 schema 失敗樣本（最多 20 筆）都會保存；token、cookie、password、secret 欄位會遮蔽。Facebook 或 Actor schema 改變且所有已確認 fallback 皆失敗時，服務會停止該輪並通知，不會自行切換到未核准的付費 Actor。完整擷取是指 Actor 在未登入狀態實際可取得的公開內容；影片直接網址與留言附件不保證存在。

5 美元預算很低，大型帳號的首次貼文／留言回溯可能需要多個月。優先級依序為公開狀態、個人檔案、近期貼文、近期留言、歷史回溯、完整核對。

## 測試

```bash
python -m pip install -e '.[test]'
pytest
```

測試不會呼叫 Facebook、Apify 或 Telegram。
