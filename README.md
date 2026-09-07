# FB Public Monitor

在 Ubuntu 以 Docker Compose 長期監控最多 16 個 Facebook 個人帳號。服務以 SerpApi／Bright Data 偵測個人檔案訊號；Capture V2 會在 Actor 通過契約測試後，以可續接游標分批擷取公開貼文，並為同批附件與每篇留言建立獨立覆蓋狀態。登入 Chromium 可低頻補部分 API 缺口。尚未具備獨立契約或可靠終點的來源會明確標示 `source_limited`，不能把貼文完成狀態解讀為所有 Facebook surface 皆已完整。

## 已實作行為

- 每個帳號在上次完成後隨機 6–8 小時再次拜訪；全域工作間隔隨機 20–30 分鐘。
- 舊版 `unseenuser/fb-profile` 不接受可靠回溯游標，部署預設停止其付費回溯與七日重抓，避免反覆購買同一批貼文。
- Capture V2 先由操作人明確核准一輪全域共用最多 `$0.20` 的 posts-cursor grant，再對具備完整 V2 adapter 的 Actor 執行游標／重播／已知 ID 邊界契約測試；不會對每個帳號重複核准 `$0.20`。目前只有 `spbotdel/facebook-profile-posts-all-photos-scraper` 納入貼文 V2 候選；`unseenuser/fb-posts` 沒有可跨 run 的 cursor/known-ID input，不會被誤排成無法通過的付費測試。只有相同 fingerprint 的契約通過後才允許正式付費回溯。此契約只證明貼文游標，不證明媒體或留言完整。正式流程預設每批 20 篇（可調 1–50）；每篇貼文會建立媒體 checkpoint 並保存 Actor 同批附件，但在獨立媒體契約通過前一律如實標示 `source_limited`，不會把附件數量誤當相簿完整。
- 完整歷史結案後，6–8 小時巡檢會直接以最近 20 個已知貼文 ID 作停止邊界，上限 20 篇，不再先付費購買重複的最新 1 篇；每個 UTC 月另核對最近 5 篇，同一月份不重複建立工作。
- 契約 fixture 必須以「最新」匿名、身分一致的強證據確認公開；若後續強證據已確認私人，不會因歷史曾公開而繼續付費契約測試。所有 Web 變更與可能產生付費工作的 POST 在瀏覽器提供 `Origin`/`Referer` 時必須同源；無這兩個 header 的本機 CLI/curl 保留相容。
- 每個付費批次先建立唯一 request hash，保存 Actor run ID 與 gzip raw 結果後才匯入並前進游標；只有 `start()` 回應不明確才停在 `needs_reconcile`。已保存 run ID 的狀態查詢、dataset 或 KVS 暫時失敗會延後重試同一 run，不會重新購買。
- 個人檔案每 48 小時最多更新一次姓名、ID、網址、公開狀態、簡介、地點、學歷、工作、追蹤者、大頭照、封面與最多 6 張來源可取得照片；查詢順序為 SerpApi → Bright Data → 已登入 Chromium。
- 已登入 Chromium 的「可讀取」只代表 `authenticated_visible`，不能據此判定匿名公開。一般 SerpApi／Apify 訊號先標為疑似公開，再由無登入瀏覽器或通過公開判定契約的匿名來源確認。
- Chromium 每次直接擷取會先等待 3 秒，再依姓名區塊與圖片載入狀態最多等待 5 秒；並依帳號覆寫保存最新畫面，首頁個人卡片與個人詳細頁的「瀏覽器擷取畫面」可直接開啟查看。
- 所有可能啟動 Chromium 的手動拜訪、匿名公開驗證、金絲雀與個人資料備援都經過同一個 OCI/IP 共用的 `BrowserGuard`，共同實施全域／單帳號隨機冷卻、每日批次上限與 challenge 熔斷。匿名或登入瀏覽器任一方遇到真正的 checkpoint、challenge 或 429，都會暫停全部 Chromium 工作；單純匿名登入牆只記為未知，不會誤開熔斷器。被延後的手動／驗證工作保留同一筆 job 稍後再跑，自動備援則安全略過並寫入非通知事件。
- Facebook 出現 checkpoint／challenge／HTTP 429 時會啟動 24 小時全域熔斷，72 小時內重複發生則延長為 72 小時；已登入瀏覽器單純登入失效只要求重新登入，不會誤開平台風控熔斷。現場畫面以 lossless WebP 保存 180 天，並以 500 MiB 總上限每日自動清理。
- Chromium 每次最多處理 2 篇有固定連結的貼文；單一相簿批次預設最多 20 次操作或 3 分鐘（可用 `browser_guard.album_operations` / `batch_seconds` 向下調整），每次切換隨機等待 3–7 秒。照片集合採累積式 checkpoint；暫時找不到相片連結、圖片沒有切換或游標失效只會標示 stalled／source-limited，不會誤判完成。
- 每張帳號卡片可獨立排入「擷取全部照片」，並選擇「自動」、「僅登入帳號可見」或「僅 Apify 公開照片」。自動模式優先使用已登入 Chromium，同時讀取操作帳號可見的本人上傳 `/photos_by` 與被標註 `/photos_of` 兩個照片頁；遇到 `daily_limit` 或 `breaker_open` 這類長延遲時可先用受預算控制的 Apify 照片 Actor 補抓，並在 BrowserGuard 原定時間排回「僅登入帳號」續抓；短時的 profile/global cooldown 不會為此啟動付費 Actor。每批最多 20 張並為兩個頁面各自保存續抓 checkpoint；照片以 Facebook media ID 建立 `photo` entity，再沿用 SHA-256／感知雜湊去重。照片回溯使用獨立的分代狀態簿，不會佔用或污染 Capture V2 的付費 Actor epoch。只有兩個照片頁都到達可驗證的終點、所有發現的照片都已處理，且列出的媒體實檔均已下載，才會將該分代標成完成。Apify 結果只完成 Actor 可見範圍，不會被當成登入帳號可見清冊的完整證據。暫時下載失敗會在補抓期內只刷新失敗照片的 permalink，不重掃整個網格；登入失效、DOM 停滯、Actor 受限或無法解析的永久連結則如實標成 `source_limited`。首次完整回溯只發一則摘要，後續完整核對才逐項通知真正新增／變更且未重複的照片。
- 內容消失需連續兩次成功核對才確認；Actor 失敗不會改變 Facebook 狀態。
- SQLite 保存實體、版本、事件、排程、通知 outbox、SerpApi 額度、本地費用估算與 Apify 官方用量快照。JSON、Markdown 和媒體保存在 `/data`。
- 每日在健康摘要時段統計本專案的圖片、影片／附件、SQLite、JSON／Markdown、縮圖快取與 Chromium 用量；首頁可進入最近 30 天詳細頁，Telegram 每日傳送相較前一日的增加量。專案總用量不包含其他 Docker、Docker Images、Build Cache 或 Ubuntu 系統檔案。
- 媒體以 Facebook media ID／canonical URL／SHA-256 去重；相同影像不同解析度只將最高解析度保留為有效檔案。磁碟少於 30 GB 時暫停媒體下載；貼文清冊游標仍可保存，失敗項目保留補抓狀態。
- Chromium 個人檔案中與大頭照或封面照同一 CDN 資產的模糊預覽不會下載；升級重啟時也會移除既有重複關聯、原檔與縮圖快取。
- 每次 Actor 執行前查詢 Apify Billing 的官方當期用量，並在 SQLite 原子 launch claim 內重新核算本帳期已結算費用與所有未結算保留額。已達 5 美元、剩餘額不足一筆結果，或官方 API 查詢失敗時，都不會啟動 Actor；Actor run 也帶入剩餘費用上限。正式貼文 Actor 明確失敗時會凍結該帳號的 Apify；SUMMARY、身分、schema 或游標契約不符時也會作廢 exact fingerprint，等待人工重測或切換。
- Telegram 先傳文字、後補媒體；同一通知的照片使用媒體群組傳送，每組最多 10 張，超過時自動分組，避免逐張訊息干擾。單張圖片、影片與其他檔案維持個別傳送；過大檔案降級為本機路徑提示。通知失敗持久化補發。
- Telegram 使用固定中文摘要並直接上傳真正變更的照片／影片，不傳 CDN 網址或原始 JSON diff。
- 每天 08:00（Asia/Taipei）發健康摘要，帳號優先顯示 SerpApi 解析的真實姓名，不使用 `FB-數字ID`。
- FastAPI/Jinja2/HTMX Web UI 顯示監控人數、自動名稱、設定別名、Facebook ID 與大頭照；首頁可驗證並新增 Facebook 個人網址、停止監控並保留歷史資料、拖曳保存卡片順序，以及立即排程單一／全部帳號拜訪或單一帳號可見照片完整回溯；帳號詳細頁新增「照片」分頁，可燈箱預覽及下載原檔，並顯示每輪擷取的存取範圍與來源，另提供永久 Actor 診斷頁。登入 Chromium 會合併「本人上傳」與「被標註」兩個照片表面，並以登入帳號範圍雜湊隔離不同登入身分；Apify Actor 不使用本機登入 cookie，只補抓其自身可取得範圍，不能單獨證明登入帳號可見清冊完整。預設只由 Docker 發布至主機 `127.0.0.1:8080`，也可用 `WEB_BIND_IP` 綁定 Tailscale IP。
- 貼文與留言列表使用實際媒體卡片：圖片縮圖、影片原地播放、最多四格附件、文字摘要、媒體篩選及已消失遮罩；個人檔案分頁使用封面＋大頭照概覽卡。
- 圖片列表採延遲生成的 640px 縮圖，快取位於 `/data/cache/thumbnails/`；原始媒體不變，lightbox 與詳細頁仍可下載原檔。
- SerpApi 與 Bright Data 只負責個人資料／公開狀態訊號，不宣稱歷史貼文完整；登入 Chromium 只作低頻缺口補抓。批量貼文必須使用通過契約的 Apify Actor。
- 升級後會重解析既有 raw JSON 並自動錯開執行 `repair_scan`，不重送歷史項目，只發修復摘要。

### 尚未自動啟用

- 每篇貼文會建立留言 coverage checkpoint，但 comments Actor 尚未通過獨立游標、nested replies、終點與費用契約，因此目前就地標為 `source_limited`，不再產生每篇一筆、實際不執行的空 job。
- Actor 同批附件已保存並核對；逐篇貼文相簿的獨立 API 缺圖補抓尚未啟用。`reels`、`videos`、`avatar_history`、`cover_history` 仍無各自通過契約的 collector，不能宣稱這些 surface 已完整回溯。Capture V2 的 `public_photo_pages` 仍會誠實保持 `source_limited`；帳號卡片啟動的照片回溯改以已登入帳號的可見範圍為清冊，並在獨立狀態簿中記錄各次分代的來源、範圍與終點證據。這不代表 Capture V2 的公開性判定或公開貼文契約已改變。

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
- `SERPAPI_KEY`：SerpApi 帳號 API key；程式先查免費 Account API，剩餘次數為 0 時不會執行個人檔案查詢。
- `BRIGHTDATA_API_TOKEN`：Bright Data API token。設定後，SerpApi 額度用完、連線失敗或查無結果時，才呼叫 Facebook Profiles Scraper API 作備援。
- `BRIGHTDATA_DATASET_ID`：Bright Data Facebook Profiles dataset ID，預設 `gd_mf0urb782734ik94dz`，通常不需修改。
- `FACEBOOK_BROWSER_ENABLED=1`：啟用已登入 Chromium 個人檔案備援與帳號可見照片回溯；匿名公開驗證仍使用獨立空白資料目錄。
- `FACEBOOK_BROWSER_DATA_DIR=/browser-data`：容器內持久化瀏覽器登入狀態的路徑。
- `PHOTO_ACTOR_FALLBACK_ENABLED=1`：登入照片擷取失敗時啟用 Apify 照片 Actor 後備。
- `PHOTO_ACTOR_FALLBACK_ON_BROWSER_GUARD_LONG_DEFERRAL=1`：BrowserGuard 因每日上限或熔斷而長時間延後 Chromium 時，允許自動策略先用 Apify 公開照片 Actor 補抓；短時冷卻不會啟動付費 Actor。
- `PHOTO_ACTOR_MAX_CHARGE_USD=0.50`：單次照片 Actor 硬上限；仍受 `budget.monthly_usd` 全域上限約束。
- `BROWSER_LOGIN_BIND_IP`：互動式登入 noVNC 網頁的主機綁定 IP；OCI 應設為 Tailscale IP。
- `TELEGRAM_BOT_TOKEN`：由 BotFather 建立的 bot token。
- `TELEGRAM_CHAT_ID`：單一 Telegram 群組 ID；把 bot 加入群組並授權傳送訊息。

Web UI 在 Ubuntu 本機開啟 `http://127.0.0.1:8080`。從其他電腦查看時使用 SSH tunnel：

```bash
ssh -L 8080:127.0.0.1:8080 ubuntu@your-server
```

然後在本機瀏覽器開啟 `http://127.0.0.1:8080`。

若 OCI 已加入 Tailscale，可在 OCI 的 `.env` 設定：

```env
WEB_BIND_IP=100.x.x.x
```

實際 IP 可用 `tailscale ip -4` 查詢。重建容器後，即可從同一 tailnet 使用 `http://100.x.x.x:8080` 存取；此設定保存在不受 Git 管理的 `.env`，後續更新不會被覆寫。

### 首次 Facebook 互動式登入

先在 OCI 專案 `.env` 加入（IP 改為 `tailscale ip -4` 顯示的值）：

```env
FACEBOOK_BROWSER_ENABLED=1
FACEBOOK_BROWSER_DATA_DIR=/browser-data
FACEBOOK_BROWSER_TIMEOUT_SECONDS=60
BROWSER_LOGIN_BIND_IP=100.x.x.x
```

建立只存在 OCI 的瀏覽器資料目錄，並先停止監控容器，避免兩個 Chromium 同時開啟同一登入檔案：

```bash
cd /home/ubuntu/fb-public-monitor
mkdir -p browser-data
chmod 700 browser-data
docker compose stop monitor
docker compose --profile browser-login up --build browser-login
```

從同一 Tailscale tailnet 的電腦開啟：

```text
http://100.x.x.x:6080/vnc.html?autoconnect=1&resize=scale
```

在畫面中手動完成 Facebook 帳密、雙重驗證與安全檢查。終端顯示「登入狀態：已登入」後，按 `Ctrl+C`，然後重啟長期監控：

```bash
docker compose --profile browser-login down
docker compose up -d monitor
docker compose logs --tail=100 monitor
```

帳密不會寫入 `.env` 或 GitHub；Cookie 及瀏覽器狀態只保留在 `/home/ubuntu/fb-public-monitor/browser-data/`。noVNC 沒有額外密碼，因此 `BROWSER_LOGIN_BIND_IP` 只能綁定 Tailscale IP，不要設成 `0.0.0.0`。日後 Telegram 通知登入失效時，重複本節流程即可。

## 管理

編輯 `config.yaml` 可新增、停用或移除網址，服務會自動重新載入；最多 16 個。移除只停止監控，不刪除歷史資料。

### 啟用 Capture V2

更新後會先以安全模式啟動：`APIFY_V1_BACKFILL_ENABLED=0` 會停止舊版無游標回溯，`CAPTURE_V2_ENABLED=0` 代表 V2 尚未派送任何工作。確認頁面與既有資料正常後，在 OCI 私有 `.env` 改成：

```env
APIFY_V1_BACKFILL_ENABLED=0
CAPTURE_V2_ENABLED=1
```

重新部署後開啟 `/capture-v2`。先按「核准新一輪 `$0.20`」，再從已有匿名公開證據、且操作人預期有至少 25 篇公開歷史貼文的 fixture 執行 Actor 測試。若 Chromium 尚無法建立 `confirmed_public`，可對數字 Facebook ID fixture 額外勾選「只用於契約測試」；它不會直接解鎖正式回溯，只有契約四個 case 皆通過且 SUMMARY 身分吻合後才建立強公開證據。這個 grant 是整輪、跨帳號共用的付費上限；若未在 grant 到期前通過，會要求人工核准新輪，不會把舊 job 複製到下個帳期。測試會驗證第 1 頁、第 2 頁游標、同游標重播與已知 ID 停止邊界。只有顯示 `passed` 且 fingerprint 與目前 Actor 設定一致時，「繼續 V2 回溯」才會建立正式付費工作。貼文契約 `passed` 不可解讀為相簿、影片、公開照片頁或留言已完整。

正式回溯預設每批取得 20 篇貼文與它們的附件，同帳號下一批間隔 5–10 分鐘。可在 `config.yaml` 的 `capture_v2.posts_batch_size`、`posts_batch_spacing_min_minutes` 與 `posts_batch_spacing_max_minutes` 調整；批次上限為 50。`photo_capture.actor_fallback_on_browser_guard_long_deferral` 預設啟用，BrowserGuard 長時間無法取得 Chromium 時可改走付費照片 Actor，但其結果不會被當成登入帳號可見相片的完整性證據。

若任一帳號卡片顯示「Apify 已凍結」，契約測試、公開偵測中的 Apify slot 與正式捕獲都不會對該帳號啟動新 run；需由使用者明確解除凍結。登入 Chromium 可繼續補資料，但不會繞過凍結，也不會把登入後可見誤當成匿名公開。

金絲雀模式的預設值如下；即使舊版 `config.yaml` 沒有這段，也會自動採用相同設定：

```yaml
browser_canary:
  enabled: true
  max_posts: 2
  cooldown_hours: 72
```

```bash
docker compose exec monitor fb-monitor status
# 唯讀檢查最近成功過舊：來源錯誤、瀏覽器限制、巡檢佇列與最近內容入庫
docker compose exec -T monitor fb-monitor health
docker compose exec monitor fb-monitor scan 1
docker compose exec monitor fb-monitor scan example-account
docker compose exec monitor fb-monitor diagnose
docker compose exec monitor fb-monitor diagnose example-account
# Actor 啟動結果不明時，連回已存在的照片 run（不會重新購買）
docker compose exec monitor fb-monitor reconcile-photo-batch 12 --run-id EXISTING_RUN_ID --dataset-id EXISTING_DATASET_ID
# 已在 Apify Console 確認根本未啟動時，關閉舊 request 並排入安全重試
docker compose exec monitor fb-monitor reconcile-photo-batch 12 --confirm-not-launched
# 已人工切換照片 Actor／schema，放棄無法匯入的舊 raw 並安全重試（raw 仍保留）
docker compose exec monitor fb-monitor reconcile-photo-batch 12 --abandon-import-failed
# Capture V2 貼文 launch 回應不明：連回 Console 中已存在的 run
docker compose exec monitor fb-monitor reconcile-source-batch 34 --run-id EXISTING_RUN_ID --dataset-id EXISTING_DATASET_ID
# 已確認貼文 run 根本沒有啟動：封存舊 identity，建立一次安全重試
docker compose exec monitor fb-monitor reconcile-source-batch 34 --confirm-not-launched
# 契約測試 launch 回應不明：連回已存在的 run（不受其後 grant 到期／凍結影響）
docker compose exec monitor fb-monitor reconcile-contract-run 7 --run-id EXISTING_RUN_ID --dataset-id EXISTING_DATASET_ID
# 已確認契約 run 未啟動：保留失敗稽核；grant 仍有效時才重排
docker compose exec monitor fb-monitor reconcile-contract-run 7 --confirm-not-launched
```

健康摘要的「最近成功」記錄成功取得個人資料，或 V2 成功確認公開存取的時間，不代表貼文／照片工作完成時間；不會因為工作結束或排入佇列就更新。超過個人資料更新間隔加一次巡檢最長間隔（預設 48 + 8 小時），摘要會標示逾期、最近失敗原因與待補跑時間。帳號較多時分頁發送，避免後段被 Telegram 截斷。

`fb-monitor health` 不啟動 Actor、不消耗查詢額度，也不修改資料庫。它包含每個帳號最近巡檢／SerpApi 結果、來源存取證據、瀏覽器安全閘門及來源用量快照，方便區分登入失效、來源無結果、額度／每日限制與工作卡住。來源快照可能不是即時用量，單靠 Apify 尚有餘額不能判定其他來源也可使用。

巡檢備援被瀏覽器閘門延後時，會建立去重且持久化的 `profile_browser_fallback` 工作，於允許時間補跑；此工作不重查 SerpApi／Bright Data。登入失效仍需人工重新登入，不會繞過驗證或安全限制。服務中斷留下、且租約已過期的巡檢會保留為失敗紀錄並恢復後續排程，而非直接重播可能付費的舊請求。

### OCI 維運選單

將專案內的 `fb.sh` 部署到 OCI 的 `/home/ubuntu/fb.sh` 後，可用互動式選單查看服務狀態、排程時間與已保存貼文，或將指定帳號的抓取工作排至佇列最前端。選單不會等待抓取完成，且仍遵守既有預算與全域間隔。

```bash
install -m 750 ~/fb-public-monitor/fb.sh ~/fb.sh
~/fb.sh
```

`scan` 只將工作排到最前端，仍遵守全域間隔與預算。Web UI 提供首頁卡片排序、SerpApi 個人檔案與剩餘額度、新增／移除監控帳號、全部／單人立即拜訪、單人「擷取全部照片」、Apify 官方用量快照與通知佇列管理。照片回溯優先使用已登入 Chromium，並沿用共用 BrowserGuard；直接瀏覽受限時才使用 Apify 照片 Actor 後備。按下按鈕只會排程，不會繞過全域／單帳號冷卻、challenge 熔斷或 Apify 預算上限。

### 從 GitHub 更新 OCI

程式碼更新後，在 OCI 執行：

```bash
cd /home/ubuntu/fb-public-monitor
bash scripts/deploy.sh
```

`deploy.sh` 會先從 GitHub fast-forward 更新，再將當前 Git 短 commit 與 commit 時間寫入 Docker 映像。所有網頁頁尾會以台北時間顯示實際部署版本與更新時間。排程器維護旗標寫入主機擁有的 `deploy-state/` 並唯讀掛入容器；部署前備份則由一次性的 monitor helper 容器以資料擁有者身分寫入 `data/backups/deploy/`，確保 SQLite WAL／SHM 與未 checkpoint 的資料都安全納入。部署不會使用 `sudo`、`chown` 或更改 container-owned `data/` 的擁有者。

`.env`、`config.yaml`、`/data`、`browser-data/` 與部署備份不在 Git 版本控制中，更新程式不會覆寫權杖、監控帳號設定、Facebook 登入狀態或既有資料。公開倉庫只保存不含真實帳號的 `config.example.yaml`。首頁新增或移除監控帳號時會直接更新私有 `config.yaml`，因此該檔案的容器掛載與主機權限必須允許 UID 1000 寫入。

## Actor 設定與限制

個人檔案使用 SerpApi `facebook_profile`；預設 Apify Actor：

- 貼文與貼文附件：`spbotdel/facebook-profile-posts-all-photos-scraper`
- 照片補抓：`vulnv/facebook-profile-scraper`
- 留言與回覆：`apify/facebook-comments-scraper`

Actor ID 與額外輸入可在 `config.yaml` 的 `actors` 區塊覆寫。每次 Actor 呼叫、輸入格式、結果數、SUMMARY、錯誤與 schema 失敗樣本（最多 20 筆）都會保存；一般 Actor 診斷會遮蔽 token、cookie、password、secret 欄位。照片 Actor 的 input 另會直接拒絕 cookie、`c_user`、session、password、登入 token 等認證資料，既不傳送也不新增付費批次。Facebook 或 Actor schema 改變且所有已確認 fallback 皆失敗時，服務會停止該輪並通知，不會自行切換到未核准的付費 Actor。照片 Actor 結果標記為 `actor_visible`，只代表 Actor 自身可取得範圍，不等同匿名公開內容，也不等同登入帳號可見清冊；影片直接網址與留言附件不保證存在。

5 美元預算很低，大型帳號的首次貼文／留言回溯可能需要多個月。優先級依序為公開狀態、個人檔案、近期貼文、近期留言、歷史回溯、完整核對。

## 測試

```bash
python -m pip install -e '.[test]'
pytest
```

測試不會呼叫 Facebook、SerpApi、Apify 或 Telegram。
