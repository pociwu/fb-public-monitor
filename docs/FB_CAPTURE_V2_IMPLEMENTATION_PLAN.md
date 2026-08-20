# Facebook Capture V2 實作與驗收計畫

狀態：安全上線候選已實作；正式付費流程預設關閉，待部署後由操作人核准 `$0.20` 契約 canary。尚未具備獨立來源契約的 surface 會維持 `source_limited`，不宣稱完整。

## 目標

V2 必須把「來源成功執行」與「內容已完整回溯」分開。系統只在資料來源提供可驗證的終點時宣告完成，並以可續接、可重播、可稽核的批次保存公開貼文、多媒體及留言。特殊帳號 `100027675104517` 一旦由強證據確認公開，立即建立最高優先捕獲；「立即」指立即排程並開始第一個安全批次，不代表跳過 Facebook 風控一次完成全部下載。

## 不可破壞的規則

- 登入 Chromium 可見只表示「監控帳號可瀏覽」，不得直接判定公開。
- 貼文清冊、多媒體與留言各自擁有狀態、游標及終點證據。
- 沒有游標、日期邊界或明確終點時只能標記「來源受限」。
- 每次付費 Actor 呼叫先建立唯一批次；原始結果保存後才匯入，匯入完成後才提交游標。
- 同一輸入邊界失敗時重播已購買的原始結果，不重新呼叫 Actor。
- 同游標或同貼文集合連續兩批即熔斷。
- 同一媒體只保留已證實相同項目的最高解析度檔案；不確定的相似圖片不得刪除。
- Chromium 自動與手動工作共用單工、冷卻、每日上限及全域風控熔斷。
- 不自動產生 Apify 超額費用。

## 第 0 階段：部署前保護

1. 先停止接受與 dequeue 新工作，停用未通過契約的 V1 無游標 backfill/audit 新付費呼叫；讓已啟動的付費 run 完成並至少進入 `raw_saved`，若無法確認則保存 run/dataset 線索並進入 `needs_reconcile`，不得中斷或重買。
2. 確認沒有 in-flight paid run 需要寫入後，才進入 maintenance mode，停止 scheduler、web mutations、outbox 及所有資料庫 writer；以 SQLite Backup API 或等價一致性機制建立具時間戳備份，不分開複製仍在使用的 DB/WAL 檔。
3. 記錄目前 Git commit、資料庫 schema 版本、Apify 帳期及官方用量，保留 immutable `last-good` 映像，加入預設關閉的 `CAPTURE_V2_ENABLED=0`；個人資料與現有唯讀介面於維護結束後恢復。

驗收：24 小時內 cursorless backfill/audit 新付費呼叫為零；備份可還原、SQLite integrity check 通過、舊容器可由 `last-good` 映像啟動。

## 第 1 階段：V2 狀態與資料遷移

新增或等價實作下列持久模型：

- `access_observations`：來源、登入範圍、判定、身分證據、時間及原始證據引用。
- `actor_contracts` 與 `contract_runs`：provider、用途、Actor build／schema fingerprint、輸入 mapping、狀態、測試費用、失效時間與測試證據；每個 production paid batch 必須外鍵指向仍有效的 `passed` contract。
- `capture_epochs`：觸發原因、目標範圍、狀態、特殊帳號優先級與預算保留。
- `coverage_streams`：`posts`、`media`、`comments` 各自的 surface、provider、輸入/輸出游標、覆蓋區段、缺口、低水位、狀態與受限原因；以 `UNIQUE(epoch_id, stream, surface, scope_type, scope_id)` 保證每篇標準貼文各有獨立 media/comment checkpoint。
- `paid_source_batches`：以資料庫 `UNIQUE(request_hash)` 強制唯一輸入意圖／邊界，保存 Actor run/dataset/KVS、費用、原始檔、匯入統計與游標。
- `post_aliases` 與 `media_aliases`：跨來源標準身分及來源網址。
- `post_media_coverage`：每篇貼文的相簿/影片續接位置、已見媒體、狀態與錯誤。
- `browser_limits` 與 `browser_evidence`：全域熔斷、冷卻、每日使用量及證據截圖。
- `large_media_approvals`：超過 1 GB 影片的人工核准狀態。
- `profile_name_candidates`：名稱來源、信任等級、人工鎖定與拒絕歷史，避免瀏覽器雜訊覆寫可信名稱。
- `profile_source_controls`：每個帳號的 Apify 凍結及原因；凍結禁止新付費 run，但不禁止重播已購原始資料。

遷移規則：

- 保留舊欄位供回退，但 V2 不再以 `backfill_done` 作完整性真相。
- 舊 `backfill_done=1` 若無終點證據，改列「未驗證」或「來源受限」。
- 只有登入 Chromium 支持的 `public` 改列「監控帳號可瀏覽」。
- 將既有 `apify_frozen`、可信 `display_name`、人工名稱修正及拒絕名稱歷史 seed 到新控制表；migration 連跑兩次後凍結仍為真、目前可信名稱不得改變。
- 此階段只建立貼文／媒體別名與待修復清單，不刪除、覆寫或實體合併既有內容。V2 canary 通過後，才在證據充分時將所有引用原子切換至最高解析度，再刪除低解析度檔案。
- 第一次啟動進行冪等 reconcile；重啟不得重複建立 capture epoch 或工作。
- Migration 在單一交易內完成，只有所有 schema/data checks 通過後才於交易最後寫入 `schema_migrations`。契約尚未通過時，特殊補救 epoch 只能為 `awaiting_contract`，不得建立可執行付費 job。

驗收：以生產資料庫副本連跑兩次 migration/reconcile，第二次零新增；貼文/媒體引用完整；不存在 `complete` 卻無 terminal evidence；特殊帳號若已確認公開但無終點證據，恰好建立一個 `awaiting_contract` 補救 epoch；契約通過前可執行付費工作為零。

## 第 2 階段：來源介面與 Apify 契約閘門

1. 將個人資料、存取觀察、貼文、留言及媒體拆成明確 provider 介面；provider build、schema fingerprint 或輸入 mapping 變更時自動使舊契約失效。
2. SerpApi 只提供個人資料及匿名公開訊號；空結果為無法判定。
3. Bright Data 只在 SerpApi 無結果或需交叉驗證時補個人資料及公開訊號，不證明內容回溯完成。
4. 貼文主要候選 Actor 使用 `spbotdel/facebook-profile-posts-all-photos-scraper`，備援候選使用 `unseenuser/fb-posts`；`unseenuser/fb-profile` 不再承擔回溯。
5. 先以最多 `$0.20` 執行四次受限契約測試：十篇第一頁、十篇第二頁、重播第二頁游標，以及最多兩篇的已知 ID／時間停止邊界測試。
6. 任一契約條件失敗即停用該 provider fingerprint 與用途，正式捕獲保持「來源受限」，不得假裝完成；備援需獨立通過契約。
7. Media contract 另以已知跨頁多圖貼文驗證 media IDs、gallery cursor／next、expected count 與 terminal；comments contract 以超過一頁且含 nested replies 的貼文驗證游標前進、重播一致及 cap 不等於 complete。未通過的 stream/surface 不得正式啟用。
8. 一次性 `$0.20` 是操作人明確核准的本輪全域 grant，跨帳號、跨候選 Actor 共用同一筆實際計費＋模糊保留總上限。主要候選失敗後，備援只能使用同輪已知剩餘；若不足，必須無模糊 run、結束本輪後再由操作人明確核准新 grant，不自動追加支出。

驗收：測試帳號由操作人確認預期至少有 25 篇非置頂可見內容；排除置頂後第一、二頁不重疊；第二頁重播的身分集合及游標一致；增量停止邊界不付費回傳已知內容；`SUMMARY` 與貼文終點可解析；本輪 ledger 的實際計費加模糊保留未超過 `$0.20`。這份 posts-cursor contract 不代表媒體完整性；媒體依第 7 點獨立驗收。

## 第 3 階段：可續接捕獲引擎

每次付費批次使用 `request_hash = capture intent + deterministic observation window + profile + epoch + stream/surface + contract fingerprint + normalized cursor/boundary/input` 唯一識別，以單一交易 `INSERT ... ON CONFLICT` 取得或重用資料庫 `UNIQUE(request_hash)` 的 batch，只有成功持有該 batch lease 的 worker 可由 `prepared` 進入 `launching`。狀態依 `prepared → launching → run_started → raw_saved → imported → committed` 前進。retry 沿用同一 intent/window；下一個月的近期核對使用新的固定月份 window，因此能合法重讀而不破壞 crash 防重。Apify 接受 run 後立即保存 run ID；若程序在已送出呼叫但尚未保存 run ID 的模糊窗口中斷，批次進入 `needs_reconcile`，禁止自動重買，必須先以 Apify run 時間、Actor 與 request fingerprint 對帳。

### 貼文

- 特殊公開捕獲每批最多 50 篇；其他回溯依同一機制但較低優先。
- 每批先持久化壓縮原始 JSON，再標準化貼文身分、寫入資料庫並建立媒體工作，最後提交下一游標。
- 啟動下一批前比較游標及 identity set；連續兩批相同即停止並通知。
- 明確 terminal 才把貼文清冊標記完成。

### 多媒體

- 每篇貼文建立獨立、可續接的相簿/影片工作；貼文游標前進不會遺失未完成相簿。
- 支援 `permalink.php`、`story_fbid`、`/posts/`、`/photos/`、`/photo.php?fbid=`、`/photo/?fbid=`、Reels 及影片標準身分。
- 媒體集合採累積 upsert，不以單批 DOM 覆蓋既有集合。
- 短暫無圖、selector 改變、逾時或單一媒體反覆出現只能重試/受限，不得標記 complete。只有 declared count/position 抵達終點，或已成功前進遍歷多個不同 media IDs 後由可靠 viewer sequence 回到首張，才能完成。
- 照片下載最高可取得解析度；影片不超過 1 GB 自動下載，超過則等待核准。

### 留言

- 貼文清冊抵達明確終點後開始，一篇貼文一個 Actor 工作；不必等待大型影片核准或所有媒體補缺完成。
- 只有來源明確抵達終點才完成；費用或結果上限為來源受限/預算暫停。
- 留言 ID 與 parent ID 去重；後續只在留言數增加時核對最新部分。

### 公開內容 surface

- `timeline_posts`：由通過游標契約的貼文 Actor 盤點。
- `reels` 與 `videos`：主要候選能列舉且通過各自 terminal 契約才完成，否則 `source_limited`；Browser 只補匿名來源已列舉項目。
- `post_albums`：由 `expandAllPhotos`／media contract 列舉，缺圖才交 Browser 獨立 media job。
- `public_photo_pages`、`avatar_history`、`cover_history`：候選 Actor 的 all-photos 能力或無登入 Browser collector 必須各自通過游標／終點測試；尚未支援就明列 `source_limited`，不能由個人頁前六張預覽推定完成。
- 每個 surface 均保存自己的 coverage row、範圍、缺口及 terminal evidence；總體「所有可回溯內容完成」要求所有納入範圍的 surface 均已 complete 或有明確不可用 tombstone。

驗收：服務於呼叫已送出但回應未保存、run ID 保存、Actor 完成、原始檔保存、DB 匯入與游標提交位置分別被強制終止後，重啟均不會自動重買已計費或狀態模糊的批次且能在對帳後續接。超過 20 次 Browser 操作的相簿跨多輪後仍保留完整累積集合；每個承諾 surface 皆有獨立 fixture 與 terminal/source-limited 結果。

## 第 4 階段：公開偵測與特殊帳號流程

1. 特殊帳號先解析為標準 ID `100027675104517`，每兩小時執行一個匿名探測時槽；每 36 個時槽固定安排 25 次 SerpApi、11 次最多一筆結果 Apify。此排程不得直接啟動登入 Chromium。
2. API 空資料、逾時、登入牆、解析錯誤或身分不符均為無法判定。
3. 強私人證據只限身分一致的匿名頁面明確 privacy/locked marker，或通過契約來源明確回傳 private；空資料、逾時與一般 no-items 都不是私人證據。第一次強證據只進疑似私人並暫停新付費回溯，30–60 分鐘後第二來源／第二觀察才確認私人。
4. API 取得公開訊號後先列疑似公開，再以無登入 Chromium 或已通過匿名公開判定契約的明確來源確認。
5. 確認公開時，以唯一鍵原子建立 capture epoch、取消/取代舊的無效回溯工作並保留既有資料。五分鐘內必須建立／更新最高優先 epoch；只有 contract passed、未凍結、預算 reservation 已取得且 scheduler 正常，才在既有付費 run 安全完成後派送第一批，否則進入明確 awaiting/paused 狀態而不 launch。
6. 一旦確認公開，停止兩小時私人偵測；初始捕獲完成後改用 6–8 小時增量巡檢。

驗收：并發兩個相同公開事件只產生一個 epoch；已有舊貼文的特殊帳號仍會啟動補救捕獲；登入 Chromium 單獨可見不會產生 `profile_opened` 或大批 Apify 工作。

## 第 5 階段：Chromium 補缺與風控

- 所有 browser job 由一個全域 worker 執行，手動按鈕只提高優先級。
- 登入 Chromium 只補抓已被匿名來源列舉的標準貼文與媒體；登入狀態下獨有的內容不得進入公開清冊。
- 同帳號 30–60 分鐘、跨帳號 2–5 分鐘；每批最多兩篇、20 次相簿操作或三分鐘；每帳號每日八批。
- 每次瀏覽前後偵測登入失效、checkpoint、驗證碼及封鎖；第一次全域熔斷 24 小時，七天內重複則 72 小時。
- 風控事件只保存一張最寬 1600px、約 1 MB 內的 WebP，保存 180 個 24 小時；總量硬上限 `500 MiB = 524,288,000 bytes`，先清除逾期檔，再清除已結案最舊項目。
- 一般擷取每帳號只保留最新十張；只有 `committed`、epoch 已完成、超過 90 天且 checksum 可驗證的成功原始 JSON 才可清理。`launch_ambiguous`、`needs_reconcile`、`raw_saved`、`import_failed`、爭議或未提交批次一律保留到對帳結案。

驗收：所有自動/手動入口均無法繞過 limiter；模擬 challenge 後不再開新頁；截圖去重、到期及 `500 MiB` 上限清理可重現。

## 第 6 階段：增量巡檢、排程與預算

- 已確認公開帳號每 6–8 小時巡檢，呼叫前傳最近 20 個已知貼文 ID 作停止邊界；取消付費「抓一篇再去重」。
- 取消每七天清空歷史游標，改為每月每帳號核對最近五篇。
- 同時只跑一個付費 Apify 批次；每四個最高優先工作至少放行一個到期一般工作。
- 以帳期／用途 ledger 原子保留最多 `$4.00` 特殊捕獲、約 `$0.55` 特殊偵測；一般 call 可用額 = 官方剩餘 − 特殊 outstanding reserve − 已啟動未結算批次。一般工作不得侵占尚未釋放的保留額，不足時保存游標並暫停，不自動超額。
- 同一批已購買結果重播優先於任何新 Actor 呼叫。
- 單一帳號 Apify 凍結時，所有新付費工作（含特殊捕獲）保持人工暫停；解除後從原游標續接，離線重播不受阻擋。
- 特殊帳號凍結時，原定 Apify 探測時槽先改用仍有保留額度的 SerpApi，其次 Bright Data；兩者都不可用就記錄 `detection_degraded_apify_frozen` 並通知，不得繞過凍結或假稱維持兩小時目標。

驗收：24 小時模擬排程中不存在已知一篇探測；一般帳號不餓死；預算耗盡後零新付費 run，次帳期能由原游標續接。

## 第 7 階段：介面、通知與清理

- 卡片顯示公開證據、三條覆蓋摘要、下一工作及特殊捕獲狀態。
- 詳情頁列出 cursor、低水位、來源、終點證據、Apify 費用及 new/update/duplicate、Chromium 冷卻與每日使用量。
- 操作只提供續接、重新驗證、核准額度及大型影片；不得提供容易誤觸的普通重頭回溯。
- Telegram 只通知公開狀態轉換、每 250 篇或有進度且相隔至少 60 分鐘的回溯里程碑、暫停/受限/熔斷狀態變化，以及初始完成後真正的新內容。
- 每日清理只處理本服務的到期原始檔、截圖、暫存與無引用檔案；磁碟剩餘低於 30 GB 只暫停媒體下載。

驗收：所有時間為台北時間；同一歷史貼文/相簿不重複通知；首頁與詳情的數字能由批次帳本及資料庫查詢重算一致。

## 上線順序與回退

1. 在生產資料副本跑 migration、reconcile、單元及整合測試。
2. 建立新映像但保持 scheduler 關閉，執行 SQLite integrity 與只讀介面檢查。
3. 啟用來源契約測試；通過後才開啟 V2 scheduler。
4. 先只允許特殊帳號執行一個最多 50 篇的 posts batch，人工核對 cursor、identity、raw data 與官方費用；接著只對該批啟用 bounded media canary。媒體驗收通過後，才讓特殊帳號 posts 與逐批 media pipeline 一起自動續跑到 terminal；comments 最後啟用，再加入一個低量一般帳號並逐步開啟全部帳號。
5. 監看 Apify 官方用量、重複率、游標前進、Telegram 噪音及磁碟 24 小時。
6. 若 schema、游標、費用或風控驗收失敗，先停止新 dequeue、關閉 V2 並把受影響 profiles 的 `apify_frozen=1` commit；V1 pending backfill/audit 可標 `superseded/paused_rollback`，已 running 的付費 run 必須完成到 `raw_saved/committed`，或保存 run/dataset 後轉 `needs_reconcile`，不得直接中斷。確認 queue 零 pending 付費工作、沒有未處理 in-flight run，再保留 session/cursor/raw data 並回到 `last-good` 映像；additive schema 不降級。只有 migration 或 SQLite integrity 本身失敗且服務尚未接受新寫入時，才停機還原部署前資料庫，避免遺失部署後已保存內容。

## 最終完成條件

- 特殊帳號的公開訊號一旦可由排定來源觀察，從 `signal_available_at` 到下一個有效探測時槽不超過兩小時，並建立唯一高優先捕獲；另記錄實際轉公開至發現的延遲，但不對供應商索引 freshness 作無法驗證的保證。
- 貼文、媒體及留言各自顯示可查證的 `complete`、`source_limited`、`budget_paused` 或 `failed`，不再使用含糊的單一完成旗標。
- 任意重啟或匯入失敗不會重買相同 Apify 批次。
- 同一貼文不因 URL 形式或 provider 不同而重複；同一媒體只保留已證實最高解析度版本。
- 特殊帳號的所有當下公開可取得貼文與多媒體，只有在來源明確終點及所有媒體工作結案後才宣告完成。
- Chromium 遇風控頁後自動停止，且 180 天證據截圖總量不超過 `500 MiB = 524,288,000 bytes`。
