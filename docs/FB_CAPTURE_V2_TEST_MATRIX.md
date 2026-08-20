# Facebook Capture V2 必備測試矩陣

狀態：核心自動化 Gate 已實作並通過；正式付費流程仍須在 OCI 依序完成 `$0.20` Actor 契約 canary 與特殊帳號首批人工核對後，才逐步啟用。未具備獨立來源契約的 surface 維持 `source_limited`。

## 核心不變量

1. 貼文盤點、媒體補全與留言補全是三個獨立狀態。
2. 付費批次原始資料、標準貼文及媒體工作成功提交後，才可前移貼文游標。
3. challenge、登入牆、逾時、空 DOM 或暫時解析失敗不得前移游標、改變公開狀態或標記完成。

## P0：貼文游標與付費批次

- 首次取得 0、1、2 與超過批次上限的邊界行為正確；0 筆且無明確 terminal 不可完成。
- 五篇內容以 `2/2/1` 跨三輪保存後恰有五個標準貼文；只有第四輪看見可靠終點才完成。
- 第一批後重啟，從持久游標續抓第二批，不回到首頁第一篇。
- 在 Actor 呼叫已送出但 run ID 尚未保存、run ID 保存、原始資料保存前、匯入中、媒體工作建立中及游標提交後分別模擬 crash；模糊 launch 必須 `needs_reconcile`，重啟不自動重買已計費批次、不重複新增實體。
- 回溯期間頂端新增內容或置頂貼文重排，不使歷史游標倒退或產生重複。
- 找不到既有游標時進入 `stalled_cursor`，以已見 ID 與發佈時間水位重定位；不得持續重抓第一頁。
- 連續兩批游標或標準身分集合相同，觸發來源熔斷。
- 第一頁、第二頁及重播第二頁的 Apify posts-cursor 契約測試滿足不重疊、可重現及費用上限；通過狀態不得誤標為媒體或留言完整。
- 沒有人工核准 grant 時零付費契約工作；一輪 `$0.20` 跨帳號與 primary/fallback 共用，同一 Actor fingerprint 不重複費用。
- primary 失敗後 fallback 只得到同輪剩餘；`launching/run_started/needs_reconcile` 依授權上限保留，未 reconcile 前不可結束本輪或核准新輪。
- 契約 fixture 必須有最新匿名、身分一致的強存取證據確認公開，並要求操作人確認預期有 25+ 篇公開歷史貼文；後續強私人證據必須壓過舊的公開證據。
- 瀏覽器付費 POST 的 `Origin`/`Referer` 若存在必須同源；跨站表單不得建立 grant、契約測試或回溯工作，無這兩個 header 的 CLI 仍可用。

## P0：網址與跨來源標準身分

以 table-driven tests 驗證：

- `/photo.php?fbid=123`、`/photo/?fbid=123` 與 `/{user}/photos/.../123/` 皆為同一 `photo:123`。
- `permalink.php?story_fbid=X&id=U`、`/U/posts/X` 與 query 順序/追蹤參數變形皆為同一貼文。
- `/share/p/`、`/reel/`、`/videos/` 各產生穩定且有類型的身分。
- `m.facebook.com`、`mbasic.facebook.com` 與 `www.facebook.com` 的同內容別名合併；非 Facebook、redirect、JavaScript 與 data URL 拒絕。
- article 同時包含作者頭像、分享內容與時間戳連結時，只選該 article 的永久連結。
- Browser 先保存 `/photo.php`、Apify 後回傳 permalink 時仍只有一個 entity。
- 不同來源 ID 即使文字或圖片相同仍不得誤合併。

## P0：相簿與影片續抓

- 10、20、21 與 65 張 Browser 相簿分別驗證；65 張需跨 `20/20/20/5` 及服務重啟後仍恰有 65 張。
- 達到 20 次 Browser 相簿操作或三分鐘只是批次邊界，有下一張時必須保存續接位置而非完成。
- 貼文游標已前進時，獨立 media job 仍能繼續未完成相簿。
- resume URL 過期後，重新開標準貼文並以已見 media IDs 重定位，不回到第一張重存。
- 相簿順序改變或中途新增照片時採累積 union，不移除既有媒體。
- 暫無 photo link、lazy load、下一張 selector 變更、圖片延遲切換、HTTP/登入錯誤皆是可重試/受限，不可完成。
- 只有 declared count/position 抵達終點，或已成功前進並遍歷多個不同 media IDs 後依可靠 viewer sequence 回到首張，才可完成；同一張反覆出現為 `stalled_media_cycle/source_limited`，不得完成。
- 圖片與 `<video>/<source>` 混合時分別建立正確媒體實體。
- 所有例外在工作、事件與介面可查，包含貼文 URL、階段與嘗試次數，不得靜默成功。

## P0：只保留最高解析度

- 同 CDN path 不同 query、不同 path 但同 media ID、完全相同 SHA 均只保留一個 logical asset。
- 200px 後取得 1080px，current reference 原子切換至 1080px；低解析度只有在所有引用切換完成後才刪除。
- 1080px 檔案較小仍以有效像素而非 bytes 選為主要版本。
- 同 pHash 但裁切或長寬比不同不得合併；無 media ID 的 pHash 候選需同帳號/貼文、比例相近且非不同裁切。
- 高解析下載失敗、檔案損壞或 HTML 偽裝圖片時保留既有低圖，成功後才切換。
- 高低解析度併發完成順序任意，最終仍為高解析度且永不降級。
- GC 只刪無引用檔案；被其他 entity 共用的 SHA 檔案不得刪除。
- 動圖及影片不走靜態圖片 pHash 合併。

## P0：公開狀態與特殊帳號

- 登入 Chromium 能看到內容只新增 `authenticated_visible` 觀察，不改成已確認公開。
- SerpApi/Apify 空結果、逾時、解析錯誤、登入牆及身分錯配均為無法判定。
- 特殊帳號的強公開證據并發出現兩次，仍只建立一個高優先 capture epoch。
- 已有舊貼文或舊 `backfill_done=1` 的特殊帳號，升級後仍建立補救捕獲。
- Migration 將既有 `apify_frozen`、可信名稱、人工校正及拒絕名稱 history 完整 seed；連跑兩次仍保持凍結且現名不變。
- 公開訊號一旦已由排定來源提供，`signal_available_at` 到下一有效探測時槽不超過兩小時；另記錄但不對供應商索引 freshness 作保證。確認公開後停止兩小時輪詢並切換初始捕獲。
- 空結果不暫停付費工作；第一次身分一致的匿名 privacy/locked 強證據只進疑似私人，30–60 分鐘後第二來源／第二觀察才確認私人。
- 特殊帳號 Apify 凍結時不執行 11 個 Apify slots，依序改用有保留額的 SerpApi、Bright Data，皆不可用則顯示 `detection_degraded_apify_frozen`；不得繞過凍結。
- 初始捕獲中額度耗盡、重啟及下個帳期恢復，皆從同一持久游標續跑。

## P0：Chromium 風控熔斷

- 在 profile、timeline 與 album 三階段分別模擬 checkpoint、challenge、login form、429；全部開啟全域熔斷且不推進內容狀態。
- 熔斷綁定瀏覽器登入身分，其他帳號的 Browser 工作也延後，但 SerpApi 與 Apify 可繼續。
- 手動按鈕無法繞過熔斷、同/跨帳號冷卻、每日八批及批次操作上限。
- 24 小時邊界後只允許一個 half-open probe；成功關閉，重複 challenge 則延長至 72 小時。
- 服務重啟後熔斷狀態、原因與下一次 probe 時間仍存在。
- timeout/5xx 使用短退避，不誤開 identity breaker；正常頁面的普通「安全」文字不誤判 challenge。

## P0：證據截圖保存

精確定義：保留 180 個 24 小時；總量上限採 `500 MiB = 524,288,000 bytes`。先刪逾期檔，再以 `captured_at, id` 最舊優先清理已結案事件。

- 每個風控事件只建立一張不可變 WebP 與完整 metadata；重試不重複建立。
- 179 天 23:59:59、180 天整及超過 180 天的時間邊界有固定結果。
- 總量恰為 500 MiB 保留，新增一個檔案後清理至不超過上限。
- 寫入採 temporary file 加原子 rename；清理與擷取并發不會提供半張圖或雙重刪除。
- DB 有紀錄但檔案遺失、孤兒檔、零位元組、損壞 WebP 皆可 reconcile。
- cleanup 僅作用於 evidence root，拒絕 path traversal 及越界 symlink。
- 刪除失敗時不先移除 DB 紀錄，並留下可重試錯誤。
- 介面用量、檔數、最舊/最新時間及最近錯誤與磁碟實際值一致。
- 截圖與 metadata 不保存 cookie/token，HTTP 回應採 `Cache-Control: no-store`。

## P1：增量巡檢、留言、預算與通知

- 6–8 小時巡檢在 Actor 呼叫前帶最近 20 個已知 IDs，不存在付費抓一篇再去重的路徑。
- 月度每帳號五篇核對不重設歷史游標；捕獲保留額不足時延後。
- 同月份 retry 沿用相同 deterministic audit window/request hash；下一月份產生新 observation window，能合法核對同五篇而不破壞 crash 防重。
- 留言一篇一個 job；明確 terminal 才完成，且不阻塞貼文及媒體完成。
- 同時只跑一個付費 Actor；每四個高優先批次至少放行一個到期一般工作。
- 歷史資料不逐篇通知；里程碑、暫停、受限及熔斷只在符合門檻或狀態改變時發送。
- 磁碟低於 30 GB 只暫停媒體下載，貼文清冊及游標仍前進。

## 最終端對端 Gate

用測試資料模擬 120 篇貼文、至少一個 65 張相簿、照片型 permalink、影片、一次預算暫停、一次服務重啟及一次 challenge：

- 最終恰有 120 個標準貼文與全部可取得媒體；沒有跨來源或解析度重複。
- 貼文、媒體、留言各自有可查證的完成/受限/暫停狀態。
- 每個 Apify run 都有輸入游標、輸出游標、官方費用及 new/update/duplicate 統計。
- 重啟及重試不重買同一輸入邊界；challenge 後無 Browser 工作繞過熔斷。
- 儀表板、Telegram 與資料庫可重算數字一致，所有顯示時間為台北時間。
- `timeline_posts`、`reels`、`videos`、`post_albums`、`public_photo_pages`、`avatar_history`、`cover_history` 各有獨立 fixture、coverage row 與 terminal evidence；未獲契約支持者明確為 `source_limited`，不得讓總體顯示 complete。
