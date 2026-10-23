# Bright Data 非同步修復與 ScrapeCreators 免費試用

## 範圍

Bright Data HTTP 202 保存 snapshot ID，查詢 progress，ready 才下載 JSON。
pending 或暫時下載失敗保留 ID，下次巡檢續查，不重新 POST。
提交逾時、非 JSON 或缺少 snapshot ID 時保留 launching，禁止自動重送；需人工在供應商後台核對。
終止失敗也不自動重新計費。正常結果短暫快取 5 分鐘。

ScrapeCreators 加入個人檔案備援：SerpApi → Bright Data → ScrapeCreators（選用）→ 瀏覽器。
Bright Data 還在處理時不立即連鎖呼叫其他來源。
結果只能作為公開訊號，不會升格成已確認公開，也不證明完整歷史。
貼文提供單頁試用命令，原始回應和 cursor 保存在 SQLite，尚未啟用自動完整回溯。

## 設定位置

OCI `/home/ubuntu/fb-public-monitor/.env`，不要提交 key：

```dotenv
SCRAPECREATORS_API_KEY=你的金鑰
SCRAPECREATORS_ENABLED=0
SCRAPECREATORS_FREE_ONLY_CONFIRMED=0
SCRAPECREATORS_TRIAL_LIMIT=5
```

確認供應商帳號僅有免費點數、未啟用自動儲值後，才把 `FREE_ONLY_CONFIRMED` 改為 `1`。
API 只能回報餘額，不能證明點數來源；此確認是必要的人工前提，不是 API 自動驗證。
免費點數至少須覆蓋設定的試用次數，測試時不要讓其他程式共享這份免費額度。
本機預設最多 5 次、硬上限 10 次，跨 profile/端點共享，不因重啟或日期而歸零。
所有外送請求先扣本機次數；失敗、逾時也不退回，避免不明結果重複耗點。
沒有購買、自動儲值或付費切換程式。不得用已混合付費點數的帳號作免費保證。

## 更新程式後測試（此文件不代表已部署）

Compose 使用 env_file 載入 .env；修改後須重建容器環境：

```bash
cd ~/fb-public-monitor
docker compose up -d --build --force-recreate monitor
docker compose exec -T monitor fb-monitor source-status
docker compose exec -T monitor fb-monitor source-trial 2
docker compose exec -T monitor fb-monitor source-trial 2 --surface posts
```

每次試用僅一次 API 請求；同端點/參數在 24 小時內使用已保存結果。
試用不發 Telegram、不修改帳號公開狀態、不匯入貼文/留言，也不下載所有照片。
確認個人檔案結果可用後，可將 `SCRAPECREATORS_ENABLED=1` 啟用受相同試用上限約束的自動個人檔案備援。
用完即停止；`source-status` 是本機查詢，不耗供應商點數。

## 官方契約

- https://docs.brightdata.com/products/scrapers/facebook/quickstart
- https://docs.scrapecreators.com/v1/facebook/profile/
- https://docs.scrapecreators.com/v1/facebook/profile/posts/
- https://docs.scrapecreators.com/v1/account/credit-balance/

餘額 API 自身也標示每次 1 credit，因此程式不為每次請求額外查餘額。
