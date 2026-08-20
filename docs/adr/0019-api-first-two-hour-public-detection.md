# 特殊帳號每兩小時以 API 輪替偵測公開狀態

特殊帳號每兩小時執行一個匿名探測時槽，按每 36 個時槽中 25 次 SerpApi、11 次最多一筆結果 Apify 的固定加權排程，約使用每 30 天 250 次 SerpApi 與 110 次 Apify。這是輪詢週期；搜尋索引或供應商 freshness 可能使實際發現超過兩小時。空結果只算無法判定，API 公開訊號只進入疑似狀態並啟動無登入 Chromium 驗證；登入 Chromium 不作定時輪詢。特殊帳號尚未確認公開時，SerpApi 免費額度優先保留給此排程，其他帳號沿用快取、Bright Data 備援或人工更新；確認公開後才釋放未使用 SerpApi 額度。若特殊帳號設定 Apify 凍結，Apify 時槽依序改用仍有保留額度的 SerpApi、Bright Data；皆不可用時顯示 `detection_degraded_apify_frozen`，不得繞過凍結或宣稱維持兩小時目標。
