# 單一帳號 Apify 凍結禁止新付費呼叫但保留離線重播

卡片的 Apify 凍結是使用者明確設定的最高優先付費保護，包含一般巡檢、補救捕獲、特殊公開捕獲、留言及媒體 Actor；凍結時仍可建立或保留 capture session，但狀態為人工暫停，不能啟動新的 Apify run。已保存於本機的 dataset、原始 JSON 與資料庫匯入可離線重播，SerpApi、Bright Data 及受全域風控限制的 Chromium 仍照常運作。解除凍結後從既有游標與批次帳本續接，不重頭購買。
