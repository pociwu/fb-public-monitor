# 付費 Actor 結果先持久化再提交回溯游標

每次 Apify 呼叫在執行前以 capture intent、deterministic observation window、provider fingerprint 與 normalized input 建立唯一付費來源批次，保存 run、dataset、key-value store、官方費用、原始 JSON、匯入統計與輸出游標。retry 沿用同一 intent/window；下一個月的合法近期核對使用新的月份 window。只有資料成功寫入並建立後續媒體工作才前移游標；程式重啟或匯入失敗只重播既有 dataset 或本機原始資料。若呼叫可能已送出但 run ID 未保存，狀態進入 `needs_reconcile`，在對帳前不得再次購買。批次 metadata、輸入／輸出雜湊與存取觀察摘要永久保留，實際原始檔則依保存期限清理。
