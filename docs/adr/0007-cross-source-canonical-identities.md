# 以 Facebook 穩定身分進行跨來源去重

貼文優先以 `source_post_id` 建立標準身分，`story_fbid`、`fbid` 與各種永久連結作為別名；不同貼文 ID 即使內容相同仍分開保存。媒體優先使用 `source_media_id`，完全相同位元組以 SHA-256 合併，同一媒體只提升最高解析度為主要檔案；pHash 只提供相似提示。一般巡檢在呼叫 Actor 前傳入已知貼文邊界。連續兩批游標或貼文集合相同時，凍結該 `(provider fingerprint, purpose, input boundary)`，永久保存診斷批次並停止排程器再次購買；只有獨立通過契約的備援可接手，原來源需人工重新驗證後才能解凍。
