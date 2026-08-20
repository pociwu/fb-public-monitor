# 只使用通過契約測試的 Apify Actor 回溯內容

貼文回溯將 `spbotdel/facebook-profile-posts-all-photos-scraper` 列為主要候選 Actor，`unseenuser/fb-posts` 列為備援候選，停止使用無可續接游標的 `unseenuser/fb-profile` 回溯貼文。候選只有通過對應用途的契約測試後才能進入 `validated/enabled`；Actor build、輸出 fingerprint 或計費模型變更會使契約失效。貼文增量邊界、留言、相簿、照片頁、Reels 與影片若使用外部來源，也必須各自驗證游標／日期邊界、終點語義與費用；失敗時只凍結該 provider fingerprint 與用途，備援仍需獨立通過驗證。
