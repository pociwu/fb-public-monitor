# Facebook 非 Apify 独立数据源评估

核查日期：2026-10-23。仅阅读供应商一手文档；没有注册、购买、调用收费采集接口或修改部署。本文区分「文档支持」与「已实测」，所有候选均未用本项目目标个人主页实测。

## 结论

建议依序验证 **Bright Data、ScrapeCreators、Refetcher**，将 **Scrappa** 留作实验候补。不存在已被本次证据证明能完整取得任意普通个人主页的全部历史贴文、照片、留言的方案。尤其不要将 endpoint 名字中的 profile、粉专示例中的 User 类型，直接解释为普通个人账户历史覆盖保证。

「无需 Facebook 登录」是调用者不需要提供 Facebook 账户、cookies 或 Graph token；仍需供应商账号/API key，并通常按量收费。它也不能单独证明供应商底层绝不使用自己的会话资源。

| 候选 | 最有价值用途 | 普通个人主页证据与局限 | 本项目优先级 |
| --- | --- | --- | --- |
| Bright Data | 个人主页公开信号；已知贴文 URL / 留言补抓 | 个人 Profiles 有明确示例；feed 产品仍名为 Pages Posts by Profile URL | 先修已有接入，再做分端点验证 |
| ScrapeCreators | cursor 分页贴文、独立照片流、留言及回复 | posts 明示免登录 profile posts；photos 明写 page，不保证个人照片分页 | 最优新增 REST 验证候选 |
| Refetcher | 发现近期公开贴文链接、已知 URL 详情和媒体 | 明确只有 links-only recent timeline，partial 状态；不是完整历史抓取器 | 轻量第三来源 |
| Scrappa | profile feed、photo grid 与单图详情 | feed 明确为 one timeline unit；分页、历史和个人覆盖仍待核验 | 实验候补，不直接替代主来源 |

## 1. Bright Data：现有基础最容易复用

调用 `POST https://api.brightdata.com/datasets/v3/scrape?dataset_id=...&format=json`，Bearer 为 Bright Data API key，输入为 URL 对象数组。Profiles 的个人示例是 `facebook.com/zuck`，返回 `id`、头像、封面等；文档说明 HTTP 202 与 `snapshot_id` 表示超过一分钟后转异步，不是没有数据。401/403 首先检查供应商凭证；空/部分数据另检查 URL 的公开可达性。[Quickstart](https://docs.brightdata.com/products/scrapers/facebook/quickstart)

| 用途 | Dataset ID | 已核查边界 |
| --- | --- | --- |
| Profiles | `gd_mf0urb782734ik94dz` | 人物身份及 `photos` 数组；没有照片分页完整性契约 |
| Pages and Profiles | `gd_mf124a0511bauquyow` | 另一身份资料产品，不等于 feed |
| Pages Posts by Profile URL | `gd_lkaxegm826bjpoo9m5` | 官方示例 NASA；普通个人历史待验证 |
| Posts by post URL | `gd_lyclm1571iy3mv57zw` | 已知贴文 URL 详情 |
| Comments | `gd_lkay758p1eanlolqw8` | 留言 ID、parent ID、reply 标志及附件 |
| Reels by profile URL | `gd_lyclm3ey2q6rww027t` | 另一独立内容流 |

上述 IDs 与字段来自官方 endpoint 目录，不能把多个数据集能力视为当前单一 Profiles 调用已具备。[Endpoints](https://docs.brightdata.com/products/scrapers/facebook/send-first-request)

Comments 输入可指定 `get_all_replies`、`limit_records`、`comments_sort`、`include_comments`；该 schema 没有公开调用者 cursor。应区分采集数量上限与下载结果分页，不臆造 Facebook 历史分页参数。[Comments JSON schema](https://docs.brightdata.com/scrapers/gd_lkay758p1eanlolqw8.json)

当前 Facebook 产品页列出 5,000 records/月免费额度、PAYG **$1.50/1,000 records**，Scale $499/月含 384,000 records；它按交付记录而非 HTTP 请求计价。网页顶部其他产品的起价不可替代该价目；购买前以账户实际报价核对。[Facebook pricing](https://brightdata.com/products/web-scraper/facebook)

## 2. ScrapeCreators：新增采集流最值得小样验证

基础 URL `https://api.scrapecreators.com`，认证头 `x-api-key`。以下端点文档列示 1 credit/request；不是每条内容 1 credit。

- `GET /v1/facebook/profile/posts`：`url` 或 `pageId`，`cursor` 分页，说明每页 3 条公开免登录贴文。输出 `posts[].id`、`url`、`permalink`、author 与视频 SD/HD 地址；保留多种 ID，不把 permalink 中的视频 ID 强制等同 `id`。[Posts](https://docs.scrapecreators.com/v1/facebook/profile/posts/)
- `GET /v1/facebook/profile/photos`：参数 `url`，翻页必须同时保存 `next_page_id` 与 `cursor`；输出同时有包装 `id`、`photo_id`、`viewer_image.uri`、尺寸和缩略图。官方文字写 public Facebook page，故普通个人照片页只能标记待测。[Photos](https://docs.scrapecreators.com/v1/facebook/profile/photos/)
- `GET /v1/facebook/post`：给已知 URL 获取详情；文档明确 Reel views 可能为 null 或低于界面 badge，不保证精确值。[Post](https://docs.scrapecreators.com/v1/facebook/post/)
- `GET /v1/facebook/post/comments`：`url` 或从详情取得的 `feedback_id`，下一页传 `cursor`；返回 `has_next_page`，留言有字符串 ID、时间、回复数，可能含图片/GIF/视频/贴纸。API 目录另列独立 Comment Replies，不能把根留言分页结束误认为回复完成。[Comments](https://docs.scrapecreators.com/v1/facebook/post/comments/)；[API directory](https://docs.scrapecreators.com/)
- `GET /v1/facebook/profile`：返回身份资料。文档示例同时有 `isPrivate` 与商家字段，不能凭此推断所有个人账户适配。文档列 200/404/403/500，但不能把供应商 403/404 无条件转换为 Facebook 私密/删除。[Profile](https://docs.scrapecreators.com/v1/facebook/profile/)

当前首页价格为 100 free credits、$47/25,000 credits、$497/500,000 credits，不到期；供应商旧博客还出现不同包价，应以当前定价/购买页面为准。缓存结果免费不代表适合实时监控，应记录数据采集时间并避免把旧缓存当新证据。[Current pricing](https://scrapecreators.com/#pricing)

## 3. Refetcher：补充发现与详情，不承诺完整 timeline

官方说明仅需 Refetcher key，不要 Facebook 登录/cookies/token。`POST https://api.refetcher.com/`，头 `X-API-Key`，JSON `{ "url": "..." }`；最多 50 URLs/batch。个人页只提供公开资料及近期 timeline 的链接发现，明确标记 partial timeline。贴文返回 `post.id`、`post.identifiers`、`normalizedUrl`、caption、媒体 children 和临时 HD/SD variants。产品页的 comments 指标是数量，不是已验证的完整留言正文接口。标价 $0.90/1,000 successful scrapes，3,000 免费额度、失败不收费；均属供应商声明，未实测。[Facebook product/schema](https://www.refetcher.com/facebook-scraper-api)

适合把新发现 URL 送入独立详情流；不能用于证明「已抓完全部历史」。错误码、profile pagination 具体字段仍需逐一契约验证。[Official documentation](https://www.refetcher.com/docs)

## 4. Scrappa：候补，勿把单个 timeline unit 当完整 feed

官方宣称公开 pages/profiles 无需 Facebook 登录；认证 `X-API-KEY`。列出的 GET 路径包括 `/api/facebook/profile`、`/api/facebook/profile/feed`、`/api/facebook/photos`、`/api/facebook/photo`、`/api/facebook/post`。关键局限：feed 的描述是 **one timeline unit**，产品页没有证明任意个人历史 cursor 能完整遍历；目录也没有已核验的普通贴文留言正文端点。样例提供 `meta.classification`、`billable` 与耗时，适合保留原始诊断。起价 $0.20/1,000 requests、500 免费 monthly credits，准确端点成本需另核对。[Official Facebook catalog](https://scrappa.co/apis/facebook-api)

父代理进一步核对照片专页：唯一输入为 `handle`；一次只返回一页照片网格，文档测量值为 8 张，并明确 next-photo 是单步而非 cursor。因此它不是已证实可分页抓完个人相册的替代来源，不建议立即做 production adapter。[Photos docs](https://scrappa.co/docs/facebook-api/facebook_photos)

## 不列入采购候选的两个名字

- **Scrapingdog**：访问原 Facebook 产品 URL 被重定向到 Social Media APIs 总目录；当前所列专用社交产品是 LinkedIn Profile、X、YouTube 等，不能用 LinkedIn 的「No Cookies」宣称当成 Facebook 支持。未确认到当前 Facebook 专用契约。[Official catalog / redirect destination](https://www.scrapingdog.com/social-media-apis/)
- **EnsembleData**：官方称不用社交账号凭证，但当前产品与定价列表没有 Facebook；不能因有 Instagram/Threads 就推断 Facebook endpoint 存在。完整 docs 页面读取超过工具大小限制，故结论是「未验证支持」，不是断言永远没有。[Provider statement](https://ensembledata.com/)；[Current supported products/pricing](https://ensembledata.com/pricing)

## 现有项目接入评估（父代理代码检查结果）

以下为同一工作任务父代理提供的本地代码检查结论，不是本次线上实测：

- `fb_monitor/brightdata.py` 的 `BrightDataGateway` 已调用 Profiles，但只接受即时 list 或 `{data:list}`；缺少 202/snapshot 恢复流程，并把 `photos` 截为前六项。
- ADR 0024 将其定位为 profile/public-signal，不是 posts/comments/full-history。`_try_brightdata_fallback` 到 `_store_profile_details` 不能覆盖其他采集流。
- `_ingest_capture_v2_posts` 的别名记录硬编码 `provider='apify'`。新增来源须参数化 provider，并独立保存预算、cursor、原始响应和来源证据。
- 现有 `ProbeSource` 只有 `serpapi`、`apify`、`bright_data`，原始 artifact 类型含 `APIFY_RAW`；新来源应明确扩展，不能伪装成 Apify。既有每月 $5 预算不能默认视为新供应商的额外付款授权；优先使用用户授权范围内的免费小样额度。

**首项工程建议**：先修现有 Bright Data 202 任务生命周期；202 文档与现有即时响应假设构成明确兼容缺口，但没有证据证明这是历史失败的实际原因。异步等待不应重新 trigger 同一收费任务。

## 接受测试与状态规则（工程建议，非供应商保证）

### 成本与实现边界

- ScrapeCreators 当前列示的 $47 充值包超过用户原先 $5 预算；不应未经批准购买。先以免费额度验证，额外供应商预算需独立授权与记账，不能自动视为原 Apify 预算已覆盖。
- 新接口仅加入资料获取层：保留现有 SQLite、媒体下载/内容哈希去重、版本比对与 Telegram 持久化 outbox，不另建一套通知路径。
- `ProbeSource`、provider 标签与持久化 batch/checkpoint 需扩充；不得把非 Apify 结果记作 Apify，也不得跨供应商复用 cursor。
- 首次补入历史内容应静默建立基线或只发摘要；重复扫描不产生新增事件，缺失字段不能清空已有可靠资料。只有内容真实变化才通知。
- 将上游 202 任务 ID 持久化，重启后继续查询结果而不是再次发起收费任务；每次调用记录实际费用/credits、原始响应与失败类别。

### 开源方案

[`kevinzg/facebook-scraper`](https://github.com/kevinzg/facebook-scraper) 提供 `get_posts`、`get_profile`、留言与媒体字段，也明确记录 cookies/credentials 选项及部分功能需要登录。源码可作为实验参考，但本次没有证据证明当前能稳定免登录采集这些普通个人目标；不列为无需维护的生产备援。免费源码不等于免费、稳定的数据来源。

1. 使用用户既有目标中的公开普通个人账户，另设已知公开、登录墙、无近期贴文对照；粉专成功不能代替个人成功。
2. 分别测身份、近况贴文、照片、已知贴文详情、根留言、回复与媒体下载，逐流报告覆盖，不汇总成一个虚假的 success。
3. 保留 numeric ID、pfbid、包装 ID、URL 之间的证据关联；媒体 CDN 链接不是永久身份键。
4. 存储每页 cursor、输入、供应商、时间与收费信息，限量跑到预算边界；cursor 重复/缺失、partial、空页、HTTP 错误分别归类。
5. 空数组、缺字段、供应商 403/404/500、单次不存在都不能自动断言账户转私密或内容删除；只记录此次采集观测。
6. 在另获授权的有上限试跑前，以上只能作为采购/适配候选，不应修改现有默认供应商或启用持续收费任务。
