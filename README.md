# futures-news-monitor

多源期货快讯聚合 → Telegram 推送。

单文件程序，零第三方依赖（只用 Python 标准库）：抓取 21 个中英文快讯、化工现货与研报源，按品种词表与噪声规则逐条判定，
按 30 分钟窗口聚合成一条摘要后推送到 Telegram。由 launchd 以 `StartInterval=120` 定时拉起，
每轮最多运行 120 秒，文件锁保证单实例。

## 特性

- **单文件**：`monitor.py` 内含抓取、解析、判定、去重、聚合与发送，无第三方 Python 依赖、无需安装。
- **多源容错**：中文 6 源 + 英文 8 源 + 化工现货 4 源 + 研报 3 源，单源异常只影响该源本轮成败，不影响其它源与整体推送。
- **研报正文全文**：研报类条目自动下载 PDF 并提取正文全文（上限 24000 字），超长按 `(i/N)` 分片连发，
  中断后从下一片续发、不重发已发片；解析失败或 PDF 无文本层时降级为只发链接。
- **词表驱动判定**：品种别名单一事实源派生强/弱信号词表，中文与英文各一套词表层，按词边界匹配。
- **聚合推送**：默认 30 分钟窗口、窗口内至少 3 条才发；积压上限、单日上限 160 条（触顶后冻结到次日 0 点、
  队列保留）与最长滞留时间都有硬约束。
- **发送可靠**：直连 → 代理 → 直连重试三条腿，仅对传输层错误换腿；429 按 `Retry-After` 退避且不标记已读。
- **可恢复**：已读状态与待发队列落盘，崩溃或重启不会重复推送；单实例文件锁防止定时任务重入。

## 数据源

中文源（6 个，全部直连）：

| 源 | 接口 |
|---|---|
| 财联社 | `www.cls.cn/v1/roll/get_roll_list` |
| 东财 | `np-listapi.eastmoney.com/comm/web/getFastNewsList` |
| 同花顺 | `news.10jqka.com.cn/tapp/news/push/stock` |
| 新浪 | `zhibo.sina.com.cn/api/zhibo/feed?zhibo_id=152` |
| 见闻商品 | `api-one-wscn.awtmt.com/apiv1/content/lives?channel=commodity-channel` |
| SMM | `news.smm.cn/live`（SSR 页面解析） |

英文源（8 个，其中 **Investing** 与 **Hellenic** 需代理）：

| 源 | 接口 | 链路 |
|---|---|---|
| Mining | `www.mining.com/feed` | 直连 |
| OilPrice | `oilprice.com/rss/main` | 直连 |
| MiningTech | `www.mining-technology.com/feed/` | 直连 |
| Nasdaq | `www.nasdaq.com/feed/rssoutbound?category=Commodities` | 直连 |
| CNBC | `www.cnbc.com/id/100727362/device/rss/rss.html` | 直连 |
| MarketWatch | `feeds.content.dowjones.io/public/rss/mw_topstories` | 直连 |
| Investing | `www.investing.com/rss/news_11.rss` | 代理 |
| Hellenic | `www.hellenicshippingnews.com/feed/` | 代理 |

两个代理源与发送链路共用同一 `proxy` 配置；未配置代理时这两个源按失败处理，不回退直连。

## 化工现货源（4 个，全部直连）

| 源 | 接口 | 内容 |
|---|---|---|
| 化销网 | `www.huaxiaowang.com/index/quote/infolist?catid=22`（6 类目） | 甲醇产业链：上游/下游/国内/国际/期货/撮合，下游含 MTBE、甲醛等报价与装置动态 |
| 隆众 | `list.oilchem.net/1/9/` | 甲醇/纯苯等早间提示与装置停车、检修快讯 |
| 我的钢铁 | `nenghua.mysteel.com/breed/jcjxy/` | 能化「甲醇及下游」事件雷达：标题 + 导语，只读公开区 |
| 东财甲醇 | `futures.eastmoney.com/qihuo/ma.html` | 甲醇 tag 聚合页，含金联创被源站挡住的转载长文 |

**准入两级（宁缺毋滥）**：列表级要求「标题含品种词 且（事件词 或 提示/前瞻栏目词）」才值得拉详情；
详情级要求「标题 ∪ 正文同时含事件词与品种词」才入队。事件词是供给侧动作（装置、停车、检修、投产、
减产、复产、限产、事故、不可抗力……），不含涨跌行情词；唯一的歧义词「开工」在「开工率/开工负荷/
开工83.8%」等产能统计语境下不算事件。单提甲醇下游（MTBE/甲醛/二甲醚/醋酸/MTO/MTP）的非期货品种
消息，在命中源内下游词而**未**命中全局品种词时，文本前加 `甲醇下游·` 前缀交给全局判定——全局词表
一字不改，前缀幂等。我的钢铁导语只有 ~181 字（「XX 装置快讯」是固定企业简介模板），故退回标题级
判定并剔除无方向的「装置」；东财聚合页噪声大，列表级先按标题含「甲醇」筛，详情按稿件 id 轮转、
每轮最多 1 篇。

化销网 6 类目按价值排序抓取：上游(13)/下游(14) 先取列表并立刻拉详情，其余类目剩余预算不足时截断
——慢轮不牺牲装置动态，也不会因类目合并而放大源数。

**东财按【甲醇】切段**：东财 tag 会混入多品种日报（实测正文 2605 字、12 个`【xx】`分节，甲醇段从
第 1009 字才开始）。正文有分节标记时，只保留`【甲醇】`起、到下一个`【xx】`之前的段落；**找不到
`【甲醇】`段即丢弃该条**，绝不回退成整段正文（否则会把原油/燃料油/沥青内容挂在甲醇标题下）。
正文没有任何分节标记（纯甲醇日评）时整篇保留。全角/半角括号均识别。

## 研报源（3 个）

| 源 | 接口 | 内容 |
|---|---|---|
| 东财研报 | `reportapi.eastmoney.com/report/list` | 行业/策略研报列表，按商品词表筛选；正文 PDF 取自 `pdf.dfcfw.com` |
| 中金所 | `www.cffex.com.cn/sj/` | 月报 PDF（提取正文全文）+ 日/周官方数据链接 |
| 中期协 | `www.cfachina.org/qx-search/api/wcmSearch/searchDocsByProgram` | 全国期货市场交易情况简报，JSON 列表 + PDF 正文 |

研报源排在全部快讯源之后，按 30 分钟源级节流；条目走研报专用准入（品种层 + 产业层，见下「研报准入门槛」），不再复用快讯五层词表。

PDF 正文只在条目首次进入队列时解析一次：`curl` 下载（单篇 8MB / 8 秒上限）→ `pdftotext` 取前 32 页 →
清洗后保存全文，上限 `SNIPPET_MAX=24000` 字（此前截断到 240 字摘要）。解析侧另有 40000 字硬上限，
只用于识别「被存储上限截断」并追加截断标记。每轮最多解析 2 篇、总预算 14 秒，解析预算与抓取预算
解耦，不挤占快讯源；下载失败、非 PDF 响应、无文本层或 `pdftotext` 缺失时该条降级为「无摘要」，只发原文链接。

**全文分片**：单条消息装不下时，正文按句末/分句边界切成多片（优先在不切断 HTML 标签与实体的位置切），
每片头行追加 `(i/N)` 标记（i 从 1 起），末片附「查看原文」链接。分片独占消息，不与普通条目同条；
每轮最多发 `RESEARCH_PARTS_PER_ROUND=12` 片（约 6 msg/min，低于单聊限流）；续发进度落在队列条目上，
中断后从下一片继续、**不重发已发片**；同一片连续失败 3 次判毒丸，放弃该篇剩余片并出队。

### 研报正文清洗

PDF 末页常带整段券商合规声明（免责/版权/投资评级说明/地址电话），全文推送后会独占最后几条 `(i/N)` 消息。清洗分两道，共用同一套判定：

- **抓取时**：`pdftotext` 原文先按行裁掉尾部声明段，再按存储上限截断；声明段不占推送配额，也不会成为最后一条分片。
- **渲染时**：推送前对队列里的 `snippet` 再裁一次（同一套规则，先按句末标点还原成伪行），覆盖此前已入库的历史条目；不裁时逐字节返回原串，干净正文零改动、重复执行幂等，且不改写 `state`/`queue`。

裁切只从全文**第一个**声明标记处动刀，并叠四道护栏，缺一不裁（宁可少裁，不可误裁）：

1. 标记起点落在全文 **55%** 之后；
2. 标记之前仍有 **≥600 字**正文（短报告不清空）；
3. 标记之后不得再出现正文特征（`数据来源` / `图表` / `库存` / `同比` / `环比` 等）；
4. 自我印证：标记起 **4 行内至少 2 处**命中标记（渲染层的伪行更短，改用 12 段有界窗口）。

**噪声抑制**：同一行重复 **≥3 次**的页眉/页脚/水印不算声明标记，东财「XX证券版权所有发送给…eastmoney.com」「第 8 页共 8 页」这类水印同样排除——重复页脚不能当裁切起点。

**尾部残片**按闭集模式清理，只用 URL / 邮箱 / 电话传真 / 页码（另含邮编、门牌）等高精度模式，并遵循**正文逃逸优先**：命中 `数据来源` / `库存` / `同比` / `万吨` / `元` / `%` / `涨` / `跌` / `图:` / `表:` 的行整行保留；残片必须从行首开始（允许 `·` `-` `|` 装饰符）或「整行基本就是该残片」才删。**删除集只减不增**：后续修复只把行判回正文，不新增删除类别，也不用长度/句末符启发式扩大裁切面；没有声明的研报（如开源证券那份）一字不裁。

### 研报准入门槛

研报只认**品种层**与**产业层**，不复用快讯的五层词表；`MACRO` / `ANCHOR` / `CONTEXT` 不参与研报判定。

- **品种层**：由 `PRODUCT_ALIASES` 派生的品种名与全部别名（`STRONG_EXTRA` 的价格口语与交易场所名不在内，「苹果 / 玻璃」这类裸字仍排除）。
- **产业层**：30 个商品产业链的行业/环节词（煤炭、钢铁、有色金属、农产品、航运、大宗商品等）。
- **硬否决**：`RESEARCH_DENY` 的 13 个词（ETF、基金持仓、机构持仓、北向持仓、信用风险、债券、评级调整、股东、股份回购、回购股份、业绩预告、分红、股息）先于层判定命中即丢——这些是权益、信用与分红口径的稿件，不是商品研究。（窄化口径：「逆回购」「沪铜持仓分析」属品种/公开市场口径，不误杀。）

把「油价 / 金价」这类词留在门外，是因为它们是**价格口语而不是品种**；宏观稿标题几乎不带品种名，所以**标题层判定已经足够**，不需要再引入宏观或语境层。

中金所与中期协**不套**这扇新门：两者的标题是交易所/协会的官方统计口径（「中金所月度数据统计」「全国期货市场交易情况简报」），本身不含品种词，套门会永久归零。新门只接在东财研报的标题 + `industryName` 上。

## 判定管线

`SKIP → STRONG → NOISE_HARD → 目标价条件 → MACRO → ANCHOR → NOISE_SOFT → CONTEXT`：
逐层短路，任一否决层命中即丢弃，只有全部通过的条目才进入聚合队列。

## 失败归因与告警

抓取失败的诊断按 `curl` 退出码归因，不再一律标成「预算耗尽」：

| 退出码 | 归因文案 |
|---|---|
| 28 | `curl 超时` |
| 6 / 7 / 35 | `连接失败(SSL/DNS)` |
| 16 | `HTTP/2 帧错误` |
| 63 | `超过体积上限` |

错误信息同时带 `rc` 与耗时（如 `curl 超时 (rc=28, 6.1s)`）。只有「一次尝试都没发起」就因剩余份额不足 2 秒退出时，
才报「预算耗尽」并注明「未发起尝试」——预算耗尽因此是一个可区分的事实，而不是兜底说法。
归因只改诊断文案，不改抓取时序：单次尝试仍一次用满剩余份额。

告警分两级：全源失败首轮发 1 条，之后每 `ALERT_EVERY=3600` 秒汇总 1 条，恢复再发 1 条；
单源连续失败满 `SRC_ALERT_ROUNDS=6` 轮发 1 条（可由配置 `src_alert_rounds` 覆盖，下限 1），
同轮多个失败源合并成 1 条，恢复发 1 条并清零。全源故障轮不重复计单源失败，恢复轮只发全源恢复那一条。

## 运行

要求 Python 3.11+，不依赖任何第三方 Python 包。

```bash
python3 monitor.py --selftest   # 内嵌断言回归
python3 monitor.py              # 跑一轮：抓取 → 判定 → 聚合 → 推送
python3 monitor.py --dry-run    # 只判定不发送（调试用）
```

PDF 解析依赖 `pdftotext`（poppler，`brew install poppler`），缺失时自动降级为只发链接，其余功能不受影响。

**单日推送上限**默认 160 条（`DAILY_MSG_CAP`）：当日累计触顶后冻结到次日 0 点，新条目照常入队而非丢弃
（队列上限 200 条，超出按最旧丢弃），0 点后恢复推送。取值依据：2026-11-11 实测单日 90 条——研报分片
（单篇可占 8–13 条）叠加新增的 4 个化工源后，消息量较早期注释的「44/天」翻倍，故上限由 80 翻倍至 160 并留出余量。

## 配置

配置文件为 `~/.pi/agent/etc/futures-news.json`，权限必须为 `0600`：

```json
{
  "token": "123456789:AA...",
  "chat_id": "-1001234567890",
  "proxy": "http://127.0.0.1:8080"
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `token` | 是 | Telegram Bot Token；也可用环境变量 `FUTNEWS_TOKEN`（优先级更高，适合配合密钥管理） |
| `chat_id` | 否 | 推送目标会话 ID；缺省时自动从最近消息里的 chat 发现 |
| `proxy` | 否 | 代理 URL，供 2 个需代理的英文源与发送链路使用；也可用 `HTTPS_PROXY` |

已读状态、待发队列与锁文件默认落在 `~/.pi/agent/state/`（首次运行自动创建）。

launchd 定时任务由使用者自行创建，推荐 `StartInterval` 设为 120 秒、标准输出与错误分别重定向到日志文件。

## 退出码

| 码 | 含义 |
|---|---|
| 0 | 正常（含“另一实例在跑，本轮跳过”） |
| 1 | 全源失败 |
| 2 | 发送失败 |
| 3 | 缺少 bot token |
| 4 | 状态文件不可读 |
| 5 | 状态落盘失败 |
| 6 | 队列落盘失败 |
| 7 | 锁文件不可写 |

检视模式：`--stats` / `--queue` 为只读检视，**零落盘**——不写 state / queue / pending，损坏文件只留痕不改名，
退出码恒为 0（不因源失败返 1）。`--dry-run` 仍按普通 dry 语义：只判定不发送，状态照常推进。

## 测试

```bash
python3 monitor.py --selftest        # 内嵌断言回归（560 条，含 B19 --collect 钉）
python3 items_server.py --selftest   # items 读面离线自检（不监听端口）
python3 tests/sim.py                 # 23 个行为场景（临时目录，不碰真实状态）
python3 tests/probe2.py              # 69 条语料判定回归
python3 tests/probe4.py              # 词表边界正反例
python3 tests/ab.py                  # 与旧版本 baseline-v4.py 的语料 A/B 对照
python3 tests/golden.py --check      # 31 个确定性场景的行为等价基线
python3 tests/test_items_archive.py  # items 存档契约（JSONL/去重/轮转/dump）
python3 tests/test_items_server.py   # items 读面契约（FS-01..FS-11，真端口）
python3 tests/test_collect.py        # --collect 采集模式（MC-01..MC-04，实跑 main）
```

测试全部离线运行，使用临时目录隔离状态，不会发送真实消息。

### selftest 的真实 PDF 样本

`--selftest` 里的 PDF 全文解析断言（B11-04）跑的是**真实 PDF**，不是打桩：

- 样本以 base64 内嵌在 `monitor.py`（4,079B 的合成 PDF，6 页、含中文正文），运行期解码到临时文件，
  断言结束在 `finally` 里删除——**单文件**即可自测，不依赖 `tests/fixtures/` 或任何外部夹具。
- `FUTNEWS_SAMPLE_PDF=/path/to/sample.pdf` 可覆盖成外部真 PDF，优先级高于内嵌样本。
- `pdftotext` 默认在 `/opt/homebrew/bin`、`/usr/local/bin` 探测，可用 `FUTNEWS_PDFTOTEXT` 指定路径。

缺少 `pdftotext` 时这条断言**显式打印 `SKIP` 并单独计数**，不计入 ok，其余断言照跑；环境变量指向的样本
不存在、内嵌样本解码失败、解析失败（含 `pdftotext` 报错或取不到正文）一律判红。summary 行因此是三段式
`selftest: <ok> ok / <skip> skip / <BAD> BAD`，`ok + skip + BAD` 恒等于断言总数。

设计原则：**测试跑不了必须说出来，不能显示通过**——样本或工具缺失只降级为 `SKIP` 并单独计数，
不允许用打桩结果冒充 `ok`；需要真解析的断言宁可红，也不静默放过。

### 行为等价基线

`tests/golden.py` 把 31 个确定性场景的**可观测输出**冻结成快照：stdout / stderr、发送记录（`sends.jsonl`）、
落盘状态文件与退出码。场景覆盖抓取失败组合、告警阈值、聚合窗口与积压、限流退避、发送时间闸门、
状态损坏与落盘失败、锁不可写、发送三腿回退以及真实源函数等。快照连同逐文件 SHA256 存放在
`tests/golden/`（182 个夹具文件 + `MANIFEST.json`，约 760K）。

```bash
python3 tests/golden.py --check                        # 默认模式：先校验 MANIFEST，再跑 31 场景比对
python3 tests/golden.py --check --scenario proxy_leg    # 只跑单个场景（仍先校验整个 MANIFEST）
```

`--check` 先校验每个夹具的 SHA256 与 `MANIFEST.json` 记录一致（防止快照被手工改动，不一致直接报错退出），
再逐场景重放并与快照逐字节比对。用途是给重构上锁：判定、聚合、发送与状态机的**行为**必须保持等价，
任何 diff 一律按回归处理。只有确认行为变化符合预期时才允许 `--record` 重录，且必须同时提供
`--reason` 与环境变量 `GOLDEN_RECORD_APPROVED`（两者取值一致），重录会重算 MANIFEST 的哈希。

## 部署

```bash
./deploy.sh                # selftest → 备份 → 同目录原子替换 → md5 复核
./deploy.sh --with-tests   # 先跑 sim / probe2 / probe4 再部署
```

脚本默认使用 `PATH` 中的 `python3`，可用 `PY=/path/to/python3 ./deploy.sh` 指定解释器。
目标目录为 `~/.pi/agent/bin/`，替换失败或部署后 selftest 不通过会自动回滚到备份。

## items 读面服务（items_server.py）

`items_server.py` 是 items 存档的只读 HTTP 读面，供 LAN 内消费端（如 commodity-trade-brief 的
`fnm_adapter --fnm-url` / `--fnm-config`）跨机拉取 `~/.pi/agent/state/futures-news-items.jsonl`。
单文件、stdlib-only，launchd `KeepAlive` 常驻；每请求现读存档，无缓存。

### 契约

- `GET /bot<token>/items` → `200 {"schema":1,"items":[...]}`（裸形，非 TG ok 包络）
- `GET /bot/items` + `Authorization: Bearer <token>` → 同上（bearer 形，token 不进 URL/进程表）
- 存档缺席或为空 → `200 {"schema":1,"items":[]}`（不是 404）
- 错误一律 TG 包络形：`401 unauthorized` / `404 not found` / `405 method not allowed`
- 请求日志中 token 段一律掩码为字面 `/bot<token>/items`；token 永不进日志/响应体

### 配置（`~/.pi/agent/etc/futures-news.json` 同档）

| 字段 | 缺省 | 说明 |
|---|---|---|
| `items_token` | 无（必填） | 读面 token；也可用环境变量 `FNM_ITEMS_TOKEN`（优先级更高）。两者皆无 → 启动即以退码 3 退出 |
| `items_bind` | `127.0.0.1` | 绑定地址；开放 LAN 时写 LAN IP 或 `0.0.0.0` |
| `items_port` | `8766` | 监听端口（非法值 → 退码 2） |
| `items_cidrs` | 无 | 网段白名单列表，如 `["192.168.31.0/24"]`；缺席时仅环回可达（fail-closed），环回始终放行 |

生产建议 bearer 形（消费端走 `--fnm-config` 文件径）：URL-path 形的 token 会出现在
URL 与对端进程命令行。token 轮换只需改 conf/env 后重启服务（`launchctl kickstart -k`）。

### 部署

```bash
cp items_server.py ~/.pi/agent/bin/futures-news-items.py
chmod +x ~/.pi/agent/bin/futures-news-items.py
python3 ~/.pi/agent/bin/futures-news-items.py --selftest   # 离线自检, 不监听端口
cp deploy/com.user.futures-news-items.plist ~/Library/LaunchAgents/
# 按实际用户名修正 plist 内 /Users/meow 路径
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.user.futures-news-items.plist
```

stdout/stderr 分别落在 `/tmp/futures-news-items.stdout.log` / `/tmp/futures-news-items.stderr.log`。

采集侧配套：无 TG 出口的机器（如 meow）用 `python3 monitor.py --collect` 跑抓取——跳过 token 闸
与全部 TG 出站（告警/发送/恢复通知全免），数据面照常 `fetch → queue_upsert → items 存档 →
queue/state 落盘`，退码语义不变；与 `--dry-run` 正交，组合取交集（零落盘 + 零 TG）。

## 目录

```
monitor.py          主程序（部署源）
items_server.py     items 存档只读 HTTP 读面（部署为 futures-news-items.py）
baseline-v4.py      旧版判定实现，供 tests/ab.py 对照
deploy.sh           部署脚本
deploy/             launchd plist 模板（items server KeepAlive）
tests/              行为场景、语料回归与 A/B 对照
tests/golden.py     行为等价基线：31 场景录制/校验
tests/golden/       快照夹具 + MANIFEST.json（逐文件 SHA256）
```

## License

MIT，见 [LICENSE](LICENSE)。
