# futures-news-monitor

多源期货快讯聚合 → Telegram 推送。

单文件程序，零第三方依赖（只用 Python 标准库）：抓取 17 个中英文快讯与研报源，按品种词表与噪声规则逐条判定，
按 30 分钟窗口聚合成一条摘要后推送到 Telegram。由 launchd 以 `StartInterval=120` 定时拉起，
每轮最多运行 120 秒，文件锁保证单实例。

## 特性

- **单文件**：`monitor.py` 内含抓取、解析、判定、去重、聚合与发送，无第三方 Python 依赖、无需安装。
- **多源容错**：中文 6 源 + 英文 8 源 + 研报 3 源，单源异常只影响该源本轮成败，不影响其它源与整体推送。
- **研报正文摘要**：研报类条目自动下载 PDF 并提取正文摘要，附原文链接；解析失败或 PDF 无文本层时降级为只发链接。
- **词表驱动判定**：品种别名单一事实源派生强/弱信号词表，中文与英文各一套词表层，按词边界匹配。
- **聚合推送**：默认 30 分钟窗口、窗口内至少 3 条才发；积压上限、单日条数上限与最长滞留时间都有硬约束。
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

## 研报源（3 个）

| 源 | 接口 | 内容 |
|---|---|---|
| 东财研报 | `reportapi.eastmoney.com/report/list` | 行业/策略研报列表，按商品词表筛选；正文 PDF 取自 `pdf.dfcfw.com` |
| 中金所 | `www.cffex.com.cn/sj/` | 月报 PDF（提取正文摘要）+ 日/周官方数据链接 |
| 中期协 | `www.cfachina.org/qx-search/api/wcmSearch/searchDocsByProgram` | 全国期货市场交易情况简报，JSON 列表 + PDF 正文 |

研报源排在全部快讯源之后，按 30 分钟源级节流；条目仍走栏目与噪声否决，但豁免「品种锚点」硬门槛（研报标题常只写品种名，不带信号词）。

PDF 正文只在条目首次进入队列时解析一次：`curl` 下载（单篇 8MB / 8 秒上限）→ `pdftotext` 取前 8 页 → 清洗截断为摘要。每轮最多解析 2 篇、总预算 14 秒，解析预算与抓取预算解耦，不挤占快讯源。下载失败、非 PDF 响应、无文本层或 `pdftotext` 缺失时该条降级为「无摘要」，只发原文链接。

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
python3 monitor.py --selftest    # 内嵌断言回归（414 条）
python3 tests/sim.py             # 23 个行为场景（临时目录，不碰真实状态）
python3 tests/probe2.py          # 69 条语料判定回归
python3 tests/probe4.py          # 词表边界正反例
python3 tests/ab.py              # 与旧版本 baseline-v4.py 的语料 A/B 对照
python3 tests/golden.py --check  # 28 个确定性场景的行为等价基线
```

测试全部离线运行，使用临时目录隔离状态，不会发送真实消息。

### 行为等价基线

`tests/golden.py` 把 28 个确定性场景的**可观测输出**冻结成快照：stdout / stderr、发送记录（`sends.jsonl`）、
落盘状态文件与退出码。场景覆盖抓取失败组合、告警阈值、聚合窗口与积压、限流退避、发送时间闸门、
状态损坏与落盘失败、锁不可写、发送三腿回退以及真实源函数等。快照连同逐文件 SHA256 存放在
`tests/golden/`（163 个夹具文件 + `MANIFEST.json`，约 600K）。

```bash
python3 tests/golden.py --check                        # 默认模式：先校验 MANIFEST，再跑 28 场景比对
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

## 目录

```
monitor.py          主程序（部署源）
baseline-v4.py      旧版判定实现，供 tests/ab.py 对照
deploy.sh           部署脚本
tests/              行为场景、语料回归与 A/B 对照
tests/golden.py     行为等价基线：28 场景录制/校验
tests/golden/       快照夹具 + MANIFEST.json（逐文件 SHA256）
```

## License

MIT，见 [LICENSE](LICENSE)。
