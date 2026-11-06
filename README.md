# futures-news-monitor

多源期货快讯聚合 → Telegram 推送。

单文件程序，零第三方依赖（只用 Python 标准库）：抓取 14 个中英文快讯源，按品种词表与噪声规则逐条判定，
按 30 分钟窗口聚合成一条摘要后推送到 Telegram。由 launchd 以 `StartInterval=120` 定时拉起，
每轮最多运行 120 秒，文件锁保证单实例。

## 特性

- **单文件**：`monitor.py` 内含抓取、解析、判定、去重、聚合与发送，无外部依赖、无需安装。
- **多源容错**：中文 6 源 + 英文 8 源，单源异常只影响该源本轮成败，不影响其它源与整体推送。
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

## 判定管线

`SKIP → STRONG → NOISE_HARD → 目标价条件 → MACRO → ANCHOR → NOISE_SOFT → CONTEXT`：
逐层短路，任一否决层命中即丢弃，只有全部通过的条目才进入聚合队列。

## 运行

要求 Python 3.11+，不依赖任何第三方包。

```bash
python3 monitor.py --selftest   # 内嵌断言回归
python3 monitor.py              # 跑一轮：抓取 → 判定 → 聚合 → 推送
python3 monitor.py --dry-run    # 只判定不发送（调试用）
```

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
| `chat_id` | 是 | 推送目标会话 ID |
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

## 测试

```bash
python3 tests/sim.py       # 23 个行为场景（临时目录，不碰真实状态）
python3 tests/probe2.py    # 69 条语料判定回归
python3 tests/probe4.py    # 词表边界正反例
python3 tests/ab.py        # 与旧版本 baseline-v4.py 的语料 A/B 对照
```

测试全部离线运行，使用临时目录隔离状态，不会发送真实消息。

## 部署

```bash
./deploy.sh                # selftest → 备份 → 同目录原子替换 → md5 复核
./deploy.sh --with-tests   # 先跑 sim / probe2 / probe4 再部署
```

脚本默认使用 `PATH` 中的 `python3`，可用 `PY=/path/to/python3 ./deploy.sh` 指定解释器。
目标目录为 `~/.pi/agent/bin/`，替换失败或部署后 selftest 不通过会自动回滚到备份。

## 目录

```
monitor.py        主程序（部署源）
baseline-v4.py    旧版判定实现，供 tests/ab.py 对照
deploy.sh         部署脚本
tests/            行为场景、语料回归与 A/B 对照
```

## License

MIT，见 [LICENSE](LICENSE)。
