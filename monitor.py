#!/usr/bin/env python3
"""国内期货快讯聚合监控 (v5)

多源聚合 -> 跨源精确去重 + 时间窗近邻合并 -> 期货相关度过滤 -> 新鲜度闸门
  -> 聚合队列 (攒窗口再发, 不再「有就推」) -> Telegram 推送

数据源 (中文 6 源直连; 英文 8 源中 6 直连 + 2 代理):
  财联社   www.cls.cn/v1/roll/get_roll_list        (需 sign=md5(sha1(排序query)))
  东财     np-listapi.eastmoney.com getFastNewsList
  同花顺   news.10jqka.com.cn/tapp/news/push/stock
  新浪     zhibo.sina.com.cn feed zhibo_id=152
  见闻商品 api-one-wscn.awtmt.com content/lives channel=commodity-channel (黑色/农产/能化, 低频高值)
  SMM      news.smm.cn/live (SSR __NEXT_DATA__, 有色产业快讯; 站点强制 gzip, --compressed 是硬依赖)

  英文源 (Batch 6, RSS 2.0; 前 6 个直连, 后 2 个经 conf.proxy / HTTPS_PROXY):
  Mining      www.mining.com/feed                                 (36 条, 矿业/金属)
  OilPrice    oilprice.com/rss/main                               (15 条, 原油/天然气)
  MiningTech  www.mining-technology.com/feed/                     (10 条, 需 Feed 客户端 UA)
  Nasdaq      www.nasdaq.com/feed/rssoutbound?category=Commodities (15 条, RTTNews)
  CNBC        www.cnbc.com/id/100727362/device/rss/rss.html       (30 条, 全球宏观)
  MarketWatch feeds.content.dowjones.io/public/rss/mw_topstories  (10 条, 道琼斯系, GMT)
  Investing   www.investing.com/rss/news_11.rss                   (10 条, 仅有标题)
  Hellenic    www.hellenicshippingnews.com/feed/                  (20 条, 干散货/运价)

凭据: bot token 只从环境变量 FUTNEWS_TOKEN 或 ~/.pi/agent/etc/futures-news.json 的
      token 字段读取; 两者皆无时打印错误并以非 0 退出 (源码中不留任何凭据)。

发送: 三条腿 直连(8s) -> 代理(20s, 取自配置 proxy 或 HTTPS_PROXY) -> 直连重试(20s)。
      只有传输层瞬时错误 (SSL EOF / DNS / 无路由 / 超时) 才换腿, HTTP 错误直接放弃;
      每条腿之间 sleep 3s。历史 err 里 "u16=1019~1520 发送失败" 全是 SSL EOF 被压成
      一句话的误标, 与长度无关 (真超长走的是 split_msg 分支)。

退出码: 0 正常 / 1 全源失败 / 2 发送失败 / 3 缺 token / 4 state 不可读
        5 state 落盘失败 / 6 队列落盘失败 / 7 锁文件不可写

用法:
  futures-news-monitor.py [--dry-run] [--seed] [--all] [--limit N] [--stats] [--explain]
                          [--flush-now] [--queue] [--selftest] [--dump-items PATH]

读面 (spec §3.11): 新进队列的条目按 key 去重真追加到 items 存档
      ~/.pi/agent/state/futures-news-items.jsonl (JSONL 每行一条
      {"src","ts","text","key","url"?,"snippet"?,"schema":1}), 供 fnm_adapter 聚合;
      schema 版本另记旁路 futures-news-items.meta.json (§3.9);
      行数 > 10000 时原子轮转保留最近 6000 (R-9); --dump-items 原子导出快照。
"""
from __future__ import annotations

import difflib
import email.utils
import fcntl
import hashlib
import json
import math
import os
import html
import inspect
import io
import re
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:                                     # POSIX only: pdftotext 子进程内存上限 (Batch 7 修复 A5)
    import resource
except ImportError:                      # Windows 无 resource; 降级为不设上限 (本项目目标 macOS)
    resource = None

HOME = Path.home()
CONF = HOME / ".pi/agent/etc/futures-news.json"
STATE = HOME / ".pi/agent/state/futures-news-seen.json"
PENDING = HOME / ".pi/agent/state/futures-news-pending.json"   # 已发送但 state 落盘失败的待确认键
QUEUE = HOME / ".pi/agent/state/futures-news-queue.json"       # v5 聚合队列 (攒到窗口再发)
LOCK = HOME / ".pi/agent/state/futures-news.lock"
ITEMS_ARCHIVE = HOME / ".pi/agent/state/futures-news-items.jsonl"      # items 存档 (读面数据形, spec §3.11)
ITEMS_META = HOME / ".pi/agent/state/futures-news-items.meta.json"     # schema 版本闸旁路 (spec §3.9)
LOG = Path("/tmp/futures-news.out")
LOG_MAX = 2 * 1024 * 1024
ERRLOG = Path("/tmp/futures-news.err")   # launchd StandardErrorPath; 必须原地轮转 (U3)

TZ_CN = ZoneInfo("Asia/Shanghai")   # 源站与用户都在 +08:00; 不依赖宿主 TZ
FUTURE_TOL = 120                     # 未来时间容差(秒), 超出的条目直接丢弃
NEAR_WINDOW = 30 * 60                # 近邻合并时间窗(秒)
NEAR_RATIO = 0.90                    # 近邻合并阈值(实测依据见 w2-fix 报告 F-13)
NEAR_MIN_LEN = 8                     # 太短的签名不做近似比对(误合并风险高)
NUM_RE = re.compile(r"\d+")          # 数字不同视为不同事件, 避免「库存减12万吨/减10万吨」被误合并
ALERT_EVERY = 3600                   # 全源故障期间两条告警的最小间隔(秒)
SRC_ALERT_ROUNDS = 3                 # 单源连续失败多少轮发告警 (可被 conf.src_alert_rounds 覆盖)
SRC_ALERT_EVERY = ALERT_EVERY        # 单源告警抑制窗口(秒), 与全源告警保持一致
SRC_FAIL_KEY = "_src_fail"           # seen 里的单源失败状态块; '_' 前缀保证 prune 保留
SRC_SKIP = "__budget_skip__"         # outcomes 哨兵: 全局预算跳过, 既不算成功也不算失败 (Batch 6 U4)
BUDGET_S = 45                        # 全局抓取预算(秒): BUDGET_S + 单次 send 最坏(按 send_plan() 推导) + 本地开销 < 120s
# 单源最小份额(秒)依据: 生产日志 626 轮实测 p99/max —— 见闻商品 5.40/7.50s、Nasdaq 4.10/4.20s、
# 东财 max 5.20s; 5.0s 会把见闻商品约 1% 的轮次砍成失败, 故抬到 7.0 (Batch 6.1 U-B / fix C1)
MIN_SLICE = 7.0                      # 单源抓取最小份额(秒)

# ---- Batch 7 研报源 (东财研报 / 中金所 / 中期协): 列表抓取 + PDF 正文摘要
# 设计口径: PDF 只在「条目首次进入队列」时解析一次 (见 extract_research), 历史条目
# 永远不重复下载; 解析预算与 45s 抓取预算解耦, 保证既有 14 源的时间行为不变。
RESEARCH_EVERY_S = 1800              # 研报源最小运行间隔(秒): 研报是分钟级信息, 30 分钟足够
RESEARCH_MAX_PDF = 2                 # 每轮最多解析的 PDF 篇数 (串行, 单篇实测 1-2s)
PDF_TIMEOUT = 6.0                    # pdftotext 子进程最长秒数
# 每轮 PDF 总预算 = 下载 8s + 解析 6s = 14s: 45(抓取)+14=59 <= ROUND_SOFT_S(60)。
# ROUND_MARGIN_S(6s 本地余量)另计, 最坏情况只会把发送推迟到下一轮, 不会丢消息。
PDF_BUDGET_S = 14.0                  # 每轮 PDF 下载+解析总预算(秒); 硬上界见 extract_research
PDF_DL_BUDGET_S = PDF_BUDGET_S - PDF_TIMEOUT   # 单篇下载子预算(8s): 必须给 pdftotext 留出解析时间
PDF_MAX_BYTES = 8 * 1024 * 1024      # 单个 PDF 上限 8MB (实测最大为中金所月报 2.17MB)
PDF_CAP = 10                         # curl 最长秒数上限; 生产路径被 PDF_DL_BUDGET_S 夹到 <=8s
PDF_RLIMIT_AS = 256 * 1024 * 1024    # pdftotext 子进程内存上限: 391KB PDF 炸弹实测 926MB RSS; 真实 PDF 仅 ~10MB
PDF_MAX_PAGES = 8                    # 只解析前 N 页: 8 页保住中金所月报 240 字摘要, 后半是表格/附录
RESEARCH_URL_MAX = 1000              # 研报链接上限; 超长 URL 会让单条 u16>4096 且 split_msg 无法拆
RESEARCH_META_MAX = 200              # 研报元信息(text 的「机构 · 日期」段)上限
PDF_MIN_CHARS = 40                   # 清洗后正文短于此数 -> 判「无文本层」, 摘要留空
SNIPPET_MAX = 240                    # 摘要取前 N 字 (需求区间 200-300)
RESEARCH_TAG = "研究"                # 研报 key 前缀 (含 ':' -> 与 dedupe_key 输出天然不相交)
RESEARCH_SRCS = ("东财研报", "中金所", "中期协")
# pdftotext 为 poppler CLI (非 Python 依赖); 不存在时 pdf_text 直接降级为「无摘要」
PDFTOTEXT = (os.environ.get("FUTNEWS_PDFTOTEXT")
             or next((p for p in ("/opt/homebrew/bin/pdftotext", "/usr/local/bin/pdftotext")
                      if os.path.exists(p)), ""))

ROUND_SOFT_S = 60                    # 单轮软截止(秒): 120 - 单次 send 最坏(由 send_plan() 推导, 当前 54) - 余量 6
ROUND_HARD_S = 120                   # launchd StartInterval 硬上界(秒): 单轮总耗时不得超过
ROUND_MARGIN_S = 6                   # 本地阶段(锁/落盘/过滤)余量(秒)
ROUND_START = 0.0                    # main 每轮起点(time.monotonic); 逐次发送闸门共用 (Batch 6 F-02)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
PROXY = os.environ.get("HTTPS_PROXY") or ""   # 未设置则纯直连
CFG: dict = {}                        # main 每轮注入的配置快照 (英文代理源读 proxy; Batch 6 U2)
SEEN_MAX = 3000
MSG_MAX = 3800                       # 按 UTF-16 码元计长, 留出 4096 上限余量
ITEM_MAX = 700

# ---- v5 聚合节奏 (治「每 2 分钟一条、46% 只有一条」)
DIGEST_MIN = 30                      # 聚合窗口(分钟): 到点才发一次
DIGEST_MIN_ITEMS = 3                 # 窗口到点也要至少这么多条才发 (整点单条不响)
DIGEST_MAX_ITEMS = 40                # 队列攒到这么多立刻提前发 (尖峰保护)
MAX_HOLD_MIN = 45                    # 最早一条在队里最多待这么久就强制发 (V2: 90 -> 45, 单条迟到减半)
QUEUE_MAX = 200                      # 队列硬上限, 超出的最旧条目丢弃并记账
DAILY_MSG_CAP = 80                   # 单日通知条数上限 (安全阀; 实测自然峰值 44/天)

# ---- items 存档 (spec §3.11 / R-9: JSONL 真追加 + key 去重 + 行数轮转)
ITEMS_ARCHIVE_MAX = 10000            # 存档行数硬上限, 超过即原子轮转
ITEMS_ARCHIVE_KEEP = 6000            # 轮转后保留最近 N 行 (丢弃最旧 ~40%)
ITEMS_SCHEMA = 1                     # items 数据形 schema 版本 (§3.9)

# 通用短标题: 不能单独作为去重签名, 必须回退到正文
GENERIC_TITLES = {"快讯", "要闻", "早报", "午报", "晚报", "研报", "提醒", "公告", "周报",
                  "日报", "快评", "观点", "盘前", "盘中", "收盘", "下周提醒", "本周提醒",
                  "今日提醒", "重要", "突发"}
# 栏目类内容 (日历/提醒/榜单, 不是快讯): 判定前先剥离日期/括号前缀, 再做子串判定
SKIP_RE = re.compile(r"(下周提醒|本周提醒|今日提醒|数据前瞻|财经日历|日历|重点关注数据|"
                     r"重要数据|今日看点|盘前必读|收盘综述|涨跌榜|数据公布时间|会议日程)")
BOILER = re.compile(
    r"^(财联社|SHMET|上海金属网|格隆汇|金十数据|新华财经|上证报|中证报|期货日报)?"
    r"\s*\d{1,2}月\d{1,2}日(讯|电)[，,、:：]?\s*")
PUNCT = re.compile(r"[\s\u3000·:：,，。、;；!！?？\"'“”‘’()（）\[\]【】<>《》—\-…\.\|]")
WEEKDAY = re.compile(r"(?:周|星期)[一二三四五六日天]")   # 不同星期几的日历/提醒条目不能近邻合并
# 日期/时刻前缀 (用于剥离后再判栏目, F-11)
DATE_PREFIX = re.compile(
    r"^\s*(?:(?:19|20)\d{2}\s*[年\-/\.]\s*)?\d{1,2}\s*[月\-/\.]\s*\d{1,2}\s*日?"
    r"(?:\s*(?:周|星期)[一二三四五六日天])?"
    r"(?:\s*\d{1,2}\s*[:：]\s*\d{2}(?:\s*[:：]\s*\d{2})?)?\s*[：:，,、]?\s*")
TIME_PREFIX = re.compile(r"^\s*\d{1,2}\s*[:：]\s*\d{2}\s*[：:，,、]?\s*")

# ---------------- 词表 (Batch 2 v2: 表驱动 + 五层判定) ----------------
# 单一事实源: 品种 -> 该品种的 STRONG 别名 (含交易所/外盘前缀写法)
#   裸品种名自动进 ANCHOR; 新增品种只需在这里加一行, STRONG/ANCHOR/ACRONYMS 全部自动跟随
PRODUCT_ALIASES: dict[str, tuple[str, ...]] = {
    "铜": ("沪铜", "期铜", "伦铜", "LME铜", "国际铜", "电解铜", "铜精矿"),
    "铝": ("沪铝", "期铝", "伦铝", "LME铝", "电解铝"),
    "锌": ("沪锌", "期锌", "伦锌", "LME锌"),
    "铅": ("沪铅", "期铅", "伦铅", "LME铅"),
    "镍": ("沪镍", "期镍", "伦镍", "LME镍"),
    "锡": ("沪锡", "期锡", "伦锡", "LME锡"),
    "黄金": ("沪金", "期金", "伦金", "COMEX黄金", "伦敦金"),
    "白银": ("沪银", "期银", "伦银", "COMEX白银", "伦敦银", "白银期货"),
    "氧化铝": ("氧化铝", "AO"),
    "不锈钢": ("不锈钢",),
    "玻璃": ("玻璃期货", "玻璃连续", "玻璃主力"),
    "螺纹钢": ("螺纹钢", "螺纹"),
    "热卷": ("热卷", "热轧"),
    "铁矿石": ("铁矿石", "铁矿"),
    "焦煤": ("焦煤", "炼焦煤"),
    "焦炭": ("焦炭",),
    "硅铁": ("硅铁",),
    "锰硅": ("锰硅",),
    "原油": ("原油", "布油", "美油", "WTI", "Brent", "Brent原油"),
    "燃料油": ("燃料油", "低硫燃料油", "低硫燃油", "高硫燃油", "燃油"),
    "沥青": ("沥青",),
    "液化石油气": ("液化石油气", "液化气"),
    "甲醇": ("甲醇",),
    "PTA": ("PTA",),
    "乙二醇": ("乙二醇",),
    "短纤": ("短纤",),
    "聚丙烯": ("聚丙烯", "PP"),
    "聚乙烯": ("聚乙烯", "塑料", "LLDPE"),
    "PVC": ("PVC",),
    "尿素": ("尿素",),
    "纯碱": ("纯碱",),
    "纸浆": ("纸浆",),
    "橡胶": ("橡胶", "天胶", "RU"),
    "苯乙烯": ("苯乙烯",),
    "烧碱": ("烧碱",),
    "对二甲苯": ("对二甲苯", "PX"),
    "豆粕": ("豆粕",),
    "菜粕": ("菜粕",),
    "豆油": ("豆油",),
    "棕榈油": ("棕榈油",),
    "玉米": ("玉米",),
    "淀粉": ("淀粉",),
    "白糖": ("白糖", "原糖", "ICE糖"),
    "棉花": ("棉花", "郑棉"),
    "棉纱": ("棉纱",),
    "生猪": ("生猪",),
    "鸡蛋": ("鸡蛋",),
    "苹果": ("苹果期货", "苹果连续", "苹果主力"),
    "红枣": ("红枣",),
    "花生": ("花生",),
    "工业硅": ("工业硅", "金属硅", "多晶硅"),
    "碳酸锂": ("碳酸锂", "锂精矿", "锂矿", "磷酸铁锂", "氢氧化锂"),
    "原木": ("原木",),
    "集运指数": ("集运指数", "SCFIS", "欧线"),
    "瓶片": ("瓶片",),
    "纯苯": ("纯苯",),
    "瓦楞纸": ("瓦楞纸",),
    "双胶纸": ("双胶纸",),
    "铂": ("铂金", "NYMEX铂金"),
    "天然气": ("天然气", "LNG", "亨利港"),
    "汽油": ("汽油", "NYMEX汽油", "RBOB"),
    "取暖油": ("取暖油", "柴油", "馏分油", "精炼油"),
}
# STRONG 额外项: 交易所 / 股指期货全称 / 黄金需求指标 (非品种别名)
STRONG_EXTRA = [
    "LME", "COMEX", "SHFE", "DCE", "CZCE", "INE", "NYMEX", "CBOT", "ICE",
    "上期所", "大商所", "郑商所", "广期所", "中金所", "上期能源",
    "铜价", "铝价", "锌价", "铅价", "镍价", "锡价", "金价", "银价", "油价",
    "洲际交易所", "芝加哥期货交易所", "纽约商品交易所",
    "黄金储备", "央行购金", "世界黄金协会", "黄金协会",
    "中证1000指数期货", "中证500指数期货", "沪深300指数期货", "上证50指数期货",
    "中证1000股指期货", "中证500股指期货", "沪深300股指期货", "上证50股指期货",
]
# STRONG: 具体期货品种/合约/交易所 —— 命中即保留, 即使同时出现噪声词
STRONG = [a for al in PRODUCT_ALIASES.values() for a in al] + STRONG_EXTRA

# NOISE_HARD: 股票/大盘/境外股指/非商品口径 —— **前置否决** (在 MACRO 之前)
#   ("沪深300股指期货涨停" 靠 STRONG 先返回, 不会被 "股指期货" 否决)
NOISE_HARD = [
    "解禁", "股东", "减持", "增持", "回购", "业绩", "净利", "营收", "股价", "市值",
    "中标", "签约", "战略合作", "排产", "新品", "发布会", "上市首日", "涨停", "连板",
    "龙虎榜", "定增", "增发", "并购", "重组", "股权激励", "员工持股", "分红", "预增",
    "预减", "扭亏", "亏损", "三季报", "半年报", "年报", "招股", "苹果公司", "iPhone",
    # 指数/大盘/个股语境 (F-10)
    "沪指", "上证指数", "深证成指", "深成指", "创业板指", "创业板", "科创50", "北交所",
    "两市", "成交额", "北向资金", "南向资金", "涨跌家数", "大盘", "个股", "板块", "概念股",
    "A股", "港股", "恒生指数", "恒生科技",
    "融资余额", "主力资金", "沪股通", "深股通", "券商", "公募", "私募", "基金净值",
    # 境外股指期货/股市快讯 (原来靠 CORE 的「期货」二字混进来, 实测 127 条)
    "纳指", "纳斯达克", "标普500", "道指", "日经", "富时", "韩国综指", "德国DAX",
    "美国股指期货", "美股", "欧股", "亚太股市",
    # 显式非商品口径 (R1 §3a: 让这些「靠只剩期货二字」的丢弃有可读原因)
    "国债期货", "利率期货", "股指期货", "恒指期货", "股票期货", "债券期货",
    # 同形词黑名单 (R2/R3 实测: 单字锚点误命中地名/人名/公司名/加工品)
    "无锡", "麦肯锡", "尹锡悦", "苏锡", "锡林郭勒",
    "中国铝业", "铜陵有色", "云南铜业", "江西铜业", "北方铜业",
    "铝箔", "铜箔", "铜棒", "铜带", "铝型材", "铜管", "铜板", "铝板",
    "垣铂", "苹果产品", "苹果官方", "苹果Apple", "苹果手机", "真金白银",
    "黄金概念", "黄金股", "金矿股", "黄金时代",
    "铜仁", "铜川", "锡山", "锡盟", "铝城", "铅山", "镍都", "铜锣湾", "铅笔",
    "紧急降落", "迫降",
    "锌合金", "铅酸", "玻璃钢", "玻璃纤维", "玻璃幕墙", "玻璃杯",
    "股权", "收购", "并购", "增持", "回购", "股东", "营业利润", "净利润", "营收", "毛利率", "每股收益",
    "加息概率", "降息概率", "山东黄金", "中金黄金", "赤峰黄金", "紫金矿业",
    "洛阳钼业", "天齐锂业", "赣锋锂业", "锂电",
]
# MACRO: 宏观事件 —— 大宗商品的定价变量; 放在 NOISE_HARD 之后 (纳指+降息 -> 丢)
MACRO = [
    "美联储", "非农", "OPEC", "欧佩克", "石油输出国组织", "降息", "加息", "降准",
    "大宗商品", "霍尔木兹", "中东局势", "地缘局势", "非农就业", "利率决议",
]
# MACRO_EVENT: 「宏观词 + 其中之一」才算真事件; 否则只是官员表态/概率快讯 (V4R: 173 条官话)
MACRO_EVENT = [
    "霍尔木兹", "中东局势", "地缘局势", "OPEC", "欧佩克", "石油输出国组织",
    "非农就业", "非农数据", "利率决议", "决议", "基点", "bp", "议息", "会议纪要",
    "加息25", "降息25", "降息50", "加息50", "降息75", "加息75",
    "宣布降息", "宣布加息", "宣布降准", "降息预期", "加息预期", "维持利率不变",
    "第二次降息", "第二次加息", "第三次降息", "第三次加息",
    "第四次降息", "第四次加息", "第五次降息", "第五次加息", "连续降息", "连续加息",
    "开启降息", "重启降息", "降息周期", "加息周期",
    "个百分点", "BP", "降准1",
    "大宗商品", "CPI", "PMI",
]
# ANCHOR: 商品/产业锚点 —— 命中即保留 (裸品种名自动来自 PRODUCT_ALIASES)
ANCHOR_EXTRA = [
    "铂", "钯", "高炉", "电炉", "粗钢", "钢材", "钢坯", "铁水", "废钢", "钢协", "中钢协",
    "煤炭", "煤矿", "电煤", "焦化", "丙烯", "豆一", "豆二", "早籼", "粳米",
    "仓单", "基差", "升贴水", "交割", "到港", "装置", "冶炼", "矿山", "矿山机械",
    "能繁母猪", "成品油", "炼油", "猪价", "鸡苗", "石油",
    "锂", "稀土", "钴", "钼", "稀土永磁",
    "运价", "集装箱", "航运指数", "干散货", "期市",
    "小麦", "咖啡", "可可", "牛肉", "化肥", "钾肥", "磷肥",
    "粮食出口", "食用油", "植物油", "葵花籽油", "石化产品", "海上平台",
    "AISI", "Mysteel", "产能利用率", "桶/日", "万桶", "炼气厂", "气田",
    "亿立方英尺", "LNG船",
    "沙特阿美", "SPDR", "SilverTrust", "iShares",
]
# 裸字泛化太强、必须靠合约别名命中的品种 (苹果=公司/水果, 玻璃=建材/日用)
BARE_ANCHOR_SKIP = {"苹果", "玻璃"}
_STRONG_SET = set(STRONG)
# 裸品种名进 ANCHOR; 已在 STRONG 的 (原油/甲醇/纯碱...) 不重复登记, 避免层级重叠
ANCHOR_EXTRA += ["玻璃现货", "玻璃库存", "玻璃生产线", "玻璃厂", "浮法玻璃",
                 "苹果现货", "苹果库存", "苹果产地", "苹果主产区", "苹果价格"]
ANCHOR = sorted(set(ANCHOR_EXTRA) | {p for p in PRODUCT_ALIASES
                if p not in _STRONG_SET and p not in BARE_ANCHOR_SKIP})
# NOISE_SOFT: 研报/机构口径 —— **放在 ANCHOR 之后**
#   (R1: 「标普预计铜价14000美元」含真品种预测, 不能被「评级」一刀切; 「工业有色ETF」无锚点才丢)
NOISE_SOFT = [
    "研报", "评级", "ETF", "黄金ETF", "投资者关系", "港交所", "香港交易所", "分析师",
    "持股比例", "概念",
]
# CONTEXT: 泛产业链/交易语境 —— **单独命中不足以入选**
# (实测: 3000 条已推里 912 条只有这类词, 「大众汽车召回部分进口汽车」就是靠「进口」进的)
CONTEXT = [
    "期货", "主力合约", "夜盘", "开盘", "收盘", "套保", "期现", "库存", "开工率",
    "检修", "减产", "停产", "复产", "限产", "产能", "招标", "收储", "抛储", "配额",
    "关税", "出口", "进口", "制裁", "罢工", "持仓", "CPI",
]
# 兼容旧接口/conf.keywords: 默认词表 = 所有层并集
DEFAULT_KEYWORDS = list(dict.fromkeys(STRONG + MACRO + ANCHOR + CONTEXT))
# 全大写缩写需要词边界, 否则 PE 命中 APPLE / PP 命中 APP;
# 覆盖所有词表 (含 NOISE), 大小写不敏感匹配 -> 源站写小写 lme/pta 也不漏
ACRONYMS = sorted({w.upper() for w in
                   set(STRONG + MACRO + ANCHOR + CONTEXT + NOISE_HARD + NOISE_SOFT)
                   if w.isascii() and w.isalpha() and w.isupper()} | {"BRENT"})

# ---------------- 英文词表 (Batch 6: 与中文五层一一对应的三层) ----------------
# EN_STRONG: 具体品种/合约/交易所 -> 命中即保留 (即使同时出现 EN_NOISE)
# EN_CONTEXT: 泛产业链/交易语境 -> **单独命中不足以入选**
# EN_NOISE: 股票/公司财报口径 -> **前置否决**
# 一律小写 + 词边界; 只用于「英文正文」分支, 不并入 DEFAULT_KEYWORDS (保证中文 A/B 不变)
EN_STRONG = [
    # 金属
    "copper", "aluminium", "aluminum", "zinc", "lead", "nickel", "tin", "gold",
    "silver", "platinum", "palladium", "lithium", "cobalt", "uranium",
    "molybdenum", "tungsten", "graphite", "potash", "rare earth", "rare earths",
    "antimony", "manganese", "silicon",
    # 黑色
    "iron ore", "steel", "rebar", "hot-rolled coil", "pig iron", "scrap steel",
    "coking coal", "thermal coal", "coal", "coke",
    # 能源
    "crude oil", "crude", "oil", "wti", "brent", "opec", "opec+", "natural gas",
    "lng", "liquefied natural gas", "gasoline", "diesel", "jet fuel",
    "heating oil", "fuel oil", "propane",
    # 农产品
    "corn", "wheat", "soybean", "soybeans", "soymeal", "soybean oil", "palm oil",
    "rapeseed", "sugar", "cotton", "coffee", "cocoa", "rice", "hog", "hogs",
    "cattle", "live cattle",
    # 化工/软商品
    "rubber", "pulp", "soda ash", "urea", "methanol", "pta", "pvc", "ethylene",
    "propylene",
    # 交易所/基准
    "lme", "comex", "nymex", "cbot", "shfe", "dce", "czce", "ine",
    "baltic dry index",
]
EN_CONTEXT = [
    "market", "markets", "price", "prices", "trading", "trader", "traders",
    "futures", "contract", "contracts", "demand", "supply", "output", "production",
    "inventory", "inventories", "export", "exports", "import", "imports",
    "tariff", "tariffs", "sanction", "sanctions", "strike", "quota", "stockpile",
    "stockpiles", "mine", "mines", "mining", "miner", "miners", "smelter",
    "smelters", "refinery", "refineries", "ore", "shipment", "shipments", "cargo",
    "freight", "shipping", "index", "data", "report", "forecast", "outlook", "gas",
]
EN_NOISE = [
    "stock", "stocks", "share", "shares", "shareholder", "shareholders",
    "earnings", "revenue", "revenues", "profit", "profits", "ipo", "dividend",
    "buyback", "merger", "acquisition", "takeover", "ceo", "cfo", "board",
    "nasdaq", "dow", "s&p 500", "wall street", "equity", "equities", "bond",
    "bonds", "treasury", "yields", "valuation", "investor", "investors",
    "hedge fund", "etf", "market cap",
]
ASCII_RATIO = 0.85                    # 英文正文判定: 非空白字符里 ASCII 占比下限
CJK_RE = re.compile(r"[\u4e00-\u9fff]")   # 只要含汉字就不走英文分支 (A/B 语料 4408 条实测 0 误入)


def _en_re(words: list[str]) -> re.Pattern:
    """词表 -> 词边界正则 (忽略大小写); 长词优先, 避免短词先吃掉长词.

    多词短语内部用 `[\\s\\-]+` 连接: 英语里 `iron-ore` / `Baltic-Dry-Index` 这类
    连字符写法与空格写法等价 (Batch 6 M3)。单词项不受影响。
    """
    alts = "|".join(r"[\s\-]+".join(re.escape(p) for p in re.split(r"[\s\-]+", w))
                    for w in sorted(words, key=len, reverse=True))
    return re.compile(rf"(?<![A-Za-z0-9])(?:{alts})(?![A-Za-z0-9])", re.I)


EN_STRONG_RE = _en_re(EN_STRONG)
EN_CONTEXT_RE = _en_re(EN_CONTEXT)
EN_NOISE_RE = _en_re(EN_NOISE)


def ascii_dominant(text: str) -> bool:
    """英文正文判定 (双闸门): 不含汉字 且 非空白字符里 ASCII 占比 >= ASCII_RATIO.

    实测 (2026-11-06): 3000 条已推语料与 1408 条旧丢弃池里,**含汉字者 4408/4408**,
    故「无汉字」一条即可保证英文分支在现有中文语料上一次都不会进入 —— 这是 A/B
    874/27 逐字不变的硬保证。ratio 单闸门**不够**: 语料里有 4 条纯拉丁垃圾串
    (如 `COMEX黄金库存统计brDOCTYPEhtml…`) ratio 达 0.90-0.92, 但它们含汉字。
    """
    t = str(text or "")
    if CJK_RE.search(t):
        return False
    s = "".join(t.split())
    if not s:
        return False
    return sum(1 for c in s if c.isascii()) / len(s) >= ASCII_RATIO


def en_reason(text: str) -> str | None:
    """英文分支: "" = 保留 / 非空 = 丢弃原因 / None = 不表态 (交回中文管线)."""
    t = str(text or "")
    if not ascii_dominant(t):
        return None
    m = EN_STRONG_RE.search(t)
    if m:
        return ""
    m = EN_NOISE_RE.search(t)
    if m:
        return f"英文噪声:{m.group(0).lower()}"
    m = EN_CONTEXT_RE.search(t)
    if m:
        return f"泛语境无品种:{m.group(0).lower()}"
    return None


# 时间不可解析计数 (每轮清零), 供 stats 输出
TS_BAD: dict[str, int] = {}

TS_MIN = 946684800                   # 2000-01-01, 早于此的时间串视为解析失败
TS_MAX_AHEAD = 366 * 86400           # 允许源站领先至多一年; 近期未来值由 FUTURE_TOL 丢弃并计数


# ------------------------------------------------------------------ utils
def log(msg: str) -> None:
    print(f"[{datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def err(msg: str) -> None:
    """对用户可见的失败: 同时进日志与 stderr (两边都带时间戳, 否则 err 文件无法定位)."""
    log(msg)
    print(f"[{datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}] {msg}",
          file=sys.stderr, flush=True)


def load_json(path: Path, default):
    """宽容读取 (仅用于 conf 之类无事务语义的文件); state 用 load_state."""
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def load_state(path: Path) -> tuple[dict, str]:
    """读 state, 返回 (数据, 错误描述).

    区分「文件不存在」(首轮, 返回空 dict) 与「存在但不可读」(返回错误, 调用方不得覆盖).
    """
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return {}, ""
    except OSError as e:
        return {}, f"{type(e).__name__}: {e}"
    if not raw.strip():
        return {}, "文件存在但为空 (可能掉电/截断)"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        return {}, f"JSON 解析失败: {e.msg} (line {e.lineno})"
    if isinstance(data, list):
        return {str(k): 0 for k in data}, ""
    if not isinstance(data, dict):
        return {}, f"顶层类型为 {type(data).__name__}, 期望 object"
    return data, ""


def save_json(path: Path, data, tries: int = 3) -> bool:
    """原子落盘 + fsync; 失败重试后返回 False (调用方决定退出码)."""
    payload = json.dumps(data, ensure_ascii=False)
    last = ""
    for i in range(tries):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            with open(tmp, "w") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            if path == CONF:                      # token 可能在这里, 权限收到 0600
                os.chmod(tmp, 0o600)
            tmp.replace(path)
            try:
                dfd = os.open(str(path.parent), os.O_RDONLY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass
            return True
        except OSError as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(0.2 * (i + 1))
    err(f"save_json 失败 {path.name}: {last}")
    return False


def prune(state: dict) -> dict:
    """按插入顺序截断 SEEN_MAX, 但保留 '_' 前缀的元数据键 (如 _alert)."""
    meta = {k: v for k, v in state.items() if k.startswith("_")}
    keys = [(k, v) for k, v in state.items() if not k.startswith("_")][-SEEN_MAX:]
    return {**dict(keys), **meta}


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def esc_url(u: str) -> str:
    """HTML 属性转义 (Batch 7): esc() 不转义引号, href 里一个 `"` 就会截断标签."""
    return esc(u).replace('"', "&quot;")


def esc_quote(s: str) -> str:
    """研报文本字段转义 (Batch 7 修复 C2): 在 esc() 之上再实体化双引号.

    标题/元信息/摘要都来自外部 PDF 与源站, 与 href 同等对待; 非研报路径仍用 esc(),
    逐字节行为不变 (由 B6/B7 的旧断言守住)。
    """
    return esc(s).replace('"', "&quot;")


def is_http_url(u) -> bool:
    """URL 协议白名单 (Batch 7 修复 C1): 只有 http(s) 允许落盘与渲染成链接.

    与 load_queue 原口径一致; 非字符串 (int/None/dict) 一律 False, 不再抛 AttributeError。
    """
    return isinstance(u, str) and u.startswith(("http://", "https://"))


# ------------------------------------------------------------------ queue (v5)
def load_queue() -> tuple[list[dict], str, int]:
    """读聚合队列; 返回 (条目, 问题描述). 文件不存在 = 空队列 (非错误)."""
    try:
        raw = QUEUE.read_text()
    except FileNotFoundError:
        return [], "", 0
    except OSError as e:
        return [], f"{type(e).__name__}: {e}", 0
    if not raw.strip():
        return [], "文件存在但为空", 0
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        return [], f"JSON 解析失败: {e.msg}", 0
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return [], "结构不符 (缺 items 列表)", 0
    out, bad = [], 0
    for it in items:
        if not isinstance(it, dict):
            bad += 1
            continue
        ts, text, key = it.get("ts"), it.get("text"), it.get("key")
        if not isinstance(ts, int) or not isinstance(text, str) or not text or not key:
            bad += 1
            continue
        ts = sane_ts(ts)                       # F-31: 跑飞时间戳会让 build_messages 抛 ValueError
        if not ts:
            bad += 1
            continue
        out.append({"src": str(it.get("src") or "?"), "ts": ts, "text": text, "key": str(key)})
        # Batch 7: 研报条目额外携带 url/snippet; 非研报条目仍只有上面 4 个字段 (JSON 逐字节不变)
        url = it.get("url")
        if is_http_url(url):
            out[-1]["url"] = url
            snip = it.get("snippet")
            if isinstance(snip, str):
                out[-1]["snippet"] = snip
    if bad:
        if out:
            log(f"队列有 {bad} 条结构/时间戳异常, 已跳过 (保留其余 {len(out)} 条)")
            return out, "", bad                 # V1-P2-2: 计数入 dirty, 坏条目要被写回覆盖
        return [], f"{bad} 条结构/时间戳异常, 无可用条目", bad
    return out, "", 0


def save_queue(items: list[dict]) -> bool:
    return save_json(QUEUE, {"items": items, "ts": int(time.time())})


def persist_state(seen: dict) -> bool:
    return save_json(STATE, prune(seen))


def queue_upsert(queue: list[dict], items: list[dict]) -> tuple[int, int]:
    """入队 (按 key 去重); 返回 (新增, 已在队列中)."""
    have = {q["key"] for q in queue}
    added = 0
    for it in items:
        if it["key"] in have:
            continue
        rec = {"src": it["src"], "ts": it["ts"], "text": it["text"], "key": it["key"]}
        # Batch 7: 研报条目附带 url/snippet (snippet=None 表示待解析, 不落盘 -> 下轮重试)
        url = it.get("url")
        if is_http_url(url):
            rec["url"] = url
            if it.get("snippet") is not None:
                rec["snippet"] = str(it["snippet"])
        queue.append(rec)
        have.add(it["key"])
        added += 1
    return added, len(items) - added


# ------------------------------------------------------- items 存档 (spec §3.11)
def atomic_write_text(path: Path, text: str) -> bool:
    """同目录 temp + replace 原子写文本 + fsync (存档轮转 / dump 导出共用)."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")   # pid 后缀: 无锁并发 dump/轮转同 PATH 不互撕
        with open(tmp, "w", encoding="utf-8", newline="") as fh:   # newline="" -> 恒 LF, 不做 CRLF 翻译
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(path)
        try:
            dfd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
        return True
    except OSError as e:
        err(f"atomic_write_text 失败 {path.name}: {type(e).__name__}: {e}")
        return False


def read_items_archive() -> list[dict]:
    """读 JSONL 存档 -> 条目列表; 文件不存在=空, 坏行跳过 (dump / 去重共用)."""
    out: list[dict] = []
    try:
        with open(ITEMS_ARCHIVE, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    out.append(rec)
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as e:              # ValueError 含 UnicodeDecodeError: 非法 UTF-8 存档不崩栈
        err(f"items 存档读取失败 {ITEMS_ARCHIVE.name}: {type(e).__name__}: {e}")
    return out


def items_archive_append(new_items: list[dict]) -> int:
    """items 存档真追加 + key 去重 + 行数轮转; 返回实际落档行数.

    契约 (spec §3.11 / T-31 / T-32 / R-9):
      - 行形 {"src","ts","text","key","url"?,"snippet"?,"schema":1}, JSONL 真追加 (非 JSON 数组)
      - 存档侧 key 集合去重: 已归档 key 跳过 (重复命中不重复落档); 调用方只传「本轮新进队列」的条目
      - 追加用 O_APPEND 单批 write + close 前 fsync; 追加后行数 > ITEMS_ARCHIVE_MAX 则原子重写保留最近 ITEMS_ARCHIVE_KEEP 行
      - 存档读不出 -> 去重无据, 宁可不追加也不重复落档; 写失败只记 err, 不影响主流程退码 (队列才是真源)
      - meta schema 闸自愈每次调用即执行 (读存档之前), 不被全 dup 轮/读失败闸住
    """
    if not new_items:
        return 0
    meta = load_json(ITEMS_META, None)                # §3.9 schema 闸: 缺失/版本不符时自愈重写
    if not (isinstance(meta, dict) and meta.get("schema") == ITEMS_SCHEMA):
        save_json(ITEMS_META, {"schema": ITEMS_SCHEMA})
    keys: set[str] = set()
    old_lines: list[str] = []
    try:
        with open(ITEMS_ARCHIVE, "r", encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                old_lines.append(s)
                try:
                    k = json.loads(s).get("key")
                except (json.JSONDecodeError, AttributeError):
                    continue                            # 坏行照原样保留, 仅不参与 key 集合
                if k:
                    keys.add(str(k))
    except FileNotFoundError:
        pass                                          # 首轮: 无存档 = 空 key 集合
    except (OSError, ValueError) as e:
        err(f"items 存档读取失败, 本轮跳过追加 {ITEMS_ARCHIVE.name}: {type(e).__name__}: {e}")
        return 0
    rows: list[dict] = []
    for it in new_items:
        if not isinstance(it, dict):
            continue
        key, ts, text = it.get("key"), it.get("ts"), it.get("text")
        if not key or not isinstance(ts, int) or isinstance(ts, bool) \
                or not isinstance(text, str) or not text:
            continue                                  # 形不合的条目不进档 (与 load_queue 同口径)
        key = str(key)
        if key in keys:
            continue                                  # T-31: 同 key 重复命中只落一行
        keys.add(key)
        rec = {"src": str(it.get("src") or "?"), "ts": ts, "text": text, "key": key}
        url = it.get("url")
        if is_http_url(url):                          # 与 queue_upsert 同口径: 仅 http(s) url 落档
            rec["url"] = url
            snip = it.get("snippet")
            if isinstance(snip, str):
                rec["snippet"] = snip
        rec["schema"] = ITEMS_SCHEMA
        rows.append(rec)
    if not rows:
        return 0
    try:
        ITEMS_ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        err(f"items 存档目录不可建 {ITEMS_ARCHIVE.parent}: {type(e).__name__}: {e}")
        return 0
    payload = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")
    view = memoryview(payload)                        # 重试只续写剩余 view: 部分写后整批重写会重复落档
    last = ""
    for i in range(3):                                # 与 save_json 同口径的轻量重试
        try:
            fd = os.open(ITEMS_ARCHIVE, os.O_WRONLY | os.O_CREAT | os.O_APPEND
                         | getattr(os, "O_BINARY", 0))   # O_APPEND 单批 write; Windows 需 O_BINARY 保 LF
            try:
                while view:
                    n = os.write(fd, view)
                    if n <= 0:
                        raise OSError("os.write 返回 0, 写不进")   # 防 0 写字节死循环
                    view = view[n:]
                os.fsync(fd)                          # close 前 fsync: 与 save_json/atomic_write_text 耐久口径一致
            finally:
                os.close(fd)
            # P3-7: O_CREAT 新档的目录项不随文件 fsync 落盘——崩溃后新档
            # 目录项可丢; 补父目录 fsync (save_json:513-520 /
            # atomic_write_text:648-655 同款写法; 既有档追加也做,
            # 代价微小且免 exists() TOCTOU 分辨)
            try:
                dfd = os.open(str(ITEMS_ARCHIVE.parent), os.O_RDONLY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass
            break
        except OSError as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(0.2 * (i + 1))
    else:
        err(f"items 存档追加失败 {ITEMS_ARCHIVE.name}: {last}")
        return 0
    total = len(old_lines) + len(rows)
    if total > ITEMS_ARCHIVE_MAX:                     # R-9 硬约束: 原子重写保留最近 6000 行
        keep = (old_lines + [json.dumps(r, ensure_ascii=False) for r in rows])[-ITEMS_ARCHIVE_KEEP:]
        if atomic_write_text(ITEMS_ARCHIVE, "\n".join(keep) + "\n"):
            log(f"items 存档轮转: {total} -> {len(keep)} 行 (上限 {ITEMS_ARCHIVE_MAX})")
    return len(rows)


# --dump-items 目标守卫 (P3-8): resolve 归一后命中任一 state/conf 件即拒——
# JSON 快照覆盖 JSONL 存档/seen/queue/pending/meta/conf 会毁形真源;
# 覆盖 LOCK 会把锁文件换成新 inode, 持旧 inode 锁者与后继 flock 者可并发进入。
# 清单在函数体内逐次取模块全局 (非 import 期绑死)——测试经 setattr
# 重定向路径后守卫仍生效, 与旧 ITEMS_ARCHIVE 单件守卫同口径。
def dump_items(path: Path) -> int:
    """--dump-items PATH: 存档原子导出 {"schema":1,"ts":<now>,"items":[...]} (spec §3.11).

    独立子命令语义: 不进抓取/锁/发送主路径; 返回 0 成功 / 1 落盘失败。
    """
    out = Path(path)
    resolved = out.resolve()
    for guarded in (ITEMS_ARCHIVE, ITEMS_META, STATE, QUEUE, PENDING,
                    LOCK, CONF):                      # 任一 state/conf 件命中即拒
        if resolved == guarded.resolve():
            err(f"--dump-items 目标不能覆盖 state/conf 件 {guarded}")
            return 1
    items = read_items_archive()
    payload = {"schema": ITEMS_SCHEMA, "ts": int(time.time()), "items": items}
    if not atomic_write_text(out, json.dumps(payload, ensure_ascii=False) + "\n"):
        return 1
    log(f"items 存档导出 {len(items)} 条 -> {out}")
    return 0


def to_int(v, default: int = 0) -> int:
    """state 里的计数被写坏时不抛异常 (V1-P2-6 / V3-P2: 含 OverflowError)."""
    if isinstance(v, bool):
        return int(v)
    try:
        return int(v)
    except (TypeError, ValueError, OverflowError):
        return default


def to_float(v, default: float = 0.0) -> float:
    """同上; NaN / ±Inf 一律回默认值, 否则 fromtimestamp/比较会抛 (V3-P2)."""
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


def cap_notified_already(day: dict) -> bool:
    """cap_notified 被写成字符串 "false" 时不能当真 (V3-P2)."""
    v = day.get("cap_notified")
    if isinstance(v, str):
        return v.strip().lower() not in ("", "0", "false", "no")
    return bool(v)


def digest_meta(seen: dict) -> dict:
    """取/修正 state 里的聚合元数据块 (_digest: last_flush)."""
    meta = seen.get("_digest")
    if not isinstance(meta, dict):
        meta = {}
        seen["_digest"] = meta
    return meta


def day_counter(seen: dict, today: str) -> dict:
    """取/重置当日通知计数 (_day: {date, msgs})."""
    day = seen.get("_day")
    if not isinstance(day, dict) or day.get("date") != today:
        day = {"date": today, "msgs": 0}
        seen["_day"] = day
    elif not isinstance(day.get("msgs"), int):
        day["msgs"] = to_int(day.get("msgs"))
    return day


def u16(s: str) -> int:
    """Telegram 的 4096 上限按 UTF-16 码元计; 非 BMP 字符占 2 单元 (F-09)."""
    return len(s.encode("utf-16-le")) // 2


def rotate_log() -> None:
    try:
        if LOG.exists() and LOG.stat().st_size > LOG_MAX:
            tail = LOG.read_bytes()[-LOG_MAX // 2:]
            LOG.write_bytes(tail)
    except Exception:
        pass


def rotate_file(fd: int, path: Path, max_bytes: int | None = None) -> bool:
    """原地轮转 fd 背后那个普通文件: 先截断再写回尾部一半 (与 rotate_log 同阈值同保留策略).

    launchd 用 StandardErrorPath 打开的是 O_APPEND 的 fd; rename/重建文件只会让 launchd
    继续写旧 inode, 所以必须 os.ftruncate(fd) + os.write(fd) 作用在同一个 inode 上,
    O_APPEND 保证写回落在文件末尾. 非普通文件 / 身份对不上 / 任何异常都静默跳过.
    前提: 无并发写者 (main 单线程); 「读-截断-写回」窗口不是原子的, 期间的并发写入会丢失/错序.
    """
    max_bytes = LOG_MAX if max_bytes is None else max_bytes
    try:
        st = os.fstat(fd)
        if st.st_size <= max_bytes:            # 文件不存在 / 0 字节 / 恰好等于阈值: 不动
            return False
        if not os.path.isfile(path):           # tty / 管道 / /dev/null: 静默跳过
            return False
        cur = os.stat(path)
        if (cur.st_dev, cur.st_ino) != (st.st_dev, st.st_ino):
            return False                       # path 已不是 fd 指向的 inode (被删/rename/重建)
        keep = max_bytes // 2
        with open(path, "rb") as fh:
            fh.seek(max(0, st.st_size - keep))
            tail = fh.read(keep)               # 先读尾部再截断, 最近内容不丢
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)           # 非 O_APPEND fd 也从头写回, 避免稀疏增长 (U3-F-02)
        while tail:                            # O_APPEND: 写回落在末尾
            n = os.write(fd, tail)
            if n <= 0:
                break
            tail = tail[n:]
        return True
    except Exception:
        return False                           # 轮转失败不得抛异常, 也不得阻断主流程


def rotate_stderr() -> None:
    """轮转 launchd 的 StandardErrorPath (/tmp/futures-news.err), 阈值/保留与 .out 一致."""
    rotate_file(2, ERRLOG)


# 源站残缺 HTML 的两种残留 (U4): 缺 '>' 的标签片段 与 裸露属性.
# 只吃 ASCII 形态的标签名/属性值, 所以「涨幅>5%」「5%<10%」「a<b」和中文开头的 '<' 都不受影响
TAG_RESIDUE = re.compile(
    r"<\s*(?:/\s*[A-Za-z][A-Za-z0-9:_-]*"                        # 闭合残片: </span
    r"|[A-Za-z][A-Za-z0-9:_-]*(?:\s+[A-Za-z_:][A-Za-z0-9_.:-]*"  # 开标签残片: 至少要一个属性
    r"(?:\s*=\s*(?:\"[^\"]*\"|'[^']*'|[A-Za-z0-9#%.,:;/()+*?=&~_@-]*))?)+)"
    r"\s*/?")
ATTR_RESIDUE = re.compile(
    r"(?<![A-Za-z])(?:(?:span|div|font|p|b|i|u|em|strong|td|tr|th|li|ul|ol|h[1-6])?"
    r"(?:style|class|id|align|width|height|color|bgcolor|colspan|rowspan|"
    r"href|src|target|rel|title|font-size|font-family))"
    r"\s*=\s*(?:\"[^\"]*\"|'[^']*'|[A-Za-z0-9#%.,:;/()+*?=&~_@-]*)", re.I)


def clean_text(raw) -> str:
    """去标签 + 反转义 HTML 实体 + 清理残缺标签/裸露属性残留 (U4); 非字符串一律 str() 兜底."""
    t = re.sub(r"<[^>]+>", "", str(raw or ""))
    t = html.unescape(t).replace("\xa0", " ")
    t = re.sub(r"<[^>]+>", "", t)           # 实体编码的完整标签反转义后才出现, 再剔一遍
    t = TAG_RESIDUE.sub("", t)              # 缺 '>' 的残缺标签 (如 <span style=color:#ff0000)
    t = ATTR_RESIDUE.sub("", t)             # 裸露属性 (如 spanstyle=color:rgb(255,0,0))
    t = re.sub(r"[ \t\u3000]{2,}", " ", t)  # 收拾残留清掉后留下的多余空白/标点
    t = re.sub(r"\s+([，。、；：！？,.;:!?])", r"\1", t)
    return t.strip()


def http(url: str, referer: str | None = None, tries: int = 3,
         deadline: float | None = None, cap: int = 12,
         proxy: str | None = None, ua: str | None = None,
         max_bytes: int = 0) -> bytes:
    """curl 取数据; 非 2xx 视作失败并重试; deadline 为本次调用的预算截止 (time.monotonic).

    Batch 6 新增三个**默认等于旧行为**的关键字参数:
      cap   单次 curl 最长秒数 (中文源仍 12s; 英文 RSS 用 FEED_HTTP["cap"] = 8)
      proxy 非空时走 `--proxy <url>`, 且**不再**加 `--noproxy *` (curl 中二者互斥,
            同给会让 --noproxy 生效、代理被静默忽略)
      ua    非空时替换 UA (仅 MiningTech 需要 Feed 阅读器 UA)
    proxy 为空时 argv 与 Batch 6 之前逐元素相同 —— 这是「不得改变现有中文源抓取行为」
    的落点, 由 selftest「无代理时 curl 命令行与旧版逐元素一致」守住。

    Batch 7 新增 max_bytes (默认 0 = 不追加任何参数, argv 与旧版逐元素一致):
    仅 PDF 下载使用, 交给 curl --max-filesize 在超过上限时中止 (exit 63), 避免
    2GB 级错误文件把内存吃光。

    Batch 7 修复 A2: 一律检查 curl 退出码 (超时 28 / 超限 63 / chunked 残体 18 都非 0),
    非 0 即视为失败, 不再把 `-w` 打印的 200 当成成功。对 max_bytes 路径另做两道严格校验:
    ① `--max-filesize` 由 curl 侧在超过声明体积时中止; ② 返回体长度必须 <= max_bytes。
    Batch 7 修复 A5: max_bytes>0 (仅 PDF) 时不再请求 --compressed —— gzip 64MB 正文
    压缩后仅 ~64KB, curl 的 --max-filesize 只认 Content-Length(压缩后), 解压峰值 128MB;
    不发 Accept-Encoding 即从源头消掉解压放大。非研报路径的 argv 保持逐元素不变。
    """
    last = ""
    strict = max_bytes > 0
    for i in range(tries):
        left = (deadline - time.monotonic()) if deadline else 30.0
        if left < 2:
            raise RuntimeError(f"http 预算耗尽 ({last or 'no-attempt'})")
        max_time = max(2, min(cap, int(left)))
        cmd = ["curl", "-sS"]
        cmd += ["--proxy", proxy] if proxy else ["--noproxy", "*"]
        if not strict:                       # PDF 路径不请求压缩, 见 docstring A5
            cmd += ["--compressed"]
        if strict:
            cmd += ["--max-filesize", str(int(max_bytes))]
        cmd += ["--max-time", str(max_time), "-A", ua or UA,
                "-w", "\n%{http_code}", url]
        if referer:
            cmd += ["-H", f"Referer: {referer}"]
        try:
            kw: dict = {"capture_output": True, "timeout": max_time + 4}
            if proxy:
                # curl 的 no_proxy/NO_PROXY 优先级高于显式 --proxy: 命中会静默直连,
                # 击穿「代理失败不得回退直连」语义 (Batch 6 M4)。子进程环境里显式清空,
                # argv 不变以免影响既有命令行断言。
                kw["env"] = {**os.environ, "no_proxy": "", "NO_PROXY": ""}
            r = subprocess.run(cmd, **kw)
            rc = getattr(r, "returncode", 0)          # 旧测试桩无该属性时按成功处理
            if rc != 0:                               # A2: curl 中止/超时/超限一律失败
                last = f"curl rc={rc}"
                raw = b""
            else:
                raw = r.stdout
            body, _, code = raw.rpartition(b"\n")
            last = code.decode(errors="ignore").strip() if rc == 0 else last
            if body and last.startswith("2"):
                if strict and len(body) > max_bytes:  # A2/A4: 体积兜底 (chunked 无长度时)
                    raise RuntimeError(f"body 超上限 {len(body)}>{max_bytes}")
                return body
        except Exception as e:
            last = f"{type(e).__name__}"
        nap = float(1 + i)
        if deadline:
            nap = min(nap, max(0.0, deadline - time.monotonic() - 1.0))
        if nap > 0:
            time.sleep(nap)
    raise RuntimeError(f"http failed ({last or 'empty'})")


def http_ok(url: str, deadline: float | None = None, timeout: int = 6) -> bool:
    """HEAD 探测资源是否存在 (2xx): 只取状态码, 不下载 body (Batch 7).

    中金所的日/周报是 0.5MB 级 XML、非交易日/未发布时会 302 到错误页, 用 GET 探测
    等于每轮白拉半兆; 故只发 `curl -I`。网络失败/超时/参数非法一律返回 False,
    由调用方按「本期未发布」处理 (不抛错、不计入源失败)。
    """
    left = (deadline - time.monotonic()) if deadline else 30.0
    if left < 1.5:
        return False
    t = max(1, min(timeout, int(left)))
    cmd = ["curl", "-sS", "--noproxy", "*", "-I", "--max-time", str(t),
           "-A", UA, "-o", "/dev/null", "-w", "%{http_code}", url]
    try:
        raw = subprocess.run(cmd, capture_output=True, timeout=t + 3).stdout
    except Exception:
        return False
    return raw.decode(errors="ignore").strip().startswith("2")


# ---- Batch 7 PDF 正文提取 (只跑 poppler CLI; 任一失败都降级为「无摘要」)
PDF_TABLE = re.compile(r"^\[?[Tt]able[_\-\w]*\]?$|^[图表]\s*\d")     # 版式标记/图表编号行
PDF_NUM = re.compile(r"^[\s\d.,%+\-—–/()（）亿元万股手吨桶美元]+$")   # 纯数字/单位行 (表格碎片)
PDF_CJK = re.compile(r"[\u4e00-\u9fff]")


def pdf_clean(text: str, limit: int = SNIPPET_MAX) -> str:
    """把 pdftotext 输出收拾成可推的一段摘要 (Batch 7).

    版式噪音三类: ① 页码/页眉页脚 (纯数字、`- 3 -`); ② 表格碎片 (价格/占比/产品代码行);
    ③ 版式标记 (`[Table_StockNameRptType]`、`图 2`). 只保留「含 ≥6 个汉字且 ≥10 字」的
    成句行; 成句行合计不足 limit 时退回全部非空行 —— 中金所月报几乎全是表格, 只有
    小节标题是成句行, 退回策略保证有文本层的 PDF 不被误判成「无文本层」。
    """
    lines: list[str] = []
    for raw in (text or "").splitlines():
        s = raw.strip()
        if not s or PDF_TABLE.match(s) or PDF_NUM.match(s):
            continue
        lines.append(s)
    good = [s for s in lines if len(s) >= 10 and len(PDF_CJK.findall(s)) >= 6]
    body = re.sub(r"\s+", " ", " ".join(good or lines)).strip()
    body = re.sub(r"[\ue000-\uf8ff\ufffd\x0c]", "", body)   # Wingdings 项目符号/换页符
    return body[:limit].strip()


def _pdf_child_limits() -> None:
    """pdftotext 子进程的地址空间硬上限 (Batch 7 修复 A5).

    PDF 内部 Flate 流可以 391KB -> 926MB RSS; `PDF_MAX_BYTES` 只限传输字节,
    `PDF_TIMEOUT` 只限时间, 二者都不是内存上限。RLIMIT_AS 让 poppler 在超限时
    分配失败并退出 (rc != 0), 由 pdf_text 按「确定性无文本」降级。
    """
    if resource is None:                     # Windows: preexec_fn 不可用, 由调用方传 None
        return
    try:
        resource.setrlimit(resource.RLIMIT_AS, (PDF_RLIMIT_AS, PDF_RLIMIT_AS))
    except (ValueError, OSError):
        pass                                 # 平台不支持时退回「仅靠超时」


def _pdftotext_children() -> list[tuple[int, int]]:
    """返回 (pid, rss_kb) 形式的 pdftotext 直接子进程 (Batch 7 修复 A5 内存看门狗).

    macOS 的 RLIMIT_AS 无法设到进程当前 VSZ 以下 (setrlimit 返回 EINVAL), 因此
    preexec_fn 里的上限在 macOS 上不生效; 这里用 ps 轮询子进程 RSS 并在超限时 kill,
    让 391KB -> 926MB 的 PDF 炸弹在 ~256MB 处终止 (Linux 上则由 RLIMIT_AS 直接生效)。
    """
    try:
        raw = subprocess.run(["ps", "-axo", "pid=,ppid=,rss=,comm="],
                             capture_output=True, timeout=2).stdout
    except Exception:
        return []
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    me, out = os.getpid(), []
    for line in str(raw).splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid, ppid, rss = int(parts[0]), int(parts[1]), int(parts[2])
        except ValueError:
            continue
        if ppid == me and "pdftotext" in parts[3]:
            out.append((pid, rss))
    return out


def pdf_text(url: str, deadline: float | None = None,
             referer: str | None = None) -> str | None:
    """下载 PDF 并提取正文摘要; 绝不抛错 (Batch 7).

    返回语义 (修复 A1):
      None  瞬时失败 —— 下载失败/超时/预算耗尽, **下轮重试** (extract_research 保持延迟入队)
      ""    确定性无文本 —— 非 PDF 响应、pdftotext 缺失、子进程非 0 退出、清洗后过短,
            **视为已判定, 不再重试** (避免一次抖动把该篇摘要永久写死成 "")

    安全上限 (修复 A5): 下载 8MB + pdftotext 前 8 页 + 子进程内存上限 256MB
    (preexec_fn 设 RLIMIT_AS, Linux 直接生效; macOS 上由 ps RSS 看门狗 kill)。
    """
    if not url:
        return ""
    dl = time.monotonic() + PDF_DL_BUDGET_S
    if deadline:                              # 总预算里给解析预留 PDF_TIMEOUT (修复 A3)
        dl = min(dl, deadline - PDF_TIMEOUT)
    try:
        raw = http(url, referer=referer, tries=1, deadline=dl,
                   cap=PDF_CAP, max_bytes=PDF_MAX_BYTES)
    except Exception as e:
        log(f"pdf 下载失败, 下轮重试: {type(e).__name__}: {str(e)[:60]}")
        return None
    if not raw.lstrip()[:5].startswith(b"%PDF"):
        log("pdf 响应不是 PDF (疑似 302 错误页), 无摘要")
        return ""
    if not PDFTOTEXT or not os.path.exists(PDFTOTEXT):
        log("pdftotext 不存在, 无摘要 (降级)")
        return ""
    _res: dict = {}

    def _run_pdf() -> None:
        try:
            _res["r"] = subprocess.run(
                [PDFTOTEXT, "-f", "1", "-l", str(PDF_MAX_PAGES), "-", "-"],
                input=raw, capture_output=True, timeout=PDF_TIMEOUT,
                preexec_fn=_pdf_child_limits if os.name == "posix" else None)
        except BaseException as e:            # 线程内异常必须显式传递, 否则会静默丢失
            _res["e"] = e

    _t = threading.Thread(target=_run_pdf, daemon=True)
    _t.start()
    _killed = False
    while _t.is_alive():                      # macOS 上 RLIMIT_AS 无法降到进程当前 VSZ 以下
        for _pid, _rss in _pdftotext_children():
            if _rss > PDF_RLIMIT_AS // 1024:  # ps 的 rss 单位是 KB
                try:
                    os.kill(_pid, signal.SIGKILL)
                    _killed = True
                except OSError:
                    pass
        _t.join(0.1)
    _exc = _res.get("e")
    if isinstance(_exc, subprocess.TimeoutExpired):
        log("pdftotext 超时, 下轮重试")
        return None
    if _exc is not None:
        log(f"pdftotext 不可用({type(_exc).__name__}), 无摘要")
        return ""
    r = _res.get("r")
    if _killed:
        log(f"pdftotext 内存超限 (>{PDF_RLIMIT_AS // 1048576}MB), 已终止, 无摘要")
        return ""
    if r is None:
        return ""
    if r.returncode != 0:
        log(f"pdftotext 退出码 {r.returncode}, 无摘要")
        return ""
    body = pdf_clean(r.stdout.decode("utf-8", "ignore"))
    if len(body) < PDF_MIN_CHARS:
        log(f"pdf 无文本层 (清洗后仅 {len(body)} 字), 无摘要")
        return ""
    return body


def research_key(src: str, ident: str) -> str:
    """研报稳定去重键 (Batch 7): 同一篇研报每轮拉回都是同一个 key.

    ident 取源站唯一 ID (东财 infoCode / 中金所 YYYYMM(DD) / 中期协 docId), 无 ID 的源
    退化为「标题+日期」。前缀带 ':' —— norm_text() 会删除标点, 故 dedupe_key() 的输出
    永远不含 ':', 两类 key 命名空间天然不相交。
    """
    return f"{RESEARCH_TAG}:{src}:{ident}"[:120]


def research_item(src: str, ident: str, title: str, url: str, ts: int,
                  meta: str = "", snippet: str | None = None) -> dict:
    """构造研报条目 (Batch 7): 新增 url/snippet 两个持久化字段, 其余沿用既有 item 结构.

    text 保持纯文本「标题｜机构 · 发布日期」(全角竖线分隔, 不新增第三个持久化字段):
    过滤、日志、--queue/--dry-run 打印全部沿用既有逻辑, 只有 build_messages 见到 url
    才走 HTML 链接样式。snippet=None = 待提取, "" = 已判定无文本层 (不重试)。

    ts 一律取**首次发现时刻**(int(time.time())): 研报源只给日期级发布时间(00:00), 直接
    用会把「当天上午发布的研报」判成 12 小时前, 触发 max_age 老化与 max_hold 立即冲刷;
    真实发布日期写在 text 的元信息里, 人眼可读, 机器不拿它做时间闸门。
    """
    text = f"{title}｜{meta}" if meta else title
    return {"src": src, "ts": ts, "text": text, "url": url,
            "snippet": snippet, "key": research_key(src, ident)}


def research_reason(text: str) -> str:
    """研报源的过滤口径 (Batch 7): 豁免「品种锚点」硬门槛, 保留栏目/噪声否决.

    中金所/中期协的标题是官方统计口径 (「中金所月度数据统计」「全国期货市场交易情况
    简报」), 走 filter_reason 会被判「泛语境无品种」; 但栏目/日历、超短内容、股票
    口径噪声 (业绩/个股/沪深指数…) 仍然要丢。
    顺序与 filter_reason 一致: 具体品种/产业锚点优先于噪声词 —— 否则「夯实股东回报」
    里的「股东」会把「电解铝量价齐升」这种正牌商品研报误杀。
    """
    t = (text or "").strip()
    if SKIP_RE.search(strip_decor(t)):
        return "栏目/日历"
    if len(norm_text(t)) < 4:
        return "内容过短"
    if hit(t, STRONG) or hit(t, ANCHOR):
        return ""
    hard = first_hit(t, NOISE_HARD)
    if hard:
        return f"噪声:{hard}"
    return ""


def extract_research(queue: list[dict], fresh: list[dict]) -> tuple[int, int]:
    """只给「研报类 + 队列里还没有摘要」的条目提取 PDF 正文 (Batch 7).

    返回 (实际解析篇数, 被补上摘要的队列条目数)。串行执行: pdftotext 是 CPU 密集
    子进程, 并发只会抢同一份 PDF 预算。每轮最多 RESEARCH_MAX_PDF 篇、总预算
    PDF_BUDGET_S(下载 8s + 解析 6s) —— 研报条目的正文晚一轮补上也比拖垮 45s 抓取预算好。
    """
    known = {q["key"]: q.get("snippet", "") for q in queue
             if q.get("url") and "snippet" in q}
    qids = {id(q) for q in queue}
    todo: list[dict] = []                       # 待解析条目 (队列内的先于 fresh)
    seen_keys: set[str] = set()
    for it in list(queue) + list(fresh):
        if not it.get("url"):
            continue
        if it["key"] in known:                  # 同篇已解析过 (空串 = 无文本层) -> 直接复用
            it["snippet"] = known[it["key"]]
            continue
        if it.get("snippet") is not None:       # 源已给出结论 (如中金所日/周报无正文)
            continue
        if it["key"] in seen_keys:              # 队列与 fresh 同时出现同一篇 -> 只解析一次
            continue
        seen_keys.add(it["key"])
        todo.append(it)
    dl = time.monotonic() + PDF_BUDGET_S
    calls = q_changed = 0
    for it in todo:
        if (calls >= RESEARCH_MAX_PDF or time.monotonic() >= dl
                or time.monotonic() + PDF_TIMEOUT > dl):   # 给解析留时间 -> 14s 是硬上界 (A3)
            break
        got = pdf_text(it["url"], deadline=dl)
        calls += 1
        if got is None:                      # A1: 瞬时失败 -> snippet 保持 None, 下轮重试
            log(f"研报正文提取瞬时失败, 下轮重试: {it['key']}")
            continue
        it["snippet"] = got                  # "" = 已判定无文本层, 不再重试
        q_changed += int(id(it) in qids)
    return calls, q_changed


def sane_ts(v: int) -> int:
    """时间戳合理性闸门: 缺年份格式 (如 `10/14 09:00` -> 1900) 与跑飞值都判失败 (F-02)."""
    return v if TS_MIN <= v <= int(time.time()) + TS_MAX_AHEAD else 0


def parse_ts(raw) -> int:
    """兼容多种时间串; 无时区串按 Asia/Shanghai 解释; 解析失败返回 0 (由调用方丢弃)."""
    s = str(raw or "").strip()
    if not s:
        return 0
    if s.isdigit():                                  # 秒/毫秒时间戳
        v = int(s)
        if v > 10 ** 12:
            v //= 1000
        return sane_ts(v)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%a %b %d %H:%M:%S %z %Y"):
        try:
            dt = datetime.strptime(s, fmt)
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ_CN)             # 源站时间均按北京时间解释
        return sane_ts(int(dt.timestamp()))
    try:
        dt = datetime.fromisoformat(s.replace("+08:00", "+0800"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ_CN)
        return sane_ts(int(dt.timestamp()))
    except Exception:
        return 0


def bad_ts(src: str) -> None:
    """时间不可解析: 计数后丢弃该条 (F-02, 与金属网口径一致)."""
    TS_BAD[src] = TS_BAD.get(src, 0) + 1
    TS_BAD["total"] = TS_BAD.get("total", 0) + 1


# ------------------------------------------------------------------ filter
def norm_text(s: str) -> str:
    return PUNCT.sub("", s or "")


def strip_decor(text: str) -> str:
    """剥离日期/时刻前缀与短括号前缀, 用于栏目判定 (F-11)."""
    s = (text or "").strip()
    for _ in range(3):
        old = s
        s = re.sub(r"^【[^】]{0,24}】\s*", "", s).strip()
        s = DATE_PREFIX.sub("", s, count=1).strip()
        s = TIME_PREFIX.sub("", s, count=1).strip()
        if s == old:
            break
    return s


def split_title(text: str) -> tuple[str, str]:
    """拆出 【标题】 与其余正文."""
    t = (text or "").strip()
    m = re.match(r"^【([^】]+)】", t)
    if not m:
        return "", t
    return m.group(1), t[m.end():]


def dedupe_key(text: str) -> str:
    """跨源去重签名 (F-01): 同时保留实义标题与正文, 通用标题回退正文.

    - 实义标题 + 归一化正文 -> 签名, 避免「【栏目前缀】不同正文」被坍缩成同一签名
    - 通用标题 (快讯/要闻/…)、正文为空或已被标题覆盖 -> 只用标题, 保证
      「【快讯】X」与裸文本「X」签名相同
    """
    t = (text or "").strip()
    title_raw, rest = split_title(t)
    title = norm_text(title_raw)
    body = norm_text(BOILER.sub("", rest))
    if not title:
        base = body or norm_text(t)
    elif title in GENERIC_TITLES or len(title) < 2:
        base = body or title
    elif len(body) < 4 or body in title:
        base = title
    else:
        base = title + body
    return (base[:60] or norm_text(t)[:60]).lstrip("_")


def first_hit(text: str, words: list[str]) -> str:
    """返回第一个命中词; 全大写缩写加词边界并忽略大小写 (源站写 lme/pta 也算)."""
    up = None
    for w in words:
        if w.upper() in ACRONYMS:
            if up is None:
                up = text.upper()
            if re.search(rf"(?<![A-Za-z]){w.upper()}(?![A-Za-z])", up):
                return w
        elif w in text:
            return w
    return ""


def hit(text: str, words: list[str]) -> bool:
    return bool(first_hit(text, words))


NON_COMMODITY = {"国债期货", "利率期货", "股指期货", "恒指期货", "股票期货", "债券期货"}


def filter_reason(text: str, keywords: list[str], use_filter: bool) -> str:
    """返回丢弃原因 (空串=保留); 供 is_relevant 与丢弃样本日志共用."""
    t = (text or "").strip()
    if SKIP_RE.search(strip_decor(t)):
        return "栏目/日历"
    if len(norm_text(t)) < 4:
        return "内容过短"
    if not use_filter:
        return ""
    en = en_reason(t)                         # 英文正文先过英文三层 (含汉字/中文占比高则不表态)
    if en is not None:
        return en
    if hit(t, STRONG):                        # 1 具体品种/合约/交易所 -> 直接保留
        return ""
    price_ctx = any(w in t for w in ("收购价", "采购价", "报价", "出厂价", "现货价"))
    equity_only = {"股权", "收购", "并购", "增持", "回购", "股东"}
    # 2 股票/境外股指/非商品口径 -> 先否决; 价格语境的「收购价」不算股权事件
    hard = first_hit(t, [w for w in NOISE_HARD
                         if not (price_ctx and w in equity_only)])
    if hard:
        tag = "非商品口径" if hard in NON_COMMODITY else "噪声"
        return f"{tag}:{hard}"
    if "目标价" in t:                          # 品种目标价是有效信息, 股票目标价才是噪声
        if any(c in t for c in ("港元", "A股", "港股", "股价", "市值", "董事")):
            return "噪声:目标价"
        if not hit(t, ANCHOR):
            return "噪声:目标价"
        return ""

    macro = first_hit(t, MACRO)               # 3 宏观事件: 还需品种锚点或事件词
    if macro:
        if hit(t, ANCHOR) or hit(t, MACRO_EVENT):
            return ""
        return f"宏观无品种:{macro}"
    if hit(t, ANCHOR):                        # 4 商品/产业锚点 -> 保留
        return ""
    soft = first_hit(t, NOISE_SOFT)           # 5 研报/机构口径 -> 无锚点才否决
    if soft:
        return f"噪声:{soft}"
    # 6 只剩泛语境 -> 丢; 任一非泛语境命中即保留 (R3: 原来只看 first_hit, 顺序决定结论)
    strong_kw = first_hit(t, [w for w in keywords if w not in CONTEXT])
    if strong_kw:
        return ""
    weak = first_hit(t, keywords)
    if weak:
        return f"泛语境无品种:{weak}"
    return "无期货相关词"


def is_relevant(text: str, keywords: list[str] | None = None,
                use_filter: bool = True) -> bool:
    """模块级过滤判定 (F-21): 便于 --selftest 覆盖 F-10/F-11/F-16."""
    return not filter_reason(text, keywords or DEFAULT_KEYWORDS, use_filter)


def merge_near(items: list[dict]) -> tuple[list[dict], list[tuple]]:
    """时间窗内高相似度条目合并 (同一事件不同措辞, F-13); 保留最早一条作为主条目."""
    kept: list[dict] = []
    pairs: list[tuple] = []
    for it in items:
        dup = None
        for prev in reversed(kept):
            if it["ts"] - prev["ts"] > NEAR_WINDOW:
                break
            if it.get("url") or prev.get("url"):
                continue                      # Batch 7: 研报按稳定 key 精确去重, 不做近似合并
            a, b = prev.get("key", ""), it.get("key", "")
            if len(a) < NEAR_MIN_LEN or len(b) < NEAR_MIN_LEN:
                continue
            if NUM_RE.findall(a) != NUM_RE.findall(b):
                continue                      # 数字不一致 -> 不同事件 (指标型快讯最常见)
            if WEEKDAY.findall(a) != WEEKDAY.findall(b):
                continue                      # 周一/周三等不一致 -> 不同日的日历条目
            ratio = difflib.SequenceMatcher(None, a, b).ratio()
            if ratio >= NEAR_RATIO:
                dup = (prev, ratio)
                break
        if dup:
            pairs.append((dup[0], it, dup[1]))
        else:
            kept.append(it)
    return kept, pairs


# ------------------------------------------------------------------ sources
def src_cls(deadline: float | None = None) -> list[dict]:
    p = {"app": "CailianpressWeb", "category": "", "lastTime": "", "last_time": "",
         "os": "web", "refresh_type": "1", "rn": "40", "sv": "8.4.6"}
    q = urllib.parse.urlencode(sorted(p.items()))
    sign = hashlib.md5(hashlib.sha1(q.encode()).hexdigest().encode()).hexdigest()
    d = json.loads(http(f"https://www.cls.cn/v1/roll/get_roll_list?{q}&sign={sign}",
                        "https://www.cls.cn/telegraph", deadline=deadline))
    out = []
    for it in d.get("data", {}).get("roll_data", []):
        text = (it.get("title") or it.get("brief") or "").strip()
        if not text:
            continue
        ts = parse_ts(it.get("ctime"))
        if not ts:                       # 时间不可解析 -> 丢弃并计数, 不再伪造 now()
            bad_ts("财联社")
            continue
        out.append({"src": "财联社", "ts": ts, "text": text})
    return out


def src_em(deadline: float | None = None) -> list[dict]:
    u = ("https://np-listapi.eastmoney.com/comm/web/getFastNewsList?client=web&biz=web_724"
         f"&fastColumn=102&sortEnd=&pageSize=40&req_trace={int(time.time()*1000)}")
    d = json.loads(http(u, "https://kuaixun.eastmoney.com/", deadline=deadline))
    out = []
    for it in d.get("data", {}).get("fastNewsList", []):
        text = (it.get("summary") or "").strip()
        if not text:
            continue
        ts = parse_ts(it.get("showTime"))
        if not ts:
            bad_ts("东财")
            continue
        out.append({"src": "东财", "ts": ts, "text": text})
    return out


def merge_title_digest(title: str, digest: str) -> str:
    """短标题不得丢弃 digest (F-12): 拼接为「标题：digest」."""
    if not title:
        return digest
    if not digest:
        return title
    if len(title) >= 8 or digest in title:
        return title
    if title in digest:
        return digest
    return f"{title}：{digest}"


def src_ths(deadline: float | None = None) -> list[dict]:
    u = "https://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=&track=website&pagesize=40"
    d = json.loads(http(u, deadline=deadline))
    out = []
    for it in d.get("data", {}).get("list", []):
        title = re.sub(r"<[^>]+>", "", (it.get("title") or "")).strip()
        digest = re.sub(r"<[^>]+>", "", (it.get("digest") or "")).strip()
        body = merge_title_digest(title, digest)
        if not body:
            continue
        ts = parse_ts(it.get("ctime"))
        if not ts:
            bad_ts("同花顺")
            continue
        out.append({"src": "同花顺", "ts": ts, "text": body})
    return out


def src_sina(deadline: float | None = None) -> list[dict]:
    u = ("https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=40"
         "&zhibo_id=152&tag_id=0&dire=f&dpc=1")
    d = json.loads(http(u, deadline=deadline))
    out = []
    for it in d.get("result", {}).get("data", {}).get("feed", {}).get("list", []):
        text = re.sub(r"<[^>]+>", "", (it.get("rich_text") or it.get("content") or "")).strip()
        if not text:
            continue
        ts = parse_ts(it.get("create_time"))
        if not ts:
            bad_ts("新浪")
            continue
        out.append({"src": "新浪", "ts": ts, "text": text})
    return out


def src_wscn_commodity(deadline: float | None = None) -> list[dict]:
    """华尔街见闻 商品频道: 品种面最宽 (黑色/农产品/能化), 低频但有效率高."""
    u = ("https://api-one-wscn.awtmt.com/apiv1/content/lives"
         "?channel=commodity-channel&client=pc&limit=50")
    d = json.loads(http(u, deadline=deadline))
    out = []
    for it in (d.get("data") or {}).get("items") or []:
        text = clean_text(it.get("content_text"))
        if not text:
            continue
        ts = parse_ts(it.get("display_time"))
        if not ts:
            bad_ts("见闻商品")
            continue
        out.append({"src": "见闻商品", "ts": ts, "text": text})
    return out


def _err_line() -> str:
    """抓 err() 写到 stderr 的那一行 (log() 只进 stdout)."""
    buf = io.StringIO()
    real = sys.stderr
    try:
        sys.stderr = buf
        err("selftest probe")
    finally:
        sys.stderr = real
    return buf.getvalue()


def _send_retry_ok() -> bool:
    """第一次腿抛 SSL EOF (19:48 那批的真实错因), 必须换腿重试且成功."""
    calls: list[str] = []
    real_tg, real_sleep = globals()["tg"], time.sleep

    def fake_tg(method, payload, tk, proxy, timeout=25):
        calls.append(proxy or "直连")
        if len(calls) == 1:
            raise urllib.error.URLError(ssl.SSLEOFError(8, "EOF in violation of protocol"))
        return {"ok": True}

    try:
        globals()["tg"] = fake_tg
        time.sleep = lambda s: None
        ok = send("probe", {"chat_id": 1, "proxy": "http://127.0.0.1:8080"}, "tk")
        return ok and len(calls) == 2 and calls[1] == "http://127.0.0.1:8080"
    finally:
        globals()["tg"], time.sleep = real_tg, real_sleep


def _send_429(header: str | None) -> tuple[float | None, int, bool]:
    """模拟 429 (可带 Retry-After): 返回 (SEND_RETRY_AFTER, tg 调用次数, send 结果)."""
    calls: list[str] = []
    hdrs = {} if header is None else {"Retry-After": header}
    real_tg = globals()["tg"]

    def fake_tg(method, payload, tk, proxy, timeout=25):
        calls.append(proxy or "直连")
        raise urllib.error.HTTPError("https://api.telegram.org", 429, "Too Many",
                                     hdrs, io.BytesIO(b'{"ok":false}'))

    try:
        globals()["tg"] = fake_tg
        ok = send("probe", {"chat_id": 1, "proxy": ""}, "tk")
    finally:
        globals()["tg"] = real_tg
    return SEND_RETRY_AFTER, len(calls), ok


def _smm_empty_raises() -> bool:
    """变异体保护: listList 为空/缺失时必须 RuntimeError, 不能当成「今天没新闻」."""
    fake = {"props": {"pageProps": {"liveListData": {"data": {"data": {}}}}}}
    raw = ('<script id="__NEXT_DATA__" type="application/json">'
           + json.dumps(fake) + "</script>")
    real = http
    try:
        globals()["http"] = lambda *a, **k: raw.encode()
        try:
            src_smm(None)
        except RuntimeError:
            return True
        return False
    finally:
        globals()["http"] = real


# SMM 源内噪声: 分钟级行情预警 + 无对应合约的产业稿 (印尼煤炭/钼/有机硅),
# 后者占实测有效条目的 45%, 不剔会每天净增 30-40 条无品种噪声。
SMM_NOISE = ("上期所合约价格预警", "煤炭快讯", "氧化钼", "钼快讯", "钼铁",
             "有机硅", "硅烷", "硅弹性体", "硅橡胶")


def smm_keep(text: str) -> bool:
    """SMM 源内取舍 (可单测)."""
    t = text.strip()
    return bool(t) and not any(w in t for w in SMM_NOISE)


def src_smm(deadline: float | None = None) -> list[dict]:
    """上海有色网快讯 (Next.js SSR, 解析 __NEXT_DATA__); 无公开分页 API, 只取首页."""
    raw = http("https://news.smm.cn/live", deadline=deadline).decode("utf-8", "ignore")
    mt = re.search(r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', raw, re.S)
    if not mt:
        raise RuntimeError("SMM: 未找到 __NEXT_DATA__ (前端结构变了)")
    d = json.loads(mt.group(1))
    pp = (d.get("props") or {}).get("pageProps") or {}
    lld = pp.get("liveListData") or {}
    lst = ((lld.get("data") or {}).get("data") or {}).get("listList") or []
    if not lst:
        raise RuntimeError("SMM: listList 为空 (前端结构变了)")
    out = []
    for it in lst:
        title = clean_text(it.get("title"))
        profile = clean_text(it.get("profile"))
        # 5/30 条标题在正文开头重复一遍, 只留正文
        text = profile if title and profile.startswith(title[:min(len(title), 24)]) else \
            clean_text(f"{title} {profile}")
        if not smm_keep(text):
            continue                      # 分钟级行情异动预警 = 纯噪声, 不进过滤器
        
        ts = parse_ts(it.get("pubDate"))
        if not ts:
            bad_ts("SMM")
            continue
        out.append({"src": "SMM", "ts": ts, "text": text})
    return out


# --------------------------------------------------------------- 英文源 (Batch 6)
# 8 个外媒 RSS: 6 个直连 + 2 个经本机代理. 与中文源同构 [{'src','ts','text'}], 复用
# clean_text / sane_ts / ITEM_MAX; 解析失败或 0 条一律抛 RuntimeError
# (「空结果 = 失败」约定, 不静默当成「今天没新闻」).
FEED_HTTP = {"tries": 2, "cap": 8}     # 新源: 2 次重试 / 单次上限 8s (中文源仍 3 次 / 12s)
FEED_UA = ("Feedly/1.0 (+http://www.feedly.com/fetcher.html; 4 subscribers; )")
                                       # 部分站点按 UA 拦浏览器指纹: 浏览器 UA 403, Feed 客户端 UA 200
FEED_MINING = "https://www.mining.com/feed"
FEED_OILPRICE = "https://oilprice.com/rss/main"
FEED_MININGTECH = "https://www.mining-technology.com/feed/"
FEED_NASDAQ = "https://www.nasdaq.com/feed/rssoutbound?category=Commodities"
FEED_CNBC = "https://www.cnbc.com/id/100727362/device/rss/rss.html"
FEED_MARKETWATCH = "https://feeds.content.dowjones.io/public/rss/mw_topstories"
FEED_INVESTING = "https://www.investing.com/rss/news_11.rss"
FEED_HELLENIC = "https://www.hellenicshippingnews.com/feed/"
FEED_AGENCY = re.compile(r"^\s*\((?:RTTNews|Reuters|Bloomberg|Dow Jones|CNBC|AP|AFP)\)\s*[-–—]\s*")
FEED_CREDIT = re.compile(
    r"\s*(?:The post .{0,140}? appeared first on .{0,60}?"
    r"|Read more(?: on .{0,40})?"
    r"|Continue reading.{0,40}?"
    r"|\(?Source: .{0,60}?\)?)\s*$", re.I)


def pub_ts(raw, tz=timezone.utc) -> int:
    """RSS pubDate -> 时间戳, 失败返回 0 (调用方计 TS_BAD 并丢弃, 不伪造 now).

    不用 parse_ts(): 它的 RFC-822 分支是 `"%a %b %d %H:%M:%S %z %Y"` (无逗号), 而真实
    feed 一律是 `Thu, 05 Nov 2026 14:23:00 +0000` (有逗号) -> parse_ts 对这 8 个源的
    156 条全部返回 0 (2026-11-06 实测)。email.utils 两种都吃。
    无时区串按 tz=UTC 解释: Investing 的 `2026-11-05 15:18:28` 实测为 UTC, 若按
    parse_ts 默认的 +08:00 解释会把整源推前 8h, 全被 max_age 丢掉。
    """
    s = str(raw or "").strip()
    if not s:
        return 0
    try:
        dt = email.utils.parsedate_to_datetime(s)
    except (TypeError, ValueError):
        dt = None
    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:                       # ISO-8601: 2026-11-05T15:18:28Z / +00:00 / +08:00
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return sane_ts(int(dt.timestamp()))


def _rss_triples(raw: bytes) -> list[tuple[str, str, str]]:
    """(title, description, pubDate) 三元组; 非法 XML (源站未转义 &) 退化为正则切分.

    元素型 HTML 的 `<description><p>a</p><p>b</p></description>` 必须拼回所有文本段
    (findtext 只取首段会静默丢正文; Batch 6 U1-F-02), 段间补空格后再折叠空白。
    """
    def _all_text(node, tag: str) -> str:
        el = node.find(tag)
        if el is None:
            return ""
        return re.sub(r"\s+", " ", " ".join(el.itertext())).strip()

    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        root = None
    if root is not None:
        return [(_all_text(n, "title"), _all_text(n, "description"),
                 _all_text(n, "pubDate")) for n in root.findall(".//item")]
    out = []
    for block in re.findall(r"<item[\s>].*?</item>",
                            raw.decode("utf-8", "ignore"), re.S):
        row = []
        for tag in ("title", "description", "pubDate"):
            m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", block, re.S)
            v = (m.group(1) if m else "").strip()
            row.append(re.sub(r"^<!\[CDATA\[(.*)\]\]>$", r"\1", v, flags=re.S))
        out.append(tuple(row))
    return out


def feed_text(title, desc) -> str:
    """标题 + 摘要 -> 单条正文: 去 HTML / 折叠空白 / 去源站署名前缀与尾巴 / 截到 ITEM_MAX."""
    t = re.sub(r"\s+", " ", clean_text(title)).strip()
    d = re.sub(r"\s+", " ", clean_text(desc)).strip()
    d = FEED_AGENCY.sub("", d).strip()
    d = FEED_CREDIT.sub("", d).strip()
    if not d or d in t:
        body = t
    elif t and t not in d:
        body = f"{t}：{d}"
    else:
        body = d
    return body[:ITEM_MAX]


def rss_items(raw: bytes, src: str, tz=timezone.utc) -> list[dict]:
    """RSS 2.0 -> [{'src','ts','text'}]; 0 个可用条目抛 RuntimeError."""
    triples = _rss_triples(raw)
    out: list[dict] = []
    for title, desc, pub in triples:
        text = feed_text(title, desc)
        if not text:
            continue
        ts = pub_ts(pub, tz)
        if not ts:                        # 时间不可解析 -> 丢弃并计数, 不用 now() 兜底
            bad_ts(src)
            continue
        out.append({"src": src, "ts": ts, "text": text})
    if not out:
        raise RuntimeError(f"{src}: RSS 无可用条目 ({len(triples)} 个 item)")
    return out


def feed_http(url: str, deadline: float | None, proxy: str | None = None,
              ua: str | None = None) -> bytes:
    """英文源统一取数: 更小的 tries/cap (FEED_HTTP), 可指定代理与 UA."""
    return http(url, tries=FEED_HTTP["tries"], deadline=deadline,
                cap=FEED_HTTP["cap"], proxy=proxy, ua=ua)


def src_mining(deadline: float | None = None) -> list[dict]:
    """MINING.COM (直连): 36 条, 覆盖 ~3 天, 矿业/金属供给面."""
    return rss_items(feed_http(FEED_MINING, deadline), "Mining")


def src_oilprice(deadline: float | None = None) -> list[dict]:
    """OilPrice.com 主 feed (直连): 15 条, 原油/天然气/电力, 更新最密."""
    return rss_items(feed_http(FEED_OILPRICE, deadline), "OilPrice")


def src_miningtech(deadline: float | None = None) -> list[dict]:
    """Mining Technology (直连): 10 条. 浏览器 UA 复测可 200; 仍保留 Feedly 客户端 UA (FEED_UA) 作兜底."""
    return rss_items(feed_http(FEED_MININGTECH, deadline, ua=FEED_UA), "MiningTech")


def src_nasdaq(deadline: float | None = None) -> list[dict]:
    """Nasdaq Commodities (直连, RTTNews 供稿): 15 条, 摘要带 "(RTTNews) - " 前缀."""
    return rss_items(feed_http(FEED_NASDAQ, deadline), "Nasdaq")


def src_cnbc(deadline: float | None = None) -> list[dict]:
    """CNBC World (直连): 30 条, 全球宏观/能源, 与商品无关的多数会被词表丢掉."""
    return rss_items(feed_http(FEED_CNBC, deadline), "CNBC")


def src_marketwatch(deadline: float | None = None) -> list[dict]:
    """MarketWatch Top Stories (直连): 10 条, 道琼斯系; pubDate 为 GMT, 摘要齐全."""
    return rss_items(feed_http(FEED_MARKETWATCH, deadline), "MarketWatch")


def feed_proxy() -> str:
    """英文代理源取值口径与 send_plan() 完全一致: conf.proxy -> 环境 HTTPS_PROXY."""
    return str(CFG.get("proxy") or PROXY or "").strip()


def _feed_via_proxy(url: str, deadline: float | None, src: str) -> bytes:
    """代理腿: 未配置代理直接失败; 失败后不回退直连 (理由见 Batch 6 规格 U2 ⑥)."""
    purl = feed_proxy()
    if not purl:
        raise RuntimeError(f"{src}: 未配置代理 (conf.proxy / HTTPS_PROXY 皆空)")
    return feed_http(url, deadline, proxy=purl)


def src_investing(deadline: float | None = None) -> list[dict]:
    """Investing.com 商品频道 (代理): 10 条, 只有标题没有摘要, pubDate 为 UTC 无时区串."""
    return rss_items(_feed_via_proxy(FEED_INVESTING, deadline, "Investing"), "Investing")


def src_hellenic(deadline: float | None = None) -> list[dict]:
    """Hellenic Shipping News (代理): 20 条, 干散货/运价/船燃, 滞后 3-6h."""
    return rss_items(_feed_via_proxy(FEED_HELLENIC, deadline, "Hellenic"), "Hellenic")


# ------------------------------------------------------------------ Batch 7 研报源
def _month_back(now: datetime, back: int) -> str:
    """往前数 back 个自然月的 YYYYMM (back>=1; 2026-11 + back=1 -> '202610')."""
    y, m = now.year, now.month - back
    while m <= 0:
        m += 12
        y -= 1
    return f"{y}{m:02d}"


def src_em_report(deadline: float | None = None) -> list[dict]:
    """东财券商研报 (Batch 7): 全市场研报列表 -> 既有期货词表筛 -> 只留商品/期货相关.

    CTO 授权偏差 (覆盖规格 U4 的 qType=0): 只接 qType=1(行业研报) 与 qType=2(策略研报);
    qType=0(个股) 实测 0/100 命中不接, 合并 qType=1,2 会 HTTP 400, 故两次请求并按
    infoCode 去重. 实测 3 天窗口: qType=1 放行 6/100, qType=2 放行 2/100.
    publishDate 只精确到日期 ("2026-11-06 00:00:00.000"), 故 ts 取首次发现时刻,
    真实日期只写进 text 的元信息 (理由见 research_item)。
    """
    end = datetime.now(TZ_CN).date()
    begin = end - timedelta(days=3)          # 覆盖周末/长假: 列表按发布时间倒序, 100 条足够
    out = []
    now_ts = int(time.time())
    seen_codes: set[str] = set()
    for qtype in (1, 2):                     # CTO 裁决: 行业研报 + 策略研报, 不接个股(0)
        u = ("https://reportapi.eastmoney.com/report/list?industryCode=*&pageSize=100"
             f"&industry=*&rating=&ratingChange=&beginTime={begin:%Y-%m-%d}"
             f"&endTime={end:%Y-%m-%d}&pageNo=1&fields=&qType={qtype}&orgCode=&code=*"
             "&rcode=&p=1&pageNum=1")
        d = json.loads(http(u, "https://data.eastmoney.com/report/", deadline=deadline))
        for it in d.get("data") or []:
            title = clean_text(it.get("title"))
            if not title:
                continue
            org = clean_text(it.get("orgSName"))
            industry = clean_text(it.get("industryName"))
            if filter_reason(f"{title} {industry}", DEFAULT_KEYWORDS, True):
                continue                      # 与快讯同一套词表: 无品种/产业锚点的研报不留
            code = str(it.get("infoCode") or "").strip()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            day = str(it.get("publishDate") or "")[:10]
            out.append(research_item("东财研报", code, title,
                                     f"https://pdf.dfcfw.com/pdf/H3_{code}_1.pdf", now_ts,
                                     f"{org} · {day}" if day else org))
    return out


def src_cffex(deadline: float | None = None) -> list[dict]:
    """中金所 (Batch 7): 月报是 PDF 正文; 日/周报是逐合约行情 XML, 无正文可提取.

    实测路径 (2026-11):
      日报 /sj/hqsj/rtj/YYYYMM/DD/index.xml   交易日 200, 非交易日 302
      周报 /sj/hqsj/ztj/YYYYMM/MM/index.xml   MM = 该周周一 (不是周序号; 周五日期会 302)
      月报 /sj/monthlyReport/YYYYMM/YYYYMMMonthlyReport.pdf  次月上旬发布, 2.17MB/9 页
    日报/周报正文是 540KB/455KB 的逐合约数字表, 摘要化没有意义; 故只做 HEAD 存在性
    探测 + 官方 CSV 链接的提示条目 (不下载 body), 月报才下载 PDF 提摘要。
    """
    out: list[dict] = []
    now = datetime.now(TZ_CN)
    # 1) 月报: 从上月起回看两个月, 取最新一份已发布的 PDF (当月未发布时 302)
    for back in (1, 2):
        ym = _month_back(now, back)
        pdf = f"http://www.cffex.com.cn/sj/monthlyReport/{ym}/{ym}MonthlyReport.pdf"
        if not http_ok(pdf, deadline):
            continue
        title = f"中金所月度数据统计 {ym[:4]}年{int(ym[4:])}月"
        out.append(research_item("中金所", f"m{ym}", title, pdf,
                                 int(time.time()), "中国金融期货交易所"))
        break
    # 2) 日报: 只回看最近 3 天 (周末直接跳过, 省掉必然 302 的探测)
    for back in range(0, 3):
        d = now - timedelta(days=back)
        if d.weekday() >= 5:
            continue
        ymd = d.strftime("%Y%m%d")
        u = f"http://www.cffex.com.cn/sj/hqsj/rtj/{d:%Y%m}/{d:%d}/index.xml"
        if not http_ok(u, deadline):
            continue
        csv = f"http://www.cffex.com.cn/sj/hqsj/rtj/{d:%Y%m}/{d:%d}/{ymd}_1.csv"
        out.append(research_item("中金所", f"d{ymd}",
                                 f"中金所日度行情统计 {d:%Y年%m月%d日}", csv,
                                 int(time.time()), "中国金融期货交易所 · 收盘后发布", ""))
        break
    # 3) 周报: 本周周一 / 上周周一的汇总 XML (当前周数据自周一起滚动更新)
    monday = now - timedelta(days=now.weekday())
    for back in (0, 1):
        d = monday - timedelta(days=7 * back)
        u = f"http://www.cffex.com.cn/sj/hqsj/ztj/{d:%Y%m}/{d:%d}/index.xml"
        if not http_ok(u, deadline):
            continue
        csv = f"http://www.cffex.com.cn/sj/hqsj/ztj/{d:%Y%m}/{d:%d}/{d:%Y%m%d}_1.csv"
        out.append(research_item("中金所", f"w{d:%Y%m%d}",
                                 f"中金所周度数据统计 {d:%Y年%m月%d日}当周", csv,
                                 int(time.time()), "中国金融期货交易所 · 周度汇总", ""))
        break
    return out


def src_cfa(deadline: float | None = None) -> list[dict]:
    """中期协「全国期货市场交易情况简报」(Batch 7): 月频, 正文是 4 页 PDF (有文本层).

    搜索接口实测 pageSize=20 回列表、按 docRelTime 降序; 只取最新 2 期 (本月与上月),
    再多就是历史归档, 首轮接入会一次性灌一堆旧简报。docRelTime 只有日期, 真实发布日期
    写进 text 元信息, ts 取首次发现时刻 (理由见 research_item)。
    """
    q = urllib.parse.urlencode({"programName": "交易情况简报", "pageNo": 1, "pageSize": 20})
    u = ("https://www.cfachina.org/qx-search/api/wcmSearch/searchDocsByProgram?" + q)
    d = json.loads(http(u, "https://www.cfachina.org/", deadline=deadline))
    rows = (d.get("data") or {}).get("dataList") or []
    out = []
    now_ts = int(time.time())
    for it in rows[:2]:
        title = clean_text(it.get("docTitle"))
        path = str(it.get("docPubUrl") or "").strip()
        if not title or not path.startswith("/"):
            continue
        ident = str(it.get("docId") or path.rsplit("/", 1)[-1])
        day = str(it.get("docRelTime") or "")[:10]
        out.append(research_item("中期协", ident, title,
                                 "https://www.cfachina.org" + path, now_ts,
                                 f"中国期货业协会 · {day}" if day else "中国期货业协会"))
    return out


# 2026-11-04 源体检 (7 源同批抓取实测, 双评审):
#   · 见闻商品: 页面 50 条跨 ~15h, 滞后 75min, 有效 44%; 单 180min 窗均值 ~2 条 / 3.7 品种,
#     价值在黑色/农产/能化等其它源 0 覆盖的品种, 不是实时宽度 (别据此放宽 max_age_min)。
#   · SMM: 有色产业快讯, 滞后 ~19min, 有效 83% 但 45% 是无合约产业稿 -> 源内先剔 (SMM_NOISE)。
#     SSR 单页无分页 API, 页面深度仅 ~5h, 结构一变即 RuntimeError。
#   · 金属网(akshare): 独家品种 0、滞后 90min、只回 10/100 条 -> 退役, akshare 依赖一并移除。
#     遗留缺口: SHFE 仓单/基差日报等日频数据面暂无替代源。
#   · 金十 flash: 有效 16%、独家品种 0、含栏目广告、需私有 x-app-id -> 不接。
#   · RSSHub 公共实例: 暖缓存可用(0.6-1.1s)但冷启动 40s 超时 + 内容滞后 48min-19d -> 不接。
SOURCES = [("财联社", src_cls), ("东财", src_em), ("同花顺", src_ths),
           ("新浪", src_sina), ("见闻商品", src_wscn_commodity), ("SMM", src_smm),
           # Batch 6 英文外媒: 中文 6 源必须保持最前, 其起始时刻与 deadline 余量因此
           # 与加源前逐次一致; 代理腿排在最后, 避免其 20s 级阻塞饿死直连腿。
           ("Mining", src_mining), ("OilPrice", src_oilprice),
           ("MiningTech", src_miningtech), ("Nasdaq", src_nasdaq),
           ("CNBC", src_cnbc), ("MarketWatch", src_marketwatch),
           ("Investing", src_investing), ("Hellenic", src_hellenic),
           # Batch 7 研报源: 排在最后, 前 14 源的先后与 MIN_SLICE 份额不变; 每 30 分钟
           # 才真正跑一次 (见 main 的 _research 节流), PDF 正文在抓取循环之后单独提。
           ("东财研报", src_em_report), ("中金所", src_cffex), ("中期协", src_cfa)]


# ------------------------------------------------------------------ telegram
def resolve_token(conf: dict) -> str:
    """token 只来自环境变量或 0600 配置文件; 源码不含凭据 (F-03)."""
    return str(os.environ.get("FUTNEWS_TOKEN") or conf.get("token") or "").strip()


def tg(method: str, payload: dict | None, tk: str, proxy, timeout: int = 25) -> dict:
    """proxy: 代理 URL (如 http://127.0.0.1:8080) 或空/False 表示直连."""
    url = f"https://api.telegram.org/bot{tk}/{method}"
    data = urllib.parse.urlencode(payload).encode() if payload else None
    purl = proxy if isinstance(proxy, str) else ""
    handler = (urllib.request.ProxyHandler({"http": purl, "https": purl}) if purl
               else urllib.request.ProxyHandler({}))
    with urllib.request.build_opener(handler).open(
            urllib.request.Request(url, data=data), timeout=timeout) as r:
        return json.loads(r.read().decode())


def discover_chat_id(conf: dict, tk: str) -> int | None:
    try:
        for upd in reversed(tg("getUpdates", {"timeout": 0, "limit": 20}, tk, "").get("result", [])):
            chat = (upd.get("message") or upd.get("channel_post") or {}).get("chat") or {}
            if chat.get("id"):
                conf["chat_id"] = int(chat["id"])
                save_json(CONF, conf)
                return conf["chat_id"]
    except Exception as e:
        log(f"getUpdates failed: {e}")
    return None


def split_msg(text: str) -> list[str]:
    """按段落切半, 保证不切断单条条目的 HTML 标签 (F-09)."""
    parts = text.split("\n\n")
    if len(parts) < 2:
        return [text]
    total, acc, idx = u16(text), 0, 1
    for i, part in enumerate(parts[:-1]):
        acc += u16(part) + 2
        if acc >= total / 2:
            idx = i + 1
            break
    return ["\n\n".join(parts[:idx]), "\n\n".join(parts[idx:])]


SEND_ERR = ""                          # 最近一次发送失败原因 (供调用方写进 err 行)
SEND_RETRY_AFTER: float | None = None  # 429 的 Retry-After 建议秒数 (None=头缺失/不可解析)
RA_MIN = 30                            # Retry-After 夹取下限(秒): 0/负数也不立刻重打
RA_MAX = 1800                          # Retry-After 夹取上限(秒): 与指数退避封顶一致, 不冻结队列


def parse_retry_after(raw: str | None) -> float | None:
    """解析 429 的 Retry-After (整数秒 / HTTP-date), 夹取到 [RA_MIN, RA_MAX].

    头缺失、非数字垃圾、其它解析失败一律返回 None, 由调用方完全回退既有指数退避;
    HTTP-date 按 RFC 9110 的 GMT 形态解析, 已经过去的时间同样夹到下限.
    """
    s = str(raw or "").strip()
    if not s:
        return None
    secs: float | None = None
    try:
        # 先在整数域夹取再转 float: 309~4300 位的纯数字串不会抛 OverflowError (U1-F-01)
        secs = float(max(RA_MIN, min(int(s), RA_MAX)))
    except ValueError:
        try:
            secs = float(s)                     # 兼容 "120.5" 这类非整数字面量
        except ValueError:
            try:
                dt = email.utils.parsedate_to_datetime(s)
            except (TypeError, ValueError):
                return None
            if dt is None:
                return None
            if dt.tzinfo is None:               # 无时区日期按 UTC 解释 (HTTP-date 只允许 GMT)
                dt = dt.replace(tzinfo=timezone.utc)
            secs = dt.timestamp() - time.time()
    if secs is None or not math.isfinite(secs):
        return None
    return float(min(RA_MAX, max(RA_MIN, secs)))


def retry_gap(fallback: float, ra: float | None = None) -> float:
    """本轮退避秒数: 有 429 建议值就用它, 否则完全回退既有退避值 (U1)."""
    return fallback if ra is None else float(ra)


def is_transient(e: BaseException) -> bool:
    """传输层瞬时错误 (握手被重置/DNS/无路由/超时) 值得立刻换腿重试;
    HTTPError(400/401/429 等) 是确定性失败, 重试只会重复打接口."""
    if isinstance(e, urllib.error.HTTPError):
        return False
    transient_types = (ssl.SSLError, socket.gaierror, socket.timeout,
                       ConnectionError, TimeoutError)
    # URLError 会把真正的错误塞在 .reason 里, 两层都要看
    for c in (e, getattr(e, "reason", None)):
        if c is None:
            continue
        if isinstance(c, transient_types):
            return True
        if isinstance(c, OSError) and getattr(c, "errno", None) in (51, 54, 60, 61, 65):
            return True                     # ENETUNREACH/ECONNRESET/ETIMEDOUT/ECONNREFUSED/EHOSTUNREACH
    return False


def send_plan(conf: dict) -> list[tuple[str, int]]:
    """发送腿: 直连(快) -> 代理 -> 直连重试. 代理 URL 来自配置或 HTTPS_PROXY."""
    purl = str(conf.get("proxy") or PROXY or "").strip()
    plan = [("", 8)]
    if purl:
        plan.append((purl, 20))
    plan.append(("", 20))
    return plan


def send_worst(conf: dict) -> int:
    """单次 send 的最坏耗时(秒): 各腿 timeout 之和 + 腿间 sleep 3s."""
    plan = send_plan(conf)
    return sum(t for _, t in plan) + 3 * (len(plan) - 1)


SEND_WORST = send_worst({"proxy": "http://127.0.0.1:8080"})   # 生产口径(3 腿): 8+20+20+3×2=54


def send_time_ok() -> bool:
    """逐次发送闸门: 现在再发一次 send 仍不会顶到 ROUND_HARD_S - ROUND_MARGIN_S.

    ROUND_START 为 0 (直接单测 / 非 main 调用) 视为无耗时压力, 不阻断.
    每个 send 点发送前都必须先过这道闸门; 不满足就把该条消息推迟到下一轮, 不算失败.
    """
    start = ROUND_START or time.monotonic()
    return (time.monotonic() - start) + SEND_WORST <= ROUND_HARD_S - ROUND_MARGIN_S


def send(text: str, conf: dict, tk: str, depth: int = 0) -> bool:
    global SEND_ERR, SEND_RETRY_AFTER
    SEND_RETRY_AFTER = None                  # 本轮未遇 429 时必须为 None, 不得沿用上一轮
    chat_id = conf.get("chat_id") or discover_chat_id(conf, tk)
    if not chat_id:
        SEND_ERR = "no chat_id (先给 bot 发一条消息)"
        log(SEND_ERR)
        return False
    base = {"chat_id": chat_id, "disable_web_page_preview": "true"}
    for i, (proxy, timeout) in enumerate(send_plan(conf)):
        if i:
            time.sleep(3)                            # 瞬时抖动给 3s 缓和, 别整轮作废
        try:
            tg("sendMessage", {**base, "text": text, "parse_mode": "HTML"}, tk, proxy, timeout)
            SEND_ERR = ""
            return True
        except urllib.error.HTTPError as e:
            detail = e.read()[:200].decode(errors="ignore")
            log(f"send proxy={proxy} HTTP {e.code}: {detail}")
            if e.code == 400 and "too long" in detail.lower() and depth < 4:
                parts = split_msg(text)                  # 超长 -> 拆批重发, 不换代理重试同一 payload
                if len(parts) > 1:
                    log(f"消息超长 (u16={u16(text)}), 拆成 {len(parts)} 批重发")
                    return all(send(p, conf, tk, depth + 1) for p in parts)
            if e.code == 400 and "parse" in detail.lower():
                try:                                     # HTML 实体炸了 -> 降级纯文本
                    tg("sendMessage", {**base, "text": re.sub(r"<[^>]+>", "", text)}, tk, proxy, timeout)
                    return True
                except Exception as e2:
                    log(f"plain-text fallback failed: {e2}")
            if e.code == 429:
                hdrs = getattr(e, "headers", None)
                SEND_RETRY_AFTER = parse_retry_after(hdrs.get("Retry-After") if hdrs else None)
                SEND_ERR = "HTTP 429 限流"
                if SEND_RETRY_AFTER is not None:
                    SEND_ERR += f" (Retry-After {int(SEND_RETRY_AFTER)}s)"
                return False                             # 限流: 下轮再推, 不标记已读
            SEND_ERR = f"HTTP {e.code}: {detail[:120]}"
            return False                                 # 确定性失败, 不换腿重试同一 payload
        except Exception as e:
            SEND_ERR = f"{type(e).__name__}: {e}"
            log(f"send proxy={proxy or '直连'} failed: {SEND_ERR}"
                + ("" if is_transient(e) else " (非瞬时, 不再换腿)"))
            if not is_transient(e):
                return False
    return False


# ------------------------------------------------------------------ state extras
def take_pending() -> list[str]:
    """上一轮「已发送但 state 落盘失败」的待确认键 (F-07)."""
    data, problem = load_state(PENDING)
    if problem:
        err(f"待确认标记文件不可读, 已忽略: {problem}")
        return []
    keys = data.get("keys") or []
    return [k for k in keys if isinstance(k, str) and k]


def write_pending(keys: list[str]) -> bool:
    if not keys:
        return False
    return save_json(PENDING, {"keys": keys, "ts": int(time.time())})


def clear_pending() -> None:
    try:
        PENDING.unlink()
    except FileNotFoundError:
        pass
    except OSError as e:
        log(f"清理待确认标记失败: {e}")


def record_stale(seen: dict, stale: list[dict], max_age: int) -> None:
    """stale 记账: 放在推送之后, 且失败不阻断推送 (F-08)."""
    if not stale:
        return
    try:
        now_ts = int(time.time())
        for it in stale:
            seen[it["key"]] = now_ts
        saved = save_json(STATE, prune(seen))
        log(f"skipped {len(stale)} stale (> {max_age // 60}min)"
            f"{'' if saved else ' (记账落盘失败: 这些键未写 seen, 可能被源站重发)'}")
    except Exception as e:
        log(f"stale 记账异常, 已忽略: {type(e).__name__}: {e}")


def alert_all_failed(seen: dict, errors: list[str], conf: dict, tk: str, dry: bool,
                     skipped: int = 0) -> None:
    """全源失败告警状态机 (F-15): 首轮发 1 条 -> 之后每 ALERT_EVERY 汇总 -> 恢复另有通知.

    skipped: 本轮被预算跳过的源数, 只进文案计数, 不进失败源名单 (Batch 6.1 U-A).
    """
    now = int(time.time())
    alert = seen.get("_alert")
    alert = alert if isinstance(alert, dict) else {}
    detail = esc(", ".join(errors)) or "-"
    suffix = f" (跳过 {skipped} 源)" if skipped else ""
    if alert.get("failing"):
        alert["rounds"] = int(alert.get("rounds") or 0) + 1
        last = int(alert.get("last_sent") or 0)
        if last and now - last < ALERT_EVERY:
            log(f"全源故障第 {alert['rounds']} 轮, 告警已抑制 (上次 {now - last}s 前)")
            seen["_alert"] = alert
            return
        msg = f"⚠️ 期货快讯源仍全部失败 (累计 {alert['rounds']} 轮): {detail}" + suffix
    else:
        alert = {"failing": True, "since": now, "rounds": 1, "last_sent": 0, "alerts": 0}
        msg = f"⚠️ 期货快讯源全部抓取失败: {detail}" + suffix
    if dry:
        log("(dry-run) 告警未发送")
        seen["_alert"] = alert
        return
    if not send_time_ok():              # 逐次闸门: 再发会顶到 120s -> 推迟, rounds 已递增, last_sent 不更新
        log("全源失败告警推迟到下一轮 (本轮再发会顶到 120s 上界)")
        seen["_alert"] = alert
        return
    ok = send(msg, conf, tk)
    if ok:
        alert["last_sent"] = now
        alert["alerts"] = int(alert.get("alerts") or 0) + 1
        log(f"故障告警已发送 (累计 {alert['alerts']} 条)")
    else:
        log("故障告警发送失败, 下轮重试")      # 发送结果不丢弃
    seen["_alert"] = alert


def note_recovered(seen: dict, conf: dict, tk: str, dry: bool) -> bool:
    """从全源故障恢复: 故障告警已送达 (alerts>=1) 才发 1 条恢复通知, 否则只静默复位."""
    alert = seen.get("_alert")
    if not isinstance(alert, dict) or not alert.get("failing"):
        return False
    rounds = int(alert.get("rounds") or 0)
    since = int(alert.get("since") or 0)
    mins = max(0, (int(time.time()) - since) // 60)
    alerts = int(alert.get("alerts") or 0)
    if not alerts:                       # 告警从未送达 (发送全失败 / dry-run): 不发假"已恢复" (U-A-2)
        log(f"全源恢复: 故障告警从未送达 (故障 {rounds} 轮), 静默复位, 不发恢复通知")
    elif not dry and not send_time_ok():  # 逐次闸门: 恢复通知推迟, _alert 状态保留, 下轮重试
        log("全源恢复通知推迟到下一轮 (本轮再发会顶到 120s 上界)")
        return False
    elif not dry:
        ok = send(f"✅ 期货快讯源已恢复 (故障 {rounds} 轮 / 约 {mins} 分钟)", conf, tk)
        log(f"恢复通知{'已发送' if ok else '发送失败'} (故障 {rounds} 轮)")
    seen["_alert"] = {"failing": False, "since": 0, "rounds": 0, "last_sent": 0, "alerts": 0}
    return True


# ---------------- 单源连续失败告警 (U2)
def src_fail_state(seen: dict) -> dict:
    """取单源失败状态块 (_src_fail: {源名: 计数块}); 类型不对时返回空 dict, 由调用方覆盖."""
    state = seen.get(SRC_FAIL_KEY)
    return state if isinstance(state, dict) else {}


def src_alert_rounds(conf: dict) -> int:
    """单源告警阈值: conf.src_alert_rounds 覆盖, 缺省 SRC_ALERT_ROUNDS, 下限 1."""
    return max(1, to_int(conf.get("src_alert_rounds"), SRC_ALERT_ROUNDS))


def source_health(seen: dict, outcomes: list[tuple[str, str]], conf: dict,
                  tk: str, dry: bool) -> bool:
    """按源维护连续失败轮数: 连续 N 轮发 1 条, 之后每 SRC_ALERT_EVERY 汇总 1 条带累计轮数;
    该源恢复且此前告过警时发 1 条恢复通知并清零.

    outcomes = [(源名, ""), (源名, "错误摘要"), (源名, SRC_SKIP), ...] (与 SOURCES 同序).
    三态: "" = 成功 / 非空错误摘要 = 失败 / SRC_SKIP = 全局预算跳过 (不计失败、不清零、
    不告警、不发恢复; 跳过也不写入 errors/stats 的失败列表).
    全源失败整轮交给 alert_all_failed, 这里不计数也不发, 避免重复告警.
    返回是否改动过状态 (调用方据此决定落不落盘); dry=True 只记账不发送.
    """
    if not outcomes:
        return False
    all_failed = all(bool(e) and e != SRC_SKIP for _, e in outcomes)
    now = int(time.time())
    state = src_fail_state(seen)
    need = src_alert_rounds(conf)
    changed = False
    due_list: list[tuple[str, dict]] = []
    for name, detail in outcomes:
        if detail == SRC_SKIP:              # 全局预算跳过: 不计失败、不清零、不告警 (Batch 6 U4)
            continue
        cur = state.get(name)
        cur = dict(cur) if isinstance(cur, dict) else {}
        if all_failed:                      # 全源故障: 既有 alert_all_failed 负责, 本机不发
            continue
        if not detail:                      # 本轮成功
            if not to_int(cur.get("alerts")):
                if name in state:           # 没告过警的失败计数直接清零, 不发恢复通知
                    state.pop(name, None)
                    changed = True
                continue
            rounds = to_int(cur.get("rounds"))
            alert = seen.get("_alert")
            if isinstance(alert, dict) and alert.get("failing"):
                # 全源恢复轮: note_recovered() 本轮会发 1 条全源恢复通知, 这里只静默清块 (U2-F-01)
                state.pop(name, None)
                changed = True
                continue
            if dry:
                log(f"(dry-run) 单源恢复通知未发送 ({name})")
                state.pop(name, None)
                changed = True
                continue
            if not send_time_ok():          # 逐次闸门: 恢复通知推迟, 状态保留不清零
                log(f"单源恢复通知推迟到下一轮 ({name})")
                continue
            ok = send(f"✅ 快讯源「{esc(name)}」已恢复 (此前连续失败 {rounds} 轮)", conf, tk)
            log(f"单源恢复通知{'已发送' if ok else '发送失败'} ({name})")
            if ok:
                state.pop(name, None)
            else:
                cur["rounds"] = rounds      # 保留真实轮数: 重试文案不能显示「0 轮」(U2-F-03)
                state[name] = cur           # 通知没发出: 留状态下轮重试, 状态不许丢
            changed = True
            continue
        cur["rounds"] = to_int(cur.get("rounds")) + 1
        cur["since"] = to_int(cur.get("since")) or now
        cur["last_err"] = detail[:120]      # 只留最近一次错误摘要, 消息体不随轮数膨胀
        rounds = cur["rounds"]
        last = to_int(cur.get("last_sent"))
        alerts = to_int(cur.get("alerts"))
        due = rounds >= need and (bool(cur.get("pend")) or not last
                                  or now - last >= SRC_ALERT_EVERY)
        if due:
            cur["pend"] = True              # 先记账再发: 发送失败也不丢状态, 下轮重试
            due_list.append((name, cur))
        state[name] = cur
        changed = True
    # 同一轮多个源同时 due -> 合并为 1 条发送 (CTO 裁决 D); 成功后统一更新 last_sent/alerts
    if due_list:
        if dry:
            for name, cur in due_list:
                log(f"(dry-run) 单源故障告警未发送 ({name}, 连续 {cur['rounds']} 轮)")
        elif not send_time_ok():       # 逐次闸门: 告警推迟, pend/alerts/rounds 全部保留, 下轮重试
            for name, cur in due_list:
                log(f"单源故障告警推迟到下一轮 ({name}, 连续 {cur['rounds']} 轮)")
        else:
            lines = []
            for name, cur in due_list:
                alerts = to_int(cur.get("alerts"))
                head = (f"⚠️ 快讯源「{esc(name)}」已连续失败 {cur['rounds']} 轮" if not alerts
                        else f"⚠️ 快讯源「{esc(name)}」仍连续失败 (累计 {cur['rounds']} 轮)")
                lines.append(f"{head}: {esc(cur['last_err'])}")
            ok = send("\n".join(lines), conf, tk)
            if ok:
                for name, cur in due_list:
                    cur["last_sent"] = now
                    cur["alerts"] = to_int(cur.get("alerts")) + 1
                    cur["pend"] = False
                    log(f"单源故障告警已发送 ({name}, 累计 {cur['alerts']} 条)")
            else:
                for name, cur in due_list:
                    log(f"单源故障告警发送失败, 下轮重试 ({name})")
    if state:
        seen[SRC_FAIL_KEY] = state
    else:
        seen.pop(SRC_FAIL_KEY, None)
    return changed


def research_line(it: dict, dt: datetime) -> str:
    """研报条目渲染 (Batch 7): 标题 / 机构·日期 / 摘要 / 「查看原文」链接.

    只对带 url 的条目生效; 非研报条目的行拼接仍走 build_messages 原分支, 逐字节不变。
    text 的「标题｜机构 · 日期」按第一个全角竖线切开 (标题里的括号不再参与解析)。

    修复 C1/C3: 非 http(s) / 非字符串 url 退回纯文本 (不渲染链接); 标题/元信息/摘要/
    URL 各有硬上限, 保证单条 u16 <= MSG_MAX (否则 split_msg 拆不开, 整批永久重试)。
    """
    url = it.get("url")
    if not is_http_url(url):                 # C1 协议白名单 + C 轻微: int url 不再抛 AttributeError
        body = it["text"]
        if len(body) > ITEM_MAX:
            body = body[:ITEM_MAX] + "…"
        return f"<b>{esc(it['src'])} {dt:%m-%d}</b>\n{esc(body)}"
    head, _, meta = it["text"].partition("｜")
    title = head[:ITEM_MAX] + ("…" if len(head) > ITEM_MAX else "")
    lines = [f"<b>{esc(it['src'])} {dt:%m-%d}</b>", f"<b>{esc_quote(title)}</b>"]
    if meta:
        lines.append(esc_quote(meta[:RESEARCH_META_MAX]))
    snip = str(it.get("snippet") or "").strip()
    if snip:
        lines.append(esc_quote(snip[:SNIPPET_MAX + 20]))
    if len(url) > RESEARCH_URL_MAX:           # C3: 超长 URL 退化为纯文本提示, 保证可拆批
        lines.append(f"（原文链接过长 {len(url)} 字符, 已省略）")
    else:
        lines.append(f'<a href="{esc_url(url)}">查看原文</a>')
    return "\n".join(lines)


# ------------------------------------------------------------------ main
def build_messages(batch: list[dict]) -> list[tuple[str, list[dict]]]:
    """整行拼接, 不切断标签; 按 UTF-16 码元装满一条就开下一条 (F-09 拆批发送)."""
    msgs: list[tuple[str, list[dict]]] = []
    lines, used, budget = [], [], 0
    now_date = datetime.now(TZ_CN).date()
    for it in batch:
        body = it["text"]
        if len(body) > ITEM_MAX:
            body = body[:ITEM_MAX] + "…"
        dt = datetime.fromtimestamp(it["ts"], TZ_CN)
        stamp = dt.strftime("%H:%M") if dt.date() == now_date else dt.strftime("%m-%d %H:%M")
        line = (research_line(it, dt) if is_http_url(it.get("url"))
                else f"<b>{esc(it['src'])} {stamp}</b>\n{esc(body)}")
        add = u16(line) + (2 if lines else 0)
        if lines and budget + add > MSG_MAX:
            msgs.append(("\n\n".join(lines), used))
            lines, used, budget = [], [], 0
            add = u16(line)
        lines.append(line)
        used.append(it)
        budget += add
    if lines:
        msgs.append(("\n\n".join(lines), used))
    return msgs


def build_message(batch: list[dict]) -> tuple[str, list[dict]]:
    """兼容旧调用: 只返回第一条消息."""
    msgs = build_messages(batch)
    return msgs[0] if msgs else ("", [])


def slice_deadline(deadline: float, now: float, remaining: int,
                   min_slice: float = MIN_SLICE) -> float:
    """给单个源分配本轮抓取份额的 deadline (Batch 6.1 U-B).

    share = max(min_slice, 剩余预算 / 剩余源数), 再与全局 deadline 取小:
    单个慢源/失败源最多吃到自己的份额, 不会把整轮预算吃光; 份额本身不早于
    min_slice, 覆盖生产实测 p99 最慢的正常源 (见闻商品 5.40s, max 7.50s)。
    快源在自己的份额内正常返回,
    因此正常轮总耗时不变 (份额只是上限, 不是等待)。
    """
    left = deadline - now
    share = (left / remaining) if remaining > 0 else left
    return min(deadline, now + max(min_slice, share))


def main() -> int:
    global ROUND_START
    args = sys.argv[1:]
    dry = "--dry-run" in args
    seed = "--seed" in args
    force_all = "--all" in args
    stats = "--stats" in args or dry
    explain = "--explain" in args

    dump_arg = next((a for a in args                     # §3.11 存档导出子命令: 独立早退, 不进抓取/锁/网络主路径
                     if a == "--dump-items" or a.startswith("--dump-items=")), None)
    if dump_arg is not None:                         # (可与 --dry-run 并置; 导出即其全部语义)
        if dump_arg.startswith("--dump-items="):     # 等号形: --dump-items=PATH
            out_arg = dump_arg.split("=", 1)[1]
        else:
            idx = args.index(dump_arg)
            out_arg = args[idx + 1] if idx + 1 < len(args) else ""
        if not out_arg or out_arg.startswith("--"):
            err("--dump-items 需要 PATH 参数")
            return 2
        return dump_items(Path(out_arg))

    conf = load_json(CONF, {})
    CFG.clear()
    CFG.update(conf)                  # 供无参源函数读取 proxy (Batch 6 U2)
    force_flush = "--flush-now" in args
    show_queue = "--queue" in args
    try:
        digest_items = max(1, int(conf.get("digest_max_items") or DIGEST_MAX_ITEMS))
    except (TypeError, ValueError):
        digest_items = DIGEST_MAX_ITEMS
    try:
        digest_min_items = max(1, int(conf.get("digest_min_items") or DIGEST_MIN_ITEMS))
    except (TypeError, ValueError):
        digest_min_items = DIGEST_MIN_ITEMS
    try:
        max_hold = max(300, int(conf.get("max_hold_min") or MAX_HOLD_MIN) * 60)
    except (TypeError, ValueError):
        max_hold = MAX_HOLD_MIN * 60
    if "--limit" in args:                                # 兼容旧参数: 直接当作队列上限
        try:
            digest_items = max(1, int(args[args.index("--limit") + 1]))
        except (IndexError, ValueError):
            pass
    try:
        digest_min = max(1, int(conf.get("digest_min") or DIGEST_MIN))
    except (TypeError, ValueError):
        digest_min = DIGEST_MIN

    keywords = conf.get("keywords") or DEFAULT_KEYWORDS
    use_filter = conf.get("filter", "futures") == "futures" and not force_all

    rotate_log()

    # 单实例锁: 防止 launchd 上一轮未结束又起一轮 -> 重复推送
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock_fh = open(LOCK, "w")
    except OSError as e:
        err(f"锁文件不可写, 本轮不跑: {type(e).__name__}: {e} ({LOCK})")
        print(f"锁文件不可写: {e}")
        return 7                                      # 7 = 锁文件不可写 (原与 4 混用)
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        err("另一实例在跑 (launchd 正在抓取), 本轮跳过")   # 唯一一条提示
        return 0

    rotate_stderr()                                    # 锁内轮转: 两个实例不会同时截断同一个 fd

    # state: 文件不存在 = 首轮; 存在但不可读 = 告警 + 不覆盖 + 非 0 (F-04)
    seen, state_problem = load_state(STATE)
    if state_problem:
        err(f"state 文件不可读, 本轮不推送也不覆盖: {state_problem} ({STATE})")
        return 4                                      # 4 = state 不可读 (7 才是锁不可写)

    tk = resolve_token(conf)
    if not dry and not tk:
        err(f"缺少 bot token: 需设置 FUTNEWS_TOKEN 或 {CONF} 的 token 字段")
        return 3

    pending = take_pending()
    if pending:
        log(f"合并上轮待确认已发送标记: {len(pending)} 条")
        for key in pending:
            seen.setdefault(key, int(time.time()))
        if not dry:                                  # 合并成功才清标记, 否则下轮重试 (F-07)
            if save_json(STATE, prune(seen)):
                clear_pending()
            else:
                err("待确认标记合并落盘失败, 保留标记下轮重试")

    budget = BUDGET_S
    try:
        budget = max(15, int(conf.get("timeout_s") or BUDGET_S))
    except (TypeError, ValueError):
        budget = BUDGET_S
    round_start = time.monotonic()          # 本轮起点: 聚合软截止 + 逐次发送闸门共用 (Batch 6 M1/F-02)
    ROUND_START = round_start
    deadline = time.monotonic() + budget

    TS_BAD.clear()
    items, errors, outcomes = [], [], []
    # Batch 7: 研报源按源节流 (默认 30 分钟一轮); 跳过记 SRC_SKIP, 不算失败也不告警.
    # 失败源 5 分钟后重试, 避免「一坏就静默半小时」.
    ra: dict = seen.get("_research") if isinstance(seen.get("_research"), dict) else {}
    now_run = int(time.time())
    research_ran: list[tuple[str, bool]] = []
    total = len(SOURCES)
    for idx, (name, fn) in enumerate(SOURCES):
        if name in RESEARCH_SRCS:
            last_run = to_int(ra.get(name))
            if last_run > now_run + 60:       # B1 修复: 时钟回拨/NTP 前跳后校正 -> 视为未运行
                log(f"{name}: 研报节流时间戳在未来 ({last_run - now_run}s), 按未运行处理")
                last_run = 0
            if now_run - last_run < RESEARCH_EVERY_S:
                outcomes.append((name, SRC_SKIP))
                log(f"{name}: 跳过 (研报源 {RESEARCH_EVERY_S // 60} 分钟节流)")
                continue
        now = time.monotonic()
        left = deadline - now
        if left < 2:
            outcomes.append((name, SRC_SKIP))   # 跳过不是失败: 不写 errors (Batch 6 U4-F-05)
            log(f"{name}: 跳过, 全局预算 {budget}s 已耗尽")
            continue
        src_dl = slice_deadline(deadline, now, total - idx)   # 单源份额: 慢源吃不掉整轮 (6.1 U-B)
        t0 = time.monotonic()
        ok = False
        try:
            got = fn(src_dl)
            items.extend(got)
            outcomes.append((name, ""))
            ok = True
            bad = TS_BAD.get(name, 0)
            log(f"{name}: {len(got)} 条 ({time.monotonic() - t0:.1f}s)"
                + (f", 丢弃 {bad} 条时间不可解析" if bad else ""))
        except Exception as e:
            errors.append(name)
            outcomes.append((name, f"{type(e).__name__}: {str(e)[:90]}"))
            log(f"{name} FAILED: {type(e).__name__}: {str(e)[:90]} "
                f"({time.monotonic() - t0:.1f}s)")
        if name in RESEARCH_SRCS:
            research_ran.append((name, ok))

    if research_ran:                          # 节流时间戳随 state 落盘 (_ 前缀不会被 prune)
        ra = dict(ra)
        for name, ok in research_ran:
            ra[name] = now_run if ok else now_run - RESEARCH_EVERY_S + 300
        seen["_research"] = ra

    skipped_all = bool(outcomes) and all(e == SRC_SKIP for _, e in outcomes)
    if skipped_all:                           # 全轮被预算跳过: 既无成功也无失败 -> 不告警、不返回失败
        log(f"全部 {len(outcomes)} 源被预算跳过, 本轮不抓取也不告警")
        return 0

    if not items:
        skipped = sum(1 for _, _e in outcomes if _e == SRC_SKIP)
        if skipped:                           # 无条目但有预算跳过: 仍按全源故障告警, 文案补跳过计数 (U-A)
            log(f"本轮无可用条目: 失败 {len(errors)} 源 / 跳过 {skipped} 源: {errors}")
        else:
            log(f"全部源失败 {errors}")
        alert_all_failed(seen, errors, conf, tk, dry, skipped)
        if not dry and not save_json(STATE, prune(seen)):
            err("告警状态落盘失败")
        return 1

    if source_health(seen, outcomes, conf, tk, dry):
        if not dry and not save_json(STATE, prune(seen)):
            err("单源告警状态落盘失败 (下轮计数从旧值起算)")
    if note_recovered(seen, conf, tk, dry):
        if not dry and not save_json(STATE, prune(seen)):
            err("恢复状态落盘失败")

    now_ts = int(time.time())
    live, dropped_future = [], 0
    for it in items:
        if it["ts"] > now_ts + FUTURE_TOL:           # 未来时间戳丢弃, 不再改写成 now (F-05)
            dropped_future += 1
            continue
        live.append(it)
    if dropped_future:
        log(f"丢弃 {dropped_future} 条未来时间戳 (> +{FUTURE_TOL}s)")

    best: dict[str, dict] = {}
    dropped_short = 0
    for it in live:
        # Batch 7: 研报条目自带稳定 key (源站 ID), 其余源仍旧按正文算跨源签名
        key = it.get("key") or dedupe_key(it["text"])
        if not key:
            dropped_short += 1
            continue
        it["key"] = key
        cur = best.get(key)
        if cur is None or it["ts"] < cur["ts"]:      # 保留最早一条(原始发布源)
            best[key] = it
    merged_exact = sorted(best.values(), key=lambda x: (x["ts"], x["src"]))
    merged, near_pairs = merge_near(merged_exact)

    relevant, dropped = [], []
    for it in merged:
        # Batch 7: 研报条目豁免「品种锚点」硬门槛 (中金所/中期协标题是官方统计口径),
        # 但仍走栏目/超短/股票口径噪声检查; 非研报条目一字不改地走原 filter_reason。
        reason = (research_reason(it["text"]) if it.get("url")
                  else filter_reason(it["text"], keywords, use_filter))
        if reason:
            dropped.append((reason, it))
        else:
            relevant.append(it)

    seen_keys = {k for k in seen if not k.startswith("_")}
    if (seed or not seen_keys) and not dry:
        for it in relevant:
            seen[it["key"]] = now_ts
        if not save_json(STATE, prune(seen)):
            err("seed 落盘失败, 下轮会重复 seed")
        clear_pending()
        log(f"seeded {len(relevant)} items (跨源已合并), nothing pushed")
        return 0

    fresh = [it for it in relevant if it["key"] not in seen]
    max_age = int(conf.get("max_age_min", 180)) * 60
    stale = [it for it in fresh if now_ts - it["ts"] > max_age]
    fresh = [it for it in fresh if now_ts - it["ts"] <= max_age]

    # ---- v5 聚合: 本轮的 fresh 全部入队, 攒到窗口再合成一条发
    # (替掉 v4 的「有就推 + 超出 limit 直接丢」)
    queue, q_problem, q_bad = load_queue()
    if q_problem:                                     # 队列坏了: 先留证据再重建, 不静默清空
        badpath = QUEUE.with_name(f"{QUEUE.name}.bad-{int(time.time())}")
        try:
            QUEUE.replace(badpath)
            err(f"聚合队列不可读({q_problem}), 已备份为 {badpath.name} 后重建")
        except OSError as e:
            err(f"聚合队列不可读({q_problem}), 备份失败({e}), 已重建空队列")
        queue = []
    keep = [it for it in queue if it["key"] not in seen]   # seen 与队列分叉时自愈
    diverged = len(queue) - len(keep)
    if diverged:
        log(f"队列清理: 剔除 {diverged} 条已读残留")
        queue = keep
    # Batch 7: 只给「新进队列且还没有摘要」的研报提正文 (队列里已有的复用, 不重复下载);
    # 独立 8s 预算 + 串行, 排在抓取循环之后, 不挤占既有 14 源的 45s 预算。
    pdf_calls, pdf_q = extract_research(queue, fresh)
    if pdf_calls:
        log(f"研报正文: 解析 {pdf_calls} 篇 PDF, 队列补摘要 {pdf_q} 条")
    # 摘要还没取到的研报本轮先不入队: 宁可晚一轮, 也不把「只有标题没有正文」的研报推出去
    kept_fresh = [it for it in fresh
                  if not (it.get("url") and it.get("snippet") is None)]
    if len(kept_fresh) < len(fresh):
        log(f"研报摘要待补: {len(fresh) - len(kept_fresh)} 篇推迟到下一轮入队")
    fresh = kept_fresh
    added, dup_q = queue_upsert(queue, fresh)
    if added and not dry:                          # items 存档 (§3.11): 只记本轮真正新进队列的条目;
        items_archive_append(queue[-added:])       # dry-run 不落档 (干跑不改动任何状态文件)

    meta = digest_meta(seen)
    interval = digest_min * 60
    now_f = time.time()
    last_flush = to_float(meta.get("last_flush"))
    if not last_flush or last_flush > now_f + 60:     # 首轮 / 时钟回拨
        last_flush = now_f
        meta["last_flush"] = last_flush
    waited = now_f - last_flush
    next_try = to_float(meta.get("next_try"))
    fails_n = to_int(meta.get("fails"))
    day = day_counter(seen, datetime.now(TZ_CN).strftime("%Y-%m-%d"))
    cap_hit = to_int(day.get("msgs")) >= DAILY_MSG_CAP and not force_flush

    # 老化 (V1-P1-2 + V3-P1): 只有「本来能发」的轮次才清理; 被 cap/退避堵住的时长
    # block_acc>0 时暂停老化 -> 解冻后先补发而不是先判死 (它只当布尔门闩用)。
    oldest_age = (now_f - min((it["ts"] for it in queue), default=now_f)) if queue else 0
    # fails_n > 0 表示上一轮发失败: 退避到期必须重试, 不能等别的条件凑齐 (V1-P2-7)
    due = (bool(queue) and (force_flush
           or len(queue) >= digest_items
           or (waited >= interval and len(queue) >= digest_min_items)
           or oldest_age >= max_hold
           or (bool(meta.get("left_pending")) and waited >= 60)
           or fails_n > 0))
    if next_try > now_f and not force_flush:          # 发送失败后的退避
        due = False

    frozen = (next_try > now_f and not force_flush) or cap_hit
    last_round = to_float(meta.get("last_round"))
    rollback = last_round > now_f + 60               # 时钟回拨 (NTP 跳变/手改表)
    if rollback:
        last_round = 0.0
    gap = now_f - last_round if (last_round and now_f > last_round) else 0.0
    if rollback:
        gap = max(gap, 3600.0)                       # 按长间隔计 -> 关掉老化, 先补发
    meta["last_round"] = now_f
    # block_acc > 0 表示「上次排空后被堵过」; 它只当布尔门闩用, 累加秒数仅供诊断
    block_acc = to_float(meta.get("block_acc"))
    if frozen or gap >= 3600:                         # 退避/触顶/休眠长间隔都算阻塞 (V4-P1)
        block_acc = min(7 * 86400.0, block_acc + max(gap, 120.0))
    meta["block_acc"] = block_acc
    aging_on = block_acc <= 0                         # 有过 cap/退避阻塞 -> 暂停老化直到排空
    aged: list[dict] = []                             # 老化清理在「发送成功后」才做 (V3-P1)
    dropped_q: list[dict] = []
    if len(queue) > QUEUE_MAX:
        queue.sort(key=lambda x: (x["ts"], x["src"]))
        cut = len(queue) - QUEUE_MAX
        dropped_q = queue[:cut]
        queue = queue[cut:]
        stale.extend(dropped_q)                   # 记账, 免被源站反复拉回再丢
        log(f"队列溢出: 丢弃最旧 {cut} 条 (已按 stale 记账, 不再重推)")

    queue_dirty = bool(added or dropped_q or q_problem or diverged or q_bad or pdf_q)

    if stats:
        log(f"raw={len(items)} merged_exact={len(merged_exact)} merged={len(merged)} "
            f"near_merged={len(near_pairs)} relevant={len(relevant)} fresh={len(fresh)} "
            f"stale={len(stale)} queued={len(queue)}(+{added}/dup{dup_q}) "
            f"queue_drop={len(dropped_q)} due={due} cap={cap_hit} oldest={oldest_age / 60:.0f}m "
            f"next_try={max(0, next_try - now_f) / 60:.0f}m "
            f"dropped_ts={TS_BAD.get('total', 0)} dropped_future={dropped_future} "
            f"dropped_short={dropped_short} filter={'on' if use_filter else 'off'} "
            f"errors={errors or '-'}")
    per_src: dict[str, int] = {}
    shown = 0
    for reason, it in dropped:                    # 每源最多 2 行, 共 12 行
        src = it["src"]
        if per_src.get(src, 0) >= 2 or shown >= 12:
            continue
        per_src[src] = per_src.get(src, 0) + 1
        shown += 1
        log(f"  丢弃[{reason}] {src}: {it['text'][:42]}")
    if explain:
        print(f"--- 过滤样本 (共 {len(dropped)} 条) ---")
        for reason, it in dropped[:20]:
            print(f"  [{reason}] {it['src']} {it['text'][:80]}")
        for prev, dup, ratio in near_pairs[:20]:
            print(f"  [近邻合并 {ratio:.3f}] {dup['src']} {dup['text'][:46]} "
                  f"-> {prev['src']} {prev['text'][:46]}")

    if show_queue:
        for it in queue[:30]:
            print(f"  {datetime.fromtimestamp(it['ts'], TZ_CN).strftime('%m-%d %H:%M')} "
                  f"[{it['src']}] {it['text'][:60]}")
        last_flush_txt = (datetime.fromtimestamp(last_flush, TZ_CN).strftime("%m-%d %H:%M")
                          if last_flush > 0 else "无")
        print(f"--- 队列 {len(queue)} 条 / 上次发送 {last_flush_txt}"
              f"({waited / 60:.0f} 分钟前) / 窗口 {digest_min} 分钟 (最少 {digest_min_items} 条)"
              f" / 今日已发 {int(day.get('msgs') or 0)}/{DAILY_MSG_CAP} 条"
              f" / 最早一条 {oldest_age / 60:.0f} 分钟前 ---")
        return 0

    if dry:
        for it in queue[-15:]:
            print(f"  {datetime.fromtimestamp(it['ts'], TZ_CN).strftime('%H:%M')} "
                  f"[{it['src']}] {it['text'][:70]}")
        print(f"--- (dry-run) 队列 {len(queue)} 条, due={due}, "
              f"还差 {max(0, interval - waited) / 60:.0f} 分钟到窗口 ---")
        return 0

    if force_flush and not queue:                     # V1-P1-1: 空队列强制刷新不能算发送失败
        record_stale(seen, stale, max_age)
        persist_state(seen)
        print("队列为空, 无需发送")
        return 0

    if not due or cap_hit:
        why = ("队列空" if not queue else
               (f"今日通知已达上限 {DAILY_MSG_CAP}" if cap_hit else
                (f"退避中, {max(0, next_try - now_f) / 60:.0f} 分钟后重试"
                 if next_try > now_f else
                 (f"未到窗口 ({waited / 60:.0f}/{digest_min} 分钟, 队列 {len(queue)} 条)"
                  if waited < interval else
                  f"窗口已过, 但只有 {len(queue)} 条 < {digest_min_items} 条, "
                  f"最早一条 {oldest_age / 60:.0f}/{max_hold / 60:.0f} 分钟 (到点兜底发)"))))
        notice_ok = True
        if cap_hit and not cap_notified_already(day):
            if not send_time_ok():       # 逐次闸门: 触顶提示推迟, cap_notified/msgs 不动
                log("今日触顶提示推迟到下一轮 (本轮再发会顶到 120s 上界)")
            else:
                day["cap_notified"] = True
                day["msgs"] = int(day.get("msgs") or 0) + 1
                notice_ok = send(f"⚠️ 今日快讯已达 {DAILY_MSG_CAP} 条上限, 新条目暂缓推送, "
                                 f"明天 0 点恢复 (队列上限 {QUEUE_MAX} 条, 超出按最旧丢弃)", conf, tk)
                if not notice_ok:
                    day["cap_notified"] = False          # 没发出去就下轮再试
                    day["msgs"] = to_int(day.get("msgs")) - 1
                    meta["next_try"] = now_f + retry_gap(600.0, SEND_RETRY_AFTER)  # 别每 120s 打一次 (V1-P2-3a)
                    err(f"触顶提示发送失败, {int(meta['next_try'] - now_f)}s 后重试")
        if queue_dirty and not save_queue(queue):
            err("队列落盘失败, 下轮可能重复入队")
            queue_save_ok = False
        else:
            queue_save_ok = True
        record_stale(seen, stale, max_age)            # 有 stale 时内部会落盘
        if not stale or not notice_ok:                # P0-1: _digest/_day 必须每轮落盘
            if not persist_state(seen):
                err("state 落盘失败 (本轮未发送), 下轮计数从旧值起算")
        log(f"queued 队列 {len(queue)} 条 (+{added}), {why}, 本轮不发送")
        return 0 if queue_save_ok else 6

    # ---- 本轮软截止 (M1/F-02): 抓取 + 已发消息耗时过长 -> 聚合发送推迟到下一轮,
    # 队列原样保留 (本轮新增条目照常入队落盘), 不算发送失败. 与逐次闸门取严.
    if time.monotonic() - round_start > ROUND_SOFT_S or not send_time_ok():
        log("本轮耗时超软截止, 聚合发送推迟到下一轮")
        queue_save_ok = True
        if queue_dirty and not save_queue(queue):
            err("队列落盘失败, 下轮可能重复入队")
            queue_save_ok = False
        record_stale(seen, stale, max_age)
        if not stale:
            if not persist_state(seen):
                err("state 落盘失败 (软截止推迟), 下轮计数从旧值起算")
        return 0 if queue_save_ok else 6

    # ---- 到点: 按时间排序, 单轮最多发 digest_items 条 (避免一口气几十条撞限流)
    queue.sort(key=lambda x: (x["ts"], x["src"]))
    batch = queue[:digest_items]
    rest_after = queue[digest_items:]
    msgs = build_messages(batch)
    sent: list[dict] = []
    sent_msgs = 0
    failed = False
    for idx, (text, used) in enumerate(msgs, 1):
        if not send(text, conf, tk):
            failed = True
            err(f"第 {idx}/{len(msgs)} 批发送失败 (u16={u16(text)}, 原因: {SEND_ERR or '未知'}), "
                f"已送达 {len(sent)} 条")
            break
        sent.extend(used)
        sent_msgs = idx
    ok = sent_msgs > 0 and not failed
    sent_keys = {it["key"] for it in sent}
    left = rest_after + [it for it in batch if it["key"] not in sent_keys]
    cur_msgs = to_int(day.get("msgs")) + sent_msgs
    if sent and force_flush and cur_msgs > DAILY_MSG_CAP:
        err(f"--flush-now 越限推送: 今日 {cur_msgs}/{DAILY_MSG_CAP} 条")   # 人工覆盖留痕
    if sent:
        for it in sent:                               # 只有真正送达才标记已读
            seen[it["key"]] = now_ts
        day["msgs"] = int(day.get("msgs") or 0) + sent_msgs
    if ok and aging_on:                               # 老化只在「刚发成功且无阻塞史」时清理剩余
        aged = [it for it in left if now_ts - it["ts"] > max_age]
        if aged:
            akeys = {it["key"] for it in aged}
            left = [it for it in left if it["key"] not in akeys]
            stale.extend(aged)
    if sent and not failed and not left:
        meta["last_flush"] = now_f                    # 老化后才算真排空, 排空才推进窗口
    if not left:                                      # 队列排空 -> 阻塞补偿归零
        meta["block_acc"] = 0.0
    rest_count = len(left)                            # 老化清掉的不能算剩货 (V4-P2)
    meta["left_pending"] = bool(rest_count > 0 and ok)   # 部分/单轮封顶后继续补发
    if not ok:                                        # 失败退避: 120s 起翻倍, 封顶 30 分钟
        fails = to_int(meta.get("fails")) + 1
        meta["fails"] = fails
        meta["next_try"] = now_f + retry_gap(min(1800, 120 * (2 ** min(fails, 4))),
                                             SEND_RETRY_AFTER)
        err(f"第 {fails} 次发送失败, {int(meta['next_try'] - now_f)}s 后重试"
            + (" (采纳 Retry-After)" if SEND_RETRY_AFTER is not None else ""))
    else:
        meta["fails"] = 0
        meta.pop("next_try", None)

    queue_save_ok = True
    if not dry:
        state_ok = persist_state(seen)                # 先落 state, 再落队列
        if sent and not state_ok:
            keys = sorted(sent_keys | set(pending))
            wrote = write_pending(keys)
            err(f"state 落盘失败, 已写待确认标记{'成功' if wrote else '失败'}, "
                f"下轮不会重复推送同一批")
        elif sent:
            clear_pending()
        if (queue_dirty or sent or not ok) and not save_queue(left):
            err("队列落盘失败, 已发条目下轮可能重复推送")
            queue_save_ok = False
        if sent and not state_ok:
            return 5
    log(f"pushed {len(sent)}/{len(batch)} msgs={sent_msgs} ok={ok} left={len(left)} "
        f"day_msgs={int(day.get('msgs') or 0)}")
    if not dry:
        record_stale(seen, stale, max_age)            # F-08: 推送之后记账, 失败不阻断
    if not ok:
        if not queue_save_ok:
            err("注意: 本轮发送失败且队列落盘失败, 返回 2 (队列异常需人工检查)")
        return 2
    return 0 if queue_save_ok else 6


# ------------------------------------------------------------------ selftest
def selftest() -> int:
    """回归用例: F-01 去重签名 / F-10 大盘噪声 / F-11 日历 / F-16 漏报词表 / F-21 可单测."""
    fails: list[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        print(f"{'ok ' if cond else 'BAD'} {name}{(' -> ' + detail) if not cond else ''}")
        if not cond:
            fails.append(name)

    # ---- F-01: 签名必须同时保留实义标题与正文
    k1 = dedupe_key("【钢联数据快讯】螺纹钢库存环比减少12万吨")
    k2 = dedupe_key("【钢联数据快讯】热卷产量环比增加8万吨")
    check("F-01 栏目前缀不同正文签名不同", k1 != k2, f"{k1!r} vs {k2!r}")
    check("F-01 签名保留正文(库存)", "库存" in k1, k1)
    check("F-01 签名保留正文(产量)", "产量" in k2, k2)
    check("F-01 【快讯】X 与裸文本 X 同签名",
          dedupe_key("【快讯】沪铜夜盘上涨1.2%") == dedupe_key("沪铜夜盘上涨1.2%"),
          f"{dedupe_key('【快讯】沪铜夜盘上涨1.2%')!r} vs {dedupe_key('沪铜夜盘上涨1.2%')!r}")
    check("F-01 通用标题回退正文",
          dedupe_key("【下周提醒】非农就业数据") == "非农就业数据",
          dedupe_key("【下周提醒】非农就业数据"))
    check("F-01 源站前缀可剥离",
          dedupe_key("财联社10月24日电，美国驻阿联酋大使馆表示，可能会实施人员调整。")
          == dedupe_key("美国驻阿联酋大使馆表示，可能会实施人员调整。"))
    check("F-01 标题+正文=签名 (不同正文不同签名)",
          dedupe_key("【西藏巴青震区电力供应全面恢复】国网通报")
          != dedupe_key("【西藏巴青震区电力供应全面恢复】水利部通报"))
    check("F-01 正文已被标题覆盖时只留标题",
          dedupe_key("【美国驻阿联酋大使馆：可能调整人员】正文")
          == dedupe_key("美国驻阿联酋大使馆：可能调整人员"))

    # ---- F-05/F-02: 解析不依赖宿主 TZ; 缺年份/漂移格式一律判失败
    want = int(datetime(2026, 10, 24, 20, 0, tzinfo=TZ_CN).timestamp())
    check("F-05 无时区串按 Asia/Shanghai", parse_ts("2026-10-24 20:00:00") == want,
          str(parse_ts("2026-10-24 20:00:00")))
    check("F-05 坏时间返回 0", parse_ts("昨天 09:00") == 0)
    check("F-02 缺年份格式不伪造 1900", parse_ts("10/14 09:00") == 0,
          str(parse_ts("10/14 09:00")))
    check("F-02 跑飞时间戳判失败", parse_ts("2099-01-01 00:00:00") == 0,
          str(parse_ts("2099-01-01 00:00:00")))

    # ---- F-09: UTF-16 计长 + 超长批次必须拆成多条
    check("F-09 emoji 计 2 码元", u16("🚨") == 2, str(u16("🚨")))
    fake = [{"src": "财联社", "ts": int(time.time()), "text": "🚨" * 700} for _ in range(10)]
    chunks = build_messages(fake)
    check("F-09 10x700 emoji 拆成多批", len(chunks) > 1, f"msgs={len(chunks)}")
    check("F-09 每批 UTF-16 <= MSG_MAX", all(u16(t) <= MSG_MAX for t, _ in chunks),
          str([u16(t) for t, _ in chunks]))
    check("F-09 拆批不丢条目", sum(len(u) for _, u in chunks) == 10,
          str([len(u) for _, u in chunks]))

    # ---- F-10: 纯 A 股大盘新闻不得放行
    for text in ("沪指收盘上涨0.52%，两市成交额1.2万亿元",
                 "创业板指开盘走低，市场情绪谨慎",
                 "北向资金今日净买入32亿元",
                 "深成指涨0.3%，涨跌家数比2:1"):
        check(f"F-10 丢弃大盘: {text[:12]}", not is_relevant(text, DEFAULT_KEYWORDS), text)
    for text in ("中证1000指数期货主力合约涨1.2%",
                 "白银现货升水扩大",
                 "生猪出栏均价环比上涨",
                 "国内成品油价格按机制上调",
                 "螺纹钢库存连续三周下降"):
        check(f"F-10 保留期货: {text[:12]}", is_relevant(text, DEFAULT_KEYWORDS), text)

    # ---- F-11: 带日期前缀的日历必须丢弃, 加息快讯必须保留
    check("F-11 日期前缀日历丢弃",
          not is_relevant("2026年10月24日财经日历：今日重点关注美联储议息", DEFAULT_KEYWORDS))
    check("F-11 括号日历丢弃", not is_relevant("【财经日历】今日重要数据公布时间表", DEFAULT_KEYWORDS))
    check("F-11 加息快讯保留", is_relevant("美联储宣布加息25bp", DEFAULT_KEYWORDS))
    check("F-11 非日历宏观保留", is_relevant("美联储议息会议纪要公布", DEFAULT_KEYWORDS))

    # ---- F-16: 关键指标型快讯不得漏报
    for text in ("白银现货升水扩大", "农业农村部：能繁母猪存栏环比下降0.5%",
                 "国内成品油价格按机制上调", "铂钯价格上涨", "全国猪价连续三周回落",
                 "鸡苗价格环比上涨5%", "沪金夜盘涨1.2%，报782元/克",
                 "国常会：加大大宗商品保供稳价力度"):
        check(f"F-16 保留: {text[:12]}", is_relevant(text, DEFAULT_KEYWORDS), text)

    # ---- F-12: 短标题不丢 digest
    check("F-12 短标题合并 digest",
          merge_title_digest("沪铜上涨", "上海期货交易所沪铜主力合约夜盘上涨2.1%")
          == "沪铜上涨：上海期货交易所沪铜主力合约夜盘上涨2.1%",
          merge_title_digest("沪铜上涨", "上海期货交易所沪铜主力合约夜盘上涨2.1%"))
    check("F-12 长标题保持原样", merge_title_digest("沪铜主力合约夜盘上涨2.1%", "x")
          == "沪铜主力合约夜盘上涨2.1%")

    # ---- 旧回归点
    check("苹果公司不误伤", not hit("苹果公司发布iPhone 18", STRONG))
    check("苹果期货命中", hit("苹果期货主力合约涨2%", STRONG))
    check("PE 词边界", not hit("APPLE 发布会", ["PE", "PP"]))
    check("PE 命中", hit("PE 价格", ["PE", "PP"]))
    # ---- Batch 2 v2 (B2R1/R2/R3 评审后新增): 词表自洽 + 判定分支覆盖
    dup = set(STRONG) & set(ANCHOR)
    check("B2 词表无跨层重复(STRONG∩ANCHOR)",
          not dup, f"重复: {sorted(dup)[:6]}")
    dup2 = (set(STRONG) | set(ANCHOR)) & set(NOISE_HARD)
    check("B2 词表无跨层重复(锚点∩噪声)", not dup2, f"重复: {sorted(dup2)[:6]}")
    check("发送腿顺序 直连->代理->直连重试",
          [x[0] for x in send_plan({"proxy": "http://127.0.0.1:8080"})]
          == ["", "http://127.0.0.1:8080", ""]
          and [x[0] for x in send_plan({})] == ["", ""],
          str(send_plan({})))
    check("传输瞬时错误可重试 / HTTP 错误不重试",
          is_transient(urllib.error.URLError(ssl.SSLEOFError(8, "EOF")))
          and is_transient(urllib.error.URLError(OSError(65, "No route to host")))
          and is_transient(socket.gaierror(8, "nodename"))
          and not is_transient(urllib.error.HTTPError("u", 400, "Bad", None, None))
          and not is_transient(urllib.error.HTTPError("u", 429, "Many", None, None)))
    check("err() 写 stderr 时带时间戳",
          "[2026-" in _err_line())
    check("瞬时失败会换腿重试并最终成功", _send_retry_ok())
    check("源列表含新接入的见闻商品/SMM",
          {n for n, _ in SOURCES} >= {"见闻商品", "SMM"},
          f"实际: {[n for n, _ in SOURCES]}")
    check("源列表无重复源名", len({n for n, _ in SOURCES}) == len(SOURCES))
    check("http 命令行带 --compressed (SMM 强制 gzip, 缺了整源失败)",
          "--compressed" in inspect.getsource(http))
    check("SMM 空 listList 必须抛错, 不静默返回空",
          _smm_empty_raises())
    check("SMM 源内剔除行情异动预警",
          not smm_keep("上期所合约价格预警:2026-11-04 21:56:22 【行情异动】沪镍主连涨1.2%")
          and smm_keep("【SMM快讯】国内电解铜社会库存环比减少1.2万吨"))
    check("SMM 源内剔除无合约产业稿 (印尼煤/钼/有机硅)",
          not smm_keep("【SMM煤炭快讯】印尼2027年电力行业煤炭需求预计达1.7383亿吨")
          and not smm_keep("某有机硅大厂公开氨基硅烷偶联剂发明专利")
          and not smm_keep("欧亚氧化钼价格同步站上33.60美元"))
    check("B2 裸名跳过集合未被清空", BARE_ANCHOR_SKIP == {"苹果", "玻璃"},
          f"实际: {sorted(BARE_ANCHOR_SKIP)}")
    check("B2 裸名跳过仍拦长尾同形词",
          not is_relevant("苹果2027年将推出新款折叠屏手机", DEFAULT_KEYWORDS))
    no_alias = [p for p, al in PRODUCT_ALIASES.items() if not al or
                any(not a.strip() for a in al)]
    check("B2 每个品种都有非空别名", not no_alias, f"空别名: {no_alias[:6]}")
    # 可证伪: 用「别名大写 + 空格」再匹配一次, 能抓到别名小写化(WTI->wti)、
    # 正则注入、边界写坏; 单纯 a in STRONG 的自匹配是恒真的, 不算检查。
    miss_up = [a for al in PRODUCT_ALIASES.values() for a in al
               if not hit(a.upper() + " ", STRONG)]
    check("B2 别名大写形态仍可命中", not miss_up, f"未命中: {miss_up[:6]}")
    alias_noise = sorted({a for al in PRODUCT_ALIASES.values() for a in al
                          if a in set(NOISE_HARD)})
    check("B2 别名不与噪声层重叠", not alias_noise, f"冲突: {alias_noise[:6]}")
    skip_leak = sorted(p for p, al in PRODUCT_ALIASES.items()
                       if p in BARE_ANCHOR_SKIP and any(a == p for a in al))
    check("B2 裸名跳过品种不得把裸名当别名", not skip_leak, f"泄漏: {skip_leak[:6]}")
    check("B2 ACRONYMS 覆盖噪声层",
          "ETF" in ACRONYMS and "SPDR" in ACRONYMS, f"ETF={('ETF' in ACRONYMS)}")
    check("B2 ACRONYMS 忽略大小写",
          bool(first_hit("lme 铜库存下降", STRONG)) and bool(first_hit("pta 主力涨2%", STRONG)))
    check("B2 3GPP 不误命中 PP",
          filter_reason("高通全球首个符合3GPP最新定义的6G终端原型机将展出",
                        DEFAULT_KEYWORDS, True) != "")
    # 泛语境无品种 (最大新分支, 873 条里 574 条走这里)
    for text, want in (("某公司延长出口禁令至2027年1月底", "泛语境无品种:出口"),
                       ("国内期货夜盘收盘多数下跌", "泛语境无品种:期货"),
                       ("工信部发布钢铁行业产能置换办法", "泛语境无品种:产能"),
                       ("企业库存周转天数下降", "泛语境无品种:库存")):
        got = filter_reason(text, DEFAULT_KEYWORDS, True)
        check(f"B2 泛语境丢弃: {text[:12]}", got == want, f"got={got!r} want={want!r}")
    # 单个泛语境词不足以入选, 但任意一个非泛语境词即可 (R3: 原来顺序决定结论)
    check("B2 泛语境词单独不入选", not is_relevant("库存下降", DEFAULT_KEYWORDS))
    check("B2 自定义词任一命中即保留",
          is_relevant("红海航线中断，库存下降", ["库存", "红海"])
          and is_relevant("红海航线中断，库存下降", ["红海", "库存"]))
    check("B2 自定义词等于泛语境词时无效",
          not is_relevant("库存下降", ["库存"]))
    # 同形词锚点 (R2/R3 实测 53 条误放)
    for text in ("探访无锡惠山国产智算集群满载运行", "尹锡悦涉嫌违反公职选举法",
                 "麦肯锡报告人才短缺", "海亮铜箔实现量产交付",
                 "贝莱德在中国铝业H股的持股比例下降", "产业资本真金白银入场"):
        check(f"B2 同形词不误放: {text[:10]}", not is_relevant(text, DEFAULT_KEYWORDS), text)
    # 降级品种 + 别名补齐 (R1 硬误杀 116 条里的代表)
    for text in ("玻璃期货涨停", "白银期货涨停", "纽约期银日内涨超2%",
                 "对二甲苯连续主力合约日内跌4%", "燃油主力合约日内大跌6%",
                 "塑料连续主力合约日内涨2%", "NYMEX取暖油主力合约跌超3%",
                 "俄方延长针对生产商的柴油出口禁令", "吕梁离石主流煤矿复产",
                 "上海出口集装箱结算运价指数上涨4.8%", "欧佩克+将维持产量配额",
                 "伊朗称霍尔木兹海峡不能重新开放", "中钢协钢材库存处于四年高位",
                 "全球最大黄金ETF SPDR持仓较上日减少2.85吨"):
        check(f"B2 补词后保留: {text[:12]}", is_relevant(text, DEFAULT_KEYWORDS), text)
    # NOISE_SOFT 放在 ANCHOR 之后: 品种预测不被「评级/研报」一刀切
    check("B2 评级不误杀品种预测",
          is_relevant("标普预计2026年国际铜价平均14000美元/吨", DEFAULT_KEYWORDS))
    check("B2 无锚点时研报仍丢",
          not is_relevant("某券商研报：看好消费板块复苏", DEFAULT_KEYWORDS))
    # MACRO 在 NOISE_HARD 之后: 宏观+美股 不再泄漏
    check("B2 宏观+美股被否决",
          not is_relevant("美股本周直面中期选举非农和财报冲击", DEFAULT_KEYWORDS))
    check("B2 纯宏观保留", is_relevant("美联储宣布降息25个基点", DEFAULT_KEYWORDS))
    check("B2 非商品口径原因可读",
          filter_reason("国债期货开盘多数下跌", DEFAULT_KEYWORDS, True).startswith("非商品口径"))
    # ---- Batch 2 v2 第二轮: 四组正反例 (让 MACRO 门闩/裸名跳过/目标价/公司词黑名单有回归保护)
    for text in ("美联储宣布降息", "美联储连续第二次降息", "美联储维持利率不变",
                 "美联储加息50BP", "央行降准1个百分点"):
        check(f"B2 政策事件保留: {text[:12]}", is_relevant(text, DEFAULT_KEYWORDS), text)
    for text in ("美联储洛根：最终目标是让政策适度收紧",
                 "美联储12月加息的概率约为79%", "美联储穆萨莱姆：风险偏向通胀上行"):
        check(f"B2 官员表态丢弃: {text[:12]}", not is_relevant(text, DEFAULT_KEYWORDS), text)
    for text in ("玻璃现货价格", "浮法玻璃生产线冷修", "苹果主产区遭遇霜冻减产"):
        check(f"B2 裸名跳过仍保行业措辞: {text[:10]}", is_relevant(text, DEFAULT_KEYWORDS), text)
    for text in ("玻璃杯套餐销量增长", "玻璃纤维行业景气度上行",
                 "苹果CEO库克：关税退款影响毛利率"):
        check(f"B2 裸名跳过拦同形词: {text[:10]}", not is_relevant(text, DEFAULT_KEYWORDS), text)
    for text in ("高盛将铜目标价下调至9000美元", "花旗银行下调锂市场目标价"):
        check(f"B2 品种目标价保留: {text[:10]}", is_relevant(text, DEFAULT_KEYWORDS), text)
    check("B2 股票目标价丢弃",
          not is_relevant("老铺黄金目标价上调至1200港元", DEFAULT_KEYWORDS))
    check("B2 公司名+品种字仍丢",
          not is_relevant("韩国锌业第三季度营业利润同比下降48%", DEFAULT_KEYWORDS))
    check("B2 冶炼事故不被公司词黑名单杀",
          is_relevant("韩国锌业温山冶炼厂发生火灾", DEFAULT_KEYWORDS))
    check("F-21 is_relevant 为模块级可调用",
          callable(globals().get("is_relevant")) and is_relevant("螺纹钢库存下降", DEFAULT_KEYWORDS))

    # ---- v5 聚合队列 (F-30): 队列读写/去重/损坏识别/日计数
    import tempfile
    global QUEUE, STATE
    _q, _s = QUEUE, STATE
    try:
        with tempfile.TemporaryDirectory() as td:
            QUEUE = Path(td) / "q.json"
            STATE = Path(td) / "s.json"
            check("F-30 队列缺失=空且无错误", load_queue() == ([], "", 0), str(load_queue()))
            QUEUE.write_text('{"items":[{"src":"a","ts":1793800000,"text":"x","key":"k1"},'
                             '{"src":"b","ts":"1793800001","text":"y","key":"k2"}]}')
            got, prob, badn = load_queue()
            check("F-30 结构不符条目被跳过且计入脏",
                  len(got) == 1 and not prob and badn == 1, f"{len(got)} {prob} {badn}")
            QUEUE.write_text('{"items":[{"src":"a","ts":1,"text":"x","key":"k9"}]}')
            got, prob, badn = load_queue()
            check("F-31 全部条目异常时报 problem", got == [] and bool(prob) and badn == 1,
                  f"{prob} {badn}")
            check("F-31 去重键不产生下划线前缀",
                  dedupe_key("_沪铜夜盘主力合约上涨1.2%").startswith("沪铜"))
            QUEUE.write_text('{"items": [')
            got, prob, _ = load_queue()
            check("F-30 半截 JSON 报 problem", got == [] and bool(prob), prob)
            q0: list[dict] = []
            now_t = int(time.time()) - 60
            a1, d1 = queue_upsert(q0, [{"src": "a", "ts": now_t, "text": "x", "key": "k1"},
                                       {"src": "a", "ts": now_t, "text": "x", "key": "k1"}])
            a2, d2 = queue_upsert(q0, [{"src": "a", "ts": now_t, "text": "x", "key": "k1"}])
            check("F-30 入队按 key 去重", (a1, d1, a2, d2) == (1, 1, 0, 1), f"{a1},{d1},{a2},{d2}")
            check("F-30 队列落盘读回一致", save_queue(q0) and len(load_queue()[0]) == 1)
            st: dict = {}
            d_old = day_counter(st, "2026-11-04")
            d_old["msgs"] = 7
            d_new = day_counter(st, "2026-11-05")
            check("F-30 跨日计数重置", d_new["msgs"] == 0 and d_new["date"] == "2026-11-05"
                  and st["_day"] is d_new)
            check("F-30 prune 保留下划线元数据",
                  prune({"_day": {"date": "x"}, "k": 1}) == {"k": 1, "_day": {"date": "x"}})
            meta = digest_meta({})
            meta["last_flush"] = 1
            check("F-30 _digest 可持久化", digest_meta({}) is not meta)
            check("V1 元数据脏值不抛异常",
                  (to_int("abc"), to_int(None), to_float("soon")) == (0, 0, 0.0))
            bad_day: dict = {"_day": {"date": "2026-11-04", "msgs": "lots"}}
            check("V1 _day.msgs 脏值归零", day_counter(bad_day, "2026-11-04")["msgs"] == 0)
    finally:
        QUEUE, STATE = _q, _s

    # ---- U1: 429 Retry-After 精确退避 (整数秒 / HTTP-date / 夹取 / 回退)
    check("U1 Retry-After 整数秒", parse_retry_after("120") == 120.0,
          str(parse_retry_after("120")))
    check("U1 Retry-After 0 夹到下限", parse_retry_after("0") == RA_MIN,
          str(parse_retry_after("0")))
    check("U1 Retry-After 负数夹到下限", parse_retry_after("-5") == RA_MIN,
          str(parse_retry_after("-5")))
    check("U1 Retry-After 超大夹到上限", parse_retry_after("999999") == RA_MAX,
          str(parse_retry_after("999999")))
    check("U1 Retry-After 垃圾值回退指数退避",
          parse_retry_after("soon") is None and parse_retry_after("") is None
          and parse_retry_after(None) is None,
          f"{parse_retry_after('soon')} {parse_retry_after('')}")
    check("U1 已过去的 HTTP-date 夹到下限",
          parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == RA_MIN,
          str(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT")))
    _gmt = email.utils.format_datetime(datetime.now(timezone.utc) + timedelta(seconds=90),
                                       usegmt=True)
    _ra = parse_retry_after(_gmt)
    check("U1 GMT 日期按剩余秒数算", _ra is not None and 85 <= _ra <= 95, f"{_gmt} -> {_ra}")
    _ra, _calls, _ok429 = _send_429("120")
    check("U1 429 把建议值从 send 传出", _ra == 120.0, str(_ra))
    check("U1 429 不换腿重试同一 payload", _calls == 1 and _ok429 is False,
          f"calls={_calls} ok={_ok429}")
    check("U1 头缺失时不留建议值", _send_429(None)[0] is None, str(_send_429(None)[0]))
    check("U1 建议值优先于指数退避", retry_gap(240.0, 300.0) == 300.0,
          str(retry_gap(240.0, 300.0)))
    check("U1 无建议值时完全回退指数退避",
          retry_gap(240.0) == 240.0 and retry_gap(1800.0, None) == 1800.0,
          f"{retry_gap(240.0)} {retry_gap(1800.0, None)}")

    # ---- U1-F-01: 超长纯数字 Retry-After 不得抛异常 (309~4300 位曾 OverflowError)
    try:
        _big_ra = parse_retry_after("9" * 400)
        _big_ok = _big_ra is not None and RA_MIN <= _big_ra <= RA_MAX
    except Exception as _e:
        _big_ra, _big_ok = f"{type(_e).__name__}: {_e}", False
    check("U1 400 位整数不抛异常且夹取在界内", _big_ok, str(_big_ra))
    try:
        _huge_ra = parse_retry_after("9" * 4301)
        _huge_ok = _huge_ra is None or RA_MIN <= _huge_ra <= RA_MAX
    except Exception as _e:
        _huge_ra, _huge_ok = f"{type(_e).__name__}: {_e}", False
    check("U1 4301 位整数不抛异常 (None 或夹取)", _huge_ok, str(_huge_ra))

    # ---- U1 轻微: send() 入口必须清零上一轮的 SEND_RETRY_AFTER (变异: 删掉复位即红)
    _real_tg_reset = globals()["tg"]
    try:
        globals()["tg"] = lambda method, payload, tk, proxy, timeout=25: {}
        globals()["SEND_RETRY_AFTER"] = 999.0
        _reset_ok = send("reset-probe", {"chat_id": 1}, "tk")
        _reset_left = globals().get("SEND_RETRY_AFTER")
        check("U1 send() 入口清零 SEND_RETRY_AFTER",
              _reset_ok and _reset_left is None, f"ok={_reset_ok} left={_reset_left}")
    finally:
        globals()["tg"] = _real_tg_reset
        globals()["SEND_RETRY_AFTER"] = None

    # ---- U2-F-02: parse_mode=HTML 文案里的源名/错误摘要必须转义 (否则 400 降级吞掉摘要)
    _real_tg_esc = globals()["tg"]
    _payloads: list[dict] = []
    try:
        globals()["tg"] = (lambda method, payload, tk, proxy, timeout=25:
                           (_payloads.append(payload) or {}))
        _ue: dict = {}
        for _i in range(3):
            source_health(_ue, [("财联社<&", "URLError: <urlopen error timed out>"), ("东财", "")],
                          {"chat_id": 1, "src_alert_rounds": 3}, "tk", False)
        _utext = str((_payloads[-1] if _payloads else {}).get("text"))
        check("U2 告警错误摘要里的 < 已转义",
              len(_payloads) == 1 and "&lt;urlopen error timed out&gt;" in _utext,
              f"calls={len(_payloads)} text={_utext!r}")
        check("U2 告警源名里的 <& 已转义",
              "财联社&lt;&amp;" in _utext, _utext)
    finally:
        globals()["tg"] = _real_tg_esc

    # ---- U2: 单源连续失败告警 / 抑制 / 恢复 / 全源失败不重复 / 发送失败重试
    _real_send = globals()["send"]
    _sent: list[str] = []
    try:
        globals()["send"] = lambda text, conf, tk, depth=0: (_sent.append(text) or True)
        u2: dict = {}
        for _i in range(3):
            source_health(u2, [("财联社", f"RuntimeError: boom{_i}"), ("东财", "")],
                          {"src_alert_rounds": 3}, "tk", False)
        check("U2 连续 3 轮失败发 1 条告警",
              len(_sent) == 1 and "财联社" in _sent[0], f"{len(_sent)} {_sent}")
        check("U2 告警带最近一次错误摘要", "boom2" in _sent[0], str(_sent[:1]))
        _sent.clear()
        u2b: dict = {}
        for _i in range(2):
            source_health(u2b, [("见闻商品", "RuntimeError: x"), ("东财", "")],
                          {"src_alert_rounds": 3}, "tk", False)
        check("U2 未达阈值不发告警",
              not _sent and (u2b.get(SRC_FAIL_KEY) or {}).get("见闻商品", {}).get("rounds") == 2,
              f"{_sent} {u2b}")
        source_health(u2, [("财联社", "RuntimeError: boom3"), ("东财", "")],
                      {"src_alert_rounds": 3}, "tk", False)
        check("U2 抑制窗口内不重复告警", not _sent, str(_sent))
        check("U2 抑制期仍累计轮数",
              (u2.get(SRC_FAIL_KEY) or {}).get("财联社", {}).get("rounds") == 4,
              str(u2.get(SRC_FAIL_KEY)))
        source_health(u2, [("财联社", ""), ("东财", "")], {"src_alert_rounds": 3}, "tk", False)
        check("U2 恢复发 1 条并清零",
              len(_sent) == 1 and "已恢复" in _sent[0] and SRC_FAIL_KEY not in u2,
              f"{_sent} {u2}")
        _sent.clear()
        u3: dict = {}
        for _i in range(4):
            source_health(u3, [("财联社", "RuntimeError: x"), ("东财", "RuntimeError: y")],
                          {"src_alert_rounds": 3}, "tk", False)
        check("U2 全源失败不发单源告警", not _sent and SRC_FAIL_KEY not in u3, f"{_sent} {u3}")
        _sent.clear()
        globals()["send"] = lambda text, conf, tk, depth=0: False
        u4: dict = {}
        for _i in range(3):
            source_health(u4, [("财联社", "RuntimeError: z"), ("东财", "")],
                          {"src_alert_rounds": 3}, "tk", False)
        _ent = (u4.get(SRC_FAIL_KEY) or {}).get("财联社") or {}
        check("U2 告警发送失败不丢状态",
              _ent.get("rounds") == 3 and _ent.get("pend") is True
              and not _ent.get("last_sent"), str(_ent))
        globals()["send"] = lambda text, conf, tk, depth=0: (_sent.append(text) or True)
        source_health(u4, [("财联社", "RuntimeError: z4"), ("东财", "")],
                      {"src_alert_rounds": 3}, "tk", False)
        check("U2 发送失败后下轮重试告警",
              len(_sent) == 1 and "财联社" in _sent[0], str(_sent))
        _sent.clear()
        u5: dict = {}
        for _i in range(2):
            source_health(u5, [("SMM", "RuntimeError: q"), ("东财", "")],
                          {"src_alert_rounds": "2"}, "tk", False)
        check("U2 配置键覆盖阈值(2 轮)", len(_sent) == 1 and "SMM" in _sent[0], str(_sent))
        _sent.clear()
        u6: dict = {}
        for _i in range(3):
            source_health(u6, [("财联社", "RuntimeError: d"), ("东财", "")],
                          {"src_alert_rounds": 3}, "tk", True)
        check("U2 dry-run 只记账不发送",
              not _sent and (u6.get(SRC_FAIL_KEY) or {}).get("财联社", {}).get("rounds") == 3,
              f"{_sent} {u6}")
        check("U2 prune 保留单源失败状态",
              prune({"k": 1, SRC_FAIL_KEY: {"财联社": {"rounds": 3}}})
              == {"k": 1, SRC_FAIL_KEY: {"财联社": {"rounds": 3}}},
              str(prune({"k": 1, SRC_FAIL_KEY: {"财联社": {"rounds": 3}}})))
        check("U2 main 每轮调用 source_health",
              "source_health(seen, outcomes" in inspect.getsource(main))
    finally:
        globals()["send"] = _real_send

    # ---- U2-F-01/F-03/F-04 + CTO 裁决 D: 恢复不双发 / 失败重试保留轮数 / 同轮多源合并
    _real_send2 = globals()["send"]
    _sent2: list[str] = []
    _conf2 = {"chat_id": 1, "src_alert_rounds": 3}
    try:
        globals()["send"] = lambda text, conf, tk, depth=0: (_sent2.append(text) or True)
        # (a) 全源故障后恢复: 单源恢复静默, 只留 note_recovered 的 1 条
        _u7: dict = {}
        for _i in range(3):
            source_health(_u7, [("财联社", "RuntimeError: boom"), ("东财", "")],
                          _conf2, "tk", False)
        alert_all_failed(_u7, ["财联社"], _conf2, "tk", False)
        _sent2.clear()
        _u7_changed = source_health(_u7, [("财联社", ""), ("东财", "")], _conf2, "tk", False)
        _u7_note = note_recovered(_u7, _conf2, "tk", False)
        check("U2 全源恢复轮不双发 (单源恢复静默清块)",
              _u7_changed and _u7_note and len(_sent2) == 1
              and "已恢复" in _sent2[0] and "财联社" not in _sent2[0],
              f"calls={len(_sent2)} {_sent2}")

        # (b) 恢复通知发送失败: 保留真实轮数, 下轮重试成功且文案仍带真实轮数
        _sent2.clear()
        _u8: dict = {}
        for _i in range(3):
            source_health(_u8, [("财联社", "RuntimeError: x"), ("东财", "")], _conf2, "tk", False)
        _sent2.clear()
        globals()["send"] = lambda text, conf, tk, depth=0: False
        source_health(_u8, [("财联社", ""), ("东财", "")], _conf2, "tk", False)
        _ent8 = (_u8.get(SRC_FAIL_KEY) or {}).get("财联社") or {}
        check("U2 恢复发送失败保留真实轮数",
              _ent8.get("alerts") == 1 and _ent8.get("rounds") == 3, str(_ent8))
        globals()["send"] = lambda text, conf, tk, depth=0: (_sent2.append(text) or True)
        source_health(_u8, [("财联社", ""), ("东财", "")], _conf2, "tk", False)
        check("U2 恢复通知失败后下轮重试成功",
              len(_sent2) == 1 and "已恢复" in _sent2[0] and "连续失败 3 轮" in _sent2[0]
              and SRC_FAIL_KEY not in _u8, f"{_sent2} {_u8}")

        # (c) 抑制窗口到点后汇总告警带累计轮数
        _sent2.clear()
        _u9: dict = {}
        for _i in range(3):
            source_health(_u9, [("财联社", "RuntimeError: s"), ("东财", "")], _conf2, "tk", False)
        _sent2.clear()
        _e9 = (_u9.get(SRC_FAIL_KEY) or {}).get("财联社") or {}
        _e9["last_sent"] = int(_e9.get("last_sent") or 0) - SRC_ALERT_EVERY - 1
        source_health(_u9, [("财联社", "RuntimeError: s4"), ("东财", "")], _conf2, "tk", False)
        check("U2 抑制窗口到点后汇总带累计轮数",
              len(_sent2) == 1 and "累计 4 轮" in _sent2[0] and "s4" in _sent2[0],
              str(_sent2))

        # (d) 同轮多个源同时 due (另有源成功, 避免整轮走全源失败分支) -> 只发 1 条
        _sent2.clear()
        _u10: dict = {}
        for _i in range(3):
            source_health(_u10, [("财联社", "RuntimeError: a"), ("东财", "RuntimeError: b"),
                                 ("同花顺", "")],
                          _conf2, "tk", False)
        _e10 = _u10.get(SRC_FAIL_KEY) or {}
        check("U2 同轮两源 due 合并为 1 条",
              len(_sent2) == 1 and "财联社" in _sent2[0] and "东财" in _sent2[0]
              and all((_e10.get(n) or {}).get("alerts") == 1
                      and not (_e10.get(n) or {}).get("pend")
                      and (_e10.get(n) or {}).get("last_sent") for n in ("财联社", "东财")),
              f"calls={len(_sent2)} {_sent2} {_e10}")
    finally:
        globals()["send"] = _real_send2

    # ---- U3: err 文件轮转 (原地 ftruncate, 阈值/保留与 .out 一致)
    with tempfile.TemporaryDirectory() as _td:
        _ep = Path(_td) / "err.log"
        _ep.write_bytes(b"")
        _fh = open(_ep, "ab", buffering=0)              # 模拟 launchd 的 O_APPEND fd
        try:
            _fd = _fh.fileno()
            _fh.write(b"x" * LOG_MAX)                   # 恰好等于阈值: 不轮转
            _rot_eq = rotate_file(_fd, _ep)
            _size_eq = _ep.stat().st_size
            _fh.write(b"y" * 100 + b"TAILMARK")
            _rot = rotate_file(_fd, _ep)
            _size_after = _ep.stat().st_size
            _tail = _ep.read_bytes()[-8:]
        finally:
            _fh.close()
        check("U3 恰好等于阈值不轮转", _rot_eq is False and _size_eq == LOG_MAX,
              f"{_rot_eq} {_size_eq}")
        check("U3 超阈值轮转到一半", _rot is True and _size_after == LOG_MAX // 2,
              f"{_rot} {_size_after}")
        check("U3 轮转保留最近内容", _tail == b"TAILMARK", str(_tail))
        _empty = Path(_td) / "empty.log"
        _empty.write_bytes(b"")
        _fh = open(_empty, "ab", buffering=0)
        try:
            check("U3 0 字节不轮转",
                  rotate_file(_fh.fileno(), _empty) is False and _empty.stat().st_size == 0)
        finally:
            _fh.close()
        _gone = Path(_td) / "gone.log"
        _gone.write_bytes(b"")
        _fh = open(_gone, "ab", buffering=0)
        try:
            _fh.write(b"z" * (LOG_MAX + 10))
            _gone.unlink()                              # 文件被外部删除: 不抛异常也不重建
            check("U3 文件被删除时静默跳过",
                  rotate_file(_fh.fileno(), _gone) is False and not _gone.exists())
        finally:
            _fh.close()
        _dn = os.open(os.devnull, os.O_WRONLY)
        try:
            check("U3 fd 指向 /dev/null 时静默跳过",
                  rotate_file(_dn, Path(os.devnull)) is False)
        finally:
            os.close(_dn)
        _other = Path(_td) / "other.log"
        _other.write_bytes(b"OTHER" * 100)              # 500 B, 远小于阈值
        _mism = Path(_td) / "mism.log"
        _mism.write_bytes(b"m" * (LOG_MAX + 64))        # 超阈值但不是 path 指向的 inode
        _fh = open(_mism, "ab", buffering=0)
        try:
            check("U3 路径与 fd inode 不一致时不轮转也不截断",
                  rotate_file(_fh.fileno(), _other) is False
                  and _other.stat().st_size == 500
                  and _mism.stat().st_size == LOG_MAX + 64,
                  f"{_other.stat().st_size} {_mism.stat().st_size}")
        finally:
            _fh.close()
        check("U3 ERRLOG 指向 launchd 的 StandardErrorPath",
              ERRLOG == Path("/tmp/futures-news.err"))
        check("U3 main 每轮调用 rotate_stderr", "rotate_stderr()" in inspect.getsource(main))

    # ---- U4: 残缺标签/属性残留清洗 (不碰正文里的 < > 与普通单词)
    check("U4 残缺 span 标签被清除",
          clean_text("<span style=color:#ff0000 沪铜夜盘上涨1.2%") == "沪铜夜盘上涨1.2%",
          clean_text("<span style=color:#ff0000 沪铜夜盘上涨1.2%"))
    check("U4 残缺标签不吞后面的正文",
          clean_text("<span style=color:rgb(255,0,0)沪铜现货报价") == "沪铜现货报价",
          clean_text("<span style=color:rgb(255,0,0)沪铜现货报价"))
    check("U4 裸露属性(带引号/分号)被清除",
          clean_text('沪铜报价 style="color:red;font-size:12px" 上涨') == "沪铜报价 上涨",
          clean_text('沪铜报价 style="color:red;font-size:12px" 上涨'))
    check("U4 粘在标签名后的裸属性被清除",
          clean_text("spanstyle=color:rgb(255,0,0)沪铜现货报价") == "沪铜现货报价",
          clean_text("spanstyle=color:rgb(255,0,0)沪铜现货报价"))
    check("U4 比较式里的 < > 不误删",
          clean_text("涨幅>5%，5%<10%，a<b") == "涨幅>5%，5%<10%，a<b",
          clean_text("涨幅>5%，5%<10%，a<b"))
    check("U4 普通单词 style 不误删",
          clean_text("该品种 style 风格偏保守") == "该品种 style 风格偏保守",
          clean_text("该品种 style 风格偏保守"))
    check("U4 实体编码的标签也能清除",
          clean_text("&lt;span style=color:#ff0000&gt;沪铜上涨") == "沪铜上涨",
          clean_text("&lt;span style=color:#ff0000&gt;沪铜上涨"))
    check("U4 完整标签仍按原逻辑剥离",
          clean_text("<b>沪铜夜盘上涨1.2%</b>") == "沪铜夜盘上涨1.2%",
          clean_text("<b>沪铜夜盘上涨1.2%</b>"))
    check("U4 闭合残片无 '>' 时被清除",
          clean_text("沪铜</span") == "沪铜", clean_text("沪铜</span"))
    _url_keep = "详见 https://x.com/?b=1&c=2"
    check("U4 URL 查询参数原样保留",
          clean_text(_url_keep) == _url_keep, clean_text(_url_keep))
    _idem = ["<span style=color:#ff0000 沪铜夜盘上涨1.2%",
             '沪铜报价 style="color:red;font-size:12px" 上涨']
    check("U4 clean_text 幂等 (2 个样本)",
          all(clean_text(clean_text(x)) == clean_text(x) for x in _idem),
          str([(clean_text(x), clean_text(clean_text(x))) for x in _idem]))

    # ---- Batch 6 U1: 英文 RSS 直连源 (全离线: 内联样本 + monkeypatch http)
    _b6_rss = ('<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel>'
               '<title>t</title>'
               '<item><title>Copper rally lifts miners</title>'
               '<description>&lt;p&gt;Copper prices rose 2% after a mine halt.&lt;/p&gt;'
               '&lt;p&gt;The post Copper rally lifts miners appeared first on MINING.COM.&lt;/p&gt;'
               '</description><pubDate>Thu, 05 Nov 2026 14:23:00 +0000</pubDate></item>'
               '<item><title>Gold eases</title><description></description>'
               '<pubDate>Thu, 05 Nov 2026 13:00:00 +0000</pubDate></item>'
               '<item><title>Bad clock</title><description>x</description>'
               '<pubDate>yesterday</pubDate></item>'
               '</channel></rss>')
    _b6_http0 = globals()["http"]
    TS_BAD.clear()
    try:
        globals()["http"] = lambda *a, **k: _b6_rss.encode()
        _b6_got = src_mining(None)
    finally:
        globals()["http"] = _b6_http0
    check("B6 U1 RSS 解析出 2 条 (坏时间那条被丢弃)", len(_b6_got) == 2, str(_b6_got))
    check("B6 U1 pubDate 按 RFC-822 带时区解析",
          bool(_b6_got) and _b6_got[0]["ts"] == parse_ts("Thu Nov 05 14:23:00 +0000 2026"),
          str([g["ts"] for g in _b6_got]))
    check("B6 U1 文本=标题+摘要, 去掉 HTML 与源站署名尾巴",
          bool(_b6_got) and _b6_got[0]["text"].startswith("Copper rally lifts miners")
          and "<" not in _b6_got[0]["text"]
          and "appeared first on" not in _b6_got[0]["text"],
          _b6_got[0]["text"] if _b6_got else "")
    check("B6 U1 摘要缺失只用标题", len(_b6_got) > 1 and _b6_got[1]["text"] == "Gold eases",
          _b6_got[1]["text"] if len(_b6_got) > 1 else "")
    check("B6 U1 时间不可解析计入 TS_BAD 且不改用 now",
          TS_BAD.get("Mining") == 1 and all(g["ts"] for g in _b6_got), str(TS_BAD))
    check("B6 U1 文本长度不超过 ITEM_MAX",
          all(len(g["text"]) <= ITEM_MAX for g in _b6_got))
    try:
        globals()["http"] = lambda *a, **k: b'<?xml version="1.0"?><rss><channel></channel></rss>'
        try:
            src_oilprice(None)
            _b6_empty = False
        except RuntimeError:
            _b6_empty = True
    finally:
        globals()["http"] = _b6_http0
    check("B6 U1 空 feed 抛 RuntimeError (空结果=失败)", _b6_empty)
    _b6_trip = _rss_triples(b'<rss><channel><item><title>A & B</title>'
                            b'<description>d</description>'
                            b'<pubDate>Thu, 05 Nov 2026 14:00:00 +0000</pubDate>'
                            b'</item></channel></rss>')
    check("B6 U1 非法 XML 退化为正则解析",
          len(_b6_trip) == 1 and _b6_trip[0][0] == "A & B", str(_b6_trip))
    _b6_multi = _rss_triples(b'<rss><channel><item><title>T</title>'
                             b'<description><p>para one</p><p>para two</p></description>'
                             b'<pubDate>Thu, 05 Nov 2026 14:00:00 +0000</pubDate>'
                             b'</item></channel></rss>')
    _b6_multi_text = feed_text(_b6_multi[0][0], _b6_multi[0][1]) if _b6_multi else ""
    check("B6 U1 多段 description 全部拼回正文 (U1-F-02)",
          len(_b6_multi) == 1 and _b6_multi_text == "T：para one para two",
          f"{_b6_multi} -> {_b6_multi_text!r}")
    _b6_iso = [pub_ts("2026-11-05T15:18:28Z"),
               pub_ts("2026-11-05T15:18:28+00:00"),
               pub_ts("2026-11-05T23:18:28+08:00")]
    check("B6 U1 pub_ts 支持 ISO-8601 (Z / +00:00 / +08:00, U1-F-05)",
          _b6_iso == [1793891908, 1793891908, 1793891908], str(_b6_iso))
    check("B6 U1 MiningTech UA 说明客观且保留 Feed UA 兜底 (U1-F-06)",
          "403" not in (src_miningtech.__doc__ or "")
          and "FEED_UA" in (src_miningtech.__doc__ or ""),
          str(src_miningtech.__doc__))
    _b6_en = {"Mining", "OilPrice", "MiningTech", "Nasdaq", "CNBC", "MarketWatch",
              "Investing", "Hellenic"}
    check("B6 U1/U2 8 个英文源函数齐备",
          all(callable(globals().get("src_" + n.lower())) for n in _b6_en),
          str([n for n in sorted(_b6_en) if not callable(globals().get("src_" + n.lower()))]))
    _b6_direct = {"Mining", "OilPrice", "MiningTech", "Nasdaq", "CNBC", "MarketWatch"}
    check("B6 U1 6 个直连英文源源名齐全 (CTO 修订: MarketWatch)",
          all(callable(globals().get("src_" + n.lower())) for n in _b6_direct)
          and _b6_direct <= {n for n, _ in SOURCES},
          str(sorted(_b6_direct - {n for n, _ in SOURCES})))
    check("B6 U1 中文 6 源仍居 SOURCES 最前且顺序不变",
          [n for n, _ in SOURCES][:6] == ["财联社", "东财", "同花顺", "新浪", "见闻商品", "SMM"],
          str([n for n, _ in SOURCES][:6]))
    check("B6 U1 SOURCES 含全部 8 个英文源",
          {n for n, _ in SOURCES} >= _b6_en, str([n for n, _ in SOURCES]))

    # ---- Batch 6 U2: 代理源 + curl 命令行 (全离线: 打桩 subprocess.run 抓 argv)
    _b6_realrun = subprocess.run
    _b6_argv: list = []

    class _B6R:
        def __init__(self, out):
            self.stdout = out

    try:
        subprocess.run = lambda cmd, **kw: (_b6_argv.append(list(cmd)) or _B6R(b"ok\n200"))
        _b6_body = http("https://x/y")
    finally:
        subprocess.run = _b6_realrun
    check("B6 U2 无代理时 curl 命令行与旧版逐元素一致",
          _b6_body == b"ok" and _b6_argv == [["curl", "-sS", "--noproxy", "*",
                                             "--compressed", "--max-time", "12",
                                             "-A", UA, "-w", "\n%{http_code}",
                                             "https://x/y"]],
          str(_b6_argv[:1]))
    _b6_argv.clear()
    try:
        subprocess.run = lambda cmd, **kw: (_b6_argv.append(list(cmd)) or _B6R(b"ok\n200"))
        http("https://x/y", proxy="http://127.0.0.1:8080", cap=8,
             ua="probe-agent/1.0")
    finally:
        subprocess.run = _b6_realrun
    _b6_px = _b6_argv[0] if _b6_argv else []
    check("B6 U2 有代理时用 --proxy 且不再出现 --noproxy",
          "--proxy" in _b6_px and "--noproxy" not in _b6_px
          and _b6_px[_b6_px.index("--proxy") + 1] == "http://127.0.0.1:8080"
          and _b6_px[_b6_px.index("--max-time") + 1] == "8"
          and _b6_px[_b6_px.index("-A") + 1] == "probe-agent/1.0",
          str(_b6_px))
    _b6_kwargs: list = []
    try:
        subprocess.run = lambda cmd, **kw: (_b6_kwargs.append(dict(kw)) or _B6R(b"ok\n200"))
        http("https://x/y")
        http("https://x/y", proxy="http://127.0.0.1:8080")
    finally:
        subprocess.run = _b6_realrun
    check("B6 U2 代理分支清空 no_proxy/NO_PROXY, 直连分支不传 env (M4)",
          len(_b6_kwargs) == 2 and "env" not in _b6_kwargs[0]
          and _b6_kwargs[1].get("env", {}).get("no_proxy") == ""
          and _b6_kwargs[1].get("env", {}).get("NO_PROXY") == "",
          str([sorted(k) for k in _b6_kwargs]))
    check("B6 U2 http 新参数默认值即旧行为",
          inspect.signature(http).parameters["proxy"].default is None
          and inspect.signature(http).parameters["cap"].default == 12
          and inspect.signature(http).parameters["ua"].default is None,
          str(inspect.signature(http)))
    check("B6 U2 --compressed 仍在 http() 源码内 (SMM 强制 gzip 的既有断言)",
          "--compressed" in inspect.getsource(http))
    _b6_kw: dict = {}
    _b6_proxy0, _b6_cfg0 = PROXY, dict(CFG)
    try:
        globals()["PROXY"] = ""
        CFG.clear()
        CFG["proxy"] = "http://127.0.0.1:8080"
        globals()["http"] = lambda *a, **k: (_b6_kw.update(k) or _b6_rss.encode())
        _b6_inv = src_investing(None)
    finally:
        globals()["http"] = _b6_http0
        globals()["PROXY"] = _b6_proxy0
        CFG.clear()
        CFG.update(_b6_cfg0)
    check("B6 U2 代理源把 conf.proxy 交给 http 且用更小的 tries/cap",
          _b6_kw.get("proxy") == "http://127.0.0.1:8080"
          and _b6_kw.get("tries") == FEED_HTTP["tries"] == 2
          and _b6_kw.get("cap") == FEED_HTTP["cap"] == 8,
          str(_b6_kw))
    check("B6 U2 代理源的源名与条数正确",
          bool(_b6_inv) and _b6_inv[0]["src"] == "Investing", str(_b6_inv[:1]))
    _b6_nopx = ""
    try:
        globals()["PROXY"] = ""
        CFG.clear()
        try:
            src_hellenic(None)
        except RuntimeError as _b6_e:
            _b6_nopx = str(_b6_e)
    finally:
        globals()["PROXY"] = _b6_proxy0
        CFG.clear()
        CFG.update(_b6_cfg0)
    check("B6 U2 未配置代理时明确失败 (消息含「代理」)",
          "代理" in _b6_nopx, _b6_nopx)
    _b6_fp = ""
    _b6_fp_kw: dict = {}
    try:
        globals()["PROXY"] = "http://127.0.0.1:8080"
        CFG.clear()
        _b6_fp = feed_proxy()
        globals()["http"] = lambda *a, **k: (_b6_fp_kw.update(k) or _b6_rss.encode())
        try:
            src_investing(None)
        except RuntimeError:
            pass
    finally:
        globals()["http"] = _b6_http0
        globals()["PROXY"] = _b6_proxy0
        CFG.clear()
        CFG.update(_b6_cfg0)
    check("B6 U2 feed_proxy 在 CFG 空时回退环境 PROXY 并交给 http (U2-F-04a)",
          _b6_fp == "http://127.0.0.1:8080"
          and _b6_fp_kw.get("proxy") == "http://127.0.0.1:8080",
          f"{_b6_fp!r} {_b6_fp_kw}")
    _b6_calls: list = []

    def _b6_boom(url, **kw):
        _b6_calls.append(url)
        raise RuntimeError("connect refused")

    try:
        CFG.clear()
        CFG["proxy"] = "http://127.0.0.1:8080"
        globals()["http"] = _b6_boom
        try:
            src_hellenic(None)
        except RuntimeError:
            pass
    finally:
        globals()["http"] = _b6_http0
        CFG.clear()
        CFG.update(_b6_cfg0)
    check("B6 U2 代理失败不回退直连 (只尝试 1 次)", len(_b6_calls) == 1, str(_b6_calls))

    # ---- Batch 6 U3: 英文词表层 (全离线)
    check("B6 U3 英文词表小写且三层互不相交",
          EN_STRONG == [w.lower() for w in EN_STRONG]
          and EN_CONTEXT == [w.lower() for w in EN_CONTEXT]
          and EN_NOISE == [w.lower() for w in EN_NOISE]
          and not (set(EN_STRONG) & set(EN_CONTEXT))
          and not (set(EN_STRONG) & set(EN_NOISE))
          and not (set(EN_CONTEXT) & set(EN_NOISE)),
          str([sorted(set(EN_STRONG) & set(EN_CONTEXT))[:4],
               sorted(set(EN_STRONG) & set(EN_NOISE))[:4]]))
    check("B6 U3 词边界: gold 不吞 golden/B2Gold, lead 不吞 leader",
          bool(EN_STRONG_RE.search("Gold prices rise"))
          and not EN_STRONG_RE.search("Golden Agri rises")
          and not EN_STRONG_RE.search("B2Gold reports")
          and bool(EN_STRONG_RE.search("Lead prices fall"))
          and not EN_STRONG_RE.search("Leaders meet in Brussels"),
          str(EN_STRONG_RE.search("Golden Agri rises")))
    _b6_hyph = ["Iron-ore prices surge", "Natural-gas demand climbs",
                "Rare-earth exports fall", "Hot-rolled-coil output rises",
                "Baltic-Dry-Index hits a high"]
    check("B6 U3 多词强词兼容连字符写法 (5 形态, U3-F-01)",
          all(filter_reason(x, DEFAULT_KEYWORDS, True) == "" for x in _b6_hyph),
          str([(x, filter_reason(x, DEFAULT_KEYWORDS, True)) for x in _b6_hyph]))
    check("B6 U3 有 EN_STRONG 即保留",
          filter_reason("Copper prices fall as mine strike enters week two",
                        DEFAULT_KEYWORDS, True) == "",
          filter_reason("Copper prices fall as mine strike enters week two",
                        DEFAULT_KEYWORDS, True))
    check("B6 U3 无 STRONG 有 EN_NOISE 丢弃且原因可读",
          filter_reason("Earnings beat estimates as demand cools",
                        DEFAULT_KEYWORDS, True) == "英文噪声:earnings",
          filter_reason("Earnings beat estimates as demand cools",
                        DEFAULT_KEYWORDS, True))
    check("B6 U3 只有 EN_CONTEXT 时丢弃 (泛语境无品种)",
          filter_reason("Freight rates climb on seasonal demand",
                        DEFAULT_KEYWORDS, True) == "泛语境无品种:freight",
          filter_reason("Freight rates climb on seasonal demand",
                        DEFAULT_KEYWORDS, True))
    check("B6 U3 三层皆不命中时交回中文管线 (不新增静默放行)",
          filter_reason("Iran braces for Trump's next move after vote",
                        DEFAULT_KEYWORDS, True) == "无期货相关词",
          filter_reason("Iran braces for Trump's next move after vote",
                        DEFAULT_KEYWORDS, True))
    check("B6 U3 含汉字即不进英文分支",
          not ascii_dominant("COMEX黄金库存统计")
          and not ascii_dominant("沪指收盘上涨0.52%，两市成交额1.2万亿元")
          and ascii_dominant("Copper prices fall"))
    check("B6 U3 中文判定逐字不变",
          filter_reason("沪指收盘上涨0.52%，两市成交额1.2万亿元",
                        DEFAULT_KEYWORDS, True) == "噪声:沪指"
          and filter_reason("苹果公司发布iPhone 18",
                            DEFAULT_KEYWORDS, True) == "噪声:苹果公司"
          and filter_reason("螺纹钢库存连续三周下降", DEFAULT_KEYWORDS, True) == "",
          filter_reason("沪指收盘上涨0.52%，两市成交额1.2万亿元",
                        DEFAULT_KEYWORDS, True))
    check("B6 U3 EN_* 未并入 DEFAULT_KEYWORDS",
          not (set(EN_STRONG) & set(DEFAULT_KEYWORDS))
          and not (set(EN_NOISE) & set(DEFAULT_KEYWORDS)),
          str(sorted(set(EN_NOISE) & set(DEFAULT_KEYWORDS))[:4]))

    # ---- Batch 6 U4: 预算与失败语义 (全离线)
    _b6_send0 = globals()["send"]
    _b6_sent: list[str] = []
    _b6_conf = {"chat_id": 1, "src_alert_rounds": 3}
    try:
        globals()["send"] = lambda text, conf, tk, depth=0: (_b6_sent.append(text) or True)
        _b6_u1: dict = {}
        for _ in range(3):
            source_health(_b6_u1, [("Mining", SRC_SKIP), ("东财", "")],
                          _b6_conf, "tk", False)
        check("B6 U4 预算跳过不计失败轮数、不告警",
              SRC_FAIL_KEY not in _b6_u1 and not _b6_sent, f"{_b6_u1} {_b6_sent}")
        _b6_u2: dict = {}
        for _ in range(2):
            source_health(_b6_u2, [("Mining", "RuntimeError: boom"), ("东财", "")],
                          _b6_conf, "tk", False)
        _b6_pre = (src_fail_state(_b6_u2).get("Mining") or {}).get("rounds")
        source_health(_b6_u2, [("Mining", SRC_SKIP), ("东财", "")], _b6_conf, "tk", False)
        _b6_post = (src_fail_state(_b6_u2).get("Mining") or {}).get("rounds")
        check("B6 U4 跳过不清零也不累加既有失败计数",
              _b6_pre == 2 and _b6_post == 2, f"{_b6_pre} -> {_b6_post}")
        _b6_sent.clear()
        _b6_u3: dict = {}
        source_health(_b6_u3, [("Mining", SRC_SKIP), ("东财", SRC_SKIP)],
                      _b6_conf, "tk", False)
        check("B6 U4 全轮跳过不算全源失败、不误报恢复",
              SRC_FAIL_KEY not in _b6_u3 and not _b6_sent, f"{_b6_u3} {_b6_sent}")
        # (M2/E11) 跳过与失败同轮出现: 哨兵必须从 all_failed 中排除, 否则失败源不计数
        _b6_sent.clear()
        _b6_u4: dict = {"seedmarker": 1,
                        SRC_FAIL_KEY: {"A": {"rounds": 1, "since": 1, "last_err": "old"}}}
        source_health(_b6_u4, [("A", "RuntimeError: boom"), ("B", SRC_SKIP)],
                      _b6_conf, "tk", False)
        _b6_e11 = (src_fail_state(_b6_u4).get("A") or {}).get("rounds")
        check("B6 U4 跳过+其余失败: 失败源照常计数 (all_failed 排除哨兵)",
              _b6_e11 == 2 and not _b6_sent, f"rounds={_b6_e11} sends={_b6_sent}")
        # (M2) 跳过且已有告警计数: 不得误发恢复, 计数原样保留
        _b6_sent.clear()
        _b6_u5: dict = {SRC_FAIL_KEY: {"Mining": {"rounds": 5, "since": 1, "last_err": "x",
                                                  "alerts": 2, "last_sent": 1}}}
        source_health(_b6_u5, [("Mining", SRC_SKIP), ("东财", "")],
                      _b6_conf, "tk", False)
        _b6_m5 = (src_fail_state(_b6_u5).get("Mining") or {})
        check("B6 U4 跳过且 alerts>0 不发恢复、状态保留",
              not _b6_sent and _b6_m5.get("rounds") == 5 and _b6_m5.get("alerts") == 2,
              f"{_b6_sent} {_b6_m5}")
    finally:
        globals()["send"] = _b6_send0

    # 断言 4: 预算上界必须由 send_plan() 实时推导 (不写死 54/6)
    _b6_plan = send_plan({"proxy": "http://127.0.0.1:8080"})
    _b6_send_worst = sum(t for _, t in _b6_plan) + 3 * (len(_b6_plan) - 1)
    check("B6 U4 BUDGET_S=45 且抓取+一次发送最坏 <=120 (由 send_plan 推导)",
          BUDGET_S == 45 and BUDGET_S + _b6_send_worst <= 120,
          f"{BUDGET_S}+{_b6_send_worst}={BUDGET_S + _b6_send_worst}")
    check("B6 U4 ROUND_SOFT_S 由 send_plan 推导 (120 - send_worst - 6)",
          ROUND_SOFT_S == 120 - _b6_send_worst - 6,
          f"阈值={ROUND_SOFT_S} 期望={120 - _b6_send_worst - 6} send_worst={_b6_send_worst}")
    check("B6 U4 SEND_WORST 由 send_plan 推导 (当前 54)",
          SEND_WORST == _b6_send_worst == 54
          and ROUND_HARD_S == 120 and ROUND_MARGIN_S == 6,
          f"SEND_WORST={SEND_WORST} send_worst={_b6_send_worst}")
    check("B6 U4 逐次闸门阈值 = 120 - SEND_WORST - 余量 = 60",
          ROUND_HARD_S - SEND_WORST - ROUND_MARGIN_S == ROUND_SOFT_S == 60,
          f"{ROUND_HARD_S}-{SEND_WORST}-{ROUND_MARGIN_S}={ROUND_HARD_S - SEND_WORST - ROUND_MARGIN_S}")

    # 断言 5: 8 个英文源都真的把 FEED_HTTP 的 tries/cap 交给 http (运行级, 不只看常量)
    _b6_u6_kw: list = []
    _b6_http1 = globals()["http"]
    _b6_proxy1, _b6_cfg1 = PROXY, dict(CFG)
    try:
        globals()["http"] = lambda *a, **k: (_b6_u6_kw.append(dict(k)) or _b6_rss.encode())
        CFG.clear()
        CFG["proxy"] = "http://127.0.0.1:8080"
        for _fn in (src_mining, src_oilprice, src_miningtech, src_nasdaq,
                    src_cnbc, src_marketwatch, src_investing, src_hellenic):
            _fn(None)
    finally:
        globals()["http"] = _b6_http1
        globals()["PROXY"] = _b6_proxy1
        CFG.clear()
        CFG.update(_b6_cfg1)
    check("B6 U4 英文源单次/重试上限小于中文源",
          len(_b6_u6_kw) == 8
          and all(k.get("tries") == FEED_HTTP["tries"]
                  and k.get("cap") == FEED_HTTP["cap"] for k in _b6_u6_kw)
          and FEED_HTTP["tries"] < 3 and FEED_HTTP["cap"] < 12,
          str(_b6_u6_kw[:2]))
    check("B6 U4 source_health docstring 记录第三态 SRC_SKIP",
          "三态" in (source_health.__doc__ or "")
          and "SRC_SKIP" in (source_health.__doc__ or ""),
          str(source_health.__doc__)[:80])

    # 断言 6: main 预算跳过分支持 SRC_SKIP 哨兵 (字面量检查保留; 运行级见下方 main 全跳过)
    check("B6 U4 main 预算耗尽分支改用 SRC_SKIP 哨兵",
          "SRC_SKIP" in inspect.getsource(main)
          and "全局预算耗尽, 本轮跳过" not in inspect.getsource(main),
          "main 仍含旧字面量")

    # ---- Batch 6 U4 运行级: 假 SOURCES + 假时钟实跑 main() (全离线)
    class _B6Clock:
        """假时钟: main 的 round_start/deadline 用 monotonic, 业务逻辑用 time."""

        def __init__(self, t0: int, jump_after: int | None = None):
            self.t = float(t0)
            self.mono = 1000.0
            self.calls = 0
            self.jump_after = jump_after

        def time(self) -> float:
            return self.t

        def monotonic(self) -> float:
            self.calls += 1
            if self.jump_after is not None and self.calls > self.jump_after:
                return self.mono + 10000.0
            return self.mono

        def sleep(self, s: float) -> None:
            self.t += s
            self.mono += s

    _b6_never = lambda deadline=None: (_ for _ in ()).throw(
        AssertionError("全跳过路径不应调用源函数"))

    def _b6_run_main(tmp: Path, clock, srcs, conf_extra=None, state=None, tg_hook=None):
        """在 tempdir 内实跑 main(); 用假 tg 捕获 payload, 返回 rc/载荷/state/队列/CFG.

        tg_hook(text, clock): 可选, 模拟某类发送的真实耗时 (推进假时钟).
        """
        st = tmp / "state"
        st.mkdir(parents=True, exist_ok=True)
        kept = {k: globals()[k] for k in ("STATE", "QUEUE", "PENDING", "LOCK", "CONF",
                                          "LOG", "ERRLOG", "SOURCES", "time", "tg",
                                          "resolve_token", "rotate_log", "rotate_stderr",
                                          "ITEMS_ARCHIVE", "ITEMS_META")}
        payloads: list[str] = []
        try:
            globals()["STATE"] = st / "seen.json"
            globals()["QUEUE"] = st / "queue.json"
            globals()["PENDING"] = st / "pending.json"
            globals()["LOCK"] = st / "lock"
            globals()["CONF"] = st / "conf.json"
            globals()["LOG"] = st / "out.log"
            globals()["ERRLOG"] = st / "err.log"
            globals()["ITEMS_ARCHIVE"] = st / "items.jsonl"      # items 存档隔离到 tempdir
            globals()["ITEMS_META"] = st / "items.meta.json"
            globals()["SOURCES"] = srcs
            globals()["time"] = clock

            def _fake_tg(method, payload, tk, proxy, timeout=25):
                text = str((payload or {}).get("text") or "")
                payloads.append(text)
                if tg_hook is not None:
                    tg_hook(text, clock)
                return {"ok": True}

            globals()["tg"] = _fake_tg
            globals()["rotate_log"] = lambda: None
            globals()["rotate_stderr"] = lambda: None
            conf = {"token": "x", "chat_id": 1, "max_age_min": 180,
                    "digest_min": 30, "digest_max_items": 40}
            conf.update(conf_extra or {})
            globals()["CONF"].write_text(json.dumps(conf))
            if state is not None:
                globals()["STATE"].write_text(json.dumps(state))
            old_argv = sys.argv
            try:
                sys.argv = ["monitor.py"]
                rc = main()
            finally:
                sys.argv = old_argv
            got_state = (json.loads(globals()["STATE"].read_text())
                         if globals()["STATE"].exists() else {})
            got_queue, _, _ = load_queue()
            return rc, payloads, got_state, got_queue, dict(CFG)
        finally:
            for k, v in kept.items():
                globals()[k] = v

    _b6_cfg_keep = dict(CFG)
    try:
        # (S1/M2-③) 全跳过: 不告警、不写 _alert、rc=0
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000, jump_after=2)   # 第 3 次起 monotonic 跳 10000s
            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [(f"S{i}", _b6_never) for i in range(14)],
                state={"seedmarker": 1})
            check("B6 U4 main 全跳过: rc=0 且不告警 (S1/M2-③)",
                  _rc == 0 and not _pay and not _stt.get("_alert"),
                  f"rc={_rc} sends={_pay} state={_stt}")
        # (S1-② -> U-A-1) 半跳过 + 其余失败: 无条目但有跳过 -> 仍告警 (rc=1), 文案带跳过计数
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _mk_fail(cost):
                def _f(deadline=None):
                    _clk.mono += cost
                    raise RuntimeError("boom")
                return _f

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [(f"S{i}", _mk_fail(20.0)) for i in range(4)],
                state={"seedmarker": 1})
            _txt = _pay[0] if _pay else ""
            check("B6.1 U-A 半跳过+其余失败: rc=1 且告警含失败源与跳过计数 (S1-② 更新)",
                  _rc == 1 and len(_pay) == 1 and "S0" in _txt and "S3" not in _txt
                  and "跳过 1 源" in _txt and "预算耗尽" not in _txt,
                  f"rc={_rc} {_txt!r}")
        # (M2-① / U2-F-04b) main 路径: 跳过保留既有计数; main 把 conf 注入 CFG
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)
            _cfg_seen: list = []

            def _slow_ok(deadline=None):
                _cfg_seen.append(CFG.get("proxy"))
                _clk.mono += 50.0
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            CFG.clear()
            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _slow_ok), ("S1", _b6_never)],
                conf_extra={"proxy": "http://127.0.0.1:8080"},
                state={"seedmarker": 1,
                       SRC_FAIL_KEY: {"S1": {"rounds": 2, "since": 1, "last_err": "old"}}})
            _rounds = ((_stt.get(SRC_FAIL_KEY) or {}).get("S1") or {}).get("rounds")
            check("B6 U4 main 预算跳过保留既有失败计数 (M2-①)",
                  _rc == 0 and _rounds == 2 and not _pay,
                  f"rc={_rc} rounds={_rounds} sends={_pay}")
            check("B6 U2 main 每轮把 conf 注入 CFG (U2-F-04b)",
                  _cfg_seen == ["http://127.0.0.1:8080"],
                  f"{_cfg_seen} cfg={_cfg}")
        # (M1-②) 软截止触发: 不发送、队列原样保留、rc=0
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _slow_ok2(deadline=None):
                _clk.mono += 100.0
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _slow_ok2)],
                conf_extra={"digest_max_items": 1}, state={"seedmarker": 1})
            check("B6 U4 软截止触发: 不发送/队列原样/rc=0 (M1-②)",
                  _rc == 0 and not _pay and len(_q) == 1,
                  f"rc={_rc} sends={_pay} queue={len(_q)}")

        # (F-01) 阈值 = 120 - send 最坏 - 余量: 告警耗时 25s/54s 后聚合必须零调用.
        def _b6_cost_hook(alert_cost, digest_cost):
            def _hook(text, clk):
                if "快讯源" in text:          # 单源告警/恢复
                    clk.mono += alert_cost
                    clk.t += alert_cost
                else:                          # 聚合消息
                    clk.mono += digest_cost
                    clk.t += digest_cost
            return _hook

        for _alert_cost in (25.0, 54.0):
            with tempfile.TemporaryDirectory() as _td:
                _clk = _B6Clock(1793800000)

                def _fail0(deadline=None):
                    _clk.mono += 20.0
                    raise RuntimeError("boom")

                def _ok1(deadline=None):
                    _clk.mono += 25.0
                    return [{"src": "S1", "ts": int(_clk.time()) - 60,
                             "text": "沪铜库存变化1吨环比1%"}]

                _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                    Path(_td), _clk, [("S0", _fail0), ("S1", _ok1)],
                    conf_extra={"digest_max_items": 1},
                    state={"seedmarker": 1,
                           SRC_FAIL_KEY: {"S0": {"rounds": 2, "since": 1,
                                                 "last_err": "old"}}},
                    tg_hook=_b6_cost_hook(_alert_cost, 54.0))
                _elapsed = _clk.mono - 1000.0
                _alert_n = sum(1 for t in _pay if "快讯源" in t)
                _digest_n = len(_pay) - _alert_n
                check(f"B6 U4 F-01 告警 send {int(_alert_cost)}s 后聚合零调用/队列不变/整轮<=120",
                      _rc == 0 and _alert_n == 1 and _digest_n == 0
                      and len(_q) == 1 and _elapsed <= 120,
                      f"rc={_rc} alert={_alert_n} digest={_digest_n} "
                      f"queue={len(_q)} elapsed={_elapsed}")
        # (F-01-③) 无告警的正常一轮: 聚合仍要能发, 不能把正常路径堵死
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _ok_normal(deadline=None):
                _clk.mono += 1.0
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _ok_normal)],
                conf_extra={"digest_max_items": 1}, state={"seedmarker": 1},
                tg_hook=_b6_cost_hook(0.0, 54.0))
            _elapsed = _clk.mono - 1000.0
            check("B6 U4 F-01 无告警正常一轮仍能聚合发送 (不堵死正常路径)",
                  _rc == 0 and len(_pay) == 1 and "快讯源" not in _pay[0]
                  and not _q and _elapsed <= 120,
                  f"rc={_rc} sends={_pay} queue={len(_q)} elapsed={_elapsed}")

        # ---- (F-02) 逐次发送闸门: 每个 send 点发送前都判 send_time_ok()
        # ① 告警 54s 后还有全源恢复要发: 恢复零调用、_alert 保留、rc=0、整轮<=120
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _fail_a(deadline=None):
                _clk.mono += 20.0
                raise RuntimeError("boom")

            def _ok_a(deadline=None):
                _clk.mono += 25.0
                return [{"src": "S1", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _pre_a = {"seedmarker": 1,
                      "_alert": {"failing": True, "since": 1793790000, "rounds": 2,
                                 "last_sent": 1793790000, "alerts": 1},
                      SRC_FAIL_KEY: {
                          "S0": {"rounds": 2, "since": 1793790000, "last_err": "old"},
                          "S1": {"rounds": 3, "since": 1793790000, "alerts": 1,
                                 "last_sent": 1793790000, "last_err": "old"}}}
            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _fail_a), ("S1", _ok_a)],
                conf_extra={"digest_max_items": 1}, state=_pre_a,
                tg_hook=_b6_cost_hook(54.0, 54.0))
            _elapsed = _clk.mono - 1000.0
            _alert_n = sum(1 for t in _pay if "连续失败" in t)
            _rec_n = sum(1 for t in _pay if "已恢复" in t)
            _al = _stt.get("_alert") or {}
            check("B6 U4 F-02 告警54s后恢复send零调用/状态保留/整轮<=120",
                  _rc == 0 and _alert_n == 1 and _rec_n == 0
                  and _al.get("failing") is True and _al.get("rounds") == 2
                  and len(_q) == 1 and _elapsed <= 120,
                  f"rc={_rc} alert={_alert_n} recovery={_rec_n} state={_al} "
                  f"queue={len(_q)} elapsed={_elapsed}")
        # ② 全源失败 + 耗时 70s: 告警不发送但 _alert.rounds 递增, 下轮补发
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _fail70(deadline=None):
                _clk.mono += 70.0
                raise RuntimeError("boom")

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _fail70)], state={"seedmarker": 1})
            _al1 = _stt.get("_alert") or {}
            check("B6 U4 F-02 全源失败耗时70s: 告警不发送但 rounds 递增",
                  _rc == 1 and not _pay and _al1.get("failing") is True
                  and _al1.get("rounds") == 1 and _al1.get("last_sent") == 0
                  and _al1.get("alerts") == 0,
                  f"rc={_rc} sends={_pay} alert={_al1}")
            _clk2 = _B6Clock(1793800000)

            def _fail0(deadline=None):
                raise RuntimeError("boom")

            _rc2, _pay2, _stt2, _q2, _cfg2 = _b6_run_main(
                Path(_td), _clk2, [("S0", _fail0)])
            _al2 = _stt2.get("_alert") or {}
            check("B6 U4 F-02 全源失败告警下轮可补发",
                  _rc2 == 1 and len(_pay2) == 1 and _al2.get("rounds") == 2
                  and _al2.get("last_sent") and _al2.get("alerts") == 1,
                  f"rc={_rc2} sends={_pay2} alert={_al2}")
        # ③/④/⑤ source_health 告警/恢复与正常路径 (直接函数级, 假时钟 + 假 send)
        _f2_sent: list[str] = []
        _f2_send0 = globals()["send"]
        _f2_time0 = globals()["time"]
        _f2_rs0 = ROUND_START
        try:
            globals()["send"] = lambda text, conf, tk, depth=0: (_f2_sent.append(text) or True)
            _f2_clk = _B6Clock(1793800000)
            globals()["time"] = _f2_clk
            _f2_conf = {"chat_id": 1, "src_alert_rounds": 3}
            # ③ 单源告警: 70s 时推迟, rounds/pend 保留; 压力消失后补发
            globals()["ROUND_START"] = _f2_clk.mono - 70.0
            _f2_st: dict = {SRC_FAIL_KEY: {"S0": {"rounds": 2, "since": 1,
                                                  "last_err": "old"}}}
            source_health(_f2_st, [("S0", "RuntimeError: boom"), ("S1", "")],
                          _f2_conf, "tk", False)
            _f2_a = (src_fail_state(_f2_st).get("S0") or {})
            check("B6 U4 F-02 单源告警耗时70s推迟: 零send且pend/rounds保留",
                  not _f2_sent and _f2_a.get("rounds") == 3
                  and _f2_a.get("pend") is True,
                  f"{_f2_sent} {_f2_a}")
            globals()["ROUND_START"] = _f2_clk.mono
            source_health(_f2_st, [("S0", "RuntimeError: boom"), ("S1", "")],
                          _f2_conf, "tk", False)
            _f2_a2 = (src_fail_state(_f2_st).get("S0") or {})
            check("B6 U4 F-02 单源告警下轮补发",
                  len(_f2_sent) == 1 and _f2_a2.get("alerts") == 1
                  and not _f2_a2.get("pend"),
                  f"{_f2_sent} {_f2_a2}")
            # ④ 单源恢复: 70s 时推迟且不清零; 压力消失后补发
            _f2_sent.clear()
            globals()["ROUND_START"] = _f2_clk.mono - 70.0
            _f2_st2: dict = {SRC_FAIL_KEY: {"S0": {"rounds": 3, "since": 1,
                                                   "alerts": 1, "last_sent": 1,
                                                   "last_err": "old"}}}
            source_health(_f2_st2, [("S0", ""), ("S1", "")], _f2_conf, "tk", False)
            _f2_b = (src_fail_state(_f2_st2).get("S0") or {})
            check("B6 U4 F-02 单源恢复耗时70s推迟且状态保留",
                  not _f2_sent and _f2_b.get("alerts") == 1
                  and _f2_b.get("rounds") == 3,
                  f"{_f2_sent} {_f2_b}")
            globals()["ROUND_START"] = _f2_clk.mono
            source_health(_f2_st2, [("S0", ""), ("S1", "")], _f2_conf, "tk", False)
            check("B6 U4 F-02 单源恢复下轮补发",
                  len(_f2_sent) == 1 and SRC_FAIL_KEY not in _f2_st2,
                  f"{_f2_sent} {_f2_st2}")
            # ⑤ 正常路径: 无耗时压力时 alert_all_failed/source_health/恢复/note_recovered 均能发
            _f2_sent.clear()
            globals()["ROUND_START"] = _f2_clk.mono
            _f2_na: dict = {}
            alert_all_failed(_f2_na, ["S0"], {"chat_id": 1}, "tk", False)
            _f2_nb: dict = {SRC_FAIL_KEY: {"S0": {"rounds": 2, "since": 1,
                                                  "last_err": "old"}}}
            source_health(_f2_nb, [("S0", "RuntimeError: x"), ("S1", "")],
                          _f2_conf, "tk", False)
            _f2_nc: dict = {SRC_FAIL_KEY: {"S0": {"rounds": 3, "since": 1,
                                                  "alerts": 1, "last_sent": 1,
                                                  "last_err": "old"}}}
            source_health(_f2_nc, [("S0", ""), ("S1", "")], _f2_conf, "tk", False)
            _f2_nd: dict = {"_alert": {"failing": True, "since": 1, "rounds": 2,
                                       "last_sent": 1, "alerts": 1}}
            note_recovered(_f2_nd, {"chat_id": 1}, "tk", False)
            check("B6 U4 F-02 正常路径四类 send 均能发出 (不堵死)",
                  len(_f2_sent) == 4
                  and any("全部抓取失败" in t for t in _f2_sent)
                  and any("连续失败" in t for t in _f2_sent)
                  and sum(1 for t in _f2_sent if "已恢复" in t) == 2,
                  str(_f2_sent))
        finally:
            globals()["send"] = _f2_send0
            globals()["time"] = _f2_time0
            globals()["ROUND_START"] = _f2_rs0

        # ---- Batch 6.1 U-A: 全源告警文案准确性 (U-A-1) + 恢复通知对账 (U-A-2)
        # ① / ② failing=True 的恢复轮: alerts=0 (告警从未送达) 静默复位; alerts>=1 仍发「已恢复」
        _ua_sent: list[str] = []
        _ua_send0 = globals()["send"]
        _ua_time0 = globals()["time"]
        _ua_rs0 = ROUND_START
        try:
            globals()["send"] = lambda text, conf, tk, depth=0: (_ua_sent.append(text) or True)
            _ua_clk2 = _B6Clock(1793800000)
            globals()["time"] = _ua_clk2
            globals()["ROUND_START"] = _ua_clk2.mono
            _ua_s0: dict = {"seedmarker": 1,
                            "_alert": {"failing": True, "since": 1793790000, "rounds": 2,
                                       "last_sent": 0, "alerts": 0}}
            _ua_r0 = note_recovered(_ua_s0, {"chat_id": 1}, "tk", False)
            _ua_al0 = _ua_s0.get("_alert") or {}
            check("B6.1 U-A alerts=0 恢复轮: 零发送且静默复位 (U-A-2)",
                  not _ua_sent and _ua_r0 is True and _ua_al0.get("failing") is False
                  and int(_ua_al0.get("rounds") or 0) == 0
                  and int(_ua_al0.get("alerts") or 0) == 0,
                  f"sends={_ua_sent} ret={_ua_r0} state={_ua_al0}")
            _ua_sent.clear()
            _ua_s1: dict = {"seedmarker": 1,
                            "_alert": {"failing": True, "since": 1793790000, "rounds": 2,
                                       "last_sent": 1793790000, "alerts": 1}}
            _ua_r1 = note_recovered(_ua_s1, {"chat_id": 1}, "tk", False)
            _ua_al1 = _ua_s1.get("_alert") or {}
            check("B6.1 U-A alerts>=1 恢复轮: 发 1 条「已恢复」并复位 (U-A-2)",
                  len(_ua_sent) == 1 and "已恢复" in _ua_sent[0] and _ua_r1 is True
                  and _ua_al1.get("failing") is False
                  and int(_ua_al1.get("rounds") or 0) == 0,
                  f"sends={_ua_sent} ret={_ua_r1} state={_ua_al1}")
        finally:
            globals()["send"] = _ua_send0
            globals()["time"] = _ua_time0
            globals()["ROUND_START"] = _ua_rs0

        # ③ 1 源失败 + 3 源被预算跳过 (无条目): 仍按全源故障告警, 但文案必须带跳过计数
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _ua_fail1(deadline=None):
                _clk.mono += 29.0
                _clk.t += 29.0
                raise RuntimeError("boom")

            _ua_buf = io.StringIO()
            _ua_out0 = sys.stdout
            try:
                sys.stdout = _ua_buf
                _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                    Path(_td), _clk,
                    [("S0", _ua_fail1)] + [(f"S{i}", _b6_never) for i in range(1, 4)],
                    conf_extra={"timeout_s": 30}, state={"seedmarker": 1})
            finally:
                sys.stdout = _ua_out0
            _ua_log = _ua_buf.getvalue()
            _ua_txt = _pay[0] if _pay else ""
            check("B6.1 U-A 部分失败+部分跳过: rc=1/文案含失败源与跳过计数/不含跳过源 (U-A-1)",
                  _rc == 1 and len(_pay) == 1
                  and "S0" in _ua_txt and "跳过 3 源" in _ua_txt
                  and "S1" not in _ua_txt and "S2" not in _ua_txt and "S3" not in _ua_txt
                  and "本轮无可用条目: 失败 1 源 / 跳过 3 源" in _ua_log
                  and "全部源失败" not in _ua_log,
                  f"rc={_rc} sends={_pay} log={_ua_log!r}")

        # ④ 全部被跳过: 既有语义不变 -> rc=0 / 零发送 / 不写 _alert
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000, jump_after=2)
            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [(f"S{i}", _b6_never) for i in range(3)],
                state={"seedmarker": 1})
            check("B6.1 U-A 全部被跳过: rc=0/零发送/不写 _alert (既有行为不变)",
                  _rc == 0 and not _pay and "_alert" not in _stt,
                  f"rc={_rc} sends={_pay} state={_stt}")

        # ⑤ main 级实景: 上轮 failing 且 alerts=0, 本轮全源正常 -> 不发「已恢复」且复位落盘
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _ua_ok(deadline=None):
                _clk.mono += 1.0
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _ua_ok)], conf_extra={"digest_max_items": 1},
                state={"seedmarker": 1,
                       "_alert": {"failing": True, "since": 1793790000, "rounds": 2,
                                  "last_sent": 0, "alerts": 0}})
            _ual5 = _stt.get("_alert") or {}
            check("B6.1 U-A 告警未送达后的恢复轮: 不发「已恢复」且复位落盘 (U-A-2 实景)",
                  _rc == 0 and not any("已恢复" in t for t in _pay)
                  and _ual5.get("failing") is False,
                  f"rc={_rc} sends={_pay} alert={_ual5}")

        # ⑥ 生产端不变式: alerts 只计「告警确实送达」—— 发送失败时 alerts/last_sent 保持 0 (F-01)
        _ua_sent6: list[str] = []
        _ua_send6 = globals()["send"]
        _ua_time6 = globals()["time"]
        _ua_rs6 = ROUND_START
        try:
            globals()["send"] = lambda text, conf, tk, depth=0: (_ua_sent6.append(text) or False)
            _ua_clk6 = _B6Clock(1793800000)
            globals()["time"] = _ua_clk6
            globals()["ROUND_START"] = _ua_clk6.mono
            _ua_s6: dict = {"seedmarker": 1}
            alert_all_failed(_ua_s6, ["S0"], {"chat_id": 1}, "tk", False)
            _ua_al6 = _ua_s6.get("_alert") or {}
            check("B6.1 U-A 告警发送失败不计入 alerts: alerts=0 且 last_sent=0 (F-01 生产端)",
                  len(_ua_sent6) == 1
                  and _ua_al6.get("failing") is True
                  and int(_ua_al6.get("since") or 0) == 1793800000
                  and int(_ua_al6.get("rounds") or 0) == 1
                  and int(_ua_al6.get("last_sent") or 0) == 0
                  and int(_ua_al6.get("alerts") or 0) == 0,
                  f"sends={_ua_sent6} state={_ua_al6}")
            # 未送达 -> 次轮恢复必须零发送且静默复位 (事故形态: 告警全失败后不得发假「已恢复」)
            _ua_sent6.clear()
            globals()["send"] = lambda text, conf, tk, depth=0: (_ua_sent6.append(text) or True)
            _ua_r6 = note_recovered(_ua_s6, {"chat_id": 1}, "tk", False)
            _ua_al6b = _ua_s6.get("_alert") or {}
            check("B6.1 U-A 告警未送达后的恢复轮零发送且 _alert 复位 (F-01 全链)",
                  not _ua_sent6 and _ua_r6 is True
                  and _ua_al6b.get("failing") is False
                  and int(_ua_al6b.get("alerts") or 0) == 0
                  and int(_ua_al6b.get("rounds") or 0) == 0,
                  f"sends={_ua_sent6} ret={_ua_r6} state={_ua_al6b}")
        finally:
            globals()["send"] = _ua_send6
            globals()["time"] = _ua_time6
            globals()["ROUND_START"] = _ua_rs6

        # ⑦ 累积轮文案: 第 2 轮全失败 + 本轮 2 源被跳过 -> 含「跳过 2 源」且不含跳过源名 (F-02)
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _ua_fail7(deadline=None):
                _clk.mono += 29.0
                _clk.t += 29.0
                raise RuntimeError("boom")

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk,
                [("S0", _ua_fail7), ("S1", _b6_never), ("S2", _b6_never)],
                conf_extra={"timeout_s": 30},
                state={"seedmarker": 1,
                       "_alert": {"failing": True, "since": 1793700000, "rounds": 1,
                                  "last_sent": 1793790000, "alerts": 1}})
            _ua_txt7 = _pay[0] if _pay else ""
            _ua_al7 = _stt.get("_alert") or {}
            check("B6.1 U-A 累积轮告警文案含跳过计数且不含跳过源名 (F-02)",
                  _rc == 1 and len(_pay) == 1
                  and "仍全部失败 (累计 2 轮)" in _ua_txt7
                  and "跳过 2 源" in _ua_txt7 and "S0" in _ua_txt7
                  and "S1" not in _ua_txt7 and "S2" not in _ua_txt7
                  and int(_ua_al7.get("rounds") or 0) == 2,
                  f"rc={_rc} sends={_pay} alert={_ua_al7}")

    finally:
        CFG.clear()
        CFG.update(_b6_cfg_keep)

    # ---- Batch 6.1 U-B: 抓取预算按源切片 (单源不得吃掉整轮预算)
    # 旧码没有 MIN_SLICE / slice_deadline: 取默认值让本块逐条 BAD, 而不是 NameError 中断.
    _ub_min = float(globals().get("MIN_SLICE") or 0.0)
    _ub_sd = globals().get("slice_deadline")

    def _ub_hang(clock):
        """假源: 把分到的份额 deadline 精确烧完再失败 (模拟 DNS 抖动吃掉整段份额)."""
        spent: list[float] = []

        def _f(deadline=None):
            t0 = clock.mono
            if deadline is not None and deadline > t0:
                clock.mono = deadline
                clock.t += clock.mono - t0
            spent.append(clock.mono - t0)
            raise RuntimeError("http failed (000)")

        return _f, spent

    # MIN_SLICE 硬下限: < 6.5s 即覆盖不住生产实测 p99 最慢源 (见闻商品 5.40s), 低于即红
    _ub_floor = 6.5
    check("B6.1 U-B MIN_SLICE >= 6.5s 且覆盖 p99 最慢正常源 (见闻商品 5.40s / max 7.50s)",
          _ub_floor <= _ub_min < BUDGET_S, f"MIN_SLICE={_ub_min}")

    # ① 卡满份额的慢源: 单源耗时 <= MIN_SLICE + 容差, 后继源仍能跑且条目进聚合
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)
        _hang, _spent = _ub_hang(_clk)
        _ok_calls: list[int] = []

        def _ok(deadline=None):
            _ok_calls.append(1)
            return [{"src": "S1", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk,
            [("HANG", _hang)] + [(f"OK{_i}", _ok) for _i in range(13)],
            conf_extra={"digest_max_items": 1}, state={"seedmarker": 1})
        check("B6.1 U-B 卡满份额的慢源 <= MIN_SLICE+容差 且后继源仍能跑",
              len(_spent) == 1 and _spent[0] <= _ub_min + 0.5
              and len(_ok_calls) == 13
              and any("沪铜库存变化1吨环比1%" in t for t in _pay),
              f"spent={_spent} ok={len(_ok_calls)} sends={len(_pay)} rc={_rc}")

    # ② 14 源全慢: 每源 <= 份额, 至少 2 个源真的被尝试过, 整轮 <= BUDGET_S
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)
        _srcs, _spent2 = [], []
        for _i in range(14):
            _h, _s = _ub_hang(_clk)
            _srcs.append((f"S{_i}", _h))
            _spent2.append(_s)
        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk, _srcs, state={"seedmarker": 1})
        _flat = [c for s in _spent2 for c in s]
        _elapsed = _clk.mono - 1000.0
        check("B6.1 U-B 14 源全慢: 每源 <= 份额, 多源仍被尝试, 整轮 <= BUDGET_S",
              len(_flat) >= 2 and all(c <= _ub_min + 0.5 for c in _flat)
              and _elapsed <= BUDGET_S + 1e-6,
              f"attempts={len(_flat)} max={(max(_flat) if _flat else -1)} "
              f"elapsed={_elapsed} rc={_rc}")

    # ③ 快源序列: 结果与切片前一致 (条数/内容不变), 切片零额外等待, 份额下限必须真正被守住
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)
        _got_dl: list[float] = []
        # 14 源 => 份额 = 45/14 ≈ 3.2s < MIN_SLICE, 必然落到下限 (3 源时份额 15s 恒不触发下限)
        _costs = (0.2, 2.7, 0.3, 0.1) + (0.2,) * 10   # 快源实测 0.1-0.3s / 慢源样本 Nasdaq 2.7s
        _texts = [f"沪铜库存变化{_i}吨环比1%" for _i in range(len(_costs))]

        def _mk_fast(text, cost):
            def _f(deadline=None):
                _got_dl.append(1045.0 if deadline is None else deadline)
                _clk.mono += cost
                return [{"src": "S", "ts": int(_clk.time()) - 60, "text": text}]
            return _f

        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk,
            [(f"F{_i}", _mk_fast(_t, _costs[_i])) for _i, _t in enumerate(_texts)],
            conf_extra={"digest_max_items": len(_texts)}, state={"seedmarker": 1})
        _want = sum(_costs)
        _elapsed = _clk.mono - 1000.0
        _hit = [t for t in _texts if any(t in m for m in _pay)]
        check("B6.1 U-B 快源序列结果一致(含最慢正常源)且切片零额外等待/守住下限",
              len(_hit) == len(_texts) and not _q
              and abs(_elapsed - _want) < 1e-9
              and min(_got_dl) - 1000.0 >= _ub_floor
              and all(d <= 1000.0 + BUDGET_S + 1e-6 for d in _got_dl),
              f"hit={len(_hit)}/{len(_texts)} elapsed={_elapsed}/{_want} "
              f"dl_min={min(_got_dl) - 1000.0 if _got_dl else None} floor={_ub_floor} "
              f"queue={len(_q)} rc={_rc}")

    # ④ 份额 < MIN_SLICE 用 MIN_SLICE; 剩余预算 < MIN_SLICE 时不越过全局 deadline
    check("B6.1 U-B 份额<MIN_SLICE 时按 MIN_SLICE 且不越过全局 deadline",
          callable(_ub_sd)
          and abs(_ub_sd(1045.0, 1000.0, 14) - (1000.0 + _ub_min)) < 1e-9
          and _ub_sd(1003.0, 1000.0, 2) == 1003.0
          and _ub_sd(1045.0, 1000.0, 1) == 1045.0,
          f"share14={_ub_sd(1045.0, 1000.0, 14) if callable(_ub_sd) else None}")

    # ---- Batch 7: 研报源 (PDF 正文提取 / 稳定 key / 渲染 / 队列字段) 全离线
    _a7_raw = "\n".join([
        "[Table_StockNameRptType]", "12.13", "- 3 -", "第 2 页",
        "2026年8月全国期货市场交易情况简报",
        "1-8月全国期货市场累计成交量为7194.34百万手；同比增长20.36%",
        "8,000", "百万手", "产品 当月 占比 去年同期 同比 上月 环比",
        "上期所1-8月累计成交量为1717.83百万手，占全国市场的23.88%，同比增长15.08%"])
    _a7_clean = pdf_clean(_a7_raw)
    check("B7-01 pdf_clean 去页码/表格碎片且只留成句行",
          _a7_clean.startswith("2026年8月全国期货市场交易情况简报")
          and "12.13" not in _a7_clean and "- 3 -" not in _a7_clean
          and "[Table_" not in _a7_clean and "8,000" not in _a7_clean
          and "上期所" in _a7_clean, repr(_a7_clean[:80]))
    _a7_long = pdf_clean("沪铜" * 500)
    check("B7-02 pdf_clean 按 SNIPPET_MAX 截断",
          len(_a7_long) == SNIPPET_MAX and len(pdf_clean("沪铜" * 500, 50)) == 50,
          f"{len(_a7_long)}")

    # 打桩 http / subprocess / PDFTOTEXT, 全程不联网也不依赖真实 PDF
    _a7_http, _a7_run, _a7_bin = globals()["http"], subprocess.run, globals()["PDFTOTEXT"]

    def _a7_http_bytes(data):
        return lambda *a, **k: data

    class _A7R:
        def __init__(self, out):
            self.stdout = out
            self.returncode = 0

    try:
        globals()["PDFTOTEXT"] = ""
        globals()["http"] = _a7_http_bytes(b"%PDF-1.7\nfake")
        check("B7-03 pdf_text 无 pdftotext 时降级为空摘要", pdf_text("https://x/a.pdf") == "")
        globals()["PDFTOTEXT"] = sys.executable           # 真实存在的路径占位; run 已打桩
        subprocess.run = _a7_run                         # 未走到子进程即返回
        globals()["http"] = _a7_http_bytes(b"<html>302 Found</html>")
        check("B7-04 pdf_text 非 PDF 响应(302 错误页)降级为空摘要",
              pdf_text("https://x/a.pdf") == "")
        globals()["http"] = _a7_http_bytes(b"%PDF-1.7\nfake")
        subprocess.run = lambda *a, **k: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("pdftotext", 6))
        check("B7-05 pdf_text 子进程超时降级为可重试(None), 绝不抛错",
              pdf_text("https://x/a.pdf") is None)
        subprocess.run = lambda *a, **k: _A7R("  12.13 \n - 3 - \n 百万手".encode())
        check("B7-06 pdf_text 无文本层(仅表格碎片)降级为空摘要",
              pdf_text("https://x/a.pdf") == "")
        _a7_body = "2026年8月全国期货市场交易情况简报 " + "1-8月全国期货市场累计成交量同比增长20.36%；" * 8
        subprocess.run = lambda *a, **k: _A7R(_a7_body.encode())
        _a7_pdf = pdf_text("https://x/a.pdf")
        check("B7-07 pdf_text 正常提取出摘要且不超过 SNIPPET_MAX",
              0 < len(_a7_pdf) <= SNIPPET_MAX and _a7_pdf.startswith("2026年8月"),
              repr(_a7_pdf[:60]))
    finally:
        globals()["http"] = _a7_http
        globals()["PDFTOTEXT"] = _a7_bin
        subprocess.run = _a7_run

    _a7_k1 = research_key("东财研报", "AP202611041831390808")
    check("B7-08 research_key 同 ID 稳定/异 ID 不同且与 dedupe_key 命名空间不相交",
          _a7_k1 == research_key("东财研报", "AP202611041831390808")
          and _a7_k1 != research_key("东财研报", "AP202611041831390809")
          and ":" in _a7_k1
          and ":" not in dedupe_key("研究:东财研报:AP1")
          and ":" not in dedupe_key("【研究】沪铜夜盘上涨"), _a7_k1)

    _a7_ts = int(time.time())
    _a7_dt = datetime.fromtimestamp(_a7_ts, TZ_CN)
    _a7_mix = [{"src": "财联社", "ts": _a7_ts, "text": "沪铜夜盘上涨1.2%"},
               {"src": "中期协", "ts": _a7_ts,
                "text": "2026年8月全国期货市场交易情况简报｜中国期货业协会",
                "url": 'https://x/y?a=1&b="2"', "snippet": "1-8月累计成交量同比增长20.36%"}]
    _a7_msg = build_messages(_a7_mix)[0][0]
    check("B7-09 研报条目渲染出可点击链接且非研报行逐字节不变",
          '<a href="https://x/y?a=1&amp;b=&quot;2&quot;">查看原文</a>' in _a7_msg
          and "<b>中期协" in _a7_msg and "<b>2026年8月" in _a7_msg
          and f"<b>财联社 {_a7_dt:%H:%M}</b>\n{esc('沪铜夜盘上涨1.2%')}" in _a7_msg,
          _a7_msg[:100])

    with tempfile.TemporaryDirectory() as _td:
        _a7_qp = Path(_td) / "q.json"
        _a7_old = {"src": "财联社", "ts": _a7_ts, "text": "沪铜夜盘上涨1.2%", "key": "沪铜夜盘上涨12"}
        _a7_new = {"src": "中期协", "ts": _a7_ts, "text": "简报｜协会", "key": "研究:中期协:1",
                   "url": "https://x/y.pdf", "snippet": "摘要"}
        _a7_saved = save_json(_a7_qp, {"items": [_a7_old, _a7_new], "ts": _a7_ts})
        _a7_glob = globals()["QUEUE"]
        globals()["QUEUE"] = _a7_qp
        try:
            _a7_got, _a7_prob, _a7_bad = load_queue()
        finally:
            globals()["QUEUE"] = _a7_glob
        check("B7-10 队列往返保留 url/snippet 且非研报条目字段与顺序不变",
              _a7_saved and not _a7_prob and _a7_bad == 0 and len(_a7_got) == 2
              and list(_a7_got[0].keys()) == ["src", "ts", "text", "key"]
              and _a7_got[1].get("url") == "https://x/y.pdf"
              and _a7_got[1].get("snippet") == "摘要", str(_a7_got))

    check("B7-11 研报过滤豁免官方统计口径但仍拦栏目/日历与股票噪声",
          research_reason("2026年8月全国期货市场交易情况简报｜中国期货业协会") == ""
          and filter_reason("2026年8月全国期货市场交易情况简报｜中国期货业协会",
                            DEFAULT_KEYWORDS, True) != ""
          and research_reason("【财经日历】今日重要数据公布时间表") == "栏目/日历"
          and research_reason("英伟达美股盘前涨超2%") == "噪声:美股"
          and research_reason("天山铝业：电解铝量价齐升，夯实股东回报｜华安证券") == "",
          research_reason("英伟达美股盘前涨超2%"))

    # 东财: 列表 -> PDF 直链 + 稳定 key; 非商品研报被既有词表拦掉
    _a7_em_json = json.dumps({"hits": 2, "data": [
        {"title": "沪铜周报：库存去化", "orgSName": "某证券",
         "publishDate": "2026-11-06 00:00:00.000", "infoCode": "AP1", "industryName": ""},
        {"title": "英伟达美股盘前涨超2%", "orgSName": "某证券",
         "publishDate": "2026-11-06 00:00:00.000", "infoCode": "AP2", "industryName": ""}]})
    globals()["http"] = _a7_http_bytes(_a7_em_json.encode())
    try:
        _a7_em = src_em_report(time.monotonic() + 5)
    finally:
        globals()["http"] = _a7_http
    check("B7-12 东财研报走 PDF 直链/稳定 key, 且非商品研报被词表拦掉",
          len(_a7_em) == 1
          and _a7_em[0]["url"] == "https://pdf.dfcfw.com/pdf/H3_AP1_1.pdf"
          and _a7_em[0]["key"] == "研究:东财研报:AP1"
          and _a7_em[0]["snippet"] is None, str(_a7_em)[:160])

    # 中金所: 月报有 PDF(待提取), 日/周报只发链接不发正文
    _a7_ok_urls: list[str] = []
    _a7_ok = globals()["http_ok"]
    globals()["http_ok"] = lambda url, deadline=None, timeout=6: (
        _a7_ok_urls.append(url) or url.endswith("MonthlyReport.pdf"))
    try:
        _a7_cffex = src_cffex(time.monotonic() + 5)
    finally:
        globals()["http_ok"] = _a7_ok
    check("B7-13 中金所月报走 PDF 摘要、日/周报无正文只留官方链接",
          len(_a7_cffex) == 1 and _a7_cffex[0]["url"].endswith("MonthlyReport.pdf")
          and _a7_cffex[0]["snippet"] is None
          and _a7_cffex[0]["key"].startswith("研究:中金所:m")
          and any("/ztj/" not in u for u in _a7_ok_urls), str(_a7_cffex)[:160])

    # 研报 ts = 首次发现时刻, 发布日期只进元信息 (避免 max_age/max_hold 把日期级时间误判)
    _a7_t0 = int(time.time())
    globals()["http"] = _a7_http_bytes(_a7_em_json.encode())
    try:
        _a7_em2 = src_em_report(time.monotonic() + 5)
    finally:
        globals()["http"] = _a7_http
    check("B7-14 研报 ts 取首次发现时刻、真实发布日期保留在元信息里",
          len(_a7_em2) == 1 and abs(_a7_em2[0]["ts"] - _a7_t0) <= 2
          and "2026-11-06" in _a7_em2[0]["text"], str(_a7_em2)[:140])

    # 每轮最多 2 篇; 已入队条目不重复下载, 被预算挤掉的下一轮补齐
    with tempfile.TemporaryDirectory() as _td:
        _a7_clk = _B6Clock(1793800000)
        _a7_calls: list[str] = []
        _a7_pdf_real = globals()["pdf_text"]
        globals()["pdf_text"] = lambda url, deadline=None, referer=None: (
            _a7_calls.append(url) or f"摘要{len(_a7_calls)}")

        def _a7_three(deadline=None):
            return [research_item("东财研报", f"AP{i}", f"沪铜研报{i}",
                                  f"https://x/{i}.pdf", int(_a7_clk.time()) - 60,
                                  "某证券 · 2026-11-06") for i in range(3)]

        try:
            _n = []
            for _round in range(3):
                _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                    Path(_td), _a7_clk, [("研报源", _a7_three)],
                    conf_extra={"digest_max_items": 40}, state={"seedmarker": 1})
                _n.append(len(_a7_calls) - sum(_n))
        finally:
            globals()["pdf_text"] = _a7_pdf_real
        check("B7-15 每轮最多解析 2 篇且已入队条目不重复解析",
              _n == [2, 1, 0] and len(_q) == 3
              and sum(1 for x in _q if x.get("snippet")) == 3, f"calls={_n} q={len(_q)}")

    # 研报不做近邻合并: 同日期日/周报的 key 只差一个字母、数字完全相同, difflib 会把它们判成同一事件
    _a7_mts = int(time.time())
    _a7_m1 = research_item("中金所", "d20261105", "中金所日度行情统计 2026年11月05日",
                           "https://x/d.csv", _a7_mts, "中国金融期货交易所", "")
    _a7_m2 = research_item("中金所", "w20261105", "中金所周度数据统计 2026年11月05日当周",
                           "https://x/w.csv", _a7_mts, "中国金融期货交易所", "")
    _a7_kept, _a7_pairs = merge_near([_a7_m1, _a7_m2])
    check("B7-16 研报条目不做近邻合并 (同日期日/周报不会互相吞掉)",
          len(_a7_kept) == 2 and not _a7_pairs,
          f"kept={len(_a7_kept)} pairs={len(_a7_pairs)}")

    # ==== Batch 7 w4 修复回归 (A1-A5 / B1-B3 / C1-C3 + 轻微) ====
    class _A7RR:
        def __init__(self, out=b"", rc=0):
            self.stdout = out
            self.returncode = rc

    # ---- A1: 瞬时失败 -> None (下轮重试); 确定性无文本 -> "" (不再重试)
    _a7_fix_http, _a7_fix_run, _a7_fix_bin = globals()["http"], subprocess.run, globals()["PDFTOTEXT"]
    try:
        globals()["PDFTOTEXT"] = sys.executable
        globals()["http"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("curl timeout"))
        _a7_transient = pdf_text("https://x/a.pdf")
        globals()["http"] = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("http 预算耗尽"))
        _a7_budgetfail = pdf_text("https://x/a.pdf")
        globals()["http"] = _a7_http_bytes(b"%PDF-1.7\nfake")
        subprocess.run = lambda *a, **k: _A7RR(b"", rc=1)
        _a7_det = pdf_text("https://x/a.pdf")
    finally:
        globals()["http"] = _a7_fix_http
        globals()["PDFTOTEXT"] = _a7_fix_bin
        subprocess.run = _a7_fix_run
    check("B7-17 pdf_text 瞬时失败(下载/预算)返回 None 可重试, 确定性无文本返回 '' (A1)",
          _a7_transient is None and _a7_budgetfail is None and _a7_det == "",
          f"transient={_a7_transient!r} budget={_a7_budgetfail!r} det={_a7_det!r}")

    _a7_retry = {"n": 0, "val": None}
    _a7_pdf0 = globals()["pdf_text"]
    globals()["pdf_text"] = lambda url, deadline=None, referer=None: (
        _a7_retry.__setitem__("n", _a7_retry["n"] + 1) or _a7_retry["val"])
    try:
        _a7_rit = research_item("东财研报", "AP1", "沪铜周报：库存去化",
                                "https://x/1.pdf", _a7_ts, "某证券 · 2026-11-06")
        _a7_c1, _a7_qn1 = extract_research([], [_a7_rit])
        _a7_first = _a7_rit.get("snippet")
        _a7_retry["val"] = "1-8月全国期货市场累计成交量同比增长20.36%"
        _a7_c2, _a7_qn2 = extract_research([], [_a7_rit])
    finally:
        globals()["pdf_text"] = _a7_pdf0
    check("B7-18 extract_research 对瞬时失败保持延迟入队, 下一轮重试成功 (A1)",
          _a7_c1 == 1 and _a7_first is None and _a7_retry["n"] == 2
          and _a7_rit.get("snippet", "").startswith("1-8月")
          and _a7_c2 == 1 and _a7_qn1 == 0 and _a7_qn2 == 0,
          f"calls={_a7_retry['n']} first={_a7_first!r} snip={_a7_rit.get('snippet')!r}")

    # A1 第二条路径: "" = 已判定无文本层, 队列/源内都不再重试
    _a7_known_calls: list[str] = []
    _a7_pdf1 = globals()["pdf_text"]
    globals()["pdf_text"] = lambda url, deadline=None, referer=None: (
        _a7_known_calls.append(url) or "不应重试")
    try:
        _a7_qknown = [research_item("中金所", "d20261105", "中金所日度行情统计",
                                    "https://x/d.csv", _a7_ts, "中金所", "")]
        _a7_ec, _a7_eq = extract_research(_a7_qknown, [])
        _a7_fknown = [research_item("中金所", "w20261102", "中金所周度数据统计",
                                    "https://x/w.csv", _a7_ts, "中金所", "")]
        _a7_ec2, _a7_eq2 = extract_research([], _a7_fknown)
    finally:
        globals()["pdf_text"] = _a7_pdf1
    check("B7-34 已判定无文本层(snippet=\"\")的队列/源内条目不再重试 pdf_text (A1 第二条路径)",
          _a7_ec == 0 and _a7_ec2 == 0 and not _a7_known_calls
          and _a7_qknown[0].get("snippet") == "" and _a7_fknown[0].get("snippet") == "",
          f"calls={_a7_known_calls} ec={_a7_ec}/{_a7_ec2}")

    # ---- A2/A4/A5: curl 退出码、--max-filesize、体积兜底、PDF 路径不请求 gzip
    _a7_hcmd, _a7_hrun = [], subprocess.run
    try:
        subprocess.run = lambda cmd, **kw: (_a7_hcmd.append(list(cmd))
                                            or _A7RR(b"x" * 1024 + b"\n200"))
        _a7_ok_body = http("https://x/a.pdf", tries=1, max_bytes=1024,
                           deadline=time.monotonic() + 10)
        _a7_capped_argv = list(_a7_hcmd[-1])
        subprocess.run = lambda cmd, **kw: (_a7_hcmd.append(list(cmd))
                                            or _A7RR(b"x" * 1025 + b"\n200"))
        _a7_oversize = None
        try:
            http("https://x/a.pdf", tries=1, max_bytes=1024,
                 deadline=time.monotonic() + 10)
        except RuntimeError as e:
            _a7_oversize = str(e)
        subprocess.run = lambda cmd, **kw: (_a7_hcmd.append(list(cmd))
                                            or _A7RR(b"%PDF" + b"x" * 64 + b"\n200", rc=63))
        _a7_rcfail = None
        try:
            http("https://x/a.pdf", tries=1, max_bytes=1024,
                 deadline=time.monotonic() + 10)
        except RuntimeError as e:
            _a7_rcfail = str(e)
    finally:
        subprocess.run = _a7_hrun
    check("B7-19 http 检查 curl 退出码 + max_bytes 双闸且 PDF 路径不请求 gzip (A2/A4/A5)",
          _a7_ok_body == b"x" * 1024
          and "--max-filesize" in _a7_capped_argv
          and _a7_capped_argv[_a7_capped_argv.index("--max-filesize") + 1] == "1024"
          and "--compressed" not in _a7_capped_argv
          and _a7_oversize is not None and "http failed" in _a7_oversize
          and _a7_rcfail is not None and "curl rc=63" in _a7_rcfail,
          f"argv={_a7_capped_argv} over={_a7_oversize!r} rc={_a7_rcfail!r}")

    _a7_hokrun = subprocess.run
    try:
        subprocess.run = lambda cmd, **kw: _A7RR(b"200")
        _a7_hok200 = http_ok("https://x/a.pdf", deadline=time.monotonic() + 5)
        subprocess.run = lambda cmd, **kw: _A7RR(b"302")
        _a7_hok302 = http_ok("https://x/a.pdf", deadline=time.monotonic() + 5)
        subprocess.run = lambda cmd, **kw: _A7RR(b"404")
        _a7_hok404 = http_ok("https://x/a.pdf", deadline=time.monotonic() + 5)
        subprocess.run = lambda *a, **k: (_ for _ in ()).throw(OSError("probe down"))
        _a7_hokexc = http_ok("https://x/a.pdf", deadline=time.monotonic() + 5)
    finally:
        subprocess.run = _a7_hokrun
    check("B7-20 http_ok 只认 2xx: 200 True / 302 False / 404 False / 异常 False (A4)",
          _a7_hok200 and not _a7_hok302 and not _a7_hok404 and not _a7_hokexc,
          f"{_a7_hok200}/{_a7_hok302}/{_a7_hok404}/{_a7_hokexc}")

    # ---- A3/A5: PDF 子预算含解析时间; pdftotext 限页 + RLIMIT_AS
    _a7_now2 = time.monotonic()
    _a7_pkw, _a7_prun = {}, []
    _a7_bin3 = globals()["PDFTOTEXT"]
    try:
        globals()["PDFTOTEXT"] = sys.executable
        globals()["http"] = lambda url, **kw: (_a7_pkw.update(kw) or b"%PDF-1.7\nfake")
        subprocess.run = lambda cmd, **kw: (_a7_prun.append((list(cmd), dict(kw)))
                                            or _A7RR(("正文" * 60).encode()))
        pdf_text("https://x/a.pdf", deadline=_a7_now2 + PDF_BUDGET_S)
    finally:
        globals()["http"] = _a7_fix_http
        globals()["PDFTOTEXT"] = _a7_bin3
        subprocess.run = _a7_fix_run
    _a7_pentry = next(((c, k) for c, k in _a7_prun if "-f" in c), ([], {}))
    _a7_pargv, _a7_pokw = _a7_pentry
    check("B7-21 PDF 预算=下载8+解析6且给解析预留 PDF_TIMEOUT; pdftotext 限页+RLIMIT_AS (A3/A5)",
          BUDGET_S + PDF_BUDGET_S <= ROUND_SOFT_S
          and PDF_BUDGET_S >= 14.0 and PDF_DL_BUDGET_S >= 8.0
          and abs(PDF_DL_BUDGET_S - (PDF_BUDGET_S - PDF_TIMEOUT)) < 1e-9
          and _a7_pkw.get("deadline", 1e18) <= _a7_now2 + PDF_DL_BUDGET_S + 0.5
          and _a7_pkw.get("deadline", 1e18) <= _a7_now2 + PDF_BUDGET_S - PDF_TIMEOUT + 0.5
          and "-f" in _a7_pargv and _a7_pargv[_a7_pargv.index("-f") + 1] == "1"
          and "-l" in _a7_pargv and _a7_pargv[_a7_pargv.index("-l") + 1] == str(PDF_MAX_PAGES)
          and PDF_RLIMIT_AS == 256 * 1024 * 1024
          and callable(_a7_pokw.get("preexec_fn"))
          and "setrlimit" in inspect.getsource(_pdf_child_limits),
          f"dl={_a7_pkw.get('deadline')} argv={_a7_pargv}")

    # ---- B1: 未来节流时间戳 -> 按未运行处理 (下一轮必须真跑并改写)
    with tempfile.TemporaryDirectory() as _td:
        _b1_clk = _B6Clock(1793800000)
        _b1_calls: list[int] = []

        def _b1_src(deadline=None):
            _b1_calls.append(1)
            return [research_item("东财研报", "AP1", "沪铜周报：库存去化",
                                  "https://x/1.pdf", int(_b1_clk.time()) - 10,
                                  "某证券 · 2026-11-06", "")]

        _b1_r = _b6_run_main(Path(_td), _b1_clk, [("东财研报", _b1_src)],
                             state={"seedmarker": 1,
                                    "_research": {"东财研报": 1793800000 + 7200}})
    check("B7-22 main 对未来研报节流时间戳自愈(真跑并改写为当前时刻) (B1)",
          len(_b1_calls) == 1
          and _b1_r[2].get("_research", {}).get("东财研报") == 1793800000,
          f"calls={len(_b1_calls)} ra={_b1_r[2].get('_research')}")

    # ---- B2①: 第二轮 30 分钟节流生效 (main 级)
    with tempfile.TemporaryDirectory() as _td:
        _b2_clk = _B6Clock(1793800000)
        _b2_calls: list[int] = []

        def _b2_src(deadline=None):
            _b2_calls.append(int(_b2_clk.time()))
            return [research_item("东财研报", "AP1", "沪铜周报：库存去化",
                                  "https://x/1.pdf", int(_b2_clk.time()) - 10,
                                  "某证券 · 2026-11-06", "")]

        _b2_r1 = _b6_run_main(Path(_td), _b2_clk, [("东财研报", _b2_src)],
                              state={"seedmarker": 1})
        _b2_clk.t += 60
        _b2_clk.mono += 1.0
        _b2_buf, _b2_stdout = io.StringIO(), sys.stdout
        try:
            sys.stdout = _b2_buf
            _b2_r2 = _b6_run_main(Path(_td), _b2_clk, [("东财研报", _b2_src)])
        finally:
            sys.stdout = _b2_stdout
        _b2_log = _b2_buf.getvalue()
    check("B7-23 main 第二轮研报源 30 分钟节流生效(不重复抓取) (B2①)",
          len(_b2_calls) == 1 and "跳过 (研报源" in _b2_log
          and _b2_r2[2].get("_research", {}).get("东财研报") == 1793800000,
          f"calls={_b2_calls} log={_b2_log[-120:]!r}")

    # ---- B2②: 摘要未取到的研报当轮不入队, 下一研报轮补齐 (main 级)
    with tempfile.TemporaryDirectory() as _td:
        _b3_clk = _B6Clock(1793800000)
        _b3_pdf_calls: list[str] = []
        _b3_pdf0 = globals()["pdf_text"]
        globals()["pdf_text"] = lambda url, deadline=None, referer=None: (
            _b3_pdf_calls.append(url) or f"1-8月全国期货市场累计成交量同比增长20.36%{len(_b3_pdf_calls)}")

        def _b3_three(deadline=None):
            return [research_item("东财研报", f"AP{i}", f"沪铜周报{i}：库存去化{i}",
                                  f"https://x/{i}.pdf", int(_b3_clk.time()) - 10,
                                  "某证券 · 2026-11-06") for i in range(3)]

        try:
            _b3_r1 = _b6_run_main(Path(_td), _b3_clk, [("东财研报", _b3_three)],
                                  conf_extra={"digest_max_items": 40,
                                              "digest_min": 120},
                                  state={"seedmarker": 1})
            _b3_q1 = _b3_r1[3]
            _b3_clk.t += RESEARCH_EVERY_S
            _b3_clk.mono += 1.0
            _b3_r2 = _b6_run_main(Path(_td), _b3_clk, [("东财研报", _b3_three)],
                                  conf_extra={"digest_max_items": 40,
                                              "digest_min": 120})
            _b3_q2 = _b3_r2[3]
        finally:
            globals()["pdf_text"] = _b3_pdf0
    check("B7-24 main 摘要未取到的研报当轮不入队、下一研报轮补齐 (B2②)",
          len(_b3_q1) == 2 and len(_b3_q2) == 3 and len(_b3_pdf_calls) == 3
          and all(q.get("snippet") for q in _b3_q2),
          f"q1={len(_b3_q1)} q2={len(_b3_q2)} calls={len(_b3_pdf_calls)}")

    # ---- B2③: 研报条目走 research_reason, 非研报 filter_reason (main 级)
    with tempfile.TemporaryDirectory() as _td:
        _b4_clk = _B6Clock(1793800000)

        def _b4_report(deadline=None):
            return [research_item("中金所", "m202610",
                                  "2026年8月全国期货市场交易情况简报",
                                  "https://x/m.pdf", int(_b4_clk.time()) - 10,
                                  "中国期货业协会", "")]

        _b4_r = _b6_run_main(Path(_td), _b4_clk, [("中金所", _b4_report)],
                             state={"seedmarker": 1})
    check("B7-25 main 研报条目走 research_reason 而非 filter_reason (B2③)",
          len(_b4_r[3]) == 1 and _b4_r[3][0]["key"] == "研究:中金所:m202610",
          f"queue={_b4_r[3]}")

    # ---- B2④: 主流程用 it["key"] (稳定研报键), 不用 dedupe_key(text) (main 级)
    with tempfile.TemporaryDirectory() as _td:
        _b5_clk = _B6Clock(1793800000)

        def _b5_item(deadline=None):
            return [research_item("东财研报", "AP999", "沪铜周报：库存去化",
                                  "https://x/9.pdf", int(_b5_clk.time()) - 10,
                                  "某证券 · 2026-11-06", "")]

        _b5_r = _b6_run_main(Path(_td), _b5_clk, [("东财研报", _b5_item)],
                             state={"seedmarker": 1})
    check("B7-26 main 队列 key 取自 it['key'] 而非 dedupe_key(text) (B2④)",
          len(_b5_r[3]) == 1 and _b5_r[3][0]["key"] == "研究:东财研报:AP999",
          str(_b5_r[3])[:160])

    # ---- B3: 中金所日/周报是 CSV, 绝不能进 pdf_text; 月报才解析
    _b6_ok_urls: list[str] = []
    _b6_hok = globals()["http_ok"]
    globals()["http_ok"] = lambda url, deadline=None, timeout=6: (
        _b6_ok_urls.append(url) or True)
    try:
        _b6_cffex = src_cffex(time.monotonic() + 5)
    finally:
        globals()["http_ok"] = _b6_hok
    _b6_m = [x for x in _b6_cffex if x["key"].startswith("研究:中金所:m")]
    _b6_d = [x for x in _b6_cffex if x["key"].startswith("研究:中金所:d")]
    _b6_w = [x for x in _b6_cffex if x["key"].startswith("研究:中金所:w")]
    _b6_pdf_calls: list[str] = []
    _b6_pdf0 = globals()["pdf_text"]
    globals()["pdf_text"] = lambda url, deadline=None, referer=None: (
        _b6_pdf_calls.append(url) or "不应被调用的正文")
    try:
        _b6_ecalls, _b6_echg = extract_research([], _b6_d + _b6_w)
    finally:
        globals()["pdf_text"] = _b6_pdf0
    check("B7-27 中金所日/周报(CSV)不会进 pdf_text, 月报才解析 (B3)",
          len(_b6_m) == 1 and len(_b6_d) == 1 and len(_b6_w) == 1
          and _b6_d[0]["url"].endswith("_1.csv") and _b6_d[0]["snippet"] == ""
          and _b6_w[0]["url"].endswith("_1.csv") and _b6_w[0]["snippet"] == ""
          and _b6_m[0]["url"].endswith("MonthlyReport.pdf") and _b6_m[0]["snippet"] is None
          and _b6_ecalls == 0 and not _b6_pdf_calls,
          f"m={_b6_m} d={_b6_d} w={_b6_w} calls={_b6_ecalls}")

    _b7_em_urls: list[str] = []

    def _b7_em_http(url, *a, **k):
        _b7_em_urls.append(url)
        return _a7_em_json.encode()

    globals()["http"] = _b7_em_http
    try:
        _b7_em = src_em_report(time.monotonic() + 5)
    finally:
        globals()["http"] = _a7_http
    _b7_qtypes = [re.search(r"[?&]qType=(\d+)", u).group(1) for u in _b7_em_urls]
    check("B7-28 东财研报按 qType=1+2 两次请求, 不回退 qType=0 (B3)",
          _b7_qtypes == ["1", "2"] and "qType=0" not in " ".join(_b7_em_urls)
          and len(_b7_em) == 1, f"qtypes={_b7_qtypes} got={len(_b7_em)}")

    # ---- C1: 协议白名单 (渲染 + 入队)
    _c1_js = research_item("东财研报", "AP1", "沪铜周报：库存去化",
                           "javascript:alert(document.cookie)", _a7_ts,
                           "某证券 · 2026-11-06", "摘要")
    _c1_data = research_item("东财研报", "AP2", "沪铜周报：库存去化",
                             "data:text/html;base64,PHNjcmlwdD4=", _a7_ts,
                             "某证券 · 2026-11-06", "摘要")
    _c1_msg = build_messages([_c1_js])[0][0]
    _c1_line = research_line(_c1_js, _a7_dt)
    _c1_q: list[dict] = []
    queue_upsert(_c1_q, [_c1_js, _c1_data])
    check("B7-29 非 http(s) URL 不渲染成链接且不入队 url (协议白名单三处统一) (C1)",
          "<a href" not in _c1_msg and "javascript:" not in _c1_msg
          and "<a href" not in _c1_line
          and len(_c1_q) == 2 and all("url" not in q for q in _c1_q),
          f"msg={_c1_msg[:80]!r} q={_c1_q}")

    # ---- C2: 研报标题/元信息/摘要的 HTML 实体化
    _c2_it = research_item("东财研报", "AP3", "沪铜<b>深度</b>", "https://x/3.pdf",
                           _a7_ts, "某证券 & <i>2026</i>",
                           '摘要 "研报" <script>alert(1)</script>')
    _c2_line = research_line(_c2_it, _a7_dt)
    check("B7-30 研报标题/元信息/摘要的 < & \" 全部实体化 (C2)",
          "&lt;b&gt;深度&lt;/b&gt;" in _c2_line
          and "某证券 &amp; &lt;i&gt;2026&lt;/i&gt;" in _c2_line
          and "&quot;研报&quot;" in _c2_line
          and "&lt;script&gt;" in _c2_line
          and "<b>深度" not in _c2_line,
          _c2_line[:160])

    # ---- C3: 超长 URL/元信息 -> 单条 u16 <= MSG_MAX (可正常拆批)
    _c3_url = "https://x.example/" + "a" * 4500
    _c3_it = research_item("东财研报", "AP4", "沪铜周报：库存去化", _c3_url,
                           _a7_ts, "某证券 · 2026-11-06", "摘要")
    _c3_line = research_line(_c3_it, _a7_dt)
    _c3_msg = build_messages([_c3_it])[0][0]
    _c3_meta_it = research_item("东财研报", "AP5", "沪铜周报：库存去化",
                                "https://x/5.pdf", _a7_ts, "机" * 4000, "摘要")
    _c3_line2 = research_line(_c3_meta_it, _a7_dt)
    check("B7-31 超长 URL/元信息被约束, 单条 u16 <= MSG_MAX 可拆批 (C3)",
          u16(_c3_line) <= MSG_MAX and "<a href" not in _c3_line
          and "链接过长" in _c3_line and u16(_c3_msg) <= MSG_MAX
          and u16(_c3_line2) <= MSG_MAX,
          f"url_line={u16(_c3_line)} meta_line={u16(_c3_line2)}")

    # ---- C 轻微: queue_upsert 字段语义 + 非字符串 url 容错
    _c4_items = [
        {"src": "财联社", "ts": _a7_ts, "text": "沪铜夜盘上涨1.2%", "key": "k1"},
        {"src": "中期协", "ts": _a7_ts, "text": "简报｜协会", "key": "研究:中期协:1",
         "url": "https://x/1.pdf", "snippet": None},
        {"src": "中期协", "ts": _a7_ts, "text": "简报2｜协会", "key": "研究:中期协:2",
         "url": "https://x/2.pdf", "snippet": ""},
        {"src": "中期协", "ts": _a7_ts, "text": "简报3｜协会", "key": "研究:中期协:3",
         "url": "javascript:alert(1)", "snippet": "s"},
        {"src": "中期协", "ts": _a7_ts, "text": "简报4｜协会", "key": "研究:中期协:4",
         "url": 123, "snippet": "s"}]
    _c4_q: list[dict] = []
    _c4_added, _c4_dup = queue_upsert(_c4_q, _c4_items)
    check("B7-32 queue_upsert: 4字段键序/None不落盘/\"\"落6字段/非http不落url (C 轻微)",
          _c4_added == 5 and _c4_dup == 0
          and list(_c4_q[0].keys()) == ["src", "ts", "text", "key"]
          and list(_c4_q[1].keys()) == ["src", "ts", "text", "key", "url"]
          and list(_c4_q[2].keys()) == ["src", "ts", "text", "key", "url", "snippet"]
          and "url" not in _c4_q[3] and "url" not in _c4_q[4],
          str([list(q.keys()) for q in _c4_q]))

    _c5_it = {"src": "东财研报", "ts": _a7_ts,
              "text": "沪铜周报：库存去化｜某证券", "url": 123, "snippet": "摘要",
              "key": "研究:东财研报:AP6"}
    _c5_msg = build_messages([_c5_it])[0][0]
    _c5_line = research_line(_c5_it, _a7_dt)
    check("B7-33 research_line/build_messages 对非字符串 url 容错(int 不抛 AttributeError) (C 轻微)",
          "<a href" not in _c5_msg and "<a href" not in _c5_line
          and "沪铜周报" in _c5_msg,
          _c5_msg[:80])

    print(f"selftest: {'PASS' if not fails else str(len(fails)) + ' FAILED: ' + ', '.join(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
