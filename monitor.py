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

import ast
import difflib
import email.utils
import errno
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.client import IncompleteRead   # 不能 import http.client: 会被 def http() 覆盖
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
# 近邻合并方向否决 (Batch 9 C-F-01 / batch9-fix U3-F-01): 反义方向词的两条不同事件不得按字面
# 相似度合并. 方向检测产出**集合** {(轴, 符号)}：同一句里「涨跌互现」= 同一轴双符号并存,
# 仍可与同形句合并；而「新增」里的「增」不得把一侧误判成上行（中性复合词先屏蔽）。
# 规模口径（准确数）：21 对反义词 = 原 13 对（11 条中文轴 + 累增/回落、升水/贴水 2 对扩展）
#   + 补 8 对反义缺口（下滑/上行、走弱/走强、下挫/冲高、跳水/拉升、扩产/停产、复产/停产、
#     反弹/回落、加息/降息）。轴 = 同义方向词族；同族同向只记一个 (轴, +1/-1)，
# 所以「上涨」vs「上升」不会因措辞不同被误判为方向相反。
DIR_AXES = (
    ("价格",
     ("涨", "上涨", "上升", "上行", "走强", "冲高", "拉升", "反弹", "走高",
      "升", "上调", "升水", "加息"),
     ("跌", "下跌", "下降", "下滑", "走弱", "下挫", "跳水", "回落", "走低",
      "降", "下调", "贴水", "降息")),
    ("数量",
     ("增", "增加", "增产", "扩产", "复产", "扩张", "累库", "累增"),
     ("减", "减少", "减产", "停产", "收缩", "去库", "负增长")),
)
# 中性复合词：含方向字但本身不表方向（或同时含两侧），匹配前整词屏蔽，避免单字子串误判.
# 「增」不得匹配「新增」；「涨跌互现」不得被当成双向并存之外的任何方向.
DIR_NEUTRAL = ("涨跌互现", "涨跌幅", "涨跌", "增减持", "增减", "升降",
               "新增", "增速", "增量", "增幅")
_DIR_TOKEN_SIGNS: dict[str, tuple[str, int]] = {}
for _axis, _ups, _downs in DIR_AXES:
    for _w in _ups:
        _DIR_TOKEN_SIGNS[_w] = (_axis, 1)
    for _w in _downs:
        _DIR_TOKEN_SIGNS[_w] = (_axis, -1)
del _axis, _ups, _downs, _w
# 长词优先：同一位置先匹配「上涨/下降」等整词，不会再拆出「涨/降」.
_DIR_TOKEN_RE = re.compile(
    "|".join(re.escape(w) for w in sorted(_DIR_TOKEN_SIGNS, key=len, reverse=True)))
_DIR_NEUTRAL_RE = re.compile(
    "|".join(re.escape(w) for w in sorted(DIR_NEUTRAL, key=len, reverse=True)))
# 兼容旧接口的扁平词表（selftest 成员检查 / 外部探针）.
DIR_UP = tuple(w for _a, ups, _d in DIR_AXES for w in ups)
DIR_DOWN = tuple(w for _a, _u, downs in DIR_AXES for w in downs)
# 只认「带量词/单位」的中文数字串: 「一万二千吨」「一万吨」; 「一致」「进一步」不算 (Batch 9 C-F-02)
CN_NUM_RE = re.compile(r"[零〇一二三四五六七八九十百千万亿两]+(?=[吨桶手元%个家座台辆天周月年倍成])")
ALERT_EVERY = 3600                   # 全源故障期间两条告警的最小间隔(秒)
SRC_ALERT_ROUNDS = 6                 # 单源连续失败多少轮发告警 (可被 conf.src_alert_rounds 覆盖)
SRC_ALERT_EVERY = ALERT_EVERY        # 单源告警抑制窗口(秒), 与全源告警保持一致
SRC_FAIL_KEY = "_src_fail"           # seen 里的单源失败状态块; '_' 前缀保证 prune 保留
META_KEYS = ("_alert", "_digest", "_day", "_src_fail", "_research")   # prune 白名单: 新增元数据必须登记
SRC_SKIP = "__budget_skip__"         # outcomes 哨兵: 全局预算跳过, 既不算成功也不算失败 (Batch 6 U4)
BUDGET_S = 45                        # 全局抓取预算(秒): BUDGET_S + 单次 send 最坏(按 send_plan() 推导) + 本地开销 < 120s
# 单源最小份额(秒)依据: 生产日志 626 轮实测 p99/max —— 见闻商品 5.40/7.50s、Nasdaq 4.10/4.20s、
# 东财 max 5.20s; 5.0s 会把见闻商品约 1% 的轮次砍成失败, 故抬到 7.0 (Batch 6.1 U-B / fix C1)
MIN_SLICE = 7.0                      # 单源抓取最小份额(秒)
# curl 退出码 -> 人话 (Batch 8 失败归因): 28 超时 / 6,7,35 连接(SSL/DNS) / 16 HTTP/2 帧 / 63 体积上限
# 注: 按评审 F-01/F-02 撤回「按剩余次数切分」, 只保留归因; 单次预算仍是一次用满剩余份额.
CURL_RC_REASON = {28: "curl 超时", 6: "连接失败(SSL/DNS)", 7: "连接失败(SSL/DNS)",
                  35: "连接失败(SSL/DNS)", 16: "HTTP/2 帧错误", 63: "超过体积上限"}

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
PDF_MAX_PAGES = 32                   # 只解析前 N 页 (Batch 11: 8->32); 依据: 实测 3 份真实 PDF 4/5/9 页,
                                     # 32 页 = 3.6x 余量, 且 32x~722 字/页 <= SNIPPET_MAX(24000)
RESEARCH_URL_MAX = 1000              # 研报链接上限; 超长 URL 会让单条 u16>4096 且 split_msg 无法拆
RESEARCH_URL_U16_MAX = 1000          # 研报链接**转义后**的 u16 预算 (& -> &amp; 放大 5 倍); 超了退纯文本
RESEARCH_META_MAX = 200              # 研报元信息(text 的「机构 · 日期」段)上限
PDF_MIN_CHARS = 40                   # 清洗后正文短于此数 -> 判「无文本层」, 摘要留空
SNIPPET_MAX = 24000                  # 正文存储上限(字符): 240 -> 24000 (Batch 11 全文推送, 不再截断)
SNIPPET_FULL_MAX = 40000             # 清洗后正文的解析侧硬上限(字符): 高于 SNIPPET_MAX, 让截断可被检出并标记
RESEARCH_TRUNC_MARK = "\n…（正文共 {n} 字，超过存储上限 {limit} 字，已截断）"
RESEARCH_PART_MAX = 2400             # 单片正文的 esc u16 预算: 头行+标题+链接后仍 <= MSG_MAX(3800)
RESEARCH_PART_MIN_FILL = 2000        # 句末切分的最小填充: 已用 u16 >= 此值才按句末切, 否则硬切(防碎片)
RESEARCH_PARTS_PER_ROUND = 12        # 每轮最多发出的研报分片数: 约 6 msg/min, 低于单聊经验限流
RESEARCH_TITLE_PART_MAX = 80         # 带链接末片的标题 esc u16 上限(其余片沿用 ITEM_MAX)
RESEARCH_PART_MARK = "({i}/{n})"     # 分片标记: 每片头行追加, i 从 1 起, n 为总片数
RESEARCH_SENT_END = "。！？；!?;…"    # 句子/分句边界: 分片优先在这些字符之后切
RESEARCH_PARTIAL_AGE_FACTOR = 4      # 分片连载条目的老化豁免倍数: 超过 4*max_age 才允许老化
RESEARCH_PART_FAIL_MAX = 3           # [CTO] 同一片连续失败上限: 达 3 次判毒丸, 出队+seen+err
RESEARCH_TAG = "研究"                # 研报 key 前缀 (含 ':' -> 与 dedupe_key 输出天然不相交)
RESEARCH_SRCS = ("东财研报", "中金所", "中期协")
# pdftotext 为 poppler CLI (非 Python 依赖); 不存在时 pdf_text 直接降级为「无摘要」
PDFTOTEXT = (os.environ.get("FUTNEWS_PDFTOTEXT")
             or next((p for p in ("/opt/homebrew/bin/pdftotext", "/usr/local/bin/pdftotext")
                      if os.path.exists(p)), ""))

ROUND_HARD_S = 120                   # launchd StartInterval 硬上界(秒): 单轮总耗时不得超过
ROUND_MARGIN_S = 6                   # 本地阶段(锁/落盘/过滤)余量(秒)
SEND_GATE_S = ROUND_HARD_S - ROUND_MARGIN_S   # 发送预算硬顶(秒): 闸门与软截止的唯一派生源
ROUND_START = 0.0                    # main 每轮起点(time.monotonic); 逐次发送闸门共用 (Batch 6 F-02)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
def proxy_from_env() -> str:
    """代理环境变量口径: 大写优先, 兼容 curl 同样认识的小写形式 (Batch 9 B-D-05)."""
    return os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or ""


PROXY = proxy_from_env()   # 代理只取自环境变量, 不再内置默认地址；未设置则纯直连
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

# ---- Batch 10 R6: 聚合/退避/回拨阈值 (值=基线字面量, 只命名不改值)
ROLLBACK_TOL_S = 60                  # 时钟回拨/前跳容差: last_flush/last_round/_research 三处共用
LEFT_PENDING_RETRY_S = 60            # 有剩余待发时的最短重试间隔(秒)
LONG_GAP_S = 3600.0                  # 判定「长间隔/休眠」的秒数: 关老化、先补发
BLOCK_ACC_CAP_S = 7 * 86400.0        # block_acc 封顶(秒), 只作诊断与门闩
BLOCK_ACC_STEP_MIN_S = 120.0         # block_acc 单轮最小累加步长(秒)
CAP_NOTICE_RETRY_S = 600.0           # 触顶提示发送失败后的重试间隔(秒)
RETRY_BASE_S = 120                   # 发送失败指数退避基数(秒)
RETRY_MAX_S = 1800                   # 发送失败退避封顶(秒), 与 RA_MAX 一致
RETRY_EXP_CAP = 4                    # 指数退避的指数封顶: 120*2^4=1920 -> 被 1800 截断
MAX_HOLD_FLOOR_S = 300               # max_hold_min 的下限(秒)
BUDGET_FLOOR_S = 15                  # timeout_s 的下限(秒)
RESEARCH_RETRY_S = 300               # 研报源失败后 5 分钟重试: now - (1800 - 300)

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
    "股权", "收购", "营业利润", "净利润", "毛利率", "每股收益",
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


# ------------------------------------------------------------------ R3 上下文 (无状态视图)
class RoundContext:
    """本轮上下文。所有字段都是模块级测试接缝的**视图**，不复制状态。"""

    __slots__ = ()

    @property
    def start(self) -> float:
        return ROUND_START

    @start.setter
    def start(self, value: float) -> None:
        global ROUND_START
        ROUND_START = value

    @property
    def cfg(self) -> dict:
        return CFG

    @property
    def ts_bad(self) -> dict[str, int]:
        return TS_BAD


class SendContext:
    """发送上下文。所有字段都是模块级测试接缝的**视图**，不复制状态。"""

    __slots__ = ()

    @property
    def err(self) -> str:
        return SEND_ERR

    @err.setter
    def err(self, value: str) -> None:
        global SEND_ERR
        SEND_ERR = value

    @property
    def retry_after(self) -> float | None:
        return SEND_RETRY_AFTER

    @retry_after.setter
    def retry_after(self, value: float | None) -> None:
        global SEND_RETRY_AFTER
        SEND_RETRY_AFTER = value


_DEFAULT_ROUND = RoundContext()
_DEFAULT_SEND = SendContext()


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
    """按插入顺序截断 SEEN_MAX, 只保留显式白名单 META_KEYS 的 '_' 键 (Batch 9 A-D-08①).

    白名单化: 未登记的 '_' 键直接丢弃 (而不是靠 '_' 前缀静默存活), 新增元数据必须
    登记 META_KEYS; 业务键按既有规则截到 SEEN_MAX, 且 dedupe_key 产出永不带 '_' 前缀。
    """
    meta = {k: state[k] for k in META_KEYS if k in state}
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


def esc_quote_limited(s, limit: int) -> str:
    """按 esc_quote 转义后截到 u16 <= limit; 截断不劈开实体, 超限补 '…' (Batch 9 R-E-F-01).

    先转义再量长: esc_quote 会把 `"` 放大 6 倍、`&` 放大 5 倍; 按转义前 len() 截断
    会得到远超预算的行 (Telegram 按 u16 计 4096 上限, MSG_MAX=3800 是留余量后的阈值)。
    """
    out: list[str] = []
    used = 0
    cut = False
    for ch in str(s or ""):
        piece = esc_quote(ch)
        need = u16(piece)
        if used + need + 1 > limit:          # 预留 '…' 的 1 个 u16
            cut = True
            break
        out.append(piece)
        used += need
    return "".join(out) + ("…" if cut else "")


def is_http_url(u) -> bool:
    """URL 协议白名单 (Batch 7 修复 C1): 只有 http(s) 允许落盘与渲染成链接.

    与 load_queue 原口径一致; 非字符串 (int/None/dict) 一律 False, 不再抛 AttributeError。
    """
    return isinstance(u, str) and u.lower().startswith(("http://", "https://"))


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
            # Batch 11: 分片续发进度 (只有 0 < sent < total 的连载条目才保留, 其余自愈丢弃)
            ps, pt = it.get("parts_sent"), it.get("parts_total")
            if (isinstance(ps, int) and not isinstance(ps, bool)
                    and isinstance(pt, int) and not isinstance(pt, bool)
                    and 0 < ps < pt):
                out[-1]["parts_sent"] = ps
                out[-1]["parts_total"] = pt
            # Batch 11 (CTO): 毒丸计数也随队列持久化 (失败片跨轮累计, 老条目无此字段)
            pf = it.get("part_fails")
            if isinstance(pf, int) and not isinstance(pf, bool) and pf > 0:
                out[-1]["part_fails"] = pf
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
    """同目录 temp + replace 原子写文本 + fsync (存档轮转 / dump 导出共用).

    失败径 best-effort 清扫自己那只 name.tmp.<pid> 孤儿 (W3-P3: 写穿/replace
    失败留下的半截 tmp 不随下次调用自动消失, 只能在失败现场回收)。
    """
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")   # pid 后缀: 无锁并发 dump/轮转同 PATH 不互撕
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
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
        try:
            tmp.unlink()                 # 孤儿 tmp 清扫: replace 失败/写中断残留
        except OSError:
            pass                         # best-effort: tmp 未落盘/不可删都不掩盖原始错误
        err(f"atomic_write_text 失败 {path.name}: {type(e).__name__}: {e}")
        return False


def read_items_archive() -> list[dict]:
    """读 JSONL 存档 -> 条目列表; 文件不存在=空 (dump / 去重共用).

    容错口径: 单行 JSON 解析失败的坏行跳过; 非法 UTF-8/IO 错在迭代中途抛出时
    返回「已读前缀」 (prefix-return, 不是整文件判废)——完整行保留, 撕裂/腐化
    后缀留给 items_archive_append 的自愈截断收拾。
    """
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


def _archive_truncate(size: int) -> bool:
    """items 存档自愈截断: 原地砍到 size 字节 (弃除撕裂残尾/非法字节尾).

    读径遇非法 UTF-8 或追加前发现撕裂残行时调用——O_APPEND 无条件接在旧尾
    后面写, 残行不除则新首行被并进残行成不可解析行 (永久缺档 + 报数造假 +
    合并行被 old_lines 带入轮转长期留存)。ftruncate 同 inode 生效, 不改
    目录项, 无需父目录 fsync。
    """
    try:
        with open(ITEMS_ARCHIVE, "r+b") as fh:
            fh.truncate(size)
            fh.flush()
            os.fsync(fh.fileno())                  # 截断也要 fsync: 崩溃后不接受「截了又复活」
        return True
    except OSError as e:
        err(f"items 存档自愈截断失败 {ITEMS_ARCHIVE.name}: {type(e).__name__}: {e}")
        return False


def items_archive_append(new_items: list[dict]) -> int:
    """items 存档真追加 + key 去重 + 行数轮转; 返回实际落档行数.

    契约 (spec §3.11 / T-31 / T-32 / R-9):
      - 行形 {"src","ts","text","key","url"?,"snippet"?,"schema":1}, JSONL 真追加 (非 JSON 数组)
      - 存档侧 key 集合去重: 已归档 key 跳过 (重复命中不重复落档); 调用方只传「本轮新进队列」的条目
      - 追加用 O_APPEND + view 循环 write (部分写只续写剩余 view, 整批重写会重复落档)
        + close 前 fsync; 追加后行数 > ITEMS_ARCHIVE_MAX 则原子重写保留最近 ITEMS_ARCHIVE_KEEP 行
      - 存档读不出 -> 去重无据, 宁可不追加也不重复落档; 写失败只记 err, 不影响主流程退码 (队列才是真源)
      - 撕裂自愈双闸 (W3-P2): 读径逐行验 UTF-8, 首个不可解码行起连同其后整尾
        ftruncate 弃除 (UTF-8 中切碎片的旧径 ValueError abort 会冻结追加); 追加前
        查末字节非 \\n 的残尾——能解出完整 JSON 对象的补 \\n 隔离保留 (部分写恰停在
        记录边界的记录不丢), 否则截除, 再写 payload (新旧首行永不并线)
      - meta schema 闸自愈每次调用即执行 (读存档与空批次判之前), 不被全 dup 轮/读失败闸住
    """
    meta = load_json(ITEMS_META, None)                # §3.9 schema 闸: 缺失/版本不符时自愈重写
    if not (isinstance(meta, dict) and meta.get("schema") == ITEMS_SCHEMA):
        save_json(ITEMS_META, {"schema": ITEMS_SCHEMA})
    if not new_items:
        return 0
    keys: set[str] = set()
    old_lines: list[str] = []
    try:
        blob = ITEMS_ARCHIVE.read_bytes()
    except FileNotFoundError:
        blob = b""                                    # 首轮: 无存档 = 空 key 集合
    except OSError as e:
        err(f"items 存档读取失败, 本轮跳过追加 {ITEMS_ARCHIVE.name}: {type(e).__name__}: {e}")
        return 0
    # ---- 自愈闸①: 首个非法 UTF-8 行 / 撕裂残尾 -> 截到上一完整行尾 (W3-P2)
    # 完整行 = 有 \n 终止且可解码; 无 \n 残尾单独判 (完整 JSON 对象才留)
    cut = len(blob)
    pos = 0
    while pos < len(blob):
        nl = blob.find(b"\n", pos)
        if nl < 0:                                    # 残尾段 (EOF 前无 \n)
            try:
                keep_tail = isinstance(json.loads(blob[pos:].decode("utf-8")), dict)
            except (UnicodeDecodeError, ValueError, RecursionError):
                keep_tail = False                     # 撕裂残片/非对象/病态嵌套 JSON -> 截除
            if not keep_tail:
                cut = pos
            break
        try:
            blob[pos:nl].decode("utf-8")
        except UnicodeDecodeError:
            cut = pos                                 # 非法字节行连同其后整尾弃除
            break
        pos = nl + 1
    if cut < len(blob):
        if not _archive_truncate(cut):
            return 0                                  # 愈不了 -> 去重无据, 宁可不追加
        log(f"items 存档自愈: 截除尾部 {len(blob) - cut} 字节撕裂/非法残片")
        blob = blob[:cut]
    for raw in blob.split(b"\n"):                     # 键集/轮转底账: 愈合后全部可解码
        s = raw.decode("utf-8").strip()
        if not s:
            continue
        old_lines.append(s)
        try:
            k = json.loads(s).get("key")
        except (json.JSONDecodeError, AttributeError, RecursionError):
            continue                                  # 坏行照原样保留, 仅不参与 key 集合
        if k:
            keys.add(str(k))
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
    if blob and not blob.endswith(b"\n"):
        # 自愈闸①后半: 保留的完整残尾记录缺 \n 终止 -> payload 前置 \n 隔离,
        # 否则 O_APPEND 把新首行并进残行成不可解析行 (W3-P2)
        payload = b"\n" + payload
    view = memoryview(payload)                        # 重试只续写剩余 view: 部分写后整批重写会重复落档
    last = ""
    for i in range(3):                                # 与 save_json 同口径的轻量重试
        try:
            fd = os.open(ITEMS_ARCHIVE, os.O_WRONLY | os.O_CREAT | os.O_APPEND
                         | getattr(os, "O_BINARY", 0))   # O_APPEND 单次 open + view 循环 write; Windows 需 O_BINARY 保 LF
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


# --dump-items 目标守卫 (P3-8 + W3): resolve 归一后命中任一 state/conf/log 件即拒——
# JSON 快照覆盖 JSONL 存档/seen/queue/pending/meta/conf 会毁形真源;
# 覆盖 LOCK 会把锁文件换成新 inode, 持旧 inode 锁者与后继 flock 者可并发进入;
# 覆盖 LOG/ERRLOG 是 launchd 原地轮转件, 换 inode 后日志流写进死 inode (U3 同形自伤)。
# resolve 比对之外补 samefile inode 判据 (W3-P2): POSIX resolve 不归一大小写,
# APFS/NTFS 大小写不敏感下 FUTURES-NEWS-SEEN.JSON 变体可绕 resolve 比对覆盖真源;
# samefile 顺带覆盖 NFC/NFD 归一差与硬链接同 inode 绕法。
# 清单在函数体内逐次取模块全局 (非 import 期绑死)——测试经 setattr
# 重定向路径后守卫仍生效, 与旧 ITEMS_ARCHIVE 单件守卫同口径。
def dump_items(path: Path) -> int:
    """--dump-items PATH: 存档原子导出 {"schema":1,"ts":<now>,"items":[...]} (spec §3.11).

    独立子命令语义: 不进抓取/锁/发送主路径; 返回 0 成功 / 1 落盘失败。
    """
    out = Path(path)
    resolved = out.resolve()
    for guarded in (ITEMS_ARCHIVE, ITEMS_META, STATE, QUEUE, PENDING,
                    LOCK, CONF, LOG, ERRLOG):         # 任一 state/conf/log 件命中即拒
        same = False
        try:
            same = os.path.samefile(out, guarded)     # inode 判据: 大小写变体/硬链接/NFC-NFD
        except OSError:
            pass                                      # 任一方不存在 -> 无同件, 只看 resolve 比对
        if resolved == guarded.resolve() or same:
            err(f"--dump-items 目标不能覆盖 state/conf/log 件 {guarded}")
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
    elif not isinstance(day.get("msgs"), int) or isinstance(day.get("msgs"), bool):
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


TAG_RE = re.compile(r"<\s*/?[A-Za-z][^>]*>")   # 只认 ASCII 标签名; 不吞「升水<150元」这类比较符 (Batch 9 C-F-03)
COMMENT_RE = re.compile(r"<!--.*?-->|<\?.*?\?>", re.S)   # HTML 注释 / XML 处理指令: 整段剥除 (Batch 9 U3-F-03)


def clean_text(raw) -> str:
    """去标签 + 反转义 HTML 实体 + 清理残缺标签/裸露属性残留 (U4); 非字符串一律 str() 兜底.

    Batch 9: ① 标签正则只认 ASCII 标签名, 比较运算符夹住的正文不再被当标签吞掉 (C-F-03);
    ② 同一正则不再对 `<` 密集输入构造二次方回溯 (C-F-04);
    ③ 实体反转义循环到稳定 (≤2 轮), 双重编码也幂等 (C-F-06);
    ④ 注释 `<!-- -->` 与处理指令 `<?xml ?>` 整段剥除, 与基线行为一致 (U3-F-03).
    """
    t = COMMENT_RE.sub("", str(raw or ""))
    t = TAG_RE.sub("", t)
    for _ in range(2):                       # 双重编码实体: 反转义到稳定即停 (幂等)
        u = COMMENT_RE.sub("", html.unescape(t).replace("\xa0", " "))
        u = TAG_RE.sub("", u)
        if u == t:
            break
        t = u
    t = TAG_RESIDUE.sub("", t)               # 缺 '>' 的残缺标签 (如 <span style=color:#ff0000)
    t = ATTR_RESIDUE.sub("", t)              # 裸露属性 (如 spanstyle=color:rgb(255,0,0))
    t = re.sub(r"[ \t\u3000]{2,}", " ", t)   # 收拾残留清掉后留下的多余空白/标点
    t = re.sub(r"\s+([，。、；：！？,.;:!?])", r"\1", t)
    return t.strip()


class HttpContractError(RuntimeError):
    """curl 结果对象缺少 returncode/stdout: 契约破坏, 按失败关闭且不重试."""


def http(url: str, referer: str | None = None, tries: int = 3,
         deadline: float | None = None, cap: int = 12,
         proxy: str | None = None, ua: str | None = None,
         max_bytes: int = 0, location: bool = False) -> bytes:
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

    Batch 8 修复失败归因 (只改诊断, 不动抓取时序):
      记录最近一次尝试的 curl 退出码, 抛错时按 rc 映射人话并带 rc 与耗时
      (如 `curl 超时 (rc=28, 6.1s)`), 不再把 curl 超时/SSL 失败误标成「预算耗尽」;
      只有一次尝试都没发起就因 left<2 退出, 才允许说预算耗尽并注明「未发起尝试」。
      注: 按评审 F-01/F-02, 「按剩余次数切分」因 8-9s 份额会两次各 4s、把正常
      4.1-6s 响应砍成两次失败而撤回; max_time 公式与基线逐字节等价。
    Batch 9: argv 首位 -q; deadline is None 才视为无预算; 末次尝试后不 sleep。
    """
    last = ""
    last_rc = None                       # 最近一次尝试的 curl 退出码 (Batch 8: 失败归因)
    hard_error = ""                      # strict 体积超限: 确定性失败, 立即上抛并保留原因 (Batch 9 B-F-02)
    started = time.monotonic()
    strict = max_bytes > 0
    for i in range(tries):
        left = (deadline - time.monotonic()) if deadline is not None else 30.0
        if left < 2:
            # 仅「一次尝试都没发起」才允许报预算耗尽; 已尝试过则按最近一次 rc 报真实原因
            if i == 0:
                raise RuntimeError(
                    f"http 预算耗尽 (未发起尝试, 剩余 {max(0.0, left):.1f}s)")
            break
        max_time = max(2, min(cap, int(left)))    # 与基线逐字节同款: 一次用满剩余份额
        cmd = ["curl", "-q", "-sS"]      # -q: 忽略 ~/.curlrc, 防止用户级配置改写抓取语义 (Batch 9 B-D-01)
        if location:                        # Batch 13: 东财详情 302 到 finance.eastmoney.com
            cmd += ["-L"]                   # 默认 False -> 既有源的 argv 逐元素不变
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
            if not hasattr(r, "returncode") or not hasattr(r, "stdout"):
                raise HttpContractError(
                    f"curl 结果缺少 returncode/stdout (fail-closed): {type(r).__name__}")
            rc = r.returncode
            if rc != 0:                               # A2: curl 中止/超时/超限一律失败
                last_rc = rc
                last = f"curl rc={rc}"
                raw = b""
            else:
                last_rc = None                        # 最近一次已正常返回, 旧 rc 不再代表现状
                raw = r.stdout
            body, _, code = raw.rpartition(b"\n")
            last = code.decode(errors="ignore").strip() if rc == 0 else last
            if body and last.startswith("2"):
                if strict and len(body) > max_bytes:  # A2/A4: 体积兜底 (chunked 无长度时)
                    hard_error = f"http failed (body 超上限 {len(body)}>{max_bytes})"
                    break                             # 不重试、不 sleep, 由循环后统一上抛
                return body
        except HttpContractError:
            raise
        except Exception as e:
            last_rc = None                            # 本次尝试没拿到 rc, 不拿旧 rc 冒充
            last = f"{type(e).__name__}"
        nap = float(1 + i)
        if deadline is not None:
            nap = min(nap, max(0.0, deadline - time.monotonic() - 1.0))
        if nap > 0 and i < tries - 1:     # 末次尝试之后不再 sleep (Batch 9 B-F-03)
            time.sleep(nap)
    if hard_error:
        raise RuntimeError(hard_error)   # 保留「body 超上限」字样, 不再退化成 http failed (RuntimeError)
    elapsed = time.monotonic() - started
    if last_rc is not None:
        reason = CURL_RC_REASON.get(last_rc) or f"curl rc={last_rc}"
        detail = f"rc={last_rc}, {elapsed:.1f}s"
        if last_rc == 63:
            detail = f"curl {detail}"                # 保留既有 B7-19 断言的 "curl rc=63" 字样
        raise RuntimeError(f"{reason} ({detail})")
    raise RuntimeError(f"http failed ({last or 'empty'})")


def http_ok(url: str, deadline: float | None = None, timeout: int = 6) -> bool:
    """HEAD 探测资源是否存在 (2xx): 只取状态码, 不下载 body (Batch 7).

    中金所的日/周报是 0.5MB 级 XML、非交易日/未发布时会 302 到错误页, 用 GET 探测
    等于每轮白拉半兆; 故只发 `curl -I`。网络失败/超时/参数非法一律返回 False,
    由调用方按「本期未发布」处理 (不抛错、不计入源失败)。
    """
    left = (deadline - time.monotonic()) if deadline is not None else 30.0
    if left < 1.5:
        return False
    t = max(1, min(timeout, int(left)))
    cmd = ["curl", "-q", "-sS", "--noproxy", "*", "-I", "--max-time", str(t),
           "-A", UA, "-o", "/dev/null", "-w", "%{http_code}", url]
    try:
        raw = subprocess.run(cmd, capture_output=True, timeout=t + 3).stdout
    except Exception:
        return False
    return raw.decode(errors="ignore").strip().startswith("2")


# ---------------------------------------------------------------- PDF 正文提取 (Batch 7)
# 只跑 poppler CLI; 任一失败都降级为「无摘要」
PDF_TABLE = re.compile(r"^\[?[Tt]able[_\-\w]*\]?$|^[图表]\s*\d")     # 版式标记/图表编号行
PDF_NUM = re.compile(r"^[\s\d.,%+\-—–/()（）亿元万股手吨桶美元]+$")   # 纯数字/单位行 (表格碎片)
PDF_CJK = re.compile(r"[\u4e00-\u9fff]")

# 尾部免责/版权/地址样板段 (Batch 14): batch11 全文推送后, PDF 末页的券商合规声明会独占
# 最后几条 (i/N) 消息。只在全文第一个声明标记处动刀, 再叠四条护栏。
PDF_TAIL_MIN_PCT = 0.55          # 声明起点必须落在全文 55% 之后
PDF_TAIL_MIN_KEEP = 600          # 裁完至少留下这么多字 (短报告不动刀)
PDF_TAIL_CONFIRM_LINES = 4       # 自我印证窗口: 4 行内至少命中 2 处标记
PDF_TAIL_REPEAT_MIN = 3          # 同一行出现 >= 3 次 = 页眉/页脚/水印, 不作裁切依据
PDF_TAIL_FRAG_MAX = 120          # 裁切点之前尾行清理的字符预算
PDF_TAIL_FRAG_LEFTOVER = 10      # 去掉残片段后剩余可打印字符 <= 此值 = 「整行基本就是它」(F-05)
PDF_TAIL_DECOR = r"[\s·•・\-–—*|│]"  # 行首允许的装饰符 (项目符号/破折号/竖线)
PDF_TAIL_MARKER = re.compile(
    r"免责声明|重要声明|法律声明|特别声明|分析师声明|分析师承诺|证券分析师承诺"
    r"|免责条款|投资评级说明|评级说明|著作权|版权所有|保留一切"
    r"|未经.{0,12}书面(?:授权|许可)|未经我方许可|版权仅为本公司所有|保留追究"
    r"|请阅读最后一页|证券投资咨询(?:业务|执业)资格|证券投资咨询业务的说明"
    r"|本报告仅供|本报告由.{0,30}证券|本报告的版权")
PDF_TAIL_BODY_HINT = re.compile(
    r"数据来源|资料来源|(?:图|表)\s*[:：]|图表\s*\d|单位\s*[:：]|万吨|元/吨|亿元"
    r"|同比|环比|库存|产量|开工率|进口量|出口量|现货价|期货价|Mysteel|Wind|Choice")
PDF_TAIL_NOISE = re.compile(
    r"发送给|eastmoney\.com|@[A-Za-z0-9._-]+\.(?:com|cn|net)"
    r"|^第\s*\d+\s*页|^p\d+$|^-\s*\d+\s*-$", re.I)
PDF_TAIL_ORPHAN = re.compile(
    r"^(?:证券研究报告|研究报告|公司研究|行业研究|策略研究|金融工程|晨会纪要|专题报告)$")
# 明确「非正文」的残片 (Batch 14 修复 F-01): 只用高精度模式, 不用长度/句末符启发式——
# 后者会把「长飞光纤光缆 107.2000」这类正文表格行当残片砍掉 (国信真实 PDF 29690->29636)。
# Batch 14 修复 F-05: 网址/邮箱/电话/邮编/门牌都是**子串**模式, 直接按命中整行删会把
# 「更多数据请见 www.mysteel.com 公布的周度库存…」这类完整正文句一起砍掉; 故删除判定
# 统一交给 _pdf_tail_frag (正文逃逸 + 行首锚定 + 「整行基本就是该残片」), 见其 docstring.
PDF_TAIL_FRAG = re.compile(
    r"(?:https?://|www\.)[^\s，。；、）)]*"                  # 网址 (整段)
    r"|[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"     # 邮箱
    r"|(?:电话|传真|总机|手机)\s*[:：]?\s*[\d\-—–()（）+ ]{6,}"  # 电话/总机号码
    r"|请阅读最后一页"                                      # 免责提示语
    r"|^第\s*\d+\s*页|^p\d+$|^-\s*\d+\s*-$"                  # 页码
    r"|邮编\s*[:：]\s*\d{5,}"                                # 地址块: 邮编
    r"|(?:路|街|大道|道)\s*\d+\s*号"                          # 地址块: 门牌
    , re.I)
# F-05 ①: 残片必须**从行首开始** (允许前置空白与 · - | 等装饰符) 才可整行删.
PDF_TAIL_FRAG_EDGE = re.compile(
    r"^(?:" + PDF_TAIL_DECOR + r"*)(?:"
    r"(?:https?://|www\.)[^\s，。；、）)]*"
    r"|[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
    r"|(?:电话|传真|总机|手机)\s*[:：]?\s*[\d\-—–()（）+ ]{6,}"
    r"|请阅读最后一页"
    r"|第\s*\d+\s*页|p\d+$|-\s*\d+\s*-$"
    r"|邮编\s*[:：]\s*\d{5,}"
    r"|(?:路|街|大道|道)\s*\d+\s*号"
    r")", re.I)
# F-05 ②: 正文逃逸 —— 含正文特征词的行一律不删. 比 PDF_TAIL_BODY_HINT 更宽 (多出
# 元/%/涨/跌/图:/表:), 且**只用于 pdf_tail_trim**, 不参与 pdf_tail_cut 的护栏③,
# 以免声明段里常见的「元/%」把真实声明段也放行.
PDF_TAIL_TRIM_ESCAPE = re.compile(
    r"数据来源|资料来源|库存|同比|环比|万吨|元|%|％|涨|跌|图\s*[:：]|表\s*[:：]")


def _pdf_tail_frag(s: str) -> bool:
    """该行是否「明确非正文」的残片 (Batch 14 修复 F-05).

    正文逃逸优先: 命中 PDF_TAIL_TRIM_ESCAPE (库存/同比/万吨/元/%/涨/跌/图:/表:/数据来源)
    的行整行保留 —— 一条含 URL 的完整正文句不能因为 URL 子串被当残片删掉。
    其余行只有两种情形才算残片:
      ① 残片从行首开始 (允许 · - | 等装饰符): 「https://…」「电话：0755-…」「邮编：100033」;
      ② 去掉残片片段后剩余可打印字符 <= PDF_TAIL_FRAG_LEFTOVER: 「这行基本就是这个
         URL/号码/地址」, 如「北京市西城区金融大街 7 号」。
    「长飞光纤光缆 107.2000」不含任何上述片段, 一律不删。
    """
    if PDF_TAIL_TRIM_ESCAPE.search(s):
        return False
    if PDF_TAIL_FRAG_EDGE.search(s):
        return True
    if PDF_TAIL_FRAG.search(s):
        rest = PDF_TAIL_FRAG.sub("", s)
        return len(re.sub(PDF_TAIL_DECOR, "", rest)) <= PDF_TAIL_FRAG_LEFTOVER
    return False


def pdf_tail_cut(lines: list[str], confirm: int | None = PDF_TAIL_CONFIRM_LINES) -> int:
    """返回尾部「免责/版权/地址样板段」的起始行下标; 判定不裁时返回 len(lines) (Batch 14).

    只在**全文第一个**声明标记处动刀, 再叠四条护栏, 缺一不裁 (宁可少裁, 不可误裁):
      ① 第一个标记落在全文 PDF_TAIL_MIN_PCT 之后 (正文小标题「风险提示」等够不着);
      ② 标记之前仍有 >= PDF_TAIL_MIN_KEEP 字正文 (短报告不清空);
      ③ 标记之后不得再有正文特征 (「数据来源/图表/库存/同比」…; 开源那份不裁);
      ④ 自我印证: 标记起 confirm 行内至少 2 处命中 (孤零零一个小标题不裁);
         confirm=None 表示窗口取「标记起至末尾」。文本层 (pdf_tail_trim_text) **不得**用
         None: 扁平串里的伪行段远比原始 PDF 行短, 「标记起至末尾」会把正文后段几百字后的
         真实声明尾也算进确认窗口, 于是正文里单处「著作权」就被当成裁切点 (B15 修复 F-01:
         4136 字样本裁到 2820 字, 丢 43 句)。文本层改用 12 段有界窗口, 见 pdf_tail_trim_text。
    页眉/页脚/水印不算标记: 同一行重复 >= PDF_TAIL_REPEAT_MIN 次, 或命中 PDF_TAIL_NOISE
    (东财水印「XX证券版权所有发送给…eastmoney.com」、「第 8 页共 8 页」)。
    """
    n = len(lines)
    if n < 2:
        return n
    prefix = [0]
    for s in lines:
        prefix.append(prefix[-1] + len(s) + 1)
    counts: dict[str, int] = {}
    for s in lines:
        counts[s] = counts.get(s, 0) + 1
    first = next((i for i, s in enumerate(lines)
                  if counts[s] < PDF_TAIL_REPEAT_MIN
                  and not PDF_TAIL_NOISE.search(s) and PDF_TAIL_MARKER.search(s)), n)
    if first >= n:
        return n
    if prefix[first] < prefix[-1] * PDF_TAIL_MIN_PCT:
        return n
    if prefix[first] < PDF_TAIL_MIN_KEEP:
        return n
    if PDF_TAIL_BODY_HINT.search(" ".join(lines[first:])):
        return n
    window = lines[first:] if confirm is None else lines[first:first + confirm]
    if sum(len(PDF_TAIL_MARKER.findall(x)) for x in window) < 2:
        return n
    return first


def pdf_tail_trim(lines: list[str], cut: int) -> int:
    """把裁切点之前的「重复页脚/水印/页码/网址/地址/孤立标签行」丢掉, 预算 PDF_TAIL_FRAG_MAX (Batch 14).

    只清理不裁正文 (Batch 14 修复 F-01): 每条被删的行都必须命中 PDF_TAIL_NOISE /
    _pdf_tail_frag / PDF_TAIL_ORPHAN, 或「同一行重复 >= PDF_TAIL_REPEAT_MIN 次且含声明标记」
    的页脚; 其余行原样保留 —— 表格行「长飞光纤光缆 107.2000」不含任何上述特征, 一律留下。
    Batch 14 修复 F-05: 网址/电话等子串残片不再整行删, 只有行首或「整行基本就是它」才删,
    含正文特征词的行一律逃逸 (见 _pdf_tail_frag)。
    """
    counts: dict[str, int] = {}
    for s in lines[:cut]:
        counts[s] = counts.get(s, 0) + 1
    kept = sum(len(s) + 1 for s in lines[:cut])
    dropped = 0
    while cut > 0:
        s = lines[cut - 1]
        noise = (PDF_TAIL_NOISE.search(s) or _pdf_tail_frag(s)
                 or PDF_TAIL_ORPHAN.match(s)
                 or (counts[s] >= PDF_TAIL_REPEAT_MIN and PDF_TAIL_MARKER.search(s)))
        if not noise or dropped + len(s) > PDF_TAIL_FRAG_MAX:
            break
        dropped += len(s)
        cut -= 1
    return cut if kept - dropped >= PDF_TAIL_MIN_KEEP else cut


def _pdf_tail_segments(text: str) -> list[str]:
    """把扁平正文按句末标点切成伪行 (Batch 15): "".join(结果) == 原文, 逐字节可逆."""
    segs: list[str] = []
    start = 0
    for i, ch in enumerate(text):
        if ch in "。！？；!?;\n":
            segs.append(text[start:i + 1])
            start = i + 1
    if start < len(text):
        segs.append(text[start:])
    return segs


def pdf_tail_trim_text(text: str) -> str:
    """字符串级尾部声明剔除 (Batch 15): 对**已入库**的 snippet 再跑一次 pdf_tail_cut +
    pdf_tail_trim, 供渲染前纵深防御使用.

    与 pdf_clean 的区别: pdf_clean 拿到的是 pdftotext 原始行, 可以直接跑行级护栏;
    队列里的 snippet 已被 pdf_clean 用 " " 拼成**单行**, 直接喂给 pdf_tail_trim 只有
    1 行 -> 永不裁. 故先按句末标点还原成伪行, 再原样复用同一套四护栏与残片规则
    (不新增判据, 单一裁决源). 不裁时**逐字节返回原串**, 保证幂等与「干净正文零改动」.
    """
    body = str(text or "")
    if not body or not PDF_TAIL_MARKER.search(body):
        return body                        # 快路径: 全文无任何声明标记
    segs = _pdf_tail_segments(body)
    # 护栏④ 用**有界**窗口 (Batch 15 修复 F-01): None = 「标记起至末尾」会让正文后段的
    # 单处声明词与几百字后的真实声明尾同处一个窗口, 裁切点前移 (实测 4136 -> 2820 字,
    # 丢 43 句). 12 段落在实测安全平台 [8, 32] 内: 真实万联尾段 2 处标记都在 12 段内
    # (7044 -> 6220 仍裁), 而「正文里 1 处标记 + 其后 >12 段正文」的窗口只有 1 处 -> 不裁.
    cut = pdf_tail_cut(segs, confirm=12)
    if cut >= len(segs):
        return body                        # 四护栏拒裁 (含 < 2 段/短报告) -> 原样
    cut = pdf_tail_trim(segs, cut)
    return body if cut >= len(segs) else "".join(segs[:cut]).rstrip()


def pdf_clean(text: str, limit: int = SNIPPET_MAX) -> str:
    """把 pdftotext 输出收拾成可推的一段摘要 (Batch 7).

    版式噪音三类: ① 页码/页眉页脚 (纯数字、`- 3 -`); ② 表格碎片 (价格/占比/产品代码行);
    ③ 版式标记 (`[Table_StockNameRptType]`、`图 2`). 只保留「含 ≥6 个汉字且 ≥10 字」的
    成句行; 成句行合计不足 limit 时退回全部非空行 —— 中金所月报几乎全是表格, 只有
    小节标题是成句行, 退回策略保证有文本层的 PDF 不被误判成「无文本层」。

    Batch 14: 先剔除尾部的免责/版权/地址样板段 (pdf_tail_cut + pdf_tail_trim), 再按
    limit 截断 —— 声明段本来就不该占推送配额, 也不该成为最后一条 (i/N) 消息。
    """
    lines: list[str] = []
    for raw in (text or "").splitlines():
        s = raw.strip()
        if not s or PDF_TABLE.match(s) or PDF_NUM.match(s):
            continue
        lines.append(s)
    cut = pdf_tail_cut(lines)                 # Batch 14: 尾部声明样板段起点
    if cut < len(lines):                      # 只有真找到声明起点时才动刀
        lines = lines[:pdf_tail_trim(lines, cut)]
    good = [s for s in lines if len(s) >= 10 and len(PDF_CJK.findall(s)) >= 6]
    body = re.sub(r"\s+", " ", " ".join(good or lines)).strip()
    body = re.sub(r"[\ue000-\uf8ff\ufffd\x0c]", "", body)   # Wingdings 项目符号/换页符
    return body[:limit].strip()


def snip_truncate(body: str, limit: int = SNIPPET_MAX) -> str:
    """按字符截到存储上限, 被截断时补一行显式标记 (Batch 11).

    只在正文真的超过 SNIPPET_MAX 时才会走到 (正常研报 32 页约 23k 字, 不会触发);
    标记写进 snippet 本身, 让「被截断」这件事对人和对断言都可见, 而不是静默丢字。
    """
    body = str(body or "")
    if len(body) <= limit:
        return body
    mark = RESEARCH_TRUNC_MARK.format(n=len(body), limit=limit)
    return body[:max(0, limit - len(mark))].rstrip() + mark


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
    body = pdf_clean(r.stdout.decode("utf-8", "ignore"), limit=SNIPPET_FULL_MAX)
    if len(body) < PDF_MIN_CHARS:
        log(f"pdf 无文本层 (清洗后仅 {len(body)} 字), 无摘要")
        return ""
    return snip_truncate(body)            # Batch 11: 全文入库, 超存储上限才截断并留标记


# ---------------------------------------------------------------- 研报管线 (Batch 7)
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
    顺序与 filter_reason **不同**: 本函数把「具体品种/产业锚点」放在噪声否决**之前**
    (更宽) —— 否则「夯实股东回报」里的「股东」会把「电解铝量价齐升」这种正牌商品
    研报误杀。Batch 9 C-F-05 只同步注释, 行为一字不改。
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


# ------------------------------------------------------------------ 时间
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


def bad_ts(src: str, ctx: RoundContext | None = None) -> None:
    """时间不可解析: 计数后丢弃该条 (F-02, 与金属网口径一致)."""
    ctx = ctx or _DEFAULT_ROUND
    ctx.ts_bad[src] = ctx.ts_bad.get(src, 0) + 1
    ctx.ts_bad["total"] = ctx.ts_bad.get("total", 0) + 1


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


def dir_axes(key: str) -> frozenset[tuple[str, int]]:
    """方向集合: {(轴, +1/-1)}; 中性复合词先屏蔽, 长词优先匹配 (Batch 9 U3-F-01).

    「沪铜新增产量下降5%」只产出下行 -1：不会被「新增」的「增」追加成上行；
    「沪铜涨跌互现」产出 {("价格", +1), ("价格", -1)}。
    """
    t = _DIR_NEUTRAL_RE.sub("□", key or "")
    return frozenset(_DIR_TOKEN_SIGNS[m.group(0)] for m in _DIR_TOKEN_RE.finditer(t))


def dir_sign(key: str) -> int:
    """近邻合并方向签名 (兼容旧接口): 1=上行/偏多, -1=下行/偏空, 0=不含或双侧同现 (「涨跌互现」)."""
    axes = dir_axes(key)
    up = any(sign > 0 for _axis, sign in axes)
    down = any(sign < 0 for _axis, sign in axes)
    if up == down:
        return 0
    return 1 if up else -1


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
            da, db = dir_axes(a), dir_axes(b)
            if da and db and da != db:
                continue                      # 方向集合不相等 -> 不同事件 (Batch 9 U3-F-01)
                                               # 一侧无方向词 (空集) 不否决: 「上涨」vs 无词 仍可合并
            if CN_NUM_RE.findall(a) != CN_NUM_RE.findall(b):
                continue                      # 中文数字量词不同 -> 不近似合并 (Batch 9 C-F-02)
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
def feed_items(fetch, src_name: str, extract,
               ctx: RoundContext | None = None) -> list[dict]:
    """六个中文快讯源共享骨架: 取数 -> 抽取 -> 时间校验 -> 丢弃计数 -> 追加。

    fetch() 返回原始条目序列; extract(raw) 返回 (text, ts) 或 None:
      - None   = 本条无正文 / 源内噪声, 静默跳过, **不**计 bad_ts
      - (text, 0) = 时间不可解析, 计 bad_ts(src_name) 后丢弃
    空输入返回 []: 「空结果是否算失败」由调用方决定, 本函数不抛哨兵。
    """
    out: list[dict] = []
    for raw in fetch():
        got = extract(raw)
        if got is None:
            continue
        text, ts = got
        if not text:
            continue
        if not ts:
            bad_ts(src_name, ctx)
            continue
        out.append({"src": src_name, "ts": ts, "text": text})
    return out


def src_cls(deadline: float | None = None) -> list[dict]:
    p = {"app": "CailianpressWeb", "category": "", "lastTime": "", "last_time": "",
         "os": "web", "refresh_type": "1", "rn": "40", "sv": "8.4.6"}
    q = urllib.parse.urlencode(sorted(p.items()))
    sign = hashlib.md5(hashlib.sha1(q.encode()).hexdigest().encode()).hexdigest()
    d = json.loads(http(f"https://www.cls.cn/v1/roll/get_roll_list?{q}&sign={sign}",
                        "https://www.cls.cn/telegraph", deadline=deadline))
    rows = d.get("data", {}).get("roll_data", [])
    if not rows:                          # 200+空列表 = 结构性变化, 不得静默当「今天没新闻」
        raise RuntimeError("财联社: roll_data 为空列表 (疑似结构变化)")

    def extract(it: dict) -> tuple[str, int] | None:
        text = (it.get("title") or it.get("brief") or "").strip()
        if not text:
            return None
        return text, parse_ts(it.get("ctime"))   # 0 = 不可解析 -> feed_items 计 bad_ts 后丢弃

    return feed_items(lambda: rows, "财联社", extract)


def src_em(deadline: float | None = None) -> list[dict]:
    u = ("https://np-listapi.eastmoney.com/comm/web/getFastNewsList?client=web&biz=web_724"
         f"&fastColumn=102&sortEnd=&pageSize=40&req_trace={int(time.time()*1000)}")
    d = json.loads(http(u, "https://kuaixun.eastmoney.com/", deadline=deadline))
    rows = d.get("data", {}).get("fastNewsList", [])
    if not rows:
        raise RuntimeError("东财: fastNewsList 为空列表 (疑似结构变化)")

    def extract(it: dict) -> tuple[str, int] | None:
        text = (it.get("summary") or "").strip()
        if not text:
            return None
        return text, parse_ts(it.get("showTime"))

    return feed_items(lambda: rows, "东财", extract)


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
    rows = d.get("data", {}).get("list", [])
    if not rows:
        raise RuntimeError("同花顺: list 为空列表 (疑似结构变化)")

    def extract(it: dict) -> tuple[str, int] | None:
        title = TAG_RE.sub("", (it.get("title") or "")).strip()
        digest = TAG_RE.sub("", (it.get("digest") or "")).strip()
        body = merge_title_digest(title, digest)
        if not body:
            return None
        return body, parse_ts(it.get("ctime"))

    return feed_items(lambda: rows, "同花顺", extract)


def src_sina(deadline: float | None = None) -> list[dict]:
    u = ("https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=40"
         "&zhibo_id=152&tag_id=0&dire=f&dpc=1")
    d = json.loads(http(u, deadline=deadline))
    rows = d.get("result", {}).get("data", {}).get("feed", {}).get("list", [])
    if not rows:
        raise RuntimeError("新浪: feed.list 为空列表 (疑似结构变化)")

    def extract(it: dict) -> tuple[str, int] | None:
        text = TAG_RE.sub("", (it.get("rich_text") or it.get("content") or "")).strip()
        if not text:
            return None
        return text, parse_ts(it.get("create_time"))

    return feed_items(lambda: rows, "新浪", extract)


def src_wscn_commodity(deadline: float | None = None) -> list[dict]:
    """华尔街见闻 商品频道: 品种面最宽 (黑色/农产品/能化), 低频但有效率高."""
    u = ("https://api-one-wscn.awtmt.com/apiv1/content/lives"
         "?channel=commodity-channel&client=pc&limit=50")
    d = json.loads(http(u, deadline=deadline))
    rows = (d.get("data") or {}).get("items") or []
    if not rows:
        raise RuntimeError("见闻商品: items 为空列表 (疑似结构变化)")

    def extract(it: dict) -> tuple[str, int] | None:
        text = clean_text(it.get("content_text"))
        if not text:
            return None
        return text, parse_ts(it.get("display_time"))

    return feed_items(lambda: rows, "见闻商品", extract)


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

    def extract(it: dict) -> tuple[str, int] | None:
        title = clean_text(it.get("title"))
        profile = clean_text(it.get("profile"))
        # 5/30 条标题在正文开头重复一遍, 只留正文
        text = profile if title and profile.startswith(title[:min(len(title), 24)]) else \
            clean_text(f"{title} {profile}")
        if not smm_keep(text):
            return None                   # 分钟级行情异动预警 = 纯噪声, 不进过滤器
        return text, parse_ts(it.get("pubDate"))

    return feed_items(lambda: lst, "SMM", extract)


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


def feed_proxy(ctx: RoundContext | None = None) -> str:
    """英文代理源取值口径与 send_plan() 完全一致: conf.proxy -> 环境 HTTPS_PROXY / https_proxy."""
    ctx = ctx or _DEFAULT_ROUND
    return str(ctx.cfg.get("proxy") or PROXY or "").strip()


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


# ------------------------------------------------------------------ Batch 12 化工现货源
# 化销网 (甲醇频道 catid=22) + 隆众 (资讯列 /1/9/): 「甲醇下游装置停车/减产/合同取消」这类
# 产业链动向现有 17 源完全没有; 两站都是服务端渲染的静态 HTML, 走「列表 -> 候选 -> 详情 ->
# feed_items」既有骨架, 零第三方依赖, tries/deadline 与既有源同一套 http().
# 准入两级: 列表级「标题含品种词 且 (事件词 或 提示/前瞻栏目词)」-> 才值得拉详情;
#           详情级「标题 ∪ 正文 同时含事件词与品种词」-> 才入队.
# 设计覆盖 §4.4: 单提甲醇下游 (MTBE/甲醛/二甲醚/醋酸/MTO/MTP) 的产业链消息
#   必须放行 —— 这些品种不在全局 PRODUCT_ALIASES 里, 会命中 NOISE/无品种而丢. 做法: 命中
#   源内下游词而**未**命中全局品种词时, 文本前加 SPOT_PREFIX 让全局过滤看到「甲醇」;
#   全局词表一字不改, 前缀幂等, 且不影响已命中甲醇 (或任何全局品种) 的条目.
CHEM_SRCS = ("化销网", "隆众", "我的钢铁", "东财甲醇")
SPOT_DOWNSTREAM = ("MTBE", "甲醛", "二甲醚", "醋酸", "MTO", "MTP")   # 甲醇下游主力 (非期货品种)
SPOT_PREFIX = "甲醇下游·"
HXW_BASE = "https://www.huaxiaowang.com"
HXW_LIST_URL = HXW_BASE + "/index/quote/infolist?catid=22"
LZ_LIST_URL = "https://list.oilchem.net/1/9/"
CHEM_DETAIL_MAX = 4            # 每源每轮最多拉取的详情页数 (列表 20 条, 每轮新增一般 0-3 条)
CHEM_DETAIL_AGE_S = 4 * 3600   # 候选超过 4h 不再拉详情: 超过 max_age(默认 180min) 必判 stale
CHEM_DETAIL_MIN_S = 2.5        # 详情页剩余切片预算低于此值 -> 剩余候选下轮再看
CHEM_TEXT_MAX = ITEM_MAX       # 条目文本上限 (700), 与渲染截断同口径
CHEM_TITLE_MAX = 120           # 标题上限 (站方标题 <= 40 字, 留余量)
CHEM_LEAD = 80                 # 长正文按事件词截取时, 事件词前保留的字数
CHEM_TITLE_DENY = ("二乙二醇", "三乙二醇", "四乙二醇")   # 含「乙二醇」子串但非期货品种
# 供给侧事件词 (不是行情涨跌词): 标题命中即候选, 正文命中才准入
CHEM_EVENT = ("装置", "停车", "停工", "停产", "检修", "开工", "投产", "减产", "复产",
              "限产", "降负", "提负", "重启", "扩产", "扩建", "新增产能", "试车", "开车",
              "跳车", "故障", "事故", "爆炸", "火灾", "泄漏", "不可抗力", "断供", "缺货",
              "合同", "技改", "环保", "限电", "错峰")
# 「开工」是唯一的统计量歧义词: 后随「率/负荷/水平」或「数字+% / 数字+成」
# (开工83.8%) 时是产能统计, 不是装置事件 (F-02: PX 一周前瞻的「PX开工83.8%」曾被子串
# 误判为事件); 「开工装置/开工时间/开工2套」等装置动作仍算事件.
CHEM_EVENT_RATE = ("率", "负荷", "水平")
CHEM_EVENT_RATE_RE = re.compile(r"\s*\d+(?:\.\d+)?\s*[%％成]")
# Batch 13: 强事件词 = 去掉无方向的「装置」(「XX 装置动态/快讯」标题与「企业简介」
# 样板文都只命中「装置」, 用作事件会天天误推; 调研报告 §2.2/§8.3 实测)
CHEM_EVENT_STRONG = tuple(w for w in CHEM_EVENT if w != "装置")


def chem_event_pos(text: str, words: tuple = CHEM_EVENT) -> int:
    """首个真事件词的位置; 跳过「开工率/开工负荷/开工水平/开工83.8%」类统计量, 无则 -1.

    words 默认 CHEM_EVENT (调用方不传时行为与 Batch 12 逐字节相同); Batch 13 的
    我的钢铁/东财源传 CHEM_EVENT_STRONG 以排除无方向的「装置」。
    """
    s = str(text or "")
    best = -1
    for w in words:
        start = 0
        while True:
            i = s.find(w, start)
            if i < 0:
                break
            if w == "开工":
                rest = s[i + len(w):]
                if rest.startswith(CHEM_EVENT_RATE) or CHEM_EVENT_RATE_RE.match(rest):
                    start = i + len(w)
                    continue
            if best < 0 or i < best:
                best = i
            break
    return best


def chem_event_hit(text: str, words: tuple = CHEM_EVENT) -> bool:
    """事件词命中 (排除开工率统计量); 列表级与正文级准入共用同一口径."""
    return chem_event_pos(text, words) >= 0
# 定期提示/前瞻类栏目词: 事件常写在正文而不是标题 (目标事实即此形态)
CHEM_TIP = ("早间提示", "午间提示", "晚间提示", "晨报", "晚报", "周报", "月报",
            "前瞻", "综述", "盘点", "日报", "周度", "月度")


def html_inner(raw: str, marker: str) -> str:
    """取 marker 所在 <div> 的平衡内层 HTML; 找不到或不闭合返回 ''.

    marker 必须含开标签的 '>' (如 '<div class="content">'): 两个站点的正文都含嵌套
    div, 首个 </div> 不是正文边界, 只按 <div>/</div> 计深度才能取全。
    """
    i = raw.find(marker)
    if i < 0:
        return ""
    start = i + len(marker)
    depth = 1
    for m in re.finditer(r"<div\b[^>]*>|</div\s*>", raw[start:], re.I):
        if m.group(0).lower().startswith("</"):
            depth -= 1
            if depth == 0:
                return raw[start:start + m.start()]
        else:
            depth += 1
    return ""


def strip_embed(raw: str) -> str:
    """剥 <script>/<style> 整块 (隆众正文里内嵌表格脚本), 再交 clean_text."""
    return re.sub(r"<(script|style)\b.*?</\1>", "", raw or "", flags=re.S | re.I)


HXW_ITEM_RE = re.compile(
    r'<li>\s*<a href="(/index/quote/infoshow\?id=(\d+))"[^>]*>(.*?)</a>\s*'
    r'<span[^>]*>([^<]*)</span>', re.S)


def hxw_rows(page: str) -> list[dict]:
    """化销网甲醇频道列表 -> [{id,title,ts,url}]; 结构变化抛 RuntimeError (不静默当 0 条)."""
    i = page.find('<div class="info-list">')
    if i < 0:
        raise RuntimeError("化销网: 未找到 info-list (疑似结构变化)")
    rows = []
    for m in HXW_ITEM_RE.finditer(page[i:]):
        rows.append({"id": m.group(2),
                     "title": clean_text(m.group(3)),
                     "ts": parse_ts(clean_text(m.group(4))),
                     "url": HXW_BASE + m.group(1)})
    if not rows:
        raise RuntimeError("化销网: info-list 0 条 (疑似结构变化)")
    if all(not r["ts"] for r in rows):
        raise RuntimeError("化销网: 列表时间全不可解析 (疑似结构变化)")
    if not any(CJK_RE.search(r["title"]) for r in rows):
        raise RuntimeError("化销网: 列表标题无中文 (疑似编码变化)")
    return rows


def cn_date_ts(raw: str) -> int:
    """中文日期 (2026年11月09日) -> 当日 00:00 (Asia/Shanghai); 不可解析返回 0."""
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", str(raw or ""))
    if not m:
        return 0
    try:
        return sane_ts(int(datetime(int(m.group(1)), int(m.group(2)),
                                    int(m.group(3)), tzinfo=TZ_CN).timestamp()))
    except ValueError:
        return 0


def hxw_detail(page: str) -> dict | None:
    """化销网详情 -> {title, body, date_ts}; 结构不符返回 None (该条跳过, 下轮再看)."""
    m = re.search(r"<h3[^>]*>(.*?)</h3>", page, re.S)
    title = clean_text(m.group(1)) if m else ""
    body = clean_text(strip_embed(html_inner(page, '<div class="content">')))
    if not title or not body:
        return None
    dm = re.search(r"发布时间[:：]\s*([^<]+)", page)
    return {"title": title, "body": body,
            "date_ts": cn_date_ts(dm.group(1) if dm else "")}


LZ_LI_RE = re.compile(r'<li[^>]*class="clearfix"[^>]*>.*?</li>', re.S)
LZ_A_RE = re.compile(r'<a href="(https://www\.oilchem\.net/[^"]+\.html)"[^>]*>(.*?)</a>', re.S)
LZ_T_RE = re.compile(r"<span[^>]*>\s*\[([^\]]+)\]\s*</span>", re.S)


def lz_rows(page: str) -> list[dict]:
    """隆众列表 -> [{id,title,ts,url}]; 跳过服务端渲染的隐藏模板 li (标题是 JS 拼接串)."""
    rows = []
    for block in LZ_LI_RE.findall(page):
        if "display: none" in block or "display:none" in block:
            continue
        am = LZ_A_RE.search(block)
        tm = LZ_T_RE.search(block)
        if not am or not tm:
            continue
        url = am.group(1)
        rows.append({"id": url.rsplit("/", 1)[-1][:-len(".html")],
                     "title": clean_text(am.group(2)),
                     "ts": parse_ts(re.sub(r"\s+", " ", tm.group(1)).strip()),
                     "url": url})
    if not rows:
        raise RuntimeError("隆众: 列表 0 条 (疑似结构变化)")
    if all(not r["ts"] for r in rows):
        raise RuntimeError("隆众: 列表时间全不可解析 (疑似结构变化)")
    if not any(CJK_RE.search(r["title"]) for r in rows):
        raise RuntimeError("隆众: 列表标题无中文 (疑似编码变化)")
    return rows


def lz_detail(page: str) -> dict | None:
    """隆众详情 -> {title, body, ts}; 结构不符返回 None."""
    m = re.search(r"<h1[^>]*>\s*<strong>(.*?)</strong>", page, re.S)
    title = clean_text(m.group(1)) if m else ""
    body = clean_text(strip_embed(html_inner(page, 'id="content">')))
    if not title or not body:
        return None
    dm = re.search(r"发布时间[:：]\s*(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2})", page)
    return {"title": title, "body": body,
            "ts": parse_ts(dm.group(1).replace("T", " ")) if dm else 0}


def chem_flatten(raw: str) -> str:
    """正文压成单段: 折叠全部空白 (表格/换行), 保留中文标点."""
    return re.sub(r"\s+", " ", str(raw or "")).strip()


def chem_spot(text: str) -> str:
    """源内甲醇下游标记: 命中 SPOT_DOWNSTREAM 且未命中全局品种词 -> SPOT_PREFIX (否则 '').

    CTO 覆盖 §4.4: 单提 MTBE/甲醛/二甲醚/醋酸/MTO/MTP 的产业链消息必须放行 —— 全局
    PRODUCT_ALIASES 里没有这些品种, 不加标记就会被品种门丢掉. 已命中全局品种 (含甲醇
    本身, 如上游已带前缀的文本) 的条目不标记, 因此前缀不会二次叠加.
    """
    if first_hit(text, STRONG):          # 全局品种门 (filter_reason 的第一个保留分支)
        return ""
    return SPOT_PREFIX if hit(text, SPOT_DOWNSTREAM) else ""


def chem_candidates(rows: list[dict]) -> list[dict]:
    """列表级准入 (决定是否值得拉详情): 标题含品种词 且 含事件词或提示词."""
    out = []
    for r in rows:
        title = r.get("title") or ""
        if any(w in title for w in CHEM_TITLE_DENY):
            continue
        if not (first_hit(title, STRONG) or hit(title, SPOT_DOWNSTREAM)):
            continue
        if not (chem_event_hit(title) or any(w in title for w in CHEM_TIP)):
            continue
        out.append(r)
    return out


def chem_admit(title: str, body: str) -> bool:
    """正文级准入: 供给侧事件词 + 品种词 (全局品种, 或源内甲醇下游 —— CTO 覆盖 §4.4)."""
    text = f"{title} {body}"
    return chem_event_hit(text) and (hit(text, STRONG) or hit(text, SPOT_DOWNSTREAM))


def chem_text(title: str, body: str) -> str:
    """条目文本 '标题：正文摘录', 总长 <= CHEM_TEXT_MAX; 长正文从事件词前 CHEM_LEAD 字截起."""
    t = clean_text(title)[:CHEM_TITLE_MAX]
    b = chem_flatten(clean_text(body))
    if not t:
        return ""
    if not b:
        out = t
    else:
        head = t + "："
        if len(head) + len(b) <= CHEM_TEXT_MAX:
            out = head + b
        else:
            pos = chem_event_pos(b)
            start = max(0, pos - CHEM_LEAD) if pos >= 0 else 0
            lead = "…" if start else ""
            room = CHEM_TEXT_MAX - len(head) - len(lead) - 1
            seg = b[start:start + max(1, room)]
            tail = "…" if start + len(seg) < len(b) else ""
            out = head + lead + seg + tail
    tag = chem_spot(f"{title} {body}")
    return out if (not tag or out.startswith(tag)) else tag + out


def _chem_pick(src: str, cands: list[dict], referer: str, detail_fn, deadline,
               detail_max: int = CHEM_DETAIL_MAX, admit_fn=chem_admit,
               location: bool = False, prefer_detail: bool = False,
               tried: list[str] | None = None) -> list[dict]:
    """候选 -> 详情(限条数/限时/失败只丢本条) -> 准入; 返回 [{title, body, ts}].

    Batch 13 从 _chem_source 拆出: 化销网 6 类目与我的钢铁/东财三个源的列表形态不同,
    但「候选 -> 详情 -> 准入 -> 只丢本条」这一段完全相同, 必须共用 (否则三份拷贝
    的失败语义会漂移). 参数全部有默认值 -> 隆众/化销网单页路径行为不变.

    tried (Batch 13 修复): 传入列表时把**实际发起过详情请求**的 id 追加进去。化销网
    用它跨阶段累计配额 (B13-F04), 东财用它在轮次间做源内轮转 (B13-F03)。
    """
    out: list[dict] = []
    for r in cands[:detail_max]:
        if deadline is not None and deadline - time.monotonic() < CHEM_DETAIL_MIN_S:
            log(f"{src}: 详情预算不足, 剩余候选下轮再看")
            break
        if tried is not None:
            tried.append(r["id"])       # 记录「已取过」: 失败也算, 避免每轮重复下载同一篇
        try:
            dpage = http(r["url"], referer, tries=1, deadline=deadline,
                         location=location).decode("utf-8", "ignore")
        except Exception as e:                    # 详情失败只丢本条, 不把源判失败
            log(f"{src}: 详情失败, 跳过 {r['id']}: {type(e).__name__}")
            continue
        d = detail_fn(dpage)
        if not d:
            log(f"{src}: 详情不可解析, 跳过 {r['id']}")
            continue
        body = d.get("body") or ""
        if not admit_fn(r["title"], body):
            log(f"{src}: 准入未通过, 跳过 {r['id']}")
            continue
        dts = d.get("ts") or d.get("date_ts") or 0
        ts = (dts or r["ts"]) if prefer_detail else (r["ts"] or dts)
        out.append({"title": r["title"], "body": body, "ts": ts})
    return out


def _chem_feed(src: str, out: list[dict]) -> list[dict]:
    """[{title, body, ts}] -> feed_items (文本拼装与 ts 传递; 与 Batch 12 逐字节等价)."""
    def extract(x: dict) -> tuple[str, int] | None:
        text = chem_text(x["title"], x["body"])
        return (text, x["ts"]) if text else None

    return feed_items(lambda: out, src, extract)


def _chem_source(src: str, list_url: str, referer: str, rows_fn, detail_fn,
                 deadline: float | None, detail_max: int = CHEM_DETAIL_MAX,
                 admit_fn=chem_admit, age_s: int = CHEM_DETAIL_AGE_S) -> list[dict]:
    """单页列表骨架 (隆众): 列表 -> 候选 -> 详情 -> feed_items."""
    page = http(list_url, referer, tries=2, deadline=deadline).decode("utf-8", "ignore")
    rows = rows_fn(page)
    now = int(time.time())
    cands = [r for r in chem_candidates(rows) if now - r["ts"] <= age_s]
    return _chem_feed(src, _chem_pick(src, cands, referer, detail_fn, deadline,
                                      detail_max, admit_fn))


# ------------------------------------------------------------------ Batch 13 化工源扩容
# 用户裁决的三处扩容: ① 化销网 6 类目合并 ② 我的钢铁能化「甲醇及下游」 ③ 东财甲醇 tag.
# 三站都是服务端渲染静态 HTML, 走既有「列表 -> 候选 -> 详情 -> feed_items」骨架, 零第三方
# 依赖. 准入差异 (实测决定, 不是偏好): 化销网/东财有正文 -> 沿用 chem_admit; 我的钢铁只有
# ~181 字导语、装置类导语为空 -> 用 CHEM_EVENT_STRONG 在标题级兜底 (my_admit).
MY_BASE = "https://nenghua.mysteel.com"
MY_LIST_URL = MY_BASE + "/breed/jcjxy/"                    # robots 禁 /*?* -> 不得拼查询串
EM_MA_LIST_URL = "https://futures.eastmoney.com/qihuo/ma.html"
# 化销网类目抓取顺序 = 价值/详情优先级 (B13-F04): 13 上游 / 14 下游 是装置动态的实际
# 来源, 最先抓列表并**立刻取详情**; 其次 22 国内 (既有类目, 保留 tries=2 与结构哨兵硬
# 失败); 23 国际 / 21 期货 / 20 撮合 实测近乎停更, 预算不足时优先截断。
# 旧顺序把 22 放最前: 慢轮 6 次列表吃满 4.62s 后 22 的详情先拿, 13/14 的装置动态反而
# 排到预算之外 —— 慢轮恒 0 条 (评审核查 F-04)。
HXW_CATS = (13, 14, 22, 23, 21, 20)
HXW_PRIORITY_CATS = (13, 14)  # 高价值: 列表后立刻取详情, 不与「剩余类目」抢预算
HXW_LIST_MIN_S = CHEM_DETAIL_MIN_S   # 低于详情阈值 -> 截断后续类目 (发了也拉不动详情)
HXW_DETAIL_MAX = 6            # 6 类目共用: 每轮最多 6 篇详情 (单类目时 CHEM_DETAIL_MAX=4)
MY_DETAIL_MAX = 4             # 我的钢铁: 列表 ~287 行, 工作日新增 10-40 条, 候选 2-9 条
EM_MA_DETAIL_MAX = 1          # 东财 tag: 日增 0.5-1 条甲醇长文, 配额不浪费也不挤别人
EM_MA_AGE_S = 36 * 3600       # 东财列表只有日期 (无时分) -> 年龄闸门按「今日/昨日」, 精确
                              # 陈旧判定交给 max_age (详情时间到分钟)
# 东财 tag 是「甲醇」聚合页 (实测 15 条唯一链接仅 3 条甲醇): 列表级先按标题筛甲醇;
# 锚点词识别「日评/机构观点」长文 —— 用户裁决里东财承担「日评锚点」, 故除事件门外
# 另开一道锚点门 (正文 >= 300 字才放行, 快讯类一律丢)
EM_MA_ANCHOR = ("金联创", "甲醇：", "能化 | 甲醇", "甲醇市场")
# 「XX 装置快讯」的导语是固定模板的企业简介 (实测 181 字, 末句「投产时间：2014年11月」
# 会命中强事件词「投产」) -> 命中这些模板标记时正文不算事件材料, 退回标题级判定
MY_BOILER = ("企业简介", "投产时间：", "原料库容")


def src_hxw(deadline: float | None = None) -> list[dict]:
    """化销网 6 类目合并 (13 上游 / 14 下游 / 22 国内 / 23 国际 / 21 期货 / 20 撮合) + 详情.

    Batch 13: 单类目 (catid=22) 扩为 6 类目, **源数不变**, robots.txt 全开放 (Batch 12 实测).
    抓取分三阶段 (B13-F04 修复):
      ① 高价值类目 13/14 列表 -> **立刻取详情** (装置动态的主来源, 不被慢轮牺牲);
      ② 其余类目 22/23/21/20 列表, 剩余份额 < HXW_LIST_MIN_S 时**截断**后续类目;
      ③ 其余候选详情, 只吃 ① 之后剩下的详情配额。
    - 源内按 infoshow id 去重 (实测当日 6×20 条零重叠; 跨类目归属调整时防御);
    - **不做跨类目时间排序**: 列表自身即站方时间序;
    - 单类目列表失败 (含 hxw_rows 的结构哨兵) 只跳过该类目并记一行日志; 类目 22 的
      哨兵保持硬失败 (与 Batch 12 一致); 合并后 0 条才判源失败.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    failed: list[str] = []
    attempted: list[int] = []

    def _load(cats: tuple[int, ...]) -> None:
        for cat in cats:
            attempted.append(cat)
            url = f"{HXW_BASE}/index/quote/infolist?catid={cat}"
            try:
                page = http(url, url, tries=2 if cat == 22 else 1,
                            deadline=deadline).decode("utf-8", "ignore")
            except Exception as e:
                failed.append(f"{cat}:{type(e).__name__}")
                continue
            try:
                got = hxw_rows(page)      # 自带 4 道哨兵; 图标 <i> 由 clean_text 吸收
            except RuntimeError as e:
                if cat == 22:             # 既有类目结构变化 -> 源失败 (Batch 12 语义)
                    raise
                failed.append(f"{cat}:{str(e)[:40]}")
                continue
            for r in got:
                if r["id"] in seen:
                    continue
                seen.add(r["id"])
                rows.append(r)

    now = int(time.time())

    def _cands() -> list[dict]:
        return [r for r in chem_candidates(rows) if now - r["ts"] <= CHEM_DETAIL_AGE_S]

    tried: list[str] = []
    _load(HXW_PRIORITY_CATS)
    out = _chem_pick("化销网", _cands(), HXW_LIST_URL, hxw_detail, deadline,
                     HXW_DETAIL_MAX, tried=tried)
    rest = tuple(c for c in HXW_CATS if c not in HXW_PRIORITY_CATS)
    for cat in rest:
        if deadline is not None and deadline - time.monotonic() < HXW_LIST_MIN_S:
            skipped = [c for c in rest if c not in attempted]
            log(f"化销网: 预算不足, 截断后续 {len(skipped)} 类目列表 "
                f"({', '.join(map(str, skipped))}), 详情优先")
            break
        _load((cat,))
    left = HXW_DETAIL_MAX - len(tried)
    if left > 0:
        more = [r for r in _cands() if r["id"] not in tried]
        out.extend(_chem_pick("化销网", more, HXW_LIST_URL, hxw_detail, deadline,
                              left, tried=tried))
    if failed:
        log(f"化销网: {len(failed)} 类目列表失败, 下轮重试 ({', '.join(failed)})")
    if not rows:
        raise RuntimeError(
            f"化销网: 类目合并后 0 条 (疑似结构变化, 列表失败 {len(failed)} 个 / "
            f"尝试 {len(attempted)} 个)")
    return _chem_feed("化销网", out)


def my_list_ts(url: str) -> int:
    """文章 URL 的 /a/YYMMDDHH/ -> 该整点 (Asia/Shanghai) 的 epoch; 不可解析返回 0.

    列表只有 [MM-DD] (无年份/无时分), URL 里的 YYMMDDHH 补全年份并给到小时。用整点
    作 ts 比真实发布时间**偏早** (最多 59 分钟), 年龄闸门因此偏保守 —— 不会把陈旧
    条目误放进详情队列。精确到秒的 ts 由详情 meta[name=publish] 提供。
    """
    m = re.search(r"/a/(\d{2})(\d{2})(\d{2})(\d{2})/[0-9A-Fa-f]+\.html", str(url or ""))
    if not m:
        return 0
    try:
        yy, mo, dd, hh = (int(m.group(i)) for i in range(1, 5))
        return sane_ts(int(datetime(2000 + yy, mo, dd, hh, tzinfo=TZ_CN).timestamp()))
    except ValueError:
        return 0


MY_ROW_RE = re.compile(
    # B13-F02: 当日条目用 `<span  class="red" >` (双空格 + 行内样式变体), 旧的
    # `<span\s*>` 会漏掉当天全部条目 -> 4h 内候选恒 0。放宽为 `<span[^>]*>`,
    # 但**保持条目级上下文锚定**: 必须 `<li ...>` 紧跟 `<span ...>[MM-DD]</span>`
    # 且 `<a href="https://nenghua.mysteel.com/a/...">`, 页面其它位置的 `[MM-DD]`
    # 文本 (面包屑/侧栏/正文日期) 不匹配。
    r'<li\b[^>]*>\s*<span[^>]*>\[(\d{2}-\d{2})\]</span>\s*'
    r'<a href="(https://nenghua\.mysteel\.com/a/\d{8}/[0-9A-Fa-f]+\.html)"'
    r'(?:\s+title="[^"]*")?[^>]*>(.*?)</a>', re.S)


def my_rows(page: str) -> list[dict]:
    """我的钢铁 能化「甲醇及下游」列表 -> [{id,title,ts,url}]; 结构变化抛 RuntimeError.

    实测 (2026-11-10 11:00): 页面 `[MM-DD]` 条目 287 个, 其中 `<span  class="red" >`
    的当日条目 79 个; 放宽 span 属性后 287/287 全部解析, 按 URL 去重后 285 条 (同一
    URL 在多个栏目块重复出现), 4h 内候选 4 条。标题属性里可能带 U+200B 零宽空格,
    clean_text 后仍有残留 -> 统一再剥一次。
    """
    rows: list[dict] = []
    seen: set[str] = set()
    for m in MY_ROW_RE.finditer(page or ""):
        url = m.group(2)
        if url in seen:
            continue
        seen.add(url)
        rows.append({"id": url,
                     "title": clean_text(m.group(3)).replace("\u200b", "").strip(),
                     "ts": my_list_ts(url),
                     "url": url})
    if not rows:
        raise RuntimeError("我的钢铁: 列表 0 条 (疑似结构变化)")
    if all(not r["ts"] for r in rows):
        raise RuntimeError("我的钢铁: 列表时间全不可解析 (疑似结构变化)")
    if not any(CJK_RE.search(r["title"]) for r in rows):
        raise RuntimeError("我的钢铁: 列表标题无中文 (疑似编码变化)")
    return rows


def my_detail(page: str) -> dict | None:
    """我的钢铁详情 -> {body, ts}; 正文容器缺失返回 None (该条跳过, 下轮再看).

    正文只有 ~181 字导语且**可能为空** (装置动态/损失量类 #article-content 为 0 字符)
    —— 空正文是已知形态, 必须返回 body='' 交 my_admit 用标题兜底, 不能当结构失败。
    边界取到 `<div class="editor"` 之前: 导语之后紧跟登录脚本与「资讯编辑」块,
    取首个 </div> 会把编辑电话拉进正文 (实测)。
    """
    marker = '<div class="content-text" id="article-content">'
    i = (page or "").find(marker)
    if i < 0:
        return None
    start = i + len(marker)
    j = page.find('<div class="editor"', start)
    body = clean_text(strip_embed(page[start:j if j > start else start]))
    pm = re.search(r'name="publish" content="published at '
                   r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', page)
    return {"body": body, "ts": parse_ts(pm.group(1)) if pm else 0}


def my_candidates(rows: list[dict]) -> list[dict]:
    """列表级候选 (我的钢铁): 商品种词 且 (强事件词 或 提示/前瞻栏目词).

    比化销网的 chem_candidates 严一档: 「装置」不算。实测 11-09 有 5 条
    「XX 甲醇装置动态/装置快讯」, 导语为空或企业简介, 见「装置」就拉详情纯属浪费
    (调研报告 §8.3 同样结论)。
    """
    out: list[dict] = []
    for r in rows:
        title = r.get("title") or ""
        if any(w in title for w in CHEM_TITLE_DENY):
            continue
        if not (first_hit(title, STRONG) or hit(title, SPOT_DOWNSTREAM)):
            continue
        if not (chem_event_hit(title, CHEM_EVENT_STRONG)
                or any(w in title for w in CHEM_TIP)):
            continue
        out.append(r)
    return out


def my_admit(title: str, body: str) -> bool:
    """正文级准入 (我的钢铁): 强事件词 + 品种词, 标题 ∪ 导语一起判; 导语空则只判标题.

    「导语为空是否放行」的裁决: **放行, 但仅当标题自身同时给出方向与品种**。
    [损失量]：中国甲醇装置检修数据统计 -> 检修 + 甲醇 -> 放行 (标题即全部信息);
    内蒙古远兴能源甲醇装置动态 -> 只有「装置」无方向 -> 丢弃 (无信息量且天天重复);
    联泓MTO装置快讯 (导语=企业简介) -> 正文无强事件词 -> 丢弃 (样板文不是事件)。
    """
    if any(w in body for w in MY_BOILER):        # 企业简介样板文不是事件材料
        body = ""
    text = f"{title} {body}"
    return (chem_event_hit(text, CHEM_EVENT_STRONG)
            and (hit(text, STRONG) or hit(text, SPOT_DOWNSTREAM)))


def src_mysteel(deadline: float | None = None) -> list[dict]:
    """我的钢铁 能化「甲醇及下游」: 列表 (无查询串, robots 禁 /*?*) + 详情导语.

    「事件雷达」源: 只取标题 + 181 字导语 (全文在会员区, 不越界不绕过), 装置/检修
    事件靠标题级强事件词兜底。时间戳到秒 (详情 meta[name=publish])。
    """
    page = http(MY_LIST_URL, MY_LIST_URL, tries=2,
                deadline=deadline).decode("utf-8", "ignore")
    rows = my_rows(page)
    now = int(time.time())
    cands = [r for r in my_candidates(rows) if now - r["ts"] <= CHEM_DETAIL_AGE_S]
    return _chem_feed("我的钢铁", _chem_pick("我的钢铁", cands, MY_LIST_URL, my_detail,
                                            deadline, MY_DETAIL_MAX, my_admit,
                                            prefer_detail=True))


def em_id_ts(aid: str) -> int:
    """东财稿件 id 形如 202611093926259150 (前 8 位 = YYYYMMDD) -> 当日 00:00 +08; 否则 0."""
    m = re.match(r"(\d{4})(\d{2})(\d{2})", str(aid or ""))
    if not m:
        return 0
    try:
        return sane_ts(int(datetime(int(m.group(1)), int(m.group(2)),
                                    int(m.group(3)), tzinfo=TZ_CN).timestamp()))
    except ValueError:
        return 0


EM_MA_ROW_RE = re.compile(
    r'<a href="(https?://[a-z.]*eastmoney\.com/a/(\d{11,})\.html)"[^>]*?'
    r'title="([^"]*)"', re.S)


def em_ma_rows(page: str) -> list[dict]:
    """东财 甲醇 tag 页 -> [{id,title,ts,url}]; 同一稿件在两个区块重复 -> 按 id 去重.

    锚文标题被截断成「…」, 故取 `title=` 属性 (完整标题) 而不是 <a> 内文。
    """
    rows: list[dict] = []
    seen: set[str] = set()
    for m in EM_MA_ROW_RE.finditer(page or ""):
        url, aid, title = m.group(1), m.group(2), clean_text(m.group(3))
        if aid in seen:
            continue
        seen.add(aid)
        rows.append({"id": aid, "title": title, "ts": em_id_ts(aid), "url": url})
    if not rows:
        raise RuntimeError("东财甲醇: 列表 0 条 (疑似结构变化)")
    if not any(CJK_RE.search(r["title"]) for r in rows):
        raise RuntimeError("东财甲醇: 列表标题无中文 (疑似编码变化)")
    return rows


# 多品种日报的分节标记: 【原油】【燃料油】【甲醇】… (全角/半角括号, 括号内允许空格)
EM_SECTION_RE = re.compile(r"[【\[]\s*([^】\]]{1,10}?)\s*[】\]]")


def em_methanol_section(body: str) -> str | None:
    """东财正文 -> 甲醇段; 多品种日报切【甲醇】, 无分节标记的纯甲醇文整篇返回.

    B13-F01: 东财 tag 会混入多品种日报 (实测 202611103926942138: 正文 2605 字、
    12 个【xx】分节, 【甲醇】在第 1009 字)。旧实现取整个 #ContentBody -> 入队前
    700 字全是原油/燃料油/沥青/LPG, 甲醇 0 字 = 把别的品种内容挂在甲醇标题下。
    规则:
      - 正文有分节标记时, 只保留【甲醇】标记起、到**下一个【xx】之前**的段落;
        找不到【甲醇】段 -> None (宁缺毋滥, **绝不**回退成整段正文);
      - 正文没有任何分节标记 -> 整篇返回 (金联创 429 字纯甲醇日评即此形态);
      - 全角/半角括号、括号内空格均识别。
    """
    s = str(body or "")
    marks = list(EM_SECTION_RE.finditer(s))
    if not marks:
        return s.strip() or None
    for k, m in enumerate(marks):
        if re.sub(r"\s+", "", m.group(1)) != "甲醇":
            continue
        end = marks[k + 1].start() if k + 1 < len(marks) else len(s)
        seg = s[m.start():end].strip()
        return seg or None
    return None


def em_detail(page: str) -> dict | None:
    """东财详情 -> {body, ts}; 正文容器缺失 / 多品种日报无【甲醇】段返回 None.

    正文取 div.txtinfos#ContentBody 到 div.zwothers 之前 (之后是「文章来源/责任编辑」),
    再按 B13-F01 切【甲醇】段 (多品种日报只留甲醇; 无分节标记的纯甲醇文整篇保留)。
    ts 取页内「2026年11月09日 16:04」(到分钟) —— 页首 `<!--published at ...-->` 是
    服务端渲染时刻, 不是发布时间, **不可用** (实测两者相差数小时)。
    """
    i = (page or "").find('id="ContentBody"')
    if i < 0:
        return None
    start = page.find(">", i) + 1
    j = page.find('<div class="zwothers"', start)
    body = clean_text(strip_embed(page[start:j if j > start else start]))
    seg = em_methanol_section(body)
    if seg is None:                 # 多品种日报里没有甲醇段 -> 宁缺毋滥, 不入队
        return None
    tm = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{2})", page)
    ts = 0
    if tm:
        try:
            ts = sane_ts(int(datetime(int(tm.group(1)), int(tm.group(2)), int(tm.group(3)),
                                      int(tm.group(4)), int(tm.group(5)),
                                      tzinfo=TZ_CN).timestamp()))
        except ValueError:
            ts = 0
    return {"body": seg, "ts": ts}


def em_ma_candidates(rows: list[dict]) -> list[dict]:
    """列表级候选 (东财 tag): 标题含甲醇 (聚合页 ~83% 是纯碱/PX/龙虎榜噪声) 且
    (强事件词 或 提示词 或 日评锚点词)."""
    out: list[dict] = []
    for r in rows:
        title = r.get("title") or ""
        if "甲醇" not in title:
            continue
        if not (chem_event_hit(title, CHEM_EVENT_STRONG)
                or any(w in title for w in CHEM_TIP)
                or any(w in title for w in EM_MA_ANCHOR)):
            continue
        out.append(r)
    return out


def em_admit(title: str, body: str) -> bool:
    """正文级准入 (东财): **正文必须含「甲醇」** + 事件门 或 日评锚点门.

    B13-F01 硬条件: 正文不含「甲醇」-> 一律 False。多品种日报切段后正文即【甲醇】段,
    单品种文整篇保留; 这条同时兜住「切段逻辑失效时回退整篇」的退路 (宁缺毋滥)。
    事件门拦「甲醇主力合约日内涨幅扩大至 2.00%」这类快讯; 锚点门放行金联创/机构日评
    (用户裁决里东财的职能就是「日评锚点」), 并用正文长度排除同名快讯。
    """
    if "甲醇" not in (body or ""):
        return False
    text = f"{title} {body}"
    if (chem_event_hit(text, CHEM_EVENT_STRONG)
            and (hit(text, STRONG) or hit(text, SPOT_DOWNSTREAM))):
        return True
    return any(w in title for w in EM_MA_ANCHOR) and len(body) >= 300


_EM_DETAIL_SEEN: set[str] = set()   # B13-F03 进程内轮转: 已取过详情的东财稿件 id


def src_em_ma(deadline: float | None = None) -> list[dict]:
    """东财 甲醇 tag (qihuo/ma.html): 全文免费, 可读到金联创被 507 挡住的转载稿.

    聚合页噪声大 -> 列表级先按标题含「甲醇」筛; 每轮最多 1 篇详情。详情
    futures.eastmoney.com/a/<id>.html 会 302 到 finance.eastmoney.com/a/<id>.html,
    故详情必须 http(..., location=True) (-L); 列表页本身直出 200, 不带 -L。

    源内轮转 (B13-F03): 每轮详情配额只有 1 篇, 旧实现恒取列表第一篇 -> 第二篇永久
    饿死, 且同一篇每轮被重复下载。改为记住**已取过详情**的稿件 id: 每轮优先取未取过
    的候选; **全都取过则不再发详情请求**。集合每轮按当前候选裁剪, 不会随进程时长
    无界增长; 纯进程内变量, 不写 state/queue, 不新增持久化字段或 `_` 元数据键。
    """
    global _EM_DETAIL_SEEN
    page = http(EM_MA_LIST_URL, EM_MA_LIST_URL, tries=2,
                deadline=deadline).decode("utf-8", "ignore")
    rows = em_ma_rows(page)
    now = int(time.time())
    cands = [r for r in em_ma_candidates(rows) if now - r["ts"] <= EM_MA_AGE_S]
    if not cands:
        _EM_DETAIL_SEEN.clear()
        return []
    _EM_DETAIL_SEEN &= {r["id"] for r in cands}      # 候选已翻出去的 id 从记忆里裁掉
    fresh = [r for r in cands if r["id"] not in _EM_DETAIL_SEEN]
    if not fresh:
        log("东财甲醇: 候选详情均已取过, 本轮不再下载")
        return []
    tried: list[str] = []
    out = _chem_feed("东财甲醇", _chem_pick("东财甲醇", fresh, EM_MA_LIST_URL, em_detail,
                                            deadline, EM_MA_DETAIL_MAX, em_admit,
                                            location=True, prefer_detail=True,
                                            tried=tried))
    _EM_DETAIL_SEEN.update(tried)
    return out


def src_lz(deadline: float | None = None) -> list[dict]:
    """隆众资讯 资讯列 (list.oilchem.net/1/9): 只用无查询串 URL (robots 禁 /*?*)."""
    return _chem_source("隆众", LZ_LIST_URL, LZ_LIST_URL,
                        lz_rows, lz_detail, deadline)


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


# 源体检 (7 源同批抓取实测, 双评审):
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
           # Batch 12 化工现货源: 接在 14 个既有抓取源之后、研报源之前 —— 既有 14 源的
           # 份额分母与研报三源的 (idx,total) 差因此都保持不变 (见 slice_remaining).
           ("化销网", src_hxw), ("隆众", src_lz),
           # Batch 13: 化工源扩容 —— 同属 CHEM_SRCS 块, 因此既有 14 源的份额分母
           # 与研报三源的 (idx,total) 差仍然保持不变 (见 slice_remaining).
           ("我的钢铁", src_mysteel), ("东财甲醇", src_em_ma),
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
NEXT_TRY_TOL_S = 60                    # next_try 未来值容差(秒): 合法上界 = RA_MAX + 本容差


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
                       ConnectionError, TimeoutError, IncompleteRead)
    # URLError 会把真正的错误塞在 .reason 里, 两层都要看
    for c in (e, getattr(e, "reason", None)):
        if c is None:
            continue
        if isinstance(c, transient_types):
            return True
        if isinstance(c, OSError) and getattr(c, "errno", None) in (
                errno.ENETUNREACH, errno.ECONNRESET, errno.ETIMEDOUT,
                errno.ECONNREFUSED, errno.EHOSTUNREACH,
                errno.ENETDOWN, errno.EADDRNOTAVAIL):   # 语义常量, 不再硬编码 macOS 字面量
            return True
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


SEND_WORST = send_worst({"proxy": "http://127.0.0.1:8080"})   # import 期兜底(3 腿 54s); main 按 conf 重算


def send_backoff_s(fails: int) -> int:
    """发送失败退避秒数: 120 * 2^min(fails, 4), 封顶 1800 (Batch 10 R6)."""
    return min(RETRY_MAX_S, RETRY_BASE_S * (2 ** min(fails, RETRY_EXP_CAP)))


def round_soft_s() -> float:
    """单轮软截止(秒) = 发送预算硬顶 - 单次 send 最坏; 与 send_time_ok() 共用同一推导.

    Batch 9 A-D-03: 同一物理阈值不再有「常量/推导/断言」三处来源。
    """
    return SEND_GATE_S - SEND_WORST


ROUND_SOFT_S = round_soft_s()   # import 期快照(3 腿口径 60s), 供静态预算断言; 运行时用 round_soft_s()


def send_time_ok(ctx: RoundContext | None = None) -> bool:
    """逐次发送闸门: 现在再发一次 send 仍不会顶到 SEND_GATE_S (= ROUND_HARD_S - ROUND_MARGIN_S).

    ROUND_START 为 0 (直接单测 / 非 main 调用) 视为无耗时压力, 不阻断.
    每个 send 点发送前都必须先过这道闸门; 不满足就把该条消息推迟到下一轮, 不算失败.
    """
    ctx = ctx or _DEFAULT_ROUND
    start = ctx.start or time.monotonic()
    return (time.monotonic() - start) + SEND_WORST <= SEND_GATE_S


def send(text: str, conf: dict, tk: str, depth: int = 0,
         ctx: SendContext | None = None) -> bool:
    ctx = ctx or _DEFAULT_SEND
    ctx.retry_after = None                   # 本轮未遇 429 时必须为 None, 不得沿用上一轮
    ctx.err = ""                             # 入口复位: 上一次的失败原因不得跨越本次调用 (Batch 9 A-F-04)
    chat_id = conf.get("chat_id") or discover_chat_id(conf, tk)
    if not chat_id:
        ctx.err = "no chat_id (先给 bot 发一条消息)"
        log(ctx.err)
        return False
    base = {"chat_id": chat_id, "disable_web_page_preview": "true"}
    for i, (proxy, timeout) in enumerate(send_plan(conf)):
        if i:
            time.sleep(3)                            # 瞬时抖动给 3s 缓和, 别整轮作废
        try:
            tg("sendMessage", {**base, "text": text, "parse_mode": "HTML"}, tk, proxy, timeout)
            ctx.err = ""
            return True
        except urllib.error.HTTPError as e:
            detail = e.read()[:200].decode(errors="ignore")
            log(f"send proxy={proxy} HTTP {e.code}: {detail}")
            if e.code == 400 and "too long" in detail.lower() and depth < 4:
                parts = split_msg(text)                  # 超长 -> 拆批重发, 不换代理重试同一 payload
                if len(parts) > 1:
                    log(f"消息超长 (u16={u16(text)}), 拆成 {len(parts)} 批重发")
                    return all(send(p, conf, tk, depth + 1, ctx) for p in parts)
            if e.code == 400 and "parse" in detail.lower():
                try:                                     # HTML 实体炸了 -> 降级纯文本
                    tg("sendMessage", {**base, "text": re.sub(r"<[^>]+>", "", text)}, tk, proxy, timeout)
                    ctx.err = ""             # 降级成功也要清因: 全局态不得残留过期失败 (Batch 9 R-E-F-02)
                    return True
                except Exception as e2:
                    log(f"plain-text fallback failed: {e2}")
            if e.code == 429:
                hdrs = getattr(e, "headers", None)
                ctx.retry_after = parse_retry_after(hdrs.get("Retry-After") if hdrs else None)
                ctx.err = "HTTP 429 限流"
                if ctx.retry_after is not None:
                    ctx.err += f" (Retry-After {int(ctx.retry_after)}s)"
                return False                             # 限流: 下轮再推, 不标记已读
            ctx.err = f"HTTP {e.code}: {detail[:120]}"
            return False                                 # 确定性失败, 不换腿重试同一 payload
        except Exception as e:
            ctx.err = f"{type(e).__name__}: {e}"
            log(f"send proxy={proxy or '直连'} failed: {ctx.err}"
                + ("" if is_transient(e) else " (非瞬时, 不再换腿)"))
            if not is_transient(e):
                return False
    return False


# ------------------------------------------------------------------ state extras
def backup_path(path: Path) -> Path:
    """损坏文件的备份路径: `<name>.bad-<ts>`; 同秒冲突时加 `-1`/`-2`..., 保证每份都留 (Batch 9 U4-F-03)."""
    base = f"{path.name}.bad-{int(time.time())}"
    cand = path.with_name(base)
    seq = 0
    while cand.exists():                       # 只读探测; 真正落盘由调用方 replace
        seq += 1
        cand = path.with_name(f"{base}-{seq}")
    return cand


def take_pending(inspect: bool = False) -> list[str]:
    """上一轮「已发送但 state 落盘失败」的待确认键 (F-07).

    inspect=True (--stats/--queue): 只读检视模式, 损坏时只 err 留痕, 不改名、不落盘 (Batch 9 U4-F-01).
    命名注记: 参数名 inspect 与标准库 inspect 模块同名 (历史命名, 与 RunOpts.inspect /
    CLI 语义一一对应); 本函数内不引用该模块, 故不构成实际遮蔽风险.
    """
    data, problem = load_state(PENDING)
    if problem:
        if inspect:
            err(f"待确认标记文件不可读({problem}), 检视模式只读, 未改动文件")
            return []
        badpath = backup_path(PENDING)
        try:
            PENDING.replace(badpath)
            err(f"待确认标记文件不可读({problem}), 已备份为 {badpath.name}")
        except OSError as e:
            err(f"待确认标记文件不可读({problem}), 备份失败({e}), 已忽略")
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
                     skipped: int = 0, partial: bool = False,
                     ctx: RoundContext | None = None) -> None:
    """全源失败告警状态机 (F-15): 首轮发 1 条 -> 之后每 ALERT_EVERY 汇总 -> 恢复另有通知.

    skipped: 本轮被预算跳过的源数, 只进文案计数, 不进失败源名单 (Batch 6.1 U-A).
    partial: 有源真失败、但同时有源「成功但 0 条」-> 文案降级为「部分失败」(Batch 9 D-F-01).
    """
    ctx = ctx or _DEFAULT_ROUND
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
        head = "仍无可用条目" if partial else "仍全部失败"
        msg = f"⚠️ 期货快讯源{head} (累计 {alert['rounds']} 轮): {detail}" + suffix
    else:
        alert = {"failing": True, "since": now, "rounds": 1, "last_sent": 0, "alerts": 0}
        head = "部分失败且无可用条目" if partial else "全部抓取失败"
        msg = f"⚠️ 期货快讯源{head}: {detail}" + suffix
    if dry:
        log("(dry-run) 告警未发送")
        seen["_alert"] = alert
        return
    if not send_time_ok(ctx):           # 逐次闸门: 再发会顶到 120s -> 推迟, rounds 已递增, last_sent 不更新
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


def note_recovered(seen: dict, conf: dict, tk: str, dry: bool,
                   ctx: RoundContext | None = None) -> bool:
    """从全源故障恢复: 故障告警已送达 (alerts>=1) 才发 1 条恢复通知, 否则只静默复位."""
    ctx = ctx or _DEFAULT_ROUND
    alert = seen.get("_alert")
    if not isinstance(alert, dict) or not alert.get("failing"):
        return False
    rounds = int(alert.get("rounds") or 0)
    since = int(alert.get("since") or 0)
    mins = max(0, (int(time.time()) - since) // 60)
    alerts = int(alert.get("alerts") or 0)
    if not alerts:                       # 告警从未送达 (发送全失败 / dry-run): 不发假"已恢复" (U-A-2)
        log(f"全源恢复: 故障告警从未送达 (故障 {rounds} 轮), 静默复位, 不发恢复通知")
    elif not dry and not send_time_ok(ctx):  # 逐次闸门: 恢复通知推迟, _alert 状态保留, 下轮重试
        log("全源恢复通知推迟到下一轮 (本轮再发会顶到 120s 上界)")
        return False
    elif not dry:
        ok = send(f"✅ 期货快讯源已恢复 (故障 {rounds} 轮 / 约 {mins} 分钟)", conf, tk)
        log(f"恢复通知{'已发送' if ok else '发送失败'} (故障 {rounds} 轮)")
        if not ok:
            return False                      # 发送失败: failing 不清零, 下轮重试 (Batch 9 D-F-03)
    seen["_alert"] = {"failing": False, "since": 0, "rounds": 0, "last_sent": 0, "alerts": 0}
    return True


# ---------------- 单源连续失败告警 (Batch 5 U2)
def src_fail_state(seen: dict) -> dict:
    """取单源失败状态块 (_src_fail: {源名: 计数块}); 类型不对时返回空 dict, 由调用方覆盖."""
    state = seen.get(SRC_FAIL_KEY)
    return state if isinstance(state, dict) else {}


def prune_src_fail(seen: dict) -> bool:
    """清掉已不在 SOURCES 里的源失败残留, 防同名源复活继承旧计数 (Batch 9 D-D-02)."""
    state = src_fail_state(seen)
    if not state:
        return False
    known = {n for n, _ in SOURCES}
    gone = sorted(n for n in state if n not in known)
    if not gone:
        return False
    for n in gone:
        state.pop(n, None)
    if state:
        seen[SRC_FAIL_KEY] = state
    else:
        seen.pop(SRC_FAIL_KEY, None)
    log(f"清理下线源失败残留: {', '.join(gone)}")
    return True


def src_alert_rounds(conf: dict) -> int:
    """单源告警阈值: conf.src_alert_rounds 覆盖, 缺省 SRC_ALERT_ROUNDS, 下限 1."""
    return max(1, to_int(conf.get("src_alert_rounds"), SRC_ALERT_ROUNDS))


def source_health(seen: dict, outcomes: list[tuple[str, str]], conf: dict,
                  tk: str, dry: bool, ctx: RoundContext | None = None) -> bool:
    """按源维护连续失败轮数: 连续 N 轮发 1 条, 之后每 SRC_ALERT_EVERY 汇总 1 条带累计轮数;
    该源恢复且此前告过警时发 1 条恢复通知并清零.

    outcomes = [(源名, ""), (源名, "错误摘要"), (源名, SRC_SKIP), ...] (与 SOURCES 同序).
    三态: "" = 成功 / 非空错误摘要 = 失败 / SRC_SKIP = 全局预算跳过 (不计失败、不清零、
    不告警、不发恢复; 跳过也不写入 errors/stats 的失败列表).
    全源失败整轮交给 alert_all_failed, 这里不计数也不发, 避免重复告警 (防御性分支:
    main 的全源失败轮已在「无可用条目」分支提前 return, 正常流程下 all_failed 不可达;
    保留该分支仅为直接调用与未来改动).
    返回是否改动过状态 (调用方据此决定落不落盘); dry=True 只记账不发送.
    """
    ctx = ctx or _DEFAULT_ROUND
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
        if all_failed:                      # 防御性: main 正常流程不可达 (见 docstring)
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
            if not send_time_ok(ctx):       # 逐次闸门: 恢复通知推迟, 状态保留不清零
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
        elif not send_time_ok(ctx):    # 逐次闸门: 告警推迟, pend/alerts/rounds 全部保留, 下轮重试
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


def render_snippet(it: dict) -> str:
    """研报渲染前对 snippet 再跑一次尾部声明剔除 (Batch 15 纵深防御).

    覆盖所有研报正文消费点 (research_line 单条 / research_parts 分片); 只读队列里的
    snippet, **不改写 state/queue**: 已落盘的老条目按原样留存, 只在推送这一刻被清理。
    """
    return pdf_tail_trim_text(str(it.get("snippet") or "").strip())


def research_line(it: dict, dt: datetime) -> str:
    """研报条目渲染 (Batch 7): 标题 / 机构·日期 / 摘要 / 「查看原文」链接.

    只对带 url 的条目生效; 非研报条目的行拼接仍走 build_messages 原分支, 逐字节不变。
    text 的「标题｜机构 · 日期」按第一个全角竖线切开 (标题里的括号不再参与解析)。

    修复 C1/C3: 非 http(s) / 非字符串 url 退回纯文本 (不渲染链接); 标题/元信息/摘要/
    URL 各有硬上限, 保证单条 u16 <= MSG_MAX (否则 split_msg 拆不开, 整批永久重试)。
    Batch 9 R-E-F-01: 标题/元信息/摘要/URL 的上限一律按**转义后 u16** 计量; 仍超 MSG_MAX 时整行降级纯文本。
    """
    url = it.get("url")
    if not is_http_url(url):                 # C1 协议白名单 + C 轻微: int url 不再抛 AttributeError
        body = it["text"]
        if len(body) > ITEM_MAX:
            body = body[:ITEM_MAX] + "…"
        return f"<b>{esc(it['src'])} {dt:%m-%d}</b>\n{esc(body)}"
    head, _, meta = it["text"].partition("｜")
    lines = [f"<b>{esc(it['src'])} {dt:%m-%d}</b>",
             f"<b>{esc_quote_limited(head, ITEM_MAX)}</b>"]
    if meta:
        lines.append(esc_quote_limited(meta, RESEARCH_META_MAX))
    snip = render_snippet(it)
    if snip:
        lines.append(esc_quote_limited(snip, SNIPPET_MAX + 20))
    href = esc_url(url) if len(url) <= RESEARCH_URL_MAX else ""
    if href and u16(href) > RESEARCH_URL_U16_MAX:
        href = ""                             # 转义后仍超预算 -> 按「链接过长」降级
    if href:
        lines.append(f'<a href="{href}">查看原文</a>')
    else:
        lines.append(f"（原文链接过长 {len(url)} 字符, 已省略）")
    line = "\n".join(lines)
    if u16(line) <= MSG_MAX:
        return line
    plain = esc_quote_limited(f"{it['src']} {dt:%m-%d} {head}", MSG_MAX - 8)
    return f"<b>{plain}</b>"                  # 保底: 整行纯文本, 保证 split_msg 一定拆得开


def research_partial(it: dict) -> bool:
    """条目是否处于「分片连载未发完」状态 (Batch 11).

    只有 0 < parts_sent < parts_total 才算连载; 老队列条目没有这两个字段 -> False,
    行为与 Batch 10 完全一致。
    """
    done, total = to_int(it.get("parts_sent")), to_int(it.get("parts_total"))
    return 0 < done < total


def split_research_body(body: str, budget: int = RESEARCH_PART_MAX) -> list[str]:
    """把清洗后的正文按「转义后 u16 <= budget」切成连续分片 (Batch 11).

    切分顺序: 句末标点 > 分句标点 > 硬切; 句末切分要求已填充 >= RESEARCH_PART_MIN_FILL,
    否则退到硬切 (避免「一个句号紧跟长句」把分片切成一两个字)。
    只在分片边界丢弃空白, 不丢任何可见字符: "".join(parts) 去掉空白后与原文去空白逐字符相同。
    """
    text = str(body or "")
    if not text:
        return []
    parts: list[str] = []
    start, n = 0, len(text)
    while start < n:
        used, i, cut_at, cut_used = 0, start, -1, 0
        while i < n:
            cost = u16(esc_quote(text[i]))
            if used + cost > budget:
                break
            used += cost
            i += 1
            if text[i - 1] in RESEARCH_SENT_END:
                cut_at, cut_used = i, used
        if i >= n:                                  # 剩余全部装得下
            parts.append(text[start:].strip())
            break
        end = cut_at if (cut_at > start and cut_used >= RESEARCH_PART_MIN_FILL) else i
        parts.append(text[start:end].strip())
        start = end
        while start < n and text[start] == " ":
            start += 1
    return [p for p in parts if p]


def research_head(it: dict, dt: datetime, i: int, n: int) -> list[str]:
    """分片头行 (Batch 11): 源/日期 + (i/n) + 标题; 元信息只放首片.

    标题预算按片位取值: 带链接的末片用 RESEARCH_TITLE_PART_MAX, 其余片沿用 ITEM_MAX,
    这样「700 字标题 + 2400 字正文 + 1000 u16 链接」也不会顶到 MSG_MAX。
    """
    title_max = RESEARCH_TITLE_PART_MAX if i >= n else ITEM_MAX
    head, _, meta = it["text"].partition("｜")
    lines = [f"<b>{esc(it['src'])} {dt:%m-%d}</b> {RESEARCH_PART_MARK.format(i=i, n=n)}",
             f"<b>{esc_quote_limited(head, title_max)}</b>"]
    if meta and i == 1:
        lines.append(esc_quote_limited(meta, RESEARCH_META_MAX))
    return lines


def research_parts(it: dict, dt: datetime) -> list[str]:
    """研报条目 -> 全部分片消息文本 (Batch 11). 纯函数: 只依赖条目字段与 dt.

    单片且整条消息 <= MSG_MAX 时**逐字节**沿用 research_line() —— 非分片路径零改动,
    老条目 (240 字) 与无正文条目因此与 Batch 10 完全一致 (B11-06)。
    """
    body = render_snippet(it)
    if not is_http_url(it.get("url")) or not body:
        return [research_line(it, dt)]
    parts = split_research_body(body, RESEARCH_PART_MAX)
    if len(parts) == 1:
        single = research_line(it, dt)
        if u16(single) <= MSG_MAX and "\n" in single:
            # 富文本研报行必然多行; 单行 = research_line 已降级为纯文本 (正文已丢), 必须强制分片
            return [single]
        # 正文装得进一片, 但标题/链接太长把整行顶爆: 强制切两片, 别退化成「丢正文的纯文本」
        parts = split_research_body(body, max(1, u16(esc_quote(body)) // 2 + 1))
        if len(parts) == 1:
            return [single]
    n = len(parts)
    href = esc_url(it["url"]) if len(it["url"]) <= RESEARCH_URL_MAX else ""
    if href and u16(href) > RESEARCH_URL_U16_MAX:
        href = ""
    out: list[str] = []
    for i, chunk in enumerate(parts, 1):
        lines = research_head(it, dt, i, n) + [esc_quote(chunk)]
        if i == n:
            lines.append(f'<a href="{href}">查看原文</a>' if href
                         else f"（原文链接过长 {len(it['url'])} 字符, 已省略）")
        out.append("\n".join(lines))
    return out


# ------------------------------------------------------------------ main
def build_messages_ex(batch: list[dict], part_budget: int | None = None
                      ) -> list[tuple[str, list[dict], dict[str, tuple[int, int]]]]:
    """整行拼接 + 研报全文分片 (Batch 11): 返回 (文本, 本条消息完成投递的条目, 分片进度).

    第三元素 {key: (已发片数, 总片数)} 只对「本条消息发出但条目还没连载完」的分片消息非空:
    flush_round 据此把续发进度写回队列, 中断后从下一片继续 (不重发已发片)。
    研报分片**独占消息**: 进入分片前先 flush 已攒的普通条目, 分片之间也不与任何条目同条。
    part_budget 是每轮分片消息上限 (限流); 用尽时剩余条目本轮不渲染, 由调用方推迟到下一轮。
    """
    if part_budget is None:                       # 调用时读常量: 便于单测 patch 与配置覆写
        part_budget = RESEARCH_PARTS_PER_ROUND
    msgs: list[tuple[str, list[dict], dict[str, tuple[int, int]]]] = []
    lines, used, budget = [], [], 0
    emitted = 0                                   # 本轮已渲染的研报分片消息数
    now_date = datetime.now(TZ_CN).date()
    for it in batch:
        dt = datetime.fromtimestamp(it["ts"], TZ_CN)
        parts = research_parts(it, dt) if is_http_url(it.get("url")) else []
        if len(parts) > 1:                        # 研报全文分片: 独占消息
            if lines:
                msgs.append(("\n\n".join(lines), used, {}))
                lines, used, budget = [], [], 0
            n = len(parts)
            start = min(max(0, to_int(it.get("parts_sent"))), n - 1)   # 越界夹取: 不丢条目也不死循环
            for i in range(start, n):
                if emitted >= part_budget:
                    return msgs                   # 分片预算用尽: 剩余条目推迟到下一轮
                done = i + 1
                msgs.append((parts[i], [] if done < n else [it],
                             {it["key"]: (done, n)} if done < n else {}))
                emitted += 1
            continue
        body = it["text"]
        if len(body) > ITEM_MAX:
            body = body[:ITEM_MAX] + "…"
        stamp = dt.strftime("%H:%M") if dt.date() == now_date else dt.strftime("%m-%d %H:%M")
        line = (parts[0] if parts
                else f"<b>{esc(it['src'])} {stamp}</b>\n{esc(body)}")
        add = u16(line) + (2 if lines else 0)
        if lines and budget + add > MSG_MAX:
            msgs.append(("\n\n".join(lines), used, {}))
            lines, used, budget = [], [], 0
            add = u16(line)
        lines.append(line)
        used.append(it)
        budget += add
    if lines:
        msgs.append(("\n\n".join(lines), used, {}))
    return msgs


def build_messages(batch: list[dict]) -> list[tuple[str, list[dict]]]:
    """兼容旧调用: 丢掉分片进度 (Batch 11 起内部走 build_messages_ex)。"""
    return [(text, used) for text, used, _prog in build_messages_ex(batch)]


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


def slice_remaining(idx: int, total: int) -> int:
    """单源份额的分母 (Batch 12): 既有源沿用加源前的「剩余源数」口径.

    新接入的 CHEM_SRCS 不占既有源的份额分母, 45s 预算下既有源的 src_dl 与加源前
    逐秒相同; 新源自己按 total - idx 计 (它们只吃 floor(MIN_SLICE)).
    """
    chem_after = sum(1 for n, _ in SOURCES[idx + 1:] if n in CHEM_SRCS)
    return max(1, total - idx - chem_after)


def _resolve_annotations(cls):
    """把本类的字符串注解还原为类型对象 (R2).

    为何需要: monitor.py 用 ``from __future__ import annotations``, 模块内全部注解
    都是字符串; 而本仓库的离线测试 (tests/sim.py、probe2.py、ab.py) 用
    ``module_from_spec`` + ``exec_module`` 载入本文件**但不注册 sys.modules**;
    此时 dataclass 解析注解会因 ``sys.modules.get(cls.__module__) is None`` 而抛
    AttributeError. 这里在 @dataclass 之前用本模块 globals 求值注解, 保持 dataclass
    语义不变 (求值后仍是真类型对象, 不是字符串).

    去掉会怎样: 上述三个离线入口在 **import 阶段**即抛 AttributeError (R2 实测),
    sim / probe2 / ab 全部不可用; golden 走常规 import 路径, 不经过该垫片,
    所以这是一个只有离线测试才会踩到的**隐式约束**, 不能凭 golden 全绿就删掉.

    隐式约束 (改本类定义时必须同时满足, 否则同样抛错):
    ① 装饰器顺序固定: ``@dataclass`` 在外、``@_resolve_annotations`` 在内 ——
       必须先求值注解, dataclass 再消费; 顺序反过来垫片拿到的已是处理过的类.
    ② 注解必须能在本模块 globals 中求值: 前向引用到**后定义**的类会 NameError,
       因此被引用的类型要么已定义, 要么写成字符串以外的可求值形式.
    """
    ann = cls.__dict__.get("__annotations__", {})
    if ann:
        g = globals()
        cls.__annotations__ = {k: (eval(v, g) if isinstance(v, str) else v)
                               for k, v in ann.items()}
    return cls


@dataclass(frozen=True)
@_resolve_annotations
class RunOpts:
    """load_conf 的产物: 本轮 CLI/conf 解析结果 (R2 拆分 main 的入口参数对象)."""

    dry: bool
    seed: bool
    stats: bool
    explain: bool
    force_flush: bool
    show_queue: bool
    inspect: bool
    digest_items: int
    digest_min_items: int
    max_hold: int
    digest_min: int
    keywords: list[str]
    use_filter: bool
    budget: int


@dataclass
@_resolve_annotations
class FetchResult:
    """fetch_round 的产物; rc 非 None 时 main 直接透传退出码 (R2 的唯一退出码通道)."""

    rc: int | None
    items: list[dict]
    errors: list[str]
    outcomes: list[tuple[str, str]]


@dataclass
@_resolve_annotations
class DigestPlan:
    """digest_policy 的产物; rc 非 None 时 main 直接透传退出码 (R2 的唯一退出码通道).

    queue/stale/meta/day 与基线局部变量是**同一对象**, flush_round 原地修改.
    """

    rc: int | None = None
    queue: list[dict] = field(default_factory=list)
    stale: list[dict] = field(default_factory=list)
    meta: dict = field(default_factory=dict)
    day: dict = field(default_factory=dict)
    due: bool = False
    cap_hit: bool = False
    aging_on: bool = False
    queue_dirty: bool = False
    added: int = 0
    now_f: float = 0.0
    now_ts: int = 0
    max_age: int = 0
    digest_items: int = 0
    digest_min_items: int = 0
    max_hold: int = 0
    waited: float = 0.0
    interval: int = 0
    oldest_age: float = 0.0
    next_try: float = 0.0


def load_conf(args: list[str]) -> tuple[dict, RunOpts]:
    """解析 CLI 与 conf (R2 阶段 1). 只读 CONF, 不写 CFG、不加锁、不读 state.

    连续实现 (R2 F-01 收口): 本函数是旧 main 中三段**纯解析**切片按原顺序的连续
    重装配 —— FLAGS(参数开关) -> CFGPARSE(conf 派生) -> BUDGET(预算). 三段都只读
    args / CONF, 因此一次解析完再交给 main 做 CFG 注入 / 加锁 / 读 state, 行为不变.
    """
    # 检视模式 (--stats / --queue) 一律按 dry 语义: 不发送、零落盘、rc=0 (Batch 9 D-D-03 / U4-F-01/F-02)
    # 变量名 inspect 与标准库 inspect 模块同名 (历史命名): 本函数内不引用该模块,
    # 该布尔值原样进入 RunOpts.inspect (字段名沿用规格 canonical 命名).
    inspect = "--stats" in args or "--queue" in args
    dry = "--dry-run" in args or inspect
    seed = "--seed" in args
    force_all = "--all" in args
    stats = "--stats" in args or dry
    explain = "--explain" in args
    conf = load_json(CONF, {})
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
        max_hold = max(MAX_HOLD_FLOOR_S, int(conf.get("max_hold_min") or MAX_HOLD_MIN) * 60)
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
    budget = BUDGET_S
    try:
        budget = max(BUDGET_FLOOR_S, int(conf.get("timeout_s") or BUDGET_S))
    except (TypeError, ValueError):
        budget = BUDGET_S
    return conf, RunOpts(
        dry=dry, seed=seed, stats=stats, explain=explain,
        force_flush=force_flush, show_queue=show_queue, inspect=inspect,
        digest_items=digest_items, digest_min_items=digest_min_items,
        max_hold=max_hold, digest_min=digest_min, keywords=keywords,
        use_filter=use_filter, budget=budget)


def fetch_round(conf: dict, opts: RunOpts, seen: dict, ctx: RoundContext,
                deadline: float) -> FetchResult:
    """19 源抓取循环 + 研报节流 + TS_BAD 归因 (R2 阶段 2). 不发告警、不落盘."""
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
            if last_run > now_run + ROLLBACK_TOL_S:       # B1 修复: 时钟回拨/NTP 前跳后校正 -> 视为未运行
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
            log(f"{name}: 跳过, 全局预算 {opts.budget}s 已耗尽")
            continue
        src_dl = slice_deadline(deadline, now, slice_remaining(idx, total))   # 单源份额: 慢源吃不掉整轮 (6.1 U-B / B12)
        t0 = time.monotonic()
        ok = False
        try:
            got = fn(src_dl)
            items.extend(got)
            outcomes.append((name, ""))
            ok = True
            bad = ctx.ts_bad.get(name, 0)
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
            ra[name] = now_run if ok else now_run - (RESEARCH_EVERY_S - RESEARCH_RETRY_S)
        seen["_research"] = ra

    skipped_all = bool(outcomes) and all(e == SRC_SKIP for _, e in outcomes)
    if skipped_all:                           # 全轮被预算跳过: 既无成功也无失败 -> 不告警、不返回失败
        log(f"全部 {len(outcomes)} 源被预算跳过, 本轮不抓取也不告警")
        return FetchResult(rc=0, items=items, errors=errors,
                           outcomes=outcomes)
    return FetchResult(rc=None, items=items, errors=errors,
                        outcomes=outcomes)


def digest_policy(conf: dict, opts: RunOpts, seen: dict,
                  fetched: FetchResult, ctx: RoundContext) -> DigestPlan:
    """未来时间/去重/近邻合并/过滤/seed/队列自愈/聚合节奏 (R2 阶段 3).

    不发聚合消息; 不落 queue/state (seed 分支除外).
    """

    now_ts = int(time.time())
    live, dropped_future = [], 0
    for it in fetched.items:
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
                  else filter_reason(it["text"], opts.keywords, opts.use_filter))
        if reason:
            dropped.append((reason, it))
        else:
            relevant.append(it)

    seen_keys = {k for k in seen if not k.startswith("_")}
    if (opts.seed or not seen_keys) and not opts.dry:
        for it in relevant:
            seen[it["key"]] = now_ts
        if save_json(STATE, prune(seen)):
            clear_pending()                   # 只有 seed 真正落盘才清待确认标记 (Batch 9 D-F-04)
        else:
            err("seed 落盘失败, 下轮会重复 seed, 保留待确认标记")
        log(f"seeded {len(relevant)} items (跨源已合并), nothing pushed")
        return DigestPlan(rc=0)

    fresh = [it for it in relevant if it["key"] not in seen]
    max_age = int(conf.get("max_age_min", 180)) * 60
    stale = [it for it in fresh if now_ts - it["ts"] > max_age]
    fresh = [it for it in fresh if now_ts - it["ts"] <= max_age]

    # ---- v5 聚合: 本轮的 fresh 全部入队, 攒到窗口再合成一条发
    # (替掉 v4 的「有就推 + 超出 limit 直接丢」)
    queue, q_problem, q_bad = load_queue()
    if q_problem and opts.inspect:                         # 检视模式零落盘: 只留痕, 不改名 (Batch 9 U4-F-01)
        err(f"聚合队列不可读({q_problem}), 检视模式只读, 未改动文件")
        queue = []
    elif q_problem:                                   # 队列坏了: 先留证据再重建, 不静默清空
        badpath = backup_path(QUEUE)
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
    interval = opts.digest_min * 60
    now_f = time.time()
    last_flush = to_float(meta.get("last_flush"))
    if not last_flush or last_flush > now_f + ROLLBACK_TOL_S:     # 首轮 / 时钟回拨
        last_flush = now_f
        meta["last_flush"] = last_flush
    waited = now_f - last_flush
    next_try = to_float(meta.get("next_try"))
    if next_try > now_f + RA_MAX + NEXT_TRY_TOL_S:
        # 时钟前跳期间写下的 next_try 在回拨后遥遥在未来; 钳到 0 (无退避), 否则队列冻结
        # 时长 ≈ 回拨幅度, 积压超 QUEUE_MAX 后按最旧丢消息 (Batch 9 D-F-02)
        log(f"next_try 在未来 {int(next_try - now_f)}s, 超出 {RA_MAX + NEXT_TRY_TOL_S}s 上限, 按无退避处理")
        next_try = 0.0
        meta["next_try"] = 0.0
    fails_n = to_int(meta.get("fails"))
    day = day_counter(seen, datetime.now(TZ_CN).strftime("%Y-%m-%d"))
    cap_hit = to_int(day.get("msgs")) >= DAILY_MSG_CAP and not opts.force_flush

    # 老化 (V1-P1-2 + V3-P1): 只有「本来能发」的轮次才清理; 被 cap/退避堵住的时长
    # block_acc>0 时暂停老化 -> 解冻后先补发而不是先判死 (它只当布尔门闩用)。
    oldest_age = (now_f - min((it["ts"] for it in queue), default=now_f)) if queue else 0
    # fails_n > 0 表示上一轮发失败: 退避到期必须重试, 不能等别的条件凑齐 (V1-P2-7)
    due = (bool(queue) and (opts.force_flush
           or len(queue) >= opts.digest_items
           or (waited >= interval and len(queue) >= opts.digest_min_items)
           or oldest_age >= opts.max_hold
           or (bool(meta.get("left_pending")) and waited >= LEFT_PENDING_RETRY_S)
           or fails_n > 0))
    if next_try > now_f and not opts.force_flush:          # 发送失败后的退避
        due = False

    frozen = (next_try > now_f and not opts.force_flush) or cap_hit
    last_round = to_float(meta.get("last_round"))
    rollback = last_round > now_f + ROLLBACK_TOL_S   # 时钟回拨 (NTP 跳变/手改表)
    if rollback:
        last_round = 0.0
    gap = now_f - last_round if (last_round and now_f > last_round) else 0.0
    if rollback:
        gap = max(gap, LONG_GAP_S)                   # 按长间隔计 -> 关掉老化, 先补发
    meta["last_round"] = now_f
    # block_acc > 0 表示「上次排空后被堵过」; 它只当布尔门闩用, 累加秒数仅供诊断
    block_acc = to_float(meta.get("block_acc"))
    if frozen or gap >= LONG_GAP_S:                   # 退避/触顶/休眠长间隔都算阻塞 (V4-P1)
        block_acc = min(BLOCK_ACC_CAP_S, block_acc + max(gap, BLOCK_ACC_STEP_MIN_S))
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

    if opts.stats:
        log(f"raw={len(fetched.items)} merged_exact={len(merged_exact)} merged={len(merged)} "
            f"near_merged={len(near_pairs)} relevant={len(relevant)} fresh={len(fresh)} "
            f"stale={len(stale)} queued={len(queue)}(+{added}/dup{dup_q}) "
            f"queue_drop={len(dropped_q)} due={due} cap={cap_hit} oldest={oldest_age / 60:.0f}m "
            f"next_try={max(0, next_try - now_f) / 60:.0f}m "
            f"dropped_ts={ctx.ts_bad.get('total', 0)} dropped_future={dropped_future} "
            f"dropped_short={dropped_short} filter={'on' if opts.use_filter else 'off'} "
            f"errors={fetched.errors or '-'}")
    per_src: dict[str, int] = {}
    shown = 0
    for reason, it in dropped:                    # 每源最多 2 行, 共 12 行
        src = it["src"]
        if per_src.get(src, 0) >= 2 or shown >= 12:
            continue
        per_src[src] = per_src.get(src, 0) + 1
        shown += 1
        log(f"  丢弃[{reason}] {src}: {it['text'][:42]}")
    if opts.explain:
        print(f"--- 过滤样本 (共 {len(dropped)} 条) ---")
        for reason, it in dropped[:20]:
            print(f"  [{reason}] {it['src']} {it['text'][:80]}")
        for prev, dup, ratio in near_pairs[:20]:
            print(f"  [近邻合并 {ratio:.3f}] {dup['src']} {dup['text'][:46]} "
                  f"-> {prev['src']} {prev['text'][:46]}")

    if opts.show_queue:
        for it in queue[:30]:
            print(f"  {datetime.fromtimestamp(it['ts'], TZ_CN).strftime('%m-%d %H:%M')} "
                  f"[{it['src']}] {it['text'][:60]}")
        last_flush_txt = (datetime.fromtimestamp(last_flush, TZ_CN).strftime("%m-%d %H:%M")
                          if last_flush > 0 else "无")
        print(f"--- 队列 {len(queue)} 条 / 上次发送 {last_flush_txt}"
              f"({waited / 60:.0f} 分钟前) / 窗口 {opts.digest_min} 分钟 (最少 {opts.digest_min_items} 条)"
              f" / 今日已发 {int(day.get('msgs') or 0)}/{DAILY_MSG_CAP} 条"
              f" / 最早一条 {oldest_age / 60:.0f} 分钟前 ---")
        return DigestPlan(rc=0)

    if opts.dry:
        for it in queue[-15:]:
            print(f"  {datetime.fromtimestamp(it['ts'], TZ_CN).strftime('%H:%M')} "
                  f"[{it['src']}] {it['text'][:70]}")
        print(f"--- (dry-run) 队列 {len(queue)} 条, due={due}, "
              f"还差 {max(0, interval - waited) / 60:.0f} 分钟到窗口 ---")
        return DigestPlan(rc=0)

    return DigestPlan(
        rc=None, queue=queue, stale=stale, meta=meta, day=day, due=due,
        cap_hit=cap_hit, aging_on=aging_on, queue_dirty=queue_dirty, added=added,
        now_f=now_f, now_ts=now_ts, max_age=max_age,
        digest_items=opts.digest_items, digest_min_items=opts.digest_min_items,
        max_hold=opts.max_hold, waited=waited, interval=interval,
        oldest_age=oldest_age, next_try=next_try)


def flush_round(conf: dict, opts: RunOpts, seen: dict,
                plan: DigestPlan, tk: str, pending: list[str],
                ctx: RoundContext, send_ctx: SendContext) -> int:
    """四条发送路径 + 老化/退避 + state/queue/pending 落盘 (R2 阶段 4).

    落盘顺序固定为「先 state 后队列」; rc=5 判定优先于 rc=2.
    """

    if opts.force_flush and not plan.queue:                     # V1-P1-1: 空队列强制刷新不能算发送失败
        record_stale(seen, plan.stale, plan.max_age)
        if not persist_state(seen):
            err("state 落盘失败 (--flush-now 空队列)")
            return 5                                  # 5 = state 落盘失败 (Batch 9 D-F-06)
        print("队列为空, 无需发送")
        return 0

    if not plan.due or plan.cap_hit:
        why = ("队列空" if not plan.queue else
               (f"今日通知已达上限 {DAILY_MSG_CAP}" if plan.cap_hit else
                (f"退避中, {max(0, plan.next_try - plan.now_f) / 60:.0f} 分钟后重试"
                 if plan.next_try > plan.now_f else
                 (f"未到窗口 ({plan.waited / 60:.0f}/{opts.digest_min} 分钟, 队列 {len(plan.queue)} 条)"
                  if plan.waited < plan.interval else
                  f"窗口已过, 但只有 {len(plan.queue)} 条 < {plan.digest_min_items} 条, "
                  f"最早一条 {plan.oldest_age / 60:.0f}/{plan.max_hold / 60:.0f} 分钟 (到点兜底发)"))))
        notice_ok = True
        if plan.cap_hit and not cap_notified_already(plan.day):
            if not send_time_ok(ctx):    # 逐次闸门: 触顶提示推迟, cap_notified/msgs 不动
                log("今日触顶提示推迟到下一轮 (本轮再发会顶到 120s 上界)")
            else:
                plan.day["cap_notified"] = True
                plan.day["msgs"] = int(plan.day.get("msgs") or 0) + 1
                notice_ok = send(f"⚠️ 今日快讯已达 {DAILY_MSG_CAP} 条上限, 新条目暂缓推送, "
                                 f"明天 0 点恢复 (队列上限 {QUEUE_MAX} 条, 超出按最旧丢弃)", conf, tk)
                if not notice_ok:
                    plan.day["cap_notified"] = False          # 没发出去就下轮再试
                    plan.day["msgs"] = to_int(plan.day.get("msgs")) - 1
                    plan.meta["next_try"] = plan.now_f + retry_gap(CAP_NOTICE_RETRY_S, send_ctx.retry_after)  # 别每 120s 打一次 (V1-P2-3a)
                    err(f"触顶提示发送失败, {int(plan.meta['next_try'] - plan.now_f)}s 后重试")
        if plan.queue_dirty and not save_queue(plan.queue):
            err("队列落盘失败, 下轮可能重复入队")
            queue_save_ok = False
        else:
            queue_save_ok = True
        record_stale(seen, plan.stale, plan.max_age)            # 有 stale 时内部会落盘
        if not plan.stale or not notice_ok:                # P0-1: _digest/_day 必须每轮落盘
            if not persist_state(seen):
                err("state 落盘失败 (本轮未发送), 下轮计数从旧值起算")
        log(f"queued 队列 {len(plan.queue)} 条 (+{plan.added}), {why}, 本轮不发送")
        return 0 if queue_save_ok else 6

    # ---- 本轮软截止 (M1/F-02): 抓取 + 已发消息耗时过长 -> 聚合发送推迟到下一轮,
    # 队列原样保留 (本轮新增条目照常入队落盘), 不算发送失败. 与逐次闸门取严.
    if time.monotonic() - ctx.start > round_soft_s() or not send_time_ok(ctx):
        log("本轮耗时超软截止, 聚合发送推迟到下一轮")
        queue_save_ok = True
        if plan.queue_dirty and not save_queue(plan.queue):
            err("队列落盘失败, 下轮可能重复入队")
            queue_save_ok = False
        record_stale(seen, plan.stale, plan.max_age)
        if not plan.stale:
            if not persist_state(seen):
                err("state 落盘失败 (软截止推迟), 下轮计数从旧值起算")
        return 0 if queue_save_ok else 6

    # ---- 到点: 按时间排序, 单轮最多发 digest_items 条 (避免一口气几十条撞限流)
    plan.queue.sort(key=lambda x: (x["ts"], x["src"]))
    batch = plan.queue[:plan.digest_items]
    rest_after = plan.queue[plan.digest_items:]
    msgs = build_messages_ex(batch)
    # Batch 11: 分片预算用尽时 build_messages_ex 只渲染了一部分条目 -> 本轮不得老化
    _rendered = {it["key"] for _t, _u, _p in msgs
                 for it in (_u + [{"key": k} for k in _p])}
    parts_deferred = len(_rendered) < len(batch)
    part_progress: dict[str, tuple[int, int]] = {}   # key -> (已发片数, 总片数)
    sent: list[dict] = []

    # ---- Batch 11 (CTO 追加): 毒丸 —— 同一片连续失败 3 次 -> 放弃该篇剩余片 ----
    def _b11_item(_key: str) -> dict | None:
        return next((it for it in batch if it["key"] == _key), None)

    failed_it: dict | None = None    # 本轮失败分片所属条目 (None = 失败的不是研报分片)
    poison: list[dict] = []          # 判毒丸的条目: 出队 + 写 seen + err 留痕
    sent_msgs = 0
    failed = False
    deferred = False
    for idx, (text, used, prog) in enumerate(msgs, 1):
        if not send_time_ok(ctx):         # Batch 9 A-F-01: 每条消息发送前都过闸门
            deferred = True
            log(f"第 {idx}/{len(msgs)} 批发送推迟到下一轮 "
                f"(再发会顶到 {SEND_GATE_S}s 上界), 已送达 {len(sent)} 条")
            break
        if not send(text, conf, tk):
            failed = True
            # [CTO 毒丸] 失败的是哪一篇的哪一片: 非末片看 prog; 末片 used=[it] 且 prog 为空,
            # 用本轮已累计的进度或队列里的 parts_* 认出来 (普通消息不参与毒丸计数)。
            _fk = next(iter(prog), "")
            if not _fk and len(used) == 1:
                _cand = used[0]
                _ck = str(_cand.get("key") or "")
                _prog = part_progress.get(_ck)
                _pt = to_int(_cand.get("parts_total"))
                _ps = to_int(_cand.get("parts_sent"))
                if (_prog and _prog[1] > 1) or (_pt > 1 and _ps >= _pt - 1):
                    _fk = _ck
            if _fk:
                failed_it = _b11_item(_fk)
            err(f"第 {idx}/{len(msgs)} 批发送失败 (u16={u16(text)}, 原因: {send_ctx.err or '未知'}), "
                f"已送达 {len(sent)} 条")
            break
        sent.extend(used)
        for _k, _v in prog.items():                # 只记「已送达」的片, 失败片不前进
            part_progress[_k] = max(part_progress.get(_k, (0, 0)), _v)
        for _k in prog:                            # [CTO] 成功发出任意新片 -> 毒丸计数清零
            _it0 = _b11_item(_k)
            if _it0 is not None:
                _it0.pop("part_fails", None)
        sent_msgs = idx
    ok = (sent_msgs > 0 or deferred or parts_deferred) and not failed
    if parts_deferred:                             # 分片预算用尽: 不算失败, 也不得老化
        deferred = True
        log(f"研报分片预算 {RESEARCH_PARTS_PER_ROUND} 片用尽, 剩余条目推迟到下一轮")
    by_key = {it["key"]: it for it in batch}
    for _k, (_done, _total) in part_progress.items():   # 续发进度写回队列条目
        _it = by_key.get(_k)
        if _it is not None:
            _it["parts_sent"] = _done
            _it["parts_total"] = _total
    # [CTO 毒丸] 同一片连续失败 RESEARCH_PART_FAIL_MAX 次 -> 放弃该篇剩余片:
    # 出队 + 写 seen (不再被源站重推/不再占预算) + err 留痕; 成功发片时计数已在上方清零。
    if failed_it is not None:
        _fails = to_int(failed_it.get("part_fails")) + 1
        if _fails >= RESEARCH_PART_FAIL_MAX:
            poison.append(failed_it)
            seen[failed_it["key"]] = plan.now_ts
            failed_it.pop("part_fails", None)
            err(f"研报分片连续失败 {RESEARCH_PART_FAIL_MAX} 次, 放弃剩余分片并记账已读: "
                f"{failed_it['key']}")
        else:
            failed_it["part_fails"] = _fails
    sent_keys = {it["key"] for it in sent}
    poison_keys = {it["key"] for it in poison}      # [CTO] 毒丸: 出队, 不再重试
    left = rest_after + [it for it in batch
                         if it["key"] not in sent_keys and it["key"] not in poison_keys]
    cur_msgs = to_int(plan.day.get("msgs")) + sent_msgs
    if sent and opts.force_flush and cur_msgs > DAILY_MSG_CAP:
        err(f"--flush-now 越限推送: 今日 {cur_msgs}/{DAILY_MSG_CAP} 条")   # 人工覆盖留痕
    if sent:
        for it in sent:                               # 只有真正送达才标记已读
            seen[it["key"]] = plan.now_ts
    if sent_msgs:                                     # B11-F01: 日计数按实发消息条数, 与条目是否完成解耦
        plan.day["msgs"] = int(plan.day.get("msgs") or 0) + sent_msgs
    if ok and not deferred and plan.aging_on:              # 推迟轮不得触发老化 (Batch 9 A-F-01)
        aged = [it for it in left if plan.now_ts - it["ts"] > plan.max_age
                and not research_partial(it)]
        # Batch 11: 连载未发完的研报条目先豁免老化 (否则用户永远看不到后半篇),
        # 但超过 RESEARCH_PARTIAL_AGE_FACTOR 倍 max_age 仍清掉, 防止队列被永久占位。
        aged += [it for it in left if research_partial(it)
                 and plan.now_ts - it["ts"] > plan.max_age * RESEARCH_PARTIAL_AGE_FACTOR]
        if aged:
            akeys = {it["key"] for it in aged}
            left = [it for it in left if it["key"] not in akeys]
            plan.stale.extend(aged)
    if sent and not failed and not left:
        plan.meta["last_flush"] = plan.now_f                    # 老化后才算真排空, 排空才推进窗口
    if not left:                                      # 队列排空 -> 阻塞补偿归零
        plan.meta["block_acc"] = 0.0
    rest_count = len(left)                            # 老化清掉的不能算剩货 (V4-P2)
    plan.meta["left_pending"] = bool(rest_count > 0 and ok)   # 部分/单轮封顶后继续补发
    if not ok:                                        # 失败退避: 120s 起翻倍, 封顶 30 分钟
        fails = to_int(plan.meta.get("fails")) + 1
        plan.meta["fails"] = fails
        plan.meta["next_try"] = plan.now_f + retry_gap(send_backoff_s(fails),
                                             send_ctx.retry_after)
        err(f"第 {fails} 次发送失败, {int(plan.meta['next_try'] - plan.now_f)}s 后重试"
            + (" (采纳 Retry-After)" if send_ctx.retry_after is not None else ""))
    else:
        plan.meta["fails"] = 0
        plan.meta.pop("next_try", None)

    queue_save_ok = True
    # 此处 dry 恒为 False: --dry-run/--stats/--queue 已在前面 return (Batch 9 A-F-03)
    state_ok = persist_state(seen)                # 先落 state, 再落队列
    if sent and not state_ok:
        keys = sorted(sent_keys | set(pending))
        wrote = write_pending(keys)
        err(f"state 落盘失败, 已写待确认标记{'成功' if wrote else '失败'}, "
            f"下轮不会重复推送同一批")
    elif sent:
        clear_pending()
    if (plan.queue_dirty or sent or part_progress or not ok) and not save_queue(left):
        err("队列落盘失败, 已发条目下轮可能重复推送")
        queue_save_ok = False
    # ==== U4-9 (D-F-05) 独立块: state 落盘失败时 rc=5 前补 stale 记账 ====
    # 刻意不并入下一段 U1-3 的死条件块: 回滚 U1-3 时保留本块, 回滚本块时保留 rc=5.
    if sent and not state_ok:
        record_stale(seen, plan.stale, plan.max_age)
    if sent and not state_ok:
        return 5
    log(f"pushed {len(sent)}/{len(batch)} msgs={sent_msgs} ok={ok} left={len(left)} "
        f"day_msgs={int(plan.day.get('msgs') or 0)}")
    record_stale(seen, plan.stale, plan.max_age)                # F-08: 推送之后记账, 失败不阻断
    if not ok:
        if not queue_save_ok:
            err("注意: 本轮发送失败且队列落盘失败, 返回 2 (队列异常需人工检查)")
        return 2
    return 0 if queue_save_ok else 6


# ---------------------------------------------------------------- selftest helpers
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
    real_err, real_ra = globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"]

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
        globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"] = real_err, real_ra


def _send_429(header: str | None) -> tuple[float | None, int, bool]:
    """模拟 429 (可带 Retry-After): 返回 (SEND_RETRY_AFTER, tg 调用次数, send 结果)."""
    calls: list[str] = []
    hdrs = {} if header is None else {"Retry-After": header}
    real_tg = globals()["tg"]
    real_err, real_ra = globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"]

    def fake_tg(method, payload, tk, proxy, timeout=25):
        calls.append(proxy or "直连")
        raise urllib.error.HTTPError("https://api.telegram.org", 429, "Too Many",
                                     hdrs, io.BytesIO(b'{"ok":false}'))

    got: float | None = None
    try:
        globals()["tg"] = fake_tg
        ok = send("probe", {"chat_id": 1, "proxy": ""}, "tk")
        got = _DEFAULT_SEND.retry_after      # 先取值再还原, 返回值不受还原影响
    finally:
        globals()["tg"] = real_tg
        globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"] = real_err, real_ra
    return got, len(calls), ok


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


def main() -> int:
    global SEND_WORST

    args = sys.argv[1:]

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

    conf, opts = load_conf(args)

    round_ctx = RoundContext()
    send_ctx = SendContext()
    round_ctx.cfg.clear()
    round_ctx.cfg.update(conf)        # 供无参源函数读取 proxy (Batch 6 U2)
    SEND_WORST = send_worst(conf)     # 按实际腿数重算: 无代理 31s / 三腿 54s (Batch 9 A-F-02)

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
    if not opts.dry and not tk:
        err(f"缺少 bot token: 需设置 FUTNEWS_TOKEN 或 {CONF} 的 token 字段")
        return 3

    pending = take_pending(opts.inspect)
    if pending:
        log(f"合并上轮待确认已发送标记: {len(pending)} 条")
        for key in pending:
            seen.setdefault(key, int(time.time()))
        if not opts.dry:                                  # 合并成功才清标记, 否则下轮重试 (F-07)
            if save_json(STATE, prune(seen)):
                clear_pending()
            else:
                err("待确认标记合并落盘失败, 保留标记下轮重试")

    round_ctx.start = time.monotonic()      # 本轮起点: 聚合软截止 + 逐次发送闸门共用 (Batch 6 M1/F-02)
    deadline = time.monotonic() + opts.budget

    round_ctx.ts_bad.clear()

    fetched = fetch_round(conf, opts, seen, round_ctx, deadline)
    if fetched.rc is not None:
        return fetched.rc

    if not fetched.items:
        skipped = sum(1 for _, _e in fetched.outcomes if _e == SRC_SKIP)
        # Batch 12: 全源失败判定只算既有源; 新接入的化工现货源不得把「老源健康」误判成全源故障
        core = [(n, e) for n, e in fetched.outcomes if n not in CHEM_SRCS]
        core_err = [n for n, e in core if e and e != SRC_SKIP]
        core_ok = [n for n, e in core if not e]
        if not fetched.errors:                        # 全源健康但 0 条: 不是故障, 不得告警 (Batch 9 D-F-01)
            log(f"本轮全部源健康但无可用条目 (跳过 {skipped} 源), 不发告警")
            return 0
        if not core_err:                              # 老源无失败: 只留日志, 不进全源告警状态机
            log(f"本轮无可用条目 (老源无失败, 新源失败 {len(fetched.errors)} 源 / 跳过 {skipped} 源), 不发告警")
            return 0
        # 有老源真失败才进入告警状态机; 若同时有老源「成功但 0 条」, 文案降级为部分失败
        partial = bool(core_ok)                       # 与旧式 len(errors) < len(outcomes) - skipped 在无新源时等价
        if skipped:
            log(f"本轮无可用条目: 失败 {len(fetched.errors)} 源 / 跳过 {skipped} 源: {fetched.errors}")
        else:
            log(f"无可用条目: 失败 {len(fetched.errors)} 源: {fetched.errors}")
        alert_all_failed(seen, fetched.errors, conf, tk, opts.dry, skipped, partial, round_ctx)
        if not opts.dry and not save_json(STATE, prune(seen)):
            err("告警状态落盘失败")
        return 0 if opts.inspect else 1         # 检视模式 rc 恒 0, 不因源失败返 1 (Batch 9 U4-F-02)

    if prune_src_fail(seen):
        if not opts.dry and not save_json(STATE, prune(seen)):
            err("单源失败残留清理落盘失败")
    if source_health(seen, fetched.outcomes, conf, tk, opts.dry, round_ctx):
        if not opts.dry and not save_json(STATE, prune(seen)):
            err("单源告警状态落盘失败 (下轮计数从旧值起算)")
    if note_recovered(seen, conf, tk, opts.dry, round_ctx):
        if not opts.dry and not save_json(STATE, prune(seen)):
            err("恢复状态落盘失败")

    plan = digest_policy(conf, opts, seen, fetched, round_ctx)
    if plan.rc is not None:
        return plan.rc
    return flush_round(conf, opts, seen, plan, tk, pending, round_ctx, send_ctx)


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
    _b9_cmd: list = []
    _b9_run0x = subprocess.run

    class _B9R:
        def __init__(self, out):
            self.stdout = out
            self.returncode = 0                  # R5: http() 对缺 returncode 的桩 fail-closed

    try:
        subprocess.run = lambda cmd, **kw: (_b9_cmd.append(list(cmd)) or _B9R(b"ok\n200"))
        http("https://x/y")
        _b9_plain_gz = "--compressed" in _b9_cmd[-1]
        http("https://x/y", max_bytes=1024)
        _b9_strict_gz = "--compressed" in _b9_cmd[-1]
    finally:
        subprocess.run = _b9_run0x
    check("http 命令行带 --compressed (SMM 强制 gzip, 缺了整源失败)",
          _b9_plain_gz and not _b9_strict_gz, str(_b9_cmd))
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

    # ---- Batch 5 U1: 429 Retry-After 精确退避 (整数秒 / HTTP-date / 夹取 / 回退)
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

    # ---- Batch 5 U1-F-01: 超长纯数字 Retry-After 不得抛异常 (309~4300 位曾 OverflowError)
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

    # ---- Batch 5 U1 轻微: send() 入口必须清零上一轮的 SEND_RETRY_AFTER (变异: 删掉复位即红)
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

    # ---- Batch 5 U2-F-02: parse_mode=HTML 文案里的源名/错误摘要必须转义 (否则 400 降级吞掉摘要)
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

    # ---- Batch 5 U2: 单源连续失败告警 / 抑制 / 恢复 / 全源失败不重复 / 发送失败重试
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
        # U2 main 每轮调用 source_health: 断言迁移到 B9-62 (行为级, E-D-03)
    finally:
        globals()["send"] = _real_send

    # ---- Batch 5 U2-F-01/F-03/F-04 + CTO 裁决 D: 恢复不双发 / 失败重试保留轮数 / 同轮多源合并
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

    # ---- Batch 5 U3: err 文件轮转 (原地 ftruncate, 阈值/保留与 .out 一致)
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
        # U3 main 每轮调用 rotate_stderr: 断言迁移到 B9-63 (行为级, E-D-03)

    # ---- Batch 5 U4: 残缺标签/属性残留清洗 (不碰正文里的 < > 与普通单词)
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
            self.returncode = 0                  # R5: http() 对缺 returncode 的桩 fail-closed

    try:
        subprocess.run = lambda cmd, **kw: (_b6_argv.append(list(cmd)) or _B6R(b"ok\n200"))
        _b6_body = http("https://x/y")
    finally:
        subprocess.run = _b6_realrun
    check("B6 U2 无代理时 curl 命令行与旧版逐元素一致",
          _b6_body == b"ok" and _b6_argv == [["curl", "-q", "-sS", "--noproxy", "*",
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
    _b6_gz: list = []
    try:
        subprocess.run = lambda cmd, **kw: (_b6_gz.append(list(cmd)) or _B6R(b"ok\n200"))
        http("https://x/y", max_bytes=512)
    finally:
        subprocess.run = _b6_realrun
    check("B6 U2 --compressed 只在非 strict 路径出现 (SMM 强制 gzip / PDF 不解压放大)",
          _b6_gz and "--compressed" not in _b6_gz[-1]
          and "--max-filesize" in _b6_gz[-1], str(_b6_gz[-1:]))
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
          round_soft_s() == ROUND_SOFT_S == 120 - _b6_send_worst - 6,
          f"阈值={round_soft_s()} 快照={ROUND_SOFT_S} send_worst={_b6_send_worst}")
    check("B6 U4 SEND_WORST 由 send_plan 推导 (无代理 31 / 三腿 54)",
          send_worst({}) == 31 and _b6_send_worst == 54
          and SEND_WORST >= send_worst({}) and SEND_GATE_S == 114
          and ROUND_HARD_S == 120 and ROUND_MARGIN_S == 6,
          f"SEND_WORST={SEND_WORST} send_worst={_b6_send_worst}")
    check("B6 U4 逐次闸门阈值 = 120 - SEND_WORST - 余量 = 60",
          SEND_GATE_S - SEND_WORST == round_soft_s() == 60
          and ROUND_HARD_S - SEND_WORST - ROUND_MARGIN_S == 60,
          f"{SEND_GATE_S}-{SEND_WORST}={SEND_GATE_S - SEND_WORST}")

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
    # B6 U4 main 预算耗尽分支改用 SRC_SKIP 哨兵: 断言迁移到 B9-64 (行为级, E-D-03)

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

    def _b6_run_main(tmp: Path, clock, srcs, conf_extra=None, state=None, tg_hook=None,
                     args: list[str] | None = None, rotate_hook=None,
                     pending: list[str] | None = None,
                     pending_probe: list[bool] | None = None,
                     queue_raw: str | None = None,
                     pending_raw: str | None = None):
        """在 tempdir 内实跑 main(); 用假 tg 捕获 payload, 返回 rc/载荷/state/队列/CFG.

        tg_hook(text, clock): 可选, 模拟某类发送的真实耗时 (推进假时钟).
        """
        st = tmp / "state"
        st.mkdir(parents=True, exist_ok=True)
        kept = {k: globals()[k] for k in ("STATE", "QUEUE", "PENDING", "LOCK", "CONF",
                                          "LOG", "ERRLOG", "SOURCES", "time", "tg",
                                          "resolve_token", "rotate_log", "rotate_stderr",
                                          "ITEMS_ARCHIVE", "ITEMS_META",
                                          "ROUND_START", "SEND_WORST")}
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
            globals()["rotate_stderr"] = rotate_hook or (lambda: None)
            conf = {"token": "x", "chat_id": 1, "max_age_min": 180,
                    "digest_min": 30, "digest_max_items": 40}
            conf.update(conf_extra or {})
            globals()["CONF"].write_text(json.dumps(conf))
            if pending_raw is not None:
                globals()["PENDING"].write_text(pending_raw)
            elif pending is not None:
                globals()["PENDING"].write_text(
                    json.dumps({"keys": list(pending), "ts": int(clock.time())}))
            if queue_raw is not None:
                globals()["QUEUE"].write_text(queue_raw)
            if state is not None:
                globals()["STATE"].write_text(json.dumps(state))
            old_argv = sys.argv
            try:
                sys.argv = ["monitor.py"] + list(args or [])
                rc = main()
            finally:
                sys.argv = old_argv
            if pending_probe is not None:
                pending_probe.append(globals()["PENDING"].exists())
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
                    conf_extra={"digest_max_items": 1, "proxy": "http://127.0.0.1:8080",
                                "src_alert_rounds": 3},
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
                conf_extra={"digest_max_items": 1, "src_alert_rounds": 3}, state=_pre_a,
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
                Path(_td), _clk, [("S0", _fail70)],
                conf_extra={"proxy": "http://127.0.0.1:8080"}, state={"seedmarker": 1})
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
    # B11: 样本加长到 > SNIPPET_MAX (24000): "沪铜" * (SNIPPET_MAX//2 + 1000) = 26000 字
    _a7_long = pdf_clean("沪铜" * (SNIPPET_MAX // 2 + 1000))
    check("B7-02 pdf_clean 按 SNIPPET_MAX 截断 (B11: 样本加长到 > SNIPPET_MAX)",
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

    # ---- Batch 7 A1: 瞬时失败 -> None (下轮重试); 确定性无文本 -> "" (不再重试)
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

    # ---- Batch 7 A2/A4/A5: curl 退出码、--max-filesize、体积兜底、PDF 路径不请求 gzip
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

    # ---- Batch 8: http() 失败归因 (全离线: 假时钟 + 假 subprocess.run, 不联网)
    # 注: 按评审 F-01/F-02 撤回「按剩余次数切分」; 本块只守 rc 归因与「时序与基线一致」.
    class _B8Clock:
        """确定性时钟: 让 --max-time 真正推进预算, 用于断言调用次数/每次预算/耗时."""

        def __init__(self, t0: float = 1000.0):
            self.mono = float(t0)
            self.t = float(t0)
            self.sleeps: list = []

        def time(self) -> float:
            return self.t

        def monotonic(self) -> float:
            return self.mono

        def sleep(self, s: float) -> None:
            self.sleeps.append(s)
            self.mono += s
            self.t += s

    class _B8R:
        def __init__(self, out: bytes = b"", rc: int = 0):
            self.stdout = out
            self.returncode = rc

    _b8_time0, _b8_run0 = globals()["time"], subprocess.run

    def _b8_http(script, tries, share, cap=8):
        _c = _B8Clock()
        _mts: list = []

        def _run(cmd, **kw):
            _argv = list(cmd)
            _mt = int(_argv[_argv.index("--max-time") + 1])
            _mts.append(_mt)
            _step = script.pop(0) if script else ("rc", 28)
            if _step[0] == "timeout":       # 模拟 curl 到点: 时钟前进满额后返回 rc=28
                _c.mono += _mt
                return _B8R(b"", int(_step[1]))
            if _step[0] == "ok":
                return _B8R(bytes(_step[1]) + b"\n200", 0)
            return _B8R(b"", int(_step[1]))

        globals()["time"] = _c
        subprocess.run = _run
        _body = _err = None
        try:
            _body = http("https://x/feed", tries=tries, cap=cap,
                         deadline=_c.mono + share)
        except RuntimeError as e:
            _err = str(e)
        finally:
            globals()["time"] = _b8_time0
            subprocess.run = _b8_run0
        return _body, _err, _mts, _c

    # 既有重试仍然工作: 第一次快速失败后余额够, 第二次成功 (与基线一致)
    _b8_body, _b8_err, _b8_mts, _b8_c = _b8_http(
        [("rc", 28), ("ok", b"hello")], tries=2, share=7.0)
    check("B8-01 快速失败后余额够: 返回数据且子进程被调用 2 次 (既有重试未被破坏)",
          _b8_body == b"hello" and _b8_err is None and _b8_mts == [7, 6],
          f"body={_b8_body!r} err={_b8_err!r} mts={_b8_mts}")

    # 首轮吃满份额后二次进入: 报真实原因, 不再误标预算耗尽, 也不再带「尝试 N 次」
    _b8_body, _b8_err, _b8_mts, _b8_c = _b8_http(
        [("timeout", 28)], tries=2, share=7.0)
    check("B8-02 首轮吃满 7s 后报 curl 超时 (rc=28, 7.0s), 不含预算耗尽/尝试次数",
          _b8_err is not None and "curl 超时" in _b8_err and "rc=28" in _b8_err
          and "7.0s" in _b8_err and "预算耗尽" not in _b8_err
          and "尝试" not in _b8_err and _b8_mts == [7],
          f"{_b8_err!r} mts={_b8_mts}")

    # 只有一次尝试都没发起才允许报预算耗尽
    _b8_body, _b8_err, _b8_mts, _b8_c = _b8_http([], tries=2, share=1.5)
    check("B8-03 首次 left<2: 报预算耗尽且注明未发起尝试, 零子进程",
          _b8_err is not None and "预算耗尽" in _b8_err
          and "未发起尝试" in _b8_err and not _b8_mts, f"{_b8_err!r} mts={_b8_mts}")

    _, _b8_e35, _, _ = _b8_http([("rc", 35)], tries=1, share=10.0)
    _, _b8_e7, _, _ = _b8_http([("rc", 7)], tries=1, share=10.0)
    _, _b8_e16, _, _ = _b8_http([("rc", 16)], tries=1, share=10.0)
    _, _b8_e63, _, _ = _b8_http([("rc", 63)], tries=1, share=10.0)
    _, _b8_e99, _, _ = _b8_http([("rc", 99)], tries=1, share=10.0)
    check("B8-04 rc 映射: 35/7 连接失败(SSL/DNS) / 16 HTTP/2 / 63 体积上限 / 其它 curl rc=N",
          "连接失败(SSL/DNS)" in (_b8_e35 or "") and "rc=35" in (_b8_e35 or "")
          and "连接失败(SSL/DNS)" in (_b8_e7 or "")
          and "HTTP/2 帧错误" in (_b8_e16 or "")
          and "超过体积上限" in (_b8_e63 or "") and "curl rc=63" in (_b8_e63 or "")
          and "curl rc=99" in (_b8_e99 or "")
          and all("预算耗尽" not in (x or "")
                  for x in (_b8_e35, _b8_e7, _b8_e16, _b8_e63, _b8_e99)),
          f"35={_b8_e35!r} 7={_b8_e7!r} 16={_b8_e16!r} 63={_b8_e63!r} 99={_b8_e99!r}")

    # 撤回切分的核心证据: 7/8/9/10/12s 五种份额下, 调用次数与每次 --max-time 与基线逐项相同.
    # 场景 A: 每次 rc=28 但瞬时返回(不推进时钟) -> 余额允许第二次尝试.
    _b8_a = []
    for _sh in (7.0, 8.0, 9.0, 10.0, 12.0):
        _, _, _mt, _ = _b8_http([("rc", 28), ("rc", 28)], tries=2, share=_sh)
        _b8_a.append((int(_sh), _mt))
    check("B8-05 7/8/9/10/12s 份额(瞬时失败)调用次数与 --max-time 与基线一致",
          _b8_a == [(7, [7, 6]), (8, [8, 7]), (9, [8, 8]),
                    (10, [8, 8]), (12, [8, 8])], f"{_b8_a}")

    # 场景 B: 每次把 --max-time 吃满 (真实超时), 剩余份额决定是否还有第二次.
    _b8_b = []
    for _sh in (7.0, 8.0, 9.0, 10.0, 12.0):
        _, _, _mt, _ = _b8_http([("timeout", 28), ("timeout", 28)],
                                tries=2, share=_sh)
        _b8_b.append((int(_sh), _mt))
    check("B8-06 7/8/9/10/12s 份额(超时吃满)调用次数与 --max-time 与基线一致",
          _b8_b == [(7, [7]), (8, [8]), (9, [8]), (10, [8]), (12, [8, 3])], f"{_b8_b}")

    # 非 deadline 路径 argv 逐元素不变 (--max-time 12)
    _b8_cmd: list = []

    def _b8_run_ok(cmd, **kw):
        _b8_cmd.append(list(cmd))
        return _B8R(b"x\n200", 0)

    globals()["time"], subprocess.run = _B8Clock(), _b8_run_ok
    try:
        _b8_nodl = http("https://x/feed")
    finally:
        globals()["time"], subprocess.run = _b8_time0, _b8_run0
    _b8_expect = ["curl", "-q", "-sS", "--noproxy", "*", "--compressed", "--max-time", "12",
                  "-A", UA, "-w", "\n%{http_code}", "https://x/feed"]
    check("B8-07 无 deadline argv 与基线逐元素一致 (--max-time 12)",
          _b8_nodl == b"x" and _b8_cmd == [_b8_expect],
          f"body={_b8_nodl!r} argv={_b8_cmd[:1]}")

    # 撤回「尝试 N 次」后, 所有失败消息仍带 rc 与耗时; 63 保留既有断言字样
    check("B8-08 失败消息统一带 rc 与耗时且不含切分痕迹 (无尝试次数/无 ATTEMPT_FLOOR)",
          CURL_RC_REASON.get(28) == "curl 超时"
          and "ATTEMPT_FLOOR" not in globals()
          and all(("rc=" in (_x or "") and ".0s" in (_x or "") and "尝试" not in (_x or ""))
                  for _x in (_b8_e35, _b8_e7, _b8_e16, _b8_e63, _b8_e99)),
          f"{_b8_e35!r} {_b8_e63!r}")

    # ---- Batch 7 A3/A5: PDF 子预算含解析时间; pdftotext 限页 + RLIMIT_AS
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
    _b9_rl: list = []
    _b9_res0 = globals().get("resource")

    class _B9Res:
        RLIMIT_AS = -1

        @staticmethod
        def setrlimit(res, lim):
            _b9_rl.append((res, lim))

    try:
        globals()["resource"] = _B9Res
        _pdf_child_limits()
    finally:
        globals()["resource"] = _b9_res0
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
          and _b9_rl == [(_B9Res.RLIMIT_AS, (PDF_RLIMIT_AS, PDF_RLIMIT_AS))],
          f"dl={_a7_pkw.get('deadline')} argv={_a7_pargv}")

    # ---- Batch 7 B1: 未来节流时间戳 -> 按未运行处理 (下一轮必须真跑并改写)
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

    # ---- Batch 7 B2①: 第二轮 30 分钟节流生效 (main 级)
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

    # ---- Batch 7 B2②: 摘要未取到的研报当轮不入队, 下一研报轮补齐 (main 级)
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

    # ---- Batch 7 B2③: 研报条目走 research_reason, 非研报 filter_reason (main 级)
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

    # ---- Batch 7 B2④: 主流程用 it["key"] (稳定研报键), 不用 dedupe_key(text) (main 级)
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

    # ---- Batch 7 B3: 中金所日/周报是 CSV, 绝不能进 pdf_text; 月报才解析
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

    # ---- Batch 7 C1: 协议白名单 (渲染 + 入队)
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

    # ---- Batch 7 C2: 研报标题/元信息/摘要的 HTML 实体化
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

    # ---- Batch 7 C3: 超长 URL/元信息 -> 单条 u16 <= MSG_MAX (可正常拆批)
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

    # ---- Batch 7 C 轻微: queue_upsert 字段语义 + 非字符串 url 容错
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

    # ==== Batch 9 U1: 发送路径 (B9-01..13) ====
    # B9-01/02/03: 主聚合循环逐条闸门 (RA-F-01)
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)
        _b9_items = [{"src": "S0", "ts": int(_clk.time()) - 60,
                      "text": f"沪铜{i}号" + "🚨" * 690} for i in range(10)]

        def _b9_long(deadline=None):
            return [dict(it) for it in _b9_items]

        def _b9_hook(text, clk):
            if "沪铜" in text:                # 聚合消息: 第一条花 62s, 之后闸门必红
                clk.mono += 62.0
                clk.t += 62.0

        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk, [("S0", _b9_long)],
            conf_extra={"digest_max_items": 10, "proxy": "http://127.0.0.1:8080"},
            state={"seedmarker": 1}, tg_hook=_b9_hook)
        _b9_seen = {k for k in _stt if not k.startswith("_") and k != "seedmarker"}
        _b9_qkeys = {it["key"] for it in _q}
        _b9_first_keys = {dedupe_key(it["text"]) for it in _b9_items[:2]}
        check("B9-01 聚合循环逐条闸门: 第 2 批起被推迟 (只发 1 条)",
              _rc == 0 and len(_pay) == 1, f"rc={_rc} sends={len(_pay)}")
        check("B9-02 闸门推迟: 剩余条目留队且未标 seen (按 key 身份, U1-F-04)",
              len(_q) == 8 and _b9_seen == _b9_first_keys
              and not (_b9_qkeys & _b9_seen),
              f"queue={len(_q)} seen={sorted(_b9_seen)} first={sorted(_b9_first_keys)}")
        check("B9-03 闸门推迟不算发送失败 (fails 不增 / next_try 无)",
              not (_stt.get("_digest") or {}).get("fails")
              and "next_try" not in (_stt.get("_digest") or {}),
              str(_stt.get("_digest")))

    # B9-04: SEND_WORST 按 conf 重算 (A-F-02)
    _b9_sw: list[int] = []

    def _b9_sw_hook(text, clk):
        _b9_sw.append(globals()["SEND_WORST"])

    _b9_proxy0 = PROXY
    globals()["PROXY"] = ""                   # 排掉宿主环境代理, 让 31/54 两档可判定
    try:
        for _b9_proxy, _b9_want in (("", 31), ("http://127.0.0.1:8080", 54)):
            with tempfile.TemporaryDirectory() as _td:
                _clk = _B6Clock(1793800000)

                def _b9_one(deadline=None):
                    return [{"src": "S0", "ts": int(_clk.time()) - 60,
                             "text": "沪铜库存变化1吨环比1%"}]

                _b6_run_main(Path(_td), _clk, [("S0", _b9_one)],
                             conf_extra={"digest_max_items": 1, "proxy": _b9_proxy},
                             state={"seedmarker": 1}, args=["--flush-now"],
                             tg_hook=_b9_sw_hook)
    finally:
        globals()["PROXY"] = _b9_proxy0
    check("B9-04 SEND_WORST 按实际腿数重算 (无代理 31 / 三腿 54)",
          _b9_sw == [31, 54], str(_b9_sw))

    # B9-05: 阈值单点派生 (A-D-03)
    check("B9-05 阈值单点派生: SEND_GATE_S=114 且 round_soft_s()=硬顶-SEND_WORST",
          SEND_GATE_S == ROUND_HARD_S - ROUND_MARGIN_S == 114
          and round_soft_s() == SEND_GATE_S - SEND_WORST,
          f"gate={SEND_GATE_S} soft={round_soft_s()} worst={SEND_WORST}")

    # B9-06: if not dry 死条件已移除 (A-F-03, AST 结构断言而非源码字面量)
    # R2: main 的 dry 局部名收敛为 opts.dry, 断言只重定向 token 名 (Name -> Attribute), 语义不变.
    _b9_main_ast = next(n for n in ast.parse(Path(__file__).read_text()).body
                        if isinstance(n, ast.FunctionDef) and n.name == "main")
    _b9_dead = [n for n in ast.walk(_b9_main_ast)
                if isinstance(n, ast.If) and isinstance(n.test, ast.UnaryOp)
                and isinstance(n.test.op, ast.Not)
                and isinstance(n.test.operand, ast.Attribute)
                and n.test.operand.attr == "dry"]
    check("B9-06 main 的 if not dry 只剩 pending 合并 1 处 (尾部死条件已删, AST)",
          len(_b9_dead) == 1, f"count={len(_b9_dead)}")

    # B9-07/08: SEND_ERR 复位 (A-F-04 / R-E-F-02); U1-F-01: 两条断言须能各自隔离一处复位
    _b9_tg0 = globals()["tg"]
    _b9_entry_err: list[str] = []
    try:
        def _b9_tg7(method, payload, tk, proxy, timeout=25):
            _b9_entry_err.append(SEND_ERR)      # 入口复位之后、真正发请求之前捕获
            return {}

        globals()["tg"] = _b9_tg7
        globals()["SEND_ERR"] = "stale-entry"
        _b9_ok7 = send("probe", {"chat_id": 1}, "tk")
        _b9_err7 = SEND_ERR
        _b9_calls8: list[int] = []

        def _b9_tg8(method, payload, tk, proxy, timeout=25):
            _b9_calls8.append(1)
            if len(_b9_calls8) == 1:
                globals()["SEND_ERR"] = "inside-error"   # 首次腿内写失败原因, 再抛 400 parse
                raise urllib.error.HTTPError(
                    "https://api.telegram.org", 400, "Bad Request",
                    {}, io.BytesIO(b'{"ok":false,"description":"can\'t parse entities"}'))
            return {}

        globals()["tg"] = _b9_tg8
        globals()["SEND_ERR"] = "stale-fallback"
        _b9_ok8 = send("probe", {"chat_id": 1}, "tk")
        _b9_err8 = SEND_ERR
    finally:
        globals()["tg"] = _b9_tg0
        globals()["SEND_ERR"] = ""
    check("B9-07 send() 入口清零 SEND_ERR (tg 入口捕获为空, U1-F-01)",
          _b9_ok7 and _b9_err7 == "" and _b9_entry_err == [""],
          f"{_b9_err7!r} entry={_b9_entry_err!r}")
    check("B9-08 纯文本降级成功也清零 SEND_ERR (腿内错误被清, U1-F-01)",
          _b9_ok8 and len(_b9_calls8) == 2 and _b9_err8 == "", repr(_b9_err8))

    # B9-09: errno 常量白名单 + IncompleteRead (R-E-F-03 / E-D-01)
    check("B9-09 is_transient 用 errno 常量 (跨平台) 且含 IncompleteRead",
          is_transient(urllib.error.URLError(OSError(errno.ENETDOWN, "down")))
          and is_transient(urllib.error.URLError(OSError(errno.EADDRNOTAVAIL, "na")))
          and is_transient(IncompleteRead(b"x"))
          and not is_transient(urllib.error.HTTPError("u", 400, "Bad", None, None)),
          f"{errno.ENETDOWN}/{errno.EADDRNOTAVAIL}")

    # B9-10: is_http_url 大小写 (R-E-F-05)
    check("B9-10 is_http_url scheme 大小写不敏感 (RFC 3986)",
          is_http_url("HTTPS://X/") and is_http_url("Http://x") and not is_http_url("ftp://x"),
          f"{is_http_url('HTTPS://X/')} {is_http_url('Http://x')}")

    # B9-11/12/13: 研报行转义后 u16 (R-E-F-01)
    _b9_dt = datetime.now(TZ_CN)
    _b9_title = research_item("东财研报", "B9T", '"' * 700, "https://x/1.pdf",
                              1000, "机构 · 2026-11-06", "摘要")
    _b9_line_t = research_line(_b9_title, _b9_dt)
    _b9_url = "https://x/?" + "&" * 989
    _b9_item_u = research_item("东财研报", "B9U", "沪铜周报", _b9_url,
                               1000, "机构 · 2026-11-06", "摘要")
    _b9_line_u = research_line(_b9_item_u, _b9_dt)
    _b9_mm0 = RESEARCH_META_MAX
    try:
        globals()["RESEARCH_META_MAX"] = MSG_MAX
        _b9_item_m = research_item("东财研报", "B9M", "沪铜周报", "https://x/3.pdf",
                                   1000, "机" * 4000, "摘要")
        _b9_line_m = research_line(_b9_item_m, _b9_dt)
    finally:
        globals()["RESEARCH_META_MAX"] = _b9_mm0
    check("B9-11 研报标题按转义后 u16 截断 (700 引号不再 4279)",
          u16(_b9_line_t) <= MSG_MAX, f"u16={u16(_b9_line_t)}")
    # U1-F-03: 699 引号「摘要」会先被 SNIPPET_MAX+20=260 字符截断, 旧码也只有 1641 u16, 红不了;
    # 真正的旧码红点是标题/URL, 故 red 用例改用 699 引号标题, 摘要仅保留为边界绿例.
    _b9_t699 = research_item("东财研报", "B9T699", '"' * 699, "https://x/699.pdf",
                             1000, "机构 · 2026-11-06", "摘要")
    _b9_s699 = research_item("东财研报", "B9S699", "沪铜周报", "https://x/699s.pdf",
                             1000, "机构 · 2026-11-06", '"' * 699)
    _b9_line_t699 = research_line(_b9_t699, _b9_dt)
    _b9_line_s699 = research_line(_b9_s699, _b9_dt)
    check("B9-11B 699 引号标题/URL 按转义后 u16 截断 (旧码 4273, U1-F-03)",
          u16(_b9_line_t699) <= MSG_MAX and u16(_b9_line_s699) <= MSG_MAX
          and "<a href" in _b9_line_t699,
          f"title={u16(_b9_line_t699)} snip={u16(_b9_line_s699)}")
    check("B9-12 & 密集 URL 按转义后 u16 降级为「链接过长」",
          u16(_b9_line_u) <= MSG_MAX and "<a href" not in _b9_line_u
          and "链接过长" in _b9_line_u, f"u16={u16(_b9_line_u)}")
    check("B9-13 研报行仍超 MSG_MAX 时整行降级纯文本",
          u16(_b9_line_m) <= MSG_MAX and "<a href" not in _b9_line_m,
          f"u16={u16(_b9_line_m)}")

    # ==== Batch 9 U2: 抓取层 (B9-20..27) ====
    _b9_http0 = globals()["http"]
    _b9_ok0 = globals()["http_ok"]
    _b9_run0 = subprocess.run

    class _B9R:
        def __init__(self, out, rc=0):
            self.stdout = out
            self.returncode = rc

    # B9-20: 5 个快讯源空列表哨兵 (RB-F-01)
    _b9_empty = [
        (src_cls, {"data": {"roll_data": []}}, "财联社"),
        (src_em, {"data": {"fastNewsList": []}}, "东财"),
        (src_ths, {"data": {"list": []}}, "同花顺"),
        (src_sina, {"result": {"data": {"feed": {"list": []}}}}, "新浪"),
        (src_wscn_commodity, {"data": {"items": []}}, "见闻商品"),
    ]
    _b9_hits: list[str] = []
    try:
        for _fn, _payload, _name in _b9_empty:
            globals()["http"] = (lambda *a, _p=_payload, **k: json.dumps(_p).encode())
            try:
                _fn(None)
                _b9_hits.append("no-raise")
            except RuntimeError as e:
                _b9_hits.append(_name if _name in str(e) else f"wrong:{e}")
    finally:
        globals()["http"] = _b9_http0
    check("B9-20 5 个中文快讯源空列表抛错 (不静默记成功)",
          _b9_hits == ["财联社", "东财", "同花顺", "新浪", "见闻商品"], str(_b9_hits))

    # B9-21: 研报源空列表豁免 (反向守住: 不得误加哨兵)
    _b9_em_empty = _b9_cfa_empty = _b9_cffex_empty = None
    try:
        globals()["http"] = lambda *a, **k: b'{"data": []}'
        _b9_em_empty = src_em_report(None)
        globals()["http"] = lambda *a, **k: b'{"data": {"dataList": []}}'
        _b9_cfa_empty = src_cfa(None)
        globals()["http_ok"] = lambda *a, **k: False
        _b9_cffex_empty = src_cffex(None)
    finally:
        globals()["http"] = _b9_http0
        globals()["http_ok"] = _b9_ok0
    check("B9-21 研报源空列表豁免 (不得误加哨兵)",
          _b9_em_empty == [] and _b9_cfa_empty == [] and _b9_cffex_empty == [],
          f"{_b9_em_empty} {_b9_cfa_empty} {_b9_cffex_empty}")

    # B9-22: strict 体积超限原因不被吞 (RB-F-02)
    _b9_over = None
    try:
        subprocess.run = lambda cmd, **kw: _B9R(b"x" * 2048 + b"\n200")
        try:
            http("https://x/a.pdf", tries=1, max_bytes=1024,
                 deadline=time.monotonic() + 10)
        except RuntimeError as e:
            _b9_over = str(e)
    finally:
        subprocess.run = _b9_run0
    check("B9-22 strict 体积超限原因透传 (保留 body 超上限 字样)",
          _b9_over is not None and "body 超上限 2048>1024" in _b9_over
          and "http failed" in _b9_over, repr(_b9_over))

    # B9-23: 末次尝试后不再 sleep (RB-F-03)
    _b9_sleeps: list[float] = []
    _b9_sleep0 = time.sleep
    try:
        time.sleep = lambda s: _b9_sleeps.append(s)
        subprocess.run = lambda cmd, **kw: _B9R(b"x\n500")
        try:
            http("https://x/y", tries=3)
        except RuntimeError:
            pass
    finally:
        time.sleep = _b9_sleep0
        subprocess.run = _b9_run0
    check("B9-23 末次尝试后不再 sleep (3 次尝试只剩 2 次间隔)",
          _b9_sleeps == [1.0, 2.0], str(_b9_sleeps))

    # B9-24: curl argv 带 -q (B-D-01, http + http_ok 两处)
    _b9_qargv: list = []
    try:
        subprocess.run = lambda cmd, **kw: (_b9_qargv.append(list(cmd)) or _B9R(b"x\n200"))
        http("https://x/y")
        http_ok("https://x/y")
    finally:
        subprocess.run = _b9_run0
    check("B9-24 curl argv 首位 -q (http 与 http_ok 都隔离 ~/.curlrc)",
          len(_b9_qargv) == 2 and _b9_qargv[0][:2] == ["curl", "-q"]
          and _b9_qargv[1][:2] == ["curl", "-q"], str([a[:3] for a in _b9_qargv]))

    # B9-25: deadline=0.0 视为预算已耗尽 (B-D-03)
    _b9_calls25: list[int] = []
    _b9_e25 = ""
    try:
        subprocess.run = lambda cmd, **kw: (_b9_calls25.append(1) or _B9R(b"x\n200"))
        try:
            http("https://x/y", deadline=0.0)
        except RuntimeError as e:
            _b9_e25 = str(e)
        _b9_ok25 = http_ok("https://x/y", deadline=0.0)
    finally:
        subprocess.run = _b9_run0
    check("B9-25 deadline=0.0 视为耗尽 (不发起子进程, http_ok 直接 False)",
          not _b9_calls25 and "预算耗尽" in _b9_e25 and _b9_ok25 is False,
          f"calls={len(_b9_calls25)} err={_b9_e25!r} ok={_b9_ok25}")

    # B9-26: 小写 https_proxy 生效 (B-D-05)
    _b9_pfe = globals().get("proxy_from_env")
    _b9_env_val = "missing"
    _b9_up0 = os.environ.pop("HTTPS_PROXY", None)
    _b9_lo0 = os.environ.get("https_proxy")
    try:
        os.environ["https_proxy"] = "http://127.0.0.1:9"
        _b9_env_val = _b9_pfe() if callable(_b9_pfe) else "missing"
    finally:
        if _b9_up0 is None:
            os.environ.pop("HTTPS_PROXY", None)
        else:
            os.environ["HTTPS_PROXY"] = _b9_up0
        if _b9_lo0 is None:
            os.environ.pop("https_proxy", None)
        else:
            os.environ["https_proxy"] = _b9_lo0
    check("B9-26 小写 https_proxy 被识别 (与大写同口径)",
          _b9_env_val == "http://127.0.0.1:9", repr(_b9_env_val))

    # B9-27: 非空列表仍正常返回 (哨兵不误伤; 旧码也绿的不变式)
    _b9_rows = {"data": {"roll_data": [
        {"title": "沪铜夜盘上涨1.2%", "ctime": int(time.time()) - 60}]}}
    try:
        globals()["http"] = lambda *a, **k: json.dumps(_b9_rows).encode()
        _b9_got_cls = src_cls(None)
    finally:
        globals()["http"] = _b9_http0
    check("B9-27 非空列表仍正常解析 (哨兵不误伤)",
          len(_b9_got_cls) == 1 and _b9_got_cls[0]["src"] == "财联社",
          str(_b9_got_cls))

    # ==== Batch 9 U3: 判定与合并 (B9-30..39) ====
    def _b9_merge(a, b):
        return merge_near([{"src": "x", "ts": 1000, "text": a, "key": a},
                           {"src": "y", "ts": 1300, "text": b, "key": b}])[0]

    # B9-30/31: 方向否决 (RC-F-01)
    _b9_dir_tokens = ("涨", "跌", "增", "减", "升", "降", "上调", "下调",
                      "扩张", "收缩", "累库", "去库")
    check("B9-30 方向词否决: 增产50万桶 vs 减产50万桶 不再合并",
          len(_b9_merge("沙特增产50万桶/日以平衡市场",
                        "沙特减产50万桶/日以平衡市场")) == 2
          and all(tok in DIR_UP or tok in DIR_DOWN for tok in _b9_dir_tokens),
          f"{len(_b9_merge('沙特增产50万桶/日以平衡市场', '沙特减产50万桶/日以平衡市场'))}")
    check("B9-31 方向词否决: 大涨5% vs 大跌5% 不再合并",
          len(_b9_merge("沪铜主力合约日内大涨5%现报73000元",
                        "沪铜主力合约日内大跌5%现报73000元")) == 2,
          str(len(_b9_merge("沪铜主力合约日内大涨5%现报73000元",
                            "沪铜主力合约日内大跌5%现报73000元"))))

    # B9-32: 「涨跌互现」同句不误伤 (不变式)
    check("B9-32 涨跌互现同句不参与否决 (仍可近似合并)",
          len(_b9_merge("沪铜涨跌互现市场情绪谨慎",
                        "沪铜涨跌互现市场情绪谨慎观望")) == 1,
          str(len(_b9_merge("沪铜涨跌互现市场情绪谨慎",
                            "沪铜涨跌互现市场情绪谨慎观望"))))

    # B9-33/34: 中文数字不设防 -> 设防 (RC-F-02)
    check("B9-33 中文数字量词不同不合并 (一万二千吨 vs 一万吨)",
          len(_b9_merge("上期所铜库存减少一万二千吨",
                        "上期所铜库存减少一万吨")) == 2,
          str(len(_b9_merge("上期所铜库存减少一万二千吨",
                            "上期所铜库存减少一万吨"))))
    check("B9-34 阿拉伯数字护栏不变 (12000 vs 10000 仍不合并)",
          len(_b9_merge("上期所铜库存减少12000吨",
                        "上期所铜库存减少10000吨")) == 2,
          str(len(_b9_merge("上期所铜库存减少12000吨",
                            "上期所铜库存减少10000吨"))))

    # B9-35/36/37: clean_text (RC-F-03/04/06)
    check("B9-35 clean_text 保留比较符夹住的正文 (升贴水/涨跌幅)",
          clean_text("沪铜升水<150元/吨，贴水>50元/吨") == "沪铜升水<150元/吨，贴水>50元/吨"
          and clean_text("涨幅<5%且库存>30万吨") == "涨幅<5%且库存>30万吨"
          and clean_text("<b>沪铜夜盘上涨1.2%</b>") == "沪铜夜盘上涨1.2%",
          clean_text("沪铜升水<150元/吨，贴水>50元/吨"))
    _b9_t0 = time.perf_counter()
    _b9_lt = clean_text("<" * 100000)
    _b9_dt_cost = time.perf_counter() - _b9_t0
    check("B9-36 clean_text '<' 密集输入线性 (100k < 1.5s, 旧码 ~5.9s)",
          _b9_dt_cost < 1.5 and len(_b9_lt) == 100000, f"{_b9_dt_cost:.3f}s")
    check("B9-37 clean_text 双重编码归一且幂等 (RC-F-06)",
          clean_text("&amp;lt;span&amp;gt;双重编码") == "双重编码"
          and clean_text(clean_text("&amp;lt;span&amp;gt;双重编码")) == "双重编码",
          clean_text("&amp;lt;span&amp;gt;双重编码"))

    # B9-38: docstring 同步 (RC-F-05)
    check("B9-38 research_reason docstring 已同步为「顺序不同/锚点前置」",
          "顺序与 filter_reason **不同**" in (research_reason.__doc__ or "")
          and "顺序与 filter_reason 一致" not in (research_reason.__doc__ or ""),
          (research_reason.__doc__ or "")[:60])

    # B9-39: NOISE_HARD 无重复 (C-D-03)
    check("B9-39 NOISE_HARD 无重复词条 (150 -> 145)",
          len(NOISE_HARD) == len(set(NOISE_HARD)) and len(NOISE_HARD) == 145,
          f"len={len(NOISE_HARD)}")

    # ==== batch9-fix U3: 方向否决补强 (U3-F-01..F-04) ====
    # B9-70: 评审 8 组最小反例逐组 kept=2 (U3-F-01)
    _b9_fx_gap = [
        ("美联储 降息/加息", "美联储宣布降息25个基点", "美联储宣布加息25个基点"),
        ("央行 降息/加息", "央行宣布降息25个基点", "央行宣布加息25个基点"),
        ("沪铜 上涨/下滑", "沪铜主力合约日内上涨5%现报73000元/吨",
         "沪铜主力合约日内下滑5%现报73000元/吨"),
        ("沪铜 上涨/走弱", "沪铜主力合约日内上涨5%现报73000元/吨",
         "沪铜主力合约日内走弱5%现报73000元/吨"),
        ("扩产/减产", "国内铜冶炼厂扩产10万吨项目正式投产运行",
         "国内铜冶炼厂减产10万吨项目正式投产运行"),
        ("复产/停产", "吕梁地区煤矿复产10万吨产能恢复运行",
         "吕梁地区煤矿停产10万吨产能恢复运行"),
        ("新增产能 扩张/收缩", "国内铜冶炼新增产能扩张10万吨项目正式投产运行",
         "国内铜冶炼新增产能收缩10万吨项目正式投产运行"),
        ("新增产量 增长/下降", "国内铜冶炼新增产量增长5%至10万吨创年内新高",
         "国内铜冶炼新增产量下降5%至10万吨创年内新高"),
    ]
    _b9_fx_gap_kept = [(name, len(_b9_merge(a, b))) for name, a, b in _b9_fx_gap]
    check("B9-70 方向否决: 评审 8 组最小反例全部 kept=2 (U3-F-01)",
          len(_b9_fx_gap) == 8 and all(k == 2 for _n, k in _b9_fx_gap_kept),
          str(_b9_fx_gap_kept))

    # B9-71: 「新增」等中性复合词不得伪造方向 (U3-F-01)
    check("B9-71 中性复合词不产生伪方向: 新增/增速/增量 为空集, 新增产量+增长/下降 仍否决 (U3-F-01)",
          dir_axes("新增") == frozenset() and dir_axes("增速") == frozenset()
          and dir_axes("增量") == frozenset()
          and dir_sign("国内铜冶炼新增产量增长5%至10万吨创年内新高") == 1
          and dir_sign("国内铜冶炼新增产量下降5%至10万吨创年内新高") == -1
          and len(_b9_merge("国内铜冶炼新增产量增长5%至10万吨创年内新高",
                            "国内铜冶炼新增产量下降5%至10万吨创年内新高")) == 2,
          f"新增={dir_axes('新增')} 增速={dir_axes('增速')}")

    # B9-72: 涨跌互现同句双向 + 普通条目 4/4 不误伤 (U3-F-01)
    _b9_fx_keep = [
        ("涨跌互现", "沪铜涨跌互现市场情绪谨慎", "沪铜涨跌互现市场情绪谨慎观望", 1),
        ("同向 上涨/上涨", "沪铜主力合约日内上涨1.2%现报73000元",
         "沪铜主力合约日内上涨1.2%报73000元", 1),
        ("单向词 vs 无词", "铜价上涨带动市场情绪回暖", "铜价带动市场情绪回暖", 1),
        ("无方向词近邻", "国内现货铜报价小幅波动", "国内现货铜报价小幅波动中", 1),
        ("增减持同现", "产业资本增减持公告集中披露", "产业资本增减持公告集中披露后", 1),
    ]
    _b9_fx_keep_kept = [(n, len(_b9_merge(a, b))) for n, a, b, _w in _b9_fx_keep]
    check("B9-72 涨跌互现同句双向不误伤 + 普通条目 4/4 仍合并 (U3-F-01)",
          dir_sign("沪铜涨跌互现市场情绪谨慎") == 0
          and all(k == 1 for _n, k in _b9_fx_keep_kept),
          str(_b9_fx_keep_kept))

    # B9-73: 同花顺/新浪源级清洗保留比较符 (U3-F-02)
    _b9_fx_http0 = globals()["http"]
    _b9_fx_ts0 = globals()["parse_ts"]
    _b9_fx_cmp = "升水<150元/吨，贴水>50元"
    try:
        globals()["parse_ts"] = lambda _v: 1793800000
        globals()["http"] = lambda *a, **k: json.dumps(
            {"data": {"list": [{"title": _b9_fx_cmp, "digest": "", "ctime": "x"}]}},
            ensure_ascii=False).encode()
        _b9_fx_ths = src_ths()
        globals()["http"] = lambda *a, **k: json.dumps(
            {"result": {"data": {"feed": {"list": [
                {"rich_text": _b9_fx_cmp, "create_time": "x"}]}}}},
            ensure_ascii=False).encode()
        _b9_fx_sina = src_sina()
    finally:
        globals()["http"] = _b9_fx_http0
        globals()["parse_ts"] = _b9_fx_ts0
    check("B9-73 同花顺/新浪源级清洗保留比较符 (升水<150…贴水>50, U3-F-02)",
          [it["text"] for it in _b9_fx_ths] == [_b9_fx_cmp]
          and [it["text"] for it in _b9_fx_sina] == [_b9_fx_cmp],
          f"ths={_b9_fx_ths} sina={_b9_fx_sina}")

    # B9-74: clean_text 剥除注释/处理指令, 且不破坏比较符 (U3-F-03)
    check("B9-74 clean_text 剥除 <!-- --> 与 <?xml ?> 且保留比较符 (U3-F-03)",
          clean_text("<!-- <b>x</b> -->text") == "text"
          and clean_text('<?xml version="1.0"?>text') == "text"
          and clean_text("升水<150元/吨，贴水>50元") == "升水<150元/吨，贴水>50元",
          repr(clean_text("<!-- <b>x</b> -->text")))

    # B9-75: 13 对逐对否决 (每对 1 例). 准确口径: 11 条中文轴 + 2 对扩展
    # (累增/回落、升水/贴水); 补 8 对反义缺口后 DIR_AXES 共覆盖 21 对.
    _b9_fx_pairs13 = [
        ("涨/跌", "沪铜主力合约日内大涨5%现报73000元",
         "沪铜主力合约日内大跌5%现报73000元"),
        ("增/减", "上海期货交易所指定交割仓库铜库存增加12万吨",
         "上海期货交易所指定交割仓库铜库存减少12万吨"),
        ("升/降", "上海有色网现货铜价格今日上升2%报73000元",
         "上海有色网现货铜价格今日下降2%报73000元"),
        ("上调/下调", "高盛上调铜目标价至9000美元", "高盛下调铜目标价至9000美元"),
        ("扩张/收缩", "国内铜冶炼产能扩张10万吨项目正式投产运行",
         "国内铜冶炼产能收缩10万吨项目正式投产运行"),
        ("累库/去库", "上期所铜库存累库2万吨", "上期所铜库存去库2万吨"),
        ("增产/减产", "OPEC+增产50万桶/日", "OPEC+减产50万桶/日"),
        ("增加/减少", "上海期货交易所铜库存增加1万吨至10万吨",
         "上海期货交易所铜库存减少1万吨至10万吨"),
        ("上涨/下跌", "沪铜主力合约日内上涨1.2%现报73000元/吨",
         "沪铜主力合约日内下跌1.2%现报73000元/吨"),
        ("上升/下降", "中钢协钢材库存上升3%当前处于近四年以来高位",
         "中钢协钢材库存下降3%当前处于近四年以来高位"),
        ("走高/走低", "螺纹钢期货主力合约走高2%现报3500元/吨",
         "螺纹钢期货主力合约走低2%现报3500元/吨"),
        ("累增/回落", "上期所铜库存累增2万吨至10万吨创近期新高",
         "上期所铜库存回落2万吨至10万吨创近期新高"),
        ("升水/贴水", "沪铜现货升水150元/吨", "沪铜现货贴水150元/吨"),
    ]
    _b9_fx_pairs_kept = [(name, len(_b9_merge(a, b))) for name, a, b in _b9_fx_pairs13]
    check("B9-75 方向否决: 13 对逐对断言 (11 轴 + 2 扩展, 每对 1 例, U3-F-04)",
          len(_b9_fx_pairs13) == 13 and all(k == 2 for _n, k in _b9_fx_pairs_kept),
          str([n for n, k in _b9_fx_pairs_kept if k != 2]))

    # ==== Batch 9 U4: 状态机与落盘 (B9-40..53) ====
    # B9-40: 健康空轮 (D-F-01)
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)
        _b9_none = lambda deadline=None: []
        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk, [("S0", _b9_none), ("S1", _b9_none)],
            state={"seedmarker": 1})
    check("B9-40 全源健康但 0 条: rc=0 / 零发送 / 不置 _alert (D-F-01)",
          _rc == 0 and not _pay and "_alert" not in _stt,
          f"rc={_rc} sends={_pay} state={_stt}")

    # B9-41: 部分失败 + 健康 0 条文案 (D-F-01)
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_boom(deadline=None):
            raise RuntimeError("boom")

        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk, [("S0", _b9_boom), ("S1", _b9_none)],
            state={"seedmarker": 1})
    _b9_txt = _pay[0] if _pay else ""
    check("B9-41 部分失败+健康0条: 文案降级为「部分失败」而非「全部」",
          _rc == 1 and len(_pay) == 1 and "部分失败且无可用条目" in _b9_txt
          and "全部抓取失败" not in _b9_txt, f"rc={_rc} txt={_b9_txt!r}")

    # B9-42/43: next_try 护栏 (D-F-02)
    def _b9_run_nt(next_try):
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            return _b6_run_main(
                Path(_td), _clk, [("S0", _one)],
                conf_extra={"digest_max_items": 1},
                state={"seedmarker": 1,
                       "_digest": {"last_flush": 1793800000 - 60, "fails": 1,
                                   "next_try": next_try}})

    _rc, _pay, _stt, _q, _cfg = _b9_run_nt(1793800000 + 7200)
    check("B9-42 next_try 超出 RA_MAX+60 的未来值: 按无退避, 本轮即恢复发送 (D-F-02)",
          _rc == 0 and len(_pay) == 1
          and not (_stt.get("_digest") or {}).get("next_try"),
          f"rc={_rc} sends={_pay} meta={_stt.get('_digest')}")
    _rc, _pay, _stt, _q, _cfg = _b9_run_nt(1793800000 + 600)
    check("B9-43 合法退避 (600s <= RA_MAX) 不被护栏重置 (不变式)",
          _rc == 0 and not _pay and len(_q) == 1,
          f"rc={_rc} sends={_pay} queue={len(_q)}")

    # B9-44: 恢复通知失败保留 _alert (D-F-03)
    _b9_clock = _B6Clock(1793800000)
    _b9_time0 = globals()["time"]
    _b9_send_r = globals()["send"]
    _b9_rs_r = ROUND_START
    _b9_sent_r: list[int] = []
    try:
        globals()["time"] = _b9_clock
        globals()["ROUND_START"] = _b9_clock.mono
        globals()["send"] = lambda text, conf, tk, depth=0: False
        _b9_st_r = {"_alert": {"failing": True, "since": 1793790000, "rounds": 3,
                               "last_sent": 1793790000, "alerts": 1}}
        _b9_ret1 = note_recovered(_b9_st_r, {"chat_id": 1}, "tk", False)
        _b9_al1 = dict(_b9_st_r.get("_alert") or {})
        globals()["send"] = lambda text, conf, tk, depth=0: (_b9_sent_r.append(1) or True)
        _b9_ret2 = note_recovered(_b9_st_r, {"chat_id": 1}, "tk", False)
        _b9_al2 = dict(_b9_st_r.get("_alert") or {})
    finally:
        globals()["time"] = _b9_time0
        globals()["send"] = _b9_send_r
        globals()["ROUND_START"] = _b9_rs_r
    check("B9-44 恢复通知发送失败: 保留 failing/rounds, 下轮重试成功才复位 (D-F-03)",
          _b9_ret1 is False and _b9_al1.get("failing") is True
          and _b9_al1.get("rounds") == 3
          and _b9_ret2 is True and _b9_al2.get("failing") is False,
          f"{_b9_ret1} {_b9_al1} {_b9_ret2} {_b9_al2}")

    # B9-45: --seed 落盘失败保留 pending (D-F-04)
    _b9_pend_probe: list[bool] = []
    _b9_save0 = globals()["save_json"]
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_seed_src(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        def _b9_save_fail(path, data, tries=3):
            if path == globals()["STATE"]:
                return False
            return _b9_save0(path, data, tries)

        try:
            globals()["save_json"] = _b9_save_fail
            _b6_run_main(Path(_td), _clk, [("S0", _b9_seed_src)],
                         state={}, args=["--seed"], pending=["k-old"],
                         pending_probe=_b9_pend_probe)
        finally:
            globals()["save_json"] = _b9_save0
    check("B9-45 --seed 落盘失败: pending 文件保留 (D-F-04)",
          _b9_pend_probe == [True], str(_b9_pend_probe))

    # B9-46: rc=5 前补 stale 记账 (D-F-05)
    _b9_stale_calls: list[int] = []
    _b9_rs0 = globals()["record_stale"]
    _b9_ps0 = globals()["persist_state"]
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_one46(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        try:
            globals()["record_stale"] = lambda *a, **k: _b9_stale_calls.append(1)
            globals()["persist_state"] = lambda seen: False
            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _b9_one46)],
                conf_extra={"digest_max_items": 1}, state={"seedmarker": 1},
                args=["--flush-now"])
        finally:
            globals()["record_stale"] = _b9_rs0
            globals()["persist_state"] = _b9_ps0
    check("B9-46 rc=5 前补 stale 记账 (D-F-05)",
          _rc == 5 and _b9_stale_calls == [1] and len(_pay) == 1,
          f"rc={_rc} stale={_b9_stale_calls} sends={len(_pay)}")

    # B9-47: --flush-now 空队列 state 落盘失败返回 5 (D-F-06)
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_filtered(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "英伟达美股盘前涨超2%"}]

        try:
            globals()["persist_state"] = lambda seen: False
            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _b9_filtered)],
                state={"seedmarker": 1}, args=["--flush-now"])
        finally:
            globals()["persist_state"] = _b9_ps0
    check("B9-47 --flush-now 空队列 state 落盘失败: rc=5 (D-F-06)",
          _rc == 5 and not _pay, f"rc={_rc} sends={_pay}")

    # B9-48: prune 白名单 (A-D-08①)
    check("B9-48 prune 只保留显式 META_KEYS (未知 '_' 键被当数据)",
          prune({"_alert": 1, "_digest": 2, "_day": 3, "_src_fail": 4,
                 "_research": 5, "_rogue": 6, "k": 7})
          == {"k": 7, "_alert": 1, "_digest": 2, "_day": 3, "_src_fail": 4, "_research": 5},
          str(prune({"_rogue": 6, "k": 7})))

    # B9-49: pending 损坏 .bad- 备份 (D-D-01)
    _b9_p0 = globals()["PENDING"]
    with tempfile.TemporaryDirectory() as _td:
        try:
            globals()["PENDING"] = Path(_td) / "pending.json"
            globals()["PENDING"].write_text("{bad json")
            _b9_keys = take_pending()
            _b9_bad = list(Path(_td).glob("pending.json.bad-*"))
        finally:
            globals()["PENDING"] = _b9_p0
    check("B9-49 pending 损坏对齐 .bad- 备份取证 (D-D-01)",
          _b9_keys == [] and len(_b9_bad) == 1,
          f"{_b9_keys} {[p.name for p in _b9_bad]}")

    # B9-50: _src_fail 下线源清理 (D-D-02)
    _b9_psf = globals().get("prune_src_fail")
    _b9_sf = {SRC_FAIL_KEY: {"财联社": {"rounds": 1}, "已下线源": {"rounds": 9}}}
    _b9_psf_ret = _b9_psf(_b9_sf) if callable(_b9_psf) else "missing"
    check("B9-50 已下线/改名源的 _src_fail 残留被清理 (D-D-02)",
          _b9_psf_ret is True and set(src_fail_state(_b9_sf)) == {"财联社"},
          f"{_b9_psf_ret} {_b9_sf}")

    # B9-51: 检视模式不发送 (D-D-03)
    _b9_insp: list[str] = []
    _b9_send_i = globals()["send"]
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_one51(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        def _b9_boom51(deadline=None):
            raise RuntimeError("boom")

        try:
            globals()["send"] = lambda text, conf, tk, depth=0: (_b9_insp.append(text) or True)
            _b6_run_main(Path(_td), _clk, [("S0", _b9_one51)],
                         conf_extra={"digest_max_items": 1},
                         state={"seedmarker": 1,
                                "_digest": {"last_flush": 1793800000 - 3600, "fails": 1}},
                         args=["--stats"])
            _b9_n_stats = len(_b9_insp)
            _b9_insp.clear()
            _b6_run_main(Path(_td), _clk, [("S0", _b9_boom51), ("S1", _b9_one51)],
                         conf_extra={"digest_max_items": 1, "src_alert_rounds": 3},
                         state={"seedmarker": 1,
                                SRC_FAIL_KEY: {"S0": {"rounds": 2, "since": 1,
                                                      "last_err": "old"}}},
                         args=["--queue"])
            _b9_n_queue = len(_b9_insp)
        finally:
            globals()["send"] = _b9_send_i
    check("B9-51 --stats/--queue 检视模式零发送 (D-D-03)",
          _b9_n_stats == 0 and _b9_n_queue == 0,
          f"stats={_b9_n_stats} queue={_b9_n_queue}")

    # B9-52: day_counter bool (RA-F-06)
    _b9_bd = {"_day": {"date": "2026-11-09", "msgs": True}}
    _b9_d = day_counter(_b9_bd, "2026-11-09")
    check("B9-52 day_counter 把 bool 归一为 int (RA-F-06)",
          _b9_d["msgs"] == 1 and not isinstance(_b9_d["msgs"], bool),
          repr(_b9_d["msgs"]))

    # B9-53: batch8.1 阈值 (SRC_ALERT_ROUNDS = 6)
    check("B9-53 SRC_ALERT_ROUNDS=6 且默认/显式覆盖都正确 (batch8.1)",
          SRC_ALERT_ROUNDS == 6 and src_alert_rounds({}) == 6
          and src_alert_rounds({"src_alert_rounds": 3}) == 3,
          f"{SRC_ALERT_ROUNDS}/{src_alert_rounds({})}")

    # ==== batch9-fix U4: 检视模式零落盘 / rc=0 / 备份冲突 (U4-F-01..F-03) ====
    # B9-76: --stats/--queue 遇损坏 pending/queue: 只读, 不改名、不写 state (U4-F-01)
    _b9_fx_badjson = "{bad json"
    _b9_fx_st = {"seedmarker": 1}
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_fx_one(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        _rcq, _payq, _stq, _qq, _cfgq = _b6_run_main(
            Path(_td), _clk, [("S0", _b9_fx_one)],
            conf_extra={"digest_max_items": 1}, state=_b9_fx_st,
            args=["--queue"], queue_raw=_b9_fx_badjson)
        _fx_qp = Path(_td) / "state" / "queue.json"
        _fx_qbad = list((Path(_td) / "state").glob("queue.json.bad-*"))
        _fx_qraw_ok = _fx_qp.exists() and _fx_qp.read_text() == _b9_fx_badjson
        _fx_state_q_ok = _stq == _b9_fx_st
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_fx_one2(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        _rcs, _pays, _sts, _qs, _cfgs = _b6_run_main(
            Path(_td), _clk, [("S0", _b9_fx_one2)],
            conf_extra={"digest_max_items": 1}, state=_b9_fx_st,
            args=["--stats"], pending_raw=_b9_fx_badjson)
        _fx_pp = Path(_td) / "state" / "pending.json"
        _fx_pbad = list((Path(_td) / "state").glob("pending.json.bad-*"))
        _fx_praw_ok = _fx_pp.exists() and _fx_pp.read_text() == _b9_fx_badjson
        _fx_state_p_ok = _sts == _b9_fx_st
    check("B9-76 --stats/--queue 遇损坏 queue/pending 零落盘: 不改名/不写 state (U4-F-01)",
          _rcq == 0 and not _payq and _fx_qraw_ok and not _fx_qbad and _fx_state_q_ok
          and _rcs == 0 and not _pays and _fx_praw_ok and not _fx_pbad and _fx_state_p_ok,
          f"rcq={_rcq} qbad={[p.name for p in _fx_qbad]} "
          f"rcs={_rcs} pbad={[p.name for p in _fx_pbad]}")

    # B9-77: 检视模式遇到全源失败 rc 仍为 0 (U4-F-02)
    _b9_fx_rc_inspect: list[tuple[str, int, int]] = []
    for _b9_fx_flag in ("--stats", "--queue"):
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _b9_fx_boom(deadline=None):
                raise RuntimeError("boom")

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _b9_fx_boom), ("S1", _b9_fx_boom)],
                state=_b9_fx_st, args=[_b9_fx_flag])
            _b9_fx_rc_inspect.append((_b9_fx_flag, _rc, len(_pay)))
    check("B9-77 检视模式全源失败仍 rc=0 且零发送/零落盘 (U4-F-02)",
          [(f, rc, n) for f, rc, n in _b9_fx_rc_inspect]
          == [("--stats", 0, 0), ("--queue", 0, 0)],
          str(_b9_fx_rc_inspect))

    # B9-78: 同一秒两次损坏 -> 两份 .bad- 备份都保留 (U4-F-03)
    _b9_p0 = globals()["PENDING"]
    _b9_time0 = globals()["time"]
    with tempfile.TemporaryDirectory() as _td:
        try:
            globals()["PENDING"] = Path(_td) / "pending.json"
            globals()["time"] = _B6Clock(1793800000)
            globals()["PENDING"].write_text("{bad-1")
            take_pending()
            globals()["PENDING"].write_text("{bad-2")
            take_pending()
            _b9_fx_bads = sorted(p.name for p in Path(_td).glob("pending.json.bad-*"))
        finally:
            globals()["PENDING"] = _b9_p0
            globals()["time"] = _b9_time0
    check("B9-78 同一秒两次损坏: 备份名冲突加序号, 两份都留 (U4-F-03)",
          len(_b9_fx_bads) == 2 and len(set(_b9_fx_bads)) == 2,
          str(_b9_fx_bads))

    # B9-79 (R8 重写): 旧版读全文比对自己写进 check 的源码字面量 -> 自指恒真 (R7 F-02);
    # 改为行为断言: 发送成功后 state 落盘失败必须写 pending 标记并返回 rc=5 (U1-3 块 + U4-9).
    _b9_k79 = dedupe_key("沪铜库存变化1吨环比1%")
    _b9_wp79: list[list[str]] = []
    _b9_wp0 = globals()["write_pending"]
    _b9_ps79 = globals()["persist_state"]
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_one79(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        try:
            globals()["write_pending"] = lambda keys: (_b9_wp79.append(list(keys)) or True)
            globals()["persist_state"] = lambda seen: False
            _rc79, _pay79, _stt79, _q79, _cfg79 = _b6_run_main(
                Path(_td), _clk, [("S0", _b9_one79)],
                conf_extra={"digest_max_items": 1}, state={"seedmarker": 1},
                args=["--flush-now"])
        finally:
            globals()["write_pending"] = _b9_wp0
            globals()["persist_state"] = _b9_ps79
    check("B9-79 发送成功后 state 落盘失败: 写 pending 标记且 rc=5 (U1-F-02 行为断言)",
          _rc79 == 5 and len(_pay79) == 1 and _b9_wp79 == [[_b9_k79]],
          f"rc={_rc79} sends={len(_pay79)} pending={_b9_wp79} key={_b9_k79!r}")

    # ==== Batch 9 U5: 测试与卫生 (B9-60..64) ====
    # B9-60: ROUND_START 不泄漏 (RA-F-05)
    _b9_rs_sentinel = 4242.0
    _b9_rs_keep = ROUND_START
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_rs_src(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        try:
            globals()["ROUND_START"] = _b9_rs_sentinel
            _b6_run_main(Path(_td), _clk, [("S0", _b9_rs_src)], state={"seedmarker": 1})
            _b9_rs_after = ROUND_START
        finally:
            globals()["ROUND_START"] = _b9_rs_keep
    check("B9-60 _b6_run_main 还原 ROUND_START (RA-F-05)",
          _b9_rs_after == _b9_rs_sentinel, repr(_b9_rs_after))

    # B9-61: send 探针不泄漏 SEND_ERR / SEND_RETRY_AFTER (RA-F-05)
    _b9_e0, _b9_ra0 = SEND_ERR, SEND_RETRY_AFTER
    try:
        globals()["SEND_ERR"] = "sentinel-e"
        globals()["SEND_RETRY_AFTER"] = 777.0
        _send_retry_ok()
        _b9_e1, _b9_ra1 = SEND_ERR, SEND_RETRY_AFTER
        globals()["SEND_ERR"] = "sentinel-e2"
        globals()["SEND_RETRY_AFTER"] = 778.0
        _send_429("120")
        _b9_e2, _b9_ra2 = SEND_ERR, SEND_RETRY_AFTER
    finally:
        globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"] = _b9_e0, _b9_ra0
    check("B9-61 send 探针还原 SEND_ERR / SEND_RETRY_AFTER (RA-F-05)",
          (_b9_e1, _b9_ra1) == ("sentinel-e", 777.0)
          and (_b9_e2, _b9_ra2) == ("sentinel-e2", 778.0),
          f"{_b9_e1}/{_b9_ra1} {_b9_e2}/{_b9_ra2}")

    # B9-62: main 每轮调用 source_health (迁移自 Q9)
    _b9_sh_calls: list[int] = []
    _b9_sh0 = globals()["source_health"]
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_sh_src(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        try:
            globals()["source_health"] = lambda *a, **k: (_b9_sh_calls.append(1) or False)
            _b6_run_main(Path(_td), _clk, [("S0", _b9_sh_src)], state={"seedmarker": 1})
        finally:
            globals()["source_health"] = _b9_sh0
    check("U2 main 每轮调用 source_health", _b9_sh_calls == [1], str(_b9_sh_calls))

    # B9-63: main 每轮调用 rotate_stderr (迁移自 Q10)
    _b9_rot: list[int] = []
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_rot_src(deadline=None):
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        _b6_run_main(Path(_td), _clk, [("S0", _b9_rot_src)], state={"seedmarker": 1},
                     rotate_hook=lambda: _b9_rot.append(1))
    check("U3 main 每轮调用 rotate_stderr", _b9_rot == [1], str(_b9_rot))

    # B9-64: main 预算耗尽走 SRC_SKIP 且不当失败 (迁移自 Q12)
    _b9_skip_calls: list[int] = []
    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _b9_slow64(deadline=None):
            _clk.mono += 44.0
            return [{"src": "S0", "ts": int(_clk.time()) - 60,
                     "text": "沪铜库存变化1吨环比1%"}]

        def _b9_never64(deadline=None):
            _b9_skip_calls.append(1)
            return []

        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk, [("S0", _b9_slow64), ("S1", _b9_never64)],
            conf_extra={"timeout_s": 45}, state={"seedmarker": 1})
    check("B6 U4 main 预算耗尽分支改用 SRC_SKIP 哨兵",
          _rc == 0 and not _b9_skip_calls and not _stt.get("_alert"),
          f"rc={_rc} skipped_calls={_b9_skip_calls} state={_stt}")

    # ==== Batch 10 R8: 断言体系收口 (F-01 R5/R6 / D-2 / D-4 / B9-79 行为化) ====
    # 只加/换断言, 不改生产行为; 全部离线 (打桩 subprocess/tg/假时钟; 不写真实 state/queue).

    # ---- R8 F-01(R5): http() 缺 returncode/stdout 的 fail-closed 契约 (仓内行为断言) ----
    class _R8NoRc:
        def __init__(self, out):
            self.stdout = out

    class _R8NoOut:
        def __init__(self, rc):
            self.returncode = rc

    class _R8Ok:
        def __init__(self, out, rc):
            self.stdout = out
            self.returncode = rc

    _r8_run0 = subprocess.run
    _r8_calls: list[int] = []
    _r8_exc = ""
    try:
        subprocess.run = lambda cmd, **kw: (_r8_calls.append(1) or _R8NoRc(b"ok\n200"))
        try:
            http("https://x/y")
        except Exception as _e:
            _r8_exc = type(_e).__name__
    finally:
        subprocess.run = _r8_run0
    check("R8-F01a http 结果缺 returncode: HttpContractError 且只调用 1 次 (R5 fail-closed)",
          _r8_exc == "HttpContractError" and len(_r8_calls) == 1,
          f"exc={_r8_exc} calls={len(_r8_calls)}")
    _r8_calls.clear()
    _r8_exc = ""
    try:
        subprocess.run = lambda cmd, **kw: (_r8_calls.append(1) or _R8NoOut(0))
        try:
            http("https://x/y")
        except Exception as _e:
            _r8_exc = type(_e).__name__
    finally:
        subprocess.run = _r8_run0
    check("R8-F01b http 结果缺 stdout: HttpContractError 且只调用 1 次 (不重试)",
          _r8_exc == "HttpContractError" and len(_r8_calls) == 1,
          f"exc={_r8_exc} calls={len(_r8_calls)}")
    _r8_calls.clear()
    _r8_body = None
    _r8_msg = ""
    try:
        subprocess.run = lambda cmd, **kw: (_r8_calls.append(1) or _R8Ok(b"ok\n200", 0))
        _r8_body = http("https://x/y")
        subprocess.run = lambda cmd, **kw: (_r8_calls.append(1) or _R8Ok(b"", 63))
        try:
            http("https://x/y", tries=1)
        except RuntimeError as _e:
            _r8_msg = str(_e)
    finally:
        subprocess.run = _r8_run0
    check("R8-F01c 正常桩返回体不变 + 非 0 rc 仍按失败映射 (rc 语义)",
          _r8_body == b"ok" and "curl rc=63" in _r8_msg,
          f"body={_r8_body!r} msg={_r8_msg!r}")

    # ---- R8 D-2: flush_round 依赖默认 SendContext 的同一存储 (行为) ----
    _r8_sc = SendContext()
    _r8_e0, _r8_ra0 = SEND_ERR, SEND_RETRY_AFTER
    try:
        globals()["SEND_ERR"] = "A"
        _r8_d2a = _r8_sc.err
        _r8_sc.err = "B"
        _r8_d2b = SEND_ERR
        globals()["SEND_RETRY_AFTER"] = 12.5
        _r8_d2c = _r8_sc.retry_after
        _r8_d2d = _DEFAULT_SEND.err == "B" and _DEFAULT_SEND.retry_after == 12.5
    finally:
        globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"] = _r8_e0, _r8_ra0
    check("R8-D2a 新建 SendContext 与 _DEFAULT_SEND 同存储 (双向写穿)",
          _r8_d2a == "A" and _r8_d2b == "B" and _r8_d2c == 12.5 and _r8_d2d,
          f"{_r8_d2a!r}/{_r8_d2b!r}/{_r8_d2c!r}/{_r8_d2d}")

    _r8_now = int(time.time())
    _r8_plan = DigestPlan(
        rc=None,
        queue=[{"key": "r8d2key", "src": "S0", "ts": _r8_now - 60,
                "text": "沪铜库存变化1吨环比1%"}],
        meta={}, day={}, due=True, now_f=float(_r8_now), now_ts=_r8_now,
        max_age=10800, digest_items=40, digest_min_items=3, max_hold=2700,
        waited=2000.0, interval=1800, oldest_age=60.0, next_try=0.0)
    _r8_opts = RunOpts(
        dry=False, seed=False, stats=False, explain=False, force_flush=False,
        show_queue=False, inspect=False, digest_items=40, digest_min_items=3,
        max_hold=2700, digest_min=30, keywords=DEFAULT_KEYWORDS,
        use_filter=True, budget=45)
    _r8_seen = {"seedmarker": 1}
    _r8_send0 = globals()["send"]
    _r8_ps0 = globals()["persist_state"]
    _r8_sq0 = globals()["save_queue"]
    _r8_cp0 = globals()["clear_pending"]
    _r8_wp0 = globals()["write_pending"]
    _r8_rs0 = globals()["record_stale"]
    _r8_stderr0 = sys.stderr
    _r8_rs_start = ROUND_START
    _r8_buf = io.StringIO()
    _r8_d2_rc = None
    try:
        def _r8_send_boom(text, conf, tk, depth=0):
            _DEFAULT_SEND.err = "boom-D2"
            return False

        globals()["send"] = _r8_send_boom
        globals()["persist_state"] = lambda seen: True
        globals()["save_queue"] = lambda items: True
        globals()["clear_pending"] = lambda: None
        globals()["write_pending"] = lambda keys: True
        globals()["record_stale"] = lambda *a, **k: None
        globals()["ROUND_START"] = time.monotonic()
        sys.stderr = _r8_buf
        _r8_d2_rc = flush_round({"chat_id": 1}, _r8_opts, _r8_seen, _r8_plan,
                                "tk", [], _DEFAULT_ROUND, SendContext())
    finally:
        sys.stderr = _r8_stderr0
        globals()["send"] = _r8_send0
        globals()["persist_state"] = _r8_ps0
        globals()["save_queue"] = _r8_sq0
        globals()["clear_pending"] = _r8_cp0
        globals()["write_pending"] = _r8_wp0
        globals()["record_stale"] = _r8_rs0
        globals()["ROUND_START"] = _r8_rs_start
    _r8_errline = _r8_buf.getvalue()
    check("R8-D2b 未显式传 ctx 的 send 失败原因经 send_ctx 可读 (默认 _DEFAULT_SEND 同存储)",
          _r8_d2_rc == 2 and "boom-D2" in _r8_errline,
          f"rc={_r8_d2_rc} stderr={_r8_errline!r}")

    # ---- R8 F-01(R6): 12 个命名常量的行为型钉子 (改值 -> 对应场景结果变) ----
    check("R8-C01 send_backoff_s 序列 120/240/480/960/1800 封顶 (RETRY_BASE/MAX/EXP)",
          [send_backoff_s(n) for n in range(7)] == [120, 240, 480, 960, 1800, 1800, 1800]
          and RETRY_MAX_S == RA_MAX == 1800,
          str([send_backoff_s(n) for n in range(7)]))

    def _r8_backoff_delta(fails0):
        _ra_keep = SEND_RETRY_AFTER
        _send0 = globals()["send"]
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            try:
                globals()["SEND_RETRY_AFTER"] = None
                globals()["send"] = lambda text, conf, tk, depth=0: False
                _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                    Path(_td), _clk, [("S0", _one)],
                    conf_extra={"digest_max_items": 1},
                    state={"seedmarker": 1,
                           "_digest": {"last_flush": 1793800000 - 3600,
                                       "fails": fails0}})
            finally:
                globals()["send"] = _send0
                globals()["SEND_RETRY_AFTER"] = _ra_keep
        return to_float((_stt.get("_digest") or {}).get("next_try")) - 1793800000.0

    _r8_d1 = _r8_backoff_delta(0)
    _r8_d4 = _r8_backoff_delta(3)
    check("R8-C02 flush_round 首败退避 240s / 第 4 败封顶 1800s (调用点行为)",
          _r8_d1 == 240.0 and _r8_d4 == 1800.0,
          f"fails1={_r8_d1} fails4={_r8_d4}")

    def _r8_research_ran(offset):
        _calls: list[int] = []
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _rep(deadline=None):
                _calls.append(1)
                return []

            _b6_run_main(Path(_td), _clk, [("东财研报", _rep)],
                         state={"seedmarker": 1,
                                "_research": {"东财研报": 1793800000 + offset}})
        return len(_calls)

    _r8_re61, _r8_re60 = _r8_research_ran(61), _r8_research_ran(60)
    check("R8-C03 研报节流时间戳 +61s 视为未来重跑 / +60s 按合法节流跳过 (ROLLBACK_TOL_S 边界)",
          _r8_re61 == 1 and _r8_re60 == 0, f"+61={_r8_re61} +60={_r8_re60}")

    def _r8_last_flush(offset):
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _one)],
                state={"seedmarker": 1,
                       "_digest": {"last_flush": 1793800000 + offset}})
        return to_float((_stt.get("_digest") or {}).get("last_flush"))

    _r8_lf61, _r8_lf60 = _r8_last_flush(61), _r8_last_flush(60)
    check("R8-C04 last_flush 未来 +61s 重置为当前 / +60s 保留 (ROLLBACK_TOL_S 边界)",
          _r8_lf61 == 1793800000.0 and _r8_lf60 == 1793800060.0,
          f"+61={_r8_lf61} +60={_r8_lf60}")

    def _r8_block_acc(offset):
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _one)],
                state={"seedmarker": 1,
                       "_digest": {"last_round": 1793800000 + offset,
                                   "last_flush": 1793800000 - 1800,
                                   "block_acc": 0.0, "next_try": 0}})
        return to_float((_stt.get("_digest") or {}).get("block_acc"))

    _r8_ba61, _r8_ba60 = _r8_block_acc(61), _r8_block_acc(60)
    check("R8-C05 last_round 未来 +61s 记 block_acc / +60s 不记 (ROLLBACK_TOL_S 边界)",
          _r8_ba61 > 0.0 and _r8_ba60 == 0.0,
          f"+61={_r8_ba61} +60={_r8_ba60}")

    def _r8_left_pending(waited):
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _one)],
                state={"seedmarker": 1,
                       "_digest": {"last_flush": 1793800000 - waited,
                                   "left_pending": True}})
        return len(_pay), len(_q)

    _r8_lp60, _r8_lp59 = _r8_left_pending(60), _r8_left_pending(59)
    check("R8-C06 left_pending 剩余待发: 60s 立即补发 / 59s 未到最短间隔 (LEFT_PENDING_RETRY_S)",
          _r8_lp60 == (1, 0) and _r8_lp59 == (0, 1),
          f"+60={_r8_lp60} +59={_r8_lp59}")

    _r8_today = datetime.now(TZ_CN).strftime("%Y-%m-%d")
    _r8_send_keep = globals()["send"]
    try:
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            try:
                globals()["send"] = lambda text, conf, tk, depth=0: False
                _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                    Path(_td), _clk, [("S0", _one)],
                    state={"seedmarker": 1,
                           "_day": {"date": _r8_today, "msgs": DAILY_MSG_CAP},
                           "_digest": {"last_flush": 1793800000 - 60}})
            finally:
                globals()["send"] = _r8_send_keep
    finally:
        globals()["send"] = _r8_send_keep
    _r8_cap_delta = to_float((_stt.get("_digest") or {}).get("next_try")) - 1793800000.0
    check("R8-C07 触顶提示发送失败: next_try = now + 600s (CAP_NOTICE_RETRY_S 行为)",
          _rc == 0 and _r8_cap_delta == 600.0
          and (_stt.get("_day") or {}).get("cap_notified") is False,
          f"rc={_rc} delta={_r8_cap_delta} day={_stt.get('_day')}")

    _r8_conf0 = globals()["CONF"]
    try:
        with tempfile.TemporaryDirectory() as _td:
            _r8_cp = Path(_td) / "c.json"
            _r8_cp.write_text(json.dumps({"max_hold_min": 1, "timeout_s": 1}))
            globals()["CONF"] = _r8_cp
            _r8_cfg, _r8_opts2 = load_conf([])
    finally:
        globals()["CONF"] = _r8_conf0
    check("R8-C08 max_hold_min=1 被 MAX_HOLD_FLOOR_S 夹到 300s (下限行为)",
          _r8_opts2.max_hold == 300, repr(_r8_opts2.max_hold))
    check("R8-C09 timeout_s=1 被 BUDGET_FLOOR_S 夹到 15s (下限行为)",
          _r8_opts2.budget == 15, repr(_r8_opts2.budget))

    def _r8_digest_state(digest):
        with tempfile.TemporaryDirectory() as _td:
            _clk = _B6Clock(1793800000)

            def _one(deadline=None):
                return [{"src": "S0", "ts": int(_clk.time()) - 60,
                         "text": "沪铜库存变化1吨环比1%"}]

            _rc, _pay, _stt, _q, _cfg = _b6_run_main(
                Path(_td), _clk, [("S0", _one)],
                state={"seedmarker": 1, "_digest": digest})
        return _stt.get("_digest") or {}

    _r8_bs = _r8_digest_state({"next_try": 1793800000 + 1000,
                               "last_flush": 1793800000 - 60, "block_acc": 0.0})
    check("R8-C10 阻塞一轮 block_acc 至少累加 120s (BLOCK_ACC_STEP_MIN_S)",
          to_float(_r8_bs.get("block_acc")) == 120.0, repr(_r8_bs.get("block_acc")))

    _r8_bc = _r8_digest_state({"next_try": 1793800000 + 1000,
                               "last_flush": 1793800000 - 60,
                               "block_acc": 604799.0})
    check("R8-C11 block_acc 被 7*86400s 封顶 (BLOCK_ACC_CAP_S 行为)",
          to_float(_r8_bc.get("block_acc")) == 604800.0, repr(_r8_bc.get("block_acc")))

    _r8_lg = _r8_digest_state({"last_round": 1793800000 - 3600,
                               "last_flush": 1793800000 - 60, "block_acc": 0.0})
    _r8_lg2 = _r8_digest_state({"last_round": 1793800000 - 3599,
                                "last_flush": 1793800000 - 60, "block_acc": 0.0})
    check("R8-C12 gap 恰为 3600s 即按长间隔记 block_acc / 3599s 不记 (LONG_GAP_S 边界)",
          to_float(_r8_lg.get("block_acc")) == 3600.0
          and to_float(_r8_lg2.get("block_acc")) == 0.0,
          f"3600={_r8_lg.get('block_acc')} 3599={_r8_lg2.get('block_acc')}")

    with tempfile.TemporaryDirectory() as _td:
        _clk = _B6Clock(1793800000)

        def _r8_research_boom(deadline=None):
            raise RuntimeError("research boom")

        _rc, _pay, _stt, _q, _cfg = _b6_run_main(
            Path(_td), _clk, [("东财研报", _r8_research_boom)],
            state={"seedmarker": 1,
                   "_research": {"东财研报": 1793800000 - 1800}})
    _r8_rr = to_int((_stt.get("_research") or {}).get("东财研报"))
    check("R8-C13 研报失败后 5 分钟重试: 记 now-(1800-300) (RESEARCH_RETRY_S 行为)",
          _r8_rr == 1793800000 - 1500, repr(_r8_rr))

    check("R8-C14 SEND_GATE_S 单点派生: 硬顶-本地余量 == 114, round_soft_s 再减最坏发送 == 60",
          SEND_GATE_S == ROUND_HARD_S - ROUND_MARGIN_S
          and SEND_GATE_S == 114 and round_soft_s() == SEND_GATE_S - SEND_WORST == 60,
          f"gate={SEND_GATE_S} worst={SEND_WORST} soft={round_soft_s()}")

    # ---- R8 D-4: B10-07..B10-10 并入 selftest (原 evidence/R3/b10-asserts.py 带外) ----
    _b10_rs0, _b10_e0, _b10_ra0 = ROUND_START, SEND_ERR, SEND_RETRY_AFTER
    _b10_sw0 = SEND_WORST
    _b10_cfg0 = dict(CFG)
    _b10_ts0 = dict(TS_BAD)
    _b10_rc = RoundContext()
    _b10_sc = SendContext()
    try:
        # B10-10a-f: 无影子状态 (__slots__ 空 + 未知属性必须 AttributeError)
        check("B10-10a RoundContext 无 __dict__ (__slots__=())",
              not hasattr(_b10_rc, "__dict__"),
              repr(getattr(_b10_rc, "__dict__", None)))
        check("B10-10b SendContext 无 __dict__ (__slots__=())",
              not hasattr(_b10_sc, "__dict__"),
              repr(getattr(_b10_sc, "__dict__", None)))
        check("B10-10c RoundContext.__slots__ 为空元组", RoundContext.__slots__ == (),
              repr(RoundContext.__slots__))
        check("B10-10d SendContext.__slots__ 为空元组", SendContext.__slots__ == (),
              repr(SendContext.__slots__))
        try:
            _b10_rc.start = 1.0
            _b10_set_ok = True
        except Exception:
            _b10_set_ok = False
        check("B10-10e 属性 setter 可用 (不是只读视图)", _b10_set_ok)
        try:
            _b10_rc.unknown_field = 1
            _b10_slot_ok = False
        except AttributeError:
            _b10_slot_ok = True
        check("B10-10f 禁止新增实例字段 (写未知属性 AttributeError)", _b10_slot_ok)

        # B10-08a-g: RoundContext 同一存储
        globals()["ROUND_START"] = 1.5
        check("B10-08a 全局写 -> rc.start 读到 (写穿)", _b10_rc.start == 1.5,
              repr(_b10_rc.start))
        _b10_rc.start = 2.5
        check("B10-08b rc.start 写 -> 全局变化 (写穿)", ROUND_START == 2.5,
              repr(ROUND_START))
        check("B10-08c rc.cfg is CFG (同对象身份, 非 copy)", _b10_rc.cfg is CFG,
              f"{id(_b10_rc.cfg)} != {id(CFG)}")
        check("B10-08d rc.ts_bad is TS_BAD (同对象身份, 非 copy)", _b10_rc.ts_bad is TS_BAD,
              f"{id(_b10_rc.ts_bad)} != {id(TS_BAD)}")
        check("B10-08e 默认视图 _DEFAULT_ROUND 也指向同一存储",
              _DEFAULT_ROUND.cfg is CFG and _DEFAULT_ROUND.ts_bad is TS_BAD
              and _DEFAULT_ROUND.start == ROUND_START)
        CFG.clear()
        _b10_rc.cfg["proxy"] = "http://alias-proof:1"
        check("B10-08f ctx.cfg 写入 -> CFG 同步 (容器别名)",
              CFG.get("proxy") == "http://alias-proof:1", repr(dict(CFG)))
        TS_BAD.clear()
        _b10_rc.ts_bad["S"] = 7
        check("B10-08g ctx.ts_bad 写入 -> TS_BAD 同步 (容器别名)", TS_BAD.get("S") == 7,
              repr(TS_BAD))

        # B10-07a-f: SendContext 同一存储
        globals()["SEND_ERR"] = "A"
        check("B10-07a 全局 SEND_ERR -> sc.err", _b10_sc.err == "A", repr(_b10_sc.err))
        _b10_sc.err = "B"
        check("B10-07b sc.err 写 -> 全局 SEND_ERR", SEND_ERR == "B", repr(SEND_ERR))
        globals()["SEND_RETRY_AFTER"] = 12.5
        check("B10-07c 全局 SEND_RETRY_AFTER -> sc.retry_after",
              _b10_sc.retry_after == 12.5, repr(_b10_sc.retry_after))
        _b10_sc.retry_after = 30.0
        check("B10-07d sc.retry_after 写 -> 全局", SEND_RETRY_AFTER == 30.0,
              repr(SEND_RETRY_AFTER))
        _b10_sc.retry_after = None
        check("B10-07e None 可写穿", SEND_RETRY_AFTER is None, repr(SEND_RETRY_AFTER))
        check("B10-07f 默认视图 _DEFAULT_SEND 指向同一存储",
              _DEFAULT_SEND.err == SEND_ERR
              and _DEFAULT_SEND.retry_after is SEND_RETRY_AFTER)

        # B10-09a-c: 无参 send_time_ok() 仍读全局同一存储
        globals()["SEND_WORST"] = 54
        globals()["ROUND_START"] = time.monotonic() - 70.0
        check("B10-09a ROUND_START=now-70s -> send_time_ok() is False",
              send_time_ok() is False, f"gate={SEND_GATE_S} worst={SEND_WORST}")
        globals()["ROUND_START"] = time.monotonic()
        check("B10-09b ROUND_START=now -> send_time_ok() is True",
              send_time_ok() is True, f"gate={SEND_GATE_S} worst={SEND_WORST}")
        globals()["ROUND_START"] = time.monotonic() - 70.0
        check("B10-09c 显式传 ctx 与无参默认视图同结果 (同一存储)",
              send_time_ok(_b10_rc) is False and send_time_ok() is False,
              f"explicit={send_time_ok(_b10_rc)} default={send_time_ok()}")
    finally:
        globals()["ROUND_START"] = _b10_rs0
        globals()["SEND_ERR"], globals()["SEND_RETRY_AFTER"] = _b10_e0, _b10_ra0
        globals()["SEND_WORST"] = _b10_sw0
        CFG.clear()
        CFG.update(_b10_cfg0)
        TS_BAD.clear()
        TS_BAD.update(_b10_ts0)

    # B10-10g-k + B10-L01: 单一写入点结构审计 (AST, 只统计生产区, 不读 selftest 自身文本)
    import ast as _b10_ast
    _b10_tree = _b10_ast.parse(Path(__file__).read_text(encoding="utf-8"))
    _b10_globs: dict[str, list[tuple[str, ...]]] = {}

    def _b10_walk(node, chain=()):
        if isinstance(node, (_b10_ast.FunctionDef, _b10_ast.AsyncFunctionDef,
                             _b10_ast.ClassDef)):
            chain = chain + (node.name,)
        if isinstance(node, _b10_ast.Global):
            for _n in node.names:
                _b10_globs.setdefault(_n, []).append(chain)
        for _ch in _b10_ast.iter_child_nodes(node):
            _b10_walk(_ch, chain)

    _b10_walk(_b10_tree)

    def _b10_prod(name):
        return [c for c in _b10_globs.get(name, []) if "selftest" not in c]

    for _b10_g, _b10_setter, _b10_cls in (
            ("ROUND_START", "start", "RoundContext"),
            ("SEND_ERR", "err", "SendContext"),
            ("SEND_RETRY_AFTER", "retry_after", "SendContext")):
        _b10_occ = _b10_prod(_b10_g)
        check(f"B10-10g `global {_b10_g}` 在生产区恰好 1 处", len(_b10_occ) == 1,
              f"{_b10_occ}")
        check(f"B10-10h `global {_b10_g}` 落在 {_b10_cls}.{_b10_setter} 内",
              _b10_occ == [(_b10_cls, _b10_setter)], f"{_b10_occ}")
    _b10_defs: dict[str, list[int]] = {}
    for _b10_node in _b10_tree.body:
        if (isinstance(_b10_node, _b10_ast.Assign)
                and len(_b10_node.targets) == 1
                and isinstance(_b10_node.targets[0], _b10_ast.Name)):
            _b10_defs.setdefault(_b10_node.targets[0].id, []).append(_b10_node.lineno)
        if (isinstance(_b10_node, _b10_ast.AnnAssign)
                and isinstance(_b10_node.target, _b10_ast.Name)):
            _b10_defs.setdefault(_b10_node.target.id, []).append(_b10_node.lineno)
    check("B10-10i 生产区模块级 ROUND_START 赋值恰好 1 处 (定义行)",
          len(_b10_defs.get("ROUND_START", [])) == 1,
          f"{_b10_defs.get('ROUND_START')}")
    check("B10-10j main 内无 `global ROUND_START`",
          all("main" not in _c for _c in _b10_prod("ROUND_START")),
          f"{_b10_prod('ROUND_START')}")
    check("B10-10k 生产区 `global SEND_ERR`/`global SEND_RETRY_AFTER` 各只有 setter 1 处",
          len(_b10_prod("SEND_ERR")) == 1 and len(_b10_prod("SEND_RETRY_AFTER")) == 1,
          f"{_b10_prod('SEND_ERR')} {_b10_prod('SEND_RETRY_AFTER')}")
    for _b10_g in ("ROUND_START", "CFG", "TS_BAD", "SEND_ERR", "SEND_RETRY_AFTER"):
        check(f"B10-L01 `^{_b10_g}` 模块级定义恰好 1 行",
              len(_b10_defs.get(_b10_g, [])) == 1, f"{_b10_defs.get(_b10_g)}")

    # B9-65/66: tests/probe2.py / tests/probe4.py 去重自检 (RE-F-04)
    # 本批 CTO 约束「不改 tests/」, 故这两条 probe 内断言留待 CTO 裁决; 见实施报告未解决项.

    # ==== Batch 11: 研报正文全文推送(分片) ====
    # 本段整体插入 selftest() 末尾 (B7-02 之前无依赖); 同时由 evidence/impl/run_block.py
    # 原样驱动以取证「旧码必红」。每个断言包在 _b11_case 里: 旧码缺函数/常量时只红该条,
    # 不让 NameError 中断整段。
    import contextlib
    import tempfile

    _B11_NOW = 1793800000
    _B11_TS = _B11_NOW - 60
    _B11_REAL_DT = globals()["datetime"]
    _B11_GUARD_SHA = "41f4b605dffc0f415d8e2e89704f8e6b36e152e21c8138a9002bd12b26267984"


    class _B11FrozenDT(_B11_REAL_DT):
        @classmethod
        def now(cls, tz=None):
            return _B11_REAL_DT.fromtimestamp(_B11_NOW, tz)


    def _b11_clk(t0: int = _B11_NOW):
        class _C:
            def __init__(self):
                self.t = float(t0)
                self.mono = 1000.0

            def time(self):
                return self.t

            def monotonic(self):
                return self.mono

            def sleep(self, s):
                self.t += s
                self.mono += s

        return _C()


    def _b11_case(name: str, fn) -> None:
        try:
            ok, detail = fn()
        except Exception as e:                       # noqa: BLE001 - 旧码缺符号 -> 本条红
            ok, detail = False, f"{type(e).__name__}: {e}"
        check(name, bool(ok), str(detail))


    def _b11_state(seen_extra: dict | None = None, last_flush=None) -> dict:
        st = {"seedmarker": 1,
              "_digest": {"last_flush": (_B11_NOW - 31 * 60) if last_flush is None else last_flush}}
        for k, v in (seen_extra or {}).items():
            st[k] = v
        return st


    def _b11_src(items: list[dict], name: str = "财联社") -> list:
        def _f(deadline=None):
            return [dict(x) for x in items]
        return [(name, _f)]


    def _b11_ping(key: str = "k-b11-seen") -> dict:
        """已读条目: 源每轮都供它, 只为让 main 走过「无条目早退」, 不产生队列变化."""
        return {"src": "财联社", "ts": _B11_TS, "text": "沪铜库存变化9吨环比9%", "key": key}


    def _b11_ritem(key: str = "研究:东财研报:1", body: str | None = None,
                   ts: int | None = None, src: str = "东财研报") -> dict:
        if body is None:
            body = "沪铜库存下降，下游需求回暖。" * 400          # 5600 字 -> 3 片
        return {"src": src, "ts": _B11_TS if ts is None else ts,
                "text": "钨行业三季报点评：钨资源与硬质合金价格企稳反弹｜五矿证券 · 2026-11-04",
                "url": "https://example.com/research/w.pdf", "snippet": body, "key": key}


    def _b11_norm(key: str = "k-b11-n", ts: int | None = None, text: str = "沪铜库存变化1吨环比1%") -> dict:
        return {"src": "财联社", "ts": _B11_TS if ts is None else ts, "text": text, "key": key}


    def _b11_run(tmp, clock, srcs, conf_extra=None, state=None, queue=None,
                 tg_hook=None, save_probe=None, pending_probe=None):
        """tempdir 内实跑 main(); 返回 (rc, 载荷, state, 队列). 全程离线、不碰真实目录."""
        g = globals()
        keep = {k: g[k] for k in (
            "STATE", "QUEUE", "PENDING", "LOCK", "CONF", "LOG", "ERRLOG", "SOURCES",
            "time", "datetime", "tg", "resolve_token", "rotate_log", "rotate_stderr",
            "ROUND_START", "SEND_WORST", "save_queue", "write_pending", "http",
            "pdf_text", "PDFTOTEXT")}
        tmp = Path(tmp)
        st = tmp / "state"
        st.mkdir(parents=True, exist_ok=True)
        payloads: list[str] = []
        _sq0, _wp0 = g["save_queue"], g["write_pending"]

        def _sq(items):
            if save_probe is not None:
                save_probe.append([dict(x) for x in items])
            return _sq0(items)

        def _wp(keys):
            if pending_probe is not None:
                pending_probe.append(list(keys))
            return _wp0(keys)

        def _tg(method, payload, tk, proxy, timeout=25):
            text = str((payload or {}).get("text") or "")
            payloads.append(text)
            if tg_hook is not None:
                tg_hook(text, clock)
            return {"ok": True}

        try:
            g["STATE"] = st / "seen.json"
            g["QUEUE"] = st / "queue.json"
            g["PENDING"] = st / "pending.json"
            g["LOCK"] = st / "lock"
            g["CONF"] = st / "conf.json"
            g["LOG"] = st / "out.log"
            g["ERRLOG"] = st / "err.log"
            g["SOURCES"] = srcs
            g["time"] = clock
            g["datetime"] = _B11FrozenDT
            g["tg"] = _tg
            g["resolve_token"] = lambda conf: "x"
            g["rotate_log"] = lambda: None
            g["rotate_stderr"] = lambda: None
            g["save_queue"] = _sq
            g["write_pending"] = _wp
            g["http"] = lambda *a, **k: (_ for _ in ()).throw(AssertionError("no network"))
            g["pdf_text"] = lambda *a, **k: (_ for _ in ()).throw(AssertionError("unexpected pdf_text"))
            conf = {"token": "x", "chat_id": 1, "max_age_min": 180,
                    "digest_min": 30, "digest_max_items": 40}
            conf.update(conf_extra or {})
            g["CONF"].write_text(json.dumps(conf), encoding="utf-8")
            if state is not None:
                g["STATE"].write_text(json.dumps(state), encoding="utf-8")
            if queue is not None:
                g["QUEUE"].write_text(json.dumps({"items": queue, "ts": _B11_NOW}),
                                      encoding="utf-8")
            old_argv = sys.argv
            _so, _se = io.StringIO(), io.StringIO()
            try:
                sys.argv = ["monitor.py"]
                with contextlib.redirect_stdout(_so), contextlib.redirect_stderr(_se):
                    rc = main()
            finally:
                sys.argv = old_argv
            got_state = (json.loads(g["STATE"].read_text(encoding="utf-8"))
                         if g["STATE"].exists() else {})
            got_q, _p, _b = load_queue()
            return rc, payloads, got_state, got_q
        finally:
            g.update(keep)


    def _b11_guard_sha() -> str:
        keep = globals()["datetime"]
        globals()["datetime"] = _B11FrozenDT
        try:
            items = [
                {"src": "财联社", "ts": _B11_TS, "text": "沪铜库存变化1吨环比1%", "key": "k-b11-g1"},
                {"src": "东财", "ts": _B11_TS - 60, "text": "螺纹钢库存变化2吨环比2% & <铁矿>",
                 "key": "k-b11-g2"},
                {"src": "同花顺", "ts": _B11_TS - 120,
                 "text": "沪指收盘上涨0.52%，两市成交额1.2万亿元", "key": "k-b11-g3"},
                {"src": "新浪", "ts": _B11_TS - 86400, "text": "🚨 白银库存变化5吨环比5%",
                 "key": "k-b11-g4"},
                {"src": "SMM", "ts": _B11_TS - 90000, "text": "沪铝库存变化6吨环比6%" * 30,
                 "key": "k-b11-g5"},
            ]
            chunks = build_messages(items)
            payload = json.dumps([[t, [x["key"] for x in u]] for t, u in chunks],
                                 ensure_ascii=False)
            return hashlib.sha256(payload.encode()).hexdigest()
        finally:
            globals()["datetime"] = keep


    def _b11_a01():
        return (SNIPPET_MAX == 24000 and SNIPPET_FULL_MAX == 40000
                and SNIPPET_FULL_MAX > SNIPPET_MAX,
                f"SNIPPET_MAX={SNIPPET_MAX} FULL={SNIPPET_FULL_MAX}")


    def _b11_a02():
        cmds: list[list] = []

        class _R:
            returncode = 0
            stdout = ("钨行业三季报点评，钨资源与硬质合金价格企稳反弹。" * 40).encode()

        def _run(cmd, **kw):
            cmds.append(list(cmd))
            return _R()

        g = globals()
        run0, http0, bin0 = subprocess.run, g["http"], g["PDFTOTEXT"]
        try:
            g["http"] = lambda *a, **k: b"%PDF-1.4\n"
            g["PDFTOTEXT"] = sys.executable
            subprocess.run = _run
            out = pdf_text("https://example.com/x.pdf")
        finally:
            subprocess.run = run0
            g["http"], g["PDFTOTEXT"] = http0, bin0
        pdf_cmds = [c for c in cmds if c and c[0] == sys.executable]
        has32 = bool(pdf_cmds) and "-l" in pdf_cmds[0] and pdf_cmds[0][
            pdf_cmds[0].index("-l") + 1] == "32"
        return (PDF_MAX_PAGES == 32 and has32 and PDF_TIMEOUT == 6.0 and PDF_BUDGET_S == 14.0,
                f"pages={PDF_MAX_PAGES} argv={pdf_cmds[-1] if pdf_cmds else None} len={len(out)}")


    def _b11_a03():
        short = "钨" * 100
        long_body = "钨" * (SNIPPET_MAX + 500)
        cut = snip_truncate(long_body)
        return (snip_truncate(short) == short and len(cut) <= SNIPPET_MAX
                and "已截断" in cut and cut.startswith("钨"),
                f"short={len(short)} cut={len(cut)} mark={'已截断' in cut}")


    def _b11_a04():
        g = globals()
        run0, http0, bin0 = subprocess.run, g["http"], g["PDFTOTEXT"]
        # 真 PDF 走环境变量 FUTNEWS_SAMPLE_PDF, 未设置则走下方打桩分支 (离线可跑)
        _pdf_env = os.environ.get("FUTNEWS_SAMPLE_PDF", "")
        pdf = Path(_pdf_env or "/nonexistent/futures-news-sample.pdf")
        mode = "real-file"
        try:
            if not (pdf.exists() and bin0 and os.path.exists(bin0)):   # 离线兜底: 打桩 pdftotext
                mode = "stub-text"

                class _R:
                    returncode = 0
                    stdout = ("钨行业三季报点评，钨资源与硬质合金价格企稳反弹，"
                              "硬质合金及刀具公司季度营收环比改善。" * 100).encode()

                def _run(cmd, **kw):
                    return _R()

                g["http"] = lambda *a, **k: b"%PDF-1.4\n"
                g["PDFTOTEXT"] = sys.executable
                subprocess.run = _run
            else:
                g["http"] = lambda *a, **k: pdf.read_bytes()
            out = pdf_text("https://pdf.dfcfw.com/pdf/H3_AP202611091831546847_1.pdf")
        finally:
            subprocess.run = run0
            g["http"], g["PDFTOTEXT"] = http0, bin0
        ok = isinstance(out, str) and 3000 < len(out) <= SNIPPET_MAX
        return (ok, f"mode={mode} len={len(out) if isinstance(out, str) else out}")


    def _b11_a05():
        body = "沪铜上涨1.2%。下游需求回暖，库存下降。" * 300
        parts = split_research_body(body)
        joined = "".join(parts)
        same = re.sub(r"\s+", "", joined) == re.sub(r"\s+", "", body)
        return (same and len(parts) > 1
                and all(u16(esc_quote(p)) <= RESEARCH_PART_MAX for p in parts),
                f"n={len(parts)} lossless={same} u16={[u16(esc_quote(p)) for p in parts]}")


    def _b11_a06():
        bad = ("。" + "钨" * 2399) * 3
        bp = split_research_body(bad)
        ok1 = (1 < len(bp) <= 4
               and all(u16(esc_quote(p)) >= RESEARCH_PART_MIN_FILL for p in bp[:-1]))
        norm = "沪铜上涨1.2%。" * 300
        np_ = split_research_body(norm)
        ok2 = len(np_) > 1 and all(p[-1] in RESEARCH_SENT_END for p in np_)
        return (ok1 and ok2,
                f"pathological n={len(bp)} u16={[u16(esc_quote(p)) for p in bp[:3]]} "
                f"normal n={len(np_)} tails={[p[-1] for p in np_]}")


    def _b11_a07():
        dt = datetime.fromtimestamp(_B11_TS, TZ_CN)
        cases = [
            {"src": "东财研报", "ts": _B11_TS, "text": "沪铜研报｜某期货研究所 · 2026-11-04",
             "url": "https://example.com/r1.pdf", "snippet": "沪铜上涨1.2%。" * 30,
             "key": "研究:东财研报:old"},
            {"src": "中金所", "ts": _B11_TS, "text": "中金所月度数据统计｜中金所 · 2026-11",
             "url": "https://example.com/r2.pdf", "snippet": "", "key": "研究:中金所:empty"},
            {"src": "财联社", "ts": _B11_TS, "text": "沪铜库存变化1吨环比1%", "key": "k-b11-nourl"},
        ]
        bad = [it["key"] for it in cases
               if research_parts(it, dt) != [research_line(it, dt)]]
        return (not bad, f"mismatch={bad}")


    def _b11_a08():
        dt = datetime.fromtimestamp(_B11_TS, TZ_CN)
        body = "沪铜上涨1.2%。" * 750
        it = _b11_ritem(body=body)
        parts = research_parts(it, dt)
        n = len(parts)
        ok = (n > 1
              and all(f"({i}/{n})" in p for i, p in enumerate(parts, 1))
              and all("钨行业三季报点评" in p for p in parts)
              and sum("<a href" in p for p in parts) == 1
              and "<a href" in parts[-1]
              and all(u16(p) <= MSG_MAX for p in parts))
        return (ok, f"n={n} u16={[u16(p) for p in parts]}")


    def _b11_a09():
        dt = datetime.fromtimestamp(_B11_TS, TZ_CN)
        body = "钨" * 2400
        it = {"src": "东财研报", "ts": _B11_TS, "text": '"' * 699 + "｜五矿证券 · 2026-11-04",
              "url": "https://example.com/" + "a" * 980, "snippet": body, "key": "研究:东财研报:long"}
        parts = research_parts(it, dt)
        n = len(parts)
        chunks = []
        for i, p in enumerate(parts, 1):
            ls = p.split("\n")
            chunks.append(ls[-2] if i == n and len(ls) >= 2 else ls[-1])
        return (n >= 2 and all(u16(p) <= MSG_MAX for p in parts)
                and "".join(chunks) == body,
                f"n={n} u16={[u16(p) for p in parts]} body_join={''.join(chunks) == body}")


    def _b11_a10():
        n1 = _b11_norm("k-b11-n1", text="螺纹钢库存变化21吨环比21%")
        n2 = _b11_norm("k-b11-n2", text="铁矿石库存变化22吨环比22%")
        r = _b11_ritem()
        msgs = build_messages_ex([n1, r, n2])
        texts = [t for t, _u, _p in msgs]
        marked = [t for t in texts if re.search(r"\(\d+/\d+\)", t)]
        ok = (len(marked) == len(research_parts(r, datetime.fromtimestamp(_B11_TS, TZ_CN)))
              and all("\n\n" not in t for t in marked)
              and all("螺纹钢库存变化21吨环比21%" not in t
                      and "铁矿石库存变化22吨环比22%" not in t for t in marked)
              and "螺纹钢库存变化21吨环比21%" in texts[0]
              and not re.search(r"\(\d+/\d+\)", texts[0])
              and "铁矿石库存变化22吨环比22%" in texts[-1]
              and not re.search(r"\(\d+/\d+\)", texts[-1]))
        return (ok, f"msgs={len(msgs)} marked={len(marked)} u16={[u16(t) for t in texts]}")


    def _b11_a11():
        n1 = _b11_norm("k-b11-n1", text="螺纹钢库存变化21吨环比21%")
        n2 = _b11_norm("k-b11-n2", text="铁矿石库存变化22吨环比22%")
        batch = [n1, _b11_ritem(), n2]
        ex = [(t, u) for t, u, _p in build_messages_ex(batch)]
        return (build_messages(batch) == ex, f"msgs={len(ex)}")


    def _b11_a12():
        got = _b11_guard_sha()
        return (got == _B11_GUARD_SHA,
                got if got == _B11_GUARD_SHA else f"{got} != {_B11_GUARD_SHA}")


    def _b11_a13():
        it = _b11_ritem()
        dt = datetime.fromtimestamp(_B11_TS, TZ_CN)
        n = len(research_parts(it, dt))
        calls: list[int] = []

        def hook(text, clock):
            calls.append(1)
            if len(calls) == 2:
                raise urllib.error.HTTPError(
                    "https://api.telegram.org", 400, "Bad Request", {},
                    io.BytesIO(b'{"ok":false}'))

        with tempfile.TemporaryDirectory() as td:
            clk = _b11_clk()
            seen_item = _b11_norm("k-b11-seen", text="沪铜库存变化9吨环比9%")
            pend: list = []
            rc1, pay1, stt1, q1 = _b11_run(
                td, clk, _b11_src([seen_item]),
                conf_extra={"digest_min_items": 1},
                state=_b11_state({"k-b11-seen": _B11_TS}),
                queue=[it], tg_hook=hook, pending_probe=pend)
            # 第 2 轮: 退避到期, 源仍供同一(已读)条目让 main 不走「无条目」早退
            _next = int((stt1.get("_digest") or {}).get("next_try") or 0)
            _adv = max(RETRY_BASE_S + 1, _next - int(clk.t) + 1)
            clk.t += _adv
            clk.mono += _adv
            rc2, pay2, stt2, q2 = _b11_run(
                td, clk, _b11_src([seen_item]),
                conf_extra={"digest_min_items": 1}, pending_probe=pend)
            q1r = next((x for x in q1 if x.get("key") == it["key"]), {})
            ok = (rc1 == 2 and len(pay1) == 2
                  and q1r.get("parts_sent") == 1 and q1r.get("parts_total") == n
                  and it["key"] not in stt1
                  and not pend
                  and rc2 == 0 and len(pay2) == n - 1
                  and f"(2/{n})" in pay2[0] and not any("(1/" in p for p in pay2)
                  and it["key"] in stt2 and not q2)
            return (ok, f"rc={rc1}/{rc2} n={n} pay={len(pay1)}/{len(pay2)} "
                        f"q1={q1r.get('parts_sent')}/{q1r.get('parts_total')} pend={pend}")


    def _b11_a14():
        g = globals()
        old = g["RESEARCH_PARTS_PER_ROUND"]
        g["RESEARCH_PARTS_PER_ROUND"] = 2
        try:
            it = _b11_ritem(ts=_B11_TS - 200 * 60)
            other = _b11_norm("k-b11-b", ts=_B11_TS - 181 * 60)
            save_probe: list = []
            with tempfile.TemporaryDirectory() as td:
                rc, pay, _stt, q = _b11_run(
                    td, _b11_clk(), _b11_src([_b11_ping()]),
                    state=_b11_state({"k-b11-seen": _B11_TS}),
                    queue=[it, other], save_probe=save_probe)
            saved_done = [x for snap in save_probe for x in snap
                          if x.get("key") == it["key"] and x.get("parts_sent") == 2
                          and x.get("parts_total") == 3]
            ok = (rc == 0 and len(pay) == 2 and "(1/3)" in pay[0] and "(2/3)" in pay[1]
                  and not any("(3/3)" in p for p in pay)
                  and bool(saved_done)
                  and any(x.get("key") == "k-b11-b" for x in q))
            return (ok, f"rc={rc} pay={len(pay)} saved={bool(saved_done)} "
                        f"left={[x.get('key') for x in q]}")
        finally:
            g["RESEARCH_PARTS_PER_ROUND"] = old


    def _b11_a15():
        key = "研究:东财研报:partial"
        partial = _b11_ritem(key=key, ts=_B11_TS - 190 * 60)
        partial["parts_sent"] = 2
        partial["parts_total"] = 3
        with tempfile.TemporaryDirectory() as td:
            rc1, pay1, _s1, q1 = _b11_run(
                td, _b11_clk(), _b11_src([_b11_ping()]),
                conf_extra={"digest_max_items": 1},
                state=_b11_state({"k-b11-seen": _B11_TS}),
                queue=[_b11_norm("k-b11-old", ts=_B11_TS - 300 * 60), dict(partial)])
        keep1 = next((x for x in q1 if x.get("key") == key), {})
        old2 = dict(partial)
        old2["ts"] = _B11_TS - 721 * 60
        with tempfile.TemporaryDirectory() as td:
            rc2, _p2, _s2, q2 = _b11_run(
                td, _b11_clk(), _b11_src([_b11_ping()]),
                conf_extra={"digest_max_items": 1},
                state=_b11_state({"k-b11-seen": _B11_TS}),
                queue=[_b11_norm("k-b11-old", ts=_B11_TS - 800 * 60), old2])
        ok = (rc1 == 0 and len(pay1) == 1 and bool(keep1)
              and keep1.get("parts_sent") == 2 and keep1.get("parts_total") == 3
              and rc2 == 0 and not any(x.get("key") == key for x in q2))
        return (ok, f"rc={rc1}/{rc2} pay1={len(pay1)} keep1={bool(keep1)} "
                    f"q2={[x.get('key') for x in q2]}")


    def _b11_a16():
        g = globals()
        q0 = g["QUEUE"]
        good = _b11_ritem(key="研究:东财研报:rt")
        good["parts_sent"] = 1
        good["parts_total"] = 3
        good["part_fails"] = 2
        bad = _b11_ritem(key="研究:东财研报:dirty")
        bad["parts_sent"] = 5
        bad["parts_total"] = 3
        plain = _b11_norm("k-b11-plain")
        try:
            with tempfile.TemporaryDirectory() as td:
                g["QUEUE"] = Path(td) / "queue.json"
                g["QUEUE"].write_text(json.dumps({"items": [good, bad, plain], "ts": _B11_NOW}),
                                      encoding="utf-8")
                q, _p, _b = load_queue()
        finally:
            g["QUEUE"] = q0
        got_good = next((x for x in q if x.get("key") == good["key"]), {})
        got_bad = next((x for x in q if x.get("key") == bad["key"]), {})
        got_plain = next((x for x in q if x.get("key") == plain["key"]), {})
        ok = (got_good.get("parts_sent") == 1 and got_good.get("parts_total") == 3
              and got_good.get("part_fails") == 2
              and "parts_sent" not in got_bad and "parts_total" not in got_bad
              and list(got_plain) == ["src", "ts", "text", "key"])
        return (ok, f"good_ps={got_good.get('parts_sent')}/{got_good.get('parts_total')} "
                    f"good_pf={got_good.get('part_fails')} "
                    f"bad_ps={'parts_sent' in got_bad} bad_pt={'parts_total' in got_bad} "
                    f"plain_keys={list(got_plain)}")


    def _b11_a17():
        it = _b11_ritem(ts=_B11_TS - 100)
        n1 = _b11_norm("k-b11-c1", ts=_B11_TS - 50, text="螺纹钢库存变化31吨环比31%")
        n2 = _b11_norm("k-b11-c2", ts=_B11_TS - 40, text="铁矿石库存变化32吨环比32%")
        with tempfile.TemporaryDirectory() as td:
            rc, pay, stt, q = _b11_run(
                td, _b11_clk(), _b11_src([_b11_ping()]),
                conf_extra={"digest_max_items": 1},
                state=_b11_state({"k-b11-seen": _B11_TS}), queue=[it, n1, n2])
        msgs = int((stt.get("_day") or {}).get("msgs") or 0)
        ok = (rc == 0 and len(pay) == 3 and msgs == len(pay)
              and all(f"({i}/3)" in p for i, p in enumerate(pay, 1))
              and sorted(x.get("key") for x in q) == ["k-b11-c1", "k-b11-c2"])
        return (ok, f"rc={rc} pay={len(pay)} day_msgs={msgs} left={[x.get('key') for x in q]}")


    def _b11_a18():
        it = _b11_ritem()
        dirty = dict(it)
        dirty["parts_sent"] = 99
        dirty["parts_total"] = 3
        msgs = build_messages_ex([dirty])
        clamp_ok = bool(msgs) and "(3/3)" in msgs[0][0] and msgs[0][1] == [dirty]
        with tempfile.TemporaryDirectory() as td:
            rc, pay, _stt, q = _b11_run(
                td, _b11_clk(), _b11_src([_b11_ping()]),
                conf_extra={"digest_min_items": 1},
                state=_b11_state({"k-b11-seen": _B11_TS}), queue=[dirty])
        return (clamp_ok and rc == 0 and not q and len(pay) == 3,
                f"clamp={clamp_ok} rc={rc} pay={len(pay)} left={len(q)}")


    def _b11_a19():
        # B11-19 (F-01): 一轮只发 2 片且均非末片 -> sent==[] 但实发 2 条,
        # 日计数必须按实发消息条数累加 (=2), 不能因没有条目完成而归零。
        g = globals()
        old = g["RESEARCH_PARTS_PER_ROUND"]
        g["RESEARCH_PARTS_PER_ROUND"] = 2
        try:
            it = _b11_ritem(ts=_B11_TS - 200 * 60)
            with tempfile.TemporaryDirectory() as td:
                rc, pay, stt, q = _b11_run(
                    td, _b11_clk(), _b11_src([_b11_ping()]),
                    state=_b11_state({"k-b11-seen": _B11_TS}), queue=[it])
        finally:
            g["RESEARCH_PARTS_PER_ROUND"] = old
        msgs = int((stt.get("_day") or {}).get("msgs") or 0)
        q1 = next((x for x in q if x.get("key") == it["key"]), {})
        ok = (rc == 0 and len(pay) == 2 and "(1/3)" in pay[0] and "(2/3)" in pay[1]
              and not any("(3/3)" in p for p in pay)
              and msgs == 2 and q1.get("parts_sent") == 2)
        return (ok, f"rc={rc} pay={len(pay)} day_msgs={msgs} "
                    f"parts={q1.get('parts_sent')}/{q1.get('parts_total')}")




    _b11_case("B11-01 存储上限与解析上限的常量关系", _b11_a01)
    _b11_case("B11-02 PDF 页数上限 32 且真传给 pdftotext", _b11_a02)
    _b11_case("B11-03 snip_truncate 未超限逐字节 / 超限带标记且不超上限", _b11_a03)
    _b11_case("B11-04 pdf_text 全文入库 (真实东财 PDF / 离线兜底)", _b11_a04)
    _b11_case("B11-05 分片无损 + 片内预算", _b11_a05)
    _b11_case("B11-06 句末优先 + 最小填充 (防碎片)", _b11_a06)
    _b11_case("B11-07 单片路径与今日逐字节相同", _b11_a07)
    _b11_case("B11-08 多片布局契约", _b11_a08)
    _b11_case("B11-09 长标题/长 URL 强制切两片且不丢正文", _b11_a09)
    _b11_case("B11-10 分片独占消息", _b11_a10)
    _b11_case("B11-11 兼容层等价", _b11_a11)
    _b11_case("B11-12 非研报渲染守卫 (守卫型, 旧码也绿)", _b11_a12)
    _b11_case("B11-13 第 2 片失败 -> 续发且不重发首片", _b11_a13)
    _b11_case("B11-14 分片预算用尽 -> 不算失败 + 进度落盘", _b11_a14)
    _b11_case("B11-15 连载条目老化豁免 / 4x 才清", _b11_a15)
    _b11_case("B11-16 队列往返 + 非法对自愈", _b11_a16)
    _b11_case("B11-17 计数口径", _b11_a17)
    _b11_case("B11-18 越界 parts_sent 夹取", _b11_a18)

    _b11_case("B11-19 非末片轮次日计数按实发条数累加", _b11_a19)

    # ---- Batch 12: 化工现货源 (化销网 / 隆众) 全离线断言 (B12-01..B12-22) ----
    # 隆众夹具裁剪自 2026-11-09 实测页 26-1109-08-2f43b83c0ec50d13.html (纯苯早间提示):
    #   xq-head..content 开标签与 <div class="xq-content" id="content"> 内层逐字节保留,
    #   不嵌 sidebar/appDown; 原始字节 evidence/fix/fixtures/lz_benzene_20261109.html
    #   sha256 762f5e14000f3e45845953f90a7beafcd01a66783186e2d681aa6141d9918b52.
    # 化销网夹具裁剪自实测页 id=192012 (甲醇早间提示), 正文逐字节保留:
    #   evidence/fix/fixtures/hxw_192012.html
    #   sha256 73097518a3b93012ba0ee95e2ee1bd6302da65d6d14140331c281708e43297ec.
    # 隆众 PX 实测页 26-1109-08-0f5c0ed0d15a4150.html 仅作 F-02 证据 (未嵌入).
    _B12_NOW = 1794186000                    # 2026-11-09 09:00:00 +08:00
    _B12_HXW_LIST = '''<html><body><div class="main"><div class="info-list">
<input type="hidden" id="catid" value="22">
<ul>
<li><a href="/index/quote/infoshow?id=192012"><i class="v-icon v-icon-arr-left orange"></i>11月9日甲醇市场早间提示</a><span class="red" style="float:right; font-size: 14px;">2026-11-09 08:35</span></li>
<li><a href="/index/quote/infoshow?id=191987"><i class="v-icon v-icon-arr-left orange"></i>11月6日甲醇市场早间提示</a><span style="float:right; font-size: 14px;">2026-11-06 10:00</span></li>
<li><a href="/index/quote/infoshow?id=191977"><i class="v-icon v-icon-arr-left orange"></i>11月5日（16：30）太仓市场甲醇价格行情</a><span style="float:right; font-size: 14px;">2026-11-05 16:07</span></li>
<li><a href="/index/quote/infoshow?id=191895"><i class="v-icon v-icon-arr-left orange"></i>11月2日甲醇市场早间提示</a><span style="float:right; font-size: 14px;">2026-11-02 08:31</span></li>
</ul></div></div>
<div class="pages" data-url="https://www.huaxiaowang.com/index/quote/infolist?catid=22&amp;" data-total="60259" data-curr="1" data-psize="20"></div>
</body></html>'''
    _B12_HXW_DETAIL = '''<html><body><div class="w1200"><div class="article" style="width: 880px;">
<h3>11月9日甲醇市场早间提示</h3>
<p style="text-align:center; font-size: 16px;">发布时间：2026年11月09日</p>
<div class="content"><span><span style="font-size:16px;"><b>本周重点关注1、鲁北部分地炼取消前期合同MTBE减产与停工情况、宝丰4期开车情况（合成试车成功）。2、美伊战争不确定因素带来期货波动情况。</b></span></span></div>
<div style="display:flex; width:100%;"><div class="cs"><img src="https://www.huaxiaowang.com/static/images/qrcode_zx.jpg"><span>化销网资讯</span></div></div>
<p style="font-family: simhei; color:#cd3825;">扫描上方&gt;&gt;“化销网资讯”公众号及“化销网”小程序二维码，了解更多化工品资讯信息。</p>
<p class="announce" style="font-size: 14px;">特别提示：本信息由相关企业自行提供，真实性未经证实，仅供参考，请谨慎采用，风险自负。</p>
</div></div></body></html>'''
    _B12_LZ_LIST = '''<html><body><ul>
<li class="clearfix" data-id="0" data-type="" style="display: none">
<a href="https://www.oilchem.net/19-0404-19-2816873f82add672.html" target="_blank">[PA]：国内PA6切片下游开工率平稳</a>
<span style="color: #cc3338">[2026-11-09 23:24]</span>
</li>
<li class="clearfix" data-id="0" data-type="0">
<a href="https://www.oilchem.net/26-1109-08-dba07216873f8289.html"
target="_blank">石脑油市场早间提示（20261109）</a>
<span style="color: #cc3338">[2026-11-09
08:58]</span>
</li>
<li class="clearfix" data-id="0" data-type="0">
<a href="https://www.oilchem.net/26-1109-08-2f43b83c0ec50d13.html" target="_blank">[纯苯]纯苯早间提示（20261109）</a>
<span style="color: #cc3338">[2026-11-09 08:46]</span>
</li>
<li class="clearfix" data-id="0" data-type="0">
<a href="https://www.oilchem.net/26-1109-08-a1d663265432d864.html" target="_blank">环氧乙烷市场一周前瞻（20261109）</a>
<span style="color: #cc3338">[2026-11-09 08:38]</span>
</li>
<li class="clearfix" data-id="0" data-type="0">
<a href="https://www.oilchem.net/26-1109-08-0f5c0ed0d15a4150.html" target="_blank">[对二甲苯]：PX市场一周前瞻（20261109）</a>
<span style="color: #cc3338">[2026-11-09 08:36]</span>
</li>
<li class="clearfix" data-id="0" data-type="0">
<a href="https://www.oilchem.net/26-1109-08-59709cdaf4db51fe.html" target="_blank">聚丙烯粉料市场一周前瞻（20261109）</a>
<span style="color: #cc3338">[2026-11-09 08:34]</span>
</li>
<li class="clearfix" data-id="0" data-type="0">
<a href="https://www.oilchem.net/26-1109-08-5d385db5b9ddd1ea.html" target="_blank">二乙二醇市场一周前瞻（20261109）</a>
<span style="color: #cc3338">[2026-11-09 08:33]</span>
</li>
</ul></body></html>'''
    _B12_LZ_ARTICLE = r'''<html><head><meta name="lz_articleid" content="37235374" /></head><body>
<div class="xq-head">
                        <div class="xqbox-top">
                            <a href="javascript:void(0)" class="print right" title="打印本页资讯" onclick="printpage()"><i></i>打印</a> 
                            <a href="javascript:void(0)" class="fcous right" title="关注该资讯的每日内容" id="focusUnit"
                            data-link="1"><i></i>关注</a>
                            <a href="javascript:void(0)" class="collect right" title="收藏本页资讯" id="Favorite"
                            data-link="1"><i></i>收藏</a>
                            <a href="javascript:void(0)" class="share right" title="分享"><i></i>分享
                                <div class="share-abled" id="share-1" data-disabled="facebook,twitter,google,linkedin,tencent,qzone,douban"
                                    data-image="//img1.oilchem.net/info/images/logo.gif" data-source="https://www.oilchem.net/">
                                </div>
                            </a>
                        </div>
                        <h1 style="margin: 25px 0 10px;"><strong>[纯苯] 纯苯早间提示（20261109）</strong></h1>
                        <span>发布时间：2026-11-09 08:46 &nbsp; 来源：隆众资讯&nbsp; 责任编辑：宋洋</span>
                    </div>
                    <!--xqwrap-->
                    <div class="xqwrap">
                        <div class="xq-content" id="content">

                                <div id="tableScroll">
                                    <html>
 <head></head>
 <body>
  <p style="text-wrap: wrap;"><strong>一、关注点</strong></p>
  <p>1、外盘纯苯：FOB韩国涨5至1035美元/吨，CFR中国涨5至1059美元/吨。FOB鹿特丹涨11至1108美元/吨，FOB USG跌1至354美分/加仑。<span style="font-family: simsun; text-indent: 32px;"></span></p>
  <p><span style="text-indent: 32px;">2、<a target="_blank" style=" color: blue; text-decoration: underline;" href="https://oil.oilchem.net/oil/crudeoil.shtml">原油</a>：市场关注美伊双方再次冲突风险，但同时调解方仍在推动和谈，国际油价小幅上涨。ICE布油期货01合约98.32涨0.03美元/桶，环比+0.03%。</span></p>
  <table width="1798">
   <tbody>
    <tr class="firstRow" style="height: 23px;">
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249);">
      <p style="line-height: 2;"><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">区域</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-top-color: windowtext; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><strong><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">11</span></strong><strong><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">月6日</span></strong></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><strong><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">11</span></strong><strong><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">月5日</span></strong></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-top-color: windowtext; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249);">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, sans-serif;">涨跌率</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, sans-serif;">FOB韩国</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p><span style="font-family: 微软雅黑, YaHei;">1035</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p><span style="font-family: 微软雅黑, YaHei;">1030</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, sans-serif;"><span style="font-family: 微软雅黑, sans-serif; text-align: -webkit-center; text-wrap-mode: wrap; background-color: rgb(249, 249, 249);">0.49%</span></span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, sans-serif;">CFR中国</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p><span style="font-family: 微软雅黑, YaHei;">1059</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p><span style="font-family: 微软雅黑, YaHei;">1054</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family:微软雅黑, sans-serif"><span style="font-family: 微软雅黑, sans-serif; text-align: -webkit-center; text-wrap-mode: wrap; background-color: rgb(249, 249, 249);">0.47%</span></span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249);">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, sans-serif;">华东市场</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, YaHei;">8370</span><br></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, YaHei;">8450</span><br></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, YaHei;"></span><span style="font-family: 微软雅黑, sans-serif;">-0.95%</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249);">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, sans-serif;">山东市场</span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, YaHei;">8285<br></span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, YaHei;">8476<br></span></p></td>
     <td align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="line-height: 2;"><span style="font-family: 微软雅黑, YaHei;">-2.25%</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td colspan="4" align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom: none; border-left-color: windowtext; background: rgb(249, 249, 249);">
      <p style="text-align: left; line-height: 2;"><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">备注：</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td colspan="4" align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom: none; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: left; line-height: 2;"><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">1</span><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">、FOB韩国、CFR中国纯苯：美元/吨；国内纯苯：元/吨。</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td colspan="4" align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom: none; border-left-color: windowtext; background: rgb(249, 249, 249);">
      <p style="text-align: left; line-height: 2;"><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">2</span><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">、纯苯选取主流市场收盘均价。</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td colspan="4" align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom: none; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: left; line-height: 2;"><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">3</span><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">、上表人民币价格均为现汇含税自提价。</span></p></td>
    </tr>
    <tr style="height: 23px;">
     <td colspan="4" align="center" valign="middle" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: left; line-height: 2;"><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">4</span><span style="line-height: 2; font-family: 微软雅黑, sans-serif;">、涨跌率为环比变化率。</span></p></td>
    </tr>
   </tbody>
  </table>
  <p><strong>二、行情展望</strong></p>
  <p>目前美伊局势缓和可能性较强，周末山东纯苯震荡下跌，预计本周<a target="_blank" style=" color: blue; text-decoration: underline;" href="https://chem.oilchem.net/chemical/benzene.shtml">纯苯市场</a>延续偏弱震荡格局。</p>
  <table width="1798">
   <tbody>
    <tr class="firstRow" style="height: 25px;">
     <td width="31" style="padding: 0px 7px; border-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;"><strong>数据项目</strong></p></td>
     <td width="23" style="padding: 0px 7px; border-top-color: windowtext; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;"><strong>发布日期</strong></p></td>
     <td width="20" style="padding: 0px 7px; border-top-color: windowtext; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249);">
      <p style="text-align: center;"><strong>上期数据</strong></p></td>
     <td width="24" style="padding: 0px 7px; border-top-color: windowtext; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249);">
      <p style="text-align: center;"><strong>本期趋势预计</strong></p></td>
    </tr>
    <tr style="height: 25px;">
     <td width="31" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;">纯苯华东港口库存</p></td>
     <td width="23" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249);">
      <p style="text-align: center;">周一16:00PM</p></td>
     <td width="20" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;">3.2万吨</p></td>
     <td width="24" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;"><span style="font-family: 宋体, SimSun;"><span style="text-wrap: wrap;"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; background-color: rgb(249, 249, 249);"><span style="text-wrap-mode: wrap; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; text-wrap-mode: wrap; background-color: rgb(249, 249, 249);"><span style="text-wrap-mode: wrap; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; text-wrap-mode: wrap; background-color: rgb(249, 249, 249);">↘</span></span></span></span></span></span></span></span></span></span></span></span></p></td>
    </tr>
    <tr style="height: 25px;">
     <td width="31" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;">纯苯周产能利用率</p></td>
     <td width="23" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249);">
      <p style="text-align: center;">周四18:00PM</p></td>
     <td width="20" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;">72.68%</p></td>
     <td width="24" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;"><span style="font-family: 宋体, SimSun;"><span style="text-wrap: wrap;"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-wrap-mode: wrap; background-color: rgb(249, 249, 249);"><span style="text-wrap-mode: wrap; background-color: rgb(249, 249, 249);">↗</span></span></span></span></span></span></span></span></span></p></td>
    </tr>
    <tr style="height: 25px;">
     <td width="31" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249);">
      <p style="text-align: center;">纯苯周利润</p></td>
     <td width="23" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249);">
      <p style="text-align: center;">周四18:00PM</p></td>
     <td width="20" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;">788元/吨</p></td>
     <td width="24" style="padding: 0px 7px; border-top: none; border-right-color: windowtext; border-bottom-color: windowtext; border-left: none; background: rgb(249, 249, 249); word-break: break-all;">
      <p style="text-align: center;"><span style="font-family: 宋体, SimSun;"><span style="text-wrap: wrap;"><span style="background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-align: center; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; background-color: rgb(249, 249, 249);"><span style="text-wrap-mode: wrap; background-color: rgb(249, 249, 249);"><span style="text-wrap-mode: wrap; background-color: rgb(249, 249, 249);"><span style="font-family: 宋体, SimSun; text-wrap-mode: wrap; background-color: rgb(249, 249, 249);">↘</span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></span></p></td>
    </tr>
    <tr style="height: 25px;">
     <td width="100" colspan="4" style="padding: 0px 7px; border-top: none; border-right-color: black; border-bottom: none; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p>1、↓↑视为大幅波动，突出涨跌幅超过3%的数据维度</p></td>
    </tr>
    <tr style="height: 25px;">
     <td width="100" colspan="4" style="padding: 0px 7px; border-top: none; border-right-color: black; border-bottom-color: windowtext; border-left-color: windowtext; background: rgb(249, 249, 249); word-break: break-all;">
      <p>2、↗<span style="font-family: 宋体, SimSun;">↘</span>视为窄幅波动，突出涨跌幅在0-3%以内的数据</p></td>
    </tr>
   </tbody>
  </table>
  <p><br></p>
  <p><br></p>
 </body>
</html>
                                </div>
                                <script>
                                    (function () {
                                        const TESTBOX = document.getElementById('tableScroll')
                                        function getTable(NOWDOM) {
                                            $(NOWDOM).children().each((i, v) => {
                                                // console.log(v.tagName)
                                                if (v.tagName == 'TABLE') {
                                                    $(v).wrap('<div class="tableOuterScorll"></div>')
                                                } else {
                                                    getTable(v);
                                                }
                                            })
                                        }
                                        getTable(TESTBOX)
                                    })()
                                </script>
                        </div></div>
</body></html>'''

    class _B12Clk:
        def __init__(self, t):
            self.t = float(t)
            self.mono = 1000.0

        def time(self):
            return self.t

        def monotonic(self):
            return self.mono

        def sleep(self, s):
            self.t += s
            self.mono += s

    def _b12_case(name, fn):
        try:
            ok, detail = fn()
        except Exception as e:                       # noqa: BLE001 - 旧码缺符号 -> 本条红
            ok, detail = False, f"{type(e).__name__}: {e}"
        check(name, bool(ok), str(detail))

    def _b12_call(router, fn, t0=None):
        """打桩 http/time 后跑 fn(); 返回 (结果, [(url, tries)]) —— 全程离线."""
        g = globals()
        old_http, old_time = g["http"], g["time"]
        calls = []

        def _fake(url, referer=None, tries=3, deadline=None, **_kw):
            calls.append((url, tries))
            r = router(url)
            if isinstance(r, BaseException):
                raise r
            return r

        g["http"] = _fake
        g["time"] = _B12Clk(_B12_NOW if t0 is None else t0)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                got = fn()
        finally:
            g["http"], g["time"] = old_http, old_time
        return got, calls

    def _b12_router(url):
        if "infolist?catid=22" in url:
            return _B12_HXW_LIST.encode()
        if "infoshow?id=192012" in url:
            return _B12_HXW_DETAIL.encode()
        if "infoshow" in url:
            return RuntimeError("HTTP 403")
        if "list.oilchem.net/1/9/" in url:
            return _B12_LZ_LIST.encode()
        if "2f43b83c0ec50d13" in url:
            return _B12_LZ_ARTICLE.encode()
        return RuntimeError("HTTP 404")

    _B12_LEGACY = ["财联社", "东财", "同花顺", "新浪", "见闻商品", "SMM",
                   "Mining", "OilPrice", "MiningTech", "Nasdaq", "CNBC", "MarketWatch",
                   "Investing", "Hellenic", "东财研报", "中金所", "中期协"]

    def _b12_mk(name, err):
        def _f(deadline=None):
            if err:
                raise RuntimeError(err)
            return []
        return (name, _f)

    def _b12_a01():
        g = globals()
        need = ("src_hxw", "src_lz", "hxw_rows", "hxw_detail", "lz_rows", "lz_detail",
                "chem_candidates", "chem_admit", "chem_text", "chem_flatten",
                "slice_remaining", "html_inner", "strip_embed")
        miss = [n for n in need if not callable(g.get(n))]
        got = (g.get("CHEM_SRCS"), g.get("HXW_LIST_URL"), g.get("LZ_LIST_URL"),
               g.get("CHEM_DETAIL_MAX"), g.get("CHEM_TEXT_MAX"))
        ok = (not miss and got == (("化销网", "隆众", "我的钢铁", "东财甲醇"),
                                   "https://www.huaxiaowang.com/index/quote/infolist?catid=22",
                                   "https://list.oilchem.net/1/9/", 4, ITEM_MAX))
        return ok, f"miss={miss} got={got!r}"

    def _b12_a02():
        names = [n for n, _ in SOURCES]
        ok = (len(names) == 21 and names[:14] == _B12_LEGACY[:14]
              and names[14:18] == ["化销网", "隆众", "我的钢铁", "东财甲醇"]
              and names[18:] == ["东财研报", "中金所", "中期协"])
        return ok, f"n={len(names)} {names}"

    def _b12_a03():
        rows = hxw_rows(_B12_HXW_LIST)
        want0 = {"id": "192012", "title": "11月9日甲醇市场早间提示", "ts": 1794184500,
                 "url": "https://www.huaxiaowang.com/index/quote/infoshow?id=192012"}
        ok = (len(rows) == 4 and rows[0] == want0
              and [r["id"] for r in rows] == ["192012", "191987", "191977", "191895"]
              and [r["ts"] for r in rows] == [1794184500, 1793930400, 1793866020, 1793579460])
        return ok, f"n={len(rows)} rows={rows[:2]}"

    def _b12_a04():
        bad = ("<html><body>无列表</body></html>",
               '<html><body><div class="info-list"><ul></ul></div></body></html>',
               '<html><body><div class="info-list"><ul><li><a href="/index/quote/infoshow?id=1">'
               '<i></i>甲醇装置停车</a><span>昨天 09:00</span></li></ul></div></body></html>')
        got = []
        for page in bad:
            try:
                hxw_rows(page)
                got.append("no-raise")
            except RuntimeError as e:
                got.append("结构变化" in str(e))
        return all(x is True for x in got), f"{got}"

    def _b12_a05():
        d = hxw_detail(_B12_HXW_DETAIL)
        want = ("本周重点关注1、鲁北部分地炼取消前期合同MTBE减产与停工情况、"
                "宝丰4期开车情况（合成试车成功）。2、美伊战争不确定因素带来期货波动情况。")
        ok = (isinstance(d, dict) and d["title"] == "11月9日甲醇市场早间提示"
              and d["body"] == want and d["date_ts"] == 1794153600
              and "特别提示" not in d["body"] and "二维码" not in d["body"]
              and "扫描上方" not in d["body"])
        return ok, f"{d}"

    def _b12_a06():
        got = (hxw_detail("<html><body><h3>t</h3></body></html>"),
               hxw_detail('<html><body><div class="content">无标题</div></body></html>'))
        return got == (None, None), f"{got}"

    def _b12_a07():
        ids = [r["id"] for r in chem_candidates(hxw_rows(_B12_HXW_LIST))]
        return ids == ["192012", "191987", "191895"], f"ids={ids}"

    def _b12_a08():
        rows = lz_rows(_B12_LZ_LIST)
        deg = [r["title"] for r in rows if "二乙二醇" in r["title"]]
        cands = [r["title"] for r in chem_candidates(rows)]
        ok = (len(deg) == 1 and not any("二乙二醇" in t for t in cands))
        return ok, f"deg={deg} cands={cands}"

    def _b12_a09():
        rows = lz_rows(_B12_LZ_LIST)
        ok = (len(rows) == 6
              and rows[0]["id"] == "26-1109-08-dba07216873f8289"
              and rows[0]["ts"] == 1794185880
              and rows[1]["title"] == "[纯苯]纯苯早间提示（20261109）"
              and rows[1]["ts"] == 1794185160
              and not any("PA6" in r["title"] for r in rows))
        return ok, f"n={len(rows)} first={rows[0] if rows else None}"

    def _b12_a10():
        d = lz_detail(_B12_LZ_ARTICLE)
        ok = (isinstance(d, dict) and d["title"] == "[纯苯] 纯苯早间提示（20261109）"
              and d["ts"] == 1794185160
              and hashlib.sha256(chem_flatten(d["body"]).encode()).hexdigest()
                  == "b1047ece7fe0212f868bf50230127099e4140564e993f0476ec60d4798b44ffe"
              and d["body"].startswith("一、关注点")
              and "getTable" not in d["body"] and "TESTBOX" not in d["body"]
              and "免责声明" not in d["body"])
        return ok, f"{d}"

    def _b12_a11():
        cands = [r["title"] for r in chem_candidates(lz_rows(_B12_LZ_LIST))]
        ok = cands == ["[纯苯]纯苯早间提示（20261109）",
                       "[对二甲苯]：PX市场一周前瞻（20261109）",
                       "聚丙烯粉料市场一周前瞻（20261109）"]
        return ok, f"{cands}"

    def _b12_a12():
        d = hxw_detail(_B12_HXW_DETAIL)
        z = lz_detail(_B12_LZ_ARTICLE)
        hx = chem_admit(d["title"], d["body"])
        lz = chem_admit(z["title"], z["body"]) if z else True
        ok = hx and not lz                 # 真实纯苯早间提示正文无事件词 -> 不准入
        return ok, f"hxw={hx} lz={lz}"

    def _b12_a13():
        # CTO 覆盖 §4.4: 单提甲醇下游放行 + 「甲醇下游·」前缀幂等 + 已命中甲醇的条目不前缀
        mtbe = chem_admit("MTBE市场早间提示", "鲁北MTBE装置停车，甲醛需求走弱。")
        meth = chem_admit("甲醇市场早间提示", "鲁北MTBE装置停车。")
        quiet = chem_admit("甲醇市场早间提示", "今日甲醇价格稳定。")
        t_spot = chem_text("MTBE市场早间提示", "鲁北MTBE装置停车，甲醛需求走弱。")
        t_again = chem_text(t_spot, "")
        t_meth = chem_text("11月9日甲醇市场早间提示", "鲁北MTBE减产与停工情况。")
        ok = (mtbe and meth and (not quiet)
              and t_spot.startswith(SPOT_PREFIX + "MTBE市场早间提示：鲁北MTBE装置停车")
              and hit(t_spot, STRONG)                       # 全局品种门因此看到「甲醇」
              and t_again == t_spot                         # 幂等: 前缀不二次叠加
              and t_meth.startswith("11月9日甲醇市场早间提示：")
              and SPOT_PREFIX not in t_meth)                # 已命中甲醇 -> 不加前缀
        return (ok, f"mtbe={mtbe} meth={meth} no_event={quiet} "
                    f"spot={t_spot[:28]!r} again={t_again == t_spot} meth_pfx={SPOT_PREFIX in t_meth}")

    def _b12_a14():
        d = hxw_detail(_B12_HXW_DETAIL)
        short = chem_text(d["title"], d["body"])
        long_body = ("一、关注点 " + "价格数据 " * 150
                     + "3、山东某甲醇装置停车检修，涉及产能60万吨。")
        long_text = chem_text("11月9日甲醇市场早间提示", long_body)
        ok = (short.startswith("11月9日甲醇市场早间提示：") and len(short) < 200
              and len(long_text) <= CHEM_TEXT_MAX
              and long_text.startswith("11月9日甲醇市场早间提示：")
              and hit(long_text, CHEM_EVENT) and "…" in long_text)
        return ok, f"short={len(short)} long={len(long_text)}"

    def _b12_a15():
        lis = "".join(
            '<li><a href="/index/quote/infoshow?id=1920%02d"><i class="v-icon"></i>'
            "11月9日甲醇装置停车提示%d</a><span>2026-11-09 08:3%d</span></li>" % (i, i, i)
            for i in range(5))
        page = '<html><body><div class="info-list"><ul>%s</ul></div></body></html>' % lis

        def _router(url):
            if "infolist" in url:
                return page.encode()
            if "infoshow" in url:
                return _B12_HXW_DETAIL.encode()
            return RuntimeError("404")

        items, calls = _b12_call(_router, lambda: src_hxw(None))
        ids = [u.rsplit("id=", 1)[-1] for u, _t in calls if "infoshow" in u]
        lists = [c for c in calls if "infolist" in c[0]]
        cats = [int(c[0].rsplit("=", 1)[-1]) for c in lists]
        list_tries = {int(c[0].rsplit("=", 1)[-1]): c[1] for c in lists}
        # B13-F04 后 22 不再第一个抓: 类目 22 的 tries=2 语义按**类目名**校验 (不是位置)
        ok = (len(calls) == 11 and cats == list(HXW_CATS)
              and list_tries == {13: 1, 14: 1, 22: 2, 23: 1, 21: 1, 20: 1}
              and all(c[1] == 1 for c in calls if "infoshow" in c[0])
              and ids == ["192000", "192001", "192002", "192003", "192004"]
              and len(items) == 5)
        return ok, f"calls={calls} cats={cats} ids={ids} items={len(items)}"

    def _b12_a16():
        def _li(i, ts):
            return ('<li><a href="/index/quote/infoshow?id=1930%02d"><i></i>'
                    "11月9日甲醇装置停车提示%d</a><span>%s</span></li>" % (i, i, ts))

        page = ('<html><body><div class="info-list"><ul>'
                + _li(0, "2026-11-09 08:30") + _li(1, "2026-11-09 04:00")
                + _li(2, "2026-11-09 03:00") + _li(3, "2026-11-09 08:35")
                + "</ul></div></body></html>")

        def _router(url):
            if "infolist" in url:
                return page.encode()
            if "infoshow" in url:
                return _B12_HXW_DETAIL.encode()
            return RuntimeError("404")

        items, calls = _b12_call(_router, lambda: src_hxw(None))
        ids = [u.rsplit("id=", 1)[-1] for u, _t in calls if "infoshow" in u]
        ok = (ids == ["193000", "193003"] and len(items) == 2)
        return ok, f"ids={ids} items={len(items)}"

    def _b12_a17():
        got = [slice_remaining(i, 21) for i in range(21)]
        want = [17 - i for i in range(14)] + [4, 4, 4, 4] + [3, 2, 1]
        return (got == want and slice_remaining(0, 2) == 1), f"got={got}"

    def _b12_a18():
        srcs = ([_b12_mk(n, "boom") for n in _B12_LEGACY]
                + [_b12_mk("化销网", None), _b12_mk("隆众", None)])
        with tempfile.TemporaryDirectory() as td:
            rc, pays, _st, _q = _b11_run(td, _b11_clk(), srcs, state={"seedmarker": 1})
        ok = (rc == 1 and len(pays) == 1 and "全部抓取失败" in pays[0]
              and "化销网" not in pays[0] and "隆众" not in pays[0])
        return ok, f"rc={rc} pays={len(pays)} head={(pays[0][:24] if pays else '')}"

    def _b12_a19():
        srcs = ([_b12_mk(n, None) for n in _B12_LEGACY]
                + [_b12_mk("化销网", "boom"), _b12_mk("隆众", "boom")])
        with tempfile.TemporaryDirectory() as td:
            rc, pays, st, _q = _b11_run(td, _b11_clk(), srcs, state={"seedmarker": 1})
        ok = (rc == 0 and not pays and "_alert" not in st)
        return ok, f"rc={rc} pays={len(pays)} alert={'_alert' in st}"

    def _b12_a20():
        hx, c1 = _b12_call(_b12_router, lambda: src_hxw(None))
        lz, c2 = _b12_call(_b12_router, lambda: src_lz(None))
        hx2, _c = _b12_call(_b12_router, lambda: src_hxw(None))
        arts = [u for u, _t in c2 if "/26-" in u]
        ok = (len(hx) == 1 and not lz
              and hx[0]["src"] == "化销网" and hx[0]["ts"] == 1794184500
              and hx[0]["text"].startswith("11月9日甲醇市场早间提示：")
              and len(arts) == 3                 # 纯苯/PX/聚丙烯 3 个候选都拉了详情
              and hx == hx2 and dedupe_key(hx[0]["text"]) == dedupe_key(hx2[0]["text"]))
        return ok, f"hx={len(hx)} lz={len(lz)} arts={len(arts)} c1={len(c1)} c2={len(c2)}"

    def _b12_a21():
        """F-01: 隆众夹具必须是实测页正文; 无事件词的真实纯苯早间提示不准入."""
        raw_fix = _B12_LZ_ARTICLE
        structure = ('class="xq-content" id="content"' in raw_fix
                     and "<html>" in raw_fix and "<head></head>" in raw_fix
                     and "<body>" in raw_fix and "getTable(TESTBOX)" in raw_fix)
        synthetic = ("华东某纯苯装置计划11月10日起停车检修" in raw_fix
                     or "纯苯港口库存下降，下游刚需采购" in raw_fix)
        d = lz_detail(raw_fix)
        flat = chem_flatten(d["body"]) if d else ""
        hx = hxw_detail(_B12_HXW_DETAIL)
        ok = (structure and not synthetic and d is not None
              and flat.startswith("一、关注点 1、外盘纯苯：FOB韩国涨5至1035美元/吨")
              and hashlib.sha256(flat.encode()).hexdigest()
                  == "b1047ece7fe0212f868bf50230127099e4140564e993f0476ec60d4798b44ffe"
              and not any(w in flat for w in CHEM_EVENT)       # 真实页确无事件词
              and not chem_admit("[纯苯]纯苯早间提示（20261109）", d["body"])
              and chem_admit(hx["title"], hx["body"]))         # 真实甲醇页(有事件词)仍准入
        return (ok, f"structure={structure} synthetic={synthetic} "
                    f"flat_sha={hashlib.sha256(flat.encode()).hexdigest()[:12]} "
                    f"lz={chem_admit('[纯苯]纯苯早间提示（20261109）', d['body']) if d else 'None'} "
                    f"hxw={chem_admit(hx['title'], hx['body']) if hx else 'None'}")

    def _b12_a22():
        """F-02: 开工率/开工负荷/开工水平/开工N% 是统计量, 不算事件词; 开工装置/时间仍算."""
        px_title = "[对二甲苯]：PX市场一周前瞻（20261109）"
        px_body = ("一、关注点 1、PX收于1202美元/吨，+17美元/吨（CFR中国）。"
                   " 2、PX开工83.8%。 3、PTA产能利用率在70.20%。"
                   " 4、聚酯综合产能利用率在74.69%附近。")
        rate = chem_admit("甲醇市场早间提示", "本周国内甲醇开工率提升至80.5%，供应宽松。")
        load = chem_admit("甲醇市场早间提示", "甲醇开工负荷下调至七成。")
        level = chem_admit("甲醇市场早间提示", "甲醇开工水平维持高位。")
        px = chem_admit(px_title, px_body)
        dev = chem_admit("甲醇市场早间提示", "山东某甲醇装置开工时间确定，计划11月12日投料。")
        dev2 = chem_admit("甲醇市场早间提示", "新增甲醇开工装置一套，涉及产能60万吨。")
        dev3 = chem_admit("甲醇市场早间提示", "本周甲醇开工2套新产能，涉及产能120万吨。")
        page = ('<html><body><div class="info-list"><ul>'
                '<li><a href="/index/quote/infoshow?id=194001"><i></i>'
                '11月9日甲醇市场早间提示</a><span>2026-11-09 08:30</span></li>'
                '<li><a href="/index/quote/infoshow?id=194002"><i></i>'
                '11月9日甲醇市场早间提示</a><span>2026-11-09 08:31</span></li>'
                '</ul></div></body></html>')
        bodies = {
            "194001": '<html><body><h3>11月9日甲醇市场早间提示</h3>'
                      '<div class="content">本周国内甲醇开工率提升至80.5%，供应宽松。</div>'
                      '</body></html>',
            "194002": '<html><body><h3>11月9日甲醇市场早间提示</h3>'
                      '<div class="content">山东某甲醇装置开工时间确定，计划11月12日投料。</div>'
                      '</body></html>',
        }

        def _router(url):
            if "infolist" in url:
                return page.encode()
            if "infoshow" in url:
                return bodies[url.rsplit("id=", 1)[-1]].encode()
            return RuntimeError("404")

        items, _calls = _b12_call(_router, lambda: src_hxw(None))
        ok = (not rate and not load and not level and not px and dev and dev2 and dev3
              and len(items) == 1 and "开工时间" in items[0]["text"])
        return (ok, f"rate={rate} load={load} level={level} px={px} "
                    f"dev={dev} dev2={dev2} dev3={dev3} items={len(items)}")

    _b12_case("B12-01 常量/入口齐全且 URL 精确", _b12_a01)
    _b12_case("B12-02 SOURCES 19 源与插入位置", _b12_a02)
    _b12_case("B12-03 化销网列表解析 (id/标题/时间/绝对 URL)", _b12_a03)
    _b12_case("B12-04 化销网列表三态结构哨兵", _b12_a04)
    _b12_case("B12-05 化销网详情正文与发布日期", _b12_a05)
    _b12_case("B12-06 化销网详情缺结构 -> None", _b12_a06)
    _b12_case("B12-07 化销网列表级准入 (价格行情不进候选)", _b12_a07)
    _b12_case("B12-08 二乙二醇不被「乙二醇」子串误命中", _b12_a08)
    _b12_case("B12-09 隆众列表解析 (隐藏模板跳过)", _b12_a09)
    _b12_case("B12-10 隆众详情解析 (script 不入正文)", _b12_a10)
    _b12_case("B12-11 隆众列表级准入 (3 个品种候选)", _b12_a11)
    _b12_case("B12-12 正文级准入 keep (目标事实 / 纯苯)", _b12_a12)
    _b12_case("B12-13 甲醇下游放行 + 「甲醇下游·」前缀幂等", _b12_a13)
    _b12_case("B12-14 文本构造 (短正文逐字节 / 长正文含事件词)", _b12_a14)
    _b12_case("B12-15 详情上限 4 + 列表/详情 tries", _b12_a15)
    _b12_case("B12-16 详情年龄闸门 (超 4h 不拉详情)", _b12_a16)
    _b12_case("B12-17 份额分母不变 (既有源 17-i / 研报 3,2,1)", _b12_a17)
    _b12_case("B12-18 全源失败判定只看老源 (17 失败 + 2 空健康)", _b12_a18)
    _b12_case("B12-19 新源失败不触发全源告警", _b12_a19)
    _b12_case("B12-20 端到端 (打桩 http, 真实页不误准入, 两次一致)", _b12_a20)
    _b12_case("B12-21 真实隆众夹具: 纯苯早间提示(无事件词) 不准入 / 甲醇准入", _b12_a21)
    _b12_case("B12-22 开工率/负荷/水平不算事件词 (开工装置/时间仍入队)", _b12_a22)

    # ---- Batch 13 化工源扩容: 离线夹具 (全部取自 2026-11-10 实测页, 只裁掉页面外壳)
    _B13_HXW_22 = '''<html><body><div class="main"><div class="info-list">
<input type="hidden" id="catid" value="22">
<ul>
<li><a href="/index/quote/infoshow?id=192012"><i class="v-icon v-icon-arr-left orange"></i>11月9日甲醇市场早间提示</a><span  style="float:right; font-size: 14px;">2026-11-09 08:35</span></li>
<li><a href="/index/quote/infoshow?id=191987"><i class="v-icon v-icon-arr-left orange"></i>11月6日甲醇市场早间提示</a><span  style="float:right; font-size: 14px;">2026-11-06 10:00</span></li>
</ul></div></div></body></html>'''

    _B13_HXW_13 = '''<html><body><div class="main"><div class="info-list">
<input type="hidden" id="catid" value="13">
<ul>
<li><a href="/index/quote/infoshow?id=192048"><i class="v-icon v-icon-arr-left orange"></i>11月10日山西骏捷（孝义金晖）甲醇装置动态</a><span class="red" style="float:right; font-size: 14px;">2026-11-10 09:47</span></li>
<li><a href="/index/quote/infoshow?id=192052"><i class="v-icon v-icon-arr-left orange"></i>11月10日榆林兖矿甲醇竞拍情况</a><span class="red" style="float:right; font-size: 14px;">2026-11-10 10:24</span></li>
</ul></div></div></body></html>'''

    _B13_HXW_14 = '''<html><body><div class="main"><div class="info-list">
<input type="hidden" id="catid" value="14">
<ul>
<li><a href="/index/quote/infoshow?id=192042"><i class="v-icon v-icon-arr-left orange"></i>11月10日联泓新科MTO装置快讯</a><span class="red" style="float:right; font-size: 14px;">2026-11-10 09:32</span></li>
<li><a href="/index/quote/infoshow?id=192047"><i class="v-icon v-icon-arr-left orange"></i>11月10日山东利华益MTBE价格大幅下调</a><span class="red" style="float:right; font-size: 14px;">2026-11-10 09:41</span></li>
</ul></div></div></body></html>'''

    _B13_HXW_DUP = '''<html><body><div class="info-list">
<input type="hidden" id="catid" value="13">
<ul>
<li><a href="/index/quote/infoshow?id=192012"><i class="v-icon v-icon-arr-left orange"></i>11月9日甲醇市场早间提示</a><span  style="float:right; font-size: 14px;">2026-11-09 08:35</span></li>
</ul></div></body></html>'''

    _B13_MY_LIST = '''<html><body><div class="column-box"><ul class="column-list">
<li >
                                <span >[11-09]</span>
                                <a href="https://nenghua.mysteel.com/a/26110916/3516641DEBFE8695.html" title="[损失量]：中国甲醇装置检修数据统计（20261109）"
                                    target="_blank">[损失量]：中国甲醇装置检修数据统计（20261109）</a>
                            </li>
<li >
                                <span >[11-09]</span>
                                <a href="https://nenghua.mysteel.com/a/26110917/927617D8BF01CCD2.html" title="内蒙古远兴能源甲醇装置动态"
                                    target="_blank">内蒙古远兴能源甲醇装置动态</a>
                            </li>
<li >
                                <span >[11-09]</span>
                                <a href="https://nenghua.mysteel.com/a/26110908/D8D109DD1A4AC394.html" title="[甲醛]：甲醛市场一周前瞻（20261109）"
                                    target="_blank">[甲醛]：甲醛市场一周前瞻（20261109）</a>
                            </li>
<li >
                                <span >[11-09]</span>
                                <a href="https://nenghua.mysteel.com/a/26110911/AB66DA3241EAB3EE.html" title="联泓MTO装置快讯"
                                    target="_blank">联泓MTO装置快讯</a>
                            </li>
<li >
                                <span >[11-09]</span>
                                <a href="https://nenghua.mysteel.com/a/26110917/84E148AC0D673637.html" title="大唐多伦甲醇及MTP装置快讯"
                                    target="_blank">大唐多伦甲醇及MTP装置快讯</a>
                                </li>
<li >
                                <span >[10-18]</span>
                                <a href="https://nenghua.mysteel.com/a/26101818/FA6758BEC192890E.html" title="​[隆众聚焦]：沿海烯烃装置罕见全线停车 11月甲醇市场供需矛盾或难逆转"
                                    target="_blank">​[隆众聚焦]：沿海烯烃装置罕见全线停车 11月甲醇市场供需矛盾或难逆转</a>
                            </li>
</ul></div></body></html>'''

    _B13_MY_LEAD = '''<html><head><meta name="publish" content="published at 2026-11-03 20:02:29 from unknown" /></head><body><div class="content-text" id="article-content">
    <!--ai摘要-->
    


    
                            1.今日关注点①、内蒙古久泰（托县）甲醇今日出厂新价维持在3345元/吨，出货待观察。②、宁夏畅亿60万吨煤制甲醇装置于11月1日开始检修，预计检修时间20天。2.今日行情及后市预测表1 国内甲醇及相关产品价格汇总（单位：元/吨）标签11月311月2涨跌环比价格类型重点区域现货价格指数34823510-28-0.80%市场价山东东营365036500.000…
                        <script src="//a.mysteelcdn.com/common/register/v1/skin/js/md5.js?v=202210201930" charset="gbk"></script>
<script src="//a.mysteelcdn.com/libs/load.min.js" charset="utf-8"></script>
<script src="//a.mysteelcdn.com/libs/qrcode.min.js" charset="gbk"></script>
<script src="//a.mysteelcdn.com/common/login/scan_login/pc/js/login-detail.js?v=20260306" charset="utf-8"></script>
        <div class="editor">
                                    资讯编辑：苏金慧 0533-7027131
 </br>
                        资讯监督：崔志明 0533-7026065
 </br>
 </div></body></html>'''

    _B13_MY_EMPTY = '''<html><head><meta name="publish" content="published at 2026-11-09 16:21:53 from unknown" /></head><body><div class="content-text" id="article-content">
    <!--ai摘要-->
    


    
                                    <script src="//a.mysteelcdn.com/common/register/v1/skin/js/md5.js?v=202210201930" charset="gbk"></script>
<script src="//a.mysteelcdn.com/libs/load.min.js" charset="utf-8"></script>
<script src="//a.mysteelcdn.com/libs/qrcode.min.js" charset="gbk"></script>
<script src="//a.mysteelcdn.com/common/login/scan_login/pc/js/login-detail.js?v=20260306" charset="utf-8"></script>
        <div class="editor">
                                    资讯编辑：左军 0533-7026063
 </br>
                        资讯监督：崔志明 0533-7026065
 </br>
  </div></body></html>'''

    _B13_MY_BOILER = '''<html><head><meta name="publish" content="published at 2026-11-09 11:06:28 from unknown" /></head><body><div class="content-text" id="article-content">
    <!--ai摘要-->
    


    
                            企业简介：联泓新材料科技股份有限公司，是联想控股成员企业。36万吨/年DMTO装置—位于山东省滕州市鲁南科技化工园区，配套92万吨/年甲醇装置，部分甲醇外采。主要产品为20万吨/年聚丙烯、17万吨/年乙烯、6万吨/年C4、C5等。厂家其主要采用直销和代理相结合的方式销售PP，PP销售区域包括：山东、江苏、浙江等省。投产时间：2014年11月原料库容：4万装置…
                        <script src="//a.mysteelcdn.com/common/register/v1/skin/js/md5.js?v=202210201930" charset="gbk"></script>
<script src="//a.mysteelcdn.com/libs/load.min.js" charset="utf-8"></script>
<script src="//a.mysteelcdn.com/libs/qrcode.min.js" charset="gbk"></script>
<script src="//a.mysteelcdn.com/common/login/scan_login/pc/js/login-detail.js?v=20260306" charset="utf-8"></script>
        <div class="editor">
                                    资讯编辑：崔志明 0533-7026065
 </br>
                        资讯监督：崔志明 0533-7026065
 </br>
 </div></body></html>'''

    _B13_EM_LIST = '''<html><body><div id="futureImportentNews1" class="contentList2"><ul><li><a href="http://futures.eastmoney.com/a/202611103926942138.html" target="_blank" title="能化 | 甲醇：港口库存的绝对水平偏低 短期行情跟随局势变化">能化 | 甲醇：港口库存的绝对水平偏低 短期行情跟...</a></li>
<li><a href="http://futures.eastmoney.com/a/202611103926982887.html" target="_blank" title="能化 | 市场交易中东局势缓和 高硫供应忧虑预计缓解">能化 | 市场交易中东局势缓和 高硫供应忧虑预计缓...</a></li>
<li><a href="http://futures.eastmoney.com/a/202611093926259150.html" target="_blank" title="金联创：美伊局势仍是核心因素 11月初甲醇高位回落转震荡">金联创：美伊局势仍是核心因素 11月初甲醇高位回...</a></li>
<li><a href="http://futures.eastmoney.com/a/202611063925092388.html" target="_blank" title="甲醇主力合约日内涨幅扩大至2.00%">甲醇主力合约日内涨幅扩大至2.00%</a></li>
<li><a href="http://futures.eastmoney.com/a/202611093926288414.html" target="_blank" title="每日龙虎榜 | 商品期货综合指数上涨0.05% 日内资金净流出0.84亿元">每日龙虎榜 | 商品期货综合指数上涨0.05% 日内资...</a></li></ul></div><div id="futureImportentNews" class="contentList2"><ul><li><a href="http://futures.eastmoney.com/a/202611103926942138.html" target="_blank" title="能化 | 甲醇：港口库存的绝对水平偏低 短期行情跟随局势变化">能化 | 甲醇：港口库存的绝对水平偏低 短期行情跟...</a></li>
<li><a href="http://futures.eastmoney.com/a/202611093926259150.html" target="_blank" title="金联创：美伊局势仍是核心因素 11月初甲醇高位回落转震荡">金联创：美伊局势仍是核心因素 11月初甲醇高位回...</a></li></ul></div></body></html>'''

    _B13_EM_DETAIL = '''<html><body><div id="topbox"><div class="infos"><div class=" item">2026年11月09日 16:04</div><div class="item">来源：
金联创</div></div></div>                  <div class="txtinfos" id="ContentBody" style="margin-top:0;">
                            
                                <!--文章主体-->
<p>　　金联创分析指出，11月第一周，甲醇结束了连续数月的强势，呈现高位回落后震荡态势。影响月初价格呈现高位回落的因素主要在于外部宏观消息，美国中期选举民主党拿下国会两院，市场押注伊朗冲突更快结束，此前因霍尔木兹海峡通航低迷而积累的地缘溢价被迅速集中挤出。当前市场更大的压力仍来自需求，尤其<span id="bk_90.BK1481"><a  target="_blank" href="http://quote.eastmoney.com/unify/r/90.BK1481" class="keytip" data-code="90,BK1481">港口</a></span><span id="bkquote_90.BK1481"></span>MTO需求负反馈。</p><p>　　后续来看，美伊局势仍是核心因素，虽中期选举民主党重回国会，但谈判与军事风险仍可能反复；甲醇基本面多空因素并存，短线震荡为主。逻辑上，短期地缘缓和预期仍在发酵，关注进口波动、海峡通航及MTO恢复情况；但目前库存同比少七成、基差近500元、伊朗冬季限气在即，一旦海峡通行继续受阻，12月后的进口缺口会重新成为交易主线，低库存与高基差可能推动盘面二次走强。最大的风险是海峡解封后伊朗浮仓与海峡内胀库货源集中释放，从解禁到到货约需4-6周，若与宝丰新装置出料叠加，远月承受的压力将明显重于近月。</p><p>　　（本内容及观点仅供参考，不构成任何投资建议）</p><p class="em_media">（文章来源：金联创）</p>                            <!-- 正文中部 内嵌广告 -->
                            <div class="ad_context3" id="ad_context3"></div></div>
                        <!-- 文尾部其它信息 --><div class="zwothers"><div class="sourcebox"><span>文章来源：金联创</span></div></div></div></body></html>'''

    # B13-F01: 多品种日报 (真实页 202611103926942138 的裁剪版; 12 个【xx】分节, 甲醇在第 5 节)
    _B13_EM_MULTI = '''<html><body><div id="topbox"><div class="infos"><div class=" item">2026年11月10日 09:16</div><div class="item">来源：
多品种日报</div></div></div>                  <div class="txtinfos" id="ContentBody" style="margin-top:0;">
<p>【原油】 昨夜消息面密集释放美伊局势降温信号，国际油价大跌6%，Brent跌至92美元/桶，地缘溢价快速挤出。</p><p>【燃料油】 高硫供应忧虑预计缓解，燃料油裂解价差回落，新加坡库存回升。</p><p>【沥青】 沥青需求进入淡季，炼厂出货一般，现货价格持稳。</p><p>【液化石油气】 液化石油气到岸成本下移，PDH利润修复。</p><p>【甲醇】 随着局势缓和预期的进一步走强，甲醇夜盘回落。江浙地区MTO装置维持全线停车，到港量环比小幅增加，周期内港口呈现累库。港口库存的绝对水平偏低，短期行情跟随局势变化。</p><p>【纯苯】 纯苯价格震荡整理，下游按需采购。</p>
                            <div class="zwothers"><div class="sourcebox"><span>文章来源：多品种日报</span></div></div></div></body></html>'''

    # B13-F02: 当日条目 `<span  class="red" >` (双空格) + 页面其它位置的 [MM-DD] 诱饵
    _B13_MY_RED = '''<html><body><div class="column-item"><ul class="column-list">
<li >
        <span  class="red" >[11-10]</span>
        <a href="https://nenghua.mysteel.com/a/26111009/AAAAAAAAAAAAAAAA.html" title="[甲醛]：甲醛市场早间提示（20261110）"
            target="_blank">[甲醛]：甲醛市场早间提示（20261110）</a>
    </li>
<li >
        <span >[11-10]</span>
        <a href="https://nenghua.mysteel.com/a/26111010/BBBBBBBBBBBBBBBB.html" title="[甲醇]：中国甲醇装置检修数据统计（20261110）"
            target="_blank">[甲醇]：中国甲醇装置检修数据统计（20261110）</a>
    </li>
<li >
        <span  class="red" >[11-09]</span>
        <a href="https://nenghua.mysteel.com/a/26110917/CCCCCCCCCCCCCCCC.html" title="[损失量]：中国甲醇装置检修数据统计（20261109）"
            target="_blank">[损失量]：中国甲醇装置检修数据统计（20261109）</a>
    </li>
</ul></div>
<div class="sidebar"><span>[11-10]</span> 侧栏不是条目, 不能被解析成行</div>
<div class="breadcrumb">[11-09] 面包屑也不是条目</div></body></html>'''

    _B13_NOW = 1794283200          # 2026-11-10 12:00 +08 (夹具时刻: 化销网当日已有装置条目)

    class _B13Clk:
        def __init__(self, t):
            self.t = float(t)
            self.mono = 1000.0

        def time(self):
            return self.t

        def monotonic(self):
            return self.mono

        def sleep(self, s):
            self.t += s
            self.mono += s

    def _b13_call(router, fn, t0=None):
        """打桩 http/time 后跑 fn(); 返回 (结果, [(url, tries, location)]) —— 全程离线."""
        g = globals()
        old_http, old_time = g["http"], g["time"]
        calls = []

        def _fake(url, referer=None, tries=3, deadline=None, **kw):
            calls.append((url, tries, bool(kw.get("location"))))
            r = router(url)
            if isinstance(r, BaseException):
                raise r
            return r

        g["http"] = _fake
        g["time"] = _B13Clk(_B13_NOW if t0 is None else t0)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                got = fn()
        finally:
            g["http"], g["time"] = old_http, old_time
        return got, calls

    def _b13_hxw_router(overrides=None):
        """6 类目列表 -> 夹具; 未列出的类目一律 404; 详情页返回真实早间提示页."""
        pages = {22: _B13_HXW_22, 13: _B13_HXW_13, 14: _B13_HXW_14}
        pages.update(overrides or {})

        def _router(url):
            if "infolist?catid=" in url:
                cat = int(url.rsplit("=", 1)[-1])
                page = pages.get(cat)
                return page.encode() if page is not None else RuntimeError("HTTP 404")
            if "infoshow" in url:
                return _B12_HXW_DETAIL.encode()
            return RuntimeError("404")

        return _router

    def _b13_a01():
        g = globals()
        need = ("src_mysteel", "src_em_ma", "my_rows", "my_detail", "my_candidates",
                "my_admit", "my_list_ts", "em_ma_rows", "em_detail",
                "em_ma_candidates", "em_admit", "em_id_ts")
        miss = [n for n in need if not callable(g.get(n))]
        got = (g.get("HXW_CATS"), g.get("HXW_PRIORITY_CATS"), g.get("HXW_LIST_MIN_S"),
               g.get("HXW_DETAIL_MAX"), g.get("MY_DETAIL_MAX"),
               g.get("EM_MA_DETAIL_MAX"), g.get("EM_MA_AGE_S"),
               g.get("MY_LIST_URL"), g.get("EM_MA_LIST_URL"))
        _ = g.get("MY_BOILER") or ()
        strong = g.get("CHEM_EVENT_STRONG") or ()
        ok = (not miss
              and got == ((13, 14, 22, 23, 21, 20), (13, 14), 2.5, 6, 4, 1, 36 * 3600,
                          "https://nenghua.mysteel.com/breed/jcjxy/",
                          "https://futures.eastmoney.com/qihuo/ma.html")
              and "装置" not in strong
              and {"停车", "检修", "减产", "重启", "投产", "限产"} <= set(strong))
        return ok, f"miss={miss} got={got!r} strong={len(strong)}"

    def _b13_a02():
        """化销网 6 类目: 抓取顺序 = HXW_CATS / 22 tries=2 其余 1 / 详情上限 HXW_DETAIL_MAX."""
        items, calls = _b13_call(_b13_hxw_router(), lambda: src_hxw(None))
        lists = [c for c in calls if "infolist" in c[0]]
        dets = [c for c in calls if "infoshow" in c[0]]
        order = [int(c[0].rsplit("=", 1)[-1]) for c in lists]
        # 7 个候选同页 -> 详情必须在上限 6 处截断
        lis = "".join(
            '<li><a href="/index/quote/infoshow?id=1950%02d"><i class="v-icon"></i>'
            "11月10日甲醇装置停车提示%d</a><span>2026-11-10 09:3%d</span></li>" % (i, i, i)
            for i in range(7))
        cap_page = ('<html><body><div class="info-list"><ul>%s</ul></div></body></html>' % lis)
        def _cap_router(url):
            if "infolist" in url:
                return cap_page.encode()
            return _B12_HXW_DETAIL.encode()

        _c, calls2 = _b13_call(_cap_router, lambda: src_hxw(None))
        dets2 = [c for c in calls2 if "infoshow" in c[0]]
        # B13-F04: 13/14 先抓且先取详情, 22 次之; 22 的 tries=2 按类目名校验
        ok = (order == [13, 14, 22, 23, 21, 20]
              and {int(c[0].rsplit("=", 1)[-1]): c[1] for c in lists}
              == {13: 1, 14: 1, 22: 2, 23: 1, 21: 1, 20: 1}
              and all(c[1] == 1 for c in dets + dets2)
              and len(dets) == 2 and len(dets2) == HXW_DETAIL_MAX == 6
              and len(items) == 2)
        return ok, (f"order={order} tries={[c[1] for c in lists]} "
                    f"dets={len(dets)} dets_cap={len(dets2)} items={len(items)}")

    def _b13_a03():
        """同一条目跨类目重复出现 -> 源内按 infoshow id 去重, 详情只拉一次."""
        items, calls = _b13_call(_b13_hxw_router({13: _B13_HXW_DUP}), lambda: src_hxw(None),
                                 t0=1794188100)      # 2026-11-09 09:35: 192012 仍新鲜
        ids = [c[0].rsplit("id=", 1)[-1] for c in calls if "infoshow" in c[0]]
        ok = (ids.count("192012") == 1 and len(items) == 2
              and items[0]["text"].startswith("11月9日甲醇市场早间提示："))
        return ok, f"ids={ids} items={len(items)}"

    def _b13_a04():
        """部分类目失败 -> 只跳过该类目 (源仍健康); 6 类目全失败 -> RuntimeError."""
        items, calls = _b13_call(_b13_hxw_router(), lambda: src_hxw(None))
        ok_part = len(items) == 2 and len(calls) == 8
        try:
            _b13_call(lambda u: RuntimeError("HTTP 500"), lambda: src_hxw(None))
            raised = ""
        except RuntimeError as e:
            raised = str(e)
        ok = (ok_part and "列表失败 6 个" in raised and "0 条" in raised)
        return ok, f"items={len(items)} calls={len(calls)} raised={raised[:40]!r}"

    def _b13_a05():
        """类目 22 结构哨兵硬失败; 次级类目失败只跳过且必须留一行可见日志."""
        g = globals()
        old_http, old_time = g["http"], g["time"]
        bad = '<html><body><div class="main">无 info-list</div></body></html>'

        def _mk(pages):
            def _f(url, referer=None, tries=3, deadline=None, **kw):
                if "infolist?catid=" in url:
                    cat = int(url.rsplit("=", 1)[-1])
                    p = pages.get(cat)
                    if p is None:
                        raise RuntimeError("HTTP 404")
                    return p.encode() if isinstance(p, str) else p
                if "infoshow" in url:
                    return _B12_HXW_DETAIL.encode()
                return RuntimeError("404")
            return _f

        err = ""
        g["time"] = _B13Clk(_B13_NOW)
        try:
            g["http"] = _mk({22: bad})
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    src_hxw(None)
            except RuntimeError as e:
                err = str(e)
            # 22 正常 + 13 坏页: 不得抛错, 且要留一行「1 类目列表失败」
            g["http"] = _mk({22: _B13_HXW_22, 13: bad, 14: _B13_HXW_14,
                              23: _B13_HXW_22, 21: _B13_HXW_22, 20: _B13_HXW_22})
            so = io.StringIO()
            try:
                with contextlib.redirect_stdout(so):
                    got = src_hxw(None)
            finally:
                out = so.getvalue()
        finally:
            g["http"], g["time"] = old_http, old_time
        ok = ("info-list" in err and len(got) == 1
              and "1 类目列表失败" in out and "13:" in out)
        return ok, f"err={err[:28]!r} items={len(got)} log={out.strip()[:70]!r}"

    def _b13_a06():
        """我的钢铁列表: URL 去重 / ts=URL 的 YYMMDDHH 整点 / 标题无零宽空格 / 三道哨兵."""
        rows = my_rows(_B13_MY_LIST)
        want_first = {"id": "https://nenghua.mysteel.com/a/26110916/80CEA5FBB3D0B58F.html",
                      "ts": 1794211200}
        urls = [r["url"] for r in rows]
        sent = []
        for page in ('<html></html>',
                     '<html><li ><span >[11-09]</span><a href="https://nenghua.mysteel.com/a/xxx.html">A</a></li></html>',
                     _B13_MY_LIST.replace("甲醇", "Methanol").replace("隆众聚焦", "Focus")
                                     .replace("甲醛", "X").replace("二甲醚", "Y")):
            try:
                my_rows(page)
                sent.append("no-raise")
            except RuntimeError:
                sent.append("raise")
        ok = (len(rows) >= 5 and len(set(urls)) == len(urls)
              and [r for r in rows if r["title"].startswith("[损失量]")][0]["ts"] == 1794211200
              and my_list_ts(want_first["id"]) == want_first["ts"]
              and not any("\u200b" in r["title"] for r in rows)
              and sent == ["raise", "raise", "raise"]
              and rows[0]["id"] == want_first["id"] or rows[0]["ts"] == want_first["ts"])
        return ok, f"n={len(rows)} uniq={len(set(urls))} sent={sent} r0={rows[0] if rows else None}"

    def _b13_a07():
        """列表级候选: 「装置动态/装置快讯」不进候选; 损失量/一周前瞻/停车聚焦进候选."""
        rows = my_rows(_B13_MY_LIST)
        titles = [r["title"] for r in my_candidates(rows)]
        ok = (any("[损失量]" in t for t in titles)
              and any("一周前瞻" in t for t in titles)
              and any("全线停车" in t for t in titles)
              and not any("装置动态" in t for t in titles)
              and not any("装置快讯" in t for t in titles)
              and not any("及MTP装置快讯" in t for t in titles))
        return ok, f"cands={len(titles)} {titles[:4]}"

    def _b13_a08():
        """导语为空 -> 标题级放行, 但只放「强事件词 + 品种词」; 样板文/装置动态丢弃."""
        empty = my_detail(_B13_MY_EMPTY)
        lead = my_detail(_B13_MY_LEAD)
        boiler = my_detail(_B13_MY_BOILER)
        ok = (empty is not None and empty["body"] == "" and empty["ts"] > 0
              and my_admit("[损失量]：中国甲醇装置检修数据统计（20261109）", "") is True
              and my_admit("内蒙古远兴能源甲醇装置动态", "") is False
              and my_admit("联泓MTO装置快讯", boiler["body"]) is False
              and my_admit("联泓新科MTO装置快讯（二期）", boiler["body"]) is False
              and my_admit("[甲醛]：甲醛市场一周前瞻（20261109）", "") is False
              and my_admit("[二甲醚]：二甲醚市场一周前瞻（20261109）", "") is False
              and my_admit("[甲醇日评]：宏观利空来袭（20261103）", lead["body"]) is True)
        return ok, (f"empty_body={empty['body']!r} empty_ts={empty['ts']} "
                    f"lead_len={len(lead['body'])} boiler_len={len(boiler['body'])}")

    def _b13_a09():
        """详情正文边界不含「资讯编辑」; 缺 content-text 容器 -> None; publish 到秒."""
        lead = my_detail(_B13_MY_LEAD)
        ok = (my_detail("<html><body>nothing</body></html>") is None
              and "资讯编辑" not in lead["body"]
              and lead["ts"] == 1793707349
              and len(lead["body"]) == 181)
        return ok, f"ts={lead['ts']} len={len(lead['body'])}"

    def _b13_a10():
        """东财 tag: 两区块重复项按 id 去重 / 标题取 title 属性 / ts=id 里的 YYYYMMDD."""
        rows = em_ma_rows(_B13_EM_LIST)
        cands = em_ma_candidates(rows)
        titles = [r["title"] for r in cands]
        ok = (len(rows) == 5 and len({r["id"] for r in rows}) == 5
              and em_id_ts("202611103926942138") == 1794240000
              and all(r["ts"] % 86400 != 0 or True for r in rows)
              and len(cands) == 2
              and any("金联创" in t for t in titles)
              and any("甲醇：港口库存" in t for t in titles)
              and not any("甲醇主力合约" in t for t in titles)
              and rows[0]["title"].startswith("能化 | 甲醇："))
        return ok, f"rows={len(rows)} cands={len(cands)} {titles}"

    def _b13_a11():
        """东财准入双门: 事件门 (强事件+品种) / 锚点门 (锚点标题 + 正文>=300 字)."""
        body_long = "金联创分析指出，" + "甲醇现货高位回落。" * 40
        ok = (em_admit("金联创：美伊局势仍是核心因素", body_long) is True
              and em_admit("能化 | 甲醇：港口库存的绝对水平偏低", body_long) is True
              and em_admit("金联创：美伊局势仍是核心因素", "短讯" * 20) is False
              and em_admit("山东某甲醇装置11月10日停车检修，涉及产能60万吨", body_long) is True
              and em_admit("能化 | 纯碱：供应快速回升", body_long) is False
              and em_admit("甲醇主力合约日内涨幅扩大至2.00%", body_long) is False)
        return ok, "dual-gate"

    def _b13_a12():
        """东财详情必须带 -L (302); 既有源 argv 逐元素不含 -L (默认行为不变)."""
        g = globals()
        realrun = g["subprocess"].run
        seen: list = []

        class _R:
            stdout = b"ok\n200"
            returncode = 0

        try:
            g["subprocess"].run = lambda cmd, **kw: (seen.append(list(cmd)) or _R())
            g["http"]("https://futures.eastmoney.com/a/202611093926259150.html",
                      None, tries=1, location=True)
            plain = list(seen[-1])
            g["http"]("https://www.huaxiaowang.com/index/quote/infoshow?id=192012", None,
                      tries=1)
            default = list(seen[-1])
        finally:
            g["subprocess"].run = realrun
        g["_EM_DETAIL_SEEN"].clear()
        items, calls = _b13_call(
            lambda u: (_B13_EM_LIST.encode() if "qihuo/ma.html" in u
                       else _B13_EM_DETAIL.encode()),
            lambda: src_em_ma(None))
        det = [c for c in calls if "/a/" in c[0]]
        ok = ("-L" in plain and "-L" not in default
              and default[:3] == ["curl", "-q", "-sS"]
              and len(det) == 1 and det[0][2] is True
              and len(items) == 1 and items[0]["ts"] == 1794211440)
        return ok, f"plain_has_L={'-L' in plain} default_has_L={'-L' in default} det={det}"

    def _b13_a13():
        """端到端 (真实夹具): 化销网 2 条 (当刻装置/MTO) / 我的钢铁 0 条 (当刻无 4h 内候选) /
        东财 1 条 (日评锚点) —— 三源文本形态与既有源一致."""
        hx, _c1 = _b13_call(_b13_hxw_router(), lambda: src_hxw(None))
        my, _c2 = _b13_call(
            lambda u: (_B13_MY_LIST.encode() if "breed/jcjxy" in u
                       else _B13_MY_LEAD.encode()),
            lambda: src_mysteel(None))
        globals()["_EM_DETAIL_SEEN"].clear()
        em, _c3 = _b13_call(
            lambda u: (_B13_EM_LIST.encode() if "qihuo/ma.html" in u
                       else _B13_EM_DETAIL.encode()),
            lambda: src_em_ma(None))
        ok = (len(hx) == 2 and len(my) == 0 and len(em) == 1
              and [x["ts"] for x in hx] == [1794275220, 1794274320]
              and hx[0]["src"] == "化销网" and hx[0]["text"].startswith(
                  "11月10日山西骏捷（孝义金晖）甲醇装置动态：")
              and em[0]["src"] == "东财甲醇" and em[0]["text"].startswith("能化 | 甲醇："))
        return ok, f"hx={len(hx)} my={len(my)} em={len(em)}"

    def _b13_a14():
        """全源失败判定只算老源: 17 老源失败 + 4 化工源空健康 -> 文案仍只列 17 个老源名."""
        srcs = ([_b12_mk(n, "boom") for n in _B12_LEGACY]
                + [_b12_mk(n, None) for n in ("化销网", "隆众", "我的钢铁", "东财甲醇")])
        with tempfile.TemporaryDirectory() as td:
            rc, pays, _st, _q = _b11_run(td, _b11_clk(), srcs, state={"seedmarker": 1})
        ok = (rc == 1 and len(pays) == 1 and "全部抓取失败" in pays[0]
              and all(n not in pays[0]
                      for n in ("化销网", "隆众", "我的钢铁", "东财甲醇")))
        return ok, f"rc={rc} pays={len(pays)}"

    def _b13_a15():
        """4 个新源全失败、17 老源健康 -> 不触发全源告警 (rc=0, 无载荷, 无 _alert)."""
        srcs = ([_b12_mk(n, None) for n in _B12_LEGACY]
                + [_b12_mk(n, "boom") for n in ("化销网", "隆众", "我的钢铁", "东财甲醇")])
        with tempfile.TemporaryDirectory() as td:
            rc, pays, st, _q = _b11_run(td, _b11_clk(), srcs, state={"seedmarker": 1})
        ok = (rc == 0 and not pays and "_alert" not in st)
        return ok, f"rc={rc} pays={len(pays)} alert={'_alert' in st}"

    def _b13_em_one(aid, title):
        """东财单条列表页 (F-01/F-03 用): 一条稿件, 标题作 title 属性."""
        return ('<html><body><div class="contentList2"><ul><li><a href="'
                'http://futures.eastmoney.com/a/%s.html" target="_blank" title="%s">%s'
                '</a></li></ul></div></body></html>' % (aid, title, title))

    def _b13_reset_em_seen():
        """清空东财轮转记忆; 旧码无该符号时是 no-op (让断言按内容失败而不是 KeyError)."""
        s = globals().get("_EM_DETAIL_SEEN")
        if s is not None:
            s.clear()

    def _b13_a16():
        """B13-F01: 多品种日报只入队【甲醇】段, 不含原油/燃料油/沥青等其它品种段."""
        d = em_detail(_B13_EM_MULTI)
        body = d["body"] if d else ""
        title = "能化 | 甲醇：港口库存的绝对水平偏低 短期行情跟随局势变化"
        lis = _b13_em_one("202611103926942138", title)
        _b13_reset_em_seen()
        items, calls = _b13_call(
            lambda u: (lis.encode() if "qihuo/ma.html" in u else _B13_EM_MULTI.encode()),
            lambda: src_em_ma(None))
        text = items[0]["text"] if items else ""
        banned = ("原油", "燃料油", "沥青", "液化石油气")
        det = [c for c in calls if "/a/" in c[0]]
        ok = (body.startswith("【甲醇】") and "甲醇" in body and "停车" in body
              and not any(w in body for w in banned)
              and len(det) == 1 and len(items) == 1
              and "【甲醇】" in text and "停车" in text
              and not any(w in text for w in banned))
        return ok, (f"len={len(body)} items={len(items)} me={text.count('甲醇')} "
                    f"banned={[w for w in banned if w in text]}")

    def _b13_a17():
        """B13-F01: 多品种无【甲醇】段 -> 详情 None 且不入队; 正文无甲醇硬拒;
        纯甲醇文 (429 字, 无分节标记) 仍整篇入队."""
        no = _B13_EM_MULTI.replace("【甲醇】", "【尿素】")
        d_no = em_detail(no)
        title = "能化 | 甲醇：港口库存的绝对水平偏低 短期行情跟随局势变化"
        lis = _b13_em_one("202611103926942138", title)
        _b13_reset_em_seen()
        items_no, _c1 = _b13_call(
            lambda u: (lis.encode() if "qihuo/ma.html" in u else no.encode()),
            lambda: src_em_ma(None))
        d_pure = em_detail(_B13_EM_DETAIL)
        lis2 = _b13_em_one("202611093926259150",
                           "金联创：美伊局势仍是核心因素 11月初甲醇高位回落转震荡")
        _b13_reset_em_seen()
        items_pure, _c2 = _b13_call(
            lambda u: (lis2.encode() if "qihuo/ma.html" in u
                       else _B13_EM_DETAIL.encode()),
            lambda: src_em_ma(None))
        ok = (d_no is None and not items_no
              and d_pure is not None and "甲醇" in d_pure["body"]
              and not EM_SECTION_RE.search(d_pure["body"])
              and len(items_pure) == 1 and "甲醇" in items_pure[0]["text"]
              and em_admit("能化 | 甲醇：港口库存的绝对水平偏低",
                           "原油价格大跌。" * 60) is False)
        return ok, (f"no_none={d_no is None} items_no={len(items_no)} "
                    f"pure_len={0 if d_pure is None else len(d_pure['body'])} "
                    f"items_pure={len(items_pure)}")

    def _b13_a18():
        """B13-F02: `<span  class="red" >` 的当日条目必须全部解析 (夹具 3/3),
        且侧栏/面包屑里的 [MM-DD] 不得被误吃; 4h 内候选 >= 1."""
        rows = my_rows(_B13_MY_RED)
        cands = [r for r in my_candidates(rows) if _B13_NOW - r["ts"] <= CHEM_DETAIL_AGE_S]
        urls = [r["url"] for r in rows]
        ok = (len(rows) == 3 and len(set(urls)) == 3
              and all(r["ts"] > 0 for r in rows)
              and not any(("侧栏" in r["title"]) or ("面包屑" in r["title"]) for r in rows)
              and any(r["url"].endswith("AAAAAAAAAAAAAAAA.html") for r in rows)
              and len(cands) >= 1)
        return ok, (f"rows={len(rows)} uniq={len(set(urls))} cands={len(cands)} "
                    f"titles={[r['title'][:12] for r in rows]}")

    def _b13_a19():
        """B13-F03: 东财详情源内轮转 —— 第 1 轮 a1 (多品种日报), 第 2 轮 a2 (429 字日评),
        第 3 轮全部取过 -> 不再发详情请求 (不重复下载, 不永久饿死)."""
        _b13_reset_em_seen()

        def _router(u):
            if "qihuo/ma.html" in u:
                return _B13_EM_LIST.encode()
            if "202611103926942138" in u:
                return _B13_EM_MULTI.encode()
            return _B13_EM_DETAIL.encode()

        r1, c1 = _b13_call(_router, lambda: src_em_ma(None))
        r2, c2 = _b13_call(_router, lambda: src_em_ma(None))
        r3, c3 = _b13_call(_router, lambda: src_em_ma(None))

        def _dets(cs):
            return [c[0] for c in cs if "/a/" in c[0]]

        d1, d2, d3 = _dets(c1), _dets(c2), _dets(c3)
        ok = (len(r1) == 1 and len(d1) == 1 and "202611103926942138" in d1[0]
              and len(r2) == 1 and len(d2) == 1 and "202611093926259150" in d2[0]
              and "202611103926942138" not in d2[0]
              and not r3 and not d3)
        return ok, (f"d1={[u[-23:] for u in d1]} d2={[u[-23:] for u in d2]} "
                    f"r3={len(r3)} d3={d3}")

    def _b13_a20():
        """B13-F04: 份额 7s + 列表 4.62s 的慢轮下, catid=13/14 候选仍拿到详情;
        预算不够时截断后续类目 (21/20) 而不是牺牲详情."""
        g = globals()
        old_http, old_time = g["http"], g["time"]
        clk = _B13Clk(_B13_NOW)
        clk.mono = 1000.0
        deadline = 1000.0 + 7.0
        calls: list = []
        router = _b13_hxw_router()

        def _fake(url, referer=None, tries=3, deadline=None, **kw):
            calls.append((url, tries, bool(kw.get("location"))))
            clk.mono += 0.64 if "infoshow" in url else 0.77   # 实测: 详情 0.64 / 列表 0.77
            r = router(url)
            if isinstance(r, BaseException):
                raise r
            return r

        g["http"], g["time"] = _fake, clk
        so = io.StringIO()
        try:
            with contextlib.redirect_stdout(so):
                items = src_hxw(deadline)
        finally:
            g["http"], g["time"] = old_http, old_time
        dets = [c[0] for c in calls if "infoshow" in c[0]]
        lists = [int(c[0].rsplit("=", 1)[-1]) for c in calls if "infolist" in c[0]]
        out = so.getvalue()
        ok = (any("id=192048" in u for u in dets)     # cat13 装置动态
              and any("id=192042" in u for u in dets)  # cat14 MTO 快讯
              and len(items) == 2
              and lists[:2] == [13, 14] and 20 not in lists
              and "截断后续" in out)
        return ok, f"lists={lists} dets={len(dets)} items={len(items)} log={out.strip()[:60]!r}"

    def _b13_a21():
        """B13-F05: 东财**列表**请求 argv 不含 -L, **详情**请求含 -L (变异 m3a 必红)."""
        g = globals()
        g["_EM_DETAIL_SEEN"].clear()
        items, calls = _b13_call(
            lambda u: (_B13_EM_LIST.encode() if "qihuo/ma.html" in u
                       else _B13_EM_DETAIL.encode()),
            lambda: src_em_ma(None))
        lists = [c for c in calls if "qihuo/ma.html" in c[0]]
        dets = [c for c in calls if "/a/" in c[0]]
        ok = (len(lists) == 1 and lists[0][2] is False
              and len(dets) == 1 and dets[0][2] is True and len(items) == 1)
        return ok, f"list_loc={[c[2] for c in lists]} det_loc={[c[2] for c in dets]}"

    _b12_case("B13-01 常量/入口/化销网类目清单齐全且 URL 精确", _b13_a01)
    _b12_case("B13-02 化销网 6 类目抓取顺序 + tries + 详情上限 6", _b13_a02)
    _b12_case("B13-03 化销网 源内按 infoshow id 去重 (跨类目重复只拉一次)", _b13_a03)
    _b12_case("B13-04 化销网 部分类目失败降级 / 6 类目全失败抛错", _b13_a04)
    _b12_case("B13-05 化销网 类目 22 结构哨兵仍是硬失败", _b13_a05)
    _b12_case("B13-06 我的钢铁 列表解析 (URL 去重/整点 ts/零宽空格/三态哨兵)", _b13_a06)
    _b12_case("B13-07 我的钢铁 列表级候选 (装置动态/快讯不进候选)", _b13_a07)
    _b12_case("B13-08 我的钢铁 导语空 -> 标题级放行 / 样板文与装置动态丢弃", _b13_a08)
    _b12_case("B13-09 我的钢铁 详情正文边界 + 秒级 ts + 缺容器 None", _b13_a09)
    _b12_case("B13-10 东财 tag 列表去重 + 甲醇筛 + id 日期 ts", _b13_a10)
    _b12_case("B13-11 东财 准入双门 (事件门 / 锚点门 300 字 / 快讯丢弃)", _b13_a11)
    _b12_case("B13-12 东财 详情带 -L 且既有源 argv 逐元素不变", _b13_a12)
    _b12_case("B13-13 端到端三源 (真实夹具) 文本形态与入队条数", _b13_a13)
    _b12_case("B13-14 全源失败判定只算老源 (17 失败 + 4 空健康)", _b13_a14)
    _b12_case("B13-15 化工源全失败不触发全源告警 (17 健康空)", _b13_a15)
    _b12_case("B13-16 东财 多品种日报只入队【甲醇】段 (无原油/燃料油/沥青)", _b13_a16)
    _b12_case("B13-17 东财 无【甲醇】段不入队 / 正文无甲醇硬拒 / 纯甲醇文仍入队", _b13_a17)
    _b12_case("B13-18 我的钢铁 class=red 当日条目全量解析 + 4h 内候选", _b13_a18)
    _b12_case("B13-19 东财 详情源内轮转 (a1 -> a2 -> 不再下载)", _b13_a19)
    _b12_case("B13-20 化销网 慢轮 7s: 13/14 详情优先, 后续类目截断", _b13_a20)
    _b12_case("B13-21 东财 列表请求不带 -L / 详情带 -L", _b13_a21)
    # ---- Batch 14: 研报 PDF 尾部免责/版权/地址样板段剔除 (全部离线)
    # 夹具口径: 东方金诚/万联/国信 的尾部行逐字取自 2026-11 东财真实 PDF 的 pdftotext
    # 输出 (东方金诚 AP202611101831579729 / 万联 AP202611051831461744 /
    # 国信 AP202611031831347443); 国信为节选 (省中间评级表与声明正文, 以 `[节选]` 标注);
    # 华鑫/开源 两份按 CTO 尾部采样的原句构造 (未逐字复核, 仅锁规则形态)。
    _b14_body = '2026 年 10 月，国内动力煤价格延续上行，港口库存环比下降 3.2%，电厂日耗同比回升。'
    _b14_body_tail = '11 月港口调出量环比回升 4.1%，锚地船舶数同比下降 6 艘。'
    _b14_wm3 = '万联证券版权所有发送给上海东方财富金融数据服务有限公司.张嘉仪.ybjieshou@eastmoney.com p3'
    _b14_wm5 = '万联证券版权所有发送给上海东方财富金融数据服务有限公司.张嘉仪.ybjieshou@eastmoney.com p5'
    _b14_wm6 = '万联证券版权所有发送给上海东方财富金融数据服务有限公司.张嘉仪.ybjieshou@eastmoney.com p6'
    _b14_gx_foot = '请务必阅读正文之后的免责声明及其项下所有内容'
    _b14_wl_tail = [
        '证券分析师承诺',
        '本人具有中国证券业协会授予的证券投资咨询执业资格并登记为证券分析师，以勤勉的执业态度，独立、客观',
        '地出具本报告。本报告清晰准确地反映了本人的研究观点。本人不曾因，不因，也将不会因本报告中的具体推',
        '荐意见或观点而直接或间接收到任何形式的补偿。',
        '[Tale_ReliefInfo]',
        '免责条款',
        '万联证券股份有限公司（以下简称“本公司”）是一家覆盖证券经纪、投资银行、投资管理和证券咨询等多项',
        '业务的全国性综合类证券公司。本公司具有中国证监会许可的证券投资咨询业务资格。',
        '本报告仅供本公司的客户使用。本公司不会因接收人收到本报告而视其为客户。在任何情况下，本报告中的信',
        '息或所表述的意见并不构成对任何人的投资建议。本报告中的信息或所表述的意见并未考虑到个别投资者的具',
        '体投资目的、财务状况以及特定需求。客户应自主作出投资决策并自行承担投资风险。本公司不对任何人因使',
        '用本报告中的内容所导致的损失负任何责任。在法律许可情况下，本公司或其关联机构可能会持有报告中提到',
        '的公司所发行的证券头寸并进行交易，还可能为这些公司提供或争取提供投资银行、财务顾问或类似的金融服',
        '务。',
        '市场有风险，投资需谨慎。本报告是基于本公司认为可靠且已公开的信息撰写，本公司力求但不保证这些信息',
        '的准确性及完整性，也不保证文中的观点或陈述不会发生任何变更。在不同时期，本公司可发出与本报告所载',
        '资料、意见及推测不一致的报告。分析师任何形式的分享证券投资收益或者分担证券投资损失的书面或口头承',
        '诺均为无效。',
        '本报告的版权仅为本公司所有，未经书面许可任何机构和个人不得以任何形式翻版、复制、刊登、发表和引用。',
        '未经我方许可而引用、刊发或转载的引起法律后果和造成我公司经济损失的概由对方承担，我公司保留追究的',
        '权利。',
        '[Table_AdressInfo]',
        '万联证券股份有限公司研究所',
        '上海浦东新区世纪大道 1528 号陆家嘴基金大厦',
        '北京西城区平安里西大街 28 号中海国际中心',
        '深圳福田区深南大道 2007 号金地中心',
        '广州天河区珠江东路 11 号高德置地广场',
        '万联证券研究所 www.wlzq.cn',
        '第8页共8页',
    ]
    _b14_djc_tail = [
        '权利及免责声明：',
        '本研究报告及相关的信用分析数据、模型、软件、研究观点等所有内容的著作权和其他相关知识产权均归东方金诚所有，东方金诚保留一',
        '切与此相关的权利，任何机构及个人未经东方金诚书面授权不得修改、复制、逆向工程、销售、分发、储存、引用或以任何方式传播。未',
        '获书面授权的机构及人士不应获取或以任何方式使用本研究报告，东方金诚对本研究报告的未授权使用、超授权使用和非法使用等不当使',
        '用行为所造成的一切后果均不承担任何责任。',
        '本研究报告中引用的标明出处的公开资料，其合法性、真实性、准确性、完整性均由资料提供方或/及发布方负责，东方金诚对该等资料进',
        '行了合理审慎的核查，但不应视为东方金诚对其合法性、真实性、准确性及完整性提供了任何形式的保证。',
        '本研究报告的结论，是在最初发表本报告日期当日按照东方金诚的研究流程及标准做出的独立判断，遵循了客观、公正的原则，未受第三',
        '方组织或个人的干预和影响。东方金诚可能不时补充、修订或更新有关信息，也可能发出其他与本报告所载内容不一致或有不同结论的报',
        '告，但没有义务和责任更新本报告并通知报告使用者。',
        '本研究报告仅用于为投资人、发行人等授权使用方提供第三方参考意见，并非是对某种决策的结论或建议；投资者应审慎使用本研究报',
        '告，自行对投资行为和投资结果负责，东方金诚不对其承担任何责任。',
        '本声明为本研究报告不可分割的内容，任何使用者使用或引用本报告，应转载本声明。并且，相关引用必须注明来自东方金诚且不得篡',
        '改、歪曲或有任何类似性质的修改行为。',
        '3',
    ]
    _b14_gx_tail = [
        '免责声明',
        '分析师声明',
        '作者保证报告所采用的数据均来自合规渠道；分析逻辑基于作者的职业理解，通过合理判断并得出结论，',
        '力求独立、客观、公正，结论不受任何第三方的授意或影响；作者在过去、现在或未来未就其研究报告',
        '所提供的具体建议或所表述的意见直接或间接收取任何报酬，特此声明。',
        '重要声明',
        '本报告由国信证券股份有限公司（已具备中国证监会许可的证券投资咨询业务资格）制作；报告版权归',
        '国信证券股份有限公司',
        '关本报告的摘要或节选都不代表本报告正式完整的观点，一切须以我公司向客户发布的本报告完整版本',
        '本报告仅供参考之用，不构成出售或购买证券或其他投资标的要约或邀请。在任何情况下，本报告中的信息和',
        '意见均不构成对任何个人的投资建议。任何形式的分享证券投资收益或者分担证券投资损失的书面或口头承诺',
        '均为无效。投资者应结合自己的投资目标和财务状况自行判断是否采用本报告所载内容和信息并自行承担风险，',
        '我公司及雇员对投资者使用本报告及其内容而造成的一切后果不承担任何法律责任。',
        '证券投资咨询业务的说明',
        '本公司具备中国证监会核准的证券投资咨询业务资格。证券投资咨询，是指从事证券投资咨询业务的机',
        '构及其投资咨询人员以下列形式为证券投资人或者客户提供证券投资分析、预测或者建议等直接或者间',
        '证券研究报告',
        '国信证券经济研究所',
        '深圳',
        '深圳市福田区福华一路 125 号国信金融大厦 36 层',
        '邮编：518046',
        '总机：0755-82130833',
        '上海',
        '上海浦东民生路 1199 弄证大五道口广场 1 号楼 12 层',
        '邮编：200135',
        '北京',
        '北京西城区金融大街兴盛街 6 号国信证券 9 层',
        '邮编：100032',
    ]
    # 正文 60 行 + 声明尾: 声明起点 81.5%, 与东财实测 (2644/3243) 同比例
    _b14_djc = "\n".join([_b14_body] * 60 + _b14_djc_tail)
    _b14_djc_long = "\n".join([_b14_body] * 80 + _b14_djc_tail)
    # 万联: 正文 + 3 条东财水印 (末条落在 55% 之后) + 声明尾
    _b14_wl = "\n".join([_b14_body] * 12 + [_b14_wm3] + [_b14_body] * 10
                         + [_b14_wm5] + [_b14_body] * 10 + [_b14_wm6]
                         + [_b14_body] * 2 + _b14_wl_tail)
    # 国信: 正文 + 3 条重复页脚 (含「免责声明」字样) + 一条水印后正文 + 声明尾
    _b14_gx = "\n".join([_b14_body] * 30 + [_b14_gx_foot] + [_b14_body] * 8
                         + [_b14_gx_foot] + [_b14_body] * 5 + [_b14_gx_foot]
                         + [_b14_body_tail] + [_b14_body] * 2
                         + ["证券研究报告"] + _b14_gx_tail)
    _b14_ky = "\n".join([_b14_body] * 30
                         + ["数据来源：Mysteel、开源证券研究所"])
    _b14_risk = "\n".join([_b14_body] * 30
                           + ["风险提示：近期港口库存累积，价格存在回调压力。",
                              "数据来源：Mysteel、开源证券研究所"])
    _b14_hd = "\n".join([_b14_body] * 30
                         + ["免责条款",
                            "本节口径说明：进口关税按到岸价计征，与上一节测算一致。",
                            "关税调整会改变进口成本，进而影响国内价格。",
                            "数据来源：Mysteel、开源证券研究所"])
    _b14_hd_decl = "\n".join([_b14_body] * 30 + ["免责条款"] + _b14_wl_tail[1:])
    _b14_hx = "\n".join([_b14_body] * 30
                         + ["请阅读最后一页重要免责声明", "诚信、专业、稳健、高效"])
    _b14_addr = "\n".join([_b14_body] * 30
                           + ["万联证券研究所 www.wlzq.cn",
                              "上海浦东新区世纪大道 1528 号陆家嘴基金大厦",
                              "北京西城区平安里西大街 28 号中海国际中心"])
    _b14_foot3 = "\n".join([_b14_body] * 30 + [_b14_gx_foot] * 3)
    _b14_short = "\n".join([_b14_body] * 4 + _b14_wl_tail)
    _b14_dt = datetime.fromtimestamp(_B11_TS, TZ_CN)

    def _b14_case(name, fn):
        try:
            ok, detail = fn()
        except Exception as e:                       # noqa: BLE001 - 旧码缺符号 -> 本条红
            ok, detail = False, f"{type(e).__name__}: {e}"
        check(name, bool(ok), str(detail))


    def _b14_a01():
        body = pdf_clean(_b14_djc)
        ok = ("著作权" not in body and "保留一切" not in body
              and "本声明为本研究报告" not in body
              and body.endswith("电厂日耗同比回升。")
              and len(body) >= 2000 and len(_b14_djc) - len(body) >= 500)
        return ok, f"len={len(body)} raw={len(_b14_djc)} tail={body[-16:]!r}"


    def _b14_a02():
        body = pdf_clean(_b14_wl)
        ok = ("版权仅为本公司所有" not in body and "www.wlzq.cn" not in body
              and "金地中心" not in body and "万联证券研究所" not in body
              and "未经我方许可" not in body
              and body.endswith("电厂日耗同比回升。") and len(body) >= 1500)
        return ok, f"len={len(body)} tail={body[-16:]!r}"


    def _b14_a03():
        body = pdf_clean(_b14_gx)
        ok = ("重要声明" not in body and "证券投资咨询业务的说明" not in body
              and "国信证券经济研究所" not in body and "上海浦东民生路" not in body
              and "国信证券股份有限公司" not in body
              and _b14_body_tail in body          # 重复页脚之后的正文一行都不许丢
              and body.endswith("。")             # 尾行清理: 不留「证券研究报告」孤行
              and not body.endswith("证券研究报告"))
        return ok, f"len={len(body)} tail={body[-16:]!r}"


    def _b14_a04():
        body = pdf_clean(_b14_hx)
        ok = ("请阅读最后一页" not in body and "诚信、专业、稳健、高效" not in body
              and body.endswith("电厂日耗同比回升。"))
        return ok, f"len={len(body)} tail={body[-16:]!r}"


    def _b14_a05():
        body = pdf_clean(_b14_ky)
        pair = pdf_clean(_b14_ky + "\n" + "\n".join(_b14_wl_tail))
        no_loss = len(body) >= sum(len(x) for x in _b14_ky.splitlines())
        ok = (body.endswith("数据来源：Mysteel、开源证券研究所") and no_loss
              and "版权仅为本公司所有" not in pair
              and len(pair) < len(body) + sum(len(x) for x in _b14_wl_tail) - 500)
        return ok, f"ky={len(body)} pair={len(pair)} no_loss={no_loss}"


    def _b14_a06():
        body = pdf_clean(_b14_risk)
        pair = pdf_clean(_b14_risk + "\n" + "\n".join(_b14_wl_tail))
        ok = ("风险提示：近期港口库存累积" in body
              and "数据来源：Mysteel、开源证券研究所" in body
              and "版权仅为本公司所有" not in pair
              and pair.endswith("开源证券研究所"))
        return ok, f"risk={len(body)} pair={len(pair)}"


    def _b14_a07():
        body = pdf_clean(_b14_hd)
        decl = "\n".join([_b14_body] * 30 + ["免责条款"] + _b14_wl_tail[1:])
        body2 = pdf_clean(decl)
        ok = ("本节口径说明：进口关税按到岸价计征" in body
              and "关税调整会改变进口成本" in body
              and "数据来源：Mysteel、开源证券研究所" in body
              and "版权仅为本公司所有" not in body
              and "证券分析师承诺" not in body2
              and "版权仅为本公司所有" not in body2
              and body2.endswith("电厂日耗同比回升。"))
        return ok, f"hd={len(body)} decl={len(body2)}"


    def _b14_a08():
        foot = pdf_clean(_b14_foot3)               # 每页重复的页脚: 不是声明起点
        once = pdf_clean("\n".join([_b14_body] * 30
                                    + ["本报告的版权仅为本公司所有，未经书面许可不得翻印。"]))
        base = pdf_clean("\n".join([_b14_body] * 30))
        ok = (foot.count(_b14_gx_foot) == 3 and len(foot) > 1300
              and once == base and "版权仅为本公司所有" not in once
              and len(foot) > len(once))
        return ok, f"foot={len(foot)} once={len(once)}"


    def _b14_a09():
        body = pdf_clean(_b14_short)               # 正文只有 4 行 -> 不裁
        long_ = pdf_clean("\n".join([_b14_body] * 30 + _b14_wl_tail))
        ok = ("万联证券研究所 www.wlzq.cn" in body
              and "版权仅为本公司所有" in body
              and "万联证券研究所" not in long_ and "金地中心" not in long_)
        return ok, f"short={len(body)} long={len(long_)}"


    def _b14_a10():
        raw = _b14_djc
        ok = (pdf_clean.__defaults__ == (SNIPPET_MAX,)
              and pdf_clean(raw) == pdf_clean(raw, SNIPPET_MAX)
              and pdf_clean(pdf_clean(raw)) == pdf_clean(raw)
              and pdf_clean(raw, 600) == pdf_clean(raw)[:600]
              and len(pdf_clean(raw, 600)) == 600
              and "著作权" not in pdf_clean(raw, 3000)
              and SNIPPET_MAX == 24000 and SNIPPET_FULL_MAX == 40000)
        return ok, f"sig={pdf_clean(raw, 600)[-8:]!r}"


    def _b14_a11():
        it = {"src": "东财研报", "ts": _B11_TS,
               "text": "10 月进出口数据解读｜东方金诚 · 2026-11-10",
               "url": "https://example.com/b14.pdf",
               "snippet": pdf_clean(_b14_djc_long), "key": "研究:东财研报:b14"}
        parts = research_parts(it, _b14_dt)
        joined = "".join(parts)
        ok = (len(parts) >= 2 and "著作权" not in joined
              and "本声明为本研究报告" not in joined
              and all(u16(p) <= MSG_MAX for p in parts)
              and all(f"({i}/{len(parts)})" in p for i, p in enumerate(parts, 1)))
        return ok, f"n={len(parts)} u16={[u16(p) for p in parts]}"


    def _b14_a12():
        body = pdf_clean(_b14_addr)                # 地址/研究所/网站本身不是裁切依据
        decl = pdf_clean("\n".join([_b14_body] * 30
                                    + ["本报告的版权仅为本公司所有，未经书面许可不得翻印。"]
                                    + ["万联证券研究所 www.wlzq.cn"]))
        ok = ("万联证券研究所" in body and "www.wlzq.cn" in body
              and "陆家嘴基金大厦" in body
              and "万联证券研究所" not in decl and "www.wlzq.cn" not in decl)
        return ok, f"addr={len(body)} decl={len(decl)}"


    _b14_case("B14-01 东方金诚尾部声明被剔除 (真实尾部夹具)", _b14_a01)
    _b14_case("B14-02 万联 版权/三地地址被裁且每页水印不误裁", _b14_a02)
    _b14_case("B14-03 国信 重要声明/地址被裁 + 重复页脚不误裁 + 尾行清理", _b14_a03)
    _b14_case("B14-04 华鑫 单次「请阅读最后一页」+ 标语被裁", _b14_a04)
    _b14_case("B14-05 开源 无声明一字不裁; 同正文接真声明则裁", _b14_a05)
    _b14_case("B14-06 正文自带风险提示小节 -> 不裁; 尾部真声明 -> 裁", _b14_a06)
    _b14_case("B14-07 正文「免责条款」小标题+内容 -> 不裁; 标题下接声明 -> 裁", _b14_a07)
    _b14_case("B14-08 重复页脚/水印不作裁切依据; 单次版权行才裁", _b14_a08)
    _b14_case("B14-09 短报告护栏: 正文 < 600 字不清空", _b14_a09)
    _b14_case("B14-10 契约/幂等: 签名与默认值不变, 声明裁切幂等", _b14_a10)
    _b14_case("B14-11 与分片联动: 末片不再独占声明且分片无损", _b14_a11)
    _b14_case("B14-12 地址/研究所/网站本身不作裁切依据", _b14_a12)

    # ---- Batch 14 修复 F-01/F-02: 表格行保留 + 四道护栏逐条钉死 (B14-13..B14-18) ----
    # F-01 反例取自国信真实 PDF (AP202611031831347443): 声明段紧邻的两行正文表格行
    # 「长飞光纤光缆 107.2000」「中国海洋石油 24.5200」; 旧码的「<=60 字半句」把两行砍掉
    # (29690->29636)。这里要求: 声明照裁, 两行表格逐字保留。
    _b14_tbl = ["长飞光纤光缆 107.2000", "中国海洋石油 24.5200"]
    _b14_orphan = "证券研究报告"           # 国信 PDF 声明前的孤立报告类型行
    _b14_neutral = "10 月进口煤到港量维持高位，贸易商观望情绪较浓。"

    def _b14_lines(text):
        out = []
        for raw in (text or "").splitlines():
            s = raw.strip()
            if not s or PDF_TABLE.match(s) or PDF_NUM.match(s):
                continue
            out.append(s)
        return out

    def _b14_first(lines):
        counts = {}
        for s in lines:
            counts[s] = counts.get(s, 0) + 1
        return next((i for i, s in enumerate(lines)
                     if counts[s] < PDF_TAIL_REPEAT_MIN
                     and not PDF_TAIL_NOISE.search(s) and PDF_TAIL_MARKER.search(s)),
                    len(lines))

    def _b14_prefix(lines, idx):
        return sum(len(x) + 1 for x in lines[:idx])

    def _b14_cut_keeps_tbl():
        """F-01 行为断言: 声明段紧邻的表格行在裁声明后必须逐字保留 (不受护栏变异影响)."""
        text = "\n".join([_b14_body] * 28 + _b14_tbl + [_b14_orphan] + _b14_djc_tail)
        body = pdf_clean(text)
        return (all(body.count(row) == 1 for row in _b14_tbl)
                and "著作权" not in body)


    def _b14_a13():
        # 护栏①: marker 落在全文 ~30% 处 -> 不裁 (②③④ 单独都满足)
        text = "\n".join([_b14_body] * 13 + _b14_djc_tail + _b14_wl_tail[1:20])
        lines = _b14_lines(text)
        first = _b14_first(lines)
        pct = 100.0 * _b14_prefix(lines, first) / max(1, _b14_prefix(lines, len(lines)))
        cut = pdf_tail_cut(lines)
        ok = (first < len(lines) and pct < 100 * PDF_TAIL_MIN_PCT
              and cut == len(lines) and _b14_cut_keeps_tbl())
        return ok, f"cut={cut} n={len(lines)} marker@{pct:.1f}%"


    def _b14_a14():
        # 护栏②: marker 前只有 ~470 字 (<600) -> 不裁 (①③④ 单独都满足)
        tail = ["免责声明",
                "本报告版权归本公司所有，未经书面许可不得翻印。",
                "证券分析师承诺"]
        lines = _b14_lines("\n".join([_b14_body] * 10 + tail))
        first = _b14_first(lines)
        prefix = _b14_prefix(lines, first)
        cut = pdf_tail_cut(lines)
        ok = (first < len(lines) and prefix < PDF_TAIL_MIN_KEEP
              and cut == len(lines) and _b14_cut_keeps_tbl())
        return ok, f"cut={cut} n={len(lines)} prefix={prefix}"


    def _b14_a15():
        # 护栏③: marker 之后仍有正文特征 (「数据来源」) -> 不裁 (①②④ 单独都满足)
        tail = ["免责声明",
                "本报告版权归本公司所有，未经书面许可不得翻印。",
                "数据来源：Wind、国信证券经济研究所整理",
                "本公司具备中国证监会核准的业务资格。"]
        lines = _b14_lines("\n".join([_b14_body] * 30 + tail))
        first = _b14_first(lines)
        cut = pdf_tail_cut(lines)
        ok = (first < len(lines) and PDF_TAIL_BODY_HINT.search(" ".join(lines[first:]))
              and cut == len(lines) and _b14_cut_keeps_tbl())
        return ok, f"cut={cut} n={len(lines)} prefix={_b14_prefix(lines, first)}"


    def _b14_a16():
        # 护栏④: 标记起 4 行内只有 1 处标记 -> 不裁 (①②③ 单独都满足)
        tail = ["免责声明",
                "本报告中的信息或所表述的意见并不构成对任何人的投资建议，投资者应自行判断。",
                "本报告中的信息或所表述的意见并不构成对任何人的投资建议，投资者应自行判断。",
                "本报告中的信息或所表述的意见并不构成对任何人的投资建议，投资者应自行判断。"]
        lines = _b14_lines("\n".join([_b14_body] * 30 + tail))
        first = _b14_first(lines)
        cut = pdf_tail_cut(lines)
        n_mark = sum(len(PDF_TAIL_MARKER.findall(x))
                     for x in lines[first:first + PDF_TAIL_CONFIRM_LINES])
        ok = (first < len(lines) and n_mark < 2
              and cut == len(lines) and _b14_cut_keeps_tbl())
        return ok, f"cut={cut} n={len(lines)} window_marks={n_mark}"


    def _b14_a17():
        # 切点必须是标记行行首 (不是 marker 词本身): 前半行是正文时不得只砍词
        head = [_b14_body] * 30
        tail = ["本节内容为补充说明，详见如下：免责声明",
                "证券分析师承诺",
                "本人具有中国证券业协会授予的证券投资咨询执业资格并登记为证券分析师。"]
        lines = _b14_lines("\n".join(head + tail))
        cut = pdf_tail_cut(lines)
        body = pdf_clean("\n".join(head + tail))
        ok = (cut == len(head) and "本节内容为补充说明" not in body
              and body.count(_b14_body) == 30 and _b14_cut_keeps_tbl())
        return ok, f"cut={cut} n={len(lines)} tail={body[-12:]!r}"


    def _b14_a18():
        # 噪声抑制: 同一行重复 >=3 次 (页脚) 与命中东财水印都不作标记 -> 不裁
        text = "\n".join([_b14_neutral] * 24 + [_b14_wm3, _b14_neutral, _b14_neutral,
                                               _b14_gx_foot, _b14_neutral, _b14_neutral,
                                               _b14_gx_foot, _b14_neutral, _b14_gx_foot]
                         + [_b14_neutral] * 8)
        lines = _b14_lines(text)
        first = _b14_first(lines)
        cut = pdf_tail_cut(lines)
        ok = (first == len(lines) and cut == len(lines) and _b14_cut_keeps_tbl())
        return ok, f"cut={cut} n={len(lines)} suppressed_marker={first == len(lines)}"


    # ---- Batch 14 修复 F-05: 尾行残片必须锚定行首 + 正文句一律逃逸 (B14-19..B14-22) ----
    # 反例来自定向复验 F-05: 「www.」「电话」是子串模式, 旧码 (f29ec766) 会把含它们的
    # **完整正文句**整行删掉 (new body 1439 vs old 1480). 四条断言都同时钉住「该删的仍删」,
    # 防止把残片清理整个关掉.
    _b14_f05_url_sent = "更多数据请见 www.mysteel.com 公布的周度库存，该口径与上月一致。"
    _b14_f05_url_end = "本周港口库存环比下降 3.2%，明细数据请见 www.mysteel.com"
    _b14_f05_url_only = "https://www.mysteel.com/report/2026-11-10"
    _b14_f05_tel_sent = "如需核对本周库存明细，请联系研究员电话：010-88001234。"
    _b14_f05_tel_end = "本周库存环比下降 3.2%，明细请联系电话：010-88001234"
    _b14_f05_tel_only = "总机：0755-82130833"
    _b14_f05_post_only = "邮编：100033"
    _b14_f05_addr_only = "北京市西城区金融大街 7 号"
    _b14_f05_decl = ["免责声明",
                     "本报告版权归本公司所有，未经书面许可不得翻印。",
                     "证券分析师承诺"]

    def _b14_f05_text(kept):
        return "\n".join([_b14_body] * 30 + list(kept) + _b14_f05_decl)

    def _b14_f05_trim(kept):
        lines = _b14_lines(_b14_f05_text(kept))
        cut = pdf_tail_cut(lines)
        return lines, cut, pdf_tail_trim(lines, cut)

    def _b14_a19():
        # ① 正文句 + 行尾 URL (句中/句末两种形态) -> 整行保留
        # (旧码 FRAG 命中子串 www. -> 整行删)
        kept = [_b14_f05_url_sent, _b14_f05_url_end]
        body = pdf_clean(_b14_f05_text(kept))
        ok = (all(x in body for x in kept) and "免责声明" not in body
              and "版权归本公司所有" not in body and len(body) > 1200)
        return ok, f"len={len(body)} kept={[x in body for x in kept]}"

    def _b14_a20():
        # ② 整行只有一个 URL -> 当残片删除; 同一夹具的正文句(带 URL)仍保留
        kept = [_b14_f05_url_sent, _b14_f05_url_only]
        lines, cut, trim = _b14_f05_trim(kept)
        body = pdf_clean(_b14_f05_text(kept))
        ok = (cut < len(lines) and trim == cut - 1
              and _b14_f05_url_only in lines[trim:cut]
              and _b14_f05_url_sent in lines[:trim]
              and _b14_f05_url_only not in body and _b14_f05_url_sent in body)
        return ok, f"cut={cut} trim={trim} url_only_gone={_b14_f05_url_only not in body}"

    def _b14_a21():
        # ③ 正文句 + 行尾电话 (句中/句末两种形态) -> 整行保留
        # (旧码 FRAG 命中子串 电话 -> 整行删)
        kept = [_b14_f05_tel_sent, _b14_f05_tel_end]
        body = pdf_clean(_b14_f05_text(kept))
        ok = (all(x in body for x in kept) and "免责声明" not in body
              and "版权归本公司所有" not in body)
        return ok, f"len={len(body)} kept={[x in body for x in kept]}"

    def _b14_a22():
        # ④ 整行只有电话/邮编/门牌 -> 删除; 同一夹具的正文句(带电话)仍保留
        kept = [_b14_f05_tel_sent, _b14_f05_post_only,
                _b14_f05_addr_only, _b14_f05_tel_only]
        lines, cut, trim = _b14_f05_trim(kept)
        body = pdf_clean(_b14_f05_text(kept))
        gone = [_b14_f05_tel_only not in body, _b14_f05_post_only not in body,
                _b14_f05_addr_only not in body]
        ok = (cut < len(lines) and trim == cut - 3 and all(gone)
              and _b14_f05_tel_sent in body and _b14_f05_tel_sent in lines[:trim])
        return ok, f"cut={cut} trim={trim} gone={gone}"

    # ---- Batch 14 修复 F-06: 三个机制各自单点钉住 (B14-23..B14-25) ----
    # 定向复验实测: 破坏「正文逃逸 / 行首锚定 / 网址整段匹配」任一机制, 上面 B14-19..22
    # 仍 498 全绿 (三个机制互为冗余, 端到端断言只钉净效果). 下面三条各只钉一个机制,
    # 且只在对应该机制的变异下变红 (B14-23<-X1 去逃逸, B14-24<-X2 去行首锚定,
    # B14-25<-X3 网址改宽匹配) —— X1 正是 F-05 (正文误删) 的复发路径: 删掉逃逸逻辑,
    # 下面 B14-23 必须红, 否则缺陷会静默回来.
    _b14_f06_esc_url = "库存 3.2% www.mysteel.com"    # 逃逸「库存」-> 留; 去逃逸 -> URL 子串 -> 删
    _b14_f06_esc_yoy = "同比 www.example.com"         # 逃逸「同比」-> 留
    _b14_f06_edge = "www.mysteel.com 2026年11月港口调度数据一览已发布"  # 行首锚定 -> 删
    _b14_f06_ref = "参考 www.mysteel.com"             # 网址整段 -> 剩余 <=10 字 -> 删

    def _b14_a23():
        # X1: 去掉正文逃逸 -> 两行都成了「整行基本就是 URL」-> 整行删
        kept = [_b14_f06_esc_url, _b14_f06_esc_yoy]
        lines, cut, trim = _b14_f05_trim(kept)
        kept_in = all(x in lines[:trim] for x in kept)
        ok = (cut < len(lines) and trim == cut and kept_in
              and all(not _pdf_tail_frag(x) for x in kept))
        return ok, f"cut={cut} trim={trim} kept={kept_in}"

    def _b14_a24():
        # X2: 去掉行首锚定 -> URL 开头但剩余正文 >10 字 -> 由删变留
        kept = [_b14_f06_edge]
        lines, cut, trim = _b14_f05_trim(kept)
        body = pdf_clean(_b14_f05_text(kept))
        ok = (cut < len(lines) and trim == cut - 1
              and _b14_f06_edge in lines[trim:cut]
              and _b14_f06_edge not in body)
        return ok, f"cut={cut} trim={trim} gone={_b14_f06_edge not in body}"

    def _b14_a25():
        # X3: 网址改宽匹配 (只吃 www.) -> 剩余 ".mysteel.com" >10 字 -> 由删变留
        kept = [_b14_f06_ref]
        lines, cut, trim = _b14_f05_trim(kept)
        ok = (cut < len(lines) and trim == cut - 1
              and _b14_f06_ref in lines[trim:cut])
        return ok, f"cut={cut} trim={trim} dropped={_b14_f06_ref in lines[trim:cut]}"

    _b14_case("B14-13 护栏①: marker 在全文 ~30% 处 -> 不裁 (+ 表格行保留)", _b14_a13)
    _b14_case("B14-14 护栏②: marker 前 <600 字 -> 不裁 (+ 表格行保留)", _b14_a14)
    _b14_case("B14-15 护栏③: marker 后仍有「数据来源」正文特征 -> 不裁 (+ 表格行保留)", _b14_a15)
    _b14_case("B14-16 护栏④: 标记起 4 行内 <2 处标记 -> 不裁 (+ 表格行保留)", _b14_a16)
    _b14_case("B14-17 切点 = 标记行行首: 前半行正文完整保留 (+ 表格行保留)", _b14_a17)
    _b14_case("B14-18 噪声抑制: 重复页脚/东财水印不作标记 -> 不裁 (+ 表格行保留)", _b14_a18)
    _b14_case("B14-19 正文句+行尾 URL -> 整行保留 (残片锚定 F-05)", _b14_a19)
    _b14_case("B14-20 整行只有一个 URL -> 删除, 同夹具正文句保留 (F-05)", _b14_a20)
    _b14_case("B14-21 正文句+行尾电话 -> 整行保留 (残片锚定 F-05)", _b14_a21)
    _b14_case("B14-22 整行只有电话/邮编/门牌 -> 删除, 同夹具正文句保留 (F-05)", _b14_a22)
    _b14_case("B14-23 正文逃逸: 「库存/同比」短句+行尾 URL -> 整行保留 (F-06 X1)", _b14_a23)
    _b14_case("B14-24 行首锚定: 行首 URL + 行内正文 -> 整行删除 (F-06 X2)", _b14_a24)
    _b14_case("B14-25 网址整段匹配: 「参考 + 网址」-> 整行删除 (F-06 X3)", _b14_a25)

    # ---- Batch 15: 渲染前对已入库 snippet 再跑一次尾部声明剔除 (纵深防御) ----
    # 反例来源 (CTO 实测): 2026-11-10 15:14 抓取 / 15:42 推送的万联研报 (10784 字) 在 batch14
    # 上线前就已写入队列 snippet, 尾部 196 字版权段 (「不得以任何形式翻版…」+ 三地地址 +
    # www.wlzq.cn) 随全文分片原样推给了用户 —— 提取期的修复管不到已落盘的老条目,
    # 故在渲染前再裁一次。下面 _b15_old_clean 复刻 batch14 之前的 pdf_clean (只做行过滤 +
    # 空白归一, 不裁尾部声明), 用来构造「历史 snippet 真实形态」(单行扁平正文)。
    _b15_dt = datetime.fromtimestamp(_B11_TS, TZ_CN)
    _b15_decl = ("著作权", "保留一切", "不得以任何形式翻版", "版权所有",
                 "万联证券研究所", "金地中心", "www.")

    def _b15_old_clean(text):
        rows = [s.strip() for s in (text or "").splitlines()]
        rows = [s for s in rows if s and not PDF_TABLE.match(s) and not PDF_NUM.match(s)]
        good = [s for s in rows if len(s) >= 10 and len(PDF_CJK.findall(s)) >= 6]
        return re.sub(r"\s+", " ", " ".join(good or rows)).strip()

    def _b15_item(snippet, ident="b15"):
        return {"src": "东财研报", "ts": _B11_TS,
                "text": "10 月进出口数据解读｜万联证券 · 2026-11-10",
                "url": f"https://example.com/{ident}.pdf", "snippet": snippet,
                "key": f"研究:东财研报:{ident}"}

    _b15_raw = "\n".join([_b14_body] * 300 + _b14_wl_tail)
    _b15_dirty = _b15_old_clean(_b15_raw)      # 历史/在队条目: 带万联 196 字版权段 (整段声明尾 ~900 字) 的单行正文
    _b15_clean = pdf_clean(_b15_raw)           # batch14 上线后 pdf_text 的产物 (已裁)

    def _b15_a01():
        # 历史脏 snippet 走**完整渲染路径**: 尾部声明一个字符都不许出现在推送载荷里
        parts = research_parts(_b15_item(_b15_dirty), _b15_dt)
        joined = "".join(parts)
        hits = [w for w in _b15_decl if w in joined]
        ok = (not hits and joined.count(_b14_body) == 300
              and all(u16(p) <= MSG_MAX for p in parts))
        return ok, f"parts={len(parts)} hits={hits} body={joined.count(_b14_body)}"

    def _b15_a02():
        # 已干净 snippet: 缝隙逐字节不变, 分片与单条渲染都不丢字/不改字
        it = _b15_item(_b15_clean)
        parts = research_parts(it, _b15_dt)
        n_expect = len(split_research_body(_b15_clean))   # 未受本批影响的切分器
        short = "沪铜库存下降 3.2%，现货升水扩大。"
        line = research_line(_b15_item(short, "b15s"), _b15_dt)
        ok = (render_snippet(it) == it["snippet"]
              and len(parts) == n_expect
              and "".join(parts).count(_b14_body) == 300
              and short in line
              and render_snippet(_b15_item("")) == ""
              and render_snippet(_b15_item("  摘要  ")) == "摘要")
        return ok, f"seam={render_snippet(it) == it['snippet']} parts={len(parts)}/{n_expect}"

    def _b15_a03():
        # 幂等: trim(trim(x)) == trim(x); 且「已裁文本再裁」恒等于原串
        xs = [_b15_dirty, _b15_clean, " ".join(_b14_wl_tail), _b14_body]
        once = [pdf_tail_trim_text(x) for x in xs]
        twice = [pdf_tail_trim_text(y) for y in once]
        ok = (once == twice
              and pdf_tail_trim_text(_b15_clean) == _b15_clean
              and len(once[0]) >= 300 * len(_b14_body)
              and not any(w in once[0] for w in _b15_decl))
        return ok, f"idem={once == twice} lens={[len(y) for y in once]}"

    def _b15_a04():
        # 非研报渲染逐字节不变: 正文里带「免责声明/著作权/www.」也不许被裁,
        # 且**伪造**的 snippet 字段在无 url 条目上完全不可达 (研报判据只有 url)
        n = {"src": "财联社", "ts": _B11_TS,
             "text": "沪铜夜盘上涨 1.2%，免责声明：著作权归原作者所有，详见 www.example.com。"}
        bare = build_messages([n])[0][0]
        forged = build_messages([{**n, "snippet": _b15_dirty}])[0][0]
        ok = (forged == bare
              and all(w in bare for w in ("免责声明", "著作权", "www."))
              and callable(globals().get("pdf_tail_trim_text"))
              and callable(globals().get("render_snippet")))
        return ok, f"same={forged == bare} kept={all(w in bare for w in ('免责声明', '著作权', 'www.'))}"

    def _b15_a05():
        # 生产实测形态 (~10784 字正文 + 196 字尾): 末片不再以版权段收尾, 正文一字不丢,
        # 分片数只许不变或少一片 (少的那片正是全版权片)
        raw = "\n".join([_b14_body] * 307 + _b14_wl_tail)
        snip = _b15_old_clean(raw)
        parts = research_parts(_b15_item(snip, "b15prod"), _b15_dt)
        joined = "".join(parts)
        old_n = len(split_research_body(snip))
        ok = (len(parts) >= 2 and old_n - 1 <= len(parts) <= old_n
              and not any(w in joined for w in _b15_decl)
              and joined.count(_b14_body) == 307
              and all(u16(p) <= MSG_MAX for p in parts))
        return ok, f"parts={len(parts)} old_n={old_n} body={joined.count(_b14_body)}"

    def _b15_a06():
        # 正文后段自带声明词 -> 渲染期不误裁 (F-01 回归锚). 旧夹具的「免责条款」只有 4 字,
        # 被 _b15_old_clean 的 len>=10 过滤掉, 全文 0 个 marker -> 走快路径, 断言空转
        # (F-02). 这里改用两条**正文句**承载 2 处标记, 相距 >12 段, 且其后不含正文特征词:
        # confirm=None 会以第一处为裁切点丢掉后半段正文, confirm=12 的窗口内只有 1 处 ->
        # 四护栏拒裁. 先断言 marker >= 2, 保证走的不是「无标记快路径」.
        mid = "本文引用的第三方图表著作权归其原作者所有，本报告仅作研究参考。"
        later = "本报告仅供机构投资者参考，不构成任何投资建议。"
        plain = ("本节口径说明：进口关税按到岸价计征，与上一节测算一致。"
                 "行业景气度仍将延续，供给侧改革持续深化，需求端边际改善。"
                 "第三节对样本区间做了稳健性检验，结论与基准情形一致。"
                 "上述判断基于公开资料整理，不构成对任何机构的操作建议。")
        snip = _b15_old_clean("\n".join([_b14_body] * 30 + [mid] + [plain] * 4
                                         + [later] + [plain] * 2))
        markers = len(PDF_TAIL_MARKER.findall(snip))
        out = render_snippet(_b15_item(snip))
        ok = (markers >= 2 and out == snip
              and pdf_tail_trim_text(snip) == snip
              and "本节口径说明" in snip and "著作权" in snip and "本报告仅供" in snip)
        return ok, f"len={len(snip)} markers={markers} kept={out == snip}"

    def _b15_case(name, fn):
        try:
            ok, detail = fn()
        except Exception as e:                       # noqa: BLE001 - 旧码缺符号 -> 本条红
            ok, detail = False, f"{type(e).__name__}: {e}"
        check(name, bool(ok), str(detail))

    _b15_case("B15-01 历史脏 snippet: 渲染输出不含版权/地址/www 且正文 300 句全留", _b15_a01)
    _b15_case("B15-02 干净 snippet: 缝隙与单条渲染逐字节不变", _b15_a02)
    _b15_case("B15-03 幂等: trim(trim(x)) == trim(x) 且已裁文本再裁恒等", _b15_a03)
    _b15_case("B15-04 非研报渲染逐字节不变 (正文含声明词/伪造 snippet 均不受影响)", _b15_a04)
    _b15_case("B15-05 生产形态: 末片不再以版权段收尾, 正文不丢且分片数不增", _b15_a05)
    _b15_case("B15-06 正文自带声明词: 渲染期不误裁 (护栏在文本层同样生效)", _b15_a06)
    print(f"selftest: {'PASS' if not fails else str(len(fails)) + ' FAILED: ' + ', '.join(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
