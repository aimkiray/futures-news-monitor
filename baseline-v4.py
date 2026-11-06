#!/usr/bin/env python3
"""国内期货快讯聚合监控 (v4)

多源聚合 -> 跨源精确去重 + 时间窗近邻合并 -> 期货相关度过滤 -> 新鲜度闸门 -> Telegram 推送

数据源 (直连, 不走代理):
  财联社   www.cls.cn/v1/roll/get_roll_list        (需 sign=md5(sha1(排序query)))
  东财     np-listapi.eastmoney.com getFastNewsList
  同花顺   news.10jqka.com.cn/tapp/news/push/stock
  新浪     zhibo.sina.com.cn feed zhibo_id=152
  金属网   akshare.futures_news_shmet (有色)

凭据: bot token 只从环境变量 FUTNEWS_TOKEN 或 ~/.pi/agent/etc/futures-news.json 的
      token 字段读取; 两者皆无时打印错误并以非 0 退出 (源码中不留任何凭据)。

退出码: 0 正常 / 1 全源失败 / 2 发送失败 / 3 缺 token / 4 state 不可读 / 5 state 落盘失败

用法:
  futures-news-monitor.py [--dry-run] [--seed] [--all] [--limit N] [--stats] [--explain] [--selftest]
"""
from __future__ import annotations

import difflib
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

HOME = Path.home()
CONF = HOME / ".pi/agent/etc/futures-news.json"
STATE = HOME / ".pi/agent/state/futures-news-seen.json"
PENDING = HOME / ".pi/agent/state/futures-news-pending.json"   # 已发送但 state 落盘失败的待确认键
LOCK = HOME / ".pi/agent/state/futures-news.lock"
LOG = Path("/tmp/futures-news.out")
LOG_MAX = 2 * 1024 * 1024

TZ_CN = ZoneInfo("Asia/Shanghai")   # 源站与用户都在 +08:00; 不依赖宿主 TZ
FUTURE_TOL = 120                     # 未来时间容差(秒), 超出的条目直接丢弃
NEAR_WINDOW = 30 * 60                # 近邻合并时间窗(秒)
NEAR_RATIO = 0.90                    # 近邻合并阈值(实测依据见 w2-fix 报告 F-13)
NEAR_MIN_LEN = 8                     # 太短的签名不做近似比对(误合并风险高)
NUM_RE = re.compile(r"\d+")          # 数字不同视为不同事件, 避免「库存减12万吨/减10万吨」被误合并
ALERT_EVERY = 3600                   # 全源故障期间两条告警的最小间隔(秒)
BUDGET_S = 50                        # 全局抓取预算(秒), 远小于 launchd StartInterval=120

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
PROXY = os.environ.get("HTTPS_PROXY") or ""   # 未设置则纯直连
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
SEEN_MAX = 3000
MSG_MAX = 3800                       # 按 UTF-16 码元计长, 留出 4096 上限余量
ITEM_MAX = 700

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

# 品种核心词: 命中即视为期货相关 (强信号)
CORE = [
    "沪铜", "沪铝", "沪锌", "沪铅", "沪镍", "沪锡", "氧化铝", "不锈钢", "螺纹", "热卷",
    "铁矿石", "焦煤", "焦炭", "硅铁", "锰硅", "原油", "燃料油", "沥青", "液化石油气",
    "甲醇", "PTA", "乙二醇", "短纤", "聚丙烯", "聚乙烯", "PVC", "尿素", "纯碱", "玻璃",
    "纸浆", "橡胶", "苯乙烯", "烧碱", "豆粕", "菜粕", "豆油", "棕榈油", "玉米", "淀粉",
    "白糖", "棉花", "棉纱", "生猪", "鸡蛋", "苹果期货", "红枣", "花生", "工业硅",
    "碳酸锂", "多晶硅", "原木", "集运指数", "期铜", "伦铜", "伦镍", "LME", "COMEX",
    "主力合约", "期货",
    # 关键指标/品种裸名 (F-16): 上期所/大商所主力品种与产业指标
    "黄金", "白银", "沪金", "沪银", "铂", "钯", "能繁母猪", "成品油", "猪价", "鸡苗",
    "低硫燃料油", "菜籽", "菜油", "粳米", "瓦楞纸", "双胶纸", "PX", "瓶片", "纯苯",
]
# A股公司/大盘新闻噪声: 仅在无强信号时用于剔除
NOISE = [
    "解禁", "股东", "减持", "增持", "回购", "业绩", "净利", "营收", "股价", "市值",
    "中标", "签约", "战略合作", "排产", "新品", "发布会", "上市首日", "涨停", "连板",
    "龙虎榜", "定增", "增发", "并购", "重组", "股权激励", "员工持股", "分红", "预增",
    "预减", "扭亏", "亏损", "三季报", "半年报", "年报", "招股", "苹果公司", "iPhone",
    # 指数/大盘/个股语境 (F-10)
    "沪指", "上证指数", "深证成指", "深成指", "创业板指", "创业板", "科创50", "北交所",
    "两市", "成交额", "北向资金", "南向资金", "涨跌家数", "大盘", "个股", "板块", "概念股",
    "沪深300", "中证500", "中证1000", "上证50", "A股", "港股", "恒生指数", "恒生科技",
    "融资余额", "主力资金", "沪股通", "深股通", "券商", "公募", "私募", "基金净值",
]
DEFAULT_KEYWORDS = [
    "铜", "铝", "锌", "铅", "镍", "锡", "铁矿", "高炉", "电炉", "粗钢", "液化气", "LPG",
    "丙烯", "瓶片", "菜油", "豆一", "豆二", "早籼", "粳米", "欧线", "仓单", "基差",
    "升贴水", "交割", "持仓", "夜盘", "开盘", "收盘", "套保", "期现", "库存", "开工率",
    "检修", "减产", "停产", "复产", "限产", "产能", "到港", "装置", "招标", "收储",
    "抛储", "配额", "关税", "出口", "进口", "制裁", "罢工", "矿山", "冶炼",
    "美联储", "非农", "OPEC", "大宗商品",
]
# 全大写缩写需要词边界, 否则 PE 命中 APPLE / PP 命中 APP
ACRONYMS = ["PE", "PP", "CPI", "PMI"]

# 时间不可解析计数 (每轮清零), 供 stats 输出
TS_BAD: dict[str, int] = {}

TS_MIN = 946684800                   # 2000-01-01, 早于此的时间串视为解析失败
TS_MAX_AHEAD = 366 * 86400           # 允许源站领先至多一年; 近期未来值由 FUTURE_TOL 丢弃并计数


# ------------------------------------------------------------------ utils
def log(msg: str) -> None:
    print(f"[{datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def err(msg: str) -> None:
    """对用户可见的失败: 同时进日志与 stderr."""
    log(msg)
    print(msg, file=sys.stderr, flush=True)


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


def http(url: str, referer: str | None = None, tries: int = 3,
         deadline: float | None = None) -> bytes:
    """curl 直连取数据; 非 2xx 视作失败并重试; deadline 为全局预算 (time.monotonic)."""
    last = ""
    for i in range(tries):
        left = (deadline - time.monotonic()) if deadline else 30.0
        if left < 2:
            raise RuntimeError(f"http 预算耗尽 ({last or 'no-attempt'})")
        max_time = max(2, min(12, int(left)))
        cmd = ["curl", "-sS", "--noproxy", "*", "--max-time", str(max_time), "-A", UA,
               "-w", "\n%{http_code}", url]
        if referer:
            cmd += ["-H", f"Referer: {referer}"]
        try:
            raw = subprocess.run(cmd, capture_output=True, timeout=max_time + 4).stdout
            body, _, code = raw.rpartition(b"\n")
            last = code.decode(errors="ignore").strip()
            if body and last.startswith("2"):
                return body
        except Exception as e:
            last = f"{type(e).__name__}"
        nap = float(1 + i)
        if deadline:
            nap = min(nap, max(0.0, deadline - time.monotonic() - 1.0))
        if nap > 0:
            time.sleep(nap)
    raise RuntimeError(f"http failed ({last or 'empty'})")


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
    return base[:60] or norm_text(t)[:60]


def first_hit(text: str, words: list[str]) -> str:
    for w in words:
        if w in ACRONYMS:
            if re.search(rf"(?<![A-Za-z]){w}(?![A-Za-z])", text):
                return w
        elif w in text:
            return w
    return ""


def hit(text: str, words: list[str]) -> bool:
    return bool(first_hit(text, words))


def filter_reason(text: str, keywords: list[str], use_filter: bool) -> str:
    """返回丢弃原因 (空串=保留); 供 is_relevant 与丢弃样本日志共用."""
    t = (text or "").strip()
    if SKIP_RE.search(strip_decor(t)):
        return "栏目/日历"
    if len(norm_text(t)) < 4:
        return "内容过短"
    if not use_filter:
        return ""
    if hit(t, CORE):
        return ""
    if hit(t, keywords):
        noise = first_hit(t, NOISE)
        if noise:
            return f"噪声:{noise}"
        return ""
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


def call_bounded(fn, timeout: float):
    """守护线程执行 fn, 超时抛 TimeoutError (F-14: akshare 没有超时参数)."""
    box: dict = {}

    def runner() -> None:
        try:
            box["v"] = fn()
        except BaseException as e:                   # noqa: BLE001 - 原样回抛给调用方
            box["e"] = e

    th = threading.Thread(target=runner, daemon=True)
    th.start()
    th.join(max(0.1, timeout))
    if th.is_alive():
        raise TimeoutError(f"超时 {timeout:.1f}s")
    if "e" in box:
        raise box["e"]
    return box.get("v")


def src_shmet(deadline: float | None = None) -> list[dict]:
    left = (deadline - time.monotonic()) if deadline else 30.0
    if left < 2:
        raise RuntimeError("金属网: 全局预算耗尽")

    def fetch():
        saved = {k: os.environ.pop(k, None) for k in PROXY_VARS}
        try:
            import akshare as ak
            return ak.futures_news_shmet(symbol="全部")
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    df = call_bounded(fetch, min(left, 20))
    out = []
    for _, r in df.iterrows():
        text = str(r.get("内容", "")).strip()
        if not text:
            continue
        ts = parse_ts(str(r.get("发布时间", "")))
        if not ts:
            bad_ts("金属网")
            continue
        out.append({"src": "金属网", "ts": ts, "text": text})
    return out


SOURCES = [("财联社", src_cls), ("东财", src_em), ("同花顺", src_ths),
           ("新浪", src_sina), ("金属网", src_shmet)]


# ------------------------------------------------------------------ telegram
def resolve_token(conf: dict) -> str:
    """token 只来自环境变量或 0600 配置文件; 源码不含凭据 (F-03)."""
    return str(os.environ.get("FUTNEWS_TOKEN") or conf.get("token") or "").strip()


def tg(method: str, payload: dict | None, tk: str, proxy: bool, timeout: int = 25) -> dict:
    url = f"https://api.telegram.org/bot{tk}/{method}"
    data = urllib.parse.urlencode(payload).encode() if payload else None
    handler = (urllib.request.ProxyHandler({"http": PROXY, "https": PROXY}) if proxy
               else urllib.request.ProxyHandler({}))
    with urllib.request.build_opener(handler).open(
            urllib.request.Request(url, data=data), timeout=timeout) as r:
        return json.loads(r.read().decode())


def discover_chat_id(conf: dict, tk: str) -> int | None:
    try:
        for upd in reversed(tg("getUpdates", {"timeout": 0, "limit": 20}, tk, False).get("result", [])):
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


def send(text: str, conf: dict, tk: str, depth: int = 0) -> bool:
    chat_id = conf.get("chat_id") or discover_chat_id(conf, tk)
    if not chat_id:
        log("no chat_id (先给 bot 发一条消息)")
        return False
    base = {"chat_id": chat_id, "disable_web_page_preview": "true"}
    # 直连优先(快)；只有显式配置了 PROXY 才追加代理重试腿
    attempts = ((False, 8), (True, 25)) if PROXY else ((False, 25),)
    for proxy, timeout in attempts:
        try:
            tg("sendMessage", {**base, "text": text, "parse_mode": "HTML"}, tk, proxy, timeout)
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
                return False                             # 限流: 下轮再推, 不标记已读
        except Exception as e:
            log(f"send proxy={proxy} failed: {type(e).__name__}: {e}")
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
            f"{'' if saved else ' (记账落盘失败, 下轮重试)'}")
    except Exception as e:
        log(f"stale 记账异常, 已忽略: {type(e).__name__}: {e}")


def alert_all_failed(seen: dict, errors: list[str], conf: dict, tk: str, dry: bool) -> None:
    """全源失败告警状态机 (F-15): 首轮发 1 条 -> 之后每 ALERT_EVERY 汇总 -> 恢复另有通知."""
    now = int(time.time())
    alert = seen.get("_alert")
    alert = alert if isinstance(alert, dict) else {}
    detail = ", ".join(errors) or "-"
    if alert.get("failing"):
        alert["rounds"] = int(alert.get("rounds") or 0) + 1
        last = int(alert.get("last_sent") or 0)
        if last and now - last < ALERT_EVERY:
            log(f"全源故障第 {alert['rounds']} 轮, 告警已抑制 (上次 {now - last}s 前)")
            seen["_alert"] = alert
            return
        msg = f"⚠️ 期货快讯源仍全部失败 (累计 {alert['rounds']} 轮): {detail}"
    else:
        alert = {"failing": True, "since": now, "rounds": 1, "last_sent": 0, "alerts": 0}
        msg = f"⚠️ 期货快讯源全部抓取失败: {detail}"
    if dry:
        log("(dry-run) 告警未发送")
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
    """从全源故障恢复: 发 1 条恢复通知并复位告警状态."""
    alert = seen.get("_alert")
    if not isinstance(alert, dict) or not alert.get("failing"):
        return False
    rounds = int(alert.get("rounds") or 0)
    since = int(alert.get("since") or 0)
    mins = max(0, (int(time.time()) - since) // 60)
    if not dry:
        ok = send(f"✅ 期货快讯源已恢复 (故障 {rounds} 轮 / 约 {mins} 分钟)", conf, tk)
        log(f"恢复通知{'已发送' if ok else '发送失败'} (故障 {rounds} 轮)")
    seen["_alert"] = {"failing": False, "since": 0, "rounds": 0, "last_sent": 0, "alerts": 0}
    return True


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
        line = f"<b>{esc(it['src'])} {stamp}</b>\n{esc(body)}"
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


def main() -> int:
    args = sys.argv[1:]
    dry = "--dry-run" in args
    seed = "--seed" in args
    force_all = "--all" in args
    stats = "--stats" in args or dry
    explain = "--explain" in args

    conf = load_json(CONF, {})
    limit = 10
    try:
        limit = max(1, int(conf.get("limit") or 10))
    except (TypeError, ValueError):
        limit = 10
    if "--limit" in args:
        try:
            limit = max(1, int(args[args.index("--limit") + 1]))
        except (IndexError, ValueError):
            pass

    keywords = conf.get("keywords") or DEFAULT_KEYWORDS
    use_filter = conf.get("filter", "futures") == "futures" and not force_all

    rotate_log()

    # 单实例锁: 防止 launchd 上一轮未结束又起一轮 -> 重复推送
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    lock_fh = open(LOCK, "w")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("另一实例在跑, 本轮跳过")
        return 0

    # state: 文件不存在 = 首轮; 存在但不可读 = 告警 + 不覆盖 + 非 0 (F-04)
    seen, state_problem = load_state(STATE)
    if state_problem:
        err(f"state 文件不可读, 本轮不推送也不覆盖: {state_problem} ({STATE})")
        return 4

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
    deadline = time.monotonic() + budget

    TS_BAD.clear()
    items, errors = [], []
    for name, fn in SOURCES:
        left = deadline - time.monotonic()
        if left < 2:
            errors.append(f"{name}(预算耗尽)")
            log(f"{name}: 跳过, 全局预算 {budget}s 已耗尽")
            continue
        t0 = time.monotonic()
        try:
            got = fn(deadline)
            items.extend(got)
            bad = TS_BAD.get(name, 0)
            log(f"{name}: {len(got)} 条 ({time.monotonic() - t0:.1f}s)"
                + (f", 丢弃 {bad} 条时间不可解析" if bad else ""))
        except Exception as e:
            errors.append(name)
            log(f"{name} FAILED: {type(e).__name__}: {str(e)[:90]} "
                f"({time.monotonic() - t0:.1f}s)")

    if not items:
        log(f"全部源失败 {errors}")
        alert_all_failed(seen, errors, conf, tk, dry)
        if not dry and not save_json(STATE, prune(seen)):
            err("告警状态落盘失败")
        return 1

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
        key = dedupe_key(it["text"])
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
        reason = filter_reason(it["text"], keywords, use_filter)
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
    max_age = int(conf.get("max_age_min", 90)) * 60
    stale = [it for it in fresh if now_ts - it["ts"] > max_age]
    fresh = [it for it in fresh if now_ts - it["ts"] <= max_age]
    # F-06: 超过 limit 的部分不进入待推队列, 计数后丢弃 (不再声称"下一轮继续推")
    backlog = fresh[:max(0, len(fresh) - limit)]
    batch = fresh[len(backlog):]

    if stats:
        log(f"raw={len(items)} merged_exact={len(merged_exact)} merged={len(merged)} "
            f"near_merged={len(near_pairs)} relevant={len(relevant)} fresh={len(fresh)} "
            f"stale={len(stale)} dropped_backlog={len(backlog)} "
            f"dropped_ts={TS_BAD.get('total', 0)} dropped_future={dropped_future} "
            f"dropped_short={dropped_short} filter={'on' if use_filter else 'off'} "
            f"errors={errors or '-'}")
    for reason, it in dropped[:5]:
        log(f"  丢弃[{reason}] {it['src']}: {it['text'][:42]}")
    if explain:
        print(f"--- 过滤样本 (共 {len(dropped)} 条) ---")
        for reason, it in dropped[:20]:
            print(f"  [{reason}] {it['src']} {it['text'][:80]}")
        for prev, dup, ratio in near_pairs[:20]:
            print(f"  [近邻合并 {ratio:.3f}] {dup['src']} {dup['text'][:46]} "
                  f"-> {prev['src']} {prev['text'][:46]}")

    if dry:
        for it in fresh[-15:]:
            print(f"  {datetime.fromtimestamp(it['ts'], TZ_CN).strftime('%H:%M')} "
                  f"[{it['src']}] {it['text'][:70]}")
        return 0

    if not fresh:
        record_stale(seen, stale, max_age)
        return 0

    msgs = build_messages(batch)
    sent: list[dict] = []
    failed = False
    for idx, (text, used) in enumerate(msgs, 1):
        if not send(text, conf, tk):
            failed = True
            err(f"第 {idx}/{len(msgs)} 批发送失败 (u16={u16(text)}), 已送达 {len(sent)} 条")
            break
        sent.extend(used)
    ok = bool(sent) and not failed

    if ok:                                            # 只有真正送达才标记已读
        for it in sent + backlog:
            seen[it["key"]] = now_ts
        if not save_json(STATE, prune(seen)):
            keys = sorted({it["key"] for it in sent} | set(pending))
            wrote = write_pending(keys)
            err(f"state 落盘失败, 已写待确认标记{'成功' if wrote else '失败'}, "
                f"下轮不会重复推送同一批")
            return 5
        clear_pending()
    log(f"pushed {len(sent)}/{len(fresh)} msgs={len(msgs)} ok={ok} "
        f"dropped_backlog={len(backlog) if ok else 0}")
    record_stale(seen, stale, max_age)                # F-08: 推送之后记账, 失败不阻断
    return 0 if ok else 2


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
    check("苹果公司不误伤", not hit("苹果公司发布iPhone 18", CORE))
    check("苹果期货命中", hit("苹果期货主力合约涨2%", CORE))
    check("PE 词边界", not hit("APPLE 发布会", ["PE", "PP"]))
    check("PE 命中", hit("PE 价格", ["PE", "PP"]))
    check("F-21 is_relevant 为模块级可调用",
          callable(globals().get("is_relevant")) and is_relevant("螺纹钢库存下降", DEFAULT_KEYWORDS))

    print(f"selftest: {'PASS' if not fails else str(len(fails)) + ' FAILED: ' + ', '.join(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
