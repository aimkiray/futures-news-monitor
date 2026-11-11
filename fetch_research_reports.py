#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_research_reports.py — U1: 东财研报一年窗口抓取 + PDF 正文提取.

规格源: spec-source.md (候选规格 A) §4. 流程:
  阶段 L  qType=1→2 翻页抓取 (限速 1.0-2.0s 抖动, 页级 3 试, 连 5 败熔断 exit 3)
          -> 归一化 + filter_reason 宽口径 + STRONG/「期货」紧口径
          -> 追加 research-records.jsonl
  阶段 P  tight 条目逐篇: reports\\<record_key>.pdf 缓存优先 -> monitor.http 下载
          -> pdftotext 前 8 页 -> pdf_clean(3000) -> 追加 research-extracts.jsonl
          瞬时失败 ≤3 pass (pass 间 sleep 60); 终态 ok|no_text|failed|no_pdf
  阶段 A  汇总 -> research-raw.json (原子写)

用法:
    python fetch_research_reports.py [--max-pages N] [--max-pdf N] [--fresh] [--no-pdf]
      --max-pages N  调试帽: 每个 qType 最多抓 N 页 (默认 0 = 不限; 截断的 qType
                     在 fetch-state.json 记 done=false + debug_truncated, 续跑继续翻页;
                     截断轮退出码 2 且不写 research-raw.json)
      --max-pdf N    调试帽: 本次运行最多处理 N 篇 PDF (默认 0 = 不限)
      --fresh        清除既有中间状态 (fetch-state.json / 两个 jsonl / reports\\*.pdf) 重跑
      --no-pdf       调试: 跳过阶段 P (遗留 pending -> 退出码 2, 不写 research-raw.json)

退出码: 0 全部完成且 research-raw.json 已落盘; 2 未完成已安全保存断点 (Ctrl-C /
        --max-pages 调试帽截断 / --max-pdf 截断 / 仍有 pending); 3 列表 API 熔断;
        4 预检失败 (pdftotext 不可用 / 产物根不可写). 非 0 一律不产出不改写
        research-raw.json.
"""
from __future__ import annotations

# ---------------- [canonical] C-4 Windows 兼容引导段 (import monitor 之前, 逐字节语义) ----------------
import os
import sys
import types
import zoneinfo
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.modules.setdefault("fcntl", types.ModuleType("fcntl"))            # (1) fcntl 占位
try:                                                                 # (2) tzdata 探测回退
    zoneinfo.ZoneInfo("Asia/Shanghai")
except Exception:
    zoneinfo.ZoneInfo = lambda key: timezone(timedelta(hours=8))
os.environ.setdefault(                                               # (3) pdftotext 注入
    "FUTNEWS_PDFTOTEXT",
    r"C:\Users\laevatain\scoop\apps\git\2.51.0\mingw64\bin\pdftotext.exe")
sys.path.insert(0, str(Path(__file__).resolve().parent))
import monitor                                                       # noqa: E402
if not (monitor.PDFTOTEXT and os.path.exists(monitor.PDFTOTEXT)):
    monitor.PDFTOTEXT = r"C:\Users\laevatain\scoop\apps\git\2.51.0\mingw64\bin\pdftotext.exe"
if not os.path.exists(monitor.PDFTOTEXT):
    sys.exit(4)                                                      # preflight 失败, 不带病进入抓取

import argparse                                                      # noqa: E402
import hashlib                                                       # noqa: E402
import json                                                          # noqa: E402
import math                                                          # noqa: E402
import platform                                                      # noqa: E402
import random                                                        # noqa: E402
import subprocess                                                    # noqa: E402
import time                                                          # noqa: E402

# ---------------- 路径与常量 (C-1) ----------------
ART = Path(r"C:\Users\laevatain\artifacts\research-reports-2026-11-07")
RECORDS = ART / "research-records.jsonl"        # 阶段 L 追加式记录流
EXTRACTS = ART / "research-extracts.jsonl"      # 阶段 P 追加式提取结果流
STATE = ART / "fetch-state.json"                # 翻页 + 提取检查点
PROGRESS = ART / "fetch-progress.log"           # 进度行日志 (<=300s 间隔)
REPORTS_DIR = ART / "reports"                   # PDF 原件缓存 + 本单元报告
ANALYSIS_DIR = ART / "analysis"                 # U2 分片输出目录 (预检需可建)
RAW_OUT = ART / "research-raw.json"             # 本单元汇总输出
MONITOR_PY = Path(__file__).resolve().parent / "monitor.py"

# §1-2 模板原值 (beginTime/endTime 固定, 禁止动态计算); 仅 {pn}/{qt} 为变量
LIST_TEMPLATE = ("https://reportapi.eastmoney.com/report/list?industryCode=*&pageSize=100"
                 "&industry=*&rating=&ratingChange=&beginTime=2025-11-07&endTime=2026-11-07"
                 "&pageNo={pn}&fields=&qType={qt}&orgCode=&code=*&rcode=&p=1&pageNum=1")
REFERER = "https://data.eastmoney.com/report/"
PDF_URL_TEMPLATE = "https://pdf.dfcfw.com/pdf/H3_{info}_1.pdf"
WINDOW = {"begin": "2025-11-07", "end": "2026-11-07"}
EXTRACT_LIMIT = 3000                            # §4.5.3 允许的唯一数字面量 (AI 分析深度)
TZ8 = timezone(timedelta(hours=8))              # 固定 +08:00, 与 monitor TZ_CN 口径一致
MISSING_TITLE = "缺标题"                        # §4.4.2 本地哨兵 (不调用 monitor)
PROGRESS_GAP_S = 250                            # 进度行兜底间隔 (<300s 硬约束)
PAGE_TRIES = 3                                  # §4.3 同页最多尝试次数
PAGE_RETRY_SLEEP = 30                           # §4.3 页失败后重试前休眠
BREAKER_LIMIT = 5                               # §4.3 连续页级失败熔断阈值
MAX_PASSES = 3                                  # §4.5.7 瞬时失败重试轮数上限
PASS_SLEEP = 60                                 # §4.5.7 pass 间隔
TERMINAL = {"ok", "no_text", "failed", "no_pdf"}

# ---------------- 运行态 ----------------
ITEMS: dict[str, dict] = {}                     # record_key -> 归一化记录 (§5 items schema)
INFO_MAP: dict[str, str] = {}                   # infoCode -> record_key (去重索引)
STATE_D: dict = {}                              # fetch-state.json 内存镜像
CTR = {"kept": 0, "tight": 0, "pdf_ok": 0,
       "pdf_no_text": 0, "pdf_failed": 0}       # 进度行实时计数
SEQ = 0                                         # 全局扫描序 (0 起连续)
CONSEC_FAIL = 0                                 # 跨页连续页级失败 (成功清零)
MADE_REQUEST = False                            # 首请求前不做限速 sleep
_LAST_PROG = 0.0                                # 上次进度行时刻 (monotonic)


def atomic_write(path: Path, data: bytes) -> None:
    """tmp + os.replace 原子落盘 (§2.2: JSONL 追加外一律原子化)."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def save_json(path: Path, obj) -> None:
    atomic_write(path, json.dumps(obj, ensure_ascii=False).encode("utf-8"))


def append_jsonl(path: Path, obj) -> None:
    """O_APPEND 追加单行, 一次写完且自带 \\n (§2.2)."""
    line = json.dumps(obj, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8", newline="") as f:
        f.write(line)


def save_state() -> None:
    save_json(STATE, STATE_D)


def progress(phase: str, qtype="-", page="-", msg="", force=False) -> None:
    """§4.7 进度行: stdout 回显 + fetch-progress.log 追加; 相邻 <=300s 由 force/兜底保证."""
    global _LAST_PROG
    now = time.monotonic()
    if not force and now - _LAST_PROG < PROGRESS_GAP_S:
        return
    _LAST_PROG = now
    line = (f'{datetime.now(TZ8).isoformat(timespec="seconds")} '
            f"phase={phase} qType={qtype} page={page} "
            f"scanned={STATE_D.get('stats', {}).get('fetched_total', 0)} "
            f"kept={CTR['kept']} tight={CTR['tight']} pdf_ok={CTR['pdf_ok']} "
            f"pdf_no_text={CTR['pdf_no_text']} pdf_failed={CTR['pdf_failed']}"
            + (f" msg={msg}" if msg else ""))
    try:
        print(line, flush=True)
    except UnicodeEncodeError:                   # Windows 控制台 gbk 回显兜底, 文件不受影响
        print(line.encode("utf-8", "replace").decode("gbk", "replace"), flush=True)
    with open(PROGRESS, "a", encoding="utf-8", newline="") as f:
        f.write(line + "\n")


# ---------------- 预检 (§4.2) ----------------
def preflight() -> None:
    """产物根/子目录/jsonl 可建可写; 失败退出码 4."""
    try:
        ART.mkdir(parents=True, exist_ok=True)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
        for p in (RECORDS, EXTRACTS, PROGRESS):
            with open(p, "a", encoding="utf-8", newline=""):
                pass
    except OSError as e:
        print(f"preflight: 产物根不可写 {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(4)


# ---------------- 断点续跑重建 (§4.6) ----------------
def default_state() -> dict:
    return {"phase": "list",
            "pages": {"1": {"next_page": 1, "done": False, "hits": 0,
                            "stopped_reason": "", "pages_fetched": 0},
                      "2": {"next_page": 1, "done": False, "hits": 0,
                            "stopped_reason": "", "pages_fetched": 0}},
            "extract_done": {},
            "stats": {"fetched_total": 0, "dups_skipped": 0},
            "generated_at": ""}


def apply_extract(rec: dict, status: str, extract: str, snippet: str,
                  err: str, attempts: int) -> None:
    rec["extract_status"] = status
    rec["extract"] = extract
    rec["extract_chars"] = len(extract)
    rec["snippet"] = snippet
    rec["extract_error"] = err
    rec["extract_attempts"] = attempts


def load_resume() -> None:
    """records jsonl (后写覆盖) -> extracts jsonl -> fetch-state.json (§4.6 顺序)."""
    global SEQ
    bad_lines = 0                                # 坏行计数 (坏 JSON / 合法 JSON 但非 dict)
    if RECORDS.exists():
        with open(RECORDS, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                if not isinstance(rec, dict):    # "123"/[] 等合法但非记录 -> 拒载
                    bad_lines += 1
                    continue
                key = rec.get("record_key")
                if key:
                    ITEMS[key] = rec             # 后写覆盖先写
    for rec in ITEMS.values():
        ic = rec.get("infoCode")
        if ic:
            INFO_MAP[ic] = rec["record_key"]
    SEQ = (max((r.get("seq", -1) for r in ITEMS.values())) + 1) if ITEMS else 0

    ext_results: dict[str, dict] = {}
    if EXTRACTS.exists():
        with open(EXTRACTS, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                if not isinstance(r, dict):      # 同上: 非 dict 行拒载
                    bad_lines += 1
                    continue
                if r.get("record_key"):
                    ext_results[r["record_key"]] = r   # 后写覆盖先写, 回建终态

    state_ok = False
    try:
        loaded = json.loads(STATE.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):             # 合法 JSON 但非 dict 不带病合并
            STATE_D.update(loaded)
            state_ok = True
    except Exception:
        pass
    if ITEMS and not state_ok:
        # records 非空但 state 缺/损: 续跑会全量重抓致 dups≈fetched, §5 恒等式必破;
        # 拒绝带病加载 (断点数据完好, 提示 --fresh 重跑)
        progress("resume", msg="FATAL fetch-state.json 缺失/损坏而 records 非空, 拒绝续跑, 请 --fresh",
                 force=True)
        sys.exit(2)
    base = default_state()
    for k, v in base.items():
        STATE_D.setdefault(k, v)
    if not isinstance(STATE_D.get("pages"), dict):      # 畸形 pages 容器回退默认
        STATE_D["pages"] = {}
    for qt in ("1", "2"):
        pg = STATE_D["pages"].setdefault(qt, {})
        if not isinstance(pg, dict):                    # 子项非 dict 回退默认
            pg = STATE_D["pages"][qt] = {}
        for k, v in base["pages"][qt].items():
            pg.setdefault(k, v)
    if not isinstance(STATE_D.get("stats"), dict):      # 畸形 stats 同理回退
        STATE_D["stats"] = dict(base["stats"])
    if bad_lines:
        progress("resume", msg=f"warn load_resume 跳过坏行={bad_lines}", force=True)

    # extract_done 只填 extracts jsonl 未覆盖的缺口 (jsonl 写在先, 信息量更全)
    ext_done = STATE_D.get("extract_done", {})
    if not isinstance(ext_done, dict):
        ext_done = {}
    for key, v in ext_done.items():
        rec = ITEMS.get(key)
        if rec is not None and key not in ext_results:
            st = v.get("status", "pending") if isinstance(v, dict) else "pending"
            try:
                att = int(v.get("attempts", 0)) if isinstance(v, dict) else 0
            except (TypeError, ValueError):
                att = 0
            if st == "ok":
                # jsonl 无该键佐证却复活 ok+空 extract -> 违反 §5 不变式;
                # 降级 pending 让阶段 P 重新提取 (保守方向)
                st = "pending"
            apply_extract(rec, st, "", "",
                          rec.get("extract_error", ""), att)
    for key, r in ext_results.items():
        rec = ITEMS.get(key)
        if rec is not None:
            try:
                att = int(r.get("attempts", 0))
            except (TypeError, ValueError):
                att = 0
            apply_extract(rec, r.get("extract_status", "pending"),
                          r.get("extract", ""), r.get("snippet", ""),
                          r.get("extract_error", ""), att)

    CTR["kept"] = sum(1 for r in ITEMS.values() if r.get("keep"))
    CTR["tight"] = sum(1 for r in ITEMS.values() if r.get("tight"))
    CTR["pdf_ok"] = sum(1 for r in ITEMS.values() if r.get("extract_status") == "ok")
    CTR["pdf_no_text"] = sum(1 for r in ITEMS.values() if r.get("extract_status") == "no_text")
    CTR["pdf_failed"] = sum(1 for r in ITEMS.values() if r.get("extract_status") == "failed")


# ---------------- 阶段 L: 列表抓取 (§4.3) ----------------
def fetch_page(qt: int, page: int) -> tuple[list, int]:
    """单页抓取: monitor.http 传输; 页级 3 试 + sleep(30); 连 5 败熔断 exit 3."""
    global CONSEC_FAIL, MADE_REQUEST
    if MADE_REQUEST:                             # 相邻两次页面请求之间限速
        time.sleep(1.0 + random.random())        # 1.0-2.0s 均匀抖动
    MADE_REQUEST = True
    url = LIST_TEMPLATE.replace("{pn}", str(page)).replace("{qt}", str(qt))
    last_err = ""
    for attempt in range(PAGE_TRIES):
        try:
            body = monitor.http(url, REFERER, deadline=time.monotonic() + 30)
            d = json.loads(body)
            data = d.get("data") if isinstance(d, dict) else None
            if not isinstance(data, list):
                raise ValueError("data 非 list")
            try:
                hits = int(d.get("hits") or 0)
            except (TypeError, ValueError):
                hits = 0
            CONSEC_FAIL = 0
            return data, hits
        except KeyboardInterrupt:
            raise
        except Exception as e:
            last_err = f"{type(e).__name__}"
            CONSEC_FAIL += 1
            progress("list", qt, page, msg=f"页失败 {last_err} consec={CONSEC_FAIL} attempt={attempt + 1}", force=True)
            if CONSEC_FAIL >= BREAKER_LIMIT:
                save_state()
                progress("list", qt, page, msg=f"熔断 连续{CONSEC_FAIL}败", force=True)
                sys.exit(3)
            if attempt < PAGE_TRIES - 1:
                time.sleep(PAGE_RETRY_SLEEP)
    save_state()
    progress("list", qt, page, msg=f"页{PAGE_TRIES}试耗尽 {last_err}", force=True)
    sys.exit(3)


def normalize(it: dict, qt: int, seq: int) -> dict:
    """§4.4 归一化 + 两级过滤; 返回 §5 items[] 全字段记录."""
    def cs(key):
        return monitor.clean_text(it.get(key))

    title = cs("title")
    industry = cs("industryName")
    info = cs("infoCode")
    author_raw = it.get("author")
    if isinstance(author_raw, list):
        author_raw = ",".join(str(x) for x in author_raw)      # §5: 数组以 , 连接
    author = monitor.clean_text(author_raw)
    try:
        attach = int(it.get("attachPages"))
    except (TypeError, ValueError):
        attach = None                                          # 非数字 -> null
    if not title:
        reason = MISSING_TITLE                                 # 哨兵, 不调用 monitor
    else:
        reason = monitor.filter_reason(f"{title} {industry}",  # stage1: title+industry
                                       monitor.DEFAULT_KEYWORDS, True)
    keep = (reason == "")
    tight_hit = ""
    if keep:                                                   # stage2: 仅对 title 求值 (故意两口径)
        w = monitor.first_hit(title, monitor.STRONG)
        if w:
            tight_hit = w
        elif "期货" in title:
            tight_hit = "期货"
    tight = keep and (tight_hit != "")
    return {
        "seq": seq,
        "record_key": info if info else f"S-{seq}",
        "infoCode": info,
        "title": title,
        "orgSName": cs("orgSName"),
        "orgName": cs("orgName"),
        "orgCode": cs("orgCode"),
        "publishDate": str(it.get("publishDate") or ""),       # API 原值
        "industryName": industry,
        "industryCode": cs("industryCode"),
        "stockName": cs("stockName"),
        "stockCode": cs("stockCode"),
        "emRatingName": cs("emRatingName"),
        "author": author,
        "encodeUrl": cs("encodeUrl"),
        "reportType": cs("reportType"),
        "attachPages": attach,
        "qType": qt,
        "qtypes": [qt],
        "filter_reason": reason,
        "keep": keep,
        "tight": tight,
        "tight_hit": tight_hit,
        "pdf_url": PDF_URL_TEMPLATE.format(info=info) if info else None,
        "extract_status": "pending" if tight else "skipped",
        "extract": "",
        "extract_chars": 0,
        "snippet": "",
        "extract_attempts": 0,
        "extract_error": "",
    }


def ingest_page(data: list, qt: int) -> None:
    """页内逐条: 去重 (infoCode 首现胜) -> 归一化 -> 追加 jsonl."""
    global SEQ
    stats = STATE_D["stats"]
    for it in data:
        stats["fetched_total"] += 1
        if not isinstance(it, dict):
            it = {}
        info = monitor.clean_text(it.get("infoCode"))
        if info and info in INFO_MAP:                          # 交叉重复: 首现胜
            stats["dups_skipped"] += 1
            winner = ITEMS[INFO_MAP[info]]
            if qt not in winner["qtypes"]:
                winner["qtypes"] = sorted(set(winner["qtypes"]) | {qt})
                append_jsonl(RECORDS, winner)                  # 后写覆盖 -> qtypes 更新
            continue
        rec = normalize(it, qt, SEQ)
        SEQ += 1
        ITEMS[rec["record_key"]] = rec
        if info:
            INFO_MAP[info] = rec["record_key"]
        if rec["keep"]:
            CTR["kept"] += 1
        if rec["tight"]:
            CTR["tight"] += 1
        append_jsonl(RECORDS, rec)


def run_list(max_pages: int) -> None:
    """qType=1->2 翻页至终止; 两 qType 均 done 后 phase -> pdf."""
    progress("list", msg="list 阶段开始/续跑", force=True)
    for qt in (1, 2):
        p = STATE_D["pages"][str(qt)]
        if p["done"]:
            continue
        page = p["next_page"]
        fetched_run = 0
        first_line = False
        while True:
            if max_pages and fetched_run >= max_pages:          # 调试帽 (非 empty_page)
                p["stopped_reason"] = "page_cap"
                p["debug_truncated"] = True                     # 截断标记: done=false 使续跑继续翻页
                p["done"] = False                               # 非 --fresh 正式跑不得复用截断态
                save_state()
                progress("list", qt, page, msg="warn 终止=max_pages调试帽 done=false待续跑", force=True)
                break
            if p["hits"] and page > math.ceil(p["hits"] / 100) + 2:   # 防御上限
                p["stopped_reason"] = "page_cap"
                p["debug_truncated"] = False
                p["done"] = True
                save_state()
                progress("list", qt, page, msg="warn 终止=page_cap防御上限", force=True)
                break
            data, hits = fetch_page(qt, page)
            if not p["hits"]:
                p["hits"] = hits                               # 首个成功响应的 hits
            p["pages_fetched"] += 1                            # 含空页: 实际成功抓取页数
            fetched_run += 1
            if not first_line:
                progress("list", qt, page, msg=f"qType{qt}首页 hits={hits}", force=True)
                first_line = True
            if not data:
                p["stopped_reason"] = "empty_page"             # 已核实超界返回空 data
                p["debug_truncated"] = False                   # 正常终止, 清除调试截断标记
                p["done"] = True
                save_state()
                progress("list", qt, page, msg="终止=empty_page", force=True)
                break
            ingest_page(data, qt)                              # 先落记录
            p["next_page"] = page + 1                          # 再前进页码
            save_state()
            if p["pages_fetched"] % 25 == 0:
                progress("list", qt, page, force=True)
            else:
                progress("list", qt, page)                     # 兜底 <=300s
            page += 1
    if all(STATE_D["pages"][q]["done"] for q in ("1", "2")):
        STATE_D["phase"] = "pdf"
        save_state()
        progress("pdf", msg="list 阶段完成, 进入 pdf 阶段", force=True)


# ---------------- 阶段 P: PDF 下载与文本提取 (§4.5) ----------------
def write_extract(rec: dict, status: str, err: str) -> None:
    """终态/瞬时结果: 追加 extracts 行 + extract_done + state 同步."""
    apply_extract(rec, status, rec["extract"], rec["snippet"], err, rec["extract_attempts"])
    append_jsonl(EXTRACTS, {"record_key": rec["record_key"],
                            "extract_status": status,
                            "extract": rec["extract"],
                            "snippet": rec["snippet"],
                            "extract_error": err,
                            "attempts": rec["extract_attempts"]})
    STATE_D["extract_done"][rec["record_key"]] = {"status": status,
                                                 "attempts": rec["extract_attempts"]}
    save_state()


def finish_transient(rec: dict, err: str) -> None:
    """瞬时失败: attempts<3 保持 pending; 已达 3 次直接判 failed (§4.5.7)."""
    if rec["extract_attempts"] >= PAGE_TRIES:
        write_extract(rec, "failed", err)
        CTR["pdf_failed"] += 1
    else:
        write_extract(rec, "pending", err)


def process_pdf(rec: dict) -> None:
    """单篇: 缓存优先 -> monitor.http -> pdftotext 前8页 -> pdf_clean(3000)."""
    if not rec["pdf_url"]:                                     # infoCode 空, 结构性终态
        write_extract(rec, "no_pdf", "no_pdf_url")
        return
    rec["extract_attempts"] += 1
    safe_key = "".join(c if (c.isalnum() or c in "-_") else "_"   # 防 infoCode 带 /.. 越出 reports\
                       for c in rec["record_key"]) or "unnamed"
    pdf_path = REPORTS_DIR / f"{safe_key}.pdf"
    if not pdf_path.exists():
        try:
            raw = monitor.http(rec["pdf_url"], referer=REFERER, tries=1,
                               deadline=time.monotonic() + monitor.PDF_DL_BUDGET_S,
                               cap=monitor.PDF_CAP,
                               max_bytes=monitor.PDF_MAX_BYTES)
        except Exception:
            finish_transient(rec, "http")                      # 瞬时: 下轮 pass 重试
            return
        if not raw.lstrip()[:5].startswith(b"%PDF"):
            write_extract(rec, "no_text", "non_pdf")           # 302 错误页等确定性无文本
            CTR["pdf_no_text"] += 1
            return
        atomic_write(pdf_path, raw)                            # 原件缓存供续跑免下载
    # 偏差说明 (u1-impl.md §2): 规格 §4.5.2 原 argv 以 stdin 喂 PDF (`- -`); 本机 C-1 钉死的
    # pdftotext 实为 xpdf 4.00 (非 poppler), 两处不兼容按实测修正:
    #   ① xpdf 不接受 stdin 输入 (`-` 作 PDF 参数直接 rc=99 打印 usage) -> 改为读取
    #      已原子落盘的缓存文件路径 reports\<key>.pdf;
    #   ② xpdf 默认输出编码非 UTF-8, CJK 字形全部丢弃 (实测 0/5126 汉字) -> 加
    #      `-enc UTF-8` (实测 CJK 正常; 该参数对 poppler 亦合法, 其默认即 UTF-8).
    # 若按原 argv 全量跑会得到 ~444 条假 ok/假 no_text (spec §10 明确「宁可不跑也不产生
    # 假 no_text」); 语义不变: 前 8 页 + PDF_TIMEOUT + 相同错误类映射.
    try:
        r = subprocess.run(
            [monitor.PDFTOTEXT, "-enc", "UTF-8",
             "-f", "1", "-l", str(monitor.PDF_MAX_PAGES),
             str(pdf_path), "-"],
            capture_output=True, timeout=monitor.PDF_TIMEOUT,
            preexec_fn=monitor._pdf_child_limits if os.name == "posix" else None)
    except subprocess.TimeoutExpired:
        finish_transient(rec, "pdftotext_timeout")
        return
    except OSError:
        write_extract(rec, "no_text", "pdftotext_unavailable")
        CTR["pdf_no_text"] += 1
        return
    if r.returncode != 0:
        write_extract(rec, "no_text", "pdftotext_rc")
        CTR["pdf_no_text"] += 1
        return
    extract = monitor.pdf_clean(r.stdout.decode("utf-8", "ignore"), EXTRACT_LIMIT)
    if len(extract) < monitor.PDF_MIN_CHARS:
        write_extract(rec, "no_text", "short_clean")
        CTR["pdf_no_text"] += 1
        return
    rec["extract"] = extract
    rec["snippet"] = extract[:monitor.SNIPPET_MAX]             # 同源截断 == pdf_clean(.,240)
    write_extract(rec, "ok", "")
    CTR["pdf_ok"] += 1


def run_pdf(max_pdf: int) -> str:
    """pending 按 seq 升序, 至多 3 pass; 返回 done|aborted."""
    processed = 0
    pass_no = 0
    aborted = False
    while pass_no < MAX_PASSES:
        pend = sorted((r for r in ITEMS.values() if r["extract_status"] == "pending"),
                      key=lambda r: r["seq"])
        if not pend:
            break
        pass_no += 1
        if pass_no > 1:
            progress("pdf", msg=f"pass{pass_no - 1}毕 余pending={len(pend)} sleep{PASS_SLEEP}", force=True)
            time.sleep(PASS_SLEEP)
        progress("pdf", msg=f"pass{pass_no} 开始 pending={len(pend)}", force=True)
        for rec in pend:
            if rec["extract_status"] != "pending":
                continue
            if rec["extract_attempts"] >= PAGE_TRIES:          # 重建遗留: 已 3 试 -> failed
                write_extract(rec, "failed", rec["extract_error"] or "http")
                CTR["pdf_failed"] += 1
                continue
            if rec["pdf_url"] and max_pdf and processed >= max_pdf:
                aborted = True                                 # --max-pdf 截断: 保留 pending
                break
            process_pdf(rec)
            if rec["pdf_url"]:
                processed += 1
            if processed and processed % 25 == 0:
                progress("pdf", msg=f"已处理{processed}篇", force=True)
            else:
                progress("pdf")                                # 兜底 <=300s
            time.sleep(0.5 + random.random() * 0.5)            # 条目间限速
        if aborted:
            break
    if aborted:
        save_state()
        progress("pdf", msg=f"warn --max-pdf={max_pdf}截断 processed={processed}", force=True)
        return "aborted"
    for rec in ITEMS.values():                                 # 3 pass 后仍 TRANSIENT -> failed
        if rec["extract_status"] == "pending":
            write_extract(rec, "failed", rec["extract_error"] or "http")
            CTR["pdf_failed"] += 1
    return "done"


# ---------------- 阶段 A: 汇总 research-raw.json (§5) ----------------
def monitor_sha256() -> str:
    h = hashlib.sha256()
    with open(MONITOR_PY, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def assemble() -> None:
    progress("assemble", msg="汇总 research-raw.json", force=True)
    items = sorted(ITEMS.values(), key=lambda r: r["seq"])
    stats = STATE_D["stats"]
    counts = {
        "fetched_total": stats["fetched_total"],
        "unique_items": len(items),
        "dups_skipped": stats["dups_skipped"],
        "kept": sum(1 for r in items if r["keep"]),
        "tight": sum(1 for r in items if r["tight"]),
        "extract_ok": sum(1 for r in items if r["extract_status"] == "ok"),
        "extract_no_text": sum(1 for r in items if r["extract_status"] == "no_text"),
        "extract_failed": sum(1 for r in items if r["extract_status"] == "failed"),
        "extract_no_pdf": sum(1 for r in items if r["extract_status"] == "no_pdf"),
    }
    if not STATE_D.get("generated_at"):                        # 幂等: 首跑冻结, 复跑复用
        STATE_D["generated_at"] = datetime.now(TZ8).isoformat(timespec="seconds")
        save_state()                                           # 先冻结时间戳再落产物, 防崩溃口径漂移
    crawl = {}
    for q in ("1", "2"):
        p = STATE_D["pages"][q]
        crawl[q] = {"hits": p["hits"], "pages_fetched": p["pages_fetched"],
                    "stopped_reason": p["stopped_reason"]}
    raw = {
        "schema_version": "1.0",
        "generated_at": STATE_D["generated_at"],
        "window": WINDOW,
        "source": {"api": "https://reportapi.eastmoney.com/report/list",
                   "params_template": LIST_TEMPLATE,
                   "referer": REFERER,
                   "qtypes": [1, 2],
                   "page_size": 100},
        "crawl": crawl,
        "counts": counts,
        "env": {"python": platform.python_version(),
                "pdftotext": monitor.PDFTOTEXT,
                "monitor_sha256": monitor_sha256()},
        "items": items,
    }
    save_json(RAW_OUT, raw)
    STATE_D["phase"] = "done"
    save_state()
    progress("done", msg=(f"unique={counts['unique_items']} kept={counts['kept']} "
                          f"tight={counts['tight']} ok={counts['extract_ok']} "
                          f"no_text={counts['extract_no_text']} failed={counts['extract_failed']} "
                          f"no_pdf={counts['extract_no_pdf']}"), force=True)


# ---------------- 入口 (§4.1) ----------------
def main() -> int:
    ap = argparse.ArgumentParser(description="东财研报一年窗口抓取 + PDF 正文提取 (U1)")
    ap.add_argument("--max-pages", type=int, default=0,
                    help="每个 qType 最多抓 N 页 (0=不限; 截断退出码 2 不落成品)")
    ap.add_argument("--max-pdf", type=int, default=0, help="本次运行最多处理 N 篇 PDF (0=不限)")
    ap.add_argument("--fresh", action="store_true", help="清除既有中间状态从头重跑")
    ap.add_argument("--no-pdf", action="store_true", help="跳过 PDF 阶段 (遗留 pending -> 退出码 2)")
    args = ap.parse_args()

    if args.fresh:
        for p in (STATE, RECORDS, EXTRACTS):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        if REPORTS_DIR.exists():
            for p in REPORTS_DIR.glob("*.pdf"):                # 仅清 PDF 缓存, 报告文件保留
                try:
                    p.unlink()
                except OSError:
                    pass
    preflight()
    load_resume()
    phase = STATE_D.get("phase", "list")
    progress("list" if phase == "list" else phase, force=True,
             msg=(f"env python={platform.python_version()} "
                  f"pdftotext={monitor.PDFTOTEXT} root={ART} phase={phase}"))
    try:
        if STATE_D["phase"] == "list":
            run_list(args.max_pages)
        if STATE_D["phase"] == "list":
            # --max-pages 调试帽截断 (pages[*].debug_truncated): list 未完整终止,
            # 不落 research-raw.json, 按 §4.1「未完成已保存断点」退出码 2;
            # 续跑 --max-pages 0 补齐剩余页, empty_page 完整终止后方进 assemble.
            progress("list", msg="warn 调试帽截断 list 未完成, 不落 research-raw.json, 续跑继续翻页",
                     force=True)
            save_state()
            return 2
        if STATE_D["phase"] == "pdf":
            if args.no_pdf:
                progress("pdf", msg="--no-pdf 跳过, 遗留 pending", force=True)
                return 2
            if run_pdf(args.max_pdf) == "aborted":
                return 2
        if any(r["extract_status"] == "pending" for r in ITEMS.values()):
            progress("pdf", msg="仍有 pending, 未完成", force=True)
            save_state()
            return 2
        assemble()
        return 0
    except KeyboardInterrupt:
        save_state()
        try:
            progress(STATE_D.get("phase", "list"), msg="Ctrl-C 断点已存", force=True)
        except Exception:
            pass
        return 2
    except Exception:                                # 未捕获异常 (如磁盘满 OSError):
        import traceback                             #  traceback 留 stderr 诊断,
        traceback.print_exc()                        #  断点保存后按「未完成」退出码 2
        try:
            save_state()
            progress(STATE_D.get("phase", "list"),
                     msg="warn 未捕获异常 断点已存", force=True)
        except Exception:
            pass
        return 2


if __name__ == "__main__":
    sys.exit(main())
