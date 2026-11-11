#!/usr/bin/env python3
"""Batch 10 R1 — 行为等价基线 (golden harness)。

用途
  在**不联网、不发真实 Telegram、不碰真实 state/queue** 的前提下，用 28 个确定性场景
  （spec §3.3 冻结 14 + CTO 补 G15–G17 + 复验补 S1–S8 + R3 补 G18–G19 + R4 补 G20）
  跑 `monitor.main()`，把「退出码 + queue/seen/pending 原始字节 + 发送载荷 + 归一化
  stdout/stderr」冻结成 golden 快照。R2–R8 的每一次重构都必须让本脚本 --check 全绿。

用法
  python3.11 tests/golden.py --list
  python3.11 tests/golden.py --record --reason "R1 基线 (B9-FREEZE adc0795)"
  python3.11 tests/golden.py                      # 默认 = --check
  python3.11 tests/golden.py --check --scenario digest_due
  python3.11 tests/golden.py --selfcheck          # 归一化/确定性/防篡改自检
  python3.11 tests/golden.py --bench main --rounds 300
  python3.11 tests/golden.py --bench fetch --rounds 500
  python3.11 tests/golden.py --bench http --rounds 5000

更新纪律 (R1 冻结, 违反按篡改处理)
  1. `--record` 必须同时给出 `--reason <文本>` 与环境变量 `GOLDEN_RECORD_APPROVED=<同一文本>`
     （缺一即 exit 2；`--allow-dirty` 不能绕过），且只在 R1 落地时、Batch 9 已冻结后执行；执行前 `git rev-parse HEAD`
     与 `git status --porcelain` 必须干净（脏树判定排除 `tests/golden.py` 与
     `tests/golden/**`，它们就是本次产物）；其余已跟踪文件的未提交改动都算脏，
     `--record` 直接 exit 2（除非显式 `--allow-dirty` 并在报告登记）。
  2. R2–R8 任何一步出现 golden diff：**默认判回归，停止该单元**；禁止用 `--record`
     掩盖。
  3. 只有 CTO 书面认可「行为差异是预期的」后，才允许在新提交中 `--record`，提交信息
     必须含 `golden: record baseline after <原因> (CTO-approved)`，并附 diff 逐条解释。
  4. `--record` 必须带 `--reason <文本>`，并写入 MANIFEST 的 `record_reason`。
  5. `--record` 打印每个场景的 WROTE/UNCHANGED 并重算 MANIFEST；`--check` 先校验
     MANIFEST 中每个文件 SHA256，再运行场景比对；MANIFEST 不符直接
     `golden: FAIL (manifest)`，不进入运行。
  6. 快照目录只允许由 `--record` 写入；手工编辑快照视为篡改。

归一化
  只归一化时间戳 / 临时路径 / `.bad-<epoch>` / PID（`normalize_text`）；时长**不**归一化。
  queue.json / seen.json / pending.json / queue.bad.json **不做任何归一化**——日期稳定性
  由 `FrozenDatetime` 冻结 `_day.date` 保证。
"""
from __future__ import annotations

import argparse
import contextlib
import dataclasses
import difflib
import gc
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import types
import urllib.error
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent if (HERE.parent / "monitor.py").exists() else HERE
MONITOR_PY = ROOT / "monitor.py"
GOLDEN_DIR = HERE / "golden"

# 规格 §3.2.1 [canonical]
NOW0 = 1793800000                 # 2026-11-04 21:06:40 +08:00; 与 tests/sim.py 同起点
MONO0 = 1000.0

SRC_NAMES = ("财联社", "东财", "同花顺", "新浪", "见闻商品", "SMM",
             "Mining", "OilPrice", "MiningTech", "Nasdaq", "CNBC", "MarketWatch",
             "Investing", "Hellenic", "化销网", "隆众", "我的钢铁", "东财甲醇",
             "东财研报", "中金所", "中期协")
RESEARCH_SRCS = ("东财研报", "中金所", "中期协")
OPTIONAL_FILES = ("queue.json", "seen.json", "pending.json", "queue.bad.json")

SCENARIO_NAMES = (
    "normal_round", "all_sources_fail", "all_sources_skipped", "digest_due",
    "throttle_skip", "research_delayed", "clock_rollback", "state_corrupt",
    "queue_corrupt", "send_429", "state_write_fail", "queue_write_fail",
    "lock_unwritable", "missing_token",
    # G15–G17: CTO 明示补充（单源告警阈值 / 日上限 / 老化与 MAX_HOLD）
    "src_alert_threshold", "daily_cap", "aging_stale",
    # R1 v3: 评审新增覆盖（拆批 / 近邻合并 / 发送闸门 / 回拨容差 / 退避 / 代理腿 / 告警抑制 / 软截止）
    "msg_max_split", "near_merge", "send_gate", "rollback_tolerance",
    "retry_backoff", "proxy_leg", "alert_suppression", "soft_deadline",
    # R3 评审补强：Context 容器 / start 的写穿契约（M3 / M6 变异必须检出）
    "context_container_persistence", "round_start_visibility",
    # R4 补强：直接调用 6 个真实中文源函数（不打桩 SOURCES）
    "real_source_functions",
    # Batch 11 (CTO-approved): 研报正文全文分片 / 失败续发 / 存储上限
    "research_fullsplit", "research_resume", "research_store_cap",
)

# monitor 只按路径导入，避免任何 sys.path 污染；导入本身无副作用（不读 state）.
sys.path.insert(0, str(ROOT))
import monitor  # noqa: E402  (必须在路径修正之后)


# ------------------------------------------------------------------ fakes
# 规格 §3.2.2 [canonical]
REAL_DATETIME = monitor.datetime


class FrozenDatetime(REAL_DATETIME):
    @classmethod
    def now(cls, tz=None):
        return REAL_DATETIME.fromtimestamp(CLOCK.t, tz)

    @classmethod
    def fromtimestamp(cls, ts, tz=None):
        return REAL_DATETIME.fromtimestamp(ts, tz)


# 规格 §3.2.1 [canonical]
class FakeClock:
    def __init__(self, t0: float, jump_after: int | None = None) -> None:
        self.t = float(t0)
        self.mono = MONO0
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


CLOCK = FakeClock(NOW0)
CALLS: list[dict] = []
FAIL_QUEUE: list[BaseException] = []
SEND_COST = 0.0
PDF_QUEUE: list = []
PDF_CALLS = 0
NEVER_CALLED: list[str] = []
TMP = ""


# 规格 §3.2.3 [canonical]
def make_src(name: str, items: list[dict], error: str | None, cost: float,
             clock: FakeClock, never: bool = False):
    def _src(deadline=None):
        clock.mono += cost
        if never:
            raise AssertionError(f"source {name} should not be called")
        if error:
            raise RuntimeError(error)
        return [dict(it) for it in items]

    return _src


# 规格 §3.2.4 [canonical]
def fake_tg(method, payload, tk, proxy, timeout=25):
    CALLS.append({"method": method, "payload": dict(payload or {}),
                  "proxy": proxy, "timeout": timeout})
    if FAIL_QUEUE:
        exc = FAIL_QUEUE.pop(0)
        if exc is not None:              # Batch 11: None = 本次成功 (让「第 k 条失败」可定点)
            raise exc
    CLOCK.mono += SEND_COST
    CLOCK.t += SEND_COST
    return {"ok": True}


def fake_pdf(url: str, deadline: float | None = None,
             referer: str | None = None):
    global PDF_CALLS
    PDF_CALLS += 1
    if PDF_QUEUE:
        got = PDF_QUEUE.pop(0)
        # Batch 11: pdf_text 的产物契约是「已按 SNIPPET_MAX 截断 (带标记)」; fake 同源处理
        return monitor.snip_truncate(got) if isinstance(got, str) else got
    raise AssertionError("unexpected pdf_text")


def _no_network(*_a, **_k):
    raise AssertionError("no network")


def _no_subprocess(*_a, **_k):
    raise AssertionError("unexpected subprocess")


def make_probe_src(name: str, kind: str, clock: FakeClock, never: bool = False):
    """R3 补强：在源调用时刻读取模块级兼容垫片（CFG / ROUND_START）并写进条目文本。

    R3 的 Context 是**无状态视图**：`main` 通过 `round_ctx.cfg` / `round_ctx.start`
    写入后，模块级 `CFG` / `ROUND_START` 必须仍可见（B10-L01 的写穿契约；selftest
    也以此取证）。若 Context 改成副本（M3/M6），探针读到的就是空配置 / 0.0。
    """

    def _src(deadline=None):
        if never:
            raise AssertionError(f"source {name} should not be called")
        if kind == "cfg":
            text = (f"沪铜配置探针 CFG.chat_id={monitor.CFG.get('chat_id')!r} "
                    f"CFG.max_age_min={monitor.CFG.get('max_age_min')!r} 库存变化1吨环比1%")
        elif kind == "start":
            text = f"沪铜启动探针 ROUND_START={monitor.ROUND_START!r} 库存变化2吨环比2%"
        else:
            raise AssertionError(f"unknown probe kind {kind!r}")
        return [{"src": name, "ts": BASE_TS, "text": text}]

    return _src


def make_audited_src(name: str, spec: dict, clock: FakeClock):
    """canonical make_src 的薄包装：记录 never 源是否被调用（违规立即硬失败）。"""
    if spec.get("probe"):
        return make_probe_src(name, str(spec["probe"]), clock, bool(spec.get("never")))
    base = make_src(name, list(spec.get("items") or []), spec.get("error"),
                    float(spec.get("cost") or 0.0), clock, bool(spec.get("never")))
    if not spec.get("never"):
        return base

    def _src(deadline=None):
        NEVER_CALLED.append(name)
        return base(deadline)

    return _src


# 规格 §3.4 [canonical]
def normalize_text(text: str, tmpdir: str) -> str:
    text = re.sub(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]", "[<TS>]", text)
    text = text.replace(str(tmpdir), "<TMP>")
    text = re.sub(r"\.bad-\d+", ".bad-<EPOCH>", text)
    text = re.sub(r"\bpid=\d+", "pid=<PID>", text)
    return text


class VerificationError(RuntimeError):
    """场景自身未能按规格成立（rc 不符 / 不该被调用的源被调用 / main 抛错）。"""


# ------------------------------------------------------------------ scenarios
BASE_TS = NOW0 - 60

ZH_ITEMS = {
    "财联社": ("沪铜库存变化1吨环比1%", "螺纹钢库存变化2吨环比2%"),
    "东财": ("铁矿石库存变化3吨环比3%", "焦煤库存变化4吨环比4%"),
    "同花顺": ("白银库存变化5吨环比5%", "黄金库存变化6吨环比6%"),
    "新浪": ("原油库存变化7吨环比7%", "天然气库存变化8吨环比8%"),
    "见闻商品": ("豆粕库存变化9吨环比9%", "棕榈油库存变化10吨环比10%"),
    "SMM": ("电解铜库存变化11吨环比11%", "沪铝库存变化12吨环比12%"),
}
EN_ITEMS = {
    "Mining": "铁矿石发运量变化13万吨环比13%",
    "OilPrice": "WTI原油库存变化14万桶环比14%",
    "MiningTech": "铜精矿产量变化15万吨环比15%",
    "Nasdaq": "COMEX黄金库存变化16吨环比16%",
    "CNBC": "美国天然气库存变化17亿立方英尺环比17%",
    "MarketWatch": "LME铜库存变化18吨环比18%",
    "Investing": "焦炭库存变化19万吨环比19%",
    "Hellenic": "铁矿石运价指数变化20点环比20%",
}
FILTERED_TEXT = "沪指收盘上涨0.52%，两市成交额1.2万亿元"


def it(src: str, text: str, ts: int | None = None) -> dict:
    return {"src": src, "ts": BASE_TS if ts is None else ts, "text": text}


def rsrch(src: str, ident: str, snippet, ts: int | None = None) -> dict:
    return {"src": src, "ts": BASE_TS if ts is None else ts,
            "text": f"{src}沪铜研报第{ident}期｜示例期货研究所 · 2026-11-04",
            "url": f"https://example.com/research/{src}-{ident}.pdf",
            "snippet": snippet, "key": f"研究:{src}:{ident}"}


def src_spec(items: list[dict] | None = None, error: str | None = None,
             cost: float = 0.0, never: bool = False, probe: str | None = None) -> dict:
    return {"items": list(items or []), "error": error, "cost": cost,
            "never": never, "probe": probe}


def normal_sources() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name, texts in ZH_ITEMS.items():
        out[name] = src_spec([it(name, t) for t in texts])
    for name, text in EN_ITEMS.items():
        out[name] = src_spec([it(name, text)])
    for i, name in enumerate(RESEARCH_SRCS, 1):
        out[name] = src_spec([rsrch(name, str(i), "")])
    return out


def empty_sources(**overrides: dict) -> dict[str, dict]:
    out = {name: src_spec([]) for name in SRC_NAMES}
    out.update(overrides)
    return out


def never_sources() -> dict[str, dict]:
    return {name: src_spec([], never=True) for name in SRC_NAMES}


def fail_sources() -> dict[str, dict]:
    return {name: src_spec([], error="boom") for name in SRC_NAMES}


def base_conf() -> dict:
    return {"token": "x", "chat_id": 1, "max_age_min": 180,
            "digest_min": 30, "digest_max_items": 40}


def queue3() -> list[dict]:
    return [
        {"src": "财联社", "ts": BASE_TS, "text": "沪铜库存变化21吨环比21%", "key": "k-queue-1"},
        {"src": "东财", "ts": BASE_TS - 60, "text": "螺纹钢库存变化22吨环比22%", "key": "k-queue-2"},
        {"src": "同花顺", "ts": BASE_TS - 120, "text": "铁矿石库存变化23吨环比23%", "key": "k-queue-3"},
    ]


def digest_state() -> dict:
    return {"seedmarker": 1, "_digest": {"last_flush": NOW0 - 31 * 60}}


@dataclasses.dataclass
class Scenario:
    name: str
    rounds: list[dict]
    expected_rcs: list[int]
    state: dict | None = None
    state_raw: bytes | None = None
    queue: list[dict] | None = None
    queue_raw: bytes | None = None
    pending: dict | None = None
    args: tuple = ()
    conf: dict | None = None
    lock_dir: bool = False
    drop_token: bool = False
    persist_state_fail: bool = False
    save_queue_fail: bool = False
    fail_tg: list | None = None
    send_cost: float = 0.0
    pdf_returns: list | None = None
    expect_pdf_calls: int | None = None
    expect_tg_calls: int | None = None
    source_probe: dict | None = None


def _http_429() -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://api.telegram.org", 429, "Too Many",
                                  {"Retry-After": "120"}, io.BytesIO(b'{"ok":false}'))


def _http_400() -> urllib.error.HTTPError:
    """Batch 11 续发场景: 确定性 400 (无 Retry-After), 触发「已发片保留 + 条目留队」."""
    return urllib.error.HTTPError("https://api.telegram.org", 400, "Bad Request",
                                  {}, io.BytesIO(b'{"ok":false}'))


def real_source_fixtures() -> dict[str, list[tuple[str, bytes]]]:
    """G20：6 个真实中文源的固定响应体（normal / empty / missing / renamed 四变体）。

    empty/missing 走各源的结构哨兵（多数 raise）；missing/renamed 里的“字段改名”是
    Batch 9 遗留 M-02：行还在但文本字段名变了 -> extract 返回 None -> feed_items 静默
    返回 [] 且源被记健康。G20 把当前行为（无论好坏）冻结下来，本批不改代码。
    """
    def js(obj) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode()

    cls_row = {"title": "沪铜库存变化1吨环比1%", "ctime": "2026-11-04 21:00:00"}
    cls_row2 = {"brief": "螺纹钢库存变化2吨环比2%", "ctime": "2026-11-04 21:01:00"}
    cls_bad = {"title": "焦煤库存变化3吨环比3%", "ctime": "昨天 09:00"}
    em_row = {"summary": "沪铜库存变化4吨环比4%", "showTime": "2026-11-04 21:02:00"}
    ths_row = {"title": "沪铜库存变化5吨环比5%", "digest": "上海期货交易所",
               "ctime": "2026-11-04 21:03:00"}
    sina_row = {"rich_text": "<p>沪铜库存变化6吨环比6%</p>",
                "create_time": "2026-11-04 21:04:00"}
    wscn_row = {"content_text": "<p>沪铜&nbsp;库存变化8吨环比8%</p>  <span>追加</span>",
                "display_time": "2026-11-04 21:05:00"}
    smm_item = {"title": "沪铜库存变化10吨环比10%",
                "profile": "沪铜库存变化10吨环比10%，上海有色网",
                "pubDate": "2026-11-04 21:06:00"}
    smm_noise = {"title": "煤炭快讯：某矿停产", "profile": "煤炭快讯：某矿停产详情",
                 "pubDate": "2026-11-04 21:06:30"}

    def smm_page(list_list) -> bytes:
        payload = {"props": {"pageProps": {"liveListData": {"data": {"data": {
            "listList": list_list}}}}}}
        return ('<html><body><script id="__NEXT_DATA__" type="application/json">'
                + json.dumps(payload, ensure_ascii=False)
                + "</script></body></html>").encode()

    return {
        "财联社": [
            ("normal", js({"data": {"roll_data": [cls_row, cls_row2, cls_bad]}})),
            ("empty", js({"data": {"roll_data": []}})),
            ("missing", js({"data": {}})),
            ("renamed", js({"data": {"rollData": [dict(cls_row)]}})),
        ],
        "东财": [
            ("normal", js({"data": {"fastNewsList": [em_row]}})),
            ("empty", js({"data": {"fastNewsList": []}})),
            ("missing", js({"data": {}})),
            ("renamed", js({"data": {"fastNews": [dict(em_row)]}})),
        ],
        "同花顺": [
            ("normal", js({"data": {"list": [ths_row]}})),
            ("empty", js({"data": {"list": []}})),
            ("missing", js({"data": {}})),
            ("renamed", js({"data": {"rows": [dict(ths_row)]}})),
        ],
        "新浪": [
            ("normal", js({"result": {"data": {"feed": {"list": [sina_row]}}}})),
            ("empty", js({"result": {"data": {"feed": {"list": []}}}})),
            ("missing", js({"result": {"data": {"feed": {"list": [
                {"create_time": "2026-11-04 21:04:00"}]}}}})),
            ("renamed", js({"result": {"data": {"feed": {"list": [
                {"richText": "<p>沪铜库存变化6吨环比6%</p>",
                 "create_time": "2026-11-04 21:04:00"}]}}}})),
        ],
        "见闻商品": [
            ("normal", js({"data": {"items": [wscn_row]}})),
            ("empty", js({"data": {"items": []}})),
            ("missing", js({"data": {"items": [
                {"display_time": "2026-11-04 21:05:00"}]}})),
            ("renamed", js({"data": {"items": [
                {"contentText": "<p>沪铜库存变化8吨环比8%</p>",
                 "display_time": "2026-11-04 21:05:00"}]}})),
        ],
        "SMM": [
            ("normal", smm_page([smm_item, smm_noise])),
            ("empty", smm_page([])),
            ("missing", js({"props": {"pageProps": {}}})),
            ("renamed", smm_page([{"heading": "沪铜库存变化10吨环比10%",
                                   "desc": "沪铜库存变化10吨环比10%，上海有色网",
                                   "published": "2026-11-04 21:06:00"}])),
        ],
    }


SOURCE_FUNCS = {"财联社": "src_cls", "东财": "src_em", "同花顺": "src_ths",
                "新浪": "src_sina", "见闻商品": "src_wscn_commodity", "SMM": "src_smm"}
SOURCE_ORDER = ("财联社", "东财", "同花顺", "新浪", "见闻商品", "SMM")


def _norm_probe(text: str) -> str:
    return normalize_text(text, "\x00no-tmp\x00")


def _run_source_probe(sc: Scenario) -> tuple[dict[str, bytes], dict]:
    """G20：直接调用 6 个真实源函数（monitor.http 逐变体打桩），快照产出/抛错/ts_bad。"""
    saved_http = monitor.http
    saved_dt = monitor.datetime
    cases: list[dict] = []
    try:
        monitor.datetime = FrozenDatetime
        for src_name in SOURCE_ORDER:
            fn = getattr(monitor, SOURCE_FUNCS[src_name])
            for variant, body in (sc.source_probe or {}).get(src_name, []):
                monitor.http = (lambda *a, _b=body, **k: _b)
                monitor.TS_BAD.clear()
                so, se = io.StringIO(), io.StringIO()
                ok, err, items = True, None, []
                try:
                    with contextlib.redirect_stdout(so), contextlib.redirect_stderr(se):
                        got = fn(None)
                    items = [{"src": g.get("src"), "ts": g.get("ts"),
                              "text": _norm_probe(str(g.get("text", "")))}
                             for g in got]
                except BaseException as e:  # noqa: BLE001
                    ok, err = False, f"{type(e).__name__}: {e}"
                cases.append({"src": src_name, "variant": variant, "ok": ok,
                              "error": err, "items": items,
                              "ts_bad": dict(sorted(monitor.TS_BAD.items())),
                              "stdout": _norm_probe(so.getvalue()),
                              "stderr": _norm_probe(se.getvalue())})
    finally:
        monitor.http = saved_http
        monitor.datetime = saved_dt
    payload = (json.dumps(cases, ensure_ascii=False, sort_keys=True) + "\n").encode()
    files = {"sources.json": payload, "rc.json": b"[0]\n", "sends.jsonl": b"",
             "stdout.txt": b"", "stderr.txt": b""}
    extras = {"backup_name": None,
              "absent": [n for n in OPTIONAL_FILES if n not in files],
              "rcs": [0], "pdf_calls": 0, "tg_calls": 0}
    return files, extras


def _rnd(sources: dict, advance: int | None = None,
         jump_after: int | None = None) -> dict:
    rnd: dict = {"sources": sources}
    if advance:
        rnd["advance_after"] = advance
    if jump_after is not None:
        rnd["jump_after"] = jump_after
    return rnd


def _src_fail_round() -> dict:
    """G15 前 7 轮：财联社单源失败，东财持续供 1 条（让 main 走 source_health 而非全源告警）。"""
    return _rnd(empty_sources(
        财联社=src_spec([], error="boom"),
        东财=src_spec([it("东财", "螺纹钢库存变化31吨环比31%")])))


def long_item(n: int, length: int = 700) -> dict:
    """S1/S3：构造 u16 接近 MSG_MAX 的长条目（默认 700 字符）。"""
    base = f"沪铜库存变化{n}吨环比{n}%"
    return {"src": "财联社", "ts": BASE_TS,
            "text": base + "。" * max(0, length - len(base)), "key": f"k-long-{n}"}


def old_queue5() -> list[dict]:
    """G17：5 条全部早于 max_age(180min) 的队列条目。"""
    base = NOW0 - 181 * 60
    return [
        {"src": "财联社", "ts": base - 10, "text": "沪铜库存变化41吨环比41%", "key": "k-old-1"},
        {"src": "东财", "ts": base - 20, "text": "螺纹钢库存变化42吨环比42%", "key": "k-old-2"},
        {"src": "同花顺", "ts": base - 30, "text": "铁矿石库存变化43吨环比43%", "key": "k-old-3"},
        {"src": "新浪", "ts": base - 40, "text": "白银库存变化44吨环比44%", "key": "k-old-4"},
        {"src": "SMM", "ts": base - 50, "text": "沪铝库存变化45吨环比45%", "key": "k-old-5"},
    ]


# Batch 11: 分片正文夹具 (含句末标点; *120 ≈ 5.3k 字 -> 3 片)
RESEARCH_BODY_B11 = ("钨行业三季报点评：钨资源与硬质合金价格企稳反弹，"
                     "硬质合金及刀具公司季度营收环比改善。") * 120


def build_scenarios() -> list[Scenario]:
    advance = monitor.RESEARCH_EVERY_S + 1
    out = [
        Scenario("normal_round",
                 [{"sources": normal_sources()}], [0],
                 state={"seedmarker": 1}, conf=base_conf()),
        Scenario("all_sources_fail",
                 # Batch 12/13: 新源空健康 -> 本场景继续冻结「17 老源全失败」的文案/载荷
                 [{"sources": {**fail_sources(),
                               "化销网": src_spec([]), "隆众": src_spec([]),
                               "我的钢铁": src_spec([]), "东财甲醇": src_spec([])}}], [1],
                 state={"seedmarker": 1}, conf=base_conf()),
        Scenario("all_sources_skipped",
                 [{"sources": never_sources(), "jump_after": 2}], [0],
                 state={"seedmarker": 1}, conf=base_conf()),
        Scenario("digest_due",
                 [{"sources": empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]))}], [0],
                 state=digest_state(), queue=queue3(),
                 conf={**base_conf(), "digest_min_items": 3}),
        Scenario("throttle_skip",
                 [{"sources": empty_sources(**{
                     name: src_spec([], never=True) for name in RESEARCH_SRCS})}], [0],
                 state={"seedmarker": 1,
                        "_research": {name: NOW0 - 100 for name in RESEARCH_SRCS}},
                 conf=base_conf()),
        Scenario("research_delayed",
                 [{"sources": empty_sources(**{
                      "东财研报": src_spec([rsrch("东财研报", "1", None)])}),
                   "advance_after": advance},
                  {"sources": empty_sources(**{
                      "东财研报": src_spec([rsrch("东财研报", "1", None,
                                                  BASE_TS + advance)])})}],
                 [0, 0], state={"seedmarker": 1}, conf=base_conf(),
                 pdf_returns=[None, "摘要"], expect_pdf_calls=2,
                 expect_tg_calls=0),
        Scenario("clock_rollback",
                 [{"sources": empty_sources(
                     财联社=src_spec([it("财联社", "沪铜库存变化24吨环比24%")]))}], [0],
                 state={"seedmarker": 1,
                        "_digest": {"last_flush": NOW0 + 3600, "last_round": NOW0 + 3600},
                        "_research": {name: NOW0 + 3600 for name in RESEARCH_SRCS}},
                 queue=queue3(), conf=base_conf()),
        Scenario("state_corrupt",
                 [{"sources": never_sources()}], [4],
                 state_raw="{坏 json".encode(), conf=base_conf()),
        Scenario("queue_corrupt",
                 [{"sources": empty_sources(
                     财联社=src_spec([it("财联社", "沪铜库存变化25吨环比25%")]))}], [0],
                 state={"seedmarker": 1}, queue_raw="{坏 json".encode(),
                 conf=base_conf()),
        Scenario("send_429",
                 [{"sources": empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]))}], [2],
                 state=digest_state(), queue=queue3(), conf=base_conf(),
                 fail_tg=[_http_429()]),
        Scenario("state_write_fail",
                 [{"sources": empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]))}], [5],
                 state=digest_state(), queue=queue3(), conf=base_conf(),
                 persist_state_fail=True),
        Scenario("queue_write_fail",
                 [{"sources": empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]))}], [6],
                 state=digest_state(), queue=queue3(), conf=base_conf(),
                 save_queue_fail=True),
        Scenario("lock_unwritable",
                 [{"sources": never_sources()}], [7],
                 state={"seedmarker": 1}, conf=base_conf(), lock_dir=True),
        Scenario("missing_token",
                 [{"sources": never_sources()}], [3],
                 state={"seedmarker": 1},
                 conf={k: v for k, v in base_conf().items() if k != "token"},
                 drop_token=True),
        # G15: 单源连续失败 5 轮零告警 / 第 6 轮 1 条 / 第 7 轮抑制 / 恢复 1 条
        Scenario("src_alert_threshold",
                 [_src_fail_round() for _ in range(7)]
                 + [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", "沪铜库存变化32吨环比32%")]),
                     东财=src_spec([it("东财", "螺纹钢库存变化31吨环比31%")])))],
                 [0] * 8, state={"seedmarker": 1}, conf=base_conf(),
                 expect_tg_calls=2, expect_pdf_calls=0),
        # G16: 日上限已到（msgs=80）-> 第 1 轮发 1 条触顶提示，第 2 轮不再重复
        Scenario("daily_cap",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]))),
                  _rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)])))],
                 [0, 0],
                 state={"seedmarker": 1,
                        "_digest": {"last_flush": NOW0 - 31 * 60},
                        "_day": {"date": "2026-11-04", "msgs": 80}},
                 queue=queue3(), conf=base_conf(),
                 expect_tg_calls=1),
        # G17: 第 1 轮队列 5 条全部超 max_age -> 发 3 条、剩余 2 条按 stale 记账丢弃；
        #      第 2 轮 1 条 46 分钟前条目 -> oldest_age >= MAX_HOLD 强制发
        Scenario("aging_stale",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]))),
                  _rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)]),
                     东财=src_spec([it("东财", "螺纹钢库存变化52吨环比52%",
                                      NOW0 - 2760)])))],
                 [0, 0], state={"seedmarker": 1}, queue=old_queue5(),
                 conf={**base_conf(), "digest_max_items": 3},
                 expect_tg_calls=2),
        # S1: 10 条 700 字条目 -> 2 条接近 MSG_MAX 的消息（MSG_MAX 变异必红）
        Scenario("msg_max_split",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)])))],
                 [0], state=digest_state(),
                 queue=[long_item(i) for i in range(1, 11)],
                 conf=base_conf(), expect_tg_calls=2),
        # S2: A/B 高相似同数字 -> 合并；C 相似度 0.638 -> 0.90 不合并、0.50 会合并；D 不相似不合并
        Scenario("near_merge",
                 [_rnd(empty_sources(**{
                     "东财": src_spec([it("东财", "沪铜库存变化100吨环比10%，市场关注度一般")]),
                     "同花顺": src_spec([it("同花顺", "沪铜库存变化100吨环比10%，市场关注度较一般")]),
                     "新浪": src_spec([it("新浪", "沪铜库存变化100吨环比10%，下游需求平稳供应正常")]),
                     "财联社": src_spec([it("财联社", "螺纹钢产量下降200万吨，钢厂检修增加")]),
                 }))],
                 [0], state={"seedmarker": 1}, conf=base_conf(),
                 expect_tg_calls=0),
        # S3: 抓取耗时 60s + 每次发送 30s -> 第 1 条发出，第 2 条被逐次闸门挡住并留队
        Scenario("send_gate",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)], cost=60.0)))],
                 [0], state=digest_state(),
                 queue=[long_item(i) for i in range(1, 7)],
                 conf=base_conf(), send_cost=30.0, expect_tg_calls=1),
        # S4: last_round/_research 在未来 300s（落在 60~600 容差带）-> 回拨判定与 research 未来处理
        Scenario("rollback_tolerance",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)])))],
                 [0],
                 state={"seedmarker": 1,
                        "_digest": {"last_flush": NOW0 + 300,
                                    "last_round": NOW0 + 300},
                        "_research": {name: NOW0 + 300 for name in RESEARCH_SRCS}},
                 queue=queue3(), conf=base_conf()),
        # S5: 429 无 Retry-After -> next_try = now + 120*2^fails；第 2 轮退避到期后再次失败 -> 120*4
        Scenario("retry_backoff",
                 [_rnd(empty_sources(
                      财联社=src_spec([it("财联社", FILTERED_TEXT)])), advance=241),
                  _rnd(empty_sources(
                      财联社=src_spec([it("财联社", FILTERED_TEXT)])))],
                 [2, 2], state=digest_state(), queue=queue3(),
                 conf=base_conf(), fail_tg=[
                     urllib.error.HTTPError("https://api.telegram.org", 429, "Too Many",
                                            {}, io.BytesIO(b'{"ok":false}')),
                     urllib.error.HTTPError("https://api.telegram.org", 429, "Too Many",
                                            {}, io.BytesIO(b'{"ok":false}'))],
                 expect_tg_calls=2),
        # S6: conf.proxy 非空 -> 直连(8s) 失败 -> 代理(20s) 失败 -> 直连(20s) 成功，三腿进快照
        Scenario("proxy_leg",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)])))],
                 [0], state=digest_state(), queue=queue3(),
                 conf={**base_conf(), "proxy": "http://127.0.0.1:8080"},
                 fail_tg=[TimeoutError("leg1"), TimeoutError("leg2")],
                 expect_tg_calls=3),
        # S7: 12 轮单源失败、每轮 +600s -> 第 6 轮告警、7..11 抑制、第 12 轮（满 3600s）再告警
        Scenario("alert_suppression",
                 [_rnd(empty_sources(
                      财联社=src_spec([], error="boom"),
                      东财=src_spec([it("东财", "螺纹钢库存变化31吨环比31%",
                                       NOW0 + 600 * i - 60)])),
                      advance=600 if i < 11 else None)
                  for i in range(12)],
                 [0] * 12, state={"seedmarker": 1},
                 conf={**base_conf(), "digest_min": 10000,
                       "max_hold_min": 10000, "digest_min_items": 999},
                 expect_tg_calls=2),
        # S8: 抓取耗时 90s > round_soft_s()=83 -> 软截止分支把聚合发送推迟到下一轮（0 发送）
        Scenario("soft_deadline",
                 [_rnd(empty_sources(
                     财联社=src_spec([it("财联社", FILTERED_TEXT)], cost=90.0)))],
                 [0], state=digest_state(), queue=queue3(), conf=base_conf(),
                 expect_tg_calls=0),
        # G18：跨轮容器状态 + cfg 写穿。财联社两轮失败使 _src_fail.rounds=2（跨轮可见）；
        #      Mining 探针在调用时读模块级 CFG（chat_id/max_age_min），副本化后变成 None。
        Scenario("context_container_persistence",
                 [_rnd(empty_sources(**{
                       "财联社": src_spec([], error="boom"),
                       "Mining": src_spec([], probe="cfg")})),
                  _rnd(empty_sources(**{
                       "财联社": src_spec([], error="boom"),
                       "Mining": src_spec([], probe="cfg")}))],
                 [0, 0], state={"seedmarker": 1}, conf=base_conf(),
                 expect_tg_calls=0),
        # G19：同一轮内跨函数读 ROUND_START。main 赋 round_ctx.start（写穿全局），
        #      东财探针在 fetch 中读模块级 ROUND_START（=1000.0）；start 副本化后读到 0.0。
        Scenario("round_start_visibility",
                 [_rnd(empty_sources(**{
                       "财联社": src_spec([], error="boom"),
                       "东财": src_spec([], probe="start")})),
                  _rnd(empty_sources(**{
                       "财联社": src_spec([], error="boom"),
                       "东财": src_spec([], probe="start")}))],
                 [0, 0], state={"seedmarker": 1}, conf=base_conf(),
                 expect_tg_calls=0),
        # G20：不打桩 SOURCES，直接执行 6 个真实源函数；http 按变体返回固定夹具。
        Scenario("real_source_functions", [{"sources": {}}], [0],
                 conf=base_conf(), source_probe=real_source_fixtures()),
        # B11-1: 研报全文分片 -> N 条独占消息, 标记 (i/N), 链接仅末片, 末片后才写 seen
        Scenario("research_fullsplit",
                 [_rnd(empty_sources(**{
                     "东财研报": src_spec([rsrch("东财研报", "1", RESEARCH_BODY_B11)])}))],
                 [0], state=digest_state(),
                 conf={**base_conf(), "digest_min_items": 1},
                 expect_pdf_calls=0, expect_tg_calls=3),
        # B11-2: 第 2 片失败 -> 第 1 轮 rc=2, 队列留 parts_sent/parts_total/part_fails;
        #        第 2 轮从第 2 片续发 (不重发首片) 并排空. 说明: 研报源有 30 分钟节流,
        #        故跨轮推进必须 > RESEARCH_EVERY_S (规格草案写 RETRY_BASE_S+1 会跳过整个源,
        #        main 因「无可用条目」早退, 续发路径根本不会被驱动).
        Scenario("research_resume",
                 [_rnd(empty_sources(**{
                     "东财研报": src_spec([rsrch("东财研报", "1", RESEARCH_BODY_B11)])}),
                      advance=monitor.RESEARCH_EVERY_S + 1),
                  _rnd(empty_sources(**{
                      "东财研报": src_spec([rsrch("东财研报", "1", RESEARCH_BODY_B11,
                                                  BASE_TS + monitor.RESEARCH_EVERY_S + 1)])}))],
                 [2, 0], state=digest_state(),
                 conf={**base_conf(), "digest_min_items": 1},
                 fail_tg=[None, _http_400()],     # 第 1 条成功, 第 2 条 (2/N) 确定性失败
                 expect_pdf_calls=0, expect_tg_calls=4),
        # B11-3: 41000 字清洗正文 -> 存储上限 24000 + 显式截断标记 (不入队发送)
        Scenario("research_store_cap",
                 [_rnd(empty_sources(**{
                     "东财研报": src_spec([rsrch("东财研报", "1", None)])}))],
                 [0], state={"seedmarker": 1},
                 conf={**base_conf(), "digest_min": 10000,
                       "digest_max_items": 999, "max_hold_min": 10000},
                 pdf_returns=["钨" * 41000],
                 expect_pdf_calls=1, expect_tg_calls=0),
    ]
    names = tuple(s.name for s in out)
    if names != SCENARIO_NAMES:
        raise SystemExit(f"golden: scenario table mismatch: {names!r}")
    return out


# ------------------------------------------------------------------ harness
_PATH_ATTRS = ("STATE", "QUEUE", "PENDING", "LOCK", "CONF", "LOG", "ERRLOG")


def install(m: "monitor", sc: Scenario, clock: FakeClock) -> dict:
    """装上全部假件；返回用于 restore 的现场快照。"""
    saved = {name: getattr(m, name) for name in _PATH_ATTRS}
    saved["env_token"] = os.environ.get("FUTNEWS_TOKEN")
    if sc.drop_token:
        os.environ.pop("FUTNEWS_TOKEN", None)
    saved.update({
        "time": m.time, "datetime": m.datetime, "SOURCES": m.SOURCES,
        "tg": m.tg, "resolve_token": m.resolve_token, "rotate_log": m.rotate_log,
        "rotate_stderr": m.rotate_stderr, "http": m.http, "http_ok": m.http_ok,
        "pdf_text": m.pdf_text, "PROXY": m.PROXY, "persist_state": m.persist_state,
        "save_queue": m.save_queue, "subprocess_run": subprocess.run,
        "argv": sys.argv,
    })
    m.time = clock
    m.datetime = FrozenDatetime
    m.PROXY = ""
    m.tg = fake_tg
    m.resolve_token = (lambda conf: "") if sc.drop_token else (lambda conf: "x")
    m.rotate_log = lambda: None
    m.rotate_stderr = lambda: None
    m.http = _no_network
    m.http_ok = _no_network
    m.pdf_text = fake_pdf
    subprocess.run = _no_subprocess
    if sc.persist_state_fail:
        m.persist_state = lambda seen: False
    if sc.save_queue_fail:
        m.save_queue = lambda q: False
    return saved


def restore(m: "monitor", saved: dict) -> None:
    for name in _PATH_ATTRS:
        setattr(m, name, saved[name])
    m.time = saved["time"]
    m.datetime = saved["datetime"]
    m.SOURCES = saved["SOURCES"]
    m.tg = saved["tg"]
    m.resolve_token = saved["resolve_token"]
    m.rotate_log = saved["rotate_log"]
    m.rotate_stderr = saved["rotate_stderr"]
    m.http = saved["http"]
    m.http_ok = saved["http_ok"]
    m.pdf_text = saved["pdf_text"]
    m.PROXY = saved["PROXY"]
    m.persist_state = saved["persist_state"]
    m.save_queue = saved["save_queue"]
    subprocess.run = saved["subprocess_run"]
    sys.argv = saved["argv"]
    if saved.get("env_token") is not None:
        os.environ["FUTNEWS_TOKEN"] = saved["env_token"]
    else:
        os.environ.pop("FUTNEWS_TOKEN", None)


def _join_rounds(outs: list[tuple[int, str, str]], idx: int) -> str:
    parts = []
    for n, (_, so, se) in enumerate(outs, 1):
        if n > 1:
            parts.append(f"--- round {n} ---\n")
        parts.append(so if idx == 1 else se)
    return "".join(parts)


def run_scenario(sc: Scenario) -> tuple[dict[str, bytes], dict]:
    """跑一个场景，返回 (快照文件名 -> 原始字节, 附加度量)。"""
    if sc.source_probe is not None:
        return _run_source_probe(sc)
    global CLOCK, CALLS, FAIL_QUEUE, SEND_COST, PDF_QUEUE, PDF_CALLS, NEVER_CALLED, TMP
    m = monitor
    real_names = [name for name, _fn in m.SOURCES]
    if len(real_names) != 21 or set(real_names) != set(SRC_NAMES):
        raise VerificationError(
            f"monitor.SOURCES 与 golden 冻结的 21 源不符: {real_names}")
    tmp = Path(tempfile.mkdtemp(prefix="fnm-golden-"))
    TMP = str(tmp)
    saved = install(monitor, sc, FakeClock(NOW0))
    try:
        st = tmp / "state"
        st.mkdir(parents=True, exist_ok=True)
        monitor.STATE = st / "seen.json"
        monitor.QUEUE = st / "queue.json"
        monitor.PENDING = st / "pending.json"
        monitor.LOCK = st / "lock"
        monitor.CONF = tmp / "conf.json"
        monitor.LOG = tmp / "out.log"
        monitor.ERRLOG = tmp / "err.log"
        conf = dict(sc.conf if sc.conf is not None else base_conf())
        monitor.CONF.write_text(json.dumps(conf, ensure_ascii=False),
                                encoding="utf-8")
        if sc.state_raw is not None:
            monitor.STATE.write_bytes(sc.state_raw)
        elif sc.state is not None:
            monitor.STATE.write_text(json.dumps(sc.state, ensure_ascii=False),
                                     encoding="utf-8")
        if sc.queue_raw is not None:
            monitor.QUEUE.write_bytes(sc.queue_raw)
        elif sc.queue is not None:
            monitor.QUEUE.write_text(
                json.dumps({"items": sc.queue, "ts": NOW0}, ensure_ascii=False),
                encoding="utf-8")
        if sc.pending is not None:
            monitor.PENDING.write_text(
                json.dumps(sc.pending, ensure_ascii=False), encoding="utf-8")
        if sc.lock_dir:
            monitor.LOCK.mkdir(parents=True, exist_ok=True)

        NEVER_CALLED = []
        CALLS = []
        FAIL_QUEUE = list(sc.fail_tg or [])
        SEND_COST = sc.send_cost
        PDF_QUEUE = list(sc.pdf_returns or [])
        PDF_CALLS = 0
        jump = sc.rounds[0].get("jump_after") if sc.rounds else None
        CLOCK = FakeClock(NOW0, jump)
        monitor.time = CLOCK
        outs: list[tuple[int, str, str]] = []
        for rnd in sc.rounds:
            specs = rnd.get("sources") or {}
            monitor.SOURCES = [
                (name, make_audited_src(name, specs.get(name, src_spec()), CLOCK))
                for name in SRC_NAMES
            ]
            monitor.ROUND_START = 0.0
            monitor.SEND_ERR = ""
            monitor.SEND_RETRY_AFTER = None
            monitor.CFG.clear()
            monitor.TS_BAD.clear()
            sys.argv = ["monitor.py", *sc.args]
            so, se = io.StringIO(), io.StringIO()
            try:
                with contextlib.redirect_stdout(so), contextlib.redirect_stderr(se):
                    rc = monitor.main()
            except BaseException as e:  # noqa: BLE001 - 任何逃逸都是 golden 不成立
                raise VerificationError(
                    f"{sc.name}: main() raised {type(e).__name__}: {e}") from e
            finally:
                # 注入的异常对象会保留 traceback -> send 帧 -> f_back(main 帧) -> lock fd，
                # 不清掉会让下一轮 flock 误判「另一实例在跑」。清 traceback 后再 gc。
                for exc in (sc.fail_tg or []):
                    try:
                        exc.__traceback__ = None
                    except Exception:
                        pass
                gc.collect()          # 确保 lock fd 关闭
            outs.append((int(rc), so.getvalue(), se.getvalue()))
            advance = rnd.get("advance_after")
            if advance:
                CLOCK.t += advance
                CLOCK.mono += advance

        rcs = [o[0] for o in outs]
        if rcs != list(sc.expected_rcs):
            raise VerificationError(
                f"{sc.name}: rc={rcs} != expected {list(sc.expected_rcs)}")
        if NEVER_CALLED:
            raise VerificationError(
                f"{sc.name}: never-source called: {sorted(set(NEVER_CALLED))}")
        if sc.expect_pdf_calls is not None and PDF_CALLS != sc.expect_pdf_calls:
            raise VerificationError(
                f"{sc.name}: pdf_text calls={PDF_CALLS} != {sc.expect_pdf_calls}")
        if sc.expect_tg_calls is not None and len(CALLS) != sc.expect_tg_calls:
            raise VerificationError(
                f"{sc.name}: tg calls={len(CALLS)} != {sc.expect_tg_calls}")

        files: dict[str, bytes] = {}
        files["rc.json"] = (json.dumps(rcs, ensure_ascii=False) + "\n").encode()
        files["sends.jsonl"] = "".join(
            json.dumps(c, ensure_ascii=False, sort_keys=True) + "\n"
            for c in CALLS).encode()
        if files["sends.jsonl"].count(b"\n") != len(CALLS):
            raise VerificationError(
                f"{sc.name}: sends.jsonl 行数 != fake_tg 调用次数")
        files["stdout.txt"] = normalize_text(_join_rounds(outs, 1), TMP).encode()
        files["stderr.txt"] = normalize_text(_join_rounds(outs, 2), TMP).encode()
        for name, path in (("queue.json", monitor.QUEUE),
                           ("seen.json", monitor.STATE),
                           ("pending.json", monitor.PENDING)):
            try:
                files[name] = path.read_bytes()
            except FileNotFoundError:
                pass
        backup_name = None
        bad = sorted(monitor.QUEUE.parent.glob(monitor.QUEUE.name + ".bad-*"))
        if bad:
            backup_name = bad[0].name
            files["queue.bad.json"] = bad[0].read_bytes()
        absent = [n for n in OPTIONAL_FILES if n not in files]
        extras = {"backup_name": backup_name, "absent": absent, "rcs": rcs,
                  "pdf_calls": PDF_CALLS, "tg_calls": len(CALLS)}
        return files, extras
    finally:
        restore(monitor, saved)
        shutil.rmtree(tmp, ignore_errors=True)


# ------------------------------------------------------------------ golden io
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def scenario_dir(name: str, root: Path | None = None) -> Path:
    return (root or GOLDEN_DIR) / name


def write_snapshot(name: str, files: dict[str, bytes], root: Path | None = None) -> list[str]:
    d = scenario_dir(name, root)
    d.mkdir(parents=True, exist_ok=True)
    wrote = []
    for fname, data in files.items():
        p = d / fname
        if not p.exists() or p.read_bytes() != data:
            p.write_bytes(data)
            wrote.append(fname)
    for p in d.iterdir():
        if p.is_file() and p.name not in files:
            p.unlink()
            wrote.append(f"-{p.name}")
    return wrote


def read_snapshot(name: str, root: Path | None = None) -> dict[str, bytes]:
    d = scenario_dir(name, root)
    out = {}
    if not d.is_dir():
        return out
    for p in sorted(d.iterdir()):
        if p.is_file():
            out[p.name] = p.read_bytes()
    return out


def git_head() -> str:
    try:
        r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                           capture_output=True, text=True, timeout=10)
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return "unknown"


def porcelain_path(line: str) -> str:
    path = line[3:] if len(line) > 3 else ""
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    return path.strip().strip('"')


def filter_dirty(lines: list[str]) -> list[str]:
    """脏树判定：排除本次产物 tests/golden.py 与 tests/golden/**。"""
    keep = []
    for line in lines:
        if not line.strip():
            continue
        path = porcelain_path(line)
        if path == "tests/golden.py" or path.startswith("tests/golden/"):
            continue
        keep.append(line)
    return keep


def git_dirty() -> list[str]:
    try:
        r = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                           capture_output=True, text=True, timeout=10)
    except Exception as e:  # noqa: BLE001
        return [f"<git status failed: {e}>"]
    if r.returncode != 0:
        return [f"<git status rc={r.returncode}: {r.stderr.strip()[:120]}>"]
    return filter_dirty(r.stdout.splitlines())


def build_manifest(reason: str, results: dict[str, tuple[dict, dict]]) -> dict:
    scenarios = {}
    for name in SCENARIO_NAMES:
        files, extras = results[name]
        scenarios[name] = {
            "absent": extras["absent"],
            "backup_name": extras["backup_name"],
            "files": {fname: sha256_bytes(data)
                      for fname, data in sorted(files.items())},
        }
    return {
        "version": 1,
        "generated_at_commit": git_head(),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}."
                  f"{sys.version_info.micro}",
        "record_reason": reason,
        "scenarios": scenarios,
    }


def dump_manifest(man: dict) -> bytes:
    return (json.dumps(man, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def load_manifest(root: Path | None = None) -> dict | None:
    path = (root or GOLDEN_DIR) / "MANIFEST.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def verify_manifest(root: Path | None = None) -> list[str]:
    """只读校验快照与 MANIFEST；返回问题列表（空 = 通过）。"""
    root = root or GOLDEN_DIR
    man = load_manifest(root)
    if not isinstance(man, dict):
        return ["MANIFEST.json 缺失或不可解析"]
    problems: list[str] = []
    if man.get("version") != 1:
        problems.append(f"version={man.get('version')!r} != 1")
    scen = man.get("scenarios")
    if not isinstance(scen, dict):
        return problems + ["scenarios 缺失"]
    if tuple(sorted(scen)) != tuple(sorted(SCENARIO_NAMES)):
        problems.append(f"scenarios 条数/名单不符: {len(scen)}")
        return problems
    for name in SCENARIO_NAMES:
        entry = scen[name]
        files = entry.get("files") or {}
        absent = set(entry.get("absent") or [])
        d = scenario_dir(name, root)
        if not d.is_dir():
            problems.append(f"{name}: 目录缺失")
            continue
        on_disk = {p.name for p in d.iterdir() if p.is_file()}
        if on_disk != set(files):
            problems.append(f"{name}: 文件集合不符 {sorted(on_disk)} != {sorted(files)}")
        for fname, want in files.items():
            p = d / fname
            if not p.is_file():
                problems.append(f"{name}/{fname}: 缺失")
                continue
            got = sha256_bytes(p.read_bytes())
            if got != want:
                problems.append(f"{name}/{fname}: sha256 {got[:12]} != {want[:12]}")
        for fname in absent:
            if (d / fname).exists():
                problems.append(f"{name}/{fname}: 应 absent 却存在")
        backup = entry.get("backup_name")
        if backup and "queue.bad.json" not in files:
            problems.append(f"{name}: backup_name={backup} 未登记 queue.bad.json")
        if not backup and "queue.bad.json" in files:
            problems.append(f"{name}: 有 queue.bad.json 但 backup_name 为空")
    return problems


def print_diff(label: str, want: bytes, got: bytes) -> None:
    w = want.decode("utf-8", "backslashreplace").splitlines(keepends=True)
    g = got.decode("utf-8", "backslashreplace").splitlines(keepends=True)
    out = "".join(difflib.unified_diff(
        w, g, fromfile=f"golden/{label}", tofile=f"actual/{label}"))
    sys.stdout.write(out)
    if out and not out.endswith("\n"):
        sys.stdout.write("\n")

def compare_snapshot(name: str, entry: dict, files: dict[str, bytes],
                     extras: dict, root: Path | None = None) -> list[dict]:
    """比对单个场景；返回问题列表（空 = 一致）。"""
    problems: list[dict] = []
    want_files = entry.get("files") or {}
    want_absent = set(entry.get("absent") or [])
    for fname, want_hash in sorted(want_files.items()):
        got = files.get(fname)
        if got is None:
            problems.append({"file": fname, "kind": "missing"})
            continue
        if sha256_bytes(got) != want_hash:
            golden = (scenario_dir(name, root) / fname).read_bytes()
            problems.append({"file": fname, "kind": "diff",
                             "golden": golden, "actual": got})
    for fname in sorted(want_absent):
        if fname in files:
            problems.append({"file": fname, "kind": "unexpected"})
    for fname in sorted(files):
        if fname not in want_files and fname not in want_absent:
            problems.append({"file": fname, "kind": "unregistered"})
    if extras.get("backup_name") != entry.get("backup_name"):
        problems.append({"file": "backup_name", "kind": "meta",
                         "golden": str(entry.get("backup_name")).encode(),
                         "actual": str(extras.get("backup_name")).encode()})
    return problems


def cmd_check(args, root: Path) -> int:
    print("golden: record locked (requires --reason + GOLDEN_RECORD_APPROVED; "
          "R2–R8 forbid --record)")
    problems = verify_manifest(root)
    if problems:
        for p in problems:
            print(f"golden: manifest: {p}")
        print("golden: FAIL (manifest)")
        return 1
    man = load_manifest(root)
    if args.scenario and args.scenario not in SCENARIO_NAMES:
        print(f"golden: unknown scenario {args.scenario!r}")
        return 2
    names = [args.scenario] if args.scenario else list(SCENARIO_NAMES)
    scenarios = {s.name: s for s in build_scenarios()}
    passed, failed = 0, []
    for name in names:
        sc = scenarios[name]
        try:
            files, extras = run_scenario(sc)
        except VerificationError as e:
            print(f"FAIL {name}: {e}")
            failed.append(name)
            continue
        found = compare_snapshot(name, man["scenarios"][name], files, extras, root)
        if found:
            failed.append(name)
            for p in found:
                if p["kind"] == "diff":
                    print_diff(f"{name}/{p['file']}", p["golden"], p["actual"])
                print(f"FAIL {name} ({p['kind']}: {p['file']})")
        else:
            passed += 1
            print(f"PASS {name}")
    total = len(names)
    if failed:
        print(f"golden: FAIL ({passed}/{total})")
        return 1
    print(f"golden: PASS ({passed}/{total})")
    return 0


def cmd_record(args, root: Path) -> int:
    reason = str(args.reason or "").strip()
    if not reason:
        print("golden: --record requires --reason <文本> (写入 MANIFEST.record_reason)")
        return 2
    approved = os.environ.get("GOLDEN_RECORD_APPROVED", "").strip()
    if not approved:
        print("golden: --record requires GOLDEN_RECORD_APPROVED=<reason> env "
              "(audit guard; --allow-dirty does not bypass)")
        return 2
    if approved != reason:
        print("golden: GOLDEN_RECORD_APPROVED must equal --reason (audit guard)")
        return 2
    if not args.allow_dirty:
        dirty = git_dirty()
        if dirty:
            print("golden: dirty tree, --record refused (exit 2):")
            for line in dirty:
                print(f"  {line}")
            print("golden: only --allow-dirty with a CTO-approved reason may override")
            return 2
    if git_head() == "unknown":
        print("golden: cannot resolve git HEAD; --record refused (exit 2)")
        return 2
    root.mkdir(parents=True, exist_ok=True)
    results: dict[str, tuple[dict, dict]] = {}
    for sc in build_scenarios():
        try:
            files, extras = run_scenario(sc)
        except VerificationError as e:
            print(f"golden: record aborted: {e}")
            return 1
        wrote = write_snapshot(sc.name, files, root)
        results[sc.name] = (files, extras)
        state = ("WROTE " + ",".join(wrote)) if wrote else "UNCHANGED"
        print(f"{state} {sc.name}")
    man = build_manifest(reason, results)
    (root / "MANIFEST.json").write_bytes(dump_manifest(man))
    print(f"golden: recorded {len(results)} scenarios "
          f"(reason: {reason}, commit: {man['generated_at_commit']}, "
          f"python: {man['python']})")
    return 0


def cmd_list(args, root: Path) -> int:
    scenarios = build_scenarios()
    print(f"golden scenarios: {len(scenarios)} "
          f"(python {sys.version_info.major}.{sys.version_info.minor}."
          f"{sys.version_info.micro})")
    for sc in scenarios:
        print(f"  {sc.name:22s} rounds={len(sc.rounds)} "
              f"expected_rcs={sc.expected_rcs}")
    man = load_manifest(root)
    if man:
        print(f"manifest: present, commit={man.get('generated_at_commit')}, "
              f"reason={man.get('record_reason')!r}, "
              f"scenarios={len(man.get('scenarios') or {})}")
    else:
        print("manifest: absent")
    return 0


def _snapshot_hash(files: dict[str, bytes]) -> str:
    h = hashlib.sha256()
    for fname in sorted(files):
        h.update(fname.encode())
        h.update(b"\0")
        h.update(files[fname])
        h.update(b"\0")
    return h.hexdigest()


def cmd_selfcheck(args, root: Path) -> int:
    ok = True
    # B10-G05: normalize_text 单元用例
    tmp = "/tmp/fnm-golden-unit"
    cases = [
        ("[2026-11-04 21:06:40] 沪铜", "[<TS>] 沪铜"),
        (f"{tmp}/state/queue.json", "<TMP>/state/queue.json"),
        ("queue.json.bad-1793800000", "queue.json.bad-<EPOCH>"),
        ("x pid=4242 y", "x pid=<PID> y"),
    ]
    for src, want in cases:
        got = normalize_text(src, tmp)
        if got != want:
            ok = False
            print(f"FAIL normalize: {src!r} -> {got!r}")
    print(f"normalize_text: {len(cases)} cases {'PASS' if ok else 'FAIL'}")
    # 快照中不得残留真实时间戳 / 临时路径 / 未归一化 backup / pid
    pats = [re.compile(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]"),
            re.compile(r"\.bad-\d+"), re.compile(r"\bpid=\d+"),
            re.compile(r"fnm-golden-")]
    leaks = 0
    for name in SCENARIO_NAMES:
        for fname, data in read_snapshot(name, root).items():
            text = data.decode("utf-8", "ignore")
            for pat in pats:
                if pat.search(text):
                    leaks += 1
                    print(f"FAIL leak {name}/{fname}: {pat.pattern}")
    print(f"snapshot leak scan: {'PASS' if leaks == 0 else f'FAIL ({leaks})'}")
    ok = ok and leaks == 0
    # B10-G07: 同一场景 3 次跑，快照哈希三次相同
    scenarios = {s.name: s for s in build_scenarios()}
    for name in ("normal_round", "digest_due", "research_delayed",
                 "queue_corrupt", "src_alert_threshold"):
        hashes: list[str] = []
        try:
            for _ in range(3):
                files, _extras = run_scenario(scenarios[name])
                hashes.append(_snapshot_hash(files))
        except VerificationError as e:
            ok = False
            print(f"FAIL determinism {name}: {e}")
            continue
        same = len(set(hashes)) == 1
        ok = ok and same
        print(f"determinism {name}: {'PASS' if same else 'FAIL'} {hashes[0][:16]}")
    # B10-G06: 临时副本改一个字节 -> --check 必须 exit 1 且报 manifest
    tamper_root = Path(tempfile.mkdtemp(prefix="fnm-golden-tamper-"))
    try:
        shutil.copytree(root, tamper_root / "golden")
        target = tamper_root / "golden" / "normal_round" / "queue.json"
        target.write_bytes(target.read_bytes() + b"X")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = cmd_check(argparse.Namespace(scenario=None), tamper_root / "golden")
        out = buf.getvalue()
        good = code == 1 and "manifest" in out
        ok = ok and good
        print(f"manifest tamper: {'PASS' if good else 'FAIL'} (exit={code})")
    finally:
        shutil.rmtree(tamper_root, ignore_errors=True)
    # B10-G08 的脏树判定：纯函数用例（不在真实仓库制造脏改动）
    lines = ["?? tests/golden.py", " M monitor.py",
             "?? tests/golden/normal_round/rc.json", "?? README.md"]
    kept = filter_dirty(lines)
    good = kept == [" M monitor.py", "?? README.md"]
    ok = ok and good
    print(f"dirty-tree filter: {'PASS' if good else 'FAIL'} {kept}")
    print(f"selfcheck: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def _pcts(samples: list[float]) -> tuple[float, float, float, float]:
    s = sorted(samples)
    n = len(s)

    def at(q: float) -> float:
        return s[min(n - 1, max(0, int(round(q * (n - 1)))))]

    return at(0.5), at(0.99), s[0], s[-1]


def cmd_bench(args, root: Path) -> int:
    which = args.bench
    rounds = args.rounds or {"main": 300, "fetch": 500, "http": 5000}[which]
    if rounds < 1:
        print("golden: --rounds must be >= 1")
        return 2
    samples: list[float] = []
    if which in ("main", "fetch"):
        if which == "main":
            sc = {s.name: s for s in build_scenarios()}["normal_round"]
        else:
            sc = Scenario("bench_fetch", [{"sources": empty_sources()}], [0],
                          state={"seedmarker": 1}, conf=base_conf())
        for _ in range(rounds):
            t0 = time.perf_counter()
            run_scenario(sc)
            samples.append((time.perf_counter() - t0) * 1000.0)
    else:
        # http: 用本地 subprocess stub 驱动真实 monitor.http()，不联网、不发请求
        real_run = subprocess.run

        def _stub_run(*_a, **_k):
            return types.SimpleNamespace(returncode=0, stdout=b'{"ok":1}\n200')

        subprocess.run = _stub_run
        try:
            for _ in range(rounds):
                t0 = time.perf_counter()
                monitor.http("http://bench.invalid/x")
                samples.append((time.perf_counter() - t0) * 1000.0)
        finally:
            subprocess.run = real_run
    p50, p99, lo, hi = _pcts(samples)
    print(f"bench {which}: rounds={rounds} p50={p50:.4f}ms p99={p99:.4f}ms "
          f"min={lo:.4f}ms max={hi:.4f}ms")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Batch 10 R1 golden equivalence harness (offline, no network, "
                    "no real Telegram, no real state)")
    ap.add_argument("--record", action="store_true",
                    help="写快照 + MANIFEST（仅 R1 落地时；必须同时给 --reason 与 "
                         "GOLDEN_RECORD_APPROVED；R2–R8 禁止）")
    ap.add_argument("--reason", help="记录原因，写入 MANIFEST.record_reason")
    ap.add_argument("--check", action="store_true",
                    help="默认模式：先校验 MANIFEST 再运行 28 场景比对")
    ap.add_argument("--list", action="store_true", help="列出场景清单")
    ap.add_argument("--scenario", help="只跑指定场景（仍先校验整个 MANIFEST）")
    ap.add_argument("--allow-dirty", action="store_true",
                    help="允许在有未提交改动的树上 --record（须报告登记）")
    ap.add_argument("--bench", choices=["main", "fetch", "http"],
                    help="离线基准测量")
    ap.add_argument("--rounds", type=int, help="--bench 的轮数")
    ap.add_argument("--selfcheck", action="store_true",
                    help="归一化/确定性/防篡改自检")
    ap.add_argument("--golden-dir", help="覆盖快照目录（自检用）")
    args = ap.parse_args()
    root = Path(args.golden_dir).resolve() if args.golden_dir else GOLDEN_DIR
    if args.record and args.check:
        print("golden: --record 与 --check 互斥")
        return 2
    if args.record:
        return cmd_record(args, root)
    if args.list:
        return cmd_list(args, root)
    if args.selfcheck:
        return cmd_selfcheck(args, root)
    if args.bench:
        return cmd_bench(args, root)
    return cmd_check(args, root)


if __name__ == "__main__":
    sys.exit(main())
