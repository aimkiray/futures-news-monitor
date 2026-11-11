#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""extract_telegram.py — 从 Telegram Desktop 导出 JSON 中筛出期货相关消息.

过滤判定逐条复用 monitor.py 的 filter_reason (判定管线 SKIP→STRONG→NOISE_HARD
→目标价→MACRO→ANCHOR→NOISE_SOFT→CONTEXT), 不复制词表与逻辑.

用法:
    python extract_telegram.py --input <result.json> [--out-dir <目录>] [--sample N]

输出 (<out-dir> 缺省 = 输入文件所在目录):
    futures-news.json  保留消息数组 [{id,date,from,text}], 按 date 升序
    futures-news.txt   每行 [YYYY-MM-DD HH:MM] <text 前 120 字 (截断加 …), 换行替换为空格>
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import types
import zoneinfo
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

TZ8 = timezone(timedelta(hours=8))      # 固定 +08:00, 与 monitor Asia/Shanghai 口径一致

# monitor.py 顶层 `import fcntl` 在 Windows 直接 ImportError; fcntl 仅被文件锁
# 路径使用, 提取路径不触及 -> 仅 import 失败 (Windows) 时塞空模块占位, POSIX 不遮蔽真 fcntl.
try:
    import fcntl  # noqa: F401
except ImportError:
    sys.modules.setdefault("fcntl", types.ModuleType("fcntl"))
# monitor.py 模块级 `ZoneInfo("Asia/Shanghai")` 在干净 Windows (无系统时区库且无
# tzdata 包) 下 import 即崩; TZ_CN 仅日志/推送消费 -> 探测失败则换固定 +08:00 替身.
try:
    zoneinfo.ZoneInfo("Asia/Shanghai")
except Exception:
    zoneinfo.ZoneInfo = lambda key: TZ8
sys.path.insert(0, str(Path(__file__).resolve().parent))
import monitor  # noqa: E402


def join_text(t, _depth: int = 0) -> str:
    """拼接 Telegram text 字段: 字符串原样; dict 取其 text 值; 列表逐段拼接 (可嵌套, 限深 200)."""
    if _depth > 200:                             # 深嵌套恶意输入截断返回, 防 RecursionError
        return ""
    if isinstance(t, str):
        return t
    if isinstance(t, dict):
        return join_text(t.get("text", ""), _depth + 1)
    if isinstance(t, list):
        return "".join(join_text(seg.get("text", ""), _depth + 1) if isinstance(seg, dict)
                       else join_text(seg, _depth + 1) for seg in t)
    return str(t) if t is not None else ""


def cn_time(d: str) -> datetime | None:
    """ISO 串 -> naive datetime; 含时区偏移先换算 +08:00 再去 tzinfo; 解析失败返回 None."""
    try:
        dt = datetime.fromisoformat(d)
        return dt.astimezone(TZ8).replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError, OverflowError, OSError):
        return None                             # 极端日期换算溢出同解析失败处理


def hhmm(d: str) -> str:
    """ISO '2026-07-29T20:54:58' -> '2026-07-29 20:54'; 含偏移归一 +08:00; 失败退化截断."""
    dt = cn_time(d)
    return dt.strftime("%Y-%m-%d %H:%M") if dt is not None else (d or "")[:16].replace("T", " ")


def date_key(d: str) -> tuple:
    """排序键: 可解析 -> (1, naive dt) 按真实时序; 不可解析 -> (0, min) 归一组置前."""
    dt = cn_time(d)
    return (1, dt) if dt is not None else (0, datetime.min)


def write_atomic(path: Path, content: str) -> None:
    """先写 <name>.tmp 再 os.replace: 中断/盘满不留截断产物."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(content, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Telegram 导出 JSON -> 期货相关消息提取")
    ap.add_argument("--input", required=True, help="Telegram Desktop 导出的 result.json")
    ap.add_argument("--out-dir", default="", help="输出目录 (缺省=输入文件所在目录)")
    ap.add_argument("--sample", type=int, default=0, help="额外随机抽样 N 条保留+N 条丢弃核对")
    args = ap.parse_args()
    if args.sample < 0:
        ap.error("--sample 必须 >= 0")

    src = Path(args.input)
    out_dir = Path(args.out_dir) if args.out_dir else src.resolve().parent
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        ap.error(f"输出目录不可用: {out_dir} ({e})")
    jpath = out_dir / "futures-news.json"
    tpath = out_dir / "futures-news.txt"
    if jpath.resolve() == src.resolve() or tpath.resolve() == src.resolve():
        ap.error(f"产物路径与输入文件冲突 (拒绝覆盖输入): {src}")

    try:
        data = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:            # OSError=不可读; ValueError 含 JSON 损坏/BOM/UTF-16/空文件
        ap.error(f"输入不可读或 JSON 解析失败: {src} ({e})")
    if not isinstance(data, dict):
        ap.error(f"输入顶层应为 JSON object (实际: {type(data).__name__})")
    messages = data.get("messages", [])
    if not isinstance(messages, list):
        ap.error(f"messages 字段应为数组 (实际: {type(messages).__name__})")

    kept: list[dict] = []
    dropped: list[tuple] = []                    # (id, reason, text) 供抽样核对
    reasons: Counter = Counter()                 # filter_reason 丢弃原因分布
    type_counts: Counter = Counter()
    n_msg = n_empty = n_nonobj = 0

    for m in messages:
        if not isinstance(m, dict):                # 非对象元素跳过并计数
            n_nonobj += 1
            continue
        tp = m.get("type", "")
        type_counts[tp] += 1
        if tp != "message":                      # 跳过 service 等非消息条目
            continue
        n_msg += 1
        text = join_text(m.get("text"))
        if not text.strip():                     # text 空白时回退 text_entities, 防字段分歧丢内容
            text = join_text(m.get("text_entities"))
        if not text.strip():                     # 空/纯空白文本跳过
            n_empty += 1
            continue
        reason = monitor.filter_reason(text, monitor.DEFAULT_KEYWORDS, True)
        if reason:
            reasons[reason] += 1
            if args.sample:
                dropped.append((m.get("id"), reason, text))
            continue
        d = m.get("date")
        kept.append({"id": m.get("id"), "date": d if isinstance(d, str) else "",
                     "from": m.get("from", ""), "text": text})

    kept.sort(key=lambda it: date_key(it["date"]))   # 统一经 fromisoformat 按真实时序

    lines = []
    for it in kept:
        one_line = it["text"][:120].replace("\r", " ").replace("\n", " ")
        if len(it["text"]) > 120:                    # 截断行尾追加省略标记
            one_line += "…"
        lines.append(f"[{hhmm(it['date'])}] {one_line}")

    try:
        write_atomic(jpath, json.dumps(kept, ensure_ascii=False, indent=2) + "\n")
        write_atomic(tpath, "\n".join(lines) + ("\n" if lines else ""))
    except OSError as e:
        ap.error(f"产物写入失败: {e}")

    n_judge = n_msg - n_empty                    # 实际进入 filter_reason 判定的条数
    print(f"总消息数: {len(messages)}")
    print(f"非 dict 元素跳过: {n_nonobj}")
    print(f"type==message: {n_msg}  (各类型: {dict(type_counts)})")
    print(f"空/纯空白文本跳过: {n_empty}")
    print(f"进入判定: {n_judge} -> 保留: {len(kept)}  丢弃: {n_judge - len(kept)}")
    print("丢弃原因 top10:")
    for r, c in reasons.most_common(10):
        print(f"  {c:>7}  {r}")
    print(f"输出: {jpath}")
    print(f"输出: {tpath}")

    if args.sample:
        rng = random.Random(42)
        print(f"\n--- 抽样核对 (seed=42, 各 {args.sample} 条, 前 80 字) ---")
        for it in rng.sample(kept, min(args.sample, len(kept))):
            prev = it["text"][:80].replace("\r", " ").replace("\n", " ")
            print(f"[保留 id={it['id']}] {prev}")
        for mid, r, t in rng.sample(dropped, min(args.sample, len(dropped))):
            prev = t[:80].replace("\r", " ").replace("\n", " ")
            print(f"[丢弃 id={mid} 原因={r}] {prev}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
