#!/usr/bin/env python
"""Batch 2 语料 A/B: 旧词表(monitor.v4.bak.py) vs 新词表(monitor.py) 的误杀/误放对比.

用法: python ab.py [--samples N]
输入: ../pushed-corpus.txt (生产已推 3000 条) / ../dropped-recent.txt (被丢弃 1408 条)
输出: 计数 + 丢弃原因直方图 + 双向抽样, 纯计算, 不碰生产 state.
"""
import argparse
import collections
import importlib.util
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
_dirs = [HERE, HERE.parent]
CORPUS = next(d / "pushed-corpus.txt" for d in _dirs if (d / "pushed-corpus.txt").exists())
DROPPED = next(d / "dropped-recent.txt" for d in _dirs if (d / "dropped-recent.txt").exists())


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def reasons(mod, texts):
    return [mod.filter_reason(t, mod.DEFAULT_KEYWORDS, True) for t in texts]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", type=int, default=15)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    base = next(d / "baseline-v4.py" for d in (HERE, HERE.parent) if (d / "baseline-v4.py").exists())
    old = load(base, "mold")
    new = load(next(d / "monitor.py" for d in (HERE, HERE.parent) if (d / "monitor.py").exists()),
               "mnew")

    pushed = [l.split("\t", 1)[1].strip() for l in CORPUS.read_text().splitlines() if "\t" in l]
    dropped = [l.split("] ", 1)[1].split(": ", 1)[1].strip()
               for l in DROPPED.read_text().splitlines() if l.startswith("[") and "] " in l]

    k_old = [not r for r in reasons(old, pushed)]
    k_new = [not r for r in reasons(new, pushed)]
    killed = [t for t, a, b in zip(pushed, k_old, k_new) if a and not b]
    rescued = [t for t, r in zip(dropped, reasons(new, dropped)) if not r]

    print(f"已推语料 {len(pushed)} 条: 旧保留 {sum(k_old)} / 新保留 {sum(k_new)} "
          f"-> 新增丢弃 {len(killed)} ({len(killed) / len(pushed) * 100:.1f}%)")
    print(f"旧丢弃池 {len(dropped)} 条: 新放行 {len(rescued)}")
    hist = collections.Counter(reasons(new, killed))
    print("\n新增丢弃原因 (前 8):")
    for reason, n in hist.most_common(8):
        print(f"  {n:5d}  {reason}")

    print(f"\n新增丢弃抽样 ({args.samples}):")
    for t in random.sample(killed, min(args.samples, len(killed))):
        print(f"  - [{new.filter_reason(t, new.DEFAULT_KEYWORDS, True)}] {t[:88]}")
    if rescued:
        print(f"\n新放行 ({len(rescued)}):")
        for t in rescued[:args.samples]:
            print(f"  + {t[:88]}")

    # 「保住了但可能仍无关」: 只有单字金属锚点的条目(最容易误放)
    metal = {"铜", "铝", "锌", "铅", "镍", "锡", "黄金", "白银", "铂", "钯", "玻璃", "苹果"}
    kept = [t for t, r in zip(pushed, reasons(new, pushed)) if not r]
    weak_only = [t for t in kept if any(w in t for w in metal)
                 and not any(s in t for s in new.STRONG)]
    print(f"\n保留但只有裸金属锚点(误放风险区): {len(weak_only)} 条")
    for t in random.sample(weak_only, min(args.samples, len(weak_only))):
        print(f"  ? {t[:88]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
