#!/usr/bin/env python3
"""monitor.py --collect 采集模式测试 — spec-fnm-http §3.1.2 / MC-01..MC-04。

stdlib-only, Windows/POSIX 通用。fcntl stub 惯例同 test_items_archive.py:
测试导入前向 sys.modules 注入最小 stub (POSIX 部署面行为不变)。
直接运行: python tests/test_collect.py
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent

if "fcntl" not in sys.modules:                       # Windows 无 fcntl: 最小 stub 仅供 import+锁面
    _fcntl = types.ModuleType("fcntl")
    _fcntl.LOCK_EX, _fcntl.LOCK_NB, _fcntl.LOCK_UN = 2, 4, 8
    _fcntl.flock = lambda *a, **k: None
    sys.modules["fcntl"] = _fcntl

_spec = importlib.util.spec_from_file_location("monitor", ROOT / "monitor.py")
monitor = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("monitor", monitor)
_spec.loader.exec_module(monitor)

_REDIRECT_KEYS = ("STATE", "QUEUE", "PENDING", "LOCK", "CONF", "LOG", "ERRLOG",
                  "SOURCES", "tg", "rotate_log", "rotate_stderr",
                  "ITEMS_ARCHIVE", "ITEMS_META")


def _src_one(key: str = "mc-1"):
    """单源单条目假源 (正文沿用 B18 实测可过滤文本)."""
    return lambda deadline=None: [
        {"src": "S0", "ts": int(time.time()) - 60,
         "text": "沪铜库存变化9吨环比9%", "key": key}]


def run_main(td: Path, args: list[str], srcs, conf_extra=None, state=None):
    """_b6_run_main 迷你版: 临时目录重定向全部 state/conf/log 路径, stub tg 捕获载荷.

    返回 (rc, payloads, seen_dict, queue, arch_path).
    """
    st = td / "state"
    st.mkdir(parents=True, exist_ok=True)
    kept = {k: getattr(monitor, k) for k in _REDIRECT_KEYS}
    payloads: list[str] = []
    arch = st / "items.jsonl"
    try:
        monitor.STATE = st / "seen.json"
        monitor.QUEUE = st / "queue.json"
        monitor.PENDING = st / "pending.json"
        monitor.LOCK = st / "lock"
        monitor.CONF = st / "conf.json"
        monitor.LOG = st / "out.log"
        monitor.ERRLOG = st / "err.log"
        monitor.ITEMS_ARCHIVE = arch
        monitor.ITEMS_META = st / "items.meta.json"
        monitor.SOURCES = srcs
        monitor.rotate_log = lambda: None
        monitor.rotate_stderr = lambda: None

        def _fake_tg(method, payload, tk, proxy, timeout=25):
            payloads.append(str((payload or {}).get("text") or method))
            return {"ok": True}

        monitor.tg = _fake_tg
        conf = {"token": "x", "chat_id": 1, "max_age_min": 180, "digest_min": 30,
                "digest_max_items": 40}                  # token 基线同 _b6_run_main; 缺 token 用例以 conf_extra={"token":""} 覆盖
        conf.update(conf_extra or {})
        monitor.CONF.write_text(json.dumps(conf), encoding="utf-8")
        if state is not None:
            monitor.STATE.write_text(json.dumps(state), encoding="utf-8")
        old_argv = sys.argv
        try:
            sys.argv = ["monitor.py"] + list(args)
            rc = monitor.main()
        finally:
            sys.argv = old_argv
        seen = (json.loads(monitor.STATE.read_text())
                if monitor.STATE.exists() else {})
        queue, _, _ = monitor.load_queue()
        return rc, payloads, seen, queue, arch
    finally:
        for k, v in kept.items():
            setattr(monitor, k, v)


def _arch_lines(arch: Path) -> list[dict]:
    if not arch.exists():
        return []
    return [json.loads(l) for l in arch.read_text(encoding="utf-8").splitlines()
            if l.strip()]


class _CollectCase(unittest.TestCase):
    """MC 钉: 每个用例独立 tempdir, 真实跑 monitor.main()."""

    def _run(self, td: Path, args, **kw):
        return run_main(td, args, [("S0", _src_one())], **kw)

    # ---- MC-01: --collect 无 token 可启动; 对照普通模式仍退 3 ----
    def test_mc01_collect_no_token_starts(self):
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(os.environ):
                os.environ.pop("FUTNEWS_TOKEN", None)
                rc, *_ = self._run(Path(td), ["--collect"],
                                   conf_extra={"token": ""},
                                   state={"seedmarker": 1})
            self.assertEqual(0, rc)                      # 不以 3 退出

    def test_mc01b_normal_mode_no_token_still_3(self):
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(os.environ):
                os.environ.pop("FUTNEWS_TOKEN", None)
                rc, *_ = self._run(Path(td), [],
                                   conf_extra={"token": ""},
                                   state={"seedmarker": 1})
            self.assertEqual(3, rc)                      # 对照: 非 collect 缺 token 仍退 3

    # ---- MC-02: 一轮后 items 存档 + queue 落盘 ----
    def test_mc02_archive_and_queue_written(self):
        with tempfile.TemporaryDirectory() as td:
            rc, _pay, _seen, queue, arch = self._run(
                Path(td), ["--collect"], state={"seedmarker": 1})
            self.assertEqual(0, rc)
            rows = _arch_lines(arch)
            self.assertEqual(1, len(rows))               # items_archive_append 真追加
            self.assertEqual("mc-1", rows[0]["key"])
            for f in ("src", "ts", "text", "key", "schema"):
                self.assertIn(f, rows[0])
            self.assertTrue(any(it.get("key") == "mc-1" for it in queue))  # queue 落盘

    # ---- MC-03: 零 TG 出站 (四函数桩零调用 + tg 载荷零) ----
    def test_mc03_zero_tg_outbound(self):
        calls = {n: 0 for n in ("flush_round", "send", "alert_all_failed",
                                "note_recovered", "discover_chat_id")}
        kept = {n: getattr(monitor, n) for n in calls}
        for n in calls:
            setattr(monitor, n, (lambda name:
                                 (lambda *a, **kw: calls.__setitem__(name, calls[name] + 1) or False))(n))
        try:
            with tempfile.TemporaryDirectory() as td:
                rc, pay, _seen, queue, _arch = self._run(
                    Path(td), ["--collect"], state={"seedmarker": 1})
        finally:
            for n, v in kept.items():
                setattr(monitor, n, v)
        self.assertEqual(0, rc)
        self.assertEqual({"flush_round": 0, "send": 0, "alert_all_failed": 0,
                          "note_recovered": 0, "discover_chat_id": 0}, calls)
        self.assertEqual([], pay)                        # tg 传输层零调用 (getUpdates/sendMessage)
        self.assertEqual(1, len(queue))                  # 数据面未受损

    def test_mc03b_normal_mode_calls_flush_round(self):
        """对照: 非 collect 同一场景 flush_round 被调用 (证明桩有效)."""
        calls = {"flush_round": 0}
        kept = getattr(monitor, "flush_round")
        monitor.flush_round = lambda *a, **kw: calls.__setitem__(
            "flush_round", calls["flush_round"] + 1) or 0
        try:
            with tempfile.TemporaryDirectory() as td:
                self._run(Path(td), [], state={"seedmarker": 1})
        finally:
            monitor.flush_round = kept
        self.assertEqual(1, calls["flush_round"])

    # ---- MC-04: dry-run 与 collect 分离; 组合取交集 ----
    def test_mc04_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as td:
            rc, _pay, _seen, queue, arch = self._run(
                Path(td), ["--dry-run"], state={"seedmarker": 1})
            self.assertEqual(0, rc)
            self.assertEqual([], _arch_lines(arch))      # dry-run 存档不写
            self.assertEqual([], queue)                  # queue 不落盘

    def test_mc04b_collect_dry_intersection(self):
        with tempfile.TemporaryDirectory() as td:
            rc, pay, _seen, queue, arch = self._run(
                Path(td), ["--collect", "--dry-run"], state={"seedmarker": 1})
            self.assertEqual(0, rc)
            self.assertEqual([], _arch_lines(arch))      # 交集: 零落盘
            self.assertEqual([], queue)
            self.assertEqual([], pay)                    # 零 TG

    def test_mc04c_collect_then_normal_round_archives_once(self):
        """collect 已归档条目不被普通轮重复落档 (存档 key 去重钉)."""
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            self._run(td, ["--collect"], state={"seedmarker": 1})
            rows1 = _arch_lines(td / "state" / "items.jsonl")
            self.assertEqual(1, len(rows1))
            # 第 2 轮: 同 key 仍在队列 -> added=0 -> 不重复落档
            seen = json.loads((td / "state" / "seen.json").read_text())
            rc, _p, _s, _q, arch = self._run(td, ["--collect"], state=seen)
            self.assertEqual(0, rc)
            self.assertEqual(1, len(_arch_lines(arch)))


class _DeepReviewCase(unittest.TestCase):
    """四轨深审必修钉 (monitor.py 面): F2 读径毒行 / F4 conf 写回 / F5 未知参拒认."""

    def test_f2_read_archive_hugeint_line_no_tail_drop(self):
        """F2 毒行截尾: >4300 位整数字面量行 json.loads 抛裸 ValueError——旧码
        穿透内层进外层 -> 该行起整尾丢且毒行留存永不自愈; 现按坏行跳过并计数."""
        with tempfile.TemporaryDirectory() as td:
            arch = Path(td) / "items.jsonl"
            saved = monitor.ITEMS_ARCHIVE
            monitor.ITEMS_ARCHIVE = arch
            try:
                good1 = {"src": "s", "ts": 1, "text": "a", "key": "k1", "schema": 1}
                good2 = {"src": "s", "ts": 2, "text": "b", "key": "k2", "schema": 1}
                huge = '{"key": ' + "9" * 5000 + ', "ts": 1, "text": "x"}'
                arch.write_text(json.dumps(good1) + "\n" + huge + "\n"
                                + json.dumps(good2) + "\n", encoding="utf-8")
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    items = monitor.read_items_archive()
                self.assertEqual(["k1", "k2"], [i["key"] for i in items])
                self.assertIn("跳过 1 行", buf.getvalue())      # 坏行计数入日志
            finally:
                monitor.ITEMS_ARCHIVE = saved

    def test_f4_discover_chat_id_preserves_conf_keys(self):
        """F4 conf 写回清键: save_json 前重读 CONF 只合 chat_id——conf 被宽容
        读成 {} 时, 回写不再连带清掉 items_token/items_bind/items_port/
        items_cidrs/token."""
        with tempfile.TemporaryDirectory() as td:
            conf_path = Path(td) / "conf.json"
            original = {"token": "t0", "chat_id": 1,
                        "items_token": "it", "items_bind": "127.0.0.1",
                        "items_port": 8766, "items_cidrs": ["192.168.31.0/24"]}
            conf_path.write_text(json.dumps(original), encoding="utf-8")
            saved = {k: getattr(monitor, k) for k in ("CONF", "tg")}
            monitor.CONF = conf_path
            monitor.tg = lambda *a, **k: {"ok": True, "result": [
                {"message": {"chat": {"id": 777}}}]}
            try:
                cid = monitor.discover_chat_id({}, "tk")     # 内存 conf={} 模拟宽容读败
            finally:
                for k, v in saved.items():
                    setattr(monitor, k, v)
            self.assertEqual(777, cid)
            got = json.loads(conf_path.read_text(encoding="utf-8"))
            self.assertEqual(777, got["chat_id"])
            for k, v in original.items():
                if k != "chat_id":
                    self.assertEqual(v, got[k], k)           # 其余键原样保留

    def test_f5_unknown_flag_exits_2_fail_loud(self):
        """F5: 任何 --xxx 形未列白名单的参 -> stderr fail-loud + exit 2;
        --collect=x / 拼错不再静默落正常模带 TG 出站."""
        for bad in ("--bogus", "--collect=x", "--colect", "--limit=5"):
            sbuf, obuf = io.StringIO(), io.StringIO()
            with contextlib.redirect_stderr(sbuf), contextlib.redirect_stdout(obuf):
                with self.assertRaises(SystemExit) as cm:
                    monitor.load_conf([bad])
            self.assertEqual(2, cm.exception.code, bad)
            self.assertIn("未知参数", sbuf.getvalue() + obuf.getvalue(), bad)

    def test_f5_known_flags_survive_whitelist(self):
        """F5 不误杀: 既有旗标全集照常过闸 (--limit 带值 / --dump-items 两形)."""
        good = (["--collect"], ["--collect", "--dry-run"], ["--stats"],
                ["--queue"], ["--seed"], ["--all"], ["--explain"],
                ["--flush-now"], ["--dry-run"], ["--limit", "5"],
                ["--selftest"], ["--dump-items"],
                [f"--dump-items={os.devnull}"])
        for args in good:
            conf, _opts = monitor.load_conf(list(args))
            self.assertIsInstance(conf, dict, args)

    def test_f5_arg_value_never_echoed(self):
        """F5: --foo=SECRET 类拼参 -> 日志只回显旗标名 (= 后值永不入日志)."""
        sbuf, obuf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(sbuf), contextlib.redirect_stdout(obuf):
            with self.assertRaises(SystemExit):
                monitor.load_conf(["--token=SECRET-9f8e"])
        out = sbuf.getvalue() + obuf.getvalue()
        self.assertIn("--token", out)
        self.assertNotIn("SECRET-9f8e", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
