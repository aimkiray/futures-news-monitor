#!/usr/bin/env python3
"""items 存档契约测试 — spec-fnm-source-tg-notify §3.11 / T-31 / T-32 + --dump-items。

stdlib-only, Windows/POSIX 通用。monitor.py 顶层 `import fcntl` 为 POSIX 硬依赖
(部署目标 macOS launchd), 测试导入前向 sys.modules 注入最小 stub, 不改生产面。
直接运行: python tests/test_items_archive.py
"""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

if "fcntl" not in sys.modules:                       # Windows 无 fcntl: 最小 stub 仅供 import
    _fcntl = types.ModuleType("fcntl")
    _fcntl.LOCK_EX, _fcntl.LOCK_NB, _fcntl.LOCK_UN = 2, 4, 8
    _fcntl.flock = lambda *a, **k: None
    sys.modules["fcntl"] = _fcntl

_spec = importlib.util.spec_from_file_location("monitor", ROOT / "monitor.py")
monitor = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("monitor", monitor)
_spec.loader.exec_module(monitor)

_TS = 1793800000


def _item(key: str, ts: int = _TS, text: str = "测试条目", src: str = "s") -> dict:
    return {"src": src, "ts": ts, "text": text, "key": key}


class _ArchiveCase(unittest.TestCase):
    """把 ITEMS_ARCHIVE / ITEMS_META 指到 tempdir, 测试间互不影响也不碰真实存档。"""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        d = Path(self._td.name)
        self.arch = d / "items.jsonl"
        self.meta = d / "items.meta.json"
        self._saved = {k: getattr(monitor, k) for k in ("ITEMS_ARCHIVE", "ITEMS_META")}
        monitor.ITEMS_ARCHIVE = self.arch
        monitor.ITEMS_META = self.meta

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(monitor, k, v)
        self._td.cleanup()

    def _lines(self) -> list[str]:
        return [ln for ln in self.arch.read_text(encoding="utf-8").splitlines() if ln.strip()]

    def _keys(self) -> list[str]:
        return [json.loads(ln)["key"] for ln in self._lines()]


class TestItemsArchiveDedup(_ArchiveCase):
    def test_t31_same_key_single_line(self):
        """T-31: 同 key 重复命中只落一行 (跨批 + 同批内 + 已归档再撞)."""
        n1 = monitor.items_archive_append([_item("k1"), _item("k2")])
        self.assertEqual(n1, 2)
        # 第二批混有已归档 key: queue_upsert 侧判新增、存档侧仍按自身 key 集合拦截
        n2 = monitor.items_archive_append([_item("k1"), _item("k3")])
        self.assertEqual(n2, 1)
        # 同批内重复 key 也只落一行
        n3 = monitor.items_archive_append([_item("k4"), _item("k4")])
        self.assertEqual(n3, 1)
        self.assertEqual(self._keys(), ["k1", "k2", "k3", "k4"])
        for rec in (json.loads(ln) for ln in self._lines()):
            self.assertEqual(rec["schema"], 1)
            self.assertEqual(set(rec) >= {"src", "ts", "text", "key"}, True)
        # §3.9 旁路 meta: 追加后自愈写出 {"schema":1}
        self.assertEqual(json.loads(self.meta.read_text(encoding="utf-8"))["schema"], 1)

    def test_jsonl_true_append_not_array(self):
        """存档是 JSONL 真追加 (每行独立 JSON), 不是 JSON 数组。"""
        monitor.items_archive_append([_item("k1"), _item("k2")])
        raw = self.arch.read_text(encoding="utf-8")
        self.assertFalse(raw.lstrip().startswith("["))
        lines = raw.splitlines()
        self.assertEqual(len(lines), 2)
        for ln in lines:
            json.loads(ln)                            # 每行独立可解析

    def test_optional_url_snippet_shape(self):
        """行形 {src,ts,text,key,url?,snippet?,schema}: 仅 http url 落档 (与 queue_upsert 同口径)."""
        it = _item("k1")
        it["url"], it["snippet"] = "https://ex.cn/a", "摘要"
        bad = _item("k2")
        bad["url"], bad["snippet"] = "javascript:x", "s"
        monitor.items_archive_append([it, bad])
        recs = [json.loads(ln) for ln in self._lines()]
        self.assertEqual(recs[0]["url"], "https://ex.cn/a")
        self.assertEqual(recs[0]["snippet"], "摘要")
        self.assertNotIn("url", recs[1])
        self.assertNotIn("snippet", recs[1])

    def test_malformed_items_skipped(self):
        """缺 key / 非 int ts / 空 text 的形不合条目不进档。"""
        n = monitor.items_archive_append([
            _item("k1"),
            {"src": "s", "ts": _TS, "text": "无 key"},
            {"src": "s", "ts": "not-int", "text": "x", "key": "k2"},
            {"src": "s", "ts": _TS, "text": "", "key": "k3"},
        ])
        self.assertEqual(n, 1)
        self.assertEqual(self._keys(), ["k1"])


class TestItemsArchiveRotation(_ArchiveCase):
    def test_t32_rotation_keeps_recent_6000(self):
        """T-32 / R-9: 行数 > 10000 触发原子轮转, 保留最近 6000 行。"""
        max_n, keep_n = monitor.ITEMS_ARCHIVE_MAX, monitor.ITEMS_ARCHIVE_KEEP
        self.assertEqual((max_n, keep_n), (10000, 6000))          # 钉住规格常量
        # 预置正好 MAX 行 (直接写文件, 跳过逐条追加耗时; 追加 2 条触发轮转)
        seed_keys = [f"k{i:05d}" for i in range(max_n)]
        self.arch.write_text(
            "".join(json.dumps(_item(k), ensure_ascii=False) + "\n" for k in seed_keys),
            encoding="utf-8")
        n = monitor.items_archive_append([_item("new1"), _item("new2")])
        self.assertEqual(n, 2)
        keys = self._keys()
        self.assertEqual(len(keys), keep_n)
        self.assertEqual(keys[-2:], ["new1", "new2"])             # 最新两条保留在尾部
        first_expect = seed_keys[max_n + 2 - keep_n]              # 首行 = 第 (MAX+2-6000) 条
        self.assertEqual(keys[0], first_expect)
        self.assertNotIn(seed_keys[0], keys)                      # 最旧 40% 已丢弃

    def test_no_rotation_below_max(self):
        """行数 <= MAX 不轮转: 原样追加。"""
        seed = [f"k{i}" for i in range(10)]
        monitor.items_archive_append([_item(k) for k in seed])
        monitor.items_archive_append([_item("k10")])
        self.assertEqual(self._keys(), seed + ["k10"])

    def test_rotation_keeps_jsonl_readable(self):
        """轮转后存档仍是合法 JSONL 且 key 去重语义延续。"""
        max_n = monitor.ITEMS_ARCHIVE_MAX
        self.arch.write_text(
            "".join(json.dumps(_item(f"k{i}")) + "\n" for i in range(max_n)),
            encoding="utf-8")
        monitor.items_archive_append([_item("new1"), _item("new2")])
        items = monitor.read_items_archive()
        self.assertEqual(len(items), monitor.ITEMS_ARCHIVE_KEEP)
        # 轮转后重复撞已保留 key 仍被拦截
        self.assertEqual(monitor.items_archive_append([_item("new1")]), 0)


class TestDumpItems(_ArchiveCase):
    def test_dump_schema_and_parseable(self):
        """--dump-items: 导出 {"schema":1,"ts":int,"items":[...]} 且可解析。"""
        it = _item("k2", text="研报条目")
        it["url"], it["snippet"] = "https://ex.cn/r", "摘要"
        monitor.items_archive_append([_item("k1"), it, _item("k3")])
        out = Path(self._td.name) / "dump.json"
        self.assertEqual(monitor.dump_items(out), 0)
        data = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(data["schema"], 1)
        self.assertIsInstance(data["ts"], int)
        self.assertEqual([i["key"] for i in data["items"]], ["k1", "k2", "k3"])
        for i in data["items"]:
            self.assertTrue({"src", "ts", "text", "key"} <= set(i))
        self.assertEqual(data["items"][1]["url"], "https://ex.cn/r")

    def test_dump_empty_archive(self):
        """无存档 -> 导出空 items, schema 仍为 1。"""
        out = Path(self._td.name) / "dump.json"
        self.assertEqual(monitor.dump_items(out), 0)
        data = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(data["schema"], 1)
        self.assertEqual(data["items"], [])

    def test_main_dump_items_flag_with_dry_run(self):
        """CLI 面: --dump-items PATH 早退导出, 与 --dry-run 可并置 (不进抓取主路径)。"""
        monitor.items_archive_append([_item("k1")])
        out = Path(self._td.name) / "dump2.json"
        old = sys.argv
        try:
            sys.argv = ["monitor.py", "--dry-run", "--dump-items", str(out)]
            rc = monitor.main()
        finally:
            sys.argv = old
        self.assertEqual(rc, 0)
        data = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(data["schema"], 1)
        self.assertEqual([i["key"] for i in data["items"]], ["k1"])

    def test_main_dump_items_missing_path(self):
        """--dump-items 缺 PATH -> 参数错误 (退码 2), 不落盘。"""
        old = sys.argv
        try:
            sys.argv = ["monitor.py", "--dump-items"]
            rc = monitor.main()
        finally:
            sys.argv = old
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
