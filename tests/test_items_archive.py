#!/usr/bin/env python3
"""items 存档契约测试 — spec-fnm-source-tg-notify §3.11 / T-31 / T-32 + --dump-items。

stdlib-only, Windows/POSIX 通用。monitor.py 顶层 `import fcntl` 历史上是 POSIX 硬依赖
(部署目标 macOS launchd), 测试导入前向 sys.modules 注入最小 stub, 不改生产面;
深审 R7 后 fcntl 缺席降级为 None, stub 保留无害 (POSIX 面行为不变)。
直接运行: python tests/test_items_archive.py
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

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


class TestItemsArchiveRobust(_ArchiveCase):
    """容错/耐久/自愈口径钉: 评审 findings 回归。"""

    def test_read_invalid_utf8_no_crash(self):
        """非法 UTF-8 存档: 读侧 prefix-return 不崩栈 (首行即坏 -> 空);
        append 径自愈截到上一完整行 (行首即坏 -> 截空) 后续写, 不再 ValueError 冻结。"""
        self.arch.write_bytes(b"\xff\xfe\x00garbage\n" + json.dumps(_item("k1")).encode() + b"\n")
        self.assertEqual(monitor.read_items_archive(), [])       # 首行即解码失败 -> 空前缀, 不崩栈
        # W3-P2: 旧径 ValueError abort 冻结追加; 新径截除非法字节行及其后整尾再落档
        self.assertEqual(monitor.items_archive_append([_item("k9")]), 1)
        self.assertEqual(self._keys(), ["k9"])                   # 新行独立可解析, 无合并残行

    def test_meta_self_heal_all_dup_round(self):
        """meta 缺/形不符 + 全 dup 轮也要自愈 (原 `if not rows` 闸 -> adapter schema 闸永久 INPUT(3))。"""
        monitor.items_archive_append([_item("k1")])
        self.meta.unlink()                                       # meta 缺失
        self.assertEqual(monitor.items_archive_append([_item("k1")]), 0)   # 全 dup -> 0 行
        self.assertEqual(json.loads(self.meta.read_text(encoding="utf-8"))["schema"], 1)
        self.meta.write_text('{"schema": 99}', encoding="utf-8")           # schema 不符同样自愈
        self.assertEqual(monitor.items_archive_append([_item("k1")]), 0)
        self.assertEqual(json.loads(self.meta.read_text(encoding="utf-8"))["schema"], 1)

    def test_partial_write_retry_continues_remaining(self):
        """os.write 部分成功后 OSError -> 重试只续写剩余 view, 不整批重写 (行不重复落档)。"""
        real_write = os.write
        state = {"calls": 0}

        def flaky(fd, data):
            state["calls"] += 1
            if state["calls"] == 1:
                return real_write(fd, data[:5])                  # 只写前 5 字节
            if state["calls"] == 2:
                raise OSError("模拟瞬时写失败")
            return real_write(fd, data)

        with mock.patch.object(monitor.os, "write", flaky):
            n = monitor.items_archive_append([_item("k1"), _item("k2")])
        self.assertEqual(n, 2)
        self.assertEqual(self._keys(), ["k1", "k2"])             # 恰好两行, 无重复

    def test_append_fsyncs_archive_fd(self):
        """追加路径 close 前 fsync(fd): 与 save_json/atomic_write_text 耐久口径一致。"""
        real_open, real_fsync = os.open, os.fsync
        arch_fds, fsynced = [], []

        def open_spy(path, flags, mode=0o777, **kw):
            fd = real_open(path, flags, mode, **kw)
            if Path(path).name == self.arch.name:
                arch_fds.append(fd)
            return fd

        def fsync_spy(fd):
            fsynced.append(fd)
            return real_fsync(fd)

        with mock.patch.object(monitor.os, "open", open_spy), \
                mock.patch.object(monitor.os, "fsync", fsync_spy):
            self.assertEqual(monitor.items_archive_append([_item("k1")]), 1)
        self.assertTrue(arch_fds)
        self.assertTrue(all(fd in fsynced for fd in arch_fds))

    def test_append_new_file_dir_fsync(self):
        """P3-7: 首写新档 (O_CREAT) 时父目录 fd 也被 fsync——新档目录项
        不随文件 fsync 落盘。spy 手法同 test_append_fsyncs_archive_fd;
        Windows 不能 os.open 目录 -> dir open 记路径并替真文件 fd。"""
        real_open, real_fsync = os.open, os.fsync
        dir_opened, dir_fds, fsynced = [], [], []
        substitute = Path(self._td.name) / "dirfd-standin.bin"
        substitute.write_bytes(b"x")

        def open_spy(path, flags, mode=0o777, **kw):
            if Path(path).is_dir():                       # 目录 open 替身
                dir_opened.append(Path(path))
                fd = real_open(str(substitute), os.O_RDONLY)
                dir_fds.append(fd)
                return fd
            return real_open(path, flags, mode, **kw)

        def fsync_spy(fd):
            fsynced.append(fd)
            return real_fsync(fd)

        with mock.patch.object(monitor.os, "open", open_spy), \
                mock.patch.object(monitor.os, "fsync", fsync_spy):
            self.assertEqual(monitor.items_archive_append([_item("k1")]), 1)
        self.assertIn(self.arch.parent, dir_opened)       # 父目录被 open
        self.assertTrue(dir_fds)
        self.assertTrue(all(fd in fsynced for fd in dir_fds))  # 且被 fsync
        self.assertEqual(self._keys(), ["k1"])

    def test_write_zero_guard_no_hang(self):
        """os.write 返 0 防护: 不死循环, 重试耗尽返回 0 且不落半行。"""
        with mock.patch.object(monitor.os, "write", lambda fd, d: 0):
            self.assertEqual(monitor.items_archive_append([_item("k1")]), 0)
        self.assertFalse(self.arch.exists() and b"k1" in self.arch.read_bytes())

    def test_atomic_write_tmp_name_has_pid(self):
        """tmp 名带 pid: 无锁并发 dump/轮转同 PATH 不互撕同一 tmp。"""
        seen = []
        real_open = open

        def open_spy(file, *a, **k):
            seen.append(Path(file).name)
            return real_open(file, *a, **k)

        out = Path(self._td.name) / "snap.json"
        with mock.patch("builtins.open", open_spy):
            self.assertTrue(monitor.atomic_write_text(out, "{}\n"))
        self.assertIn(f"snap.json.tmp.{os.getpid()}", seen)
        self.assertEqual(out.read_text(encoding="utf-8"), "{}\n")

    def test_atomic_write_tmp_orphan_swept(self):
        """W3-P3: replace 失败径 name.tmp.<pid> 孤儿被清扫 (best-effort 不残留)。"""
        out = Path(self._td.name) / "snap.json"
        with mock.patch.object(monitor.os, "replace",
                               side_effect=OSError("模拟 replace 失败")):
            self.assertFalse(monitor.atomic_write_text(out, "{}\n"))
        self.assertFalse(out.exists())
        self.assertEqual(list(out.parent.glob("snap.json.tmp.*")), [])

    def test_meta_self_heal_empty_call(self):
        """W3-P2: meta 自愈在 `if not new_items` 闸之前——空调用也兑现
        「每次调用即执行」文档承诺 (调用方 if added 闸下本无此径, 契约仍钉死)。"""
        self.assertFalse(self.meta.exists())
        self.assertEqual(monitor.items_archive_append([]), 0)
        self.assertEqual(json.loads(self.meta.read_text(encoding="utf-8"))["schema"], 1)

    def test_append_heals_torn_tail(self):
        """W3-P2: 存档尾为无 \\n 撕裂残行 (kill/写中断) -> 追加前截除残尾,
        新行独立可解析、报数不造假、残行不随轮转留存。"""
        monitor.items_archive_append([_item("k1")])
        with open(self.arch, "ab") as fh:
            fh.write(b'{"src":"s","ts":17938')        # 半行 JSON 残尾
        n = monitor.items_archive_append([_item("k2"), _item("k3")])
        self.assertEqual(n, 2)                          # 真实落档数 (非 2+合并1 造假)
        recs = [json.loads(ln) for ln in self._lines()]  # 每行独立可解析
        self.assertEqual([r["key"] for r in recs], ["k1", "k2", "k3"])
        self.assertTrue(self.arch.read_bytes().endswith(b"\n"))
        # 再撞旧 key 仍去重: 残尾截除后键集无损
        self.assertEqual(monitor.items_archive_append([_item("k2")]), 0)

    def test_append_heals_utf8_fragment_tail(self):
        """W3-P2: UTF-8 中切残片 (多字节字符被截半) -> 读径自愈截除再落档,
        不再 ValueError abort 冻结。"""
        monitor.items_archive_append([_item("k1")])
        frag = '{"src":"s","ts":1,"text":"铜价涨'.encode("utf-8")[:-2]  # 末字符中切
        with open(self.arch, "ab") as fh:
            fh.write(frag)
        self.assertEqual(monitor.items_archive_append([_item("k2")]), 1)
        recs = [json.loads(ln) for ln in self._lines()]
        self.assertEqual([r["key"] for r in recs], ["k1", "k2"])

    def test_append_separates_complete_tail_record(self):
        """W3-P2: 残尾恰为完整 JSON 记录缺 \\n (部分写恰停记录边界) -> 补 \\n
        隔离保留该记录 (不截除丢数据), 其 key 进键集照常去重。"""
        monitor.items_archive_append([_item("k1")])
        with open(self.arch, "ab") as fh:
            fh.write(json.dumps(_item("k2"), ensure_ascii=False).encode("utf-8"))
        n = monitor.items_archive_append([_item("k3"), _item("k2")])
        self.assertEqual(n, 1)                          # 残尾 k2 已归档 -> 撞 key 只落 k3
        recs = [json.loads(ln) for ln in self._lines()]
        self.assertEqual([r["key"] for r in recs], ["k1", "k2", "k3"])
        self.assertTrue(self.arch.read_bytes().endswith(b"\n"))


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

    def test_dump_to_archive_self_refused(self):
        """--dump-items 指向存档自身: 拒绝 (JSON 快照会覆盖毁形 JSONL), 存档原样保留。"""
        monitor.items_archive_append([_item("k1")])
        before = self.arch.read_bytes()
        self.assertEqual(monitor.dump_items(self.arch), 1)
        self.assertEqual(self.arch.read_bytes(), before)
        # resolve 归一后同路径同样拦截 (.. 相对绕法)
        rel = self.arch.parent / "sub" / ".." / self.arch.name
        self.assertEqual(monitor.dump_items(rel), 1)
        self.assertEqual(self.arch.read_bytes(), before)

    def test_dump_to_other_state_files_refused(self):
        """P3-8/W3-P3: --dump-items 命中 CONF/QUEUE/STATE/PENDING/LOCK/META/
        LOG/ERRLOG 等其它 state/conf/log 件 resolve 路径同拒 (与 ITEMS_ARCHIVE
        同守卫清单); 全部守卫路径先指到 tempdir——绝不碰真实 ~/.pi 件。"""
        d = Path(self._td.name)
        guarded = {
            "CONF": d / "etc" / "futures-news.json",
            "STATE": d / "seen.json",
            "QUEUE": d / "queue.json",
            "PENDING": d / "pending.json",
            "LOCK": d / "monitor.lock",
            "ITEMS_META": self.meta,
            "LOG": d / "monitor.out",
            "ERRLOG": d / "monitor.err",
        }
        saved = {k: getattr(monitor, k) for k in guarded}
        try:
            for k, p in guarded.items():
                setattr(monitor, k, p)
            monitor.items_archive_append([_item("k1")])
            for name, p in guarded.items():
                p.parent.mkdir(parents=True, exist_ok=True)
                original = ("ORIGINAL-%s" % name).encode()
                p.write_bytes(original)
                self.assertEqual(monitor.dump_items(p), 1, name)
                self.assertEqual(p.read_bytes(), original, name)
                # .. 相对绕法同拦 (resolve 归一判据)
                rel = p.parent / "sub" / ".." / p.name
                self.assertEqual(monitor.dump_items(rel), 1, name)
            # 非守卫件仍可正常导出
            ok_out = d / "snap.json"
            self.assertEqual(monitor.dump_items(ok_out), 0)
            self.assertEqual(json.loads(ok_out.read_text(
                encoding="utf-8"))["schema"], 1)
        finally:
            for k, v in saved.items():
                setattr(monitor, k, v)

    def test_dump_case_variant_samefile_refused(self):
        """W3-P2: APFS/NTFS 大小写不敏感下 FUTURES-NEWS-SEEN.JSON 变体绕
        resolve 比对 -> samefile inode 判据同拒 (另钉硬链接同 inode 绕法)。"""
        monitor.items_archive_append([_item("k1")])
        before = self.arch.read_bytes()
        variant = self.arch.with_name(self.arch.name.upper())
        try:
            same = os.path.samefile(variant, self.arch)
        except OSError:
            self.skipTest("大小写敏感文件系统: 变体即异件, 无绕守面")
        self.assertTrue(same)
        self.assertEqual(monitor.dump_items(variant), 1)   # 变体名命中同 inode -> 拒
        self.assertEqual(self.arch.read_bytes(), before)
        link = self.arch.with_name("hardlink.jsonl")
        try:
            os.link(self.arch, link)
        except OSError:
            return                                         # FS 不支持硬链接 -> 只钉变体径
        self.assertEqual(monitor.dump_items(link), 1)      # 硬链接同 inode -> 同拒
        self.assertEqual(self.arch.read_bytes(), before)

    def test_main_dump_items_equals_form(self):
        """CLI 面: --dump-items=PATH 等号形兼容。"""
        monitor.items_archive_append([_item("k1")])
        out = Path(self._td.name) / "dump_eq.json"
        old = sys.argv
        try:
            sys.argv = ["monitor.py", f"--dump-items={out}"]
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


class TestDeepReviewFixes(_ArchiveCase):
    """深审必修批回归钉 (R1/R2 存档异常面 / R3 merge 守卫 / R4 cp936 / R7 fcntl 降级)。"""

    def test_r1_read_recursion_and_bad_lines_skipped_counted(self):
        """R1: 病态嵌套行 (RecursionError)/非对象行/语法坏行逐条跳过并计数入日志,
        完整前缀照常返回——旧码 RecursionError 穿透崩溃。"""
        good = json.dumps(_item("k1"), ensure_ascii=False)
        deep = "[" * 3000 + "]" * 3000                      # json.loads -> RecursionError
        nonobj = json.dumps([1, 2, 3])                      # 合法 JSON 但顶层非 dict
        garbage = "{not json"                               # JSONDecodeError
        self.arch.write_text("\n".join([good, deep, nonobj, garbage, good]) + "\n",
                             encoding="utf-8")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            items = monitor.read_items_archive()
        self.assertEqual([i["key"] for i in items], ["k1", "k1"])
        self.assertIn("跳过 3 行", buf.getvalue())          # 计数入日志: 不再静默丢行

    def test_r2_append_huge_int_corrupt_line_no_crash(self):
        """R2: 大整数腐化行 (json.loads -> 裸 ValueError int() 位限溢出) 不再穿透崩
        items_archive_append: 行照原样保留、不参与 key 集合, 新条目正常落档。"""
        huge = '{"key": ' + "9" * 5000 + ', "ts": 1, "text": "x"}'
        self.arch.write_text(huge + "\n", encoding="utf-8")
        n = monitor.items_archive_append([_item("k9")])
        self.assertEqual(n, 1)
        lines = self._lines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0], huge)                    # 腐化行原样留存 (轮转底账)
        self.assertEqual(json.loads(lines[1])["key"], "k9")

    def test_r3_merge_non_dict_analysis_element_no_crash(self):
        """R3: analyses 含非对象元素 -> validate_chunk 已记 error;
        覆盖/查重循环跳过它, 汇总报错退 1 而非 AttributeError 崩溃。"""
        spec = importlib.util.spec_from_file_location(
            "merge_research_reports", ROOT / "merge_research_reports.py")
        mm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mm)
        d = Path(self._td.name)
        raw = d / "research-raw.json"
        adir = d / "analysis"
        adir.mkdir()
        raw.write_text(json.dumps(
            {"items": [{"record_key": "k1", "tight": True}],
             "counts": {"unique_items": 1}}, ensure_ascii=False), encoding="utf-8")
        (adir / "chunk-001.json").write_text(json.dumps(
            {"schema_version": "1.0", "chunk": "001",
             "analyses": ["oops", {"infoCode": "k1"}]}, ensure_ascii=False),
            encoding="utf-8")
        saved = {k: getattr(mm, k) for k in ("RAW_PATH", "ANALYSIS_DIR", "OUT_PATH")}
        try:
            mm.RAW_PATH, mm.ANALYSIS_DIR = raw, adir
            mm.OUT_PATH = d / "out.json"
            errbuf = io.StringIO()
            with contextlib.redirect_stderr(errbuf):
                rc = mm.main()
        finally:
            for k, v in saved.items():
                setattr(mm, k, v)
        self.assertEqual(rc, 1)
        self.assertIn("not a JSON object", errbuf.getvalue())
        self.assertFalse((d / "out.json").exists())

    def test_r4_cp936_stdout_no_unicode_encode_error(self):
        """R4: cp936 控制台下打印含 emoji 的统计行不再 UnicodeEncodeError 假败
        (stdout reconfigure utf-8/errors=replace)。"""
        spec = importlib.util.spec_from_file_location(
            "extract_telegram", ROOT / "extract_telegram.py")
        et = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(et)
        d = Path(self._td.name)
        src = d / "result.json"
        src.write_text(json.dumps({"messages": [
            {"id": 1, "type": "message", "date": "2026-07-29T20:54:58",
             "from": "u", "text": "🚨沪铜库存变化1吨环比1%"},
        ]}, ensure_ascii=False), encoding="utf-8")
        fake_out = io.TextIOWrapper(io.BytesIO(), encoding="cp936")
        old_out, old_argv = sys.stdout, sys.argv
        try:
            sys.stdout = fake_out                             # cp936 终端: emoji 必炸
            sys.argv = ["extract_telegram.py", "--input", str(src),
                        "--out-dir", str(d), "--sample", "1"]
            rc = et.main()
        finally:
            sys.stdout = old_out
            sys.argv = old_argv
        self.assertEqual(rc, 0)
        self.assertTrue((d / "futures-news.json").exists())

    def test_r7_fcntl_absent_main_refuses_round(self):
        """R7: fcntl 缺席平台 -> import 不崩 (符号为 None), main 在锁点人话报错
        rc=7 拒跑——fail-loud, 不静默放行双实例。"""
        self.assertTrue(hasattr(monitor, "fcntl"))
        d = Path(self._td.name)
        saved = {"fcntl": monitor.fcntl}
        saved.update({k: getattr(monitor, k) for k in ("CONF", "LOCK", "LOG", "ERRLOG")})
        old_argv = sys.argv
        errbuf = io.StringIO()
        try:
            monitor.fcntl = None
            monitor.CONF = d / "conf.json"
            monitor.LOCK = d / "lock"
            monitor.LOG = d / "out.log"
            monitor.ERRLOG = d / "err.log"
            sys.argv = ["monitor.py"]
            with contextlib.redirect_stderr(errbuf):
                rc = monitor.main()
        finally:
            for k, v in saved.items():
                setattr(monitor, k, v)
            sys.argv = old_argv
        self.assertEqual(rc, 7)
        self.assertIn("fcntl", errbuf.getvalue())


if __name__ == "__main__":
    unittest.main()
