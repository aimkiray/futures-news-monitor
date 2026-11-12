#!/usr/bin/env python3
"""items_server.py 读面契约测试 — spec-fnm-http §3.1.1 / FS-01..FS-11。

stdlib-only, Windows/POSIX 通用 (items_server 仅依赖 http.server 等 stdlib)。
用 http.client 打真环回端口验证双式鉴权/裸形响应/TG 包络错误面,
另走进程内 main() 验证 token 缺位退码 3。直接运行: python tests/test_items_server.py
"""
from __future__ import annotations

import contextlib
import http.client
import importlib.util
import io
import json
import os
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent

_spec = importlib.util.spec_from_file_location("items_server", ROOT / "items_server.py")
items_server = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("items_server", items_server)
_spec.loader.exec_module(items_server)

TOKEN = "test-items-token-9f8e7d6c5b"
WRONG = "wrong-token-0000000000"


class _ServerCase(unittest.TestCase):
    """临时目录隔离 CONF/存档路径; 每个用例在环回随机端口起真 server."""

    cidrs = None                                        # None -> 仅环回白名单

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        d = Path(self._td.name)
        self.arch = d / "items.jsonl"
        self.meta = d / "items.meta.json"
        self.conf = d / "conf.json"
        self._saved = {k: getattr(items_server, k)
                       for k in ("CONF", "ITEMS_ARCHIVE", "ITEMS_META")}
        items_server.CONF = self.conf
        items_server.ITEMS_ARCHIVE = self.arch
        items_server.ITEMS_META = self.meta
        self.srv = items_server._ItemsServer(
            ("127.0.0.1", 0), TOKEN, items_server._parse_cidrs(self.cidrs))
        self.port = self.srv.server_address[1]
        self._th = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self._th.start()

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        self._th.join(timeout=5)
        for k, v in self._saved.items():
            setattr(items_server, k, v)
        self._td.cleanup()

    def _req(self, method: str, path: str, headers: dict | None = None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request(method, path, headers=headers or {})
            r = conn.getresponse()
            body = r.read()
            return r.status, body
        finally:
            conn.close()

    def _get(self, path: str, headers: dict | None = None):
        code, body = self._req("GET", path, headers)
        return code, json.loads(body)

    def _write_archive(self, recs: list[dict]):
        self.arch.write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in recs),
            encoding="utf-8")

    # ---- FS-01/02: URL-path 形鉴权 ----
    def test_fs01_urlpath_ok(self):
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(1, obj["schema"])
        self.assertIs(type(obj["schema"]), int)          # 严格 int (禁弱 schema 闸同口径)
        self.assertIsInstance(obj["items"], list)
        self.assertEqual({"schema", "items"}, set(obj))  # 裸形, 非 TG ok 包络

    def test_fs02_urlpath_wrong_token(self):
        code, obj = self._get(f"/bot{WRONG}/items")
        self.assertEqual(401, code)
        self.assertEqual({"ok": False, "error_code": 401, "description": "unauthorized"}, obj)

    def test_fs02b_urlpath_empty_token_is_bearer_path(self):
        # /bot/items 不是 url-path 形 (token 段不可为空) -> 落到 bearer 形, 无头即 401
        code, obj = self._get("/bot/items")
        self.assertEqual(401, code)
        self.assertEqual(401, obj["error_code"])

    # ---- FS-03/04: Bearer 形鉴权 ----
    def test_fs03_bearer_ok(self):
        code, obj = self._get("/bot/items",
                              {"Authorization": f"Bearer {TOKEN}"})
        self.assertEqual(200, code)
        self.assertEqual(1, obj["schema"])

    def test_fs04_bearer_wrong(self):
        code, obj = self._get("/bot/items",
                              {"Authorization": f"Bearer {WRONG}"})
        self.assertEqual(401, code)
        self.assertEqual({"ok": False, "error_code": 401, "description": "unauthorized"}, obj)

    def test_fs04b_bearer_missing(self):
        code, obj = self._get("/bot/items")
        self.assertEqual(401, code)

    # ---- FS-05/06: 未知路径 / 非 GET ----
    def test_fs05_unknown_path(self):
        for path in ("/", "/items", f"/bot{TOKEN}/", "/other"):
            code, obj = self._get(path)
            self.assertEqual(404, code, path)
            self.assertEqual({"ok": False, "error_code": 404, "description": "not found"}, obj)

    def test_fs06_post_method(self):
        code, body = self._req("POST", f"/bot{TOKEN}/items")
        self.assertEqual(405, code)
        self.assertEqual({"ok": False, "error_code": 405, "description": "method not allowed"},
                         json.loads(body))

    def test_fs06b_other_methods(self):
        for method in ("PUT", "DELETE", "HEAD"):
            code, _ = self._req(method, f"/bot{TOKEN}/items")
            self.assertEqual(405, code, method)

    def test_fs06c_head_405_no_body_keepalive(self):
        # HEAD->405 禁带体 (HTTP/1.1): 漏写体会错位同 keep-alive 连接的后续响应
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request("HEAD", f"/bot{TOKEN}/items")
            r1 = conn.getresponse()
            cl = r1.getheader("Content-Length")
            self.assertEqual(405, r1.status)
            self.assertIsNotNone(cl)
            self.assertGreater(int(cl), 0)             # Content-Length 头仍如实保留
            self.assertEqual(b"", r1.read())           # 但响应体必须为 absent
            conn.request("GET", f"/bot{TOKEN}/items")  # 同连接续请: 流未被错位的体毒化
            r2 = conn.getresponse()
            self.assertEqual(200, r2.status)
            self.assertEqual(1, json.loads(r2.read())["schema"])
        finally:
            conn.close()

    # ---- FS-07/08: 存档读面 ----
    def test_fs07_archive_absent(self):
        self.assertFalse(self.arch.exists())
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual({"schema": 1, "items": []}, obj)

    def test_fs08_archive_items(self):
        recs = [
            {"src": "财联社", "ts": 1793800000, "text": "沪铜库存变化9吨环比9%",
             "key": "k1", "schema": 1},
            {"src": "SMM", "ts": 1793800100, "text": "铝锭社会库存下降",
             "key": "k2", "url": "https://example.com/a", "snippet": "摘录", "schema": 1},
        ]
        self._write_archive(recs)
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(1, obj["schema"])
        self.assertEqual(2, len(obj["items"]))
        for it in obj["items"]:
            for field in ("src", "ts", "text", "key"):
                self.assertIn(field, it)
        self.assertEqual("k1", obj["items"][0]["key"])

    def test_fs08b_bad_lines_skipped(self):
        self.arch.write_text(
            '{"src":"s","ts":1,"text":"ok","key":"k1"}\n'
            'not-json\n'
            '[1,2,3]\n'
            '{"src":"s","ts":2,"text":"ok2","key":"k2"}\n', encoding="utf-8")
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(2, len(obj["items"]))           # 坏行/非对象行跳过

    def test_fs08c_meta_schema_mismatch_fails_closed(self):
        self._write_archive([{"src": "s", "ts": 1, "text": "x", "key": "k"}])
        self.meta.write_text('{"schema": 2}', encoding="utf-8")
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual([], obj["items"])               # meta 明示高版本 -> 空档

    def test_fs08d_meta_absent_archive_served(self):
        self._write_archive([{"src": "s", "ts": 1, "text": "x", "key": "k"}])
        self.assertFalse(self.meta.exists())
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(1, len(obj["items"]))           # meta 缺席不阻断 (行内 schema 自足)

    # ---- FS-09: 请求日志 token 掩码 ----
    def test_fs09_log_masks_token(self):
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            self._get(f"/bot{TOKEN}/items")
            self._get(f"/bot{WRONG}/items")
            self._req("POST", f"/bot{TOKEN}/items")
        logs = buf.getvalue()
        self.assertNotIn(TOKEN, logs)                    # token 明文永不入日志
        self.assertNotIn(WRONG, logs)                    # 错 token 也掩 (同段同形)
        self.assertIn("/bot<token>/items", logs)         # 字面占位符在场

    # ---- FS-10: CIDR 白名单 ----
    def test_fs10_cidr_unit_and_handler(self):
        nets = items_server._parse_cidrs(["192.168.31.0/24"])
        self.assertTrue(items_server._cidr_allows("127.0.0.1", nets))      # 环回恒通
        self.assertTrue(items_server._cidr_allows("192.168.31.95", nets))
        self.assertFalse(items_server._cidr_allows("10.0.0.5", nets))      # 非白名单拒
        # handler 级: 白名单判否 -> 401 TG 包络 (环回测试无法伪造源 IP, 桩判否即可)
        with mock.patch.object(items_server, "_cidr_allows", lambda *a, **k: False):
            code, obj = self._get(f"/bot{TOKEN}/items")
            self.assertEqual(401, code)
            self.assertEqual("unauthorized", obj["description"])

    # ---- 补充: query 串不影响路由 ----
    def test_extra_query_string_tolerated(self):
        code, obj = self._get(f"/bot{TOKEN}/items?x=1")
        self.assertEqual(200, code)
        self.assertIn("items", obj)

    def test_extra_malformed_uri_404(self):
        # 畸形 absolute-URI (urlsplit 抛 ValueError): 回 404 包络而非连接裸崩
        code, obj = self._get("//[bad")
        self.assertEqual(404, code)
        self.assertEqual({"ok": False, "error_code": 404, "description": "not found"}, obj)

    # ---- 四轨深审必修钉 (读面侧 F1/F2/F3/F7/F9) ----
    def test_f1_handler_socket_timeout_set(self):
        """F1 slowloris: setup() 后 request socket settimeout(REQ_TIMEOUT_S)——
        白名单内 connect-not-send 客户端不再无限占线程。"""
        self.assertEqual(30, items_server.REQ_TIMEOUT_S)        # 钉住规格常量
        seen = {}

        class _Probe(items_server._Handler):
            def setup(self):
                super().setup()
                seen["timeout"] = self.request.gettimeout()

        with mock.patch.object(items_server, "_Handler", _Probe):
            srv = items_server._ItemsServer(
                ("127.0.0.1", 0), TOKEN, items_server._parse_cidrs(None))
        try:
            th = threading.Thread(target=srv.serve_forever, daemon=True)
            th.start()
            conn = http.client.HTTPConnection(
                "127.0.0.1", srv.server_address[1], timeout=5)
            try:
                conn.request("GET", f"/bot{TOKEN}/items")
                self.assertEqual(200, conn.getresponse().status)
            finally:
                conn.close()
        finally:
            srv.shutdown()
            srv.server_close()
            th.join(timeout=5)
        self.assertEqual(items_server.REQ_TIMEOUT_S, seen.get("timeout"))

    def test_f2_poison_hugeint_line_no_tail_drop(self):
        """F2 毒行截尾: >4300 位整数字面量行 json.loads 抛裸 ValueError——旧码
        穿透内层进外层 -> 该行起整尾丢; 现按坏行跳过, 其后行照常读到。"""
        good1 = {"src": "s", "ts": 1, "text": "a", "key": "k1", "schema": 1}
        good2 = {"src": "s", "ts": 2, "text": "b", "key": "k2", "schema": 1}
        huge = '{"key": ' + "9" * 5000 + ', "ts": 1, "text": "x"}'
        self.arch.write_text(
            json.dumps(good1) + "\n" + huge + "\n" + json.dumps(good2) + "\n",
            encoding="utf-8")
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(["k1", "k2"], [i["key"] for i in obj["items"]])

    def test_f3_absent_archive_logged(self):
        """F3: 存档缺席按空档响应且记一行 _log——远端可分辨真空窗 vs 读径坏。"""
        self.assertFalse(self.arch.exists())
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual([], obj["items"])
        self.assertIn("items 存档缺席", buf.getvalue())

    def test_f7_read_cap_truncates(self):
        """F7 读界: 超 ITEMS_READ_MAX 行截断 + _err 记档 (腐化巨型档不内存放大)。"""
        self.assertEqual(20000, items_server.ITEMS_READ_MAX)    # 钉住规格常量
        self._write_archive([
            {"src": "s", "ts": i, "text": "x", "key": f"k{i}", "schema": 1}
            for i in range(5)])
        obuf, sbuf = io.StringIO(), io.StringIO()
        with mock.patch.object(items_server, "ITEMS_READ_MAX", 3), \
                contextlib.redirect_stdout(obuf), contextlib.redirect_stderr(sbuf):
            code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(3, len(obj["items"]))
        self.assertIn("超行数帽", obuf.getvalue() + sbuf.getvalue())

    def test_f9_log_timestamp_tz_cn_format(self):
        """F9: 请求日志时间戳改 TZ_CN 形 [YYYY-MM-DD HH:MM:SS] (与 _log/_err
        同口径), 不再 GMT 的 [12/Nov/2026 ...] Apache 形。"""
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            self._get(f"/bot{TOKEN}/items")
        logs = buf.getvalue()
        self.assertRegex(logs, r"\[20\d\d-\d\d-\d\d \d\d:\d\d:\d\d\]")
        self.assertNotRegex(logs, r"\[\d{2}/[A-Z][a-z]{2}/20\d\d")

    # ---- fnm-obs R3 钉: 存档在但读径坏 -> 503 显形 (不再吞成 200 与真空窗同形) ----
    def test_r3_1_midread_oserror_503(self):
        """R3-1: 文件在且已读 1 条后迭代中道炸 OSError -> 503 + TG 包络
        {"ok":false,"error_code":503,"description":"archive read error"};
        旧径吞异常回 200+已读前缀, CTB 侧与真空窗同形 (本钉即断此同形)。"""

        class _BoomFile:                                 # 中道败桩: 出一条真行再炸 OSError
            def __init__(self):
                self._it = iter(['{"src":"s","ts":1,"text":"x","key":"k1","schema":1}\n'])

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def __iter__(self):
                return self

            def __next__(self):
                try:
                    return next(self._it)
                except StopIteration:
                    raise OSError("simulated mid-read failure")

        self._write_archive([{"src": "s", "ts": 1, "text": "x", "key": "k1"}])
        self.assertTrue(self.arch.is_file())             # 文件在 (缺席径见 R3-2)
        with mock.patch("builtins.open", return_value=_BoomFile()):
            code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(503, code)
        self.assertEqual({"ok": False, "error_code": 503,
                          "description": "archive read error"}, obj)   # 包络精确形, 无 items 键

    def test_r3_1b_midread_decode_error_503(self):
        """R3-1 补: 存档混非法 UTF-8 字节, 迭代 decode 中道炸 UnicodeDecodeError
        (ValueError 子类) -> 同 503 包络 (真实坏字节, 非 mock)。"""
        self.arch.write_bytes(
            b'{"src":"s","ts":1,"text":"x","key":"k1"}\n\xff\xfe bad bytes\n')
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(503, code)
        self.assertEqual({"ok": False, "error_code": 503,
                          "description": "archive read error"}, obj)

    def test_r3_2_filenotfound_200_empty(self):
        """R3-2: FileNotFoundError (存档缺席) -> 200 items:[] 零差分保留 (合法空)."""
        self.assertFalse(self.arch.exists())
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual({"schema": 1, "items": []}, obj)

    def test_r3_3_meta_gate_200_empty(self):
        """R3-3: 文件在但 meta 闸 schema 不符 -> 200 items:[] 零差分保留
        (fail-closed 空档是合法语义, 不升 503)。"""
        self._write_archive([{"src": "s", "ts": 1, "text": "x", "key": "k"}])
        self.meta.write_text('{"schema": 2}', encoding="utf-8")
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual({"schema": 1, "items": []}, obj)

    # ---- fnm-obs 深审修单钉 A4/T3: meta 病态深嵌套按缺席放行 ----
    def test_a4_meta_deep_nesting_200(self):
        """A4/T3: .meta.json 病态深嵌套 (规格形 "["*2000+"]"*2000) -> json.loads
        抛 RecursionError, _load_meta 收容归 None (meta 缺席口径, meta 非真源)
        -> 200 正常读档; 旧径 RecursionError 穿透 do_GET 杀 handler 线程
        -> 掉连接+stderr traceback。
        深度取 100000: CPython 3.13 C scanner 递归预算 >2000 层 (实测 2000 解析
        成功/5000 才抛), 100000 跨版本稳触发 RecursionError。"""
        self._write_archive([{"src": "s", "ts": 1, "text": "x", "key": "k"}])
        self.meta.write_text("[" * 100000 + "]" * 100000, encoding="utf-8")
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            code, obj = self._get(f"/bot{TOKEN}/items")     # 旧径此处 RemoteDisconnected
        self.assertEqual(200, code)
        self.assertEqual(1, len(obj["items"]))              # 存档照常读到
        self.assertNotIn("Traceback", buf.getvalue())       # handler 线程不裸崩

    # ---- fnm-obs 深审修单钉 A3/T4: 读径文件类型闸 + 累计字节界 ----
    def test_a3_dir_archive_503(self):
        """A3/T4: 存档路径是目录 -> exists 真 isfile 假 -> _ArchiveReadError -> 503
        TG 包络 (目录/FIFO/设备/symlink 落非普通文件统收)。"""
        self.arch.mkdir()
        self.assertTrue(self.arch.exists())
        self.assertFalse(self.arch.is_file())
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(503, code)
        self.assertEqual({"ok": False, "error_code": 503,
                          "description": "archive read error"}, obj)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "POSIX only: Windows 无 mkfifo")
    def test_a3_fifo_archive_503(self):
        """A3/T4: 存档路径造 FIFO -> isfile 假 -> 503; 旧径 open(FIFO) 永久阻塞
        挂死 handler 线程 (连接等死+线程泄漏), 类型闸即为此截断。"""
        os.mkfifo(self.arch)
        self.assertTrue(self.arch.exists())
        self.assertFalse(self.arch.is_file())
        code, obj = self._get(f"/bot{TOKEN}/items")         # 旧径 client 5s 超时红
        self.assertEqual(503, code)
        self.assertEqual("archive read error", obj["description"])

    def test_a3_symlink_to_dir_503(self):
        """A3/T4: symlink->目录 -> exists 随链接为真 / isfile 为假 -> 503
        (exists/isfile 均 follow symlink; dangling symlink -> exists 假 -> FNF 200
        原径, 由 fs07/r3_2 同形覆盖)。Windows 无 symlink 特权时 skip——
        闸逻辑由 test_a3_dir_archive_503 同形覆盖。"""
        target = Path(self._td.name) / "realdir"
        target.mkdir()
        try:
            self.arch.symlink_to(target, target_is_directory=True)
        except (OSError, NotImplementedError) as e:
            self.skipTest(f"symlink 特权不可用: {e}")
        self.assertTrue(self.arch.exists())                 # follow -> 目录在
        self.assertFalse(self.arch.is_file())               # follow -> 非普通文件
        code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(503, code)
        self.assertEqual("archive read error", obj["description"])

    def test_a3_byte_cap_truncates_200(self):
        """A3/T4: 累计字节界 ITEMS_READ_MAX_BYTES=32MiB——首行好条目 + 33MB
        单行无换行 -> 超界 _err 记档 + break 截断 -> 200 前缀保留 (与行数帽
        同口径受控限量; 旧径无字节界, 33MB 行整吃内存照常解析进 items)。"""
        self.assertEqual(32 * 1024 * 1024, items_server.ITEMS_READ_MAX_BYTES)
        self.arch.write_bytes(
            b'{"src":"s","ts":1,"text":"ok","key":"k0"}\n'
            + b'{"key":"kbig","text":"' + b"x" * (33 * 1024 * 1024) + b'"}\n')
        obuf, sbuf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(obuf), contextlib.redirect_stderr(sbuf):
            code, obj = self._get(f"/bot{TOKEN}/items")
        self.assertEqual(200, code)
        self.assertEqual(["k0"], [i["key"] for i in obj["items"]])   # 截断留前缀
        self.assertIn("超字节界", obuf.getvalue() + sbuf.getvalue())


class _TokenGateCase(unittest.TestCase):
    """FS-11: token 两者皆无 -> 启动即退 3 + stderr fail-loud (不监听端口)."""

    def test_fs11_no_token_exit3(self):
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "conf.json"
            conf.write_text('{"items_port": 8766}', encoding="utf-8")
            saved = items_server.CONF
            items_server.CONF = conf
            buf = io.StringIO()
            try:
                with mock.patch.dict(os.environ):
                    os.environ.pop(items_server.TOKEN_ENV, None)
                    with contextlib.redirect_stderr(buf):
                        rc = items_server.main()
            finally:
                items_server.CONF = saved
            self.assertEqual(3, rc)
            self.assertIn("items_token", buf.getvalue())  # fail-loud 指明配置键
            self.assertNotIn(TOKEN, buf.getvalue())       # 不泄任何凭据

    def test_fs11b_env_token_wins_over_conf(self):
        conf = {"items_token": "conf-tok"}
        with mock.patch.dict(os.environ, {items_server.TOKEN_ENV: "env-tok"}):
            self.assertEqual("env-tok", items_server._resolve_token(conf))
        with mock.patch.dict(os.environ):
            os.environ.pop(items_server.TOKEN_ENV, None)
            self.assertEqual("conf-tok", items_server._resolve_token(conf))
            self.assertEqual("", items_server._resolve_token({}))


class _MainAndUnitCase(unittest.TestCase):
    """回归钉: 不起服务的 main() 门径与工具函数面 (bind/port/cidrs/属性前移)."""

    def _run_main(self, conf_obj: dict):
        """写临时 conf + 清 TOKEN_ENV 后跑 main(); 返回 (rc, 全部输出)."""
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "conf.json"
            conf.write_text(json.dumps(conf_obj), encoding="utf-8")
            saved = items_server.CONF
            items_server.CONF = conf
            sbuf, obuf = io.StringIO(), io.StringIO()
            try:
                with mock.patch.dict(os.environ):
                    os.environ.pop(items_server.TOKEN_ENV, None)
                    with contextlib.redirect_stderr(sbuf), \
                            contextlib.redirect_stdout(obuf):
                        rc = items_server.main()
            finally:
                items_server.CONF = saved
            return rc, sbuf.getvalue() + obuf.getvalue()

    def test_bind_occupied_exit2(self):
        # 端口占用 -> fail-loud 退 2 + 错误消息含地址, 而非裸 traceback (launchd 崩环源)
        hold = socket.socket()
        try:
            hold.bind(("127.0.0.1", 0))
            hold.listen(1)
            port = hold.getsockname()[1]
            rc, out = self._run_main({"items_token": TOKEN,
                                      "items_bind": "127.0.0.1",
                                      "items_port": port})
            self.assertEqual(2, rc)
            self.assertIn("绑定失败", out)
            self.assertIn(f"127.0.0.1:{port}", out)
            self.assertNotIn(TOKEN, out)               # 凭据永不入日志
        finally:
            hold.close()

    def test_bad_port_value_not_echoed(self):
        # 非法 items_port -> 退 2, 错误只报类型名+长度, 值本身不回显
        secret = "PORTSECRET-9f8e"                     # len 15
        rc, out = self._run_main({"items_token": TOKEN, "items_port": secret})
        self.assertEqual(2, rc)
        self.assertIn("items_port 非法", out)
        self.assertIn("str(len 15)", out)
        self.assertNotIn(secret, out)

    def test_cidrs_non_list_warns_not_silent(self):
        # items_cidrs 写成裸字符串 -> 警告径 + fail-closed 仅环回 (原先静默)
        sbuf, obuf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(sbuf), contextlib.redirect_stdout(obuf):
            nets = items_server._parse_cidrs("CIDRSECRET")
        out = sbuf.getvalue() + obuf.getvalue()
        self.assertIn("items_cidrs 须为列表", out)
        self.assertIn("str", out)                      # 只记类型名
        self.assertNotIn("CIDRSECRET", out)            # 值不回显
        self.assertEqual(2, len(nets))                 # 仅环回 = fail-closed

    def test_cidrs_bad_entry_no_echo(self):
        # 坏条目逐个忽略, 错误消息不带条目值 (类型+长度口径)
        sbuf, obuf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(sbuf), contextlib.redirect_stdout(obuf):
            nets = items_server._parse_cidrs(["ENTRYSECRET", "10.0.0.0/8"])
        out = sbuf.getvalue() + obuf.getvalue()
        self.assertNotIn("ENTRYSECRET", out)           # len 11 不回显值
        self.assertIn("str(len 11)", out)
        self.assertEqual(3, len(nets))                 # 环回×2 + 有效条目
        self.assertTrue(items_server._cidr_allows("10.0.0.5", nets))

    def test_attrs_set_before_super_init(self):
        # items_token/cidrs 须在 super().__init__ (其内 bind+listen) 之前挂上,
        # 否则 bind/listen 与属性赋值窗内进来的请求吃 AttributeError 掉连接
        seen = {}

        class _Probe(items_server._ItemsServer):
            def server_bind(self):                     # 父类构造内回调; 此刻属性须在
                seen["attrs"] = (hasattr(self, "items_token"),
                                 hasattr(self, "cidrs"))
                super().server_bind()

        srv = _Probe(("127.0.0.1", 0), "tok", [])
        try:
            self.assertEqual((True, True), seen["attrs"])
            self.assertEqual("tok", srv.items_token)
            self.assertEqual([], srv.cidrs)
        finally:
            srv.server_close()

    # ---- 四轨深审必修钉 (单元素 F6/F8) ----
    def test_f6_bad_conf_logged_absent_silent(self):
        """F6: conf 文件在但坏 JSON/非 dict -> _err 记档 + {}; 缺席仍静默 (正常拓扑)."""
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "conf.json"
            saved = items_server.CONF
            items_server.CONF = conf
            try:
                sbuf, obuf = io.StringIO(), io.StringIO()     # 缺席: 静默 {}
                with contextlib.redirect_stderr(sbuf), \
                        contextlib.redirect_stdout(obuf):
                    self.assertEqual({}, items_server._load_conf())
                self.assertEqual("", sbuf.getvalue() + obuf.getvalue())
                for bad in ("{not json", "[1,2]"):            # 坏 JSON / 非 dict: 记档 + {}
                    conf.write_text(bad, encoding="utf-8")
                    sbuf, obuf = io.StringIO(), io.StringIO()
                    with contextlib.redirect_stderr(sbuf), \
                            contextlib.redirect_stdout(obuf):
                        self.assertEqual({}, items_server._load_conf())
                    self.assertIn("conf 不可解析", sbuf.getvalue() + obuf.getvalue(), bad)
            finally:
                items_server.CONF = saved

    def test_f8_client_abort_contained_no_traceback(self):
        """F8 断连收容: wfile.write BrokenPipe/ConnectionReset -> 单行 _log,
        不再每段刷 traceback (客户端 abort 是常态, 不是故障)."""

        class _DeadWfile:
            def write(self, _b):
                raise BrokenPipeError(32, "simulated client abort")

        h = items_server._Handler.__new__(items_server._Handler)
        h.request_version = "HTTP/1.1"
        h.requestline = "GET /bot<token>/items HTTP/1.1"
        h.command = "GET"
        h.client_address = ("127.0.0.1", 0)
        h.wfile = _DeadWfile()
        sbuf, obuf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(sbuf), contextlib.redirect_stdout(obuf):
            h._send_json(200, {"schema": 1, "items": []})      # 不得抛
        out = sbuf.getvalue() + obuf.getvalue()
        self.assertIn("断连", out)
        self.assertNotIn("Traceback", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
