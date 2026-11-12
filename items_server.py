#!/usr/bin/env python3
"""futures-news-items: FNM items 存档只读 HTTP 读面 (spec-fnm-http §3.1.1)。

镜像 wxa GET /bot<token>/items 读面契约, 双式鉴权并存:
  - URL-path 形: GET /bot<token>/items          (token 在路径段)
  - Bearer 形:   GET /bot/items + Authorization: Bearer <token>

响应契约 (非 TG ok 包络——纯裸形):
  200  {"schema":1,"items":[...]}          存档缺席/空 -> items=[] (非 404)
错误一律 TG 包络形 (与 wxa 鉴权错误同款):
  401  {"ok":false,"error_code":401,"description":"unauthorized"}
  404  {"ok":false,"error_code":404,"description":"not found"}
  405  {"ok":false,"error_code":405,"description":"method not allowed"}
  503  {"ok":false,"error_code":503,"description":"archive read error"}
       (存档在但读径坏——显形上报, 不吞成 200 与真空窗同形; spec-fnm-obs R3)

凭据: token 只从环境变量 FNM_ITEMS_TOKEN 或 ~/.pi/agent/etc/futures-news.json 的
      items_token 字段读取 (env 优先); 两者皆无 -> stderr fail-loud + exit 3
      (与 monitor.py token 缺位退码口径对齐)。token 永不写入日志/响应体/异常消息。

配置 (同档 futures-news.json):
  items_bind   绑定地址 (缺省 127.0.0.1, fail-closed——不默认公网)
  items_port   端口      (缺省 8766; 非法值 -> exit 2 fail-loud)
  items_cidrs  网段白名单字符串列表 (缺席 -> 仅环回可达, fail-closed;
               环回始终放行, 名单只追加外部网段)

存档: 每请求现读 ~/.pi/agent/state/futures-news-items.jsonl (JSONL 逐行解析,
      与 monitor.read_items_archive 同口径; 不 import monitor——部署面文件名是
      futures-news-monitor.py, 非合法模块标识符, 自重读才是可部署形态)。
      meta 旁路闸: .meta.json 为 dict 且 schema 键明示 != 1 -> 空档 (fail-closed);
      meta 缺席/坏 JSON/无 schema 键 -> 不阻断 (行内 schema 自足, 写侧才是真源)。
      读径坏 (文件在但非普通文件/open/迭代 OSError 或 decode 错) -> 503, 不吃前缀装空档。

服务模型: ThreadingHTTPServer 单进程多线程, launchd KeepAlive 保活;
      请求日志中 /bot<...>/items 的 token 段一律掩码为 /bot<token>/items。

用法: futures-news-items.py [--selftest]
依赖: 仅标准库; 单文件, 与 monitor.py 同口径 (中文注释, LF)。
"""
from __future__ import annotations

import ipaddress
import json
import os
import re
import sys
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

HOME = Path.home()
CONF = HOME / ".pi/agent/etc/futures-news.json"                    # 与 monitor.py 同档
ITEMS_ARCHIVE = HOME / ".pi/agent/state/futures-news-items.jsonl"      # 读面数据形
ITEMS_META = HOME / ".pi/agent/state/futures-news-items.meta.json"     # schema 版本闸旁路
ITEMS_SCHEMA = 1                            # 与 monitor.ITEMS_SCHEMA 同值 (§3.9)
TOKEN_ENV = "FNM_ITEMS_TOKEN"
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8766
ITEMS_READ_MAX = 20000                  # 读界: 每请求最多解析的非空行数, 超帽截断 (腐化巨型档不内存放大)
ITEMS_READ_MAX_BYTES = 32 * 1024 * 1024  # 读界: 迭代累计字节界, 超界截断 (单行巨型行不内存放大, 与行数帽同口径)
REQ_TIMEOUT_S = 30                      # 连接读/写超时(秒): 白名单内 connect-not-send 不再无限占线程 (F1)

TZ_CN = ZoneInfo("Asia/Shanghai")           # 与 monitor.py 日志时区一致

# 路由: url-path 形恰好一段 token (空段不匹配 -> 落到 bearer 形判定);
# 先 match 再 unquote, %2F 无法偷渡额外路径段。
_BOT_ITEMS_RE = re.compile(r"^/bot([^/]+)/items$")        # 提取用 (锚定整段)
_BOT_ITEMS_PATH_RE = re.compile(r"/bot[^/\s\"']+/items")  # 掩码用 (非锚定: 命中 requestline/查询串内的路径形)
_BEARER_PATH = "/bot/items"
_BEARER_RE = re.compile(r"^\s*bearer\s+(\S+)\s*$", re.IGNORECASE)

_LOOPBACK_NETS = (ipaddress.ip_network("127.0.0.0/8"),
                  ipaddress.ip_network("::1/128"))


def _log(msg: str) -> None:
    print(f"[{datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _err(msg: str) -> None:
    """对用户可见的失败: 同时进日志与 stderr (与 monitor.err 同口径)."""
    _log(msg)
    print(f"[{datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}] {msg}",
          file=sys.stderr, flush=True)


# ------------------------------------------------------------------ 配置/凭据
def _load_conf() -> dict:
    """宽容读取 conf (仅启动期一次); 缺席 -> {} (正常拓扑, 静默);
    文件在但坏 JSON/非 dict -> _err 记档 + {} (不再静默落空; 值/内容永不回显)."""
    try:
        data = json.loads(CONF.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}                                   # 缺席 = 正常拓扑 (env token 面), 静默
    except (OSError, ValueError) as e:
        _err(f"conf 不可解析, 按空配置处理: {type(e).__name__}")
        return {}
    if not isinstance(data, dict):
        _err(f"conf 不可解析, 按空配置处理: {type(data).__name__}")
        return {}
    return data


def _resolve_token(conf: dict) -> str:
    """env FNM_ITEMS_TOKEN > conf items_token; 皆无 -> 空串 (调用方退 3)."""
    return str(os.environ.get(TOKEN_ENV) or conf.get("items_token") or "").strip()


def _conf_desc(v) -> str:
    """配置值安全描述: 类型名(+str 时长度); 值本身永不入日志/响应体."""
    return type(v).__name__ + (f"(len {len(v)})" if isinstance(v, str) else "")


def _resolve_port(conf: dict) -> int | None:
    """items_port 缺省 8766; 显式但非法 (含 bool/str 非数字/越界) -> None (调用方退 2)."""
    v = conf.get("items_port")
    if v is None:
        return DEFAULT_PORT
    if isinstance(v, bool):
        return None
    try:
        port = int(v)
    except (TypeError, ValueError):
        return None
    return port if 0 < port < 65536 else None


# ------------------------------------------------------------------ CIDR 白名单
def _parse_cidrs(raw) -> list:
    """白名单 = 环回 ∪ items_cidrs; 坏条目逐个忽略 (fail-closed, 不因错条目放开)."""
    nets = list(_LOOPBACK_NETS)
    if isinstance(raw, list):
        for c in raw:
            try:
                nets.append(ipaddress.ip_network(str(c), strict=False))
            except ValueError:
                _err(f"items_cidrs 条目不可解析, 已忽略: {_conf_desc(c)}")
    elif raw is not None:
        # 非 list 配置 (如裸字符串) 原先静默落仅环回——记警告 (只记类型名不回显值)
        _err(f"items_cidrs 须为列表, 已按仅环回(收到 {type(raw).__name__})")
    return nets


def _cidr_allows(ip: str, nets) -> bool:
    """来源 IP 是否落在白名单; 不可解析地址 -> 拒 (fail-closed)."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    if getattr(addr, "ipv4_mapped", None):                # ::ffff:a.b.c.d 归一
        addr = addr.ipv4_mapped
    return any(addr in n for n in nets)


# ------------------------------------------------------------------ 路由/鉴权
def _url_path_token(path: str) -> str | None:
    """/bot<token>/items -> token; 形不合 -> None (再交 bearer 形判定)."""
    m = _BOT_ITEMS_RE.match(path)
    return urllib.parse.unquote(m.group(1)) if m else None


def _bearer_token(header: str | None) -> str | None:
    """Authorization: Bearer <token> -> token; 形不合 -> None."""
    if not header:
        return None
    m = _BEARER_RE.match(header)
    return m.group(1) if m else None


def _mask_path(s: str) -> str:
    """请求日志掩码: /bot<...>/items -> /bot<token>/items (凭据永不入日志).

    用非锚定正则: requestline「GET /botX/items HTTP/1.1」与查询串内的路径形
    同样命中掩码; 误伤非路由路径无害 (日志面宁掩勿泄)。
    """
    return _BOT_ITEMS_PATH_RE.sub("/bot<token>/items", s)


# ------------------------------------------------------------------ 存档读面
def _load_meta():
    """旁路 .meta.json 宽容读取; 缺席/坏 JSON/病态深嵌套 -> None (不阻断, 见 _read_items)."""
    try:
        return json.loads(ITEMS_META.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError):
        return None                                # RecursionError: 病态深嵌套 meta,
                                                   # 与存档行级收容同口径 (A4)


class _ArchiveReadError(Exception):
    """内部哨兵: 存档在但读径坏 (非普通文件 / open/迭代 OSError 或 decode 错) ——
    _read_items 抛出, do_GET 捕之回 503 TG 包络; 读径坏显形, 不吞成
    200 前缀/空档与真空窗同形 (spec-fnm-obs R3/A3, CTB 侧经 HTTPError 衔接)。"""


def _read_items() -> list[dict]:
    """每请求现读 JSONL 存档 (与 monitor.read_items_archive 同口径, 不 import monitor).

    逐行解析: 坏行/非对象行跳过计数入日志; 文件缺席 -> [];
    非法 UTF-8/IO 错 -> 抛 _ArchiveReadError (本请求 503, 已读前缀随整尾丢弃)。
    类型闸: 路径在但非普通文件 (目录/FIFO/设备/symlink 落非普通) -> _ArchiveReadError
    -> 503 (FIFO open 永久阻塞/线程泄漏在此截断); 不存在走 FileNotFoundError 原径。
    读界双帽: 行数帽 ITEMS_READ_MAX 与累计字节界 ITEMS_READ_MAX_BYTES 同口径——
    超帽/超界 _err 记档 + break 截断 -> 200 前缀 (受控限量, 非读径坏不升 503)。
    meta 闸: meta 为 dict 且显含 schema 键且 != ITEMS_SCHEMA -> [] (fail-closed);
    其余 meta 形态不阻断 (monitor 写侧每次追加都自愈 meta, 缺席=陈旧非不兼容)。
    """
    meta = _load_meta()
    if isinstance(meta, dict) and "schema" in meta and meta["schema"] != ITEMS_SCHEMA:
        _log("items meta schema 不符, 本请求按空档响应")
        return []
    if os.path.exists(ITEMS_ARCHIVE) and not os.path.isfile(ITEMS_ARCHIVE):
        # A3 文件类型闸: exists/isfile 均 follow symlink——dangling symlink 落
        # exists 假走 FNF 200 原径; 在而非常规文件 (目录/FIFO/设备/链接落非普通)
        # 一律 503 显形, 不在 open 上永久阻塞 (FIFO) 或裸吃 OSError 后才 503
        _err(f"items 存档非普通文件 {ITEMS_ARCHIVE.name}: "
             f"本请求 503 显形 (目录/FIFO/设备/非普通文件统收)")
        raise _ArchiveReadError("non-regular archive")
    out: list[dict] = []
    skipped = 0
    nbytes = 0
    try:
        with open(ITEMS_ARCHIVE, "r", encoding="utf-8") as fh:
            for line in fh:
                nbytes += len(line.encode("utf-8", errors="replace"))  # 读径字节估算
                if nbytes > ITEMS_READ_MAX_BYTES:           # A3 字节界: 单行巨型行
                    _err(f"items 存档超字节界 {ITEMS_READ_MAX_BYTES}, 截断 "   # 不整吃
                         f"(已留 {len(out)} 条, 其后整尾丢弃)")   # 内存; 与行数帽同口径
                    break
                line = line.strip()
                if not line:
                    continue
                if len(out) + skipped >= ITEMS_READ_MAX:
                    _err(f"items 存档超行数帽 {ITEMS_READ_MAX}, 截断 "
                         f"(已留 {len(out)} 条, 其后整尾丢弃)")   # F7 读界
                    break
                try:
                    rec = json.loads(line)
                except (json.JSONDecodeError, RecursionError, ValueError):
                    skipped += 1                  # 病态嵌套/超位大整数 (>4300 位 int 字面量
                    continue                      # 抛裸 ValueError) 同坏行跳过——不穿外层丢整尾
                if isinstance(rec, dict):
                    out.append(rec)
                else:
                    skipped += 1                  # 顶层非对象不算有效条目
    except FileNotFoundError:
        _log("items 存档缺席, 按空档响应")          # F3: 缺席才记一行, 远端可分辨真空窗 vs 读径坏
    except (OSError, ValueError) as e:              # ValueError 含 UnicodeDecodeError
        _err(f"items 存档读取失败 {ITEMS_ARCHIVE.name}: {type(e).__name__}: {e} "
             f"(已读前缀 {len(out)} 条随整尾丢弃, 本请求 503 显形)")  # R3: 读径坏不回前缀
        raise _ArchiveReadError(f"{type(e).__name__}: {e}") from e
    if skipped:
        _log(f"items 存档读取: 跳过 {skipped} 行坏行/非对象行, 保留 {len(out)} 条")
    return out


# ------------------------------------------------------------------ HTTP 面
def _json_body(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    server_version = "fnm-items/1.0"
    protocol_version = "HTTP/1.1"                     # 每响应都显式 Content-Length

    def setup(self) -> None:
        super().setup()
        self.request.settimeout(REQ_TIMEOUT_S)        # F1: connect-not-send 不再无限占线程

    def _send_json(self, code: int, payload: dict) -> None:
        body = _json_body(payload)
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        try:
            self.end_headers()                   # flush_headers 亦走 wfile.write, 同收容
            if self.command == "HEAD":           # HEAD 响应禁带体; Content-Length 头仍如实保留
                return
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError,
                ConnectionResetError, TimeoutError):
            _log(f"客户端断连, 响应未写完 ({self.client_address[0]})")   # F8: 单行收容, 不刷 traceback

    def _send_tg_error(self, code: int, desc: str) -> None:
        self._send_json(code, {"ok": False, "error_code": code, "description": desc})

    def do_GET(self) -> None:
        # 1) CIDR 白名单先行 (fail-closed): 非白名单连路由形状都探不到
        if not _cidr_allows(self.client_address[0], self.server.cidrs):
            self._send_tg_error(401, "unauthorized")
            return
        try:
            path = urllib.parse.urlsplit(self.path).path   # 查询串不影响路由
        except ValueError:                       # 畸形 absolute-URI -> 404 包络, 不让连接裸崩
            self._send_tg_error(404, "not found")
            return
        token = _url_path_token(path)                  # url-path 形
        if token is None:
            if path == _BEARER_PATH:
                token = _bearer_token(self.headers.get("Authorization"))   # bearer 形
            else:
                self._send_tg_error(404, "not found")
                return
        # 2) 严格 == 等值判定 (禁 startswith/正则松判); 无法验证一律 401
        if token != self.server.items_token:
            self._send_tg_error(401, "unauthorized")
            return
        try:
            items = _read_items()
        except _ArchiveReadError:
            # R3: 读径坏显形——503 TG 包络, 不回 200 前缀/空档与真空窗同形
            self._send_tg_error(503, "archive read error")
            return
        self._send_json(200, {"schema": ITEMS_SCHEMA, "items": items})

    def _m405(self) -> None:
        self._send_tg_error(405, "method not allowed")

    do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = _m405

    def log_message(self, fmt, *args) -> None:
        """请求日志: token 段掩码 + 客户端 IP + TZ_CN 时间戳 (与 _log/_err 同口径); 凭据永不入日志."""
        msg = fmt % args if args else fmt
        sys.stderr.write(f"[{datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')}] "
                         f"{self.client_address[0]} {_mask_path(msg)}\n")


class _ItemsServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, token: str, cidrs) -> None:
        # 先挂属性再 super(): 父类构造内已 bind+listen, 窗内请求须能读到 (免 AttributeError)
        self.items_token = token
        self.cidrs = cidrs
        super().__init__(addr, _Handler)


# ------------------------------------------------------------------ selftest
def selftest() -> int:
    """离线自检: 不监听端口, 不读档——路由/掩码/CIDR/序列化/token 解析."""
    fails: list[str] = []
    oks = 0

    def check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal oks
        print(f"{'ok ' if cond else 'BAD'} {name}{(' -> ' + detail) if not cond else ''}")
        if cond:
            oks += 1
        else:
            fails.append(name)

    # 1) URL-path 路由正则提取 token
    check("url-path token 提取", _url_path_token("/botABC123/items") == "ABC123")
    check("url-path 空段不匹配 (让位 bearer 形)", _url_path_token("/bot/items") is None)
    check("url-path 尾斜杠不匹配", _url_path_token("/botABC/items/") is None)
    check("url-path 多余段不匹配", _url_path_token("/botABC/items/x") is None)
    check("url-path 前缀段不匹配", _url_path_token("/x/botABC/items") is None)
    check("url-path 百分号解码", _url_path_token("/bot%41%42/items") == "AB")

    # 2) Bearer 头解析
    check("bearer 解析", _bearer_token("Bearer tok-1") == "tok-1")
    check("bearer scheme 大小写兼容", _bearer_token("bearer tok-1") == "tok-1")
    check("bearer 缺头 None", _bearer_token(None) is None)
    check("bearer 非 Bearer 形 None", _bearer_token("Basic dGVzdA==") is None)
    check("bearer 空值 None", _bearer_token("") is None)

    # 3) token 掩码
    check("掩码 url-path", _mask_path("/botSECRET/items") == "/bot<token>/items")
    check("掩码 requestline",
          _mask_path("GET /botSECRET/items HTTP/1.1") == "GET /bot<token>/items HTTP/1.1")
    check("掩码不伤 bearer 形路径", _mask_path("/bot/items") == "/bot/items")

    # 4) CIDR 判定
    nets = _parse_cidrs(["192.168.31.0/24"])
    check("cidr 环回恒通", _cidr_allows("127.0.0.1", nets)
          and _cidr_allows("::1", nets))
    check("cidr 名单内放行", _cidr_allows("192.168.31.95", nets))
    check("cidr 名单外拒绝", not _cidr_allows("10.0.0.5", nets))
    check("cidr 非法地址拒绝", not _cidr_allows("not-an-ip", nets))
    check("cidr 缺席仅环回 (fail-closed)",
          not _cidr_allows("192.168.31.95", _parse_cidrs(None)))
    check("cidr 坏条目忽略不放开", len(_parse_cidrs(["bad-net", "10.0.0.0/8"])) == 3)

    # 5) 序列化形: 空档裸形 / TG 包络
    obj = json.loads(_json_body({"schema": ITEMS_SCHEMA, "items": []}))
    check("空档形 {\"schema\":1,\"items\":[]}", obj == {"schema": 1, "items": []})
    check("schema 严格 int", type(obj["schema"]) is int)
    check("TG 包络形", json.loads(
        _json_body({"ok": False, "error_code": 401, "description": "unauthorized"}))
        == {"ok": False, "error_code": 401, "description": "unauthorized"})

    # 6) token 解析: env 优先, 皆无空串 (selftest 内临时改 env 后还原)
    env0 = os.environ.pop(TOKEN_ENV, None)
    try:
        check("token conf 读取", _resolve_token({"items_token": "conf-tok"}) == "conf-tok")
        os.environ[TOKEN_ENV] = "env-tok"
        check("token env 优先", _resolve_token({"items_token": "conf-tok"}) == "env-tok")
        os.environ.pop(TOKEN_ENV, None)
        check("token 皆无 -> 空串", _resolve_token({}) == "")
    finally:
        if env0 is None:
            os.environ.pop(TOKEN_ENV, None)
        else:
            os.environ[TOKEN_ENV] = env0

    print(f"selftest: {oks} ok / {len(fails)} BAD")
    print(f"selftest: {'PASS' if not fails else 'FAILED: ' + ', '.join(fails)}")
    return 0 if not fails else 1


# ------------------------------------------------------------------ main
def main() -> int:
    conf = _load_conf()
    token = _resolve_token(conf)
    if not token:
        _err(f"缺少 items token: 需设置 {TOKEN_ENV} 或 {CONF} 的 items_token 字段")
        return 3                                       # 与 monitor.py token 缺位退码对齐
    port = _resolve_port(conf)
    if port is None:
        _err(f"items_port 非法 (需 1-65535 整数): 类型 {_conf_desc(conf.get('items_port'))}")
        return 2                                       # 配置非法 fail-loud, 不静默回退
    bind = str(conf.get("items_bind") or DEFAULT_BIND).strip() or DEFAULT_BIND
    cidrs = _parse_cidrs(conf.get("items_cidrs"))
    try:
        srv = _ItemsServer((bind, port), token, cidrs)
    except OSError as e:
        _err(f"items server 绑定失败 {bind}:{port}: {e}")
        return 2                                       # bind 失败 fail-loud 退 2, 不裸崩
    _log(f"items server 启动 {bind}:{port} (白名单 {len(cidrs)} 网段, 每请求现读存档)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(main())
