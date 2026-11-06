#!/usr/bin/env python3
"""离线驱动 futures-news-monitor 的假时钟仿真器 (不发任何真消息).

用法: python3 sim.py [场景名]
  默认跑全部场景; 每个场景都验证「聚合节奏 / 队列不丢 / 失败重试 / 单日上限」。
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent if (HERE.parent / "monitor.py").exists() else HERE
def _res(name: str) -> Path:
    """monitor.py 可能在 tests/ 的上一层(工程布局) 或同层(历史 work 快照)."""
    for c in (ROOT / name, HERE / name):
        if c.exists():
            return c
    raise FileNotFoundError(name)
NOW0 = 1793800000          # 固定起点, 单位秒
TZ_OFF = 8 * 3600
FILTERED = {"src": "仿真源", "ts": 0, "text": "英伟达美股盘前涨超2%"}   # 稳定被过滤, 用来避免「全源失败」


class FakeTime(types.SimpleNamespace):
    def __init__(self, t0: float):
        self.t = t0
        self.mono = 1000.0

    def time(self) -> float:
        return self.t

    def monotonic(self) -> float:
        return self.mono

    def sleep(self, s: float) -> None:
        self.t += s
        self.mono += s


def load_monitor(tmp: Path):
    spec = importlib.util.spec_from_file_location("m", _res("monitor.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    st = tmp / "state"
    st.mkdir(parents=True, exist_ok=True)
    m.STATE = st / "seen.json"
    m.QUEUE = st / "queue.json"
    m.PENDING = st / "pending.json"
    m.LOCK = st / "lock"
    m.CONF = tmp / "conf.json"
    m.CONF.write_text(json.dumps({"token": "x", "chat_id": 1, "max_age_min": 180,
                                  "digest_min": 30, "digest_max_items": 40}))
    m.LOG = tmp / "out.log"
    # 预填一条已读, 避免触发首轮 seed (seed 会吞掉第一波)
    (st / "seen.json").write_text(json.dumps({"seedmarker": 1}))
    m.TS_BAD.clear()
    return m


def run(m, ft: FakeTime, items: list[dict], fail_send: bool = False, args: list[str] | None = None):
    """跑一轮 main(), 返回 (本轮发出的消息列表, 退出码)."""
    m.time = ft
    def _src(deadline=None):
        return [dict(it, ts=int(ft.time()) - 60) if it.get("ts") == 0 else dict(it) for it in items]
    m.SOURCES = [("仿真源", _src)]
    out: list[str] = []
    m.send = lambda text, conf, tk, depth=0: (out.append(text), not fail_send)[1]
    m.resolve_token = lambda conf: "x"
    m.rotate_log = lambda: None
    sys.argv = ["monitor.py", *(args or [])]
    code = m.main()
    return out, code


def mk(n: int, base_ts: int, tag: str = "") -> list[dict]:
    return [{"src": "仿真源", "ts": base_ts - i, "text": f"沪铜库存第{n}批{tag}变化{i}吨环比{n}%"}
            for i in range(n)]


def scenario_cadence() -> bool:
    """30 分钟窗口: 前 14 轮只入队不发送, 攒够 40 条提前发."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    sends, total_items = [], 0
    for r in range(30):                        # 30 轮 x 2 分钟 = 60 分钟
        items = mk(3, int(ft.time()) - 60, tag=f"r{r}")
        total_items += len(items)
        out, _ = run(m, ft, items)
        sends.extend(out)
        ft.t += 120
    q, _, _ = m.load_queue()
    print(f"  cadence: 30轮 {total_items}条 -> {len(sends)}次发送, 队列剩{len(q)}条")
    ok = len(sends) <= 4 and len(q) <= 40 and len(sends) >= 2
    # 未发送的不许凭空消失
    ok = ok and (sum(t.count("沪铜库存") for t in sends) + len(q) == total_items)
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_noloss() -> bool:
    """发送失败时条目必须留在队列里, 不标记已读."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    out, code = run(m, ft, mk(45, int(ft.time()) - 60, tag="f0"), fail_send=True)
    q_before, q_after = [], m.load_queue()[0]
    q_before = list(q_after)                    # 失败后应当一条不少地留下
    ft.t += 120
    q_after, _, _ = m.load_queue()
    seen, _ = m.load_state(m.STATE)
    sent_keys = {k for k in seen if not k.startswith("_") and k != "seedmarker"}
    print(f"  noloss: 喂 45 条且发送全失败 -> 队列{len(q_after)} 条, "
          f"已读{sent_keys.__len__()} 条, 退出码{code}")
    ok = len(q_after) == 45 and len(sent_keys) == 0 and code == 2
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_window() -> bool:
    """未到窗口必须不发送 (单轮 3 条, 只等 10 分钟)."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    sends = []
    for r in range(5):                         # 5 轮 = 10 分钟
        out, _ = run(m, ft, mk(1, int(ft.time()) - 60, tag=f"w{r}"))
        sends.extend(out)
        ft.t += 120
    print(f"  window: 10 分钟内 {len(sends)} 次发送 (期望 0)")
    shutil.rmtree(tmp, ignore_errors=True)
    return len(sends) == 0


def scenario_cap() -> bool:
    """单日通知上限生效: 把 cap 压到 2, 之后继续攒不许再发."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    m.DAILY_MSG_CAP = 2
    ft = FakeTime(NOW0)
    sends = 0
    for r in range(60):                        # 2 小时, 每轮 3 条
        out, _ = run(m, ft, mk(3, int(ft.time()) - 60, tag=f"c{r}"))
        sends += len(out)
        ft.t += 120
    q, _, _ = m.load_queue()
    print(f"  cap: 2 小时内发送 {sends} 条消息 (上限 2 + 1 条触顶提示), 队列积压 {len(q)} 条")
    ok = sends <= 3 and len(q) <= m.QUEUE_MAX
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_dedup_queue() -> bool:
    """同一轮重复推送的同一事件不得在队列里堆两份."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    items = mk(3, int(ft.time()) - 60, tag="d")
    for _ in range(10):                        # 同一批内容喂 10 轮
        run(m, ft, items)
        ft.t += 120
    q, _, _ = m.load_queue()
    print(f"  dedup: 同一批喂 10 轮, 队列 {len(q)} 条 (期望 3)")
    shutil.rmtree(tmp, ignore_errors=True)
    return len(q) == 3



def scenario_persist() -> bool:
    """回归 P0: 未到窗口的轮次必须把 _digest.last_flush 落盘, 窗口才会真的走."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    sends, flush_at = [], None
    for r in range(20):                       # 每轮 1 条, 40 分钟内总量远不到 40 条
        out, _ = run(m, ft, mk(1, int(ft.time()) - 60, tag=f"p{r}"))
        if out and flush_at is None:
            flush_at = (r + 1) * 2
        sends.extend(out)
        ft.t += 120
    st, _ = m.load_state(m.STATE)
    has_meta = isinstance(st.get("_digest"), dict) and st["_digest"].get("last_flush")
    print(f"  persist: 20 轮共发 {len(sends)} 次, 首发达 {flush_at} 分钟, "
          f"last_flush 落盘={bool(has_meta)}")
    ok = bool(has_meta) and len(sends) == 1 and flush_at is not None and 28 <= flush_at <= 34
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_divergence() -> bool:
    """回归 P1: 队列里残留的已读条目不得被重推 (seen 与队列分叉)."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    run(m, ft, mk(5, int(ft.time()) - 60, tag="dx"))     # 入队 5 条
    q, _, _ = m.load_queue()
    keys = {it["key"]: int(it["ts"]) for it in q}
    st, _ = m.load_state(m.STATE)
    st.update({k: v for k, v in keys.items()})           # 假装这些键已读, 但队列没清
    st["_digest"] = {"last_flush": int(ft.time()) - 3600}   # 窗口已过 -> 会 flush
    m.save_json(m.STATE, st)
    irrelevant = [{"src": "仿真源", "ts": int(ft.time()) - 60, "text": "英伟达美股盘前涨超2%"}]
    out, _ = run(m, ft, irrelevant)          # 喂一条被过滤掉的, 避免触发「全源失败」提前返回
    q2, _, _ = m.load_queue()
    pushed = [t for t in out if "沪铜库存" in t]
    print(f"  divergence: 队列残留 5 条已读 -> 重推 {len(pushed)} 条, 残留清后队列 {len(q2)} 条")
    ok = len(pushed) == 0 and len(q2) == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_backoff() -> bool:
    """回归 P1: 发送失败后必须退避, 不能在 2 分钟后立刻重打."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    run(m, ft, mk(45, int(ft.time()) - 60, tag="b0"), fail_send=True)
    ft.t += 120
    out, code = run(m, ft, mk(50, int(ft.time()) - 60, tag="b1"))
    q, _, _ = m.load_queue()
    print(f"  backoff: 失败后 2 分钟再跑 -> 发送 {len(out)} 次, 退出码 {code}, 队列 {len(q)} 条")
    ok = len(out) == 0 and code == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_overflow() -> bool:
    """队列硬上限: 溢出条目要记账 (写 seen), 否则会被源站反复拉回再丢."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    run(m, ft, mk(250, int(ft.time()) - 60, tag="ov"))
    q, _, _ = m.load_queue()
    st, _ = m.load_state(m.STATE)
    seen_n = len([k for k in st if not k.startswith("_") and k != "seedmarker"])
    print(f"  overflow: 喂 250 条 -> 队列 {len(q)} 条, seen 记 {seen_n} 条")
    ok = len(q) <= m.QUEUE_MAX and seen_n >= 250 - m.QUEUE_MAX
    shutil.rmtree(tmp, ignore_errors=True)
    return ok

def scenario_flushnow_empty() -> bool:
    """V1-P1-1 回归: 空队列 --flush-now 不得伪造一次发送失败."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    out, code = run(m, ft, [FILTERED], args=["--flush-now"])
    st, _ = m.load_state(m.STATE)
    meta = st.get("_digest") or {}
    print(f"  flushnow: 空队列退出码 {code}, 消息 {len(out)}, fails={meta.get('fails')}, "
          f"next_try={'有' if meta.get('next_try') else '无'}")
    ok = code == 0 and not out and not meta.get("fails") and not meta.get("next_try")
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_freeze_aging() -> bool:
    """V1-P1-2 回归: 退避期间队列条目不得按 180min 老化写 seen."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    old_ts = int(ft.time()) - 200 * 60
    keys = {f"frz{i}" for i in range(3)}
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": old_ts - i,
                                     "text": f"沪铜库存冻结测试{i}吨", "key": f"frz{i}"}
                                    for i in range(3)], "ts": old_ts})
    st, _ = m.load_state(m.STATE)
    st["_digest"] = {"last_flush": int(ft.time()) - 60, "fails": 1,
                     "next_try": int(ft.time()) + 600}
    m.save_json(m.STATE, st)
    out, code = run(m, ft, [FILTERED])
    q, _, _ = m.load_queue()
    seen, _ = m.load_state(m.STATE)
    kept = [it for it in q if it["key"] in keys]
    marked = [k for k in seen if k.startswith("frz")]
    print(f"  freeze: 退避中跑一轮 -> 队列留 {len(kept)} 条, 被标已读 {len(marked)} 条, 退出码 {code}")
    ok = len(kept) == 3 and not marked and code == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_retry_after_fail() -> bool:
    """V1-P2-7 回归: 发失败后到点必须重试, 不能等 40 条/30 分钟凑齐."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    run(m, ft, mk(5, int(ft.time()) - 60, tag="rt"), fail_send=True, args=["--flush-now"])
    st, _ = m.load_state(m.STATE)
    meta = st.get("_digest") or {}
    wait = max(0, int(meta.get("next_try") or 0) - int(ft.time()))
    ft.t += wait + 5                          # 退避到期
    out, code = run(m, ft, [FILTERED])        # 无新条目
    print(f"  retry: 失败后等 {wait}s 重试 -> 发出 {len(out)} 条消息, 退出码 {code}")
    ok = len(out) == 1 and code == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_cap_freeze() -> bool:
    """V1-P1-2 回归: 触顶期间不老化, 条目留到次日而不是被标已读丢弃."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    m.DAILY_MSG_CAP = 1
    ft = FakeTime(NOW0)
    old_ts = int(ft.time()) - 200 * 60
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": old_ts - i,
                                     "text": f"沪铜库存触顶测试{i}吨", "key": f"cap{i}"}
                                    for i in range(4)], "ts": old_ts})
    st, _ = m.load_state(m.STATE)
    st["_day"] = {"date": m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d"), "msgs": 1,
                  "cap_notified": True}
    m.save_json(m.STATE, st)
    out, code = run(m, ft, [FILTERED])
    q, _, _ = m.load_queue()
    seen, _ = m.load_state(m.STATE)
    marked = [k for k in seen if k.startswith("cap")]
    print(f"  capfreeze: 触顶中跑一轮 -> 队列留 {len(q)} 条, 被标已读 {len(marked)} 条, 退出码 {code}")
    ok = len(q) == 4 and not marked and code == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_hold() -> bool:
    """V2 回归: 孤立单条最迟 45 分钟兜底发出 (原 90 分钟)."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    sent_at = None
    items = [{"src": "仿真源", "ts": 0, "text": "纽约原油暗盘跌破103美元 日内跌超12%"}]
    run(m, ft, items)                                  # 入队
    for _ in range(30):                                # 每轮 2 分钟, 最多 60 分钟
        ft.t += 120
        out, _code = run(m, ft, [FILTERED])
        if out:
            sent_at = (ft.t - NOW0) / 60
            break
    print(f"  hold: 孤立单条约 {sent_at or 0:.0f} 分钟发出 (期望 45±3)")
    ok = sent_at is not None and 42 <= sent_at <= 48
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_unfreeze() -> bool:
    """V3-P1 回归: 触顶/退避解除后, 隔夜条目应补发而不是第一轮就被老化判死."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    old_ts = int(ft.time()) - 400 * 60                  # 远超 max_age=180min
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": old_ts - i,
                                     "text": f"沪铜库存隔夜测试{i}吨", "key": f"ng{i}"}
                                    for i in range(3)], "ts": old_ts})
    today = m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d")
    st, _ = m.load_state(m.STATE)
    st["_day"] = {"date": today, "msgs": 999, "cap_notified": True}   # 当天触顶
    st["_digest"] = {"last_flush": int(ft.time()) - 3000, "last_round": int(ft.time()) - 3000}
    m.save_json(m.STATE, st)
    run(m, ft, [FILTERED])                              # 触顶中, 累积 block_acc
    q, _, _ = m.load_queue()
    marked_mid = [k for k in (m.load_state(m.STATE)[0]) if k.startswith("ng")]
    ft.t += 3000                                        # 2.5 小时后"次日"
    st, _ = m.load_state(m.STATE)
    st["_day"] = {"date": m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d"), "msgs": 0,
                  "cap_notified": False}
    m.save_json(m.STATE, st)
    out = []
    for _ in range(5):
        o, _c = run(m, ft, [FILTERED])
        out.extend(o)
        ft.t += 130
    seen_after = m.load_state(m.STATE)[0]
    marked = [k for k in seen_after if k.startswith("ng")]
    pushed_msgs = sum(1 for t in out if "隔夜测试" in t)
    print(f"  unfreeze: 触顶后队列 {len(q)} 条 / 解冻前已读 {len(marked_mid)} 条 -> "
          f"补发消息 {len(out)} 条(含隔夜 {pushed_msgs}) / 隔夜条目落 seen {len(marked)} 条")
    ok = len(marked_mid) == 0 and pushed_msgs == 1 and len(marked) == 3
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_nofail_loop() -> bool:
    """V4 回归: fails>0 + 空队列 + 退避已过期 -> 不得零尝试再记失败 (自锁 30 分钟)."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    st, _ = m.load_state(m.STATE)
    st["_digest"] = {"last_flush": int(ft.time()) - 600, "fails": 2,
                     "next_try": int(ft.time()) - 10}       # 退避已过期, 队列空
    m.save_json(m.STATE, st)
    codes, fails_seq = [], []
    for _ in range(6):
        _o, c = run(m, ft, [FILTERED])
        codes.append(c)
        fails_seq.append((m.load_state(m.STATE)[0].get("_digest") or {}).get("fails"))
        ft.t += 130
    print(f"  nofail: 6 轮退出码 {codes}, fails 轨迹 {fails_seq} (期望全 0 / 全 2)")
    ok = all(c == 0 for c in codes) and all(f == 2 for f in fails_seq)
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_sleep_wake() -> bool:
    """V4-P1 回归: 健康态带积压休眠 12h 后醒来, 不得把 20 分钟内还是新货的条目老化丢掉."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": int(ft.time()) - 60,
                                     "text": f"沪铜库存测试{i}吨", "key": f"sw{i}"}
                                    for i in range(100)], "ts": int(ft.time())})
    st, _ = m.load_state(m.STATE)
    st["_digest"] = {"last_flush": int(ft.time()) - 3600,
                     "last_round": int(ft.time()) - 3600}   # 上一轮心跳, 之后休眠
    m.save_json(m.STATE, st)
    ft.t += 12 * 3600                                   # 休眠 12 小时
    msgs = 0
    for _ in range(6):
        out, _c = run(m, ft, [FILTERED])
        msgs += sum(1 for t in out if "库存测试" in t)
        ft.t += 130
    seen_after = m.load_state(m.STATE)[0]
    marked = [k for k in seen_after if k.startswith("sw")]
    q, _, _ = m.load_queue()
    print(f"  sleepwake: 睡 12h 后 6 轮 -> 含积压的消息 {msgs} 条, 落 seen {len(marked)}/100, "
          f"队列剩 {len(q)}")
    ok = msgs == 3 and len(marked) == 100 and len(q) == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_unfreeze_big() -> bool:
    """V4 回归: 解冻剩货 ≥41 条时不得被老化判死 (覆盖 block_acc 门闩, 原来零覆盖)."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    old = int(ft.time()) - 400 * 60
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": old - i,
                                     "text": f"焦煤夜盘测试{i}吨", "key": f"ub{i}"}
                                    for i in range(60)], "ts": old})
    today = m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d")
    st, _ = m.load_state(m.STATE)
    st["_day"] = {"date": today, "msgs": 999, "cap_notified": True}
    st["_digest"] = {"last_flush": int(ft.time()) - 7200, "last_round": int(ft.time()) - 7200}
    m.save_json(m.STATE, st)
    run(m, ft, [FILTERED])                              # 触顶一轮, 累 block_acc
    ft.t += 3600
    st, _ = m.load_state(m.STATE)
    st["_day"] = {"date": m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d"), "msgs": 0,
                  "cap_notified": False}
    m.save_json(m.STATE, st)
    msgs = 0
    for _ in range(4):                                  # 40*2 = 80 容量 > 60
        out, _c = run(m, ft, [FILTERED])
        msgs += sum(1 for t in out if "焦煤夜盘测试" in t)
        ft.t += 130
    q, _, _ = m.load_queue()
    marked = [k for k in m.load_state(m.STATE)[0] if k.startswith("ub")]
    print(f"  unfreezebig: 60 条 400 分钟积压解冻后 -> 含积压的消息 {msgs} 条, "
          f"落 seen {len(marked)}/60, 队列剩 {len(q)}")
    ok = msgs == 2 and len(marked) == 60 and len(q) == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def to_int_safe(v) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def scenario_dirty_meta() -> bool:
    """V3-P2 回归: fails/msgs 各类脏值 + 真失败路径都不得崩."""
    cases = {"fails-x": {"fails": "x"}, "fails-inf": {"fails": float("inf")},
             "msgs-inf": {"_DAY_INF": True}, "lf-nan": {"last_flush": float("nan")}}
    bad = []
    for name, patch in cases.items():
        tmp = Path(tempfile.mkdtemp())
        m = load_monitor(tmp)
        ft = FakeTime(NOW0)
        st, _ = m.load_state(m.STATE)
        if patch.get("_DAY_INF"):
            st["_day"] = {"date": m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d"),
                          "msgs": float("inf")}
        else:
            st["_digest"] = dict(patch, last_flush=int(ft.time()) - 600)
        m.save_json(m.STATE, st)
        try:
            _o, c = run(m, ft, mk(3, int(ft.time()) - 60), fail_send=True,
                        args=["--flush-now"])
            if c not in (0, 2):
                bad.append(f"{name}=code{c}")
        except Exception as e:                     # noqa: BLE001
            bad.append(f"{name}={type(e).__name__}")
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"  dirty: 4 种脏值 -> {'全部不崩' if not bad else '崩: ' + ', '.join(bad)}")
    return not bad




def scenario_aging_drain_window() -> bool:
    """V6-#2 回归: 老化把那轮剩货清空后, 必须推进 last_flush (窗口重开).

    构造: 健康态 + 队列 50 条 200 分钟旧货 -> 第 1 轮发 40、老化 10、队列 0。
    紧接 3 条新货 (5s 后) 必须「不发」; 若 last_flush 没推进会立刻发 1 条。
    """
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    old = int(ft.time()) - 200 * 60
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": old - i,
                                     "text": f"螺纹钢积压测试{i}元", "key": f"ad{i}"}
                                    for i in range(50)], "ts": old})
    st, _ = m.load_state(m.STATE)
    st["_digest"] = {"last_flush": int(ft.time()) - 3600,
                     "last_round": int(ft.time()) - 120}
    m.save_json(m.STATE, st)
    run(m, ft, [FILTERED])                             # 第 1 轮: 发 40 + 老化 10
    q, _, _ = m.load_queue()
    ft.t += 5
    out, _c = run(m, ft, mk(3, int(ft.time()) - 60, tag="adnew"))
    print(f"  agingdrain: 第1轮后队列 {len(q)} 条 -> 5s 后 3 条新货发出 "
          f"{sum(1 for t in out if '沪铜库存' in t)} 条 (期望 0)")
    ok = len(q) == 0 and sum(1 for t in out if "沪铜库存" in t) == 0
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_flushnow_overcap_log() -> bool:
    """V6-#3 回归: --flush-now 越限必须留痕 (日志里有且仅有一句「越限」)."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    m.DAILY_MSG_CAP = 1
    ft = FakeTime(NOW0)
    today = m.datetime.now(m.TZ_CN).strftime("%Y-%m-%d")
    st, _ = m.load_state(m.STATE)
    st["_day"] = {"date": today, "msgs": 1}
    st["_digest"] = {"last_flush": int(ft.time()) - 900}
    m.save_json(m.STATE, st)
    seen_err: list[str] = []
    real_err = m.err
    m.err = lambda msg: (seen_err.append(msg), real_err(msg))[1]
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": int(ft.time()) - 900,
                                     "text": "沪锡越限测试合约涨1%", "key": "oc1"}],
                          "ts": int(ft.time()) - 900})
    out, _c = run(m, ft, mk(3, int(ft.time()) - 60, tag="fc"), args=["--flush-now"])
    m.err = real_err
    hits = sum(1 for x in seen_err if "越限推送" in x)
    print(f"  overcap: 发出 {sum(1 for t in out if '沪铜库存' in t)} 条, "
          f"日志「越限推送」{hits} 行 (期望 1)")
    # 不带 --flush-now 时不许误报
    tmp2 = Path(tempfile.mkdtemp())
    m2 = load_monitor(tmp2)
    m2.DAILY_MSG_CAP = 1
    ft2 = FakeTime(NOW0)
    st2, _ = m2.load_state(m2.STATE)
    st2["_day"] = {"date": m2.datetime.now(m2.TZ_CN).strftime("%Y-%m-%d"), "msgs": 1}
    st2["_digest"] = {"last_flush": int(ft2.time()) - 900}
    m2.save_json(m2.STATE, st2)
    err2: list[str] = []
    real_err2 = m2.err
    m2.err = lambda msg: (err2.append(msg), real_err2(msg))[1]
    m2.save_json(m2.QUEUE, {"items": [{"src": "仿真源", "ts": int(ft2.time()) - 900,
                                       "text": "沪锡越限测试合约涨1%", "key": "oc2"}],
                            "ts": int(ft2.time()) - 900})
    run(m2, ft2, mk(3, int(ft2.time()) - 60, tag="fc2"))
    m2.err = real_err2
    n2 = sum(1 for x in err2 if "越限推送" in x)
    ok = hits == 1 and n2 == 0
    print(f"  overcap: 普通轮次误报 {n2} 行 (期望 0)")
    shutil.rmtree(tmp, ignore_errors=True)
    shutil.rmtree(tmp2, ignore_errors=True)
    return ok


def scenario_clock_rollback() -> bool:
    """V6-#7 回归: last_round 在将来 (时钟回拨) + 长休眠 -> 不得老化丢货."""
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": int(ft.time()) - 60 - i,
                                     "text": f"沪镍积压回拨{i}吨", "key": f"rb{i}"}
                                    for i in range(100)], "ts": int(ft.time())})
    st, _ = m.load_state(m.STATE)
    st["_digest"] = {"last_round": int(ft.time()) + 86400,     # 表跳到明天
                     "last_flush": int(ft.time()) - 1800}
    m.save_json(m.STATE, st)
    stale_hits: list[int] = []
    real_stale = m.record_stale
    m.record_stale = lambda seen, stale, max_age: (
        stale_hits.append(len(stale)), real_stale(seen, stale, max_age))[1]
    ft.t += 12 * 3600                                          # 睡 12 小时
    msgs = 0
    for _ in range(4):
        out, _c = run(m, ft, [FILTERED])
        msgs += sum(1 for t in out if "沪镍积压回拨" in t)
        ft.t += 130
    marked = [k for k in m.load_state(m.STATE)[0] if k.startswith("rb")]
    q, _, _ = m.load_queue()
    print(f"  rollback: 回拨 + 睡12h 后 -> 含积压的消息 {msgs} 条, "
          f"落 seen {len(marked)}/100, 队列剩 {len(q)}")
    ok = len(marked) == 100 and len(q) == 0 and sum(stale_hits) == 0
    msgs = msgs  # 消息条数只作展示
    shutil.rmtree(tmp, ignore_errors=True)
    return ok


def scenario_exit_codes() -> bool:
    """V6-#8 回归: 4/5/6/7 语义唯一 (state 不可读=4, 锁不可写=7)."""
    bad = []
    # 4: state 坏 JSON
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    m.STATE.write_text("{坏 json")
    _o, c = run(m, FakeTime(NOW0), [FILTERED])
    if c != 4:
        bad.append(f"state-bad={c}")
    shutil.rmtree(tmp, ignore_errors=True)
    # 7: 锁是目录 (open 失败)
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    m.LOCK.mkdir()
    _o, c = run(m, FakeTime(NOW0), [FILTERED])
    if c != 7:
        bad.append(f"lock-dir={c}")
    shutil.rmtree(tmp, ignore_errors=True)
    # 5: state 落盘失败 + 已发送
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    m.save_json(m.QUEUE, {"items": [{"src": "仿真源", "ts": int(ft.time()) - 900,
                                     "text": "沪铅落盘测试合约跌1%", "key": "ec1"}],
                          "ts": int(ft.time()) - 900})
    m.persist_state = lambda *a, **k: False
    _o, c = run(m, ft, mk(3, int(ft.time()) - 60, tag="ec"), args=["--flush-now"])
    if c != 5:
        bad.append(f"state-write={c}")
    shutil.rmtree(tmp, ignore_errors=True)
    # 2: 发送失败
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    _o, c = run(m, FakeTime(NOW0), mk(45, int(FakeTime(NOW0).time()) - 60, tag="e2"),
                fail_send=True, args=["--flush-now"])
    if c != 2:
        bad.append(f"send-fail={c}")
    shutil.rmtree(tmp, ignore_errors=True)
    # 6: queue 落盘失败 + 已发送
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    ft = FakeTime(NOW0)
    m.save_queue = lambda *a, **k: False
    _o, c = run(m, ft, mk(45, int(ft.time()) - 60, tag="e6"), args=["--flush-now"])
    if c != 6:
        bad.append(f"queue-write={c}")
    shutil.rmtree(tmp, ignore_errors=True)
    # 0: 锁被占 + --queue
    tmp = Path(tempfile.mkdtemp())
    m = load_monitor(tmp)
    import fcntl as _f
    fh = open(m.LOCK, "w")
    _f.flock(fh, _f.LOCK_EX | _f.LOCK_NB)
    _o, c = run(m, FakeTime(NOW0), [FILTERED], args=["--queue"])
    if c != 0:
        bad.append(f"lock-busy-queue={c}")
    fh.close()
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"  exitcodes: {'2/4/5/6/7/0 全部正确' if not bad else '错: ' + ', '.join(bad)}")
    return not bad



SCENARIOS = {
    "cadence": scenario_cadence,
    "noloss": scenario_noloss,
    "window": scenario_window,
    "cap": scenario_cap,
    "dedup": scenario_dedup_queue,
    "persist": scenario_persist,
    "divergence": scenario_divergence,
    "backoff": scenario_backoff,
    "overflow": scenario_overflow,
    "flushnow": scenario_flushnow_empty,
    "freeze": scenario_freeze_aging,
    "retry": scenario_retry_after_fail,
    "capfreeze": scenario_cap_freeze,
    "hold": scenario_hold,
    "unfreeze": scenario_unfreeze,
    "nofail": scenario_nofail_loop,
    "dirty": scenario_dirty_meta,
    "sleepwake": scenario_sleep_wake,
    "unfreezebig": scenario_unfreeze_big,
    "agingdrain": scenario_aging_drain_window,
    "overcap": scenario_flushnow_overcap_log,
    "rollback": scenario_clock_rollback,
    "exitcodes": scenario_exit_codes,
}

if __name__ == "__main__":
    picked = sys.argv[1:] or list(SCENARIOS)
    bad = []
    for name in picked:
        print(f"[{name}]")
        try:
            ok = SCENARIOS[name]()
        except Exception as e:                 # noqa: BLE001
            print(f"  EXC {type(e).__name__}: {e}")
            ok = False
        print(f"  {'PASS' if ok else 'FAIL'}")
        if not ok:
            bad.append(name)
    print(f"sim: {'PASS' if not bad else 'FAILED: ' + ', '.join(bad)}")
    sys.exit(0 if not bad else 1)


