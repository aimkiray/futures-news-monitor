#!/usr/bin/env bash
# 部署 monitor.py 到生产位置: 校验 selftest -> 备份 -> 同目录原子替换 -> md5 复核
# 用法: ./deploy.sh [--with-tests]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC="$ROOT/monitor.py"
DST="$HOME/.pi/agent/bin/futures-news-monitor.py"
PY="${PY:-python3}"
command -v "$PY" >/dev/null || { echo "找不到 python3: $PY" >&2; exit 1; }
PY="$(command -v "$PY")"

[ -x "$PY" ] || { echo "找不到 python: $PY" >&2; exit 1; }

if [ "${1:-}" = "--with-tests" ]; then
  "$PY" "$ROOT/tests/sim.py"    >/dev/null || { echo "sim 失败, 拒绝部署" >&2; exit 1; }
  "$PY" "$ROOT/tests/probe2.py" >/dev/null || { echo "probe2 失败, 拒绝部署" >&2; exit 1; }
  "$PY" "$ROOT/tests/probe4.py" >/dev/null || { echo "probe4 失败, 拒绝部署" >&2; exit 1; }
  echo "regression: sim/probe2/probe4 PASS"
fi

"$PY" "$SRC" --selftest >/dev/null || { echo "selftest 失败, 拒绝部署" >&2; exit 1; }

if [ -f "$DST" ] && [ "$(md5 -q "$SRC")" = "$(md5 -q "$DST")" ]; then
  echo "生产已是同一版本 ($(md5 -q "$DST")), 无需部署"; exit 0
fi

BAK="/tmp/fnm-$(date +%Y%m%d-%H%M%S).bak"
[ -f "$DST" ] && cp -p "$DST" "$BAK"
cp "$SRC" "$DST.new"
mv "$DST.new" "$DST"                       # 同目录 mv = 原子替换, 不会出现半个文件

if [ "$(md5 -q "$SRC")" != "$(md5 -q "$DST")" ]; then
  echo "md5 不一致, 回滚" >&2
  [ -n "${BAK:-}" ] && cp -p "$BAK" "$DST"
  exit 1
fi
"$PY" "$DST" --selftest >/dev/null || { echo "部署后 selftest 失败, 回滚" >&2; cp -p "$BAK" "$DST"; exit 1; }
echo "已部署 $(md5 -q "$DST")"
echo "备份    $BAK"
echo "回滚    cp -p $BAK $DST"
