#!/usr/bin/env python3
"""merge_research_reports.py — U2 merge, spec §7 (cto-flow v1.3.1).

Batch: research-reports-2026-11-07.

Steps (spec §7.1):
  1. read research-raw.json -> T = {record_key | tight == true}
  2. glob analysis\\chunk-*.json (excluding *-keys.json / *-input.json),
     validate each chunk against the §6.3 schema
  3. coverage check: union of all chunk infoCode == T, no duplicates
     (mismatch -> nonzero exit, print diff sets, write nothing)
  4. merge reports sorted by publishDate desc, record_key asc (tie-break);
     empty publishDate sorts last; atomic write to project root
  5. print counts summary; exit 0

Pure standard library; no `import monitor`; Windows/posix safe.
"""

import hashlib
import json
import os
import platform
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- [canonical] C-1 path constants -----------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
ARTIFACT_ROOT = Path(r"C:\Users\laevatain\artifacts\research-reports-2026-11-07")
RAW_PATH = ARTIFACT_ROOT / "research-raw.json"
ANALYSIS_DIR = ARTIFACT_ROOT / "analysis"
OUT_PATH = PROJECT_ROOT / "research-reports.json"
MONITOR_PATH = PROJECT_ROOT / "monitor.py"
PDFTOTEXT = r"C:\Users\laevatain\scoop\apps\git\2.51.0\mingw64\bin\pdftotext.exe"

CST = timezone(timedelta(hours=8))
CHUNK_NAME_RE = re.compile(r"^chunk-(\d{3})\.json$")

# raw item fields dropped on merge (spec §7.1)
DROP_FIELDS = {"filter_reason", "keep", "tight", "extract_attempts"}

# §6.3 analyses[] field contract (closed set; `notes` optional)
ANALYSIS_REQUIRED = {
    "infoCode", "commodities", "direction", "confidence",
    "summary", "key_points", "data_points", "basis",
}
ANALYSIS_ALLOWED = ANALYSIS_REQUIRED | {"notes"}
DIRECTIONS = {"看多", "看空", "中性", "数据类"}
BASES = {"extract", "metadata"}

MAX_ERRORS_PRINTED = 60


def _is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _is_str_n(x, lo, hi):
    return isinstance(x, str) and lo <= len(x) <= hi


def _is_str_list(x, max_len, lo, hi):
    return (
        isinstance(x, list)
        and len(x) <= max_len
        and all(_is_str_n(s, lo, hi) for s in x)
    )


def validate_chunk(path, raw_by_key):
    """Validate one chunk file against §6.3. Returns (chunk_dict_or_None, errors)."""
    errs = []
    name = path.name
    nnn = CHUNK_NAME_RE.match(name).group(1)
    try:
        blob = path.read_bytes()
        d = json.loads(blob.decode("utf-8"))
    except Exception as e:  # noqa: BLE001 - report any parse/IO failure
        return None, [f"{name}: cannot parse JSON: {e}"]
    if b"\r\n" in blob:  # spec §2.2: all JSON uses \n newlines (R2-F1 lint)
        errs.append(f"{name}: file uses CRLF line endings (spec §2.2 requires LF)")
    if not isinstance(d, dict):
        return None, errs + [f"{name}: top level is not a JSON object"]
    if d.get("schema_version") != "1.0":
        errs.append(f"{name}: schema_version != '1.0' (got {d.get('schema_version')!r})")
    if d.get("chunk") != nnn:
        errs.append(f"{name}: chunk field {d.get('chunk')!r} != filename {nnn!r}")
    analyses = d.get("analyses")
    if not isinstance(analyses, list):
        return None, errs + [f"{name}: 'analyses' is not a list"]

    for i, a in enumerate(analyses):
        loc = f"{name} analyses[{i}]"
        if not isinstance(a, dict):
            errs.append(f"{loc}: not a JSON object")
            continue
        keys = set(a)
        extra = keys - ANALYSIS_ALLOWED
        missing = ANALYSIS_REQUIRED - keys
        if extra:
            errs.append(f"{loc}: extra fields {sorted(extra)}")
        if missing:
            errs.append(f"{loc}: missing fields {sorted(missing)}")
            continue
        ic = a["infoCode"]
        if not (isinstance(ic, str) and ic):
            errs.append(f"{loc}: infoCode must be a non-empty string")
        if not _is_str_list(a["commodities"], 10, 1, 30):
            errs.append(f"{loc}: commodities must be a list of <=10 strings (1-30 chars)")
        if not isinstance(a["direction"], str) or a["direction"] not in DIRECTIONS:
            errs.append(f"{loc}: direction {a['direction']!r} not in {sorted(DIRECTIONS)}")
        cf = a["confidence"]
        if not (_is_num(cf) and 0.0 <= cf <= 1.0):
            errs.append(f"{loc}: confidence {cf!r} not a number in [0.0, 1.0]")
        if not _is_str_n(a["summary"], 1, 80):
            errs.append(f"{loc}: summary must be a string of 1-80 chars")
        if not _is_str_list(a["key_points"], 6, 1, 60):
            errs.append(f"{loc}: key_points must be a list of <=6 strings (1-60 chars)")
        if not _is_str_list(a["data_points"], 8, 1, 80):
            errs.append(f"{loc}: data_points must be a list of <=8 strings (1-80 chars)")
        b = a["basis"]
        if not isinstance(b, str) or b not in BASES:
            errs.append(f"{loc}: basis {b!r} not in {sorted(BASES)}")
        else:
            if b == "metadata" and _is_num(cf) and cf > 0.5:
                errs.append(f"{loc}: basis='metadata' but confidence {cf} > 0.5")
            # §6.3/§6.2 linkage vs the raw record: 'extract' allowed only when
            # the cleaned text is non-empty; 'metadata' mandatory otherwise.
            it = raw_by_key.get(ic) if isinstance(ic, str) else None
            if it is not None:
                has_text = it.get("extract_status") == "ok" and bool(it.get("extract"))
                if b == "extract" and not has_text:
                    errs.append(f"{loc}: basis='extract' but raw extract is empty")
                if b == "metadata" and has_text:
                    errs.append(f"{loc}: basis='metadata' but raw extract is non-empty")
        if "notes" in a and not _is_str_n(a["notes"], 0, 200):
            errs.append(f"{loc}: notes must be a string of <=200 chars")
    return d, errs


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001 - non-UTF8 console is not fatal
        pass

    # -- step 1: load raw, build T and record_key -> item map ------------------
    try:
        raw = json.loads(RAW_PATH.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"FATAL: cannot read {RAW_PATH}: {e}", file=sys.stderr)
        return 1
    items = raw.get("items")
    if not isinstance(items, list):
        print(f"FATAL: {RAW_PATH} has no 'items' list", file=sys.stderr)
        return 1
    try:
        tight_items = [it for it in items if isinstance(it, dict) and it.get("tight") is True]
        T = {it["record_key"] for it in tight_items}
        raw_by_key = {it["record_key"]: it for it in items if isinstance(it, dict)}
    except Exception as e:  # noqa: BLE001 - malformed raw -> FATAL, not traceback
        print(f"FATAL: malformed items in {RAW_PATH}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    # -- step 2: glob + §6.3 schema validation ---------------------------------
    chunk_paths = sorted(
        p for p in ANALYSIS_DIR.glob("chunk-*.json") if CHUNK_NAME_RE.match(p.name)
    )
    if not chunk_paths:
        print(f"FATAL: no chunk-NNN.json files under {ANALYSIS_DIR}", file=sys.stderr)
        return 1

    errors = []
    chunks = []  # (path, parsed dict)
    analysis_by_key = {}  # infoCode -> chunk record
    dup = []  # (infoCode, first_file, second_file)
    for p in chunk_paths:
        d, errs = validate_chunk(p, raw_by_key)
        errors.extend(errs)
        if d is None:
            continue
        chunks.append((p, d))
        for a in d["analyses"]:
            if not isinstance(a, dict):
                continue                    # 非对象元素已被 validate_chunk 记 error, 不参与覆盖/查重
            ic = a.get("infoCode")
            if not isinstance(ic, str):
                continue
            if ic in analysis_by_key:
                dup.append((ic, analysis_by_key[ic][0], p.name))
            else:
                analysis_by_key[ic] = (p.name, a)

    # -- step 3: coverage check -------------------------------------------------
    union = set(analysis_by_key)
    missing = sorted(T - union)  # tight keys with no analysis
    surplus = sorted(union - T)  # analyzed keys outside the tight set
    if errors or missing or surplus or dup:
        if errors:
            print(f"SCHEMA ERRORS ({len(errors)}):", file=sys.stderr)
            for e in errors[:MAX_ERRORS_PRINTED]:
                print(f"  {e}", file=sys.stderr)
            if len(errors) > MAX_ERRORS_PRINTED:
                print(f"  ... and {len(errors) - MAX_ERRORS_PRINTED} more", file=sys.stderr)
        if missing:
            print(f"COVERAGE: {len(missing)} tight record_key(s) missing analysis:", file=sys.stderr)
            for k in missing[:MAX_ERRORS_PRINTED]:
                print(f"  {k}", file=sys.stderr)
        if surplus:
            print(f"COVERAGE: {len(surplus)} analyzed infoCode(s) not in tight set:", file=sys.stderr)
            for k in surplus[:MAX_ERRORS_PRINTED]:
                print(f"  {k}", file=sys.stderr)
        if dup:
            print(f"COVERAGE: {len(dup)} duplicate infoCode(s) across chunks:", file=sys.stderr)
            for k, f1, f2 in dup[:MAX_ERRORS_PRINTED]:
                print(f"  {k} in {f1} and {f2}", file=sys.stderr)
        print("merge aborted; no output written", file=sys.stderr)
        return 1

    # -- step 4: merge + sort ---------------------------------------------------
    reports = []
    for it in tight_items:
        rep = {k: v for k, v in it.items() if k not in DROP_FIELDS}
        rep["analysis"] = {
            k: v for k, v in analysis_by_key[it["record_key"]][1].items() if k != "infoCode"
        }
        reports.append(rep)
    # publishDate desc (API raw string, lexicographic; "" = minimum -> last),
    # record_key asc tie-break via stable two-pass sort.
    reports.sort(key=lambda r: r["record_key"])
    reports.sort(key=lambda r: r.get("publishDate") or "", reverse=True)

    try:
        monitor_sha256 = hashlib.sha256(MONITOR_PATH.read_bytes()).hexdigest()
        scanned = raw["counts"]["unique_items"]
    except Exception as e:  # noqa: BLE001 - missing monitor.py / counts -> FATAL
        print(f"FATAL: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    counts = {
        "scanned": scanned,
        "kept": sum(1 for it in items if isinstance(it, dict) and it.get("keep") is True),
        "tight": len(reports),
        "pdf_ok": sum(1 for r in reports if r.get("extract_status") == "ok"),
        "pdf_no_text": sum(1 for r in reports if r.get("extract_status") == "no_text"),
        "pdf_failed": sum(1 for r in reports if r.get("extract_status") == "failed"),
        "analyzed": len(reports),
        "basis_extract": sum(1 for r in reports if r["analysis"]["basis"] == "extract"),
        "basis_metadata": sum(1 for r in reports if r["analysis"]["basis"] == "metadata"),
    }
    # §7.2 identities are invariants by construction; verify defensively.
    assert counts["analyzed"] == len(reports) == counts["tight"]
    assert counts["basis_extract"] + counts["basis_metadata"] == counts["analyzed"]

    out = {
        "schema_version": "1.0",
        "meta": {
            "generated_at": datetime.now(CST).isoformat(timespec="seconds"),
            "window": raw.get("window") or {"begin": "2025-11-07", "end": "2026-11-07"},
            "source": {
                "api": "https://reportapi.eastmoney.com/report/list",
                "referer": "https://data.eastmoney.com/report/",
                "qtypes": [1, 2],
            },
            "counts": counts,
            "chunks": [
                {"file": f"analysis/{p.name}", "items": len(d["analyses"])}
                for p, d in chunks
            ],
            "env": {
                "python": platform.python_version(),
                "pdftotext": PDFTOTEXT,
                "monitor_sha256": monitor_sha256,
            },
        },
        "reports": reports,
    }

    # -- atomic write (tmp + os.replace; UTF-8, ensure_ascii=False, \n) ---------
    tmp_path = OUT_PATH.with_name(OUT_PATH.name + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(out, f, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp_path, OUT_PATH)
    except Exception as e:  # noqa: BLE001
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass
        print(f"FATAL: cannot write {OUT_PATH}: {e}", file=sys.stderr)
        return 1

    # -- step 5: counts summary --------------------------------------------------
    print(f"wrote {OUT_PATH}")
    print(f"chunks = {len(chunks)}")
    for k, v in counts.items():
        print(f"{k} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
