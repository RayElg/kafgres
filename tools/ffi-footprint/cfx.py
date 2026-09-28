#!/usr/bin/env python3
"""C-side effects of the Postgres functions an extension links against.

Reads the server's LLVM bitcode, builds a call graph, and searches breadth-first from
each called function for effects: raises ERROR, FATAL or PANIC (PANIC(io) for an I/O
failure), file I/O, fsync, blocking, interrupt checks, or exit. Each effect carries its
shortest witness call chain and that chain's depth. Also reports the extension's
dynamic symbols: what it exports and imports.

Stdlib only. Needs `llvm-dis` and `nm`.

    cfx.py --bitcode DIR --lib EXT.so --postgres BIN --pg 16 --out effects.json
           [--symbols extra.txt] [--conf effects.json] [--indirect typed|none]
"""
import argparse
import concurrent.futures as cf
import heapq
import json
import os
import re
import shutil
import subprocess
import sys

DEFAULTS = {
    # Functions whose arg N is an elevel; the call site's constant decides the effect.
    "elevel_calls": {"errstart": 0, "errstart_cold": 0, "elog_finish": 0},
    # Functions returning an elevel: "arg0" means their first argument passes through.
    "elevel_fns": {"data_sync_elevel": ["arg0", "PANIC"]},
    # Elevels from these functions, or selects on these globals, come from an I/O failure.
    "io_elevel_fns": ["data_sync_elevel"],
    "io_elevel_globals": ["data_sync_retry"],
    # Fallback elevel numbers if calibration from errstart_cold fails.
    "elevels": {"13": {"ERROR": 20, "FATAL": 21, "PANIC": 22},
                "default": {"ERROR": 21, "FATAL": 22, "PANIC": 23}},
    "effects": {
        "ERROR": ["pg_re_throw", "ReThrowError"],
        "exit": ["proc_exit", "abort", "exit", "_exit", "pg_abort"],
        "fsync": ["fsync", "fdatasync", "sync_file_range", "msync", "syncfs"],
        "fd-io": ["pread", "pwrite", "preadv", "pwritev", "pread64", "pwrite64", "read", "write",
                  "open", "open64", "openat", "unlink", "rename", "ftruncate", "ftruncate64",
                  "fallocate", "fallocate64", "posix_fallocate", "posix_fallocate64", "lseek",
                  "lseek64", "mkdir", "rmdir", "opendir", "readdir", "link", "symlink", "fstat",
                  "stat", "lstat", "__xstat", "__fxstat", "statx"],
        "blocks": ["epoll_wait", "epoll_pwait", "poll", "ppoll", "select", "pselect", "nanosleep",
                   "usleep", "pg_usleep", "WaitLatch", "WaitLatchOrSocket", "WaitEventSetWait",
                   "LWLockAcquire", "LWLockWaitForVar", "LockAcquire", "LockAcquireExtended",
                   "ConditionVariableSleep", "ConditionVariableTimedSleep", "ProcSleep",
                   "pg_sleep", "sem_wait", "PGSemaphoreLock", "semop"],
        "interrupts": ["ProcessInterrupts"],
        "catalog": ["SearchSysCache", "SearchSysCache1", "SearchSysCache2", "SearchSysCache3",
                    "SearchSysCache4", "SearchCatCache", "systable_beginscan", "table_open",
                    "relation_open", "try_relation_open", "RelationIdGetRelation"],
        "alloc": ["palloc", "palloc0", "palloc_extended", "MemoryContextAlloc",
                  "MemoryContextAllocZero", "MemoryContextAllocExtended", "repalloc"],
    },
    # Modules/functions whose internals are not followed (else everything reports as everything).
    "sink_modules": ["utils/error/elog", "utils/error/assert"],
    "sink_functions": ["proc_exit", "abort", "exit", "_exit", "shmem_exit"],
    "max_indirect_targets": 12,
    # A field/global/param holding more functions than this is a dispatch point, reported by name.
    "max_field_targets": 64,
    # Global function pointers that only extensions set: reported as hooks.
    "hook_pattern": r"_hook(_\w+)?$|_hook_str$",
    # Calls that exist in the code but not in any process an extension runs in.
    "cut_edges": ["InitPostgres>StartupXLOG"],
}

SEVERITY = ["PANIC(io)", "PANIC", "FATAL", "ERROR", "error?", "exit", "fsync", "fd-io", "blocks",
            "interrupts", "catalog", "alloc", "indirect?"]

from irmodel import analyze_module, solve  # noqa: E402


def nm(path, *flags):
    out = subprocess.run(["nm", "-D", *flags, path], capture_output=True, text=True, check=True).stdout
    syms = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            name = parts[-1].split("@")[0]
            syms[name] = parts[-2]
    return syms


def bitcode_origin(path):
    marker = os.path.join(os.path.dirname(path.rstrip("/")), "VERSION")
    if os.path.exists(marker):
        with open(marker) as f:
            return f.read().strip()
    return "packaged JIT bitcode (-O2)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg", required=True)
    ap.add_argument("--lib-name", help="with --pg-config, locates <pkglibdir>/<name>.so")
    ap.add_argument("--pg-config", help="default: /usr/lib/postgresql/<pg>/bin/pg_config, then pg_config")
    ap.add_argument("--bitcode", help="default: <pkglibdir>/bitcode/postgres")
    ap.add_argument("--lib", help="default: <pkglibdir>/<lib-name>.so")
    ap.add_argument("--postgres", help="default: <bindir>/postgres")
    ap.add_argument("--out", required=True)
    ap.add_argument("--symbols", help="extra symbols to report, one per line")
    ap.add_argument("--conf", help="JSON overriding DEFAULTS keys")
    ap.add_argument("--indirect", choices=["typed", "none"], default="typed")
    ap.add_argument("--llvm-dis", default=None)
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 2)
    a = ap.parse_args()

    if not (a.bitcode and a.lib and a.postgres):
        pgc = a.pg_config or next((p for p in [f"/usr/lib/postgresql/{a.pg}/bin/pg_config"]
                                   if os.path.exists(p)), None) or shutil.which("pg_config")
        if not pgc:
            sys.exit("pg_config not found; pass --bitcode, --lib and --postgres")

        def q(flag):
            return subprocess.run([pgc, flag], capture_output=True, text=True, check=True).stdout.strip()

        pkglib, bindir = q("--pkglibdir"), q("--bindir")
        built = "/opt/ffi-bitcode/postgres"
        a.bitcode = a.bitcode or (built if os.path.isdir(built) else os.path.join(pkglib, "bitcode", "postgres"))
        a.postgres = a.postgres or os.path.join(bindir, "postgres")
        if not a.lib:
            if not a.lib_name:
                sys.exit("--lib or --lib-name is required")
            a.lib = os.path.join(pkglib, a.lib_name + ".so")
    for p in (a.bitcode, a.lib, a.postgres):
        if not os.path.exists(p):
            sys.exit(f"not found: {p}")

    conf = dict(DEFAULTS)
    if a.conf:
        with open(a.conf) as f:
            conf.update(json.load(f))
    llvm_dis = a.llvm_dis or shutil.which("llvm-dis") or next(
        (p for v in range(30, 10, -1) for p in [shutil.which(f"llvm-dis-{v}")] if p), None)
    if not llvm_dis:
        sys.exit("llvm-dis not found")

    lib_def = nm(a.lib, "--defined-only")
    lib_undef = nm(a.lib, "--undefined-only")
    pg_def = nm(a.postgres, "--defined-only")
    exports = sorted(n for n, t in lib_def.items() if t in "TW")
    imports_fn = sorted(n for n in lib_undef if pg_def.get(n, "") in ("T", "W", "t"))
    imports_data = sorted(n for n in lib_undef if pg_def.get(n, "") in ("D", "B", "R", "V", "d", "b"))
    other_undef = sorted(n for n in lib_undef if n not in pg_def)

    bcs = []
    for root, _, files in os.walk(a.bitcode):
        for f in files:
            if f.endswith(".bc"):
                p = os.path.join(root, f)
                bcs.append((p, os.path.relpath(p, a.bitcode)[:-3], llvm_dis, conf))
    bcs.sort()
    graph, taken_all, errors = {}, set(), []
    fnsets, copies, members = {}, set(), {}
    with cf.ProcessPoolExecutor(max_workers=a.jobs) as ex:
        for res in ex.map(analyze_module, bcs, chunksize=4):
            if "error" in res:
                errors.append(f"{res['rel']}: {res['error']}")
                continue
            for k, d in res["defs"].items():
                d["module"] = res["rel"]
                if k in graph:
                    g = graph[k]
                    g["calls"] = sorted(set(g["calls"]) | set(d["calls"]))
                    g["indirect"] += d["indirect"]
                    for c, l in d["call_locs"].items():
                        g["call_locs"].setdefault(c, l)
                    for e, det in d["local"].items():
                        g["local"].setdefault(e, det)
                else:
                    graph[k] = d
            taken_all.update(res["taken"])
            for t, fns in res["fnsets"].items():
                fnsets.setdefault(t, set()).update(fns)
            copies.update(res["copies"])
            members.update(res["members"])

    held = solve(fnsets, copies)

    def label(t):
        if t[0] == "field":
            ty = t[1].split("::")[-1]
            short = ty.split(".", 1)[1] if "." in ty else ty
            names = members.get(t[1])
            return f"{short}.{names[t[2]]}" if names and t[2] < len(names) else f"{short}#{t[2]}"
        if t[0] == "global":
            return t[1].split("::")[-1]
        if t[0] == "param":
            return f"argument {t[2] + 1} of {t[1].split('::')[-1]}"
        return "unknown"

    # Calibrate from errstart_cold literals. The most common is ERROR; -O0 keeps dead
    # ternary arms that pass lower levels, so the minimum is not.
    levels = conf["elevels"].get(str(a.pg), conf["elevels"]["default"])
    cold = [int(e.split(":")[1]) for d in graph.values() for e, det in d["local"].items()
            if e.startswith("elevel:") and det[0] == "errstart_cold" and e.split(":")[1].lstrip("-").isdigit()]
    calibrated = None
    if cold:
        base = max(set(cold), key=cold.count)
        calibrated = {"ERROR": base, "FATAL": base + 1, "PANIC": base + 2}
        if calibrated != levels:
            print(f"note: elevels calibrated from errstart_cold: {calibrated} (table had {levels})", file=sys.stderr)
        levels = calibrated
    by_num = {v: k for k, v in levels.items()}

    sink_mods = tuple(conf["sink_modules"])
    sinks = set(conf["sink_functions"])
    for k, d in graph.items():
        local = {}
        for e, det in d["local"].items():
            det, loc = det if isinstance(det, list) else (det, None)
            at = f" at {loc}" if loc else ""
            if e.startswith("elevel:"):
                parts = e.split(":")
                lv, io = parts[1], len(parts) > 2
                site = det.rstrip("*")
                if lv == "dynamic":
                    local.setdefault("error?", f"{site}(elevel not constant){at}")
                    continue
                if lv in levels:
                    name = lv
                elif lv.lstrip("-").isdigit() and int(lv) >= levels["ERROR"]:
                    name = by_num.get(int(lv), "PANIC")
                else:
                    continue
                if io and name == "PANIC":
                    local.setdefault("PANIC(io)", f"{site}(PANIC) via data_sync_elevel{at}")
                else:
                    local.setdefault(name, f"{site}({name}){at}")
            else:
                local[e] = det + at
        d["sink"] = d["module"].startswith(sink_mods) or d["name"] in sinks
        # A sink contributes only what callers attach at the call site, never its internals.
        d["local"] = {} if d["sink"] else local

    # Leaf nodes for functions the server calls but does not define.
    for d in list(graph.values()):
        for c in d["calls"]:
            if c not in graph:
                graph[c] = {"name": c.split("::")[-1], "sig": "", "calls": [], "indirect": [],
                            "call_locs": {}, "local": {}, "module": "<external>", "sink": c in sinks}
                for eff, names in conf["effects"].items():
                    if graph[c]["name"] in names:
                        graph[c]["local"][eff] = "external"

    by_sig = {}
    for k in taken_all:
        if k in graph and graph[k]["sig"]:
            by_sig.setdefault(graph[k]["sig"], []).append(k)
    stats = {"functions": len(graph), "address_taken": len(taken_all), "indirect_sites": 0,
             "indirect_by_field": 0, "indirect_dispatch": 0, "indirect_hooks": 0, "indirect_by_signature": 0,
             "indirect_unresolved": 0}
    cut = {}
    for pair in conf["cut_edges"]:
        caller, callee = pair.split(">", 1)
        cut.setdefault(caller.strip(), set()).add(callee.strip())
    max_field = conf["max_field_targets"]
    dispatch, unresolved = {}, {}
    edges = {}
    for k, d in graph.items():
        if d["sink"]:
            edges[k] = []
            continue
        out = set(d["calls"])
        for site in d["indirect"]:
            stats["indirect_sites"] += 1
            terms = [tuple(t) for t in site["terms"]]
            targets = set()
            for t in terms:
                if t[0] == "fn":
                    targets.add(t[1])
                elif t[0] in ("field", "global", "param"):
                    targets |= held.get(t, set())
            targets = {t for t in targets if t in graph}
            via = ", ".join(sorted({label(t) for t in terms if t[0] != "fn"})) or "a local"
            at = f" at {site['loc']}" if site.get("loc") else ""
            if targets and len(targets) <= max_field:
                stats["indirect_by_field"] += 1
                for t in targets:
                    out.add(t)
                    if site.get("loc"):
                        d["call_locs"].setdefault(t, site["loc"])
                continue
            if len(targets) > max_field:
                stats["indirect_dispatch"] += 1
                dispatch[via] = len(targets)
                d["local"].setdefault("indirect?", f"dispatch through {via} ({len(targets)} targets, not followed){at}")
                continue
            hooks = [t for t in terms if t[0] == "global" and re.search(conf["hook_pattern"], t[1])]
            if hooks and not targets:
                stats["indirect_hooks"] += 1
                d["local"].setdefault("indirect?", f"extension hook {label(hooks[0])} (set by extensions, not core){at}")
                continue
            cands = by_sig.get(site["sig"], []) if a.indirect == "typed" else []
            if 0 < len(cands) <= conf["max_indirect_targets"]:
                out.update(cands)
                stats["indirect_by_signature"] += 1
            else:
                stats["indirect_unresolved"] += 1
                unresolved[via] = unresolved.get(via, 0) + 1
                d["local"].setdefault("indirect?", f"indirect call through {via} {site['sig']} ({len(cands)} signature matches){at}")
        edges[k] = sorted(out - cut.get(d["name"], set()))
    stats["indirect_resolved"] = stats["indirect_by_field"] + stats["indirect_by_signature"]
    stats["dispatch_points"] = dict(sorted(dispatch.items(), key=lambda x: -x[1])[:20])
    stats["unresolved_through"] = dict(sorted(unresolved.items(), key=lambda x: -x[1])[:30])

    # Cheapest path per effect: crossing modules costs a hop, in-module calls are free,
    # depth breaks ties. Measures distance in subsystems, so -O0/-O2 bitcode rank alike.
    def effects_of(root):
        dist, parent = {root: (0, 0)}, {root: None}
        heap, found = [(0, 0, root)], {}
        while heap:
            h, dp, k = heapq.heappop(heap)
            if dist.get(k) != (h, dp):
                continue
            for e, det in graph[k]["local"].items():
                if e not in found:
                    found[e] = (k, h, dp, det)
            km = graph[k]["module"]
            for c in edges.get(k, ()):
                cost = (h + (graph[c]["module"] != km), dp + 1)
                if c not in dist or cost < dist[c]:
                    dist[c], parent[c] = cost, k
                    heapq.heappush(heap, (cost[0], cost[1], c))
        out = {}
        for e, (k, h, dp, det) in sorted(found.items(), key=lambda x: (SEVERITY.index(x[0]) if x[0] in SEVERITY else 99)):
            path = []
            while k is not None:
                path.append(k)
                k = parent[k]
            path.reverse()
            at = [graph[path[i]]["call_locs"].get(path[i + 1]) for i in range(len(path) - 1)]
            out[e] = {"depth": dp, "hops": h, "path": [p.split("::")[-1] for p in path], "at": at,
                      "detail": det, "module": graph[path[-1]]["module"]}
        return out, len(dist)

    wanted = set(imports_fn)
    if a.symbols:
        with open(a.symbols) as f:
            wanted |= {l.strip() for l in f if l.strip()}
    report = {}
    for sym in sorted(wanted):
        if sym not in graph or graph[sym]["module"] == "<external>":
            report[sym] = {"defined": False}
            continue
        effs, reach = effects_of(sym)
        report[sym] = {"defined": True, "module": graph[sym]["module"], "reach": reach,
                       "effects": effs}

    out = {
        "tool": "ffi-footprint cfx", "pg": str(a.pg), "llvm_dis": llvm_dis,
        "bitcode": a.bitcode, "bitcode_origin": bitcode_origin(a.bitcode),
        "elevels": levels, "elevels_calibrated": calibrated is not None,
        "exports": exports, "imports": imports_fn, "imports_data": imports_data,
        "undefined_non_postgres": other_undef, "stats": stats, "errors": errors[:20],
        "symbols": report,
    }
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    print(f"cfx: pg{a.pg}: {stats['functions']} functions ({bitcode_origin(a.bitcode)}), "
          f"{len(imports_fn)} imports; indirect calls {stats['indirect_sites']}: "
          f"{stats['indirect_by_field']} by field, {stats['indirect_dispatch']} dispatch, {stats['indirect_hooks']} hooks, "
          f"{stats['indirect_by_signature']} by signature, {stats['indirect_unresolved']} unresolved; "
          f"{len(errors)} module errors", file=sys.stderr)


if __name__ == "__main__":
    main()
