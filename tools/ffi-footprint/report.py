#!/usr/bin/env python3
"""Joins scanner facts with cfx effects into the FFI footprint report: entry points,
reach, FFI sites with server-side effects, unsafe blocks, findings, baseline diff.
Stdlib only.

    report.py --config merged.json --facts facts-16.json --cfx cfx-16.json [...pairs]
              --md report.md --json report.json [--baseline old.json]
"""
import argparse
import collections
import fnmatch
import json
import re
import sys

SEV_EFFECTS = ["PANIC(io)", "PANIC", "FATAL", "ERROR", "error?", "exit", "fsync", "fd-io", "blocks",
               "interrupts", "catalog", "alloc", "indirect?"]
RAISING = ("PANIC(io)", "PANIC", "FATAL", "ERROR", "error?")


def indirect_breakdown(st):
    if "indirect_by_field" not in st:
        return f"{st['indirect_resolved']} resolved by signature, {st['indirect_unresolved']} not"
    return (f"{st['indirect_by_field']} resolved by struct field, {st['indirect_dispatch']} dispatch points "
            f"(named, not followed), {st['indirect_hooks']} extension hooks, {st['indirect_by_signature']} "
            f"by signature, {st['indirect_unresolved']} unresolved")


def hops(v, sep=" → "):
    """A witness chain with each hop's call site, where the bitcode has debug info."""
    at = v.get("at") or []
    out = []
    for i, name in enumerate(v["path"]):
        loc = at[i] if i < len(at) and at[i] else None
        out.append(f"{name} ({loc})" if loc else name)
    return sep.join(out)


def near(v, limit):
    """Direct within `limit` module crossings; files without hops fall back to call depth."""
    return v.get("hops", v["depth"]) <= limit


def ekey(e):
    return SEV_EFFECTS.index(e) if e in SEV_EFFECTS else 99


class Crate:
    """One scanner run: functions, call resolution, handler inference, entries."""

    def __init__(self, facts, conf):
        self.conf = conf
        self.fns = {f["id"]: f for f in facts["functions"]}
        self.by_name = collections.defaultdict(list)
        for f in facts["functions"]:
            if f["kind"] != "closure-entry":
                self.by_name[f["name"]].append(f)
        self.common = set(conf["report"]["common_methods"])
        self.roots = conf["handlers"]["roots"]
        self.pgrx_api = conf.get("pgrx_api", {})
        self._resolve_all()
        self._infer_wrappers()
        self._mark_handled()

    # ------------------------------------------------------------ resolution

    def resolve(self, call, f):
        if call.get("ffi"):
            return []
        path = call.get("path")
        if path:
            segs = [s for s in path if s not in ("crate", "self", "super")]
            if not segs:
                return []
            cands = self.by_name.get(segs[-1], [])
            if len(segs) == 1:
                free = [c for c in cands if c["kind"] == "fn"]
                same = [c for c in free if c["module"] == f.get("module")]
                return same or free
            qual = segs[-2]
            if qual == "Self":
                return [c for c in cands if c.get("self_ty") == f.get("self_ty")]
            out = [c for c in cands if c.get("self_ty") == qual
                   or (c["kind"] == "fn" and c["module"].split("::")[-1] == qual)]
            return out
        m = call.get("method")
        if m:
            cands = [c for c in self.by_name.get(m, []) if c["kind"] in ("method", "trait-default")]
            if call.get("recv_self"):
                mine = [c for c in cands if c.get("self_ty") == f.get("self_ty")]
                if mine:
                    return mine
            if m in self.common:
                return []
            return cands
        return []

    def _resolve_all(self):
        for f in self.fns.values():
            for c in f["calls"]:
                c["_targets"] = [t["id"] for t in self.resolve(c, f)]

    def handler_positions(self, call):
        path = call.get("path") or []
        joined = "::".join(path)
        for root, pos in self.roots.items():
            if joined == root or joined.endswith("::" + root) or (len(path) == 1 and root == path[0]):
                return set(pos)
        out = set()
        for t in call.get("_targets", []):
            out |= set(self.fns[t].get("_wrapper", ()))
        return out

    def _infer_wrappers(self):
        changed = True
        while changed:
            changed = False
            for f in self.fns.values():
                params = f.get("params") or []
                if not params:
                    continue
                have = set(f.get("_wrapper", ()))
                for c in f["calls"]:
                    for h in self.handler_positions(c):
                        args = c.get("args") or []
                        if h >= len(args):
                            continue
                        idents = set(args[h]["idents"])
                        for i, p in enumerate(params):
                            if p in idents and i not in have:
                                have.add(i)
                                changed = True
                if have:
                    f["_wrapper"] = sorted(have)

    def handled(self, f, rec):
        for cid, aidx in rec.get("parents") or []:
            if aidx in self.handler_positions(f["calls"][cid]):
                return True
        return False

    def _mark_handled(self):
        for f in self.fns.values():
            for kind in ("calls", "panics", "raises"):
                for r in f[kind]:
                    r["_handled"] = self.handled(f, r)

    # ------------------------------------------------------------ entries

    def entries(self):
        conf = self.conf["entries"]
        registered = {}
        for f in self.fns.values():
            for c in f["calls"]:
                name = (c.get("method") or (c.get("path") or [""])[-1])
                kind = conf["by_string"].get(name)
                if kind:
                    for s in c.get("str_args", []):
                        registered[s] = (kind, f"{f['file']}:{c['line']}")
        out = []
        for f in self.fns.values():
            attrs = set(f.get("attrs") or [])
            exported = bool({"no_mangle", "export_name"} & attrs) or f["name"] in conf["names"]
            sym = f.get("export_name") or f["name"]
            kind, why = None, ""
            if "pg_extern" in attrs:
                kind, why = "sql-function", "#[pg_extern]"
            elif f["name"] in conf["names"] and f.get("abi"):
                kind, why = conf["names"][f["name"]], "fixed symbol name"
            elif sym in registered and f.get("abi"):
                kind, why = registered[sym][0], f"registered by name at {registered[sym][1]}"
            elif exported and f.get("abi"):
                kind, why = "exported", "exported symbol"
            elif f["kind"] == "closure-entry":
                args = " ".join(f.get("reg_args") or [])
                for rule in conf["closures"]:
                    if re.search(rule["match"], args):
                        kind = rule["kind"]
                        break
                why = f"closure passed to {f.get('reg_call')}({args})"
            if kind:
                out.append({"id": f["id"], "kind": kind, "why": why, "symbol": sym if f.get("abi") else None})
        # Function pointers escaping to C.
        seen = {e["id"] for e in out}
        for f in self.fns.values():
            for c in f["calls"]:
                if not c.get("ref"):
                    continue
                for t in c["_targets"]:
                    tf = self.fns[t]
                    if not tf.get("abi") or t in seen:
                        continue
                    kind = "callback"
                    for rule in conf["escapes"]:
                        if re.search(rule["match"], c.get("ctx", "")):
                            kind = rule["kind"]
                            break
                    out.append({"id": t, "kind": kind, "why": f"pointer escapes at {f['file']}:{c['line']}: {c.get('ctx', '')}",
                                "symbol": None})
                    seen.add(t)
        for e in out:
            f = self.fns[e["id"]]
            attrs = set(f.get("attrs") or [])
            e["guarded"] = "pg_guard" in attrs or "pg_extern" in attrs or f["kind"] == "closure-entry"
            e["abi"] = f.get("abi")
            e["file"], e["line"] = f["file"], f["line"]
        return sorted(out, key=lambda e: (e["kind"], e["id"]))

    def reach(self, entry_id):
        """Nodes reachable from an entry: {id: handled}. Handled means every path to the
        node passes through a handler position."""
        best = {entry_id: False}
        work = [(entry_id, False)]
        while work:
            n, h = work.pop()
            if best.get(n) is not None and best[n] is False and h:
                continue
            for c in self.fns[n]["calls"]:
                if c.get("registers_closure"):
                    continue
                if c.get("ref") and any(self.fns[t].get("abi") for t in c["_targets"]):
                    continue
                hh = h or c["_handled"]
                for t in c["_targets"]:
                    prev = best.get(t)
                    if prev is None or (prev and not hh):
                        best[t] = hh
                        work.append((t, hh))
        return best


def load(path):
    with open(path) as f:
        return json.load(f)


def site_effects(cfx, syms):
    """Worst-first effects of a set of C symbols, each with its shallowest witness."""
    merged = {}
    for s in syms:
        info = cfx["symbols"].get(s)
        if not info or not info.get("defined"):
            continue
        for e, v in info["effects"].items():
            if e not in merged or v["depth"] < merged[e]["depth"]:
                merged[e] = dict(v, via=s)
    return dict(sorted(merged.items(), key=lambda x: ekey(x[0])))


def fmt_eff(effs, depth_limit, only=None, limit=6):
    parts = []
    for e, v in effs.items():
        if only and e not in only:
            continue
        mark = "" if near(v, depth_limit) else "~"
        parts.append(f"{mark}{e}@{v['depth']}")
    return ", ".join(parts[:limit]) + (" …" if len(parts) > limit else "")


def analyze(pg, facts, cfx, conf):
    crate = Crate(facts, conf)
    rep = conf["report"]
    dl = rep["direct_hops"]
    entries = crate.entries()
    fns = crate.fns
    kinds = conf["entry_kinds"]

    # FFI and pgrx-API sites.
    sites = []
    for f in fns.values():
        for c in f["calls"]:
            syms, via = None, None
            if c.get("ffi"):
                syms, via = [c["ffi"]], c.get("via", "ffi")
            else:
                joined = "::".join(c.get("path") or [])
                for k, v in crate.pgrx_api.items():
                    if joined == k or joined.endswith("::" + k):
                        syms, via = v, "pgrx:" + k
                        break
            if syms:
                sites.append({"fn": f["id"], "file": f["file"], "line": c["line"], "syms": syms,
                              "via": via, "handled": c["_handled"],
                              "in_unsafe": c.get("unsafe") is not None,
                              "effects": site_effects(cfx, syms)})

    # Reachability from each entry.
    entry_reach = {}
    for e in entries:
        entry_reach[e["id"]] = crate.reach(e["id"])
    reached_by = collections.defaultdict(list)
    for eid, r in entry_reach.items():
        for n, h in r.items():
            reached_by[n].append((eid, h))

    def exposure(fn_id, local_handled):
        """Entries reaching a site unhandled: [(entry, severity)]."""
        out = []
        for eid, h in reached_by.get(fn_id, []):
            if not h and not local_handled:
                out.append(eid)
        return out

    for s in sites:
        s["entries_unhandled"] = exposure(s["fn"], s["handled"])
        s["entries_all"] = [eid for eid, _ in reached_by.get(s["fn"], [])]
        # Unhandled: some entry reaches it outside every handler, or no entry reaches it.
        s["exposed"] = not s["handled"] and (bool(s["entries_unhandled"]) or not s["entries_all"])

    findings = []

    by_key = {}

    def add(kind, key, sev, msg, where):
        k = f"{kind}:{key}"
        if k in by_key:
            by_key[k]["count"] += 1
            if where and where not in by_key[k]["sites"]:
                by_key[k]["sites"].append(where)
            return
        by_key[k] = {"kind": kind, "key": k, "severity": sev, "message": msg, "where": where, "count": 1,
                     "sites": [where] if where else []}
        findings.append(by_key[k])

    # Entries.
    for e in entries:
        if e["abi"] and not e["guarded"]:
            add("unguarded-extern", e["id"], "high",
                f"`{e['id']}` is called from C ({e['kind']}) without #[pg_guard]: a panic or ERROR "
                f"crosses the FFI boundary", f"{e['file']}:{e['line']}")
        if e["abi"] == "C" and e["guarded"] and e["kind"] != "sql-function":
            add("c-abi-entry", e["id"], "low", f"`{e['id']}` uses extern \"C\"; pgrx 0.16 guards expect \"C-unwind\"",
                f"{e['file']}:{e['line']}")

    # Unsafe blocks.
    unsafe_rows = []
    for f in fns.values():
        n = 0
        for b in f["unsafe_blocks"]:
            ops = collections.Counter(o["op"] for o in b["ops"])
            real ={k: v for k, v in ops.items() if k not in ("call", "method")}
            unsafe_calls = []
            for o in b["ops"]:
                if o["op"] == "call":
                    name = o["detail"].split("::")[-1]
                    if any(t.get("unsafe_fn") for t in crate.by_name.get(name, [])):
                        unsafe_calls.append(o["detail"])
            if unsafe_calls:
                real["unsafe-fn-call"] = len(unsafe_calls)
            row = {"fn": f["id"], "file": f["file"], "line": b["line"], "kind": b["kind"],
                   "safety": b["safety"], "ops": real,
                   "ffi": sorted({o["detail"] for o in b["ops"] if o["op"] == "ffi"})}
            unsafe_rows.append(row)
            if b["kind"] == "block":
                if not b["safety"]:
                    add("unsafe-no-safety", f"{f['id']}#{n}", "medium",
                        f"unsafe block without a SAFETY comment ({', '.join(f'{k}×{v}' for k, v in real.items()) or 'no unsafe op found'})",
                        f"{f['file']}:{b['line']}")
                n += 1
            elif not b["safety"]:
                add("unsafe-fn-no-safety-doc", f["id"], "low", "unsafe fn without a `# Safety` doc section",
                    f"{f['file']}:{b['line']}")

    # FFI sites reachable outside every handler that can PANIC via data_sync_elevel
    # (I/O failure). An invariant PANIC in the server's transaction machinery is
    # listed in the site table, not flagged.
    for s in sites:
        for eff in rep["panic_effects"]:
            p = s["effects"].get(eff)
            if p and near(p, rep["panic_hops"]) and s["exposed"]:
                # One finding per server function; sites are where it's hit unhandled.
                add("ffi-panic-unhandled", p["via"], "high",
                    f"`{p['via']}` can {eff} ({hops(p, ' > ')}: {p['detail']}); called outside "
                    f"an error handler", f"{s['file']}:{s['line']}")
                break

    # Critical entries: anything raising reachable unhandled.
    for e in entries:
        sev = kinds.get(e["kind"], {}).get("severity", 2)
        if sev < rep["critical_severity"]:
            continue
        r = entry_reach[e["id"]]
        for s in sites:
            if s["fn"] in r and not r[s["fn"]] and not s["handled"]:
                raising = [x for x in RAISING if x in s["effects"] and near(s["effects"][x], dl)]
                if raising:
                    add("critical-entry-error", f"{e['id']}:{s['fn']}:{'+'.join(s['syms'])}", "high",
                        f"{e['kind']} `{e['id']}` reaches `{'+'.join(s['syms'])}` unhandled, which can raise "
                        f"{'/'.join(raising)}: {kinds[e['kind']]['failure']}", f"{s['file']}:{s['line']}")
        for n, h in r.items():
            if h:
                continue
            f = fns[n]
            for p in f["panics"]:
                if not p["_handled"]:
                    add("critical-entry-panic", f"{e['id']}:{n}:{p['kind']}@{p['line'] - f['line']}", "high",
                        f"{e['kind']} `{e['id']}` reaches a `{p['kind']}` panic site unhandled",
                        f"{f['file']}:{p['line']}")
            for p in f["raises"]:
                if not p["_handled"] and p["level"] in RAISING:
                    add("critical-entry-panic", f"{e['id']}:{n}:{p['macro']}@{p['line'] - f['line']}", "high",
                        f"{e['kind']} `{e['id']}` reaches `{p['macro']}!` ({p['level']}) unhandled",
                        f"{f['file']}:{p['line']}")

    # nm cross-checks.
    ext_names = {f["name"] for f in fns.values() if "pg_extern" in (f.get("attrs") or [])}
    src_syms = {}
    for f in fns.values():
        attrs = set(f.get("attrs") or [])
        if f.get("abi") and ({"no_mangle", "export_name", "pg_guard"} & attrs):
            src_syms[f.get("export_name") or f["name"]] = f
    ex = conf["exports"]
    pattern_names = {}
    for n in ext_names:
        for pat in ex["pg_extern"]:
            pattern_names[pat.replace("{name}", n)] = n
    unmapped = []
    for sym in cfx["exports"]:
        if sym in pattern_names or sym in src_syms or sym in ex["ignore"] or any(sym.startswith(p) for p in ex["ignore_prefixes"]):
            continue
        unmapped.append(sym)
        add("export-unmapped", sym, "medium", f"exported symbol `{sym}` maps to no source function", "")
    missing_exports = sorted(s for s, f in src_syms.items()
                             if ({"no_mangle", "export_name"} & set(f.get("attrs") or [])) and s not in cfx["exports"])
    for s in missing_exports:
        add("export-missing", s, "low", f"`{s}` is marked for export but the library does not export it", "")
    src_ffi = sorted({s for x in sites if not x["via"].startswith("pgrx:") for s in x["syms"]})
    not_linked = [s for s in src_ffi if s not in cfx["imports"]]
    for s in not_linked:
        d = cfx["symbols"].get(s, {})
        how = "defined in the server bitcode but not imported (inlined by the compiler?)" if d.get("defined") \
            else "not a server function: pgrx implements it in Rust or it is a static inline in the headers"
        add("ffi-not-linked", s, "info", f"`{s}`: {how}", "")
    via_pgrx = sorted(set(cfx["imports"]) - set(src_ffi))

    # Accepted findings keep severity, carry the project's reason, never fail baseline.
    for f in findings:
        for pattern, reason in conf.get("accepted", {}).items():
            if fnmatch.fnmatchcase(f["key"], pattern):
                f["accepted"] = reason
                break

    return {
        "pg": pg, "crate": crate, "entries": entries, "entry_reach": entry_reach, "sites": sites,
        "unsafe": unsafe_rows, "findings": findings, "unmapped_exports": unmapped,
        "missing_exports": missing_exports, "not_linked": not_linked, "via_pgrx": via_pgrx,
        "cfx": cfx, "facts": facts,
    }


LIBC_GROUPS = {
    "process": r"^(fork|vfork|exec|posix_spawn|waitpid|waitid|wait4|kill|setsid|setpgid|setuid|setgid|setgroups|chroot|pidfd)",
    "files": r"^(open|openat|close|read|write|pread|pwrite|readv|writev|lseek|fstat|stat|lstat|statx|fsync|fdatasync|ftruncate|unlink|rename|mkdir|rmdir|opendir|readdir|closedir|fdopendir|readlink|realpath|futimens|chdir|getcwd|dirfd|dup)",
    "network": r"^(socket|bind|listen|accept|connect|send|recv|getaddrinfo|freeaddrinfo|gai_strerror|setsockopt|getsockopt|shutdown|socketpair|__res_init)",
    "memory": r"^(malloc|calloc|realloc|free|posix_memalign|mmap|munmap|memcpy|memmove|memset|memcmp|bcmp)",
    "threads": r"^(pthread_|__cxa_thread)",
    "unwind": r"^(_Unwind_|__cxa|_ITM_|__gmon)",
}


def libc_groups(names):
    out = collections.defaultdict(list)
    for n in names:
        g = next((k for k, r in LIBC_GROUPS.items() if re.match(r, n)), "other")
        out[g].append(n)
    return out


def render_md(results, conf, new_findings):
    dl = conf["report"]["direct_hops"]
    kinds = conf["entry_kinds"]
    L = []
    w = L.append
    main = results[0]
    w(f"# FFI footprint: `{conf['project'].get('lib') or main['facts']['src']}`")
    w("")
    w(f"PG {', '.join(r['pg'] for r in results)}. Generated by `tools/ffi-footprint`. Effects are "
      f"written `EFFECT@depth`: depth is the number of calls inside the server from the called "
      f"function to the one that raises or performs it; `~` marks an effect more than {dl} "
      f"source module(s) away from the called function (indirect).")
    w("")
    for r in results:
        cfx = r["cfx"]
        st = cfx["stats"]
        fs = r["findings"]
        by = collections.Counter(f["severity"] for f in fs if not f.get("accepted"))
        acc = sum(1 for f in fs if f.get("accepted"))
        w(f"## PG {r['pg']}")
        w("")
        w(f"- Source: {len(r['facts']['files'])} files, {len(r['crate'].fns)} functions, "
          f"{sum(1 for u in r['unsafe'] if u['kind'] == 'block')} unsafe blocks "
          f"({sum(1 for u in r['unsafe'] if u['kind'] == 'block' and not u['safety'])} without SAFETY), "
          f"{sum(1 for u in r['unsafe'] if u['kind'] != 'block')} unsafe fn bodies.")
        w(f"- FFI: {len(r['sites'])} call sites into the server "
          f"({sum(1 for s in r['sites'] if s['exposed'])} reachable outside any error handler); "
          f"{len(cfx['imports'])} server functions linked, {len(r['via_pgrx'])} of them only through pgrx.")
        w(f"- Server model: {st['functions']} functions from {cfx.get('bitcode_origin', 'bitcode')}; elevels "
          f"{'calibrated' if cfx['elevels_calibrated'] else 'from table'} {cfx['elevels']}.")
        w(f"- Indirect calls: {st['indirect_sites']} sites; {indirect_breakdown(st)}.")
        w(f"- Open findings: " + (", ".join(f"{by[s]} {s}" for s in ("high", "medium", "low", "info") if by[s]) or "none")
          + (f"; {acc} accepted." if acc else "."))
        if r is main and new_findings is not None:
            w(f"- Baseline: {len(new_findings)} new finding(s) of a failing kind.")
        w("")

    r = main
    crate = r["crate"]
    w(f"## Entry points (PG {r['pg']})")
    w("")
    w("| entry | kind | guard | on ERROR/panic | reach | unsafe | FFI sites (unhandled) | panic sites (unhandled) | worst direct server effect, unhandled |")
    w("|---|---|---|---|---|---|---|---|---|")
    for e in r["entries"]:
        reach = r["entry_reach"][e["id"]]
        ids = list(reach)
        unsafe_n = sum(1 for u in r["unsafe"] if u["fn"] in reach)
        fsites = [s for s in r["sites"] if s["fn"] in reach]
        fun = [s for s in fsites if not s["handled"] and not reach[s["fn"]]]
        pan = sum(len(crate.fns[n]["panics"]) for n in ids)
        pun = sum(1 for n in ids if not reach[n] for p in crate.fns[n]["panics"] if not p["_handled"])
        worst = None
        for s in fun:
            for eff, v in s["effects"].items():
                if eff in RAISING + ("exit", "fsync") and near(v, dl):
                    if worst is None or ekey(eff) < ekey(worst[0]) or (eff == worst[0] and v["depth"] < worst[1]):
                        worst = (eff, v["depth"], v["via"])
        guard = "pg_guard" if e["guarded"] else ("**none**" if e["abi"] else "n/a")
        w(f"| `{e['id'].removeprefix('crate::')}` | {e['kind']} | {guard} | {kinds.get(e['kind'], {}).get('failure', '')} | "
          f"{len(ids)} fns | {unsafe_n} | {len(fsites)} ({len(fun)}) | {pan} ({pun}) | "
          f"{f'{worst[0]}@{worst[1]} via `{worst[2]}`' if worst else ''} |")
    w("")

    w("## FFI call sites")
    w("")
    w("Every call from Rust into the server, with what the server can do behind it. `handled`: "
      "the call runs inside a Postgres error handler (`PgTryBuilder` or a function inferred to "
      "wrap one), so an ERROR there becomes a Rust `Err` instead of unwinding the entry.")
    w("")
    w("| site | calls | handled | reached from (unhandled) | direct effects | indirect |")
    w("|---|---|---|---|---|---|")
    for s in sorted(r["sites"], key=lambda s: (not s["exposed"], min((ekey(e) for e, v in s["effects"].items() if near(v, dl)), default=99), s["file"], s["line"])):
        direct = {e: v for e, v in s["effects"].items() if near(v, dl) and e not in ("alloc", "indirect?")}
        indirect = {e: v for e, v in s["effects"].items() if not near(v, dl) and e in RAISING + ("fsync", "exit")}
        name = "+".join(s["syms"]) if not s["via"].startswith("pgrx:") else s["via"][5:]
        ents = sorted({ent_kind(r, x) for x in s["entries_unhandled"]})
        w(f"| `{s['file']}:{s['line']}` `{s['fn'].split('::')[-1]}` | `{name}` | {'yes' if s['handled'] else ('**no**' if s['exposed'] else 'by callers')} | "
          f"{len(s['entries_all'])} ({', '.join(ents)}) | {fmt_eff(direct, dl, limit=8)} | {fmt_eff(indirect, dl, limit=4)} |")
    w("")

    w("## Witnesses for direct PANIC, FATAL and fsync effects")
    w("")
    seen = set()
    for s in r["sites"]:
        for e in ("PANIC(io)", "PANIC", "FATAL", "fsync", "exit"):
            v = s["effects"].get(e)
            if v and near(v, dl) and (v["via"], e) not in seen:
                seen.add((v["via"], e))
                w(f"- `{v['via']}` {e}: {hops(v)} ({v['detail']}, `{v['module']}`)")
    w("")

    w("## Findings")
    w("")
    order = {"high": 0, "medium": 1, "low": 2, "info": 3}
    newkeys = {f["key"] for f in new_findings} if new_findings is not None else set()
    for f in sorted((f for f in r["findings"] if not f.get("accepted")),
                    key=lambda f: (order[f["severity"]], f["kind"], f["where"])):
        new = " **new**" if f["key"] in newkeys else ""
        more = f" (+{f['count'] - 1} more sites)" if f.get("count", 1) > 1 else ""
        w(f"- **{f['severity']}** `{f['kind']}`{new} {f['where'] and '`' + f['where'] + '`'}: {f['message']}{more}")
    accepted = sorted((f for f in r["findings"] if f.get("accepted")), key=lambda f: (order[f["severity"]], f["key"]))
    if accepted:
        w("")
        w("### Accepted")
        w("")
        for f in accepted:
            w(f"- {f['severity']} `{f['key']}` {f['where'] and '`' + f['where'] + '`'}: {f['message']}")
            w(f"  - Accepted: {f['accepted']}")
    w("")

    w("## Unsafe blocks")
    w("")
    w("| site | fn | kind | SAFETY | operations | FFI |")
    w("|---|---|---|---|---|---|")
    for u in sorted(r["unsafe"], key=lambda u: (u["file"], u["line"])):
        ops = ", ".join(f"{k}×{v}" for k, v in sorted(u["ops"].items()))
        w(f"| `{u['file']}:{u['line']}` | `{u['fn'].split('::')[-1]}` | {u['kind']} | {'yes' if u['safety'] else '**no**'} | {ops} | {', '.join(u['ffi'])} |")
    w("")

    io_paths = set(conf["report"]["rust_io_paths"])
    io_methods = set(conf["report"]["rust_io_methods"])
    io = []
    for f in crate.fns.values():
        for c in f["calls"]:
            p = c.get("path") or []
            if (len(p) > 1 and io_paths & set(p[:-1])) or c.get("method") in io_methods:
                io.append((f["file"], c["line"], f["id"].split("::")[-1], "::".join(p) or "." + c["method"]))
    if io:
        w("## Rust-side file I/O")
        w("")
        w("File operations the extension performs itself, outside the server's fd layer.")
        w("")
        for fl, ln, fn, what in sorted(io):
            w(f"- `{fl}:{ln}` `{fn}`: `{what}`")
        w("")

    cfx = r["cfx"]
    w("## Linkage (nm)")
    w("")
    w(f"- Exports: {len(cfx['exports'])}; unmapped to source: {', '.join(f'`{x}`' for x in r['unmapped_exports']) or 'none'}.")
    w(f"- Marked for export but absent: {', '.join(f'`{x}`' for x in r['missing_exports']) or 'none'}.")
    w(f"- Called in source but not linked: {', '.join(f'`{x}`' for x in r['not_linked']) or 'none'}.")
    w(f"- Server functions reached only through pgrx: {', '.join(f'`{x}`' for x in r['via_pgrx'])}.")
    w(f"- Server data symbols used: {', '.join(f'`{x}`' for x in cfx['imports_data'])}.")
    groups = libc_groups(cfx["undefined_non_postgres"])
    w("- Non-server symbols linked (libc and the Rust runtime; linked is not the same as called):")
    for g in ("process", "files", "network", "threads", "memory", "unwind", "other"):
        if groups.get(g):
            w(f"  - {g}: {', '.join(groups[g])}")
    w("")

    if len(results) > 1:
        w("## Differences across PG versions")
        w("")
        syms = sorted({s for x in results for site in x["sites"] for s in site["syms"]})
        rows = []
        for s in syms:
            cells = []
            for x in results:
                d = x["cfx"]["symbols"].get(s)
                if not d:
                    cells.append("not used")
                elif not d.get("defined"):
                    cells.append("absent")
                else:
                    cells.append(fmt_eff({e: v for e, v in d["effects"].items() if near(v, dl) and e in RAISING + ("fsync", "exit", "blocks", "fd-io")}, dl, limit=8) or "none")
            if len(set(cells)) > 1:
                rows.append((s, cells))
        if rows:
            w("| symbol | " + " | ".join(f"PG {x['pg']}" for x in results) + " |")
            w("|---|" + "---|" * len(results))
            for s, cells in rows:
                w(f"| `{s}` | " + " | ".join(cells) + " |")
        else:
            w("No differences in direct effects.")
        w("")
    return "\n".join(L)


def ent_kind(r, eid):
    for e in r["entries"]:
        if e["id"] == eid:
            return e["kind"]
    return "?"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--facts", action="append", required=True)
    ap.add_argument("--cfx", action="append", required=True)
    ap.add_argument("--md", required=True)
    ap.add_argument("--json", required=True)
    ap.add_argument("--html", help="also write a self-contained HTML page")
    ap.add_argument("--baseline")
    ap.add_argument("--write-baseline", help="write the findings' keys as a new baseline")
    a = ap.parse_args()
    if len(a.facts) != len(a.cfx):
        sys.exit("--facts and --cfx must pair up")
    conf = load(a.config)
    results = []
    for fp, cp in zip(a.facts, a.cfx):
        cfx = load(cp)
        results.append(analyze(cfx["pg"], load(fp), cfx, conf))
    main_r = results[0]
    all_findings = {}
    for r in results:
        for f in r["findings"]:
            all_findings.setdefault(f["key"], dict(f, pg=[]))["pg"].append(r["pg"])
    new = None
    if a.baseline:
        try:
            base = {f["key"] for f in load(a.baseline)["findings"]}
        except FileNotFoundError:
            base = set()
        fail = set(conf["report"]["fail_on"])
        new = [f for k, f in all_findings.items()
               if k not in base and f["kind"] in fail and not f.get("accepted")]
    with open(a.md, "w") as f:
        f.write(render_md(results, conf, new))
    if a.html:
        from html_report import render_html
        with open(a.html, "w") as f:
            f.write(render_html(results, conf, new, ent_kind))
    out = {
        "pg": [r["pg"] for r in results],
        "findings": sorted(all_findings.values(), key=lambda f: f["key"]),
        "entries": main_r["entries"],
        "sites": [{k: v for k, v in s.items()} for s in main_r["sites"]],
        "unsafe": main_r["unsafe"],
    }
    with open(a.json, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    if a.write_baseline:
        with open(a.write_baseline, "w") as f:
            json.dump({"pg": out["pg"], "findings": [
                {k: v for k, v in x.items() if k in ("key", "kind", "severity", "where")}
                for x in out["findings"]]}, f, indent=1, sort_keys=True)
            f.write("\n")
    by = collections.Counter(f["severity"] for f in all_findings.values())
    print(f"report: {len(all_findings)} findings ({dict(by)}); {a.md}", file=sys.stderr)
    if new:
        print(f"report: {len(new)} new finding(s) not in the baseline:", file=sys.stderr)
        for f in new:
            print(f"  {f['severity']} {f['key']} {f['where']}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
