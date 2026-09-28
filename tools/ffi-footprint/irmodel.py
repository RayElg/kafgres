"""Per-module LLVM IR model for cfx.py: call graph facts, local effects with source
locations, and points-to constraints over the places a function pointer can sit (struct
fields, globals, function parameters). Stdlib only; reads `llvm-dis` text."""
import os
import re
import subprocess

NAME = r'("(?:[^"\\]|\\.)*"|[-\w$.]+)'
DEFINE_RE = re.compile(r"^define\s+(.*?)@" + NAME + r"\((.*)$")
DECLARE_RE = re.compile(r"^declare\s+(.*?)@" + NAME + r"\(")
CALL_KW_RE = re.compile(r"\b(?:call|invoke|callbr)\b")
CALLEE_RE = re.compile(r"([@%])" + NAME + r"\(")
GLOBAL_REF_RE = re.compile(r"@" + NAME)
ASSIGN_RE = re.compile(r"^\s*(%[-\w.]+)\s*=\s*(.*)$")
TYPE_RE = re.compile(r'^(%(?:"[^"]+"|[-\w.$]+)) = type (.*)$')
GLOBAL_RE = re.compile(r"^@" + NAME + r" = (.*)$")
META_RE = re.compile(r"^!(\d+) = (?:distinct )?!(DI\w+)\((.*)\)\s*$")
TUPLE_RE = re.compile(r"^!(\d+) = (?:distinct )?!\{(.*)\}\s*$")
DBG_RE = re.compile(r"!dbg !(\d+)")
INT_RE = re.compile(r"-?\d+")
GEP_FLAG_RE = re.compile(r"^\s*(?:inbounds|nuw|nusw|inrange\([^)]*\))\s+")
ARRAY_RE = re.compile(r"^\[\s*\d+\s*x\s*(.*)\]$")
PREFIX_WORDS = {
    "private", "internal", "available_externally", "linkonce", "weak", "common", "appending",
    "extern_weak", "linkonce_odr", "weak_odr", "external", "default", "hidden", "protected",
    "dllimport", "dllexport", "dso_local", "dso_preemptable", "ccc", "fastcc", "coldcc",
    "noundef", "zeroext", "signext", "inreg", "noalias", "nonnull", "tail", "musttail",
    "notail", "call", "invoke", "nsz", "fast", "nnan", "ninf", "arcp", "contract", "afn",
    "reassoc", "local_unnamed_addr", "unnamed_addr",
}


def unq(name):
    return name[1:-1] if name.startswith('"') else name


def split_top(s):
    """Split on commas at nesting depth zero."""
    out, depth, cur = [], 0, []
    for ch in s:
        if ch in "([{<":
            depth += 1
        elif ch in ")]}>":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def balanced_end(s, start):
    """Index just past the bracket group opening at s[start]."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] in "([{<":
            depth += 1
        elif s[i] in ")]}>":
            depth -= 1
            if depth == 0:
                return i + 1
    return len(s)


def first_type(tok):
    tok = tok.strip()
    if not tok:
        return ""
    if tok[0] in "{[<":
        return tok[:balanced_end(tok, 0)]
    return tok.split()[0]


def ret_type(prefix):
    """The return type is the last type-looking token of a define/call prefix."""
    words, i, toks = prefix.strip(), 0, []
    while i < len(words):
        ch = words[i]
        if ch.isspace():
            i += 1
            continue
        if ch in "{[<":
            j = balanced_end(words, i)
            toks.append(words[i:j])
            i = j
            continue
        j = i
        while j < len(words) and not words[j].isspace():
            if words[j] == "(":
                j = balanced_end(words, j)
                continue
            j += 1
        toks.append(words[i:j])
        i = j
    toks = [t for t in toks if t not in PREFIX_WORDS and not re.match(
        r"^(align|dereferenceable|dereferenceable_or_null|range|nofpclass|cc|addrspace)\b", t)
        and not t.startswith("#")]
    return toks[-1] if toks else "void"


def param_sig(params_text):
    if not params_text.strip():
        return ""
    return ",".join("..." if p.strip() == "..." else first_type(p) for p in split_top(params_text))


def meta_fields(body):
    out = {}
    for part in split_top(body):
        k, _, v = part.partition(":")
        out[k.strip()] = v.strip()
    return out


def struct_fields(body):
    body = body.strip()
    if body.startswith("<{"):
        return split_top(body[2:-2])
    if body.startswith("{"):
        return split_top(body[1:-1])
    return None


def fn_ref(tok):
    """The function name in `@name` or `@"name"`, else None."""
    m = re.match(NAME, tok[1:]) if tok.startswith("@") else None
    return unq(m.group(1)) if m else None


class Module:
    """One bitcode module."""

    def __init__(self, rel, ir, conf):
        self.rel, self.conf = rel, conf
        self.lines = ir.split("\n")
        self.types, self.meta, self.tuples = {}, {}, {}
        self.internal, self.gl_internal, self.fn_names, self.globals = set(), set(), set(), []
        for line in self.lines:
            c = line[:1]
            if c == "%":
                m = TYPE_RE.match(line)
                if m:
                    self.types[m.group(1)] = struct_fields(m.group(2))
            elif c == "!":
                m = META_RE.match(line)
                if m:
                    self.meta[m.group(1)] = (m.group(2), m.group(3))
                    continue
                m = TUPLE_RE.match(line)
                if m:
                    self.tuples[m.group(1)] = [x.strip().lstrip("!") for x in split_top(m.group(2))]
            elif c == "d":
                m = DEFINE_RE.match(line)
                if m:
                    n = unq(m.group(2))
                    self.fn_names.add(n)
                    if re.search(r"\b(internal|private)\b", m.group(1)):
                        self.internal.add(n)
                else:
                    m = DECLARE_RE.match(line)
                    if m:
                        self.fn_names.add(unq(m.group(2)))
            elif c == "@":
                m = GLOBAL_RE.match(line)
                if m:
                    self.globals.append((unq(m.group(1)), m.group(2)))
                    if re.match(r"(internal|private)\b", m.group(2)):
                        self.gl_internal.add(unq(m.group(1)))
        self.effect_by_name = {}
        for eff, names in conf["effects"].items():
            for n in names:
                self.effect_by_name.setdefault(n, []).append(eff)
        self.loc_cache = {}
        self.defs, self.taken = {}, set()
        self.fnsets, self.copies = {}, set()

    def key(self, n):
        return f"{self.rel}::{n}" if n in self.internal else n

    def gkey(self, n):
        return f"{self.rel}::@{n}" if n in self.gl_internal else "@" + n

    def tkey(self, t):
        t = t.strip()
        return f"{self.rel}::{t}" if (".anon" in t or not t.startswith("%")) else t

    def file_of(self, sid, depth=0):
        md = self.meta.get(sid)
        if not md or depth > 30:
            return None
        f = meta_fields(md[1])
        fid = f.get("file", "").lstrip("!")
        if fid in self.meta and self.meta[fid][0] == "DIFile":
            return os.path.basename(meta_fields(self.meta[fid][1]).get("filename", "").strip('"'))
        sc = f.get("scope", "").lstrip("!")
        return self.file_of(sc, depth + 1) if sc else None

    def dbg_loc(self, line):
        m = DBG_RE.search(line)
        if not m:
            return None
        i = m.group(1)
        if i not in self.loc_cache:
            r = None
            md = self.meta.get(i)
            if md and md[0] == "DILocation":
                f = meta_fields(md[1])
                fl = self.file_of(f.get("scope", "").lstrip("!"))
                if fl:
                    r = f"{fl}:{f.get('line', '?')}"
            self.loc_cache[i] = r
        return self.loc_cache[i]

    def members(self):
        """LLVM struct type -> C member names, where debug info lines them up one to one."""
        comp, typedefs = {}, {}
        for i, (kind, body) in self.meta.items():
            if kind == "DICompositeType":
                f = meta_fields(body)
                el = f.get("elements", "").lstrip("!")
                if el in self.tuples:
                    comp[i] = el
                    n = f.get("name", "").strip('"')
                    if n:
                        comp.setdefault("name:" + n, el)
            elif kind == "DIDerivedType":
                f = meta_fields(body)
                if f.get("tag") == "DW_TAG_typedef":
                    typedefs[f.get("name", "").strip('"')] = f.get("baseType", "").lstrip("!")
        out = {}
        for tname, fields in self.types.items():
            if not fields or not tname.startswith(("%struct.", "%union.")):
                continue
            short = tname.split(".", 1)[1].strip('"')
            el = comp.get("name:" + short) or comp.get(typedefs.get(short, ""))
            if not el:
                continue
            names = []
            for x in self.tuples[el]:
                md = self.meta.get(x)
                if md and md[0] == "DIDerivedType":
                    f = meta_fields(md[1])
                    if f.get("tag") == "DW_TAG_member":
                        names.append(f.get("name", "").strip('"'))
            if len(names) == len(fields):
                out[self.tkey(tname)] = names
        return out

    def gep_loc(self, text):
        """What a getelementptr designates: (field, type, k), (global, g) or (unknown,)."""
        t = text.strip()
        while True:
            m = GEP_FLAG_RE.match(t)
            if not m:
                break
            t = t[m.end():]
        if t.startswith("("):
            t = t[1:t.rfind(")")]
        # Trailing attachments (`, !dbg !12`, `, align 8`) are not indices.
        parts = [p for p in split_top(t) if not p.startswith(("!", "align"))]
        if len(parts) < 2:
            return ("unknown",)
        cur = first_type(parts[0])
        btoks = parts[1].split()
        base = btoks[-1] if btoks else ""
        loc = None
        for n, p in enumerate(parts[2:]):
            if n == 0:
                continue
            idx = p.split()[-1] if p.split() else ""
            cur = cur.strip()
            m = ARRAY_RE.match(cur)
            if m:
                cur = m.group(1)
                continue
            fields = self.types.get(cur) if cur.startswith("%") else struct_fields(cur)
            if fields is None or not INT_RE.fullmatch(idx) or int(idx) >= len(fields):
                return ("unknown",)
            loc = ("field", self.tkey(cur), int(idx))
            cur = fields[int(idx)]
        if loc:
            return loc
        if base.startswith("@"):
            return ("global", self.gkey(unq(base[1:])))
        return ("unknown",)

    def const_fns(self, ty, val, dst):
        """Functions named in a constant initializer, attributed to the field or global."""
        ty, val = ty.strip(), val.strip()
        if val.startswith("@"):
            n = fn_ref(val)
            if n in self.fn_names and dst:
                self.fnsets.setdefault(dst, set()).add(self.key(n))
            return
        if ARRAY_RE.match(ty) and val.startswith("["):
            for el in split_top(val[1:-1]):
                ety = first_type(el)
                self.const_fns(ety, el[len(ety):], dst)
            return
        if val.startswith("{") or val.startswith("<{"):
            for k, el in enumerate(struct_fields(val) or []):
                ety = first_type(el)
                self.const_fns(ety, el[len(ety):], ("field", self.tkey(ty), k))

    def run(self):
        for g, rest in self.globals:
            m = re.search(r"\b(global|constant)\s+", rest)
            if m:
                after = rest[m.end():]
                ty = first_type(after)
                val = split_top(after[len(ty):])
                if val:
                    self.const_fns(ty, val[0], ("global", self.gkey(g)))
            for r in GLOBAL_REF_RE.finditer(rest):
                n = unq(r.group(1))
                if n in self.fn_names:
                    self.taken.add(self.key(n))
        header, body = None, []
        for line in self.lines:
            if header is None:
                if line.startswith("define"):
                    header, body = line, []
                continue
            if line.startswith("}"):
                self.function(header, body)
                header = None
                continue
            body.append(line)
        for d in self.defs.values():
            d["calls"] = sorted(d["calls"])
        return {"rel": self.rel, "defs": self.defs, "taken": sorted(self.taken),
                "fnsets": {k: sorted(v) for k, v in self.fnsets.items()},
                "copies": sorted(self.copies), "members": self.members()}

    def function(self, header, body):
        m = DEFINE_RE.match(header)
        name = unq(m.group(2))
        rest = m.group(3)
        pend = balanced_end("(" + rest, 0) - 1
        ptext = rest[:pend - 1]
        fkey = self.key(name)
        sig = f"{ret_type(m.group(1))}({param_sig(ptext)})"
        params = {}
        for i, p in enumerate(split_top(ptext) if ptext.strip() else []):
            toks = p.split()
            if toks and toks[-1].startswith("%"):
                params[toks[-1]] = i
        d = self.defs.setdefault(fkey, {"name": name, "sig": sig, "calls": set(), "call_locs": {},
                                        "indirect": [], "local": {}})
        assign, slots, stores = {}, {}, []
        for bl in body:
            am = ASSIGN_RE.match(bl)
            if am:
                assign[am.group(1)] = am.group(2)
            s = bl.strip()
            if s.startswith("store "):
                s = s[6:]
                s = s[9:] if s.startswith("volatile ") else s
                parts = split_top(s)
                if len(parts) >= 2:
                    vty = first_type(parts[0])
                    lt = parts[1].strip()
                    stores.append((vty, parts[0][len(vty):].strip(), lt[3:].strip() if lt.startswith("ptr") else lt))
        conf = self.conf

        def locof(tok, depth=0):
            tok = tok.strip()
            if tok.startswith("@"):
                return {("global", self.gkey(unq(tok[1:])))}
            if tok.startswith("getelementptr"):
                return {self.gep_loc(tok[len("getelementptr"):])}
            rhs = assign.get(tok)
            if rhs is None or depth > 6:
                return {("unknown",)}
            if rhs.startswith("alloca"):
                return {("slot", tok)}
            if rhs.startswith("getelementptr"):
                return {self.gep_loc(rhs[len("getelementptr"):])}
            if rhs.startswith("select"):
                out = set()
                for op in split_top(rhs[6:])[1:3]:
                    out |= locof(op.split()[-1], depth + 1)
                return out
            if rhs.startswith("phi"):
                out = set()
                for inc in re.findall(r"\[\s*([^,\]]+),", rhs):
                    out |= locof(inc, depth + 1)
                return out
            return {("unknown",)}

        for vty, val, lt in stores:
            for loc in locof(lt):
                if loc[0] == "slot":
                    slots.setdefault(loc[1], []).append(val)

        def loaded_from(rhs):
            parts = split_top(rhs[4:].replace("volatile", "", 1))
            if len(parts) < 2:
                return None
            src = parts[1].strip()
            return src[3:].strip() if src.startswith("ptr") else src

        def absval(tok, depth=0):
            """Where a pointer value can come from, as constraint terms."""
            tok = tok.strip()
            if tok.startswith("@"):
                n = fn_ref(tok)
                return {("fn", self.key(n))} if n in self.fn_names else set()
            if not tok or tok in ("null", "undef", "poison", "zeroinitializer"):
                return set()
            if not tok.startswith("%") or depth > 8:
                return {("unknown",)}
            if tok in params:
                return {("param", fkey, params[tok])}
            rhs = assign.get(tok)
            if rhs is None:
                return {("unknown",)}
            if rhs.startswith("load"):
                src = loaded_from(rhs)
                out = set()
                for loc in (locof(src, depth + 1) if src else {("unknown",)}):
                    if loc[0] == "slot":
                        vals = slots.get(loc[1], [])
                        for v in vals:
                            out |= absval(v, depth + 1)
                        if not vals:
                            out.add(("unknown",))
                    elif loc[0] in ("field", "global"):
                        out.add(loc)
                    else:
                        out.add(("unknown",))
                return out
            if rhs.startswith("select"):
                out = set()
                for op in split_top(rhs[6:])[1:3]:
                    out |= absval(op.split()[-1], depth + 1)
                return out
            if rhs.startswith("phi"):
                out = set()
                for inc in re.findall(r"\[\s*([^,\]]+),", rhs):
                    out |= absval(inc, depth + 1)
                return out
            return {("unknown",)}

        def constrain(dst, val):
            for t in absval(val):
                if t[0] == "fn":
                    self.fnsets.setdefault(dst, set()).add(t[1])
                elif t[0] in ("field", "global", "param"):
                    self.copies.add((dst, t))

        for vty, val, lt in stores:
            if vty == "ptr":
                for loc in locof(lt):
                    if loc[0] in ("field", "global"):
                        constrain(loc, val)

        io_globals = set(conf.get("io_elevel_globals", []))
        io_fns = set(conf.get("io_elevel_fns", []))

        def mentions_io(tok, depth=0):
            tok = tok.strip()
            if depth > 5 or tok not in assign:
                return False
            rhs = assign[tok]
            if any(unq(g.group(1)) in io_globals for g in GLOBAL_REF_RE.finditer(rhs)):
                return True
            return any(mentions_io(t, depth + 1) for t in re.findall(r"%[-\w.]+", rhs))

        def resolve_elevel(tok, depth=0):
            """Possible elevels of an operand, as (value, from_io) pairs; from_io marks a
            level chosen from an I/O failure."""
            tok = tok.strip()
            if INT_RE.fullmatch(tok):
                return {(int(tok), False)}
            if depth > 10 or not tok.startswith("%") or tok not in assign:
                return {("dynamic", False)}
            rhs = assign[tok]
            if rhs.startswith("load"):
                src = loaded_from(rhs)
                out = set()
                for loc in (locof(src) if src else ()):
                    if loc[0] == "slot" and slots.get(loc[1]):
                        for v in slots[loc[1]]:
                            out |= resolve_elevel(v, depth + 1)
                return out or {("dynamic", False)}
            if rhs.startswith("select"):
                ops = split_top(rhs[len("select"):])
                io = mentions_io(ops[0].split()[-1]) if ops else False
                out = set()
                for op in ops[1:3]:
                    out |= {(v, t or io) for v, t in resolve_elevel(op.split()[-1], depth + 1)}
                return out
            if rhs.startswith("phi"):
                out = set()
                for inc in re.findall(r"\[\s*([^,\]]+),", rhs):
                    out |= resolve_elevel(inc, depth + 1)
                return out
            if re.match(r"(zext|sext|trunc)\b", rhs):
                return resolve_elevel(rhs.split()[2], depth + 1)
            cm = CALLEE_RE.search(rhs)
            if cm and cm.group(1) == "@" and unq(cm.group(2)) in conf["elevel_fns"]:
                fn = unq(cm.group(2))
                io = fn in io_fns
                out = set()
                args = split_top(rhs[cm.end():rhs.rfind(")")])
                for spec in conf["elevel_fns"][fn]:
                    if spec.startswith("arg"):
                        i = int(spec[3:])
                        if i < len(args):
                            out |= {(v, t or io) for v, t in resolve_elevel(args[i].split()[-1], depth + 1)}
                    else:
                        out.add((spec, io))
                return out
            return {("dynamic", False)}

        elevel_calls = conf["elevel_calls"]
        for bl in body:
            kw = CALL_KW_RE.search(bl)
            callee_span = None
            if kw:
                cm = CALLEE_RE.search(bl, kw.end())
                if cm:
                    callee_span = cm.span()
                    cname = unq(cm.group(2))
                    args = split_top(bl[cm.end():balanced_end(bl, cm.end() - 1) - 1])
                    loc = self.dbg_loc(bl)
                    at = f" at {loc}" if loc else ""
                    if cm.group(1) == "@":
                        if not cname.startswith("llvm."):
                            ck = self.key(cname)
                            d["calls"].add(ck)
                            if loc:
                                d["call_locs"].setdefault(ck, loc)
                            for i, a in enumerate(args):
                                if first_type(a) == "ptr" and a.split():
                                    constrain(("param", ck, i), a.split()[-1])
                            if cname in elevel_calls:
                                idx = elevel_calls[cname]
                                tok = args[idx].split()[-1] if idx < len(args) and args[idx].split() else "dynamic"
                                # A non-literal value is marked with `*` so calibration sees only literals.
                                literal = INT_RE.fullmatch(tok) is not None
                                for lv, io in resolve_elevel(tok):
                                    k = f"elevel:{lv}:io" if io else f"elevel:{lv}"
                                    d["local"].setdefault(k, [cname if literal else cname + "*", loc])
                            for eff in self.effect_by_name.get(cname, []):
                                d["local"].setdefault(eff, [f"calls {cname}", loc])
                    else:
                        before = bl[kw.end():cm.start()]
                        explicit = re.search(r"\(([^()]*)\)\s*$", before)
                        if explicit:
                            isig = f"{ret_type(before[:explicit.start()])}({param_sig(explicit.group(1))})"
                        else:
                            isig = f"{ret_type(before)}({param_sig(', '.join(args))})"
                        d["indirect"].append({"sig": isig, "terms": sorted(absval("%" + cname)), "loc": loc})
            for g in GLOBAL_REF_RE.finditer(bl):
                if callee_span and g.start() == callee_span[0]:
                    continue
                n = unq(g.group(1))
                if n in self.fn_names and not n.startswith("llvm."):
                    self.taken.add(self.key(n))


def solve(fnsets, copies):
    """Field-based points-to: each field, global or parameter holds the functions stored
    into it, plus everything held by whatever is copied into it."""
    held = {t: set(v) for t, v in fnsets.items()}
    feeds = {}
    for dst, src in copies:
        feeds.setdefault(tuple(src), set()).add(tuple(dst))
    work = list(held)
    while work:
        src = work.pop()
        for dst in feeds.get(src, ()):
            have = held.setdefault(dst, set())
            before = len(have)
            have |= held[src]
            if len(have) != before:
                work.append(dst)
    return held


def analyze_module(args):
    path, rel, llvm_dis, conf = args
    try:
        ir = subprocess.run([llvm_dis, "-o", "-", path], capture_output=True, text=True,
                            check=True).stdout
    except subprocess.CalledProcessError as e:
        return {"rel": rel, "error": e.stderr.strip()[:300]}
    return Module(rel, ir, conf).run()
