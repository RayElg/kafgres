"""Self-contained HTML rendering of the FFI footprint report (see report.py --html)."""
import collections
import datetime
import html

SEV_COLOR = {
    "PANIC(io)": "panic-io", "PANIC": "panic", "FATAL": "fatal", "ERROR": "error", "error?": "error-q",
    "exit": "exit", "fsync": "io", "fd-io": "io", "blocks": "blocks", "interrupts": "muted",
    "catalog": "muted", "alloc": "muted", "indirect?": "muted",
}
SHOWN = ("PANIC(io)", "PANIC", "FATAL", "ERROR", "error?", "exit", "fsync", "fd-io", "blocks", "interrupts")


def e(s):
    return html.escape(str(s), quote=True)


def chip(eff, v, dl):
    cls = SEV_COLOR.get(eff, "muted")
    far = v.get("hops", v["depth"]) > dl
    title = f"{' → '.join(v['path'])}: {v['detail']}"
    return (f'<span class="chip {cls}{" far" if far else ""}" title="{e(title)}">'
            f'{"~" if far else ""}{e(eff)}<b>@{v["depth"]}</b></span>')


def chain(v):
    at = v.get("at") or []
    steps = "".join(f"<li>{e(p)}{f' <small>{e(at[i])}</small>' if i < len(at) and at[i] else ''}</li>"
                    for i, p in enumerate(v["path"]))
    return f'<ol class="chain">{steps}</ol><span class="why">{e(v["detail"])} · <code>{e(v["module"])}</code></span>'


CSS = """
:root{
  --ground:#F3F5F3;--surface:#FFFFFF;--sunk:#E8ECE9;--ink:#18201E;--muted:#5B6763;--rule:#D5DCD8;
  --accent:#1F6B66;--accent-ink:#FFFFFF;
  --panic:#B42318;--panic-bg:#FDECEA;--fatal:#C2410C;--fatal-bg:#FDF0E7;--error:#A16207;--error-bg:#FBF3DC;
  --io:#2563A8;--io-bg:#E7EFF9;--blocks:#6B4FA3;--blocks-bg:#EFEAF8;--exit:#4B5563;--exit-bg:#ECEEF0;
  --sans:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;
  --cond:"IBM Plex Sans Condensed","IBM Plex Sans",system-ui,sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,"SFMono-Regular",Menlo,Consolas,monospace;
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;
  --ground:#111514;--surface:#171D1B;--sunk:#1E2624;--ink:#E2E8E5;--muted:#93A19C;--rule:#2A3431;
  --accent:#4FB3AA;--accent-ink:#0B1413;
  --panic:#F2766B;--panic-bg:#3A1714;--fatal:#F59A5C;--fatal-bg:#3A2213;--error:#E3B341;--error-bg:#352A10;
  --io:#7FB0EA;--io-bg:#15263A;--blocks:#B49BEA;--blocks-bg:#251D38;--exit:#A7B0BB;--exit-bg:#232A2F;}}
:root[data-theme="dark"]{color-scheme:dark;
  --ground:#111514;--surface:#171D1B;--sunk:#1E2624;--ink:#E2E8E5;--muted:#93A19C;--rule:#2A3431;
  --accent:#4FB3AA;--accent-ink:#0B1413;
  --panic:#F2766B;--panic-bg:#3A1714;--fatal:#F59A5C;--fatal-bg:#3A2213;--error:#E3B341;--error-bg:#352A10;
  --io:#7FB0EA;--io-bg:#15263A;--blocks:#B49BEA;--blocks-bg:#251D38;--exit:#A7B0BB;--exit-bg:#232A2F;}
*{box-sizing:border-box}
body{background:var(--ground);color:var(--ink);font:15px/1.55 var(--sans);padding-inline:20px;padding-block:0 64px}
.wrap{max-width:1180px;margin:0 auto}
header.top{padding-block:36px 20px;display:grid;gap:6px}
.eyebrow{font:600 12px/1 var(--mono);letter-spacing:.08em;text-transform:uppercase;color:var(--accent)}
h1{font:600 clamp(28px,4vw,40px)/1.1 var(--cond);margin:0;text-wrap:balance}
h1 code{font-family:var(--mono);font-weight:500;font-size:.8em}
.sub{color:var(--muted);max-width:75ch;margin:0}
nav.sections{position:sticky;top:env(safe-area-inset-top,0px);z-index:5;background:var(--ground);border-bottom:1px solid var(--rule);
  display:flex;gap:4px;overflow-x:auto;padding-block:8px;margin-bottom:24px}
nav.sections a{font:500 13px/1 var(--sans);color:var(--muted);text-decoration:none;padding:8px 10px;border-radius:6px;white-space:nowrap}
nav.sections a:hover,nav.sections a:focus-visible{color:var(--ink);background:var(--sunk);outline:none}
section{margin-block:40px;scroll-margin-top:64px}
h2{font:600 22px/1.2 var(--cond);margin:0 0 6px;text-wrap:balance}
h2 + p{color:var(--muted);margin:0 0 16px;max-width:80ch}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:1px;background:var(--rule);border:1px solid var(--rule);border-radius:10px;overflow:hidden}
.stat{background:var(--surface);padding:14px 16px;display:grid;gap:2px}
.stat .n{font:600 26px/1.1 var(--cond);font-variant-numeric:tabular-nums}
.stat .l{font-size:13px;color:var(--muted)}
.stat .n .of{font:500 15px var(--sans);color:var(--muted)}
.model{margin-top:10px;font:13px/1.5 var(--mono);color:var(--muted)}
.findings{display:grid;gap:8px}
.finding{display:grid;grid-template-columns:auto 1fr;gap:4px 12px;background:var(--surface);border:1px solid var(--rule);border-radius:8px;padding:10px 14px}
.sev{font:600 11px/1 var(--mono);text-transform:uppercase;letter-spacing:.06em;padding:5px 7px;border-radius:4px;align-self:start}
.sev.high{background:var(--panic-bg);color:var(--panic)}.sev.medium{background:var(--error-bg);color:var(--error)}
.sev.low{background:var(--sunk);color:var(--muted)}.sev.info{background:var(--io-bg);color:var(--io)}
.finding .kind{font:500 13px var(--mono);color:var(--muted)}
.finding .kind .where{color:var(--ink)}
.finding .msg{grid-column:2;overflow-wrap:anywhere}
.finding .msg code,td code{font:13px var(--mono)}
.more{color:var(--muted);font-size:13px}
.reason{display:block;margin-top:6px;padding-left:10px;border-left:2px solid var(--rule);color:var(--muted);font-size:14px}
.group-title{font:600 13px/1 var(--mono);letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin:18px 0 8px}
.tablebox{overflow-x:auto;border:1px solid var(--rule);border-radius:10px;background:var(--surface)}
table{border-collapse:collapse;width:100%;font-size:14px}
th{font:600 12px/1.2 var(--sans);text-align:left;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;padding:10px 12px;border-bottom:1px solid var(--rule);background:var(--sunk);white-space:nowrap}
td{padding:9px 12px;border-bottom:1px solid var(--rule);vertical-align:top}
tr:last-child td{border-bottom:0}
td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
td.sym{font:13px var(--mono);white-space:nowrap}
td.site{font:13px var(--mono)}
td.site .fn{color:var(--muted)}
.chips{display:flex;flex-wrap:wrap;gap:4px}
.chip{font:500 12px/1 var(--mono);padding:4px 6px;border-radius:4px;white-space:nowrap;border:1px solid transparent}
.chip b{font-weight:400;opacity:.75}
.chip.panic-io{background:var(--panic);color:var(--surface)}
.chip.panic{background:var(--panic-bg);color:var(--panic);border-color:var(--panic)}
.chip.fatal{background:var(--fatal-bg);color:var(--fatal)}
.chip.error{background:var(--error-bg);color:var(--error)}
.chip.error-q{color:var(--error);border-color:var(--error)}
.chip.io{background:var(--io-bg);color:var(--io)}
.chip.blocks{background:var(--blocks-bg);color:var(--blocks)}
.chip.exit{background:var(--exit-bg);color:var(--exit)}
.chip.muted{background:var(--sunk);color:var(--muted)}
.chip.far{opacity:.55}
.state{font:600 12px/1 var(--mono);padding:4px 6px;border-radius:4px;white-space:nowrap}
.state.exposed{background:var(--panic-bg);color:var(--panic)}
.state.handled{background:var(--io-bg);color:var(--io)}
.state.callers{background:var(--sunk);color:var(--muted)}
.state.none{background:var(--panic-bg);color:var(--panic)}
details summary{cursor:pointer;color:var(--accent);font-size:13px;margin-top:6px}
details summary:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.witness{display:grid;gap:8px;margin-top:8px}
.witness .eff{display:flex;gap:8px;align-items:flex-start;flex-wrap:wrap}
ol.chain{list-style:none;margin:0;padding:0;display:flex;flex-wrap:wrap;gap:2px 0;font:12px/1.6 var(--mono)}
ol.chain li+li::before{content:"→";color:var(--muted);padding:0 6px}
ol.chain small{color:var(--muted);font-size:11px}
.why{display:block;width:100%;font-size:12px;color:var(--muted)}
.why code{font:12px var(--mono)}
.toolbar{display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:10px;font-size:14px}
.toolbar label{display:flex;gap:6px;align-items:center;cursor:pointer}
.toolbar input[type=search]{font:14px var(--sans);padding:6px 10px;border:1px solid var(--rule);border-radius:6px;background:var(--surface);color:var(--ink);min-width:0;width:260px;max-width:100%}
.toolbar input:focus-visible{outline:2px solid var(--accent);outline-offset:1px}
.count{color:var(--muted);font-variant-numeric:tabular-nums}
.legend{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:13px;color:var(--muted);margin-bottom:14px}
.legend span{display:flex;gap:6px;align-items:center}
.cols{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}
.panel{background:var(--surface);border:1px solid var(--rule);border-radius:10px;padding:14px 16px}
.panel h3{font:600 15px/1.3 var(--cond);margin:0 0 8px}
.panel p{margin:0;color:var(--muted);font-size:14px}
.taglist{display:flex;flex-wrap:wrap;gap:4px;font:12px var(--mono)}
.taglist span{background:var(--sunk);padding:3px 6px;border-radius:4px}
.limits{color:var(--muted);font-size:14px;max-width:80ch}
.no{color:var(--panic);font-weight:600}
@media (max-width:600px){body{padding-inline:16px}.finding{grid-template-columns:1fr}.finding .msg{grid-column:1}}
@media (prefers-reduced-motion:reduce){*{scroll-behavior:auto!important}}
"""

JS = """
(function(){
  var box=document.getElementById('exposed-only'), q=document.getElementById('site-q');
  var rows=[].slice.call(document.querySelectorAll('#sites tbody tr')), count=document.getElementById('site-count');
  function apply(){
    var t=(q.value||'').toLowerCase(), n=0;
    rows.forEach(function(r){
      var show=(!box.checked||r.dataset.exposed==='1')&&(!t||r.textContent.toLowerCase().indexOf(t)>=0);
      r.hidden=!show; if(show)n++;
    });
    count.textContent=n+' of '+rows.length+' sites';
  }
  box.addEventListener('change',apply); q.addEventListener('input',apply); apply();
})();
"""


def render_html(results, conf, new_findings, kinds_fn):
    dl = conf["report"]["direct_hops"]
    kinds = conf["entry_kinds"]
    r = results[0]
    crate = r["crate"]
    cfx = r["cfx"]
    lib = conf["project"].get("lib") or "extension"
    pgs = ", ".join(x["pg"] for x in results)
    fs = r["findings"]
    by = collections.Counter(f["severity"] for f in fs if not f.get("accepted"))
    accepted = [f for f in fs if f.get("accepted")]
    exposed = sum(1 for s in r["sites"] if s["exposed"])
    blocks = [u for u in r["unsafe"] if u["kind"] == "block"]
    nosafety = sum(1 for u in blocks if not u["safety"])
    st = cfx["stats"]
    out = []
    w = out.append
    w(f"<title>{e(lib)} FFI footprint</title>")
    w('<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>')
    w('<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@600&family=IBM+Plex+Sans:wght@400;500;600&display=swap">')
    w(f"<style>{CSS}</style>")
    w('<div class="wrap">')
    w('<header class="top">')
    w(f'<span class="eyebrow">ffi-footprint · PG {e(pgs)} · {datetime.date.today().isoformat()}</span>')
    w(f"<h1>FFI footprint of <code>{e(lib)}.so</code></h1>")
    w('<p class="sub">Where Postgres calls into the extension, where the extension calls into Postgres, '
      "what the server can do behind each call, and which calls run outside a Postgres error handler. "
      "Built from the Rust source, the server's LLVM bitcode, and the library's dynamic symbols.</p>")
    w("</header>")
    w('<nav class="sections" aria-label="Sections">')
    for a, t in [("summary", "Summary"), ("findings", "Findings"), ("entries", "Entry points"), ("sites", "FFI call sites"),
                 ("unsafe", "Unsafe blocks"), ("linkage", "Linkage")] + ([("versions", "PG versions")] if len(results) > 1 else []) + [("limits", "Limits")]:
        w(f'<a href="#{a}">{t}</a>')
    w("</nav>")

    # Summary
    w('<section id="summary"><h2>Summary</h2>')
    w(f"<p>PG {e(r['pg'])} in detail{'; other versions are compared under PG versions' if len(results) > 1 else ''}.</p>")
    w('<div class="stats">')
    tiles = [
        (f'{by["high"]}<span class="of"> + {len(accepted)} accepted</span>', "open high-severity findings"),
        (f'{exposed}<span class="of"> / {len(r["sites"])}</span>', "FFI sites reachable outside a handler"),
        (f'{len(r["entries"])}', "entry points Postgres calls"),
        (f'{nosafety}<span class="of"> / {len(blocks)}</span>', "unsafe blocks without SAFETY"),
        (f'{len(cfx["imports"])}', f'server functions linked ({len(r["via_pgrx"])} only via pgrx)'),
    ]
    for n, l in tiles:
        w(f'<div class="stat"><span class="n">{n}</span><span class="l">{e(l)}</span></div>')
    w("</div>")
    from report import indirect_breakdown  # noqa: E402
    w(f'<div class="model">server model: {st["functions"]:,} functions · {e(cfx.get("bitcode_origin", "bitcode"))} · '
      f'elevels {"calibrated" if cfx["elevels_calibrated"] else "from table"} ERROR={cfx["elevels"]["ERROR"]}<br>'
      f'indirect calls: {st["indirect_sites"]:,} sites; {e(indirect_breakdown(st))}<br>'
      f'{len(crate.fns)} Rust functions in {len(r["facts"]["files"])} files</div>')
    w("</section>")

    # Findings
    order = ["high", "medium", "low", "info"]
    newkeys = {f["key"] for f in new_findings} if new_findings else set()
    w('<section id="findings"><h2>Findings</h2>')
    w(f'<p>{len(fs)} findings with stable keys for the baseline. Identical findings at several sites are folded into one.</p>')
    for sev in order:
        group = sorted((f for f in fs if f["severity"] == sev and not f.get("accepted")), key=lambda f: (f["kind"], f["where"]))
        if not group:
            continue
        w(f'<div class="group-title">{sev} · {len(group)}</div><div class="findings">')
        for f in group:
            others = [x for x in f.get("sites", []) if x != f["where"]]
            more = (f'<details><summary>{len(others)} more site{"s" if len(others) > 1 else ""}</summary>'
                    f'<div class="taglist">{"".join(f"<span>{e(x)}</span>" for x in others)}</div></details>') if others else ""
            new = ' <span class="sev high">new</span>' if f["key"] in newkeys else ""
            w(f'<div class="finding"><span class="sev {sev}">{sev}</span>'
              f'<span class="kind">{e(f["kind"])}{" · " if f["where"] else ""}<span class="where">{e(f["where"])}</span>{new}</span>'
              f'<span class="msg">{md_code(f["message"])}{more}</span></div>')
        w("</div>")
    if accepted:
        w(f'<div class="group-title">accepted · {len(accepted)}</div>'
          '<p class="limits">Findings the project accepts, with its reason. They keep their severity and never fail a baseline.</p>'
          '<div class="findings">')
        for f in sorted(accepted, key=lambda f: (order.index(f["severity"]), f["key"])):
            others = [x for x in f.get("sites", []) if x != f["where"]]
            more = (f'<details><summary>{len(others)} more site{"s" if len(others) > 1 else ""}</summary>'
                    f'<div class="taglist">{"".join(f"<span>{e(x)}</span>" for x in others)}</div></details>') if others else ""
            w(f'<div class="finding accepted"><span class="sev low">accepted</span>'
              f'<span class="kind">{e(f["kind"])} · {e(f["severity"])}{" · " if f["where"] else ""}<span class="where">{e(f["where"])}</span></span>'
              f'<span class="msg">{md_code(f["message"])}{more}<span class="reason">{md_code(f["accepted"])}</span></span></div>')
        w("</div>")
    w("</section>")

    # Entries
    w('<section id="entries"><h2>Entry points</h2>')
    w("<p>Every way Postgres calls into the library, what an ERROR or panic escaping it becomes, and what it can reach. "
      "Unhandled counts are sites reachable from the entry along a path with no error handler.</p>")
    w('<div class="tablebox"><table><thead><tr><th>Entry</th><th>Kind</th><th>Guard</th><th>If an ERROR escapes</th>'
      '<th>Reach</th><th>Unsafe</th><th>FFI sites (unhandled)</th><th>Panic sites (unhandled)</th><th>Worst direct effect, unhandled</th></tr></thead><tbody>')
    for en in r["entries"]:
        reach = r["entry_reach"][en["id"]]
        ids = list(reach)
        unsafe_n = sum(1 for u in r["unsafe"] if u["fn"] in reach)
        fsites = [s for s in r["sites"] if s["fn"] in reach]
        fun = [s for s in fsites if not s["handled"] and not reach[s["fn"]]]
        pan = sum(len(crate.fns[n]["panics"]) for n in ids)
        pun = sum(1 for n in ids if not reach[n] for p in crate.fns[n]["panics"] if not p["_handled"])
        worst = None
        for s in fun:
            for eff, v in s["effects"].items():
                if eff in SHOWN[:7] and v.get("hops", v["depth"]) <= dl:
                    if worst is None or SHOWN.index(eff) < SHOWN.index(worst[0]) or (eff == worst[0] and v["depth"] < worst[1]["depth"]):
                        worst = (eff, v)
        guard = "pg_guard" if en["guarded"] else ('<span class="no">none</span>' if en["abi"] else "n/a")
        w(f'<tr><td class="sym">{e(en["id"].removeprefix("crate::"))}</td><td>{e(en["kind"])}</td><td>{guard}</td>'
          f'<td>{e(kinds.get(en["kind"], {}).get("failure", ""))}</td><td class="num">{len(ids)}</td><td class="num">{unsafe_n}</td>'
          f'<td class="num">{len(fsites)} ({len(fun)})</td><td class="num">{pan} ({pun})</td>'
          f'<td>{chip(worst[0], worst[1], dl) + " <code>" + e(worst[1]["via"]) + "</code>" if worst else ""}</td></tr>')
    w("</tbody></table></div></section>")

    # Sites
    w('<section id="sites"><h2>FFI call sites</h2>')
    w(f"<p>Every call from Rust into the server. Chips read <code>EFFECT@depth</code>: depth is the number of calls inside the "
      f"server between the called function and the one that raises or performs the effect. Faded chips with <code>~</code> "
      f"reach more than {dl} source module(s) beyond the called function. Hover a chip, or open a row, for the call chain "
      f"that proves it, with the source line of every call.</p>")
    w('<div class="legend">')
    for eff, desc in [("PANIC(io)", "PANIC chosen by an I/O failure"), ("PANIC", "invariant PANIC"), ("FATAL", ""),
                      ("ERROR", ""), ("fsync", "fsync / fd I/O"), ("blocks", "waits or locks"), ("exit", "")]:
        w(f'<span><span class="chip {SEV_COLOR[eff]}">{e(eff)}</span>{e(desc)}</span>')
    w("</div>")
    w('<div class="toolbar"><label><input type="checkbox" id="exposed-only" checked> Reachable outside a handler only</label>'
      '<input type="search" id="site-q" placeholder="Filter by symbol, file or function" aria-label="Filter sites">'
      '<span class="count" id="site-count"></span></div>')
    w('<div class="tablebox"><table id="sites"><thead><tr><th>Site</th><th>Calls</th><th>Handler</th><th>Reached from</th><th>Server effects</th></tr></thead><tbody>')
    rank = lambda s: (not s["exposed"], min((SHOWN.index(x) for x, v in s["effects"].items() if x in SHOWN and v.get("hops", v["depth"]) <= dl), default=99), s["file"], s["line"])
    for s in sorted(r["sites"], key=rank):
        name = "+".join(s["syms"]) if not s["via"].startswith("pgrx:") else s["via"][5:]
        state = ('<span class="state handled">inside</span>' if s["handled"] else
                 '<span class="state exposed">none</span>' if s["exposed"] else '<span class="state callers">by callers</span>')
        ents = collections.Counter(kinds_fn(r, x) for x in s["entries_all"])
        reached = ", ".join(f"{k}{' ×' + str(v) if v > 1 else ''}" for k, v in sorted(ents.items())) or "no entry"
        effs = [(x, v) for x, v in s["effects"].items() if x in SHOWN]
        chips = "".join(chip(x, v, dl) for x, v in effs if v.get("hops", v["depth"]) <= dl or x in SHOWN[:5])
        wit = "".join(f'<div class="eff">{chip(x, v, dl)}{chain(v)}</div>' for x, v in effs if x in SHOWN[:7])
        det = f'<details><summary>Call chains</summary><div class="witness">{wit}</div></details>' if wit else ""
        w(f'<tr data-exposed="{1 if s["exposed"] else 0}"><td class="site">{e(s["file"])}:{s["line"]}<br><span class="fn">{e(s["fn"].removeprefix("crate::"))}</span></td>'
          f'<td class="sym">{e(name)}</td><td>{state}</td><td>{e(reached)}</td><td><div class="chips">{chips}</div>{det}</td></tr>')
    w("</tbody></table></div></section>")

    # Unsafe
    w('<section id="unsafe"><h2>Unsafe blocks</h2>')
    w(f"<p>{len(blocks)} unsafe blocks and {len(r['unsafe']) - len(blocks)} unsafe fn bodies, with the operations each performs. "
      "A SAFETY comment counts on the block's line, above its statement, or on its first line.</p>")
    w('<div class="tablebox"><table><thead><tr><th>Site</th><th>Function</th><th>Kind</th><th>SAFETY</th><th>Operations</th><th>Server calls</th></tr></thead><tbody>')
    for u in sorted(r["unsafe"], key=lambda u: (u["file"], u["line"])):
        ops = ", ".join(f"{k} ×{v}" for k, v in sorted(u["ops"].items()))
        w(f'<tr><td class="site">{e(u["file"])}:{u["line"]}</td><td class="sym">{e(u["fn"].split("::")[-1])}</td><td>{e(u["kind"])}</td>'
          f'<td>{"yes" if u["safety"] else "<span class=no>no</span>"}</td><td>{e(ops)}</td><td class="sym">{e(", ".join(u["ffi"]))}</td></tr>')
    w("</tbody></table></div></section>")

    # Linkage
    w('<section id="linkage"><h2>Linkage</h2>')
    w("<p>What <code>nm -D</code> finds in the built library, checked against the source.</p><div class=\"cols\">")
    w(f'<div class="panel"><h3>Exports</h3><p>{len(cfx["exports"])} exported symbols; '
      f'{"none" if not r["unmapped_exports"] else ", ".join(r["unmapped_exports"])} unexplained by the source; '
      f'{"none" if not r["missing_exports"] else ", ".join(r["missing_exports"])} marked for export but absent.</p></div>')
    w(f'<div class="panel"><h3>Called in source, not linked</h3><p>pgrx implements these in Rust or they are header inlines, so the server model has nothing on them.</p>'
      f'<div class="taglist">{"".join(f"<span>{e(x)}</span>" for x in r["not_linked"]) or "<span>none</span>"}</div></div>')
    w(f'<div class="panel"><h3>Server functions reached only through pgrx</h3><div class="taglist">{"".join(f"<span>{e(x)}</span>" for x in r["via_pgrx"])}</div></div>')
    from report import libc_groups  # noqa: E402 (import cycle avoided at module load)
    groups = libc_groups(cfx["undefined_non_postgres"])
    w('<div class="panel"><h3>Non-server symbols linked</h3><p>libc and the Rust runtime. Linked is not the same as called.</p>')
    for g in ("process", "files", "network", "threads", "memory", "unwind", "other"):
        if groups.get(g):
            w(f'<div class="group-title">{g}</div><div class="taglist">{"".join(f"<span>{e(x)}</span>" for x in groups[g])}</div>')
    w("</div></div></section>")

    if len(results) > 1:
        w('<section id="versions"><h2>PG versions</h2><p>Server functions whose direct effects differ between versions.</p>')
        syms = sorted({s for x in results for site in x["sites"] for s in site["syms"]})
        rows = []
        for sname in syms:
            cells = []
            for x in results:
                d = x["cfx"]["symbols"].get(sname)
                if not d:
                    cells.append(("not used", None))
                elif not d.get("defined"):
                    cells.append(("absent", None))
                else:
                    effs = {k: v for k, v in d["effects"].items() if v.get("hops", v["depth"]) <= dl and k in SHOWN[:9]}
                    cells.append((",".join(f"{k}@{v['depth']}" for k, v in effs.items()), effs))
            if len({c[0] for c in cells}) > 1:
                rows.append((sname, cells))
        w('<div class="tablebox"><table><thead><tr><th>Symbol</th>' + "".join(f"<th>PG {e(x['pg'])}</th>" for x in results) + "</tr></thead><tbody>")
        for sname, cells in rows:
            tds = "".join(f'<td><div class="chips">{"".join(chip(k, v, dl) for k, v in c[1].items()) if c[1] else e(c[0])}</div></td>' for c in cells)
            w(f'<tr><td class="sym">{e(sname)}</td>{tds}</tr>')
        w("</tbody></table></div></section>")

    w('<section id="limits"><h2>Limits</h2><div class="limits">')
    w("<p>Rust calls resolve by name, so trait dispatch over-approximates and common std method names are not followed. "
      "The server model is flow-insensitive: a path that exists only in single-user mode or behind a setting still counts, "
      "except edges listed in <code>cut_edges</code>. Indirect calls resolve to address-taken functions with the same "
      f"signature when there are at most {e(cfx.get('max_indirect_targets', 12)) if 'max_indirect_targets' in cfx else 12} candidates; "
      "the rest are counted, not followed. Panic sites are syntactic, and integer overflow is not included.</p>")
    w("</div></section>")
    w("</div>")
    w(f"<script>{JS}</script>")
    return "\n".join(out)


def md_code(s):
    """Render `code` spans in a finding message."""
    parts = str(s).split("`")
    return "".join(f"<code>{e(p)}</code>" if i % 2 else e(p) for i, p in enumerate(parts))
