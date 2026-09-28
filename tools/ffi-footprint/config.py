#!/usr/bin/env python3
"""Config plumbing for the driver. Stdlib only (tomllib: Python 3.11+).

    config.py merge PROJECT.toml MERGED.json SCANNER.json SERVER.json   prints shell assignments
    config.py symbols MERGED.json FACTS.json                prints C symbols to analyze
"""
import json
import os
import shlex
import sys
import tomllib

HERE = os.path.dirname(os.path.abspath(__file__))


def deep_merge(base, over):
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def merge(project_path, merged_path, scanner_path, server_path):
    with open(os.path.join(HERE, "defaults.toml"), "rb") as f:
        conf = tomllib.load(f)
    with open(project_path, "rb") as f:
        conf = deep_merge(conf, tomllib.load(f))
    p = conf["project"]
    root = os.path.dirname(os.path.abspath(project_path))
    p["crate"] = os.path.normpath(os.path.join(root, p["crate"]))
    p["root"] = root
    if not p.get("lib"):
        sys.exit("ffi-footprint: [project] lib is required")
    with open(merged_path, "w") as f:
        json.dump(conf, f, indent=1)
    s = conf["scanner"]
    with open(scanner_path, "w") as f:
        json.dump({k: v for k, v in s.items() if k != "cfg"}, f, indent=1)
    with open(server_path, "w") as f:
        json.dump(conf.get("server", {}), f, indent=1)
    shell = {
        "FF_ROOT": root,
        "FF_CRATE": p["crate"],
        "FF_LIB": p["lib"],
        "FF_PG": " ".join(str(x) for x in p["pg"]),
        "FF_FEATURES": ",".join(p["features"]),
        "FF_IMAGE": p.get("image", ""),
        "FF_BUILD": p.get("build", ""),
        "FF_CFG": ",".join(s.get("cfg", [])),
        "FF_BITCODE": p.get("bitcode", "source"),
    }
    for k, v in shell.items():
        print(f"{k}={shlex.quote(v)}")


def symbols(merged_path, facts_path):
    with open(merged_path) as f:
        conf = json.load(f)
    with open(facts_path) as f:
        facts = json.load(f)
    out = set()
    for fn in facts["functions"]:
        for c in fn["calls"]:
            if c.get("ffi"):
                out.add(c["ffi"])
    for syms in conf.get("pgrx_api", {}).values():
        out.update(syms)
    print("\n".join(sorted(out)))


if __name__ == "__main__":
    cmd, *args = sys.argv[1:]
    {"merge": merge, "symbols": symbols}[cmd](*args)
