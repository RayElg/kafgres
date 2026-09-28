#!/usr/bin/env python3
"""Regression test for the scanner and the report, no Docker or Postgres needed: scans
tests/fixture with the real scanner, pairs it with a synthetic server model, and checks
the classification.

    python3 tools/ffi-footprint/tests/test_ffi_footprint.py
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.dirname(HERE)
SCANNER = os.path.join(TOOL, "scanner", "target", "release", "ffi-footprint-scanner")

CFX = {
    "pg": "16", "elevels": {"ERROR": 21, "FATAL": 22, "PANIC": 23}, "elevels_calibrated": True,
    "exports": ["_PG_init", "_PG_output_plugin_init", "fx_worker_main", "fx_unguarded",
                "fx_produce_wrapper", "pg_finfo_fx_produce_wrapper", "Pg_magic_func", "stray"],
    "imports": ["FileSync", "FileClose"], "imports_data": [], "undefined_non_postgres": ["fork"],
    "stats": {"functions": 3, "address_taken": 0, "indirect_sites": 0, "indirect_resolved": 0,
              "indirect_unresolved": 0},
    "errors": [],
    "symbols": {
        "FileSync": {"defined": True, "module": "storage/file/fd", "effects": {
            "PANIC(io)": {"depth": 2, "hops": 0, "path": ["FileSync", "FileAccess", "LruDelete"],
                          "detail": "errstart(PANIC) via data_sync_elevel", "module": "storage/file/fd"},
            "fsync": {"depth": 0, "path": ["FileSync"], "detail": "calls fsync", "module": "storage/file/fd"}}},
        "FileClose": {"defined": True, "module": "storage/file/fd", "effects": {
            "PANIC(io)": {"depth": 0, "hops": 0, "path": ["FileClose"], "detail": "errstart(PANIC) via data_sync_elevel",
                          "module": "storage/file/fd"}}},
        "DefineCustomIntVariable": {"defined": True, "module": "utils/misc/guc", "effects": {
            "FATAL": {"depth": 1, "path": ["DefineCustomIntVariable", "init_custom_variable"],
                      "detail": "errstart_cold(FATAL)", "module": "utils/misc/guc"}}},
    },
}


def run(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


class FootprintTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["cargo", "build", "--release", "-q"], cwd=os.path.join(TOOL, "scanner"), check=True)
        cls.tmp = tempfile.mkdtemp()
        cfg = os.path.join(cls.tmp, "ffi-footprint.toml")
        with open(cfg, "w") as f:
            f.write(f'[project]\ncrate = "{os.path.join(HERE, "fixture")}"\nlib = "fx"\n'
                    '[accepted]\n"export-unmapped:str*" = "fixture: accepted on purpose"\n')
        t = lambda n: os.path.join(cls.tmp, n)  # noqa: E731
        run(sys.executable, os.path.join(TOOL, "config.py"), "merge", cfg, t("config.json"),
            t("scanner.json"), t("server.json"))
        facts = run(SCANNER, "--src", os.path.join(HERE, "fixture"), "--features", "pg16",
                    "--config", t("scanner.json"))
        with open(t("facts.json"), "w") as f:
            f.write(facts)
        with open(t("cfx.json"), "w") as f:
            json.dump(CFX, f)
        subprocess.run([sys.executable, os.path.join(TOOL, "report.py"), "--config", t("config.json"),
                        "--facts", t("facts.json"), "--cfx", t("cfx.json"), "--md", t("report.md"),
                        "--json", t("report.json")], check=True, capture_output=True)
        cls.facts = json.loads(facts)
        with open(t("report.json")) as f:
            cls.report = json.load(f)
        cls.keys = {f["key"] for f in cls.report["findings"]}

    def fn(self, suffix):
        return next(f for f in self.facts["functions"] if f["id"].endswith(suffix))

    def test_cfg_selects_the_feature(self):
        ids = {f["id"] for f in self.facts["functions"]}
        self.assertIn("crate::not_on_17", ids)
        self.assertNotIn("crate::only_on_17", ids)

    def test_entries_are_classified(self):
        kinds = {e["id"]: e["kind"] for e in self.report["entries"]}
        self.assertEqual(kinds["crate::_PG_init"], "library-load")
        self.assertEqual(kinds["crate::fx_worker_main"], "bgworker")
        self.assertEqual(kinds["crate::fx_produce"], "sql-function")
        self.assertEqual(kinds["crate::startup"], "output-plugin")
        self.assertEqual(kinds["crate::fx_produce::{closure@43}"], "xact-commit-callback")

    def test_unguarded_extern_functions(self):
        self.assertIn("unguarded-extern:crate::fx_unguarded", self.keys)
        self.assertIn("unguarded-extern:crate::startup", self.keys)
        self.assertNotIn("unguarded-extern:crate::fx_worker_main", self.keys)

    def test_handler_wrappers_are_inferred_transitively(self):
        sites = {s["syms"][0]: s for s in self.report["sites"]}
        self.assertFalse(sites["FileClose"]["exposed"], "close_handled runs under guarded()")
        self.assertTrue(sites["FileSync"]["exposed"])
        self.assertIn("ffi-panic-unhandled:FileSync", self.keys)
        self.assertNotIn("ffi-panic-unhandled:FileClose", self.keys)

    def test_safety_comments(self):
        self.assertIn("unsafe-no-safety:crate::storage::sync_unhandled#0", self.keys)
        self.assertNotIn("unsafe-no-safety:crate::storage::close_handled#0", self.keys)

    def test_panics_in_a_commit_callback(self):
        release = self.fn("storage::release")
        kinds = sorted(p["kind"] for p in release["panics"])
        self.assertEqual(kinds, ["div", "index"], "the clamp(1, ..) divisor is not a panic site")
        crit = [k for k in self.keys if k.startswith("critical-entry-panic:crate::fx_produce::{closure@43}")]
        self.assertEqual(len(crit), 2)

    def test_linkage(self):
        self.assertIn("export-unmapped:stray", self.keys)
        self.assertNotIn("export-unmapped:fx_produce_wrapper", self.keys)

    def test_accepted_findings_keep_their_reason(self):
        f = next(f for f in self.report["findings"] if f["key"] == "export-unmapped:stray")
        self.assertEqual(f["accepted"], "fixture: accepted on purpose")
        others = [f for f in self.report["findings"] if f["key"] != "export-unmapped:stray"]
        self.assertFalse(any(f.get("accepted") for f in others))


IR = r'''
%struct.Methods = type { ptr, ptr }
%struct.Ctx = type { i32, ptr }

@methods_a = internal constant %struct.Methods { ptr @a_alloc, ptr @a_free }, align 8
@demo_hook = global ptr null, align 8

define internal ptr @a_alloc(ptr noundef %0) {
  ret ptr null
}

define internal void @a_free(ptr noundef %0) {
  ret void
}

define void @do_free(ptr noundef %0) !dbg !12 {
  %2 = alloca ptr, align 8
  store ptr %0, ptr %2, align 8
  %3 = load ptr, ptr %2, align 8, !dbg !13
  %4 = getelementptr inbounds %struct.Ctx, ptr %3, i32 0, i32 1, !dbg !13
  %5 = load ptr, ptr %4, align 8, !dbg !13
  %6 = getelementptr inbounds %struct.Methods, ptr %5, i32 0, i32 1, !dbg !13
  %7 = load ptr, ptr %6, align 8, !dbg !13
  call void %7(ptr noundef %3), !dbg !13
  %8 = load ptr, ptr @demo_hook, align 8, !dbg !14
  call void %8(), !dbg !14
  %9 = call i32 @data_sync_elevel(i32 noundef 15), !dbg !15
  %10 = call i1 @errstart_cold(i32 noundef %9, ptr noundef null), !dbg !15
  ret void
}

define void @register(ptr noundef %0) {
  %2 = alloca ptr, align 8
  store ptr %0, ptr %2, align 8
  %3 = load ptr, ptr %2, align 8
  store ptr %3, ptr @demo_hook, align 8
  ret void
}

define void @caller() {
  call void @register(ptr noundef @a_free)
  ret void
}

declare i32 @data_sync_elevel(i32)
declare i1 @errstart_cold(i32, ptr)

!1 = !DIFile(filename: "demo.c", directory: "/src")
!12 = distinct !DISubprogram(name: "do_free", scope: !1, file: !1, line: 10)
!13 = !DILocation(line: 12, column: 3, scope: !12)
!14 = !DILocation(line: 13, column: 3, scope: !12)
!15 = !DILocation(line: 14, column: 3, scope: !12)
!20 = !DICompositeType(tag: DW_TAG_structure_type, name: "Methods", file: !1, line: 1, size: 128, elements: !21)
!21 = !{!22, !23}
!22 = !DIDerivedType(tag: DW_TAG_member, name: "alloc", scope: !20, file: !1, line: 2, baseType: null, size: 64)
!23 = !DIDerivedType(tag: DW_TAG_member, name: "free_p", scope: !20, file: !1, line: 3, baseType: null, size: 64, offset: 64)
'''


class IrModelTest(unittest.TestCase):
    """The server-side resolver on a hand-written -O0-style module."""

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, TOOL)
        import cfx
        import irmodel
        cls.irmodel = irmodel
        cls.res = irmodel.Module("demo", IR, cfx.DEFAULTS).run()
        cls.held = irmodel.solve(cls.res["fnsets"], cls.res["copies"])
        cls.sites = cls.res["defs"]["do_free"]["indirect"]

    def test_a_call_through_a_struct_field_resolves_to_what_the_table_holds(self):
        terms = [tuple(t) for t in self.sites[0]["terms"]]
        self.assertEqual(terms, [("field", "%struct.Methods", 1)])
        self.assertEqual(self.held[terms[0]], {"demo::a_free"})
        self.assertEqual(self.sites[0]["loc"], "demo.c:12")

    def test_a_global_set_through_a_parameter_resolves(self):
        terms = [tuple(t) for t in self.sites[1]["terms"]]
        self.assertEqual(terms, [("global", "@demo_hook")])
        self.assertEqual(self.held[terms[0]], {"demo::a_free"}, "caller passes @a_free to register()")

    def test_io_elevel_and_member_names(self):
        local = self.res["defs"]["do_free"]["local"]
        self.assertIn("elevel:PANIC:io", local)
        self.assertEqual(local["elevel:PANIC:io"][1], "demo.c:14")
        self.assertEqual(self.res["members"]["%struct.Methods"], ["alloc", "free_p"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
