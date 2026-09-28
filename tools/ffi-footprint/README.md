# ffi-footprint

A mostly static report of a pgrx extension's FFI footprint in both directions: where the
server calls into Rust, where Rust calls into the server, what the server can do behind
each of those calls, and which of them run outside a Postgres error handler.

It combines three views, each of which covers the others' blind spots:

| layer | reads | answers |
|---|---|---|
| source (`scanner/`, Rust + `syn`) | the crate's source, `cfg`-evaluated per PG feature | entry points and their guards, call sites into `pg_sys`, handler regions, unsafe blocks and their operations, panic sites |
| server (`cfx.py`, `irmodel.py`) | the server backend's LLVM bitcode, built at `-O0 -g` from the upstream source of the installed version | for every server function the extension calls: can it raise ERROR, FATAL, PANIC; does it do file I/O, fsync, block, check interrupts, exit; with the call chain proving each, and the source line of every call in it |
| linkage (`cfx.py`, `nm -D`) | the built `.so` and the `postgres` binary | what the library really exports and imports, checked against the source |

`report.py` joins the three into `report.md`, `report.json` and a self-contained `report.html`.

## Running it

```sh
tools/ffi-footprint/ffi-footprint.sh                    # every [project] pg major
tools/ffi-footprint/ffi-footprint.sh --pg 13 --pg 18    # these majors
tools/ffi-footprint/ffi-footprint.sh --pg 16 --image kafgres-postgres   # existing image, no build
tools/ffi-footprint/ffi-footprint.sh --write-baseline ffi-footprint.baseline.json
tools/ffi-footprint/ffi-footprint.sh --baseline ffi-footprint.baseline.json   # exit 1 on new findings of a fail_on kind
python3 tools/ffi-footprint/tests/test_ffi_footprint.py # the tool's own test, no Docker
```

Output goes to `target/ffi-footprint/`. Requirements are bash and Docker; host `cargo`
and Python 3.11+ are used when present, containers otherwise. The server analysis runs in
a layer over the extension image (`Dockerfile` here) that adds the `llvm-dis` matching
the server's own libLLVM, so the bitcode is always readable.

### Server bitcode

With `bitcode = "source"` (the default), the analysis image downloads the upstream source
of the exact installed version and builds the backend's bitcode at `-O0 -g`
(`build-bitcode.sh`), once per image and needing network access. This resolves far more
function pointers, by the struct field they are loaded from, than the packaged `-O2` JIT
bitcode allows: at `-O2` LLVM rewrites most field accesses to byte offsets and the struct
type is gone, leaving only a signature match. It also gives every call in a witness chain
a file and line, for example
`FileWrite (fd.c:2159) > FileAccess (fd.c:1460) > … > LruDelete: errstart_cold(PANIC)
via data_sync_elevel at fd.c:1257`.

`bitcode = "package"` skips the build and uses the shipped `-O2` bitcode.

## Using it in another pgrx project

Copy `tools/ffi-footprint/` and write an `ffi-footprint.toml` at the project root:

```toml
[project]
crate = "."                 # directory with src/lib.rs
lib = "myext"               # myext.so
pg = [16, 17]
features = ["pg{pg}"]
image = "myext:pg{pg}"      # an image with the extension installed into a Debian/PGDG server
build = "docker build -q --build-arg PG_MAJOR={pg} -t {image} ."
```

Everything else has a pgrx default in `defaults.toml`. Tables merge key by key and lists
replace. Sections most likely to need project entries:

- `[handlers] roots`: calls that run a closure under an error handler. Wrapper functions
  that forward a parameter into one (`fn guarded(f) { with_sub(|| f()) }`) are inferred,
  transitively.
- `[pgrx_api]`: safe pgrx calls mapped to the server functions they enter, so their
  effects are reported like direct `pg_sys` calls.
- `[entries]`: symbol names, registrations and pointer escapes that make a function an
  entry, and `[entry_kinds]`: what an escaping ERROR or panic becomes in each.
- `[server]`: overrides for the server model in `cfx.py` (`DEFAULTS`): effect tables,
  sinks, elevel functions, edges to cut. Each key set here replaces that key's whole
  default, tables included.
- `[accepted]`: finding-key patterns (`fnmatch`, e.g. `"ffi-panic-unhandled:File*"`) mapped
  to the reason for accepting them. Accepted findings keep their severity, are listed
  separately with the reason, and never fail a baseline.

## Reading the report

Effects are written `EFFECT@depth`, where depth is the number of calls inside the server
between the called function and the one that raises or performs the effect; depth 0 means
the called function does it itself.

Distance, for what counts as direct, is measured in **source-module crossings**, not
calls: a chain that stays inside fd.c is at distance 0 however many helpers it passes
through, and a call from latch.c into fd.c costs 1. The search picks the chain with the
fewest crossings, then the fewest calls, keeping the ranking the same at `-O0`, where
nothing is inlined, and at `-O2`, where a file's helpers collapse into their callers.
`~` marks an effect more than `direct_hops` crossings away.

| effect | meaning |
|---|---|
| `PANIC(io)` | a PANIC whose level an I/O failure chooses: `data_sync_elevel`, or a select on `data_sync_retry`. With `data_sync_retry = off` (the default) a failed `close`, `fsync` or writeback in fd.c takes the whole server down |
| `PANIC` | a constant PANIC: an invariant inside the server (for example "cannot abort transaction, it was already committed") |
| `FATAL`, `ERROR` | constant elevels at `errstart`/`errstart_cold` |
| `error?` | an `ereport` whose level is computed and could not be resolved to constants |
| `fsync`, `fd-io` | reaches `fsync`/`fdatasync`/`sync_file_range`, or other file-descriptor calls |
| `blocks` | reaches `epoll_wait`, latches, LWLocks, heavyweight locks, sleeps |
| `interrupts` | reaches `ProcessInterrupts`, which can raise query-cancel ERROR or terminate FATAL |
| `exit` | reaches `proc_exit`/`abort` |
| `indirect?` | reaches a function-pointer call that was not followed: a dispatch point, an extension hook, or one neither method resolved (the detail says which) |

Indirect calls are resolved in this order:

| outcome | meaning |
|---|---|
| by struct field | the pointer is loaded from a struct field, global or parameter, and resolves to the functions stored there: constant tables (`@mcxt_methods`, `@heapam_methods`), stores (`planstate->ExecProcNode = ExecSeqScan`), and copies between them, solved as a field-based points-to analysis |
| dispatch point | the field holds more than `max_field_targets` functions, for example `FmgrInfo.fn_addr` with every SQL-callable builtin; named, not followed |
| extension hook | a global matching `hook_pattern` that nothing in core sets |
| by signature | the fallback: address-taken functions with the same LLVM signature, when there are at most `max_indirect_targets` |
| unresolved | the rest, mostly implementations that live outside the backend (FDWs, `libpqwalreceiver`, `src/port`) |

A site is **handled** when it sits lexically inside a handler region, **exposed** when some
entry reaches it along a path with no handler region, or no entry reaches it at all.

Finding kinds, with stable keys for the baseline:

| kind | severity | when |
|---|---|---|
| `unguarded-extern` | high | a function the server calls (exported, registered by name, or whose pointer escapes to C) has no `#[pg_guard]` |
| `ffi-panic-unhandled` | high | a server function the extension calls from an exposed site can reach one of `panic_effects` (default `PANIC(io)`) within `panic_hops`; one finding per server function, listing its sites |
| `critical-entry-error` / `critical-entry-panic` | high | an entry at `critical_severity` or above (xact callbacks: a panic there is a PANIC) reaches a raising FFI site or a Rust panic site outside any handler |
| `unsafe-no-safety` | medium | an `unsafe` block with no `SAFETY` comment on it, above its statement, or on its first line |
| `export-unmapped` | medium | the library exports a symbol no source function explains |
| `unsafe-fn-no-safety-doc`, `export-missing`, `c-abi-entry` | low | as named |
| `ffi-not-linked` | info | a `pg_sys` call the library does not import from the server: pgrx implements it in Rust, or it is a header inline |

## What it cannot see

The source layer resolves calls by name. Trait dispatch and method calls on receivers of
unknown type over-approximate, and methods named like common std methods
(`report.common_methods`) are not resolved at all. Function pointers built at runtime
inside the crate, and closures stored and called later other than through
`closure_entries`, are invisible to reachability.

The server layer is flow-insensitive: a path that exists in the code but only runs in
single-user mode, during recovery, or behind a GUC is still a path; `cut_edges` removes
known ones. The points-to analysis is field-based, so a call through a struct field
reaches everything ever stored in that field across every instance. Function pointers
returned from calls, and pointers into arrays reached by pointer arithmetic, are not
tracked. Only the backend is built, so code in `src/port`, `src/common` and loadable
modules (`libpqwalreceiver`, FDWs) has no effects here. Static inline functions from the
headers compile into the extension, not the server's bitcode; `ffi-not-linked` lists them.

Panic sites are syntactic: `unwrap`/`expect`, panicking macros, indexing, and division by
anything not provably non-zero. Integer overflow is not included; release builds wrap.
