//! Emits `kafgres-codec` from the vendored Kafka message schemas.
//!
//!     cargo run -p kafgres-codec-gen
//!
//! Output is checked into the repository on purpose. CI re-runs this and fails on a
//! non-empty `git diff`, which is what keeps the checked-in copy honest.

mod emit;
mod resolve;
mod schema;
mod versions;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use versions::Versions;

static KAFKA_VERSION: OnceLock<String> = OnceLock::new();

pub fn kafka_version() -> &'static str {
    KAFKA_VERSION.get().map(String::as_str).unwrap_or("unknown")
}

/// Produce advertises a min version it does not accept.
///
/// `ProduceRequest.json` says `validVersions: 3-13`, but real brokers put 0 in the
/// ApiVersions response to work around librdkafka bug KAFKA-18659 — upstream
/// `ApiKeys.java`, `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION = 0`. Advertising 3 here
/// breaks every librdkafka client while leaving the Java client working, which is a
/// miserable thing to debug.
const PRODUCE_API_KEY: i16 = 0;
const PRODUCE_ADVERTISED_MIN: i16 = 0;

/// What the ApiVersions response should claim for an API we implement over
/// `implemented`, given the schema's `valid` range and its unstable flag.
///
/// Returns `None` when nothing should be advertised at all.
fn advertised_range(
    api_key: i16,
    implemented: Versions,
    valid: Versions,
    latest_version_unstable: bool,
) -> Option<(i16, i16)> {
    if implemented.is_empty() {
        return None;
    }
    // Never advertise a version whose schema is still in flux; brokers gate these
    // behind `unstable.api.versions.enable` and a client that negotiates one sends
    // fields the handler has never modelled.
    let mut hi = implemented.hi.min(valid.hi);
    if latest_version_unstable && hi >= valid.hi {
        hi = valid.hi - 1;
    }
    if hi < implemented.lo {
        return None;
    }
    let lo = if api_key == PRODUCE_API_KEY {
        PRODUCE_ADVERTISED_MIN
    } else {
        implemented.lo
    };
    Some((lo, hi))
}

/// `codec/implemented.toml` — the single hand-maintained list of what we serve.
/// Deliberately tiny; a full TOML parser would be more machinery than the format needs.
fn load_implemented(path: &Path) -> Result<BTreeMap<String, Versions>, String> {
    let mut out = BTreeMap::new();
    if !path.exists() {
        return Ok(out);
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap().trim();
        if line.is_empty() || line.starts_with('[') {
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| format!("{}:{}: expected `Name = \"range\"`", path.display(), n + 1))?;
        let v = v.trim().trim_matches('"');
        let range = Versions::parse(v).map_err(|e| format!("{}:{}: {e}", path.display(), n + 1))?;
        out.insert(k.trim().to_string(), range);
    }
    Ok(out)
}

fn main() {
    if let Err(e) = run() {
        eprintln!("codec-gen: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let codec = root.join("codec");
    let schemas = codec.join("schemas");
    let out_dir = codec.join("src").join("generated");

    let version = std::fs::read_to_string(codec.join("KAFKA_VERSION"))
        .map_err(|e| format!("KAFKA_VERSION: {e}"))?
        .trim()
        .to_string();
    KAFKA_VERSION.set(version.clone()).ok();

    let implemented = load_implemented(&codec.join("implemented.toml"))?;

    let mut paths: Vec<PathBuf> = std::fs::read_dir(&schemas)
        .map_err(|e| format!("{}: {e}", schemas.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    paths.sort();

    // Every schema must parse, including the internal record types we do not emit
    // code for. A parse failure is the earliest warning a version bump can give.
    let mut messages = Vec::new();
    let mut parsed = 0usize;
    for path in &paths {
        let spec = schema::load(path)?;
        parsed += 1;
        if spec.kind != "request" && spec.kind != "response" {
            continue;
        }
        // APIs removed in Kafka 4.0 declare `validVersions: "none"`; there is no version to
        // encode, so nothing is emitted. Upstream's generator skips them the same way.
        if Versions::parse(&spec.valid_versions)?.is_empty() {
            continue;
        }
        let module = resolve::module_name(&spec.name);
        let msg = resolve::resolve(&spec, module).map_err(|e| format!("{}: {e}", spec.name))?;
        messages.push(msg);
    }

    if out_dir.exists() {
        // Replace wholesale — a schema deleted upstream must not leave a stale module
        // behind, still compiling and still wrong.
        std::fs::remove_dir_all(&out_dir).map_err(|e| format!("{}: {e}", out_dir.display()))?;
    }
    std::fs::create_dir_all(&out_dir).map_err(|e| format!("{}: {e}", out_dir.display()))?;

    for msg in &messages {
        let code = emit::Emitter::new(msg).emit();
        let file = out_dir.join(format!("{}.rs", msg.module));
        std::fs::write(&file, code).map_err(|e| format!("{}: {e}", file.display()))?;
    }

    write_mod_rs(&out_dir, &messages)?;
    let advertised = write_apis_rs(&out_dir, &messages, &implemented, &version)?;

    eprintln!(
        "codec-gen: Kafka {version} — {parsed} schemas parsed, {} messages emitted, \
         {} APIs implemented, {advertised} advertised",
        messages.len(),
        implemented.len(),
    );
    Ok(())
}

fn write_mod_rs(out_dir: &Path, messages: &[resolve::Message]) -> Result<(), String> {
    let mut s = String::new();
    writeln!(s, "// @generated by kafgres-codec-gen. DO NOT EDIT.").unwrap();
    writeln!(
        s,
        "//! One module per Kafka message schema. Struct names collide across messages by\n\
         //! design (many define their own `TopicData`), so nothing is glob re-exported."
    )
    .unwrap();
    s.push('\n');
    writeln!(s, "pub mod apis;").unwrap();
    for m in messages {
        writeln!(s, "pub mod {};", m.module).unwrap();
    }
    s.push('\n');

    // A registry the conformance suite can fold over: a new schema is covered by the tests
    // the moment it is generated, with no test file to remember to update.
    writeln!(
        s,
        "/// Invokes `$m!(Type)` once per generated top-level message.\n\
         #[macro_export]\n\
         macro_rules! for_each_message {{\n    ($m:ident) => {{"
    )
    .unwrap();
    for m in messages {
        writeln!(
            s,
            "        $m!($crate::generated::{}::{});",
            m.module, m.name
        )
        .unwrap();
    }
    writeln!(s, "    }};\n}}").unwrap();

    std::fs::write(out_dir.join("mod.rs"), s).map_err(|e| e.to_string())
}

fn write_apis_rs(
    out_dir: &Path,
    messages: &[resolve::Message],
    implemented: &BTreeMap<String, Versions>,
    version: &str,
) -> Result<usize, String> {
    let mut by_key: BTreeMap<i16, &resolve::Message> = BTreeMap::new();
    for m in messages {
        if m.kind != "request" {
            continue;
        }
        if let Some(k) = m.api_key {
            by_key.insert(k, m);
        }
    }

    // Reject an implemented range the schemas cannot support. This check is the reason
    // implemented.toml is read by the generator rather than by a handler.
    let mut impl_by_key: BTreeMap<i16, Versions> = BTreeMap::new();
    for (name, range) in implemented {
        let (key, req) = by_key
            .iter()
            .find(|(_, req)| req.name.trim_end_matches("Request") == name)
            .ok_or_else(|| format!("implemented.toml names unknown API {name:?}"))?;
        if !req.valid.covers(*range) {
            return Err(format!(
                "implemented.toml: {name} = {range:?} exceeds the schema's validVersions {:?}",
                req.valid
            ));
        }
        impl_by_key.insert(*key, *range);
    }

    let mut s = String::new();
    writeln!(
        s,
        "// @generated by kafgres-codec-gen from Kafka {version}. DO NOT EDIT."
    )
    .unwrap();
    writeln!(
        s,
        "//! API key table, and what the ApiVersions response should advertise."
    )
    .unwrap();
    s.push('\n');
    writeln!(s, "use crate::prelude::*;").unwrap();
    s.push('\n');
    writeln!(
        s,
        "/// Everything the vendored schemas define, whether or not we serve it."
    )
    .unwrap();
    writeln!(s, "pub static SCHEMA_APIS: &[ApiSpec] = &[").unwrap();
    for (key, req) in &by_key {
        writeln!(
            s,
            "    ApiSpec {{ key: {}, name: {:?}, valid: VersionRange::new({}, {}), \
             flexible: VersionRange::new({}, {}), latest_version_unstable: {} }},",
            key,
            req.name.trim_end_matches("Request"),
            req.valid.lo,
            req.valid.hi,
            req.flexible.lo,
            req.flexible.hi,
            req.latest_version_unstable
        )
        .unwrap();
    }
    writeln!(s, "];").unwrap();
    s.push('\n');

    writeln!(
        s,
        "/// What ApiVersions advertises. Derived from codec/implemented.toml, then adjusted:\n\
         /// Produce is floored to 0 (KAFKA-18659) and an unstable latest version is never\n\
         /// offered. Deriving this from the same table the dispatcher uses is what stops the\n\
         /// two drifting apart."
    )
    .unwrap();
    writeln!(s, "pub static ADVERTISED: &[ApiVersionRange] = &[").unwrap();
    let mut advertised = 0usize;
    for (key, req) in &by_key {
        let imp = impl_by_key.get(key).copied().unwrap_or(versions::NONE);
        if let Some((lo, hi)) = advertised_range(*key, imp, req.valid, req.latest_version_unstable)
        {
            writeln!(
                s,
                "    ApiVersionRange {{ api_key: {key}, min_version: {lo}, max_version: {hi} }},"
            )
            .unwrap();
            advertised += 1;
        }
    }
    writeln!(s, "];").unwrap();
    s.push('\n');
    if advertised == 0 {
        writeln!(
            s,
            "// Empty in phase 0: no handlers exist yet, so we advertise nothing and every\n\
             // client sees an empty ApiVersions response. Phase 1 adds ApiVersions and\n\
             // Metadata to codec/implemented.toml and this table fills itself in."
        )
        .unwrap();
        s.push('\n');
    }

    writeln!(s, "impl ApiSpec {{").unwrap();
    writeln!(
        s,
        "    pub fn lookup(api_key: i16) -> Option<&'static ApiSpec> {{"
    )
    .unwrap();
    writeln!(s, "        SCHEMA_APIS.iter().find(|a| a.key == api_key)").unwrap();
    writeln!(s, "    }}").unwrap();
    writeln!(s, "}}").unwrap();

    std::fs::write(out_dir.join("apis.rs"), s).map_err(|e| e.to_string())?;
    Ok(advertised)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Versions {
        Versions::parse(s).unwrap()
    }

    #[test]
    fn nothing_implemented_advertises_nothing() {
        assert_eq!(advertised_range(3, versions::NONE, v("0-13"), false), None);
    }

    #[test]
    fn produce_is_floored_to_zero() {
        // Schema says 3-13 and we implement 3-9, but librdkafka must see min 0.
        assert_eq!(
            advertised_range(PRODUCE_API_KEY, v("3-9"), v("3-13"), false),
            Some((0, 9))
        );
    }

    #[test]
    fn other_apis_are_not_floored() {
        assert_eq!(
            advertised_range(1, v("4-12"), v("4-18"), false),
            Some((4, 12))
        );
    }

    #[test]
    fn unstable_latest_version_is_withheld() {
        // InitProducerId: valid 0-6 with v6 unstable. Implementing "everything" must
        // still advertise at most 5.
        assert_eq!(advertised_range(22, v("0-6"), v("0-6"), true), Some((0, 5)));
        // Implementing below the unstable top is unaffected.
        assert_eq!(advertised_range(22, v("0-4"), v("0-6"), true), Some((0, 4)));
    }

    #[test]
    fn implemented_range_is_clamped_to_schema() {
        assert_eq!(
            advertised_range(3, v("0-99"), v("0-13"), false),
            Some((0, 13))
        );
    }
}
