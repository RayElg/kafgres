//! Schema → Rust type resolution, struct collection, and identifier naming.

use crate::schema::{FieldSpec, MessageSpec};
use crate::versions::Versions;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint16,
    Uint32,
    Float64,
    Str,
    Uuid,
    Bytes,
    Records,
    Struct(String),
    Array(Box<FieldType>),
}

impl FieldType {
    pub fn parse(s: &str) -> Result<Self, String> {
        if let Some(inner) = s.strip_prefix("[]") {
            return Ok(FieldType::Array(Box::new(FieldType::parse(inner)?)));
        }
        Ok(match s {
            "bool" => FieldType::Bool,
            "int8" => FieldType::Int8,
            "int16" => FieldType::Int16,
            "int32" => FieldType::Int32,
            "int64" => FieldType::Int64,
            "uint16" => FieldType::Uint16,
            "uint32" => FieldType::Uint32,
            "float64" => FieldType::Float64,
            "string" => FieldType::Str,
            "uuid" => FieldType::Uuid,
            "bytes" => FieldType::Bytes,
            "records" => FieldType::Records,
            other => {
                if other.is_empty() || !other.chars().next().unwrap().is_ascii_uppercase() {
                    // An unrecognised lowercase name is a new primitive we do not model: fail
                    return Err(format!("unknown primitive type {other:?}"));
                }
                FieldType::Struct(other.to_string())
            }
        })
    }

    pub fn rust_type(&self) -> String {
        match self {
            FieldType::Bool => "bool".into(),
            FieldType::Int8 => "i8".into(),
            FieldType::Int16 => "i16".into(),
            FieldType::Int32 => "i32".into(),
            FieldType::Int64 => "i64".into(),
            FieldType::Uint16 => "u16".into(),
            FieldType::Uint32 => "u32".into(),
            FieldType::Float64 => "f64".into(),
            FieldType::Str => "String".into(),
            FieldType::Uuid => "Uuid".into(),
            // Bytes aliases the connection read buffer rather than copying it.
            FieldType::Bytes | FieldType::Records => "Bytes".into(),
            FieldType::Struct(n) => n.clone(),
            FieldType::Array(inner) => format!("Vec<{}>", inner.rust_type()),
        }
    }
}

/// A field, resolved.
pub struct Field {
    pub name: String,
    pub rust_name: String,
    pub ty: FieldType,
    pub versions: Versions,
    pub nullable: Versions,
    pub tagged: Versions,
    /// Field-level flexibility override, if the schema gave one.
    pub flexible: Option<Versions>,
    pub tag: Option<i32>,
    pub default: Option<serde_json::Value>,
    pub ignorable: bool,
    pub about: Option<String>,
}

impl Field {
    /// The declared type is `Option<T>` when the field is nullable in any version we
    pub fn is_optional(&self, valid: Versions) -> bool {
        !self.nullable.intersect(valid).is_empty()
    }

    pub fn declared_type(&self, valid: Versions) -> String {
        if self.is_optional(valid) {
            format!("Option<{}>", self.ty.rust_type())
        } else {
            self.ty.rust_type()
        }
    }
}

pub struct Struct {
    pub name: String,
    pub fields: Vec<Field>,
}

pub struct Message {
    pub name: String,
    pub module: String,
    pub api_key: Option<i16>,
    pub kind: String,
    pub valid: Versions,
    pub flexible: Versions,
    pub latest_version_unstable: bool,
    /// Top-level struct first, then nested structs in definition order.
    pub structs: Vec<Struct>,
}

fn resolve_field(f: &FieldSpec) -> Result<Field, String> {
    let ty = FieldType::parse(&f.kind).map_err(|e| format!("field {}: {e}", f.name))?;
    let versions = Versions::parse(&f.versions).map_err(|e| format!("field {}: {e}", f.name))?;
    let nullable = match &f.nullable_versions {
        Some(s) => Versions::parse(s).map_err(|e| format!("field {}: {e}", f.name))?,
        None => crate::versions::NONE,
    };
    let tagged = match &f.tagged_versions {
        Some(s) => Versions::parse(s).map_err(|e| format!("field {}: {e}", f.name))?,
        None => crate::versions::NONE,
    };
    let flexible = match &f.flexible_versions {
        Some(s) => Some(Versions::parse(s).map_err(|e| format!("field {}: {e}", f.name))?),
        None => None,
    };
    if tagged.is_empty() != f.tag.is_none() {
        return Err(format!(
            "field {}: tag and taggedVersions must be set together",
            f.name
        ));
    }
    Ok(Field {
        rust_name: snake_case(&f.name),
        name: f.name.clone(),
        ty,
        versions,
        nullable,
        tagged,
        flexible,
        tag: f.tag,
        default: f.default.clone(),
        ignorable: f.ignorable,
        about: f.about.clone(),
    })
}

/// Walk a field list, appending any struct definitions found to `out`.
fn collect(
    fields: &[FieldSpec],
    out: &mut Vec<Struct>,
    seen: &mut BTreeMap<String, usize>,
) -> Result<Vec<Field>, String> {
    let mut resolved = Vec::new();
    for f in fields {
        let rf = resolve_field(f)?;
        let struct_name = match &rf.ty {
            FieldType::Struct(n) => Some(n.clone()),
            FieldType::Array(inner) => match &**inner {
                FieldType::Struct(n) => Some(n.clone()),
                _ => None,
            },
            _ => None,
        };
        if let Some(name) = struct_name {
            if !f.fields.is_empty() {
                if seen.contains_key(&name) {
                    return Err(format!("struct {name} defined twice"));
                }
                // Reserve the slot before recursing so a self-referential type
                seen.insert(name.clone(), out.len());
                out.push(Struct {
                    name: name.clone(),
                    fields: Vec::new(),
                });
                let idx = seen[&name];
                let inner = collect(&f.fields, out, seen)?;
                out[idx].fields = inner;
            } else if !seen.contains_key(&name) {
                return Err(format!(
                    "field {} references struct {name}, which is never defined",
                    f.name
                ));
            }
        } else if !f.fields.is_empty() {
            return Err(format!(
                "field {} has nested fields but is not a struct",
                f.name
            ));
        }
        resolved.push(rf);
    }
    Ok(resolved)
}

pub fn resolve(spec: &MessageSpec, module: String) -> Result<Message, String> {
    let valid = Versions::parse(&spec.valid_versions)?;
    let flexible = match &spec.flexible_versions {
        Some(s) => Versions::parse(s)?,
        None => crate::versions::NONE,
    };

    let mut structs = Vec::new();
    let mut seen = BTreeMap::new();

    // commonStructs are defined up front so inline references can find them.
    seen.insert(spec.name.clone(), 0usize);
    structs.push(Struct {
        name: spec.name.clone(),
        fields: Vec::new(),
    });
    for cs in &spec.common_structs {
        if seen.contains_key(&cs.name) {
            return Err(format!("common struct {} collides", cs.name));
        }
        seen.insert(cs.name.clone(), structs.len());
        structs.push(Struct {
            name: cs.name.clone(),
            fields: Vec::new(),
        });
    }
    for cs in &spec.common_structs {
        let idx = seen[&cs.name];
        let fields = collect(&cs.fields, &mut structs, &mut seen)?;
        structs[idx].fields = fields;
    }

    let top = collect(&spec.fields, &mut structs, &mut seen)?;
    structs[0].fields = top;

    Ok(Message {
        name: spec.name.clone(),
        module,
        api_key: spec.api_key,
        kind: spec.kind.clone(),
        valid,
        flexible,
        latest_version_unstable: spec.latest_version_unstable,
        structs,
    })
}

// ---------------------------------------------------------------------------

const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for",
    "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
    "self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use", "where",
    "while", "async", "await", "dyn", "abstract", "become", "box", "do", "final", "macro",
    "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
];

/// `PascalCase` / `camelCase` → `snake_case`, handling acronym runs so that
pub fn snake_case(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len() + 4);
    for i in 0..chars.len() {
        let c = chars[i];
        if c.is_ascii_uppercase() {
            let prev_lower =
                i > 0 && (chars[i - 1].is_ascii_lowercase() || chars[i - 1].is_ascii_digit());
            let next_lower = i + 1 < chars.len() && chars[i + 1].is_ascii_lowercase();
            let prev_upper = i > 0 && chars[i - 1].is_ascii_uppercase();
            if i > 0 && (prev_lower || (prev_upper && next_lower)) {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    if RUST_KEYWORDS.contains(&out.as_str()) {
        out.push('_');
    }
    out
}

/// `ProduceRequest` → `produce_request`, used for the generated module file name.
pub fn module_name(s: &str) -> String {
    snake_case(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_case_handles_acronyms() {
        assert_eq!(snake_case("ErrorCode"), "error_code");
        assert_eq!(snake_case("TopicId"), "topic_id");
        // Acronym runs split before a trailing capitalised word, matching the
        assert_eq!(snake_case("KRaftVersion"), "k_raft_version");
        assert_eq!(snake_case("ISRs"), "is_rs");
        assert_eq!(snake_case("LeaderId"), "leader_id");
        assert_eq!(snake_case("ThrottleTimeMs"), "throttle_time_ms");
        assert_eq!(snake_case("PartitionLeaderEpoch"), "partition_leader_epoch");
        assert_eq!(snake_case("Enable2Pc"), "enable2_pc");
    }

    #[test]
    fn snake_case_escapes_keywords() {
        assert_eq!(snake_case("Type"), "type_");
        assert_eq!(snake_case("Match"), "match_");
    }

    #[test]
    fn parses_nested_array_types() {
        assert_eq!(FieldType::parse("int32").unwrap(), FieldType::Int32);
        assert_eq!(
            FieldType::parse("[]string").unwrap(),
            FieldType::Array(Box::new(FieldType::Str))
        );
        assert_eq!(
            FieldType::parse("[]TopicData").unwrap(),
            FieldType::Array(Box::new(FieldType::Struct("TopicData".into())))
        );
        assert!(FieldType::parse("int128").is_err());
    }
}
