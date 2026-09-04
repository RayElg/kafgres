//! Serde model of the Kafka message schema language. See `codec/schemas/UPSTREAM-README.md` —

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // declared to satisfy deny_unknown_fields; ignored on purpose
pub struct MessageSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "apiKey")]
    pub api_key: Option<i16>,
    #[serde(rename = "validVersions")]
    pub valid_versions: String,
    /// Absent on APIs removed in Kafka 4.0, which declare `validVersions: "none"` and
    #[serde(rename = "flexibleVersions")]
    pub flexible_versions: Option<String>,
    #[serde(rename = "deprecatedVersions")]
    pub deprecated_versions: Option<String>,
    /// Top version is not yet stable; brokers gate it behind `unstable.api.versions.enable`;
    #[serde(rename = "latestVersionUnstable", default)]
    pub latest_version_unstable: bool,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    /// Structs defined once and referenced by name from several places.
    #[serde(rename = "commonStructs", default)]
    pub common_structs: Vec<CommonStruct>,

    // --- accepted and ignored ---
    #[serde(default)]
    pub listeners: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // declared to satisfy deny_unknown_fields; ignored on purpose
pub struct CommonStruct {
    pub name: String,
    pub versions: String,
    pub fields: Vec<FieldSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // declared to satisfy deny_unknown_fields; ignored on purpose
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub versions: String,
    #[serde(rename = "nullableVersions")]
    pub nullable_versions: Option<String>,
    #[serde(rename = "taggedVersions")]
    pub tagged_versions: Option<String>,
    /// Field-level flexibility override. Exactly one field in the whole 4.3.1 schema set uses
    #[serde(rename = "flexibleVersions")]
    pub flexible_versions: Option<String>,
    pub tag: Option<i32>,
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,

    // --- accepted and ignored ---
    #[serde(default)]
    pub ignorable: bool,
    /// Documentation only.
    #[serde(default)]
    pub about: Option<String>,
    /// Documentation only — `topicName`, `groupId`, `producerId`, ...
    #[serde(rename = "entityType", default)]
    pub entity_type: Option<String>,
    /// Upstream turns the array into a keyed set in memory. Explicitly documented as
    #[serde(rename = "mapKey", default)]
    pub map_key: bool,
    /// Upstream hint that the field aliases the read buffer. We do that unconditionally
    #[serde(rename = "zeroCopy", default)]
    pub zero_copy: bool,
}

/// Strip `//` line comments, which the schemas use and JSON does not allow.
pub fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for line in src.lines() {
        let mut in_str = false;
        let mut escaped = false;
        let mut cut = line.len();
        let bytes = line.as_bytes();
        for i in 0..bytes.len() {
            let ch = bytes[i];
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                b'\\' if in_str => escaped = true,
                b'"' => in_str = !in_str,
                b'/' if !in_str && i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                    cut = i;
                    break;
                }
                _ => {}
            }
        }
        out.push_str(&line[..cut]);
        out.push('\n');
    }
    out
}

pub fn load(path: &Path) -> Result<MessageSpec, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let stripped = strip_comments(&raw);
    serde_json::from_str(&stripped).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_slashes_inside_strings() {
        let src = r#"{ "about": "see http://x/y", "n": 1 } // trailing"#;
        let out = strip_comments(src);
        assert!(out.contains("http://x/y"));
        assert!(!out.contains("trailing"));
    }

    #[test]
    fn handles_escaped_quotes() {
        let src = r#"{ "a": "he said \"hi\" // not a comment" } // yes"#;
        let out = strip_comments(src);
        assert!(out.contains("// not a comment"));
        assert!(!out.contains("// yes"));
    }
}
