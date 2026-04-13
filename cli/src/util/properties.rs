// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared parsing helpers for `KEY=VALUE` style configuration maps used by the
//! Kafka cluster and subscription commands. Inputs are accepted as:
//!
//! * Trailing positional `key=value` pairs (`parse_kv_arg`),
//! * librdkafka-style properties files (`.properties`, `.conf`, or stdin),
//! * TOML files with a `[properties]` (or caller-supplied) table.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

/// Placeholder string used by the admin server in place of redacted secret
/// values returned from `GET` responses.
pub const REDACTION_PLACEHOLDER: &str = "***";

/// Parses a single `KEY=VALUE` argument. The first `=` separates the key from
/// the value, so values may contain `=` (e.g. `sasl.jaas.config=... password=foo`).
///
/// Used as a clap `value_parser`.
pub fn parse_kv_arg(s: &str) -> Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=VALUE, got `{s}`"))?;
    let k = k.trim();
    if k.is_empty() {
        return Err(format!("property key is empty in `{s}`"));
    }
    Ok((k.to_string(), v.to_string()))
}

/// Collects a slice of `(key, value)` pairs into a map, rejecting duplicates so
/// the user notices `--set` typos rather than silently losing the first value.
pub fn collect_kv_pairs<I>(pairs: I) -> Result<HashMap<String, String>>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut out = HashMap::new();
    for (k, v) in pairs {
        if out.insert(k.clone(), v).is_some() {
            bail!("property `{k}` was specified more than once");
        }
    }
    Ok(out)
}

/// Reads a properties file. Format is detected from the path extension:
///
/// * `.properties` / `.conf` → librdkafka-style (one `key=value` per line, `#`
///   comments).
/// * `-` → read from stdin and parse as librdkafka-style.
/// * anything else → TOML; the map is taken from `toml_table` (typically
///   `"properties"` or `"options"`), or the whole document if `toml_table`
///   refers to the root and the document is a flat string-string table.
pub fn parse_properties_file(path: &Path, toml_table: &str) -> Result<HashMap<String, String>> {
    let raw = if path == Path::new("-") {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read properties from stdin")?;
        buf
    } else {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read properties file `{}`", path.display()))?
    };

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());

    match ext.as_deref() {
        Some("properties") | Some("conf") | Some("env") => parse_librdkafka_properties(&raw),
        Some("toml") => parse_toml_properties(&raw, toml_table),
        _ if path == Path::new("-") => parse_librdkafka_properties(&raw),
        _ => {
            // Best-effort: try TOML first, then fall back to librdkafka.
            parse_toml_properties(&raw, toml_table)
                .or_else(|_| parse_librdkafka_properties(&raw))
                .with_context(|| {
                    format!(
                        "could not parse `{}` as TOML or librdkafka properties",
                        path.display()
                    )
                })
        }
    }
}

/// Parses a flat librdkafka-style properties string. Blank lines and lines
/// starting with `#` (after trimming) are ignored. Each value line is split on
/// the first `=`. Whitespace around the key is trimmed; the value is taken
/// verbatim after trimming a single leading space (matching librdkafka).
pub fn parse_librdkafka_properties(s: &str) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for (lineno, raw_line) in s.lines().enumerate() {
        let trimmed = raw_line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }
        let (k, v) = trimmed.split_once('=').ok_or_else(|| {
            anyhow!(
                "line {}: expected `key=value`, got `{}`",
                lineno + 1,
                raw_line
            )
        })?;
        let key = k.trim().to_string();
        if key.is_empty() {
            bail!("line {}: empty property key", lineno + 1);
        }
        // Strip a single leading space so `key = value` works as well as
        // `key=value`. Trailing whitespace (including \r from CRLF) is dropped.
        let value = v.strip_prefix(' ').unwrap_or(v).trim_end().to_string();
        if out.insert(key.clone(), value).is_some() {
            bail!(
                "line {}: property `{}` is defined more than once",
                lineno + 1,
                key
            );
        }
    }
    Ok(out)
}

fn parse_toml_properties(s: &str, table: &str) -> Result<HashMap<String, String>> {
    let doc: toml::Value = toml::from_str(s).context("failed to parse properties file as TOML")?;

    let table_value = doc
        .as_table()
        .and_then(|t| t.get(table))
        .ok_or_else(|| anyhow!("missing `[{table}]` table in TOML properties file"))?;

    let table_map = table_value
        .as_table()
        .ok_or_else(|| anyhow!("`[{table}]` is not a TOML table"))?;

    let mut out = HashMap::with_capacity(table_map.len());
    for (k, v) in table_map {
        let s = v.as_str().ok_or_else(|| {
            anyhow!(
                "`{table}.{k}` must be a string (TOML type was {:?})",
                v.type_str()
            )
        })?;
        out.insert(k.clone(), s.to_string());
    }
    Ok(out)
}

/// Serializes a properties map as a librdkafka-style `key=value` document with
/// keys sorted alphabetically. Properties whose value equals
/// [`REDACTION_PLACEHOLDER`] are emitted as commented-out `# REDACTED:` lines
/// so a round-trip through an editor never accidentally overwrites a real
/// secret with `***`.
pub fn serialize_librdkafka_properties(map: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

    let mut out = String::new();
    for k in keys {
        let v = &map[k];
        if v == REDACTION_PLACEHOLDER {
            out.push_str("# REDACTED: ");
            out.push_str(k);
            out.push_str("=\n");
        } else {
            out.push_str(k);
            out.push('=');
            out.push_str(v);
            out.push('\n');
        }
    }
    out
}

/// Returns the keys whose value is the redaction placeholder.
pub fn redacted_keys(map: &HashMap<String, String>) -> Vec<String> {
    let mut keys: Vec<String> = map
        .iter()
        .filter(|(_, v)| *v == REDACTION_PLACEHOLDER)
        .map(|(k, _)| k.clone())
        .collect();
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_arg_splits_on_first_equals() {
        assert_eq!(
            parse_kv_arg("bootstrap.servers=a:9092,b:9092").unwrap(),
            ("bootstrap.servers".to_string(), "a:9092,b:9092".to_string())
        );
        // value containing =
        assert_eq!(
            parse_kv_arg("sasl.jaas.config=foo=bar baz=qux").unwrap(),
            (
                "sasl.jaas.config".to_string(),
                "foo=bar baz=qux".to_string()
            )
        );
        assert!(parse_kv_arg("no-equals").is_err());
        assert!(parse_kv_arg("=oops").is_err());
    }

    #[test]
    fn librdkafka_properties_round_trip() {
        let input = "\
            # a comment\n\
            \n\
            bootstrap.servers=broker:9092\n\
            client.id = restate\n\
            sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=u password=p\n\
            ; semicolon comment\n\
        ";
        let map = parse_librdkafka_properties(input).unwrap();
        assert_eq!(map.get("bootstrap.servers").unwrap(), "broker:9092");
        assert_eq!(map.get("client.id").unwrap(), "restate");
        assert!(
            map.get("sasl.jaas.config").unwrap().ends_with("password=p"),
            "value with `=` survived: {:?}",
            map.get("sasl.jaas.config")
        );

        let serialized = serialize_librdkafka_properties(&map);
        let round = parse_librdkafka_properties(&serialized).unwrap();
        assert_eq!(map, round);
    }

    #[test]
    fn librdkafka_rejects_duplicate_keys() {
        let err = parse_librdkafka_properties("a=1\na=2\n").unwrap_err();
        assert!(err.to_string().contains("more than once"), "{err}");
    }

    #[test]
    fn redacted_values_are_commented_in_serialization() {
        let mut map = HashMap::new();
        map.insert("bootstrap.servers".to_string(), "broker:9092".to_string());
        map.insert("sasl.password".to_string(), REDACTION_PLACEHOLDER.into());

        let s = serialize_librdkafka_properties(&map);
        assert!(s.contains("bootstrap.servers=broker:9092"), "{s}");
        assert!(s.contains("# REDACTED: sasl.password="), "{s}");

        let parsed = parse_librdkafka_properties(&s).unwrap();
        // Redacted line is a comment and must not round-trip into the map.
        assert!(!parsed.contains_key("sasl.password"));
        assert_eq!(redacted_keys(&map), vec!["sasl.password".to_string()]);
    }

    #[test]
    fn toml_properties_table() {
        let input = r#"
            [properties]
            "bootstrap.servers" = "broker:9092"
            "client.id" = "restate"
        "#;
        let map = parse_toml_properties(input, "properties").unwrap();
        assert_eq!(map.get("bootstrap.servers").unwrap(), "broker:9092");
        assert_eq!(map.get("client.id").unwrap(), "restate");
    }

    #[test]
    fn toml_properties_rejects_non_string_values() {
        let input = r#"
            [properties]
            "request.timeout.ms" = 30000
        "#;
        let err = parse_toml_properties(input, "properties").unwrap_err();
        assert!(err.to_string().contains("must be a string"), "{err}");
    }

    #[test]
    fn collect_kv_pairs_rejects_dupes() {
        let pairs = vec![
            ("a".to_string(), "1".to_string()),
            ("a".to_string(), "2".to_string()),
        ];
        assert!(collect_kv_pairs(pairs).is_err());
    }
}
