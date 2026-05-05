// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sparse-update helper for RFC 7396 JSON Merge Patch endpoints.

/// Sparse-update helper for RFC 7396 JSON Merge Patch endpoints.
///
/// Maps to JSON Merge Patch wire semantics:
/// - `Keep` corresponds to an absent field — leave the existing value as-is.
/// - `Overwrite(v)` corresponds to a JSON value — set the field.
/// - `Delete` corresponds to a JSON `null` — clear the field (only
///   meaningful for fields where `None`/absence is a valid state).
///
/// Use as a struct field with `#[serde(default)]`:
///
/// ```rust
/// use restate_serde_util::UpdateField;
///
/// #[derive(serde::Deserialize)]
/// struct Patch {
///     #[serde(default)]
///     reason: UpdateField<String>,
/// }
/// ```
///
/// `#[serde(default)]` covers the absent-field case (yields
/// [`UpdateField::Keep`] via [`Default`]); the type's own `Deserialize`
/// covers the `null`/value cases.
///
/// # Deliberately `Deserialize`-only
///
/// Serde's data model puts "include vs. skip the field" at the parent
/// struct's level, not the value's, so any `Serialize` impl here would
/// produce `null` for `Keep` — a footgun that requires the parent to
/// remember `#[serde(skip_serializing_if = …)]` on every field. Keeping
/// this type one-way means the footgun can't exist at compile time.
/// Symmetric PATCH-shaped responses (rare) should reach for
/// `Option<Option<T>>` + `serde_with::skip_serializing_none` at the wire
/// boundary instead.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum UpdateField<T> {
    #[default]
    Keep,
    Overwrite(T),
    Delete,
}

impl<T> UpdateField<T> {
    /// Resolve a sparse update into the *new* `Option<T>` value, or
    /// `None` to mean "leave the field unchanged".
    ///
    /// This collapses the three-valued `Keep`/`Overwrite`/`Delete`
    /// shape into the form a writer needs:
    /// - `Some(Some(v))` → set the field to `v`.
    /// - `Some(None)` → clear the field.
    /// - `None` → no change requested.
    pub fn into_target(self) -> Option<Option<T>> {
        match self {
            UpdateField::Keep => None,
            UpdateField::Overwrite(v) => Some(Some(v)),
            UpdateField::Delete => Some(None),
        }
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for UpdateField<T> {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        // Serde reaches this impl only when the JSON field is *present*.
        // Absent fields are handled by `#[serde(default)]` on the parent
        // struct, which yields `UpdateField::Keep` via `Default`.
        // Present fields are either `null` → `Delete` or some value →
        // `Overwrite`.
        match Option::<T>::deserialize(de)? {
            Some(v) => Ok(UpdateField::Overwrite(v)),
            None => Ok(UpdateField::Delete),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize, Debug, PartialEq, Eq)]
    struct Patch {
        #[serde(default)]
        reason: UpdateField<String>,
        #[serde(default)]
        disabled: Option<bool>,
    }

    #[test]
    fn absent_field_deserializes_as_keep() {
        let p: Patch = serde_json::from_str("{}").unwrap();
        assert_eq!(p.reason, UpdateField::Keep);
        assert_eq!(p.disabled, None);
    }

    #[test]
    fn null_field_deserializes_as_delete() {
        let p: Patch = serde_json::from_str(r#"{"reason": null}"#).unwrap();
        assert_eq!(p.reason, UpdateField::Delete);
    }

    #[test]
    fn value_field_deserializes_as_overwrite() {
        let p: Patch = serde_json::from_str(r#"{"reason": "audit"}"#).unwrap();
        assert_eq!(p.reason, UpdateField::Overwrite("audit".to_owned()));
    }

    #[test]
    fn into_target_collapses_to_option_option() {
        assert_eq!(UpdateField::<String>::Keep.into_target(), None);
        assert_eq!(UpdateField::<String>::Delete.into_target(), Some(None));
        assert_eq!(
            UpdateField::Overwrite("x".to_owned()).into_target(),
            Some(Some("x".to_owned())),
        );
    }
}
