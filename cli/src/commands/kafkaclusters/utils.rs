// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers shared between the Kafka cluster subcommands.

use std::collections::{BTreeSet, HashMap};

use restate_cli_util::ui::stylesheet::Style;

use crate::ui::fmt::Field;
use crate::util::properties::REDACTION_PLACEHOLDER;

/// Returns `bootstrap.servers`, falling back to `metadata.broker.list`. Returns
/// `None` if neither is set or if the value is the redaction placeholder.
pub fn brokers_property(properties: &HashMap<String, String>) -> Option<&str> {
    for key in ["bootstrap.servers", "metadata.broker.list"] {
        if let Some(v) = properties.get(key)
            && v != REDACTION_PLACEHOLDER
            && !v.is_empty()
        {
            return Some(v.as_str());
        }
    }
    None
}

/// Rows (`key`, `value`) of a properties map for a formatter table, sorted by key.
/// Sensitive properties (those whose value is the redaction placeholder) are styled
/// in `Warn` so they're visually distinguishable from regular values.
pub fn properties_rows(properties: &HashMap<String, String>) -> Vec<Vec<Field>> {
    let mut keys: Vec<&String> = properties.keys().collect();
    keys.sort();
    keys.into_iter()
        .map(|k| {
            let v = &properties[k];
            let value = if v == REDACTION_PLACEHOLDER {
                Field::styled(v.as_str(), Style::Warn)
            } else {
                Field::new(v.as_str())
            };
            vec![Field::new(k.as_str()), value]
        })
        .collect()
}

/// A changed property: `(key, old, new)`, `None` meaning unset.
pub type PropertyChange<'a> = (&'a str, Option<&'a str>, Option<&'a str>);

/// The properties that differ between two maps, sorted by key.
pub fn property_diff<'a>(
    old: &'a HashMap<String, String>,
    new: &'a HashMap<String, String>,
) -> Vec<PropertyChange<'a>> {
    let keys: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    keys.into_iter()
        .filter_map(|k| {
            let (old_v, new_v) = (old.get(k), new.get(k));
            (old_v != new_v).then(|| {
                (
                    k.as_str(),
                    old_v.map(String::as_str),
                    new_v.map(String::as_str),
                )
            })
        })
        .collect()
}

/// Human rows (`property`, `old`, `new`) of a property diff. Values equal to
/// [`REDACTION_PLACEHOLDER`] are rendered as `***` so the user can see that a
/// server-redacted field is being preserved or replaced.
pub fn property_diff_rows(diff: &[PropertyChange<'_>]) -> Vec<Vec<Field>> {
    diff.iter()
        .map(|(k, old_v, new_v)| {
            let (old_field, new_field) = match (old_v, new_v) {
                (None, Some(v)) => (
                    Field::styled("(unset)", Style::Notice),
                    Field::styled(*v, Style::Success),
                ),
                (Some(v), None) => (
                    Field::styled(*v, Style::Danger),
                    Field::styled("(removed)", Style::Notice),
                ),
                (Some(o), Some(n)) => (
                    Field::styled(*o, Style::Danger),
                    Field::styled(*n, Style::Success),
                ),
                (None, None) => unreachable!(),
            };
            vec![Field::new(*k), old_field, new_field]
        })
        .collect()
}
