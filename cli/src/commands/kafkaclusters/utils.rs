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

use comfy_table::{Cell, Table};

use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;

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

/// Renders a properties map for `describe`-style output. Sensitive properties
/// (those whose value is the redaction placeholder) are styled in `Warn` so
/// they're visually distinguishable from regular values.
pub fn render_properties_table(properties: &HashMap<String, String>) -> Table {
    let mut table = Table::new_styled();
    table.set_styled_header(vec!["KEY", "VALUE"]);

    let mut keys: Vec<&String> = properties.keys().collect();
    keys.sort();
    for k in keys {
        let v = &properties[k];
        let cell = if v == REDACTION_PLACEHOLDER {
            Cell::new(format!("{}", Styled(Style::Warn, REDACTION_PLACEHOLDER)))
        } else {
            Cell::new(v)
        };
        table.add_row(vec![Cell::new(k), cell]);
    }
    table
}

/// Renders a property diff between two maps, with rows sorted by key. Values
/// equal to [`REDACTION_PLACEHOLDER`] are rendered as `***` so the user can
/// see that a server-redacted field is being preserved or replaced.
pub fn render_diff_table(old: &HashMap<String, String>, new: &HashMap<String, String>) -> Table {
    let mut keys: BTreeSet<&String> = BTreeSet::new();
    keys.extend(old.keys());
    keys.extend(new.keys());

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["PROPERTY", "OLD", "NEW"]);

    for k in keys {
        let old_v = old.get(k);
        let new_v = new.get(k);
        if old_v == new_v {
            continue;
        }
        let (old_cell, new_cell) = match (old_v, new_v) {
            (None, Some(v)) => (
                Cell::new(format!("{}", Styled(Style::Notice, "(unset)"))),
                Cell::new(format!("{}", Styled(Style::Success, v))),
            ),
            (Some(v), None) => (
                Cell::new(format!("{}", Styled(Style::Danger, v))),
                Cell::new(format!("{}", Styled(Style::Notice, "(removed)"))),
            ),
            (Some(o), Some(n)) => (
                Cell::new(format!("{}", Styled(Style::Danger, o))),
                Cell::new(format!("{}", Styled(Style::Success, n))),
            ),
            (None, None) => unreachable!(),
        };
        table.add_row(vec![Cell::new(k), old_cell, new_cell]);
    }

    table
}
