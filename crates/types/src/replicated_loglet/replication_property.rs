// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{btree_map, BTreeMap};
use std::fmt::{Display, Formatter};
use std::num::NonZeroU8;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::Context;
use enum_map::Enum;
use regex::Regex;

use crate::locality::NodeLocationScope;

static REPLICATION_PROPERTY_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?i)\{\s*(?<scopes>(?:node|zone|region)\s*:\s*\d+(?:\s*,\s*(?:node|zone|region)\s*:\s*\d+)*)\s*}$",
    ).expect("is valid pattern")
});

static REPLICATION_PROPERTY_EXTRACTOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?<scope>node|zone|region)\s*:\s*(?<factor>\d+)").expect("is valid regext")
});

/// Defines the scope of location for replication. This enum is ordered where the greatest
/// scope is at the bottom of the enum. i.e. Region > Zone > Node.
#[derive(
    Debug,
    Copy,
    Clone,
    Hash,
    Enum,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    strum::EnumIter,
    strum::Display,
    strum::EnumString,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(ascii_case_insensitive)]
pub enum LocationScope {
    Node,
    Zone,
    Region,
}

impl LocationScope {
    pub const MAX: Self = Self::Region;
    pub const MIN: Self = Self::Node;

    pub fn next_greater_scope(self) -> Option<LocationScope> {
        let next = self.into_usize() + 1;
        (next < LocationScope::LENGTH).then(|| LocationScope::from_usize(next))
    }

    pub fn next_smaller_scope(self) -> Option<LocationScope> {
        let current = self.into_usize();
        (current != 0).then(|| LocationScope::from_usize(current - 1))
    }
}

impl From<NodeLocationScope> for LocationScope {
    fn from(scope: NodeLocationScope) -> Self {
        match scope {
            NodeLocationScope::Node => Self::Node,
            NodeLocationScope::Zone => Self::Zone,
            NodeLocationScope::Region => Self::Region,
            NodeLocationScope::Root => panic!("Root is not a valid location scope"),
        }
    }
}

impl From<LocationScope> for NodeLocationScope {
    fn from(scope: LocationScope) -> Self {
        match scope {
            LocationScope::Node => Self::Node,
            LocationScope::Zone => Self::Zone,
            LocationScope::Region => Self::Region,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReplicationPropertyError(String);

/// The replication policy for appends
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ReplicationProperty(BTreeMap<LocationScope, u8>);

impl ReplicationProperty {
    pub fn new(replication_factor: NonZeroU8) -> Self {
        let mut map = BTreeMap::default();
        map.insert(LocationScope::Node, replication_factor.into());
        Self(map)
    }

    pub fn with_scope(scope: LocationScope, replication_factor: NonZeroU8) -> Self {
        let mut map = BTreeMap::default();
        map.insert(scope, replication_factor.into());
        Self(map)
    }

    pub fn iter(&self) -> btree_map::Iter<'_, LocationScope, u8> {
        self.0.iter()
    }

    pub fn set_scope(
        &mut self,
        scope: LocationScope,
        replication_factor: NonZeroU8,
    ) -> Result<&mut Self, ReplicationPropertyError> {
        // Replication factor for a scope cannot be higher lower scopes or lower than higher
        // scopes, and if it's equal, it will be ignored.

        for (s, r) in &self.0 {
            if *s < scope && *r < replication_factor.into() {
                return Err(ReplicationPropertyError(format!(
                    "Cannot set {{\"{scope}\": {replication_factor}}} as it conflicts with {{\"{s}\": {r}}}"
                )));
            }

            if *s > scope && *r > replication_factor.into() {
                return Err(ReplicationPropertyError(format!(
                    "Cannot set {{\"{scope}\": {replication_factor}}} as it conflicts with {{\"{s}\": {r}}}"
                )));
            }
        }
        self.0.insert(scope, replication_factor.into());
        Ok(self)
    }

    /// Total number of copies required to satisfy the replication property
    pub fn num_copies(&self) -> u8 {
        *self
            .0
            .first_key_value()
            .expect("must have at least one scope")
            .1
    }

    /// How many copies are required at this location scope.
    /// Returns None if no copies are defined at the given scope.
    /// For instance {zone: 2, node: 3} replication will return None at region scope.
    ///
    /// Note that it's guaranteed to get a value for replication at node-level scope.
    pub fn copies_at_scope(&self, scope: impl Into<LocationScope>) -> Option<u8> {
        let scope = scope.into();
        if scope == LocationScope::MIN {
            Some(self.num_copies())
        } else {
            self.0.get(&scope).copied()
        }
    }

    pub fn greatest_defined_scope(&self) -> LocationScope {
        *self
            .0
            .last_key_value()
            .expect("must have at least one scope")
            .0
    }
}

impl Display for ReplicationProperty {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut iter = self.0.iter();
        if let Some((scope, replication_factor)) = iter.next() {
            write!(
                f,
                "{}: {}",
                format!("{scope:?}").to_lowercase(),
                replication_factor
            )?;
            for (scope, replication_factor) in iter {
                write!(
                    f,
                    ", {}: {}",
                    format!("{scope:?}").to_lowercase(),
                    replication_factor
                )?;
            }
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl FromStr for ReplicationProperty {
    type Err = anyhow::Error;

    /// Parse a replication property from a str representation.
    /// Valid syntax is:
    /// - `<replication-factor>`
    /// - `{$(<scope>: <replication-factor>,)+}`
    ///
    /// Allowed scopes are `node`, `zone`, and `region`. `replication-factor` value
    /// must be greater than 0.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // is it just a simple number?
        if let Ok(replication_factor) = s.parse::<NonZeroU8>() {
            return Ok(ReplicationProperty::new(replication_factor));
        };

        let scopes = REPLICATION_PROPERTY_PATTERN
            .captures(s)
            .context("Invalid replication property syntax")?;

        let mut replication_property = None;
        for group in REPLICATION_PROPERTY_EXTRACTOR.captures_iter(&scopes["scopes"]) {
            let scope: LocationScope = group["scope"].parse().expect("is valid scope");
            let factor: NonZeroU8 = group["factor"]
                .parse()
                .with_context(|| format!("Replication factor for scope {scope} cannot be zero"))?;

            match replication_property {
                None => {
                    replication_property = Some(ReplicationProperty::with_scope(scope, factor));
                }
                Some(ref mut property) => {
                    property.set_scope(scope, factor)?;
                }
            }
        }

        replication_property
            .ok_or_else(|| anyhow::anyhow!("No replication property scopes defined"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::prelude::*;

    #[test]
    fn test_location_scope() {
        assert_that!(LocationScope::Zone, gt(LocationScope::Node));
        assert_that!(LocationScope::Region, gt(LocationScope::Zone));
        assert_that!(
            LocationScope::Node.next_greater_scope(),
            some(eq(LocationScope::Zone))
        );

        assert_that!(LocationScope::Region.next_greater_scope(), none());

        assert_that!(LocationScope::Node.next_smaller_scope(), none());
        assert_that!(
            LocationScope::Region.next_smaller_scope(),
            some(eq(LocationScope::Zone))
        );
    }

    #[test]
    fn test_replication_property() -> Result<()> {
        let mut r = ReplicationProperty::new(NonZeroU8::new(4).unwrap());
        assert_that!(r.num_copies(), eq(4));
        assert_that!(r.greatest_defined_scope(), eq(LocationScope::Node));

        assert_that!(r.copies_at_scope(LocationScope::Node), some(eq(4)));
        assert_that!(r.copies_at_scope(LocationScope::Zone), none());
        assert_that!(r.copies_at_scope(LocationScope::Region), none());

        r.set_scope(LocationScope::Region, NonZeroU8::new(2).unwrap())?;
        assert_that!(r.num_copies(), eq(4));

        assert_that!(r.copies_at_scope(LocationScope::Node), some(eq(4)));
        assert_that!(r.copies_at_scope(LocationScope::Zone), none());
        assert_that!(r.copies_at_scope(LocationScope::Region), some(eq(2)));

        r.set_scope(LocationScope::Zone, NonZeroU8::new(2).unwrap())?;
        assert_that!(r.num_copies(), eq(4));

        assert_that!(r.copies_at_scope(LocationScope::Node), some(eq(4)));
        assert_that!(r.copies_at_scope(LocationScope::Zone), some(eq(2)));
        assert_that!(r.copies_at_scope(LocationScope::Region), some(eq(2)));

        Ok(())
    }

    #[test]
    fn test_replication_property_parse() -> Result<()> {
        let r: ReplicationProperty = "2".parse().unwrap();
        assert_that!(r.num_copies(), eq(2));

        let r = r"{}".parse::<ReplicationProperty>();
        assert_that!(r, err(anything()));

        let r = r"{node: 5,  ZONE: 2}".parse::<ReplicationProperty>();
        let mut expected = ReplicationProperty::new(NonZeroU8::new(5).unwrap());
        expected
            .set_scope(LocationScope::Zone, NonZeroU8::new(2).unwrap())
            .unwrap();
        assert_that!(r, ok(eq(expected)));

        let r: ReplicationProperty = "{zone: 2}".parse().unwrap();
        assert_that!(r.num_copies(), eq(2));

        assert_that!(r.copies_at_scope(LocationScope::Node), some(eq(2)));
        assert_that!(r.copies_at_scope(LocationScope::Zone), some(eq(2)));
        assert_that!(r.copies_at_scope(LocationScope::Region), none());
        Ok(())
    }
}
