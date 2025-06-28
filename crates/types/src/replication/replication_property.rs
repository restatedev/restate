// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, btree_map};
use std::fmt::{Display, Formatter};
use std::num::NonZeroU8;
use std::str::FromStr;
use std::sync::LazyLock;

use crate::locality::LocationScope;
use anyhow::Context;
use regex::Regex;
use serde::{Deserialize, Deserializer};
use serde_with::{DeserializeAs, SerializeAs};

static REPLICATION_PROPERTY_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?i)\{\s*(?<scopes>(?:node|zone|region)\s*:\s*\d+(?:\s*,\s*(?:node|zone|region)\s*:\s*\d+)*)\s*}$",
    ).expect("is valid pattern")
});

static REPLICATION_PROPERTY_EXTRACTOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?<scope>node|zone|region)\s*:\s*(?<factor>\d+)").expect("is valid regext")
});

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReplicationPropertyError(String);

/// The replication policy for appends
#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
pub struct ReplicationProperty(BTreeMap<LocationScope, u8>);

impl ReplicationProperty {
    pub fn new(replication_factor: NonZeroU8) -> Self {
        let mut map = BTreeMap::default();
        map.insert(LocationScope::Node, replication_factor.into());
        Self(map)
    }

    /// Similar to [`Self::new`] but panics if replication_factor is zero
    pub fn new_unchecked(replication_factor: u8) -> Self {
        Self::new(NonZeroU8::new(replication_factor).expect("is non zero"))
    }

    pub fn with_scope(scope: LocationScope, replication_factor: NonZeroU8) -> Self {
        assert!(scope < LocationScope::Root);
        let mut map = BTreeMap::default();
        map.insert(scope, replication_factor.into());
        Self(map)
    }

    /// Returns a list of all different replication factors with their largest scope.
    /// The returned value is in the order of increasing replication factor. For instance,
    /// for replication `{region:3, zone: 3, node:5}`, we should observe `[(region, 3), (node, 5)]`
    /// since `zone` shares the same replication-factor as the bigger scope `region`.
    ///
    /// Another example: `{region: 3}` (or) `{region: 3, node: 3} will return [(region, 3)]
    ///
    /// Note that we allow `{region: 1, zone: 3}` as a replication property. It's allowed for
    /// nodeset selectors, spread selectors, and any placement logic to interpret this as a signal
    /// for locality preference. For instance, they might try and locate all copies on the
    /// same region where the sequencer is (or partition) instead of spreading to three zones
    /// across all available regions.
    pub fn distinct_replication_factors(&self) -> Vec<(LocationScope, u8)> {
        let mut scope = LocationScope::Root;
        let mut res = Vec::new();
        let mut prev_factor = 0;

        while let Some(current_scope) = scope.next_smaller_scope() {
            scope = current_scope;
            match self.0.get(&current_scope) {
                None => {
                    continue;
                }
                Some(factor) if *factor == prev_factor => {
                    continue;
                }
                Some(factor) => {
                    prev_factor = *factor;
                    res.push((current_scope, *factor));
                }
            }
        }
        res
    }

    pub fn iter(&self) -> btree_map::Iter<'_, LocationScope, u8> {
        self.0.iter()
    }

    /// Panics if scope is [`LocationScope::Root`]
    pub fn set_scope(
        &mut self,
        scope: LocationScope,
        replication_factor: NonZeroU8,
    ) -> Result<&mut Self, ReplicationPropertyError> {
        assert!(scope < LocationScope::Root);
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
        if scope == LocationScope::Node {
            Some(self.num_copies())
        } else {
            self.0.get(&scope).copied()
        }
    }

    pub fn greatest_defined_scope(&self) -> LocationScope {
        let scope = *self
            .0
            .last_key_value()
            .expect("must have at least one scope")
            .0;
        debug_assert!(scope < LocationScope::Root);
        scope
    }
}

impl std::fmt::Debug for ReplicationProperty {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
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

struct ReplicationPropertyFromNonZeroU8;

impl<'de> DeserializeAs<'de, ReplicationProperty> for ReplicationPropertyFromNonZeroU8 {
    fn deserialize_as<D>(deserializer: D) -> Result<ReplicationProperty, D::Error>
    where
        D: Deserializer<'de>,
    {
        NonZeroU8::deserialize(deserializer).map(ReplicationProperty::new)
    }
}

/// Helper struct to support serializing and deserializing the ReplicationProperty from all supported formats
///
/// Serialization:
/// - If property holds only simple replication factor `{node: N}` it's serialized as `N`
/// - Else, it's serialized using its Display implementation
///
/// Deserialization supported formats:
/// - From digits
/// - From strings as "N" and "{(<scope>: N,)+}"
/// - From map
pub struct ReplicationPropertyFromTo;

impl<'de> DeserializeAs<'de, ReplicationProperty> for ReplicationPropertyFromTo {
    fn deserialize_as<D>(deserializer: D) -> Result<ReplicationProperty, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_with::PickFirst::<(
            ReplicationPropertyFromNonZeroU8,
            serde_with::DisplayFromStr,
            serde_with::Same,
        )>::deserialize_as(deserializer)
    }
}

impl SerializeAs<ReplicationProperty> for ReplicationPropertyFromTo {
    fn serialize_as<S>(source: &ReplicationProperty, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if source.greatest_defined_scope() == LocationScope::Node {
            return serializer.serialize_u8(
                source
                    .copies_at_scope(LocationScope::Node)
                    .expect("must be set"),
            );
        }

        serde_with::DisplayFromStr::serialize_as(source, serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::prelude::*;

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
