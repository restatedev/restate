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

use enum_map::Enum;

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
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
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

    pub fn num_copies(&self) -> u8 {
        *self
            .0
            .first_key_value()
            .expect("must have at least one scope")
            .1
    }

    pub fn at_scope_or_greater(&self, scope: LocationScope) -> Option<(&LocationScope, &u8)> {
        self.0.range(scope..).next()
    }

    pub fn at_smallest_scope(&self) -> (&LocationScope, &u8) {
        self.0
            .first_key_value()
            .expect("must have at least one scope")
    }

    pub fn at_greatest_scope(&self) -> (&LocationScope, &u8) {
        self.0
            .last_key_value()
            .expect("must have at least one scope")
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
        assert_that!(r.at_greatest_scope(), eq((&LocationScope::Node, &4)));
        assert_that!(
            r.at_scope_or_greater(LocationScope::Node),
            some(eq((&LocationScope::Node, &4)))
        );

        r.set_scope(LocationScope::Region, NonZeroU8::new(2).unwrap())?;
        assert_that!(r.num_copies(), eq(4));
        assert_that!(
            r.at_scope_or_greater(LocationScope::Zone),
            some(eq((&LocationScope::Region, &2)))
        );

        r.set_scope(LocationScope::Zone, NonZeroU8::new(2).unwrap())?;
        assert_that!(r.num_copies(), eq(4));
        assert_that!(
            r.at_scope_or_greater(LocationScope::Zone),
            some(eq((&LocationScope::Zone, &2)))
        );
        Ok(())
    }
}
