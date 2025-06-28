// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use itertools::Itertools;

use crate::PlainNodeId;

use super::LocationScope;

type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

#[derive(thiserror::Error, Debug)]
#[error("Invalid node location string: {0}")]
pub struct InvalidNodeLocationError(String);

/// Delimiter for location scopes in a location string (e.g. `us-east2.use2-az3`)
pub(super) const SCOPE_DELIMITER: &str = ".";

/// Delimiter for node-id after location string (e.g. `us-east2.use2-az3:N4`)
const NODE_DELIMITER: &str = ":";

/// Stores location information of non-special scopes in range (Node, Root) exclusively.
///
/// Identifies some domain, by giving a path from [`LocationScope::Root`] to the domain.
///
/// It's a vector of labels for scopes from biggest to smallest. For instance,
/// {"us-east-2", "use2-az3"} identifies zone "use2-az3" in region "us-east-2".
///
/// The vector always starts at the biggest possible (non-Root) scope, but doesn't
/// necessarily go all the way to the smallest (non-Node) scope.
/// E.g. {"us-east-2"} is valid and it identifies region "us-east-2" but doesn't
/// specify the zone.
///
/// To construct NodeLocation, use our `FromStr` implementation, a la `"region1.zone".parse()` style.
///
/// Technical details: this is a home-made smallvec-like structure to avoid heap-allocations and to
/// improve cache-friendlyness. It's a fixed-size array of labels, where each label is a highly-likely
/// stack-inlined `SmartString`.
#[derive(
    Clone, Default, PartialEq, Eq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub struct NodeLocation {
    /// Internal storage for labels of all scopes, the order of scopes in the array
    /// is the reverse order of their type value: largest scope (i.e. Region) gets
    /// stored at index 0, and so on...
    labels: [SmartString; LocationScope::num_scopes()],
    /// number of (non-empty) scope labels specified
    num_defined_scopes: u8,
}

impl NodeLocation {
    /// Creates a new empty NodeLocation
    pub const fn new() -> Self {
        Self {
            labels: [const { SmartString::new_const() }; LocationScope::num_scopes()],
            num_defined_scopes: 0,
        }
    }
    ///  Given a scope specified in `scope`, returns a domain string that
    ///  identifies the location of the node. The domain string will include the
    ///  name for the specified scope as well as names for all parent scopes.
    ///  E.g. a possible output for scope `Region` is "us-east1".
    ///
    ///  If `node_id` is `None` and scope [`LocationScope::Node`] is treated differently:
    ///  it's equivalent to `Zone`; i.e. the returned string will identify a zone,
    ///  not the node. If `node_id` is given, and `scope` is [`LocationScope::Node`],
    ///  the returned string will be identify a node, e.g. "us-east-2.use2-az3:N4"
    ///  or ":N4" if no location was supplied.
    ///
    ///  ## Panics
    ///  if `scope` is [`LocationScope::Root`]
    pub fn domain_string(&self, scope: LocationScope, node_id: Option<PlainNodeId>) -> String {
        debug_assert!(scope != LocationScope::Root);
        if self.is_empty() && node_id.is_none() {
            return String::new();
        }
        let effective_scopes = effective_scopes(scope);

        let mut result = self
            .labels
            .iter()
            .take(self.num_defined_scopes.into())
            .take(effective_scopes)
            .join(SCOPE_DELIMITER);

        if scope == LocationScope::Node {
            if let Some(node_id) = node_id {
                result += NODE_DELIMITER;
                result += &node_id.to_string();
            }
        }

        result
    }

    /// Node location label at the given `scope`.
    ///
    /// Note that on special scopes (Root, Node), the label is an empty string.
    pub fn label_at(&self, scope: LocationScope) -> &str {
        static EMPTY_LABEL: &str = "";
        if scope.is_special() {
            return EMPTY_LABEL;
        }

        &self.labels[scope_to_index(scope)]
    }

    /// Returns true if a label is assigned at this scope
    pub fn is_scope_defined(&self, scope: LocationScope) -> bool {
        !scope.is_special() && scope_to_index(scope) < self.num_defined_scopes as usize
    }

    /// Returns true if no labels are assigned
    pub fn is_empty(&self) -> bool {
        self.num_defined_scopes == 0
    }

    /// The number of scopes with assigned labels
    pub fn num_defined_scopes(&self) -> usize {
        self.num_defined_scopes.into()
    }

    /// Returns the smallest (narrowest) defined location scope
    pub fn smallest_defined_scope(&self) -> LocationScope {
        if self.num_defined_scopes == 0 {
            return LocationScope::Root;
        }

        LocationScope::from_u8(LocationScope::Root as u8 - self.num_defined_scopes).unwrap()
    }

    /// Checks if this location is a prefix match for the input. In other words, the input location
    /// `prefix` must be a prefix of (or equal to) this location.
    pub fn matches_prefix(&self, prefix: &str) -> bool {
        let prefix = prefix.trim();
        if prefix.is_empty() || prefix == SCOPE_DELIMITER {
            return true;
        }
        // Unconditionally adding scope delimiter to our location at the end to ensure we don't
        // perform partial matching with input prefix.
        //
        // If input prefix is `region.us-east`, we don't want this to match `region.us-east1`. We
        // only consider this a prefix match if the zone matches. For this to work, we add `.`
        // suffix to the input `prefix` and unconditionally add `.` to our own value.
        //
        // The previous case will be prefix=`region.us-east.` which is not a prefix of
        // `region.us-east1.`
        let domain_str = self.domain_string(LocationScope::Node, None) + SCOPE_DELIMITER;
        if prefix.ends_with(SCOPE_DELIMITER) {
            domain_str.starts_with(prefix)
        } else {
            domain_str.starts_with(&format!("{prefix}{SCOPE_DELIMITER}"))
        }
    }

    /// Checks if this location shares the same domain with the input location at the given scope.
    /// For instance, if self is "us-east2" the input location is "us-east2.use2-az3" and the scope is `Zone`,
    /// then the function returns `false` because while we share the region, we don't share the zone.
    /// It returns `true` if the same check was done at `Region` instead.
    ///
    /// This is a little more efficient that `matches_prefix` if the location has been already
    /// parsed.
    ///
    /// * Input scope [`LocationScope::Root`] always yields `true` since Root is always shared.
    /// * Input scope [`LocationScope::Node`] always yields `false` since Node is implicit and is never shared.
    pub fn shares_domain_with(&self, location: &NodeLocation, scope: LocationScope) -> bool {
        if scope == LocationScope::Root {
            return true;
        }
        if scope == LocationScope::Node {
            return false;
        }

        let effective_scopes = effective_scopes(scope);

        // compare up-to the effective scopes of the input `scope`
        self.labels
            .iter()
            .take(effective_scopes)
            .eq(location.labels.iter().take(effective_scopes))
    }
}
impl std::fmt::Display for NodeLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.domain_string(LocationScope::Node, None))
    }
}

impl std::fmt::Debug for NodeLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl FromStr for NodeLocation {
    type Err = InvalidNodeLocationError;
    ///  Fills the NodeLocation from the given location string in which labels
    ///  of each location scope are specified.
    ///
    ///  Definition:
    ///  Location Domain: a string that identifies the location of a node, it
    ///                   consists of labels of all location scopes, separated by
    ///                   [`DELIMITER`].
    ///  Label:           part of the location domain string that belongs to
    ///                   one location scope.
    ///
    ///  Notes: the location domain string must have all [`LocationScope::num_scopes()`] separated by
    ///         [`DELIMITER`]. The left most scope must be the biggest scope defined [`LocationScope::MAX`].
    ///         An empty label is allowed, meaning that location for the scope and
    ///         all subscopes are not specified.
    ///
    ///  Legit domain string examples:
    ///                "us-east2.use2-az3", "us-east2", "us-east2.", ".", ""
    ///  Invalid examples:
    ///                "us-east2...", ".use2-az3"
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // remove leading and trailing whitespaces
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::default());
        }

        let tokens: Vec<&str> = s
            .split(SCOPE_DELIMITER)
            .map(|token| validate_token(s, token))
            .try_collect()?;

        if tokens.len() > LocationScope::num_scopes() {
            return Err(InvalidNodeLocationError(format!(
                "Wrong number of scopes in location string '{}'. Got {}, maximum {}",
                s,
                tokens.len(),
                LocationScope::num_scopes(),
            )));
        }

        let mut n_tokens: usize = 0;
        let mut labels = [const { SmartString::new_const() }; LocationScope::num_scopes()];
        let mut tokens = tokens.into_iter();

        while n_tokens < LocationScope::num_scopes() {
            match tokens.next() {
                None => break,
                Some("") => break,
                Some(token) => labels[n_tokens] = token.into(),
            }

            n_tokens += 1;
        }

        // if any of the remaining tokens are empty string, then we fail.
        if tokens.any(|s| !s.is_empty()) {
            return Err(InvalidNodeLocationError(format!(
                "Non-empty label exists after an empty label in location string: '{s}'",
            )));
        }

        Ok(Self {
            labels,
            num_defined_scopes: n_tokens as u8,
        })
    }
}

/// Converts a non-special scope into the label-index.
///
/// **Requires scope to be non-special**
const fn scope_to_index(scope: LocationScope) -> usize {
    assert!(!scope.is_special());
    // 0 - Node -- excluded
    // 1 - Zone
    // 2 - Region
    // 3 - Root -- excluded
    // num_scopes = 2
    //
    // zone = 2 - 1 == labels[1]
    // region = 2 - 2 == labels[0]
    LocationScope::num_scopes() - (scope as usize)
}

/// How many scopes actual (non-special) scopes are equal or greater than this scope.
///
/// For instance, for a Node scope, we have 2 greater "non-special" scopes (Zone, Region),
/// for Region, it's "1", and for Root, it's 0.
const fn effective_scopes(scope: LocationScope) -> usize {
    match scope {
        LocationScope::Root => 0,
        LocationScope::Node => LocationScope::num_scopes(),
        scope => scope_to_index(scope) + 1,
    }
}

fn validate_token<'a>(input: &'a str, token: &'a str) -> Result<&'a str, InvalidNodeLocationError> {
    if token.contains(" ") || token.contains(":") {
        return Err(InvalidNodeLocationError(format!(
            "Illegal character(s) in location string: '{input}', in label '{token}'"
        )));
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    // write tests for NodeLocation string parsing
    use super::*;
    use googletest::prelude::*;

    #[test]
    fn node_location_parsing_simple() {
        // valid case 1
        let location = NodeLocation::from_str("us-east2.use2-az3").unwrap();
        assert_that!(location.smallest_defined_scope(), eq(LocationScope::Zone));
        assert_that!(location.label_at(LocationScope::Region), eq("us-east2"));
        assert_that!(location.label_at(LocationScope::Zone), eq("use2-az3"));
        assert_that!(location.label_at(LocationScope::Node), eq(""));
        assert_that!(location.label_at(LocationScope::Root), eq(""));
        assert_that!(location.num_defined_scopes(), eq(2));
        assert_that!(
            location.domain_string(LocationScope::Region, None),
            eq("us-east2")
        );
        assert_that!(
            location.domain_string(LocationScope::Zone, None),
            eq("us-east2.use2-az3")
        );
        assert_that!(
            location.domain_string(LocationScope::Node, None),
            eq("us-east2.use2-az3")
        );
        assert_that!(
            location.domain_string(LocationScope::Node, Some(PlainNodeId::new(4))),
            eq("us-east2.use2-az3:N4")
        );
        // node-id is ignored unless we are printing the Node scope.
        assert_that!(
            location.domain_string(LocationScope::Zone, Some(PlainNodeId::new(4))),
            eq("us-east2.use2-az3")
        );

        assert_that!(location.to_string(), eq("us-east2.use2-az3"));

        // valid case 2
        let location = NodeLocation::from_str("us-east2.").unwrap();
        assert_that!(location.smallest_defined_scope(), eq(LocationScope::Region));
        assert_that!(location.label_at(LocationScope::Region), eq("us-east2"));
        assert_that!(location.label_at(LocationScope::Zone), eq(""));
        assert_that!(location.label_at(LocationScope::Node), eq(""));
        assert_that!(location.num_defined_scopes(), eq(1));
        assert_that!(
            location.domain_string(LocationScope::Region, None),
            eq("us-east2")
        );
        assert_that!(
            location.domain_string(LocationScope::Zone, None),
            eq("us-east2")
        );
        assert_that!(
            location.domain_string(LocationScope::Node, None),
            eq("us-east2")
        );
        assert_that!(
            location.domain_string(LocationScope::Node, Some(PlainNodeId::new(4))),
            eq("us-east2:N4")
        );

        assert_that!(location.to_string(), eq("us-east2"));
        assert_that!(location, eq(NodeLocation::from_str("us-east2").unwrap()));

        // valid case 3
        let location = NodeLocation::from_str("us-east1").unwrap();
        assert_that!(location.label_at(LocationScope::Region), eq("us-east1"));
        assert_that!(location.label_at(LocationScope::Zone), eq(""));
        assert_that!(location.label_at(LocationScope::Node), eq(""));
        assert_that!(location.num_defined_scopes(), eq(1));
        assert_that!(
            location.domain_string(LocationScope::Region, None),
            eq("us-east1")
        );
        assert_that!(
            location.domain_string(LocationScope::Node, None),
            eq("us-east1")
        );
        assert_that!(
            location.domain_string(LocationScope::Node, Some(5.into())),
            eq("us-east1:N5")
        );
    }

    #[test]
    fn node_location_parsing_with_empty_labels() {
        // valid case 1 -- empty str
        let location = NodeLocation::from_str("").unwrap();
        assert_that!(location.smallest_defined_scope(), eq(LocationScope::Root));
        assert_that!(location.label_at(LocationScope::Region), eq(""));
        assert_that!(location.label_at(LocationScope::Zone), eq(""));
        assert_that!(location.label_at(LocationScope::Node), eq(""));
        assert_that!(location.num_defined_scopes(), eq(0));
        assert_that!(location.domain_string(LocationScope::Region, None), eq(""));
        assert_that!(location.domain_string(LocationScope::Node, None), eq(""));
        assert_that!(
            location.domain_string(LocationScope::Node, Some(5.into())),
            eq(":N5")
        );

        assert_that!(location, eq(NodeLocation::from_str(".").unwrap()));
        assert_that!(location, eq(NodeLocation::from_str(" ").unwrap()));
        assert_that!(location, eq(NodeLocation::from_str("   ").unwrap()));

        // valid case 1 -- region only
    }

    #[test]
    fn node_location_parsing_invalid() {
        // invalid case 1 -- empty region, but zone is set
        assert!(NodeLocation::from_str(".az1").is_err());

        // invalid case 2 -- too many tokens
        assert!(NodeLocation::from_str("region1.zone1.cluster5").is_err());

        // invalid case 3 -- too many tokens
        assert!(NodeLocation::from_str("region1..").is_err());

        // invalid case 4 -- too many tokens
        assert!(NodeLocation::from_str("region1..as").is_err());

        // invalid case 5 -- illegal characters
        assert!(NodeLocation::from_str(":").is_err());
        assert!(NodeLocation::from_str("my region").is_err());
        assert!(NodeLocation::from_str("region.my zone").is_err());
        assert!(NodeLocation::from_str("region.my:zone").is_err());
    }

    #[test]
    fn node_location_matching() {
        let location = NodeLocation::from_str("us-east2.use2-az3").unwrap();
        assert_that!(location.matches_prefix(""), eq(true));
        assert_that!(location.matches_prefix("."), eq(true));
        assert_that!(location.matches_prefix("us-east2"), eq(true));
        assert_that!(location.matches_prefix("us-east2."), eq(true));
        assert_that!(location.matches_prefix("us-east"), eq(false));
        assert_that!(location.matches_prefix("us-east2.use2"), eq(false));
        assert_that!(location.matches_prefix("us-east2.use2-az3"), eq(true));
        assert_that!(location.matches_prefix("us-east2.use2-az3."), eq(true));

        let location2 = NodeLocation::from_str("us-east2.use2").unwrap();
        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Root),
            eq(true)
        );

        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Region),
            eq(true)
        );

        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Zone),
            eq(false)
        );

        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Node),
            eq(false)
        );

        // even identical locations can't match on node scope
        assert_that!(
            location.shares_domain_with(&location, LocationScope::Node),
            eq(false)
        );

        // validates the same behaviour when locations are of different lengths
        //
        let location2 = NodeLocation::from_str("us-east2").unwrap();
        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Root),
            eq(true)
        );

        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Region),
            eq(true)
        );

        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Zone),
            eq(false)
        );

        assert_that!(
            location.shares_domain_with(&location2, LocationScope::Node),
            eq(false)
        );
        // same if the check is flipped over
        assert_that!(
            location2.shares_domain_with(&location, LocationScope::Root),
            eq(true)
        );

        assert_that!(
            location2.shares_domain_with(&location, LocationScope::Region),
            eq(true)
        );

        assert_that!(
            location2.shares_domain_with(&location, LocationScope::Zone),
            eq(false)
        );

        assert_that!(
            location2.shares_domain_with(&location, LocationScope::Node),
            eq(false)
        );
    }
}
