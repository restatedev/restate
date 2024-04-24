// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A generational node identifier. Nodes with the same ID but different generations
/// represent the same node across different instances (restarts) of its lifetime.
///
/// Note about equality checking. When comparing two node ids, we should always compare the same
/// type. For instance, if any side of the comparison is a generational node id, the other side
/// must be also generational and the generations must match. If you are only interested in
/// checking the id part, then compare using `x.id() == y.id()` instead of `x == y`.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Display,
    strum_macros::EnumIs,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum NodeId {
    Plain(PlainNodeId),
    Generational(GenerationalNodeId),
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Display,
    serde::Serialize,
    serde::Deserialize,
)]
#[display(fmt = "{}:{}", _0, _1)]
pub struct GenerationalNodeId(PlainNodeId, u32);

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::FromStr,
    derive_more::Into,
    derive_more::Display,
    serde::Serialize,
    serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
#[display(fmt = "N{}", _0)]
pub struct PlainNodeId(u32);

impl NodeId {
    pub fn new(id: u32, generation: Option<u32>) -> NodeId {
        match generation {
            Some(generation) => Self::new_generational(id, generation),
            None => Self::new_plain(id),
        }
    }

    pub fn new_plain(id: u32) -> NodeId {
        NodeId::Plain(PlainNodeId(id))
    }

    pub fn new_generational(id: u32, generation: u32) -> NodeId {
        NodeId::Generational(GenerationalNodeId(PlainNodeId(id), generation))
    }

    /// Returns the same node ID but replaces the generation.
    pub fn with_generation(self, generation: u32) -> NodeId {
        Self::Generational(GenerationalNodeId(self.id(), generation))
    }

    /// This node is the same node (same identifier) but has higher generation.
    pub fn is_newer_than(self, other: impl Into<NodeId>) -> bool {
        let other: NodeId = other.into();
        match (self, other) {
            (NodeId::Plain(_), NodeId::Plain(_)) => false,
            (NodeId::Plain(_), NodeId::Generational(_)) => false,
            (NodeId::Generational(_), NodeId::Plain(_)) => false,
            (NodeId::Generational(my_gen), NodeId::Generational(their_gen)) => {
                my_gen.is_newer_than(their_gen)
            }
        }
    }

    /// A unique identifier for the node. A stateful node will carry the same node id across
    /// restarts.
    pub fn id(self) -> PlainNodeId {
        match self {
            NodeId::Plain(id) => id,
            NodeId::Generational(gen) => gen.as_plain(),
        }
    }

    /// Generation identifies exact instance of the process running this node.
    /// In cases where we need to have an exact identity comparison (to fence off old instances),
    /// we should use this field to validate.
    pub fn as_generational(self) -> Option<GenerationalNodeId> {
        match self {
            NodeId::Plain(_) => None,
            NodeId::Generational(generational) => Some(generational),
        }
    }
}

impl PartialEq<GenerationalNodeId> for NodeId {
    fn eq(&self, other: &GenerationalNodeId) -> bool {
        match self {
            NodeId::Plain(_) => false,
            NodeId::Generational(this) => this == other,
        }
    }
}

impl PartialEq<PlainNodeId> for NodeId {
    fn eq(&self, other: &PlainNodeId) -> bool {
        match self {
            NodeId::Plain(this) => this == other,
            NodeId::Generational(_) => false,
        }
    }
}

impl PlainNodeId {
    pub fn with_generation(self, generation: u32) -> GenerationalNodeId {
        GenerationalNodeId(self, generation)
    }

    pub fn next(mut self) -> Self {
        self.0 += 1;
        self
    }
}

impl PartialEq<NodeId> for PlainNodeId {
    fn eq(&self, other: &NodeId) -> bool {
        match other {
            NodeId::Plain(id) => self == id,
            NodeId::Generational(_) => false,
        }
    }
}

impl GenerationalNodeId {
    pub fn new(id: u32, generation: u32) -> GenerationalNodeId {
        Self(PlainNodeId(id), generation)
    }

    pub fn raw_id(self) -> u32 {
        self.0 .0
    }

    pub fn raw_generation(self) -> u32 {
        self.1
    }

    pub fn as_plain(self) -> PlainNodeId {
        self.0
    }

    pub fn id(self) -> u32 {
        self.0.into()
    }

    pub fn generation(self) -> u32 {
        self.1
    }

    pub fn bump_generation(&mut self) {
        self.1 += 1;
    }

    pub fn is_newer_than(self, other: GenerationalNodeId) -> bool {
        self.0 == other.0 && self.1 > other.1
    }
}

impl PartialEq<NodeId> for GenerationalNodeId {
    fn eq(&self, other: &NodeId) -> bool {
        match other {
            NodeId::Plain(_) => false,
            NodeId::Generational(other) => self == other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //test display of NodeId and equality
    #[test]
    fn test_display() {
        let plain = NodeId::Plain(PlainNodeId(1));
        let generational = NodeId::Generational(GenerationalNodeId(PlainNodeId(1), 2));
        assert_eq!("N1", plain.to_string());
        assert_eq!("N1:2", generational.to_string());
        assert_eq!("N1", PlainNodeId(1).to_string());
        assert_eq!("N1:2", GenerationalNodeId(PlainNodeId(1), 2).to_string());
    }

    #[test]
    fn test_equality() {
        let plain1 = NodeId::Plain(PlainNodeId(1));
        let generational1 = NodeId::Generational(GenerationalNodeId(PlainNodeId(1), 2));
        let generational1_3 = NodeId::Generational(GenerationalNodeId(PlainNodeId(1), 3));

        assert_eq!(NodeId::new_plain(1), plain1);

        assert_ne!(plain1, generational1);
        assert_eq!(plain1.id(), generational1.id());
        assert_eq!(plain1, generational1.id());
        assert_eq!(NodeId::new_generational(1, 2), generational1);
        assert_ne!(NodeId::new_generational(1, 3), generational1);

        assert_eq!(generational1, generational1.as_generational().unwrap());
        assert_eq!(generational1.as_generational().unwrap(), generational1);

        // same node, different generations
        assert_ne!(generational1, generational1_3);
        // Now they are equal
        assert_eq!(generational1, generational1_3.with_generation(2));

        assert!(generational1_3.is_newer_than(generational1));
    }
}
