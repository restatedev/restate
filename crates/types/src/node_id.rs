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

use bytes::{Buf, BufMut};
use restate_encoding::{BilrostNewType, NetSerde};

/// A generational node identifier. Nodes with the same ID but different generations
/// represent the same node across different instances (restarts) of its lifetime.
///
/// Note about equality checking. When comparing two node ids, we should always compare the same
/// type. For instance, if any side of the comparison is a generational node id, the other side
/// must be also generational and the generations must match. If you are only interested in
/// checking the id part, then compare using `x.id() == y.id()` instead of `x == y`.
#[derive(
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Display,
    derive_more::Debug,
    derive_more::IsVariant,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum NodeId {
    Plain(PlainNodeId),
    Generational(GenerationalNodeId),
}

#[derive(
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Display,
    derive_more::Debug,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Message,
    NetSerde,
)]
#[display("{}:{}", _0, _1)]
#[debug("{}:{}", _0, _1)]
pub struct GenerationalNodeId(PlainNodeId, u32);

impl From<crate::protobuf::common::GenerationalNodeId> for GenerationalNodeId {
    fn from(value: crate::protobuf::common::GenerationalNodeId) -> Self {
        Self(PlainNodeId(value.id), value.generation)
    }
}

impl From<&GenerationalNodeId> for NodeId {
    fn from(value: &GenerationalNodeId) -> Self {
        NodeId::Generational(*value)
    }
}

impl From<&PlainNodeId> for NodeId {
    fn from(value: &PlainNodeId) -> Self {
        NodeId::Plain(*value)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid plain node id: {0}")]
pub struct MalformedPlainNodeId(String);

#[derive(Debug, thiserror::Error)]
#[error("invalid generational node id: {0}")]
pub struct MalformedGenerationalNodeId(String);

impl FromStr for GenerationalNodeId {
    type Err = MalformedGenerationalNodeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // generational node id can be in "N<id>:<generation>" format or <id>:<gen> format.
        // parse the id and generation and construct GenerationalNodeId from this string
        let (id_part, gen_part) = s
            .split_once(':')
            .ok_or_else(|| MalformedGenerationalNodeId(s.to_string()))?;
        let id = id_part
            .trim_start_matches('N')
            .parse()
            .map_err(|_| MalformedGenerationalNodeId(s.to_string()))?;

        let generation = gen_part
            .parse()
            .map_err(|_| MalformedGenerationalNodeId(s.to_string()))?;

        Ok(GenerationalNodeId::new(id, generation))
    }
}

impl GenerationalNodeId {
    pub const INVALID: GenerationalNodeId = PlainNodeId::INVALID.with_generation(0);
    pub const INITIAL_NODE_ID: GenerationalNodeId = PlainNodeId::MIN.with_generation(1);

    pub fn decode<B: Buf>(mut data: B) -> Self {
        // generational node id is stored as two u32s next to each other, each in big-endian.
        let plain_id = data.get_u32();
        let generation = data.get_u32();
        Self(PlainNodeId(plain_id), generation)
    }

    /// Encodes this value into its binary representation and advances the underlying buffer
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        debug_assert!(buf.remaining_mut() >= Self::size());
        buf.put_u32(self.0.0);
        buf.put_u32(self.1);
    }

    /// Encodes this value into its binary representation on the stack
    pub fn to_binary_array(self) -> [u8; Self::size()] {
        let mut buf = [0u8; Self::size()];
        self.encode(&mut &mut buf[..]);
        buf
    }

    /// The number of bytes required for the binary representation of this value
    pub const fn size() -> usize {
        size_of::<Self>()
    }

    /// Same plain node-id but not the same generation
    #[inline(always)]
    pub fn is_same_but_different(&self, other: &GenerationalNodeId) -> bool {
        self.0 == other.0 && self.1 != other.1
    }
}

impl From<GenerationalNodeId> for u64 {
    fn from(value: GenerationalNodeId) -> Self {
        (u64::from(value.id()) << 32) | u64::from(value.generation())
    }
}

impl From<u64> for GenerationalNodeId {
    fn from(value: u64) -> Self {
        GenerationalNodeId::new((value >> 32) as u32, value as u32)
    }
}

#[derive(
    Default,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    derive_more::Debug,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
#[cfg_attr(feature = "utoipa-schema", derive(utoipa::ToSchema))]
#[display("N{}", _0)]
#[debug("N{}", _0)]
pub struct PlainNodeId(u32);

impl FromStr for PlainNodeId {
    type Err = MalformedPlainNodeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // plain id can be in "N<id>" format or <id> format.
        let id = s
            .trim_start_matches('N')
            .parse()
            .map_err(|_| MalformedPlainNodeId(s.to_string()))?;

        Ok(PlainNodeId::new(id))
    }
}

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
            NodeId::Generational(generation) => generation.as_plain(),
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

impl From<crate::protobuf::common::NodeId> for NodeId {
    fn from(node_id: crate::protobuf::common::NodeId) -> Self {
        NodeId::new(node_id.id, node_id.generation)
    }
}

impl From<NodeId> for crate::protobuf::common::NodeId {
    fn from(node_id: NodeId) -> Self {
        crate::protobuf::common::NodeId {
            id: node_id.id().into(),
            generation: node_id.as_generational().map(|g| g.generation()),
        }
    }
}

impl From<PlainNodeId> for crate::protobuf::common::NodeId {
    fn from(node_id: PlainNodeId) -> Self {
        let id: u32 = node_id.into();
        crate::protobuf::common::NodeId {
            id,
            generation: None,
        }
    }
}

impl From<GenerationalNodeId> for crate::protobuf::common::NodeId {
    fn from(node_id: GenerationalNodeId) -> Self {
        crate::protobuf::common::NodeId {
            id: node_id.raw_id(),
            generation: Some(node_id.generation()),
        }
    }
}

impl From<GenerationalNodeId> for PlainNodeId {
    fn from(value: GenerationalNodeId) -> Self {
        value.0
    }
}

impl PlainNodeId {
    // Start with 1 as plain node id to leave 0 as a special value in the future
    pub const INVALID: PlainNodeId = PlainNodeId::new(0);
    pub const MIN: PlainNodeId = PlainNodeId::new(1);

    pub const fn new(id: u32) -> PlainNodeId {
        PlainNodeId(id)
    }

    pub fn is_valid(self) -> bool {
        self.0 != 0
    }

    pub const fn with_generation(self, generation: u32) -> GenerationalNodeId {
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
    pub const fn new(id: u32, generation: u32) -> GenerationalNodeId {
        Self(PlainNodeId(id), generation)
    }

    pub fn raw_id(self) -> u32 {
        self.0.0
    }

    pub const fn as_plain(self) -> PlainNodeId {
        self.0
    }

    pub fn id(self) -> u32 {
        self.0.into()
    }

    pub fn is_valid(self) -> bool {
        self.0.is_valid() && self.1 != 0
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
    fn test_parse_plain_node_id_string() {
        let plain = NodeId::Plain(PlainNodeId(25));
        assert_eq!("N25", plain.to_string());
        let parsed_1: PlainNodeId = "N25".parse().unwrap();
        assert_eq!(parsed_1, plain);
        let parsed_2: PlainNodeId = "25".parse().unwrap();
        assert_eq!(parsed_2, plain);
        // invalid
        assert!("25:10".parse::<PlainNodeId>().is_err());
        // invalid
        assert!("N25:".parse::<PlainNodeId>().is_err());
        // invalid
        assert!("N25:10".parse::<PlainNodeId>().is_err());
    }

    #[test]
    fn test_parse_generational_node_id_string() {
        let generational = GenerationalNodeId::new(25, 18);
        assert_eq!("N25:18", generational.to_string());
        let parsed_1: GenerationalNodeId = "N25:18".parse().unwrap();
        assert_eq!(parsed_1, generational);
        let parsed_2: GenerationalNodeId = "25:18".parse().unwrap();
        assert_eq!(parsed_2, generational);
        // invalid
        assert!("25".parse::<GenerationalNodeId>().is_err());
        // invalid
        assert!("N25".parse::<GenerationalNodeId>().is_err());
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
