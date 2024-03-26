// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Mute clippy until this file is used.
#![allow(dead_code)]

use std::collections::HashMap;

use enumset::{EnumSet, EnumSetType};

use crate::net::AdvertisedAddress;
use crate::{GenerationalNodeId, NodeId, PlainNodeId};
use crate::{Version, Versioned};

#[derive(Debug, thiserror::Error)]
pub enum NodesConfigError {
    #[error("node {0} was not found in config")]
    UnknownNodeId(NodeId),
    #[error("node {0} has been permanently deleted from config")]
    Deleted(NodeId),
    #[error("node was found but has a mismatching generation (expected={expected} found={found})")]
    GenerationMismatch { expected: NodeId, found: NodeId },
    #[error("node config has an invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
}

// PartialEq+Eq+Clone+Copy are implemented by EnumSetType
#[derive(Debug, Hash, EnumSetType, strum_macros::Display)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", enumset(serialize_repr = "list"))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Role {
    Worker,
    Admin,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodesConfiguration {
    version: Version,
    cluster_name: String,
    nodes: HashMap<PlainNodeId, MaybeNode>,
    name_lookup: HashMap<String, PlainNodeId>,
}

#[derive(Debug, Clone, Eq, PartialEq, strum_macros::EnumIs)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum MaybeNode {
    Tombstone,
    Node(NodeConfig),
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeConfig {
    pub name: String,
    pub current_generation: GenerationalNodeId,
    pub address: AdvertisedAddress,
    pub roles: EnumSet<Role>,
}

impl NodeConfig {
    pub fn new(
        name: String,
        current_generation: GenerationalNodeId,
        address: AdvertisedAddress,
        roles: EnumSet<Role>,
    ) -> Self {
        Self {
            name,
            current_generation,
            address,
            roles,
        }
    }
}

impl NodesConfiguration {
    pub fn new(version: Version, cluster_name: String) -> Self {
        Self {
            version,
            cluster_name,
            nodes: HashMap::new(),
            name_lookup: HashMap::new(),
        }
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn increment_version(&mut self) {
        self.version += Version::from(1);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn set_version(&mut self, version: Version) {
        self.version = version;
    }

    /// Insert or replace a node with a config.
    pub fn upsert_node(&mut self, node: NodeConfig) {
        debug_assert!(!node.name.is_empty());

        let plain_id = node.current_generation.as_plain();
        let name = node.name.clone();
        let existing = self.nodes.insert(plain_id, MaybeNode::Node(node));
        if let Some(MaybeNode::Node(existing)) = existing {
            self.name_lookup.remove(&existing.name);
        }

        self.name_lookup.insert(name, plain_id);
    }

    /// Current version of the config
    pub fn version(&self) -> Version {
        self.version
    }

    /// Find a node by its ID. If called with a generational ID, the node config will only return
    /// if the node has the same generation, otherwise, it returns
    /// NodeConfigError::GenerationMismatch. If called with a plain node id (either PlainNodeId, or
    /// NodeId::Plain) then it won't care about the generation and will only return based on the ID
    /// match.
    pub fn find_node_by_id(&self, id: impl Into<NodeId>) -> Result<&NodeConfig, NodesConfigError> {
        let node_id: NodeId = id.into();
        let maybe = self.nodes.get(&node_id.id());
        let Some(maybe) = maybe else {
            return Err(NodesConfigError::UnknownNodeId(node_id));
        };

        let found = match maybe {
            MaybeNode::Tombstone => return Err(NodesConfigError::Deleted(node_id)),
            MaybeNode::Node(found) => found,
        };

        if node_id
            .as_generational()
            .is_some_and(|requested_generational| {
                requested_generational != found.current_generation
            })
        {
            return Err(NodesConfigError::GenerationMismatch {
                expected: node_id,
                found: found.current_generation.into(),
            });
        }

        Ok(found)
    }

    /// Find a node config by its name.
    pub fn find_node_by_name(&self, name: impl AsRef<str>) -> Option<&NodeConfig> {
        let id = self.name_lookup.get(name.as_ref())?;
        self.find_node_by_id(*id).ok()
    }

    /// Returns _an_ admin node.
    pub fn get_admin_node(&self) -> Option<&NodeConfig> {
        self.nodes.values().find_map(|maybe| match maybe {
            MaybeNode::Node(node) if node.roles.contains(Role::Admin) => Some(node),
            _ => None,
        })
    }
}

impl Versioned for NodesConfiguration {
    fn version(&self) -> Version {
        self.version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_test_util::assert_eq;

    #[test]
    fn test_upsert_node() {
        let mut config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let address: AdvertisedAddress = "unix:/tmp/my_socket".parse().unwrap();
        let roles = EnumSet::only(Role::Worker);
        let current_gen = GenerationalNodeId::new(1, 1);
        let node = NodeConfig::new("node1".to_owned(), current_gen, address.clone(), roles);
        config.upsert_node(node.clone());

        let res = config.find_node_by_id(NodeId::new_plain(2));
        assert!(matches!(res, Err(NodesConfigError::UnknownNodeId(_))));

        // find by bad name.
        assert_eq!(None, config.find_node_by_name("nodeX"));

        // Find by plain id
        let found = config
            .find_node_by_id(NodeId::new_plain(1))
            .expect("known id");
        assert_eq!(&node, found);

        // Find by generational
        let found = config
            .find_node_by_id(NodeId::new_generational(1, 1))
            .expect("known id");
        assert_eq!(&node, found);

        // GenerationalNodeId type.
        let found = config.find_node_by_id(current_gen).expect("known id");
        assert_eq!(&node, found);

        // find by good name
        let found = config.find_node_by_name("node1").expect("known id");
        assert_eq!(&node, found);

        // wrong generation
        let future_gen = NodeId::new_generational(1, 2);
        let res = config.find_node_by_id(future_gen);
        assert!(matches!(
            res,
            Err(NodesConfigError::GenerationMismatch { expected, found })
            if expected == future_gen && found == current_gen
        ));

        // upsert with new generation
        let future_gen = NodeId::new_generational(1, 2);
        let node = NodeConfig::new(
            // change name
            "nodeX".to_owned(),
            future_gen.as_generational().unwrap(),
            address,
            roles,
        );
        config.upsert_node(node.clone());

        //  now new generation is found
        let found = config.find_node_by_id(future_gen).expect("found");
        assert_eq!(&node, found);

        // old gen is not
        let res = config.find_node_by_id(current_gen);
        assert!(matches!(
            res,
            Err(NodesConfigError::GenerationMismatch { expected, found })
            if expected == current_gen && found == future_gen
        ));

        // find by old name
        assert_eq!(None, config.find_node_by_name("node1"));

        // find by new name
        let found = config.find_node_by_name("nodeX").expect("known id");
        assert_eq!(&node, found);
    }
}
