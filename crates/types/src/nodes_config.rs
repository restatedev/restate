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
use std::net::SocketAddr;

use enumset::{EnumSet, EnumSetType};

use crate::{GenerationalNodeId, NodeId, PlainNodeId};

#[derive(Debug, thiserror::Error)]
pub enum NodesConfigError {
    #[error("node {0} was not found in config")]
    UnknownNodeId(NodeId),
    #[error("node {0} has been permanently deleted from config")]
    Deleted(NodeId),
    #[error("node was found but has a mismatching generation (expected={expected} found={found})")]
    GenerationMismatch { expected: NodeId, found: NodeId },
}

// PartialEq+Eq+Clone+Copy are implemented by EnumSetType
#[derive(Debug, Hash, EnumSetType, derive_more::Display)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Role {
    Worker,
    ClusterContoller,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Default,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
    derive_more::AddAssign,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[display(fmt = "v{}", _0)]
pub struct ConfigVersion(u32);

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct NodesConfiguration {
    version: ConfigVersion,
    nodes: HashMap<PlainNodeId, MaybeNode>,
    controllers: Vec<ControllerConfig>,
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
struct NodeConfig {
    pub name: String,
    pub current_generation: GenerationalNodeId,
    pub address: NetworkAddress,
    pub roles: EnumSet<Role>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct ControllerConfig {
    pub address: NetworkAddress,
}

impl NodeConfig {
    pub fn new(
        name: String,
        current_generation: GenerationalNodeId,
        address: NetworkAddress,
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NetworkAddress {
    /// Unix domain socket
    Uds(String),
    /// TCP socket address
    TcpSocketAddr(SocketAddr),
    /// Hostname or host:port pair, or any unrecognizable string.
    DnsName(String),
}

impl From<&str> for NetworkAddress {
    #[allow(clippy::manual_strip)]
    fn from(s: &str) -> Self {
        if s.starts_with("unix:") {
            NetworkAddress::Uds(s[5..].to_string())
        } else {
            // try to parse as a socket address
            let tcp_addr: Result<SocketAddr, _> = s.parse();
            match tcp_addr {
                // Fallback to DNS name
                Ok(addr) => NetworkAddress::TcpSocketAddr(addr),
                Err(_) => NetworkAddress::DnsName(s.to_owned()),
            }
        }
    }
}

impl NodesConfiguration {
    pub fn new(version: ConfigVersion) -> Self {
        Self {
            version,
            nodes: HashMap::new(),
            controllers: Vec::new(),
            name_lookup: HashMap::new(),
        }
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

        self.version += ConfigVersion(1);
    }

    /// Current version of the config
    pub fn version(&self) -> ConfigVersion {
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

    pub fn controllers(&self) -> &Vec<ControllerConfig> {
        &self.controllers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_test_util::assert_eq;

    #[test]
    fn test_upsert_node() {
        let mut config = NodesConfiguration::default();
        let address: NetworkAddress = "unix:/tmp/my_socket".into();
        let roles = EnumSet::only(Role::Worker);
        let current_gen = GenerationalNodeId::new(1, 1);
        let node = NodeConfig::new("node1".to_owned(), current_gen, address.clone(), roles);
        config.upsert_node(node.clone());

        let res = config.find_node_by_id(NodeId::new(2));
        assert!(matches!(res, Err(NodesConfigError::UnknownNodeId(_))));

        // find by bad name.
        assert_eq!(None, config.find_node_by_name("nodeX"));

        // Find by plain id
        let found = config.find_node_by_id(NodeId::new(1)).expect("known id");
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

    // test parsing networkaddress
    #[test]
    fn test_parse_network_address() -> anyhow::Result<()> {
        let tcp: NetworkAddress = "127.0.0.1:5123".into();
        assert_eq!(
            tcp,
            NetworkAddress::TcpSocketAddr("127.0.0.1:5123".parse()?)
        );

        let tcp: NetworkAddress = "localhost:5123".into();
        assert_eq!(tcp, NetworkAddress::DnsName("localhost:5123".to_string()));

        // missing port we fallback to treating this as name since this is not
        // a validating conversion.
        let tcp: NetworkAddress = "127.0.0.1".into();
        assert_eq!(tcp, NetworkAddress::DnsName("127.0.0.1".to_string()));
        Ok(())
    }
}
