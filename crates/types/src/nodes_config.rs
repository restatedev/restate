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
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use enumset::{EnumSet, EnumSetType};

use crate::{GenerationalNodeId, NodeId, PlainNodeId};

static CURRENT_CONFIG: ArcSwapOption<NodesConfiguration> = ArcSwapOption::const_empty();

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
#[cfg_attr(feature = "serde", enumset(serialize_repr = "list"))]
pub enum Role {
    Worker,
    Admin,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
    derive_more::AddAssign,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[display(fmt = "v{}", _0)]
pub struct ConfigVersion(u32);

impl ConfigVersion {
    pub const INVALID: ConfigVersion = ConfigVersion(0);
    pub const MIN: ConfigVersion = ConfigVersion(1);
}

impl Default for ConfigVersion {
    fn default() -> Self {
        Self::MIN
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodesConfiguration {
    version: ConfigVersion,
    nodes: HashMap<PlainNodeId, MaybeNode>,
    controllers: Vec<ControllerConfig>,
    name_lookup: HashMap<String, PlainNodeId>,
}

/// A type that allows updating the currently loaded configuration. This should
/// be used with care to avoid racy concurrent updates.
pub struct NodesConfigurationWriter {}

impl NodesConfigurationWriter {
    /// Silently ignore the config if it's older than existing.
    /// Note that this is not meant to be called concurrently from multiple tasks, the system have
    /// a single task responsible for setting updates.
    ///
    /// This not thread-safe.
    pub fn set_as_current_if_newer(config: NodesConfiguration) {
        let current = CURRENT_CONFIG.load_full();
        match current {
            None => {
                CURRENT_CONFIG.store(Some(Arc::new(config)));
            }
            Some(current) if config.version > current.version => {
                CURRENT_CONFIG.store(Some(Arc::new(config)));
            }
            Some(_) => { /* Do nothing, current is already newer */ }
        }
    }

    /// Sets the configuration as current without any checks.
    pub fn set_as_current_unconditional(config: NodesConfiguration) {
        CURRENT_CONFIG.store(Some(Arc::new(config)));
    }

    #[cfg(test)]
    pub fn unset_current() {
        CURRENT_CONFIG.store(None);
    }
}

impl NodesConfiguration {
    /// The currently loaded nodes configuration. This returns None if no configuration
    /// is loaded yet, this will be only the case before attaching to the controller
    /// node on startup.
    ///
    /// After attaching, it's safe to unwrap this, or use current_unchecked() variant.
    pub fn current() -> Option<Arc<NodesConfiguration>> {
        CURRENT_CONFIG.load_full()
    }

    pub fn current_unchecked() -> Arc<NodesConfiguration> {
        CURRENT_CONFIG.load_full().unwrap()
    }

    /// The currently loaded configuration version or ConfigVersion::INVALID if
    /// no configuration is loaded yet.
    pub fn current_version() -> ConfigVersion {
        CURRENT_CONFIG
            .load()
            .as_ref()
            .map(|config| config.version)
            .unwrap_or(ConfigVersion::INVALID)
    }
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
    pub address: NetworkAddress,
    pub roles: EnumSet<Role>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ControllerConfig {
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::Display)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NetworkAddress {
    /// Unix domain socket
    #[display(fmt = "unix:{}", _0)]
    Uds(String),
    /// TCP socket address
    #[display(fmt = "{}", _0)]
    TcpSocketAddr(SocketAddr),
    /// Hostname or host:port pair, or any unrecognizable string.
    #[display(fmt = "{}", _0)]
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

impl FromStr for NetworkAddress {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(NetworkAddress::from(s))
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

    pub fn increment_version(&mut self) {
        self.version += ConfigVersion(1);
    }

    #[cfg(test)]
    pub fn set_version(&mut self, version: ConfigVersion) {
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

    // test current set/get
    #[test]
    fn test_current_config() {
        NodesConfigurationWriter::unset_current();

        assert_eq!(
            ConfigVersion::INVALID,
            NodesConfiguration::current_version()
        );

        let mut config = NodesConfiguration::default();
        let address: NetworkAddress = "unix:/tmp/my_socket".into();
        let roles = EnumSet::only(Role::Worker);
        let current_gen = GenerationalNodeId::new(1, 1);
        let node = NodeConfig::new("node1".to_owned(), current_gen, address.clone(), roles);
        config.upsert_node(node.clone());
        config.increment_version();

        assert_eq!(ConfigVersion(2), config.version);
        NodesConfigurationWriter::set_as_current_unconditional(config.clone());
        assert_eq!(ConfigVersion(2), NodesConfiguration::current_version());

        // go back to version 1
        config.set_version(ConfigVersion(1));

        // current config is independent.
        assert_eq!(ConfigVersion(2), NodesConfiguration::current_version());

        // Do not allow going back in time.
        NodesConfigurationWriter::set_as_current_if_newer(config.clone());
        assert_eq!(ConfigVersion(2), NodesConfiguration::current_version());

        // force
        NodesConfigurationWriter::set_as_current_unconditional(config);
        assert_eq!(ConfigVersion(1), NodesConfiguration::current_version());
    }
}
