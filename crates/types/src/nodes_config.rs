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
use serde_with::serde_as;

use crate::net::AdvertisedAddress;
use crate::{flexbuffers_storage_encode_decode, GenerationalNodeId, NodeId, PlainNodeId};
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
#[derive(Debug, Hash, EnumSetType, strum::Display, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[enumset(serialize_repr = "list")]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", clap(rename_all = "kebab-case"))]
pub enum Role {
    /// A worker runs partition processor (journal, state, and drives invocations)
    Worker,
    /// Admin runs cluster controller and user-facing admin APIs
    Admin,
    /// Serves the metadata store
    MetadataStore,
    /// [IN DEVELOPMENT] Serves a log server for replicated loglets
    LogServer,
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodesConfiguration {
    version: Version,
    cluster_name: String,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    nodes: HashMap<PlainNodeId, MaybeNode>,
    name_lookup: HashMap<String, PlainNodeId>,
}

impl Default for NodesConfiguration {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            cluster_name: "Unspecified".to_owned(),
            nodes: Default::default(),
            name_lookup: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, strum::EnumIs, serde::Serialize, serde::Deserialize)]
enum MaybeNode {
    Tombstone,
    Node(NodeConfig),
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    pub name: String,
    pub current_generation: GenerationalNodeId,
    pub address: AdvertisedAddress,
    pub roles: EnumSet<Role>,
    #[serde(default)]
    pub log_server_config: LogServerConfig,
}

impl NodeConfig {
    pub fn new(
        name: String,
        current_generation: GenerationalNodeId,
        address: AdvertisedAddress,
        roles: EnumSet<Role>,
        log_server_config: LogServerConfig,
    ) -> Self {
        Self {
            name,
            current_generation,
            address,
            roles,
            log_server_config,
        }
    }

    pub fn has_role(&self, role: Role) -> bool {
        self.roles.contains(role)
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

    /// Returns [`StorageState::Disabled`] if a node is deleted or unrecognized
    pub fn get_log_server_storage_state(&self, node_id: &PlainNodeId) -> StorageState {
        let maybe = self.nodes.get(node_id);
        let Some(maybe) = maybe else {
            return StorageState::Disabled;
        };
        match maybe {
            MaybeNode::Tombstone => StorageState::Disabled,
            MaybeNode::Node(found) => found.log_server_config.storage_state,
        }
    }

    /// Returns _an_ admin node.
    pub fn get_admin_node(&self) -> Option<&NodeConfig> {
        self.nodes.values().find_map(|maybe| match maybe {
            MaybeNode::Node(node) if node.roles.contains(Role::Admin) => Some(node),
            _ => None,
        })
    }

    pub fn get_admin_nodes(&self) -> impl Iterator<Item = &NodeConfig> {
        self.nodes.values().filter_map(|maybe| match maybe {
            MaybeNode::Node(node) if node.roles.contains(Role::Admin) => Some(node),
            _ => None,
        })
    }

    pub fn has_worker_role(&self, node_id: &PlainNodeId) -> bool {
        self.nodes.get(node_id).is_some_and(|maybe| match maybe {
            MaybeNode::Node(node) => node.has_role(Role::Worker),
            _ => false,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (PlainNodeId, &'_ NodeConfig)> {
        self.nodes.iter().filter_map(|(k, v)| {
            if let MaybeNode::Node(node) = v {
                Some((*k, node))
            } else {
                None
            }
        })
    }

    /// Returns the maximum known plain node id.
    pub fn max_plain_node_id(&self) -> Option<PlainNodeId> {
        self.nodes.keys().max().cloned()
    }
}

impl Versioned for NodesConfiguration {
    fn version(&self) -> Version {
        self.version()
    }
}

#[derive(
    Clone,
    Debug,
    Copy,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    derive_more::IsVariant,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum StorageState {
    /// The node is not expected to be a member in any write set and the node will self-provision
    /// its log-store to `Disabled` once it's written its own storage marker on disk.
    ///
    /// The node can never transition back to `Provisioning` once it has transitioned into
    /// `Disabled`.
    ///
    /// should read from: no
    /// can write to: no
    #[default]
    Provisioning,
    /// Node's storage is not expected to be accessed in reads nor write. The node is not
    /// considered as part of the replicated log cluster (yet). Node can be safely decommissioned.
    ///
    /// should read from: no
    /// can write to: no
    Disabled,
    /// Node is not picked in new write sets and it'll reject new writes on its own storage except for
    /// critical metadata updates.
    /// should read from: yes
    /// can write to: no
    ReadOnly,
    /// Can be picked up in new write sets and accepts writes in existing write sets.
    ///
    /// should read from: yes
    /// can write to: yes
    ReadWrite,
    /// Node detected that some/all of its local storage has been deleted and it cannot be used
    /// as authoritative source for quorum-dependent queries.
    ///
    /// should read from: yes (non-quorum reads)
    /// can write to: no
    DataLoss,
}

impl StorageState {
    pub fn can_write_to(&self) -> bool {
        use StorageState::*;
        match self {
            Provisioning | Disabled | ReadOnly | DataLoss => false,
            ReadWrite => true,
        }
    }

    pub fn should_read_from(&self) -> bool {
        use StorageState::*;
        match self {
            ReadOnly | ReadWrite | DataLoss => true,
            Provisioning | Disabled => false,
        }
    }

    /// Empty nodes are automatically excluded from node sets.
    pub fn empty(&self) -> bool {
        matches!(self, StorageState::Provisioning | StorageState::Disabled)
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogServerConfig {
    pub storage_state: StorageState,
}

flexbuffers_storage_encode_decode!(NodesConfiguration);

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
        let node = NodeConfig::new(
            "node1".to_owned(),
            current_gen,
            address.clone(),
            roles,
            LogServerConfig::default(),
        );
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
            LogServerConfig::default(),
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
