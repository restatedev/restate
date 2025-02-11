// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enumset::{EnumSet, EnumSetType};
use serde_with::serde_as;

use crate::locality::NodeLocation;
use crate::net::AdvertisedAddress;
use crate::{flexbuffers_storage_encode_decode, GenerationalNodeId, NodeId, PlainNodeId};
use crate::{Version, Versioned};
use ahash::HashMap;

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
#[strum(serialize_all = "kebab-case")]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", clap(rename_all = "kebab-case"))]
pub enum Role {
    /// A worker runs partition processor (journal, state, and drives invocations)
    Worker,
    /// Admin runs cluster controller and user-facing admin APIs
    Admin,
    /// Serves the metadata store
    // For backwards-compatibility also accept the old name
    #[serde(alias = "metadata-store")]
    #[cfg_attr(feature = "clap", clap(alias = "metadata-store"))]
    // todo switch to serializing as "metadata-server" in version 1.3
    #[serde(rename(serialize = "metadata-store"))]
    MetadataServer,
    /// [PREVIEW FEATURE] Serves a log-server for replicated loglets
    LogServer,
    /// [EXPERIMENTAL FEATURE] Serves HTTP ingress requests (requires
    /// `experimental-feature-enable-separate-ingress-role` to be enabled)
    HttpIngress,
}

#[serde_as]
#[derive(derive_more::Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodesConfiguration {
    version: Version,
    cluster_name: String,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    nodes: HashMap<PlainNodeId, MaybeNode>,
    #[debug(skip)]
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
    #[serde(default)]
    pub location: NodeLocation,
    #[serde(default)]
    pub metadata_server_config: MetadataServerConfig,
}

impl NodeConfig {
    pub fn new(
        name: String,
        current_generation: GenerationalNodeId,
        location: NodeLocation,
        address: AdvertisedAddress,
        roles: EnumSet<Role>,
        log_server_config: LogServerConfig,
        metadata_server_config: MetadataServerConfig,
    ) -> Self {
        Self {
            name,
            current_generation,
            address,
            roles,
            log_server_config,
            location,
            metadata_server_config,
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
            nodes: HashMap::default(),
            name_lookup: HashMap::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.name_lookup.len()
    }

    pub fn is_empty(&self) -> bool {
        self.name_lookup.is_empty()
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
            return StorageState::Provisioning;
        };
        match maybe {
            MaybeNode::Tombstone => StorageState::Disabled,
            MaybeNode::Node(found) => found.log_server_config.storage_state,
        }
    }

    pub fn get_metadata_server_state(&self, node_id: &PlainNodeId) -> MetadataServerState {
        let maybe = self.nodes.get(node_id);
        let Some(maybe) = maybe else {
            return MetadataServerState::Standby;
        };
        match maybe {
            MaybeNode::Tombstone => MetadataServerState::Standby,
            MaybeNode::Node(found) => {
                if found.roles.contains(Role::MetadataServer) {
                    found.metadata_server_config.metadata_server_state
                } else {
                    MetadataServerState::Standby
                }
            }
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

    /// Iterate over nodes with a given role
    pub fn iter_role(&self, role: Role) -> impl Iterator<Item = (PlainNodeId, &'_ NodeConfig)> {
        self.nodes.iter().filter_map(move |(k, v)| match v {
            MaybeNode::Node(node) if node.has_role(role) => Some((*k, node)),
            _ => None,
        })
    }

    /// Iterate over all non-tombstone nodes
    pub fn iter(&self) -> impl Iterator<Item = (PlainNodeId, &'_ NodeConfig)> {
        self.nodes.iter().filter_map(|(k, v)| {
            if let MaybeNode::Node(node) = v {
                Some((*k, node))
            } else {
                None
            }
        })
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (PlainNodeId, &'_ mut NodeConfig)> {
        self.nodes.iter_mut().filter_map(|(k, v)| {
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
    /// its log-store to `ReadWrite` once it's written its own storage marker on disk.
    ///
    /// The node can never transition back to `Provisioning` once it has transitioned out of it.
    ///
    /// [authoritative]
    /// or all intents and purposes, this is equivalent to a `ReadOnly` state, except that it's
    /// excluded from nodeset generation. The difference
    /// between this and `ReadOnly` is that if a node is in Provisioning state, we are confident
    /// that it has not been added to nodesets and we can safely remove it from the cluster without
    /// checking the log-chain or trim points of loglets. Note that if this node happens to be in a
    /// nodeset (although control plan shouldn't add it) spread selectors will not write any data
    /// to it until they observe a state transition to ReadWrite. This behaviour matches `ReadOnly`
    /// state.
    ///
    /// can read from: yes (likely to not have data, but it might if it transitioned into RW)
    /// can write to: yes - but excluded from new nodesets. If you see it in a nodeset, try writing to it.
    #[default]
    Provisioning,
    /// [authoritative empty]
    /// Node's storage is not expected to be accessed in reads nor write. The node is not
    /// considered as part of the replicated log cluster. Node can be safely decommissioned.
    ///
    /// The node can never transition out of `Disabled` after it has transitioned into it.
    ///
    /// should read from: no
    /// can write to: no
    Disabled,
    /// [authoritative]
    /// Node is not picked in new write sets, but it may still accept writes on existing nodeset
    /// and it's included in critical metadata updates (seal, release, etc.)
    /// should read from: yes
    /// can write to: yes
    /// **should write to: no**
    /// **excluded from new nodesets**
    ReadOnly,
    /// [authoritative]
    /// Can be picked up in new write sets and accepts writes in existing write sets.
    ///
    /// should read from: yes
    /// can write to: yes
    ReadWrite,
    /// **[non-authoritative]**
    /// Node detected that some/all of its local storage has been deleted and it cannot be used
    /// as authoritative source for quorum-dependent queries. Some data might have permanently been
    /// lost. It behaves like ReadOnly in spread selectors, but participates unauthoritatively in
    /// f-majority checks. This node can transition back to ReadWrite if it has been repaired.
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

    // Nodes that may have data or might become readable in the future
    pub fn can_read_from(&self) -> bool {
        use StorageState::*;
        match self {
            Provisioning | ReadOnly | ReadWrite | DataLoss => true,
            Disabled => false,
        }
    }

    pub fn is_authoritative(&self) -> bool {
        use StorageState::*;
        match self {
            DataLoss => false,
            Disabled | Provisioning | ReadOnly | ReadWrite => true,
        }
    }

    /// Empty nodes are considered not part of the nodeset and they'll never join back.
    pub fn empty(&self) -> bool {
        matches!(self, StorageState::Disabled)
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
pub enum MetadataServerState {
    /// The server is not considered as part of the metadata store cluster. Node can be safely
    /// decommissioned.
    Standby,
    /// The server is an active member of the metadata store cluster.
    #[default]
    Member,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogServerConfig {
    pub storage_state: StorageState,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MetadataServerConfig {
    pub metadata_server_state: MetadataServerState,
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
            "region1.zone1".parse().unwrap(),
            address.clone(),
            roles,
            LogServerConfig::default(),
            MetadataServerConfig::default(),
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
            "region1.zone1".parse().unwrap(),
            address,
            roles,
            LogServerConfig::default(),
            MetadataServerConfig::default(),
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
