// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZero;
use std::sync::Arc;

use enumset::{EnumSet, EnumSetType};
use serde_with::serde_as;

use crate::locality::NodeLocation;
use crate::metadata::GlobalMetadata;
use crate::net::AdvertisedAddress;
use crate::net::metadata::{MetadataContainer, MetadataKind};
use crate::{GenerationalNodeId, NodeId, PlainNodeId, flexbuffers_storage_encode_decode};
use crate::{Version, Versioned};
use ahash::HashMap;

#[derive(
    Clone,
    Copy,
    derive_more::Debug,
    derive_more::Display,
    derive_more::From,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
#[debug("{_0}")]
#[display("{_0}")]
pub struct ClusterFingerprint(NonZero<u64>);

#[derive(Debug, thiserror::Error)]
#[error("invalid fingerprint value: {0}")]
pub struct InvalidFingerprint(u64);

impl TryFrom<u64> for ClusterFingerprint {
    type Error = InvalidFingerprint;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self(NonZero::new(value).ok_or(InvalidFingerprint(value))?))
    }
}

impl ClusterFingerprint {
    pub fn to_u64(self) -> u64 {
        self.0.get()
    }
}

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
    #[serde(rename(serialize = "metadata-server"))]
    MetadataServer,
    /// Serves a log-server for replicated loglets
    LogServer,
    /// [EXPERIMENTAL FEATURE] Serves HTTP ingress requests (requires
    /// `experimental-feature-enable-separate-ingress-role` to be enabled)
    HttpIngress,
}

#[serde_as]
#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodesConfiguration {
    version: Version,
    cluster_name: String,
    // a unique fingerprint for this cluster. Introduced in v1.3 for forward compatibility. Will be
    // used to uniquely identify this cluster instance in future versions.
    #[serde(default)]
    cluster_fingerprint: Option<ClusterFingerprint>,
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
            cluster_fingerprint: None,
            cluster_name: "Unspecified".to_owned(),
            nodes: Default::default(),
            name_lookup: Default::default(),
        }
    }
}

impl GlobalMetadata for NodesConfiguration {
    const KEY: &'static str = "nodes_config";

    const KIND: MetadataKind = MetadataKind::NodesConfiguration;

    fn into_container(self: Arc<Self>) -> MetadataContainer {
        MetadataContainer::NodesConfiguration(self)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, strum::EnumIs, serde::Serialize, serde::Deserialize)]
enum MaybeNode {
    Tombstone,
    Node(NodeConfig),
}

#[derive(
    Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, typed_builder::TypedBuilder,
)]
pub struct NodeConfig {
    pub name: String,
    pub current_generation: GenerationalNodeId,
    pub address: AdvertisedAddress,
    pub roles: EnumSet<Role>,
    #[serde(default)]
    #[builder(default)]
    pub log_server_config: LogServerConfig,
    #[serde(default)]
    #[builder(default)]
    pub location: NodeLocation,
    #[serde(default)]
    #[builder(default)]
    pub metadata_server_config: MetadataServerConfig,
    #[serde(default)]
    #[builder(default)]
    pub worker_config: WorkerConfig,
}

impl NodeConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        current_generation: GenerationalNodeId,
        location: NodeLocation,
        address: AdvertisedAddress,
        roles: EnumSet<Role>,
        log_server_config: LogServerConfig,
        metadata_server_config: MetadataServerConfig,
        worker_config: WorkerConfig,
    ) -> Self {
        Self {
            name,
            current_generation,
            address,
            roles,
            log_server_config,
            location,
            metadata_server_config,
            worker_config,
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
            cluster_fingerprint: None,
            cluster_name,
            nodes: HashMap::default(),
            name_lookup: HashMap::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.name_lookup.len()
    }

    pub fn contains(&self, id: &PlainNodeId) -> bool {
        self.nodes
            .get(id)
            .map(|maybe| maybe.is_node())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.name_lookup.is_empty()
    }

    pub fn cluster_fingerprint(&self) -> Option<ClusterFingerprint> {
        self.cluster_fingerprint
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn increment_version(&mut self) {
        self.version += Version::from(1);
    }

    #[cfg(feature = "test-util")]
    pub fn set_version(&mut self, version: Version) {
        self.version = version;
    }

    /// Removes the node with the given `id` by inserting a tombstone for it.
    ///
    /// # Important
    /// You should only remove nodes from the nodes configuration once they are fully drained. This
    /// means they shouldn't be part of any node set, be a member of the latest metadata cluster
    /// configuration, or run any partition processors. It is your responsibility to ensure this
    /// condition before removing a node!
    pub fn remove_node_unchecked(&mut self, id: impl Into<PlainNodeId>) {
        let node_id = id.into();
        // only keep tombstones for known nodes
        if let Some(node_config) = self.nodes.get_mut(&node_id) {
            if let MaybeNode::Node(node_config) = node_config {
                self.name_lookup.remove(&node_config.name);
            }
            *node_config = MaybeNode::Tombstone;
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

    /// Returns [`WorkerState::Disabled`] if a node is deleted or [`WorkerState::Provisioning`]
    /// if the node is not a worker or has not started yet.
    pub fn get_worker_state(&self, node_id: &PlainNodeId) -> WorkerState {
        let maybe = self.nodes.get(node_id);
        let Some(maybe) = maybe else {
            return WorkerState::Provisioning;
        };
        match maybe {
            MaybeNode::Tombstone => WorkerState::Disabled,
            MaybeNode::Node(found) => found.worker_config.worker_state,
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
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum StorageState {
    // [authoritative]
    /// The node is not expected to be a member of any write set. The node will self-provision its
    /// log-store to `ReadWrite` once it's written its own storage marker to disk.
    ///
    /// A node can never transition back to `Provisioning` once it has transitioned out of it.
    ///
    /// For all intents and purposes, this is equivalent to a `ReadOnly` state, except that it's
    /// excluded from nodeset generation. The difference between this and `ReadOnly` is that, if a
    /// node is in Provisioning state, we are confident that it has not been added to node sets, and
    /// we can safely remove it from the cluster without checking the log-chain or trim points of
    /// loglets. Note that if this node happens to be in a nodeset (although control plane shouldn't
    /// add it), spread selectors will not write any data to it until they observe a state
    /// transition to `ReadWrite`. This behaviour matches the `ReadOnly` state.
    ///
    /// - can read from: **yes** (likely to not have data, but it might if it transitioned into `ReadWrite`)
    /// - can write to: **yes** but excluded from new node sets; if you see it in a nodeset, try writing to it
    /// - new node sets: **excluded**
    #[default]
    Provisioning,

    // [authoritative empty]
    /// Node's storage is not expected to be accessed for reads or writes. The node is not
    /// considered as part of the replicated log cluster, and can be safely decommissioned.
    ///
    /// The node can never transition out of `Disabled` after it has transitioned into it.
    ///
    /// - should read from: **no**
    /// - can write to: **no**
    Disabled,

    // [authoritative]
    /// Node is not picked for new write sets, but it may still accept writes on existing nodesets,
    /// and it's included in critical metadata updates (seal, release, etc.).
    ///
    /// - should read from: **yes**
    /// - can write to: **yes**
    /// - should write to: **no**
    /// - new node sets: **excluded**
    ReadOnly,

    // [authoritative]
    /// Gone is logically equivalent to ReadOnly but it signifies that the node has been
    /// permanently lost along with all the data it might have had.
    /// Future versions of restate can decide to not even attempt to read from Gone nodes.
    ///
    /// - new node sets: **excluded**
    Gone,

    // [authoritative]
    /// Can be picked up in new write sets and accepts writes in existing write sets.
    ///
    /// - should read from: **yes**
    /// - can write to: **yes**
    /// - new node sets: **included**
    ReadWrite,

    // **[non-authoritative]**
    /// Node detected that some/all of its local storage has been lost. It cannot be used as an
    /// authoritative source for quorum-dependent queries.
    ///
    /// Some data might have permanently been lost. It behaves like ReadOnly in spread selectors,
    /// but participates non-authoritatively in f-majority checks. This node can transition back to
    /// `ReadWrite` if it has been repaired.
    ///
    /// - should read from: **may (non-quorum reads only)**
    /// - can write to: **no**
    DataLoss,
}

impl StorageState {
    pub fn can_write_to(&self) -> bool {
        use StorageState::*;
        match self {
            Provisioning | Disabled | ReadOnly | Gone | DataLoss => false,
            ReadWrite => true,
        }
    }

    // Nodes that may have data or might become readable in the future
    pub fn can_read_from(&self) -> bool {
        use StorageState::*;
        match self {
            Provisioning | ReadOnly | Gone | ReadWrite | DataLoss => true,
            Disabled => false,
        }
    }

    pub fn is_authoritative(&self) -> bool {
        use StorageState::*;
        match self {
            DataLoss => false,
            Disabled | Provisioning | ReadOnly | Gone | ReadWrite => true,
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
    /// The server should try to automatically join a metadata store cluster.
    Provisioning,
}

/// State of a node that runs the [`Role::Worker`].
#[derive(
    Clone,
    Copy,
    Debug,
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
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum WorkerState {
    /// Worker is about to start. No partitions are running on this worker yet.
    #[default]
    Provisioning,
    /// Worker can be included in replica sets for running partitions.
    Active,
    /// Worker should no longer be added to new replica sets. However, it might still be part of
    /// existing replica sets.
    Draining,
    /// Worker is no longer part of any replica sets and it won't be added to new replica sets.
    Disabled,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogServerConfig {
    pub storage_state: StorageState,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MetadataServerConfig {
    pub metadata_server_state: MetadataServerState,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig {
    pub worker_state: WorkerState,
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
        let node = NodeConfig::builder()
            .name("node1".to_owned())
            .current_generation(current_gen)
            .location("region1.zone1".parse().unwrap())
            .address(address.clone())
            .roles(roles)
            .build();
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
        let node = NodeConfig::builder()
            .name(
                // change name
                "nodeX".to_owned(),
            )
            .current_generation(future_gen.as_generational().unwrap())
            .location("region1.zone1".parse().unwrap())
            .address(address)
            .roles(roles)
            .build();
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

    #[test]
    fn test_remove_node() {
        let mut config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let address: AdvertisedAddress = "unix:/tmp/my_socket".parse().unwrap();
        let node1 = NodeConfig::new(
            "node1".to_owned(),
            GenerationalNodeId::new(1, 1),
            "region1.zone1".parse().unwrap(),
            address.clone(),
            Role::Worker.into(),
            LogServerConfig::default(),
            MetadataServerConfig::default(),
            WorkerConfig::default(),
        );
        let node2 = NodeConfig::new(
            "node2".to_owned(),
            GenerationalNodeId::new(2, 1),
            "region1.zone1".parse().unwrap(),
            address.clone(),
            Role::Worker.into(),
            LogServerConfig::default(),
            MetadataServerConfig::default(),
            WorkerConfig::default(),
        );
        config.upsert_node(node1.clone());
        config.upsert_node(node2.clone());

        assert!(config.name_lookup.contains_key("node1"));
        let found = config.find_node_by_name("node1").expect("known id");
        assert_eq!(&node1, found);

        let found = config.find_node_by_name("node2").expect("known id");
        assert_eq!(&node2, found);
        config.remove_node_unchecked(1);

        assert_eq!(None, config.find_node_by_name("node1"));
        assert!(matches!(
            config.find_node_by_id(PlainNodeId::from(1)),
            Err(NodesConfigError::Deleted(NodeId::Plain(id)))
            if id == PlainNodeId::from(1)
        ));

        // really make sure we have removed it from the name lookup table
        assert!(!config.name_lookup.contains_key("node1"));
    }
}
