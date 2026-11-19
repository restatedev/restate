// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Worker-local physical capabilities and partition owner resolution.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::physical_plan::PhysicalExpr;

use restate_core::Metadata;
use restate_core::partitions::PartitionRouting;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor};
use restate_platform::sync::Mutex;
use restate_storage_query_api::schema::sharding::ShardingColumn;
use restate_storage_query_api::{QueryMetadata, SelectionSchema};
use restate_types::GenerationalNodeId;
use restate_types::cluster_state::ClusterState;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_util_string::ReString;
use restate_worker_api::ProcessorsManagerHandle;

use crate::QuerySource;
use crate::access::{DeclaredIndex, SelectPrimary};
use crate::placement::PartitionPlacement;
use crate::sharding::PartitionSource;
use crate::table_providers::{Scan, ScanPartition};

#[derive(Clone, derive_more::Debug)]
pub struct SourceRegistry {
    #[debug(skip)]
    partition_reads: Arc<Mutex<PartitionReads>>,
    #[debug(skip)]
    partition_locator: Arc<dyn PartitionLocator>,
    partition_sources: Arc<Mutex<BTreeMap<ReString, Arc<dyn ScanPartition>>>>,
    node_sources: Arc<Mutex<BTreeMap<ReString, Arc<dyn Scan>>>>,
    #[debug(skip)]
    metadata: Metadata,
    #[debug(skip)]
    cluster_state: ClusterState,
}

pub enum PartitionLocation {
    Local,
    Remote { node_id: GenerationalNodeId },
}

/// How this node captures its partitions for queries, with the query IO pool,
/// shared with V1, that their storage reads run on.
#[derive(Clone, Default)]
enum PartitionReads {
    #[default]
    Unavailable,
    Processors(ProcessorsManagerHandle, QueryIoExecutor),
    Offline(Arc<PartitionStoreManager>, QueryIoExecutor),
}

pub trait PartitionLocator: Send + Sync + 'static {
    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation>;
}

struct MetadataAwarePartitionLocator {
    partition_routing: PartitionRouting,
    metadata: Metadata,
}

pub fn create_partition_locator(
    partition_routing: PartitionRouting,
    metadata: Metadata,
) -> Arc<dyn PartitionLocator> {
    Arc::new(MetadataAwarePartitionLocator {
        partition_routing,
        metadata,
    })
}

impl PartitionLocator for MetadataAwarePartitionLocator {
    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        let target = if placement.requires_leader() {
            self.partition_routing
                .get_leader(partition_id)
                .map(|(node, _)| node)
        } else {
            self.partition_routing.get_node_by_partition(partition_id)
        };
        match target {
            None => anyhow::bail!("node lookup for partition {partition_id} failed"),
            Some(node_id) if node_id == self.metadata.my_node_id() => Ok(PartitionLocation::Local),
            Some(node_id) => Ok(PartitionLocation::Remote { node_id }),
        }
    }
}

struct AlwaysLocalPartitionLocator;

impl PartitionLocator for AlwaysLocalPartitionLocator {
    fn get_partition_target_node(
        &self,
        _: PartitionId,
        _: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Local)
    }
}

impl SourceRegistry {
    /// Captures partitions through their processors; storage reads run on
    /// `query_io`, the pool V1 uses.
    pub fn register_processor_reads(
        &self,
        processors: ProcessorsManagerHandle,
        query_io: QueryIoExecutor,
    ) {
        *self.partition_reads.lock() = PartitionReads::Processors(processors, query_io);
    }

    /// Explicit offline/test capability. Production worker registration uses PPM.
    pub fn register_offline_reads(
        &self,
        stores: Arc<PartitionStoreManager>,
        query_io: QueryIoExecutor,
    ) {
        *self.partition_reads.lock() = PartitionReads::Offline(stores, query_io);
    }

    /// The query IO pool of the registered partition reads.
    pub(crate) fn query_io(&self) -> anyhow::Result<QueryIoExecutor> {
        match &*self.partition_reads.lock() {
            PartitionReads::Processors(_, query_io) | PartitionReads::Offline(_, query_io) => {
                Ok(query_io.clone())
            }
            PartitionReads::Unavailable => {
                anyhow::bail!("partition query admission is unavailable")
            }
        }
    }

    pub(crate) async fn capture_partition(
        &self,
        partition: PartitionId,
        require_leader: bool,
        metadata: Option<Arc<QueryMetadata>>,
        capture: restate_worker_api::PartitionCaptureRequirements,
    ) -> anyhow::Result<restate_worker_api::CapturedPartitionRead> {
        let access = self.partition_reads.lock().clone();
        match access {
            PartitionReads::Processors(processors, _) => {
                let metadata = metadata
                    .ok_or_else(|| anyhow::anyhow!("partition query has no execution metadata"))?;
                Ok(processors
                    .capture_partition(partition, require_leader, metadata, capture)
                    .await?)
            }
            PartitionReads::Offline(stores, _) => {
                let store = stores.get_partition_store(partition).await.ok_or_else(|| {
                    anyhow::anyhow!("partition {partition} doesn't exist on this node")
                })?;
                // No processor admits an offline read: it is admitted once its
                // store is found.
                let admitted = tokio::time::Instant::now();
                let mut timing = restate_worker_api::CaptureTiming::admitted_at(admitted);
                let storage = if capture.storage {
                    let read = store
                        .query_read(tokio_util::sync::CancellationToken::new())?
                        .0;
                    timing.snapshot = Some(restate_worker_api::CaptureSpan::since(admitted));
                    Some(read)
                } else {
                    None
                };
                Ok(restate_worker_api::CapturedPartitionRead {
                    partition,
                    storage,
                    live: restate_worker_api::CapturedPartitionState::empty(capture),
                    timing,
                })
            }
            PartitionReads::Unavailable => {
                anyhow::bail!("partition query admission is unavailable")
            }
        }
    }

    pub fn new(
        partition_locator: Arc<dyn PartitionLocator>,
        metadata: Metadata,
        cluster_state: ClusterState,
    ) -> Self {
        Self {
            partition_reads: Default::default(),
            partition_locator,
            partition_sources: Default::default(),
            node_sources: Default::default(),
            metadata,
            cluster_state,
        }
    }

    /// The cluster metadata the planner reads, such as the nodes' versions.
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Offline execution uses the same prepared sources without cluster routing.
    pub fn local_only(metadata: Metadata) -> Self {
        Self::new(
            Arc::new(AlwaysLocalPartitionLocator),
            metadata,
            ClusterState::default(),
        )
    }

    pub(crate) fn node_id(&self) -> GenerationalNodeId {
        self.metadata.my_node_id()
    }

    /// Choose one serving node for a complete logical shard. An available local
    /// source is live by construction; remote candidates require observed liveness
    /// for their exact configured generation. Roles are hard eligibility filters.
    pub(crate) fn singleton_owner(
        &self,
        role: Option<Role>,
        local_available: bool,
    ) -> anyhow::Result<GenerationalNodeId> {
        let me = self.node_id();
        if local_available && role.is_none() {
            return Ok(me);
        }
        let nodes = self.metadata.nodes_config_snapshot();
        if local_available
            && nodes
                .find_node_by_id(me)
                .is_ok_and(|node| role.is_none_or(|role| node.has_role(role)))
        {
            return Ok(me);
        }
        nodes
            .iter()
            .filter(|(_, node)| role.is_none_or(|role| node.has_role(role)))
            .map(|(_, node)| node.current_generation)
            .filter(|node| *node != me && self.cluster_state.is_alive((*node).into()))
            .min()
            .ok_or_else(|| {
                anyhow::anyhow!("no alive eligible node for unsharded source (role: {role:?})")
            })
    }

    pub(crate) fn partition_owner(
        &self,
        partition: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<GenerationalNodeId> {
        Ok(
            match self.get_partition_target_node(partition, placement)? {
                PartitionLocation::Local => self.node_id(),
                PartitionLocation::Remote { node_id } => node_id,
            },
        )
    }

    pub(crate) fn create_declared_source<T: PartitionSource>(&self) -> PartitionSourceDescriptor {
        PartitionSourceDescriptor {
            table: T::identity(),
            manager: self.clone(),
            require_leader: T::requires_leader(),
            sharding_columns: crate::sharding::exposed_sharding_columns::<T::Schema>(),
            primary_access: T::PRIMARY_ACCESS,
            indexes: T::INDEXES
                .iter()
                .map(crate::access::IndexAccess::declared)
                .collect(),
            can_read: T::can_read,
            ordering: T::ORDERING
                .iter()
                .map(|&column| {
                    T::Schema::sql_name(column)
                        .expect("ordered column has a SQL representation")
                        .to_owned()
                })
                .collect(),
        }
    }

    pub(crate) fn register_declared_scanner<T: PartitionSource>(
        &self,
        scanner: Arc<dyn ScanPartition>,
    ) {
        assert!(
            !scanner.requires_leader() || T::requires_leader(),
            "declaration must satisfy the backend's serving requirements"
        );
        assert_eq!(
            scanner.supports_primary_lookup(),
            T::PRIMARY_ACCESS.is_some(),
            "source capabilities must match its declaration"
        );
        assert!(
            T::INDEXES
                .iter()
                .all(|index| scanner.supports_index(index.declared().identity())),
            "declared indexes must be readable by the source's scanner"
        );
        assert!(
            T::INDEXES.iter().all(|index| {
                restate_types::SemanticRestateVersion::current()
                    .release_is_equal_or_newer_than(&index.declared().introduced())
            }),
            "a declared index can't be newer than this binary"
        );
        self.register_partition_scanner::<T>(scanner);
    }

    pub fn register_partition_scanner<T: QuerySource>(&self, scanner: Arc<dyn ScanPartition>) {
        assert!(
            self.partition_sources
                .lock()
                .insert(T::identity(), scanner)
                .is_none(),
            "duplicate local partition source"
        );
    }

    pub fn register_node_scanner<T: QuerySource>(&self, scanner: Arc<dyn Scan>) {
        assert!(
            self.node_sources
                .lock()
                .insert(T::identity(), scanner)
                .is_none(),
            "duplicate local node source"
        );
    }

    /// Removes the local source of `T`, as on a worker that predates it.
    #[cfg(test)]
    pub(crate) fn unregister_partition_scanner<T: QuerySource>(&self) {
        self.partition_sources.lock().remove(&T::identity());
    }

    pub fn local_partition_scanner(&self, table: &str) -> Option<Arc<dyn ScanPartition>> {
        self.partition_sources.lock().get(table).cloned()
    }

    pub(crate) fn local_node_scanner(&self, table: &str) -> Option<Arc<dyn Scan>> {
        self.node_sources.lock().get(table).cloned()
    }

    pub fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        self.partition_locator
            .get_partition_target_node(partition_id, placement)
    }
}

type SourceEligibility = fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool>;

#[derive(Clone, derive_more::Debug)]
pub(crate) struct PartitionSourceDescriptor {
    pub table: ReString,
    pub manager: SourceRegistry,
    pub require_leader: bool,
    /// Sharding columns at ordinals of the source's (projected) Arrow schema.
    pub sharding_columns: Vec<ShardingColumn<usize>>,
    pub ordering: Vec<String>,
    #[debug(skip)]
    pub primary_access: Option<SelectPrimary>,
    #[debug(skip)]
    pub indexes: Vec<DeclaredIndex>,
    #[debug(skip)]
    pub can_read: SourceEligibility,
}

impl PartitionSourceDescriptor {
    /// Carry sharding-column identity through the provider's ordinal projection.
    pub(crate) fn project(&self, projection: Option<&[usize]>) -> Self {
        Self {
            sharding_columns: crate::sharding::project_sharding_columns(
                &self.sharding_columns,
                projection,
            ),
            ..self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use restate_core::TaskCenter;
    use restate_core::test_env::TestCoreEnv;
    use restate_types::Version;
    use restate_types::cluster_state::NodeState;
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partitions::state::{
        LeadershipState, MemberState, PartitionReplicaSetStates, ReplicaSetState,
    };

    use crate::placement::StoragePlacementOptions;

    use super::*;

    #[restate_core::test]
    async fn placement_distinguishes_serving_replica_from_required_leader() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let me = env.metadata.my_node_id();
        TaskCenter::current()
            .cluster_state()
            .clone()
            .updater()
            .upsert_node_state(me, NodeState::Alive);
        let states = PartitionReplicaSetStates::default();
        states.note_observed_membership(
            PartitionId::MIN,
            LeadershipState::default(),
            &ReplicaSetState {
                version: Version::MIN,
                members: vec![MemberState {
                    node_id: me.as_plain(),
                    durable_lsn: Lsn::INVALID,
                }],
            },
            &None,
        );
        let locator = create_partition_locator(
            PartitionRouting::new(states.clone(), TaskCenter::current()),
            env.metadata,
        );
        let storage = PartitionPlacement::default();
        let leader_storage = PartitionPlacement {
            options: StoragePlacementOptions {
                require_leader: true,
            },
            ..storage
        };
        let live = PartitionPlacement {
            require_leader: true,
            ..storage
        };
        assert!(matches!(
            locator
                .get_partition_target_node(PartitionId::MIN, storage)
                .unwrap(),
            PartitionLocation::Local
        ));
        for placement in [leader_storage, live] {
            assert!(
                locator
                    .get_partition_target_node(PartitionId::MIN, placement)
                    .is_err()
            );
        }
        let leader = GenerationalNodeId::new(2, 7);
        states.note_observed_leader(
            PartitionId::MIN,
            LeadershipState {
                current_leader: leader,
                current_leader_epoch: LeaderEpoch::from(1),
            },
        );
        for placement in [storage, leader_storage, live] {
            assert!(
                matches!(locator.get_partition_target_node(PartitionId::MIN, placement).unwrap(), PartitionLocation::Remote { node_id } if node_id == leader)
            );
        }
    }
}
