// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;

use restate_core::Metadata;
use restate_core::partitions::PartitionRouting;
use restate_platform::sync::Mutex;
use restate_storage_query_api::QueryEngineTable;
use restate_types::NodeId;
use restate_types::identifiers::PartitionId;
use restate_types::net::remote_query_scanner::{RemoteQueryScannerOpen, ScannerId};
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::access::PrimaryKeyKind;
use crate::placement::{PartitionPlacement, PartitionSource};
use crate::remote_query_scanner_client::{
    RemoteScanner, RemoteScannerService, remote_scan_as_datafusion_stream,
};
use crate::table_providers::{Scan, ScanPartition};

// A global scanner sequence generate shared across all RemoteScannerManager
// instances to avoid scanner-id conflicts
static NEXT_SCANNER_SEQ: AtomicU64 = AtomicU64::new(1);

/// Maps stable query-source identities to local scanner implementations, independently of
/// the SQL names under which sessions expose their providers.
/// This registry is populated during local capability registration and accessed by both
/// local plans and the RemoteQueryScannerServer.
#[derive(Clone, Debug, Default)]
struct LocalPartitionScannerRegistry {
    local_store_scanners: Arc<Mutex<BTreeMap<ReString, Arc<dyn ScanPartition>>>>,
}

impl LocalPartitionScannerRegistry {
    pub fn get(&self, table_name: &str) -> Option<Arc<dyn ScanPartition>> {
        let guard = self.local_store_scanners.lock();
        guard.get(table_name).cloned()
    }

    fn register<T: QueryEngineTable>(&self, scanner: Arc<dyn ScanPartition>) {
        let mut guard = self.local_store_scanners.lock();
        match guard.entry(T::identity()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(scanner);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                panic!("duplicate local query scanner identity '{}'", entry.key());
            }
        }
    }
}

#[derive(Clone)]
pub struct RemoteScannerManager {
    remote_scanner: Arc<dyn RemoteScannerService>,
    partition_locator: Arc<dyn PartitionLocator>,
    local_store_scanners: LocalPartitionScannerRegistry,
    local_node_scanners: Arc<Mutex<BTreeMap<ReString, Arc<dyn Scan>>>>,
    metadata: Metadata,
}

impl Debug for RemoteScannerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteScannerManager")
    }
}

pub enum PartitionLocation {
    Local,
    Remote { node_id: NodeId },
}

pub trait PartitionLocator: Send + Sync + 'static {
    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation>;
}

#[derive(Clone)]
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
        let my_node_id = self.metadata.my_node_id();
        let target = if placement.requires_leader() {
            self.partition_routing
                .get_leader(partition_id)
                .map(|(node, _)| node)
        } else {
            self.partition_routing.get_node_by_partition(partition_id)
        };
        match target {
            None => {
                bail!("node lookup for partition {} failed", partition_id)
            }
            Some(node_id) if node_id == my_node_id => Ok(PartitionLocation::Local),
            Some(node_id) => Ok(PartitionLocation::Remote {
                node_id: NodeId::from(node_id),
            }),
        }
    }
}

/// A locator that reports every partition as local. Used by [`RemoteScannerManager::local_only`].
struct AlwaysLocalPartitionLocator;

impl PartitionLocator for AlwaysLocalPartitionLocator {
    fn get_partition_target_node(
        &self,
        _partition_id: PartitionId,
        _placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Local)
    }
}

/// A remote-scan service that is never invoked because [`AlwaysLocalPartitionLocator`] keeps all
/// scans local. Used by [`RemoteScannerManager::local_only`].
#[derive(Debug)]
struct NoopRemoteScanner;

#[async_trait]
impl RemoteScannerService for NoopRemoteScanner {
    async fn open(
        &self,
        _peer: NodeId,
        _req: RemoteQueryScannerOpen,
    ) -> Result<RemoteScanner, DataFusionError> {
        Err(DataFusionError::External(
            anyhow!("remote scanner is not available in local-only mode").into(),
        ))
    }
}

impl RemoteScannerManager {
    pub(crate) fn node_id(&self) -> restate_types::GenerationalNodeId {
        self.metadata.my_node_id()
    }

    pub(crate) fn partition_owner(
        &self,
        partition: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<restate_types::GenerationalNodeId> {
        match self.get_partition_target_node(partition, placement)? {
            PartitionLocation::Local => Ok(self.node_id()),
            PartitionLocation::Remote { node_id } => Ok(self
                .metadata
                .nodes_config_ref()
                .find_node_by_id(node_id)?
                .current_generation),
        }
    }

    pub fn new(
        remote_scanner: Arc<dyn RemoteScannerService>,
        partition_locator: Arc<dyn PartitionLocator>,
        metadata: Metadata,
    ) -> Self {
        Self {
            remote_scanner,
            partition_locator,
            local_store_scanners: LocalPartitionScannerRegistry::default(),
            local_node_scanners: Default::default(),
            metadata,
        }
    }

    /// Builds a manager for a single-process tool that only ever scans local partitions
    /// (e.g. an offline snapshot inspector). The remote-scan path is never exercised because
    /// the locator always reports partitions as [`PartitionLocation::Local`]; the metadata only
    /// needs to be valid enough for local scans.
    pub fn local_only(metadata: Metadata) -> Self {
        Self::new(
            Arc::new(NoopRemoteScanner),
            Arc::new(AlwaysLocalPartitionLocator),
            metadata,
        )
    }

    /// Allocates a fresh `ScannerId` for a remote scan initiated from this node.
    ///
    /// Combining this node's generational id with a process-local monotonic counter
    /// guarantees uniqueness across the cluster: the generation distinguishes restarts,
    /// and the counter distinguishes concurrent scans within one process lifetime.
    pub fn allocate_scanner_id(&self) -> ScannerId {
        ScannerId(
            self.metadata.my_node_id(),
            NEXT_SCANNER_SEQ.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// Creates a routing adapter without changing the node's registered local capabilities.
    pub(crate) fn create_partition_source<T: QueryEngineTable>(&self) -> PartitionedSource {
        PartitionedSource {
            table: T::identity(),
            manager: self.clone(),
            source: PartitionSource::Storage,
            primary_key: None,
        }
    }

    pub(crate) fn create_live_source<T: QueryEngineTable>(&self) -> PartitionedSource {
        PartitionedSource {
            source: PartitionSource::LeaderLive,
            ..self.create_partition_source::<T>()
        }
    }

    /// Registers a node-local implementation for a partition-scoped source.
    /// Registration does not open a partition or grant access to its database.
    pub fn register_partition_scanner<T: QueryEngineTable>(&self, scanner: Arc<dyn ScanPartition>) {
        self.local_store_scanners.register::<T>(scanner);
    }

    /// Registers a node-level scanner that can serve remote scan RPCs for a
    /// node-scoped table (e.g., `loglet_workers`). This wraps the `Scan` impl
    /// as a `ScanPartition` adapter so it integrates with the existing remote
    /// scanner server infrastructure.
    pub fn register_node_scanner<T: QueryEngineTable>(&self, scanner: Arc<dyn Scan>) {
        assert!(
            self.local_node_scanners
                .lock()
                .insert(T::identity(), Arc::clone(&scanner))
                .is_none(),
            "duplicate local node source"
        );
        self.local_store_scanners
            .register::<T>(Arc::new(ScanToScanPartitionAdapter(scanner)));
    }

    pub fn local_partition_scanner(&self, table: &str) -> Option<Arc<dyn ScanPartition>> {
        self.local_store_scanners.get(table)
    }

    pub(crate) fn local_node_scanner(&self, table: &str) -> Option<Arc<dyn Scan>> {
        self.local_node_scanners.lock().get(table).cloned()
    }

    pub fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
        placement: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        self.partition_locator
            .get_partition_target_node(partition_id, placement)
    }

    /// Returns a reference to the remote scanner service for use by node-fan-out tables.
    pub fn remote_scanner_service(&self) -> Arc<dyn RemoteScannerService> {
        self.remote_scanner.clone()
    }
}

// ----- remote partition scanner -----

#[derive(Clone, Debug)]
pub(crate) struct PartitionedSource {
    pub table: ReString,
    pub manager: RemoteScannerManager,
    pub source: PartitionSource,
    pub primary_key: Option<PrimaryKeyKind>,
}

impl PartitionedSource {
    pub(crate) fn with_primary_key(mut self, kind: PrimaryKeyKind) -> Self {
        self.primary_key = Some(kind);
        self
    }
}

/// Adapts a node-level `Scan` into a `ScanPartition` for the remote scanner
/// server. The partition_id and range are ignored since this is a node-scoped table.
#[derive(Debug)]
struct ScanToScanPartitionAdapter(Arc<dyn Scan>);

impl ScanPartition for ScanToScanPartitionAdapter {
    fn scan_partition(
        &self,
        _partition_id: PartitionId,
        _range: KeyRange,
        projection: SchemaRef,
        _predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        _elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        // Node-level scanners don't use partition-based predicates
        Ok(self.0.scan(projection, &[], batch_size, limit))
    }
}

// Compatibility adapter for a non-distributed environment using the old scanner RPC.
// Distributed and local-only planning build concrete access operators instead.
impl ScanPartition for PartitionedSource {
    fn partition_source(&self) -> PartitionSource {
        self.source
    }

    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        match self.manager.get_partition_target_node(
            partition_id,
            PartitionPlacement {
                source: self.source,
                ..Default::default()
            },
        )? {
            PartitionLocation::Local => {
                let scanner = self.manager.local_partition_scanner(&self.table).ok_or_else(
                    ||anyhow!("was expecting a local partition to be present on this node. It could be that this partition is being opened right now.")
                )?;
                Ok(scanner.scan_partition(
                    partition_id,
                    range,
                    projection,
                    predicate,
                    batch_size,
                    limit,
                    elapsed_compute,
                )?)
            }
            PartitionLocation::Remote { node_id } => {
                let scanner_id = self.manager.allocate_scanner_id();
                Ok(remote_scan_as_datafusion_stream(
                    self.manager.remote_scanner.clone(),
                    node_id,
                    scanner_id,
                    partition_id,
                    range,
                    self.table.clone(),
                    projection,
                    predicate,
                    batch_size,
                    limit,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use restate_core::TaskCenter;
    use restate_core::test_env::TestCoreEnv;
    use restate_types::cluster_state::NodeState;
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partitions::state::{
        LeadershipState, MemberState, PartitionReplicaSetStates, ReplicaSetState,
    };
    use restate_types::{GenerationalNodeId, Version};

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
            source: PartitionSource::LeaderLive,
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
                matches!(locator.get_partition_target_node(PartitionId::MIN, placement).unwrap(), PartitionLocation::Remote { node_id } if node_id == NodeId::from(leader))
            );
        }
    }
}
