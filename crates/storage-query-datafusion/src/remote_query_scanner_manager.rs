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
use parking_lot::Mutex;

use restate_core::Metadata;
use restate_core::partitions::PartitionRouting;
use restate_types::identifiers::PartitionId;
use restate_types::net::remote_query_scanner::{RemoteQueryScannerOpen, ScannerId};
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, NodeId};

use crate::remote_fragment::RemoteFragmentExecution;
use crate::remote_query_scanner_client::{OpenedRemoteScanner, RemoteScannerService, remote_scan};
use crate::table_providers::{DistributedPartitionScanner, Scan, ScanPartition};

/// Process-wide scanner sequence shared by all managers so client-allocated
/// scanner ids do not collide.
static NEXT_SCANNER_SEQ: AtomicU64 = AtomicU64::new(1);

/// Node-local scanners keyed by their DataFusion table name. Registration and
/// remote serving share the registry through cloned managers.
#[derive(Clone, Debug, Default)]
struct LocalPartitionScannerRegistry {
    scanners: Arc<Mutex<BTreeMap<String, Arc<dyn ScanPartition>>>>,
}

impl LocalPartitionScannerRegistry {
    fn get(&self, table_name: &str) -> Option<Arc<dyn ScanPartition>> {
        let guard = self.scanners.lock();
        guard.get(table_name).cloned()
    }

    fn register(&self, table_name: impl Into<String>, scanner: Arc<dyn ScanPartition>) {
        let mut guard = self.scanners.lock();
        guard.insert(table_name.into(), scanner);
    }
}

#[derive(Clone)]
pub struct RemoteScannerManager {
    remote_scanner: Arc<dyn RemoteScannerService>,
    partition_locator: Arc<dyn PartitionLocator>,
    local_scanners: LocalPartitionScannerRegistry,
    metadata: Metadata,
}

impl Debug for RemoteScannerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteScannerManager")
    }
}

/// The owner selected for a partition by the current routing metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionLocation {
    Local,
    Remote { node_id: NodeId },
}

/// Resolves partition ownership independently of DataFusion planning and execution.
pub trait PartitionLocator: Send + Sync + 'static {
    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
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
    ) -> anyhow::Result<PartitionLocation> {
        let my_node_id = self.metadata.my_node_id();
        match self.partition_routing.get_node_by_partition(partition_id) {
            None => bail!("node lookup for partition {partition_id} failed"),
            Some(node_id) if node_id == my_node_id => Ok(PartitionLocation::Local),
            Some(node_id) => Ok(PartitionLocation::Remote {
                node_id: NodeId::from(node_id),
            }),
        }
    }
}

/// A locator for single-process tools whose partitions are always local.
struct AlwaysLocalPartitionLocator;

impl PartitionLocator for AlwaysLocalPartitionLocator {
    fn get_partition_target_node(
        &self,
        _partition_id: PartitionId,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Local)
    }
}

/// A remote-scan service that is never invoked in local-only mode.
#[derive(Debug)]
struct NoopRemoteScanner;

#[async_trait]
impl RemoteScannerService for NoopRemoteScanner {
    async fn open(
        &self,
        _peer: NodeId,
        _req: RemoteQueryScannerOpen,
    ) -> Result<OpenedRemoteScanner, DataFusionError> {
        Err(DataFusionError::External(
            anyhow!("remote scanner is not available in local-only mode").into(),
        ))
    }
}

impl RemoteScannerManager {
    pub fn new(
        remote_scanner: Arc<dyn RemoteScannerService>,
        partition_locator: Arc<dyn PartitionLocator>,
        metadata: Metadata,
    ) -> Self {
        Self {
            remote_scanner,
            partition_locator,
            local_scanners: LocalPartitionScannerRegistry::default(),
            metadata,
        }
    }

    /// Builds a manager for a single-process tool that only ever scans local partitions
    /// (e.g. an offline snapshot inspector). The remote-scan path is never exercised; the
    /// metadata only needs to be valid enough for local scans.
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
    pub(crate) fn allocate_scanner_id(&self) -> ScannerId {
        ScannerId(
            self.metadata.my_node_id(),
            NEXT_SCANNER_SEQ.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// Registers the node-local scanner used to serve RPCs and returns the
    /// planning/execution adapter used by the table provider.
    pub(crate) fn create_distributed_scanner(
        &self,
        table_name: impl Into<String>,
        local_scanner: impl Into<Option<Arc<dyn ScanPartition>>>,
    ) -> impl DistributedPartitionScanner {
        let name = table_name.into();

        if let Some(local_scanner) = local_scanner.into() {
            self.local_scanners.register(name.clone(), local_scanner);
        }

        DistributedTableScanner::new(self.clone(), name)
    }

    /// Registers a node-level scanner that can serve remote scan RPCs for a
    /// node-scoped table (e.g., `loglet_workers`). This wraps the `Scan` impl
    /// as a `ScanPartition` adapter so it integrates with the existing remote
    /// scanner server infrastructure.
    pub fn register_node_scanner(&self, table_name: impl Into<String>, scanner: Arc<dyn Scan>) {
        self.local_scanners
            .register(table_name, Arc::new(ScanToScanPartitionAdapter(scanner)));
    }

    pub(crate) fn local_partition_scanner(&self, table: &str) -> Option<Arc<dyn ScanPartition>> {
        self.local_scanners.get(table)
    }

    fn get_partition_target_node(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<PartitionLocation> {
        self.partition_locator
            .get_partition_target_node(partition_id)
    }

    pub(crate) fn validate_partition_location(
        &self,
        partition_id: PartitionId,
        planned_location: PartitionLocation,
    ) -> anyhow::Result<()> {
        let current_location = self.get_partition_target_node(partition_id)?;
        if current_location != planned_location {
            bail!(
                "partition {partition_id} ownership changed after physical planning: planned {planned_location:?}, current {current_location:?}"
            );
        }
        Ok(())
    }

    pub(crate) fn validate_local_partition_owner(
        &self,
        partition_id: PartitionId,
        expected_owner: GenerationalNodeId,
    ) -> anyhow::Result<()> {
        if self.metadata.my_node_id() != expected_owner {
            bail!(
                "remote scan for partition {partition_id} reached {}, but was planned for {expected_owner}",
                self.metadata.my_node_id()
            );
        }
        self.validate_partition_location(partition_id, PartitionLocation::Local)
    }

    /// Returns a reference to the remote scanner service for use by node-fan-out tables.
    pub(crate) fn remote_scanner_service(&self) -> Arc<dyn RemoteScannerService> {
        self.remote_scanner.clone()
    }
}

// ----- distributed table scanner -----

/// Table-specific bridge between planning-time routing and the two execution
/// paths selected by `PartitionScanExec` and `RemoteNodeExec`.
#[derive(Debug)]
struct DistributedTableScanner {
    manager: RemoteScannerManager,
    table_name: String,
}

impl DistributedTableScanner {
    fn new(manager: RemoteScannerManager, table: impl Into<String>) -> Self {
        Self {
            manager,
            table_name: table.into(),
        }
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

impl DistributedPartitionScanner for DistributedTableScanner {
    fn partition_location(&self, partition_id: PartitionId) -> anyhow::Result<PartitionLocation> {
        self.manager.get_partition_target_node(partition_id)
    }

    fn scan_local_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.manager
            .validate_partition_location(partition_id, PartitionLocation::Local)?;
        self.manager
            .local_partition_scanner(&self.table_name)
            .ok_or_else(|| {
                anyhow!(
                    "local scanner for table {} is not registered",
                    self.table_name
                )
            })?
            .scan_partition(
                partition_id,
                range,
                projection,
                predicate,
                batch_size,
                limit,
                elapsed_compute,
            )
    }

    fn scan_remote_partition(
        &self,
        target_node: NodeId,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        fragment: Option<RemoteFragmentExecution>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let scanner_id = self.manager.allocate_scanner_id();
        Ok(remote_scan(
            self.manager.remote_scanner.clone(),
            target_node,
            scanner_id,
            partition_id,
            range,
            self.table_name.clone(),
            projection,
            predicate,
            batch_size,
            limit,
            target_node.as_generational(),
            fragment,
        ))
    }
}
