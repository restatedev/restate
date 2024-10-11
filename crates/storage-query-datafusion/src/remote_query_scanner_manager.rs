// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

use crate::table_providers::ScanPartition;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;

use crate::remote_query_scanner_client::{remote_scan_as_datafusion_stream, RemoteScannerService};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::NodeId;

/// LocalPartitionScannerRegistry is a mapping between a datafusion registered table name
/// (i.e. sys_inbox, sys_status, etc.) to an implementation of a ScanPartition.
/// This registry is populated when we register all the partitioned tables, and it is accessed
/// by the RemoteQueryScannerServer.
#[derive(Clone, Debug, Default)]
struct LocalPartitionScannerRegistry {
    local_store_scanners: Arc<Mutex<BTreeMap<String, Arc<dyn ScanPartition>>>>,
}

impl LocalPartitionScannerRegistry {
    pub fn get(&self, table_name: &str) -> Option<Arc<dyn ScanPartition>> {
        let guard = self
            .local_store_scanners
            .lock()
            .expect("something isn't right");
        guard.get(table_name).cloned()
    }

    fn register(&self, table_name: impl Into<String>, scanner: Arc<dyn ScanPartition>) {
        let mut guard = self
            .local_store_scanners
            .lock()
            .expect("something isn't right");
        guard.insert(table_name.into(), scanner);
    }
}

#[derive(Clone)]
pub struct RemoteScannerManager {
    svc: Arc<dyn RemoteScannerService>,
    local_store_scanners: LocalPartitionScannerRegistry,
}

impl Debug for RemoteScannerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteScannerManager")
    }
}

pub enum PartitionLocation {
    Local,
    #[allow(dead_code)]
    Remote {
        node_id: NodeId,
    },
}

impl RemoteScannerManager {
    pub fn new(svc: Arc<dyn RemoteScannerService>) -> Self {
        Self {
            svc,
            local_store_scanners: LocalPartitionScannerRegistry::default(),
        }
    }

    /// Combines the local partition scanner for the given table, with an RPC based partition scanner
    /// this is able to both scan partition hosted at the current node, and remote partitions hosted on
    /// other nodes via RPC.
    pub fn create_distributed_scanner(
        &self,
        table_name: impl Into<String>,
        local_scanner: Arc<dyn ScanPartition>,
    ) -> impl ScanPartition + Clone {
        let name = table_name.into();
        // make the local scanner available to serve a remote RPC.
        // see usages of [[local_partition_scanner]]
        // we use the table_name to associate a remote scanner with its local counterpart.
        self.local_store_scanners
            .register(name.clone(), local_scanner.clone());

        RemotePartitionsScanner::new(self.clone(), local_scanner, name)
    }

    pub fn local_partition_scanner(&self, table: &str) -> Option<Arc<dyn ScanPartition>> {
        self.local_store_scanners.get(table)
    }

    pub fn get_partition_target_node(&self, _partition_id: PartitionId) -> PartitionLocation {
        // TODO: obtain this information from the metadata, once exposed
        PartitionLocation::Local
    }
}

// ----- remote partition scanner -----

#[derive(Clone, Debug)]
pub struct RemotePartitionsScanner {
    manager: RemoteScannerManager,
    local_scanner: Arc<dyn ScanPartition>,
    table_name: String,
}

impl RemotePartitionsScanner {
    pub fn new(
        manager: RemoteScannerManager,
        local_scanner: Arc<dyn ScanPartition>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            manager,
            local_scanner,
            table_name: table.into(),
        }
    }
}

impl ScanPartition for RemotePartitionsScanner {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        match self.manager.get_partition_target_node(partition_id) {
            PartitionLocation::Local => {
                self.local_scanner
                    .scan_partition(partition_id, range, projection)
            }
            PartitionLocation::Remote { node_id } => remote_scan_as_datafusion_stream(
                self.manager.svc.clone(),
                node_id,
                partition_id,
                range,
                self.table_name.clone(),
                projection,
            ),
        }
    }
}
