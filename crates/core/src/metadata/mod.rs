// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: Remove after implementation is complete
#![allow(dead_code)]

mod manager;

pub use manager::{MetadataManager, TargetVersion};
use restate_types::live::{Live, Pinned};
use restate_types::schema::Schema;

use std::sync::{Arc, OnceLock};

use arc_swap::{ArcSwap, AsRaw};
use enum_map::EnumMap;
use tokio::sync::{mpsc, oneshot, watch};

use restate_types::logs::metadata::Logs;
use restate_types::net::metadata::MetadataContainer;
pub use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::FixedPartitionTable;
use restate_types::{GenerationalNodeId, NodeId, Version, Versioned};

use crate::metadata::manager::Command;
use crate::metadata_store::ReadError;
use crate::network::NetworkSender;
use crate::{ShutdownError, TaskCenter, TaskId, TaskKind};

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("failed syncing with metadata store: {0}")]
    MetadataStore(#[from] ReadError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

pub struct MetadataBuilder {
    receiver: manager::CommandReceiver,
    metadata: Metadata,
}

impl MetadataBuilder {
    pub fn to_metadata(&self) -> Metadata {
        self.metadata.clone()
    }
}

impl Default for MetadataBuilder {
    fn default() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            receiver,
            metadata: Metadata {
                inner: Default::default(),
                sender,
            },
        }
    }
}

/// The kind of versioned metadata that can be synchronized across nodes.
#[derive(Clone)]
pub struct Metadata {
    sender: manager::CommandSender,
    inner: Arc<MetadataInner>,
}

impl Metadata {
    #[inline(always)]
    pub fn nodes_config_snapshot(&self) -> Arc<NodesConfiguration> {
        self.inner.nodes_config.load_full()
    }

    #[inline(always)]
    pub fn nodes_config_ref(&self) -> Pinned<NodesConfiguration> {
        Pinned::new(&self.inner.nodes_config)
    }

    pub fn updateable_nodes_config(&self) -> Live<NodesConfiguration> {
        Live::from(self.inner.nodes_config.clone())
    }

    #[track_caller]
    pub fn my_node_id(&self) -> GenerationalNodeId {
        *self.inner.my_node_id.get().expect("my_node_id is set")
    }

    /// Returns Version::INVALID if nodes configuration has not been loaded yet.
    pub fn nodes_config_version(&self) -> Version {
        self.inner.nodes_config.load().version()
    }

    pub fn partition_table_snapshot(&self) -> Arc<FixedPartitionTable> {
        self.inner.partition_table.load_full()
    }

    #[inline(always)]
    pub fn partition_table_ref(&self) -> Pinned<FixedPartitionTable> {
        Pinned::new(&self.inner.partition_table)
    }

    pub fn updateable_partition_table(&self) -> Live<FixedPartitionTable> {
        Live::from(self.inner.partition_table.clone())
    }

    /// Returns Version::INVALID if partition table has not been loaded yet.
    pub fn partition_table_version(&self) -> Version {
        self.inner.partition_table.load().version()
    }

    pub fn version(&self, metadata_kind: MetadataKind) -> Version {
        match metadata_kind {
            MetadataKind::NodesConfiguration => self.nodes_config_version(),
            MetadataKind::Schema => self.schema_version(),
            MetadataKind::PartitionTable => self.partition_table_version(),
            MetadataKind::Logs => self.logs_version(),
        }
    }

    /// Waits until the partition table of at least min_version is available and returns it.
    pub async fn wait_for_partition_table(
        &self,
        min_version: Version,
    ) -> Result<Arc<FixedPartitionTable>, ShutdownError> {
        let partition_table = self.partition_table_ref();
        if partition_table.version() >= min_version {
            return Ok(partition_table.into_arc());
        }

        self.wait_for_version(MetadataKind::PartitionTable, min_version)
            .await?;
        Ok(self.partition_table_snapshot())
    }

    pub fn logs(&self) -> Pinned<Logs> {
        Pinned::new(&self.inner.logs)
    }

    /// Returns Version::INVALID if logs has not been loaded yet.
    pub fn logs_version(&self) -> Version {
        self.inner.logs.load().version()
    }

    pub fn schema(&self) -> Pinned<Schema> {
        Pinned::new(&self.inner.schema)
    }

    pub fn schema_version(&self) -> Version {
        self.inner.schema.load().version()
    }

    pub fn schema_ref(&self) -> Pinned<Schema> {
        Pinned::new(&self.inner.schema)
    }

    pub fn updateable_schema(&self) -> Live<Schema> {
        Live::from(self.inner.schema.clone())
    }

    pub fn updateable_logs_metadata(&self) -> Live<Logs> {
        Live::from(self.inner.logs.clone())
    }

    // Returns when the metadata kind is at the provided version (or newer)
    pub async fn wait_for_version(
        &self,
        metadata_kind: MetadataKind,
        min_version: Version,
    ) -> Result<Version, ShutdownError> {
        let mut recv = self.inner.write_watches[metadata_kind].receive.clone();
        let v = recv
            .wait_for(|v| *v >= min_version)
            .await
            .map_err(|_| ShutdownError)?;
        Ok(*v)
    }

    /// Watch for version updates of this metadata kind.
    pub fn watch(&self, metadata_kind: MetadataKind) -> watch::Receiver<Version> {
        self.inner.write_watches[metadata_kind].receive.clone()
    }

    /// Syncs the given metadata_kind from the underlying metadata store if the current version is
    /// lower than target version.
    ///
    /// Note: If the target version does not exist, then a lower version will be available after
    /// this call completes.
    pub async fn sync(
        &self,
        metadata_kind: MetadataKind,
        target_version: TargetVersion,
    ) -> Result<(), SyncError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.sender
            .send(Command::SyncMetadata(
                metadata_kind,
                target_version,
                result_tx,
            ))
            .map_err(|_| ShutdownError)?;
        result_rx.await.map_err(|_| ShutdownError)??;

        Ok(())
    }

    /// Notifies the metadata manager about a newly observed metadata version for the given kind.
    /// If the metadata can be retrieved from a node, then the [`NodeId`] can be included as well.
    pub fn notify_observed_version(
        &self,
        metadata_kind: MetadataKind,
        version: Version,
        remote_location: Option<NodeId>,
    ) {
        // check whether the version is newer than what we know
        if version > self.version(metadata_kind) {
            let mut guard = self.inner.observed_versions[metadata_kind].load();

            // check whether it is even newer than the latest observed version
            if version > guard.version {
                // Create the arc outside of loop to avoid reallocations in case of contention;
                // maybe this is guarding too much against the contended case.
                let new_version_information =
                    Arc::new(VersionInformation::new(version, remote_location));

                // maybe a simple Arc<Mutex<VersionInformation>> works better? Needs a benchmark.
                loop {
                    let cas_guard = self.inner.observed_versions[metadata_kind]
                        .compare_and_swap(&guard, Arc::clone(&new_version_information));

                    if std::ptr::eq(cas_guard.as_raw(), guard.as_raw()) {
                        break;
                    }

                    guard = cas_guard;

                    // stop trying to update the observed value if a newer one was reported before
                    if guard.version >= version {
                        break;
                    }
                }
            }
        }
    }

    /// Returns the [`VersionInformation`] for the metadata kind if a newer version than the local
    /// version has been observed.
    fn observed_version(&self, metadata_kind: MetadataKind) -> Option<VersionInformation> {
        let guard = self.inner.observed_versions[metadata_kind].load();

        if guard.version > self.version(metadata_kind) {
            Some((**guard).clone())
        } else {
            None
        }
    }
}

#[derive(Default)]
struct MetadataInner {
    my_node_id: OnceLock<GenerationalNodeId>,
    nodes_config: Arc<ArcSwap<NodesConfiguration>>,
    partition_table: Arc<ArcSwap<FixedPartitionTable>>,
    logs: Arc<ArcSwap<Logs>>,
    schema: Arc<ArcSwap<Schema>>,
    write_watches: EnumMap<MetadataKind, VersionWatch>,
    // might be subject to false sharing if independent sources want to update different metadata
    // kinds concurrently.
    observed_versions: EnumMap<MetadataKind, ArcSwap<VersionInformation>>,
}

/// Can send updates to metadata manager. This should be accessible by the rpc handler layer to
/// handle incoming metadata updates from the network, or to handle updates coming from metadata
/// service if it's running on this node. MetadataManager ensures that writes are monotonic
/// so it's safe to call update_* without checking the current version.
#[derive(Clone)]
pub struct MetadataWriter {
    sender: manager::CommandSender,
    /// strictly used to set my node id. Do not use this to update metadata
    /// directly to avoid race conditions.
    inner: Arc<MetadataInner>,
}

impl MetadataWriter {
    fn new(sender: manager::CommandSender, inner: Arc<MetadataInner>) -> Self {
        Self { sender, inner }
    }

    // Returns when the nodes configuration update is performed.
    pub async fn update(&self, value: impl Into<MetadataContainer>) -> Result<(), ShutdownError> {
        let (callback, recv) = oneshot::channel();
        let o = self.sender.send(manager::Command::UpdateMetadata(
            value.into(),
            Some(callback),
        ));
        if o.is_ok() {
            let _ = recv.await;
            Ok(())
        } else {
            Err(ShutdownError)
        }
    }

    /// Should be called once on node startup. Updates are ignored after the initial value is set.
    pub fn set_my_node_id(&self, id: GenerationalNodeId) {
        self.inner.my_node_id.set(id).expect("My node is not set");
    }

    // Fire and forget update
    pub fn submit(&self, value: impl Into<MetadataContainer>) {
        // Ignore the error, task-center takes care of safely shutting down the
        // system if metadata manager failed
        let _ = self
            .sender
            .send(manager::Command::UpdateMetadata(value.into(), None));
    }
}

struct VersionWatch {
    sender: watch::Sender<Version>,
    receive: watch::Receiver<Version>,
}

impl Default for VersionWatch {
    fn default() -> Self {
        let (send, receive) = watch::channel(Version::INVALID);
        Self {
            sender: send,
            receive,
        }
    }
}

pub fn spawn_metadata_manager<N>(
    tc: &TaskCenter,
    metadata_manager: MetadataManager<N>,
) -> Result<TaskId, ShutdownError>
where
    N: NetworkSender + 'static,
{
    tc.spawn(
        TaskKind::MetadataBackgroundSync,
        "metadata-manager",
        None,
        metadata_manager.run(),
    )
}

#[derive(Debug, Clone)]
struct VersionInformation {
    version: Version,
    remote_node: Option<NodeId>,
}

impl Default for VersionInformation {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            remote_node: None,
        }
    }
}

impl VersionInformation {
    fn new(version: Version, remote_location: Option<NodeId>) -> Self {
        Self {
            version,
            remote_node: remote_location,
        }
    }
}
