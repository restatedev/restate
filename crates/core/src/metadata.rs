// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod manager;
mod metadata_client_wrapper;
mod update_task;

use ahash::HashMap;
use tokio::time::Instant;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use enum_map::EnumMap;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::instrument;

use restate_metadata_store::{MetadataStoreClient, ReadError};
use restate_types::live::{Live, Pinned};
use restate_types::logs::metadata::Logs;
use restate_types::metadata::GlobalMetadata;
pub use restate_types::net::metadata::MetadataKind;
use restate_types::net::metadata::{self, MetadataContainer};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;
use restate_types::{GenerationalNodeId, Version, Versioned};

pub use self::manager::{MetadataManager, TargetVersion};
use crate::network::Connection;
use crate::{ShutdownError, TaskCenter, TaskId, TaskKind};

use self::metadata_client_wrapper::MetadataClientWrapper;

#[derive(Clone, Debug, thiserror::Error)]
pub enum SyncError {
    #[error("failed syncing with metadata store: {0}")]
    MetadataStore(#[from] Arc<ReadError>),
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
    pub fn try_with_current<F, R>(f: F) -> Option<R>
    where
        F: Fn(&Metadata) -> R,
    {
        TaskCenter::with_metadata(|m| f(m))
    }

    pub fn try_current() -> Option<Metadata> {
        TaskCenter::with_current(|tc| tc.metadata())
    }

    #[track_caller]
    pub fn with_current<F, R>(f: F) -> R
    where
        F: FnOnce(&Metadata) -> R,
    {
        TaskCenter::with_metadata(|m| f(m)).expect("called outside task-center scope")
    }

    #[track_caller]
    pub fn current() -> Metadata {
        TaskCenter::with_current(|tc| tc.metadata()).expect("called outside task-center scope")
    }

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

    /// Returns None if my node id has not been set yet.
    pub fn my_node_id_opt(&self) -> Option<GenerationalNodeId> {
        self.inner.my_node_id.get().cloned()
    }

    /// Returns Version::INVALID if nodes configuration has not been loaded yet.
    pub fn nodes_config_version(&self) -> Version {
        self.inner.nodes_config.load().version()
    }

    pub fn partition_table_snapshot(&self) -> Arc<PartitionTable> {
        self.inner.partition_table.load_full()
    }

    #[inline(always)]
    pub fn partition_table_ref(&self) -> Pinned<PartitionTable> {
        Pinned::new(&self.inner.partition_table)
    }

    pub fn updateable_partition_table(&self) -> Live<PartitionTable> {
        Live::from(self.inner.partition_table.clone())
    }

    /// Returns Version::INVALID if partition table has not been loaded yet.
    pub fn partition_table_version(&self) -> Version {
        self.inner.partition_table.load().version()
    }

    /// Sets the metadata container unconditionally to the supplied value.
    /// Used for testing to mutate the global metadata object without using the metadata writer.
    ///
    /// Note: This doesn't trigger any validation or version checks and can race with the metadata
    /// writer if it's running.
    #[cfg(feature = "test-util")]
    pub fn set(&self, value: MetadataContainer) {
        let new_version = value.version();

        tracing::info!(
            kind = %value.kind(),
            ?value,
            "[testing] Manually updating to {new_version}",
        );

        match value {
            MetadataContainer::NodesConfiguration(value) => {
                self.inner.nodes_config.store(value);
                self.inner.write_watches[MetadataKind::NodesConfiguration]
                    .sender
                    .send_replace(new_version);
            }
            MetadataContainer::PartitionTable(value) => {
                self.inner.partition_table.store(value);
                self.inner.write_watches[MetadataKind::PartitionTable]
                    .sender
                    .send_replace(new_version);
            }
            MetadataContainer::Logs(value) => {
                self.inner.logs.store(value);
                self.inner.write_watches[MetadataKind::Logs]
                    .sender
                    .send_replace(new_version);
            }
            MetadataContainer::Schema(value) => {
                self.inner.schema.store(value);
                self.inner.write_watches[MetadataKind::Schema]
                    .sender
                    .send_replace(new_version);
            }
        }
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
    ) -> Result<Pinned<PartitionTable>, ShutdownError> {
        // make sure that we drop the pinned partition table in case we need to wait
        {
            let partition_table = self.partition_table_ref();
            if partition_table.version() >= min_version {
                return Ok(partition_table);
            }
        }

        self.wait_for_version(MetadataKind::PartitionTable, min_version)
            .await?;
        Ok(self.partition_table_ref())
    }

    pub fn logs_ref(&self) -> Pinned<Logs> {
        Pinned::new(&self.inner.logs)
    }

    pub fn logs_snapshot(&self) -> Arc<Logs> {
        self.inner.logs.load_full()
    }

    /// Returns Version::INVALID if logs has not been loaded yet.
    pub fn logs_version(&self) -> Version {
        self.inner.logs.load().version()
    }

    pub fn schema(&self) -> Pinned<Schema> {
        Pinned::new(&self.inner.schema)
    }

    pub fn schema_snapshot(&self) -> Arc<Schema> {
        self.inner.schema.load_full()
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

    /// Returns when the metadata kind is at the provided version (or newer)
    #[instrument(level = "debug", skip(self))]
    pub async fn wait_for_version(
        &self,
        metadata_kind: MetadataKind,
        min_version: Version,
    ) -> Result<Version, ShutdownError> {
        self.inner
            .wait_for_version(metadata_kind, min_version)
            .await
    }

    /// Watch for version updates of this metadata kind.
    ///
    /// The returned receiver is primed to notify you with the current value as well.
    pub fn watch(&self, metadata_kind: MetadataKind) -> watch::Receiver<Version> {
        let mut recv = self.inner.write_watches[metadata_kind].sender.subscribe();
        recv.mark_changed();
        recv
    }

    /// Notifies the metadata manager about a newly observed metadata version for the given kind.
    /// If the metadata can be retrieved from a node, then a connection to this node can be included.
    pub fn notify_observed_version(
        &self,
        metadata_kind: MetadataKind,
        version: Version,
        remote_peer: Option<Connection>,
    ) {
        self.inner
            .notify_observed_version(metadata_kind, version, remote_peer);
    }
}

#[derive(Default)]
struct MetadataInner {
    my_node_id: OnceLock<GenerationalNodeId>,
    nodes_config: Arc<ArcSwap<NodesConfiguration>>,
    partition_table: Arc<ArcSwap<PartitionTable>>,
    logs: Arc<ArcSwap<Logs>>,
    schema: Arc<ArcSwap<Schema>>,
    write_watches: EnumMap<MetadataKind, VersionWatch>,
    observed_versions: EnumMap<MetadataKind, watch::Sender<VersionInformation>>,
}

impl MetadataInner {
    async fn wait_for_version(
        &self,
        metadata_kind: MetadataKind,
        min_version: Version,
    ) -> Result<Version, ShutdownError> {
        let mut recv = self.write_watches[metadata_kind].sender.subscribe();
        // If we are already at the metadata version, avoid tokio's yielding to
        // improve tail latencies when this is used in latency-sensitive operations.
        let v = tokio::task::unconstrained(recv.wait_for(|v| *v >= min_version))
            .await
            .map_err(|_| ShutdownError)?;
        Ok(*v)
    }

    fn version(&self, metadata_kind: MetadataKind) -> Version {
        match metadata_kind {
            MetadataKind::NodesConfiguration => self.nodes_config.load().version(),
            MetadataKind::Schema => self.schema.load().version(),
            MetadataKind::PartitionTable => self.partition_table.load().version(),
            MetadataKind::Logs => self.logs.load().version(),
        }
    }

    /// return the metadata as global metadata
    fn get<T>(&self, metadata_kind: MetadataKind) -> Arc<T>
    where
        T: GlobalMetadata + metadata::Extraction<Output = T>,
    {
        let container = match metadata_kind {
            MetadataKind::NodesConfiguration => self.nodes_config.load_full().into_container(),
            MetadataKind::Schema => self.schema.load_full().into_container(),
            MetadataKind::PartitionTable => self.partition_table.load_full().into_container(),
            MetadataKind::Logs => self.logs.load_full().into_container(),
        };

        container.extract().expect("metadata must match kind")
    }

    /// Notifies the metadata manager about a newly observed metadata version for the given kind.
    /// If the metadata can be retrieved from a node, then a connection to this node can be included.
    fn notify_observed_version(
        &self,
        metadata_kind: MetadataKind,
        version: Version,
        remote_peer: Option<Connection>,
    ) {
        // fast path to avoid watch's internal locking if we are already at
        // this version.
        if self.version(metadata_kind) >= version {
            return;
        }

        self.observed_versions[metadata_kind].send_if_modified(|info| {
            match version.cmp(&info.version) {
                std::cmp::Ordering::Greater => {
                    // new version
                    info.version = version;
                    info.peers.clear();
                    info.first_observed_at = Instant::now();
                    if let Some(peer) = remote_peer {
                        info.peers.insert(peer.peer(), peer);
                        // NOTE: commented for future use
                        // only use known peers as potential source for metadata updates
                        // if let PeerAddress::ServerNode(node_id) = peer.peer() {
                        //     info.peers.insert(*node_id, peer);
                        // }
                    }
                    true
                }
                std::cmp::Ordering::Equal => {
                    // same version
                    if let Some(peer) = remote_peer {
                        info.peers.insert(peer.peer(), peer);
                        // NOTE: commented for future use
                        // only use known peers as potential source for metadata updates
                        // if let PeerAddress::ServerNode(node_id) = peer.peer() {
                        //     info.peers.insert(*node_id, peer);
                        // }
                    }
                    // silent modification
                    false
                }
                std::cmp::Ordering::Less => {
                    // old version, ignore it.
                    false
                }
            }
        });
    }
}

/// Can send updates to metadata manager. This should be accessible by the rpc handler layer to
/// handle incoming metadata updates from the network, or to handle updates coming from metadata
/// service if it's running on this node. MetadataManager ensures that writes are monotonic
/// so it's safe to call update_* without checking the current version.
#[derive(Clone)]
pub struct MetadataWriter {
    metadata_store_client: MetadataStoreClient,
    sender: manager::CommandSender,
    /// strictly used to set my node id. Do not use this to update metadata
    /// directly to avoid race conditions.
    inner: Arc<MetadataInner>,
}

impl MetadataWriter {
    fn new(
        sender: manager::CommandSender,
        metadata_store_client: MetadataStoreClient,
        inner: Arc<MetadataInner>,
    ) -> Self {
        Self {
            metadata_store_client,
            sender,
            inner,
        }
    }

    /// The raw metadata client
    ///
    /// This should be used to access to non-global metadata. For global metadata
    /// mutations, use `global_metadata()` instead.
    pub fn raw_metadata_store_client(&self) -> &MetadataStoreClient {
        &self.metadata_store_client
    }

    /// Mutations of global metadata
    pub fn global_metadata(&self) -> MetadataClientWrapper<'_> {
        MetadataClientWrapper::new(self)
    }

    /// Pushes a new value of global metadata to metadata manager
    ///
    /// Note that this **does not** push the value to the metadata store, this assumes
    /// that the value has already been committed to metadata store.
    ///
    /// Returns when the metadata update is performed.
    pub async fn update(
        &self,
        value: impl Into<MetadataContainer>,
    ) -> Result<Version, ShutdownError> {
        let (callback, recv) = oneshot::channel();
        let _ = self.sender.send(manager::Command::UpdateMetadata(
            value.into(),
            Some(callback),
        ));
        recv.await.map_err(|_| ShutdownError)
    }

    /// Should be called once on node startup. Panics if node id value is already set.
    pub fn set_my_node_id(&self, id: GenerationalNodeId) {
        self.inner.my_node_id.set(id).expect("My node is not set");
    }

    /// Like `update()` but in a fire and forget fashion
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
}

impl Default for VersionWatch {
    fn default() -> Self {
        Self {
            sender: watch::Sender::new(Version::INVALID),
        }
    }
}

pub fn spawn_metadata_manager(metadata_manager: MetadataManager) -> Result<TaskId, ShutdownError> {
    TaskCenter::spawn(
        TaskKind::MetadataBackgroundSync,
        "metadata-manager",
        metadata_manager.run(),
    )
}

#[derive(Debug, Clone)]
struct VersionInformation {
    version: Version,
    peers: HashMap<GenerationalNodeId, Connection>,
    first_observed_at: Instant,
}

impl VersionInformation {
    /// Time since the version was first observed.
    pub fn elapsed(&self) -> Duration {
        self.first_observed_at.elapsed()
    }
}

impl Default for VersionInformation {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            peers: HashMap::default(),
            first_observed_at: Instant::now(),
        }
    }
}
