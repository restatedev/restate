// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use enum_map::EnumMap;
use std::ops::Deref;
use std::sync::Arc;
use strum::IntoEnumIterator;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use crate::cancellation_watcher;
use crate::is_cancellation_requested;
use crate::metadata_store::{MetadataStoreClient, ReadError};
use crate::network::{MessageHandler, MessageRouterBuilder, NetworkError, NetworkSender};
use crate::task_center;
use restate_types::config::Configuration;
use restate_types::logs::metadata::Logs;
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY, SCHEMA_INFORMATION_KEY,
};
use restate_types::net::metadata::{GetMetadataRequest, MetadataMessage, MetadataUpdate};
use restate_types::net::MessageEnvelope;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::FixedPartitionTable;
use restate_types::schema::Schema;
use restate_types::{GenerationalNodeId, NodeId};
use restate_types::{Version, Versioned};

use super::{Metadata, MetadataContainer, MetadataKind, MetadataWriter};
use super::{MetadataBuilder, VersionInformation};

pub(super) type CommandSender = mpsc::UnboundedSender<Command>;
pub(super) type CommandReceiver = mpsc::UnboundedReceiver<Command>;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {}

#[derive(Debug, thiserror::Error)]
enum UpdateError {
    #[error("failed reading metadata from the metadata store: {0}")]
    MetadataStore(#[from] ReadError),
    #[error(transparent)]
    Network(#[from] NetworkError),
}

#[derive(Debug)]
pub enum TargetVersion {
    Latest,
    Version(Version),
}

impl Default for TargetVersion {
    fn default() -> Self {
        Self::Latest
    }
}

impl From<Option<Version>> for TargetVersion {
    fn from(value: Option<Version>) -> Self {
        match value {
            Some(version) => TargetVersion::Version(version),
            None => TargetVersion::Latest,
        }
    }
}

pub(super) enum Command {
    UpdateMetadata(MetadataContainer, Option<oneshot::Sender<()>>),
    SyncMetadata(
        MetadataKind,
        TargetVersion,
        Option<oneshot::Sender<Result<(), ReadError>>>,
    ),
}

/// A handler for processing network messages targeting metadata manager
/// (dev.restate.common.TargetName = METADATA_MANAGER)
struct MetadataMessageHandler<N>
where
    N: NetworkSender + 'static + Clone,
{
    sender: CommandSender,
    networking: N,
    metadata: Metadata,
}

impl<N> MetadataMessageHandler<N>
where
    N: NetworkSender + 'static + Clone,
{
    fn send_metadata(
        &self,
        peer: GenerationalNodeId,
        metadata_kind: MetadataKind,
        min_version: Option<Version>,
    ) {
        match metadata_kind {
            MetadataKind::NodesConfiguration => self.send_nodes_config(peer, min_version),
            MetadataKind::PartitionTable => self.send_partition_table(peer, min_version),
            MetadataKind::Logs => self.send_logs(peer, min_version),
            MetadataKind::Schema => self.send_schema(peer, min_version),
        };
    }

    fn send_nodes_config(&self, to: GenerationalNodeId, version: Option<Version>) {
        let config = self.metadata.nodes_config_snapshot();
        self.send_metadata_internal(to, version, config.deref(), "nodes_config");
    }

    fn send_partition_table(&self, to: GenerationalNodeId, version: Option<Version>) {
        let partition_table = self.metadata.partition_table_snapshot();
        self.send_metadata_internal(to, version, partition_table.deref(), "partition_table");
    }

    fn send_logs(&self, to: GenerationalNodeId, version: Option<Version>) {
        let logs = self.metadata.logs();
        if logs.version() != Version::INVALID {
            self.send_metadata_internal(to, version, logs.deref(), "logs");
        }
    }

    fn send_schema(&self, to: GenerationalNodeId, version: Option<Version>) {
        let schema = self.metadata.schema();
        if schema.version != Version::INVALID {
            self.send_metadata_internal(to, version, schema.deref(), "schema");
        }
    }

    fn send_metadata_internal<T>(
        &self,
        to: GenerationalNodeId,
        version: Option<Version>,
        metadata: &T,
        metadata_name: &str,
    ) where
        T: Versioned + Clone + Send + Sync + 'static,
        MetadataContainer: From<T>,
    {
        if version.is_some_and(|min_version| min_version > metadata.version()) {
            // We don't have the version that the peer is asking for. Just ignore.
            info!(
                "Peer requested '{}' version {} but we have {}, ignoring their request",
                metadata_name,
                version.unwrap(),
                metadata.version()
            );
            return;
        }
        info!(
            "Sending '{}' {} to peer, requested version? {:?}",
            metadata_name,
            metadata.version(),
            version,
        );
        let metadata = metadata.clone();

        let _ = task_center().spawn_child(
            crate::TaskKind::Disposable,
            "send-metadata-to-peer",
            None,
            {
                let networking = self.networking.clone();
                async move {
                    networking
                        .send(
                            to.into(),
                            &MetadataMessage::MetadataUpdate(MetadataUpdate {
                                container: MetadataContainer::from(metadata),
                            }),
                        )
                        .await?;
                    Ok(())
                }
            },
        );
    }
}

impl<N> MessageHandler for MetadataMessageHandler<N>
where
    N: NetworkSender + 'static + Clone,
{
    type MessageType = MetadataMessage;

    async fn on_message(&self, envelope: MessageEnvelope<MetadataMessage>) {
        let (peer, msg) = envelope.split();
        match msg {
            MetadataMessage::MetadataUpdate(update) => {
                info!(
                    "Received '{}' metadata update from peer {}",
                    update.container.kind(),
                    peer
                );
                if let Err(e) = self
                    .sender
                    .send(Command::UpdateMetadata(update.container, None))
                {
                    if !is_cancellation_requested() {
                        warn!("Failed to send metadata message to metadata manager: {}", e);
                    }
                }
            }
            MetadataMessage::GetMetadataRequest(request) => {
                debug!("Received GetMetadataRequest from peer {}", peer);
                self.send_metadata(peer, request.metadata_kind, request.min_version);
            }
        };
    }
}

/// Handle to access locally cached metadata, request metadata updates, and more.
/// What is metadata manager?
///
/// MetadataManager is a long-running task that monitors shared metadata needed by
/// services running on this node. It acts as the authority for updating the cached
/// metadata. It can also perform other tasks by running sub tasks as needed.
///
/// Those include but not limited to:
/// - Syncing schema metadata, logs, nodes configuration with admin servers.
/// - Accepts adhoc requests from system components that might have observed higher
/// metadata version through other means. Metadata manager takes note and schedules a
/// sync so that we don't end up with thundering herd by direct metadata update
/// requests from components
///
/// Metadata to be managed by MetadataManager:
/// - Bifrost's log metadata
/// - Schema metadata
/// - NodesConfiguration
/// - Partition table
pub struct MetadataManager<N> {
    metadata: Metadata,
    inbound: CommandReceiver,
    networking: N,
    metadata_store_client: MetadataStoreClient,
    update_tasks: EnumMap<MetadataKind, Option<UpdateTask>>,
}

impl<N> MetadataManager<N>
where
    N: NetworkSender + 'static + Clone,
{
    pub fn new(
        metadata_builder: MetadataBuilder,
        networking: N,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        Self {
            metadata: metadata_builder.metadata,
            inbound: metadata_builder.receiver,
            networking,
            metadata_store_client,
            update_tasks: EnumMap::default(),
        }
    }

    pub fn register_in_message_router(&self, sr_builder: &mut MessageRouterBuilder) {
        sr_builder.add_message_handler(MetadataMessageHandler {
            sender: self.metadata.sender.clone(),
            networking: self.networking.clone(),
            metadata: self.metadata.clone(),
        });
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn writer(&self) -> MetadataWriter {
        MetadataWriter::new(self.metadata.sender.clone(), self.metadata.inner.clone())
    }

    /// Start and wait for shutdown signal.
    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!("Metadata manager started");

        let update_interval = Configuration::pinned()
            .common
            .metadata_update_interval
            .into();
        let mut update_interval = tokio::time::interval(update_interval);
        update_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = cancellation_watcher() => {
                    info!("Metadata manager stopped");
                    break;
                }
                Some(cmd) = self.inbound.recv() => {
                    self.handle_command(cmd).await;
                }
                _ = update_interval.tick() => {
                    if let Err(err) = self.check_for_observed_updates().await {
                        warn!("Failed checking for metadata updates: {err}");
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::UpdateMetadata(value, callback) => self.update_metadata(value, callback),
            Command::SyncMetadata(kind, target_version, callback) => {
                let result = self.sync_metadata(kind, target_version).await;
                if let Some(callback) = callback {
                    if callback.send(result).is_err() {
                        trace!("Couldn't send sync metadata reply back. System is probably shutting down.");
                    }
                }
            }
        }
    }

    fn update_metadata(&mut self, value: MetadataContainer, callback: Option<oneshot::Sender<()>>) {
        match value {
            MetadataContainer::NodesConfiguration(config) => {
                self.update_nodes_configuration(config);
            }
            MetadataContainer::PartitionTable(partition_table) => {
                self.update_partition_table(partition_table);
            }
            MetadataContainer::Logs(logs) => {
                self.update_logs(logs);
            }
            MetadataContainer::Schema(schemas) => {
                self.update_schema(schemas);
            }
        }

        if let Some(callback) = callback {
            let _ = callback.send(());
        }
    }

    async fn sync_metadata(
        &mut self,
        metadata_kind: MetadataKind,
        target_version: TargetVersion,
    ) -> Result<(), ReadError> {
        if self.has_target_version(metadata_kind, target_version) {
            return Ok(());
        }

        match metadata_kind {
            MetadataKind::NodesConfiguration => {
                if let Some(nodes_config) = self
                    .metadata_store_client
                    .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
                    .await?
                {
                    self.update_nodes_configuration(nodes_config);
                }
            }
            MetadataKind::PartitionTable => {
                if let Some(partition_table) = self
                    .metadata_store_client
                    .get::<FixedPartitionTable>(PARTITION_TABLE_KEY.clone())
                    .await?
                {
                    self.update_partition_table(partition_table);
                }
            }
            MetadataKind::Logs => {
                if let Some(logs) = self
                    .metadata_store_client
                    .get::<Logs>(BIFROST_CONFIG_KEY.clone())
                    .await?
                {
                    self.update_logs(logs);
                }
            }
            MetadataKind::Schema => {
                if let Some(schema) = self
                    .metadata_store_client
                    .get::<Schema>(SCHEMA_INFORMATION_KEY.clone())
                    .await?
                {
                    self.update_schema(schema)
                }
            }
        }

        Ok(())
    }

    fn has_target_version(
        &self,
        metadata_kind: MetadataKind,
        target_version: TargetVersion,
    ) -> bool {
        match target_version {
            TargetVersion::Latest => false,
            TargetVersion::Version(target_version) => {
                let version = match metadata_kind {
                    MetadataKind::NodesConfiguration => self.metadata.nodes_config_version(),
                    MetadataKind::Schema => self.metadata.schema_version(),
                    MetadataKind::PartitionTable => self.metadata.partition_table_version(),
                    MetadataKind::Logs => self.metadata.logs_version(),
                };

                version >= target_version
            }
        }
    }

    fn update_nodes_configuration(&mut self, config: NodesConfiguration) {
        let maybe_new_version = Self::update_internal(&self.metadata.inner.nodes_config, config);

        self.update_task_and_notify_watches(maybe_new_version, MetadataKind::NodesConfiguration);
    }

    fn update_partition_table(&mut self, partition_table: FixedPartitionTable) {
        let maybe_new_version =
            Self::update_internal(&self.metadata.inner.partition_table, partition_table);

        self.update_task_and_notify_watches(maybe_new_version, MetadataKind::PartitionTable);
    }

    fn update_logs(&mut self, logs: Logs) {
        let maybe_new_version = Self::update_internal(&self.metadata.inner.logs, logs);

        self.update_task_and_notify_watches(maybe_new_version, MetadataKind::Logs);
    }

    fn update_schema(&mut self, schema: Schema) {
        let maybe_new_version = Self::update_internal(&self.metadata.inner.schema, schema);

        self.update_task_and_notify_watches(maybe_new_version, MetadataKind::Schema);
    }

    fn update_internal<T: Versioned>(container: &ArcSwap<T>, new_value: T) -> Version {
        let current_value = container.load();
        let mut maybe_new_version = new_value.version();

        if new_value.version() > current_value.version() {
            container.store(Arc::new(new_value));
        } else {
            /* Do nothing, current is already newer */
            debug!(
                "Ignoring update {} because we are at {}",
                new_value.version(),
                current_value.version(),
            );
            maybe_new_version = current_value.version();
        }

        maybe_new_version
    }

    fn update_task_and_notify_watches(&mut self, maybe_new_version: Version, kind: MetadataKind) {
        // update tasks if they are no longer needed
        if self.update_tasks[kind]
            .as_ref()
            .is_some_and(|task| maybe_new_version >= task.version)
        {
            self.update_tasks[kind] = None;
        }

        // notify watches.
        self.metadata.inner.write_watches[kind]
            .sender
            .send_if_modified(|v| {
                if maybe_new_version > *v {
                    *v = maybe_new_version;
                    true
                } else {
                    false
                }
            });
    }

    async fn check_for_observed_updates(&mut self) -> Result<(), UpdateError> {
        for kind in MetadataKind::iter() {
            if let Some(version_information) = self.metadata.observed_version(kind) {
                if version_information.version
                    > self.update_tasks[kind]
                        .as_ref()
                        .map(|task| task.version)
                        .unwrap_or(Version::INVALID)
                {
                    self.update_tasks[kind] = Some(UpdateTask::from(version_information));
                }
            }
        }

        for metadata_kind in MetadataKind::iter() {
            let mut update_task = self.update_tasks[metadata_kind].take();

            if let Some(mut task) = update_task {
                match task.state {
                    UpdateTaskState::FromRemoteNode(node_id) => {
                        debug!(
                            "Send GetMetadataRequest to {} from {}",
                            node_id,
                            self.metadata.my_node_id()
                        );
                        // todo: Move to dedicated task if this is blocking the MetadataManager too much
                        self.networking
                            .send(
                                node_id,
                                &MetadataMessage::GetMetadataRequest(GetMetadataRequest {
                                    metadata_kind,
                                    min_version: Some(task.version),
                                }),
                            )
                            .await?;
                        // on the next tick try to sync if no update was received
                        task.state = UpdateTaskState::Sync;
                        update_task = Some(task);
                    }
                    UpdateTaskState::Sync => {
                        // todo: Move to dedicated task if this is blocking the MetadataManager too much
                        self.sync_metadata(metadata_kind, TargetVersion::Version(task.version))
                            .await?;
                        // syncing will give us >= task.version so let's stop here
                        update_task = None;
                    }
                }

                self.update_tasks[metadata_kind] = update_task;
            }
        }

        Ok(())
    }
}

enum UpdateTaskState {
    FromRemoteNode(NodeId),
    Sync,
}

struct UpdateTask {
    version: Version,
    state: UpdateTaskState,
}

impl UpdateTask {
    fn from(version_information: VersionInformation) -> Self {
        let state = if let Some(node_id) = version_information.remote_node {
            UpdateTaskState::FromRemoteNode(node_id)
        } else {
            UpdateTaskState::Sync
        };

        Self {
            version: version_information.version,
            state,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use googletest::prelude::*;
    use test_log::test;

    use restate_test_util::assert_eq;
    use restate_types::net::AdvertisedAddress;
    use restate_types::nodes_config::{NodeConfig, Role};
    use restate_types::{GenerationalNodeId, Version};

    use crate::metadata::spawn_metadata_manager;
    use crate::test_env::MockNetworkSender;
    use crate::TaskCenterBuilder;

    #[test]
    fn test_nodes_config_updates() -> Result<()> {
        test_updates(
            create_mock_nodes_config(),
            MetadataKind::NodesConfiguration,
            |metadata| metadata.nodes_config_version(),
            |value, version| value.set_version(version),
        )
    }

    #[test]
    fn test_partition_table_updates() -> Result<()> {
        test_updates(
            FixedPartitionTable::new(Version::MIN, 42),
            MetadataKind::PartitionTable,
            |metadata| metadata.partition_table_version(),
            |value, version| value.set_version(version),
        )
    }

    fn test_updates<T, F, S>(
        value: T,
        kind: MetadataKind,
        config_version: F,
        set_version_to: S,
    ) -> Result<()>
    where
        T: Into<MetadataContainer> + Versioned + Clone,
        F: Fn(&Metadata) -> Version,
        S: Fn(&mut T, Version),
    {
        let tc = TaskCenterBuilder::default().build()?;
        tc.block_on("test", None, async move {
            let metadata_builder = MetadataBuilder::default();
            let network_sender = MockNetworkSender::new(metadata_builder.to_metadata());
            let metadata_store_client = MetadataStoreClient::new_in_memory();
            let metadata = metadata_builder.to_metadata();
            let metadata_manager =
                MetadataManager::new(metadata_builder, network_sender, metadata_store_client);
            let metadata_writer = metadata_manager.writer();

            assert_eq!(Version::INVALID, config_version(&metadata));

            assert_eq!(Version::MIN, value.version());
            // updates happening before metadata manager start should not get lost.
            metadata_writer.submit(value.clone());

            let tc = task_center();
            // start metadata manager
            spawn_metadata_manager(&tc, metadata_manager)?;

            let version = metadata.wait_for_version(kind, Version::MIN).await.unwrap();
            assert_eq!(Version::MIN, version);

            // Wait should not block if waiting older version
            let version2 = metadata
                .wait_for_version(kind, Version::INVALID)
                .await
                .unwrap();
            assert_eq!(version, version2);

            // let's set the version to 3
            let mut update_value = value.clone();
            set_version_to(&mut update_value, Version::from(3));
            metadata_writer.update(update_value).await?;

            let _ = metadata.wait_for_version(kind, Version::from(3)).await;

            tc.cancel_tasks(None, None).await;
            Ok(())
        })
    }

    #[test]
    fn test_nodes_config_watchers() -> Result<()> {
        test_watchers(
            create_mock_nodes_config(),
            MetadataKind::NodesConfiguration,
            |metadata| metadata.nodes_config_version(),
            |value| value.increment_version(),
        )
    }

    #[test]
    fn test_partition_table_watchers() -> Result<()> {
        test_watchers(
            FixedPartitionTable::new(Version::MIN, 42),
            MetadataKind::PartitionTable,
            |metadata| metadata.partition_table_version(),
            |value| value.increment_version(),
        )
    }

    fn test_watchers<T, F, I>(
        value: T,
        kind: MetadataKind,
        config_version: F,
        increment_version: I,
    ) -> Result<()>
    where
        T: Into<MetadataContainer> + Versioned + Clone,
        F: Fn(&Metadata) -> Version,
        I: Fn(&mut T),
    {
        let tc = TaskCenterBuilder::default().build()?;
        tc.block_on("test", None, async move {
            let metadata_builder = MetadataBuilder::default();
            let network_sender = MockNetworkSender::new(metadata_builder.to_metadata());
            let metadata_store_client = MetadataStoreClient::new_in_memory();

            let metadata = metadata_builder.to_metadata();
            let metadata_manager =
                MetadataManager::new(metadata_builder, network_sender, metadata_store_client);
            let metadata_writer = metadata_manager.writer();

            assert_eq!(Version::INVALID, config_version(&metadata));

            assert_eq!(Version::MIN, value.version());

            // start metadata manager
            spawn_metadata_manager(&task_center(), metadata_manager)?;

            let mut watcher1 = metadata.watch(kind);
            assert_eq!(Version::INVALID, *watcher1.borrow());
            let mut watcher2 = metadata.watch(kind);
            assert_eq!(Version::INVALID, *watcher2.borrow());

            metadata_writer.update(value.clone()).await?;
            watcher1.changed().await?;

            assert_eq!(Version::MIN, *watcher1.borrow());
            assert_eq!(Version::MIN, *watcher2.borrow());

            // let's push multiple updates
            let mut value = value.clone();
            increment_version(&mut value);
            metadata_writer.update(value.clone()).await?;
            increment_version(&mut value);
            metadata_writer.update(value.clone()).await?;
            increment_version(&mut value);
            metadata_writer.update(value.clone()).await?;
            increment_version(&mut value);
            metadata_writer.update(value.clone()).await?;

            // Watcher sees the latest value only.
            watcher2.changed().await?;
            assert_eq!(Version::from(5), *watcher2.borrow());
            assert!(!watcher2.has_changed().unwrap());

            watcher1.changed().await?;
            assert_eq!(Version::from(5), *watcher1.borrow());
            assert!(!watcher1.has_changed().unwrap());

            Ok(())
        })
    }

    fn create_mock_nodes_config() -> NodesConfiguration {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        let address = AdvertisedAddress::from_str("http://127.0.0.1:5122/").unwrap();
        let node_id = GenerationalNodeId::new(1, 1);
        let roles = Role::Admin | Role::Worker;
        let my_node = NodeConfig::new("MyNode-1".to_owned(), node_id, address, roles);
        nodes_config.upsert_node(my_node);
        nodes_config
    }
}
