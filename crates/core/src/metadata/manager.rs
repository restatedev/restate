// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::panic;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tracing::{debug, info, trace};

use restate_metadata_store::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::live::Pinned;
use restate_types::logs::metadata::Logs;
use restate_types::net::RpcRequest;
use restate_types::net::metadata::GetMetadataRequest;
use restate_types::net::metadata::MetadataManagerService;
use restate_types::net::metadata::MetadataUpdate;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;
use restate_types::{Version, Versioned};

use super::MetadataBuilder;
use super::{Metadata, MetadataContainer, MetadataKind, MetadataWriter};

use crate::TaskCenter;
use crate::cancellation_watcher;
use crate::metadata::update_task::GlobalMetadataUpdateTask;
use crate::network::{
    BackPressureMode, MessageRouterBuilder, Oneshot, Reciprocal, ServiceMessage, ServiceReceiver,
    Verdict,
};

pub(super) type CommandSender = mpsc::UnboundedSender<Command>;
pub(super) type CommandReceiver = mpsc::UnboundedReceiver<Command>;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, derive_more::Display)]
pub enum TargetVersion {
    #[default]
    Latest,
    Version(Version),
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
    UpdateMetadata(MetadataContainer, Option<oneshot::Sender<Version>>),
}

/// A set of senders for global metadata update tasks
struct Updaters {
    nodes_config: mpsc::UnboundedSender<super::update_task::Command<NodesConfiguration>>,
    logs: mpsc::UnboundedSender<super::update_task::Command<Logs>>,
    partition_table: mpsc::UnboundedSender<super::update_task::Command<PartitionTable>>,
    schema: mpsc::UnboundedSender<super::update_task::Command<Schema>>,
}

/// Handle to access global metadata
///
/// MetadataManager is a long-running task that monitors shared metadata needed by
/// services running on this node. It acts as the authority for updating the cached
/// metadata. It can also perform other tasks by running sub tasks as needed.
///
/// Those include but not limited to:
/// - Syncing schema metadata, logs, nodes configuration with admin servers.
/// - Accepts adhoc requests from system components that might have observed higher
///   metadata version through other means. Metadata manager takes note and schedules a
///   sync so that we don't end up with thundering herd by direct metadata update
///   requests from components
///
/// Metadata to be managed by MetadataManager:
/// - Bifrost's log metadata (aka log chain)
/// - Schema metadata
/// - NodesConfiguration
/// - Partition table
pub struct MetadataManager {
    metadata: Metadata,
    inbound: CommandReceiver,
    metadata_store_client: MetadataStoreClient,
    service_op_rx: ServiceReceiver<MetadataManagerService>,
}

impl MetadataManager {
    pub fn new(
        metadata_builder: MetadataBuilder,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        Self {
            metadata: metadata_builder.metadata,
            inbound: metadata_builder.receiver,
            metadata_store_client,
            service_op_rx: ServiceReceiver::default(),
        }
    }

    pub fn register_in_message_router(&mut self, sr_builder: &mut MessageRouterBuilder) {
        // Using dedicated memory pool for metadata manager to ensure that metadata sync
        // messages do not starve the rest of the system.
        let pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool("metadata-manager", || {
                Configuration::pinned().networking.message_size_limit
            })
        });
        self.service_op_rx = sr_builder.register_service_with_pool(pool, BackPressureMode::Lossy);
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn writer(&self) -> MetadataWriter {
        MetadataWriter::new(
            self.metadata.sender.clone(),
            self.metadata_store_client.clone(),
            self.metadata.inner.clone(),
        )
    }

    /// Start and wait for shutdown signal.
    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!("Metadata manager started");

        let mut cancel = std::pin::pin!(cancellation_watcher());

        // Global metadata updater tasks
        let (nodes_config, nodes_config_task) = GlobalMetadataUpdateTask::start(
            self.metadata_store_client.clone(),
            self.metadata.inner.nodes_config.clone(),
            self.metadata.inner.write_watches[MetadataKind::NodesConfiguration]
                .sender
                .clone(),
            &self.metadata.inner.observed_versions[MetadataKind::NodesConfiguration],
        )?;

        let (logs, logs_task) = GlobalMetadataUpdateTask::start(
            self.metadata_store_client.clone(),
            self.metadata.inner.logs.clone(),
            self.metadata.inner.write_watches[MetadataKind::Logs]
                .sender
                .clone(),
            &self.metadata.inner.observed_versions[MetadataKind::Logs],
        )?;

        let (partition_table, partition_table_task) = GlobalMetadataUpdateTask::start(
            self.metadata_store_client.clone(),
            self.metadata.inner.partition_table.clone(),
            self.metadata.inner.write_watches[MetadataKind::PartitionTable]
                .sender
                .clone(),
            &self.metadata.inner.observed_versions[MetadataKind::PartitionTable],
        )?;

        let (schema, schema_task) = GlobalMetadataUpdateTask::start(
            self.metadata_store_client.clone(),
            self.metadata.inner.schema.clone(),
            self.metadata.inner.write_watches[MetadataKind::Schema]
                .sender
                .clone(),
            &self.metadata.inner.observed_versions[MetadataKind::Schema],
        )?;

        let updater_tasks = vec![
            nodes_config_task,
            logs_task,
            partition_table_task,
            schema_task,
        ];

        let updaters = Updaters {
            nodes_config,
            logs,
            partition_table,
            schema,
        };

        let mut network_rx = self.service_op_rx.take().start();
        loop {
            tokio::select! {
                biased;
                () = &mut cancel => {
                    drop(network_rx);
                    break;
                }
                Some(cmd) = self.inbound.recv() => {
                    self.handle_command(cmd, &updaters);
                }
                Some(service_op) = network_rx.next() => {
                    self.handle_network_message(service_op);
                }
            }
        }

        for task in updater_tasks {
            task.cancel();
        }
        Ok(())
    }

    fn handle_command(&mut self, cmd: Command, updaters: &Updaters) {
        match cmd {
            Command::UpdateMetadata(value, callback) => {
                self.update_metadata(value, updaters, callback)
            }
        }
    }

    fn handle_network_message(&mut self, msg: ServiceMessage<MetadataManagerService>) {
        match msg {
            ServiceMessage::Rpc(msg) if msg.msg_type() == GetMetadataRequest::TYPE => {
                let (reciprocal, request) = msg.into_typed::<GetMetadataRequest>().split();
                self.send_metadata(reciprocal, request.metadata_kind, request.min_version);
            }
            msg => {
                msg.fail(Verdict::MessageUnrecognized);
            }
        }
    }

    fn update_metadata(
        &mut self,
        value: MetadataContainer,
        updaters: &Updaters,
        callback: Option<oneshot::Sender<Version>>,
    ) {
        match value {
            MetadataContainer::NodesConfiguration(value) => {
                let _ = updaters
                    .nodes_config
                    .send(super::update_task::Command::Update { value, callback });
            }
            MetadataContainer::PartitionTable(value) => {
                let _ = updaters
                    .partition_table
                    .send(super::update_task::Command::Update { value, callback });
            }
            MetadataContainer::Logs(value) => {
                let _ = updaters
                    .logs
                    .send(super::update_task::Command::Update { value, callback });
            }
            MetadataContainer::Schema(value) => {
                let _ = updaters
                    .schema
                    .send(super::update_task::Command::Update { value, callback });
            }
        }
    }

    fn send_metadata(
        &self,
        to: Reciprocal<Oneshot<MetadataUpdate>>,
        metadata_kind: MetadataKind,
        min_version: Option<Version>,
    ) {
        match metadata_kind {
            MetadataKind::NodesConfiguration => self.send_nodes_config(to, min_version),
            MetadataKind::PartitionTable => self.send_partition_table(to, min_version),
            MetadataKind::Logs => self.send_logs(to, min_version),
            MetadataKind::Schema => self.send_schema(to, min_version),
        };
    }

    fn send_nodes_config(&self, to: Reciprocal<Oneshot<MetadataUpdate>>, version: Option<Version>) {
        if self.metadata.nodes_config_version() != Version::INVALID {
            let config = self.metadata.nodes_config_ref();
            self.send_metadata_internal(to, version, config, "nodes_config");
        }
    }

    fn send_partition_table(
        &self,
        to: Reciprocal<Oneshot<MetadataUpdate>>,
        version: Option<Version>,
    ) {
        if self.metadata.partition_table_version() != Version::INVALID {
            let partition_table = self.metadata.partition_table_ref();
            self.send_metadata_internal(to, version, partition_table, "partition_table");
        }
    }

    fn send_logs(&self, to: Reciprocal<Oneshot<MetadataUpdate>>, version: Option<Version>) {
        if self.metadata.logs_version() != Version::INVALID {
            let logs = self.metadata.logs_ref();
            self.send_metadata_internal(to, version, logs, "logs");
        }
    }

    fn send_schema(&self, to: Reciprocal<Oneshot<MetadataUpdate>>, version: Option<Version>) {
        if self.metadata.schema_version() != Version::INVALID {
            let schema = self.metadata.schema_ref();
            self.send_metadata_internal(to, version, schema, "schema");
        }
    }

    fn send_metadata_internal<T>(
        &self,
        to: Reciprocal<Oneshot<MetadataUpdate>>,
        version: Option<Version>,
        metadata: Pinned<T>,
        metadata_name: &str,
    ) where
        T: Versioned + Clone + Send + Sync + 'static,
        MetadataContainer: From<Arc<T>>,
    {
        if version.is_some_and(|min_version| min_version > metadata.version()) {
            // We don't have the version that the peer is asking for. Just ignore.
            info!(
                kind = metadata_name,
                version = %metadata.version(),
                requested_min_version = ?version,
                "Peer requested metadata version but we don't have it, ignoring their request",
            );
            return;
        }
        trace!(
            kind = metadata_name,
            version = %metadata.version(),
            requested_min_version = ?version,
            "Sending metadata to peer",
        );
        to.send(MetadataUpdate {
            container: MetadataContainer::from(metadata.into_arc()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use test_log::test;

    use restate_test_util::assert_eq;
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::nodes_config::{NodeConfig, Role};
    use restate_types::{GenerationalNodeId, RestateVersion, Version};

    use crate::metadata::spawn_metadata_manager;
    use crate::{TaskCenter, TaskCenterBuilder};

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
            PartitionTable::with_equally_sized_partitions(Version::MIN, 42),
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
        Arc<T>: Into<MetadataContainer>,
        T: Versioned + Clone,
        F: Fn(&Metadata) -> Version,
        S: Fn(&mut T, Version),
    {
        let tc = TaskCenterBuilder::default().build()?.into_handle();
        tc.block_on(async move {
            let metadata_builder = MetadataBuilder::default();
            let metadata_store_client = MetadataStoreClient::new_in_memory();
            let metadata = metadata_builder.to_metadata();
            let metadata_manager = MetadataManager::new(metadata_builder, metadata_store_client);
            let metadata_writer = metadata_manager.writer();

            assert_eq!(Version::INVALID, config_version(&metadata));

            assert_eq!(Version::MIN, value.version());
            // updates happening before metadata manager start should not get lost.
            metadata_writer.submit(Arc::new(value.clone()));

            // start metadata manager
            spawn_metadata_manager(metadata_manager)?;

            let version = metadata.wait_for_version(kind, Version::MIN).await.unwrap();
            assert_eq!(Version::MIN, version);

            // Wait should not block if waiting older version
            let version2 = metadata
                .wait_for_version(kind, Version::INVALID)
                .await
                .unwrap();
            assert_eq!(version, version2);

            // let's set the version to 3
            let mut update_value = value;
            set_version_to(&mut update_value, Version::from(3));
            metadata_writer.update(Arc::new(update_value)).await?;

            let _ = metadata.wait_for_version(kind, Version::from(3)).await;

            TaskCenter::current().cancel_tasks(None, None).await;
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
            PartitionTable::with_equally_sized_partitions(Version::MIN, 42),
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
        Arc<T>: Into<MetadataContainer>,
        T: Versioned + Clone,
        F: Fn(&Metadata) -> Version,
        I: Fn(&mut T),
    {
        let tc = TaskCenterBuilder::default().build()?.into_handle();
        tc.block_on(async move {
            let metadata_builder = MetadataBuilder::default();
            let metadata_store_client = MetadataStoreClient::new_in_memory();

            let metadata = metadata_builder.to_metadata();
            let metadata_manager = MetadataManager::new(metadata_builder, metadata_store_client);
            let metadata_writer = metadata_manager.writer();

            assert_eq!(Version::INVALID, config_version(&metadata));

            assert_eq!(Version::MIN, value.version());

            // start metadata manager
            spawn_metadata_manager(metadata_manager)?;

            let mut watcher1 = metadata.watch(kind);
            assert_eq!(Version::INVALID, *watcher1.borrow());
            let mut watcher2 = metadata.watch(kind);
            assert_eq!(Version::INVALID, *watcher2.borrow());

            metadata_writer.update(Arc::new(value.clone())).await?;
            watcher1.changed().await?;

            assert_eq!(Version::MIN, *watcher1.borrow());
            assert_eq!(Version::MIN, *watcher2.borrow());

            // let's push multiple updates
            let mut value = value;
            increment_version(&mut value);
            metadata_writer.update(Arc::new(value.clone())).await?;
            increment_version(&mut value);
            metadata_writer.update(Arc::new(value.clone())).await?;
            increment_version(&mut value);
            metadata_writer.update(Arc::new(value.clone())).await?;
            increment_version(&mut value);
            metadata_writer.update(Arc::new(value)).await?;

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
        let mut nodes_config = NodesConfiguration::new_for_testing();
        let address = AdvertisedAddress::default();
        let node_id = GenerationalNodeId::new(1, 1);
        let roles = Role::Admin | Role::Worker;
        let my_node = NodeConfig::builder()
            .name("MyNode-1".to_owned())
            .current_generation(node_id)
            .address(address)
            .roles(roles)
            .binary_version(RestateVersion::current())
            .build();
        nodes_config.upsert_node(my_node);
        nodes_config
    }
}
