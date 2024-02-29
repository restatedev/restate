// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwapOption;
use std::ops::Deref;
use std::sync::Arc;

use restate_node_protocol::MessageEnvelope;
use restate_node_protocol::MetadataUpdate;
use restate_node_protocol::NetworkMessage;
use restate_types::GenerationalNodeId;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::info;

use crate::cancellation_watcher;
use crate::network_sender::NetworkSender;
use crate::task_center;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::FixedPartitionTable;
use restate_types::{Version, Versioned};

use super::Metadata;
use super::MetadataContainer;
use super::MetadataInner;
use super::MetadataKind;
use super::MetadataWriter;

pub(super) type CommandSender = mpsc::UnboundedSender<Command>;
pub(super) type CommandReceiver = mpsc::UnboundedReceiver<Command>;

pub(super) enum Command {
    UpdateMetadata(MetadataContainer, Option<oneshot::Sender<()>>),
    SendMetadataToPeer(GenerationalNodeId, MetadataKind, Option<Version>),
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
pub struct MetadataManager {
    self_sender: CommandSender,
    inner: Arc<MetadataInner>,
    inbound: CommandReceiver,
    networking: Arc<dyn NetworkSender>,
    // Handle inbound network messages to update our metadata and to respond to
    // external metadata fetch requests
    network_inbound: mpsc::Receiver<MessageEnvelope>,
    network_inbound_sender: mpsc::Sender<MessageEnvelope>,
}

impl MetadataManager {
    pub fn build(networking: Arc<dyn NetworkSender>) -> Self {
        let (self_sender, inbound) = mpsc::unbounded_channel();
        let (network_inbound_sender, network_inbound) = mpsc::channel(1);

        Self {
            inner: Arc::new(MetadataInner::default()),
            inbound,
            networking,
            self_sender,
            network_inbound,
            network_inbound_sender,
        }
    }

    pub fn metadata(&self) -> Metadata {
        Metadata::new(self.inner.clone(), self.self_sender.clone())
    }

    pub fn writer(&self) -> MetadataWriter {
        MetadataWriter::new(self.self_sender.clone(), self.inner.clone())
    }

    pub fn network_inbound_sender(&self) -> mpsc::Sender<MessageEnvelope> {
        self.network_inbound_sender.clone()
    }

    /// Start and wait for shutdown signal.
    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("Metadata manager started");

        loop {
            tokio::select! {
                biased;
                _ = cancellation_watcher() => {
                    info!("Metadata manager stopped");
                    break;
                }
                Some(cmd) = self.inbound.recv() => {
                    self.handle_command(cmd)
                }
                Some(envelope) = self.network_inbound.recv() => {
                    self.handle_network_message(envelope).await
                }
            }
        }
        Ok(())
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::UpdateMetadata(value, callback) => self.update_metadata(value, callback),
            Command::SendMetadataToPeer(peer, kind, min_version) => {
                self.send_metadata(peer, kind, min_version)
            }
        }
    }

    async fn handle_network_message(&mut self, envelope: MessageEnvelope) {
        let (peer, msg) = envelope.split();
        match msg {
            NetworkMessage::MetadataUpdate(update) => self.update_metadata(update.container, None),
            NetworkMessage::GetMetadataRequest(request) => {
                debug!("Received GetMetadataRequest from peer {}", peer);
                self.send_metadata(peer, request.metadata_kind, request.min_version);
            }
        };
    }

    fn update_metadata(&mut self, value: MetadataContainer, callback: Option<oneshot::Sender<()>>) {
        match value {
            MetadataContainer::NodesConfiguration(config) => {
                self.update_nodes_configuration(config);
            }
            MetadataContainer::PartitionTable(partition_table) => {
                self.update_partition_table(partition_table);
            }
        }

        if let Some(callback) = callback {
            let _ = callback.send(());
        }
    }

    fn send_metadata(
        &mut self,
        peer: GenerationalNodeId,
        metadata_kind: MetadataKind,
        min_version: Option<Version>,
    ) {
        match metadata_kind {
            MetadataKind::NodesConfiguration => self.send_nodes_config(peer, min_version),
            _ => {
                todo!("Can't send metadata '{}' to peer", metadata_kind)
            }
        };
    }

    fn send_nodes_config(&self, to: GenerationalNodeId, version: Option<Version>) {
        let config = self.inner.nodes_config.load_full();
        let Some(config) = config else {
            return;
        };
        if version.is_some_and(|min_version| min_version > config.version()) {
            // We don't have the version that the peer is asking for. Just ignore.
            debug!(
                "Peer requested nodes config version {} but we have {}, ignoring their request",
                version.unwrap(),
                config.version()
            );
            return;
        }
        info!(
            "Sending nodes config {} to peer, requested version? {:?}",
            config.version(),
            version,
        );
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
                            &NetworkMessage::MetadataUpdate(MetadataUpdate {
                                container: MetadataContainer::NodesConfiguration(
                                    config.deref().clone(),
                                ),
                            }),
                        )
                        .await?;
                    Ok(())
                }
            },
        );
    }

    fn update_nodes_configuration(&mut self, config: NodesConfiguration) {
        let maybe_new_version = Self::update_internal(&self.inner.nodes_config, config);

        self.notify_watches(maybe_new_version, MetadataKind::NodesConfiguration);
    }

    fn update_partition_table(&mut self, partition_table: FixedPartitionTable) {
        let maybe_new_version = Self::update_internal(&self.inner.partition_table, partition_table);

        self.notify_watches(maybe_new_version, MetadataKind::PartitionTable);
    }

    fn update_internal<T: Versioned>(container: &ArcSwapOption<T>, new_value: T) -> Version {
        let current_value = container.load();
        let mut maybe_new_version = new_value.version();
        match current_value.as_deref() {
            None => {
                container.store(Some(Arc::new(new_value)));
            }
            Some(current_value) if new_value.version() > current_value.version() => {
                container.store(Some(Arc::new(new_value)));
            }
            Some(current_value) => {
                /* Do nothing, current is already newer */
                debug!(
                    "Ignoring update {} because we are at {}",
                    new_value.version(),
                    current_value.version(),
                );
                maybe_new_version = current_value.version();
            }
        }

        maybe_new_version
    }

    fn notify_watches(&mut self, maybe_new_version: Version, kind: MetadataKind) {
        // notify watches.
        self.inner.write_watches[kind].sender.send_if_modified(|v| {
            if maybe_new_version > *v {
                *v = maybe_new_version;
                true
            } else {
                false
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    use async_trait::async_trait;
    use googletest::prelude::*;
    use restate_test_util::assert_eq;
    use restate_types::nodes_config::{AdvertisedAddress, NodeConfig, Role};
    use restate_types::{GenerationalNodeId, NodeId, Version};

    use crate::metadata::spawn_metadata_manager;
    use crate::{NetworkSendError, TaskCenterFactory};

    struct MockNetworkSender;

    #[async_trait]
    impl NetworkSender for MockNetworkSender {
        async fn send(
            &self,
            _to: NodeId,
            _message: &NetworkMessage,
        ) -> std::result::Result<(), NetworkSendError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_nodes_config_updates() -> Result<()> {
        test_updates(
            create_mock_nodes_config(),
            MetadataKind::NodesConfiguration,
            |metadata| metadata.nodes_config_version(),
        )
        .await
    }

    #[tokio::test]
    async fn test_partition_table_updates() -> Result<()> {
        test_updates(
            FixedPartitionTable::new(Version::MIN, 42),
            MetadataKind::PartitionTable,
            |metadata| metadata.partition_table_version(),
        )
        .await
    }

    async fn test_updates<T, F>(value: T, kind: MetadataKind, config_version: F) -> Result<()>
    where
        T: Into<MetadataContainer> + Versioned + Clone,
        F: Fn(&Metadata) -> Version,
    {
        let network_sender = Arc::new(MockNetworkSender);
        let tc = TaskCenterFactory::create(tokio::runtime::Handle::current());
        let metadata_manager = MetadataManager::build(network_sender);
        let metadata_writer = metadata_manager.writer();
        let metadata = metadata_manager.metadata();

        assert_eq!(Version::INVALID, config_version(&metadata));

        assert_eq!(Version::MIN, value.version());
        // updates happening before metadata manager start should not get lost.
        metadata_writer.submit(value.clone());

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

        let updated = Arc::new(AtomicBool::new(false));
        tokio::spawn({
            let metadata = metadata.clone();
            let updated = Arc::clone(&updated);
            async move {
                let _ = metadata.wait_for_version(kind, Version::from(3)).await;
                updated.store(true, Ordering::Release);
            }
        });

        // let's bump the version a couple of times.
        let mut value = value.clone();
        value.increment_version();
        value.increment_version();
        value.increment_version();
        value.increment_version();

        metadata_writer.update(value).await?;
        assert_eq!(true, updated.load(Ordering::Acquire));

        tc.cancel_tasks(None, None).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_nodes_config_watchers() -> Result<()> {
        test_watchers(
            create_mock_nodes_config(),
            MetadataKind::NodesConfiguration,
            |metadata| metadata.nodes_config_version(),
        )
        .await
    }

    #[tokio::test]
    async fn test_partition_table_watchers() -> Result<()> {
        test_watchers(
            FixedPartitionTable::new(Version::MIN, 42),
            MetadataKind::PartitionTable,
            |metadata| metadata.partition_table_version(),
        )
        .await
    }

    async fn test_watchers<T, F>(value: T, kind: MetadataKind, config_version: F) -> Result<()>
    where
        T: Into<MetadataContainer> + Versioned + Clone,
        F: Fn(&Metadata) -> Version,
    {
        let network_sender = Arc::new(MockNetworkSender);
        let tc = TaskCenterFactory::create(tokio::runtime::Handle::current());
        let metadata_manager = MetadataManager::build(network_sender);
        let metadata_writer = metadata_manager.writer();
        let metadata = metadata_manager.metadata();

        assert_eq!(Version::INVALID, config_version(&metadata));

        assert_eq!(Version::MIN, value.version());

        // start metadata manager
        spawn_metadata_manager(&tc, metadata_manager)?;

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
        value.increment_version();
        metadata_writer.update(value.clone()).await?;
        value.increment_version();
        metadata_writer.update(value.clone()).await?;
        value.increment_version();
        metadata_writer.update(value.clone()).await?;
        value.increment_version();
        metadata_writer.update(value.clone()).await?;

        // Watcher sees the latest value only.
        watcher2.changed().await?;
        assert_eq!(Version::from(5), *watcher2.borrow());
        assert!(!watcher2.has_changed().unwrap());

        watcher1.changed().await?;
        assert_eq!(Version::from(5), *watcher1.borrow());
        assert!(!watcher1.has_changed().unwrap());

        Ok(())
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
