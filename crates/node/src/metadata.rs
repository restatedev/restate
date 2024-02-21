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

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use enum_map::EnumMap;
use restate_task_center::ShutdownError;
use tokio::sync::{oneshot, watch};
use tracing::info;

use restate_task_center::cancellation_watcher;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::MetadataKind;
use restate_types::Version;

type CommandSender = tokio::sync::mpsc::UnboundedSender<Command>;
type CommandReceiver = tokio::sync::mpsc::UnboundedReceiver<Command>;

// todo
struct PartitionTable;

/// Handle to access locally cached metadata, request metadata updates, and more.
#[derive(Clone)]
pub struct Metadata {
    sender: CommandSender,
    inner: Arc<MetadataInner>,
}

pub enum MetadataContainer {
    NodesConfiguration(NodesConfiguration),
}

impl MetadataContainer {
    pub fn kind(&self) -> MetadataKind {
        match self {
            MetadataContainer::NodesConfiguration(_) => MetadataKind::NodesConfiguration,
        }
    }
}

impl From<NodesConfiguration> for MetadataContainer {
    fn from(value: NodesConfiguration) -> Self {
        MetadataContainer::NodesConfiguration(value)
    }
}

impl Metadata {
    fn new(inner: Arc<MetadataInner>, sender: CommandSender) -> Self {
        Self { inner, sender }
    }

    /// Panics if nodes configuration is not loaded yet.
    pub fn nodes_config(&self) -> Arc<NodesConfiguration> {
        self.inner.nodes_config.load_full().unwrap()
    }

    /// Returns Version::INVALID if nodes configuration has not been loaded yet.
    pub fn nodes_config_version(&self) -> Version {
        let c = self.inner.nodes_config.load();
        match c.as_deref() {
            Some(c) => c.version(),
            None => Version::INVALID,
        }
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

    // Watch for version updates of this metadata kind.
    pub fn watch(&self, metadata_kind: MetadataKind) -> watch::Receiver<Version> {
        self.inner.write_watches[metadata_kind].receive.clone()
    }
}

enum Command {
    UpdateMetadata(MetadataContainer, Option<oneshot::Sender<()>>),
}

#[derive(Default)]
struct MetadataInner {
    nodes_config: ArcSwapOption<NodesConfiguration>,
    write_watches: EnumMap<MetadataKind, VersionWatch>,
}

/// Can send updates to metadata manager. This should be accessible by the rpc handler layer to
/// handle incoming metadata updates from the network, or to handle updates coming from metadata
/// service if it's running on this node. MetadataManager ensures that writes are monotonic
/// so it's safe to call update_* without checking the current version.
#[derive(Clone)]
pub struct MetadataWriter {
    sender: CommandSender,
}

impl MetadataWriter {
    fn new(sender: CommandSender) -> Self {
        Self { sender }
    }

    // Returns when the nodes configuration update is performed.
    pub async fn update(&self, value: impl Into<MetadataContainer>) -> Result<(), ShutdownError> {
        let (callback, recv) = oneshot::channel();
        let o = self
            .sender
            .send(Command::UpdateMetadata(value.into(), Some(callback)));
        if o.is_ok() {
            let _ = recv.await;
            Ok(())
        } else {
            Err(ShutdownError)
        }
    }

    // Fire and forget update
    pub fn submit(&self, value: impl Into<MetadataContainer>) {
        // Ignore the error, task-center takes care of safely shutting down the
        // system if metadata manager failed
        let _ = self
            .sender
            .send(Command::UpdateMetadata(value.into(), None));
    }
}

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
}

impl MetadataManager {
    pub fn build() -> Self {
        let (self_sender, inbound) = tokio::sync::mpsc::unbounded_channel();

        Self {
            inner: Arc::new(MetadataInner::default()),
            inbound,
            self_sender,
        }
    }

    pub fn metadata(&self) -> Metadata {
        Metadata::new(self.inner.clone(), self.self_sender.clone())
    }

    pub fn writer(&self) -> MetadataWriter {
        MetadataWriter::new(self.self_sender.clone())
    }

    /// Start and wait for shutdown signal.
    pub async fn run(mut self /*, network_sender: NetworkSender*/) -> anyhow::Result<()> {
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
            }
        }
        Ok(())
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::UpdateMetadata(value, callback) => self.update_metadata(value, callback),
        }
    }

    fn update_metadata(&mut self, value: MetadataContainer, callback: Option<oneshot::Sender<()>>) {
        match value {
            MetadataContainer::NodesConfiguration(config) => {
                self.update_nodes_configuration(config, callback);
            }
        }
    }

    fn update_nodes_configuration(
        &mut self,
        config: NodesConfiguration,
        callback: Option<oneshot::Sender<()>>,
    ) {
        let inner = &self.inner;
        let current = inner.nodes_config.load();
        let mut maybe_new_version = config.version();
        match current.as_deref() {
            None => {
                inner.nodes_config.store(Some(Arc::new(config)));
            }
            Some(current) if config.version() > current.version() => {
                inner.nodes_config.store(Some(Arc::new(config)));
            }
            Some(current) => {
                /* Do nothing, current is already newer */
                maybe_new_version = current.version();
            }
        }

        if let Some(callback) = callback {
            let _ = callback.send(());
        }

        // notify watches.
        self.inner.write_watches[MetadataKind::NodesConfiguration]
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    use googletest::prelude::*;
    use restate_task_center::{TaskCenterFactory, TaskKind};
    use restate_test_util::assert_eq;
    use restate_types::nodes_config::{AdvertisedAddress, NodeConfig, Role};
    use restate_types::GenerationalNodeId;

    #[tokio::test]
    async fn test_nodes_config_updates() -> Result<()> {
        let tc = TaskCenterFactory::create(tokio::runtime::Handle::current());
        let metadata_manager = MetadataManager::build();
        let metadata_writer = metadata_manager.writer();
        let metadata = metadata_manager.metadata();

        assert_eq!(Version::INVALID, metadata.nodes_config_version());

        let nodes_config = create_mock_nodes_config();
        assert_eq!(Version::MIN, nodes_config.version());
        // updates happening before metadata manager start should not get lost.
        metadata_writer.submit(nodes_config.clone());

        // start metadata manager
        tc.spawn(
            TaskKind::MetadataBackgroundSync,
            "metadata-manager",
            None,
            metadata_manager.run(),
        )?;

        let version = metadata
            .wait_for_version(MetadataKind::NodesConfiguration, Version::MIN)
            .await
            .unwrap();
        assert_eq!(Version::MIN, version);

        // Wait should not block if waiting older version
        let version2 = metadata
            .wait_for_version(MetadataKind::NodesConfiguration, Version::INVALID)
            .await
            .unwrap();
        assert_eq!(version, version2);

        let updated = Arc::new(AtomicBool::new(false));
        tokio::spawn({
            let metadata = metadata.clone();
            let updated = Arc::clone(&updated);
            async move {
                let _ = metadata
                    .wait_for_version(MetadataKind::NodesConfiguration, Version::from(3))
                    .await;
                updated.store(true, Ordering::Release);
            }
        });

        // let's bump the version a couple of times.
        let mut nodes_config = nodes_config.clone();
        nodes_config.increment_version();
        nodes_config.increment_version();
        nodes_config.increment_version();
        nodes_config.increment_version();

        metadata_writer.update(nodes_config).await?;
        assert_eq!(true, updated.load(Ordering::Acquire));

        tc.cancel_tasks(None, None).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_watchers() -> Result<()> {
        let tc = TaskCenterFactory::create(tokio::runtime::Handle::current());
        let metadata_manager = MetadataManager::build();
        let metadata_writer = metadata_manager.writer();
        let metadata = metadata_manager.metadata();

        assert_eq!(Version::INVALID, metadata.nodes_config_version());

        let nodes_config = create_mock_nodes_config();
        assert_eq!(Version::MIN, nodes_config.version());

        // start metadata manager
        tc.spawn(
            TaskKind::MetadataBackgroundSync,
            "metadata-manager",
            None,
            metadata_manager.run(),
        )?;

        let mut watcher1 = metadata.watch(MetadataKind::NodesConfiguration);
        assert_eq!(Version::INVALID, *watcher1.borrow());
        let mut watcher2 = metadata.watch(MetadataKind::NodesConfiguration);
        assert_eq!(Version::INVALID, *watcher2.borrow());

        metadata_writer.update(nodes_config.clone()).await?;
        watcher1.changed().await?;

        assert_eq!(Version::MIN, *watcher1.borrow());
        assert_eq!(Version::MIN, *watcher2.borrow());

        // let's push multiple updates
        let mut nodes_config = nodes_config.clone();
        nodes_config.increment_version();
        metadata_writer.update(nodes_config.clone()).await?;
        nodes_config.increment_version();
        metadata_writer.update(nodes_config.clone()).await?;
        nodes_config.increment_version();
        metadata_writer.update(nodes_config.clone()).await?;
        nodes_config.increment_version();
        metadata_writer.update(nodes_config.clone()).await?;

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
