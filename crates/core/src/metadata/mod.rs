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
pub use manager::MetadataManager;

use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use enum_map::EnumMap;
use tokio::sync::{oneshot, watch};

use restate_node_protocol::metadata::{MetadataContainer, MetadataKind};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::{GenerationalNodeId, Version};

use crate::{ShutdownError, TaskCenter, TaskId, TaskKind};

/// The kind of versioned metadata that can be synchronized across nodes.

#[derive(Clone)]
pub struct Metadata {
    sender: manager::CommandSender,
    inner: Arc<MetadataInner>,
}

impl Metadata {
    fn new(inner: Arc<MetadataInner>, sender: manager::CommandSender) -> Self {
        Self { inner, sender }
    }

    /// Panics if nodes configuration is not loaded yet.
    #[track_caller]
    pub fn nodes_config(&self) -> Arc<NodesConfiguration> {
        self.inner.nodes_config.load_full().unwrap()
    }

    #[track_caller]
    pub fn my_node_id(&self) -> GenerationalNodeId {
        *self.inner.my_node_id.get().expect("my_node_id is set")
    }

    /// Returns Version::INVALID if nodes configuration has not been loaded yet.
    pub fn nodes_config_version(&self) -> Version {
        let c = self.inner.nodes_config.load();
        match c.as_deref() {
            Some(c) => c.version(),
            None => Version::INVALID,
        }
    }

    /// Returns when the metadata kind is at the provided version (or newer)
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
}

#[derive(Default)]
struct MetadataInner {
    my_node_id: OnceLock<GenerationalNodeId>,
    nodes_config: ArcSwapOption<NodesConfiguration>,
    write_watches: EnumMap<MetadataKind, VersionWatch>,
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

pub fn spawn_metadata_manager(
    tc: &TaskCenter,
    metadata_manager: MetadataManager,
) -> Result<TaskId, ShutdownError> {
    tc.spawn(
        TaskKind::MetadataBackgroundSync,
        "metadata-manager",
        None,
        metadata_manager.run(),
    )
}
