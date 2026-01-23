// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::watch;

use crate::Merge;
pub use crate::protobuf::common::{
    AdminStatus, IngressStatus, LogServerStatus, MetadataServerStatus, NodeRpcStatus, NodeStatus,
    WorkerStatus,
};

/// All clones will share the same underlying channel
#[derive(Clone, Debug)]
pub struct Health {
    node_status: watch::Sender<NodeStatus>,
    worker_status: watch::Sender<WorkerStatus>,
    admin_status: watch::Sender<AdminStatus>,
    log_server_status: watch::Sender<LogServerStatus>,
    metadata_store_status: watch::Sender<MetadataServerStatus>,
    ingress_status: watch::Sender<IngressStatus>,
    node_rpc_status: watch::Sender<NodeRpcStatus>,
}

impl Default for Health {
    fn default() -> Self {
        Self::new()
    }
}

impl Health {
    pub fn new() -> Self {
        let node_status = watch::Sender::new(NodeStatus::Unknown);
        let worker_status = watch::Sender::new(WorkerStatus::Unknown);
        let admin_status = watch::Sender::new(AdminStatus::Unknown);
        let log_server_status = watch::Sender::new(LogServerStatus::Unknown);
        let metadata_store_status = watch::Sender::new(MetadataServerStatus::Unknown);
        let ingress_status = watch::Sender::new(IngressStatus::Unknown);
        let node_rpc_status = watch::Sender::new(NodeRpcStatus::Unknown);

        Self {
            node_status,
            worker_status,
            admin_status,
            log_server_status,
            metadata_store_status,
            ingress_status,
            node_rpc_status,
        }
    }

    pub fn current_node_status(&self) -> NodeStatus {
        *self.node_status.borrow()
    }

    pub fn current_worker_status(&self) -> WorkerStatus {
        *self.worker_status.borrow()
    }

    pub fn current_admin_status(&self) -> AdminStatus {
        *self.admin_status.borrow()
    }

    pub fn current_log_server_status(&self) -> LogServerStatus {
        *self.log_server_status.borrow()
    }

    pub fn current_metadata_store_status(&self) -> MetadataServerStatus {
        *self.metadata_store_status.borrow()
    }

    pub fn node_status(&self) -> HealthStatus<NodeStatus> {
        HealthStatus(self.node_status.clone())
    }

    pub fn worker_status(&self) -> HealthStatus<WorkerStatus> {
        HealthStatus(self.worker_status.clone())
    }

    pub fn admin_status(&self) -> HealthStatus<AdminStatus> {
        HealthStatus(self.admin_status.clone())
    }

    pub fn log_server_status(&self) -> HealthStatus<LogServerStatus> {
        HealthStatus(self.log_server_status.clone())
    }

    pub fn ingress_status(&self) -> HealthStatus<IngressStatus> {
        HealthStatus(self.ingress_status.clone())
    }

    pub fn metadata_server_status(&self) -> HealthStatus<MetadataServerStatus> {
        HealthStatus(self.metadata_store_status.clone())
    }

    pub fn node_rpc_status(&self) -> HealthStatus<NodeRpcStatus> {
        HealthStatus(self.node_rpc_status.clone())
    }
}

#[derive(Clone, Default, Debug)]
pub struct HealthStatus<T>(watch::Sender<T>);

impl<T> HealthStatus<T>
where
    T: PartialEq,
{
    /// Update the health status
    pub fn update(&self, status: T) {
        self.0.send_replace(status);
    }

    /// Merges the value with the current
    pub fn merge(&self, status: T)
    where
        T: Merge,
    {
        self.0.send_if_modified(|current| current.merge(status));
    }

    /// Conditional update. `update` can mutate the reference in-place but the no
    /// notifications will be sent if `update` returned false. It's important to return `true` if
    /// the value is actually modified.
    ///
    pub fn update_if<F>(&self, update: F)
    where
        F: FnOnce(&mut T) -> bool,
    {
        self.0.send_if_modified(update);
    }

    /// The last known status
    pub fn get(&self) -> impl std::ops::Deref<Target = T> + '_ {
        self.0.borrow()
    }

    /// Note that the receiver is primed with the current value
    pub fn subscribe(&self) -> watch::Receiver<T> {
        let mut rx = self.0.subscribe();
        rx.mark_changed();
        rx
    }

    pub async fn wait_for_value(&self, status: T) {
        // we don't expose (close) so it's safe to unwrap.
        self.0.subscribe().wait_for(|s| *s == status).await.unwrap();
    }

    pub async fn wait_for<F>(&self, condition: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.0.subscribe().wait_for(condition).await.unwrap();
    }
}
