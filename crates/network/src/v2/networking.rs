// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use restate_core::{NetworkSendError, NetworkSender};
use restate_node_protocol::{MessageEnvelope, NetworkMessage};
use restate_types::NodeId;
use tokio::sync::mpsc;

/// Access to node-to-node networking infrastructure;
#[derive(Default)]
pub struct Networking {}

impl Networking {
    #[track_caller]
    /// be called once on startup
    pub fn set_metadata_manager_subscriber(&self, _subscriber: mpsc::Sender<MessageEnvelope>) {}

    #[track_caller]
    /// be called once on startup
    pub fn set_ingress_subscriber(&self, _subscriber: mpsc::Sender<MessageEnvelope>) {}
}

#[async_trait]
impl NetworkSender for Networking {
    async fn send(&self, _to: NodeId, _message: &NetworkMessage) -> Result<(), NetworkSendError> {
        Ok(())
    }
}

static_assertions::assert_impl_all!(Networking: Send, Sync);
