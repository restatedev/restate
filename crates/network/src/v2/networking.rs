// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::mpsc;
use tracing::info;

use restate_core::{metadata, NetworkSendError, NetworkSender};
use restate_node_protocol::{MessageEnvelope, NetworkMessage};
use restate_types::NodeId;

use crate::error::NetworkError;
use crate::{ConnectionManager, ConnectionSender};

const DEFAULT_MAX_CONNECT_ATTEMPTS: u32 = 10;

/// Access to node-to-node networking infrastructure;
#[derive(Default)]
pub struct Networking {
    connections: ConnectionManager,
}

impl Networking {
    #[track_caller]
    /// must be called at most once on startup
    pub fn set_metadata_manager_handler(&self, handler: mpsc::Sender<MessageEnvelope>) {
        self.connections
            .router()
            .set_metadata_manager_handler(handler)
    }

    #[track_caller]
    /// must be called at most once on startup
    pub fn set_ingress_handler(&self, handler: mpsc::Sender<MessageEnvelope>) {
        self.connections.router().set_ingress_handler(handler)
    }

    pub fn connection_manager(&self) -> ConnectionManager {
        self.connections.clone()
    }

    /// A connection sender is pinned to a single stream, thus guaranteeing ordered delivery of
    /// messages.
    pub async fn node_connection(&self, node: NodeId) -> Result<ConnectionSender, NetworkError> {
        // find latest generation if this is not generational node id

        let node = match node.as_generational() {
            Some(node) => node,
            None => {
                metadata()
                    .nodes_config()
                    .find_node_by_id(node)?
                    .current_generation
            }
        };

        self.connections.get_node_sender(node).await
    }
}

#[async_trait]
impl NetworkSender for Networking {
    async fn send(&self, to: NodeId, message: &NetworkMessage) -> Result<(), NetworkSendError> {
        // we try to reconnect to the node for N times.
        let mut attempts = 0;
        loop {
            // random jitter up to 500ms
            let jitter = rand::thread_rng().gen_range(1..500);
            let retry_after = Duration::from_millis(250 + jitter);
            // find latest generation if this is not generational node id. We do this in the loop
            // to ensure we get the latest if it has been updated since last attempt.
            let to = match to.as_generational() {
                Some(to) => to,
                None => match metadata().nodes_config().find_node_by_id(to) {
                    Ok(node) => node.current_generation,
                    Err(e) => return Err(NetworkSendError::UnknownNode(e)),
                },
            };

            attempts += 1;
            if attempts > DEFAULT_MAX_CONNECT_ATTEMPTS {
                return Err(NetworkSendError::Unavailable(format!(
                    "failed to connect to node {} after {} attempts",
                    to, DEFAULT_MAX_CONNECT_ATTEMPTS
                )));
            }

            let sender = match self.connections.get_node_sender(to).await {
                Ok(sender) => sender,
                // retryable errors
                Err(
                    e @ NetworkError::Timeout(_)
                    | e @ NetworkError::ConnectError(_)
                    | e @ NetworkError::ConnectionClosed,
                ) => {
                    info!("Connection to node {} failed with {}, retrying...", to, e);
                    tokio::time::sleep(retry_after).await;
                    continue;
                }
                // terminal errors
                Err(NetworkError::OldPeerGeneration(e)) => {
                    return Err(NetworkSendError::OldPeerGeneration(e))
                }
                Err(e) => return Err(NetworkSendError::Unavailable(e.to_string())),
            };

            // can only fail due to codec errors or if connection is closed. Retry only if
            // connection closed.
            match sender.send(message).await {
                Ok(_) => return Ok(()),
                Err(NetworkError::ConnectionClosed) => {
                    info!(
                        "Sending messages to node {} failed due to connection reset, retrying...",
                        to
                    );
                    tokio::time::sleep(retry_after).await;
                    continue;
                }
                Err(e) => return Err(NetworkSendError::Unavailable(e.to_string())),
            }
        }
    }
}

static_assertions::assert_impl_all!(Networking: Send, Sync);
