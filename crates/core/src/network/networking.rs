// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use tracing::{info, instrument, trace};

use restate_types::net::codec::{Targeted, WireEncode};
use restate_types::retries::RetryPolicy;
use restate_types::NodeId;

use crate::Metadata;

use super::{ConnectionManager, ConnectionSender, NetworkError, NetworkSender};

/// Access to node-to-node networking infrastructure.
#[derive(Clone)]
pub struct Networking {
    connections: ConnectionManager,
    metadata: Metadata,
    connect_retry_policy: RetryPolicy,
}

impl Networking {
    pub fn new(metadata: Metadata, connect_retry_policy: RetryPolicy) -> Self {
        Self {
            connections: ConnectionManager::new(metadata.clone()),
            metadata,
            connect_retry_policy,
        }
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
                self.metadata
                    .nodes_config_ref()
                    .find_node_by_id(node)?
                    .current_generation
            }
        };

        self.connections.get_node_sender(node).await
    }
}

impl NetworkSender for Networking {
    #[instrument(level = "info", skip(self, to, message), fields(to = %to, msg = ?message.target()))]
    async fn send<M>(&self, to: NodeId, message: &M) -> Result<(), NetworkError>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        let target_is_generational = to.is_generational();
        let mut attempts = 0;
        let mut retry_policy = self.connect_retry_policy.iter();
        loop {
            // find latest generation if this is not generational node id. We do this in the loop
            // to ensure we get the latest if it has been updated since last attempt.
            let to = match to.as_generational() {
                Some(to) => to,
                None => match self.metadata.nodes_config_ref().find_node_by_id(to) {
                    Ok(node) => node.current_generation,
                    Err(e) => return Err(NetworkError::UnknownNode(e)),
                },
            };

            attempts += 1;
            if attempts > 1 {
                if let Some(next_retry_interval) = retry_policy.next() {
                    trace!(
                        attempt = ?attempts,
                        delay = ?next_retry_interval,
                        "Delaying retry",
                    );
                    tokio::time::sleep(next_retry_interval).await;
                } else {
                    return Err(NetworkError::Unavailable(format!(
                        "failed to connect to node {} after {} attempts",
                        to,
                        attempts + 1
                    )));
                }
            }

            let mut sender = match self.connections.get_node_sender(to).await {
                Ok(sender) => sender,
                // retryable errors
                Err(
                    e @ NetworkError::Timeout(_)
                    | e @ NetworkError::ConnectError(_)
                    | e @ NetworkError::ConnectionClosed,
                ) => {
                    info!(
                        "Connection to node {} failed with {}, next retry is attempt {}/{}",
                        to,
                        e,
                        attempts + 1,
                        self.connect_retry_policy
                            .max_attempts()
                            .unwrap_or(NonZeroUsize::MAX), // max_attempts() be Some at this point
                    );
                    continue;
                }
                // terminal errors
                Err(NetworkError::OldPeerGeneration(e)) => {
                    if target_is_generational {
                        // Caller asked for this specific node generation and we know it's old.
                        return Err(NetworkError::OldPeerGeneration(e));
                    }
                    info!(
                        "Connection to node {} failed with {}, next retry is attempt {}/{}",
                        to,
                        e,
                        attempts + 1,
                        self.connect_retry_policy
                            .max_attempts()
                            .unwrap_or(NonZeroUsize::MAX), // max_attempts() be Some at this point
                    );
                    continue;
                }
                Err(e) => return Err(NetworkError::Unavailable(e.to_string())),
            };

            // can only fail due to codec errors or if connection is closed. Retry only if
            // connection closed.
            match sender.send(message).await {
                Ok(_) => return Ok(()),
                Err(NetworkError::ConnectionClosed) => {
                    info!(
                        "Sending message to node {} failed due to connection reset, next retry is attempt {}/{}",
                        to,
                        attempts + 1,
                        self.connect_retry_policy.max_attempts().unwrap_or(NonZeroUsize::MAX), // max_attempts() be Some at this point
                    );
                    continue;
                }
                Err(e) => return Err(NetworkError::Unavailable(e.to_string())),
            }
        }
    }
}

static_assertions::assert_impl_all!(Networking: Send, Sync);
