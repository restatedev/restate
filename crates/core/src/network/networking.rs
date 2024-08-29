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

use restate_types::config::NetworkingOptions;
use restate_types::net::codec::{Targeted, WireEncode};
use restate_types::NodeId;

use super::{
    ConnectionManager, ConnectionSender, NetworkError, NetworkSendError, NetworkSender, Outgoing,
};
use crate::Metadata;

/// Access to node-to-node networking infrastructure.
#[derive(Clone)]
pub struct Networking {
    connections: ConnectionManager,
    metadata: Metadata,
    options: NetworkingOptions,
}

impl Networking {
    pub fn new(metadata: Metadata, options: NetworkingOptions) -> Self {
        Self {
            connections: ConnectionManager::new(metadata.clone(), options.clone()),
            metadata,
            options,
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
    #[instrument(level = "trace", skip(self, msg), fields(to = %msg.peer(), msg = ?msg.target()))]
    async fn send<M>(&self, mut msg: Outgoing<M>) -> Result<(), NetworkSendError<M>>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        let target_is_generational = msg.peer().is_generational();
        let original_peer = msg.peer();
        let mut attempts = 0;
        let mut retry_policy = self.options.connect_retry_policy.iter();
        loop {
            // find latest generation if this is not generational node id. We do this in the loop
            // to ensure we get the latest if it has been updated since last attempt.
            if !original_peer.is_generational() {
                let current_generation = match self
                    .metadata
                    .nodes_config_ref()
                    .find_node_by_id(original_peer)
                {
                    Ok(node) => node.current_generation,
                    Err(e) => return Err(NetworkSendError::new(msg, NetworkError::UnknownNode(e))),
                };
                msg.set_peer(current_generation);
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
                    let e = NetworkError::Unavailable(format!(
                        "failed to connect to node {} after {} attempts",
                        msg.peer(),
                        attempts + 1
                    ));
                    return Err(NetworkSendError::new(msg, e));
                }
            }

            let mut sender = {
                // if we already know a connection to use, let's try that unless it's dropped.
                if let Some(connection) = msg.get_connection() {
                    // let's try this connection
                    connection.sender(&self.metadata)
                } else {
                    match self
                        .connections
                        .get_node_sender(msg.peer().as_generational().unwrap())
                        .await
                    {
                        Ok(sender) => sender,
                        // retryable errors
                        Err(
                            e @ NetworkError::Timeout(_)
                            | e @ NetworkError::ConnectError(_)
                            | e @ NetworkError::ConnectionClosed,
                        ) => {
                            info!(
                                "Connection to node {} failed with {}, next retry is attempt {}/{}",
                                msg.peer(),
                                e,
                                attempts + 1,
                                self.options
                                    .connect_retry_policy
                                    .max_attempts()
                                    .unwrap_or(NonZeroUsize::MAX), // max_attempts() be Some at this point
                            );
                            continue;
                        }
                        // terminal errors
                        Err(NetworkError::OldPeerGeneration(e)) => {
                            if target_is_generational {
                                // Caller asked for this specific node generation and we know it's old.
                                return Err(NetworkSendError::new(
                                    msg,
                                    NetworkError::OldPeerGeneration(e),
                                ));
                            }
                            info!(
                                "Connection to node {} failed with {}, next retry is attempt {}/{}",
                                msg.peer(),
                                e,
                                attempts + 1,
                                self.options
                                    .connect_retry_policy
                                    .max_attempts()
                                    .unwrap_or(NonZeroUsize::MAX), // max_attempts() be Some at this point
                            );
                            continue;
                        }
                        Err(e) => {
                            return Err(NetworkSendError::new(
                                msg,
                                NetworkError::Unavailable(e.to_string()),
                            ))
                        }
                    }
                }
            };

            // can only fail due to codec errors or if connection is closed. Retry only if
            // connection closed.
            match sender.send(msg).await {
                Ok(_) => return Ok(()),
                Err(NetworkSendError {
                    message,
                    source: NetworkError::ConnectionClosed,
                }) => {
                    info!(
                        "Sending message to node {} failed due to connection reset, next retry is attempt {}/{}",
                        message.peer(),
                        attempts + 1,
                        self.options.connect_retry_policy.max_attempts().unwrap_or(NonZeroUsize::MAX), // max_attempts() be Some at this point
                    );
                    msg = message;
                    continue;
                }
                Err(e) => {
                    return Err(NetworkSendError::new(
                        e.message,
                        NetworkError::Unavailable(e.source.to_string()),
                    ))
                }
            }
        }
    }
}

static_assertions::assert_impl_all!(Networking: Send, Sync);
