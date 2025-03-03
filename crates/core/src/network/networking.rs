// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use tracing::{debug, instrument, trace};

use restate_types::NodeId;
use restate_types::config::Configuration;
use restate_types::net::codec::{Targeted, WireEncode};

use super::{
    ConnectionManager, HasConnection, NetworkError, NetworkSendError, NetworkSender, NoConnection,
    Outgoing, WeakConnection,
};
use super::{GrpcConnector, TransportConnect};
use crate::Metadata;

/// Access to node-to-node networking infrastructure.
pub struct Networking<T> {
    connections: ConnectionManager,
    connector: T,
}

impl<T: Clone> Clone for Networking<T> {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            connector: self.connector.clone(),
        }
    }
}

impl Networking<GrpcConnector> {
    pub fn with_grpc_connector() -> Self {
        Self {
            connections: ConnectionManager::default(),
            connector: GrpcConnector,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
/// used for testing. Accepts connections but can't establish new connections
impl Networking<super::FailingConnector> {
    pub fn new_incoming_only() -> Self {
        Self {
            connections: ConnectionManager::default(),
            connector: super::FailingConnector,
        }
    }
}

impl<T: TransportConnect> Networking<T> {
    pub fn with_connector(connector: T) -> Self {
        Self {
            connector,
            connections: ConnectionManager::default(),
        }
    }

    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connections
    }

    /// A connection sender is pinned to a single stream, thus guaranteeing ordered delivery of
    /// messages.
    pub async fn node_connection(
        &self,
        node: impl Into<NodeId>,
    ) -> Result<WeakConnection, NetworkError> {
        let node = node.into();
        // find latest generation if this is not generational node id
        let node = match node.as_generational() {
            Some(node) => node,
            None => {
                Metadata::with_current(|metadata| metadata.nodes_config_ref())
                    .find_node_by_id(node)?
                    .current_generation
            }
        };

        Ok(self
            .connections
            .get_or_connect(node, &self.connector)
            .await?
            .downgrade())
    }
}

impl<T: TransportConnect> NetworkSender<NoConnection> for Networking<T> {
    #[instrument(level = "error", skip(self, msg), fields(to = %msg.peer(), msg = ?msg.body().target()))]
    async fn send<M>(&self, mut msg: Outgoing<M>) -> Result<(), NetworkSendError<Outgoing<M>>>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        let metadata = Metadata::current();
        let target_is_generational = msg.peer().is_generational();
        let original_peer = *msg.peer();
        let mut attempts = 0;
        let max_attempts: usize = Configuration::pinned()
            .networking
            .connect_retry_policy
            .max_attempts()
            .unwrap_or(NonZeroUsize::MAX)
            .into(); // max_attempts() be Some at this point
        let mut retry_policy = Configuration::pinned()
            .networking
            .connect_retry_policy
            .clone()
            .into_iter();
        let mut peer_as_generational = msg.peer().as_generational();
        loop {
            // find latest generation if this is not generational node id. We do this in the loop
            // to ensure we get the latest if it has been updated since last attempt.
            if !original_peer.is_generational() {
                let current_generation = match metadata
                    .nodes_config_ref()
                    .find_node_by_id(original_peer)
                {
                    Ok(node) => node.current_generation,
                    Err(e) => return Err(NetworkSendError::new(msg, NetworkError::UnknownNode(e))),
                };
                peer_as_generational = Some(current_generation);
            };

            attempts += 1;
            let next_attempt = attempts + 1;
            if attempts > 1 {
                if let Some(next_retry_interval) = retry_policy.next() {
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

            let sender = {
                let peer_as_generational = peer_as_generational.unwrap();
                match self
                    .connections
                    .get_or_connect(peer_as_generational, &self.connector)
                    .await
                {
                    Ok(sender) => sender,
                    // retryable errors
                    Err(
                        err @ NetworkError::Timeout(_)
                        | err @ NetworkError::ConnectError(_)
                        | err @ NetworkError::ConnectionClosed(_),
                    ) => {
                        if next_attempt >= max_attempts {
                            trace!(
                                peer = %peer_as_generational,
                                ?err,
                                "Exhausted attempts to connect to node",
                            );
                        }
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
                        if next_attempt >= max_attempts {
                            trace!(
                                peer = %peer_as_generational,
                                "Exhausted attempts to connect to node",
                            );
                        }
                        continue;
                    }
                    Err(e) => {
                        return Err(NetworkSendError::new(
                            msg,
                            NetworkError::Unavailable(e.to_string()),
                        ));
                    }
                }
            };

            // can only fail due to codec errors or if connection is closed. Retry only if
            // connection closed.
            let msg_with_connection = msg.assign_connection(sender.downgrade());
            match msg_with_connection.send().await {
                Ok(_) => return Ok(()),
                Err(NetworkSendError {
                    original,
                    source: NetworkError::ConnectionClosed(_),
                }) => {
                    if next_attempt >= max_attempts {
                        debug!(
                            "Sending message to node {} failed due to connection reset",
                            peer_as_generational.unwrap(),
                        );
                    }

                    msg = original.forget_connection();
                    continue;
                }
                Err(e) => {
                    return Err(NetworkSendError::new(
                        e.original.forget_connection().set_peer(original_peer),
                        NetworkError::Unavailable(e.source.to_string()),
                    ));
                }
            }
        }
    }
}

impl<T: TransportConnect> NetworkSender<HasConnection> for Networking<T> {
    #[instrument(level = "error", skip(self, msg), fields(to = %msg.peer(), msg = ?msg.body().target()))]
    async fn send<M>(
        &self,
        msg: Outgoing<M, HasConnection>,
    ) -> Result<(), NetworkSendError<Outgoing<M, HasConnection>>>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        // connection is set. Just use it.
        msg.send().await
    }
}
