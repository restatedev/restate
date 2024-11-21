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
use std::sync::Arc;

use tracing::{debug, instrument, trace};

use restate_types::config::NetworkingOptions;
use restate_types::net::codec::{Targeted, WireEncode};
use restate_types::{GenerationalNodeId, NodeId};

use super::{
    ConnectionManager, HasConnection, NetworkError, NetworkSendError, NetworkSender, NoConnection,
    Outgoing, OwnedConnection, WeakConnection,
};
use super::{GrpcConnector, TransportConnect};
use crate::Metadata;

/// Access to node-to-node networking infrastructure.
pub struct Networking<T> {
    connections: ConnectionManager<T>,
    metadata: Metadata,
    options: NetworkingOptions,
}

impl<T> Clone for Networking<T> {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            metadata: self.metadata.clone(),
            options: self.options.clone(),
        }
    }
}

impl Networking<GrpcConnector> {
    pub fn new(metadata: Metadata, options: NetworkingOptions) -> Self {
        Self {
            connections: ConnectionManager::new(
                metadata.clone(),
                Arc::new(GrpcConnector::new(options.clone())),
                options.clone(),
            ),
            metadata,
            options,
        }
    }
}

impl<T: TransportConnect> Networking<T> {
    pub fn with_connection_manager(
        metadata: Metadata,
        options: NetworkingOptions,
        connection_manager: ConnectionManager<T>,
    ) -> Self {
        Self {
            connections: connection_manager,
            metadata,
            options,
        }
    }

    pub fn connection_manager(&self) -> &ConnectionManager<T> {
        &self.connections
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn my_node_id(&self) -> GenerationalNodeId {
        self.metadata.my_node_id()
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
                self.metadata
                    .nodes_config_ref()
                    .find_node_by_id(node)?
                    .current_generation
            }
        };

        Ok(self.connections.get_or_connect(node).await?.downgrade())
    }

    /// A connection sender is pinned to a single stream, thus guaranteeing ordered delivery of
    /// messages.
    pub async fn node_owned_connection(
        &self,
        node: impl Into<NodeId>,
    ) -> Result<Arc<OwnedConnection>, NetworkError> {
        let node = node.into();
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

        self.connections.get_or_connect(node).await
    }
}

impl<T: TransportConnect> NetworkSender<NoConnection> for Networking<T> {
    #[instrument(level = "error", skip(self, msg), fields(to = %msg.peer(), msg = ?msg.body().target()))]
    async fn send<M>(&self, mut msg: Outgoing<M>) -> Result<(), NetworkSendError<Outgoing<M>>>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        let target_is_generational = msg.peer().is_generational();
        let original_peer = *msg.peer();
        let mut attempts = 0;
        let max_attempts: usize = self
            .options
            .connect_retry_policy
            .max_attempts()
            .unwrap_or(NonZeroUsize::MAX)
            .into(); // max_attempts() be Some at this point
        let mut retry_policy = self.options.connect_retry_policy.iter();
        let mut peer_as_generational = msg.peer().as_generational();
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
                match self.connections.get_or_connect(peer_as_generational).await {
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
                        ))
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
                    ))
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
