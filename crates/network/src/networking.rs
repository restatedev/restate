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

use restate_types::retries::with_jitter;
use tracing::{info, instrument, trace};

use restate_core::metadata;
use restate_core::network::{NetworkSendError, NetworkSender};
use restate_types::net::codec::{Targeted, WireEncode};
use restate_types::NodeId;

use crate::error::NetworkError;
use crate::{ConnectionManager, ConnectionSender};

const DEFAULT_MAX_CONNECT_ATTEMPTS: u32 = 10;
// todo: make this configurable
const SEND_RETRY_BASE_DURATION: Duration = Duration::from_millis(250);

/// Access to node-to-node networking infrastructure;
#[derive(Clone, Default)]
pub struct Networking {
    connections: ConnectionManager,
}

impl Networking {
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

impl NetworkSender for Networking {
    #[instrument(level = "info", skip(self, to, message), fields(to = %to, msg = ?message.target()))]
    async fn send<M>(&self, to: NodeId, message: &M) -> Result<(), NetworkSendError>
    where
        M: WireEncode + Targeted + Send + Sync,
    {
        let target_is_generational = to.is_generational();
        // we try to reconnect to the node for N times.
        let mut attempts = 0;
        loop {
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
            if attempts > 1 {
                sleep_with_jitter(SEND_RETRY_BASE_DURATION).await;
            }

            let sender = match self.connections.get_node_sender(to).await {
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
                        DEFAULT_MAX_CONNECT_ATTEMPTS
                    );
                    continue;
                }
                // terminal errors
                Err(NetworkError::OldPeerGeneration(e)) => {
                    if target_is_generational {
                        // Caller asked for this specific node generation and we know it's old.
                        return Err(NetworkSendError::OldPeerGeneration(e));
                    }
                    info!(
                        "Connection to node {} failed with {}, next retry is attempt {}/{}",
                        to,
                        e,
                        attempts + 1,
                        DEFAULT_MAX_CONNECT_ATTEMPTS
                    );
                    continue;
                }
                Err(e) => return Err(NetworkSendError::Unavailable(e.to_string())),
            };

            // can only fail due to codec errors or if connection is closed. Retry only if
            // connection closed.
            match sender.send(message).await {
                Ok(_) => return Ok(()),
                Err(NetworkSendError::ConnectionClosed) => {
                    info!(
                        "Sending message to node {} failed due to connection reset, next retry is attempt {}/{}",
                        to, attempts + 1, DEFAULT_MAX_CONNECT_ATTEMPTS
                    );
                    continue;
                }
                Err(e) => return Err(NetworkSendError::Unavailable(e.to_string())),
            }
        }
    }
}

// todo: replace with RetryPolicy
async fn sleep_with_jitter(duration: Duration) {
    let retry_after = with_jitter(duration, 0.3);
    trace!("sleeping for {:?}", retry_after);
    tokio::time::sleep(retry_after).await;
}

static_assertions::assert_impl_all!(Networking: Send, Sync);
