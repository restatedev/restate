// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use metrics::counter;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxStream;
use tracing::{debug, instrument};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_types::PlainNodeId;
use restate_types::nodes_config::ClusterFingerprint;

use crate::metric_definitions::{
    METADATA_SERVER_REPLICATED_RECV_MESSAGE_BYTES, METADATA_SERVER_REPLICATED_RECV_MESSAGE_TOTAL,
    METADATA_SERVER_REPLICATED_SENT_MESSAGE_BYTES, METADATA_SERVER_REPLICATED_SENT_MESSAGE_TOTAL,
};
use crate::raft::network::{NetworkMessage, grpc_svc};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(
        "cluster fingerprint mismatch; rejecting connection because node seems to belong to a different cluster"
    )]
    ClusterFingerprintMismatch,
    #[error(
        "cluster name mismatch; rejecting connection because node seems to belong to a different cluster"
    )]
    ClusterNameMismatch,
}

#[derive(Clone, derive_more::Debug)]
pub struct ConnectionManager<M> {
    inner: Arc<ConnectionManagerInner<M>>,
}

impl<M> ConnectionManager<M>
where
    M: NetworkMessage + Send + 'static,
{
    pub fn new(
        identity: PlainNodeId,
        router: mpsc::Sender<M>,
        cluster_fingerprint: ClusterFingerprint,
        cluster_name: String,
    ) -> Self {
        ConnectionManager {
            inner: Arc::new(ConnectionManagerInner::new(
                identity,
                router,
                cluster_fingerprint,
                cluster_name,
            )),
        }
    }

    pub fn identity(&self) -> PlainNodeId {
        self.inner.identity
    }

    pub fn cluster_fingerprint(&self) -> ClusterFingerprint {
        self.inner.cluster_fingerprint
    }

    pub fn cluster_name(&self) -> &str {
        &self.inner.cluster_name
    }

    pub fn accept_connection(
        &self,
        raft_peer: PlainNodeId,
        cluster_fingerprint: Option<ClusterFingerprint>,
        cluster_name: Option<&str>,
        incoming_rx: tonic::Streaming<grpc_svc::NetworkMessage>,
    ) -> Result<BoxStream<grpc_svc::NetworkMessage>, ConnectionError> {
        // todo in v1.7 make this check fail if cluster_fingerprint and cluster_name are none
        // Validate cluster fingerprint if provided
        if let Some(incoming_fingerprint) = cluster_fingerprint {
            let expected_fingerprint = self.inner.cluster_fingerprint;

            if incoming_fingerprint != expected_fingerprint {
                return Err(ConnectionError::ClusterFingerprintMismatch);
            }
        }

        // Validate cluster name if provided
        if let Some(incoming_cluster_name) = cluster_name {
            let expected_cluster_name = &self.inner.cluster_name;

            if incoming_cluster_name != expected_cluster_name {
                return Err(ConnectionError::ClusterNameMismatch);
            }
        }

        let (outgoing_tx, outgoing_rx) = mpsc::channel(128);
        self.run_connection(raft_peer, outgoing_tx, incoming_rx)?;

        let outgoing_stream = ReceiverStream::new(outgoing_rx)
            .map(Result::<_, tonic::Status>::Ok)
            .boxed();
        Ok(outgoing_stream)
    }

    pub fn run_connection(
        &self,
        remote_peer: PlainNodeId,
        outgoing_tx: mpsc::Sender<grpc_svc::NetworkMessage>,
        incoming_rx: tonic::Streaming<grpc_svc::NetworkMessage>,
    ) -> Result<(), ConnectionError> {
        let mut guard = self.inner.connections.lock().unwrap();

        if guard.contains_key(&remote_peer) {
            // we already have a connection established to remote peer
            return Ok(());
        }

        let connection = Connection::new(remote_peer, outgoing_tx);
        guard.insert(remote_peer, connection);

        let reactor = ConnectionReactor {
            remote_peer,
            connection_manager: Arc::clone(&self.inner),
        };

        let _task_id = TaskCenter::spawn_child(
            TaskKind::ConnectionReactor,
            "raft-connection-reactor",
            reactor.run(incoming_rx),
        )?;

        Ok(())
    }

    pub fn get_connection(&self, target: PlainNodeId) -> Option<Connection> {
        self.inner.connections.lock().unwrap().get(&target).cloned()
    }
}

struct ConnectionReactor<M> {
    remote_peer: PlainNodeId,
    connection_manager: Arc<ConnectionManagerInner<M>>,
}

impl<M> ConnectionReactor<M>
where
    M: NetworkMessage,
{
    #[instrument(level = "debug", skip_all, fields(remote_peer = %self.remote_peer))]
    async fn run(
        self,
        mut incoming_rx: tonic::Streaming<grpc_svc::NetworkMessage>,
    ) -> anyhow::Result<()> {
        let mut shutdown = std::pin::pin!(cancellation_watcher());
        debug!("Run connection reactor");

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
                message = incoming_rx.next() => {
                    match message {
                        Some(message) => {
                            match message {
                                Ok(mut message) => {
                                    counter!(METADATA_SERVER_REPLICATED_RECV_MESSAGE_TOTAL, "peer" => self.remote_peer.to_string())
                                        .increment(1);
                                    counter!(METADATA_SERVER_REPLICATED_RECV_MESSAGE_BYTES, "peer" => self.remote_peer.to_string())
                                        .increment(message.payload.len() as u64);

                                    let message = M::deserialize(&mut message.payload)?;

                                    if message.to() != self.connection_manager.identity {
                                        debug!("Sender assumes me to be node {} but I am node {}; closing the connection", message.to(), self.connection_manager.identity);
                                        break;
                                    }

                                    if self.connection_manager.router.send(message).await.is_err() {
                                        // system is shutting down
                                        debug!("System is shutting down; closing connection");
                                        break;
                                    }
                                }
                                Err(err) => {
                                    debug!("Closing connection because received error: {err}");
                                    break;
                                }
                            }
                        }
                        None => {
                            debug!("Remote peer closed connection");
                            break
                        },
                    }
                }
            }
        }

        Ok(())
    }
}

impl<M> Drop for ConnectionReactor<M> {
    fn drop(&mut self) {
        debug!(remote_peer = %self.remote_peer, "Close connection");
        self.connection_manager
            .connections
            .lock()
            .expect("shouldn't be poisoned")
            .remove(&self.remote_peer);
    }
}

#[derive(Debug)]
struct ConnectionManagerInner<M> {
    identity: PlainNodeId,
    cluster_fingerprint: ClusterFingerprint,
    cluster_name: String,
    connections: Mutex<HashMap<PlainNodeId, Connection>>,
    router: mpsc::Sender<M>,
}

impl<M> ConnectionManagerInner<M> {
    pub fn new(
        identity: PlainNodeId,
        router: mpsc::Sender<M>,
        cluster_fingerprint: ClusterFingerprint,
        cluster_name: String,
    ) -> Self {
        ConnectionManagerInner {
            identity,
            cluster_fingerprint,
            cluster_name,
            router,
            connections: Mutex::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Connection {
    remote_peer: PlainNodeId,
    tx: mpsc::Sender<grpc_svc::NetworkMessage>,
}

impl Connection {
    pub fn new(remote_peer: PlainNodeId, tx: mpsc::Sender<grpc_svc::NetworkMessage>) -> Self {
        Connection { remote_peer, tx }
    }

    pub fn try_send(
        &self,
        message: grpc_svc::NetworkMessage,
    ) -> Result<(), TrySendError<grpc_svc::NetworkMessage>> {
        let size = message.payload.len();

        self.tx.try_send(message)?;

        // note that we assume that this message is sent while in fact
        // it's just queued. The actual message might still not get
        // sent

        counter!(METADATA_SERVER_REPLICATED_SENT_MESSAGE_TOTAL, "peer" => self.remote_peer.to_string())
            .increment(1);
        counter!(METADATA_SERVER_REPLICATED_SENT_MESSAGE_BYTES, "peer" => self.remote_peer.to_string())
            .increment(size as u64);

        Ok(())
    }
}
