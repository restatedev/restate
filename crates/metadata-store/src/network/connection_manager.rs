// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::network::grpc_svc;
use crate::network::NetworkMessage;
use futures::StreamExt;
use restate_core::{cancellation_watcher, ShutdownError, TaskCenter, TaskKind};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxStream;
use tracing::{debug, instrument};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Clone, derive_more::Debug)]
pub struct ConnectionManager<M> {
    inner: Arc<ConnectionManagerInner<M>>,
}

impl<M> ConnectionManager<M>
where
    M: NetworkMessage + Send + 'static,
{
    pub fn new(identity: u64, router: mpsc::Sender<M>) -> Self {
        ConnectionManager {
            inner: Arc::new(ConnectionManagerInner::new(identity, router)),
        }
    }

    pub fn identity(&self) -> u64 {
        self.inner.identity
    }

    pub fn accept_connection(
        &self,
        raft_peer: u64,
        incoming_rx: tonic::Streaming<grpc_svc::NetworkMessage>,
    ) -> Result<BoxStream<grpc_svc::NetworkMessage>, ConnectionError> {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(128);
        self.run_connection(raft_peer, outgoing_tx, incoming_rx)?;

        let outgoing_stream = ReceiverStream::new(outgoing_rx)
            .map(Result::<_, tonic::Status>::Ok)
            .boxed();
        Ok(outgoing_stream)
    }

    pub fn run_connection(
        &self,
        remote_peer: u64,
        outgoing_tx: mpsc::Sender<grpc_svc::NetworkMessage>,
        incoming_rx: tonic::Streaming<grpc_svc::NetworkMessage>,
    ) -> Result<(), ConnectionError> {
        let mut guard = self.inner.connections.lock().unwrap();

        if guard.contains_key(&remote_peer) {
            // we already have a connection established to remote peer
            return Ok(());
        }

        let connection = Connection::new(outgoing_tx);
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

    pub fn get_connection(&self, target: u64) -> Option<Connection> {
        self.inner.connections.lock().unwrap().get(&target).cloned()
    }
}

struct ConnectionReactor<M> {
    remote_peer: u64,
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
                                Ok(message) => {
                                    let mut cursor = Cursor::new(&message.payload);
                                    let message = M::deserialize(&mut cursor)?;

                                    assert_eq!(message.to(), self.connection_manager.identity, "Expect to only receive messages for peer '{}'", self.connection_manager.identity);

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
    identity: u64,
    connections: Mutex<HashMap<u64, Connection>>,
    router: mpsc::Sender<M>,
}

impl<M> ConnectionManagerInner<M> {
    pub fn new(identity: u64, router: mpsc::Sender<M>) -> Self {
        ConnectionManagerInner {
            identity,
            router,
            connections: Mutex::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Connection {
    tx: mpsc::Sender<grpc_svc::NetworkMessage>,
}

impl Connection {
    pub fn new(tx: mpsc::Sender<grpc_svc::NetworkMessage>) -> Self {
        Connection { tx }
    }

    pub fn try_send(
        &self,
        message: grpc_svc::NetworkMessage,
    ) -> Result<(), TrySendError<grpc_svc::NetworkMessage>> {
        self.tx.try_send(message)
    }
}
