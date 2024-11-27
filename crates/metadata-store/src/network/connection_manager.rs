// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::StreamExt;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use restate_core::{cancellation_watcher, ShutdownError, TaskCenter, TaskKind};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxStream;
use tracing::{debug, instrument};
use crate::network::grpc_svc::RaftMessage;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Clone, derive_more::Debug)]
pub struct ConnectionManager {
    inner: Arc<ConnectionManagerInner>,
}

impl ConnectionManager {
    pub fn new(identity: u64, router: mpsc::Sender<Message>) -> Self {
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
        incoming_rx: tonic::Streaming<RaftMessage>,
    ) -> Result<BoxStream<RaftMessage>, ConnectionError> {
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
        outgoing_tx: mpsc::Sender<RaftMessage>,
        incoming_rx: tonic::Streaming<RaftMessage>,
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

struct ConnectionReactor {
    remote_peer: u64,
    connection_manager: Arc<ConnectionManagerInner>,
}

impl ConnectionReactor {
    #[instrument(level = "debug", skip_all, fields(remote_peer = %self.remote_peer))]
    async fn run(self, mut incoming_rx: tonic::Streaming<RaftMessage>) -> anyhow::Result<()> {
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
                                    let message = Message::parse_from_carllerche_bytes(&message.message)?;

                                    assert_eq!(message.to, self.connection_manager.identity, "Expect to only receive messages for peer '{}'", self.connection_manager.identity);

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

impl Drop for ConnectionReactor {
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
struct ConnectionManagerInner {
    identity: u64,
    connections: Mutex<HashMap<u64, Connection>>,
    router: mpsc::Sender<Message>,
}

impl ConnectionManagerInner {
    pub fn new(identity: u64, router: mpsc::Sender<Message>) -> Self {
        ConnectionManagerInner {
            identity,
            router,
            connections: Mutex::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Connection {
    tx: mpsc::Sender<RaftMessage>,
}

impl Connection {
    pub fn new(tx: mpsc::Sender<RaftMessage>) -> Self {
        Connection { tx }
    }

    pub fn try_send(&self, message: RaftMessage) -> Result<(), TrySendError<RaftMessage>> {
        self.tx.try_send(message)
    }
}