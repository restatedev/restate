// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use enum_map::{EnumMap, enum_map};
use futures::future::OptionFuture;
use futures::{Stream, StreamExt};
use metrics::counter;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use strum::IntoEnumIterator as _;
use tokio::sync::oneshot;
use tokio::time::Sleep;
use tracing::{Instrument, Span, debug, info, trace, warn};

use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::net::ServiceTag;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;
use restate_types::{Version, Versioned};

use crate::network::incoming::{RawRpc, RawUnary, RpcReplyPort};
use crate::network::io::EgressMessage;
use crate::network::metric_definitions::NETWORK_MESSAGE_RECEIVED_BYTES;
use crate::network::protobuf::network::message::{Body, Signal};
use crate::network::protobuf::network::{Datagram, RpcReply, datagram, rpc_reply};
use crate::network::protobuf::network::{Header, Message};
use crate::network::tracking::ConnectionTracking;
use crate::network::{
    Connection, Incoming, MessageRouter, PeerMetadataVersion, ReplyEnvelope, RouterError,
    RpcReplyError,
};
use crate::{Metadata, ShutdownError, TaskCenter, TaskContext, TaskId, TaskKind};

use super::DrainReason;

enum Decision {
    Continue,
    NotifyPeerShutdown,
    Drain(DrainReason),
    DrainEgress,
    Drop,
}

#[derive(derive_more::IsVariant)]
enum State {
    Active,
    Draining { drain_timeout: Pin<Box<Sleep>> },
    WaitForEgress,
}

pub struct ConnectionReactor {
    state: State,
    connection: Connection,
    shared: super::Shared,
    context_propagator: TraceContextPropagator,
    seen_versions: Option<MetadataVersions>,
    router: Arc<MessageRouter>,
}

impl ConnectionReactor {
    #[must_use]
    pub fn new(
        connection: Connection,
        shared: super::Shared,
        peer_metadata: Option<PeerMetadataVersion>,
        router: Arc<MessageRouter>,
    ) -> Self {
        let context_propagator = TraceContextPropagator::default();
        let mut seen_versions = MetadataVersions::new(Metadata::current());
        if let Some(peer_metadata) = peer_metadata {
            seen_versions.notify(peer_metadata, &connection);
        }
        Self {
            state: State::Active,
            connection,
            shared,
            context_propagator,
            seen_versions: Some(seen_versions),
            router,
        }
    }

    pub fn start<S>(
        self,
        task_kind: TaskKind,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        is_dedicated: bool,
        incoming: S,
    ) -> Result<TaskId, ShutdownError>
    where
        S: Stream<Item = Message> + Unpin + Send + 'static,
    {
        let span = tracing::error_span!(parent: None, "network-reactor",
            task_id = tracing::field::Empty,
            peer = %self.connection.peer(),
        );

        TaskCenter::spawn(
            task_kind,
            "network-connection-reactor",
            self.run_reactor(incoming, conn_tracker, is_dedicated)
                .instrument(span),
        )
    }

    fn send_drain_signal(&self, reason: DrainReason) {
        if let Some(tx) = &self.shared.tx {
            tx.unbounded_drain(reason);
        }
    }

    fn notify_metadata_versions(&mut self, header: &Header) {
        if let Some(seen) = self.seen_versions.as_mut() {
            seen.notify(PeerMetadataVersion::from_header(header), &self.connection);
        }
    }

    fn switch_to_draining(&mut self, reason: DrainReason, conn_track: &impl ConnectionTracking) {
        trace!("Connection is draining");
        self.send_drain_signal(reason);
        self.seen_versions = None;
        self.state = State::Draining {
            drain_timeout: Box::pin(tokio::time::sleep(Duration::from_secs(15))),
        };
        conn_track.connection_draining(&self.connection);
    }

    pub async fn run_reactor<S>(
        mut self,
        mut incoming: S,
        conn_tracker: impl ConnectionTracking + Sync + Send + 'static,
        is_dedicated: bool,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = Message> + Unpin + Send,
    {
        let current_task = TaskContext::current();
        Span::current().record("task_id", tracing::field::display(current_task.id()));
        let mut cancellation = std::pin::pin!(current_task.cancellation_token().cancelled());

        conn_tracker.connection_created(&self.connection, is_dedicated);

        loop {
            let decision = match self.state {
                State::Active => {
                    // read a message from the stream
                    tokio::select! {
                        biased;
                        () = &mut cancellation => {
                            if TaskCenter::is_shutdown_requested() {
                                // We want to make the distinction between whether we are terminating the
                                // connection, or whether the node is shutting down.
                                Decision::Drain(DrainReason::Shutdown)
                            } else {
                                Decision::Drain(DrainReason::ConnectionDrain)
                            }
                            // we only drain the connection if we were the initiators of the termination
                        },
                        msg = incoming.next() => {
                            self.handle_message(msg).await
                        }
                    }
                }
                State::Draining {
                    ref mut drain_timeout,
                } => {
                    tokio::select! {
                        () = drain_timeout => {
                            debug!("Drain timed out, closing connection");
                            Decision::Drop
                        },
                        msg = incoming.next() => {
                            self.handle_message(msg).await
                        }
                    }
                }
                State::WaitForEgress => {
                    self.shared.tx.take();
                    if tokio::time::timeout(
                        Duration::from_secs(5),
                        OptionFuture::from(self.shared.drop_egress.take())
                            .log_slow_after(
                                Duration::from_secs(2),
                                tracing::Level::INFO,
                                "Waiting for connection's egress to drain",
                            )
                            .with_overdue(Duration::from_secs(3), tracing::Level::WARN),
                    )
                    .await
                    .is_err()
                    {
                        info!("Connection's egress has taken too long to drain, will drop");
                    }
                    Decision::Drop
                }
            };

            match (&self.state, decision) {
                (_, Decision::Continue) => {}
                (State::Active, Decision::Drain(reason)) => {
                    // send drain signal, and switch
                    self.switch_to_draining(reason, &conn_tracker);
                }
                (State::Active, Decision::NotifyPeerShutdown) => {
                    conn_tracker.notify_peer_shutdown(self.connection.peer());
                    self.switch_to_draining(DrainReason::ConnectionDrain, &conn_tracker);
                }
                (_, Decision::DrainEgress) => {
                    self.shared.tx.take();
                    self.state = State::WaitForEgress;
                }
                (State::Draining { .. }, Decision::NotifyPeerShutdown) => {
                    conn_tracker.notify_peer_shutdown(self.connection.peer());
                }
                (State::Draining { .. }, Decision::Drain(reason)) => {
                    // send drain signal, and switch
                    self.send_drain_signal(reason);
                }
                (_, Decision::Drop) => break,
                (State::WaitForEgress, Decision::NotifyPeerShutdown | Decision::Drain(_)) => {
                    unreachable!()
                }
            }
        }

        conn_tracker.connection_dropped(&self.connection);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_message(&mut self, msg: Option<Message>) -> Decision {
        let msg = match msg {
            Some(msg) => msg,
            None if self.state.is_active() => {
                return Decision::Drain(DrainReason::ConnectionDrain);
            }
            None => {
                // Peer has terminated the connection stream cleanly.
                // no more messages will arrive on this stream.
                return Decision::DrainEgress;
            }
        };

        // body are not allowed to be empty.
        let Some(body) = msg.body else {
            return Decision::Drain(DrainReason::CodecError(
                "Body is missing on message".to_owned(),
            ));
        };

        //  header is required on all messages
        let Some(header) = msg.header else {
            warn!("Peer sent a message without header");
            return Decision::Drop;
        };

        self.notify_metadata_versions(&header);

        match body {
            Body::ConnectionControl(ctrl_msg) => {
                debug!(
                    "Received control signal from peer {}. {} {}",
                    self.connection.peer(),
                    ctrl_msg.signal(),
                    ctrl_msg.message,
                );
                match ctrl_msg.signal() {
                    Signal::Shutdown => Decision::NotifyPeerShutdown,
                    Signal::RequestStreamDrained => {
                        // No more requests coming, but rpc responses might still arrive.
                        // We don't need tx anymore. We'll not create future responder
                        // tasks.
                        self.shared.tx.take();
                        Decision::Drain(DrainReason::ConnectionDrain)
                    }
                    Signal::ResponseStreamDrained => {
                        // No more responses coming, but rpc requests might still arrive.
                        // Peer will terminate its sender stream once if drained its two
                        // egress streams.
                        Decision::Drain(DrainReason::ConnectionDrain)
                    }
                    Signal::DrainConnection => Decision::Drain(DrainReason::ConnectionDrain),
                    Signal::CodecError => Decision::Drop,
                    Signal::Unknown => Decision::Continue,
                }
            }
            // Welcome and hello are not allowed after handshake
            Body::Welcome(_) | Body::Hello(_) => {
                warn!("Peer sent a welcome/hello message after handshake, terminating connection");
                Decision::Drop
            }

            Body::Datagram(Datagram { datagram: None }) => {
                // Wrong, we'll ignore.
                Decision::Continue
            }

            // RPC CALL
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::RpcCall(rpc_call)),
            }) => {
                let Some(tx) = self.shared.tx.as_ref() else {
                    // egress for responses has been drained
                    return Decision::Continue;
                };
                let target_service = rpc_call.service();
                let parent_context = header
                    .span_context
                    .as_ref()
                    .map(|span_ctx| self.context_propagator.extract(span_ctx));

                let encoded_len = rpc_call.payload.len();
                let (reply_port, reply_rx) = RpcReplyPort::new();
                let raw_rpc = RawRpc {
                    reply_port,
                    payload: rpc_call.payload,
                    sort_code: rpc_call.sort_code,
                    msg_type: rpc_call.msg_type,
                };
                let incoming = Incoming::new(
                    self.connection.protocol_version,
                    raw_rpc,
                    self.connection.peer,
                    PeerMetadataVersion::from(header),
                    parent_context,
                );
                trace!(
                    peer = %self.connection.peer(),
                    rpc_id = %rpc_call.id,
                    "Received RPC call: {target_service}::{}",
                    incoming.msg_type()
                );
                // ship to the service router, dropping the reply port will close the responder
                // task.
                match tokio::task::unconstrained(self.router.call_rpc(target_service, incoming))
                    .await
                {
                    Ok(()) => { /* spawn reply task */ }
                    Err(err) => {
                        send_rpc_error(tx, err, rpc_call.id);
                    }
                }

                counter!(NETWORK_MESSAGE_RECEIVED_BYTES, "target" => target_service.as_str_name())
                    .increment(encoded_len as u64);

                spawn_rpc_responder(tx.clone(), rpc_call.id, reply_rx, target_service);

                Decision::Continue
            }
            // UNARY MESSAGE
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::Unary(unary)),
            }) => {
                let parent_context = header
                    .span_context
                    .as_ref()
                    .map(|span_ctx| self.context_propagator.extract(span_ctx));
                let metadata_versions = PeerMetadataVersion::from(header);
                let target = unary.service();
                let encoded_len = unary.payload.len();
                let incoming = Incoming::new(
                    self.connection.protocol_version,
                    RawUnary {
                        payload: unary.payload,
                        sort_code: unary.sort_code,
                        msg_type: unary.msg_type,
                    },
                    self.connection.peer(),
                    metadata_versions,
                    parent_context,
                );
                trace!("Received Unary call: {target}::{}", incoming.msg_type());

                let _ = tokio::task::unconstrained(self.router.call_unary(target, incoming)).await;

                counter!(NETWORK_MESSAGE_RECEIVED_BYTES, "target" => target.as_str_name())
                    .increment(encoded_len as u64);
                Decision::Continue
            }
            // RPC REPLY
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::RpcReply(msg)),
            }) => {
                if let Some(reply_sender) = self.shared.reply_tracker.pop_rpc_sender(&msg.id) {
                    // validate the input. If no body was set, then we report "unknown" error.
                    let _ = match msg.body {
                        Some(rpc_reply::Body::Status(status)) => {
                            let status = RpcReplyError::from(status);
                            trace!(rpc_id = %msg.id, "Received RPC response with status {status}!");
                            reply_sender.send(crate::network::RawRpcReply::Error(status))
                        }
                        Some(rpc_reply::Body::Payload(payload)) => {
                            trace!(rpc_id = %msg.id, "Received RPC response with payload!");
                            reply_sender.send(crate::network::RawRpcReply::Success((
                                self.connection.protocol_version,
                                payload,
                            )))
                        }
                        None => {
                            warn!(
                                "Received RPC response for message {} with empty body!",
                                msg.id
                            );
                            reply_sender.send(crate::network::RawRpcReply::Error(
                                RpcReplyError::Unknown(0),
                            ))
                        }
                    };
                } else {
                    trace!(rpc_id = %msg.id, "Received RPC response for unknown message!");
                }
                Decision::Continue
            }
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::Watch(_watch)),
            }) => {
                // watch request
                todo!()
            }
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::WatchUpdate(_msg)),
            }) => {
                // watch message
                todo!()
            }
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::Ping(msg)),
            }) => {
                if let Some(tx) = self.shared.tx.as_ref() {
                    let datagram = Body::Datagram(Datagram {
                        datagram: Some(msg.flip().into()),
                    });
                    let _ = tx.unbounded_send(EgressMessage::Message(datagram, None));
                }
                Decision::Continue
            }
            Body::Datagram(Datagram {
                datagram: Some(datagram::Datagram::Pong(_msg)),
            }) => {
                // watch message
                // TODO: handle pong messages
                Decision::Continue
            }
        }
    }
}

fn send_rpc_error(tx: &super::UnboundedEgressSender, err: RouterError, id: u64) {
    let body = RpcReply {
        id,
        body: Some(rpc_reply::Body::Status(rpc_reply::Status::from(err) as i32)),
    };

    let datagram = Body::Datagram(Datagram {
        datagram: Some(body.into()),
    });

    let _ = tx.unbounded_send(EgressMessage::Message(datagram, None));
}

/// A task to ship the reply or an error back to the caller
fn spawn_rpc_responder(
    tx: super::UnboundedEgressSender,
    id: u64,
    reply_rx: oneshot::Receiver<ReplyEnvelope>,
    _target_service: ServiceTag,
) {
    // this is rpc-call, spawning a responder task
    tokio::spawn(async move {
        tokio::select! {
            reply = reply_rx => {
                match reply {
                    Ok(envelope) => {
                        trace!(rpc_id = %id, "Sending RPC response to caller");
                        let body = RpcReply { id, body: Some(envelope.body) };
                        let datagram = Body::Datagram(Datagram { datagram: Some(body.into())});
                        let _ = tx.unbounded_send(EgressMessage::Message(
                            datagram,
                            Some(envelope.span),
                        ));
                        // todo(asoli): here is a good place to measure total rpc
                        // processing time.
                    }
                    // reply_port was closed, we'll not respond.
                    Err(_) => {
                        trace!(rpc_id = %id, "RPC was dropped, sending dropped notification to caller");
                        let body = RpcReply { id, body: Some(rpc_reply::Body::Status(rpc_reply::Status::Dropped.into())), };
                        let datagram = Body::Datagram(Datagram { datagram: Some(body.into())});
                        let _ = tx.unbounded_send(EgressMessage::Message(
                            datagram,
                            None,
                        ));
                    }
                }
            }
            () = tx.closed() => {
                // connection was dropped. Nothing to be done here.
                trace!(rpc_id = %id, "Connection was dropped, dropping RPC responder task");
            }
        }
    });
}

#[derive(derive_more::Index, derive_more::IndexMut)]
pub struct MetadataVersions {
    #[index_mut]
    #[index]
    versions: EnumMap<MetadataKind, Version>,
    metadata: Metadata,
    nodes_config: Live<NodesConfiguration>,
    schema: Live<Schema>,
    partition_table: Live<PartitionTable>,
    logs_metadata: Live<Logs>,
}

impl MetadataVersions {
    fn new(metadata: Metadata) -> Self {
        let nodes_config = metadata.updateable_nodes_config();
        let schema = metadata.updateable_schema();
        let partition_table = metadata.updateable_partition_table();
        let logs_metadata = metadata.updateable_logs_metadata();

        let versions = enum_map! {
            MetadataKind::NodesConfiguration => Version::INVALID,
            MetadataKind::Schema => Version::INVALID,
            MetadataKind::PartitionTable => Version::INVALID,
            MetadataKind::Logs => Version::INVALID,
        };

        Self {
            versions,
            metadata,
            nodes_config,
            schema,
            partition_table,
            logs_metadata,
        }
    }

    fn notify(&mut self, peer: PeerMetadataVersion, connection: &Connection) {
        for kind in MetadataKind::iter() {
            self.update(kind, peer.get(kind), connection);
        }
    }

    fn get_latest_version(&mut self, kind: MetadataKind) -> Version {
        match kind {
            MetadataKind::NodesConfiguration => self.nodes_config.live_load().version(),
            MetadataKind::Schema => self.schema.live_load().version(),
            MetadataKind::PartitionTable => self.partition_table.live_load().version(),
            MetadataKind::Logs => self.logs_metadata.live_load().version(),
        }
    }

    fn update(&mut self, metadata_kind: MetadataKind, version: Version, connection: &Connection) {
        let current_version = self.versions[metadata_kind];
        if version > current_version {
            self.versions[metadata_kind] = version;
            if version > self.get_latest_version(metadata_kind) {
                self.metadata.notify_observed_version(
                    metadata_kind,
                    version,
                    Some(connection.clone()),
                );
            }
        }
    }
}
