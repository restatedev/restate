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
use std::time::Duration;

use enum_map::{EnumMap, enum_map};
use futures::future::OptionFuture;
use futures::{Stream, StreamExt};
use metrics::{counter, histogram};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use prost::Message as _;
use tokio::time::{Instant, Sleep};
use tracing::{Instrument, Span, debug, info, trace, warn};

use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;
use restate_types::{Version, Versioned};

use crate::metadata::Urgency;
use crate::network::metric_definitions::{
    NETWORK_MESSAGE_PROCESSING_DURATION, NETWORK_MESSAGE_RECEIVED, NETWORK_MESSAGE_RECEIVED_BYTES,
};
use crate::network::protobuf::network::message::{Body, Signal};
use crate::network::protobuf::network::{Header, Message};
use crate::network::tracking::{ConnectionTracking, PeerRouting};
use crate::network::{Connection, Handler, Incoming, PeerMetadataVersion};
use crate::{Metadata, ShutdownError, TaskCenter, TaskContext, TaskId, TaskKind};

use super::{DrainReason, DropEgressStream, UnboundedEgressSender};

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
    tx: Option<UnboundedEgressSender>,
    drop_egress: Option<DropEgressStream>,
    context_propagator: TraceContextPropagator,
    seen_versions: Option<MetadataVersions>,
}

impl ConnectionReactor {
    pub fn new(
        connection: Connection,
        tx: UnboundedEgressSender,
        drop_egress: DropEgressStream,
    ) -> Self {
        let context_propagator = TraceContextPropagator::default();
        Self {
            state: State::Active,
            connection,
            tx: Some(tx),
            drop_egress: Some(drop_egress),
            context_propagator,
            seen_versions: Default::default(),
        }
    }

    pub fn start<S>(
        self,
        task_kind: TaskKind,
        router: impl Handler + Sync + 'static,
        conn_tracker: impl ConnectionTracking + Send + Sync + 'static,
        peer_router: impl PeerRouting + Clone + Send + Sync + 'static,
        incoming: S,
        should_register: bool,
    ) -> Result<TaskId, ShutdownError>
    where
        S: Stream<Item = Message> + Unpin + Send + 'static,
    {
        let span = tracing::error_span!(parent: None, "network-reactor",
            task_id = tracing::field::Empty,
            peer = %self.connection.peer(),
        );

        if should_register {
            peer_router.register(&self.connection);
        }
        let connection = self.connection.clone();
        let peer_router_cloned = peer_router.clone();

        match TaskCenter::spawn(
            task_kind,
            "network-connection-reactor",
            self.run_reactor(router, incoming, conn_tracker, peer_router)
                .instrument(span),
        ) {
            Ok(task) => Ok(task),
            Err(e) => {
                // make sure we deregister if we failed to spawn
                peer_router_cloned.deregister(&connection);
                Err(e)
            }
        }
    }

    fn send_drain_signal(&self, reason: DrainReason) {
        if let Some(tx) = &self.tx {
            tx.unbounded_drain(reason)
        }
    }

    fn notify_metadata_versions(&mut self, header: &Header) {
        if let Some(seen) = self.seen_versions.as_mut() {
            seen.notify(header, &self.connection);
        }
    }

    fn switch_to_draining(&mut self, reason: DrainReason, peer_router: &impl PeerRouting) {
        trace!("Connection is draining");
        self.send_drain_signal(reason);
        self.seen_versions = None;
        self.state = State::Draining {
            drain_timeout: Box::pin(tokio::time::sleep(Duration::from_secs(15))),
        };
        peer_router.deregister(&self.connection);
    }

    pub async fn run_reactor<S>(
        mut self,
        router: impl Handler,
        mut incoming: S,
        conn_tracker: impl ConnectionTracking,
        peer_router: impl PeerRouting,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = Message> + Unpin + Send,
    {
        let current_task = TaskContext::current();
        let metadata = Metadata::current();
        self.seen_versions = Some(MetadataVersions::new(metadata));
        Span::current().record("task_id", tracing::field::display(current_task.id()));
        let mut cancellation = std::pin::pin!(current_task.cancellation_token().cancelled());

        conn_tracker.connection_created(&self.connection);

        loop {
            let decision = match self.state {
                State::Active => {
                    // read a message from the stream
                    tokio::select! {
                        biased;
                        _ = &mut cancellation => {
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
                            self.handle_message(msg, &router).await
                        }
                    }
                }
                State::Draining {
                    ref mut drain_timeout,
                } => {
                    tokio::select! {
                        _ = drain_timeout => {
                            debug!("Drain timed out, closing connection");
                            Decision::Drop
                        },
                        msg = incoming.next() => {
                            self.handle_message(msg, &router).await
                        }
                    }
                }
                State::WaitForEgress => {
                    self.tx.take();
                    if tokio::time::timeout(
                        Duration::from_secs(5),
                        OptionFuture::from(self.drop_egress.take())
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
                    self.switch_to_draining(reason, &peer_router);
                }
                (State::Active, Decision::NotifyPeerShutdown) => {
                    conn_tracker.notify_peer_shutdown(self.connection.peer());
                    self.switch_to_draining(DrainReason::ConnectionDrain, &peer_router);
                }
                (_, Decision::DrainEgress) => {
                    self.tx.take();
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
                (State::WaitForEgress, Decision::NotifyPeerShutdown) => unreachable!(),
                (State::WaitForEgress, Decision::Drain(_)) => unreachable!(),
            }
        }

        // we also try to deregister here because we don't always transition to "Draining" before
        // dropping
        peer_router.deregister(&self.connection);
        conn_tracker.connection_dropped(&self.connection);
        Ok(())
    }

    async fn handle_message(&mut self, msg: Option<Message>, router: &impl Handler) -> Decision {
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

        let processing_started = Instant::now();
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
                        self.tx.take();
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

            Body::Encoded(msg) => {
                let encoded_len = msg.encoded_len();
                let target = msg.target();

                let parent_context = header
                    .span_context
                    .as_ref()
                    .map(|span_ctx| self.context_propagator.extract(span_ctx));

                // unconstrained: We want to avoid yielding if the message router has capacity,
                // this is to improve tail latency of message processing. We still give tokio
                // a yielding point when reading the next message but it would be excessive to
                // introduce more than one yielding point in this reactor loop.
                if let Err(e) = tokio::task::unconstrained(
                    router.call(
                        Incoming::from_parts(
                            msg,
                            self.connection.clone(),
                            header.msg_id,
                            header.in_response_to,
                            PeerMetadataVersion::from(header),
                        )
                        .with_parent_context(parent_context),
                        self.connection.protocol_version,
                    ),
                )
                .await
                {
                    warn!(
                        target = target.as_str_name(),
                        "Error processing message: {e}"
                    );
                }
                histogram!(NETWORK_MESSAGE_PROCESSING_DURATION, "target" => target.as_str_name())
                    .record(processing_started.elapsed());
                counter!(NETWORK_MESSAGE_RECEIVED, "target" => target.as_str_name()).increment(1);
                counter!(NETWORK_MESSAGE_RECEIVED_BYTES, "target" => target.as_str_name())
                    .increment(encoded_len as u64);
                trace!(
                    target = target.as_str_name(),
                    "Processed message in {:?}",
                    processing_started.elapsed()
                );
                Decision::Continue
            }
        }
    }
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
        let mut nodes_config = metadata.updateable_nodes_config();
        let mut schema = metadata.updateable_schema();
        let mut partition_table = metadata.updateable_partition_table();
        let mut logs_metadata = metadata.updateable_logs_metadata();

        let versions = enum_map! {
            MetadataKind::NodesConfiguration => nodes_config.live_load().version(),
            MetadataKind::Schema => schema.live_load().version(),
            MetadataKind::PartitionTable => partition_table.live_load().version(),
            MetadataKind::Logs => logs_metadata.live_load().version(),
        };

        Self {
            metadata,
            versions,
            nodes_config,
            schema,
            partition_table,
            logs_metadata,
        }
    }

    pub fn notify(&mut self, header: &Header, connection: &Connection) {
        self.update(
            header.my_nodes_config_version.map(Into::into),
            header.my_partition_table_version.map(Into::into),
            header.my_schema_version.map(Into::into),
            header.my_logs_version.map(Into::into),
        )
        .into_iter()
        .for_each(|(kind, version)| {
            if let Some(version) = version {
                if version > self.get_latest_version(kind) {
                    self.metadata.notify_observed_version(
                        kind,
                        version,
                        Some(connection.clone()),
                        Urgency::Normal,
                    );
                }
                // todo: store the latest if it's higher
            }
        });
    }

    fn get_latest_version(&mut self, kind: MetadataKind) -> Version {
        match kind {
            MetadataKind::NodesConfiguration => self.nodes_config.live_load().version(),
            MetadataKind::Schema => self.schema.live_load().version(),
            MetadataKind::PartitionTable => self.partition_table.live_load().version(),
            MetadataKind::Logs => self.logs_metadata.live_load().version(),
        }
    }

    fn update(
        &mut self,
        nodes_config_version: Option<Version>,
        partition_table_version: Option<Version>,
        schema_version: Option<Version>,
        logs_version: Option<Version>,
    ) -> EnumMap<MetadataKind, Option<Version>> {
        let mut result = EnumMap::default();
        result[MetadataKind::NodesConfiguration] =
            self.update_internal(MetadataKind::NodesConfiguration, nodes_config_version);
        result[MetadataKind::PartitionTable] =
            self.update_internal(MetadataKind::PartitionTable, partition_table_version);
        result[MetadataKind::Schema] = self.update_internal(MetadataKind::Schema, schema_version);
        result[MetadataKind::Logs] = self.update_internal(MetadataKind::Logs, logs_version);

        result
    }

    fn update_internal(
        &mut self,
        metadata_kind: MetadataKind,
        version: Option<Version>,
    ) -> Option<Version> {
        if let Some(version) = version {
            if version > self.versions[metadata_kind] {
                self.versions[metadata_kind] = version;
                return Some(version);
            }
        }
        None
    }
}
