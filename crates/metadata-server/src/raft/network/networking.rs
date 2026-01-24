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
use std::collections::hash_map::Entry;

use bytes::{Buf, BufMut, BytesMut};
use futures::FutureExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::IntoStreamingRequest;
use tonic::metadata::MetadataValue;
use tracing::trace;

use restate_core::network::net_util;
use restate_core::{ShutdownError, TaskCenter, TaskHandle, TaskKind};
use restate_types::PlainNodeId;
use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::net::address::{AdvertisedAddress, FabricPort};

use crate::raft::network::connection_manager::ConnectionManager;
use crate::raft::network::grpc_svc::new_metadata_server_network_client;
use crate::raft::network::{
    CLUSTER_FINGERPRINT_METADATA_KEY, CLUSTER_NAME_METADATA_KEY, PEER_METADATA_KEY, grpc_svc,
};

#[derive(Debug, thiserror::Error)]
pub enum TrySendError<T> {
    #[error("failed sending message")]
    Send(T),
    #[error("connecting to peer")]
    Connecting(T),
    #[error("unknown peer: {0}")]
    UnknownPeer(PlainNodeId, T),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl<T> TrySendError<T> {
    pub fn into_message(self) -> Option<T> {
        match self {
            TrySendError::Send(message) => Some(message),
            TrySendError::Connecting(message) => Some(message),
            TrySendError::UnknownPeer(_, message) => Some(message),
            TrySendError::Shutdown(_) => None,
        }
    }
}

#[derive(derive_more::Debug)]
pub struct Networking<M> {
    connection_manager: ConnectionManager<M>,
    addresses: HashMap<PlainNodeId, AdvertisedAddress<FabricPort>>,
    #[debug(skip)]
    connection_attempts: HashMap<PlainNodeId, TaskHandle<anyhow::Result<()>>>,
    serde_buffer: BytesMut,
}

impl<M> Networking<M>
where
    M: NetworkMessage + Clone + Send + 'static,
{
    pub fn new(connection_manager: ConnectionManager<M>) -> Self {
        Networking {
            connection_manager,
            addresses: HashMap::default(),
            connection_attempts: HashMap::default(),
            serde_buffer: BytesMut::with_capacity(1024),
        }
    }

    /// Makes the given address for the given peer known. Returns true if the address is new.
    pub fn register_address(
        &mut self,
        peer: PlainNodeId,
        address: AdvertisedAddress<FabricPort>,
    ) -> bool {
        match self.addresses.entry(peer) {
            Entry::Occupied(mut entry) => {
                if *entry.get() != address {
                    entry.insert(address);
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(address);
                true
            }
        }
    }

    pub fn try_send(&mut self, message: M) -> Result<(), TrySendError<M>> {
        let target = message.to();
        if let Some(connection) = self.connection_manager.get_connection(target) {
            message.serialize(&mut self.serde_buffer);

            // todo: Maybe send message directly w/o indirection through NetworkMessage
            let network_message = grpc_svc::NetworkMessage {
                payload: self.serde_buffer.split().freeze(),
            };

            connection
                .try_send(network_message)
                .map_err(|_err| TrySendError::Send(message))?;
        } else if let Some(address) = self.addresses.get(&target) {
            if let Entry::Occupied(occupied) = self.connection_attempts.entry(target) {
                if !occupied.get().is_finished() {
                    return Err(TrySendError::Connecting(message));
                } else {
                    match occupied.remove().now_or_never() {
                        None => {
                            trace!(
                                "Previous connection attempt to '{target}' finished. Polling the final \
                                result failed because we most likely depleted our cooperative task budget."
                            );
                        }
                        Some(Ok(result)) => match result {
                            Ok(_) => trace!(
                                "Previous connection attempt to '{target}' succeeded but connection was closed in meantime."
                            ),
                            Err(err) => {
                                trace!("Previous connection attempt to '{target}' failed: {}", err)
                            }
                        },
                        Some(Err(_)) => {
                            trace!(
                                "Previous connection attempt to '{target}' panicked. The panic is swallowed by the TaskHandle abstraction.",
                            )
                        }
                    }
                }
            }

            self.connection_attempts.insert(
                target,
                Self::try_connecting_to(
                    self.connection_manager.clone(),
                    target,
                    address.clone(),
                    &Configuration::pinned().networking,
                )?,
            );
            return Err(TrySendError::Connecting(message));
        } else {
            return Err(TrySendError::UnknownPeer(target, message));
        }

        Ok(())
    }

    fn try_connecting_to(
        connection_manager: ConnectionManager<M>,
        target: PlainNodeId,
        address: AdvertisedAddress<FabricPort>,
        networking_options: &NetworkingOptions,
    ) -> Result<TaskHandle<anyhow::Result<()>>, ShutdownError> {
        TaskCenter::spawn_unmanaged_child(
            TaskKind::SocketHandler,
            "metadata-store-network-connection-attempt",
            {
                trace!(%target, "Try connecting to metadata store peer");
                let channel = net_util::create_tonic_channel(
                    address.clone(),
                    networking_options,
                    net_util::DNSResolution::Gai,
                );

                async move {
                    let mut network_client = new_metadata_server_network_client(channel);
                    let (outgoing_tx, outgoing_rx) = mpsc::channel(128);

                    let mut request = ReceiverStream::new(outgoing_rx).into_streaming_request();
                    // send our identity alongside with the request to the target
                    request.metadata_mut().insert(
                        PEER_METADATA_KEY,
                        MetadataValue::try_from(connection_manager.identity().to_string())?,
                    );
                    // send our cluster fingerprint for validation
                    request.metadata_mut().insert(
                        CLUSTER_FINGERPRINT_METADATA_KEY,
                        MetadataValue::try_from(
                            connection_manager
                                .cluster_fingerprint()
                                .to_u64()
                                .to_string(),
                        )?,
                    );
                    // send our cluster name for validation
                    request.metadata_mut().insert(
                        CLUSTER_NAME_METADATA_KEY,
                        MetadataValue::try_from(connection_manager.cluster_name())?,
                    );

                    let incoming_rx = network_client.connect_to(request).await?;

                    connection_manager.run_connection(
                        target,
                        outgoing_tx,
                        incoming_rx.into_inner(),
                    )?;

                    Ok(())
                }
            },
        )
    }
}

/// A message that can be sent over the network
pub trait NetworkMessage {
    /// The target of the message
    fn to(&self) -> PlainNodeId;

    /// Serialize the message into the buffer
    fn serialize<B: BufMut>(&self, buffer: &mut B);

    /// Deserialize the message from the bytes
    fn deserialize<B: Buf>(buffer: &mut B) -> anyhow::Result<Self>
    where
        Self: Sized;
}
