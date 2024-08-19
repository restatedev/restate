// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::raft::connection_manager::ConnectionManager;
use crate::raft::grpc_svc::RaftMessage;
use crate::raft::handler::RAFT_PEER_METADATA_KEY;
use bytes::{BufMut, BytesMut};
use futures::FutureExt;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use restate_core::network::net_util;
use restate_core::TaskCenter;
use restate_types::errors::GenericError;
use restate_types::net::AdvertisedAddress;
use std::collections::HashMap;
use std::mem;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::IntoStreamingRequest;
use tracing::{debug, trace};

#[derive(Debug, thiserror::Error)]
pub enum TrySendError<T> {
    #[error("failed sending message")]
    Send(T),
    #[error("failed encoding message")]
    Encode(T, GenericError),
    #[error("unknown peer: {0}")]
    UnknownPeer(u64),
}

#[derive(derive_more::Debug)]
pub struct Networking {
    connection_manager: ConnectionManager,
    addresses: HashMap<u64, AdvertisedAddress>,
    connection_attempts: HashMap<u64, JoinHandle<anyhow::Result<()>>>,
    serde_buffer: BytesMut,
    #[debug(skip)]
    task_center: TaskCenter,
}

impl Networking {
    pub fn new(task_center: TaskCenter, connection_manager: ConnectionManager) -> Self {
        Networking {
            connection_manager,
            addresses: HashMap::default(),
            connection_attempts: HashMap::default(),
            serde_buffer: BytesMut::with_capacity(1024),
            task_center,
        }
    }

    pub fn register_address(&mut self, peer: u64, address: AdvertisedAddress) {
        self.addresses.insert(peer, address);
    }

    pub fn try_send(&mut self, message: Message) -> Result<(), TrySendError<Message>> {
        let target = message.to;

        if let Some(connection) = self.connection_manager.get_connection(target) {
            let mut writer = mem::take(&mut self.serde_buffer).writer();
            message
                .write_to_writer(&mut writer)
                .map_err(|err| TrySendError::Encode(message.clone(), err.into()))?;
            self.serde_buffer = writer.into_inner();

            // todo: Maybe send message directly w/o indirection through RaftMessage
            let raft_message = RaftMessage {
                message: self.serde_buffer.split().freeze(),
            };

            connection
                .try_send(raft_message)
                .map_err(|_err| TrySendError::Send(message))?;
        } else if let Some(address) = self.addresses.get(&target) {
            if let Some(join_handle) = self.connection_attempts.remove(&target) {
                if !join_handle.is_finished() {
                    return Ok(());
                } else {
                    match join_handle.now_or_never().expect("should be finished") {
                        Ok(result) => {
                            match result {
                                Ok(_) => trace!("Previous connection attempt to '{target}' succeeded but connection was closed in meantime."),
                                Err(err) => trace!("Previous connection attempt to '{target}' failed: {}", err)
                            }

                        }
                        Err(err) => {
                            trace!("Previous connection attempt to '{target}' panicked: {}", err)
                        }
                    }
                }
            }

            self.connection_attempts.insert(
                target,
                Self::try_connecting_to(
                    self.task_center.clone(),
                    self.connection_manager.clone(),
                    target,
                    address.clone(),
                ),
            );
        } else {
            return Err(TrySendError::UnknownPeer(target));
        }

        Ok(())
    }

    fn try_connecting_to(
        task_center: TaskCenter,
        connection_manager: ConnectionManager,
        target: u64,
        address: AdvertisedAddress,
    ) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            task_center.run_in_scope("raft-connection-attempt", None, async move {
                trace!(%target, "Try connecting to raft peer");
                let channel =
                    net_util::create_tonic_channel_from_advertised_address(address.clone())?;
                let mut raft_client = crate::raft::grpc_svc::raft_metadata_store_svc_client::RaftMetadataStoreSvcClient::new(channel);
                let (outgoing_tx, outgoing_rx) = mpsc::channel(128);

                let mut request = ReceiverStream::new(outgoing_rx).into_streaming_request();
                // send our identity alongside with the request to the target
                request.metadata_mut().insert(RAFT_PEER_METADATA_KEY, MetadataValue::try_from(connection_manager.identity().to_string())?);
                let incoming_rx = raft_client.raft(request).await?;

                connection_manager.run_connection(target, outgoing_tx, incoming_rx.into_inner())?;

                Ok(())
            }).await
        })
    }
}
