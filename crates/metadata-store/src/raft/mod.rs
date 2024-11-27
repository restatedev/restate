// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod storage;
mod store;

use crate::network::{
    ConnectionManager, Message, MetadataStoreNetworkHandler, MetadataStoreNetworkSvcServer,
    Networking,
};
use crate::raft::store::BuildError;
use crate::{network, MetadataStoreRunner};
use anyhow::Context;
use bytes::{BufMut, Bytes};
use protobuf::Message as ProtobufMessage;
use restate_core::network::NetworkServerBuilder;
use restate_types::config::{RaftOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;
pub use store::RaftMetadataStore;
use tokio::sync::mpsc;

pub async fn create_store(
    raft_options: &RaftOptions,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    health_status: HealthStatus<MetadataServerStatus>,
    server_builder: &mut NetworkServerBuilder,
) -> Result<MetadataStoreRunner<RaftMetadataStore>, BuildError> {
    let (router_tx, router_rx) = mpsc::channel(128);
    let connection_manager = ConnectionManager::new(raft_options.id, router_tx);
    let store = RaftMetadataStore::create(
        raft_options,
        rocksdb_options,
        Networking::new(connection_manager.clone()),
        router_rx,
    )
    .await?;

    server_builder.register_grpc_service(
        MetadataStoreNetworkSvcServer::new(MetadataStoreNetworkHandler::new(connection_manager)),
        network::FILE_DESCRIPTOR_SET,
    );

    Ok(MetadataStoreRunner::new(
        store,
        health_status,
        server_builder,
    ))
}

impl Message for raft::prelude::Message {
    fn to(&self) -> u64 {
        self.to
    }

    fn serialize(&self, buffer: impl BufMut) {
        let mut writer = buffer.writer();
        self.write_to_writer(&mut writer)
            .expect("should be able to write message");
    }

    fn deserialize(bytes: &Bytes) -> anyhow::Result<Self> {
        ProtobufMessage::parse_from_carllerche_bytes(bytes).context("failed deserializing message")
    }
}
