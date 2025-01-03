// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{network, MetadataStoreRunner, Request};
use bytes::{Buf, BufMut};
use omnipaxos::messages::Message;
use restate_core::network::NetworkServerBuilder;
use restate_rocksdb::RocksError;
use restate_types::config::{OmniPaxosOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::{decode_from_flexbuffers, encode_as_flexbuffers};
use tokio::sync::mpsc;

use crate::network::{
    ConnectionManager, MetadataStoreNetworkHandler, MetadataStoreNetworkSvcServer, NetworkMessage,
    Networking,
};
use crate::omnipaxos::store::OmnipaxosMetadataStore;

mod storage;
mod store;

type OmniPaxosMessage = Message<Request>;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed building OmniPaxos: {0}")]
    OmniPaxos(#[from] omnipaxos::errors::ConfigError),
    #[error("failed opening RocksDb: {0}")]
    OpeningRocksDb(#[from] RocksError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {}

pub(crate) async fn create_store(
    omnipaxos_options: &OmniPaxosOptions,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    health_status: HealthStatus<MetadataServerStatus>,
    server_builder: &mut NetworkServerBuilder,
) -> Result<MetadataStoreRunner<OmnipaxosMetadataStore>, BuildError> {
    let (router_tx, router_rx) = mpsc::channel(128);
    let connection_manager = ConnectionManager::new(omnipaxos_options.id.get(), router_tx);
    let store = OmnipaxosMetadataStore::create(
        omnipaxos_options,
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

impl NetworkMessage for OmniPaxosMessage {
    fn to(&self) -> u64 {
        self.get_receiver()
    }

    fn serialize<B: BufMut>(&self, buffer: &mut B) {
        encode_as_flexbuffers(self, buffer).expect("serialization should not fail");
    }

    fn deserialize<B: Buf>(buffer: &mut B) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        decode_from_flexbuffers(buffer).map_err(Into::into)
    }
}
