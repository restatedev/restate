// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::handler::MetadataStoreHandler;
use crate::grpc::server::GrpcServer;
use crate::grpc::service_builder::GrpcServiceBuilder;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use crate::network::{
    ConnectionManager, MetadataStoreNetworkHandler, MetadataStoreNetworkSvcServer, NetworkMessage,
    Networking,
};
use crate::omnipaxos::store::OmnipaxosMetadataStore;
use crate::omnipaxos::OmniPaxosMessage;
use crate::{grpc_svc, network, Error, MetadataStoreService};
use assert2::let_assert;
use bytes::{Buf, BufMut};
use futures::TryFutureExt;
use restate_core::{TaskCenter, TaskKind};
use restate_types::config::{Kind, MetadataStoreOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::storage::{decode_from_flexbuffers, encode_as_flexbuffers};
use tokio::sync::mpsc;

pub struct OmnipaxosMetadataStoreService {
    health_status: HealthStatus<MetadataServerStatus>,
    options: BoxedLiveLoad<MetadataStoreOptions>,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
}

impl OmnipaxosMetadataStoreService {
    pub fn new(
        health_status: HealthStatus<MetadataServerStatus>,
        options: BoxedLiveLoad<MetadataStoreOptions>,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> Self {
        Self {
            options,
            rocksdb_options,
            health_status,
        }
    }
}

#[async_trait::async_trait]
impl MetadataStoreService for OmnipaxosMetadataStoreService {
    async fn run(mut self) -> Result<(), Error> {
        let store_options = self.options.live_load();
        let_assert!(Kind::Omnipaxos(omnipaxos_options) = &store_options.kind);

        let (router_tx, router_rx) = mpsc::channel(128);
        let connection_manager = ConnectionManager::new(omnipaxos_options.id.into(), router_tx);
        let store = OmnipaxosMetadataStore::create(
            omnipaxos_options,
            self.rocksdb_options,
            Networking::new(connection_manager.clone()),
            router_rx,
        )
        .await
        .map_err(Error::generic)?;

        let mut builder = GrpcServiceBuilder::default();

        builder.register_file_descriptor_set_for_reflection(grpc_svc::FILE_DESCRIPTOR_SET);
        builder.add_service(MetadataStoreSvcServer::new(MetadataStoreHandler::new(
            store.request_sender(),
        )));

        builder.register_file_descriptor_set_for_reflection(network::FILE_DESCRIPTOR_SET);
        builder.add_service(MetadataStoreNetworkSvcServer::new(
            MetadataStoreNetworkHandler::new(connection_manager),
        ));

        let grpc_server =
            GrpcServer::new(store_options.bind_address.clone(), builder.build().await?);

        TaskCenter::spawn_child(
            TaskKind::RpcServer,
            "omnipaxos-metadata-store-grpc",
            grpc_server.run(self.health_status).map_err(Into::into),
        )?;

        store.run().await.map_err(Error::generic)?;

        Ok(())
    }
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
