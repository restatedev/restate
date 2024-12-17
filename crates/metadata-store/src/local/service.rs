// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::health::HealthStatus;

use restate_core::network::NetworkServerBuilder;
use restate_core::ShutdownError;
use restate_rocksdb::RocksError;
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;

use crate::grpc_svc;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use crate::local::grpc::handler::LocalMetadataStoreHandler;
use crate::local::store::LocalMetadataStore;

pub struct LocalMetadataStoreService {
    health_status: HealthStatus<MetadataServerStatus>,
    store: LocalMetadataStore,
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("building local metadata store failed: {0}")]
    LocalMetadataStore(#[from] RocksError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("rocksdb error: {0}")]
    RocksDB(#[from] RocksError),
}

impl LocalMetadataStoreService {
    pub async fn create(
        health_status: HealthStatus<MetadataServerStatus>,
        options: &MetadataStoreOptions,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
        server_builder: &mut NetworkServerBuilder,
    ) -> Result<Self, BuildError> {
        let store = LocalMetadataStore::create(options, rocksdb_options).await?;

        server_builder.register_grpc_service(
            MetadataStoreSvcServer::new(LocalMetadataStoreHandler::new(store.request_sender())),
            grpc_svc::FILE_DESCRIPTOR_SET,
        );

        health_status.update(MetadataServerStatus::StartingUp);

        Ok(Self {
            health_status,
            store,
        })
    }

    pub async fn run(self) -> Result<(), Error> {
        let LocalMetadataStoreService {
            health_status,
            store,
        } = self;

        health_status.update(MetadataServerStatus::Ready);
        store.run().await;
        health_status.update(MetadataServerStatus::Unknown);

        Ok(())
    }
}
