// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::network::{MetadataStoreNetworkHandler, MetadataStoreNetworkSvcServer, NetworkMessage};
use crate::omnipaxos::store::OmniPaxosMetadataStore;
use crate::{network, MemberId, MetadataStoreRunner, Request, StorageId};
use bytes::{Buf, BufMut};
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use omnipaxos::ClusterConfig;
use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataWriter, ShutdownError};
use restate_rocksdb::RocksError;
use restate_types::config::RocksDbOptions;
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataStoreStatus;
use restate_types::storage::{decode_from_flexbuffers, encode_as_flexbuffers};
use std::collections::HashMap;

mod storage;
mod store;

type OmniPaxosMessage = Message<Request>;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed building OmniPaxos: {0}")]
    OmniPaxos(#[from] omnipaxos::errors::ConfigError),
    #[error("failed opening RocksDb: {0}")]
    OpeningRocksDb(#[from] RocksError),
    #[error("failed initializing the storage: {0}")]
    InitStorage(String),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct OmniPaxosConfiguration {
    own_member_id: MemberId,
    cluster_config: ClusterConfig,
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    members: HashMap<NodeId, StorageId>,
}

pub(crate) async fn create_store(
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    health_status: HealthStatus<MetadataStoreStatus>,
    metadata_writer: Option<MetadataWriter>,
    server_builder: &mut NetworkServerBuilder,
) -> Result<MetadataStoreRunner<OmniPaxosMetadataStore>, BuildError> {
    let store =
        OmniPaxosMetadataStore::create(rocksdb_options, metadata_writer, health_status).await?;

    server_builder.register_grpc_service(
        MetadataStoreNetworkSvcServer::new(MetadataStoreNetworkHandler::new(
            store.connection_manager(),
            Some(store.join_cluster_handle()),
        )),
        network::FILE_DESCRIPTOR_SET,
    );

    Ok(MetadataStoreRunner::new(store, server_builder))
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
