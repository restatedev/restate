// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod kv_memory_storage;
mod network;
mod server;
mod storage;
#[cfg(test)]
mod tests;

use crate::MemberId;
use crate::grpc::client::GrpcMetadataServerClient;
use anyhow::Context;
use bytes::{Buf, BufMut};
use network::NetworkMessage;
use protobuf::Message as ProtobufMessage;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::net_util::CommonClientConnectionOptions;
use restate_types::net::AdvertisedAddress;
use restate_types::retries::RetryPolicy;
use restate_types::{PlainNodeId, Version};
pub use server::RaftMetadataServer;
use std::sync::Arc;

type StorageMarker = restate_types::storage::StorageMarker<String>;

impl NetworkMessage for raft::prelude::Message {
    fn to(&self) -> PlainNodeId {
        to_plain_node_id(self.to)
    }

    fn serialize<B: BufMut>(&self, buffer: &mut B) {
        let mut writer = buffer.writer();
        self.write_to_writer(&mut writer)
            .expect("should be able to write message");
    }

    fn deserialize<B: Buf>(buffer: &mut B) -> anyhow::Result<Self> {
        ProtobufMessage::parse_from_reader(&mut buffer.reader())
            .context("failed deserializing message")
    }
}

/// The current state of the RaftMetadataServer that is persisted to storage.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
enum RaftServerState {
    Member {
        my_member_id: MemberId,
        // field is introduced with 1.3.1 and therefore needs to be optional
        min_expected_nodes_config_version: Option<Version>,
    },
    #[default]
    Standby,
}

fn to_plain_node_id(id: u64) -> PlainNodeId {
    PlainNodeId::from(u32::try_from(id).expect("node id is derived from PlainNodeId"))
}

fn to_raft_id(plain_node_id: PlainNodeId) -> u64 {
    u64::from(u32::from(plain_node_id))
}

/// Creates the [`MetadataStoreClient`] for the replicated metadata server.
pub fn create_replicated_metadata_client(
    addresses: Vec<AdvertisedAddress>,
    backoff_policy: Option<RetryPolicy>,
    connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
) -> MetadataStoreClient {
    let inner_client = GrpcMetadataServerClient::new(addresses, connection_options);
    MetadataStoreClient::new(inner_client, backoff_policy)
}
