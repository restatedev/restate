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

pub use server::RaftMetadataServer;

use anyhow::Context;
use bytes::{Buf, BufMut};
use network::NetworkMessage;
use protobuf::Message as ProtobufMessage;

use restate_types::{PlainNodeId, Version};

use crate::MemberId;

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
