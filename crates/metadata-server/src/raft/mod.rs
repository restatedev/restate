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

use crate::MemberId;
use anyhow::Context;
use bytes::{Buf, BufMut};
use network::NetworkMessage;
use protobuf::Message as ProtobufMessage;
pub use server::RaftMetadataServer;

type StorageMarker = restate_types::storage::StorageMarker<String>;

impl NetworkMessage for raft::prelude::Message {
    fn to(&self) -> u64 {
        self.to
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct RaftConfiguration {
    my_member_id: MemberId,
}
