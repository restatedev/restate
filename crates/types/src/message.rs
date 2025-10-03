// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module defines types used for the internal messaging between Restate components.

use crate::{bilrost_storage_encode_decode, identifiers::PartitionId};

/// Wrapper that extends a message with its target peer to which the message should be sent.
pub type PartitionTarget<Msg> = (PartitionId, Msg);

/// Index type used messages in the runtime
pub type MessageIndex = u64;

#[derive(Debug, Clone, Copy)]
pub enum AckKind {
    Acknowledge(MessageIndex),
    Duplicate {
        // Sequence number of the duplicate message.
        seq_number: MessageIndex,
        // Currently last known sequence number by the receiver for a producer.
        // See `DeduplicatingStateMachine` for more details.
        last_known_seq_number: MessageIndex,
    },
}

#[derive(Debug, Clone, Copy, bilrost::Message)]
pub struct MessageIndexRecrod {
    #[bilrost(1)]
    pub index: MessageIndex,
}

bilrost_storage_encode_decode!(MessageIndexRecrod);
