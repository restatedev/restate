// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::Message;
use bytes::BytesMut;
use restate_storage_api::outbox_table::OpaqueMessage;
use restate_types::{
    bilrost_storage_encode_decode, blob::BaseBlobId, errors::ConversionError,
    invocation::JournalCompletionTarget, sharding::WithPartitionKey, storage::StorageEncode,
};

use crate::v2::CommandKind;

/// Start of the discriminant range reserved for outbox-only kinds, i.e. kinds that
/// have no corresponding [`CommandKind`].
const OUTBOX_RANGE_START: u32 = 0xffff0000;

/// Identifies the type of message stored in the outbox.
///
/// The `u32` discriminant (see `From<OutboxKind> for u32` and `TryFrom<u32>`) is what
/// gets persisted alongside the opaque outbox message, so existing values must never change.
///
/// # Mapping to [`CommandKind`]
///
/// Outbox kinds fall into two groups:
///
/// * **Command kinds**: their discriminant is *identical* to the discriminant of the
///   corresponding [`CommandKind`] (e.g. `OutboxKind::Invoke as u32 == CommandKind::Invoke as u32`).
///   The persisted outbox payload is already an encoded command, so the shuffle can forward
///   it to the destination partition as-is, without decoding it.
///   [`OutboxKind::try_into_command_kind`] returns `Ok` for these kinds.
/// * **Outbox-only kinds**: their discriminant lives at or above `OUTBOX_RANGE_START`
///   (`0xffff0000`) so it can never collide with a [`CommandKind`]. These messages are local
///   to the partition processor that wrote them and can't be shipped to remote partitions
///   directly; they must be translated into a command first.
///   [`OutboxKind::try_into_command_kind`] returns `Err(self)` for these kinds.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    bilrost::Oneof,
    strum::Display,
    strum::IntoStaticStr,
    strum::VariantNames,
)]
#[repr(u32)]
pub enum OutboxKind {
    #[bilrost(tag(1), message)]
    TerminateInvocation = CommandKind::TerminateInvocation as u32,
    #[bilrost(tag(2), message)]
    Invoke = CommandKind::Invoke as u32,
    #[bilrost(tag(3), message)]
    AttachInvocation = CommandKind::AttachInvocation as u32,
    #[bilrost(tag(4), message)]
    InvocationResponse = CommandKind::InvocationResponse as u32,
    #[bilrost(tag(5), message)]
    NotifySignal = CommandKind::NotifySignal as u32,

    // Messages that are local and can't be shuffled to remote
    // partition processors.
    #[bilrost(tag(6), message)]
    InvocationResponseFromBlob = OUTBOX_RANGE_START + 1,
}

impl OutboxKind {
    /// Returns the [`CommandKind`] with the same discriminant if this is a command kind,
    /// or `Err(self)` if it is an outbox-only kind that needs translation before it can be
    /// sent to another partition. See [`OutboxKind`] for details on the mapping.
    pub fn try_into_command_kind(self) -> Result<CommandKind, Self> {
        self.try_into()
    }
}

impl From<OutboxKind> for u32 {
    fn from(value: OutboxKind) -> Self {
        value as u32
    }
}

impl TryFrom<u32> for OutboxKind {
    type Error = ConversionError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let kind = match value {
            5 => Self::TerminateInvocation,
            8 => Self::Invoke,
            11 => Self::AttachInvocation,
            17 => Self::InvocationResponse,
            19 => Self::NotifySignal,
            0xFFFF0001 => Self::InvocationResponseFromBlob,
            _ => {
                return Err(ConversionError::UnexpectedEnumVariant(
                    "OutboxKind",
                    value as i32,
                ));
            }
        };

        Ok(kind)
    }
}

impl TryFrom<OutboxKind> for CommandKind {
    type Error = OutboxKind;

    fn try_from(value: OutboxKind) -> Result<Self, Self::Error> {
        let kind = match value {
            OutboxKind::TerminateInvocation => CommandKind::TerminateInvocation,
            OutboxKind::Invoke => CommandKind::Invoke,
            OutboxKind::AttachInvocation => CommandKind::AttachInvocation,
            OutboxKind::InvocationResponse => CommandKind::InvocationResponse,
            OutboxKind::NotifySignal => CommandKind::NotifySignal,

            value => return Err(value),
        };

        Ok(kind)
    }
}

/// A message that can be written to the outbox.
///
/// The message is stored encoded (via [`StorageEncode`]) together with its [`Self::KIND`],
/// and is routed by the shuffle to the partition owning its [`WithPartitionKey`] key.
pub trait OutboxMessage: StorageEncode + WithPartitionKey {
    /// The kind persisted alongside the encoded message, used to decide how the shuffle
    /// handles it (see [`OutboxKind`]).
    const KIND: OutboxKind;
    /// Helper function that is only here to facilitate migration to opaque outbox message
    ///
    /// todo: Drop in v1.9.0
    fn into_outbox_message(self) -> restate_storage_api::outbox_table::OutboxMessage;
}

#[derive(Clone, Eq, PartialEq, bilrost::Message, derive_more::Into, derive_more::From)]
pub struct InvocationResponseBlobCommand {
    #[bilrost(tag(1))]
    pub target: JournalCompletionTarget,
    #[bilrost(tag(2))]
    pub blob: BaseBlobId,
}

bilrost_storage_encode_decode!(InvocationResponseBlobCommand);

impl WithPartitionKey for InvocationResponseBlobCommand {
    fn partition_key(&self) -> restate_types::sharding::PartitionKey {
        self.target.partition_key()
    }
}

impl OutboxMessage for InvocationResponseBlobCommand {
    const KIND: OutboxKind = OutboxKind::InvocationResponseFromBlob;

    fn into_outbox_message(self) -> restate_storage_api::outbox_table::OutboxMessage {
        // NOTE:
        // This is a temporary solution and can be dropped in v1.9.0
        // since the entire into_outbox_message() function will be dropped.
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        StorageEncode::encode(&self, &mut buf).expect("to encode");
        restate_storage_api::outbox_table::OutboxMessage::Opaque(OpaqueMessage {
            partition_key: self.partition_key(),
            codec: self.default_codec(),
            kind: Self::KIND.into(),
            message: buf.freeze(),
        })
    }
}
