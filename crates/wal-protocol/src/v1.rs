// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::invocation::{
    AttachInvocationRequest, GetInvocationOutputResponse, InvocationResponse,
    InvocationTermination, NotifySignalRequest, PurgeInvocationRequest,
    RestartAsNewInvocationRequest, ResumeInvocationRequest, ServiceInvocation,
};
use restate_types::logs;
use restate_types::logs::{HasRecordKeys, Keys, MatchKeyQuery};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;

use crate::control::{AnnounceLeader, PartitionDurability, VersionBarrier};
use crate::timer::TimerKeyValue;

/// The primary envelope for all messages in the system.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope {
    pub header: Header,
    pub command: Command,
}

impl Envelope {
    pub fn new(header: Header, command: Command) -> Self {
        Self { header, command }
    }
}

/// Header is set on every message
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header {
    pub source: Source,
    pub dest: Destination,
}

/// Identifies the source of a message
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Source {
    /// Message is sent from another partition processor
    Processor {
        /// if possible, this is used to reroute responses in case of splits/merges
        /// v1.4 requires this to be set.
        /// v1.5 Marked as `Option`.
        /// v1.6 always set to `None`.
        /// Will be removed in v1.7.
        #[cfg_attr(feature = "serde", serde(default))]
        partition_id: Option<PartitionId>,
        #[cfg_attr(feature = "serde", serde(default))]
        partition_key: Option<PartitionKey>,
        /// The current epoch of the partition leader. Readers should observe this to decide which
        /// messages to accept. Readers should ignore messages coming from
        /// epochs lower than the max observed for a given partition id.
        leader_epoch: LeaderEpoch,
        // Which node is this message from?
        // First deprecation in v1.1, but since v1.5 we switched to Option<PlainNodeId> and it's
        // still being set to Some(v) to maintain compatibility with v1.4.
        //
        // v1.6 field is removed. -- Kept here for reference only.
        // #[cfg_attr(feature = "serde", serde(default))]
        // node_id: Option<PlainNodeId>,

        // From v1.1 this is always set, but maintained to support rollback to v1.0.
        // Deprecated(v1.5): It's set to Some(v) to maintain support for v1.4 but
        // will be removed in v1.6. Commands that need the node-id of the sender should
        // include the node-id in the command payload itself (e.g. in the [`AnnounceLeader`])
        // v1.6 field is removed. -- Kept here for reference only.
        // #[cfg_attr(feature = "serde", serde(default))]
        // generational_node_id: Option<GenerationalNodeId>,
    },
    /// Message is sent from an ingress node
    Ingress {
        // The identity of the sender node. Generational for fencing. Ingress is
        // stateless, so we shouldn't respond to requests from older generation
        // if a new generation is alive.
        //
        // Deprecated(v1.5): This field is set to Some(v) to maintain compatibility with v1.4.
        // but will be removed in v1.6.
        // v1.6 field is removed. -- Kept here for reference only.
        // #[cfg_attr(feature = "serde", serde(default))]
        // node_id: Option<GenerationalNodeId>,

        // Last config version observed by sender. If this is a newer generation
        // or an unknown ID, we might need to update our config.
        //
        // Deprecated(v1.5): This field is set to Some(v) to maintain compatibility with v1.4.
        // but will be removed in v1.6.
        // v1.6 field is removed. -- Kept here for reference only.
        // #[cfg_attr(feature = "serde", serde(default))]
        // nodes_config_version: Option<Version>,
    },
    /// Message is sent from some control plane component (controller, cli, etc.)
    ControlPlane {
        // Reserved for future use.
    },
}

/// Identifies the intended destination of the message
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Destination {
    /// Message is sent to partition processor
    Processor {
        partition_key: PartitionKey,
        #[cfg_attr(feature = "serde", serde(default))]
        dedup: Option<DedupInformation>,
    },
}

/// State machine input commands
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants, strum::VariantNames)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    /// Updates the `PARTITION_DURABILITY` FSM variable to the given value.
    /// See [`PartitionDurability`] for more details.
    ///
    /// *Since v1.4.2*
    UpdatePartitionDurability(PartitionDurability),
    /// A version barrier to fence off state machine changes that require a certain minimum
    /// version of restate server.
    /// *Since v1.4.0*
    VersionBarrier(VersionBarrier),
    // -- Control-plane related events
    AnnounceLeader(Box<AnnounceLeader>),

    // -- Partition processor commands
    /// Manual patching of storage state
    PatchState(ExternalStateMutation),
    /// Terminate an ongoing invocation
    TerminateInvocation(InvocationTermination),
    /// Purge a completed invocation
    PurgeInvocation(PurgeInvocationRequest),
    /// Purge a completed invocation journal
    PurgeJournal(PurgeInvocationRequest),
    /// Start an invocation on this partition
    Invoke(Box<ServiceInvocation>),
    /// Truncate the message outbox up to, and including, the specified index.
    TruncateOutbox(MessageIndex),
    /// Proxy a service invocation through this partition processor, to reuse the deduplication id map.
    ProxyThrough(Box<ServiceInvocation>),
    /// Attach to an existing invocation
    AttachInvocation(AttachInvocationRequest),
    /// Resume an invocation
    ResumeInvocation(ResumeInvocationRequest),
    /// Restart as new invocation from prefix
    RestartAsNewInvocation(RestartAsNewInvocationRequest),

    // -- Partition processor events for PP
    /// Invoker is reporting effect(s) from an ongoing invocation.
    InvokerEffect(Box<restate_invoker_api::Effect>),
    /// Timer has fired
    Timer(TimerKeyValue),
    /// Schedule timer
    ScheduleTimer(TimerKeyValue),
    /// Another partition processor is reporting a response of an invocation we requested.
    ///
    /// KINDA DEPRECATED: When Journal Table V1 is removed, this command should be used only to reply to invocations.
    /// Now it's abused for a bunch of other scenarios, like replying to get promise and get invocation output.
    ///
    /// For more details see `OnNotifyInvocationResponse`.
    InvocationResponse(InvocationResponse),

    // -- New PP <-> PP commands using Journal V2
    /// Notify Get invocation output
    NotifyGetInvocationOutputResponse(GetInvocationOutputResponse),
    /// Notify a signal.
    NotifySignal(NotifySignalRequest),
}

impl Command {
    pub fn name(&self) -> &'static str {
        CommandDiscriminants::from(self).into()
    }
}

impl WithPartitionKey for Envelope {
    fn partition_key(&self) -> PartitionKey {
        match self.header.dest {
            Destination::Processor { partition_key, .. } => partition_key,
        }
    }
}

impl HasRecordKeys for Envelope {
    fn record_keys(&self) -> logs::Keys {
        match &self.command {
            // the partition_key is used as key here since the command targets the partition by ID.
            // Partitions will ignore this message at read time if the paritition ID (in body)
            // does not match. Alternatively, we could use the partition key range or `Keys::None`
            // but this would just be a waste of effort for readers after a partition has been
            // split or if the log is shared between multiple partitions.
            Command::UpdatePartitionDurability(_) => Keys::Single(self.partition_key()),
            Command::VersionBarrier(barrier) => barrier.partition_key_range.clone(),
            Command::AnnounceLeader(announce) => {
                Keys::RangeInclusive(announce.partition_key_range.clone())
            }
            Command::PatchState(mutation) => Keys::Single(mutation.service_id.partition_key()),
            Command::TerminateInvocation(terminate) => {
                Keys::Single(terminate.invocation_id.partition_key())
            }
            Command::PurgeInvocation(purge) => Keys::Single(purge.invocation_id.partition_key()),
            Command::PurgeJournal(purge) => Keys::Single(purge.invocation_id.partition_key()),
            Command::Invoke(invoke) => Keys::Single(invoke.partition_key()),
            // todo: Remove this, or pass the partition key range but filter based on partition-id
            // on read if needed.
            Command::TruncateOutbox(_) => Keys::Single(self.partition_key()),
            Command::ProxyThrough(_) => Keys::Single(self.partition_key()),
            Command::AttachInvocation(_) => Keys::Single(self.partition_key()),
            Command::ResumeInvocation(req) => Keys::Single(req.partition_key()),
            Command::RestartAsNewInvocation(req) => Keys::Single(req.partition_key()),
            // todo: Handle journal entries that request cross-partition invocations
            Command::InvokerEffect(effect) => Keys::Single(effect.invocation_id.partition_key()),
            Command::Timer(timer) => Keys::Single(timer.invocation_id().partition_key()),
            Command::ScheduleTimer(timer) => Keys::Single(timer.invocation_id().partition_key()),
            Command::InvocationResponse(response) => Keys::Single(response.partition_key()),
            Command::NotifySignal(sig) => Keys::Single(sig.partition_key()),
            Command::NotifyGetInvocationOutputResponse(res) => Keys::Single(res.partition_key()),
        }
    }
}

impl MatchKeyQuery for Envelope {
    fn matches_key_query(&self, query: &logs::KeyFilter) -> bool {
        self.record_keys().matches_key_query(query)
    }
}

#[cfg(feature = "serde")]
mod envelope {
    use bilrost::{Message, OwnedMessage};
    use bytes::{Buf, Bytes, BytesMut};

    use restate_storage_api::protobuf_types::v1 as protobuf;
    use restate_types::storage::decode::{decode_bilrost, decode_serde};
    use restate_types::storage::encode::{encode_bilrost, encode_serde};
    use restate_types::storage::{
        StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
    };

    use crate::Command;

    impl StorageEncode for crate::Envelope {
        fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
            use bytes::BufMut;
            match self.default_codec() {
                StorageCodecKind::FlexbuffersSerde => encode_serde(self, buf, self.default_codec()),
                StorageCodecKind::Custom => {
                    buf.put_slice(&encode(self)?);
                    Ok(())
                }
                _ => unreachable!("developer error"),
            }
        }

        fn default_codec(&self) -> StorageCodecKind {
            // todo: Could be changed in v1.6
            StorageCodecKind::FlexbuffersSerde
        }
    }

    impl StorageDecode for crate::Envelope {
        fn decode<B: ::bytes::Buf>(
            buf: &mut B,
            kind: StorageCodecKind,
        ) -> Result<Self, StorageDecodeError>
        where
            Self: Sized,
        {
            match kind {
                StorageCodecKind::Json
                | StorageCodecKind::BincodeSerde
                | StorageCodecKind::FlexbuffersSerde => decode_serde(buf, kind).map_err(|err| {
                    tracing::error!(%err, "{} decode failure (decoding Envelope)", kind);
                    err
                }),
                StorageCodecKind::LengthPrefixedRawBytes
                | StorageCodecKind::Protobuf
                | StorageCodecKind::Bilrost => Err(StorageDecodeError::UnsupportedCodecKind(kind)),
                StorageCodecKind::Custom => decode(buf),
            }
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum DecodeError {
        #[error("missing field codec")]
        MissingFieldCodec,
        #[error("unknown command kind")]
        UnknownCommandKind,
        #[error("unexpected codec kind {0}")]
        UnexpectedCodec(StorageCodecKind),
    }

    impl From<DecodeError> for StorageDecodeError {
        fn from(value: DecodeError) -> Self {
            Self::DecodeValue(value.into())
        }
    }

    #[derive(PartialEq, Eq, bilrost::Enumeration)]
    enum CommandKind {
        Unknown = 0,
        AnnounceLeader = 1,                     // flexbuffers
        PatchState = 2,                         // protobuf
        TerminateInvocation = 3,                // flexbuffers
        PurgeInvocation = 4,                    // flexbuffers
        Invoke = 5,                             // protobuf
        TruncateOutbox = 6,                     // flexbuffers
        ProxyThrough = 7,                       // protobuf
        AttachInvocation = 8,                   // protobuf
        InvokerEffect = 9,                      // flexbuffers
        Timer = 10,                             // flexbuffers
        ScheduleTimer = 11,                     // flexbuffers
        InvocationResponse = 12,                // protobuf
        NotifyGetInvocationOutputResponse = 13, // bilrost
        NotifySignal = 14,                      // protobuf
        PurgeJournal = 15,                      // flexbuffers
        VersionBarrier = 16,                    // bilrost
        UpdatePartitionDurability = 17,         // bilrost
        ResumeInvocation = 18,                  // flexbuffers
        RestartAsNewInvocation = 19,            // flexbuffers
    }

    #[derive(bilrost::Message)]
    struct Field {
        #[bilrost(1)]
        codec: Option<StorageCodecKind>,
        #[bilrost(2)]
        bytes: Bytes,
    }

    impl Field {
        fn encode_serde<T: serde::Serialize>(
            codec: StorageCodecKind,
            value: &T,
        ) -> Result<Self, StorageEncodeError> {
            let mut buf = BytesMut::new();
            encode_serde(value, &mut buf, codec)?;

            Ok(Self {
                codec: Some(codec),
                bytes: buf.freeze(),
            })
        }

        fn encode_bilrost<T: bilrost::Message>(value: &T) -> Result<Self, StorageEncodeError> {
            Ok(Self {
                codec: Some(StorageCodecKind::Bilrost),
                bytes: encode_bilrost(value),
            })
        }

        fn encode_protobuf<T: prost::Message>(value: &T) -> Result<Self, StorageEncodeError> {
            let mut buf = BytesMut::new();
            value
                .encode(&mut buf)
                .map_err(|err| StorageEncodeError::EncodeValue(err.into()))?;

            Ok(Self {
                codec: Some(StorageCodecKind::Protobuf),
                bytes: buf.freeze(),
            })
        }

        fn decode_serde<T: serde::de::DeserializeOwned>(mut self) -> Result<T, StorageDecodeError> {
            let codec = self.codec()?;
            if !matches!(
                codec,
                StorageCodecKind::Json
                    | StorageCodecKind::FlexbuffersSerde
                    | StorageCodecKind::BincodeSerde
            ) {
                return Err(StorageDecodeError::UnsupportedCodecKind(codec));
            }

            decode_serde(
                &mut self.bytes,
                self.codec.ok_or(DecodeError::MissingFieldCodec)?,
            )
        }

        fn decode_bilrost<T: bilrost::OwnedMessage>(mut self) -> Result<T, StorageDecodeError> {
            let codec = self.codec()?;
            if codec != StorageCodecKind::Bilrost {
                return Err(StorageDecodeError::UnsupportedCodecKind(codec));
            }

            decode_bilrost(&mut self.bytes)
        }

        fn decode_protobuf<T: prost::Message + Default>(self) -> Result<T, StorageDecodeError> {
            let codec = self.codec()?;
            if codec != StorageCodecKind::Protobuf {
                return Err(StorageDecodeError::UnsupportedCodecKind(codec));
            }

            T::decode(self.bytes).map_err(|err| StorageDecodeError::DecodeValue(err.into()))
        }

        fn codec(&self) -> Result<StorageCodecKind, DecodeError> {
            self.codec.ok_or(DecodeError::MissingFieldCodec)
        }
    }

    #[derive(bilrost::Message)]
    struct Envelope {
        #[bilrost(1)]
        header: Field,
        #[bilrost(2)]
        command_kind: CommandKind,
        #[bilrost(3)]
        command: Field,
    }

    macro_rules! codec_or_error {
        ($field:expr, $expected:path) => {{
            let codec = $field.codec()?;
            if !matches!(codec, $expected) {
                return Err(DecodeError::UnexpectedCodec(codec).into());
            }
        }};
    }

    pub fn encode(envelope: &super::Envelope) -> Result<Bytes, StorageEncodeError> {
        // todo(azmy): avoid clone? this will require change to `From` implementation
        let (command_kind, command) = match &envelope.command {
            Command::UpdatePartitionDurability(value) => (
                CommandKind::UpdatePartitionDurability,
                Field::encode_bilrost(value),
            ),
            Command::VersionBarrier(value) => {
                (CommandKind::VersionBarrier, Field::encode_bilrost(value))
            }
            Command::AnnounceLeader(value) => (
                CommandKind::AnnounceLeader,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::PatchState(value) => {
                let value = protobuf::StateMutation::from(value.clone());
                (CommandKind::PatchState, Field::encode_protobuf(&value))
            }
            Command::TerminateInvocation(value) => (
                CommandKind::TerminateInvocation,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::PurgeInvocation(value) => (
                CommandKind::PurgeInvocation,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::ResumeInvocation(value) => (
                CommandKind::ResumeInvocation,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::RestartAsNewInvocation(value) => (
                CommandKind::RestartAsNewInvocation,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::PurgeJournal(value) => (
                CommandKind::PurgeJournal,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::Invoke(value) => {
                let value = protobuf::ServiceInvocation::from(value.as_ref());
                (CommandKind::Invoke, Field::encode_protobuf(&value))
            }
            Command::TruncateOutbox(value) => (
                CommandKind::TruncateOutbox,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::ProxyThrough(value) => {
                let value = protobuf::ServiceInvocation::from(value.as_ref());
                (CommandKind::ProxyThrough, Field::encode_protobuf(&value))
            }
            Command::AttachInvocation(value) => {
                let value = protobuf::outbox_message::AttachInvocationRequest::from(value.clone());
                (
                    CommandKind::AttachInvocation,
                    Field::encode_protobuf(&value),
                )
            }
            Command::InvokerEffect(value) => (
                CommandKind::InvokerEffect,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::Timer(value) => (
                CommandKind::Timer,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::ScheduleTimer(value) => (
                CommandKind::ScheduleTimer,
                Field::encode_serde(StorageCodecKind::FlexbuffersSerde, value),
            ),
            Command::InvocationResponse(value) => {
                let value =
                    protobuf::outbox_message::OutboxServiceInvocationResponse::from(value.clone());
                (
                    CommandKind::InvocationResponse,
                    Field::encode_protobuf(&value),
                )
            }
            Command::NotifyGetInvocationOutputResponse(value) => (
                CommandKind::NotifyGetInvocationOutputResponse,
                Field::encode_bilrost(value),
            ),
            Command::NotifySignal(value) => {
                let value = protobuf::outbox_message::NotifySignal::from(value.clone());
                (CommandKind::NotifySignal, Field::encode_protobuf(&value))
            }
        };

        let dto = Envelope {
            header: Field::encode_serde(StorageCodecKind::FlexbuffersSerde, &envelope.header)?,
            command_kind,
            command: command?,
        };

        Ok(dto.encode_contiguous().into_vec().into())
    }

    pub fn decode<B: Buf>(buf: B) -> Result<super::Envelope, StorageDecodeError> {
        let envelope =
            Envelope::decode(buf).map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;

        // header is encoded with serde
        codec_or_error!(envelope.header, StorageCodecKind::FlexbuffersSerde);
        let header = envelope.header.decode_serde::<super::Header>()?;

        let command = match envelope.command_kind {
            CommandKind::Unknown => return Err(DecodeError::UnknownCommandKind.into()),
            CommandKind::UpdatePartitionDurability => {
                codec_or_error!(envelope.command, StorageCodecKind::Bilrost);
                Command::UpdatePartitionDurability(envelope.command.decode_bilrost()?)
            }
            CommandKind::VersionBarrier => {
                codec_or_error!(envelope.command, StorageCodecKind::Bilrost);
                Command::VersionBarrier(envelope.command.decode_bilrost()?)
            }
            CommandKind::AnnounceLeader => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::AnnounceLeader(envelope.command.decode_serde()?)
            }
            CommandKind::PatchState => {
                codec_or_error!(envelope.command, StorageCodecKind::Protobuf);
                let value: protobuf::StateMutation = envelope.command.decode_protobuf()?;
                Command::PatchState(value.try_into()?)
            }
            CommandKind::TerminateInvocation => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::TerminateInvocation(envelope.command.decode_serde()?)
            }
            CommandKind::PurgeInvocation => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::PurgeInvocation(envelope.command.decode_serde()?)
            }
            CommandKind::ResumeInvocation => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::ResumeInvocation(envelope.command.decode_serde()?)
            }
            CommandKind::RestartAsNewInvocation => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::RestartAsNewInvocation(envelope.command.decode_serde()?)
            }
            CommandKind::PurgeJournal => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::PurgeJournal(envelope.command.decode_serde()?)
            }
            CommandKind::Invoke => {
                codec_or_error!(envelope.command, StorageCodecKind::Protobuf);
                let value: protobuf::ServiceInvocation = envelope.command.decode_protobuf()?;
                Command::Invoke(Box::new(value.try_into()?))
            }
            CommandKind::TruncateOutbox => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::TruncateOutbox(envelope.command.decode_serde()?)
            }
            CommandKind::ProxyThrough => {
                codec_or_error!(envelope.command, StorageCodecKind::Protobuf);
                let value: protobuf::ServiceInvocation = envelope.command.decode_protobuf()?;
                Command::ProxyThrough(Box::new(value.try_into()?))
            }
            CommandKind::AttachInvocation => {
                codec_or_error!(envelope.command, StorageCodecKind::Protobuf);
                let value: protobuf::outbox_message::AttachInvocationRequest =
                    envelope.command.decode_protobuf()?;
                Command::AttachInvocation(value.try_into()?)
            }
            CommandKind::InvokerEffect => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::InvokerEffect(envelope.command.decode_serde()?)
            }
            CommandKind::Timer => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::Timer(envelope.command.decode_serde()?)
            }
            CommandKind::ScheduleTimer => {
                codec_or_error!(envelope.command, StorageCodecKind::FlexbuffersSerde);
                Command::ScheduleTimer(envelope.command.decode_serde()?)
            }
            CommandKind::InvocationResponse => {
                codec_or_error!(envelope.command, StorageCodecKind::Protobuf);
                let value: protobuf::outbox_message::OutboxServiceInvocationResponse =
                    envelope.command.decode_protobuf()?;
                Command::InvocationResponse(value.try_into()?)
            }
            CommandKind::NotifyGetInvocationOutputResponse => {
                codec_or_error!(envelope.command, StorageCodecKind::Bilrost);
                Command::NotifyGetInvocationOutputResponse(envelope.command.decode_bilrost()?)
            }
            CommandKind::NotifySignal => {
                codec_or_error!(envelope.command, StorageCodecKind::Protobuf);
                let value: protobuf::outbox_message::NotifySignal =
                    envelope.command.decode_protobuf()?;

                Command::NotifySignal(value.try_into()?)
            }
        };

        Ok(super::Envelope { header, command })
    }
}
