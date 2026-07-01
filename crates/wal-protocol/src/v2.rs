// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod commands;
mod compatibility;
mod markers;

use std::marker::PhantomData;
use std::sync::Arc;

use bilrost::encoding::encoded_len_varint;
use bilrost::{Message, OwnedMessage};
use bytes::{BufMut, Bytes, BytesMut};

use restate_encoding::U128;
use restate_types::errors::ConversionError;
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::logs::{BodyWithKeys, HasRecordKeys, Keys};
use restate_types::storage::{
    PolyBytes, StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode,
    StorageEncodeError,
};
use restate_util_string::ReString;

use crate::v1;
pub use markers::OutboxMessage;

mod sealed {
    pub trait Sealed {}
}

/// Metadata that accompanies every WAL record and carries routing, deduplication,
/// and serialization details required to interpret the payload.
#[derive(Debug, Clone, bilrost::Message)]
pub struct Header {
    #[bilrost(tag(1))]
    dedup: Dedup,
    /// Payload record kind
    #[bilrost(tag(2), encoding(fixed))]
    kind: CommandKind,
    /// Payload codec
    #[bilrost(tag(3), encoding(fixed))]
    codec: Option<StorageCodecKind>,
}

impl Header {
    pub fn dedup(&self) -> &Dedup {
        &self.dedup
    }

    pub fn kind(&self) -> CommandKind {
        self.kind
    }
}

/// Outgoing envelope used when you are sending out records
/// over bifrost.
#[derive(Clone, derive_more::Deref)]
pub struct Envelope<R> {
    #[deref]
    header: Header,
    payload: PolyBytes,
    _p: PhantomData<R>,
}

impl<C> Envelope<C> {
    pub fn header(&self) -> &Header {
        &self.header
    }
}

impl<C: Command> Envelope<C> {
    pub fn new(dedup: Dedup, payload: impl Into<Arc<C>>) -> Self {
        let payload = payload.into();
        Self {
            header: Header {
                dedup,
                kind: C::KIND,
                codec: Some(payload.default_codec()),
            },
            payload: PolyBytes::Typed(payload),
            _p: PhantomData,
        }
    }
}

impl<C: Send + Sync + 'static> StorageEncode for Envelope<C> {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Custom
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        let len = self.header.encoded_len();
        // todo(azmy): Followup! Also reserve enough space for the payload in one go
        buf.reserve(encoded_len_varint(len as u64) + len);

        self.header
            .encode_length_delimited(buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into()))?;

        match &self.payload {
            PolyBytes::Bytes(bytes) => buf.put_slice(bytes),
            PolyBytes::Typed(payload) => payload.encode(buf)?,
            PolyBytes::Both(_, bytes) => buf.put_slice(bytes),
        }

        Ok(())
    }
}

/// Marker type used with [`IncomingEnvelope`] to signal that the payload has not been
/// decoded into a typed record yet.
#[derive(Clone, Copy)]
pub struct Raw;

impl StorageDecode for Envelope<Raw> {
    fn decode<B: bytes::Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        match kind {
            StorageCodecKind::FlexbuffersSerde => {
                let envelope = v1::Envelope::decode(buf, kind)?;
                Self::try_from(envelope).map_err(|err| StorageDecodeError::DecodeValue(err.into()))
            }
            StorageCodecKind::Custom => {
                let header = Header::decode_length_delimited(&mut *buf)
                    .map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;

                Ok(Self {
                    header,
                    payload: PolyBytes::Bytes(buf.copy_to_bytes(buf.remaining())),
                    _p: PhantomData,
                })
            }
            _ => {
                panic!("unsupported encoding");
            }
        }
    }
}

impl Envelope<Raw> {
    /// Construct the raw envelope from the given inputs.
    ///
    /// It's the caller's responsibility to ensure that the bytes payload is the correct
    /// encoded value for this command kind and this codec.
    pub fn from_bytes_unchecked(
        kind: CommandKind,
        codec: StorageCodecKind,
        dedup: Dedup,
        bytes: Bytes,
    ) -> Self {
        Self {
            header: Header {
                dedup,
                kind,
                codec: Some(codec),
            },
            payload: PolyBytes::Bytes(bytes),
            _p: PhantomData,
        }
    }

    /// Converts Raw Envelope into a Typed envelope. Panics
    /// if the record kind does not match the M::KIND
    pub fn into_typed<M: Command>(self) -> Envelope<M> {
        assert_eq!(self.header.kind, M::KIND);

        let Self {
            header, payload, ..
        } = self;

        Envelope {
            header,
            payload,
            _p: PhantomData,
        }
    }
}

impl<C: Command> Envelope<C>
where
    C: StorageDecode + Clone,
{
    /// return the envelope payload
    pub fn split(self) -> Result<(Header, C), StorageDecodeError> {
        let payload = match self.payload {
            PolyBytes::Bytes(mut bytes) => {
                C::decode(&mut bytes, self.header.codec.expect("has codec kind"))?
            }
            PolyBytes::Both(typed, _) | PolyBytes::Typed(typed) => {
                let typed = typed.downcast_arc::<C>().map_err(|_| {
                    StorageDecodeError::DecodeValue("Type mismatch. Original value in PolyBytes::Typed does not match requested type".into())
                })?;

                match Arc::try_unwrap(typed) {
                    Ok(payload) => payload,
                    Err(arc) => arc.as_ref().clone(),
                }
            }
        };

        Ok((self.header, payload))
    }

    pub fn into_inner(self) -> Result<C, StorageDecodeError> {
        self.split().map(|v| v.1)
    }
}

impl<C: Command> Envelope<C> {
    pub fn into_raw(self) -> Envelope<Raw> {
        Envelope {
            header: self.header,
            payload: self.payload,
            _p: PhantomData,
        }
    }
}

impl<C: Command> From<Envelope<C>> for Envelope<Raw> {
    fn from(value: Envelope<C>) -> Self {
        value.into_raw()
    }
}

/// Enumerates the logical categories of WAL records that the partition
/// processor understands.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration, strum::Display, strum::IntoStaticStr,
)]
#[repr(u8)]
pub enum CommandKind {
    Unknown = 0,

    AnnounceLeader = 1,
    /// A version barrier to fence off state machine changes that require a certain minimum
    /// version of restate server.
    /// *Since v1.4.0*
    VersionBarrier = 2,
    /// Updates the `PARTITION_DURABILITY` FSM variable to the given value.
    /// See [`PartitionDurability`] for more details.
    ///
    /// *Since v1.4.2*
    UpdatePartitionDurability = 3,

    // -- Partition processor commands
    /// Manual patching of storage state
    PatchState = 4,
    /// Terminate an ongoing invocation
    TerminateInvocation = 5,
    /// Purge a completed invocation
    PurgeInvocation = 6,
    /// Purge a completed invocation journal
    PurgeJournal = 7,
    /// Start an invocation on this partition
    Invoke = 8,
    /// Truncate the message outbox up to, and including, the specified index.
    TruncateOutbox = 9,
    /// Proxy a service invocation through this partition processor, to reuse the deduplication id map.
    ///
    // Drop in v1.8 it's not used at the moment and is only here
    // for backward compatibility with V1.
    ProxyThrough = 10,
    /// Attach to an existing invocation
    AttachInvocation = 11,
    /// Resume an invocation
    ResumeInvocation = 12,
    /// Restart as new invocation from prefix
    RestartAsNewInvocation = 13,
    // -- Partition processor events for PP
    /// Invoker is reporting effect(s) from an ongoing invocation.
    InvokerEffect = 14,
    /// Timer has fired
    Timer = 15,
    /// Schedule timer
    ScheduleTimer = 16,
    /// Another partition processor is reporting a response of an invocation we requested.
    ///
    /// KINDA DEPRECATED: When Journal Table V1 is removed, this command should be used only to reply to invocations.
    /// Now it's abused for a bunch of other scenarios, like replying to get promise and get invocation output.
    ///
    /// For more details see `OnNotifyInvocationResponse`.
    InvocationResponse = 17,

    // -- New PP <-> PP commands using Journal V2
    /// Notify Get invocation output
    NotifyGetInvocationOutputResponse = 18,
    /// Notify a signal.
    NotifySignal = 19,

    /// UpsertSchema record type
    UpsertSchema = 20,

    /// Upsert rule book
    UpsertRuleBook = 21,

    /// VQueues scheduler decisions record type.
    VQSchedulerDecisions = 22,

    /// payload is bilrost encoded [`vqueues::VQueuesPause`]
    /// *Since v1.7.0
    VQueuesPause = 23,

    /// payload is bilrost encoded [`vqueues::VQueuesResume`]
    /// *Since v1.7.0
    VQueuesResume = 24,

    /// Pause an invocation (manual pause RPC).
    /// payload is bilrost encoded [`invocation::PauseInvocationCommand`]
    /// *Since v1.7.0
    PauseInvocation = 25,
}

impl From<CommandKind> for u8 {
    fn from(value: CommandKind) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for CommandKind {
    type Error = ConversionError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let v = match value {
            0 => Self::Unknown,
            1 => Self::AnnounceLeader,
            2 => Self::VersionBarrier,
            3 => Self::UpdatePartitionDurability,
            4 => Self::PatchState,
            5 => Self::TerminateInvocation,
            6 => Self::PurgeInvocation,
            7 => Self::PurgeJournal,
            8 => Self::Invoke,
            9 => Self::TruncateOutbox,
            10 => Self::ProxyThrough,
            11 => Self::AttachInvocation,
            12 => Self::ResumeInvocation,
            13 => Self::RestartAsNewInvocation,
            14 => Self::InvokerEffect,
            15 => Self::Timer,
            16 => Self::ScheduleTimer,
            17 => Self::InvocationResponse,
            18 => Self::NotifyGetInvocationOutputResponse,
            19 => Self::NotifySignal,
            20 => Self::UpsertSchema,
            21 => Self::UpsertRuleBook,
            22 => Self::VQSchedulerDecisions,
            23 => Self::VQueuesPause,
            24 => Self::VQueuesResume,
            25 => Self::PauseInvocation,
            other => {
                return Err(ConversionError::unexpected_enum_variant(
                    "CommandKind",
                    i32::from(other),
                ));
            }
        };

        Ok(v)
    }
}

mod bilrost_encoding {
    use bilrost::encoding::{DistinguishedProxiable, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind, Enumeration};

    use super::CommandKind;

    struct FixedCommandKindTag;

    impl Proxiable<FixedCommandKindTag> for CommandKind {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            <CommandKind as Enumeration>::to_number(self)
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = <CommandKind as Enumeration>::try_from_number(proxy)
                .unwrap_or(CommandKind::Unknown);
            Ok(())
        }
    }

    impl DistinguishedProxiable<FixedCommandKindTag> for CommandKind {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonicity::Canonical)
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Fixed)
        to encode proxied type (CommandKind) using proxy tag (FixedCommandKindTag)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}

/// Specifies the deduplication strategy that allows receivers to discard
/// duplicate WAL records safely.
#[derive(Debug, Clone, PartialEq, Eq, Default, bilrost::Oneof, bilrost::Message)]
pub enum Dedup {
    #[default]
    None,
    /// Sequence number to deduplicate messages sent by the same partition or a successor
    /// of a previous partition (a successor partition will inherit the leader epoch of its
    /// predecessor).
    #[bilrost(tag(1), message)]
    SelfProposal {
        #[bilrost(tag(0), encoding(fixed))]
        leader_epoch: LeaderEpoch,
        #[bilrost(tag(1), encoding(fixed))]
        seq: u64,
    },
    /// Sequence number to deduplicate messages from a foreign partition.
    #[bilrost(tag(2), message)]
    ForeignPartition {
        #[bilrost(tag(0), encoding(fixed))]
        partition: PartitionId,
        #[bilrost(tag(1), encoding(fixed))]
        seq: u64,
    },
    /// Sequence number to deduplicate messages from an arbitrary string prefix.
    #[bilrost(tag(3), message)]
    Arbitrary {
        // For backward compatibility with ProducerID::Other variant
        // Drop in Restate v1.8
        #[bilrost(tag(0))]
        prefix: Option<ReString>,
        #[bilrost(tag(1), encoding(fixed))]
        producer_id: U128,
        #[bilrost(tag(2), encoding(fixed))]
        seq: u64,
    },
}

/// Marker trait implemented by strongly-typed representations of WAL record
/// payloads.
pub trait Command: sealed::Sealed + StorageEncode + Sized {
    const KIND: CommandKind;

    fn envelope(self, dedup: Dedup) -> Envelope<Self> {
        Envelope::new(dedup, self)
    }

    /// Creates a new test envelope. Shortcut for new(Source::Ingress, Dedup::None, payload)
    #[cfg(any(test, feature = "test-util"))]
    fn test_envelope(payload: impl Into<Self>) -> Envelope<Raw> {
        let record = Envelope::new(Dedup::None, payload.into());
        record.into_raw()
    }
}

impl<C> sealed::Sealed for BodyWithKeys<C> {}

/// Provides record keys for a [`Command`].
///
/// Auto-implemented for any [`Command`] that also implements [`HasRecordKeys`].
/// Commands without a [`HasRecordKeys`] implementation can be paired with keys
/// explicitly via [`BodyWithKeys::new`], which also implements this trait.
///
/// This lets generic functions uniformly accept either a [`Command`] that
/// carries its own keys or a [`BodyWithKeys`] wrapper supplying them.
pub trait CommandWithKeys<C>
where
    C: Command,
{
    fn keys(&self) -> Keys;
    fn inner(self) -> C;
}

impl<C> CommandWithKeys<C> for C
where
    C: Command + HasRecordKeys,
{
    fn inner(self) -> C {
        self
    }

    fn keys(&self) -> Keys {
        self.record_keys()
    }
}

impl<C> CommandWithKeys<C> for BodyWithKeys<C>
where
    C: Command,
{
    fn keys(&self) -> Keys {
        BodyWithKeys::keys(self).clone()
    }

    fn inner(self) -> C {
        BodyWithKeys::into_inner(self)
    }
}

#[cfg(test)]
mod test {

    use bilrost::{Message, OwnedMessage};
    use bytes::BytesMut;

    use restate_encoding::U128;
    use restate_types::{
        GenerationalNodeId,
        logs::{BodyWithKeys, Keys},
        sharding::KeyRange,
        storage::{StorageCodec, StorageCodecKind},
        time::MillisSinceEpoch,
    };

    use super::Dedup;
    use crate::{
        control::{AnnounceLeaderCommand, UpdatePartitionDurabilityCommand},
        v2::{Command, CommandKind, CommandWithKeys, Envelope, Header, Raw},
    };

    #[test]
    fn header_fixed_fields_round_trip() {
        let header = Header {
            dedup: Dedup::None,
            kind: CommandKind::AnnounceLeader,
            codec: Some(StorageCodecKind::Custom),
        };
        let encoded = header.encode_to_bytes();

        assert_eq!(encoded.as_ref(), &[0x0a, 1, 0, 0, 0, 0x06, 7, 0, 0, 0]);

        let decoded = Header::decode(encoded).unwrap();
        assert_eq!(decoded.dedup, header.dedup);
        assert_eq!(decoded.kind, header.kind);
        assert_eq!(decoded.codec, header.codec);
    }

    #[test]
    fn dedup_fixed_fields_round_trip() {
        let self_proposal = Dedup::SelfProposal {
            leader_epoch: 10.into(),
            seq: 120,
        };
        let encoded = self_proposal.encode_to_bytes();
        assert_eq!(encoded.len(), 20);
        assert_eq!(Dedup::decode(encoded).unwrap(), self_proposal);

        let foreign_partition = Dedup::ForeignPartition {
            partition: 10.into(),
            seq: 120,
        };
        let encoded = foreign_partition.encode_to_bytes();
        assert_eq!(encoded.len(), 16);
        assert_eq!(Dedup::decode(encoded).unwrap(), foreign_partition);

        let arbitrary = Dedup::Arbitrary {
            prefix: None,
            producer_id: U128::from(0x1234_5678_90ab_cdef_0123_4567_89ab_cdef),
            seq: 120,
        };
        let encoded = arbitrary.encode_to_bytes();
        assert_eq!(encoded.len(), 29);
        assert_eq!(Dedup::decode(encoded).unwrap(), arbitrary);
    }

    #[test]
    fn general_encoding_keeps_command_kind_varint() {
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedCommandKind {
            #[bilrost(tag(1))]
            kind: CommandKind,
        }

        let value = EncodedCommandKind {
            kind: CommandKind::Invoke,
        };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.as_ref(), &[0x04, 8]);
        assert_eq!(EncodedCommandKind::decode(encoded).unwrap(), value);
    }

    #[test]
    fn envelope_encode_decode() {
        let payload = AnnounceLeaderCommand {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: KeyRange::new(0, u64::MAX),
            epoch_version: None,
            current_config: None,
            next_config: None,
        };

        let envelope = Envelope::new(
            Dedup::SelfProposal {
                leader_epoch: 10.into(),
                seq: 120,
            },
            payload.clone(),
        );

        let mut buf = BytesMut::new();
        StorageCodec::encode(&envelope, &mut buf).expect("to encode");

        let envelope: Envelope<Raw> = StorageCodec::decode(&mut buf).expect("to decode");

        assert_eq!(envelope.kind(), CommandKind::AnnounceLeader);
        let typed = envelope.into_typed::<AnnounceLeaderCommand>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_announce_leader_eq(&payload, &loaded_payload);
    }

    #[test]
    fn envelope_skip_encode() {
        let payload = AnnounceLeaderCommand {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: KeyRange::new(0, u64::MAX),
            epoch_version: None,
            current_config: None,
            next_config: None,
        };

        let envelope = Envelope::new(
            Dedup::SelfProposal {
                leader_epoch: 10.into(),
                seq: 120,
            },
            payload.clone(),
        );

        let envelope = envelope.into_raw();

        assert_eq!(envelope.kind(), CommandKind::AnnounceLeader);
        let typed = envelope.into_typed::<AnnounceLeaderCommand>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_announce_leader_eq(&payload, &loaded_payload);
    }

    fn make_envelope<C: Command>(cmd: impl CommandWithKeys<C>) -> Envelope<Raw> {
        Envelope::new(Dedup::None, cmd.inner()).into_raw()
    }

    #[test]
    fn command_with_has_record_keys() {
        let payload = AnnounceLeaderCommand {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: KeyRange::new(0, u64::MAX),
            epoch_version: None,
            current_config: None,
            next_config: None,
        };

        let envelope = make_envelope(payload.clone());

        assert_eq!(envelope.kind(), CommandKind::AnnounceLeader);
        let typed = envelope.into_typed::<AnnounceLeaderCommand>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_announce_leader_eq(&payload, &loaded_payload);
    }

    #[test]
    fn command_without_has_record_keys() {
        let payload = UpdatePartitionDurabilityCommand {
            durable_point: 0.into(),
            modification_time: MillisSinceEpoch::now(),
            partition_id: 10.into(),
        };

        let envelope = make_envelope(BodyWithKeys::new(payload.clone(), Keys::Single(100)));

        let mut buf = BytesMut::new();
        StorageCodec::encode(&envelope, &mut buf).unwrap();

        let loaded: Envelope<Raw> = StorageCodec::decode(&mut buf).unwrap();

        let typed = loaded.into_typed::<UpdatePartitionDurabilityCommand>();
        let inner = typed.into_inner().unwrap();

        assert_eq!(payload.durable_point, inner.durable_point);
        assert_eq!(payload.modification_time, inner.modification_time);
        assert_eq!(payload.partition_id, inner.partition_id);
    }

    #[test]
    fn command_without_has_record_keys_type_cast() {
        let payload = UpdatePartitionDurabilityCommand {
            durable_point: 0.into(),
            modification_time: MillisSinceEpoch::now(),
            partition_id: 10.into(),
        };

        let envelope = make_envelope(BodyWithKeys::new(payload.clone(), Keys::Single(100)));

        let converted = envelope.into_typed::<UpdatePartitionDurabilityCommand>();

        let inner = converted.into_inner().unwrap();

        assert_eq!(payload.durable_point, inner.durable_point);
        assert_eq!(payload.modification_time, inner.modification_time);
        assert_eq!(payload.partition_id, inner.partition_id);
    }

    #[track_caller]
    fn assert_announce_leader_eq(expected: &AnnounceLeaderCommand, actual: &AnnounceLeaderCommand) {
        assert_eq!(expected.node_id, actual.node_id);
        assert_eq!(expected.leader_epoch, actual.leader_epoch);
        assert_eq!(expected.partition_key_range, actual.partition_key_range);
        assert_eq!(expected.epoch_version, actual.epoch_version);
        assert_eq!(
            expected.current_config.is_some(),
            actual.current_config.is_some(),
        );
        assert_eq!(expected.next_config.is_some(), actual.next_config.is_some(),);
    }
}
