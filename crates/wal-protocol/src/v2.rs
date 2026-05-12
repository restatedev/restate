// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use bilrost::encoding::encoded_len_varint;
use bilrost::{Message, OwnedMessage};
use bytes::{BufMut, Bytes, BytesMut};

use restate_encoding::U128;
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::logs::{BodyWithKeys, HasRecordKeys, Keys};
use restate_types::storage::{
    PolyBytes, StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode,
    StorageEncodeError,
};
use restate_util_string::ReString;

use crate::v1;

pub mod commands;
mod compatibility;

mod sealed {
    pub trait Sealed {}
}

/// Metadata that accompanies every WAL record and carries routing, deduplication,
/// and serialization details required to interpret the payload.
#[derive(Debug, Clone, bilrost::Message)]
pub struct Header {
    #[bilrost(1)]
    dedup: Dedup,
    /// Payload record kind
    #[bilrost(2)]
    kind: CommandKind,
    /// Payload codec
    #[bilrost(3)]
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
    C: Clone,
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
        #[bilrost(0)]
        leader_epoch: LeaderEpoch,
        #[bilrost(1)]
        seq: u64,
    },
    /// Sequence number to deduplicate messages from a foreign partition.
    #[bilrost(tag(2), message)]
    ForeignPartition {
        #[bilrost(0)]
        partition: PartitionId,
        #[bilrost(1)]
        seq: u64,
    },
    /// Sequence number to deduplicate messages from an arbitrary string prefix.
    #[bilrost(tag(3), message)]
    Arbitrary {
        // For backward compatibility with ProducerID::Other variant
        // Drop in Restate v1.8
        #[bilrost(0)]
        prefix: Option<ReString>,
        #[bilrost(1)]
        producer_id: U128,
        #[bilrost(2)]
        seq: u64,
    },
}

/// A partial type-erased envelope mainly used for writing records.
/// It carries the payload part with Keys.
pub struct PartialEnvelope {
    kind: CommandKind,
    keys: Keys,
    payload: Arc<dyn StorageEncode>,
}

impl PartialEnvelope {
    pub fn kind(&self) -> CommandKind {
        self.kind
    }

    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    /// Builds an [`Envelope<Raw>`] with keys from the [`PartialEnvelope`]
    pub fn build(self, dedup: Dedup) -> BodyWithKeys<Envelope<Raw>> {
        let inner = Envelope {
            header: Header {
                dedup,
                kind: self.kind,
                codec: Some(self.payload.default_codec()),
            },
            payload: PolyBytes::Typed(self.payload),
            _p: PhantomData,
        };

        BodyWithKeys::new(inner, self.keys)
    }
}

/// Marker trait implemented by strongly-typed representations of WAL record
/// payloads.
pub trait Command: sealed::Sealed + StorageEncode + StorageDecode + Sized {
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

impl<C> From<C> for PartialEnvelope
where
    C: Command + HasRecordKeys,
{
    fn from(command: C) -> PartialEnvelope {
        PartialEnvelope {
            kind: C::KIND,
            keys: command.record_keys(),
            payload: Arc::new(command),
        }
    }
}

#[cfg(test)]
mod test {

    use bytes::BytesMut;

    use restate_types::{
        GenerationalNodeId, logs::Keys, sharding::KeyRange, storage::StorageCodec,
    };

    use super::Dedup;
    use crate::{
        control::AnnounceLeaderCommand,
        v2::{CommandKind, Envelope, PartialEnvelope, Raw},
    };

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

        // assert_eq!(envelope.record_keys(), Keys::RangeInclusive(0..=u64::MAX));

        let envelope = envelope.into_raw();

        assert_eq!(envelope.kind(), CommandKind::AnnounceLeader);
        let typed = envelope.into_typed::<AnnounceLeaderCommand>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_announce_leader_eq(&payload, &loaded_payload);
    }

    #[test]
    fn partial_envelope_with_keys() {
        let payload = AnnounceLeaderCommand {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: KeyRange::new(0, u64::MAX),
            epoch_version: None,
            current_config: None,
            next_config: None,
        };

        let envelope: PartialEnvelope = payload.clone().into();

        let keyed = envelope.build(Dedup::SelfProposal {
            leader_epoch: 10.into(),
            seq: 120,
        });

        assert_eq!(
            keyed.keys(),
            &Keys::RangeInclusive(payload.partition_key_range.into())
        );

        let envelope = keyed.into_inner();
        assert_eq!(envelope.kind(), CommandKind::AnnounceLeader);
        let envelope = envelope.into_typed::<AnnounceLeaderCommand>();

        let (_, loaded_payload) = envelope.split().expect("to decode");

        assert_announce_leader_eq(&payload, &loaded_payload);
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
