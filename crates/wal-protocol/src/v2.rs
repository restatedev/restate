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

use bilrost::encoding::encoded_len_varint;
use bilrost::{Message, OwnedMessage};
use bytes::{BufMut, Bytes, BytesMut};

use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::logs::{HasRecordKeys, Keys};
use restate_types::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
};

const ENCODING_VERSION: u8 = 1;

#[derive(Debug, Clone, bilrost::Message)]
struct EnvelopeInner {
    #[bilrost(1)]
    header: Header,
    #[bilrost(2)]
    keys: Keys,
    #[bilrost(3)]
    kind: RecordKind,
    #[bilrost(4)]
    encoding: Option<StorageCodecKind>,
}

/// The primary envelope for all messages in the system.
#[derive(Debug, Clone)]
pub struct Envelope<M> {
    inner: EnvelopeInner,
    payload: Bytes,
    phantom: PhantomData<M>,
}

impl<M: Send + Sync + 'static> StorageEncode for Envelope<M> {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Custom
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        buf.put_u8(ENCODING_VERSION);

        let len = self.inner.encoded_len();
        buf.reserve(encoded_len_varint(len as u64) + len + self.payload.len());

        self.inner
            .encode_length_delimited(buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into()))?;

        buf.put(&self.payload[..]);
        Ok(())
    }
}

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
                todo!("implement loading from envelop V1")
            }
            StorageCodecKind::Custom => {
                let version = buf.get_u8();
                if version != ENCODING_VERSION {
                    return Err(StorageDecodeError::DecodeValue(
                        anyhow::anyhow!("Unknown envelope encoding version {version}").into(),
                    ));
                }

                let inner = EnvelopeInner::decode_length_delimited(&mut *buf)
                    .map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;

                Ok(Self {
                    inner,
                    payload: buf.copy_to_bytes(buf.remaining()),
                    phantom: PhantomData,
                })
            }
            _ => {
                panic!("unsupported encoding");
            }
        }
    }
}

impl<M: Send + Sync> HasRecordKeys for Envelope<M> {
    fn record_keys(&self) -> Keys {
        self.inner.keys.clone()
    }
}

impl<M> WithPartitionKey for Envelope<M> {
    fn partition_key(&self) -> PartitionKey {
        match self.header().dest {
            Destination::None => unimplemented!("expect destinationt to be set"),
            Destination::Processor { partition_key, .. } => partition_key,
        }
    }
}

impl<M> Envelope<M> {
    #[inline]
    pub fn record_type(&self) -> RecordKind {
        self.inner.kind
    }

    #[inline]
    pub fn header(&self) -> &Header {
        &self.inner.header
    }

    pub fn kind(&self) -> RecordKind {
        self.inner.kind
    }
}

/// Tag for untyped Envelope
// #[derive(Copy, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct Raw;

impl Envelope<Raw> {
    /// Convers Raw Envelope into a Typed envelope. Panics
    /// if the record kind does not match the M::KIND
    pub fn into_typed<M: Record>(self) -> Envelope<M> {
        assert_eq!(self.inner.kind, M::KIND);

        let Self {
            inner,
            payload,
            phantom: _,
        } = self;

        Envelope {
            inner,
            payload,
            phantom: PhantomData,
        }
    }
}

impl<M: Record> Envelope<M> {
    /// Create a new typed envelope
    pub fn create(
        header: Header,
        record_keys: Keys,
        payload: M::Payload,
    ) -> Result<Self, StorageEncodeError>
    where
        M::Payload: StorageEncode,
    {
        let mut buf = BytesMut::new();
        payload.encode(&mut buf)?;

        let inner = EnvelopeInner {
            header,
            keys: record_keys,
            kind: M::KIND,
            encoding: payload.default_codec().into(),
        };

        Ok(Self {
            inner,
            payload: buf.freeze(),
            phantom: PhantomData,
        })
    }

    /// return the envelope payload
    pub fn payload(&mut self) -> Result<M::Payload, StorageDecodeError> {
        M::Payload::decode(
            &mut self.payload,
            self.inner.encoding.expect("encoding to be set"),
        )
    }

    pub fn into_raw(self) -> Envelope<Raw> {
        self.into()
    }
}

/// It's always safe to go back to Raw Envelope
impl<M: Record> From<Envelope<M>> for Envelope<Raw> {
    fn from(value: Envelope<M>) -> Self {
        let Envelope { inner, payload, .. } = value;

        Self {
            inner,
            payload,
            phantom: PhantomData,
        }
    }
}

/// Header is set on every message
#[derive(Debug, Clone, bilrost::Message)]
pub struct Header {
    #[bilrost(1)]
    pub source: Source,

    #[bilrost(2)]
    pub dest: Destination,

    #[bilrost(3)]
    pub dedup: Dedup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum RecordKind {
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
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct RawRecord {
    #[bilrost(1)]
    kind: RecordKind,
    #[bilrost(2)]
    encoding: Option<StorageCodecKind>,
    #[bilrost(3)]
    data: Bytes,
}

/// Identifies the source of a message
#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum Source {
    #[bilrost(empty)]
    None,

    /// Message is sent from an ingress node
    #[bilrost(tag = 1, message)]
    Ingress,

    /// Message is sent from some control plane component (controller, cli, etc.)
    #[bilrost(tag = 2, message)]
    ControlPlane,

    /// Message is sent from another partition processor
    #[bilrost(tag = 3, message)]
    Processor {
        /// if possible, this is used to reroute responses in case of splits/merges
        /// Marked as `Option` in v1.5. Note that v1.4 requires this to be set but as of v1.6
        /// this can be safely set to `None`.
        #[bilrost(1)]
        partition_id: Option<PartitionId>,
        #[bilrost(2)]
        partition_key: Option<PartitionKey>,
        /// The current epoch of the partition leader. Readers should observe this to decide which
        /// messages to accept. Readers should ignore messages coming from
        /// epochs lower than the max observed for a given partition id.
        #[bilrost(3)]
        leader_epoch: LeaderEpoch,
    },
}

/// Identifies the intended destination of the message
#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum Destination {
    #[bilrost(empty)]
    None,

    /// Message is sent to partition processor
    #[bilrost(tag = 1, message)]
    Processor { partition_key: PartitionKey },
}

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
        #[bilrost(0)]
        prefix: String,
        #[bilrost(1)]
        seq: u64,
    },
}

pub trait Record: sealed::Sealed + Sized {
    const KIND: RecordKind;
    type Payload: StorageDecode + 'static;

    /// Create an envelope with `this` record kind
    /// given the header, keys and payload
    fn envelope(
        header: Header,
        record_keys: Keys,
        payload: Self::Payload,
    ) -> Result<Envelope<Self>, StorageEncodeError>
    where
        Self::Payload: StorageEncode,
    {
        Envelope::create(header, record_keys, payload)
    }
}

mod sealed {
    pub trait Sealed {}
}

pub mod records {
    use restate_types::{
        invocation::{
            AttachInvocationRequest, GetInvocationOutputResponse, InvocationTermination,
            NotifySignalRequest, PurgeInvocationRequest, RestartAsNewInvocationRequest,
            ResumeInvocationRequest, ServiceInvocation,
        },
        message::MessageIndexRecrod,
        state_mut::ExternalStateMutation,
    };

    use super::sealed::Sealed;
    use super::{Record, RecordKind};
    use crate::timer::TimerKeyValue;

    macro_rules! record {
        {@name=$name:ident, @kind=$type:expr, @payload=$payload:path} => {
            #[allow(dead_code)]
            pub struct $name;
            impl Sealed for $name{}
            impl Record for $name {
                const KIND: RecordKind = $type;
                type Payload = $payload;
            }
        };
    }

    record! {
        @name=AnnounceLeader,
        @kind=RecordKind::AnnounceLeader,
        @payload=crate::control::AnnounceLeader
    }

    record! {
        @name=VersionBarrier,
        @kind=RecordKind::VersionBarrier,
        @payload=crate::control::VersionBarrier
    }

    record! {
        @name=UpdatePartitionDurability,
        @kind=RecordKind::UpdatePartitionDurability,
        @payload=crate::control::PartitionDurability
    }

    record! {
        @name=PatchState,
        @kind=RecordKind::PatchState,
        @payload=ExternalStateMutation
    }

    record! {
        @name=TerminateInvocation,
        @kind=RecordKind::TerminateInvocation,
        @payload=InvocationTermination
    }

    record! {
        @name=PurgeInvocation,
        @kind=RecordKind::PurgeInvocation,
        @payload=PurgeInvocationRequest
    }

    record! {
        @name=PurgeJournal,
        @kind=RecordKind::PurgeJournal,
        @payload=PurgeInvocationRequest
    }

    record! {
        @name=Invoke,
        @kind=RecordKind::Invoke,
        @payload=ServiceInvocation
    }

    record! {
        @name=TruncateOutbox,
        @kind=RecordKind::TruncateOutbox,
        @payload=MessageIndexRecrod
    }

    record! {
        @name=ProxyThrough,
        @kind=RecordKind::ProxyThrough,
        @payload=ServiceInvocation
    }

    record! {
        @name=AttachInvocation,
        @kind=RecordKind::AttachInvocation,
        @payload=AttachInvocationRequest
    }

    record! {
        @name=ResumeInvocation,
        @kind=RecordKind::ResumeInvocation,
        @payload=ResumeInvocationRequest
    }

    record! {
        @name=RestartAsNewInvocation,
        @kind=RecordKind::RestartAsNewInvocation,
        @payload=RestartAsNewInvocationRequest
    }

    record! {
        @name=InvokerEffect,
        @kind=RecordKind::InvokerEffect,
        @payload=restate_invoker_api::Effect
    }

    record! {
        @name=Timer,
        @kind=RecordKind::Timer,
        @payload=TimerKeyValue
    }

    record! {
        @name=ScheduleTimer,
        @kind=RecordKind::ScheduleTimer,
        @payload=TimerKeyValue
    }

    record! {
        @name=InvocationResponse,
        @kind=RecordKind::InvocationResponse,
        @payload=restate_types::invocation::InvocationResponse
    }

    record! {
        @name=NotifyGetInvocationOutputResponse,
        @kind=RecordKind::NotifyGetInvocationOutputResponse,
        @payload=GetInvocationOutputResponse
    }

    record! {
        @name=NotifySignal,
        @kind=RecordKind::NotifySignal,
        @payload=NotifySignalRequest
    }
}

mod compatibility {
    /// Compatibility module with v1. We probably can never drop this
    /// code unless we are absolutely sure there is no more records
    /// ever exited that are still using v1
    use anyhow::Context;
    use bytes::Buf;

    use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
    use restate_types::storage::{StorageCodecKind, StorageDecode, StorageDecodeError};

    use super::{
        Dedup, Destination, Envelope, EnvelopeInner, Header, Raw, Record, RecordKind, Source,
        records,
    };
    use crate::v1;

    fn decode_payload<R: Record, B: Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<R::Payload, StorageDecodeError> {
        <R::Payload as StorageDecode>::decode(buf, kind)
    }

    impl TryFrom<Header> for v1::Header {
        type Error = anyhow::Error;

        fn try_from(value: Header) -> Result<Self, Self::Error> {
            let Header {
                source,
                dest,
                dedup,
            } = value;

            let source = match source {
                Source::None => anyhow::bail!("Missing envelope header source"),
                Source::Ingress => v1::Source::Ingress {},
                Source::ControlPlane => v1::Source::ControlPlane {},
                Source::Processor {
                    partition_id,
                    partition_key,
                    leader_epoch,
                } => v1::Source::Processor {
                    partition_id,
                    partition_key,
                    leader_epoch,
                },
            };

            let dedup = match dedup {
                Dedup::None => None,
                Dedup::SelfProposal { leader_epoch, seq } => {
                    Some(DedupInformation::self_proposal(EpochSequenceNumber {
                        leader_epoch,
                        sequence_number: seq,
                    }))
                }
                Dedup::ForeignPartition { partition, seq } => {
                    Some(DedupInformation::cross_partition(partition, seq))
                }
                Dedup::Arbitrary { prefix, seq } => Some(DedupInformation::ingress(prefix, seq)),
            };

            let dest = match dest {
                Destination::None => anyhow::bail!("Missing envelope header destination"),
                Destination::Processor { partition_key } => v1::Destination::Processor {
                    partition_key,
                    dedup,
                },
            };

            Ok(v1::Header { source, dest })
        }
    }

    impl TryFrom<super::Envelope<Raw>> for v1::Envelope {
        type Error = anyhow::Error;

        fn try_from(value: Envelope<Raw>) -> Result<Self, Self::Error> {
            let Envelope {
                inner:
                    EnvelopeInner {
                        encoding,
                        header,
                        keys: _,
                        kind,
                    },
                mut payload,
                ..
            } = value;

            // todo: create a bilrost helpder for required fields so it failes
            // during decoding.
            let encoding = encoding.context("missing encoding")?;

            let command = match kind {
                RecordKind::Unknown => anyhow::bail!("Unknown record kind"),
                RecordKind::AnnounceLeader => {
                    let value =
                        decode_payload::<records::AnnounceLeader, _>(&mut payload, encoding)?;
                    v1::Command::AnnounceLeader(value.into())
                }
                RecordKind::VersionBarrier => {
                    let value =
                        decode_payload::<records::VersionBarrier, _>(&mut payload, encoding)?;
                    v1::Command::VersionBarrier(value)
                }
                RecordKind::UpdatePartitionDurability => {
                    let value = decode_payload::<records::UpdatePartitionDurability, _>(
                        &mut payload,
                        encoding,
                    )?;
                    v1::Command::UpdatePartitionDurability(value)
                }
                RecordKind::PatchState => {
                    let value = decode_payload::<records::PatchState, _>(&mut payload, encoding)?;
                    v1::Command::PatchState(value)
                }
                RecordKind::TerminateInvocation => {
                    let value =
                        decode_payload::<records::TerminateInvocation, _>(&mut payload, encoding)?;
                    v1::Command::TerminateInvocation(value)
                }
                RecordKind::PurgeInvocation => {
                    let value =
                        decode_payload::<records::PurgeInvocation, _>(&mut payload, encoding)?;
                    v1::Command::PurgeInvocation(value)
                }
                RecordKind::PurgeJournal => {
                    let value = decode_payload::<records::PurgeJournal, _>(&mut payload, encoding)?;
                    v1::Command::PurgeJournal(value)
                }
                RecordKind::Invoke => {
                    let value = decode_payload::<records::Invoke, _>(&mut payload, encoding)?;
                    v1::Command::Invoke(value.into())
                }
                RecordKind::TruncateOutbox => {
                    let value =
                        decode_payload::<records::TruncateOutbox, _>(&mut payload, encoding)?;
                    v1::Command::TruncateOutbox(value.index)
                }
                RecordKind::ProxyThrough => {
                    let value = decode_payload::<records::ProxyThrough, _>(&mut payload, encoding)?;
                    v1::Command::ProxyThrough(value.into())
                }
                RecordKind::AttachInvocation => {
                    let value =
                        decode_payload::<records::AttachInvocation, _>(&mut payload, encoding)?;
                    v1::Command::AttachInvocation(value)
                }
                RecordKind::ResumeInvocation => {
                    let value =
                        decode_payload::<records::ResumeInvocation, _>(&mut payload, encoding)?;
                    v1::Command::ResumeInvocation(value)
                }
                RecordKind::RestartAsNewInvocation => {
                    let value = decode_payload::<records::RestartAsNewInvocation, _>(
                        &mut payload,
                        encoding,
                    )?;
                    v1::Command::RestartAsNewInvocation(value)
                }
                RecordKind::InvokerEffect => {
                    let value =
                        decode_payload::<records::InvokerEffect, _>(&mut payload, encoding)?;
                    v1::Command::InvokerEffect(value.into())
                }
                RecordKind::Timer => {
                    let value = decode_payload::<records::Timer, _>(&mut payload, encoding)?;
                    v1::Command::Timer(value)
                }
                RecordKind::ScheduleTimer => {
                    let value =
                        decode_payload::<records::ScheduleTimer, _>(&mut payload, encoding)?;
                    v1::Command::ScheduleTimer(value)
                }
                RecordKind::InvocationResponse => {
                    let value =
                        decode_payload::<records::InvocationResponse, _>(&mut payload, encoding)?;
                    v1::Command::InvocationResponse(value)
                }

                RecordKind::NotifyGetInvocationOutputResponse => {
                    let value = decode_payload::<records::NotifyGetInvocationOutputResponse, _>(
                        &mut payload,
                        encoding,
                    )?;
                    v1::Command::NotifyGetInvocationOutputResponse(value)
                }

                RecordKind::NotifySignal => {
                    let value = decode_payload::<records::NotifySignal, _>(&mut payload, encoding)?;
                    v1::Command::NotifySignal(value)
                }
            };

            Ok(v1::Envelope::new(header.try_into()?, command))
        }
    }
}

#[cfg(test)]
mod test {

    use bytes::BytesMut;

    use restate_types::{
        GenerationalNodeId,
        logs::Keys,
        storage::{StorageCodecKind, StorageDecode, StorageEncode},
    };

    use super::{Dedup, Destination, Envelope, Header, Source, records};
    use crate::{
        control::AnnounceLeader,
        v2::{Raw, Record},
    };

    #[test]
    fn envelope_encode_decode() {
        let header = Header {
            dedup: Dedup::SelfProposal {
                leader_epoch: 10.into(),
                seq: 120,
            },
            dest: Destination::Processor {
                partition_key: 1234,
            },
            source: Source::Ingress,
        };

        let payload = AnnounceLeader {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: 0..=u64::MAX,
        };

        let envelope =
            records::AnnounceLeader::envelope(header, Keys::Single(1000), payload.clone())
                .expect("to work");

        let mut buf = BytesMut::new();
        envelope.encode(&mut buf).expect("to encode");

        let envelope =
            Envelope::<Raw>::decode(&mut buf, StorageCodecKind::Custom).expect("to decode");

        let mut typed = envelope.into_typed::<records::AnnounceLeader>();

        let loaded_payload = typed.payload().expect("to decode");

        assert_eq!(payload, loaded_payload);
    }
}
