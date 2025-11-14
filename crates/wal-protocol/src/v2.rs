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
use std::ops::Deref;

use bilrost::encoding::encoded_len_varint;
use bilrost::{Message, OwnedMessage};
use bytes::{Bytes, BytesMut};

use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{HasRecordKeys, Keys};
use restate_types::storage::{
    StorageCodec, StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode,
    StorageEncodeError,
};

/// Metadata that accompanies every WAL record and carries routing, deduplication,
/// and serialization details required to interpret the payload.
#[derive(Debug, Clone, bilrost::Message)]
pub struct Header {
    #[bilrost(1)]
    source: Source,
    #[bilrost(3)]
    dedup: Dedup,
    #[bilrost(5)]
    kind: RecordKind,
}

impl Header {
    pub fn source(&self) -> &Source {
        &self.source
    }

    pub fn dedup(&self) -> &Dedup {
        &self.dedup
    }

    pub fn kind(&self) -> RecordKind {
        self.kind
    }
}

impl StorageDecode for Header {
    fn decode<B: bytes::Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        // we use custom encoding because it's the length delimited version
        // of bilrost
        debug_assert_eq!(kind, StorageCodecKind::Custom);

        Self::decode_length_delimited(buf)
            .map_err(|err| StorageDecodeError::DecodeValue(err.into()))
    }
}

/// Outgoing envelope used when you are sending out records
/// over bifrost.
pub struct OutgoingEnvelope<R: Record> {
    header: Header,
    payload: R::Payload,
}

impl<R: Record> OutgoingEnvelope<R> {
    pub fn new(source: Source, dedup: Dedup, payload: R::Payload) -> Self {
        Self {
            header: Header {
                source,
                dedup,
                kind: R::KIND,
            },
            payload,
        }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn payload(&self) -> &R::Payload {
        &self.payload
    }
}

/// Only implement [`HasRecordKeys`] iff the
/// record payload implements [`HasRecordKeys`]
///
/// To send an envelope that does not has a record key
/// you will need to attach keys to envelope externally
/// by using [`BodyWithKeys`]
impl<R> HasRecordKeys for OutgoingEnvelope<R>
where
    R: Record,
    R::Payload: HasRecordKeys,
{
    fn record_keys(&self) -> Keys {
        self.payload.record_keys()
    }
}

impl<R: Record + 'static> StorageEncode for OutgoingEnvelope<R> {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Custom
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        let len = self.header.encoded_len();
        buf.reserve(encoded_len_varint(len as u64) + len);

        self.header
            .encode_length_delimited(buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into()))?;

        StorageCodec::encode(&self.payload, buf)
    }
}

/// Incoming container that couples the [`Header`] metadata with an encoded payload for a
/// particular record type.
///
/// This can only be created by [`Self::decode`] from a buffer that was previously created
/// from an [`OutgoingEnvelope`]
pub struct IncomingEnvelope<M> {
    header: Header,
    payload: Bytes,
    phantom: PhantomData<M>,
}

impl<M> Deref for IncomingEnvelope<M> {
    type Target = Header;
    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl StorageDecode for IncomingEnvelope<Raw> {
    fn decode<B: bytes::Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        match kind {
            StorageCodecKind::FlexbuffersSerde => {
                todo!("implement loading from envelope V1")
            }
            StorageCodecKind::Custom => {
                let header = StorageDecode::decode(buf, StorageCodecKind::Custom)?;

                Ok(Self {
                    header,
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

/// Marker type used with [`IncomingEnvelope`] to signal that the payload has not been
/// decoded into a typed record yet.
pub struct Raw;

impl IncomingEnvelope<Raw> {
    /// Converts Raw Envelope into a Typed envelope. Panics
    /// if the record kind does not match the M::KIND
    pub fn into_typed<M: Record>(self) -> IncomingEnvelope<M> {
        assert_eq!(self.header.kind, M::KIND);

        let Self {
            header,
            payload,
            phantom: _,
        } = self;

        IncomingEnvelope {
            header,
            payload,
            phantom: PhantomData,
        }
    }
}

impl<M: Record> IncomingEnvelope<M> {
    /// return the envelope payload
    pub fn split(mut self) -> Result<(Header, M::Payload), StorageDecodeError> {
        let payload = StorageCodec::decode(&mut self.payload)?;
        Ok((self.header, payload))
    }

    pub fn into_raw(self) -> IncomingEnvelope<Raw> {
        self.into()
    }
}

/// It's always safe to go back to Raw Envelope
impl<M: Record> From<IncomingEnvelope<M>> for IncomingEnvelope<Raw> {
    fn from(value: IncomingEnvelope<M>) -> Self {
        let IncomingEnvelope {
            header, payload, ..
        } = value;

        Self {
            header,
            payload,
            phantom: PhantomData,
        }
    }
}

/// Enumerates the logical categories of WAL records that the partition
/// processor understands.
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

    /// UpsertSchema
    UpsertSchema = 20,
}

/// Identifies which subsystem produced a given WAL record.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Oneof, bilrost::Message)]
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
        #[bilrost(0)]
        prefix: String,
        #[bilrost(1)]
        seq: u64,
    },
}

/// Marker trait implemented by strongly-typed representations of WAL record
/// payloads.
pub trait Record: sealed::Sealed + Sized {
    const KIND: RecordKind;
    type Payload: StorageEncode + StorageDecode + 'static;

    /// Create an envelope with `this` record kind
    /// given the header, keys and payload
    fn new(source: Source, dedup: Dedup, payload: Self::Payload) -> OutgoingEnvelope<Self>
    where
        Self::Payload: StorageEncode,
    {
        OutgoingEnvelope::new(source, dedup, payload)
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Phantom structs that pair [`RecordKind`] discriminants with their associated
/// payload types.
pub mod records {
    use restate_types::{
        invocation::{
            AttachInvocationRequest, GetInvocationOutputResponse, InvocationTermination,
            NotifySignalRequest, PurgeInvocationRequest, RestartAsNewInvocationRequest,
            ResumeInvocationRequest, ServiceInvocation,
        },
        message::TruncateOutboxRequest,
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
        @payload=TruncateOutboxRequest
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

    record! {
        @name=UpsertSchema,
        @kind=RecordKind::UpsertSchema,
        @payload=crate::control::UpsertSchema
    }
}

mod compatibility {
    /// Compatibility module with v1. We probably can never drop this
    /// code unless we are absolutely sure there is no more records
    /// ever exited that are still using v1
    use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};

    use super::{Dedup, Header, IncomingEnvelope, Raw, RecordKind, Source, records};
    use crate::v1;

    impl TryFrom<super::IncomingEnvelope<Raw>> for v1::Envelope {
        type Error = anyhow::Error;

        fn try_from(value: IncomingEnvelope<Raw>) -> Result<Self, Self::Error> {
            // let Envelope {
            //     header:
            //         Header {
            //             source,
            //             dedup,
            //             kind,
            //         },
            //     mut payload,
            //     ..
            // } = value;

            let (header, _command) = match value.kind() {
                RecordKind::Unknown => anyhow::bail!("Unknown record kind"),
                RecordKind::AnnounceLeader => value
                    .into_typed::<records::AnnounceLeader>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::AnnounceLeader(payload.into()))),
                RecordKind::VersionBarrier => value
                    .into_typed::<records::VersionBarrier>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::VersionBarrier(payload))),

                RecordKind::UpdatePartitionDurability => value
                    .into_typed::<records::UpdatePartitionDurability>()
                    .split()
                    .map(|(header, payload)| {
                        (header, v1::Command::UpdatePartitionDurability(payload))
                    }),
                RecordKind::PatchState => value
                    .into_typed::<records::PatchState>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::PatchState(payload))),
                RecordKind::TerminateInvocation => value
                    .into_typed::<records::TerminateInvocation>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::TerminateInvocation(payload))),
                RecordKind::PurgeInvocation => value
                    .into_typed::<records::PurgeInvocation>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::PurgeInvocation(payload))),
                RecordKind::PurgeJournal => value
                    .into_typed::<records::PurgeJournal>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::PurgeJournal(payload))),
                RecordKind::Invoke => value
                    .into_typed::<records::Invoke>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::Invoke(payload.into()))),
                RecordKind::TruncateOutbox => value
                    .into_typed::<records::TruncateOutbox>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::TruncateOutbox(payload.index))),
                RecordKind::ProxyThrough => value
                    .into_typed::<records::ProxyThrough>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::ProxyThrough(payload.into()))),
                RecordKind::AttachInvocation => value
                    .into_typed::<records::AttachInvocation>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::AttachInvocation(payload))),
                RecordKind::ResumeInvocation => value
                    .into_typed::<records::ResumeInvocation>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::ResumeInvocation(payload))),
                RecordKind::RestartAsNewInvocation => value
                    .into_typed::<records::RestartAsNewInvocation>()
                    .split()
                    .map(|(header, payload)| {
                        (header, v1::Command::RestartAsNewInvocation(payload))
                    }),
                RecordKind::InvokerEffect => value
                    .into_typed::<records::InvokerEffect>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::InvokerEffect(payload.into()))),
                RecordKind::Timer => value
                    .into_typed::<records::Timer>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::Timer(payload))),
                RecordKind::ScheduleTimer => value
                    .into_typed::<records::ScheduleTimer>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::ScheduleTimer(payload))),
                RecordKind::InvocationResponse => value
                    .into_typed::<records::InvocationResponse>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::InvocationResponse(payload))),

                RecordKind::NotifyGetInvocationOutputResponse => value
                    .into_typed::<records::NotifyGetInvocationOutputResponse>()
                    .split()
                    .map(|(header, payload)| {
                        (
                            header,
                            v1::Command::NotifyGetInvocationOutputResponse(payload),
                        )
                    }),

                RecordKind::NotifySignal => value
                    .into_typed::<records::NotifySignal>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::NotifySignal(payload))),
                RecordKind::UpsertSchema => value
                    .into_typed::<records::UpsertSchema>()
                    .split()
                    .map(|(header, payload)| (header, v1::Command::UpsertSchema(payload))),
            }?;

            let Header { source, dedup, .. } = header;

            let _source = match source {
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

            let _dedup = match dedup {
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

            unimplemented!("create `dest` for v1 envelope requires key information")

            // todo: create the dest for v1 envelope will require
            // extra information that can probably by extracted from the
            // keys (which is not part of the envelope)

            // let dest = match destination {
            //     Destination::None => anyhow::bail!("Missing envelope header destination"),
            //     Destination::Processor { partition_key } => v1::Destination::Processor {
            //         partition_key,
            //         dedup,
            //     },
            // };

            // Ok(v1::Envelope::new(v1::Header { source, dest }, command))
        }
    }
}

#[cfg(test)]
mod test {

    use bytes::BytesMut;

    use restate_types::{
        GenerationalNodeId,
        logs::{HasRecordKeys, Keys},
        storage::StorageCodec,
    };

    use super::{Dedup, Header, IncomingEnvelope, Source, records};
    use crate::{
        control::AnnounceLeader,
        v2::{Raw, Record},
    };

    #[test]
    fn envelope_encode_decode() {
        let payload = AnnounceLeader {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: 0..=u64::MAX,
        };

        let envelope = records::AnnounceLeader::new(
            Source::Ingress,
            Dedup::SelfProposal {
                leader_epoch: 10.into(),
                seq: 120,
            },
            payload.clone(),
        );

        assert_eq!(envelope.record_keys(), Keys::RangeInclusive(0..=u64::MAX));

        let mut buf = BytesMut::new();
        StorageCodec::encode(&envelope, &mut buf).expect("to encode");

        let envelope: IncomingEnvelope<Raw> = StorageCodec::decode(&mut buf).expect("to decode");

        let typed = envelope.into_typed::<records::AnnounceLeader>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_eq!(payload, loaded_payload);
    }

    #[test]
    fn header_decode_discard_payload() {
        let payload = AnnounceLeader {
            leader_epoch: 11.into(),
            node_id: GenerationalNodeId::new(1, 3),
            partition_key_range: 0..=u64::MAX,
        };

        let envelope = records::AnnounceLeader::new(
            Source::Ingress,
            Dedup::SelfProposal {
                leader_epoch: 10.into(),
                seq: 120,
            },
            payload.clone(),
        );

        let mut buf = BytesMut::new();
        StorageCodec::encode(&envelope, &mut buf).expect("to encode");

        let mut slice = &buf[..];
        // decode header only and discard the rest of the envelope
        let header: Header = StorageCodec::decode(&mut slice).expect("to decode");

        assert_eq!(header.source, Source::Ingress);

        assert_eq!(
            header.dedup,
            Dedup::SelfProposal {
                leader_epoch: 10.into(),
                seq: 120
            }
        );
    }
}
