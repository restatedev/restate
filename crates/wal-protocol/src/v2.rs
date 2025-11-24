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
use bytes::{BufMut, BytesMut};

use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::storage::{
    PolyBytes, StorageCodec, StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode,
    StorageEncodeError,
};

use crate::v1;

mod compatibility;
pub mod records;

mod sealed {
    pub trait Sealed {}
}

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
#[derive(Clone, derive_more::Deref)]
pub struct Envelope<R> {
    #[deref]
    header: Header,
    payload: PolyBytes,
    _p: PhantomData<R>,
}

impl<R> Envelope<R> {
    pub fn header(&self) -> &Header {
        &self.header
    }
}

impl<R: Record> Envelope<R> {
    pub fn new(source: Source, dedup: Dedup, payload: R::Payload) -> Self {
        Self {
            header: Header {
                source,
                dedup,
                kind: R::KIND,
            },
            payload: PolyBytes::Typed(Arc::new(payload)),
            _p: PhantomData,
        }
    }
}

impl<R: Send + Sync + 'static> StorageEncode for Envelope<R> {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Custom
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        let len = self.header.encoded_len();
        buf.reserve(encoded_len_varint(len as u64) + len);

        self.header
            .encode_length_delimited(buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into()))?;

        match &self.payload {
            PolyBytes::Bytes(bytes) => buf.put_slice(bytes),
            PolyBytes::Typed(payload) => StorageCodec::encode(payload.as_ref(), buf)?,
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
                let header = StorageDecode::decode(buf, StorageCodecKind::Custom)?;

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
    /// Converts Raw Envelope into a Typed envelope. Panics
    /// if the record kind does not match the M::KIND
    pub fn into_typed<M: Record>(self) -> Envelope<M> {
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

impl<M: Record> Envelope<M>
where
    M::Payload: Clone,
{
    /// return the envelope payload
    pub fn split(self) -> Result<(Header, M::Payload), StorageDecodeError> {
        let payload = match self.payload {
            PolyBytes::Bytes(mut bytes) => StorageCodec::decode(&mut bytes)?,
            PolyBytes::Typed(typed) => {
                let typed = typed.downcast_arc::<M::Payload>().map_err(|_| {
                    StorageDecodeError::DecodeValue("Type mismatch. Original value in PolyBytes::Typed does not match requested type".into())
                })?;

                match Arc::try_unwrap(typed) {
                    Ok(value) => value,
                    Err(arc) => arc.as_ref().clone(),
                }
            }
        };

        Ok((self.header, payload))
    }

    pub fn into_inner(self) -> Result<M::Payload, StorageDecodeError> {
        self.split().map(|v| v.1)
    }
}

impl<M: Record> Envelope<M> {
    pub fn into_raw(self) -> Envelope<Raw> {
        Envelope {
            header: self.header,
            payload: self.payload,
            _p: PhantomData,
        }
    }
}

/// Enumerates the logical categories of WAL records that the partition
/// processor understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration, strum::Display)]
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
    /// Message is sent from an ingress node
    #[bilrost(empty)]
    Ingress,

    /// Message is sent from some control plane component (controller, cli, etc.)
    #[bilrost(tag = 1, message)]
    ControlPlane,

    /// Message is sent from another partition processor
    #[bilrost(tag = 2, message)]
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
pub trait Record: sealed::Sealed + Send + Sync + Clone + Copy + Sized {
    const KIND: RecordKind;
    type Payload: StorageEncode + StorageDecode + 'static;

    /// Create an envelope with `this` record kind
    /// given the header, keys and payload
    fn new(source: Source, dedup: Dedup, payload: impl Into<Self::Payload>) -> Envelope<Self>
    where
        Self::Payload: StorageEncode,
    {
        Envelope::new(source, dedup, payload.into())
    }

    /// Creates a new test envelope. Shortcut for new(Source::Ingress, Dedup::None, payload)
    // #[cfg(test)]
    fn new_test(payload: impl Into<Self::Payload>) -> Envelope<Raw>
    where
        Self::Payload: StorageEncode,
    {
        let record = Self::new(Source::Ingress, Dedup::None, payload);
        record.into_raw()
    }
}

#[cfg(test)]
mod test {

    use bytes::BytesMut;

    use restate_types::{GenerationalNodeId, storage::StorageCodec};

    use super::{Dedup, Header, Source, records};
    use crate::{
        control::AnnounceLeader,
        v1,
        v2::{Envelope, Raw, Record, RecordKind},
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

        let mut buf = BytesMut::new();
        StorageCodec::encode(&envelope, &mut buf).expect("to encode");

        let envelope: Envelope<Raw> = StorageCodec::decode(&mut buf).expect("to decode");

        assert_eq!(envelope.kind(), RecordKind::AnnounceLeader);
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

    #[test]
    fn envelope_skip_encode() {
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

        // assert_eq!(envelope.record_keys(), Keys::RangeInclusive(0..=u64::MAX));

        let envelope = envelope.into_raw();

        assert_eq!(envelope.kind(), RecordKind::AnnounceLeader);
        let typed = envelope.into_typed::<records::AnnounceLeader>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_eq!(payload, loaded_payload);
    }

    #[test]
    fn envelope_from_to_v1() {
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

        let env_v1: v1::Envelope = envelope.try_into().expect("to work");

        let mut buf = BytesMut::new();
        StorageCodec::encode(&env_v1, &mut buf).expect("to encode");

        let envelope: Envelope<Raw> = StorageCodec::decode(&mut buf).expect("to decode");

        assert_eq!(envelope.kind(), RecordKind::AnnounceLeader);
        let typed = envelope.into_typed::<records::AnnounceLeader>();

        let (_, loaded_payload) = typed.split().expect("to decode");

        assert_eq!(payload, loaded_payload);
    }
}
