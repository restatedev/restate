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
    InvocationTermination, NotifySignalRequest, PurgeInvocationRequest, ServiceInvocation,
};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::{PlainNodeId, Version, logs};

use crate::control::AnnounceLeader;
use crate::timer::TimerKeyValue;
use restate_types::GenerationalNodeId;
use restate_types::logs::{HasRecordKeys, Keys, MatchKeyQuery};

pub mod control;
pub mod timer;

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

#[cfg(feature = "serde")]
impl Envelope {
    pub fn to_bytes(&self) -> Result<bytes::Bytes, restate_types::storage::StorageEncodeError> {
        let mut buf = bytes::BytesMut::default();
        restate_types::storage::StorageCodec::encode(self, &mut buf)?;
        Ok(buf.freeze())
    }

    pub fn from_bytes(
        bytes: impl AsRef<[u8]>,
    ) -> Result<Self, restate_types::storage::StorageDecodeError> {
        let mut bytes = bytes.as_ref();
        restate_types::storage::StorageCodec::decode::<Self, _>(&mut bytes)
    }
}

#[cfg(feature = "serde")]
restate_types::flexbuffers_storage_encode_decode!(Envelope);

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
        partition_id: PartitionId,
        partition_key: Option<PartitionKey>,
        /// The current epoch of the partition leader. Readers should observe this to decide which
        /// messages to accept. Readers should ignore messages coming from
        /// epochs lower than the max observed for a given partition id.
        leader_epoch: LeaderEpoch,
        /// Which node is this message from?
        /// deprecated(v1.1): use generational_node_id instead.
        node_id: PlainNodeId,
        /// From v1.1 this is always set, but maintained to support rollback to v1.0.
        #[cfg_attr(feature = "serde", serde(default))]
        generational_node_id: Option<GenerationalNodeId>,
    },
    /// Message is sent from an ingress node
    Ingress {
        /// The identity of the sender node. Generational for fencing. Ingress is
        /// stateless, so we shouldn't respond to requests from older generation
        /// if a new generation is alive.
        node_id: GenerationalNodeId,
        /// Last config version observed by sender. If this is a newer generation
        /// or an unknown ID, we might need to update our config.
        nodes_config_version: Version,
    },
    /// Message is sent from some control plane component (controller, cli, etc.)
    ControlPlane {
        // Reserved for future use.
    },
}

impl Source {
    pub fn is_processor_generational(&self) -> bool {
        match self {
            Source::Processor {
                generational_node_id,
                ..
            } => generational_node_id.is_some(),
            _ => false,
        }
    }
}

/// Identifies the intended destination of the message
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Destination {
    /// Message is sent to partition processor
    Processor {
        partition_key: PartitionKey,
        dedup: Option<DedupInformation>,
    },
}

/// State machine input commands
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants, strum::VariantNames)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    // -- Control-plane related events
    AnnounceLeader(AnnounceLeader),

    // -- Partition processor commands
    /// Manual patching of storage state
    PatchState(ExternalStateMutation),
    /// Terminate an ongoing invocation
    TerminateInvocation(InvocationTermination),
    /// Purge a completed invocation
    PurgeInvocation(PurgeInvocationRequest),
    /// Start an invocation on this partition
    Invoke(ServiceInvocation),
    /// Truncate the message outbox up to, and including, the specified index.
    TruncateOutbox(MessageIndex),
    /// Proxy a service invocation through this partition processor, to reuse the deduplication id map.
    ProxyThrough(ServiceInvocation),
    /// Attach to an existing invocation
    AttachInvocation(AttachInvocationRequest),

    // -- Partition processor events for PP
    /// Invoker is reporting effect(s) from an ongoing invocation.
    InvokerEffect(restate_invoker_api::Effect),
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
        // Placeholder implementation
        match &self.command {
            Command::AnnounceLeader(announce) => {
                if let Some(range) = &announce.partition_key_range {
                    Keys::RangeInclusive(range.clone())
                } else {
                    // Fallback for old restate servers that didn't have partition_key_range.
                    Keys::Single(self.partition_key())
                }
            }
            Command::PatchState(mutation) => Keys::Single(mutation.service_id.partition_key()),
            Command::TerminateInvocation(terminate) => {
                Keys::Single(terminate.invocation_id.partition_key())
            }
            Command::PurgeInvocation(purge) => Keys::Single(purge.invocation_id.partition_key()),
            Command::Invoke(invoke) => Keys::Single(invoke.partition_key()),
            // todo: Remove this, or pass the partition key range but filter based on partition-id
            // on read if needed.
            Command::TruncateOutbox(_) => Keys::Single(self.partition_key()),
            Command::ProxyThrough(_) => Keys::Single(self.partition_key()),
            Command::AttachInvocation(_) => Keys::Single(self.partition_key()),
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
