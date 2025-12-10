// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::invocation::{
    AttachInvocationRequest, GetInvocationOutputResponse, InvocationResponse,
    InvocationTermination, NotifySignalRequest, PurgeInvocationRequest,
    RestartAsNewInvocationRequest, ResumeInvocationRequest, ServiceInvocation,
};
use restate_types::logs::{self, HasRecordKeys, Keys, MatchKeyQuery};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;

use crate::control::{AnnounceLeader, UpsertSchema, VersionBarrier};
use crate::timer::TimerKeyValue;

use self::control::PartitionDurability;

pub mod control;
pub mod timer;
pub mod vqueues;

/// The primary envelope for all messages in the system.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope {
    pub header: Header,
    pub command: Command,
}

#[cfg(feature = "serde")]
restate_types::flexbuffers_storage_encode_decode!(Envelope);

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
#[derive(Debug, Clone, strum::EnumDiscriminants, strum::VariantNames)]
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

    /// Upsert schema for consistent schema across replicas
    /// *Since v1.6.0
    UpsertSchema(UpsertSchema),
    // # Commands for VQueues management
    // ----------------------------------
    /// A command to attempt a run an entry in the vqueue (invocation, or otherwise)
    /// *Since v1.6.0
    /// payload is vqueues::VQWaitingToRunning (bilrost encoded)
    VQWaitingToRunning(Bytes),
    /// payload is vqueues::VQYieldRunning (bilrost encoded)
    VQYieldRunning(Bytes),
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
            Command::UpsertSchema(schema) => schema.partition_key_range.clone(),
            Command::VQWaitingToRunning(_) => Keys::Single(self.partition_key()),
            Command::VQYieldRunning(_) => Keys::Single(self.partition_key()),
        }
    }
}

impl MatchKeyQuery for Envelope {
    fn matches_key_query(&self, query: &logs::KeyFilter) -> bool {
        self.record_keys().matches_key_query(query)
    }
}
