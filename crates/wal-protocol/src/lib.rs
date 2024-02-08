// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::invocation::{InvocationResponse, InvocationTermination, ServiceInvocation};
use restate_types::message::MessageIndex;
use restate_types::nodes_config::ConfigVersion;
use restate_types::state_mut::ExternalStateMutation;

use crate::control::AnnounceLeader;
use crate::effects::BuiltinServiceEffects;
use crate::timer::TimerValue;
use restate_types::{GenerationalNodeId, PlainNodeId};

pub mod control;
pub mod effects;
pub mod timer;

/// The primary envelope for all messages in the system.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope {
    pub header: Header,
    pub command: Command,
}

/// Header is set on every message
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header {
    pub source: Source,
    pub dest: Destination,
    pub ack_mode: AckMode,
}

/// Identifies the source of a message
#[derive(Debug, Clone)]
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
        /// This represents the logical clock of the sender at the time of writing this command.
        /// The receiver can (optionally) use this message to detect duplication.
        sequence_number: Option<MessageIndex>, // -- or sender_sequencer_number
        /// Which node is this message from?
        node_id: PlainNodeId,
    },
    /// Message is sent from an ingress node
    Ingress {
        /// The identity of the sender node. Generational for fencing. Ingress is
        /// stateless, so we shouldn't respond to requests from older generation
        /// if a new generation is alive.
        node_id: GenerationalNodeId,
        /// Last config version observed by sender. If this is a newer generation
        /// or an unknown ID, we might need to update our config.
        nodes_config_version: ConfigVersion,
        // If unset and AckMode is dedup, we use the node_id as a key.
        dedup_key: Option<String>,
        sequence_number: MessageIndex,
    },
    /// Message is sent from some control plane component (controller, cli, etc.)
    ControlPlane {
        // Reserved for future use.
    },
}

/// Identifies the intended destination of the message
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Destination {
    /// Message is sent to partition processor
    Processor { partition_key: PartitionKey },
}

/// Defines expectations from the sender on how to respond to this command.
/// Some commands require an explicit acknowledge.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AckMode {
    /// Acknowledge the command with a response. For partition processor-initiated messages, we
    /// respond by writing on their WAL (bifrost log). For ingress, we respond acknowledge by
    /// direectly sending a response to the node over the node-to-node stream.
    Ack,
    /// Received should deduplicate this message.
    Dedup,
    /// No acknowledgement is expected.
    None,
}

/// State machine input commands
#[derive(Debug, Clone, strum_macros::EnumDiscriminants, strum_macros::VariantNames)]
#[strum_discriminants(derive(strum_macros::IntoStaticStr))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    // -- Control-plane related events
    AnnounceLeader(AnnounceLeader),

    // -- Partition processor commands
    /// Manual patching of storage state
    PatchState(ExternalStateMutation),
    /// Terminate an ongoing invocation
    TerminateInvocation(InvocationTermination),
    /// Start an invocation on this partition
    Invoke(ServiceInvocation),
    /// Outbox can be truncated up to this index
    TruncateOutbox(MessageIndex),

    // -- Partition processor events for PP
    /// Invoker is reporting effect(s) from an ongoing invocation.
    InvokerEffect(restate_invoker_api::Effect),
    /// Timer has fired
    Timer(TimerValue),
    /// Another partition processor is reporting a response of an invocation we requested.
    InvocationResponse(InvocationResponse),
    /// A built-in invoker reporting effects from an invocation.
    BuiltInInvokerEffect(BuiltinServiceEffects),
}

impl Command {
    pub fn name(&self) -> &'static str {
        CommandDiscriminants::from(self).into()
    }
}
