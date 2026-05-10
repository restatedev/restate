// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use restate_encoding::Arced;
use restate_limiter::RuleBook;
use restate_storage_api::vqueue_table::scheduler::{self};
use restate_types::{
    bilrost_storage_encode_decode, flexbuffers_storage_encode_decode,
    identifiers::{WithInvocationId, WithPartitionKey},
    invocation,
    logs::{HasRecordKeys, Keys},
    message::MessageIndex,
    sharding::KeyRange,
    state_mut,
};

use super::sealed::Sealed;
use super::{Command, CommandKind};
use crate::timer::{self};

pub use crate::control::{
    AnnounceLeaderCommand, UpdatePartitionDurabilityCommand, UpsertSchemaCommand,
    VersionBarrierCommand,
};

// Create type wrappers to implement storage encode/decode
// and HasRecordKeys
#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    bilrost::Message,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct PatchStateCommand(state_mut::ExternalStateMutation);
bilrost_storage_encode_decode!(PatchStateCommand);

impl HasRecordKeys for PatchStateCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.0.service_id.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct TerminateInvocationCommand(invocation::InvocationTermination);
flexbuffers_storage_encode_decode!(TerminateInvocationCommand);

impl HasRecordKeys for TerminateInvocationCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct PurgeInvocationCommand(invocation::PurgeInvocationRequest);
flexbuffers_storage_encode_decode!(PurgeInvocationCommand);

impl HasRecordKeys for PurgeInvocationCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct PurgeJournalCommand(invocation::PurgeInvocationRequest);
flexbuffers_storage_encode_decode!(PurgeJournalCommand);

impl HasRecordKeys for PurgeJournalCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct InvokeCommand(invocation::ServiceInvocation);

flexbuffers_storage_encode_decode!(InvokeCommand);

impl HasRecordKeys for InvokeCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct TruncateOutboxCommand {
    #[bilrost(1)]
    pub index: MessageIndex,

    #[bilrost(2)]
    pub partition_key_range: Keys,
}

impl HasRecordKeys for TruncateOutboxCommand {
    fn record_keys(&self) -> Keys {
        self.partition_key_range.clone()
    }
}

bilrost_storage_encode_decode!(TruncateOutboxCommand);

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct AttachInvocationCommand(invocation::AttachInvocationRequest);

flexbuffers_storage_encode_decode!(AttachInvocationCommand);

impl HasRecordKeys for AttachInvocationCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct ResumeInvocationCommand(invocation::ResumeInvocationRequest);

flexbuffers_storage_encode_decode!(ResumeInvocationCommand);

impl HasRecordKeys for ResumeInvocationCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.0.invocation_id.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct RestartAsNewInvocationCommand(invocation::RestartAsNewInvocationRequest);

flexbuffers_storage_encode_decode!(RestartAsNewInvocationCommand);

impl HasRecordKeys for RestartAsNewInvocationCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.0.invocation_id.partition_key())
    }
}

#[derive(
    Clone, Serialize, Deserialize, derive_more::Deref, derive_more::Into, derive_more::From,
)]
pub struct InvokerEffectCommand(restate_worker_api::invoker::Effect);

flexbuffers_storage_encode_decode!(InvokerEffectCommand);

impl HasRecordKeys for InvokerEffectCommand {
    fn record_keys(&self) -> restate_types::logs::Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct TimerCommand(timer::TimerKeyValue);

flexbuffers_storage_encode_decode!(TimerCommand);

impl HasRecordKeys for TimerCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id().partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct ScheduleTimerCommand(timer::TimerKeyValue);

flexbuffers_storage_encode_decode!(ScheduleTimerCommand);

impl HasRecordKeys for ScheduleTimerCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id().partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct InvocationResponseCommand(invocation::InvocationResponse);

flexbuffers_storage_encode_decode!(InvocationResponseCommand);

impl HasRecordKeys for InvocationResponseCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct NotifyGetInvocationOutputResponseCommand(invocation::GetInvocationOutputResponse);

flexbuffers_storage_encode_decode!(NotifyGetInvocationOutputResponseCommand);

impl HasRecordKeys for NotifyGetInvocationOutputResponseCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.0.target.invocation_id().partition_key())
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::Into,
    derive_more::From,
)]
pub struct NotifySignalCommand(invocation::NotifySignalRequest);

flexbuffers_storage_encode_decode!(NotifySignalCommand);

impl HasRecordKeys for NotifySignalCommand {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.partition_key())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProxyThroughCommand {
    pub invocation: InvokeCommand,

    pub proxy_partition: Keys,
}

flexbuffers_storage_encode_decode!(ProxyThroughCommand);

impl HasRecordKeys for ProxyThroughCommand {
    fn record_keys(&self) -> Keys {
        self.proxy_partition.clone()
    }
}

#[derive(Clone, derive_more::Deref, derive_more::Into, derive_more::From, bilrost::Message)]
pub struct VQSchedulerDecisionsCommand(scheduler::SchedulerDecisions);

bilrost_storage_encode_decode!(VQSchedulerDecisionsCommand);

impl HasRecordKeys for VQSchedulerDecisionsCommand {
    fn record_keys(&self) -> Keys {
        // All records in a decision are for a single partition key
        if self.0.qids.is_empty() {
            Keys::None
        } else {
            Keys::Single(self.0.qids[0].0.partition_key())
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct UpsertRuleBookCommand {
    #[bilrost(tag(1))]
    pub partition_key_range: KeyRange,
    #[bilrost(tag(2), encoding(Arced))]
    pub rule_book: Arc<RuleBook>,
}

bilrost_storage_encode_decode!(UpsertRuleBookCommand);

impl HasRecordKeys for UpsertRuleBookCommand {
    fn record_keys(&self) -> Keys {
        Keys::RangeInclusive(self.partition_key_range.into())
    }
}

// end types

// define record types

macro_rules! command {
    {@kind=$type:expr, @command=$command:path} => {
        impl Sealed for $command{}
        impl Command for $command {
            const KIND: CommandKind = $type;
        }
    };
}

command! {
    @kind=CommandKind::AnnounceLeader,
    @command=AnnounceLeaderCommand
}

command! {
    @kind=CommandKind::VersionBarrier,
    @command=VersionBarrierCommand
}

command! {
    @kind=CommandKind::UpdatePartitionDurability,
    @command=UpdatePartitionDurabilityCommand
}

command! {
    @kind=CommandKind::PatchState,
    @command=PatchStateCommand
}

command! {
    @kind=CommandKind::TerminateInvocation,
    @command=TerminateInvocationCommand
}

command! {
    @kind=CommandKind::PurgeInvocation,
    @command=PurgeInvocationCommand
}

command! {
    @kind=CommandKind::PurgeJournal,
    @command=PurgeJournalCommand
}

command! {
    @kind=CommandKind::Invoke,
    @command=InvokeCommand
}

command! {
    @kind=CommandKind::TruncateOutbox,
    @command=TruncateOutboxCommand
}

command! {
    @kind=CommandKind::ProxyThrough,
    @command=ProxyThroughCommand
}

command! {
    @kind=CommandKind::AttachInvocation,
    @command=AttachInvocationCommand
}

command! {
    @kind=CommandKind::ResumeInvocation,
    @command=ResumeInvocationCommand
}

command! {
    @kind=CommandKind::RestartAsNewInvocation,
    @command=RestartAsNewInvocationCommand
}

command! {
    @kind=CommandKind::InvokerEffect,
    @command=InvokerEffectCommand
}

command! {
    @kind=CommandKind::Timer,
    @command=TimerCommand
}

command! {
    @kind=CommandKind::ScheduleTimer,
    @command=ScheduleTimerCommand
}

command! {
    @kind=CommandKind::InvocationResponse,
    @command=InvocationResponseCommand
}

command! {
    @kind=CommandKind::NotifyGetInvocationOutputResponse,
    @command=NotifyGetInvocationOutputResponseCommand
}

command! {
    @kind=CommandKind::NotifySignal,
    @command=NotifySignalCommand
}

command! {
    @kind=CommandKind::UpsertSchema,
    @command=UpsertSchemaCommand
}

command! {
    @kind=CommandKind::VQSchedulerDecisions,
    @command=VQSchedulerDecisionsCommand
}

command! {
    @kind=CommandKind::UpsertRuleBook,
    @command=UpsertRuleBookCommand
}
