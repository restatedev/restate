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
use super::{Record, RecordKind};
use crate::timer::{self};

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
pub struct ExternalStateMutationPayload(state_mut::ExternalStateMutation);
bilrost_storage_encode_decode!(ExternalStateMutationPayload);

impl HasRecordKeys for ExternalStateMutationPayload {
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
pub struct InvocationTerminationPayload(invocation::InvocationTermination);
flexbuffers_storage_encode_decode!(InvocationTerminationPayload);

impl HasRecordKeys for InvocationTerminationPayload {
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
pub struct PurgeInvocationRequestPayload(invocation::PurgeInvocationRequest);
flexbuffers_storage_encode_decode!(PurgeInvocationRequestPayload);

impl HasRecordKeys for PurgeInvocationRequestPayload {
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
pub struct ServiceInvocationPayload(Box<invocation::ServiceInvocation>);

flexbuffers_storage_encode_decode!(ServiceInvocationPayload);

impl HasRecordKeys for ServiceInvocationPayload {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct TruncateOutboxPayload {
    #[bilrost(1)]
    pub index: MessageIndex,

    #[bilrost(2)]
    pub partition_key_range: Keys,
}

impl HasRecordKeys for TruncateOutboxPayload {
    fn record_keys(&self) -> Keys {
        self.partition_key_range.clone()
    }
}

bilrost_storage_encode_decode!(TruncateOutboxPayload);

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
pub struct AttachInvocationRequestPayload(invocation::AttachInvocationRequest);

flexbuffers_storage_encode_decode!(AttachInvocationRequestPayload);

impl HasRecordKeys for AttachInvocationRequestPayload {
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
pub struct ResumeInvocationRequestPayload(invocation::ResumeInvocationRequest);

flexbuffers_storage_encode_decode!(ResumeInvocationRequestPayload);

impl HasRecordKeys for ResumeInvocationRequestPayload {
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
pub struct RestartAsNewInvocationRequestPayload(invocation::RestartAsNewInvocationRequest);

flexbuffers_storage_encode_decode!(RestartAsNewInvocationRequestPayload);

impl HasRecordKeys for RestartAsNewInvocationRequestPayload {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.0.invocation_id.partition_key())
    }
}

#[derive(
    Clone, Serialize, Deserialize, derive_more::Deref, derive_more::Into, derive_more::From,
)]
pub struct EffectPayload(Box<restate_worker_api::invoker::Effect>);

flexbuffers_storage_encode_decode!(EffectPayload);

impl HasRecordKeys for EffectPayload {
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
pub struct TimerKeyValuePayload(timer::TimerKeyValue);

flexbuffers_storage_encode_decode!(TimerKeyValuePayload);

impl HasRecordKeys for TimerKeyValuePayload {
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
pub struct InvocationResponsePayload(invocation::InvocationResponse);

flexbuffers_storage_encode_decode!(InvocationResponsePayload);

impl HasRecordKeys for InvocationResponsePayload {
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
pub struct GetInvocationOutputResponsePayload(invocation::GetInvocationOutputResponse);

flexbuffers_storage_encode_decode!(GetInvocationOutputResponsePayload);

impl HasRecordKeys for GetInvocationOutputResponsePayload {
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
pub struct NotifySignalRequestPayload(invocation::NotifySignalRequest);

flexbuffers_storage_encode_decode!(NotifySignalRequestPayload);

impl HasRecordKeys for NotifySignalRequestPayload {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.partition_key())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProxyThroughPayload {
    pub invocation: ServiceInvocationPayload,

    pub proxy_partition: Keys,
}

flexbuffers_storage_encode_decode!(ProxyThroughPayload);

impl HasRecordKeys for ProxyThroughPayload {
    fn record_keys(&self) -> Keys {
        self.proxy_partition.clone()
    }
}

#[derive(Clone, derive_more::Deref, derive_more::Into, derive_more::From, bilrost::Message)]
pub struct SchedulerDecisionsPayload(scheduler::SchedulerDecisions);

bilrost_storage_encode_decode!(SchedulerDecisionsPayload);

impl HasRecordKeys for SchedulerDecisionsPayload {
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
pub struct UpsertRuleBookPayload {
    #[bilrost(tag(1))]
    pub partition_key_range: KeyRange,
    #[bilrost(tag(2), encoding(Arced))]
    pub rule_book: Arc<RuleBook>,
}

bilrost_storage_encode_decode!(UpsertRuleBookPayload);

impl HasRecordKeys for UpsertRuleBookPayload {
    fn record_keys(&self) -> Keys {
        Keys::RangeInclusive(self.partition_key_range.into())
    }
}

// end types

// define record types

macro_rules! record {
    {@name=$name:ident, @kind=$type:expr, @payload=$payload:path} => {
        #[allow(dead_code)]
        #[derive(Clone, Copy)]
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
    @payload=ExternalStateMutationPayload
}

record! {
    @name=TerminateInvocation,
    @kind=RecordKind::TerminateInvocation,
    @payload=InvocationTerminationPayload
}

record! {
    @name=PurgeInvocation,
    @kind=RecordKind::PurgeInvocation,
    @payload=PurgeInvocationRequestPayload
}

record! {
    @name=PurgeJournal,
    @kind=RecordKind::PurgeJournal,
    @payload=PurgeInvocationRequestPayload
}

record! {
    @name=Invoke,
    @kind=RecordKind::Invoke,
    @payload=ServiceInvocationPayload
}

record! {
    @name=TruncateOutbox,
    @kind=RecordKind::TruncateOutbox,
    @payload=TruncateOutboxPayload
}

record! {
    @name=ProxyThrough,
    @kind=RecordKind::ProxyThrough,
    @payload=ProxyThroughPayload
}

record! {
    @name=AttachInvocation,
    @kind=RecordKind::AttachInvocation,
    @payload=AttachInvocationRequestPayload
}

record! {
    @name=ResumeInvocation,
    @kind=RecordKind::ResumeInvocation,
    @payload=ResumeInvocationRequestPayload
}

record! {
    @name=RestartAsNewInvocation,
    @kind=RecordKind::RestartAsNewInvocation,
    @payload=RestartAsNewInvocationRequestPayload
}

record! {
    @name=InvokerEffect,
    @kind=RecordKind::InvokerEffect,
    @payload=EffectPayload
}

record! {
    @name=Timer,
    @kind=RecordKind::Timer,
    @payload=TimerKeyValuePayload
}

record! {
    @name=ScheduleTimer,
    @kind=RecordKind::ScheduleTimer,
    @payload=TimerKeyValuePayload
}

record! {
    @name=InvocationResponse,
    @kind=RecordKind::InvocationResponse,
    @payload=InvocationResponsePayload
}

record! {
    @name=NotifyGetInvocationOutputResponse,
    @kind=RecordKind::NotifyGetInvocationOutputResponse,
    @payload=GetInvocationOutputResponsePayload
}

record! {
    @name=NotifySignal,
    @kind=RecordKind::NotifySignal,
    @payload=NotifySignalRequestPayload
}

record! {
    @name=UpsertSchema,
    @kind=RecordKind::UpsertSchema,
    @payload=crate::control::UpsertSchema
}

record! {
    @name=VQSchedulerDecisions,
    @kind=RecordKind::VQSchedulerDecisions,
    @payload=SchedulerDecisionsPayload
}

record! {
    @name=UpsertRuleBook,
    @kind=RecordKind::UpsertRuleBook,
    @payload=UpsertRuleBookPayload
}
