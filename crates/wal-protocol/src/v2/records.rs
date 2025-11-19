// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::{
    bilrost_storage_encode_decode, flexbuffers_storage_encode_decode,
    identifiers::{InvocationId, WithInvocationId, WithPartitionKey},
    invocation,
    logs::{HasRecordKeys, Keys},
    message::MessageIndex,
    state_mut,
};
use serde::{Deserialize, Serialize};

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
pub struct ExternalStateMutation(state_mut::ExternalStateMutation);
bilrost_storage_encode_decode!(ExternalStateMutation);

impl HasRecordKeys for ExternalStateMutation {
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
pub struct InvocationTermination(invocation::InvocationTermination);
flexbuffers_storage_encode_decode!(InvocationTermination);

impl HasRecordKeys for InvocationTermination {
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
pub struct PurgeInvocationRequest(invocation::PurgeInvocationRequest);
flexbuffers_storage_encode_decode!(PurgeInvocationRequest);

impl WithInvocationId for PurgeInvocationRequest {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
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
pub struct ServiceInvocation(Box<invocation::ServiceInvocation>);

flexbuffers_storage_encode_decode!(ServiceInvocation);

impl HasRecordKeys for ServiceInvocation {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.invocation_id.partition_key())
    }
}

#[derive(
    Debug, Clone, Copy, bilrost::Message, derive_more::Deref, derive_more::Into, derive_more::From,
)]
pub struct TruncateOutboxRequest {
    #[bilrost(1)]
    #[from]
    #[into]
    pub index: MessageIndex,
}

bilrost_storage_encode_decode!(TruncateOutboxRequest);

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
pub struct AttachInvocationRequest(invocation::AttachInvocationRequest);

flexbuffers_storage_encode_decode!(AttachInvocationRequest);

impl HasRecordKeys for AttachInvocationRequest {
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
pub struct ResumeInvocationRequest(invocation::ResumeInvocationRequest);

flexbuffers_storage_encode_decode!(ResumeInvocationRequest);

impl HasRecordKeys for ResumeInvocationRequest {
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
pub struct RestartAsNewInvocationRequest(invocation::RestartAsNewInvocationRequest);

flexbuffers_storage_encode_decode!(RestartAsNewInvocationRequest);

impl HasRecordKeys for RestartAsNewInvocationRequest {
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
pub struct Effect(Box<restate_invoker_api::Effect>);

flexbuffers_storage_encode_decode!(Effect);

impl HasRecordKeys for Effect {
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
pub struct TimerKeyValue(timer::TimerKeyValue);

flexbuffers_storage_encode_decode!(TimerKeyValue);

impl HasRecordKeys for TimerKeyValue {
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
pub struct GetInvocationOutputResponse(invocation::GetInvocationOutputResponse);

flexbuffers_storage_encode_decode!(GetInvocationOutputResponse);

impl HasRecordKeys for GetInvocationOutputResponse {
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
pub struct NotifySignalRequest(invocation::NotifySignalRequest);

flexbuffers_storage_encode_decode!(NotifySignalRequest);

impl HasRecordKeys for NotifySignalRequest {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.partition_key())
    }
}

// end types

// define record types

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
    @payload=Effect
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
    @payload=InvocationResponsePayload
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
