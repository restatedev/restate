// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Raw entries carry the serialized representation of entries.

use super::*;

use crate::invocation::Header;
use std::fmt::Debug;

/// This struct represents headers as they are received from the wire.
pub type PlainEntryHeader = EntryHeader<(), ()>;
pub type PlainRawEntry = RawEntry<(), ()>;

/// This struct represents a serialized journal entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEntry<InvokeEnrichmentResult, AwakeableEnrichmentResult> {
    header: EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
    entry: Bytes,
}

impl<InvokeEnrichmentResult, AwakeableEnrichmentResult>
    RawEntry<InvokeEnrichmentResult, AwakeableEnrichmentResult>
{
    pub const fn new(
        header: EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
        entry: Bytes,
    ) -> Self {
        Self { header, entry }
    }

    pub fn into_inner(
        self,
    ) -> (
        EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
        Bytes,
    ) {
        (self.header, self.entry)
    }

    pub fn header(&self) -> &EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult> {
        &self.header
    }

    pub fn header_mut(
        &mut self,
    ) -> &mut EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult> {
        &mut self.header
    }

    pub fn serialized_entry(&self) -> &Bytes {
        &self.entry
    }

    pub fn serialized_entry_mut(&mut self) -> &mut Bytes {
        &mut self.entry
    }

    pub fn ty(&self) -> EntryType {
        self.header.as_entry_type()
    }

    pub fn map_header<F, TargetInvokeEnrichmentResult, TargetAwakeableEnrichmentResult>(
        self,
        mapper: F,
    ) -> RawEntry<TargetInvokeEnrichmentResult, TargetAwakeableEnrichmentResult>
    where
        F: FnOnce(
            EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
            &Bytes,
        )
            -> EntryHeader<TargetInvokeEnrichmentResult, TargetAwakeableEnrichmentResult>,
    {
        let new_header = mapper(self.header, &self.entry);
        RawEntry {
            header: new_header,
            entry: self.entry,
        }
    }

    pub fn deserialize_entry<Codec: RawEntryCodec>(self) -> Result<Entry, RawEntryCodecError> {
        Codec::deserialize(self.ty(), self.entry)
    }

    pub fn deserialize_entry_ref<Codec: RawEntryCodec>(&self) -> Result<Entry, RawEntryCodecError> {
        Codec::deserialize(self.ty(), self.entry.clone())
    }

    pub fn deserialize_name<Codec: RawEntryCodec>(
        &self,
    ) -> Result<Option<String>, RawEntryCodecError> {
        Codec::read_entry_name(self.ty(), self.entry.clone())
    }

    pub fn erase_enrichment(self) -> PlainRawEntry {
        self.map_header(|h, _| h.erase_enrichment())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EntryHeader<CallEnrichmentResult, AwakeableEnrichmentResult> {
    Input,
    Output,
    GetState {
        is_completed: bool,
    },
    SetState,
    ClearState,
    GetStateKeys {
        is_completed: bool,
    },
    ClearAllState,
    GetPromise {
        is_completed: bool,
    },
    PeekPromise {
        is_completed: bool,
    },
    CompletePromise {
        is_completed: bool,
    },
    Sleep {
        is_completed: bool,
    },
    Call {
        is_completed: bool,
        enrichment_result: Option<CallEnrichmentResult>,
    },
    OneWayCall {
        enrichment_result: CallEnrichmentResult,
    },
    Awakeable {
        is_completed: bool,
    },
    CompleteAwakeable {
        enrichment_result: AwakeableEnrichmentResult,
    },
    Run,
    CancelInvocation,
    GetCallInvocationId {
        is_completed: bool,
    },
    AttachInvocation {
        is_completed: bool,
    },
    GetInvocationOutput {
        is_completed: bool,
    },
    Custom {
        code: u16,
    },
}

impl<InvokeEnrichmentResult, AwakeableEnrichmentResult>
    EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult>
{
    pub fn is_completed(&self) -> Option<bool> {
        match self {
            EntryHeader::Input => None,
            EntryHeader::Output => None,
            EntryHeader::GetState { is_completed, .. } => Some(*is_completed),
            EntryHeader::SetState => None,
            EntryHeader::ClearState => None,
            EntryHeader::ClearAllState => None,
            EntryHeader::GetStateKeys { is_completed, .. } => Some(*is_completed),
            EntryHeader::Sleep { is_completed, .. } => Some(*is_completed),
            EntryHeader::Call { is_completed, .. } => Some(*is_completed),
            EntryHeader::OneWayCall { .. } => None,
            EntryHeader::Awakeable { is_completed, .. } => Some(*is_completed),
            EntryHeader::CompleteAwakeable { .. } => None,
            EntryHeader::Run => None,
            EntryHeader::Custom { .. } => None,
            EntryHeader::GetPromise { is_completed } => Some(*is_completed),
            EntryHeader::PeekPromise { is_completed } => Some(*is_completed),
            EntryHeader::CompletePromise { is_completed } => Some(*is_completed),
            EntryHeader::CancelInvocation => None,
            EntryHeader::GetCallInvocationId { is_completed } => Some(*is_completed),
            EntryHeader::AttachInvocation { is_completed } => Some(*is_completed),
            EntryHeader::GetInvocationOutput { is_completed } => Some(*is_completed),
        }
    }

    pub fn mark_completed(&mut self) {
        match self {
            EntryHeader::Input => {}
            EntryHeader::Output => {}
            EntryHeader::GetState { is_completed, .. } => *is_completed = true,
            EntryHeader::SetState => {}
            EntryHeader::ClearState => {}
            EntryHeader::GetStateKeys { is_completed, .. } => *is_completed = true,
            EntryHeader::ClearAllState => {}
            EntryHeader::Sleep { is_completed, .. } => *is_completed = true,
            EntryHeader::Call { is_completed, .. } => *is_completed = true,
            EntryHeader::OneWayCall { .. } => {}
            EntryHeader::Awakeable { is_completed, .. } => *is_completed = true,
            EntryHeader::CompleteAwakeable { .. } => {}
            EntryHeader::Run => {}
            EntryHeader::Custom { .. } => {}
            EntryHeader::GetPromise { is_completed } => *is_completed = true,
            EntryHeader::PeekPromise { is_completed } => *is_completed = true,
            EntryHeader::CompletePromise { is_completed } => *is_completed = true,
            EntryHeader::CancelInvocation => {}
            EntryHeader::GetCallInvocationId { is_completed } => *is_completed = true,
            EntryHeader::AttachInvocation { is_completed } => *is_completed = true,
            EntryHeader::GetInvocationOutput { is_completed } => *is_completed = true,
        }
    }

    pub fn as_entry_type(&self) -> EntryType {
        match self {
            EntryHeader::Input => EntryType::Input,
            EntryHeader::Output => EntryType::Output,
            EntryHeader::GetState { .. } => EntryType::GetState,
            EntryHeader::SetState => EntryType::SetState,
            EntryHeader::ClearState => EntryType::ClearState,
            EntryHeader::GetStateKeys { .. } => EntryType::GetStateKeys,
            EntryHeader::ClearAllState => EntryType::ClearAllState,
            EntryHeader::Sleep { .. } => EntryType::Sleep,
            EntryHeader::Call { .. } => EntryType::Call,
            EntryHeader::OneWayCall { .. } => EntryType::OneWayCall,
            EntryHeader::Awakeable { .. } => EntryType::Awakeable,
            EntryHeader::CompleteAwakeable { .. } => EntryType::CompleteAwakeable,
            EntryHeader::Run => EntryType::Run,
            EntryHeader::Custom { .. } => EntryType::Custom,
            EntryHeader::GetPromise { .. } => EntryType::GetPromise,
            EntryHeader::PeekPromise { .. } => EntryType::PeekPromise,
            EntryHeader::CompletePromise { .. } => EntryType::CompletePromise,
            EntryHeader::CancelInvocation => EntryType::CancelInvocation,
            EntryHeader::GetCallInvocationId { .. } => EntryType::GetCallInvocationId,
            EntryHeader::AttachInvocation { .. } => EntryType::AttachInvocation,
            EntryHeader::GetInvocationOutput { .. } => EntryType::GetInvocationOutput,
        }
    }

    pub fn erase_enrichment(self) -> PlainEntryHeader {
        match self {
            EntryHeader::Input => EntryHeader::Input {},
            EntryHeader::Output => EntryHeader::Output {},
            EntryHeader::GetState { is_completed } => EntryHeader::GetState { is_completed },
            EntryHeader::SetState => EntryHeader::SetState {},
            EntryHeader::ClearState => EntryHeader::ClearState {},
            EntryHeader::GetStateKeys { is_completed } => {
                EntryHeader::GetStateKeys { is_completed }
            }
            EntryHeader::ClearAllState => EntryHeader::ClearAllState,
            EntryHeader::Sleep { is_completed } => EntryHeader::Sleep { is_completed },
            EntryHeader::Call { is_completed, .. } => EntryHeader::Call {
                is_completed,
                enrichment_result: None,
            },
            EntryHeader::OneWayCall { .. } => EntryHeader::OneWayCall {
                enrichment_result: (),
            },
            EntryHeader::Awakeable { is_completed } => EntryHeader::Awakeable { is_completed },
            EntryHeader::CompleteAwakeable { .. } => EntryHeader::CompleteAwakeable {
                enrichment_result: (),
            },
            EntryHeader::Run => EntryHeader::Run {},
            EntryHeader::Custom { code } => EntryHeader::Custom { code },
            EntryHeader::GetPromise { is_completed } => EntryHeader::GetPromise { is_completed },
            EntryHeader::PeekPromise { is_completed } => EntryHeader::PeekPromise { is_completed },
            EntryHeader::CompletePromise { is_completed } => {
                EntryHeader::CompletePromise { is_completed }
            }
            EntryHeader::CancelInvocation => EntryHeader::CancelInvocation,
            EntryHeader::GetCallInvocationId { is_completed } => {
                EntryHeader::GetCallInvocationId { is_completed }
            }
            EntryHeader::AttachInvocation { is_completed } => {
                EntryHeader::AttachInvocation { is_completed }
            }
            EntryHeader::GetInvocationOutput { is_completed } => {
                EntryHeader::GetInvocationOutput { is_completed }
            }
        }
    }
}

// -- Codec for RawEntry

#[derive(Debug, thiserror::Error)]
#[error("Cannot decode {ty:?}. {kind:?}")]
pub struct RawEntryCodecError {
    ty: EntryType,
    kind: ErrorKind,
}

impl RawEntryCodecError {
    pub fn new(ty: EntryType, kind: ErrorKind) -> Self {
        Self { ty, kind }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("failed to decode: {source:?}")]
    Decode {
        #[source]
        source: Option<anyhow::Error>,
    },
    #[error("Field '{0}' is missing")]
    MissingField(&'static str),
}

pub trait RawEntryCodec {
    fn serialize_as_input_entry(
        headers: Vec<Header>,
        input_message: Bytes,
    ) -> enriched::EnrichedRawEntry;

    fn serialize_get_state_keys_completion(keys: Vec<Bytes>) -> CompletionResult;

    fn deserialize(entry_type: EntryType, entry_value: Bytes) -> Result<Entry, RawEntryCodecError>;

    fn read_entry_name(
        entry_type: EntryType,
        entry_value: Bytes,
    ) -> Result<Option<String>, RawEntryCodecError>;

    fn write_completion<InvokeEnrichmentResult: Debug, AwakeableEnrichmentResult: Debug>(
        entry: &mut RawEntry<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError>;
}
