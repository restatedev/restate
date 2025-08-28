// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::ops::RangeInclusive;

use bytes::Bytes;
use bytestring::ByteString;

use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{PartitionKey, ServiceId};
use restate_types::invocation::JournalCompletionTarget;
use restate_types::journal::{CompletionResult, EntryResult};
use restate_types::journal_v2::{
    CompletePromiseValue, Failure, GetPromiseResult, PeekPromiseResult,
};

use super::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromiseResult {
    Success(Bytes),
    Failure(InvocationErrorCode, ByteString),
}

impl From<PromiseResult> for GetPromiseResult {
    fn from(value: PromiseResult) -> Self {
        match value {
            PromiseResult::Success(s) => GetPromiseResult::Success(s),
            PromiseResult::Failure(code, message) => {
                GetPromiseResult::Failure(Failure { code, message })
            }
        }
    }
}

impl From<PromiseResult> for PeekPromiseResult {
    fn from(value: PromiseResult) -> Self {
        match value {
            PromiseResult::Success(s) => PeekPromiseResult::Success(s),
            PromiseResult::Failure(code, message) => {
                PeekPromiseResult::Failure(Failure { code, message })
            }
        }
    }
}

impl From<CompletePromiseValue> for PromiseResult {
    fn from(value: CompletePromiseValue) -> Self {
        match value {
            CompletePromiseValue::Success(b) => Self::Success(b),
            CompletePromiseValue::Failure(f) => Self::Failure(f.code, f.message),
        }
    }
}

impl From<PromiseResult> for CompletionResult {
    fn from(value: PromiseResult) -> Self {
        match value {
            PromiseResult::Success(s) => CompletionResult::Success(s),
            PromiseResult::Failure(code, message) => CompletionResult::Failure(code, message),
        }
    }
}

impl From<EntryResult> for PromiseResult {
    fn from(value: EntryResult) -> Self {
        match value {
            EntryResult::Success(b) => Self::Success(b),
            EntryResult::Failure(code, message) => Self::Failure(code, message),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PromiseState {
    Completed(PromiseResult),
    NotCompleted(
        // Journal entries listening for this promise to be completed
        Vec<JournalCompletionTarget>,
    ),
}

impl Default for PromiseState {
    fn default() -> Self {
        PromiseState::NotCompleted(vec![])
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Promise {
    pub state: PromiseState,
}

impl PartitionStoreProtobufValue for Promise {
    type ProtobufType = crate::protobuf_types::v1::Promise;
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnedPromiseRow {
    pub service_id: ServiceId,
    pub key: ByteString,
    pub metadata: Promise,
}

pub trait ReadOnlyPromiseTable {
    fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> impl Future<Output = Result<Option<Promise>>> + Send;
}

pub trait ScanPromiseTable {
    fn for_each_promise<
        F: FnMut(OwnedPromiseRow) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait PromiseTable: ReadOnlyPromiseTable {
    fn put_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        promise: &Promise,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_all_promises(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<()>> + Send;
}
