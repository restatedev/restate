// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{protobuf_storage_encode_decode, Result};

use bytestring::ByteString;
use futures_util::Stream;
use restate_types::identifiers::{JournalEntryId, PartitionKey, ServiceId};
use restate_types::journal::EntryResult;
use std::future::Future;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, PartialEq)]
pub enum PromiseState {
    Completed(EntryResult),
    NotCompleted(
        // Journal entries listening for this promise to be completed
        Vec<JournalEntryId>,
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

protobuf_storage_encode_decode!(Promise);

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

    fn all_promises(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<OwnedPromiseRow>> + Send;
}

pub trait PromiseTable: ReadOnlyPromiseTable {
    fn put_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        promise: &Promise,
    ) -> impl Future<Output = ()> + Send;

    fn delete_all_promises(&mut self, service_id: &ServiceId) -> impl Future<Output = ()> + Send;
}
