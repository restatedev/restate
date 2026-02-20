// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use bytes::Bytes;
use futures::Stream;

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_types::identifiers::{PartitionKey, ServiceId};

use crate::{BudgetedReadError, Result};

pub trait ReadStateTable {
    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    /// Returns a lazy stream over all user states for the given service.
    /// The stream borrows from `self` only, not from `service_id`.
    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a>;

    /// Budget-gated state stream.
    ///
    /// Each state entry's raw byte size is peeked from the underlying store
    /// **before** deserialization. A [`LocalMemoryLease`] is acquired from
    /// `budget` for that size, and only then is the entry decoded.
    fn get_all_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>>
        + Send
        + 'a,
    >;
}

pub trait ScanStateTable {
    fn for_each_user_state<
        F: FnMut((ServiceId, Bytes, &[u8])) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteStateTable {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
        state_value: impl AsRef<[u8]> + Send,
    ) -> Result<()>;

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> Result<()>;

    fn delete_all_user_state(&mut self, service_id: &ServiceId) -> Result<()>;
}
