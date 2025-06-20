// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_types::identifiers::{PartitionKey, ServiceId};

use crate::Result;

pub trait ReadOnlyStateTable {
    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    fn get_all_user_states_for_service(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send>;
}

pub trait ScanStateTable {
    fn scan_all_user_states(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(ServiceId, Bytes, Bytes)>> + Send>;
}

pub trait StateTable: ReadOnlyStateTable {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
        state_value: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_all_user_state(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<()>> + Send;
}
