// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use bytes::Bytes;
use futures_util::Stream;
use restate_types::identifiers::{PartitionKey, ServiceId};
use std::future::Future;
use std::ops::RangeInclusive;

pub trait ReadOnlyStateTable {
    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    fn get_all_user_states_for_service(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = Result<(Bytes, Bytes)>> + Send;

    fn get_all_user_states(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(ServiceId, Bytes, Bytes)>> + Send;
}

pub trait StateTable: ReadOnlyStateTable {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send;

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send;

    fn delete_all_user_state(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<()>> + Send;
}
