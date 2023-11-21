// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetFuture, GetStream, PutFuture};
use bytes::Bytes;
use restate_types::identifiers::ServiceId;

pub trait StateTable {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> GetFuture<Option<Bytes>>;

    fn get_all_user_states(&mut self, service_id: &ServiceId) -> GetStream<(Bytes, Bytes)>;
}
