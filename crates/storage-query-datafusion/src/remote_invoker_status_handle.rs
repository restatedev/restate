// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_invoker_api::{InvocationStatusReport, StatusHandle};
use restate_types::identifiers::PartitionKey;
use std::future::Future;
use std::ops::RangeInclusive;
use std::{future, iter};

// todo implement [`StatusHandle`] for reading the status of remote invokers.
pub struct RemoteInvokerStatusHandle;

impl StatusHandle for RemoteInvokerStatusHandle {
    type Iterator = iter::Empty<InvocationStatusReport>;

    fn read_status(
        &self,
        _keys: RangeInclusive<PartitionKey>,
    ) -> impl Future<Output = Self::Iterator> + Send {
        future::ready(iter::empty())
    }
}
