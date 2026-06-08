// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::{future, iter};

use restate_types::sharding::KeyRange;
use restate_worker_api::invoker::{InvocationStatusReport, StatusHandle};
use restate_worker_api::{SchedulerStatusEntry, UserLimitCounterEntry};

use crate::context::PartitionLeaderStatusHandle;

#[derive(Clone, Debug)]
pub struct EmptyInvokerStatusHandle;

impl StatusHandle for EmptyInvokerStatusHandle {
    type Iterator = iter::Empty<InvocationStatusReport>;

    fn read_status(&self, _keys: KeyRange) -> impl Future<Output = Self::Iterator> + Send {
        future::ready(iter::empty())
    }
}

impl PartitionLeaderStatusHandle for EmptyInvokerStatusHandle {
    type SchedulerStatus = SchedulerStatusEntry;
    type SchedulerStatusIterator = std::iter::Empty<Self::SchedulerStatus>;

    type UserLimitCounter = UserLimitCounterEntry;
    type UserLimitCounterIterator = std::iter::Empty<Self::UserLimitCounter>;

    fn read_scheduler_status(
        &self,
        _keys: KeyRange,
    ) -> impl Future<Output = Self::SchedulerStatusIterator> + Send {
        future::ready(iter::empty())
    }

    fn read_user_limit_counters(
        &self,
        _keys: KeyRange,
    ) -> impl Future<Output = Self::UserLimitCounterIterator> + Send {
        future::ready(iter::empty())
    }
}
