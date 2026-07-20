// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::TransportConnect;
use restate_partition_store::PartitionStore;
use restate_types::protobuf::cluster::DetailedRunMode;
use restate_vqueues::context::HasVQueuesMut;
use restate_worker_api::processor::Processor;

use crate::partition::leadership::LeadershipState;
use crate::partition::leadership::trim_queue::HasTrimQueue;
use crate::partition::{NodeContext, ProcessorError};

/// The leadership promotion controls
pub trait LeaderPromotion {
    async fn on_barrier_applied(
        &mut self,
        node_ctx: &mut NodeContext,
        processor: impl Processor + HasTrimQueue + HasVQueuesMut,
    ) -> Result<(), ProcessorError>;

    fn current_mode(&self) -> DetailedRunMode;
}

pub(crate) struct LeadershipContext<'a, T> {
    pub leadership: &'a mut LeadershipState<T>,
    pub partition_store: &'a mut PartitionStore,
}

impl<T: TransportConnect> LeaderPromotion for LeadershipContext<'_, T> {
    fn current_mode(&self) -> DetailedRunMode {
        self.leadership.detailed_effective_mode()
    }

    async fn on_barrier_applied(
        &mut self,
        node_ctx: &mut NodeContext,
        processor: impl Processor + HasTrimQueue + HasVQueuesMut,
    ) -> Result<(), ProcessorError> {
        self.leadership
            .finish_becoming_leader(node_ctx, processor, self.partition_store)
            .await?;
        Ok(())
    }
}
