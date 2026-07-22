// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::Bifrost;
use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_worker_api::invoker::capacity::InvokerCapacity;

use crate::RuleBookCacheHandle;
use crate::partition_processor_manager::PartitionLeaderHandlesRegistry;

#[derive(Clone)]
#[non_exhaustive]
pub struct NodeContext {
    my_node_id: GenerationalNodeId,
    pub invoker_capacity: InvokerCapacity,
    pub config: Live<Configuration>,
    pub replica_set_states: PartitionReplicaSetStates,
    /// Handle into the node-level rule-book cache. The apply path
    /// notifies the cache when it learns about a newer book from
    /// Bifrost replay, so leader-state subscribers on the same node
    /// see it without waiting for the next metadata-store poll.
    pub rule_book_cache: RuleBookCacheHandle,
    pub bifrost: Bifrost,
    pub leader_handles_registry: PartitionLeaderHandlesRegistry,
}

impl NodeContext {
    pub fn new(
        my_node_id: GenerationalNodeId,
        config: Live<Configuration>,
        replica_set_states: PartitionReplicaSetStates,
        rule_book_cache: RuleBookCacheHandle,
        bifrost: Bifrost,
        invoker_capacity: InvokerCapacity,
        leader_handles_registry: PartitionLeaderHandlesRegistry,
    ) -> Self {
        Self {
            my_node_id,
            replica_set_states,
            rule_book_cache,
            bifrost,
            invoker_capacity,
            leader_handles_registry,
            config,
        }
    }

    #[inline]
    pub fn my_node_id(&self) -> GenerationalNodeId {
        self.my_node_id
    }
}
