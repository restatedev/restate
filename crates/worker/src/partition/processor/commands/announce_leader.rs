// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::DataRecord;
use restate_core::network::TransportConnect;
use restate_partition_store::{PartitionStore, PartitionStoreTransaction};
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::CachedEpochMetadata;
use restate_vqueues::context::HasVQueuesMut;
use restate_wal_protocol::control::AnnounceLeaderCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::partition::leadership::LeadershipState;
use crate::partition::leadership::trim_queue::HasTrimQueue;
use crate::partition::processor::*;
use crate::partition::state_machine::ActionCollector;
use crate::partition::{NodeContext, ProcessorError};

pub struct AnnounceLeaderContext<'a, 'b, P, T> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub node_ctx: &'a mut NodeContext,
    pub processor: P,
    pub partition_store: &'a mut PartitionStore,
    pub action_collector: &'a mut ActionCollector,
    pub leadership: &'a mut LeadershipState<T>,
}

impl<P: Processor + HasFsmMut + HasVQueuesMut + HasTrimQueue + HasStatusMut, T: TransportConnect>
    ApplyPartitionCommand<AnnounceLeaderCommand> for AnnounceLeaderContext<'_, '_, P, T>
{
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<AnnounceLeaderCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        let lsn = command.seq();
        let (header, announce_leader) = command.into_inner().split()?;
        // As it stands. AnnounceLeaderCommand will only be processed (here) iff the message
        // carries a higher epoch than the current one. This is done via the deduplication
        // mechanism. `AnnounceLeader` commands are ignored when their deduplication information
        // (ESN) is lower than any previously seen ESN on the "self" producer.

        // Commit all changes so far, this is important so that the actuators see all changes
        // when becoming leader.
        self.txn.commit().await?;
        // We can ignore all actions collected so far because as a new leader we have to instruct the
        // actuators afresh.
        self.action_collector.clear();

        // update partition store with latest epoch metadata
        if let Some(current_config) = &announce_leader.current_config {
            let announced = CachedEpochMetadata {
                version: announce_leader.epoch_version.unwrap(),
                leader_node_id: announce_leader.node_id,
                leader_epoch: announce_leader.leader_epoch,
                current: current_config.to_current_replica_set_state(),
                next: announce_leader
                    .next_config
                    .as_ref()
                    .map(|v| v.to_next_replica_set_state()),
            };

            self.processor
                .fsm_mut()
                .set_epoch_metadata(self.txn, announced);
        };

        // Setting this node as the winning leader. We fence off older epochs indirectly
        // via the deduplication table.
        self.processor
            .status_mut()
            .set_last_observed_leader_epoch(announce_leader.leader_epoch);
        self.processor
            .status_mut()
            .set_last_observed_leader_node(announce_leader.node_id);

        // Are we the leaders now?
        self.leadership
            .on_announce_leader(
                self.node_ctx,
                &mut self.processor,
                self.partition_store,
                announce_leader.leader_epoch,
            )
            .await?;

        if self.leadership.is_leader()
            && let Some(cached) = self.processor.fsm().epoch_metadata()
        {
            self.node_ctx.replica_set_states.note_observed_membership(
                self.processor.partition_id(),
                restate_types::partitions::state::LeadershipState {
                    current_leader_epoch: cached.leader_epoch,
                    current_leader: cached.leader_node_id,
                },
                &cached.current.replica_set,
                &cached.next.as_ref().map(|c| &c.replica_set).cloned(),
            );
        }

        self.node_ctx.replica_set_states.note_observed_leader(
            self.processor.partition_id(),
            restate_types::partitions::state::LeadershipState {
                current_leader_epoch: announce_leader.leader_epoch,
                current_leader: announce_leader.node_id,
            },
        );

        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}
