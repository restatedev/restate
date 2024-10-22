// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod appender;

use std::sync::{atomic::AtomicU32, Arc};

use tokio::sync::Semaphore;
use tracing::debug;

use restate_core::{
    network::{rpc_router::RpcRouter, Networking, TransportConnect},
    task_center, ShutdownError, TaskKind,
};
use restate_types::{
    config::Configuration,
    logs::{LogletOffset, Record, RecordCache, SequenceNumber},
    net::log_server::Store,
    replicated_loglet::{NodeSet, ReplicatedLogletId, ReplicatedLogletParams, ReplicationProperty},
    GenerationalNodeId,
};

use self::appender::SequencerAppender;
use super::{
    log_server_manager::RemoteLogServerManager,
    replication::spread_selector::{SelectorStrategy, SpreadSelector},
};
use crate::loglet::{util::TailOffsetWatch, LogletCommit, OperationError};

#[derive(thiserror::Error, Debug)]
pub enum SequencerError {
    #[error("loglet offset exhausted")]
    LogletOffsetExhausted,
    #[error("batch exceeds possible length")]
    InvalidBatchLength,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Sequencer shared state
pub struct SequencerSharedState {
    next_write_offset: AtomicU32,
    my_node_id: GenerationalNodeId,
    my_params: ReplicatedLogletParams,
    committed_tail: TailOffsetWatch,
    selector: SpreadSelector,
    record_cache: RecordCache,
}

impl SequencerSharedState {
    pub fn my_node_id(&self) -> &GenerationalNodeId {
        &self.my_node_id
    }

    pub fn replication(&self) -> &ReplicationProperty {
        &self.my_params.replication
    }

    pub fn nodeset(&self) -> &NodeSet {
        &self.my_params.nodeset
    }

    pub fn my_params(&self) -> &ReplicatedLogletParams {
        &self.my_params
    }

    pub fn loglet_id(&self) -> &ReplicatedLogletId {
        &self.my_params.loglet_id
    }

    pub fn global_committed_tail(&self) -> &TailOffsetWatch {
        &self.committed_tail
    }
}

/// This represents the leader sequencer for a loglet. The leader sequencer is the sole writer
/// and we guarantee that only one leader sequencer is alive per loglet-id by tying its lifetime
/// to the generation of this node-id.
pub struct Sequencer<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    log_server_manager: RemoteLogServerManager,
    networking: Networking<T>,
    rpc_router: RpcRouter<Store>,
    /// The value we read from configuration, we keep it around because we can't get the original
    /// capacity directly from `record_permits` Semaphore.
    max_in_flight_records_in_config: usize,
    /// Semaphore for the number of records in-flight.
    /// This is an Arc<> to allow sending owned permits
    record_permits: Arc<Semaphore>,
}

impl<T: TransportConnect> Sequencer<T> {
    /// Create a new sequencer instance
    pub fn new(
        my_params: ReplicatedLogletParams,
        selector_strategy: SelectorStrategy,
        networking: Networking<T>,
        rpc_router: RpcRouter<Store>,
        log_server_manager: RemoteLogServerManager,
        record_cache: RecordCache,
        global_tail: TailOffsetWatch,
    ) -> Self {
        let my_node_id = networking.my_node_id();
        let initial_tail = global_tail.latest_offset();
        // Leader sequencers start on an empty loglet offset range
        debug_assert_eq!(LogletOffset::OLDEST, initial_tail);
        let next_write_offset = AtomicU32::new(*initial_tail);

        let selector = SpreadSelector::new(
            my_params.nodeset.clone(),
            selector_strategy,
            my_params.replication.clone(),
        );

        let max_in_flight_records_in_config: usize = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .maximum_inflight_records
            .into();

        let record_permits = Arc::new(Semaphore::new(max_in_flight_records_in_config));
        // shared state with appenders
        let sequencer_shared_state = Arc::new(SequencerSharedState {
            my_node_id,
            my_params,
            selector,
            next_write_offset,
            record_cache,
            committed_tail: global_tail,
        });

        Self {
            sequencer_shared_state,
            log_server_manager,
            rpc_router,
            networking,
            record_permits,
            max_in_flight_records_in_config,
        }
    }

    pub fn sequencer_state(&self) -> &SequencerSharedState {
        &self.sequencer_shared_state
    }

    /// Number of records that can be added to the sequencer before exhausting it in-flight
    /// capacity
    pub fn available_capacity(&self) -> usize {
        self.record_permits.available_permits()
    }

    pub async fn enqueue_batch(
        &self,
        payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, OperationError> {
        if self
            .sequencer_shared_state
            .global_committed_tail()
            .is_sealed()
        {
            return Ok(LogletCommit::sealed());
        }

        if payloads.len() > self.max_in_flight_records_in_config {
            let delta = payloads.len() - self.max_in_flight_records_in_config;
            debug!(
                "Resizing sequencer in-flight records capacity to allow admission for this batch. \
                Capacity in configuration is {} and we are adding capacity of {} to it",
                self.max_in_flight_records_in_config, delta
            );
            self.record_permits.add_permits(delta);
        }

        let len = u32::try_from(payloads.len()).expect("batch sizes fit in u32");
        let permit = self
            .record_permits
            .clone()
            .acquire_many_owned(len)
            .await
            .unwrap();

        // Why is this AclRel?
        // We are updating the next write offset and we want to make sure that after this call that
        // we observe if task_center()'s shutdown signal was set or not consistently across
        // threads.
        //
        // The situation we want to avoid is that we fail to spawn an appender due to shutdown but
        // the subsequent fetch_add don't observe task-center's internal shutdown atomic.
        let offset = LogletOffset::new(
            self.sequencer_shared_state
                .next_write_offset
                .fetch_add(len, std::sync::atomic::Ordering::AcqRel),
        );

        let (loglet_commit, commit_resolver) = LogletCommit::deferred();

        let appender = SequencerAppender::new(
            Arc::clone(&self.sequencer_shared_state),
            self.log_server_manager.clone(),
            self.rpc_router.clone(),
            self.networking.clone(),
            offset,
            payloads,
            permit,
            commit_resolver,
        );

        // We are sure that if task-center is shutting down that all future appends will fail so we
        // are not so worried about the offset that was updated already above.
        task_center().spawn_child(
            TaskKind::ReplicatedLogletAppender,
            "sequencer-appender",
            None,
            async move {
                appender.run().await;
                Ok(())
            },
        )?;

        Ok(loglet_commit)
    }
}

trait RecordsExt {
    /// tail computes inflight tail after this batch is committed
    fn last_offset(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError>;
    fn estimated_encode_size(&self) -> usize;
}

impl<T: AsRef<[Record]>> RecordsExt for T {
    fn last_offset(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError> {
        let len =
            u32::try_from(self.as_ref().len()).map_err(|_| SequencerError::InvalidBatchLength)?;

        first_offset
            .checked_add(len - 1)
            .map(LogletOffset::from)
            .ok_or(SequencerError::LogletOffsetExhausted)
    }

    fn estimated_encode_size(&self) -> usize {
        self.as_ref()
            .iter()
            .map(|r| r.estimated_encode_size())
            .sum()
    }
}
