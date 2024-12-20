// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod appender;

use std::sync::{
    atomic::{AtomicU32, AtomicUsize, Ordering},
    Arc,
};

use crossbeam_utils::CachePadded;
use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;
use tracing::{debug, instrument, trace};

use restate_core::{
    network::{rpc_router::RpcRouter, Networking, TransportConnect},
    ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    config::Configuration,
    logs::{LogletId, LogletOffset, Record, RecordCache, SequenceNumber},
    net::log_server::Store,
    replicated_loglet::{NodeSet, ReplicatedLogletParams, ReplicationProperty},
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
    known_global_tail: TailOffsetWatch,
    my_params: ReplicatedLogletParams,
    selector: SpreadSelector,
    log_server_manager: RemoteLogServerManager,
}

impl SequencerSharedState {
    pub fn sequencer(&self) -> &GenerationalNodeId {
        &self.my_params.sequencer
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

    pub fn loglet_id(&self) -> &LogletId {
        &self.my_params.loglet_id
    }
}

/// This represents the leader sequencer for a loglet. The leader sequencer is the sole writer
/// and we guarantee that only one leader sequencer is alive per loglet-id by tying its lifetime
/// to the generation of this node-id.
pub struct Sequencer<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    // this is very frequently updated, let's avoid invalidating the whole cache line every time we
    // update this.
    // the other bits of data in this struct.
    next_write_offset: CachePadded<AtomicU32>,
    networking: Networking<T>,
    rpc_router: RpcRouter<Store>,
    /// The value we read from configuration, we keep it around because we can't get the original
    /// capacity directly from `record_permits` Semaphore.
    max_inflight_records_in_config: AtomicUsize,
    /// Semaphore for the number of records in-flight.
    /// This is an Arc<> to allow sending owned permits
    record_permits: Arc<Semaphore>,
    in_flight_appends: TaskTracker,
    record_cache: RecordCache,
}

impl<T: TransportConnect> Sequencer<T> {
    /// Create a new sequencer instance
    pub fn new(
        my_params: ReplicatedLogletParams,
        selector_strategy: SelectorStrategy,
        networking: Networking<T>,
        rpc_router: RpcRouter<Store>,
        record_cache: RecordCache,
        known_global_tail: TailOffsetWatch,
    ) -> Self {
        let initial_tail = known_global_tail.latest_offset();
        // Leader sequencers start on an empty loglet offset range
        debug_assert_eq!(LogletOffset::OLDEST, initial_tail);
        let next_write_offset = CachePadded::new(AtomicU32::new(*initial_tail));

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

        // todo: connections should be split from tail management, this way connections can be
        // shared across all sequencers on this node.
        let log_server_manager = RemoteLogServerManager::new(&my_params.nodeset);
        // shared state with appenders
        let sequencer_shared_state = Arc::new(SequencerSharedState {
            known_global_tail,
            log_server_manager,
            my_params,
            selector,
        });

        Self {
            sequencer_shared_state,
            next_write_offset,
            rpc_router,
            networking,
            record_permits,
            record_cache,
            max_inflight_records_in_config: AtomicUsize::new(max_in_flight_records_in_config),
            in_flight_appends: TaskTracker::default(),
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

    /// wait until all in-flight appends are drained. Note that this will cause the sequencer to
    /// return AppendError::Sealed for new appends but it won't start the seal process itself. The
    /// seal process must be started externally. _Only_ after the drain is complete, the caller
    /// can set the seal on `known_global_tail` as it's guaranteed that no more work will be
    /// done by the sequencer (No acknowledgements will be delivered for appends after the first
    /// observed global_tail with is_sealed=true)
    ///
    /// This method is cancellation safe.
    pub async fn drain(&self) -> Result<(), ShutdownError> {
        // stop issuing new permits
        self.record_permits.close();
        // required to allow in_flight.wait() to finish.
        self.in_flight_appends.close();

        // we are assuming here that seal has been already executed on majority of nodes. This is
        // important since in_flight.close() doesn't prevent new tasks from being spawned.
        if self.sequencer_shared_state.known_global_tail.is_sealed() {
            return Ok(());
        }

        // wait for in-flight tasks to complete before returning
        debug!(
            loglet_id = %self.sequencer_shared_state.my_params.loglet_id,
            "Draining sequencer, waiting for {} inflight appends to complete",
            self.in_flight_appends.len(),
        );
        self.in_flight_appends.wait().await;

        trace!(
            loglet_id = %self.sequencer_shared_state.my_params.loglet_id,
            "Sequencer drained",
        );

        Ok(())
    }

    fn ensure_enough_permits(&self, required: usize) {
        let mut available = self.max_inflight_records_in_config.load(Ordering::Relaxed);
        while available < required {
            let delta = required - available;
            match self.max_inflight_records_in_config.compare_exchange(
                available,
                required,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.record_permits.add_permits(delta);
                }
                Err(current) => {
                    available = current;
                }
            }
        }
    }

    #[instrument(
        level="trace",
        skip_all,
        fields(
            otel.name = "replicated_loglet::sequencer: enqueue_batch",
        )
    )]
    pub async fn enqueue_batch(
        &self,
        payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, OperationError> {
        // Note: caller will checks if the loglet is sealed or not, however, it doesn't mean that
        // this batch is guaranteed to succeed, but we don't want to waste cycles on unnecessary
        // check from the underlying watch sender.
        self.ensure_enough_permits(payloads.len());

        let len = u32::try_from(payloads.len()).expect("batch sizes fit in u32");
        // We drop the semaphore when the sequencer considers the loglet as sealed
        let Ok(permit) = self.record_permits.clone().acquire_many_owned(len).await else {
            return Ok(LogletCommit::sealed());
        };

        // Note: We don't need to sync order across threads here since the ordering requirement
        // requires that the user calls enqueue_batch sequentially to guarantee that original batch ordering
        // is maintained.
        let offset = LogletOffset::new(
            self.next_write_offset
                .fetch_add(len, std::sync::atomic::Ordering::Relaxed),
        );

        // Add to cache before we commit those records, we also do this to try and cache the
        // records before we transform/serialize their payloads.
        //
        // The records being in cache does not mean they are committed, all readers must respect
        // the result of find_tail() or the global_known_tail.
        self.record_cache
            .extend(*self.sequencer_shared_state.loglet_id(), offset, &payloads);

        let (loglet_commit, commit_resolver) = LogletCommit::deferred();

        // todo: serialize records here into bytes to use the caller's runtime/thread to do the cpu
        // work instead of overloading the network's thread. Why not in bifrost's appender? because
        // we don't need serialization in all loglet providers. In-memory loglet won't require this
        // setup and it would be a waste of cpu-cycles to serialize.
        let appender = SequencerAppender::new(
            Arc::clone(&self.sequencer_shared_state),
            self.rpc_router.clone(),
            self.networking.clone(),
            offset,
            payloads,
            permit,
            commit_resolver,
        );

        let fut = self.in_flight_appends.track_future(appender.run());
        // Why not managed tasks, because managed tasks are not designed to manage a potentially
        // very large number of tasks, they also require a lock acquistion on start and that might
        // be a contention point.
        //
        // Therefore, those tasks should not crash. We need to make sure that they have solid handling of errors.
        TaskCenter::spawn_unmanaged(TaskKind::SequencerAppender, "sequencer-appender", fut)?;

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
