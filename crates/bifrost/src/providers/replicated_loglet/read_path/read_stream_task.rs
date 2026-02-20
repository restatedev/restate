// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::time::Duration;

use adaptive_timeout::{AdaptiveTimeout, BackoffInterval, TimeoutConfig};
use metrics::{Counter, counter};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{info, trace};

use restate_core::network::{NetworkSender, Networking, RpcError, Swimlane, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenter, TaskHandle, TaskKind, my_node_id};
use restate_types::config::{Configuration, ReplicatedLogletOptions};
use restate_types::logs::{
    KeyFilter, LogletOffset, MatchKeyQuery, RecordCache, SequenceNumber, TailOffsetWatch,
};
use restate_types::net::log_server::{GetRecords, LogServerRequestHeader, MaybeRecord};
use restate_types::replicated_loglet::{EffectiveNodeSet, LogNodeSetExt, ReplicatedLogletParams};
use restate_types::replication::NodeSet;
use restate_types::{NodeId, PlainNodeId};

use crate::LogEntry;
use crate::loglet::OperationError;
use crate::providers::replicated_loglet::LATENCY_TRACKER;
use crate::providers::replicated_loglet::metric_definitions::{
    BIFROST_REPLICATED_READ_CACHE_FILTERED, BIFROST_REPLICATED_READ_CACHE_HIT,
    BIFROST_REPLICATED_READ_TOTAL,
};
use crate::providers::replicated_loglet::tasks::GetTrimPointTask;

#[derive(Debug, thiserror::Error)]
#[error("Impossible to read from nodeset {0:?}, all nodes are disabled")]
struct ImpossibleNodeSetError(NodeSet);

struct Stats {
    cache_filtered: Counter,
    cache_hits: Counter,
    records_read: Counter,
}

impl Default for Stats {
    fn default() -> Self {
        let cache_filtered = counter!(BIFROST_REPLICATED_READ_CACHE_FILTERED);
        let cache_hits = counter!(BIFROST_REPLICATED_READ_CACHE_HIT);
        let records_read = counter!(BIFROST_REPLICATED_READ_TOTAL);
        Self {
            cache_filtered,
            cache_hits,
            records_read,
        }
    }
}

pub struct ReadStreamTask {
    my_params: ReplicatedLogletParams,
    filter: KeyFilter,
    global_tail_watch: TailOffsetWatch,
    last_known_tail: LogletOffset,
    /// The offset of the batch to read next. This might be ahead of the actual read_pointer in the
    /// handle because of how we perform read-ahead. So, this is what we should read next from
    /// servers and _not_ what the consumer will read next from the stream.
    read_pointer: LogletOffset,
    /// Last offset to read before terminating the stream. None means "tailing" reader.
    /// *Inclusive*
    /// This must be set if `move_beyond_global_tail` is true.
    read_to: Option<LogletOffset>,
    tx: mpsc::Sender<Result<LogEntry<LogletOffset>, OperationError>>,
    record_cache: RecordCache,
    stats: Stats,
    /// If set to true, we won't wait for the global tail to be updated before requesting the next
    /// batch. This is used in tail repair tasks.
    move_beyond_global_tail: bool,
}

impl ReadStreamTask {
    #[allow(clippy::too_many_arguments)]
    pub async fn start<T: TransportConnect>(
        my_params: ReplicatedLogletParams,
        networking: Networking<T>,
        filter: KeyFilter,
        from_offset: LogletOffset,
        read_to: Option<LogletOffset>,
        known_global_tail: TailOffsetWatch,
        record_cache: RecordCache,
        move_beyond_global_tail: bool,
    ) -> Result<
        (
            mpsc::Receiver<Result<LogEntry<LogletOffset>, OperationError>>,
            TaskHandle<Result<(), OperationError>>,
        ),
        OperationError,
    > {
        if move_beyond_global_tail && read_to.is_none() {
            panic!("read_to must be set if move_beyond_global_tail=true");
        }
        let (tx, rx) = mpsc::channel(
            Configuration::pinned()
                .bifrost
                .replicated_loglet
                .readahead_records
                .get() as usize,
        );
        // Reading from INVALID resets to OLDEST.
        let from_offset = from_offset.max(LogletOffset::OLDEST);

        let task = Self {
            my_params,
            filter,
            read_pointer: from_offset,
            read_to,
            global_tail_watch: known_global_tail,
            last_known_tail: LogletOffset::OLDEST,
            tx,
            record_cache,
            stats: Stats::default(),
            move_beyond_global_tail,
        };
        let handle = TaskCenter::spawn_unmanaged(
            TaskKind::ReplicatedLogletReadStream,
            "replicatedloglet-read-stream",
            task.run(networking),
        )?;

        Ok((rx, handle))
    }

    async fn run<T: TransportConnect>(
        mut self,
        networking: Networking<T>,
    ) -> Result<(), OperationError> {
        let mut nodes_config = Metadata::with_current(|m| m.updateable_nodes_config());
        let mut configuration = Configuration::live();
        let my_node = my_node_id();
        // Channel size. This is the largest number of records we will try to readahead, if we can
        // acquire the capacity for it.
        let readahead_max = self.tx.max_capacity();
        debug_assert!(readahead_max <= u16::MAX.into());
        // This is automatically capped. This is the minimum number of slots that needs to be
        // available in order to trigger fetching a new batch.
        let readahead_trigger = {
            let ratio = configuration
                .live_load()
                .bifrost
                .replicated_loglet
                .readahead_trigger_ratio
                .clamp(0.0, 1.0) as f64;
            let trigger = (readahead_max as f64 * ratio).ceil() as usize;
            1.max(trigger)
        };
        debug_assert!(readahead_trigger >= 1 && readahead_trigger <= self.tx.max_capacity());

        let mut tail_subscriber = self.global_tail_watch.subscribe();
        if self.move_beyond_global_tail {
            self.last_known_tail = self
                .read_to
                .expect("read_to must be set with move_beyond_global_tail=true")
                .next();
        } else {
            // resolves immediately as it's pre-marked as changed.
            tail_subscriber
                .changed()
                .await
                .map_err(|_| OperationError::Shutdown(ShutdownError))?;
            self.last_known_tail = tail_subscriber.borrow_and_update().offset();
        }

        // Our initial knowledge of the trim point is determined by this request. Note that we
        // might not observe some of the future trim point updates if we already have the records
        // in the record cache. If we failed to determine the trim point, we'll ignore it and
        // continue.
        let trim_point =
            match GetTrimPointTask::new(&self.my_params, self.global_tail_watch.clone())
                .run(networking.clone())
                .await
            {
                Ok(trim_point) => trim_point,
                Err(e) => {
                    info!(
                        loglet_id = %self.my_params.loglet_id,
                        offset = %self.read_pointer,
                        "Could not determine the trim point while creating the read stream: {e}. \
                            This should not impact reading if records are cached in memory or if \
                            log-servers came back alive later.",
                    );
                    None
                }
            };
        let cluster_state = TaskCenter::with_current(|tc| tc.cluster_state().clone());

        // [important]
        // We rely on the periodic task owned by the provider to refresh our view of the tail.
        // This is our fallback mechanism to get observer updates to the global tail if we are not
        // the sequencer, and if no network messages came through with updates recently.
        'main: loop {
            // Read and ship records to the tx channel if there is capacity. We do not attempt to
            // read records if we cannot reserve capacity to avoid wasting resources.
            //
            // Once we secure enough capacity, we get the records from whatever source and write
            // them to the secured permits. The channel size limits how much read-ahead we can do
            // from log-servers but when reading from cache, we only read when we need.
            //
            // Note 1: In loglet implementations, it's safe to return records even if loglet is sealed
            // as long as we only return records before the global tail.
            //
            // Note 2: We have capacity-management impedance mismatch. We size channels by the number of
            // records, but we use number of bytes (memory) to limit our total memory consumption.
            // This can be improved in the future by using a semaphore representing the memory
            // budget and unbounded channel instead.
            //
            //
            // The server we read from will return up to its local tail if we are reading beyond
            // the global tail. So, we can effectively cache those records and use them only when
            // the global tail advances (basic read-ahead)
            //
            // If we received all records needed, we'll write to the tx all records that are both
            // below the global_tail and under our target to (the latter is assumed implicitly
            // given that we request from log-servers up to this value anyway).
            //
            // How readahead watermarks work?
            // - Capacity is 100 (`readahead_trigger=50`)
            // - We fetch 100, capacity is 0 (`readahead_max` is 100)
            // - reader reads 1
            // - Capacity is 1
            // - reader reads 10
            // - Capacity is 11
            // - reader reads 40
            // - Capacity is 51
            // - readahead_trigger exceeded. We try to fetch 49. Capacity => 0.
            // - Reader reads 1 (capacity is 1)
            //
            // What controls this read stream:
            // 1. Did we arrive to the target `read_to` already?
            // 2. Capacity is released, can we await on this? On certain triggers only. If capacity
            //    is zero, we'll try to acquire capacity of `readahead_trigger`
            // 3. Did we receive response from log-server, or failure, or timeout.
            // 4. Is there a trim-gap?
            // 5. Did the global tail advance?

            // Are we there yet?
            if self.should_terminate() {
                // Terminate by dropping ourselves. In this case, the sender is dropped (we are the only sender holder)
                trace!(
                    "ReadStreamTask: Terminating read stream as we have reached the target offset"
                );
                return Ok(());
            }

            // Are we reading after last_known_tail offset?
            // We are at tail. We need to wait until new records have been released.
            if !self.can_advance() && !self.move_beyond_global_tail {
                // HODL.
                // todo(asoli): Measure tail-change wait time in histogram
                tail_subscriber
                    .changed()
                    .await
                    .map_err(|_| OperationError::Shutdown(ShutdownError))?;
                self.last_known_tail = tail_subscriber.borrow_and_update().offset();
                // perhaps last_known_tail is updated because we have been sealed but the tail
                // didn't change. Let's revisit from the top and see if we land here again.
                continue 'main;
            }
            // We are only here because we should attempt to read something
            debug_assert!(self.last_known_tail > self.read_pointer);

            // Do we have capacity for the next read?
            // - capacity is 100, watermark is 50; we reserve 100; but if readahead_max is 80, we
            // request 80;
            // - capacity is 5, readahead_trigger is 50; we wait until 50 is available.
            let mut permits = self
                .tx
                .reserve_many(self.tx.capacity().max(readahead_trigger).min(readahead_max))
                // fails if receiver is dropped (no more read stream)
                .await
                .map_err(OperationError::terminal)?;

            // check for trim point
            if trim_point.is_some_and(|trim_point| self.read_pointer <= trim_point) {
                let trim_point = trim_point.unwrap();
                let permit = permits.next().expect("must have at least one permit");
                trace!(
                    loglet_id = %self.my_params.loglet_id,
                    offset = %self.read_pointer,
                    "Shipping a trim gap since we are reading before the trim point. Trim gap from offset {} to offset {}",
                    self.read_pointer,
                    trim_point,
                );
                permit.send(Ok(LogEntry::new_trim_gap(self.read_pointer, trim_point)));
                // fast-forward
                self.read_pointer = trim_point.next();
                continue 'main;
            }

            // Read from logservers
            let effective_nodeset = if cluster_state.is_alive(my_node.into()) {
                EffectiveNodeSet::from_iter(
                    self.my_params
                        .nodeset
                        .iter()
                        .filter(|id| cluster_state.is_alive(NodeId::from(*id)))
                        .copied(),
                    nodes_config.live_load(),
                )
            } else {
                // if my own node is not alive, we shouldn't trust the state of cluster-state.
                EffectiveNodeSet::from_iter(
                    self.my_params.nodeset.iter().copied(),
                    nodes_config.live_load(),
                )
            };

            if effective_nodeset.is_empty() {
                // if nodeset is all disabled, no readable nodes. impossible situation to resolve,
                if self
                    .my_params
                    .nodeset
                    .all_disabled(nodes_config.live_load())
                {
                    return Err(OperationError::terminal(ImpossibleNodeSetError(
                        self.my_params.nodeset.clone(),
                    )));
                } else {
                    // Some nodes might be provisioning, wait and try again after a cool off
                    // period.
                    // todo: make this configurable.
                    info!(
                        loglet_id = %self.my_params.loglet_id,
                        offset = %self.read_pointer,
                        "All nodes in the nodeset are unreadable. Retrying in 2 seconds.."
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue 'main;
                }
            }

            // Order the nodeset such that our node is the first one to attempt
            let mut mutable_effective_nodeset = effective_nodeset.shuffle_for_reads(my_node);

            'attempt_from_servers: loop {
                // Read from _somewhere_ until we reach the tail, target, or the available permits.
                // Start by reading from record cache as much as we can
                'attempt_from_cache: loop {
                    match self.send_next_from_cache(&mut permits) {
                        // fast-forward
                        CacheReadResult::Sent => {
                            self.read_pointer = self.read_pointer.next();
                            continue 'attempt_from_cache;
                        }
                        CacheReadResult::Miss => {
                            // Once a record is not in cache, we fallback to reading from log-servers until
                            // we exhaust remaining permits.
                            break 'attempt_from_cache;
                        }
                        CacheReadResult::Stop => {
                            continue 'main;
                        }
                    }
                }

                let to_offset = self.calculate_read_ahead_to_offset(permits.len());
                // If we (my node) are in the nodeset, we'll be the first to try
                let Some(server) = mutable_effective_nodeset.pop() else {
                    // no more servers to try. Going back and retrying the main loop to start over.
                    info!(
                        loglet_id = %self.my_params.loglet_id,
                        from_offset = %self.read_pointer,
                        %to_offset,
                        "Could not request record batch, exhausted all servers in the nodeset. Retrying.."
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue 'main;
                };

                let ServerReadResult::Records {
                    records,
                    next_offset,
                } = self
                    .readahead_from_server(
                        server,
                        to_offset,
                        &networking,
                        &configuration.live_load().bifrost.replicated_loglet,
                    )
                    .await?
                else {
                    // move to the next server
                    continue 'attempt_from_servers;
                };

                // Note that returned records can have gaps
                for (offset, maybe_record) in records {
                    // if offset is smaller, we just ignore.
                    if offset >= self.last_known_tail || offset > self.read_pointer {
                        // we have reached the tail, we have a record but we shouldn't ship it.
                        // Let's cache it to assist future reads instead.
                        self.add_to_cache(offset, &maybe_record);
                    } else if offset == self.read_pointer {
                        match maybe_record {
                            MaybeRecord::Unknown => {
                                unreachable!()
                            }
                            MaybeRecord::TrimGap(gap) => {
                                let permit = permits.next().expect("must have at least one permit");
                                trace!(
                                    loglet_id = %self.my_params.loglet_id,
                                    offset = %self.read_pointer,
                                    "Shipping a trim gap from node {} to offset {}",
                                    server,
                                    gap.to
                                );
                                permit.send(Ok(LogEntry::new_trim_gap(self.read_pointer, gap.to)));
                                // fast-forward
                                self.read_pointer = gap.to.next();
                            }
                            MaybeRecord::ArchivalGap(_) => {
                                todo!("We don't support reading from object-store yet")
                            }
                            MaybeRecord::FilteredGap(gap) => {
                                // records didn't match the filter.
                                let permit = permits.next().expect("must have at least one permit");
                                // There is a risk that this gap goes beyond the global_tail, so we
                                // clamp it to our known_tail to avoid drifting outside the
                                // committed window.
                                //
                                // we clamp the end of the gap to the last safe offset we can
                                // return before the tail.
                                let gap_to = self.last_known_tail.min(gap.to.next()).prev();

                                trace!(
                                    loglet_id = %self.my_params.loglet_id,
                                    offset = %self.read_pointer,
                                    "Shipping a filtered gap from node {} to offset {}",
                                    server,
                                    gap_to
                                );
                                permit.send(Ok(LogEntry::new_filtered_gap(
                                    self.read_pointer,
                                    gap_to,
                                )));
                                self.read_pointer = gap_to.next();
                            }
                            MaybeRecord::Data(record) => {
                                let permit = permits.next().expect("must have at least one permit");
                                trace!(
                                    loglet_id = %self.my_params.loglet_id,
                                    offset = %self.read_pointer,
                                    "Shipping a data record acquired from node {}",
                                    server,
                                );
                                // We do not cache this record since it's rare that we go back and
                                // read the same records that we shipped. If this assumption
                                // changes in the future, we can cache at this point.
                                self.stats.records_read.increment(1);
                                permit.send(Ok(LogEntry::new_data(self.read_pointer, record)));
                                self.read_pointer = self.read_pointer.next();
                            }
                        }
                    }
                }
                // we should try the last server again if the new read_pointer is the next_offset this server can supply.
                if next_offset.is_some_and(|next_offset| next_offset == self.read_pointer) {
                    // this server has more to send us, let's use it in the next attempt
                    mutable_effective_nodeset.push(server);
                }
            }
        }
    }

    fn add_to_cache(&self, offset: LogletOffset, maybe_record: &MaybeRecord) {
        if let MaybeRecord::Data(record) = maybe_record {
            self.record_cache
                .add(self.my_params.loglet_id, offset, record);
        }
    }

    fn calculate_read_ahead_to_offset(&self, available_permits: usize) -> LogletOffset {
        // estimate to-offset. Only limit it if this is a finite stream. Otherwise, it's okay
        // to overshoot and populate the record cache with records if the log-server's local
        // tail is beyond the global tail.
        let to_offset = LogletOffset::new(
            self.read_pointer.saturating_add(
                available_permits
                    .try_into()
                    .expect("max permits fit into u32"),
            ),
        )
        .prev();

        if let Some(read_to) = self.read_to {
            return to_offset.min(read_to);
        }
        to_offset
    }

    fn can_advance(&self) -> bool {
        self.read_pointer < self.last_known_tail
            && self.read_pointer <= self.read_to.unwrap_or(LogletOffset::MAX)
    }

    fn should_terminate(&self) -> bool {
        self.read_to
            .is_some_and(|read_to| self.read_pointer > read_to)
    }

    /// Only consumes a permit iff a record is found in cache
    ///
    /// Panics if permits is empty
    fn send_next_from_cache(
        &self,
        permits: &mut mpsc::PermitIterator<Result<LogEntry<LogletOffset>, OperationError>>,
    ) -> CacheReadResult {
        if !self.can_advance() || permits.len() == 0 {
            return CacheReadResult::Stop;
        }

        if let Some(record) = self
            .record_cache
            .get(self.my_params.loglet_id, self.read_pointer)
        {
            if !record.matches_key_query(&self.filter) {
                let permit = permits.next().expect("must have at least one permit");
                trace!(
                    loglet_id = %self.my_params.loglet_id,
                    offset = %self.read_pointer,
                    "Shipping a filtered gap from record cache at offset {}",
                    self.read_pointer,
                );
                permit.send(Ok(LogEntry::new_filtered_gap(
                    self.read_pointer,
                    self.read_pointer,
                )));
                self.stats.cache_hits.increment(1);
                self.stats.cache_filtered.increment(1);
                CacheReadResult::Sent
            } else {
                let permit = permits.next().expect("must have at least one permit");
                trace!(
                    loglet_id = %self.my_params.loglet_id,
                    offset = %self.read_pointer,
                    "Shipping record from record cache",
                );
                // Removes from cache, we are unlikely to need to read this record again, and if we need
                // to, we'll get it from log-servers.
                // Note: remove this when/if we decided to have multiple readers of the same log on
                // the same machine (i.e. sharing logs between partitions)
                self.record_cache
                    .invalidate_record(self.my_params.loglet_id, self.read_pointer);
                self.stats.cache_hits.increment(1);
                self.stats.records_read.increment(1);
                permit.send(Ok(LogEntry::new_data(self.read_pointer, record)));
                CacheReadResult::Sent
            }
        } else {
            CacheReadResult::Miss
        }
    }

    async fn readahead_from_server<T: TransportConnect>(
        &self,
        server: PlainNodeId,
        to_offset: LogletOffset,
        networking: &Networking<T>,
        options: &ReplicatedLogletOptions,
    ) -> Result<ServerReadResult, OperationError> {
        let request = GetRecords {
            header: LogServerRequestHeader::new(
                self.my_params.loglet_id,
                self.global_tail_watch.latest_offset(),
            ),
            total_limit_in_bytes: Some(options.read_batch_size.as_usize()),
            filter: self.filter.clone(),
            from_offset: self.read_pointer,
            to_offset,
        };
        trace!(
            loglet_id = %self.my_params.loglet_id,
            from_offset = %self.read_pointer,
            %to_offset,
            "Attempting to read records from {}",
            server
        );

        let adaptive_timeout = AdaptiveTimeout::new(TimeoutConfig {
            backoff: BackoffInterval::from_str("1s..15s").unwrap(),
            quantile: 0.9999, // base timeout on P99.99
            safety_factor: 2.0,
        });
        let timeout =
            adaptive_timeout.select_timeout_sync(&LATENCY_TRACKER, &[server], 1, Instant::now());

        let read_start = Instant::now();
        let maybe_records = networking
            .call_rpc(
                server,
                Swimlane::BifrostReads,
                request,
                Some(self.my_params.loglet_id.into()),
                Some(timeout),
            )
            .await;

        match maybe_records {
            Ok(records) => {
                trace!(
                    loglet_id = %self.my_params.loglet_id,
                    from_offset = %self.read_pointer,
                    %to_offset,
                    peer_next_offset = %records.next_offset,
                    "Received {} records from {}",
                    records.records.len(),
                    server,
                );
                LATENCY_TRACKER.record_latency_from(&server, read_start, Instant::now());
                self.global_tail_watch
                    .notify_offset_update(records.known_global_tail);
                // note: next_offset == read_pointer(aka. from_offset) if the server has no results
                // for us within the requested range.
                let next_offset =
                    (records.next_offset > self.read_pointer).then_some(records.next_offset);
                Ok(ServerReadResult::Records {
                    records: records.records,
                    next_offset,
                })
            }
            Err(e) => {
                if let RpcError::Timeout(spent) = e {
                    LATENCY_TRACKER.record_latency(&server, spent, Instant::now());
                }
                trace!(
                    loglet_id = %self.my_params.loglet_id,
                    from_offset = %self.read_pointer,
                    %to_offset,
                    %e,
                    "Could not request record batch from node {}", server
                );
                Ok(ServerReadResult::Skip)
            }
        }
    }
}

enum CacheReadResult {
    /// Record was found and sent
    Sent,
    /// Not in cache, read_pointer not advanced
    Miss,
    /// We should not read the next record (out of permits, or will exceed the last_known_tail)
    Stop,
}

enum ServerReadResult {
    /// Maybe got some records for you
    Records {
        records: Vec<(LogletOffset, MaybeRecord)>,
        /// if the server can send us more records within the requested offset range.
        next_offset: Option<LogletOffset>,
    },
    /// Unreachable or failing node, skip and try another
    Skip,
}
