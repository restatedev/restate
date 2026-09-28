// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use rocksdb::ReadOptions;
use zerocopy::IntoBytes;

use restate_rocksdb::{IterAction, Priority};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;
use restate_types::sharding::PartitionId;

use crate::PartitionStore;
use crate::Result;
use crate::break_on_err;
use crate::keys::KeyDecoder;
use crate::keys::filter::{IndexKeySchema, KeyMatch, PreparedKeyFilter};
use crate::stats::aggregated::Aggregate;
use crate::stats::{StatKeyPrefix, StatValueCodec};

use super::{
    AggregatedStat, DeploymentLoadKey, ServiceLoadKey, StageCounts, StageStatusCounts,
    VirtualObjectLoadKey,
};

impl PartitionStore {
    /// Scans service-load keys matching the logical filter. Key predicates are
    /// evaluated before bucket values are deserialized or handed to the caller.
    /// Invalid entry-kind sentinels in the filter are rejected during binding.
    /// The filter is prepared synchronously; the returned future does not borrow it.
    /// Callback errors fail the scan; `Break(Ok(()))` stops it successfully.
    /// Optional metrics account only for this scan, including empty scan plans.
    pub fn scan_service_load<
        F: for<'a> FnMut(
                KeyDecoder<'a, ServiceLoadKey>,
                StageStatusCounts,
            ) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    >(
        &self,
        filter: &Filter<ServiceLoad>,
        metrics: Option<restate_rocksdb::IteratorMetrics>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>> {
        self.scan_prepared_stats::<ServiceLoad, F>(
            ServiceLoadKey::prepare_filter(filter)?,
            metrics,
            f,
        )
    }

    /// Scans deployment-load keys, preparing the filter before starting the scan.
    pub fn scan_deployment_load<F>(
        &self,
        filter: &Filter<DeploymentLoad>,
        metrics: Option<restate_rocksdb::IteratorMetrics>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
    where
        F: for<'a> FnMut(KeyDecoder<'a, DeploymentLoadKey>, StageCounts) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        self.scan_prepared_stats::<DeploymentLoad, F>(
            DeploymentLoadKey::prepare_filter(filter)?,
            metrics,
            f,
        )
    }

    /// Scans virtual-object-load keys, preparing the filter before starting the scan.
    pub fn scan_virtual_object_load<F>(
        &self,
        filter: &Filter<VirtualObjectLoad>,
        metrics: Option<restate_rocksdb::IteratorMetrics>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
    where
        F: for<'a> FnMut(
                KeyDecoder<'a, VirtualObjectLoadKey>,
                StageCounts,
            ) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        self.scan_prepared_stats::<VirtualObjectLoad, F>(
            VirtualObjectLoadKey::prepare_filter(filter)?,
            metrics,
            f,
        )
    }

    /// Executes a bound filter before materializing values for any aggregate.
    fn scan_prepared_stats<S, F>(
        &self,
        prepared: PreparedKeyFilter<S::OwnedKey>,
        metrics: Option<restate_rocksdb::IteratorMetrics>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>
    where
        S: AggregatedStat,
        S::OwnedKey: IndexKeySchema + 'static,
        F: for<'a> FnMut(KeyDecoder<'a, S::OwnedKey>, S::Value) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        if let Some(metrics) = &metrics {
            metrics.mark_supported();
        }
        // Counters
        let mut counters = IteratorCounters {
            partition_id: self.partition_id(),
            iterations: 0,
            reseeks: 0,
            matches: 0,
            matcher_misses: 0,
            zeros_skipped: 0,
        };

        let base_prefix = StatKeyPrefix::of::<S>(self.partition_id());
        let future = prepared
            .into_cursor(base_prefix.as_bytes())?
            .map(|mut cursor| {
                let scan = cursor.scan().clone();
                let mut opts = ReadOptions::default();
                opts.set_async_io(true);
                // let partition_id = self.partition_id();
                // tracing::info!("[{partition_id}] scanning aggregated stats: physical: {scan:?}",);
                self.iterator_controlled_physical(
                    "df-scan-stats",
                    Priority::Low,
                    opts,
                    scan,
                    metrics,
                    move |(key, value)| {
                        let counters = &mut counters;
                        counters.iterations += 1;
                        match break_on_err(cursor.evaluate(key))? {
                            KeyMatch::Match => {
                                counters.matches += 1;
                            }
                            KeyMatch::Seek(target) => {
                                counters.matcher_misses += 1;
                                counters.reseeks += 1;
                                return ControlFlow::Continue(IterAction::Seek(target));
                            }
                            KeyMatch::Done => {
                                counters.matcher_misses += 1;
                                return ControlFlow::Break(Ok(()));
                            }
                        }
                        let (_, remaining) =
                            break_on_err(KeyDecoder::new_stat(key).decode_prefix())?;
                        let value = break_on_err(StatValueCodec::deserialize_from(value))?;
                        if S::Aggregation::should_filter(&value) {
                            counters.zeros_skipped += 1;
                            return ControlFlow::Continue(IterAction::Next);
                        }
                        f(remaining.into_decoder::<S, S::OwnedKey>(), value)
                            .map_continue(|()| IterAction::Next)
                    },
                )
            })
            .transpose()
            .map_err(|_| StorageError::OperationalError)?;
        Ok(async move {
            match future {
                Some(future) => future.await,
                None => Ok(()),
            }
        })
    }
}

struct IteratorCounters {
    partition_id: PartitionId,
    iterations: u64,
    reseeks: u64,
    matches: u64,
    zeros_skipped: u64,
    matcher_misses: u64,
}

impl Drop for IteratorCounters {
    fn drop(&mut self) {
        if self.iterations > 0 {
            tracing::info!(
                "[{}] IteratorCounters: iterations={} reseeks={} matches={} matcher_misses={}, zeros_skipped={}",
                self.partition_id,
                self.iterations,
                self.reseeks,
                self.matches,
                self.matcher_misses,
                self.zeros_skipped,
            );
        }
    }
}
