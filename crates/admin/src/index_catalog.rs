// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::debug;

use restate_core::cancellation_watcher;
use restate_storage_query_api::QueryEngine;
use restate_storage_query_datafusion::stats_tables_ready;
use restate_types::config::Configuration;
use restate_types::partitions::state::PartitionReplicaSetStates;

/// What this admin node knows about the indexes and statistics. `/version` shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexCatalog {
    /// The statistics tables answer: shown, because indexing is enabled, and
    /// without evidence that a read would fail (see [`stats_tables_ready`]).
    pub stats_tables: bool,
}

impl IndexCatalog {
    /// Before the first check: the statistics tables are assumed to answer
    /// wherever they are shown.
    pub fn initial() -> Self {
        Self {
            stats_tables: Configuration::pinned()
                .common
                .experimental
                .is_indexes_enabled(),
        }
    }

    /// Without indexing, the statistics tables aren't shown.
    const HIDDEN: Self = Self {
        stats_tables: false,
    };
}

/// The delay before the first retry of a failed check; it doubles with each
/// further failure, up to the refresh interval.
const FIRST_RETRY: Duration = Duration::from_secs(5);

/// Checks the indexes through `query_engine` every
/// `admin.query-engine.index-catalog-refresh-interval` and publishes the result
/// to `catalog`. A check that fails as a whole, or doesn't finish within the
/// interval, keeps the last catalog and is retried sooner. A worker whose
/// registry scan fails, named in the query's warnings, only leaves its replicas
/// unknown; one that never answers stalls the check until that deadline.
/// Without `experimental-enable-indexes` the statistics tables aren't shown:
/// the catalog is then all `false`, without a check. They don't depend on
/// `admin.query-engine.disable-index-reads`, which only turns off the planner's
/// index reads.
pub(crate) async fn refresh(
    query_engine: Arc<QueryEngine>,
    replica_sets: PartitionReplicaSetStates,
    catalog: watch::Sender<IndexCatalog>,
) -> anyhow::Result<()> {
    let mut cancelled = std::pin::pin!(cancellation_watcher());
    let mut retry = FIRST_RETRY;
    loop {
        let (interval, indexes) = {
            let config = Configuration::pinned();
            (
                config
                    .admin
                    .query_engine
                    .index_catalog_refresh_interval
                    .to_std(),
                config.common.experimental.is_indexes_enabled(),
            )
        };
        let delay = if !indexes {
            catalog.send_replace(IndexCatalog::HIDDEN);
            retry = FIRST_RETRY;
            interval
        } else {
            // A worker that never answers would stall the check: past the
            // interval, it fails as a whole.
            let check = async {
                tokio::time::timeout(interval, stats_tables_ready(&query_engine, &replica_sets))
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|ready| ready)
            };
            tokio::select! {
                ready = check => match ready {
                    Ok(stats_tables) => {
                        catalog.send_replace(IndexCatalog { stats_tables });
                        retry = FIRST_RETRY;
                        interval
                    }
                    Err(error) => {
                        debug!(%error, "Index catalog check failed, keeping the last catalog");
                        let delay = retry.min(interval);
                        retry = retry.saturating_mul(2);
                        delay
                    }
                },
                _ = &mut cancelled => return Ok(()),
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = &mut cancelled => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use restate_core::{TaskCenter, TaskKind};
    use restate_storage_query_api::QueryEngineVersion;
    use restate_storage_query_api::errors::SessionError;

    use super::*;

    #[restate_core::test(start_paused = true)]
    async fn starts_true_keeps_it_through_failed_checks_and_indexes_off_skips_the_check() {
        let mut config = Configuration::default();
        config.common.experimental.set_indexes(true);
        // Turning off the planner's index reads leaves the statistics tables alone.
        config.admin.query_engine.disable_index_reads = true;
        Configuration::set(config.clone());
        // With indexing on, the tables are assumed to answer until a check finds
        // evidence otherwise.
        assert_eq!(IndexCatalog::initial(), IndexCatalog { stats_tables: true });
        let checks = Arc::new(AtomicUsize::new(0));
        let engine = QueryEngine::new(QueryEngineVersion::V1, {
            let checks = Arc::clone(&checks);
            move |_| {
                checks.fetch_add(1, Ordering::Relaxed);
                Err(SessionError::EngineDisabled)
            }
        });
        let (sender, catalog) = watch::channel(IndexCatalog::initial());
        let start = tokio::time::Instant::now();
        TaskCenter::spawn(
            TaskKind::Background,
            "index-catalog",
            refresh(
                Arc::new(engine),
                PartitionReplicaSetStates::default(),
                sender,
            ),
        )
        .unwrap();
        // A check that fails as a whole keeps the last value. It is retried 5 s
        // after the first failure, then after 10, 20 and 40 s, then every minute,
        // the default refresh interval.
        for (seconds, expected) in [
            (0, 1),
            (5, 2),
            (15, 3),
            (35, 4),
            (75, 5),
            (135, 6),
            (195, 7),
        ] {
            tokio::time::sleep_until(
                start + Duration::from_secs(seconds) + Duration::from_millis(1),
            )
            .await;
            assert_eq!(
                checks.load(Ordering::Relaxed),
                expected,
                "checks after {seconds} s"
            );
        }
        assert_eq!(*catalog.borrow(), IndexCatalog { stats_tables: true });

        // Without indexing the statistics tables aren't shown: the next turn, a
        // minute later, publishes `false` without a check.
        config.common.experimental.set_indexes(false);
        Configuration::set(config);
        tokio::time::sleep_until(start + Duration::from_secs(255) + Duration::from_millis(1)).await;
        assert_eq!(
            checks.load(Ordering::Relaxed),
            7,
            "no check without indexing"
        );
        assert_eq!(*catalog.borrow(), IndexCatalog::HIDDEN);
        assert_eq!(IndexCatalog::initial(), IndexCatalog::HIDDEN);
    }
}
