// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arc_swap::ArcSwapOption;
use datafusion::arrow::array::{Array, LargeStringArray};
use futures::StreamExt;
use itertools::Itertools;
use restate_admin_rest_model::deployments::DeploymentStatus;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::identifiers::DeploymentId;
use restate_types::schema::registry::{MetadataService, SchemaRegistry};
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::Instant;

#[derive(Clone)]
pub struct DeploymentStatusSnapshot {
    cached: Arc<CachedDeploymentStatuses>,
    pub age: Duration,
}

impl DeploymentStatusSnapshot {
    pub fn statuses(&self) -> &HashMap<DeploymentId, DeploymentStatus> {
        &self.cached.statuses
    }
}

/// Status of a single deployment together with the age of the underlying cache entry.
pub struct DeploymentStatusEntry {
    pub status: Option<DeploymentStatus>,
    pub age: Duration,
}

struct CachedDeploymentStatuses {
    statuses: HashMap<DeploymentId, DeploymentStatus>,
    computed_at: Instant,
    schema_version: Version,
}

#[derive(Clone)]
pub struct DeploymentStatusCache {
    inner: Arc<Inner>,
}

struct Inner {
    /// The cached value, read-mostly.
    value: ArcSwapOption<CachedDeploymentStatuses>,
    /// Used to serialize refreshes.
    refresh: Mutex<()>,
}

trait DeploymentStatusReader {
    /// Get the current schema version.
    fn schema_version(&self) -> Version;

    /// Recomputes the status of every deployment. The expensive step (a query engine round-trip
    /// in production).
    async fn compute(&self) -> anyhow::Result<HashMap<DeploymentId, DeploymentStatus>>;
}

impl DeploymentStatusCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                value: ArcSwapOption::empty(),
                refresh: Mutex::new(()),
            }),
        }
    }

    /// Returns all the deployment statuses
    pub async fn get_all<Metadata, Discovery, Telemetry>(
        &self,
        schema_registry: &SchemaRegistry<Metadata, Discovery, Telemetry>,
        query_context: &QueryContext,
        force_refresh: bool,
    ) -> anyhow::Result<DeploymentStatusSnapshot>
    where
        Metadata: MetadataService,
    {
        let source = QueryDeploymentStatusReader {
            schema_registry,
            query_context,
        };
        let cached = self.ensure_fresh(&source, force_refresh).await?;
        let age = cached.computed_at.elapsed();
        Ok(DeploymentStatusSnapshot { cached, age })
    }

    /// Returns the status of a single deployment without cloning the whole status map.
    pub async fn get<Metadata, Discovery, Telemetry>(
        &self,
        schema_registry: &SchemaRegistry<Metadata, Discovery, Telemetry>,
        query_context: &QueryContext,
        deployment_id: DeploymentId,
        force_refresh: bool,
    ) -> anyhow::Result<DeploymentStatusEntry>
    where
        Metadata: MetadataService,
    {
        let source = QueryDeploymentStatusReader {
            schema_registry,
            query_context,
        };
        let cached = self.ensure_fresh(&source, force_refresh).await?;
        Ok(DeploymentStatusEntry {
            status: cached.statuses.get(&deployment_id).copied(),
            age: cached.computed_at.elapsed(),
        })
    }

    /// Returns a fresh cache entry, recomputing it if needed.
    async fn ensure_fresh<S: DeploymentStatusReader>(
        &self,
        source: &S,
        force_refresh: bool,
    ) -> anyhow::Result<Arc<CachedDeploymentStatuses>> {
        let requested_at = Instant::now();
        let ttl: Duration = Configuration::pinned()
            .admin
            .deployment_status_cache_ttl
            .into();
        let current = self.inner.value.load();

        if !force_refresh && let Some(current_ref) = current.as_ref() {
            // Fast path: we have a value that satisfies the requirements already.
            if current_ref.schema_version == source.schema_version()
                && current_ref.computed_at.elapsed() < ttl
            {
                return Ok(Arc::clone(current_ref));
            }

            // We need a refresh. Try lock to refresh. If already held, another refresh is happening, just return current stale data.
            return match self.inner.refresh.try_lock() {
                Ok(guard) => {
                    // Drop current to avoid holding lock.
                    drop(current);
                    self.refresh_locked(source, requested_at, false, guard)
                        .await
                }
                Err(_) => Ok(Arc::clone(current_ref)),
            };
        }

        // Drop current to avoid holding lock.
        drop(current);

        // Cold start (nothing to serve) or a forced refresh: wait our turn, then refresh.
        let guard = self.inner.refresh.lock().await;
        self.refresh_locked(source, requested_at, force_refresh, guard)
            .await
    }

    /// Recomputes the cache entry while holding the refresh lock, stores it, and returns it.
    async fn refresh_locked<S: DeploymentStatusReader>(
        &self,
        source: &S,
        requested_at: Instant,
        force_refresh: bool,
        _guard: MutexGuard<'_, ()>,
    ) -> anyhow::Result<Arc<CachedDeploymentStatuses>> {
        // Another caller may have refreshed while we entered the lock, skip refreshing in that case.
        if !force_refresh
            && let Some(current) = self.inner.value.load().as_ref()
            && current.computed_at >= requested_at
            && current.schema_version == source.schema_version()
        {
            return Ok(Arc::clone(current));
        }

        // Snapshot the schema version we compute against.
        // If it advances mid-compute, it's fine as worst case next read will refresh it.
        let schema_version = source.schema_version();
        let statuses = source.compute().await?;

        // Store the new statuses
        let refreshed = Arc::new(CachedDeploymentStatuses {
            statuses,
            computed_at: Instant::now(),
            schema_version,
        });
        self.inner.value.store(Some(Arc::clone(&refreshed)));

        Ok(refreshed)
    }
}

/// Implementation of [`DeploymentStatusReader`] backed by the schema registry and the query engine.
struct QueryDeploymentStatusReader<'a, Metadata, Discovery, Telemetry> {
    schema_registry: &'a SchemaRegistry<Metadata, Discovery, Telemetry>,
    query_context: &'a QueryContext,
}

impl<Metadata, Discovery, Telemetry> DeploymentStatusReader
    for QueryDeploymentStatusReader<'_, Metadata, Discovery, Telemetry>
where
    Metadata: MetadataService,
{
    fn schema_version(&self) -> Version {
        self.schema_registry.schema_version()
    }

    async fn compute(&self) -> anyhow::Result<HashMap<DeploymentId, DeploymentStatus>> {
        let deployments = self.schema_registry.list_deployments();

        // Figure out which deployments are not being used as "latest" by any service.
        let latest_deployments: HashSet<_> = self
            .schema_registry
            .list_services()
            .into_iter()
            .map(|service| service.deployment_id)
            .collect();
        let not_latest_deployments: Vec<_> = deployments
            .iter()
            .map(|(deployment, _)| deployment.id)
            .filter(|deployment_id| !latest_deployments.contains(deployment_id))
            .collect();

        // Of the non-latest deployments, find the ones that are pinned to any invocation.
        let mut pinned_non_latest_deployments: HashSet<DeploymentId> = HashSet::new();
        if !not_latest_deployments.is_empty() {
            let not_latest_deployments_in_clause = not_latest_deployments
                .iter()
                .map(|id| format!("'{id}'"))
                .join(", ");
            let query = if Configuration::pinned()
                .common
                .experimental
                .is_vqueues_enabled()
            {
                format!(
                    "SELECT DISTINCT deployment FROM sys_vqueues \
             WHERE entry_kind = 'invocation' AND stage IN ('inbox', 'running', 'suspended', 'paused') \
             AND deployment IN ({not_latest_deployments_in_clause})"
                )
            } else {
                // TODO v1.8.0 remove this once vqueues are enabled by default
                format!(
                    "SELECT DISTINCT pinned_deployment_id FROM sys_invocation_status \
             WHERE status != 'completed' AND pinned_deployment_id IN ({not_latest_deployments_in_clause})"
                )
            };

            // Execute the query
            let mut query_result = self.query_context.execute(&query).await?;
            while let Some(batch) = query_result.stream.next().await {
                let batch = batch?;
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .context("deployment status query returned an unexpected column type")?;
                for value in column.iter().flatten() {
                    pinned_non_latest_deployments.insert(value.parse()?);
                }
            }
        }

        // Compute statuses
        Ok(deployments
            .into_iter()
            .map(|(deployment, _)| {
                let status = if latest_deployments.contains(&deployment.id)
                    || pinned_non_latest_deployments.contains(&deployment.id)
                {
                    DeploymentStatus::Active
                } else {
                    DeploymentStatus::Drained
                };
                (deployment.id, status)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use restate_types::config::set_current_config;
    use tokio::sync::Notify;

    /// A controllable [`DeploymentStatusReader`]: counts recomputations and can gate them so a
    /// refresh can be held in-flight while another caller races.
    struct FakeSource {
        version: StdMutex<Version>,
        computes: AtomicUsize,
        result: StdMutex<HashMap<DeploymentId, DeploymentStatus>>,
        /// Signalled right after a *gated* compute starts, so a test knows the refresher now holds
        /// the refresh lock.
        entered: Notify,
        /// A gated compute waits on this before returning.
        gate: Notify,
        gated: AtomicBool,
    }

    impl FakeSource {
        fn new() -> Self {
            Self {
                version: StdMutex::new(Version::MIN),
                computes: AtomicUsize::new(0),
                result: StdMutex::new(HashMap::new()),
                entered: Notify::new(),
                gate: Notify::new(),
                gated: AtomicBool::new(false),
            }
        }

        fn computes(&self) -> usize {
            self.computes.load(Ordering::SeqCst)
        }

        fn set_version(&self, version: Version) {
            *self.version.lock().unwrap() = version;
        }

        fn set_gated(&self, gated: bool) {
            self.gated.store(gated, Ordering::SeqCst);
        }
    }

    impl DeploymentStatusReader for FakeSource {
        fn schema_version(&self) -> Version {
            *self.version.lock().unwrap()
        }

        async fn compute(&self) -> anyhow::Result<HashMap<DeploymentId, DeploymentStatus>> {
            self.computes.fetch_add(1, Ordering::SeqCst);
            if self.gated.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.gate.notified().await;
            }
            Ok(self.result.lock().unwrap().clone())
        }
    }

    fn install_config() {
        set_current_config(Configuration::default());
    }

    /// A fresh value is served from the fast path, and a clone shares the same underlying cache.
    #[tokio::test]
    async fn fast_path_and_clone_share_state() {
        install_config();
        let cache = DeploymentStatusCache::new();
        let source = FakeSource::new();

        let first = cache.ensure_fresh(&source, false).await.unwrap();
        assert_eq!(source.computes(), 1);

        // A clone shares the same inner cache: a fresh read hits the fast path, no recompute.
        let again = cache.clone().ensure_fresh(&source, false).await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(source.computes(), 1);
    }

    /// A forced refresh recomputes even when the cached value is still fresh.
    #[tokio::test]
    async fn force_refresh_recomputes() {
        install_config();
        let cache = DeploymentStatusCache::new();
        let source = FakeSource::new();

        let first = cache.ensure_fresh(&source, false).await.unwrap();
        let forced = cache.ensure_fresh(&source, true).await.unwrap();
        assert_eq!(source.computes(), 2);
        assert!(!Arc::ptr_eq(&first, &forced));
    }

    /// A schema-version bump makes the cached value stale even within the TTL.
    #[tokio::test]
    async fn schema_version_bump_recomputes() {
        install_config();
        let cache = DeploymentStatusCache::new();
        let source = FakeSource::new();

        cache.ensure_fresh(&source, false).await.unwrap();
        source.set_version(Version::MIN.next());
        let refreshed = cache.ensure_fresh(&source, false).await.unwrap();
        assert_eq!(source.computes(), 2);
        assert_eq!(refreshed.schema_version, Version::MIN.next());
    }

    /// Once the TTL elapses the next read recomputes.
    #[tokio::test(start_paused = true)]
    async fn ttl_expiry_recomputes() {
        install_config();
        let ttl: Duration = Configuration::pinned()
            .admin
            .deployment_status_cache_ttl
            .into();
        let cache = DeploymentStatusCache::new();
        let source = FakeSource::new();

        let first = cache.ensure_fresh(&source, false).await.unwrap();
        // Within the TTL: fast path, no recompute.
        let fresh = cache.ensure_fresh(&source, false).await.unwrap();
        assert!(Arc::ptr_eq(&first, &fresh));
        assert_eq!(source.computes(), 1);

        // Past the TTL: recompute.
        tokio::time::advance(ttl + Duration::from_secs(1)).await;
        let stale = cache.ensure_fresh(&source, false).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &stale));
        assert_eq!(source.computes(), 2);
    }

    /// While one refresh is in flight, a concurrent stale reader is served the old value instead of
    /// launching a second refresh.
    #[tokio::test]
    async fn stale_serve_is_single_flight() {
        install_config();
        let cache = DeploymentStatusCache::new();
        let source = Arc::new(FakeSource::new());

        // Populate, then make the entry stale by bumping the schema version and arm the gate.
        let stale = cache.ensure_fresh(&*source, false).await.unwrap();
        source.set_version(Version::MIN.next());
        source.set_gated(true);
        assert_eq!(source.computes(), 1);

        tokio::task::LocalSet::new()
            .run_until(async {
                // Refresher A wins the lock and blocks inside compute.
                let refresher = {
                    let cache = cache.clone();
                    let source = Arc::clone(&source);
                    tokio::task::spawn_local(async move {
                        cache.ensure_fresh(&*source, false).await.unwrap()
                    })
                };
                // Wait until A holds the refresh lock and is inside compute. This is the second
                // compute overall (the initial populate above was the first).
                source.entered.notified().await;
                assert_eq!(source.computes(), 2);

                // B sees the in-flight refresh and serves the stale value immediately.
                let served = cache.ensure_fresh(&*source, false).await.unwrap();
                assert!(Arc::ptr_eq(&served, &stale));
                assert_eq!(
                    source.computes(),
                    2,
                    "B must serve stale, not trigger a compute"
                );

                // Release A; it publishes the fresh value.
                source.gate.notify_one();
                let refreshed = refresher.await.unwrap();
                assert!(!Arc::ptr_eq(&refreshed, &stale));
                assert_eq!(source.computes(), 2);
            })
            .await;
    }

    /// Concurrent callers against a cold cache trigger exactly one recompute and share its result.
    #[tokio::test]
    async fn cold_start_coalesces() {
        install_config();
        let cache = DeploymentStatusCache::new();
        let source = Arc::new(FakeSource::new());
        source.set_gated(true);

        tokio::task::LocalSet::new()
            .run_until(async {
                // Three cold callers race; one wins the lock and blocks in compute, the rest queue.
                let mut handles = Vec::new();
                for _ in 0..3 {
                    let cache = cache.clone();
                    let source = Arc::clone(&source);
                    handles.push(tokio::task::spawn_local(async move {
                        cache.ensure_fresh(&*source, false).await.unwrap()
                    }));
                }
                // Wait until the winner is inside compute.
                source.entered.notified().await;
                assert_eq!(source.computes(), 1);

                // Release; the queued callers should coalesce onto the winner's result.
                source.gate.notify_one();
                let mut results = Vec::new();
                for handle in handles {
                    results.push(handle.await.unwrap());
                }
                assert_eq!(source.computes(), 1, "cold-start callers must coalesce");
                assert!(results.iter().all(|r| Arc::ptr_eq(r, &results[0])));
            })
            .await;
    }
}
