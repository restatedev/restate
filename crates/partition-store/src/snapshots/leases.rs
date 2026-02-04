// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytestring::ByteString;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};
use ulid::Ulid;

use restate_clock::{Clock, WallClock};
use restate_core::task_center::TaskHandle;
use restate_core::{Metadata, ShutdownError, TaskCenter, TaskKind};
use restate_metadata_server::{MetadataStoreClient, ReadError, WriteError};
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::metadata::Precondition;
use restate_types::retries::RetryPolicy;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, Versioned, flexbuffers_storage_encode_decode};

/// Duration for which a snapshot operation lease is valid (5 minutes).
///
/// Clock skew assumption: Nodes in the cluster should have clocks synchronized within
/// `LEASE_SAFETY_MARGIN` (1 minute). Larger clock skew could cause a node to believe a lease has
/// expired while the holder still considers it valid, leading to concurrent operations. NTP
/// synchronization typically provides sub-second accuracy, well within this tolerance.
///
/// The downside of making this longer is that this is the maximum time during which a zombie lease
/// whose owner has died can lock out other potential acquirers from making progress.
pub const LEASE_DURATION: Duration = Duration::from_mins(5);

/// Minimum remaining lease time required to continue operations (1 minute). Lease-covered
/// operations should aim to complete before `expires_at - LEASE_SAFETY_MARGIN`.
pub const LEASE_SAFETY_MARGIN: Duration = Duration::from_mins(1);

/// Interval at which lease renewal should occur (2 minutes). This should be well before lease
/// expiry to ensure smooth operations, but ideally long enough that most operations complete before
/// we have to renew.
pub const LEASE_RENEWAL_INTERVAL: Duration = Duration::from_mins(2);

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("Lease held by {holder} (lease_id: {lease_id}), expires at: {expires_at}")]
    Held {
        holder: GenerationalNodeId,
        lease_id: Ulid,
        expires_at: jiff::Timestamp,
    },
    #[error("Lease expired or taken over by another node")]
    Expired,
    #[error("Snapshot repository is read-only (no lease provider configured)")]
    Unavailable,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    MetadataRead(#[from] ReadError),
    #[error(transparent)]
    MetadataWrite(#[from] WriteError),
}

impl LeaseError {
    fn retryable(&self) -> bool {
        match self {
            LeaseError::Held { .. }
            | LeaseError::Expired
            | LeaseError::Unavailable
            | LeaseError::Shutdown(_) => false,
            LeaseError::MetadataRead(ReadError::Other(err)) => err.retryable(),
            LeaseError::MetadataRead(ReadError::Codec(_)) => false,
            LeaseError::MetadataWrite(WriteError::FailedPrecondition(_)) => false,
            LeaseError::MetadataWrite(WriteError::Other(err)) => err.retryable(),
            LeaseError::MetadataWrite(WriteError::Codec(_)) => false,
        }
    }
}

fn lease_key(partition_id: PartitionId) -> ByteString {
    ByteString::from(format!("snapshot_lease_{}", partition_id))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotLeaseValue {
    pub holder: GenerationalNodeId,
    pub lease_id: Ulid,
    pub expires_at: MillisSinceEpoch,
    version: Version,
}

impl SnapshotLeaseValue {
    fn new(holder: GenerationalNodeId, clock: &impl Clock) -> Self {
        let expires_at = clock.recent() + LEASE_DURATION;
        Self {
            holder,
            lease_id: Ulid::new(),
            expires_at,
            version: Version::MIN,
        }
    }

    fn expired(holder: GenerationalNodeId, lease_id: Ulid) -> Self {
        Self {
            holder,
            lease_id,
            expires_at: MillisSinceEpoch::UNIX_EPOCH,
            version: Version::MIN,
        }
    }

    fn is_expired(&self, clock: &impl Clock) -> bool {
        clock.recent() >= self.expires_at
    }

    fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }
}

impl Versioned for SnapshotLeaseValue {
    fn version(&self) -> Version {
        self.version
    }
}

flexbuffers_storage_encode_decode!(SnapshotLeaseValue);

/// Metadata-backed RAII lease which automatically releases when dropped
pub enum SnapshotLeaseGuard<C: Clock + Clone + Send + Sync + 'static = WallClock> {
    /// Metadata store-backed lease
    Lease(inner::MetadataBackedLeaseGuard<C>),
    /// No-op lease for testing
    #[cfg(any(test, feature = "test-util"))]
    NoOp(inner::NoOpLeaseGuard),
}

impl<C: Clock + Clone + Send + Sync + 'static> std::fmt::Display for SnapshotLeaseGuard<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lease(g) => write!(f, "{}", g.lease_id),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(_) => write!(f, "noop"),
        }
    }
}

impl<C: Clock + Clone + Send + Sync + 'static> SnapshotLeaseGuard<C> {
    /// Check if lease is still valid within the safety margin.
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Lease(g) => g.is_valid(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(g) => g.is_valid(),
        }
    }

    /// Cancellation token representing lease validity.
    ///
    /// Use this to abort long-running lease-guarded operations:
    /// ```ignore
    /// tokio::select! {
    ///     _ = lease_guard.lease_lost().cancelled() => { /* abort work */ }
    ///     result = do_work() => { /* handle result */ }
    /// }
    /// ```
    pub fn lease_lost(&self) -> CancellationToken {
        match self {
            Self::Lease(g) => g.lease_lost.clone(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(g) => g.lease_lost.clone(),
        }
    }

    /// Remaining time within the lease (accounting for the safety margin). The returned values are
    /// not monotonic - refreshing the lease will extend the remaining time. Use `run_under_lease`
    /// for a convenient wrapper that monitors lease expiry automatically.
    fn remaining_operation_time(&self) -> Duration {
        match self {
            Self::Lease(g) => g.duration_until_deadline(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(g) => g.duration_until_deadline(),
        }
    }

    /// Run a future under this lease, aborting if the lease is lost or deadline reached.
    ///
    /// This method is conservative about operation deadlines - it respects the shifting deadline
    /// that can be extended by background lease renewal. If the deadline expires without renewal,
    /// the work is aborted.
    pub async fn run_under_lease<F, T>(&self, work: F) -> Option<T>
    where
        F: std::future::Future<Output = T>,
    {
        let lease_lost = self.lease_lost();
        tokio::pin!(work);

        loop {
            let time_until_deadline = self.remaining_operation_time();

            if time_until_deadline.is_zero() && !self.is_valid() {
                warn!(lease = %self, "Snapshot lease lost (deadline expired), task was canceled");
                return None;
            }

            tokio::select! {
                biased;
                _ = lease_lost.cancelled() => {
                    warn!(lease = %self, "Snapshot lease lost (lease_lost cancelled), task was canceled");
                    return None;
                }
                // Deadline sleep BEFORE work for strict enforcement
                _ = tokio::time::sleep(time_until_deadline), if !time_until_deadline.is_zero() => {
                    if self.is_valid() {
                        // background renewal has extended deadline
                        continue;
                    }
                    warn!(lease = %self, "Snapshot lease lost (deadline reached), task was canceled");
                    return None;
                }
                result = &mut work => {
                    return Some(result);
                }
            }
        }
    }

    /// Start a background task that renews the lease at a fixed renewal interval.
    /// The task holds a weak reference to the lease and stops when the guard is dropped.
    /// The guard ensures that at most one renewal task is spawned.
    pub fn start_renewal_task(self: &Arc<Self>) -> Result<(), ShutdownError> {
        match self.as_ref() {
            Self::Lease(g) => g.start_renewal_task(Arc::downgrade(self)),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(_) => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn test_renew(&self) -> Result<(), LeaseError> {
        match self {
            Self::Lease(g) => g.renew().await,
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(_) => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_mark_released(&self) {
        match self {
            Self::Lease(g) => g.test_mark_released(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(_) => {}
        }
    }

    #[cfg(test)]
    pub(crate) fn operation_deadline(&self) -> MillisSinceEpoch {
        match self {
            Self::Lease(g) => g.operation_deadline(),
            Self::NoOp(g) => g.operation_deadline(),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl SnapshotLeaseGuard {
    pub fn noop() -> Self {
        Self::NoOp(inner::NoOpLeaseGuard::new(MillisSinceEpoch::MAX))
    }
}

mod inner {
    use std::ops::Add;

    use super::*;

    pub struct MetadataBackedLeaseGuard<C: Clock + Clone + Send + Sync + 'static = WallClock> {
        pub(super) partition_id: PartitionId,
        pub(super) lease_id: Ulid,
        holder: GenerationalNodeId,
        expires_at: AtomicU64,
        metadata_store_client: MetadataStoreClient,
        version: Mutex<Version>,
        released: AtomicBool,
        renewal_task: Mutex<Option<TaskHandle<()>>>,
        pub(super) lease_lost: CancellationToken,
        clock: C,
    }

    impl<C: Clock + Clone + Send + Sync + 'static> MetadataBackedLeaseGuard<C> {
        pub(super) fn new(
            partition_id: PartitionId,
            lease: SnapshotLeaseValue,
            metadata_store_client: MetadataStoreClient,
            version: Version,
            clock: C,
        ) -> Self {
            Self {
                partition_id,
                lease_id: lease.lease_id,
                holder: lease.holder,
                expires_at: AtomicU64::new(lease.expires_at.as_u64()),
                metadata_store_client,
                version: Mutex::new(version),
                released: AtomicBool::new(false),
                renewal_task: Mutex::new(None),
                lease_lost: CancellationToken::new(),
                clock,
            }
        }

        fn expires_at(&self) -> MillisSinceEpoch {
            MillisSinceEpoch::new(self.expires_at.load(Ordering::Acquire))
        }

        pub(super) fn is_valid(&self) -> bool {
            self.clock.recent() + LEASE_SAFETY_MARGIN < self.expires_at()
        }

        pub(super) async fn renew(&self) -> Result<(), LeaseError> {
            if self.lease_lost.is_cancelled() {
                debug!(
                    partition_id = %self.partition_id,
                    lease_id = %self.lease_id,
                    "Lease renewal skipped - lease already marked as lost"
                );
                return Err(LeaseError::Expired);
            }

            let key = lease_key(self.partition_id);
            let current_version = *self.version.lock();

            let stored: Option<SnapshotLeaseValue> =
                self.metadata_store_client.get(key.clone()).await?;
            let stored_lease = stored.ok_or(LeaseError::Expired)?;

            if stored_lease.lease_id != self.lease_id {
                warn!(
                    partition_id = %self.partition_id,
                    expected_lease_id = %self.lease_id,
                    actual_lease_id = %stored_lease.lease_id,
                    actual_holder = %stored_lease.holder,
                    "Lease was taken over by another node during renewal"
                );
                return Err(LeaseError::Expired);
            }

            let new_expiration = self.clock.recent().add(LEASE_DURATION);
            let renewed_lease = SnapshotLeaseValue {
                holder: self.holder,
                lease_id: self.lease_id,
                expires_at: new_expiration,
                version: current_version.next(),
            };

            self.metadata_store_client
                .put(
                    key,
                    &renewed_lease,
                    Precondition::MatchesVersion(current_version),
                )
                .await?;

            self.expires_at
                .store(new_expiration.as_u64(), Ordering::Release);
            *self.version.lock() = current_version.next();

            debug!(partition_id = %self.partition_id, "Snapshot lease renewed");
            Ok(())
        }

        pub(super) fn start_renewal_task(
            &self,
            guard: Weak<SnapshotLeaseGuard<C>>,
        ) -> Result<(), ShutdownError> {
            let mut renewal_task = self.renewal_task.lock();
            if renewal_task.is_some() {
                return Ok(());
            }

            let lease_lost = self.lease_lost.clone();
            let partition_id = self.partition_id;

            let handle = TaskCenter::current().spawn_unmanaged_child(
                TaskKind::Disposable,
                "snapshot-lease-renewal",
                async move {
                    loop {
                        tokio::select! {
                            _ = lease_lost.cancelled() => break,
                            _ = tokio::time::sleep(LEASE_RENEWAL_INTERVAL) => {
                                let Some(guard) = guard.upgrade() else {
                                    debug!(%partition_id, "Lease guard dropped, stopping renewal");
                                    break;
                                };
                                match guard.as_ref() {
                                    SnapshotLeaseGuard::Lease(g) => {
                                        if let Err(e) = g.renew().await {
                                            warn!(%partition_id, error = %e, "Lease renewal failed");
                                            g.lease_lost.cancel();
                                            break;
                                        }
                                    }
                                    #[cfg(any(test, feature = "test-util"))]
                                    SnapshotLeaseGuard::NoOp(_) => {}
                                }
                            }
                        }
                    }
                },
            )?;
            *renewal_task = Some(handle);
            Ok(())
        }

        fn spawn_release_task(&mut self) {
            if !self.released.swap(true, Ordering::AcqRel) {
                let client = self.metadata_store_client.clone();
                let partition_id = self.partition_id;
                let version = *self.version.lock();
                let lease_id = self.lease_id;
                let holder = self.holder;

                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        let key = lease_key(partition_id);
                        let expired = SnapshotLeaseValue::expired(holder, lease_id)
                            .with_version(version.next());

                        match client
                            .put(key, &expired, Precondition::MatchesVersion(version))
                            .await
                        {
                            Ok(()) => {
                                debug!(%partition_id, "Lease released via Drop");
                            }
                            Err(WriteError::FailedPrecondition(_)) => {
                                debug!(
                                    %partition_id,
                                    "Lease release via Drop failed: version mismatch (lease was likely taken over)"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    %partition_id,
                                    %err,
                                    "Lease release via Drop failed"
                                );
                            }
                        }
                    });
                    debug!(
                        partition_id = %partition_id,
                        "Snapshot lease dropped, spawned async release"
                    );
                } else {
                    warn!(
                        partition_id = %partition_id,
                        "Snapshot lease dropped without release - no runtime available"
                    );
                }
            }
        }

        #[cfg(test)]
        pub(super) fn test_mark_released(&self) {
            self.released.store(true, Ordering::Release);
            self.lease_lost.cancel();
        }

        #[cfg(test)]
        pub(super) fn operation_deadline(&self) -> MillisSinceEpoch {
            self.expires_at() - LEASE_SAFETY_MARGIN
        }

        pub(super) fn duration_until_deadline(&self) -> Duration {
            let now = self.clock.recent();
            let deadline = self.expires_at() - LEASE_SAFETY_MARGIN;
            deadline.duration_since(now)
        }
    }

    impl<C: Clock + Clone + Send + Sync + 'static> Drop for MetadataBackedLeaseGuard<C> {
        fn drop(&mut self) {
            self.lease_lost.cancel();
            self.spawn_release_task();
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub struct NoOpLeaseGuard {
        expires_at: MillisSinceEpoch,
        /// Cancelled on drop to match MetadataBackedLeaseGuard behavior.
        pub(super) lease_lost: CancellationToken,
    }

    #[cfg(any(test, feature = "test-util"))]
    impl NoOpLeaseGuard {
        pub(super) fn new(expires_at: MillisSinceEpoch) -> Self {
            Self {
                expires_at,
                lease_lost: CancellationToken::new(),
            }
        }

        pub(super) fn is_valid(&self) -> bool {
            WallClock.recent() + LEASE_SAFETY_MARGIN < self.expires_at
        }

        #[cfg(test)]
        pub(super) fn operation_deadline(&self) -> MillisSinceEpoch {
            self.expires_at - LEASE_SAFETY_MARGIN
        }

        pub(super) fn duration_until_deadline(&self) -> Duration {
            let deadline = self.expires_at - LEASE_SAFETY_MARGIN;
            deadline.duration_since(WallClock.recent())
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    impl Drop for NoOpLeaseGuard {
        fn drop(&mut self) {
            self.lease_lost.cancel();
        }
    }
}

#[derive(Clone)]
pub struct SnapshotLeaseManager<C: Clock + Clone + Send + Sync + 'static = WallClock> {
    metadata_store_client: MetadataStoreClient,
    /// Node id to use for leases. If None, fetched from Metadata at acquire time.
    node_id: Option<GenerationalNodeId>,
    clock: C,
}

impl SnapshotLeaseManager<WallClock> {
    pub fn new(metadata_store_client: MetadataStoreClient) -> Self {
        Self {
            metadata_store_client,
            node_id: None,
            clock: WallClock,
        }
    }
}

impl<C: Clock + Clone + Send + Sync + 'static> SnapshotLeaseManager<C> {
    #[cfg(test)]
    pub fn new_with_clock(
        metadata_store_client: MetadataStoreClient,
        node_id: GenerationalNodeId,
        clock: C,
    ) -> Self {
        Self {
            metadata_store_client,
            node_id: Some(node_id),
            clock,
        }
    }

    /// Acquire a lease for the given partition.
    ///
    /// Returns error if another valid lease exists or if the metadata store is unavailable.
    /// Transient metadata store failures are retried with exponential backoff.
    pub async fn acquire(
        &self,
        partition_id: PartitionId,
    ) -> Result<SnapshotLeaseGuard<C>, LeaseError> {
        let retry_policy: RetryPolicy = Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone();

        retry_policy
            .retry_if(|| self.try_acquire(partition_id), LeaseError::retryable)
            .await
    }

    #[instrument(level = "debug", skip_all, fields(%partition_id))]
    async fn try_acquire(
        &self,
        partition_id: PartitionId,
    ) -> Result<SnapshotLeaseGuard<C>, LeaseError> {
        let my_node_id = self
            .node_id
            .unwrap_or_else(|| Metadata::with_current(|m| m.my_node_id()));
        let key = lease_key(partition_id);

        let new_lease = SnapshotLeaseValue::new(my_node_id, &self.clock);
        let new_lease_id = new_lease.lease_id;

        let existing: Option<SnapshotLeaseValue> =
            self.metadata_store_client.get(key.clone()).await?;

        match existing {
            None => {
                let written_version = Version::MIN;
                let lease_to_write = new_lease.with_version(written_version);
                self.metadata_store_client
                    .put(key.clone(), &lease_to_write, Precondition::DoesNotExist)
                    .await?;

                debug!(lease_id = %new_lease_id, "Acquired snapshot lease (new)");

                Ok(SnapshotLeaseGuard::Lease(
                    inner::MetadataBackedLeaseGuard::new(
                        partition_id,
                        lease_to_write,
                        self.metadata_store_client.clone(),
                        written_version,
                        self.clock.clone(),
                    ),
                ))
            }
            Some(existing_lease) => {
                if existing_lease.is_expired(&self.clock) {
                    let written_version = existing_lease.version.next();
                    let lease_to_write = new_lease.with_version(written_version);
                    self.metadata_store_client
                        .put(
                            key,
                            &lease_to_write,
                            Precondition::MatchesVersion(existing_lease.version),
                        )
                        .await?;

                    debug!(
                        lease_id = %new_lease_id,
                        previous_holder = %existing_lease.holder,
                        "Acquired snapshot lease (expired takeover)"
                    );

                    Ok(SnapshotLeaseGuard::Lease(
                        inner::MetadataBackedLeaseGuard::new(
                            partition_id,
                            lease_to_write,
                            self.metadata_store_client.clone(),
                            written_version,
                            self.clock.clone(),
                        ),
                    ))
                } else {
                    Err(LeaseError::Held {
                        holder: existing_lease.holder,
                        lease_id: existing_lease.lease_id,
                        expires_at: existing_lease.expires_at.into_timestamp(),
                    })
                }
            }
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Default)]
pub struct NoOpLeaseManager;

#[cfg(any(test, feature = "test-util"))]
impl NoOpLeaseManager {
    pub fn new() -> Self {
        Self
    }

    pub async fn acquire(
        &self,
        _partition_id: PartitionId,
    ) -> Result<SnapshotLeaseGuard, LeaseError> {
        let expires_at =
            MillisSinceEpoch::new(WallClock.recent().as_u64() + 365 * 24 * 60 * 60 * 1000);
        Ok(SnapshotLeaseGuard::NoOp(inner::NoOpLeaseGuard::new(
            expires_at,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use restate_clock::MockClock;
    use restate_core::TestCoreEnv;
    use restate_test_util::let_assert;
    use restate_types::GenerationalNodeId;
    use restate_types::identifiers::PartitionId;

    use super::*;

    #[restate_core::test]
    async fn test_lease_acquire_no_existing() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            clock.clone(),
        );

        let guard = manager
            .acquire(PartitionId::MIN)
            .await
            .expect("acquire should succeed");

        assert!(guard.is_valid());
        assert_eq!(
            guard.operation_deadline(),
            clock.recent() + LEASE_DURATION - LEASE_SAFETY_MARGIN
        );

        // validity window spans (time acquired + lease duration - safety margin)
        clock.advance(LEASE_DURATION - LEASE_SAFETY_MARGIN - Duration::from_millis(1));
        assert!(guard.is_valid());

        clock.advance_ms(1);
        assert!(!guard.is_valid());
    }

    #[restate_core::test]
    async fn test_lease_acquire_expired_takeover() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let n1 = GenerationalNodeId::new(1, 1);
        let n2 = GenerationalNodeId::new(2, 1);

        let n2_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n2,
            clock.clone(),
        );
        let _guard = n2_manager
            .acquire(PartitionId::MIN)
            .await
            .expect("initial acquire should succeed");

        clock.advance(LEASE_DURATION + Duration::from_millis(100));

        let n1_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n1,
            clock.clone(),
        );
        let guard = n1_manager
            .acquire(PartitionId::MIN)
            .await
            .expect("takeover should succeed");
        assert!(guard.is_valid());
    }

    #[restate_core::test]
    async fn test_lease_acquire_held_by_other_node() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let n1 = GenerationalNodeId::new(1, 1);
        let n2 = GenerationalNodeId::new(2, 1);

        let n2_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n2,
            clock.clone(),
        );
        let _guard = n2_manager
            .acquire(PartitionId::MIN)
            .await
            .expect("initial acquire should succeed");

        let expected_expiry = clock.recent() + LEASE_DURATION;
        clock.advance_ms(500);

        let n1_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n1,
            clock.clone(),
        );
        let result = n1_manager.acquire(PartitionId::MIN).await;

        let_assert!(
            Err(LeaseError::Held {
                holder,
                expires_at,
                ..
            }) = result
        );
        assert_eq!(holder, n2);
        assert_eq!(expected_expiry.into_timestamp(), expires_at);
    }

    #[restate_core::test]
    async fn test_lease_renewal_extends_expiry() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            clock.clone(),
        );
        let guard = manager
            .acquire(PartitionId::MIN)
            .await
            .expect("acquire should succeed");

        let initial_deadline = guard.operation_deadline();

        clock.advance(Duration::from_mins(2));
        guard.test_renew().await.expect("renewal should succeed");

        assert_eq!(
            guard.operation_deadline(),
            initial_deadline + Duration::from_mins(2)
        );
    }

    #[restate_core::test]
    async fn test_lease_renewal_fails_when_taken_over() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let n1 = GenerationalNodeId::new(1, 1);
        let n2 = GenerationalNodeId::new(2, 1);

        let n1_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n1,
            clock.clone(),
        );
        let guard = n1_manager
            .acquire(PartitionId::MIN)
            .await
            .expect("acquire should succeed");

        clock.advance(LEASE_DURATION + Duration::from_millis(1000));

        // another node takes over
        let n2_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n2,
            clock.clone(),
        );
        let _n2_guard = n2_manager
            .acquire(PartitionId::MIN)
            .await
            .expect("takeover should succeed");

        // original lease can no longer be renewed
        let result = guard.test_renew().await;
        assert!(matches!(result, Err(LeaseError::Expired)));
    }

    #[restate_core::test]
    async fn test_lease_acquire_concurrent_race() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            MockClock::new(),
        );

        for _ in 0..100 {
            let (result1, result2) = tokio::join!(
                manager.acquire(PartitionId::MIN),
                manager.acquire(PartitionId::MIN)
            );

            // exactly one should succeed
            let (guard, err) = match (result1, result2) {
                (Ok(g), Err(e)) | (Err(e), Ok(g)) => (g, e),
                (Ok(_), Ok(_)) => panic!("Both acquires succeeded"),
                (Err(e1), Err(e2)) => panic!("Both acquires failed: {e1:?}, {e2:?}"),
            };
            assert!(guard.is_valid());
            assert!(matches!(
                err,
                LeaseError::Held { .. }
                    | LeaseError::MetadataWrite(WriteError::FailedPrecondition(_))
            ));

            drop(guard);
            // allow async release task to complete
            tokio::task::yield_now().await;
        }
    }

    #[restate_core::test]
    async fn test_lease_renewal_fails_when_released() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            MockClock::new(),
        );
        let guard = manager
            .acquire(PartitionId::MIN)
            .await
            .expect("acquire should succeed");

        guard.test_mark_released();

        let result = guard.test_renew().await;
        assert!(matches!(result, Err(LeaseError::Expired)));
    }

    #[restate_core::test(start_paused = true)]
    async fn test_lease_start_renewal_lifecycle() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = TokioClock::new();
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            clock,
        );

        let guard = Arc::new(
            manager
                .acquire(PartitionId::MIN)
                .await
                .expect("acquire should succeed"),
        );

        assert!(guard.is_valid());

        guard.start_renewal_task().expect("starts renewal task");
        guard.start_renewal_task().expect("no-op");

        let initial_deadline = guard.operation_deadline();

        // Sleep past the renewal interval - the renewal task will wake and extend the deadline
        tokio::time::sleep(LEASE_RENEWAL_INTERVAL + Duration::from_secs(1)).await;

        let expected_deadline = initial_deadline + LEASE_RENEWAL_INTERVAL;

        assert_eq!(
            guard.operation_deadline(),
            expected_deadline,
            "Lease should have been renewed, extending the operation deadline"
        );

        assert!(guard.is_valid());
        drop(guard); // cancel renewal task

        for _ in 0..10 {
            match manager.acquire(PartitionId::MIN).await {
                Ok(_) => return,
                Err(LeaseError::Held { .. }) => tokio::task::yield_now().await,
                Err(e) => panic!("Unexpected error during acquire: {:?}", e),
            }
        }
        panic!("Should be able to re-acquire after drop");
    }

    #[restate_core::test]
    async fn test_lease_release_on_drop() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let n1 = GenerationalNodeId::new(1, 1);
        let n2 = GenerationalNodeId::new(2, 1);

        {
            let n1_manager = SnapshotLeaseManager::new_with_clock(
                env.metadata_store_client.clone(),
                n1,
                clock.clone(),
            );
            // immediately dropped
            let _lease = n1_manager
                .acquire(PartitionId::MIN)
                .await
                .expect("acquire should succeed");
        }

        let n2_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            n2,
            clock.clone(),
        );

        let mut acquired = false;
        for _ in 0..10 {
            match n2_manager.acquire(PartitionId::MIN).await {
                Ok(_) => {
                    acquired = true;
                    break;
                }
                Err(LeaseError::Held { .. }) => {
                    // release task might not have completed yet
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("Unexpected error during acquire: {:?}", e),
            }
        }

        assert!(
            acquired,
            "other node should be able to acquire after release (within timeout)"
        );
    }

    #[restate_core::test]
    async fn test_lease_lost_on_drop_and_run_under_lease() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            MockClock::new(),
        );

        let guard = manager
            .acquire(PartitionId::MIN)
            .await
            .expect("acquire should succeed");

        let lease_lost = guard.lease_lost();
        assert!(!lease_lost.is_cancelled());

        let result = guard.run_under_lease(async { 42 }).await;
        assert_eq!(result, Some(42));

        guard.test_mark_released();
        assert!(lease_lost.is_cancelled());

        let result = guard.run_under_lease(async { 42 }).await;
        assert_eq!(result, None);
    }

    #[restate_core::test(start_paused = true)]
    async fn test_run_under_lease_respects_deadline_with_renewal() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = TokioClock::new();
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            clock,
        );

        let guard = Arc::new(
            manager
                .acquire(PartitionId::MIN)
                .await
                .expect("acquire should succeed"),
        );
        guard.start_renewal_task().expect("should start renewal");
        let initial_deadline = guard.operation_deadline();

        // the mock work future sleeps slightly longer than the renewal interval
        // renewal task's sleep future fires first, extending the deadline
        let result = guard
            .run_under_lease(async {
                tokio::time::sleep(LEASE_RENEWAL_INTERVAL + Duration::from_secs(1)).await;
                42
            })
            .await;

        assert!(
            guard.operation_deadline() > initial_deadline,
            "Deadline should have been extended by renewal"
        );
        assert_eq!(result, Some(42), "Work should complete with lease renewal");
    }

    #[restate_core::test(start_paused = true)]
    async fn test_run_under_lease_aborts_at_deadline_without_renewal() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = TokioClock::new();
        let manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            GenerationalNodeId::new(1, 1),
            clock,
        );

        let guard = manager
            .acquire(PartitionId::MIN)
            .await
            .expect("acquire should succeed");

        // no lease renewal task started

        let result = guard
            .run_under_lease(async {
                tokio::time::sleep(LEASE_DURATION).await;
                42
            })
            .await;

        assert_eq!(result, None, "Work should be aborted at deadline");
    }

    /// Handy for paused tokio tests, this clock reflects `tokio::time::advance()` calls eliminating
    /// the need to manipulate two time sources.
    #[derive(Clone)]
    struct TokioClock {
        initial_instant: tokio::time::Instant,
    }

    impl TokioClock {
        fn new() -> Self {
            Self {
                initial_instant: tokio::time::Instant::now(),
            }
        }

        /// Fixed base time for deterministic tests (well after RESTATE_EPOCH)
        const BASE_TIME: MillisSinceEpoch = MillisSinceEpoch::new(1_700_000_000_000);
    }

    impl Clock for TokioClock {
        fn recent(&self) -> MillisSinceEpoch {
            let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
            Self::BASE_TIME + elapsed
        }

        fn now(&self) -> MillisSinceEpoch {
            self.recent()
        }
    }
}
