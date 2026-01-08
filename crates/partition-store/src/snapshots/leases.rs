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
use tracing::{debug, warn};
use ulid::Ulid;

use restate_clock::{Clock, WallClock};
use restate_core::{Metadata, TaskCenter, TaskKind};
use restate_metadata_server::{MetadataStoreClient, ReadError, WriteError};
use restate_types::Version;
use restate_types::identifiers::PartitionId;
use restate_types::metadata::Precondition;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, Versioned, flexbuffers_storage_encode_decode};

/// Duration for which a snapshot operation lease is valid (5 minutes).
///
/// Clock skew assumption: Nodes in the cluster should have clocks synchronized within
/// `LEASE_SAFETY_MARGIN_MS` (1 minute). Larger clock skew could cause a node to believe
/// a lease has expired while the holder still considers it valid, leading to concurrent
/// operations. NTP synchronization typically provides sub-second accuracy, well within
/// this tolerance.
pub const LEASE_DURATION_MS: u64 = 300_000;

/// Minimum remaining lease time required to continue operations (1 minute).
/// Operations should complete before `expires_at - LEASE_SAFETY_MARGIN_MS`.
pub const LEASE_SAFETY_MARGIN_MS: u64 = 60_000;

/// Interval at which lease renewal should occur (2 minutes).
/// This should be well before lease expiry to ensure smooth operations.
pub const LEASE_RENEWAL_INTERVAL_MS: u64 = 120_000;

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("Lease held by {holder} (lease_id: {lease_id}), expires in {remaining_millis}ms")]
    Held {
        holder: GenerationalNodeId,
        lease_id: Ulid,
        remaining_millis: u64,
    },
    #[error("Concurrent operation from same node (existing lease_id: {existing}, new: {new})")]
    ConcurrentSameNode { existing: Ulid, new: Ulid },
    #[error("Lease expired or taken over by another node")]
    LeaseExpired,
    #[error("Snapshot repository is read-only (no lease provider configured)")]
    ReadOnly,
    #[error(transparent)]
    MetadataRead(#[from] ReadError),
    #[error(transparent)]
    MetadataWrite(#[from] WriteError),
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
        let now = clock.recent();
        let expires_at = MillisSinceEpoch::new(now.as_u64() + LEASE_DURATION_MS);
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

    fn remaining_millis(&self, clock: &impl Clock) -> u64 {
        let now = clock.recent().as_u64();
        self.expires_at.as_u64().saturating_sub(now)
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

impl<C: Clock + Clone + Send + Sync + 'static> SnapshotLeaseGuard<C> {
    /// Check if lease is still valid within the safety margin
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Lease(g) => g.is_valid(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(g) => g.is_valid(),
        }
    }

    /// Get the operation deadline (expires_at - safety margin)
    // todo: is this really necessary, if we have auto-renew?
    pub fn operation_deadline(&self) -> MillisSinceEpoch {
        match self {
            Self::Lease(g) => g.operation_deadline(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(g) => g.operation_deadline(),
        }
    }

    /// Start a background task that periodically renews the lease.
    /// The renewal task will be automatically cancelled when the guard is dropped.
    /// This method should be called on an `Arc<SnapshotLeaseGuard>` after acquisition.
    pub fn start_renewal(self: &Arc<Self>) {
        match self.as_ref() {
            Self::Lease(g) => g.start_renewal(Arc::downgrade(self)),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(_) => { /* no-op */ }
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
            Self::Lease(g) => g.mark_released(),
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(_) => {}
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
    use super::*;

    pub struct MetadataBackedLeaseGuard<C: Clock + Clone + Send + Sync + 'static = WallClock> {
        partition_id: PartitionId,
        lease_id: Ulid,
        holder: GenerationalNodeId,
        expires_at: AtomicU64,
        metadata_store_client: MetadataStoreClient,
        version: Mutex<Version>,
        released: AtomicBool,
        renewal_cancel: CancellationToken,
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
                renewal_cancel: CancellationToken::new(),
                clock,
            }
        }

        fn expires_at(&self) -> u64 {
            self.expires_at.load(Ordering::Acquire)
        }

        pub(super) fn is_valid(&self) -> bool {
            let now = self.clock.recent().as_u64();
            now + LEASE_SAFETY_MARGIN_MS < self.expires_at()
        }

        pub(super) fn operation_deadline(&self) -> MillisSinceEpoch {
            MillisSinceEpoch::new(self.expires_at().saturating_sub(LEASE_SAFETY_MARGIN_MS))
        }

        pub(super) async fn renew(&self) -> Result<(), LeaseError> {
            if self.released.load(Ordering::Acquire) {
                return Err(LeaseError::LeaseExpired);
            }

            let key = lease_key(self.partition_id);
            let current_version = *self.version.lock();

            let stored: Option<SnapshotLeaseValue> =
                self.metadata_store_client.get(key.clone()).await?;
            let stored_lease = stored.ok_or(LeaseError::LeaseExpired)?;

            if stored_lease.lease_id != self.lease_id {
                return Err(LeaseError::LeaseExpired);
            }

            let new_expires =
                MillisSinceEpoch::new(self.clock.recent().as_u64() + LEASE_DURATION_MS);
            let renewed_lease = SnapshotLeaseValue {
                holder: self.holder,
                lease_id: self.lease_id,
                expires_at: new_expires,
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
                .store(new_expires.as_u64(), Ordering::Release);
            *self.version.lock() = current_version.next();

            debug!(partition_id = %self.partition_id, "Snapshot lease renewed");
            Ok(())
        }

        #[cfg(test)]
        pub(super) fn mark_released(&self) {
            self.released.store(true, Ordering::Release);
        }

        /// Start the renewal task with a weak reference to the guard.
        ///
        /// Using `Weak` instead of `Arc` is critical: it prevents a circular reference where
        /// the renewal task holds a strong reference that prevents the guard from being dropped,
        /// which in turn prevents the cancellation signal that would stop the renewal task.
        pub(super) fn start_renewal(&self, guard: Weak<SnapshotLeaseGuard<C>>) {
            let cancel = self.renewal_cancel.clone();
            let partition_id = self.partition_id;

            let spawned = TaskCenter::try_with_current(|tc| {
                let _ = tc.spawn_unmanaged(
                    TaskKind::Disposable,
                    "snapshot-lease-renewal",
                    async move {
                        loop {
                            tokio::select! {
                                _ = cancel.cancelled() => break,
                                _ = tokio::time::sleep(Duration::from_millis(LEASE_RENEWAL_INTERVAL_MS)) => {
                                    // Try to upgrade the weak reference; if it fails, the guard
                                    // has been dropped and we should exit.
                                    let Some(guard) = guard.upgrade() else {
                                        debug!(%partition_id, "Lease guard dropped, stopping renewal");
                                        break;
                                    };
                                    match guard.as_ref() {
                                        SnapshotLeaseGuard::Lease(g) => {
                                            if let Err(e) = g.renew().await {
                                                warn!(%partition_id, error = %e, "Lease renewal failed");
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
                );
            });

            if spawned.is_none() {
                warn!(
                    %partition_id,
                    "Snapshot lease renewal task not started - no TaskCenter available"
                );
            }
        }

        /// Spawns a task to release the lease asynchronously.
        ///
        /// Note: This uses `tokio::runtime::Handle::try_current()` instead of TaskCenter
        /// because Drop handlers run in a synchronous context where the TaskCenter task-local
        /// context may not be available, but the Tokio runtime handle is still accessible.
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
                            Err(e) => {
                                debug!(
                                    %partition_id,
                                    error = %e,
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
    }

    impl<C: Clock + Clone + Send + Sync + 'static> Drop for MetadataBackedLeaseGuard<C> {
        fn drop(&mut self) {
            self.renewal_cancel.cancel();
            self.spawn_release_task();
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub struct NoOpLeaseGuard {
        expires_at: MillisSinceEpoch,
    }

    #[cfg(any(test, feature = "test-util"))]
    impl NoOpLeaseGuard {
        pub(super) fn new(expires_at: MillisSinceEpoch) -> Self {
            Self { expires_at }
        }

        pub(super) fn is_valid(&self) -> bool {
            let now = WallClock.recent().as_u64();
            now + LEASE_SAFETY_MARGIN_MS < self.expires_at.as_u64()
        }

        pub(super) fn operation_deadline(&self) -> MillisSinceEpoch {
            MillisSinceEpoch::new(
                self.expires_at
                    .as_u64()
                    .saturating_sub(LEASE_SAFETY_MARGIN_MS),
            )
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
    /// Transient failures (network issues, metadata store unavailability) are propagated
    /// as errors; callers should implement retry logic at a higher level if needed.
    pub async fn acquire(
        &self,
        partition_id: PartitionId,
    ) -> Result<SnapshotLeaseGuard<C>, LeaseError> {
        let my_node_id = self
            .node_id
            .unwrap_or_else(|| Metadata::with_current(|m| m.my_node_id()));
        let key = lease_key(partition_id);

        // Create new lease
        let new_lease = SnapshotLeaseValue::new(my_node_id, &self.clock);
        let new_lease_id = new_lease.lease_id;

        // Check existing lease
        let existing: Option<SnapshotLeaseValue> =
            self.metadata_store_client.get(key.clone()).await?;

        match existing {
            None => {
                // No existing lease, try to create
                let written_version = Version::MIN;
                let lease_to_write = new_lease.with_version(written_version);
                match self
                    .metadata_store_client
                    .put(key.clone(), &lease_to_write, Precondition::DoesNotExist)
                    .await
                {
                    Ok(()) => {
                        debug!(
                            partition_id = %partition_id,
                            lease_id = %new_lease_id,
                            "Acquired snapshot lease (new)"
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
                    }
                    Err(WriteError::FailedPrecondition(_)) => {
                        // Another node created a lease between our read and write, re-read
                        let current: Option<SnapshotLeaseValue> =
                            self.metadata_store_client.get(key).await?;
                        if let Some(current_lease) = current {
                            if current_lease.is_expired(&self.clock) {
                                // Race but lease is expired, caller can retry
                                Err(LeaseError::MetadataWrite(WriteError::FailedPrecondition(
                                    "Concurrent lease creation, please retry".into(),
                                )))
                            } else if current_lease.holder == my_node_id {
                                warn!(
                                    %partition_id,
                                    existing_lease_id = %current_lease.lease_id,
                                    new_lease_id = %new_lease_id,
                                    "Same node attempted concurrent lease acquisition - this may indicate a bug"
                                );
                                Err(LeaseError::ConcurrentSameNode {
                                    existing: current_lease.lease_id,
                                    new: new_lease_id,
                                })
                            } else {
                                Err(LeaseError::Held {
                                    holder: current_lease.holder,
                                    lease_id: current_lease.lease_id,
                                    remaining_millis: current_lease.remaining_millis(&self.clock),
                                })
                            }
                        } else {
                            // Very unlikely: lease was created then deleted
                            Err(LeaseError::MetadataWrite(WriteError::FailedPrecondition(
                                "Concurrent lease modification, please retry".into(),
                            )))
                        }
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Some(existing_lease) => {
                if existing_lease.is_expired(&self.clock) {
                    // Lease expired, take over
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
                        partition_id = %partition_id,
                        lease_id = %new_lease_id,
                        previous_holder = %existing_lease.holder,
                        "Acquired snapshot lease (expired takeover)"
                    );

                    // Store the WRITTEN version, not the precondition version
                    Ok(SnapshotLeaseGuard::Lease(
                        inner::MetadataBackedLeaseGuard::new(
                            partition_id,
                            lease_to_write,
                            self.metadata_store_client.clone(),
                            written_version,
                            self.clock.clone(),
                        ),
                    ))
                } else if existing_lease.holder == my_node_id {
                    // Same node but different operation - concurrent ops from same node
                    warn!(
                        %partition_id,
                        existing_lease_id = %existing_lease.lease_id,
                        new_lease_id = %new_lease_id,
                        "Same node attempted concurrent lease acquisition - this may indicate a bug"
                    );
                    Err(LeaseError::ConcurrentSameNode {
                        existing: existing_lease.lease_id,
                        new: new_lease_id,
                    })
                } else {
                    // Another node holds a valid lease
                    Err(LeaseError::Held {
                        holder: existing_lease.holder,
                        lease_id: existing_lease.lease_id,
                        remaining_millis: existing_lease.remaining_millis(&self.clock),
                    })
                }
            }
        }
    }
}

/// No-op lease manager for testing without cluster infrastructure.
/// Always succeeds and returns guards that are always valid (far-future expiry).
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
    use std::time::Duration;

    use restate_clock::MockClock;
    use restate_core::TestCoreEnv;
    use restate_types::GenerationalNodeId;
    use restate_types::identifiers::PartitionId;
    use restate_types::time::MillisSinceEpoch;

    use super::*;

    // ================== Core Acquire Tests ==================

    /// Test acquiring a lease when no existing lease exists
    #[restate_core::test]
    async fn test_lease_acquire_no_existing() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let partition_id = PartitionId::MIN;

        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        // Verify guard is valid
        assert!(guard.is_valid());

        // Verify operation deadline is set correctly (expires_at - safety margin)
        let deadline = guard.operation_deadline();
        let now = clock.recent().as_u64();
        // expires_at should be ~5 minutes from now, deadline should be ~4 minutes (5 - 1 minute margin)
        assert!(deadline.as_u64() > now + LEASE_DURATION_MS - LEASE_SAFETY_MARGIN_MS - 1000);
        assert!(deadline.as_u64() < now + LEASE_DURATION_MS);
    }

    /// Test acquiring a lease when an expired lease exists (takeover)
    #[restate_core::test]
    async fn test_lease_acquire_expired_takeover() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let other_node_id = GenerationalNodeId::new(2, 1);
        let partition_id = PartitionId::MIN;

        // First, create an expired lease from another node
        let other_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            other_node_id,
            clock.clone(),
        );
        let _guard = other_manager
            .acquire(partition_id)
            .await
            .expect("initial acquire should succeed");

        // Advance time past lease expiration
        clock.advance_ms(LEASE_DURATION_MS + 1000);

        // Now try to acquire with our node
        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("takeover should succeed");
        assert!(guard.is_valid());
    }

    /// Test that acquiring fails when another node holds a valid lease
    #[restate_core::test]
    async fn test_lease_acquire_held_by_other_node() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let other_node_id = GenerationalNodeId::new(2, 1);
        let partition_id = PartitionId::MIN;

        // First, acquire a lease with another node
        let other_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            other_node_id,
            clock.clone(),
        );
        // Don't drop the guard to keep the lease "active"
        let _guard = other_manager
            .acquire(partition_id)
            .await
            .expect("initial acquire should succeed");

        // Try to acquire with our node
        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let result = manager.acquire(partition_id).await;

        match result {
            Err(LeaseError::Held {
                holder,
                remaining_millis,
                ..
            }) => {
                assert_eq!(holder, other_node_id);
                // Remaining time should be close to LEASE_DURATION_MS
                assert!(remaining_millis > LEASE_DURATION_MS - 1000);
            }
            Err(e) => panic!("Expected LeaseError::Held, got error: {:?}", e),
            Ok(_) => panic!("Expected LeaseError::Held, but acquire succeeded"),
        }
    }

    /// Test that same node concurrent acquisition is rejected
    #[restate_core::test]
    async fn test_lease_acquire_same_node_concurrent() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        // Acquire a lease
        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let _guard = manager
            .acquire(partition_id)
            .await
            .expect("first acquire should succeed");

        // Try to acquire again with same node (simulating concurrent operation)
        let result = manager.acquire(partition_id).await;

        match result {
            Err(LeaseError::ConcurrentSameNode { .. }) => {
                // Expected
            }
            Err(e) => panic!(
                "Expected LeaseError::ConcurrentSameNode, got error: {:?}",
                e
            ),
            Ok(_) => panic!("Expected LeaseError::ConcurrentSameNode, but acquire succeeded"),
        }
    }

    // ================== Validity Boundary Tests ==================

    /// Test that is_valid returns true when within safety margin
    #[restate_core::test]
    async fn test_lease_guard_validity_within_margin() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        // Immediately after acquisition, should be valid
        assert!(guard.is_valid());

        // Advance time but stay within safety margin
        clock.advance_ms(LEASE_DURATION_MS - LEASE_SAFETY_MARGIN_MS - 10_000);
        assert!(guard.is_valid());
    }

    /// Test that is_valid returns false at the safety margin boundary
    #[restate_core::test]
    async fn test_lease_guard_validity_at_margin() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        // Advance time to exactly at the safety margin boundary
        clock.advance_ms(LEASE_DURATION_MS - LEASE_SAFETY_MARGIN_MS);
        assert!(
            !guard.is_valid(),
            "should be invalid at safety margin boundary"
        );

        // Advance time past expiry
        clock.advance_ms(LEASE_SAFETY_MARGIN_MS + 1000);
        assert!(!guard.is_valid(), "should be invalid after expiry");
    }

    /// Test operation_deadline calculation with edge values
    #[restate_core::test]
    async fn test_lease_operation_deadline_saturating() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        let deadline = guard.operation_deadline();
        // deadline should be expires_at - LEASE_SAFETY_MARGIN_MS
        // which should be now + LEASE_DURATION_MS - LEASE_SAFETY_MARGIN_MS
        let expected = clock.recent().as_u64() + LEASE_DURATION_MS - LEASE_SAFETY_MARGIN_MS;
        assert!(deadline.as_u64() >= expected - 100 && deadline.as_u64() <= expected + 100);
    }

    // ================== Renewal Tests ==================

    /// Test that renewal extends the expiry time
    #[restate_core::test]
    async fn test_lease_renewal_extends_expiry() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        let initial_deadline = guard.operation_deadline();

        // Advance time by 2 minutes
        clock.advance_ms(120_000);

        // Renew the lease
        guard.test_renew().await.expect("renewal should succeed");

        let new_deadline = guard.operation_deadline();
        // New deadline should be extended (now + LEASE_DURATION_MS - SAFETY_MARGIN)
        assert!(new_deadline.as_u64() > initial_deadline.as_u64());
    }

    /// Test that renewal fails when the lease has been taken over by another node
    #[restate_core::test]
    async fn test_lease_renewal_fails_when_taken_over() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let other_node_id = GenerationalNodeId::new(2, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        // Simulate lease expiry and takeover
        clock.advance_ms(LEASE_DURATION_MS + 1000);

        // Other node takes over
        let other_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            other_node_id,
            clock.clone(),
        );
        let _other_guard = other_manager
            .acquire(partition_id)
            .await
            .expect("takeover should succeed");

        // Original guard renewal should fail
        let result = guard.test_renew().await;
        match result {
            Err(LeaseError::LeaseExpired) => {
                // Expected - lease was taken over (lease_id changed)
            }
            Err(e) => panic!("Expected LeaseError::LeaseExpired, got error: {:?}", e),
            Ok(_) => panic!("Expected LeaseError::LeaseExpired, but renewal succeeded"),
        }
    }

    // ================== Race Condition Tests ==================

    /// Test concurrent acquire attempts - one should succeed, one should fail
    /// This exercises the race detection code path (lines 496-529)
    #[restate_core::test]
    async fn test_lease_acquire_concurrent_race() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node1 = GenerationalNodeId::new(1, 1);
        let node2 = GenerationalNodeId::new(2, 1);
        let partition_id = PartitionId::MIN;

        let manager1 = SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node1, clock.clone());
        let manager2 = SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node2, clock.clone());

        // Run both acquires concurrently
        let (result1, result2) = tokio::join!(
            manager1.acquire(partition_id),
            manager2.acquire(partition_id)
        );

        // Exactly one should succeed, one should fail
        match (result1, result2) {
            (Ok(guard), Err(e)) | (Err(e), Ok(guard)) => {
                // The successful one should have a valid guard
                assert!(guard.is_valid());

                // The failed one should be either Held or a precondition failure
                match e {
                    LeaseError::Held { .. } => {
                        // Expected: read found the other node's lease
                    }
                    LeaseError::MetadataWrite(WriteError::FailedPrecondition(_)) => {
                        // Expected: put raced and failed, re-read found expired or
                        // deleted lease (very unlikely but possible)
                    }
                    e => panic!(
                        "Unexpected error type: {:?}. Expected Held or FailedPrecondition",
                        e
                    ),
                }
            }
            (Ok(_), Ok(_)) => panic!("Both acquires succeeded - this should not happen"),
            (Err(e1), Err(e2)) => panic!("Both acquires failed: {:?}, {:?}", e1, e2),
        }
    }

    /// Test concurrent acquire from same node - one should succeed, one should fail with ConcurrentSameNode
    #[restate_core::test]
    async fn test_lease_acquire_concurrent_same_node_race() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());

        // Run both acquires concurrently from the same node
        let (result1, result2) =
            tokio::join!(manager.acquire(partition_id), manager.acquire(partition_id));

        // Exactly one should succeed, one should fail
        match (result1, result2) {
            (Ok(guard), Err(e)) | (Err(e), Ok(guard)) => {
                // The successful one should have a valid guard
                assert!(guard.is_valid());

                // The failed one should be ConcurrentSameNode or precondition failure
                match e {
                    LeaseError::ConcurrentSameNode { .. } => {
                        // Expected: detected concurrent op from same node
                    }
                    LeaseError::MetadataWrite(WriteError::FailedPrecondition(_)) => {
                        // Also acceptable: put raced
                    }
                    e => panic!(
                        "Unexpected error type: {:?}. Expected ConcurrentSameNode or FailedPrecondition",
                        e
                    ),
                }
            }
            (Ok(_), Ok(_)) => panic!("Both acquires succeeded - this should not happen"),
            (Err(e1), Err(e2)) => panic!("Both acquires failed: {:?}, {:?}", e1, e2),
        }
    }

    /// Test that renewal fails when the released flag is set
    #[restate_core::test]
    async fn test_lease_renewal_fails_when_released() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let partition_id = PartitionId::MIN;

        let manager =
            SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
        let guard = manager
            .acquire(partition_id)
            .await
            .expect("acquire should succeed");

        // Mark the guard as released (simulates what happens during drop)
        guard.test_mark_released();

        // Renewal should fail because released flag is set
        let result = guard.test_renew().await;
        match result {
            Err(LeaseError::LeaseExpired) => {
                // Expected - released flag causes early exit
            }
            Err(e) => panic!("Expected LeaseError::LeaseExpired, got error: {:?}", e),
            Ok(_) => panic!("Expected LeaseError::LeaseExpired, but renewal succeeded"),
        }
    }

    // ================== Drop/Release Tests ==================

    /// Test that dropping a guard triggers async release
    #[restate_core::test]
    async fn test_lease_release_on_drop() {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let clock = MockClock::new();
        let node_id = GenerationalNodeId::new(1, 1);
        let other_node_id = GenerationalNodeId::new(2, 1);
        let partition_id = PartitionId::MIN;

        {
            let manager =
                SnapshotLeaseManager::new_with_clock(env.metadata_store_client.clone(), node_id, clock.clone());
            let _guard = manager
                .acquire(partition_id)
                .await
                .expect("acquire should succeed");
            // guard is dropped here
        }

        // Poll until the async release task completes or timeout
        let other_manager = SnapshotLeaseManager::new_with_clock(
            env.metadata_store_client.clone(),
            other_node_id,
            clock.clone(),
        );

        let mut acquired = false;
        for _ in 0..50 {
            // 50 * 20ms = 1s max wait
            match other_manager.acquire(partition_id).await {
                Ok(_guard) => {
                    acquired = true;
                    break;
                }
                Err(LeaseError::Held { .. }) => {
                    // Release task hasn't completed yet, wait and retry
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("Unexpected error during acquire: {:?}", e),
            }
        }

        assert!(
            acquired,
            "other node should be able to acquire after release (within timeout)"
        );
    }

    #[test]
    fn test_lease_value_new_and_expiry() {
        let holder = GenerationalNodeId::new(1, 1);
        let clock = MockClock::new();
        let lease = SnapshotLeaseValue::new(holder, &clock);

        assert_eq!(lease.version, Version::MIN);
        assert!(!lease.is_expired(&clock));

        let remaining = lease.remaining_millis(&clock);
        assert!(remaining > LEASE_DURATION_MS - 100);
        assert!(remaining <= LEASE_DURATION_MS);
    }

    #[test]
    fn test_lease_value_expired() {
        let holder = GenerationalNodeId::new(1, 1);
        let lease_id = Ulid::new();
        let clock = MockClock::new();
        let lease = SnapshotLeaseValue::expired(holder, lease_id);

        assert_eq!(lease.holder, holder);
        assert_eq!(lease.lease_id, lease_id);
        assert_eq!(lease.expires_at, MillisSinceEpoch::UNIX_EPOCH);
        assert!(lease.is_expired(&clock));
    }
}
