// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Weak};

use restate_core::io::{FlightTracker, Lease};

use crate::{RocksDb, StorageOp};

/// Cold-path registry. Registration and sealing share this lock; operations only
/// touch their own tracker. Weak entries remain discoverable through owned leases
/// even after the scope's last handle is dropped.
#[derive(Default)]
pub(crate) struct AdmissionScopes {
    pub sealed: bool,
    pub trackers: Vec<Weak<FlightTracker>>,
}

/// An independently sealable work scope registered with one physical database.
/// Database shutdown seals and drains this scope as well as direct database work.
/// The scope itself does not pin native resources; admitted operations must do so.
pub struct DatabaseScope {
    database: Weak<RocksDb>,
    tracker: Arc<FlightTracker>,
}

impl DatabaseScope {
    pub(crate) fn new(database: &Arc<RocksDb>, tracker: Arc<FlightTracker>) -> Self {
        Self {
            database: Arc::downgrade(database),
            tracker,
        }
    }

    /// Acquire only this scope's admission, without touching the database registry.
    pub fn try_acquire(&self) -> Option<DatabaseLease<'_>> {
        Some(DatabaseLease {
            scope: self,
            lease: self.tracker.try_acquire()?,
        })
    }

    /// Permanently reject new operations in this scope alone.
    pub fn seal(&self) {
        self.tracker.seal();
    }

    /// Wait for a sealed scope's accepted work to release its resources.
    pub async fn drain(&self) {
        self.tracker.drain().await;
    }

    fn assert_database(&self, database: &Arc<RocksDb>) {
        assert!(
            std::ptr::eq(self.database.as_ptr(), Arc::as_ptr(database)),
            "admission scope belongs to a different database"
        );
    }

    pub(crate) fn tracker_for(&self, database: &Arc<RocksDb>) -> &Arc<FlightTracker> {
        self.assert_database(database);
        &self.tracker
    }

    /// Prepare work with deferred admission to this scope, retaining the database
    /// through execution or disposal. The caller chooses the executor and guards.
    ///
    /// # Panics
    /// Panics if `database` is not the database that registered this scope.
    pub fn prepare_op<R: Send + 'static>(
        &self,
        database: &Arc<RocksDb>,
        op: impl FnOnce(Arc<RocksDb>) -> R + Send + 'static,
    ) -> StorageOp<'_, impl FnOnce() -> R + Send + 'static> {
        self.assert_database(database);
        let database = database.clone();
        StorageOp::new(move || op(database), &self.tracker)
    }
}

/// Admission to a registered database scope. Its database association cannot be
/// forged from an unrelated flight tracker. Promotion preserves existing admission.
pub struct DatabaseLease<'a> {
    scope: &'a DatabaseScope,
    lease: Lease<'a>,
}

impl<'a> DatabaseLease<'a> {
    pub(crate) fn into_lease(self, database: &Arc<RocksDb>) -> Lease<'a> {
        self.scope.assert_database(database);
        self.lease
    }
}
