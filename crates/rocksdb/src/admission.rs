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

use restate_core::io::{Gate, Lease};

use crate::{RocksDb, StorageOp};

/// The scopes registered with one database. Only registration and sealing take
/// this lock; operations only touch their own scope's gate. Entries are weak,
/// but in-flight work keeps its gate alive, so dropping a scope while its work
/// is still running doesn't hide that work from database shutdown.
#[derive(Default)]
pub(crate) struct AdmissionScopes {
    pub sealed: bool,
    pub gates: Vec<Weak<Gate>>,
}

/// A group of operations on one database that can be sealed and drained on its
/// own. For example, the partition store registers one scope per partition, so
/// closing a partition waits only for that partition's work. Database shutdown
/// seals and drains every scope as well.
///
/// A scope doesn't keep the database open; each admitted operation holds its own
/// `Arc<RocksDb>`.
pub struct DatabaseScope {
    database: Weak<RocksDb>,
    gate: Arc<Gate>,
}

impl DatabaseScope {
    pub(crate) fn new(database: &Arc<RocksDb>, gate: Arc<Gate>) -> Self {
        Self {
            database: Arc::downgrade(database),
            gate,
        }
    }

    /// Admits one operation in this scope, or returns `None` once the scope or its
    /// database is sealed. Doesn't take the database's scope lock.
    pub fn try_acquire(&self) -> Option<DatabaseLease<'_>> {
        Some(DatabaseLease {
            scope: self,
            lease: self.gate.try_acquire()?,
        })
    }

    /// Permanently reject new operations in this scope alone.
    pub fn seal(&self) {
        self.gate.seal();
    }

    /// Wait for a sealed scope's accepted work to release its resources.
    pub async fn drain(&self) {
        self.gate.drain().await;
    }

    fn assert_database(&self, database: &Arc<RocksDb>) {
        assert!(
            std::ptr::eq(self.database.as_ptr(), Arc::as_ptr(database)),
            "admission scope belongs to a different database"
        );
    }

    pub(crate) fn gate_for(&self, database: &Arc<RocksDb>) -> &Arc<Gate> {
        self.assert_database(database);
        &self.gate
    }

    /// Like [`RocksDb::prepare_op`], but the work counts against this scope instead
    /// of the database. Admission is checked when the work is called or submitted,
    /// not now. The work keeps `database` alive until it runs or is dropped.
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
        StorageOp::new(move || op(database), &self.gate)
    }
}

/// A lease from a [`DatabaseScope`]. Unlike a bare lease, it remembers which
/// database its scope belongs to, so write APIs can check that the lease matches
/// the database they write to. It still counts as one operation when the write
/// moves to a background thread.
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
