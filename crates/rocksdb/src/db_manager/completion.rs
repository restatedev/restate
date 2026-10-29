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

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use restate_core::ShutdownError;
use restate_core::io::OwnedLease;

use super::{DbRef, RocksDbManager};
use crate::{DbCleanup, DbEnv, DbSpec, RocksDb, RocksDbReadPerfGuard, RocksError};

/// Carries one accepted open from the queue, through the native open, to
/// publishing the result. It owns the open's inputs and result, so if the queued
/// job is dropped without running, its `Drop` still reports an outcome and frees
/// the path.
pub(super) struct OpenGuard {
    pub tracked: DbRef,
    state: OpenState,
    closed: CancellationToken,
    completion: watch::Sender<Option<Result<(), RocksError>>>,
    // Lifecycle lease, released after the result is published and resources are
    // freed. On success it moves into the manager's record together with the
    // database, until the caller collects it. `manager` stays alive until then too.
    accepted: Option<OwnedLease>,
    manager: RocksDbManager,
}

enum OpenState {
    Queued(DbSpec, DbEnv),
    Preparing,
    Running,
    Completed(Result<Arc<RocksDb>, RocksError>),
}

impl OpenGuard {
    pub fn new(
        manager: RocksDbManager,
        tracked: DbRef,
        closed: CancellationToken,
        spec: DbSpec,
        env: DbEnv,
        accepted: OwnedLease,
        completion: watch::Sender<Option<Result<(), RocksError>>>,
    ) -> Self {
        Self {
            tracked,
            state: OpenState::Queued(spec, env),
            closed,
            completion,
            accepted: Some(accepted),
            manager,
        }
    }

    pub fn run(mut self) {
        let OpenState::Queued(spec, env) = std::mem::replace(&mut self.state, OpenState::Preparing)
        else {
            unreachable!("an open guard runs once");
        };
        let _perf = RocksDbReadPerfGuard::new("open-db");
        let prepared = match self.manager.prepare_open(spec, env) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.state = OpenState::Completed(Err(error));
                return;
            }
        };
        self.state = OpenState::Running;
        self.state = OpenState::Completed(prepared.open().map(|native| {
            self.manager
                .bind_db(native, self.tracked, self.closed.clone())
        }));
    }
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        let result = match std::mem::replace(&mut self.state, OpenState::Running) {
            OpenState::Queued(spec, env) => {
                // The open never ran. Drop its spec and environment, which can own native
                // resources, before freeing the path for a new open.
                drop((spec, env));
                Err(ShutdownError.into())
            }
            // Preparing the open panicked. Its inputs were already dropped while
            // unwinding, and no native database was opened, so the path can be freed.
            OpenState::Preparing => Err(ShutdownError.into()),
            // The native open panicked, so we can't tell whether RocksDB still has the
            // files open. Keep the path blocked and don't fire `closed`; waiters see the
            // result channel close instead of a result, and shutdown keeps waiting.
            OpenState::Running => return,
            OpenState::Completed(result) => result,
        };
        let result = match result {
            Ok(db) => self.manager.publish_open(
                self.tracked,
                db,
                self.accepted.take().expect("accepted open"),
            ),
            Err(error) => {
                self.manager.finish_close(self.tracked, self.closed.clone());
                Err(error)
            }
        };
        // The manager lock is no longer held here. Waiters already holding a receiver
        // get this result even if the path has been reopened since; it holds no
        // resources.
        self.completion.send_replace(Some(result));
    }
}

/// Runs one database's shutdown flush when dropped, so the flush happens even if
/// its queued job is dropped instead of run.
pub(super) struct ShutdownGuard {
    // Record the flush outcome while `db` is still alive, and release the lease last.
    db: Arc<RocksDb>,
    _active: OwnedLease,
}

impl ShutdownGuard {
    pub fn new(db: Arc<RocksDb>, active: OwnedLease) -> Self {
        Self {
            db,
            _active: active,
        }
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        let mut completion = ShutdownCompletion {
            db: &self.db,
            completed: false,
        };
        self.db.db.shutdown();
        completion.completed = true;
    }
}

/// Reports the flush outcome to the manager when dropped, also when the flush
/// panics (then as interrupted).
struct ShutdownCompletion<'a> {
    db: &'a RocksDb,
    completed: bool,
}

impl Drop for ShutdownCompletion<'_> {
    fn drop(&mut self) {
        let registration = self.db.registration.as_ref().expect("registered database");
        registration
            .manager
            .finish_shutdown(registration.tracked, self.completed);
    }
}

/// Closes a database whose last `Arc` was dropped, when this guard is dropped,
/// so the close happens even if its queued job is dropped instead of run. The
/// database is reported closed only after native destruction returns.
pub(super) struct CloseGuard {
    _cleanup: DbCleanup,
    _active: OwnedLease,
}

impl CloseGuard {
    pub fn new(cleanup: DbCleanup, active: OwnedLease) -> Self {
        Self {
            _cleanup: cleanup,
            _active: active,
        }
    }
}
