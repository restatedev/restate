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
use crate::{DbEnv, DbSpec, RocksAccess, RocksDb, RocksError};

/// Owns an accepted open through dispatch, native execution, and publication.
/// Its inputs and outcome stay with the guard, including if the job is discarded.
pub(super) struct OpenGuard {
    pub tracked: DbRef,
    state: OpenState,
    closed: CancellationToken,
    completion: watch::Sender<Option<Result<(), RocksError>>>,
    // Release accounting after resources and completion, unless transferred to
    // the manager's unclaimed result. Keep the manager alive through finalization.
    accepted: Option<OwnedLease>,
    manager: RocksDbManager,
}

enum OpenState {
    Queued(DbSpec, DbEnv),
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
        let OpenState::Queued(spec, env) = std::mem::replace(&mut self.state, OpenState::Running)
        else {
            unreachable!("an open guard runs once");
        };
        self.state = OpenState::Completed(self.manager.open_native(spec, env).map(|native| {
            RocksDb::from_access(
                self.manager.clone(),
                native,
                self.tracked,
                self.closed.clone(),
            )
        }));
    }
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        let result = match std::mem::replace(&mut self.state, OpenState::Running) {
            OpenState::Queued(spec, env) => {
                // No native open ran. Release even native-owned configurator and
                // environment resources before making the path available again.
                drop((spec, env));
                Err(ShutdownError.into())
            }
            // Native execution unwound without returning an outcome. Closing the
            // result channel reports interruption, but cannot certify physical
            // completion: retain the incarnation's path fence and closed signal.
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
        // Manager transitions have released the lock. Existing observers retain
        // this resource-free result even if the path has already been replaced.
        self.completion.send_replace(Some(result));
    }
}

/// Owns a claimed preparation. Discarding the job still performs the work.
pub(super) struct ShutdownGuard {
    // Publish the outcome before dropping this pin, and release accounting last.
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

/// Records interruption even if native preparation unwinds inside ShutdownGuard::drop.
struct ShutdownCompletion<'a> {
    db: &'a RocksDb,
    completed: bool,
}

impl Drop for ShutdownCompletion<'_> {
    fn drop(&mut self) {
        self.db
            .manager
            .finish_shutdown(self.db.registration, self.completed);
    }
}

/// Owns final-reference cleanup. Dropping a queued job still performs cleanup;
/// completion is published only after native destruction actually returns.
pub(super) struct CloseGuard {
    db: Option<RocksAccess>,
    closed: CancellationToken,
    tracked: DbRef,
    needs_shutdown: bool,
    _active: OwnedLease,
    manager: RocksDbManager,
}

impl CloseGuard {
    pub fn new(
        manager: RocksDbManager,
        db: RocksAccess,
        closed: CancellationToken,
        tracked: DbRef,
        needs_shutdown: bool,
        active: OwnedLease,
    ) -> Self {
        Self {
            db: Some(db),
            closed,
            tracked,
            needs_shutdown,
            _active: active,
            manager,
        }
    }
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        let db = self.db.take().expect("cleanup runs once");
        if self.needs_shutdown {
            db.shutdown();
        }
        drop(db);
        // Neither an unwind from shutdown/destruction nor a cancelled waiter
        // substitutes for reaching this point after actual resource release.
        self.manager.finish_close(self.tracked, self.closed.clone());
    }
}
