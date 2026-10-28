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
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use restate_core::io::OwnedLease;

use crate::{DbName, DbSpec, RocksDb, RocksError};

/// One incarnation, owned and mutated under the manager's state mutex.
/// Reopening allocates a new DbRef. A successful open is owned here until claimed.
pub(super) struct TrackedDb {
    pub name: DbName,
    pub status: Status,
    pub closed: CancellationToken,
    // Resource-free completion, also retained by existing waiters when this
    // incarnation is replaced.
    pub open_result: watch::Receiver<Option<Result<(), RocksError>>>,
}

pub(super) enum Status {
    /// An open operation has been registered with this manager.
    Opening { since: Instant },
    /// Native opening completed; monitoring and config updates are enabled.
    Open {
        since: Instant,
        handle: Weak<RocksDb>,
        // Drop order matters: release the database before its admission token,
        // so final-reference cleanup can take over accounting first.
        unclaimed: Option<(Arc<RocksDb>, OwnedLease)>,
        shutdown: ShutdownState,
    },
    /// The last `Arc<RocksDb>` was dropped and the native database is being closed
    /// on the lifecycle pool.
    Closing { since: Instant },
    /// Database is known to be closed. This says nothing about whether there are
    /// files on disk or not.
    Closed { since: Instant },
}

/// Preparatory shutdown does not end the database's native lifetime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShutdownState {
    Needed,
    Claimed,
    Completed,
    /// Preparation unwound. Final-reference cleanup must still prepare the DB.
    Interrupted,
}

impl Status {
    fn since(&self) -> Instant {
        match self {
            Self::Opening { since }
            | Self::Open { since, .. }
            | Self::Closing { since }
            | Self::Closed { since } => *since,
        }
    }
}

impl TrackedDb {
    pub fn opening(
        spec: &DbSpec,
        open_result: watch::Receiver<Option<Result<(), RocksError>>>,
    ) -> Self {
        Self {
            name: spec.name.clone(),
            status: Status::Opening {
                since: Instant::now(),
            },
            closed: CancellationToken::new(),
            open_result,
        }
    }

    pub fn opened(&mut self, db: Arc<RocksDb>, accepted: OwnedLease) -> Duration {
        debug_assert!(matches!(self.status, Status::Opening { .. }));
        let elapsed = self.status.since().elapsed();
        self.status = Status::Open {
            since: Instant::now(),
            handle: Arc::downgrade(&db),
            unclaimed: Some((db, accepted)),
            shutdown: ShutdownState::Needed,
        };
        elapsed
    }

    pub fn handle(&self) -> Option<Arc<RocksDb>> {
        match &self.status {
            Status::Open { handle, .. } => handle.upgrade(),
            _ => None,
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.status, Status::Closed { .. })
    }

    /// Transfer unclaimed ownership/accounting out of the manager lock.
    pub fn take_unclaimed(&mut self) -> Option<(Arc<RocksDb>, OwnedLease)> {
        match &mut self.status {
            Status::Open { unclaimed, .. } => unclaimed.take(),
            _ => None,
        }
    }

    pub fn claim(&mut self) -> Result<(Arc<RocksDb>, Option<OwnedLease>), RocksError> {
        let Status::Open {
            handle, unclaimed, ..
        } = &mut self.status
        else {
            return Err(RocksError::NotOpen);
        };
        if let Some((db, accepted)) = unclaimed.take() {
            Ok((db, Some(accepted)))
        } else {
            Ok((handle.upgrade().ok_or(RocksError::NotOpen)?, None))
        }
    }

    pub fn pending_cleanup(&self) -> Option<CancellationToken> {
        match &self.status {
            Status::Opening { .. } | Status::Closing { .. } => Some(self.closed.clone()),
            Status::Open {
                shutdown: ShutdownState::Interrupted,
                ..
            } => Some(self.closed.clone()),
            Status::Open { handle, .. } if handle.strong_count() == 0 => Some(self.closed.clone()),
            Status::Open { .. } | Status::Closed { .. } => None,
        }
    }

    /// Emergency shutdown must not repeat completed graceful preparation.
    pub fn unprepared_handle(&self) -> Option<Arc<RocksDb>> {
        match &self.status {
            Status::Open {
                shutdown: ShutdownState::Completed,
                ..
            } => None,
            _ => self.handle(),
        }
    }

    /// Claim preparatory shutdown while pinning the database. The caller must
    /// account for the work before unlocking, and drop the pin outside the lock.
    pub fn claim_shutdown(&mut self) -> Option<Arc<RocksDb>> {
        if let Status::Open {
            handle, shutdown, ..
        } = &mut self.status
            && *shutdown == ShutdownState::Needed
        {
            let db = handle.upgrade()?;
            *shutdown = ShutdownState::Claimed;
            Some(db)
        } else {
            None
        }
    }

    pub fn finish_shutdown(&mut self, completed: bool) {
        let Status::Open { shutdown, .. } = &mut self.status else {
            unreachable!("preparatory shutdown retains a native pin");
        };
        debug_assert_eq!(*shutdown, ShutdownState::Claimed);
        *shutdown = if completed {
            ShutdownState::Completed
        } else {
            ShutdownState::Interrupted
        };
    }

    /// Returns whether native cleanup still needs preparatory shutdown.
    pub fn begin_close(&mut self) -> bool {
        let previous = std::mem::replace(
            &mut self.status,
            Status::Closing {
                since: Instant::now(),
            },
        );
        debug_assert!(matches!(
            previous,
            Status::Opening { .. } | Status::Open { .. }
        ));
        !matches!(
            previous,
            Status::Open {
                shutdown: ShutdownState::Completed,
                ..
            }
        )
    }

    /// Call only after native destruction, or an unsuccessful open has released
    /// its resources. In particular, this is not a success-on-unwind guard.
    pub fn finish_close(&mut self) -> Duration {
        debug_assert!(matches!(
            self.status,
            Status::Opening { .. } | Status::Closing { .. }
        ));
        let elapsed = self.status.since().elapsed();
        self.status = Status::Closed {
            since: Instant::now(),
        };
        elapsed
    }
}
