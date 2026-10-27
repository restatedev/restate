// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;

use restate_util_string::ReString;

use crate::{DbName, DbSpec, RocksDb, RocksError};

/// One incarnation, owned and mutated under the manager's state mutex.
/// Reopening allocates a new DbRef. A successful open is owned here until claimed.
pub(super) struct TrackedDb {
    pub name: DbName,
    pub path: PathBuf,
    pub status: Status,
    pub closed: CancellationToken,
    // Resource-free completion, also retained by existing waiters when this
    // incarnation is replaced. Holds created without opening need no channel.
    pub open_result: Option<watch::Receiver<Option<Result<(), RocksError>>>>,
    // An active incarnation with an owner hold must become Held when its
    // native resources are released. The reason then moves into Status::Held.
    pub hold_after_close: Option<ReString>,
}

pub(super) enum Status {
    /// The slot is booked by an external owner. For instance, the database files
    /// are being deleted, or other maintenance operation on this closed database
    /// require that no one touches it.
    Held { since: Instant, reason: ReString },
    /// An open operation has been registered with this manager.
    Opening { since: Instant },
    /// The owner drives native opening and must explicitly report its outcome.
    /// Dropping a DbRef cannot release this fence or its admission accounting.
    RemoteOpen {
        since: Instant,
        completion: watch::Sender<Option<Result<(), RocksError>>>,
        accepted: TaskTrackerToken,
    },
    /// Native opening completed; monitoring and config updates are enabled.
    Open {
        since: Instant,
        handle: Weak<RocksDb>,
        // Drop order matters: release the database before its admission token,
        // so final-reference cleanup can take over accounting first.
        unclaimed: Option<(Arc<RocksDb>, TaskTrackerToken)>,
        shutdown: ShutdownState,
    },
    /// An ongoing drain+shutdown is taking place for this database.
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
            Self::Held { since, .. }
            | Self::Opening { since }
            | Self::RemoteOpen { since, .. }
            | Self::Open { since, .. }
            | Self::Closing { since }
            | Self::Closed { since } => *since,
        }
    }
}

impl TrackedDb {
    pub fn opening(
        spec: &DbSpec,
        status: Status,
        hold_after_close: Option<ReString>,
        open_result: watch::Receiver<Option<Result<(), RocksError>>>,
    ) -> Self {
        Self {
            name: spec.name.clone(),
            path: spec.path.clone(),
            status,
            closed: CancellationToken::new(),
            open_result: Some(open_result),
            hold_after_close,
        }
    }

    pub fn opened(&mut self, db: Arc<RocksDb>, accepted: TaskTrackerToken) -> Duration {
        let elapsed = self.registered(&db);
        if let Status::Open { unclaimed, .. } = &mut self.status {
            *unclaimed = Some((db, accepted));
        }
        elapsed
    }

    pub fn registered(&mut self, db: &Arc<RocksDb>) -> Duration {
        debug_assert!(matches!(self.status, Status::Opening { .. }));
        let elapsed = self.status.since().elapsed();
        self.status = Status::Open {
            since: Instant::now(),
            handle: Arc::downgrade(db),
            unclaimed: None,
            shutdown: ShutdownState::Needed,
        };
        elapsed
    }

    pub fn held(name: DbName, path: PathBuf, reason: ReString) -> Self {
        let closed = CancellationToken::new();
        closed.cancel();
        Self {
            name,
            path,
            status: Status::Held {
                since: Instant::now(),
                reason,
            },
            closed,
            open_result: None,
            hold_after_close: None,
        }
    }

    pub fn is_held(&self) -> bool {
        self.hold_after_close.is_some() || matches!(self.status, Status::Held { .. })
    }

    /// Take remote completion/accounting without dropping either under the lock.
    /// The caller then publishes the database or completes its failed lifetime.
    pub fn take_remote_open(
        &mut self,
    ) -> (
        watch::Sender<Option<Result<(), RocksError>>>,
        TaskTrackerToken,
    ) {
        let since = self.status.since();
        let status = std::mem::replace(&mut self.status, Status::Opening { since });
        let Status::RemoteOpen {
            completion,
            accepted,
            ..
        } = status
        else {
            unreachable!("remote finalization is validated under the manager lock");
        };
        (completion, accepted)
    }

    pub fn handle(&self) -> Option<Arc<RocksDb>> {
        match &self.status {
            Status::Open { handle, .. } => handle.upgrade(),
            _ => None,
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.status, Status::Closed { .. } | Status::Held { .. })
    }

    /// Transfer unclaimed ownership/accounting out of the manager lock.
    pub fn take_unclaimed(&mut self) -> Option<(Arc<RocksDb>, TaskTrackerToken)> {
        match &mut self.status {
            Status::Open { unclaimed, .. } => unclaimed.take(),
            _ => None,
        }
    }

    pub fn claim(&mut self) -> Result<(Arc<RocksDb>, Option<TaskTrackerToken>), RocksError> {
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
            Status::Opening { .. } | Status::RemoteOpen { .. } | Status::Closing { .. } => {
                Some(self.closed.clone())
            }
            Status::Open {
                shutdown: ShutdownState::Interrupted,
                ..
            } => Some(self.closed.clone()),
            Status::Open { handle, .. } if handle.strong_count() == 0 => Some(self.closed.clone()),
            Status::Open { .. } | Status::Closed { .. } | Status::Held { .. } => None,
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
        let since = Instant::now();
        self.status = match self.hold_after_close.take() {
            Some(reason) => Status::Held { since, reason },
            None => Status::Closed { since },
        };
        elapsed
    }
}
