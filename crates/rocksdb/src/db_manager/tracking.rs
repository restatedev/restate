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

use restate_core::io::OwnedLease;
use restate_util_string::ReString;

use crate::{DbName, DbSpec, RocksDb, RocksError};

/// The manager's record of one incarnation of a database, read and changed only
/// under the manager's state lock. Reopening the path creates a new record with a
/// new `DbRef`. After a successful open, the record holds the database until the
/// caller collects it.
pub(super) struct TrackedDb {
    pub name: DbName,
    pub path: PathBuf,
    pub status: Status,
    pub closed: CancellationToken,
    // Outcome of the open: success or the error, without the database itself.
    // Waiters keep their own receiver, so they still get it after the path is
    // reopened. `None` for a hold created without an open.
    pub open_result: Option<watch::Receiver<Option<Result<(), RocksError>>>>,
    // Hold placed while the database was still active. When it closes, the record
    // becomes `Held` with this reason instead of `Closed`.
    pub hold_after_close: Option<ReString>,
}

pub(super) enum Status {
    /// Closed, and reserved by its owner so nothing else opens the path, e.g. while
    /// its files are being deleted or repaired.
    Held { since: Instant, reason: ReString },
    /// The open has been accepted and is queued or running.
    Opening { since: Instant },
    /// The owner is opening the database itself and must report the result (see
    /// `RocksDbManager::remote_open_db`). Until then the path stays reserved and
    /// shutdown waits; dropping the `DbRef` doesn't change that.
    RemoteOpen {
        since: Instant,
        completion: watch::Sender<Option<Result<(), RocksError>>>,
        accepted: OwnedLease,
    },
    /// Native opening completed; monitoring and config updates are enabled.
    Open {
        since: Instant,
        handle: Weak<RocksDb>,
        // Database not yet collected by the caller, with its lifecycle lease. Drop order
        // matters: if dropping the database drops its last `Arc`, its close job takes
        // its own lease before this one is released, so shutdown can't miss it.
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

/// Progress of a database's shutdown flush (WAL and `flush_on_shutdown` column
/// families). Flushing doesn't close the database; it stays open until its last
/// `Arc` is dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShutdownState {
    Needed,
    Claimed,
    Completed,
    /// The flush panicked. The close will flush again before destroying the database.
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

    pub fn opened(&mut self, db: Arc<RocksDb>, accepted: OwnedLease) -> Duration {
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

    /// Takes the result sender and lifecycle lease out of a remote open, so they
    /// aren't dropped under the lock. The caller then publishes the database, or
    /// marks the failed open closed.
    pub fn take_remote_open(
        &mut self,
    ) -> (watch::Sender<Option<Result<(), RocksError>>>, OwnedLease) {
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

    /// Takes the uncollected database and its lease out of the record, so the caller
    /// can drop them after releasing the manager lock.
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

    /// The database, unless its shutdown flush already completed. Emergency shutdown
    /// uses this to skip databases that were already flushed.
    pub fn unprepared_handle(&self) -> Option<Arc<RocksDb>> {
        match &self.status {
            Status::Open {
                shutdown: ShutdownState::Completed,
                ..
            } => None,
            _ => self.handle(),
        }
    }

    /// Marks this database's shutdown flush as taken and returns an `Arc` to it, or
    /// `None` if there is nothing to flush. The caller must take a lifecycle lease
    /// before releasing the manager lock, and drop the `Arc` only after releasing it.
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

    /// Marks the database as closing. Returns whether it still needs a shutdown flush
    /// first, which is the case unless a flush already completed.
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

    /// Marks the database closed. Call it only once the native database is destroyed,
    /// or a failed open has released everything. Never call it on a panic path: a
    /// panic doesn't prove the resources are gone.
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
