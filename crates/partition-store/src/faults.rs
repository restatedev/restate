// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test-only fault injection for a partition's column-family work.
//!
//! Every partition cell has a [`Faults`]. A test arms a fault for a [`FaultPoint`],
//! and the next run of that work picks it up, once:
//!
//! ```ignore
//! let faults = &manager.state.get_or_default(&partition).faults;
//!
//! // The next deletion of the column family waits until `resume` is dropped.
//! let (reached, resume) = faults.pause(FaultPoint::Delete);
//! let dropping = manager.drop_partition_store(&partition);
//! // ... poll `dropping`, then:
//! reached.await?; // the deletion is now waiting at the fault point
//! drop(resume);   // let it go ahead
//!
//! // The next deletion fails with this error instead of running.
//! faults.fail(FaultPoint::Delete, RocksError::ReadOnly);
//! ```
//!
//! Outside tests, `Faults` is empty and nothing can be armed, so the hooks cost nothing.

use restate_rocksdb::RocksError;

/// Where a fault can be injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FaultPoint {
    /// Creating or importing the column family, on the lifecycle thread, before the
    /// native work starts.
    Open,
    /// Deleting the column family, once nothing uses it anymore, before the native
    /// deletion starts.
    Delete,
}

#[derive(Default)]
pub(crate) struct Faults {
    #[cfg(test)]
    open: parking_lot::Mutex<Option<Fault>>,
    #[cfg(test)]
    delete: parking_lot::Mutex<Option<Fault>>,
}

#[cfg(test)]
pub(crate) enum Fault {
    Pause {
        reached: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::watch::Receiver<()>,
    },
    Fail(RocksError),
}

#[cfg(not(test))]
pub(crate) enum Fault {}

impl Faults {
    /// Takes the fault armed for `point`, if any.
    pub(crate) fn take(&self, point: FaultPoint) -> Option<Fault> {
        #[cfg(test)]
        return self.slot(point).lock().take();
        #[cfg(not(test))]
        {
            let _ = point;
            None
        }
    }
}

#[cfg(test)]
impl Faults {
    /// Makes the next run at `point` wait. `reached` resolves once it's waiting, and
    /// it goes ahead when `resume` is dropped.
    pub(crate) fn pause(
        &self,
        point: FaultPoint,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::watch::Sender<()>,
    ) {
        let (reached_tx, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_rx) = tokio::sync::watch::channel(());
        self.arm(
            point,
            Fault::Pause {
                reached: reached_tx,
                resume: resume_rx,
            },
        );
        (reached, resume)
    }

    /// Makes the next run at `point` fail with `error` instead of doing its work.
    pub(crate) fn fail(&self, point: FaultPoint, error: RocksError) {
        self.arm(point, Fault::Fail(error));
    }

    fn arm(&self, point: FaultPoint, fault: Fault) {
        let previous = self.slot(point).lock().replace(fault);
        assert!(previous.is_none(), "a fault is already armed for {point:?}");
    }

    fn slot(&self, point: FaultPoint) -> &parking_lot::Mutex<Option<Fault>> {
        match point {
            FaultPoint::Open => &self.open,
            FaultPoint::Delete => &self.delete,
        }
    }
}

/// Runs `fault`, if one was armed: waits until the test resumes it, or returns its
/// error. Returns right away otherwise. Works on a lifecycle thread through
/// `futures::executor::block_on`, since it doesn't need a Tokio runtime.
pub(crate) async fn inject(fault: Option<Fault>) -> Result<(), RocksError> {
    match fault {
        None => Ok(()),
        #[cfg(test)]
        Some(Fault::Pause {
            reached,
            mut resume,
        }) => {
            let _ = reached.send(());
            // Fails, and so returns, once the test drops `resume`.
            let _ = resume.changed().await;
            Ok(())
        }
        #[cfg(test)]
        Some(Fault::Fail(error)) => Err(error),
        #[cfg(not(test))]
        Some(fault) => match fault {},
    }
}
