// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fault injection for a partition's work.
//!
//! Every partition has a [`Faults`], a handle that all its openings share. Arming a fault
//! for a [`FaultPoint`] makes the next run of that work in the partition pick it up,
//! once:
//!
//! ```ignore
//! let faults = manager.faults(&partition);
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
//! While a task waits at a paused point, `resume.closed()` resolves once the task is
//! dropped instead, such as when its processor stops.
//!
//! Arming needs the `faults` feature, which this crate's own tests always have. The
//! feature enables nothing else, so unlike `test-util` it leaves a build's configuration
//! as it is. Without it, [`Faults`] and what a paused point waits on are zero-sized and
//! nothing can be armed, so the points compile to nothing.

#[cfg(any(test, feature = "faults"))]
use tokio::sync::oneshot;

use restate_rocksdb::RocksError;

// Without the feature, the faults of a partition take no space and the points no code.
#[cfg(not(any(test, feature = "faults")))]
const _: () = assert!(
    size_of::<Faults>() == 0 && size_of::<Option<Resume>>() == 0,
    "faults cost nothing without the `faults` feature"
);

/// Where a fault can be injected. Every point can pause, unless it says it only
/// signals; the work at those that say so can also fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// Creating or importing the column family, after the cell switched to `Opening`
    /// and before the work is handed to the lifecycle pool. The database doesn't count
    /// the work as in flight yet. To hold up work it already counts, use
    /// `RocksDbManager::pause_lifecycle()`. Can fail.
    Open,
    /// Deleting the column family, once nothing uses it anymore, before the deletion
    /// is handed to the lifecycle pool. Can fail.
    Delete,
    /// A transaction's commit waits for its native write, whose first poll left it
    /// pending. That poll already handed the write to the storage pool, which may run
    /// or even finish it meanwhile: the pause holds the commit's waiter, not the
    /// native work. A test that needs a write that has not started holds the storage
    /// executor before the commit as well.
    NativeWriteQueued,
    /// A snapshot export is about to capture the partition. It holds the partition
    /// cell's read guard, so the partition cannot close, reopen or be deleted
    /// meanwhile, and, for an open partition, the processor's commit fence, so the
    /// processor's transaction cannot commit until the export releases it.
    SnapshotCapture,
    /// Flushing the registry, the walks and the fence a reconciliation wrote, before
    /// the reconciled indexes are installed for writes. Can fail.
    RegistryFlush,
    /// A background step of the walks is due, before the driver takes it. The partition
    /// processor keeps handling its other work while the step waits.
    WalkStep,
    /// The background driver has nothing left to do. It only signals: the driver goes
    /// idle without waiting to resume.
    WalksIdle,
    /// A walk's commit has completed, before the committed state is installed for the
    /// next writes.
    WalkPublication,
}

impl FaultPoint {
    /// Whether the work at this point can fail with an injected error.
    pub fn can_fail(self) -> bool {
        matches!(self, Self::Open | Self::Delete | Self::RegistryFlush)
    }
}

/// The faults armed for a partition, a handle whose clones share them; see the module
/// documentation.
#[derive(Clone, Default)]
pub struct Faults {
    #[cfg(any(test, feature = "faults"))]
    armed: std::sync::Arc<parking_lot::Mutex<Vec<(FaultPoint, Fault)>>>,
}

/// Resolves once the work paused at a point may go ahead.
#[cfg(any(test, feature = "faults"))]
pub(crate) type Resume = oneshot::Receiver<()>;

/// Uninhabited without the `faults` feature: no point can pause.
#[cfg(not(any(test, feature = "faults")))]
pub(crate) enum Resume {}

#[cfg(not(any(test, feature = "faults")))]
impl Future for Resume {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        match *self {}
    }
}

#[cfg(any(test, feature = "faults"))]
pub(crate) enum Fault {
    Pause {
        reached: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    },
    Fail(RocksError),
}

#[cfg(not(any(test, feature = "faults")))]
pub(crate) enum Fault {}

impl Faults {
    /// Takes the fault armed for `point`, if any.
    pub(crate) fn take(&self, point: FaultPoint) -> Option<Fault> {
        #[cfg(any(test, feature = "faults"))]
        {
            let mut armed = self.armed.lock();
            let index = armed.iter().position(|(armed, _)| *armed == point)?;
            Some(armed.swap_remove(index).1)
        }
        #[cfg(not(any(test, feature = "faults")))]
        {
            let _ = point;
            None
        }
    }

    /// Signals the pause armed for `point`, if any, and returns what resolves once the
    /// work may go ahead. For points whose work cannot fail.
    pub(crate) fn arrive(&self, point: FaultPoint) -> Option<Resume> {
        #[cfg(any(test, feature = "faults"))]
        match self.take(point)? {
            Fault::Pause { reached, resume } => {
                let _ = reached.send(());
                Some(resume)
            }
            Fault::Fail(_) => unreachable!("{point:?} cannot fail"),
        }
        #[cfg(not(any(test, feature = "faults")))]
        match self.take(point)? {}
    }

    /// Waits at `point` while a pause armed for it holds the work. For points whose
    /// work cannot fail.
    pub(crate) async fn reach(&self, point: FaultPoint) {
        if let Some(resume) = self.arrive(point) {
            let _ = resume.await;
        }
    }
}

#[cfg(any(test, feature = "faults"))]
impl Faults {
    /// Makes the next run at `point` wait. `reached` resolves once it's waiting, and
    /// it goes ahead when `resume` is sent or dropped.
    ///
    /// # Panics
    /// If a fault armed for `point` has not been taken yet. Once a run took it, the
    /// next run's fault can be armed, even while that run is still paused.
    pub fn pause(&self, point: FaultPoint) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_tx, reached) = oneshot::channel();
        let (resume, resume_rx) = oneshot::channel();
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
    ///
    /// # Panics
    /// If the work at `point` cannot fail, or a fault armed for `point` has not been
    /// taken yet.
    pub fn fail(&self, point: FaultPoint, error: RocksError) {
        assert!(point.can_fail(), "{point:?} cannot fail");
        self.arm(point, Fault::Fail(error));
    }

    fn arm(&self, point: FaultPoint, fault: Fault) {
        let mut armed = self.armed.lock();
        assert!(
            armed.iter().all(|(armed, _)| *armed != point),
            "a fault is already armed for {point:?}"
        );
        armed.push((point, fault));
    }
}

/// Calls `work` with the outcome of `fault`, for work that is started from sync code.
///
/// Without a fault, `work` runs right away on the caller's thread with `Ok(())`. With
/// one, a Tokio task runs [`inject`] and then `work`, so a paused fault holds no thread
/// while it waits. `work` must report an error it receives as its own failure.
pub(crate) fn run_after(
    fault: Option<Fault>,
    work: impl FnOnce(Result<(), RocksError>) + Send + 'static,
) {
    #[cfg(any(test, feature = "faults"))]
    if let Some(fault) = fault {
        tokio::spawn(async move { work(inject(Some(fault)).await) });
        return;
    }
    #[cfg(not(any(test, feature = "faults")))]
    let _ = fault;
    work(Ok(()));
}

/// Runs `fault`, if one was armed: waits until the test resumes it, or returns its
/// error. Returns right away otherwise.
pub(crate) async fn inject(fault: Option<Fault>) -> Result<(), RocksError> {
    match fault {
        None => Ok(()),
        #[cfg(any(test, feature = "faults"))]
        Some(Fault::Pause { reached, resume }) => {
            let _ = reached.send(());
            // Returns once the test resumes or drops `resume`.
            let _ = resume.await;
            Ok(())
        }
        #[cfg(any(test, feature = "faults"))]
        Some(Fault::Fail(error)) => Err(error),
        #[cfg(not(any(test, feature = "faults")))]
        Some(fault) => match fault {},
    }
}

/// Awaits `write`, pausing at [`FaultPoint::NativeWriteQueued`] once its first poll
/// returned pending. That poll handed the write to the storage pool; only the
/// waiter pauses.
#[cfg(any(test, feature = "faults"))]
pub(crate) async fn queued<T>(faults: &Faults, write: impl Future<Output = T>) -> T {
    // Declared before the write, so that dropping this future drops the write's
    // waiter first: once the pause's `resume.closed()` resolves, a write still
    // queued on the pool is no longer wanted and is skipped.
    let mut resume = None;
    let mut write = std::pin::pin!(write);
    if let std::task::Poll::Ready(output) = futures::poll!(write.as_mut()) {
        return output;
    }
    if let Some(arrived) = faults.arrive(FaultPoint::NativeWriteQueued) {
        let _ = resume.insert(arrived).await;
    }
    write.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[restate_core::test]
    async fn armed_faults_pause_or_fail_the_next_run_once() {
        let faults = Faults::default();
        assert!(faults.arrive(FaultPoint::SnapshotCapture).is_none());

        // A pause holds the next run, in any clone, until resumed, and only that run.
        let (reached, resume) = faults.pause(FaultPoint::SnapshotCapture);
        let waiting = tokio::spawn({
            let faults = faults.clone();
            async move { faults.reach(FaultPoint::SnapshotCapture).await }
        });
        reached.await.unwrap();
        assert!(!waiting.is_finished());
        drop(resume);
        waiting.await.unwrap();
        assert!(faults.arrive(FaultPoint::SnapshotCapture).is_none());

        // A paused run that is dropped closes its `resume`.
        let (reached, mut resume) = faults.pause(FaultPoint::NativeWriteQueued);
        let waiting = tokio::spawn({
            let faults = faults.clone();
            async move { faults.reach(FaultPoint::NativeWriteQueued).await }
        });
        reached.await.unwrap();
        waiting.abort();
        resume.closed().await;

        // A failure replaces the next run of work that can fail.
        faults.fail(FaultPoint::Delete, RocksError::ReadOnly);
        assert!(matches!(
            inject(faults.take(FaultPoint::Delete)).await,
            Err(RocksError::ReadOnly)
        ));
        assert!(inject(faults.take(FaultPoint::Delete)).await.is_ok());
    }
}
