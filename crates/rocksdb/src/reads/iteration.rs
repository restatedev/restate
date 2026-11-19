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

use bytes::{Bytes, BytesMut};
use rocksdb::{ErrorKind, ReadOptions, ReadTier};

use restate_core::io::Gate;

use super::{ColumnFamilies, ReadIterator, ReadSnapshot, ReadSource, TOKEN};
use crate::background::{CachedOpFn, OpFn, yield_now};
use crate::{
    Cancellable, Cooperative, Gated, Inline, IterAction, RocksError, StorageOp, StorageTaskKind,
};

/// How many items an iteration reads in one slice: it then lets the thread's
/// other tasks run.
const STEPS_PER_SLICE: usize = 128;

/// Consumes an iterator's borrowed rows on a storage worker.
///
/// The iterator stays on the current row until [`Self::visit`] completes, so
/// the visitor can borrow its key and value across awaits without copying them.
/// Its futures need not be `Send`: they stay on the same worker. The iteration
/// hands the visitor back once it ends without an error.
pub trait IteratorVisitor: Send + Sized + 'static {
    type Error: Send + 'static;

    /// Processes a row and chooses where to move next. An error fails the
    /// iteration immediately. Gate cancellation can drop a suspended visit;
    /// any resources it holds must be released when its future is dropped.
    fn visit(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<IterAction, Self::Error>>;
}

impl<F, E> IteratorVisitor for F
where
    F: AsyncFnMut(&[u8], &[u8]) -> Result<IterAction, E> + Send + 'static,
    E: Send + 'static,
{
    type Error = E;

    async fn visit(&mut self, key: &[u8], value: &[u8]) -> Result<IterAction, E> {
        self(key, value).await
    }
}

/// A visitor that can also take rows on its caller's thread, where it must not
/// wait. Needed to start an iteration with
/// [`call_cached`](crate::StorageOp::call_cached).
pub trait InlineVisitor: IteratorVisitor {
    /// Like [`IteratorVisitor::visit`], if it can process the row without
    /// waiting. Otherwise returns `None` and leaves the row alone: the
    /// iteration then goes on on a storage worker, where
    /// [`visit`](IteratorVisitor::visit) gets that row.
    fn try_visit(&mut self, key: &[u8], value: &[u8]) -> Option<Result<IterAction, Self::Error>>;
}

/// A successful iteration, including why it ended.
#[derive(Debug)]
pub enum IterationOutcome<V> {
    /// No rows remain within the iterator's bounds.
    Exhausted(V),
    /// The initial action or visitor requested [`IterAction::Stop`].
    Stopped(V),
}

impl<V> IterationOutcome<V> {
    /// Transforms the returned value without changing the completion reason.
    pub fn map<T>(self, f: impl FnOnce(V) -> T) -> IterationOutcome<T> {
        match self {
            Self::Exhausted(value) => IterationOutcome::Exhausted(f(value)),
            Self::Stopped(value) => IterationOutcome::Stopped(f(value)),
        }
    }
}

/// A failure while reading rows or running a visitor.
#[derive(Debug, thiserror::Error)]
pub enum IterationError<E> {
    /// The gate's owner requested early termination after admission.
    #[error("iteration cancelled")]
    Cancelled,
    #[error(transparent)]
    RocksDb(#[from] RocksError),
    #[error("iterator visitor failed: {0}")]
    Visitor(#[source] E),
}

impl<C: ColumnFamilies> ReadSnapshot<C> {
    /// Prepares an iteration over the column family `cf` as one async
    /// operation. It starts with the move `start` and hands each row to
    /// `visitor`, which returns how to move next. It is counted in `gate`,
    /// which must be inside the snapshot's database.
    ///
    /// The iteration creates its own iterator when it runs, with the options
    /// that `options` returns. So it can run in two parts:
    ///
    /// - Submitting it to an executor, for example with
    ///   [`IoExecutor::spawn`](restate_core::io::IoExecutor::spawn), runs it on
    ///   a storage worker, where a read may wait for the disk. Only then is the
    ///   snapshot cloned.
    /// - [`call_cached`](StorageOp::call_cached) first runs it on the calling
    ///   thread, borrowing the snapshot, for as long as its rows are in memory
    ///   and its visitor takes them without waiting (see [`InlineVisitor`]). It
    ///   hands back the rest of the iteration, to submit, at the first row
    ///   where it can't go on, and after
    ///   [`max_cached_rows`](StorageOp::max_cached_rows) rows.
    ///
    /// ```ignore
    /// let iteration = snapshot
    ///     .prepare_iteration(gate, cf, ReadOptions::default, IterAction::SeekToFirst, visitor)
    ///     .max_cached_rows(128);
    /// let outcome = match iteration.call_cached()? {
    ///     Inline::Done(outcome) => outcome?,
    ///     Inline::WouldBlock(rest) => executor.spawn_cancellable(rest)?.await??,
    /// };
    /// ```
    ///
    /// On a worker, the visitor can wait, for example for room in a channel.
    /// The worker runs other tasks meanwhile, so a waiting iteration doesn't
    /// hold it. Every so many rows, the iteration also lets the worker's other
    /// tasks run.
    ///
    /// The operation returns [`IterationOutcome::Exhausted`] when no rows remain,
    /// or [`IterationOutcome::Stopped`] for [`IterAction::Stop`]. Both contain the
    /// visitor. The iterator is dropped before either result is delivered.
    ///
    /// # Errors
    /// Preparing this operation does not acquire admission. Submission can fail
    /// with [`AdmissionClosed`](restate_core::io::AdmissionClosed) before the executor accepts it. After
    /// acceptance, [`IterationError::RocksDb`] reports a read failure, and
    /// [`IterationError::Visitor`] preserves the visitor's error.
    /// [`IterationError::Cancelled`] reports a gate owner's request to stop
    /// after admission. The executor's separate
    /// [`IoTaskError`](restate_core::io::IoTaskError) means accepted work ended
    /// without delivering its result, for example after a panic. Dropping a
    /// cancellable result requests worker cleanup without waiting for it.
    ///
    /// The iteration admits through `gate` at submission and holds its lease
    /// through cleanup. Sealing lets it finish; cancelling the gate stops it at
    /// a slice boundary or while its visitor is suspended. The visitor must
    /// be safe to drop while suspended. A slice bounds the number of visits,
    /// not elapsed time: native calls and synchronous visitor work cannot be
    /// interrupted.
    ///
    /// # Panics
    /// In debug builds, panics if `gate` is outside the snapshot's database.
    pub fn prepare_iteration<'g, O, V>(
        &self,
        gate: &'g Arc<Gate>,
        cf: C::Cf,
        options: O,
        start: IterAction,
        visitor: V,
    ) -> StorageOp<Cooperative<&Self>, Iteration<C::Cf, O, V>, Gated<'g>>
    where
        O: Fn() -> ReadOptions + Send + 'static,
        V: IteratorVisitor,
    {
        prepare(self, gate, cf, options, start, visitor)
    }
}

// Prepares an iteration over `source`'s column family `cf`, for the
// `prepare_iteration` of a snapshot and of a scope.
pub(super) fn prepare<'c, 'g, H: ReadSource, O, V>(
    source: &'c H,
    gate: &'g Arc<Gate>,
    cf: H::CfKey,
    options: O,
    start: IterAction,
    visitor: V,
) -> StorageOp<Cooperative<&'c H>, Iteration<H::CfKey, O, V>, Gated<'g>> {
    source.handles(TOKEN).database().debug_assert_inside(gate);
    StorageOp::from_parts(
        Cooperative(source),
        gate,
        StorageTaskKind::BackgroundIterator,
        Iteration {
            cf,
            options,
            visitor,
            seek: start,
            then: None,
            max_cached_rows: usize::MAX,
        },
    )
}

/// The work of an operation from [`ReadSnapshot::prepare_iteration`] or
/// [`ScopedReads::prepare_iteration`](super::ScopedReads::prepare_iteration).
pub struct Iteration<Cf, O, V> {
    cf: Cf,
    options: O,
    visitor: V,
    // Where the iteration goes on: `seek`, followed by `then`, leads to the
    // next row to visit.
    seek: IterAction,
    then: Option<IterAction>,
    // How many rows one `call_cached` visits before it hands back the rest.
    max_cached_rows: usize,
}

impl<'g, C, Cf, O, V> StorageOp<C, Iteration<Cf, O, V>, Gated<'g>> {
    /// Limits how many rows [`call_cached`](StorageOp::call_cached) visits on
    /// the calling thread before it hands back the rest, so that a long
    /// iteration doesn't hold up the thread even when all of its rows are in
    /// memory. Unlimited by default.
    pub fn max_cached_rows(self, max_rows: usize) -> Self {
        let (context, gate, kind, mut iteration) = self.into_parts();
        iteration.max_cached_rows = max_rows;
        StorageOp::from_parts(context, gate, kind, iteration)
    }
}

impl<H, O, V> OpFn<Cooperative<&H>> for Iteration<H::CfKey, O, V>
where
    H: ReadSource + Send + Sync + 'static,
    H::CfKey: Send + 'static,
    O: Fn() -> ReadOptions + Send + 'static,
    V: IteratorVisitor,
{
    type Output = Result<IterationOutcome<V>, IterationError<V::Error>>;

    async fn run_async(
        mut self,
        Cancellable {
            context: source,
            cancelled,
        }: Cancellable<H>,
    ) -> Self::Output {
        let completion = {
            let mut options = (self.options)();
            // A storage worker may wait for the disk.
            options.set_read_tier(ReadTier::All);
            let mut iterator = ReadIterator::new(&source, self.cf, options);
            let position = [Some(self.seek), self.then].into_iter().flatten();
            // Cancellation is checked whenever this future is polled, so it is
            // observed at each slice yield and while a visitor is suspended,
            // without a check per row.
            tokio::select! {
                biased;
                _ = cancelled.cancelled() => return Err(IterationError::Cancelled),
                completion = run(&mut iterator, &mut self.visitor, position) => completion?,
            }
        };
        Ok(completion.map(|()| self.visitor))
    }
}

// Only for a snapshot: the two parts read with different iterators, which see
// the same data only through a snapshot.
impl<'c, C, O, V> CachedOpFn<Cooperative<&'c ReadSnapshot<C>>> for Iteration<C::Cf, O, V>
where
    C: ColumnFamilies,
    ReadSnapshot<C>: Send + Sync + 'static,
    C::Cf: Send + 'static,
    O: Fn() -> ReadOptions + Send + 'static,
    V: InlineVisitor,
{
    // Rows are read from the memtables and the block cache only, and handed to
    // `InlineVisitor::try_visit`. The rest goes on at the first row that isn't
    // in memory or that the visitor can't take without waiting, and after
    // `max_cached_rows` visits while rows remain.
    fn run_cached(
        mut self,
        Cooperative(snapshot): &Cooperative<&'c ReadSnapshot<C>>,
    ) -> Inline<Self::Output, Self> {
        let ran = {
            let mut options = (self.options)();
            options.set_read_tier(ReadTier::BlockCache);
            let mut iterator = ReadIterator::new(*snapshot, self.cf, options);
            self.visit_cached_rows(&mut iterator)
        };
        match ran {
            Ok(Some(completion)) => Inline::Done(Ok(completion.map(|()| self.visitor))),
            Ok(None) => Inline::WouldBlock(self),
            // The visitor is dropped here, after the iterator.
            Err(err) => Inline::Done(Err(err)),
        }
    }
}

impl<Cf, O, V: InlineVisitor> Iteration<Cf, O, V> {
    // Runs the iteration with `iterator`, which reads from memory only and
    // isn't positioned yet. Returns `None` once going on would block, or after
    // `max_cached_rows` visits while rows remain: `seek` and `then` then lead
    // to the next row to visit.
    fn visit_cached_rows<H: ReadSource>(
        &mut self,
        iterator: &mut ReadIterator<'_, H>,
    ) -> Result<Option<IterationOutcome<()>>, IterationError<V::Error>> {
        for action in [Some(&self.seek), self.then.as_ref()].into_iter().flatten() {
            if !apply(iterator, action.clone()) {
                return Ok(Some(IterationOutcome::Stopped(())));
            }
        }
        // The last row visited, and the relative move made from it. Set while
        // `seek` and `then` don't lead to the iterator's position yet.
        let mut last_key = BytesMut::new();
        let mut moved = None;
        let mut rows = 0;
        loop {
            let (key, value) = match iterator.item() {
                Ok(Some(row)) => row,
                Ok(None) => return Ok(Some(IterationOutcome::Exhausted(()))),
                // The last move needs the disk: the worker repeats it.
                Err(RocksError::Other(err)) if err.kind() == ErrorKind::Incomplete => {
                    if let Some(moved) = moved {
                        self.seek = IterAction::Seek(last_key.freeze());
                        self.then = Some(moved);
                    }
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            };
            if rows == self.max_cached_rows {
                self.go_on_at(key);
                return Ok(None);
            }
            let Some(action) = self.visitor.try_visit(key, value) else {
                self.go_on_at(key);
                return Ok(None);
            };
            match action.map_err(IterationError::Visitor)? {
                IterAction::Stop => return Ok(Some(IterationOutcome::Stopped(()))),
                action @ (IterAction::Next | IterAction::Prev) => {
                    last_key.clear();
                    last_key.extend_from_slice(key);
                    apply(iterator, action.clone());
                    moved = Some(action);
                }
                // An absolute move leads to the next row by itself.
                action @ (IterAction::Seek(_)
                | IterAction::SeekToFirst
                | IterAction::SeekToLast) => {
                    apply(iterator, action.clone());
                    self.seek = action;
                    self.then = None;
                    moved = None;
                }
            }
            rows += 1;
        }
    }

    // The iteration goes on at the row with this key, which hasn't been visited.
    fn go_on_at(&mut self, key: &[u8]) {
        self.seek = IterAction::Seek(Bytes::copy_from_slice(key));
        self.then = None;
    }
}

/// Moves the iterator as `position` says, then reads until the iterator is
/// exhausted or the visitor stops it.
async fn run<H, V>(
    iterator: &mut ReadIterator<'_, H>,
    visitor: &mut V,
    position: impl IntoIterator<Item = IterAction>,
) -> Result<IterationOutcome<()>, IterationError<V::Error>>
where
    H: ReadSource,
    V: IteratorVisitor,
{
    for action in position {
        if !apply(iterator, action) {
            return Ok(IterationOutcome::Stopped(()));
        }
    }
    let mut steps = 0;
    while let Some((key, value)) = iterator.item()? {
        // Check the next row before yielding, so a finished iteration returns
        // without yielding once more.
        if steps == STEPS_PER_SLICE {
            yield_now().await;
            steps = 0;
        }
        let action = visitor
            .visit(key, value)
            .await
            .map_err(IterationError::Visitor)?;
        if !apply(iterator, action) {
            return Ok(IterationOutcome::Stopped(()));
        }
        steps += 1;
    }
    Ok(IterationOutcome::Exhausted(()))
}

/// Moves `iterator` as `action` says. Returns `false` for [`IterAction::Stop`].
fn apply<H>(iterator: &mut ReadIterator<'_, H>, action: IterAction) -> bool {
    match action {
        IterAction::Seek(b) => iterator.seek(b.as_ref()),
        IterAction::Prev => iterator.prev(),
        IterAction::Next => iterator.next(),
        IterAction::SeekToFirst => iterator.seek_to_first(),
        IterAction::SeekToLast => iterator.seek_to_last(),
        IterAction::Stop => return false,
    }
    true
}
