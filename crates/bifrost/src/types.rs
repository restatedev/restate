// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::{Poll, ready};

use futures::FutureExt;

use restate_types::logs::{LogletOffset, Lsn, SequenceNumber};

use crate::loglet::AppendError;

// Only implemented for LSNs
pub(crate) trait LsnExt
where
    Self: Sized + Into<u64>,
{
    /// Converts a loglet offset into the virtual address (LSN).
    ///
    /// # Panics
    ///
    /// On conversion overflow this function will panic.
    #[track_caller]
    fn offset_by(self, offset: LogletOffset) -> Lsn {
        // We subtract from OLDEST because loglets might start offsets from a non-zero value.
        // 1 is the oldest valid offset within a loglet, 0 is an invalid offset.
        debug_assert!(offset >= LogletOffset::OLDEST);
        let self_raw: u64 = self.into();
        let offset_raw: u32 = offset.into();

        let offset_from_zero = offset_raw
            .checked_sub(LogletOffset::OLDEST.into())
            .expect("offset is >= OLDEST offset");

        Lsn::from(
            self_raw
                .checked_add(offset_from_zero as u64)
                .expect("offset to lsn conversion to not overflow"),
        )
    }

    /// Convert an LSN back to a loglet offset given a base_lsn.
    ///
    /// # Panics
    ///
    /// On conversion overflow this function will panic. This also panics if
    /// base_lsn is `Lsn::INVALID`.
    #[track_caller]
    fn into_offset(self, base_lsn: Lsn) -> LogletOffset {
        assert!(base_lsn > Lsn::INVALID);
        let base_lsn_raw: u64 = base_lsn.into();
        let self_raw: u64 = self.into();
        let oldest_offset: u32 = LogletOffset::OLDEST.into();
        assert!(self_raw >= base_lsn_raw);

        LogletOffset::new(
            (self_raw - base_lsn_raw)
                .checked_add(oldest_offset as u64)
                .expect("offset+base_lsn within LSN bounds")
                .try_into()
                .expect("LogletOffset must fit within u32"),
        )
    }
}

impl LsnExt for Lsn {}

/// A future that resolves to the Lsn of the last Lsn in a committed batch.
///
/// Note: dropping this future doesn't cancel or stop the underlying enqueued append.
pub struct Commit {
    state: CommitState,
}

impl Commit {
    pub(crate) fn passthrough(base_lsn: Lsn, inner: crate::loglet::LogletCommit) -> Self {
        Self {
            state: CommitState::Passthrough { base_lsn, inner },
        }
    }

    pub(crate) fn sealed() -> Self {
        Self {
            state: CommitState::Sealed,
        }
    }
}

enum CommitState {
    Sealed,
    Passthrough {
        base_lsn: Lsn,
        inner: crate::loglet::LogletCommit,
    },
}

impl std::future::Future for Commit {
    type Output = Result<Lsn, AppendError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.state {
            CommitState::Sealed => Poll::Ready(Err(AppendError::Sealed)),
            CommitState::Passthrough {
                ref mut inner,
                base_lsn,
            } => {
                let res = ready!(inner.poll_unpin(cx));
                Poll::Ready(res.map(|offset| base_lsn.offset_by(offset)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::LsnExt;
    use restate_types::logs::{LogletOffset, Lsn, SequenceNumber};

    #[test]
    fn lsn_to_offset() {
        let lsn = Lsn::OLDEST;
        let offset = lsn.into_offset(Lsn::OLDEST);
        assert_eq!(offset, LogletOffset::OLDEST);

        // INVALID cannot be used as base_lsn
        let lsn = Lsn::INVALID;
        assert!(std::panic::catch_unwind(|| lsn.into_offset(Lsn::from(100))).is_err());

        // base_lsn > lsn
        let lsn = Lsn::OLDEST;
        assert!(std::panic::catch_unwind(|| lsn.into_offset(Lsn::from(100))).is_err());

        // base_lsn -> oldest
        let lsn = Lsn::from(100);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST);

        let lsn = Lsn::from(101);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST.next());
    }

    #[test]
    #[should_panic]
    fn invalid_offset_cannot_be_offsetted() {
        // not acceptable. offset must be > oldest
        let offset = LogletOffset::INVALID;
        let base_lsn = Lsn::OLDEST;
        base_lsn.offset_by(offset);
    }

    #[test]
    fn offset_to_lsn() {
        let offset = LogletOffset::OLDEST;
        let base_lsn = Lsn::OLDEST;
        assert_eq!(Lsn::OLDEST, base_lsn.offset_by(offset));

        let offset = LogletOffset::from(10);
        let base_lsn = Lsn::OLDEST;
        assert_eq!(Lsn::from(10), base_lsn.offset_by(offset));

        let offset = LogletOffset::from(10);
        let base_lsn = Lsn::from(100);
        assert_eq!(Lsn::from(109), base_lsn.offset_by(offset));

        // validate we panic on overflow
        let base_lsn = Lsn::new(u64::from(Lsn::MAX) - 100);
        let offset = LogletOffset::MAX;
        assert!(std::panic::catch_unwind(|| base_lsn.offset_by(offset)).is_err());
    }
}
