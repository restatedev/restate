// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Migration jobs: background work that builds derived data, such as an index or
//! a statistic, from a table that is being written at the same time.
//!
//! While a job is still running, its output reflects some rows and not others.
//! A write that changes a row must know which case it is in: if the job already
//! reflects the row, the write replaces the row's old contribution; if not, it
//! only adds the new one. The rows of the source table carry that knowledge as
//! **tokens**: a row lists the IDs of the unfinished jobs that already reflect it.
//!
//! This module holds the token list's representation, [`MigrationJobIds`]. Which
//! table carries tokens, where it stores them, and when they are written are
//! decided by the table and by the partition store that maintains the jobs. The
//! vqueue entry-status header is the first such table (its `touched_by` field).

/// The IDs of the unfinished migration jobs that already reflect one row.
///
/// For each listed job, the row's *current* state has been applied to the job's
/// output. That includes the case where the row contributes nothing, for example
/// an entry that an index doesn't cover: the job has looked at the row and
/// decided. A write to the row then replaces the old contribution for listed
/// jobs and only adds the new one for unlisted jobs.
///
/// Tokens are needed only while a job runs. A finished job reflects every row by
/// definition, so its token is dropped the next time the row is written; a row
/// that is never written again keeps the stale token, which is harmless because
/// job IDs are never reused.
///
/// A job building an index has the same ID as the index. Only the partition
/// store writes tokens; readers treat the bytes as opaque except through this
/// type.
///
/// Stored as sorted, distinct big-endian `u32`s, four bytes per job. For example,
/// a row written while jobs 64 and 70 were running stores
/// `[0, 0, 0, 64, 0, 0, 0, 70]`; once both have finished, its next write stores
/// nothing, and the field is omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MigrationJobIds<'a>(&'a [u8]);

impl<'a> MigrationJobIds<'a> {
    /// No tokens: the row is reflected by no unfinished job.
    pub const EMPTY: Self = Self(&[]);

    /// Wraps stored bytes without checking them; [`Self::ids`] validates them.
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &'a [u8] {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Fails with [`StorageError::DataIntegrityError`](crate::StorageError) if the
    /// bytes are not a sorted list of distinct big-endian `u32`s.
    pub fn validate(&self) -> crate::Result<()> {
        self.ids()
            .map(|_| ())
            .ok_or(crate::StorageError::DataIntegrityError)
    }

    /// Whether job `id` is listed. Expects validated bytes (see [`Self::ids`]).
    pub fn contains(&self, id: u32) -> bool {
        let (chunks, _) = self.0.as_chunks::<4>();
        chunks
            .binary_search_by_key(&id, |chunk| u32::from_be_bytes(*chunk))
            .is_ok()
    }

    /// The IDs in increasing order, or `None` if the bytes are not a sorted list
    /// of distinct big-endian `u32`s.
    pub fn ids(&self) -> Option<impl Iterator<Item = u32> + 'a> {
        let (chunks, rest) = self.0.as_chunks::<4>();
        let ids = chunks.iter().map(|chunk| u32::from_be_bytes(*chunk));
        (rest.is_empty() && ids.clone().is_sorted_by(|a, b| a < b)).then_some(ids)
    }

    /// The stored bytes of this list with the sorted, distinct `added` IDs listed
    /// too, keeping every ID already listed. For example, `[64, 70]` with
    /// `[65, 70]` added is `[64, 65, 70]`. Expects validated bytes (see
    /// [`Self::ids`]).
    pub fn with(&self, added: impl IntoIterator<Item = u32>) -> Vec<u8> {
        let (listed, _) = self.0.as_chunks::<4>();
        let mut listed = listed
            .iter()
            .map(|chunk| u32::from_be_bytes(*chunk))
            .peekable();
        let mut added = added.into_iter().peekable();
        let mut out = Vec::with_capacity(self.0.len() + added.size_hint().0 * 4);
        loop {
            let next = match (listed.peek(), added.peek()) {
                (None, None) => break,
                (Some(_), None) => listed.next(),
                (None, Some(_)) => added.next(),
                (Some(listed_id), Some(added_id)) => match listed_id.cmp(added_id) {
                    std::cmp::Ordering::Less => listed.next(),
                    std::cmp::Ordering::Greater => added.next(),
                    std::cmp::Ordering::Equal => {
                        added.next();
                        listed.next()
                    }
                },
            };
            let next = next.expect("peeked");
            debug_assert!(
                out.last_chunk::<4>()
                    .is_none_or(|last| u32::from_be_bytes(*last) < next),
                "added IDs are sorted and distinct"
            );
            out.extend_from_slice(&next.to_be_bytes());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::MigrationJobIds;

    #[test]
    fn ids_are_sorted_distinct_big_endian_u32s() {
        let ids = MigrationJobIds::from_bytes(&[0, 0, 0, 64, 0, 1, 0, 70]);
        assert_eq!(ids.ids().unwrap().collect::<Vec<_>>(), [64, 65_606]);
        assert!(ids.contains(64) && ids.contains(65_606) && !ids.contains(65));
        assert_eq!(MigrationJobIds::EMPTY.ids().unwrap().count(), 0);
        // Adding keeps the list sorted and distinct.
        let added = ids.with([64, 65]);
        assert_eq!(
            MigrationJobIds::from_bytes(&added)
                .ids()
                .unwrap()
                .collect::<Vec<_>>(),
            [64, 65, 65_606]
        );
        assert_eq!(MigrationJobIds::EMPTY.with([]), Vec::<u8>::new());
        for malformed in [
            &[0, 0, 0, 64, 0][..],
            &[0, 0, 0, 70, 0, 0, 0, 64],
            &[0, 0, 0, 64, 0, 0, 0, 64],
        ] {
            let ids = MigrationJobIds::from_bytes(malformed);
            assert!(
                ids.ids().is_none() && ids.validate().is_err(),
                "{malformed:?}"
            );
        }
    }
}
