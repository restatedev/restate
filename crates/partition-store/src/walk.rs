// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Walks: resumable passes, in key order, over one source table of a partition.
//!
//! A **walk** applies one job's work to every key of its source that existed when
//! it started. It records the source's last key at its start (`end`, inclusive)
//! and a **cursor**, the last key it has processed (exclusive resume position).
//! The cursor is only a bookmark: what a job did to a key is recorded on the key
//! itself, as for the index backfill's tokens, never inferred from the cursor. A
//! walk is exhausted once no key of its source remains in `(cursor, end]`; keys
//! inserted after `end` were written after the walk started, so the job's write
//! path handles them.
//!
//! The record is local to a replica and is not replicated. Walk IDs come from one
//! counter that is never reset, also when the last walk is removed.
//!
//! For example, an index backfill starting over entries `A..E` records `end = E`.
//! After a slice over `A` and `B`, its cursor is `B`, and `A` and `B` carry the
//! backfill's tokens. Once a slice finds nothing after `E`, the walk is exhausted
//! and removed, and its job publishes the result.
//!
//! ```text
//! source keys    A    B    C    D    E    F
//!                ─────────┘              │    └─ written after the walk started:
//!                  processed  cursor = B  end = E   the write path handles it
//! ```
//!
//! The `engine` runs walks a slice at a time, through the [`WalkDriver`], between
//! the partition processor's turns. What a walk's slices did, the rows,
//! time and failures, is recorded on the jobs it runs (see [`crate::job`]).

use bytes::Bytes;

use restate_util_string::ReString;

mod driver;
pub(crate) mod engine;

pub(crate) use driver::WalkStats;
pub use driver::{WalkDriver, WalkStatsSnapshot};

/// The persisted format this binary reads and writes.
const FORMAT: u32 = 1;

/// The table a walk traverses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum Source {
    #[bilrost(0)]
    Unknown,
    /// Entry statuses, in entry-status key order within the partition's key range.
    #[bilrost(1)]
    EntryStatus,
}

/// The kind of a walk: its name and the version of its semantics.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct WalkKind {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub version: u32,
}

impl WalkKind {
    /// Backfills incomplete secondary and aggregate indexes; its members are the
    /// indexes that link to the walk (see [`crate::registry`]). Version 2 records
    /// every entry it fills with its members' tokens.
    pub(crate) fn index_backfill() -> Self {
        Self {
            name: ReString::from_static("index_backfill"),
            version: 2,
        }
    }

    /// The index backfill of development builds before version 2. It filled
    /// entries without tokens, so only its cursor recorded them. This binary
    /// never runs it: opening a store retires the indexes it was building and
    /// builds them again.
    pub(crate) fn untokened_index_backfill() -> Self {
        Self {
            name: ReString::from_static("index_backfill"),
            version: 1,
        }
    }
}

/// One walk and its committed progress.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Walk {
    #[bilrost(1)]
    pub id: u64,
    #[bilrost(2)]
    pub kind: WalkKind,
    #[bilrost(3)]
    pub source: Source,
    /// The last source key when the walk started, inclusive.
    #[bilrost(4)]
    pub end: Bytes,
    /// The last processed key, exclusive resume position; `None` before the first
    /// slice.
    #[bilrost(5)]
    pub cursor: Option<Bytes>,
    /// Parameters owned and versioned by the walk kind.
    #[bilrost(6)]
    pub params: Bytes,
    // Tags 7–9 held the walk's diagnostic counters, now kept by its jobs (see
    // [`crate::job`]). They are retired: never reuse them.
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WalkError {
    #[error("all walk IDs of this store are allocated")]
    Exhausted,
    #[error("unsupported walk record format {0}; this binary supports format {FORMAT}")]
    UnsupportedFormat(u32),
    #[error("unsupported walk kind '{}' version {}", .0.name, .0.version)]
    UnsupportedKind(WalkKind),
    #[error("no walk has ID {0}")]
    UnknownId(u64),
    #[error("malformed walk record: {0}")]
    Malformed(#[from] bilrost::DecodeError),
    #[error("invalid walk record: {0}")]
    Invalid(&'static str),
}

/// The walks of one partition store and their ID counter.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Walks {
    #[bilrost(1)]
    format: u32,
    /// The next ID to issue; `u64::MAX` is never issued.
    #[bilrost(2)]
    next_id: u64,
    #[bilrost(3)]
    walks: Vec<Walk>,
}

impl Default for Walks {
    fn default() -> Self {
        Self {
            format: FORMAT,
            next_id: 1,
            walks: Vec::new(),
        }
    }
}

impl Walks {
    /// Decodes a persisted record and checks its invariants. Unknown jobs fail:
    /// this binary cannot run them, nor tell which effects they own.
    pub fn decode(bytes: &[u8]) -> Result<Self, WalkError> {
        let walks = <Self as bilrost::OwnedMessage>::decode(bytes)?;
        if walks.format != FORMAT {
            return Err(WalkError::UnsupportedFormat(walks.format));
        }
        walks.validate()?;
        Ok(walks)
    }

    fn validate(&self) -> Result<(), WalkError> {
        let invalid = |reason| Err(WalkError::Invalid(reason));
        if self.next_id == 0 {
            return invalid("the ID counter is zero");
        }
        let mut ids = std::collections::BTreeSet::new();
        for walk in &self.walks {
            if walk.id == 0 || walk.id >= self.next_id {
                return invalid("a walk ID was not issued by the counter");
            }
            if !ids.insert(walk.id) {
                return invalid("a walk ID is used twice");
            }
            if walk.kind != WalkKind::index_backfill()
                && walk.kind != WalkKind::untokened_index_backfill()
            {
                return Err(WalkError::UnsupportedKind(walk.kind.clone()));
            }
            if !walk.params.is_empty() {
                return invalid("an index backfill has parameters");
            }
            if walk.source != Source::EntryStatus {
                return invalid("a walk has an unknown source");
            }
            if walk
                .cursor
                .as_ref()
                .is_some_and(|cursor| *cursor > walk.end)
            {
                return invalid("a walk's cursor is past its end");
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        bilrost::Message::encode_to_vec(self)
    }

    pub fn entries(&self) -> &[Walk] {
        &self.walks
    }

    pub fn get(&self, id: u64) -> Option<&Walk> {
        self.walks.iter().find(|walk| walk.id == id)
    }

    /// Starts a walk of `kind` over `source` up to `end`, inclusive. The new
    /// record is checked like a decoded one, so that nothing `start` accepts can
    /// fail to load again; a rejected walk leaves the record unchanged.
    pub(crate) fn start(
        &mut self,
        kind: WalkKind,
        source: Source,
        end: Bytes,
    ) -> Result<u64, WalkError> {
        if self.next_id == u64::MAX {
            return Err(WalkError::Exhausted);
        }
        let id = self.next_id;
        self.next_id += 1;
        self.walks.push(Walk {
            id,
            kind,
            source,
            end,
            cursor: None,
            params: Bytes::new(),
        });
        if let Err(error) = self.validate() {
            self.walks.pop();
            self.next_id -= 1;
            return Err(error);
        }
        Ok(id)
    }

    /// Records a committed slice's last key. The cursor only moves forward.
    pub(crate) fn advance(&mut self, id: u64, cursor: Bytes) -> Result<(), WalkError> {
        let walk = self
            .walks
            .iter_mut()
            .find(|walk| walk.id == id)
            .ok_or(WalkError::UnknownId(id))?;
        if walk
            .cursor
            .as_ref()
            .is_some_and(|previous| cursor <= *previous)
            || cursor > walk.end
        {
            return Err(WalkError::Invalid(
                "a walk's cursor must move forward within its end",
            ));
        }
        walk.cursor = Some(cursor);
        Ok(())
    }

    /// Removes a finished or abandoned walk. Its ID stays issued.
    pub(crate) fn remove(&mut self, id: u64) -> Result<(), WalkError> {
        let before = self.walks.len();
        self.walks.retain(|walk| walk.id != id);
        if self.walks.len() == before {
            return Err(WalkError::UnknownId(id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_issue_fresh_ids_validate_and_only_move_forward() {
        let mut walks = Walks::default();
        let first = walks
            .start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::from_static(b"e"),
            )
            .unwrap();
        let second = walks
            .start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::from_static(b"e"),
            )
            .unwrap();
        assert_eq!((first, second), (1, 2));
        walks.advance(second, Bytes::from_static(b"b")).unwrap();
        assert_eq!(
            walks.get(second).unwrap().cursor.as_deref(),
            Some(&b"b"[..])
        );
        assert_eq!(
            walks.get(first).unwrap().cursor,
            None,
            "nothing before the first slice"
        );

        walks.advance(first, Bytes::from_static(b"c")).unwrap();
        assert!(
            walks.advance(first, Bytes::from_static(b"c")).is_err(),
            "no repeat"
        );
        assert!(
            walks.advance(first, Bytes::from_static(b"f")).is_err(),
            "not past end"
        );

        // Removing every walk keeps the counter.
        walks.remove(first).unwrap();
        walks.remove(second).unwrap();
        let reloaded = Walks::decode(&walks.encode()).unwrap();
        assert!(reloaded.entries().is_empty());
        let mut reloaded = reloaded;
        assert_eq!(
            reloaded
                .start(
                    WalkKind::index_backfill(),
                    Source::EntryStatus,
                    Bytes::new(),
                )
                .unwrap(),
            3
        );

        let corrupt = |change: fn(&mut Walks)| {
            let mut walks = reloaded.clone();
            change(&mut walks);
            Walks::decode(&walks.encode())
        };
        assert_eq!(
            corrupt(|walks| walks.format = FORMAT + 1),
            Err(WalkError::UnsupportedFormat(FORMAT + 1))
        );
        assert!(matches!(
            corrupt(|walks| walks.walks[0].kind.version = 3),
            Err(WalkError::UnsupportedKind(_))
        ));
        assert!(corrupt(|walks| walks.walks[0].id = walks.next_id).is_err());
        assert!(corrupt(|walks| walks.walks.push(walks.walks[0].clone())).is_err());
        assert!(corrupt(|walks| walks.walks[0].source = Source::Unknown).is_err());
        assert!(corrupt(|walks| walks.walks[0].params = Bytes::from_static(b"x")).is_err());
        assert!(corrupt(|walks| walks.walks[0].cursor = Some(Bytes::from_static(b"z"))).is_err());
        // `start` rejects what `decode` would reject, and reserves no ID for it.
        let before = reloaded.clone();
        assert!(matches!(
            reloaded.start(
                WalkKind {
                    version: 3,
                    ..WalkKind::index_backfill()
                },
                Source::EntryStatus,
                Bytes::new(),
            ),
            Err(WalkError::UnsupportedKind(_))
        ));
        assert_eq!(reloaded, before, "a rejected start changes nothing");
        let mut exhausted = Walks {
            next_id: u64::MAX,
            ..Walks::default()
        };
        assert_eq!(
            exhausted.start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::new(),
            ),
            Err(WalkError::Exhausted)
        );
    }
}
