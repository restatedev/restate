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
//! Each walk is one record under its own key (see [`crate::registry::records`]),
//! local to the replica and not replicated: a slice rewrites its walk's key, and
//! the walk's end deletes it. Walk IDs come from one counter in the registry
//! ([`crate::registry::Registry::next_walk_id`]) that is never reset, also when
//! the last walk is removed. [`Walks`] holds a store's walks in memory.
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

use crate::registry::Registry;

mod driver;
pub(crate) mod engine;

pub use driver::{DriverState, WalkDriver, WalkStatsSnapshot};
pub(crate) use driver::{PublishedDriverState, WalkStats};

/// The table a walk traverses, stored on the walk as its number.
///
/// **Compatibility.** The walk stores the number, not a bilrost enumeration, so a
/// walk over a source this binary doesn't know still decodes, and its
/// [`OnUnknown`] policy decides what an open does with it. A release adding a
/// source sets that policy on its walks, and raises the version floor first if
/// the policy is `Refuse`. Never reuse a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Entry statuses, in entry-status key order within the partition's key range.
    EntryStatus = 1,
}

impl Source {
    /// The source stored as `number`, if this binary knows it.
    pub fn from_number(number: u32) -> Option<Self> {
        (number == Self::EntryStatus as u32).then_some(Self::EntryStatus)
    }
}

/// What an open must do with a walk whose kind or source this binary doesn't
/// know, stored on the walk as its number.
///
/// **Compatibility.** A number this binary doesn't know means `Refuse`. The author
/// of a new kind or source chooses its policy: `Discard` only after showing that
/// dropping the walk and restarting its known members under the opening binary's
/// own kind keeps their meaning, with no source conversion; otherwise `Refuse`,
/// with the version floor raised before such a walk starts. A new policy needs a
/// version floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnUnknown {
    /// The open fails without changing anything.
    Refuse = 0,
    /// The open removes the walk and unlinks its members: members of known
    /// definitions start a pass of this binary's own kind, the others are retired.
    Discard = 1,
}

impl OnUnknown {
    pub fn from_number(number: u32) -> Self {
        if number == Self::Discard as u32 {
            Self::Discard
        } else {
            Self::Refuse
        }
    }
}

/// The kind of a walk: its name and the version of its semantics.
///
/// **Compatibility.** A walk of a kind this binary doesn't know still decodes; its
/// [`OnUnknown`] policy decides what an open does. Changing a kind's semantics
/// needs a new version, with a policy chosen as [`OnUnknown`] describes.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct WalkKind {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub version: u32,
}

impl WalkKind {
    /// Backfills incomplete secondary and aggregate indexes; its members are the
    /// indexes that link to the walk (see [`crate::registry`]). It records every
    /// entry it fills with its members' tokens.
    pub(crate) fn index_backfill() -> Self {
        Self {
            name: ReString::from_static("index_backfill"),
            version: 1,
        }
    }

    /// The policy this binary's walks of this kind carry. An index backfill is
    /// discardable: tokens are the only proof of coverage, so restarting its
    /// members from the start loses nothing.
    fn on_unknown(&self) -> OnUnknown {
        OnUnknown::Discard
    }
}

/// A member of a walk whose completion evidence differs from the default.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct WalkMember {
    /// The member's index ID.
    #[bilrost(1)]
    pub index: u32,
    /// Where the member joined this pass: the walk's committed cursor then.
    /// `None` means at the start of the pass.
    #[bilrost(2)]
    pub joined_at: Option<Bytes>,
    /// Rows this pass found that the member couldn't cover. Saturating.
    #[bilrost(3)]
    pub uncovered: u64,
}

/// One walk and its committed progress, stored under its own key (see
/// [`crate::registry::records`]). Persisted tags never change meaning (README
/// rule 8).
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Walk {
    #[bilrost(1)]
    pub id: u64,
    #[bilrost(2)]
    pub kind: WalkKind,
    /// A [`Source`] number.
    #[bilrost(3)]
    pub source: u32,
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
    /// Completion evidence of the members that differ from the default: joined
    /// this pass late, or found rows they couldn't cover. A member without an
    /// entry joined at the start of the pass and has no gap so far; omitting one is
    /// an encoding, never a stand-in for missing evidence. The registry decides
    /// who the members are.
    #[bilrost(7)]
    pub members: Vec<WalkMember>,
    /// An [`OnUnknown`] number.
    #[bilrost(8)]
    pub on_unknown: u32,
}

impl Walk {
    /// Decodes the record stored under walk ID `key`, which must be its own ID.
    /// Its invariants are checked with the other walks' ([`Walks::new`]).
    pub fn decode(key: u64, bytes: &[u8]) -> Result<Self, WalkError> {
        let walk = <Self as bilrost::OwnedMessage>::decode(bytes)?;
        if walk.id != key {
            return Err(WalkError::IdMismatch {
                key,
                record: walk.id,
            });
        }
        Ok(walk)
    }

    pub fn encode(&self) -> Vec<u8> {
        bilrost::Message::encode_to_vec(self)
    }

    /// The source, if this binary knows it.
    pub fn source(&self) -> Option<Source> {
        Source::from_number(self.source)
    }

    /// Whether this binary knows the walk's kind and source, so it can run it.
    pub fn is_supported(&self) -> bool {
        self.kind == WalkKind::index_backfill() && self.source() == Some(Source::EntryStatus)
    }

    /// The evidence of member `index`, if it differs from the default.
    pub fn member(&self, index: u32) -> Option<&WalkMember> {
        self.members.iter().find(|member| member.index == index)
    }

    /// Whether member `index` joined this pass after its start.
    pub fn joined_late(&self, index: u32) -> bool {
        self.member(index)
            .is_some_and(|member| member.joined_at.is_some())
    }

    /// Rows this pass found that member `index` couldn't cover.
    pub fn uncovered(&self, index: u32) -> u64 {
        self.member(index).map_or(0, |member| member.uncovered)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WalkError {
    #[error("all walk IDs of this store are allocated")]
    Exhausted,
    #[error("unsupported walk kind '{}' version {}", .0.name, .0.version)]
    UnsupportedKind(WalkKind),
    #[error(
        "walk {id} is of kind '{}' version {} over source {table}, which this binary \
         doesn't know and must not discard; open the store with the release that \
         started it",
        .kind.name, .kind.version
    )]
    Refused { id: u64, kind: WalkKind, table: u32 },
    #[error("no walk has ID {0}")]
    UnknownId(u64),
    #[error("malformed walk record: {0}")]
    Malformed(#[from] bilrost::DecodeError),
    #[error("invalid walk record: the key of walk {key} holds walk {record}")]
    IdMismatch { key: u64, record: u64 },
    #[error("invalid walk record: {0}")]
    Invalid(&'static str),
}

/// The walks of one partition store, in memory. Their IDs come from the
/// registry's counter, against which they are validated.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Walks {
    walks: Vec<Walk>,
}

impl Walks {
    /// The walks `walks`, loaded from their records, checked against each other
    /// and `next_id`, the registry's next walk ID. Walks of a kind or source
    /// this binary doesn't know are accepted too, with only their common
    /// invariants checked: an open resolves them by their policy (see
    /// [`Self::preflight`]).
    pub fn new(walks: Vec<Walk>, next_id: u64) -> Result<Self, WalkError> {
        let walks = Self { walks };
        walks.validate(next_id)?;
        Ok(walks)
    }

    fn validate(&self, next_id: u64) -> Result<(), WalkError> {
        let invalid = |reason| Err(WalkError::Invalid(reason));
        let mut ids = std::collections::BTreeSet::new();
        for walk in &self.walks {
            if walk.id == 0 || walk.id >= next_id {
                return invalid("a walk ID was not issued by the counter");
            }
            if !ids.insert(walk.id) {
                return invalid("a walk ID is used twice");
            }
            if !walk.is_supported() {
                // Its params, positions and evidence are the unknown kind's own.
                continue;
            }
            if !walk.params.is_empty() {
                return invalid("an index backfill has parameters");
            }
            if walk
                .cursor
                .as_ref()
                .is_some_and(|cursor| *cursor > walk.end)
            {
                return invalid("a walk's cursor is past its end");
            }
            let mut members = std::collections::BTreeSet::new();
            for member in &walk.members {
                if !members.insert(member.index) {
                    return invalid("a walk lists a member twice");
                }
                if member
                    .joined_at
                    .as_ref()
                    .is_some_and(|joined| walk.cursor.as_ref().is_none_or(|cursor| joined > cursor))
                {
                    return invalid("a member joined a walk past its cursor");
                }
            }
        }
        Ok(())
    }

    /// Checks, before an open changes anything, that every walk of a kind or
    /// source this binary doesn't know may be discarded.
    pub(crate) fn preflight(&self) -> Result<(), WalkError> {
        match self.walks.iter().find(|walk| {
            !walk.is_supported() && OnUnknown::from_number(walk.on_unknown) == OnUnknown::Refuse
        }) {
            Some(walk) => Err(WalkError::Refused {
                id: walk.id,
                kind: walk.kind.clone(),
                table: walk.source,
            }),
            None => Ok(()),
        }
    }

    /// Removes the walks this binary doesn't know, returning their IDs, which stay
    /// issued. Run [`Self::preflight`] first: it refuses the ones that must stay.
    pub(crate) fn discard_unknown(&mut self) -> Vec<u64> {
        let discarded = self
            .walks
            .iter()
            .filter(|walk| !walk.is_supported())
            .map(|walk| walk.id)
            .collect();
        self.walks.retain(Walk::is_supported);
        discarded
    }

    /// Records that index `index` joins walk `id` now, after its committed cursor,
    /// as a later release may. Tests only: this binary attaches members at a
    /// pass's start.
    #[cfg(test)]
    pub(crate) fn join(&mut self, id: u64, index: u32) -> Result<(), WalkError> {
        let walk = self
            .walks
            .iter_mut()
            .find(|walk| walk.id == id)
            .ok_or(WalkError::UnknownId(id))?;
        walk.members.push(WalkMember {
            index,
            joined_at: walk.cursor.clone(),
            uncovered: 0,
        });
        Ok(())
    }

    /// The walks, to change them as another binary might. Tests only.
    #[cfg(test)]
    pub(crate) fn entries_mut(&mut self) -> &mut [Walk] {
        &mut self.walks
    }

    /// Keeps the evidence of the members for which `keep(walk, index)` holds, for
    /// example those still linked to their walk.
    pub(crate) fn retain_members(&mut self, mut keep: impl FnMut(u64, u32) -> bool) {
        for walk in &mut self.walks {
            walk.members.retain(|member| keep(walk.id, member.index));
        }
    }

    /// Adds `count` rows that member `index` of walk `id` couldn't cover.
    pub(crate) fn add_uncovered(
        &mut self,
        id: u64,
        index: u32,
        count: u64,
    ) -> Result<(), WalkError> {
        if count == 0 {
            return Ok(());
        }
        let walk = self
            .walks
            .iter_mut()
            .find(|walk| walk.id == id)
            .ok_or(WalkError::UnknownId(id))?;
        match walk.members.iter_mut().find(|member| member.index == index) {
            Some(member) => member.uncovered = member.uncovered.saturating_add(count),
            None => walk.members.push(WalkMember {
                index,
                joined_at: None,
                uncovered: count,
            }),
        }
        Ok(())
    }

    pub fn entries(&self) -> &[Walk] {
        &self.walks
    }

    pub fn get(&self, id: u64) -> Option<&Walk> {
        self.walks.iter().find(|walk| walk.id == id)
    }

    /// Starts a walk of `kind` over `source` up to `end`, inclusive, with the
    /// next ID of `registry`'s walk counter. The new walk is checked like a
    /// loaded one, so that nothing `start` accepts can fail to load again; a
    /// rejected walk changes neither the walks nor the counter.
    pub(crate) fn start(
        &mut self,
        registry: &mut Registry,
        kind: WalkKind,
        source: Source,
        end: Bytes,
    ) -> Result<u64, WalkError> {
        let id = registry.next_walk_id();
        if id == u64::MAX {
            return Err(WalkError::Exhausted);
        }
        let on_unknown = kind.on_unknown() as u32;
        let walk = Walk {
            id,
            kind,
            source: source as u32,
            end,
            cursor: None,
            params: Bytes::new(),
            members: Vec::new(),
            on_unknown,
        };
        if !walk.is_supported() {
            return Err(WalkError::UnsupportedKind(walk.kind));
        }
        self.walks.push(walk);
        if let Err(error) = self.validate(id + 1) {
            self.walks.pop();
            return Err(error);
        }
        registry.issue_walk_id();
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
        let mut registry = Registry::default();
        let mut walks = Walks::default();
        let start = |walks: &mut Walks, registry: &mut Registry| {
            walks.start(
                registry,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::from_static(b"e"),
            )
        };
        let first = start(&mut walks, &mut registry).unwrap();
        let second = start(&mut walks, &mut registry).unwrap();
        assert_eq!((first, second, registry.next_walk_id()), (1, 2, 3));
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

        // A walk round trips through its record, under its own ID only.
        let walk = walks.get(second).unwrap();
        assert_eq!(Walk::decode(second, &walk.encode()).unwrap(), *walk);
        assert_eq!(
            Walk::decode(first, &walk.encode()),
            Err(WalkError::IdMismatch {
                key: first,
                record: second
            })
        );

        // Removing every walk keeps the counter, which lives in the registry.
        walks.remove(first).unwrap();
        walks.remove(second).unwrap();
        assert!(walks.entries().is_empty());
        let mut reloaded = Walks::new(Vec::new(), registry.next_walk_id()).unwrap();
        assert_eq!(start(&mut reloaded, &mut registry).unwrap(), 3);

        let corrupt = |change: fn(&mut Vec<Walk>)| {
            let mut walks = reloaded.walks.clone();
            change(&mut walks);
            Walks::new(walks, 4)
        };
        assert!(corrupt(|walks| walks[0].id = 4).is_err(), "not issued");
        assert!(corrupt(|walks| walks[0].id = 0).is_err(), "zero");
        assert!(corrupt(|walks| walks.push(walks[0].clone())).is_err());
        assert!(corrupt(|walks| walks[0].params = Bytes::from_static(b"x")).is_err());
        assert!(corrupt(|walks| walks[0].cursor = Some(Bytes::from_static(b"z"))).is_err());
        fn member(joined_at: Option<&'static [u8]>) -> WalkMember {
            WalkMember {
                index: 64,
                joined_at: joined_at.map(Bytes::from_static),
                uncovered: 1,
            }
        }
        assert!(
            corrupt(|walks| walks[0].members = vec![member(None), member(None)]).is_err(),
            "a member listed twice"
        );
        assert!(
            corrupt(|walks| walks[0].members = vec![member(Some(b"a"))]).is_err(),
            "a late join before any cursor"
        );

        // A walk of a kind or source this binary doesn't know loads with its
        // params and positions unchecked. Its policy decides: a discardable one
        // is removed with its ID kept issued; any refusing one refuses them all.
        let unknown = |on_unknown: OnUnknown| {
            let mut walks = reloaded.walks.clone();
            let walk = &mut walks[0];
            (walk.source, walk.params, walk.on_unknown) =
                (99, Bytes::from_static(b"opaque"), on_unknown as u32);
            walk.cursor = Some(Bytes::from_static(b"zz"));
            Walks::new(walks, 4).unwrap()
        };
        // Evidence of a member that left goes; the others keep theirs.
        let mut pruned = reloaded.clone();
        pruned.add_uncovered(3, 64, 2).unwrap();
        pruned.add_uncovered(3, 65, 1).unwrap();
        pruned.retain_members(|_, index| index == 64);
        assert_eq!(
            (
                pruned.get(3).unwrap().uncovered(64),
                pruned.get(3).unwrap().member(65)
            ),
            (2, None)
        );

        let mut discardable = unknown(OnUnknown::Discard);
        discardable.preflight().unwrap();
        assert_eq!(discardable.discard_unknown(), [3]);
        assert!(discardable.entries().is_empty());
        assert!(matches!(
            unknown(OnUnknown::Refuse).preflight(),
            Err(WalkError::Refused {
                id: 3,
                table: 99,
                ..
            })
        ));
        let mut future_policy = reloaded.walks.clone();
        future_policy[0].kind.version = 2;
        future_policy[0].on_unknown = 7;
        assert!(
            Walks::new(future_policy, 4).unwrap().preflight().is_err(),
            "a policy this binary doesn't know refuses"
        );
        // `start` rejects what loading would reject, and reserves no ID for it.
        let (before, counter) = (reloaded.clone(), registry.clone());
        assert!(matches!(
            reloaded.start(
                &mut registry,
                WalkKind {
                    version: 2,
                    ..WalkKind::index_backfill()
                },
                Source::EntryStatus,
                Bytes::new(),
            ),
            Err(WalkError::UnsupportedKind(_))
        ));
        assert_eq!(
            (reloaded, registry),
            (before, counter),
            "a rejected start changes nothing"
        );
    }
}
