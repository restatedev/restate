// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reads of the two status indexes, `entry_by_status_created/1` and
//! `entry_by_status_transitioned/1`, one **lane** per stage and status, merged
//! back into the order of their time:
//!
//! ```text
//! lane (Finished, Failed):    t8 t3    ─┐
//! lane (Finished, Killed):    t9       ─┼─▶  t9 t8 t5 t3 …
//! lane (Finished, Cancelled): t5       ─┘
//! ```
//!
//! Both indexes lead with stage, status, hour and service, so within a lane the
//! stage and status are fixed and the lane is an ordinary hour-bucketed
//! [`OrderedRuns`]. Lanes merge by the key bytes after those four fields, the
//! time and then the entry ID, the same order each lane yields.
//!
//! One probe finds which candidate lanes have keys, on one iterator, and only
//! those open. The probe and the lanes share one read's limits while they
//! open: one key counter against `max_discovery_keys`, and `max_runs` runs for
//! the lanes' first hours together. Past either, the caller reads another
//! way, before returning anything. A later hour of a lane merges all of its
//! runs, as any bucketed read does. After opening, each step advances at most
//! one lane, and every lane has its next key before the read hands out the
//! earliest.

use std::sync::Arc;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::scan::{CursorStep, ReadDirection};

use crate::invocation_status_table::entry_index_read::Held;
use crate::invocation_status_table::entry_index_read::SharedMemory;
use crate::keys::codecs::{StageRankCodec, StatusCodec};
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeySelection, OrderedKeySchema};
use crate::keys::{FieldCodec, KeyDecoder};
use crate::ordered_runs::{Opening, OrderedRuns, OrderedRunsDiscovery, OrderedRunsLimits};
use crate::table::PhysicalTable;
use crate::{ReadSnapshot, Result};

/// A lane's run is the entries of one hour and service in its stage and status;
/// the stage, status and hour are its bucket.
const LANE_RUN_FIELDS: usize = 4;
const LANE_BUCKET_FIELDS: usize = 3;

/// The selections of a status index read: each candidate lane's, in key order,
/// and a probe of all of them at once.
pub(crate) struct StatusLanes<K> {
    probe: Arc<KeySelection<K>>,
    lanes: Vec<(Stage, Status, Arc<KeySelection<K>>)>,
}

impl<K> StatusLanes<K> {
    /// The lanes `pairs`, in key order, each selected by `prepare` given its
    /// stage and status; the probe by `prepare` given all of them.
    pub(crate) fn new(
        pairs: impl IntoIterator<Item = (Stage, Status)>,
        prepare: impl Fn(&[Stage], &[Status]) -> Result<KeySelection<K>>,
    ) -> Result<Self> {
        let pairs = pairs.into_iter().collect::<Vec<_>>();
        let (mut stages, mut statuses) = (Vec::new(), Vec::new());
        for (stage, status) in &pairs {
            if !stages.contains(stage) {
                stages.push(*stage);
            }
            if !statuses.contains(status) {
                statuses.push(*status);
            }
        }
        let probe = Arc::new(prepare(&stages, &statuses)?);
        let lanes = pairs
            .into_iter()
            .map(|(stage, status)| Ok((stage, status, Arc::new(prepare(&[stage], &[status])?))))
            .collect::<Result<_>>()?;
        Ok(Self { probe, lanes })
    }

    /// The bytes its selections hold.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.probe.retained_bytes()
            + self.lanes.capacity() * size_of::<(Stage, Status, Arc<()>)>()
            + self
                .lanes
                .iter()
                .map(|(.., lane)| lane.retained_bytes())
                .sum::<usize>()
    }
}

/// The lanes of a status index with keys, merged into the order of its time.
pub(crate) struct Lanes<T: PhysicalTable> {
    pub(crate) lanes: Vec<Lane<T>>,
    /// The lanes to advance before the read hands out a key, the next last:
    /// every lane at first, then the lane of each key handed out.
    pub(crate) unprimed: Vec<usize>,
    /// The lane of the key the last step produced.
    current: Option<usize>,
    /// Keys the probe and the lanes' discovery examined while the read opened.
    opening: u64,
    direction: ReadDirection,
}

pub(crate) struct Lane<T: PhysicalTable> {
    runs: OrderedRuns<T>,
    /// The keys `runs` had examined once it opened, counted in
    /// [`Lanes::opening`].
    opened: u64,
    /// While the lane has a key the read hasn't handed out: where the key's
    /// merge order starts in its payload, after the run's fields.
    head: Option<usize>,
}

impl<T> Lanes<T>
where
    T: PhysicalTable,
    T::Key: OrderedKeySchema,
{
    /// Opens the lanes of `prepared` with keys on `snapshot`, within one read's
    /// `limits`, or `None` past them; then nothing of the lanes is held.
    /// `held` is charged for what the lanes need besides their own merges,
    /// `memory` for those, and `discovered` counts the keys they examine.
    pub(crate) fn open(
        snapshot: &ReadSnapshot,
        prepared: &StatusLanes<T::Key>,
        direction: ReadDirection,
        limits: OrderedRunsLimits,
        (memory, held): (&SharedMemory, &mut Held),
        discovered: &mut u64,
    ) -> Result<Option<Self>> {
        // The lanes with keys, in key order, charged while the read holds them.
        let Some((prefixes, charged)) = snapshot.distinct_prefixes::<T>(
            Arc::clone(&prepared.probe),
            2,
            (prepared.lanes.len(), limits.max_discovery_keys),
            Box::new(memory.clone()),
            discovered,
        )?
        else {
            return Ok(None);
        };
        held.bytes += charged;
        // The lanes, and the order to prime them in.
        held.grow(prefixes.len() * (size_of::<Lane<T>>() + size_of::<usize>()))?;
        let header = <<T::Key as KeySchema>::Header as KeyHeader>::LENGTH;
        let mut open = Vec::with_capacity(prefixes.len());
        // The runs the lanes' first hours may still have.
        let mut available = limits.max_runs;
        let lane_limits = OrderedRunsLimits {
            bucket_fields: LANE_BUCKET_FIELDS,
            ..limits
        };
        for prefix in &prefixes {
            let mut fields = prefix
                .get(header..)
                .ok_or(StorageError::DataIntegrityError)?;
            let stage = StageRankCodec::decode_encoded(StageRankCodec::take_encoded(&mut fields)?)?;
            let status = StatusCodec::decode_encoded(StatusCodec::take_encoded(&mut fields)?)?;
            let (.., selection) = prepared
                .lanes
                .iter()
                .find(|(s, t, _)| (*s, *t) == (stage, status))
                .ok_or(StorageError::DataIntegrityError)?;
            let opening = Opening {
                runs: available,
                examined: discovered,
            };
            match snapshot.ordered_runs_within::<T>(
                Arc::clone(selection),
                LANE_RUN_FIELDS,
                direction,
                lane_limits,
                Box::new(memory.clone()),
                opening,
            )? {
                OrderedRunsDiscovery::Ready(runs) => {
                    let opened = runs.examined();
                    available = available.saturating_sub(runs.runs());
                    if runs.runs() > 0 {
                        open.push(Lane {
                            runs,
                            opened,
                            head: None,
                        });
                    }
                }
                OrderedRunsDiscovery::TooManyRuns { .. } => return Ok(None),
            }
        }
        // The probe's prefixes are done with, and lanes without runs need no
        // place in the priming order.
        held.shrink(charged + (prefixes.len() - open.len()) * size_of::<usize>());
        drop(prefixes);
        let mut unprimed = Vec::with_capacity(open.len());
        unprimed.extend((0..open.len()).rev());
        Ok(Some(Self {
            lanes: open,
            unprimed,
            current: None,
            opening: *discovered,
            direction,
        }))
    }

    /// One step: advances the next lane without a key, if any, and hands out
    /// the earliest key once every lane has one or is exhausted.
    pub(crate) fn advance(&mut self) -> Result<CursorStep> {
        self.current = None;
        if let Some(&at) = self.unprimed.last() {
            let lane = &mut self.lanes[at];
            match lane.runs.advance()? {
                CursorStep::Skipped => return Ok(CursorStep::Skipped),
                CursorStep::Record => {
                    let payload = lane.runs.current()?.0.remaining;
                    let mut rest = payload;
                    for field in &<T::Key as OrderedKeySchema>::FIELDS[..LANE_RUN_FIELDS] {
                        field.take_encoded(&mut rest)?;
                    }
                    lane.head = Some(payload.len() - rest.len());
                }
                CursorStep::Done => {}
            }
            self.unprimed.pop();
            if !self.unprimed.is_empty() {
                return Ok(CursorStep::Skipped);
            }
        }
        let mut earliest: Option<(usize, &[u8])> = None;
        for (at, lane) in self.lanes.iter().enumerate() {
            let Some(start) = lane.head else {
                continue;
            };
            let order = &lane.runs.current()?.0.remaining[start..];
            let earlier = earliest.is_none_or(|(_, other)| match self.direction {
                ReadDirection::Forward => order < other,
                ReadDirection::Backward => order > other,
            });
            if earlier {
                earliest = Some((at, order));
            }
        }
        let Some(at) = earliest.map(|(at, _)| at) else {
            return Ok(CursorStep::Done);
        };
        self.lanes[at].head = None;
        self.unprimed.push(at);
        self.current = Some(at);
        Ok(CursorStep::Record)
    }

    /// The key the last step produced, and its value.
    pub(crate) fn current(&self) -> Result<(KeyDecoder<'_, T::Key>, &[u8])> {
        let at = self.current.ok_or(StorageError::DataIntegrityError)?;
        self.lanes[at].runs.current()
    }

    /// Keys examined so far, the probe's included.
    pub(crate) fn examined(&self) -> u64 {
        self.opening
            + self
                .lanes
                .iter()
                .map(|lane| lane.runs.examined() - lane.opened)
                .sum::<u64>()
    }

    /// Runs the lanes merge now.
    pub(crate) fn runs(&self) -> usize {
        self.lanes.iter().map(|lane| lane.runs.runs()).sum()
    }

    /// The bytes the lanes' merges charge.
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.lanes
            .iter()
            .map(|lane| lane.runs.retained_bytes())
            .sum()
    }
}
