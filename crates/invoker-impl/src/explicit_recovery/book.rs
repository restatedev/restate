// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The bookkeeping of one attempt: result ids, effects proposed and applied, results owed,
//! external results outstanding, notifications sent and forwards received. Pure.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytestring::ByteString;

use restate_service_protocol_explicit_recovery::proto::result_notification_message::Result as NotificationResult;

/// A protocol violation by the SDK.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Violation(pub String);

impl Violation {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

pub(crate) struct AttemptBook {
    /// `StartMessage.next_result_id`: no id of this attempt is below it.
    floor: u32,
    /// The highest result id used so far, 0 if none.
    last_result_id: u32,
    /// The seq of the next proposed effect.
    next_seq: u64,
    /// Every effect up to this seq is applied.
    applied_seq: u64,
    /// Internal results owed once their effect is applied, by the effect's seq.
    pending: BTreeMap<u64, Vec<(u32, NotificationResult)>>,
    /// Outstanding external results: call results and attaches.
    external: HashSet<u32>,
    /// Signal reads not satisfied yet.
    signal_waiters: HashMap<(ByteString, u32), Vec<u32>>,
    /// Notifications written to the stream: results and the cancel signal.
    pub(crate) sent: u32,
    /// Forwards taken from the channel, whatever happened to them (D34).
    pub(crate) received_forwards: u32,
    /// The attempt knows of the cancellation (D60).
    cancel_known: bool,
    /// Span ids started in this attempt.
    spans: HashSet<u32>,
}

impl AttemptBook {
    /// `first_seq` is the seq of the first effect the book proposes: effects proposed before
    /// the book exists (the attempt start) took the seqs below it.
    pub(crate) fn new(floor: u32, cancelled: bool, first_seq: u64) -> Self {
        Self {
            floor,
            last_result_id: 0,
            next_seq: first_seq,
            applied_seq: 0,
            pending: Default::default(),
            external: Default::default(),
            signal_waiters: Default::default(),
            sent: 0,
            received_forwards: 0,
            cancel_known: cancelled,
            spans: Default::default(),
        }
    }

    /// Checks a result id used after every id before it, in the order of the wire. 0 is absent.
    pub(crate) fn use_id(&mut self, id: u32) -> Result<(), Violation> {
        if id == 0 {
            return Ok(());
        }
        self.check_id(id)?;
        self.last_result_id = id;
        Ok(())
    }

    /// Checks the two ids of a `Call`, which come in either order.
    pub(crate) fn use_id_pair(&mut self, a: u32, b: u32) -> Result<(), Violation> {
        for id in [a, b] {
            if id != 0 {
                self.check_id(id)?;
            }
        }
        if a != 0 && a == b {
            return Err(Violation::new(format!("result id {a} used twice")));
        }
        self.last_result_id = self.last_result_id.max(a).max(b);
        Ok(())
    }

    fn check_id(&self, id: u32) -> Result<(), Violation> {
        if id < self.floor {
            return Err(Violation::new(format!(
                "result id {id} is below next_result_id {}",
                self.floor
            )));
        }
        if id <= self.last_result_id {
            return Err(Violation::new(format!(
                "result id {id} is not greater than {}, used before",
                self.last_result_id
            )));
        }
        Ok(())
    }

    /// The highest result id used so far, if any.
    pub(crate) fn last_result_id(&self) -> u32 {
        self.last_result_id
    }

    pub(crate) fn check_parent_span(&self, parent_span_id: u32) -> Result<(), Violation> {
        if parent_span_id != 0 && !self.spans.contains(&parent_span_id) {
            return Err(Violation::new(format!(
                "parent_span_id {parent_span_id} names no span this attempt started"
            )));
        }
        Ok(())
    }

    pub(crate) fn start_span(&mut self, span_id: u32) -> Result<(), Violation> {
        if span_id == 0 {
            return Err(Violation::new("span id 0"));
        }
        if !self.spans.insert(span_id) {
            return Err(Violation::new(format!("span id {span_id} used twice")));
        }
        Ok(())
    }

    pub(crate) fn end_span(&self, span_id: u32) -> Result<(), Violation> {
        if !self.spans.contains(&span_id) {
            return Err(Violation::new(format!(
                "span {span_id} ended, but this attempt did not start it"
            )));
        }
        Ok(())
    }

    /// Proposes an effect: `internal` are owed once it is applied, `external` will arrive later.
    pub(crate) fn propose(
        &mut self,
        internal: Vec<(u32, NotificationResult)>,
        external: impl IntoIterator<Item = u32>,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        if !internal.is_empty() {
            self.pending.insert(seq, internal);
        }
        self.external.extend(external);
        seq
    }

    /// Every effect up to `seq` is applied: returns the results now owed, in order.
    pub(crate) fn applied(&mut self, seq: u64) -> Vec<(u32, NotificationResult)> {
        self.applied_seq = self.applied_seq.max(seq);
        let rest = self.pending.split_off(&(self.applied_seq + 1));
        let due = std::mem::replace(&mut self.pending, rest);
        due.into_values().flatten().collect()
    }

    pub(crate) fn all_applied(&self) -> bool {
        self.applied_seq + 1 >= self.next_seq
    }

    /// The runtime owes the attempt something: an action not applied, or a result not sent.
    pub(crate) fn owes(&self) -> bool {
        !self.all_applied() || !self.pending.is_empty()
    }

    pub(crate) fn has_external(&self) -> bool {
        !self.external.is_empty() || !self.signal_waiters.is_empty()
    }

    pub(crate) fn wait_signal(&mut self, name: ByteString, index: u32, result_id: u32) {
        self.signal_waiters
            .entry((name, index))
            .or_default()
            .push(result_id);
    }

    /// A result forward: true if the attempt waits for it. A result for an id the attempt did
    /// not send, or that already got its result, is dropped (D64).
    pub(crate) fn on_result_forward(&mut self, result_id: u32) -> bool {
        self.received_forwards += 1;
        self.external.remove(&result_id)
    }

    /// A signal forward: the result ids of the reads it satisfies.
    pub(crate) fn on_signal_forward(&mut self, name: &ByteString, index: u32) -> Vec<u32> {
        self.received_forwards += 1;
        self.signal_waiters
            .remove(&(name.clone(), index))
            .unwrap_or_default()
    }

    /// A cancel forward: true if the attempt did not know of the cancellation yet (D60).
    pub(crate) fn on_cancel_forward(&mut self) -> bool {
        self.received_forwards += 1;
        !std::mem::replace(&mut self.cancel_known, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_service_protocol_explicit_recovery::proto::Void;

    fn void() -> NotificationResult {
        NotificationResult::Void(Void {})
    }

    #[test]
    fn result_ids() {
        let mut book = AttemptBook::new(10, false, 2);
        // Below the floor.
        assert!(book.use_id(9).is_err());
        // A call's ids in either order, then a group's members and its own id.
        book.use_id_pair(12, 11).unwrap();
        assert!(book.use_id(12).is_err(), "not greater than the last one");
        book.use_id(13).unwrap();
        book.use_id(0).unwrap();
        book.use_id(15).unwrap();
        assert_eq!(book.last_result_id(), 15);
        assert!(book.use_id_pair(16, 16).is_err());
    }

    #[test]
    fn results_wait_for_applied_and_forwards_are_counted() {
        // The attempt start took seq 1, and is not applied yet: the runtime owes it.
        let mut book = AttemptBook::new(1, true, 2);
        assert!(book.owes());

        let seq_a = book.propose(vec![(1, void())], []);
        let seq_b = book.propose(vec![(2, void())], [3]);
        assert!(book.owes());
        assert!(!book.all_applied());
        assert_eq!(book.applied(seq_a), vec![(1, void())]);
        assert!(!book.all_applied());
        assert_eq!(book.applied(seq_b), vec![(2, void())]);
        assert!(!book.owes());
        assert!(book.has_external());

        // An unknown id is dropped and counted; the awaited one is delivered once.
        assert!(!book.on_result_forward(99));
        assert!(book.on_result_forward(3));
        assert!(!book.on_result_forward(3));
        assert!(!book.has_external());

        // Cancelled at the start: the cancel forward is counted, and not delivered.
        assert!(!book.on_cancel_forward());

        book.wait_signal("s".into(), 0, 4);
        assert!(book.has_external());
        assert_eq!(book.on_signal_forward(&"s".into(), 1), Vec::<u32>::new());
        assert_eq!(book.on_signal_forward(&"s".into(), 0), vec![4]);
        assert_eq!(book.received_forwards, 6);
    }

    #[test]
    fn spans() {
        let mut book = AttemptBook::new(1, false, 2);
        assert!(book.start_span(0).is_err());
        book.start_span(1).unwrap();
        assert!(book.start_span(1).is_err());
        book.check_parent_span(1).unwrap();
        assert!(book.check_parent_span(2).is_err());
        book.end_span(1).unwrap();
        assert!(book.end_span(2).is_err());
    }
}
