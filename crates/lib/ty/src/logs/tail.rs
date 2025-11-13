// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use super::{Lsn, SequenceNumber};

/// Represents the state of the tail of the loglet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TailState<Offset = Lsn> {
    /// Loglet is open for appends
    Open(Offset),
    /// Loglet is sealed. This offset if the durable tail.
    Sealed(Offset),
}

/// "(S)" denotes that tail is sealed
impl<O: Display> Display for TailState<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(n) => write!(f, "{n}"),
            Self::Sealed(n) => write!(f, "{n} (S)"),
        }
    }
}

impl<Offset: SequenceNumber> TailState<Offset> {
    pub fn new(sealed: bool, offset: Offset) -> Self {
        if sealed {
            TailState::Sealed(offset)
        } else {
            TailState::Open(offset)
        }
    }

    /// Combines two TailStates together
    ///
    /// Only applies updates to the value according to the following rules:
    ///   - Offsets can only move forward.
    ///   - Tail cannot be unsealed once sealed.
    ///
    /// Returns true if the state was updated
    pub fn combine(&mut self, sealed: bool, offset: Offset) -> bool {
        let old_offset = self.offset();
        let is_already_sealed = self.is_sealed();

        let new_offset = std::cmp::max(self.offset(), offset);
        let new_sealed = self.is_sealed() || sealed;
        if new_sealed != is_already_sealed || new_offset > old_offset {
            *self = TailState::new(new_sealed, new_offset);
            true
        } else {
            false
        }
    }

    /// Applies a seal on the tail state without changing the tail offset
    /// Returns true if the state was updated
    pub fn seal(&mut self) -> bool {
        if self.is_sealed() {
            false
        } else {
            *self = TailState::new(true, self.offset());
            true
        }
    }
}

impl<Offset: SequenceNumber> TailState<Offset> {
    pub fn map<F, T>(self, f: F) -> TailState<T>
    where
        F: FnOnce(Offset) -> T,
    {
        match self {
            TailState::Open(offset) => TailState::Open(f(offset)),
            TailState::Sealed(offset) => TailState::Sealed(f(offset)),
        }
    }

    #[inline(always)]
    pub fn is_sealed(&self) -> bool {
        matches!(self, TailState::Sealed(_))
    }

    #[inline(always)]
    pub fn offset(&self) -> Offset {
        match self {
            TailState::Open(offset) | TailState::Sealed(offset) => *offset,
        }
    }
}
