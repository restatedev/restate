// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::time::MillisSinceEpoch;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;

pub trait Timer: Hash + Eq + Borrow<Self::TimerKey> {
    type TimerKey: TimerKey + Send;

    fn timer_key(&self) -> &Self::TimerKey;
}

/// Timer key establishes an absolute order on [`Timer`]. Naturally, this should be the key under
/// which the timer value is stored and can be retrieved.
pub trait TimerKey: Ord + Clone + Hash + Debug {
    fn wake_up_time(&self) -> MillisSinceEpoch;
}
