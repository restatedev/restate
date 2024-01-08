// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate core;

use restate_types::time::MillisSinceEpoch;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;

mod options;
mod service;

pub use options::{Options, OptionsBuilder, OptionsBuilderError};
pub use service::clock::{Clock, TokioClock};
pub use service::TimerService;

pub trait Timer: Hash + Eq + Borrow<Self::TimerKey> {
    type TimerKey: TimerKey + Send;

    fn timer_key(&self) -> &Self::TimerKey;
}

/// Timer key establishes an absolute order on [`Timer`]. Naturally, this should be key under
/// which the timer value is stored and can be retrieved.
pub trait TimerKey: Ord + Clone + Hash + Debug {
    fn wake_up_time(&self) -> MillisSinceEpoch;
}

pub trait TimerReader<T>
where
    T: Timer,
{
    /// Gets the next `num_timers` starting with the next timer after `previous_timer_key`.
    ///
    /// # Contract
    /// The returned timers need to follow the order defined by [`TimerKey`]. This entails
    /// scan timers must never return a timer whose key is <= `previous_timer_key`
    fn get_timers(
        &mut self,
        num_timers: usize,
        previous_timer_key: Option<T::TimerKey>,
    ) -> impl Future<Output = Vec<T>> + Send;
}
