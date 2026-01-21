// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate core;

use std::future::Future;

mod service;

use restate_types::timer::Timer;
pub use service::TimerService;
pub use service::clock::{Clock, TokioClock};

pub trait TimerReader<T>
where
    T: Timer,
{
    /// Gets the next `num_timers` starting with the next timer after `previous_timer_key`.
    ///
    /// # Contract
    /// The returned timers need to follow the order defined by [`restate_types::timer::TimerKey`]. This entails
    /// scan timers must never return a timer whose key is <= `previous_timer_key`
    fn get_timers(
        &mut self,
        num_timers: usize,
        previous_timer_key: Option<T::TimerKey>,
    ) -> impl Future<Output = Vec<T>> + Send;
}
