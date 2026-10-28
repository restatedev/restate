// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Blocking I/O off the async runtime, with shutdown-safe accounting.
//!
//! - [`IoOp`] describes a blocking operation that can run inline or be
//!   prepared as a background [`IoTask`].
//! - [`IoExecutor`] runs tasks on background threads.
//! - [`FlightTracker`] counts unfinished work, so an owner (e.g. a database)
//!   can stop accepting work and wait for what's in flight before closing.
//! - [`TrackedExecutor`] combines the two: every submission is counted.

mod executor;
mod flight_tracker;

pub use executor::{
    AdmissionClosed, IoExecutor, IoOp, IoTask, IoTaskError, Priority, TrackedExecutor, TrackedTask,
};
pub use flight_tracker::{FlightTracker, Lease, OwnedLease};
