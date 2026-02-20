// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Memory management utilities for Restate.
//!
//! This crate provides:
//! - [`MemoryPool`]: A named memory budget for bounding memory usage
//! - [`MemoryLease`]: RAII guard for memory leases that can be passed through channels
//! - [`EstimatedMemorySize`]: Trait for types that can estimate their memory
//!   footprint

mod controller;
mod footprint;
pub mod local_pool;
mod metric_definitions;
mod pool;

pub use controller::MemoryController;
pub use local_pool::{
    AvailabilityNotified, InvocationMemory, LocalMemoryLease, LocalMemoryPool, OutOfMemory,
};
pub use pool::{MemoryLease, MemoryPool};
pub use restate_serde_util::NonZeroByteCount;

pub use footprint::*;
