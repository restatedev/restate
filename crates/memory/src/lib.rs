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
//! - [`MemoryBudget`]: A named memory budget for bounding memory usage
//! - [`MemoryLease`]: RAII guard for memory leases that can be passed through
//!   channels
//! - [`EstimatedMemorySize`]: Trait for types that can estimate their memory
//!   footprint

mod budget;
mod footprint;

pub use budget::{MemoryBudget, MemoryLease};
pub use footprint::*;
