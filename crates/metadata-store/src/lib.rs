// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(clippy::perf)]
#![warn(
    clippy::large_futures,
    clippy::large_types_passed_by_value,
    clippy::use_debug,
    clippy::mutex_atomic
)]

mod metadata_store;
mod metric_definitions;

pub mod protobuf;
pub use metadata_store::*;

#[cfg(feature = "test-util")]
mod test_util;
