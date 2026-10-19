// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logical dimensions and filters for current VQueue stage/status counts.
//!
//! Each retained entry contributes to its service, to its pinned deployment when
//! present, and to its virtual object when applicable. Finished entries contribute
//! until deletion. These are current-state gauges, not cumulative transition counts.
//!
//! Filters constrain grouping dimensions; stage/status counters are stored in the
//! value. Storage implementations choose physical key order and aggregation encoding.

pub mod deployment_load;
pub mod service_load;
pub mod virtual_object_load;
