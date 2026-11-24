// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The statistics tables read registered status-gauge instances, which a replica
//! admits only once they are Complete. Their sources carry `.v2` identities, distinct
//! from the table names that sources had before the registry: a worker without
//! that admission does not know them and fails the work instead of reading its
//! own gauges, and a worker with it rejects work from an older coordinator.

mod aggregated_stat_table;
pub(crate) mod deployment_stats;
pub(crate) mod service_stats;
pub(crate) mod virtual_object_stats;

#[cfg(test)]
pub(crate) mod tests;
