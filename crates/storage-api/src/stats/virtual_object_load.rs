// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_sharding::PartitionKey;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

crate::define_table! {
    /// Current virtual-object entry counts, including external state mutations.
    /// Separates scope, object key, handler, entry kind, and routing partition key.
    /// Workflow and unkeyed service entries are excluded.
    pub VirtualObjectLoad;
}

crate::define_selector! {
    /// Selects the grouping dimensions of virtual-object entry counts.
    pub VirtualObjectLoad {
        /// The service name.
        service_name: ReString => starts_with,
        /// The virtual-object key.
        key: ReString => starts_with,
        /// The scope, or NULL for an unscoped object.
        scope: Option<ReString> => starts_with,
        /// The handler name, or NULL for an external state mutation (not a rollup).
        handler: Option<ReString> => starts_with,
        /// The kind of the entry or operation.
        kind: EntryKind,
        /// The entry's routing partition key, which may come from a custom queue.
        partition_key: PartitionKey,
    }
}
