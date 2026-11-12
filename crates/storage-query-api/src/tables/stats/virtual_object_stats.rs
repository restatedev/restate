// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;
use strum::VariantArray;

use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Current virtual-object entry counts, including external state mutations.
    /// Separates scope, object key, handler, entry kind, and routing partition key.
    /// Workflow and unkeyed service entries are excluded.
    virtual_object_stats {
        name: "sys_virtual_object_stats",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey },
        columns {
            /// The service name.
            #[starts_with]
            service_name: ReString => DataType::LargeUtf8,
            /// The scope, or NULL for an unscoped object.
            #[starts_with]
            scope: Option<ReString> => DataType::LargeUtf8,
            /// The virtual-object key.
            #[starts_with]
            key: ReString => DataType::LargeUtf8,
            /// The handler name, or NULL for an external state mutation (not a rollup).
            #[starts_with]
            handler: Option<ReString> => DataType::LargeUtf8,
            /// The kind of the entry or operation.
            #[values([EntryKind::Invocation, EntryKind::StateMutation])]
            kind: EntryKind => DataType::LargeUtf8,
            /// VQueue stage, including unknown and finished entries.
            #[values(Stage::VARIANTS.iter().copied())]
            stage: Stage => DataType::LargeUtf8,
            /// Entry processing status, including unknown and terminal statuses.
            #[values(Status::VARIANTS.iter().copied())]
            status: Status => DataType::LargeUtf8,
            /// Count in this partition-local stage/status bucket.
            num_entries: u64 => DataType::UInt64,
            /// The entry's routing partition key, which may come from a custom queue.
            partition_key: PartitionKey => DataType::UInt64,
        }
    }
}
