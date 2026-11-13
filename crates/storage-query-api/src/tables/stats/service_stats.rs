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
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Current VQueue entry counts by service, handler, and entry kind, across scopes.
    /// Includes service, virtual-object, and workflow invocations and state mutations.
    service_stats {
        name: "sys_service_stats",
        sharding: ShardingScheme::ByPartitionId { is_leader_required: false },
        // Rows aggregate a partition's entries; no column identifies the partition.
        sharding_columns {},
        columns {
            /// The service name.
            #[starts_with]
            service_name: ReString => DataType::LargeUtf8,
            /// The handler name, or NULL for an external state mutation (not a rollup).
            #[starts_with]
            handler: Option<ReString> => DataType::LargeUtf8,
            /// The kind of the entry or operation.
            #[values([EntryKind::Invocation, EntryKind::StateMutation])]
            kind: EntryKind => DataType::LargeUtf8,
            /// VQueue stage, including unknown, inbox, running, suspended, paused, and finished.
            #[values(Stage::VARIANTS.iter().copied())]
            stage: Stage => DataType::LargeUtf8,
            /// Entry processing status, including unknown and terminal statuses.
            #[values(Status::VARIANTS.iter().copied())]
            status: Status => DataType::LargeUtf8,
            /// Number of entries. Partition-local contributions are summed by the public SQL view.
            num_entries: u64 => DataType::UInt64,
        }
    }
}
