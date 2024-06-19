// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_table!(sys_invocation_state(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    id: DataType::LargeUtf8,

    /// If true, the invocation is currently in-flight
    in_flight: DataType::Boolean,

    /// The number of attempts since the last successful attempt of this invocation.
    /// Increments on start, so 2 or more means a failure occurred.
    retry_count: DataType::UInt64,

    /// Timestamp indicating the start of the most recent attempt of this invocation.
    last_start_at: DataType::Date64,

    // The deployment that was selected in the last invocation attempt. This is
    // guaranteed to be set unlike in `sys_status` table which require that the
    // deployment to be committed before it is set.

    /// The opaque service deployment ID that was used in the most recent attempt of this
    /// invocation; this will be set before a journal entry is stored, but can change later.
    last_attempt_deployment_id: DataType::LargeUtf8,

    /// Server/SDK version, e.g. `restate-sdk-java/1.0.1`
    last_attempt_server: DataType::LargeUtf8,

    /// Timestamp indicating the start of the next attempt of this invocation.
    next_retry_at: DataType::Date64,

    /// An error message describing the most recent failed attempt of this invocation, if any.
    last_failure: DataType::LargeUtf8,

    /// The error code of the most recent failed attempt of this invocation, if any.
    last_failure_error_code: DataType::LargeUtf8,

    /// The index of the journal entry that caused the failure, if any. It may be out-of-bound
    /// of the currently stored entries in `sys_journal`.
    last_failure_related_entry_index: DataType::UInt64,

    /// The name of the journal entry that caused the failure, if any.
    last_failure_related_entry_name: DataType::LargeUtf8,

    /// The type of the journal entry that caused the failure, if any. You can check all the
    /// available entry types in [`entries.rs`](https://github.com/restatedev/restate/blob/main/crates/types/src/journal/entries.rs).
    last_failure_related_entry_type: DataType::LargeUtf8,
));
