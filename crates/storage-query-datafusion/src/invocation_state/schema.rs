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

define_table!(state(
    partition_key: DataType::UInt64,
    id: DataType::LargeUtf8,

    in_flight: DataType::Boolean,
    retry_count: DataType::UInt64,
    last_start_at: DataType::Date64,

    // The deployment that was selected in the last invocation attempt. This is
    // guaranteed to be set unlike in `sys_status` table which require that the
    // deployment to be committed before it is set.
    last_attempt_deployment_id: DataType::LargeUtf8,
    last_attempt_server: DataType::LargeUtf8,
    next_retry_at: DataType::Date64,

    last_failure: DataType::LargeUtf8,
    last_failure_error_code: DataType::LargeUtf8,
    last_failure_related_entry_index: DataType::UInt64,
    last_failure_related_entry_name: DataType::LargeUtf8,
    last_failure_related_entry_type: DataType::LargeUtf8,
));
