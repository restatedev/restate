// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Optional to have but adds description/help message to the metrics emitted to
/// the metrics' sink.
use metrics::{describe_counter, Unit};

pub const PARTITION_APPLY_COMMAND: &str = "partition.apply_command.total";
pub const PARTITION_ACTUATOR_HANDLED: &str = "partition.actuator_handled.total";
pub const PARTITION_TIMER_DUE_HANDLED: &str = "partition.timer_due_handled.total";
pub const PARTITION_STORAGE_TX_CREATED: &str = "partition.storage_tx_created.total";
pub const PARTITION_STORAGE_TX_COMMITTED: &str = "partition.storage_tx_committed.total";

pub(crate) fn describe_metrics() {
    describe_counter!(
        PARTITION_APPLY_COMMAND,
        Unit::Count,
        "Total consensus commands processed by partition processor"
    );
    describe_counter!(
        PARTITION_ACTUATOR_HANDLED,
        Unit::Count,
        "Number of actuator operation outputs processed"
    );
    describe_counter!(
        PARTITION_TIMER_DUE_HANDLED,
        Unit::Count,
        "Number of due timer instances processed"
    );
    describe_counter!(
        PARTITION_STORAGE_TX_CREATED,
        Unit::Count,
        "Storage transactions created by from processing state machine commands"
    );
    describe_counter!(
        PARTITION_STORAGE_TX_COMMITTED,
        Unit::Count,
        "Storage transactions committed by applying partition state machine commands"
    );
}
