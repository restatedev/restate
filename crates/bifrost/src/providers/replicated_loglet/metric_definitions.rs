// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use metrics::{Unit, describe_counter, describe_histogram};

pub(crate) const BIFROST_REPLICATED_READ_CACHE_HIT: &str =
    "restate.bifrost.replicatedloglet.read_record_cache_hit.total";
pub(crate) const BIFROST_REPLICATED_READ_CACHE_FILTERED: &str =
    "restate.bifrost.replicatedloglet.read_record_cache_filtered.total";
pub(crate) const BIFROST_REPLICATED_READ_TOTAL: &str =
    "restate.bifrost.replicatedloglet.read_record.total";
pub(crate) const BIFROST_RECORDS_ENQUEUED_TOTAL: &str =
    "restate.bifrost.replicatedloglet.enqueued_records.total";
pub(crate) const BIFROST_RECORDS_ENQUEUED_BYTES: &str =
    "restate.bifrost.replicatedloglet.enqueued_records.bytes.total";

pub(crate) const BIFROST_SEQ_RECORDS_COMMITTED_TOTAL: &str =
    "restate.bifrost.sequencer.committed_records.total";
pub(crate) const BIFROST_SEQ_RECORDS_COMMITTED_BYTES: &str =
    "restate.bifrost.sequencer.committed_records.bytes.total";
pub(crate) const BIFROST_SEQ_STORE_DURATION: &str =
    "restate.bifrost.sequencer.store_duration.seconds";
pub(crate) const BIFROST_SEQ_APPEND_DURATION: &str =
    "restate.bifrost.sequencer.append_duration.seconds";

pub(crate) fn describe_metrics() {
    describe_counter!(
        BIFROST_REPLICATED_READ_CACHE_HIT,
        Unit::Count,
        "Number of records read from RecordCache"
    );

    describe_counter!(
        BIFROST_REPLICATED_READ_CACHE_FILTERED,
        Unit::Count,
        "Number of records filtered out while reading from RecordCache"
    );

    describe_counter!(
        BIFROST_REPLICATED_READ_TOTAL,
        Unit::Count,
        "Number of records read"
    );

    describe_counter!(
        BIFROST_RECORDS_ENQUEUED_TOTAL,
        Unit::Count,
        "Number of records enqueued for writing"
    );

    describe_counter!(
        BIFROST_RECORDS_ENQUEUED_BYTES,
        Unit::Bytes,
        "Size of records enqueued for writing"
    );

    describe_counter!(
        BIFROST_SEQ_RECORDS_COMMITTED_TOTAL,
        Unit::Count,
        "Number of records committed"
    );

    describe_counter!(
        BIFROST_SEQ_RECORDS_COMMITTED_BYTES,
        Unit::Bytes,
        "Size of records committed"
    );

    describe_histogram!(
        BIFROST_SEQ_APPEND_DURATION,
        Unit::Seconds,
        "Append batch duration in seconds as measured by the sequencer"
    );

    describe_histogram!(
        BIFROST_SEQ_STORE_DURATION,
        Unit::Seconds,
        "Log server store duration in seconds as measured by the sequencer"
    );
}
