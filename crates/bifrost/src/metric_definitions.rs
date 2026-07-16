// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_histogram};

pub(crate) const BIFROST_READ_STREAM_STATE_DURATION: &str =
    "restate.bifrost.read_stream_state_duration.seconds";

pub(crate) fn describe_metrics() {
    describe_histogram!(
        BIFROST_READ_STREAM_STATE_DURATION,
        Unit::Seconds,
        "Time spent in Bifrost read stream states"
    );
}
