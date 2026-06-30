// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge};

pub(crate) const DATAFUSION_NUM_ACTIVE_SCANNERS: &str = "restate.datafusion.num_active_scanners";
pub(crate) const DATAFUSION_NUM_SCANNERS: &str = "restate.datafusion.num_scanners.total";

pub fn describe_metrics() {
    describe_gauge!(
        DATAFUSION_NUM_ACTIVE_SCANNERS,
        Unit::Count,
        "Number of currently active DataFusion scanners"
    );

    describe_counter!(
        DATAFUSION_NUM_SCANNERS,
        Unit::Count,
        "Total number of DataFusion scanners created"
    );
}
