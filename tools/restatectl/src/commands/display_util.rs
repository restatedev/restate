// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Local};
use restate_cli_util::_comfy_table::{Cell, Color};
use restate_cli_util::ui::{Tense, timestamp_as_human_duration};
use std::time::SystemTime;

pub fn render_as_duration(ts: Option<prost_types::Timestamp>, tense: Tense) -> Cell {
    let ts: Option<SystemTime> = ts
        .map(TryInto::try_into)
        .transpose()
        .expect("valid timestamp");
    if let Some(ts) = ts {
        let ts: DateTime<Local> = ts.into();
        Cell::new(timestamp_as_human_duration(ts, tense))
    } else {
        Cell::new("-").fg(Color::Red)
    }
}
