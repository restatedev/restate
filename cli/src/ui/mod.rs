// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;
use chrono_humanize::{Accuracy, Tense};

pub mod component_methods;
pub mod console;
pub mod deployments;
pub mod invocations;
pub mod output;
pub mod stylesheet;
pub mod watcher;

pub fn duration_to_human_precise(duration: Duration, tense: Tense) -> String {
    let duration = chrono_humanize::HumanTime::from(
        Duration::try_milliseconds(duration.num_milliseconds()).expect("valid milliseconds"),
    );
    duration.to_text_en(Accuracy::Precise, tense)
}

pub fn duration_to_human_rough(duration: Duration, tense: Tense) -> String {
    let duration = chrono_humanize::HumanTime::from(duration);
    duration.to_text_en(Accuracy::Rough, tense)
}
