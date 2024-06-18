// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeDelta};
pub use chrono_humanize::{Accuracy, Tense};

use crate::CliContext;

pub mod console;
pub mod output;
pub mod stylesheet;
pub mod watcher;

pub fn timestamp_as_human_duration<Tz: chrono::TimeZone>(ts: DateTime<Tz>, tense: Tense) -> String {
    match CliContext::get().time_format() {
        crate::opts::TimeFormat::Human => {
            let since = ts.signed_duration_since(chrono::Local::now());
            duration_to_human_precise(since, tense)
        }
        crate::opts::TimeFormat::Iso8601 => ts.to_rfc3339(),
        crate::opts::TimeFormat::Rfc2822 => ts.to_rfc2822(),
    }
}

pub fn duration_to_human_precise(duration: TimeDelta, tense: Tense) -> String {
    match CliContext::get().time_format() {
        crate::opts::TimeFormat::Iso8601 => duration.to_string(),
        crate::opts::TimeFormat::Rfc2822 => duration.to_string(),
        crate::opts::TimeFormat::Human => {
            let duration = chrono_humanize::HumanTime::from(
                // truncate nanos
                TimeDelta::try_milliseconds(duration.num_milliseconds())
                    .expect("valid milliseconds"),
            );
            duration.to_text_en(Accuracy::Precise, tense)
        }
    }
}

pub fn duration_to_human_rough(duration: TimeDelta, tense: Tense) -> String {
    match CliContext::get().time_format() {
        crate::opts::TimeFormat::Iso8601 => duration.to_string(),
        crate::opts::TimeFormat::Rfc2822 => duration.to_string(),
        crate::opts::TimeFormat::Human => {
            let duration = chrono_humanize::HumanTime::from(duration);
            duration.to_text_en(Accuracy::Rough, tense)
        }
    }
}
