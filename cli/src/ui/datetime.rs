// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::SystemTime;

use chrono::{DateTime, Local, TimeZone};
use humantime::Timestamp;

pub trait DateTimeExt {
    /// Human-friendly local rendering (for tables).
    fn display(&self) -> String;
    /// Machine-friendly RFC 3339 / ISO-8601 rendering (for `--json`).
    fn iso(&self) -> String;
}

impl<Tz> DateTimeExt for DateTime<Tz>
where
    Tz: TimeZone,
{
    fn display(&self) -> String {
        let dt: DateTime<Local> = self.with_timezone(&Local);
        dt.format("%a %d %h %Y %X %Z").to_string()
    }

    fn iso(&self) -> String {
        self.with_timezone(&Local).to_rfc3339()
    }
}

impl DateTimeExt for Timestamp {
    fn display(&self) -> String {
        let dt = DateTime::<Local>::from(SystemTime::from(*self));
        dt.display()
    }

    fn iso(&self) -> String {
        DateTime::<Local>::from(SystemTime::from(*self)).to_rfc3339()
    }
}
