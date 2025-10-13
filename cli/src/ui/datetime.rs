// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
    fn display(&self) -> String;
}

impl<Tz> DateTimeExt for DateTime<Tz>
where
    Tz: TimeZone,
{
    fn display(&self) -> String {
        let dt: DateTime<Local> = self.with_timezone(&Local);
        dt.format("%a %d %h %Y %X %Z").to_string()
    }
}

impl DateTimeExt for Timestamp {
    fn display(&self) -> String {
        let dt = DateTime::<Local>::from(SystemTime::from(*self));
        dt.display()
    }
}
