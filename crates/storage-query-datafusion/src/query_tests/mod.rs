// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

macro_rules! json_row {
    ($($key:literal: $value:tt),+ $(,)?) => {{
        let mut row = serde_json::Map::new();
        $(row.insert($key.to_owned(), serde_json::json!($value));)+
        serde_json::Value::Object(row)
    }};
}

mod data;
mod fixture;
mod state;
mod sys_invocation;
