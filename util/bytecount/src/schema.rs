// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ByteCount;

impl schemars::JsonSchema for ByteCount<true> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "HumanBytes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(std::concat!(std::module_path!(), "::", "HumanBytes").to_owned())
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": r"^\d+(\.\d+)? ?[KMG]B$",
            "minLength": 1,
            "title": "Human-readable bytes",
            "description": "Human-readable bytes",
        })
    }
}

impl schemars::JsonSchema for ByteCount<false> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NonZeroHumanBytes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(
            std::concat!(std::module_path!(), "::", "NonZeroHumanBytes").to_owned(),
        )
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": r"^\d+(\.\d+)? ?[KMG]B$",
            "minLength": 1,
            "title": "Non-zero human-readable bytes",
            "description": "Non-zero human-readable bytes",
        })
    }
}
