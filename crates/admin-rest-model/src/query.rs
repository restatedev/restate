// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct QueryRequest {
    /// SQL query to run against the storage
    pub query: String,
    /// Values for the placeholders in the query. Use an array for positional
    /// placeholders (`$1`, `$2`, ...) or an object for named placeholders (`$name`).
    /// Values must be strings, numbers, booleans or `null`.
    #[serde(default)]
    pub params: Option<QueryParams>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged, expecting = "expected an array or an object")]
pub enum QueryParams {
    Positional(Vec<serde_json::Value>),
    Named(HashMap<String, serde_json::Value>),
}

// The derived schema would allow any JSON value, but only scalars are accepted.
#[cfg(feature = "schema")]
impl utoipa::PartialSchema for QueryParams {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{
            ArrayBuilder, ObjectBuilder, OneOfBuilder, SchemaType, Type,
        };

        let value = || {
            ObjectBuilder::new().schema_type(SchemaType::from_iter([
                Type::String,
                Type::Number,
                Type::Boolean,
                Type::Null,
            ]))
        };
        OneOfBuilder::new()
            .item(ArrayBuilder::new().items(value()))
            .item(
                ObjectBuilder::new()
                    .schema_type(Type::Object)
                    .additional_properties(Some(value())),
            )
            .into()
    }
}

#[cfg(feature = "schema")]
impl utoipa::ToSchema for QueryParams {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("QueryParams")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn deserialize_params() {
        let req: QueryRequest = serde_json::from_str(r#"{"query": "SELECT 1"}"#).unwrap();
        assert!(req.params.is_none());

        let req: QueryRequest =
            serde_json::from_str(r#"{"query": "", "params": [null, 1, "a"]}"#).unwrap();
        let Some(QueryParams::Positional(values)) = req.params else {
            panic!("expected positional params");
        };
        assert_eq!(values, vec![json!(null), json!(1), json!("a")]);

        let req: QueryRequest =
            serde_json::from_str(r#"{"query": "", "params": {"id": "inv_1"}}"#).unwrap();
        let Some(QueryParams::Named(values)) = req.params else {
            panic!("expected named params");
        };
        assert_eq!(values["id"], json!("inv_1"));

        let err = serde_json::from_str::<QueryRequest>(r#"{"query": "", "params": "a"}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected an array or an object"), "{err}");
    }
}
