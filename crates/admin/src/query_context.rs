// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{HeaderMap, HeaderValue};

use restate_storage_query_api::QueryEngineVersion;

/// Selects the query engine for one request: `v1` or `v2`. Responses name the
/// engine that served the request in the same header. HTTP and gRPC requests
/// (as metadata) use the same name.
pub(crate) const QUERY_ENGINE_HEADER: &str = "x-restate-query-engine";

/// The engine `headers` select with [`QUERY_ENGINE_HEADER`], or `default` when
/// they don't select one. `None` when the selection is invalid: a value other
/// than `v1` or `v2`, or the header sent more than once.
pub(crate) fn engine_version_from_headers(
    headers: &HeaderMap,
    default: QueryEngineVersion,
) -> Option<QueryEngineVersion> {
    let mut values = headers.get_all(QUERY_ENGINE_HEADER).iter();
    let version = match values.next().map(HeaderValue::as_bytes) {
        None => default,
        Some(b"v1") => QueryEngineVersion::V1,
        Some(b"v2") => QueryEngineVersion::V2,
        _ => return None,
    };
    values.next().is_none().then_some(version)
}

/// Diagnostic request context collected by both HTTP and cluster query gRPC.
const QUERY_CONTEXT_HEADERS: &[&str] = &[
    "x-restate-query-client",
    "x-restate-query-origin",
    "x-restatecloud-user-id",
    "x-restatecloud-environment-id",
    "x-restatecloud-caller-principal",
];

pub(crate) fn collect_query_headers(headers: &HeaderMap) -> HeaderMap {
    let mut collected = HeaderMap::new();
    for &name in QUERY_CONTEXT_HEADERS {
        for value in headers.get_all(name) {
            collected.append(name, value.clone());
        }
    }
    collected
}
