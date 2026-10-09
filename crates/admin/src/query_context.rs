// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::HeaderMap;

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
