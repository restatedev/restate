// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;
use serde_with::serde_as;

/// Response header carrying a JSON array of `{"origin": .., "message": ..}` naming the
/// scans the server skipped while answering a query, e.g.
/// `[{"origin":"partition 22","message":"partition 22 is currently unavailable: ..."}]`.
///
/// Present only when something was skipped, in which case the rows are **incomplete**:
/// aggregates are lower bounds and a lookup may return nothing for a record that exists.
/// JSON responses carry the same list under the body's `warnings` key, which — unlike this
/// header — also covers partitions that became unavailable after the response started.
pub const QUERY_WARNINGS_HEADER: &str = "x-restate-query-warnings";

#[serde_as]
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct QueryRequest {
    /// SQL query to run against the storage
    pub query: String,
}
