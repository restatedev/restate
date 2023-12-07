// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A set of common queries needed by the CLI

use super::DataFusionHttpClient;

use restate_meta_rest_model::endpoints::EndpointId;

use anyhow::Result;

pub async fn count_endpoint_active_inv(
    client: &DataFusionHttpClient,
    endpoint_id: &EndpointId,
) -> Result<i64> {
    Ok(client
        .run_count_query(format!(
            "SELECT COUNT(id) AS inv_count \
            FROM sys_status \
            WHERE pinned_endpoint_id = '{}' \
            GROUP BY pinned_endpoint_id",
            endpoint_id
        ))
        .await?)
}
