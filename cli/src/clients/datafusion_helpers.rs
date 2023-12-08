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

use arrow::array::{as_string_array, AsArray};
use restate_meta_rest_model::endpoints::EndpointId;

use anyhow::Result;

pub async fn count_deployment_active_inv(
    client: &DataFusionHttpClient,
    endpoint_id: &EndpointId,
) -> Result<i64> {
    Ok(client
        .run_count_agg_query(format!(
            "SELECT COUNT(id) AS inv_count \
            FROM sys_status \
            WHERE pinned_endpoint_id = '{}' \
            GROUP BY pinned_endpoint_id",
            endpoint_id
        ))
        .await?)
}

pub struct ServiceMethodUsage {
    pub service: String,
    pub method: String,
    pub inv_count: i64,
}

pub async fn count_deployment_active_inv_by_method(
    client: &DataFusionHttpClient,
    endpoint_id: &EndpointId,
) -> Result<Vec<ServiceMethodUsage>> {
    let query = format!(
        "SELECT service, method, COUNT(id) AS inv_count \
            FROM sys_status \
            WHERE pinned_endpoint_id = '{}' \
            GROUP BY pinned_endpoint_id, service, method",
        endpoint_id
    );

    let resp = client.run_query(query).await?;
    let batches = resp.batches;

    let mut output = vec![];
    for batch in batches {
        let col = batch.column(0);
        let services = as_string_array(col);
        let col = batch.column(1);
        let methods = as_string_array(col);
        let col = batch.column(2);
        let inv_counts = col.as_primitive::<arrow::datatypes::Int64Type>();

        for i in 0..batch.num_rows() {
            let service = services.value(i).to_owned();
            let method = methods.value(i).to_owned();
            let inv_count = inv_counts.value(i);
            output.push(ServiceMethodUsage {
                service,
                method,
                inv_count,
            });
        }
    }
    Ok(output)
}
