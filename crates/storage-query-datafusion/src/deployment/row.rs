// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::DeploymentBuilder;
use crate::table_util::format_using;
use restate_schema_api::deployment::{DeploymentMetadata, DeploymentType};

#[inline]
pub(crate) fn append_deployment_row(
    builder: &mut DeploymentBuilder,
    output: &mut String,
    deployment_metadata: DeploymentMetadata,
) {
    let mut row = builder.row();
    row.id(format_using(output, &deployment_metadata.id()));

    match deployment_metadata.ty {
        DeploymentType::Http { .. } => {
            row.ty("http");
        }
        DeploymentType::Lambda { .. } => {
            row.ty("lambda");
        }
    }

    row.endpoint(format_using(output, &deployment_metadata.address_display()));
    row.created_at(deployment_metadata.created_at.as_u64() as i64);
}
