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
use restate_types::schema::deployment::{Deployment, DeploymentType};

#[inline]
pub(crate) fn append_deployment_row(
    builder: &mut DeploymentBuilder,
    output: &mut String,
    deployment: Deployment,
) {
    let mut row = builder.row();
    row.id(format_using(output, &deployment.id));

    match deployment.metadata.ty {
        DeploymentType::Http { .. } => {
            row.ty("http");
        }
        DeploymentType::Lambda { .. } => {
            row.ty("lambda");
        }
    }

    row.endpoint(format_using(output, &deployment.metadata.address_display()));
    row.created_at(deployment.metadata.created_at.as_u64() as i64);
}
