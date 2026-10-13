// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::SysDeploymentBuilder;
use restate_types::schema::deployment::{Deployment, DeploymentType};

#[inline]
pub(crate) fn append_deployment_row(
    builder: &mut SysDeploymentBuilder,
    deployment: Deployment,
    service_names: impl IntoIterator<Item = impl AsRef<str>>,
) {
    let mut row = builder.row();
    row.fmt_id(deployment.id);

    match deployment.ty {
        DeploymentType::Http { .. } => {
            row.ty("http");
        }
        DeploymentType::Lambda { .. } => {
            row.ty("lambda");
        }
    }

    row.fmt_endpoint(deployment.address_display());
    row.created_at(deployment.created_at.as_u64() as i64);
    row.min_service_protocol_version(
        deployment
            .supported_protocol_versions
            .start()
            .unsigned_abs(),
    );
    row.max_service_protocol_version(deployment.supported_protocol_versions.end().unsigned_abs());
    row.services(service_names.into_iter().map(|s| Some(s)));
}
