// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::ServiceBuilder;

use crate::table_util::format_using;
use restate_types::invocation::ServiceType;
use restate_types::schema::service::ServiceMetadata;

#[inline]
pub(crate) fn append_service_row(
    builder: &mut ServiceBuilder,
    output: &mut String,
    service_metadata: ServiceMetadata,
) {
    let mut row = builder.row();
    row.name(service_metadata.name);
    row.revision(service_metadata.revision as u64);
    row.public(service_metadata.public);
    row.deployment_id(format_using(output, &service_metadata.deployment_id));
    row.ty(match service_metadata.ty {
        ServiceType::Service => "service",
        ServiceType::VirtualObject => "virtual_object",
        ServiceType::Workflow => "workflow",
    })
}
