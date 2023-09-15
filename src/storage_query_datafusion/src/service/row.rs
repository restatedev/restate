// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::service::schema::ServiceBuilder;
use restate_schema_api::service::ServiceMetadata;

#[inline]
pub(crate) fn append_service_row(builder: &mut ServiceBuilder, service_row: ServiceMetadata) {
    let ServiceMetadata {
        name,
        methods,
        instance_type,
        endpoint_id,
        revision,
        public,
    } = service_row;

    let mut row = builder.row();
    row.name(name);
    row.methods(methods.into_iter().map(Some));
    row.service_type(format!("{:?}", instance_type));
    row.endpoint_id(endpoint_id);
    row.revision(revision);
    row.public(public);
}
