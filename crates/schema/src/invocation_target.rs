// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use restate_schema_api::invocation_target::{InvocationTargetMetadata, InvocationTargetResolver};

impl InvocationTargetResolver for SchemaRegistry {
    fn resolve_latest_invocation_target(
        &self,
        component_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<InvocationTargetMetadata> {
        self.use_component_schema(component_name.as_ref(), |component_schemas| {
            component_schemas
                .handlers
                .get(handler_name.as_ref())
                .map(|handler_schemas| handler_schemas.target_meta.clone())
        })
        .flatten()
    }
}

impl InvocationTargetResolver for SchemaView {
    fn resolve_latest_invocation_target(
        &self,
        component_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<InvocationTargetMetadata> {
        self.0
            .load()
            .resolve_latest_invocation_target(component_name, handler_name)
    }
}
