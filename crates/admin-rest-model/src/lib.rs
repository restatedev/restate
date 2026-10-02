// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod deployments;
pub mod handlers;
pub mod invocations;
pub mod kafka_clusters;
pub mod query;
pub mod rules;
pub mod services;
pub mod subscriptions;
pub mod version;

/// `$ref` location of `T`'s component schema, for `#[schema(discriminator(mapping(...)))]`
/// entries. Deriving it from the schema name keeps the mapping in sync with the referenced type.
#[cfg(feature = "schema")]
pub(crate) fn schema_ref<T: utoipa::ToSchema>() -> String {
    utoipa::openapi::Ref::from_schema_name(T::name()).ref_location
}
