// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::live::Pinned;
use crate::schema::Schema;
use crate::schema::registry::SchemaRegistryError;

pub trait MetadataService {
    fn get(&self) -> Pinned<Schema>;

    fn update<T: Send, F>(
        &self,
        modify: F,
    ) -> impl Future<Output = Result<(T, Arc<Schema>), SchemaRegistryError>> + Send
    where
        F: (Fn(Schema) -> Result<(T, Schema), SchemaRegistryError>) + Send + Sync;
}
