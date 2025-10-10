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

#[cfg(any(test, feature = "test-util"))]
pub mod mocks {
    use super::*;

    use arc_swap::ArcSwap;

    pub fn mock_arc_schema() -> Arc<ArcSwap<Schema>> {
        Arc::new(ArcSwap::from_pointee(Schema::default()))
    }

    impl MetadataService for Arc<ArcSwap<Schema>> {
        fn get(&self) -> Pinned<Schema> {
            Pinned::new(self)
        }

        fn update<T: Send, F>(
            &self,
            modify: F,
        ) -> impl Future<Output = Result<(T, Arc<Schema>), SchemaRegistryError>> + Send
        where
            F: Fn(Schema) -> Result<(T, Schema), SchemaRegistryError> + Send + Sync,
        {
            let old_schema = self.load().as_ref().clone();

            let update_res = modify(old_schema);
            std::future::ready(match update_res {
                Ok((t, schema)) => {
                    let new_arc = Arc::new(schema);
                    self.store(Arc::clone(&new_arc));
                    Ok((t, new_arc))
                }
                Err(e) => Err(e),
            })
        }
    }
}
