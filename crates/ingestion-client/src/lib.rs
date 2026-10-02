// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod chunks_size;
mod client;
mod session;

pub use client::{IngestFuture, IngestionClient, IngestionError};
use restate_types::sharding::PartitionKey;
pub use session::{
    CancelledError, RecordCommit, SessionClosed, SessionOptions, SessionOptionsBuilder,
};

use crate::client::InputRecord;

pub trait Ingestion<V> {
    type Future: Future<Output = Result<RecordCommit, IngestionError>>;

    fn ingest(
        &mut self,
        partition_key: PartitionKey,
        record: impl Into<InputRecord<V>>,
    ) -> Self::Future;
}

#[cfg(any(test, feature = "test-util"))]
pub mod test {
    use std::marker::PhantomData;

    use futures::{FutureExt, future::BoxFuture};
    use restate_types::logs::Keys;

    use crate::{Ingestion, IngestionError, RecordCommit, client::InputRecord};

    pub trait MockIngestHandler<V> {
        fn handle(
            &mut self,
            keys: Keys,
            record: V,
        ) -> impl Future<Output = Result<RecordCommit, IngestionError>> + Send + Sync + 'static;
    }

    impl<F, Fut, V> MockIngestHandler<V> for F
    where
        F: FnMut(Keys, V) -> Fut,
        Fut: Future<Output = Result<RecordCommit, IngestionError>> + Send + Sync + 'static,
    {
        fn handle(
            &mut self,
            keys: Keys,
            record: V,
        ) -> impl Future<Output = Result<RecordCommit, IngestionError>> + Send + Sync + 'static
        {
            self(keys, record)
        }
    }

    pub struct MockIngestionClient<F, V> {
        handler: F,
        _p: PhantomData<V>,
    }

    impl<F, V> MockIngestionClient<F, V>
    where
        F: MockIngestHandler<V>,
    {
        pub fn new(handler: F) -> Self {
            Self {
                handler,
                _p: PhantomData,
            }
        }
    }

    impl<F, V> Ingestion<V> for MockIngestionClient<F, V>
    where
        F: MockIngestHandler<V> + Send + 'static,
        V: Send + 'static,
    {
        type Future = BoxFuture<'static, Result<RecordCommit, IngestionError>>;

        fn ingest(
            &mut self,
            _partition_key: restate_types::sharding::PartitionKey,
            record: impl Into<crate::client::InputRecord<V>>,
        ) -> Self::Future {
            let InputRecord { keys, record } = record.into();
            self.handler.handle(keys, record).boxed()
        }
    }

    #[tokio::test]
    async fn smoke_test() {
        let mut client = MockIngestionClient::new(async |_keys, value: String| {
            assert_eq!(value, "hello");

            Ok(RecordCommit::resolved())
        });

        let commit = client
            .ingest(0, InputRecord::from_str("hello"))
            .await
            .unwrap();

        assert!(commit.await.is_ok());
    }
}
