// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;

use restate_limiter::rule_book::RuleBookObserver;
use restate_limiter::{PersistedRule, RuleBook, RulePattern};
use restate_metadata_store::MetadataStoreClient;
use restate_types::metadata_store::keys::RULE_BOOK_KEY;
use restate_types::{Version, Versioned};
use restate_util_string::ReString;

use super::row::append_rule_row;
use super::schema::{SysRulesBuilder, SysRulesTable};
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

impl SysRulesTable {
    pub(crate) fn create_provider(
        metadata_store_client: MetadataStoreClient,
        rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    ) -> Arc<dyn TableProvider> {
        let table = GenericTableProvider::new(
            SysRulesBuilder::schema(),
            Arc::new(RulesScanner {
                metadata_store_client,
                rule_book_observer,
            }),
        );
        Arc::new(table)
    }
}

#[derive(Clone, derive_more::Debug)]
#[debug("RulesScanner")]
struct RulesScanner {
    metadata_store_client: MetadataStoreClient,
    rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
}

impl RulesScanner {
    async fn send_rule_batches(
        rules: impl IntoIterator<Item = (&RulePattern<ReString>, &PersistedRule)>,
        schema: SchemaRef,
        batch_size: usize,
        tx: Sender<Result<RecordBatch, DataFusionError>>,
    ) {
        let mut builder = SysRulesBuilder::new(schema);
        for (id, rule) in rules.into_iter() {
            append_rule_row(&mut builder, id, rule);
            if builder.num_rows() >= batch_size {
                let batch = builder.finish_and_new();
                if tx.send(batch).await.is_err() {
                    return;
                }
            }
        }
        if !builder.empty() {
            let _ = tx.send(builder.finish()).await;
        }
    }
}

impl Scan for RulesScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let client = self.metadata_store_client.clone();
        let observer = self.rule_book_observer.clone();

        stream_builder.spawn(async move {
            let cached_rule_book = observer.as_ref().map(|observer| observer.get());

            if let Some(cached_rule_book) = cached_rule_book
                && cached_rule_book.version() != Version::INVALID
            {
                Self::send_rule_batches(cached_rule_book.iter(), schema, batch_size, tx).await;
            } else {
                let book = client
                    .get::<RuleBook>(RULE_BOOK_KEY.clone())
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .unwrap_or_default();

                Self::send_rule_batches(book.iter(), schema, batch_size, tx).await;

                if let Some(observer) = observer {
                    observer.notify_observed(book);
                }
            }

            Ok(())
        });
        stream_builder.build()
    }
}
