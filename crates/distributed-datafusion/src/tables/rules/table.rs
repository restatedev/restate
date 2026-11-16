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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;

use restate_limiter::RuleBook;
use restate_limiter::rule_book::RuleBookObserver;
use restate_metadata_store::MetadataStoreClient;
use restate_storage_query_api::tables::rules::{Rules, RulesColumn};
use restate_types::metadata_store::keys::RULE_BOOK_KEY;
use restate_types::{Version, Versioned};

use super::row::append_rule_row;
use super::schema::{SysRulesBuilder, SysRulesTable};
use crate::source_registry::SourceRegistry;
use crate::statistics::{RULES_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{GenericTableProvider, Scan};

impl SysRulesTable {
    pub(crate) fn create_provider(sources: SourceRegistry) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<Rules>::new()
            .with_num_rows_estimate(RULES_ROW_ESTIMATE)
            .with_primary_key(RulesColumn::Pattern);
        Arc::new(
            GenericTableProvider::declared::<Self>(None, Some(sources))
                .with_statistics(statistics.build()),
        )
    }

    /// Reads the co-located worker's cached rule book when it has observed one;
    /// otherwise reads the metadata store and shares the result with that cache.
    pub(crate) fn create_local_scanner(
        metadata_store_client: MetadataStoreClient,
        rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    ) -> impl Scan {
        RulesScanner {
            metadata_store_client,
            rule_book_observer,
        }
    }
}

#[derive(Clone, derive_more::Debug)]
#[debug("RulesScanner")]
struct RulesScanner {
    metadata_store_client: MetadataStoreClient,
    rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
}

impl Scan for RulesScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let client = self.metadata_store_client.clone();
        let observer = self.rule_book_observer.clone();

        stream_builder.spawn(async move {
            let cached = observer
                .as_ref()
                .map(|observer| observer.get())
                .filter(|book| book.version() != Version::INVALID);
            if let Some(book) = cached {
                send_rule_batches(&book, schema, batch_size, limit, tx).await;
            } else {
                let book = client
                    .get::<RuleBook>(RULE_BOOK_KEY.clone())
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .unwrap_or_default();
                send_rule_batches(&book, schema, batch_size, limit, tx).await;
                if let Some(observer) = observer {
                    observer.notify_observed(book);
                }
            }
            Ok(())
        });
        stream_builder.build()
    }
}

async fn send_rule_batches(
    book: &RuleBook,
    schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
) {
    let mut builder = SysRulesBuilder::new(schema);
    for (pattern, rule) in book.iter().take(limit.unwrap_or(usize::MAX)) {
        append_rule_row(&mut builder, pattern, rule);
        // Stop producing once the consumer hangs up.
        if builder.num_rows() >= batch_size && tx.send(builder.finish()).await.is_err() {
            return;
        }
    }
    if builder.num_rows() > 0 {
        let _ = tx.send(builder.into_batch()).await;
    }
}
